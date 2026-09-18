# booking-service-cinema

Orquestador de la saga de compra: crea la orden, coordina inventario y pago
por eventos, confirma o cancela, y gestiona reembolsos y validación de
boletos.

## Responsabilidad

Dueño de `cinema_booking` — la única base de datos del stack con
migraciones **Alembic** (los demás servicios usan `create_all` de
SQLAlchemy). Además de sus propias tablas (`purchases`, `tickets`),
mantiene copias locales de referencia de `users` y `movies`, sincronizadas
por Kafka desde `auth-service`/`admin-service` — para poder validar una
compra sin llamadas HTTP síncronas a esos servicios.

## Stack

FastAPI + SQLAlchemy 2.0 + Alembic, Postgres, `aiokafka`, `httpx`, puerto
`8004`. Ver `requirements.txt`.

## API

| Método | Ruta | Qué hace |
|---|---|---|
| `POST` | `/api/v1/purchases` | Crea la compra en `PENDING` y publica `order.created` (arranca la saga) |
| `GET` | `/api/v1/purchases` | Historial de compras del usuario autenticado |
| `GET` | `/api/v1/purchases/{id}` | Detalle de una compra (solo el dueño) |
| `POST` | `/api/v1/purchases/{id}/cancel` | Reembolsa una compra `CONFIRMED` (bloquea si falta <30 min para la función) |
| `POST` | `/api/v1/purchases/tickets/{ticket_code}/validate` | Valida un boleto en sala (rol `admin`/`scanner`); detecta y anula boletos duplicados por doble venta |
| `GET` | `/api/v1/purchases/showtimes/{id}/occupied-seats` | Asientos ocupados de una función, con fallback para datos históricos sin `showtime_id` |
| `GET` | `/api/v1/purchases/internal/movies/{id}/used-ticket` | Interno, sin auth — usado por `catalog-service` para reseñas |
| `GET` | `/health` | Estado del servicio + si el consumer de Kafka y el reconciler siguen vivos |

## Eventos Kafka

Detalle de payload y semántica de cada uno en
`../kafka-schemas-cinema/event_contracts_operativos.md`.

**Publica:**
- `order.created` — arranca la saga, al crear la compra
- `payment.initiated` — dispara el cobro (con datos de tarjeta/PSE) tras `inventory.reserved`
- `purchase.confirmed` — solo si la compra se confirmó sin conflictos; dispara el email de confirmación
- `inventory.release` — compensación cuando el pago falla
- `order.refunded` — al cancelar una compra confirmada
- `ticket.validated` — al escanear un boleto en sala

**Consume:**
`inventory.reserved`, `inventory.insufficient`, `payment.success`,
`payment.failed`, `movie.created`, `movie.updated`, `movie.deactivated`,
`user.registered`, `user.deactivated` — estos dos últimos mantienen
sincronizada la copia local de `users`.

## Resiliencia de la saga

El contexto de cada compra en curso (montos, tarjeta, snapshot de la
película) vive en **Redis con TTL**, no solo en Postgres — es lo que le
permite a `trigger_payment_for_order` reintentar sin perder datos. Un
**guard de vuelo** evita reiniciar un pago si el intento anterior es
reciente. Todos los handlers son idempotentes por `order_id` (`ON CONFLICT`,
chequeos de estado antes de actuar), por lo que el consumer arranca con
`auto_offset_reset="earliest"` y puede reprocesar sin duplicar efectos.

Un **reconciler** (`reconcile_pending_purchases`, tarea de fondo en
`app/main.py`, cada `PAYMENT_RETRY_INTERVAL_SECONDS`) barre las compras
`PENDING` y decide qué hacer si un evento se perdió: reintenta el pago,
verifica si `inventory-service` ya reservó stock sin que el evento llegara,
o cancela liberando lo que corresponda tras `PAYMENT_FLOW_STALE_SECONDS` /
`INVENTORY_DECISION_TIMEOUT_SECONDS`.

Las llamadas a `payment-service` para reembolsos pasan por un **circuit
breaker respaldado en Redis** (`app/core/circuit_breaker.py`): tras 3 fallos
consecutivos abre por 60s y responde `503` sin intentar la llamada.

## Migraciones (Alembic)

```bash
alembic current          # revisión actual de la BD
alembic upgrade head     # aplicar migraciones pendientes
```

`app/main.py` ya hace esto automáticamente al arrancar (estampa la
revisión base si la BD nunca pasó por Alembic, luego `upgrade head`). Guía
completa de restauración/migración de la BD:
`../db_asuntos/docDBcambios.md` (Parte 6 para el detalle de comandos).

## Variables de entorno clave

| Variable | Para qué sirve |
|---|---|
| `DATABASE_URL` | Postgres de `cinema_booking` — misma instancia Railway `mainline` que auth/catalog/user (discrepancia corregida 2026-09-18, ver `../IMPLEMENTATION-GUIDE.md` Fase 0) |
| `JWT_SECRET` / `JWT_ALGORITHM` | Debe coincidir con `auth-service`, que es la fuente de verdad de autenticación |
| `KAFKA_ENABLED`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_API_KEY`, `KAFKA_API_SECRET` | Confluent Cloud — si `KAFKA_ENABLED=false` las compras se crean pero la saga nunca corre (queda advertido en el log de arranque) |
| `PAYMENT_SERVICE_URL` | Para reembolsos (HTTP, no Kafka) |
| `CATALOG_SERVICE_URL` | Para validar showtimes al crear una compra |
| `PAYMENT_RETRY_INTERVAL_SECONDS`, `PAYMENT_MAX_INIT_ATTEMPTS`, `PAYMENT_FLOW_STALE_SECONDS`, `INVENTORY_DECISION_TIMEOUT_SECONDS` | Tuning del reconciler |
| `REDIS_URL` | Contexto de pago en curso, circuit breaker, y blacklist de JWT compartida con `auth-service` |

## Dependencias

- **HTTP** → `payment-service` (solo reembolsos; la iniciación del pago va por Kafka), `catalog-service` (validar showtime al crear la compra)
- **Kafka** → `inventory-service` (reserva/libera stock), `payment-service` (inicia/resuelve el cobro), `catalog-service` y `notification-service` (reaccionan a `purchase.confirmed`/`order.refunded`), `admin-service`/`auth-service` (sincronizan `movies`/`users` locales)

## Correr en local

```bash
# Standalone (requiere Postgres y Redis accesibles, ver .env)
uvicorn app.main:app --reload --port 8004

# Como parte del stack completo (recomendado)
cd ../infra-cinema
docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build booking-service
```
