# app/core/config.py
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SERVICE_NAME: str = "booking-service"

    # BD principal del stack (cinema_booking y cinema_catalog viven en el mismo cluster Postgres)
    DATABASE_URL: str

    # JWT — mismo secret que auth-service (fuente de verdad de autenticación)
    JWT_SECRET: str
    JWT_ALGORITHM: str = "HS256"

    # Kafka
    KAFKA_ENABLED: bool = False
    KAFKA_BOOTSTRAP_SERVERS: str = ""
    KAFKA_API_KEY: str = ""
    KAFKA_API_SECRET: str = ""

    # payment-service URL
    PAYMENT_SERVICE_URL: str = "http://payment-service:8003"
    # catalog-service URL — para validar showtimes al crear una compra
    CATALOG_SERVICE_URL: str = "http://catalog-service:8006"
    # También es el intervalo del loop del reconciliador (ver
    # _run_pending_purchase_reconciler en main.py, que duerme este mismo
    # valor entre corridas) — subido de 30 a 60s tras ver en producción que
    # el round-trip real payment.initiated -> payment-service procesa ->
    # publica payment.success -> booking lo consume puede tardar más de
    # 30s. Con 30s, el reconciliador reintentaba (publicando un
    # payment.initiated nuevo) ANTES de que la respuesta del intento
    # anterior llegara — payment-service aprobó la misma orden dos veces
    # (dos transacciones simuladas distintas) y ambas se perdieron porque
    # confirm_purchase_from_payment descarta cualquier payment.success que
    # llegue cuando la compra ya no está PENDING (correcto para evitar
    # revivir una cancelada, pero evidencia de que el reintento fue
    # prematuro). No es una solución completa — sin un idempotency key por
    # intento en payment-service, retriggerar sigue pudiendo producir una
    # aprobación duplicada si el primer intento igual estaba en camino —
    # pero reduce mucho la ventana real de colisión.
    PAYMENT_RETRY_INTERVAL_SECONDS: int = 60
    PAYMENT_MAX_INIT_ATTEMPTS: int = 3
    # Catch-all final para cualquier estado que ninguna otra rama del
    # reconciliador resuelva antes. Da margen sobre los ~180s que tardan los
    # 3 reintentos de payment (ver rama awaiting_payment_result) y el
    # timeout de inventario (2 min) — muy por debajo de los 15 min de antes.
    PAYMENT_FLOW_STALE_SECONDS: int = 5 * 60
    INVENTORY_DECISION_TIMEOUT_SECONDS: int = 2 * 60

    # Redis — shared blacklist with auth-service (key: blacklist:<sha256>)
    REDIS_URL: str = ""

    # Secreto compartido para autenticar llamadas servicio-a-servicio a
    # rutas /internal/* (Traefik no filtra esas rutas, y en producción cada
    # servicio de Render es alcanzable por su URL pública sin pasar por
    # Traefik — ver ARCHITECTURE.md, "Aislamiento de base de datos por
    # servicio"). Debe coincidir con el mismo valor en catalog-service y
    # user-service (quienes llaman a estas rutas).
    INTERNAL_SERVICE_TOKEN: str = ""

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }


settings = Settings()
