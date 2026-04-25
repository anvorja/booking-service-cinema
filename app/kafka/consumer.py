# app/kafka/consumer.py — booking-service
#
# Consume eventos de dominio que afectan la capa de reservas:
#   • user.registered → inserta/actualiza usuario en cinema_booking.users
#     (publicado por auth-service al registrar un usuario nuevo)
#   • user.deactivated → marca usuario inactivo en cinema_booking.users
#     (publicado por admin-service o user-service al desactivar una cuenta)
#
# La tabla cinema_booking.users es una referencia local (sin password_hash)
# que se mantiene sincronizada vía Kafka con cinema_auth como fuente de verdad.
#
import asyncio
import json
import logging
import ssl
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer
from app.core.config import settings
from app.services.booking import (
    confirm_purchase_from_payment,
    fail_purchase_from_payment,
    mark_purchase_cancelled,
    trigger_payment_for_order,
)

logger = logging.getLogger(__name__)


async def _handle_user_registered(payload: dict, db_factory) -> None:
    """
    Inserta o actualiza el usuario en cinema_booking.users.
    Idempotente: usa ON CONFLICT DO UPDATE para reenvíos seguros.
    """
    from sqlalchemy import text

    required = {"id", "email", "first_name", "last_name", "phone", "role"}
    if not required.issubset(payload):
        logger.warning("user.registered payload incompleto: %s", payload)
        return

    with db_factory() as db:
        db.execute(
            text("""
                INSERT INTO users
                    (id, email, first_name, last_name, phone, role,
                     is_active, created_at, updated_at)
                VALUES
                    (:id, :email, :first_name, :last_name, :phone, :role,
                     true, now(), now())
                ON CONFLICT (id) DO UPDATE
                    SET email      = EXCLUDED.email,
                        first_name = EXCLUDED.first_name,
                        last_name  = EXCLUDED.last_name,
                        phone      = EXCLUDED.phone,
                        role       = EXCLUDED.role,
                        updated_at = now()
            """),
            {
                "id":         payload["id"],
                "email":      payload["email"],
                "first_name": payload["first_name"],
                "last_name":  payload["last_name"],
                "phone":      payload["phone"],
                "role":       payload["role"],
            },
        )
        db.commit()
    logger.info("Usuario sincronizado en cinema_booking | user_id=%s", payload["id"])


async def _handle_user_deactivated(payload: dict, db_factory) -> None:
    """Marca el usuario como inactivo en cinema_booking.users."""
    from sqlalchemy import text

    user_id = payload.get("user_id")
    if not user_id:
        logger.warning("user.deactivated payload sin user_id: %s", payload)
        return

    with db_factory() as db:
        db.execute(
            text("UPDATE users SET is_active = false, updated_at = now() WHERE id = :uid"),
            {"uid": user_id},
        )
        db.commit()
    logger.info("Usuario desactivado en cinema_booking | user_id=%s", user_id)


_ALLOWED_MOVIE_FIELDS = {"price", "available_tickets", "max_capacity", "title", "genre", "duration", "rating", "poster_url"}


async def _handle_movie_updated(payload: dict, db_factory) -> None:
    """
    Recibe movie.updated publicado por admin-service.
    Actualiza los campos relevantes en cinema_booking.movies.
    Solo actualiza campos presentes en _ALLOWED_MOVIE_FIELDS para evitar
    sobreescribir columnas que booking gestiona de forma independiente.
    """
    from sqlalchemy import text

    movie_id = payload.get("movie_id")
    if not movie_id:
        logger.warning("movie.updated payload sin movie_id: %s", payload)
        return

    fields = {k: v for k, v in payload.items() if k in _ALLOWED_MOVIE_FIELDS and v is not None}
    if not fields:
        logger.info("movie.updated sin campos aplicables | movie_id=%s", movie_id)
        return

    set_clause = ", ".join(f"{k} = :{k}" for k in fields)
    with db_factory() as db:
        result = db.execute(
            text(f"UPDATE movies SET {set_clause}, updated_at = now() WHERE id = :movie_id"),
            {"movie_id": movie_id, **fields},
        )
        db.commit()

    if result.rowcount:
        logger.info("cinema_booking.movies actualizado por movie.updated | movie_id=%s campos=%s", movie_id, list(fields.keys()))
    else:
        logger.warning("movie.updated: película no encontrada en cinema_booking | movie_id=%s", movie_id)


async def _handle_movie_created(payload: dict, db_factory) -> None:
    """
    Inserta la nueva película en cinema_booking.movies cuando admin-service la crea.
    Idempotente: usa ON CONFLICT DO NOTHING para reenvíos.
    """
    from sqlalchemy import text

    movie_id = payload.get("movie_id")
    if not movie_id:
        logger.warning("movie.created payload sin movie_id: %s", payload)
        return

    with db_factory() as db:
        db.execute(
            text("""
                INSERT INTO movies
                    (id, title, genre, duration, rating, price,
                     available_tickets, max_capacity, poster_url,
                     is_active, created_at, updated_at)
                VALUES
                    (:id, :title, :genre, :duration, :rating, :price,
                     :available_tickets, :max_capacity, :poster_url,
                     true, now(), now())
                ON CONFLICT (id) DO NOTHING
            """),
            {
                "id":                movie_id,
                "title":             payload.get("title", ""),
                "genre":             payload.get("genre"),
                "duration":          payload.get("duration"),
                "rating":            payload.get("rating"),
                "price":             payload.get("price", 0.0),
                "available_tickets": payload.get("available_tickets", 0),
                "max_capacity":      payload.get("max_capacity", 0),
                "poster_url":        payload.get("poster_url"),
            },
        )
        db.commit()
    logger.info("Película insertada en cinema_booking por movie.created | movie_id=%s", movie_id)


async def _handle_movie_deactivated(payload: dict, db_factory) -> None:
    """Marca la película como inactiva en cinema_booking.movies para bloquear nuevas compras."""
    from sqlalchemy import text

    movie_id = payload.get("movie_id")
    if not movie_id:
        logger.warning("movie.deactivated payload sin movie_id: %s", payload)
        return

    with db_factory() as db:
        db.execute(
            text("UPDATE movies SET is_active = false, updated_at = now() WHERE id = :mid"),
            {"mid": movie_id},
        )
        db.commit()
    logger.info("Película desactivada en cinema_booking por movie.deactivated | movie_id=%s", movie_id)


async def _handle_inventory_reserved(payload: dict, db_factory) -> None:
    order_id = payload.get("order_id")
    if not order_id:
        logger.warning("inventory.reserved payload sin order_id: %s", payload)
        return

    # Idempotencia explícita: solo actuar si la compra sigue en PENDING.
    # Evita iniciar pago si inventory.reserved llega duplicado después del guard de vuelo.
    from app.models.purchase import Purchase, PurchaseStatus
    with db_factory() as db:
        purchase = db.query(Purchase).filter(Purchase.id == order_id).first()
        if not purchase:
            logger.warning("inventory.reserved para orden inexistente | order_id=%s", order_id)
            return
        if purchase.status != PurchaseStatus.PENDING:
            logger.info(
                "inventory.reserved ignorado — orden ya no está PENDING | order_id=%s status=%s",
                order_id, purchase.status.value,
            )
            return

    await trigger_payment_for_order(db_factory, order_id)


async def _handle_inventory_insufficient(payload: dict, db_factory) -> None:
    order_id = payload.get("order_id")
    if not order_id:
        logger.warning("inventory.insufficient payload sin order_id: %s", payload)
        return
    # mark_purchase_cancelled ya libera los holds Redis internamente
    await mark_purchase_cancelled(
        db_factory,
        order_id=order_id,
        reason="Inventario insuficiente para completar la compra.",
        release_inventory=False,
    )


async def _handle_payment_success(payload: dict, db_factory) -> None:
    await confirm_purchase_from_payment(db_factory, payload)


async def _handle_payment_failed(payload: dict, db_factory) -> None:
    await fail_purchase_from_payment(db_factory, payload)


# Registro único de todos los handlers. Cualquier topic que no aparezca aquí
# se ignora en silencio (el consumer hace commit del offset igualmente).
_HANDLERS: dict = {
    "user.registered":        _handle_user_registered,
    "user.deactivated":       _handle_user_deactivated,
    "movie.created":          _handle_movie_created,
    "movie.updated":          _handle_movie_updated,
    "movie.deactivated":      _handle_movie_deactivated,
    "inventory.reserved":     _handle_inventory_reserved,
    "inventory.insufficient": _handle_inventory_insufficient,
    "payment.success":        _handle_payment_success,
    "payment.failed":         _handle_payment_failed,
}

_RESTART_DELAY = 10


async def _send_to_dlq(topic: str, payload: dict, error: Exception) -> None:
    """Publica el mensaje fallido en el topic DLQ correspondiente antes de commitear el offset."""
    from app.kafka.producer import publish_event
    dlq_topic = f"{topic}.dlq"
    try:
        await publish_event(dlq_topic, {
            "original_topic": topic,
            "original_payload": payload,
            "error": str(error),
            "failed_at": datetime.now(timezone.utc).isoformat(),
        })
        logger.warning("Mensaje enviado al DLQ | dlq_topic=%s", dlq_topic)
    except Exception as dlq_exc:
        logger.error("No se pudo publicar al DLQ | dlq_topic=%s | error=%s", dlq_topic, dlq_exc)


async def _run_consumer(db_factory) -> None:
    ssl_context = ssl.create_default_context()
    consumer = AIOKafkaConsumer(
        *_HANDLERS.keys(),
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=settings.KAFKA_API_KEY,
        sasl_plain_password=settings.KAFKA_API_SECRET,
        ssl_context=ssl_context,
        group_id="booking-service-group",
        # "earliest" garantiza que si el consumer se reinicia antes de hacer commit
        # del offset (p.ej. durante un cold-start), no pierde mensajes ya publicados.
        # Todos los handlers son idempotentes, así que reprocesar es seguro.
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    await consumer.start()
    logger.info("Booking consumer iniciado | topics=%s", list(_HANDLERS.keys()))

    try:
        async for msg in consumer:
            handler = _HANDLERS.get(msg.topic)
            if handler:
                try:
                    await handler(msg.value, db_factory)
                except Exception as e:
                    logger.error("Error procesando %s: %s", msg.topic, e)
                    await _send_to_dlq(msg.topic, msg.value, e)
            await consumer.commit()
    except asyncio.CancelledError:
        raise
    finally:
        await consumer.stop()
        logger.info("Booking consumer detenido")


async def start_consumer(db_factory) -> None:
    if not settings.KAFKA_ENABLED:
        logger.info("Kafka deshabilitado — booking consumer no iniciado")
        return

    while True:
        try:
            await _run_consumer(db_factory)
            break
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("Booking consumer error: %s — reiniciando en %ds", e, _RESTART_DELAY)
            await asyncio.sleep(_RESTART_DELAY)
