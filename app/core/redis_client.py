# app/core/redis_client.py — Redis client: blacklist check + seat holds
import hashlib
import logging
from typing import List, Optional
from .config import settings

logger = logging.getLogger(__name__)

_SEAT_HOLD_TTL = 900  # 15 minutos

_client = None


def _get_client():
    global _client
    if _client is not None:
        return _client
    if not settings.REDIS_URL:
        return None
    try:
        import redis
        c = redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_timeout=1,
            socket_connect_timeout=1,
        )
        c.ping()
        _client = c
        logger.info("Redis connected (booking redis_client)")
        return _client
    except Exception as exc:
        logger.warning("Redis unavailable — booking redis_client disabled (%s)", exc)
        return None


def is_blacklisted(token: str) -> bool:
    """Return True if token has been blacklisted by auth-service."""
    client = _get_client()
    if client is None:
        return False
    try:
        digest = hashlib.sha256(token.encode()).hexdigest()
        return client.exists(f"blacklist:{digest}") == 1
    except Exception as exc:
        logger.error("Blacklist check error: %s", exc)
        return False


def hold_seat(showtime_id: int, seat_code: str, order_id: int, ttl: int = _SEAT_HOLD_TTL) -> bool:
    """
    SETNX atómico para reservar temporalmente un asiento.
    Retorna True si el hold fue adquirido, False si ya estaba tomado por otro usuario.
    """
    client = _get_client()
    if client is None:
        # Sin Redis no podemos garantizar unicidad — rechazar para evitar doble venta
        logger.error("Redis no disponible para hold_seat | showtime_id=%s seat=%s", showtime_id, seat_code)
        return False
    try:
        key = f"seat:hold:{showtime_id}:{seat_code}"
        result = client.set(key, str(order_id), ex=ttl, nx=True)
        return result is True
    except Exception as exc:
        logger.error("hold_seat error | showtime_id=%s seat=%s | %s", showtime_id, seat_code, exc)
        return False


def reassign_hold(showtime_id: int, seat_code: str, new_order_id: int) -> None:
    """
    Actualiza el valor de un hold existente al order_id real sin liberar la clave.
    Usa SET sin NX para no crear una ventana de carrera entre release + re-acquire.
    """
    client = _get_client()
    if client is None:
        return
    try:
        key = f"seat:hold:{showtime_id}:{seat_code}"
        client.set(key, str(new_order_id), keepttl=True)
    except Exception as exc:
        logger.error("reassign_hold error | showtime_id=%s seat=%s | %s", showtime_id, seat_code, exc)


def release_seat(showtime_id: int, seat_code: str) -> None:
    """Elimina el hold de un asiento. Seguro de llamar aunque no exista."""
    client = _get_client()
    if client is None:
        return
    try:
        client.delete(f"seat:hold:{showtime_id}:{seat_code}")
    except Exception as exc:
        logger.error("release_seat error | showtime_id=%s seat=%s | %s", showtime_id, seat_code, exc)


def inventory_was_reserved(order_id: int) -> bool:
    """
    Returns True if inventory-service already created a reservation for this order in Redis.
    Used by the reconciler to recover orders where the inventory.reserved Kafka event was
    published but the booking consumer missed it (e.g. consumer restarted with offset=latest).
    Both services share the same Redis instance; key format mirrors inventory-service.
    """
    client = _get_client()
    if client is None:
        return False
    try:
        return client.exists(f"inv:reservation:order:{order_id}") == 1
    except Exception as exc:
        logger.error("inventory_was_reserved check error | order_id=%s | %s", order_id, exc)
        return False


def release_seats_for_order(showtime_id: int, seat_codes: List[str]) -> None:
    """Elimina todos los holds de un order en un pipeline Redis."""
    if not seat_codes:
        return
    client = _get_client()
    if client is None:
        return
    try:
        pipe = client.pipeline(transaction=False)
        for code in seat_codes:
            pipe.delete(f"seat:hold:{showtime_id}:{code}")
        pipe.execute()
    except Exception as exc:
        logger.error(
            "release_seats_for_order error | showtime_id=%s seats=%s | %s",
            showtime_id, seat_codes, exc,
        )
