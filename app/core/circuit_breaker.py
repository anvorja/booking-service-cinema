# app/core/circuit_breaker.py
"""
Redis-backed circuit breaker for payment-service calls.

States:
  CLOSED     — normal operation
  OPEN       — failing fast, no calls for RECOVERY_TIMEOUT seconds
  HALF_OPEN  — one probe allowed after timeout expires

Redis keys:
  cb:payment:failures    — consecutive failure count (int, no TTL — cleared on success)
  cb:payment:open_until  — unix timestamp: circuit stays OPEN until this time
"""
import logging
from datetime import datetime, timezone

from app.core import redis_client

logger = logging.getLogger(__name__)

FAILURE_THRESHOLD = 3    # trips to OPEN after this many consecutive failures
RECOVERY_TIMEOUT  = 60   # seconds before HALF_OPEN probe


def _now() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _client():
    return redis_client._get_client()


def is_open() -> bool:
    """
    Return True if the circuit should block the call (OPEN state).
    HALF_OPEN returns False — the probe is allowed through.
    """
    client = _client()
    if client is None:
        return False   # Redis down → fail open (availability over safety in dev)
    try:
        raw = client.get("cb:payment:open_until")
        if raw is None:
            return False
        open_until = int(raw)
        if _now() < open_until:
            logger.warning(
                "Circuit OPEN — payment-service blocked for %ds more",
                open_until - _now(),
            )
            return True
        # Timeout expired → HALF_OPEN, let one probe through
        return False
    except Exception as exc:
        logger.error("Circuit breaker check error: %s", exc)
        return False


def record_success() -> None:
    """Reset circuit to CLOSED."""
    client = _client()
    if client is None:
        return
    try:
        client.delete("cb:payment:failures", "cb:payment:open_until")
        logger.info("Circuit CLOSED — payment-service recovered")
    except Exception as exc:
        logger.error("Circuit breaker success record error: %s", exc)


def record_failure() -> None:
    """Increment failure count and trip to OPEN if threshold reached."""
    client = _client()
    if client is None:
        return
    try:
        failures = client.incr("cb:payment:failures")
        logger.warning("Circuit failure recorded: %d/%d", failures, FAILURE_THRESHOLD)

        if failures >= FAILURE_THRESHOLD:
            open_until = _now() + RECOVERY_TIMEOUT
            client.set("cb:payment:open_until", open_until)
            logger.error(
                "Circuit OPEN — payment-service tripped after %d failures. "
                "Will retry in %ds",
                failures,
                RECOVERY_TIMEOUT,
            )
    except Exception as exc:
        logger.error("Circuit breaker failure record error: %s", exc)
