# app/main.py
import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.kafka.producer import start_producer, stop_producer
from app.api.purchases import router as purchases_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

_consumer_task: asyncio.Task | None = None
_reconciler_task: asyncio.Task | None = None


async def _run_pending_purchase_reconciler() -> None:
    from app.core.database import SessionLocal
    from app.services.booking import reconcile_pending_purchases

    while True:
        try:
            await reconcile_pending_purchases(SessionLocal)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Pending purchase reconciler error: %s", exc)
        await asyncio.sleep(settings.PAYMENT_RETRY_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global _consumer_task, _reconciler_task
    logger.info("Starting Booking Service...")

    from app.models.base import Base
    from app.core.database import engine
    from sqlalchemy import text as _text
    from alembic.config import Config as AlembicConfig
    from alembic import command as alembic_command

    # create_all garantiza que las tablas existan en BDs nuevas (dev/test).
    # En producción ya existen; esta llamada es idempotente.
    Base.metadata.create_all(bind=engine)

    # Ensure the partial unique index exists (idempotent; create_all skips
    # indexes on already-existing tables).
    with engine.connect() as _conn:
        _conn.execute(_text(
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_ticket_showtime_seat "
            "ON tickets (showtime_id, seat_number) "
            "WHERE showtime_id IS NOT NULL"
        ))
        _conn.commit()

    # Ejecutar migraciones Alembic pendientes.
    # Si la BD no tiene historial Alembic (p.ej. creada con create_all sin
    # haber pasado por Alembic antes), se estampa en la revisión base primero.
    _alembic_ini = os.path.join(os.path.dirname(__file__), '..', 'alembic.ini')
    _alembic_cfg = AlembicConfig(_alembic_ini)
    with engine.connect() as _conn:
        _has_history = _conn.execute(_text(
            "SELECT EXISTS ("
            "  SELECT FROM information_schema.tables "
            "  WHERE table_name = 'alembic_version'"
            ")"
        )).scalar()
    if not _has_history:
        logger.info("Alembic: no migration history found — stamping baseline db52472f73cb")
        alembic_command.stamp(_alembic_cfg, 'db52472f73cb')
    logger.info("Alembic: running upgrade head")
    alembic_command.upgrade(_alembic_cfg, 'head')

    if not settings.KAFKA_ENABLED:
        logger.warning(
            "KAFKA_ENABLED=false — Kafka producer and consumer are DISABLED. "
            "Purchases will be created but the saga (inventory → payment → confirm) "
            "will NEVER run. Set KAFKA_ENABLED=true in Render env vars."
        )

    await start_producer()

    from app.kafka.consumer import start_consumer
    from app.core.database import SessionLocal
    _consumer_task = asyncio.create_task(start_consumer(SessionLocal))
    _reconciler_task = asyncio.create_task(_run_pending_purchase_reconciler())

    logger.info("Booking Service ready")
    yield

    logger.info("Shutting down Booking Service...")
    if _consumer_task:
        _consumer_task.cancel()
        try:
            await _consumer_task
        except asyncio.CancelledError:
            pass
    if _reconciler_task:
        _reconciler_task.cancel()
        try:
            await _reconciler_task
        except asyncio.CancelledError:
            pass
    await stop_producer()


app = FastAPI(title="Booking Service", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(purchases_router)


@app.get("/health")
async def health():
    consumer_running = _consumer_task is not None and not _consumer_task.done()
    reconciler_running = _reconciler_task is not None and not _reconciler_task.done()
    return {
        "status": "healthy",
        "service": "booking-service",
        "kafka_consumer": "running" if consumer_running else "stopped",
        "pending_reconciler": "running" if reconciler_running else "stopped",
    }
