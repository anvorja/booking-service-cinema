"""Alembic environment — booking-service-cinema.

Lee DATABASE_URL desde la variable de entorno (o el archivo .env a través de
app.core.config.settings) y registra todos los modelos SQLAlchemy del servicio
para que autogenerate detecte cambios de esquema correctamente.
"""
import os
import sys
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool
from alembic import context

# ── Asegurar que el paquete `app` sea importable desde cualquier CWD ──────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ── Importar configuración y modelos ─────────────────────────────────────────
from app.core.config import settings          # noqa: E402  — DATABASE_URL aquí
from app.models.base import Base              # noqa: E402  — DeclarativeBase
import app.models.user                        # noqa: E402,F401 — registrar tabla users
import app.models.movie                       # noqa: E402,F401 — registrar tabla movies
import app.models.purchase                    # noqa: E402,F401 — registrar tablas purchases + tickets

# ── Alembic Config object ────────────────────────────────────────────────────
config = context.config

# Inyectar la URL real desde settings (sobreescribe el placeholder de alembic.ini)
config.set_main_option("sqlalchemy.url", settings.DATABASE_URL)

# Configurar logging según alembic.ini
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Metadata objetivo para autogenerate
target_metadata = Base.metadata


# ── Runners ──────────────────────────────────────────────────────────────────

def run_migrations_offline() -> None:
    """Modo offline: genera SQL sin conectarse a la BD."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Modo online: conecta a la BD y aplica migraciones."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
