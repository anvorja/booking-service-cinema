"""initial_schema — baseline

Revisión de referencia que representa el esquema de la BD tal como fue
creada originalmente por Base.metadata.create_all() sin Alembic.

Para una BD ya existente (producción), ejecutar:
    alembic stamp db52472f73cb

Para una BD nueva (dev/test), la secuencia de startup es:
    1. create_all() crea las tablas con el esquema actual de los modelos.
    2. alembic stamp db52472f73cb  — marcar como ya en este punto de base.
    3. alembic upgrade head        — aplicar migraciones posteriores.

Revision ID: db52472f73cb
Revises:
Create Date: 2026-04-15 10:08:42.550061
"""
from typing import Sequence, Union

revision: str = 'db52472f73cb'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """No-op — baseline para BDs existentes creadas con create_all."""
    pass


def downgrade() -> None:
    """No-op — baseline no tiene estado previo al que volver."""
    pass
