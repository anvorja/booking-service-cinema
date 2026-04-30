"""initial_schema — baseline

Crea el esquema original de cinema_booking en el estado que tenía antes
de que se introdujeran las migraciones posteriores (schema_align, etc.).

Esta revisión ahora puede correr sobre una BD completamente vacía:
  alembic upgrade head    → crea tablas + aplica todas las migraciones

Para una BD que ya tiene tablas (ej. creada por el servicio en startup):
  alembic stamp head      → marca como al día sin ejecutar DDL

Para una BD existente que llegó hasta aquí por su cuenta (legacy):
  alembic stamp db52472f73cb
  alembic upgrade head

Revision ID: db52472f73cb
Revises:
Create Date: 2026-04-15 10:08:42.550061
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'db52472f73cb'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Crea las 4 tablas de cinema_booking en su estado original (pre-schema_align).

    Es idempotente: si las tablas ya existen (BD creada por el servicio vía
    create_all), las operaciones se saltan. schema_align corrige el esquema
    resultante en ambos casos.
    """
    bind = op.get_bind()
    insp = sa.inspect(bind)
    existing = set(insp.get_table_names())

    # ── ENUM types ────────────────────────────────────────────────────────────
    purchasestatus = sa.Enum(
        'pending', 'confirmed', 'cancelled', 'refunded',
        name='purchasestatus',
    )
    ticketstatus = sa.Enum(
        'active', 'used', 'cancelled',
        name='ticketstatus',
    )
    purchasestatus.create(bind, checkfirst=True)
    ticketstatus.create(bind, checkfirst=True)

    # ── users (referencia local de auth-service) ──────────────────────────────
    if 'users' not in existing:
        op.create_table(
            'users',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('email', sa.String(255), nullable=False),
            sa.Column('first_name', sa.String(100), nullable=False),
            sa.Column('last_name', sa.String(100), nullable=False),
            sa.Column('phone', sa.String(20), nullable=False,
                      server_default=sa.text("''")),
            sa.Column('role', sa.String(20), nullable=False,
                      server_default=sa.text("'customer'")),
            sa.Column('is_active', sa.Boolean(), nullable=False,
                      server_default=sa.text('true')),
            sa.Column('created_at', sa.DateTime(timezone=True),
                      server_default=sa.func.now(), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True),
                      server_default=sa.func.now(), nullable=False),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('email', name='users_email_key'),
        )
        op.create_index('ix_users_email', 'users', ['email'], unique=True)

    # ── movies (referencia local de catalog-service) ──────────────────────────
    # genre/duration/rating se definen NOT NULL aquí; schema_align los hará nullable.
    if 'movies' not in existing:
        op.create_table(
            'movies',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('title', sa.String(200), nullable=False),
            sa.Column('genre', sa.String(50), nullable=False),
            sa.Column('duration', sa.Integer(), nullable=False),
            sa.Column('rating', sa.String(10), nullable=False),
            sa.Column('price', sa.Float(), nullable=False,
                      server_default=sa.text('0.0')),
            sa.Column('available_tickets', sa.Integer(), nullable=False,
                      server_default=sa.text('100')),
            sa.Column('max_capacity', sa.Integer(), nullable=False,
                      server_default=sa.text('100')),
            sa.Column('poster_url', sa.String(1000), nullable=True),
            sa.Column('is_active', sa.Boolean(), nullable=False,
                      server_default=sa.text('true')),
            sa.Column('created_at', sa.DateTime(timezone=True),
                      server_default=sa.func.now(), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True),
                      server_default=sa.func.now(), nullable=False),
            sa.PrimaryKeyConstraint('id'),
        )

    # ── purchases ─────────────────────────────────────────────────────────────
    if 'purchases' not in existing:
        op.create_table(
            'purchases',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('user_id', sa.Integer(), nullable=False),
            sa.Column('movie_id', sa.Integer(), nullable=False),
            sa.Column('quantity', sa.Integer(), nullable=False),
            sa.Column('total_amount', sa.Float(), nullable=False),
            sa.Column('payment_info', sa.JSON(), nullable=True),
            sa.Column('status', purchasestatus, nullable=False,
                      server_default=sa.text("'confirmed'")),
            sa.Column('show_date', sa.Date(), nullable=True),
            sa.Column('show_time', sa.String(10), nullable=True),
            sa.Column('showtime_id', sa.Integer(), nullable=True),
            sa.Column('is_active', sa.Boolean(), nullable=False,
                      server_default=sa.text('true')),
            sa.Column('created_at', sa.DateTime(timezone=True),
                      server_default=sa.func.now(), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True),
                      server_default=sa.func.now(), nullable=False),
            sa.ForeignKeyConstraint(['movie_id'], ['movies.id']),
            sa.ForeignKeyConstraint(['user_id'], ['users.id']),
            sa.PrimaryKeyConstraint('id'),
        )
        op.create_index('ix_purchases_user_id',  'purchases', ['user_id'])
        op.create_index('ix_purchases_movie_id', 'purchases', ['movie_id'])

    # ── tickets ───────────────────────────────────────────────────────────────
    # is_active se incluye aquí; schema_align la eliminará del esquema real.
    # ticket_code usa UNIQUE CONSTRAINT (no index); schema_align lo reemplaza.
    if 'tickets' not in existing:
        op.create_table(
            'tickets',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('purchase_id', sa.Integer(), nullable=False),
            sa.Column('ticket_code', sa.String(20), nullable=False),
            sa.Column('seat_number', sa.String(20), nullable=False),
            sa.Column('status', ticketstatus, nullable=False,
                      server_default=sa.text("'active'")),
            sa.Column('showtime_id', sa.Integer(), nullable=True),
            sa.Column('is_active', sa.Boolean(), nullable=False,
                      server_default=sa.text('true')),
            sa.Column('created_at', sa.DateTime(timezone=True),
                      server_default=sa.func.now(), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True),
                      server_default=sa.func.now(), nullable=False),
            sa.ForeignKeyConstraint(['purchase_id'], ['purchases.id'],
                                    ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('ticket_code', name='tickets_ticket_code_key'),
        )
        op.create_index('ix_tickets_purchase_id', 'tickets', ['purchase_id'])


def downgrade() -> None:
    """Elimina las tablas creadas por upgrade. NO ejecutar sobre BDs con datos."""
    bind = op.get_bind()
    insp = sa.inspect(bind)
    existing = set(insp.get_table_names())

    for table in ('tickets', 'purchases', 'movies', 'users'):
        if table in existing:
            op.drop_table(table)

    sa.Enum(name='ticketstatus').drop(bind, checkfirst=True)
    sa.Enum(name='purchasestatus').drop(bind, checkfirst=True)
