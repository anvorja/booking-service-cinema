"""schema_align

Alinea el esquema real de la BD con el estado actual de los modelos
SQLAlchemy. Todas las operaciones son idempotentes: comprueban el estado
real antes de actuar para no fallar en BDs que ya tienen el esquema
correcto (creadas por create_all() con modelos actualizados).

Cambios aplicados
─────────────────
movies:
  - genre:    VARCHAR(50) NOT NULL → VARCHAR(100) NULLABLE
  - duration: NOT NULL → NULLABLE
  - rating:   NOT NULL → NULLABLE
  - agrega índice ix_movies_id

purchases:
  - agrega índices: ix_purchases_id, ix_purchases_movie_id,
                    ix_purchases_showtime_id, ix_purchases_status,
                    ix_purchases_user_id

tickets:
  - reemplaza la unique constraint tickets_ticket_code_key
    por el índice unique ix_tickets_ticket_code (semánticamente equivalente)
  - agrega índices: ix_tickets_id, ix_tickets_purchase_id,
                    ix_tickets_showtime_id, ix_tickets_status
  - elimina la columna is_active (redundante; sobreescrita como @property
    derivada del campo status en el modelo Ticket)

users:
  - agrega índice ix_users_id

Revision ID: eeb8bf47283c
Revises: db52472f73cb
Create Date: 2026-04-15 10:15:00.000000
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'eeb8bf47283c'
down_revision: Union[str, Sequence[str], None] = 'db52472f73cb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    # ── movies ────────────────────────────────────────────────────────────────
    movies_cols = {c['name']: c for c in insp.get_columns('movies')}

    if not movies_cols.get('genre', {}).get('nullable', True):
        op.alter_column('movies', 'genre',
                        existing_type=sa.VARCHAR(length=50),
                        type_=sa.String(length=100),
                        nullable=True)

    if not movies_cols.get('duration', {}).get('nullable', True):
        op.alter_column('movies', 'duration',
                        existing_type=sa.INTEGER(),
                        nullable=True)

    if not movies_cols.get('rating', {}).get('nullable', True):
        op.alter_column('movies', 'rating',
                        existing_type=sa.VARCHAR(length=10),
                        nullable=True)

    existing_movie_idx = {i['name'] for i in insp.get_indexes('movies')}
    if 'ix_movies_id' not in existing_movie_idx:
        op.create_index('ix_movies_id', 'movies', ['id'], unique=False)

    # ── purchases ─────────────────────────────────────────────────────────────
    existing_purchase_idx = {i['name'] for i in insp.get_indexes('purchases')}
    for idx_name, cols in [
        ('ix_purchases_id',          ['id']),
        ('ix_purchases_movie_id',    ['movie_id']),
        ('ix_purchases_showtime_id', ['showtime_id']),
        ('ix_purchases_status',      ['status']),
        ('ix_purchases_user_id',     ['user_id']),
    ]:
        if idx_name not in existing_purchase_idx:
            op.create_index(idx_name, 'purchases', cols, unique=False)

    # ── tickets ───────────────────────────────────────────────────────────────
    existing_ticket_idx = {i['name'] for i in insp.get_indexes('tickets')}
    existing_ticket_constraints = {
        c['name'] for c in insp.get_unique_constraints('tickets')
    }

    # Reemplazar unique constraint por unique index (equivalente, más flexible)
    if 'tickets_ticket_code_key' in existing_ticket_constraints:
        op.drop_constraint('tickets_ticket_code_key', 'tickets', type_='unique')

    if 'ix_tickets_ticket_code' not in existing_ticket_idx:
        op.create_index('ix_tickets_ticket_code', 'tickets', ['ticket_code'],
                        unique=True)

    for idx_name, cols in [
        ('ix_tickets_id',          ['id']),
        ('ix_tickets_purchase_id', ['purchase_id']),
        ('ix_tickets_showtime_id', ['showtime_id']),
        ('ix_tickets_status',      ['status']),
    ]:
        if idx_name not in existing_ticket_idx:
            op.create_index(idx_name, 'tickets', cols, unique=False)

    # Eliminar is_active de tickets — queda como @property en el modelo
    tickets_cols = {c['name'] for c in insp.get_columns('tickets')}
    if 'is_active' in tickets_cols:
        op.drop_column('tickets', 'is_active')

    # ── users ─────────────────────────────────────────────────────────────────
    existing_user_idx = {i['name'] for i in insp.get_indexes('users')}
    if 'ix_users_id' not in existing_user_idx:
        op.create_index('ix_users_id', 'users', ['id'], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    # ── users ─────────────────────────────────────────────────────────────────
    existing_user_idx = {i['name'] for i in insp.get_indexes('users')}
    if 'ix_users_id' in existing_user_idx:
        op.drop_index('ix_users_id', table_name='users')

    # ── tickets ───────────────────────────────────────────────────────────────
    tickets_cols = {c['name'] for c in insp.get_columns('tickets')}
    if 'is_active' not in tickets_cols:
        op.add_column('tickets', sa.Column(
            'is_active', sa.BOOLEAN(),
            server_default=sa.text('true'),
            nullable=False,
        ))

    existing_ticket_idx = {i['name'] for i in insp.get_indexes('tickets')}
    for idx_name in ['ix_tickets_id', 'ix_tickets_purchase_id',
                     'ix_tickets_showtime_id', 'ix_tickets_status']:
        if idx_name in existing_ticket_idx:
            op.drop_index(idx_name, table_name='tickets')

    if 'ix_tickets_ticket_code' in existing_ticket_idx:
        op.drop_index('ix_tickets_ticket_code', table_name='tickets')

    existing_ticket_constraints = {
        c['name'] for c in insp.get_unique_constraints('tickets')
    }
    if 'tickets_ticket_code_key' not in existing_ticket_constraints:
        op.create_unique_constraint('tickets_ticket_code_key', 'tickets',
                                    ['ticket_code'])

    # ── purchases ─────────────────────────────────────────────────────────────
    existing_purchase_idx = {i['name'] for i in insp.get_indexes('purchases')}
    for idx_name in ['ix_purchases_id', 'ix_purchases_movie_id',
                     'ix_purchases_showtime_id', 'ix_purchases_status',
                     'ix_purchases_user_id']:
        if idx_name in existing_purchase_idx:
            op.drop_index(idx_name, table_name='purchases')

    # ── movies ────────────────────────────────────────────────────────────────
    existing_movie_idx = {i['name'] for i in insp.get_indexes('movies')}
    if 'ix_movies_id' in existing_movie_idx:
        op.drop_index('ix_movies_id', table_name='movies')

    movies_cols = {c['name']: c for c in insp.get_columns('movies')}
    if movies_cols.get('rating', {}).get('nullable'):
        op.alter_column('movies', 'rating',
                        existing_type=sa.VARCHAR(length=10),
                        nullable=False)
    if movies_cols.get('duration', {}).get('nullable'):
        op.alter_column('movies', 'duration',
                        existing_type=sa.INTEGER(),
                        nullable=False)
    if movies_cols.get('genre', {}).get('nullable'):
        op.alter_column('movies', 'genre',
                        existing_type=sa.String(length=100),
                        type_=sa.VARCHAR(length=50),
                        nullable=False)
