"""Una silla reembolsada vuelve a venderse

ix_ticket_showtime_seat contaba también los tickets CANCELLED: tras reembolsar
una compra, su silla aparecía libre en el mapa pero la siguiente persona que
la pagaba no recibía boletas (el insert fallaba y la compra quedaba pendiente
con el dinero cobrado). El índice pasa a ignorar los tickets cancelados.

Revision ID: c8d2e3f4a5b6
Revises: b7c1d2e3f4a5
Create Date: 2026-09-25

"""
from typing import Sequence, Union

from alembic import op

revision: str = 'c8d2e3f4a5b6'
down_revision: Union[str, Sequence[str], None] = 'b7c1d2e3f4a5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_ticket_showtime_seat")
    op.execute(
        "CREATE UNIQUE INDEX ix_ticket_showtime_seat ON tickets (showtime_id, seat_number) "
        "WHERE showtime_id IS NOT NULL AND status <> 'CANCELLED'"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_ticket_showtime_seat")
    op.execute(
        "CREATE UNIQUE INDEX ix_ticket_showtime_seat ON tickets (showtime_id, seat_number) "
        "WHERE showtime_id IS NOT NULL"
    )
