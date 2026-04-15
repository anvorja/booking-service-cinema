"""fix_historical_general_seats

Corrige los tickets históricos con seat_number='GENERAL-X' cuyo seat real se
conoce pero se perdió porque el frontend no enviaba selected_seats en el
payload al momento de la compra.

Cambios aplicados
─────────────────
tickets:
  - CINE-8AZAXK7: seat_number 'GENERAL-1' → 'K23', showtime_id NULL → 62

purchases:
  - purchase #75 (padre de CINE-8AZAXK7):
      show_date '2026-04-15' → '2026-04-14'
      showtime_id NULL → 62

Idempotencia: todas las operaciones comprueban el estado actual antes de
actuar; pueden ejecutarse sobre una BD ya corregida sin errores.

Revision ID: a1b2c3d4e5f6
Revises: eeb8bf47283c
Create Date: 2026-04-15 11:00:00.000000
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = 'eeb8bf47283c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Datos del ticket afectado
_TICKET_CODE = 'CINE-8AZAXK7'
_CORRECT_SEAT = 'K23'
_CORRECT_SHOWTIME_ID = 62
_PURCHASE_ID = 75
_CORRECT_SHOW_DATE = '2026-04-14'


def upgrade() -> None:
    bind = op.get_bind()

    # ── 1. Verificar que no existe ya un ticket con showtime_id=62 y seat K23 ──
    #    (evita violar el índice único parcial ix_ticket_showtime_seat)
    conflict = bind.execute(
        sa.text(
            "SELECT id FROM tickets "
            "WHERE showtime_id = :sid AND seat_number = :seat "
            "  AND ticket_code <> :code"
        ),
        {"sid": _CORRECT_SHOWTIME_ID, "seat": _CORRECT_SEAT, "code": _TICKET_CODE},
    ).fetchone()

    if conflict:
        raise RuntimeError(
            f"No se puede corregir {_TICKET_CODE}: ya existe otro ticket "
            f"(id={conflict[0]}) con showtime_id={_CORRECT_SHOWTIME_ID} "
            f"y seat_number='{_CORRECT_SEAT}'. Revisión manual necesaria."
        )

    # ── 2. Corregir el ticket solo si aún tiene el valor incorrecto ─────────────
    ticket_row = bind.execute(
        sa.text("SELECT id, seat_number, showtime_id FROM tickets WHERE ticket_code = :code"),
        {"code": _TICKET_CODE},
    ).fetchone()

    if ticket_row is None:
        # El ticket no existe — nada que corregir
        pass
    elif ticket_row[1] != _CORRECT_SEAT or ticket_row[2] != _CORRECT_SHOWTIME_ID:
        bind.execute(
            sa.text(
                "UPDATE tickets "
                "SET seat_number = :seat, showtime_id = :sid "
                "WHERE ticket_code = :code"
            ),
            {"seat": _CORRECT_SEAT, "sid": _CORRECT_SHOWTIME_ID, "code": _TICKET_CODE},
        )

    # ── 3. Corregir la purchase padre solo si aún tiene valores incorrectos ─────
    purchase_row = bind.execute(
        sa.text("SELECT id, show_date, showtime_id FROM purchases WHERE id = :pid"),
        {"pid": _PURCHASE_ID},
    ).fetchone()

    if purchase_row is None:
        pass
    elif (str(purchase_row[1]) != _CORRECT_SHOW_DATE
          or purchase_row[2] != _CORRECT_SHOWTIME_ID):
        bind.execute(
            sa.text(
                "UPDATE purchases "
                "SET show_date = :date, showtime_id = :sid "
                "WHERE id = :pid"
            ),
            {
                "date": _CORRECT_SHOW_DATE,
                "sid": _CORRECT_SHOWTIME_ID,
                "pid": _PURCHASE_ID,
            },
        )


def downgrade() -> None:
    bind = op.get_bind()

    # ── Revertir ticket ──────────────────────────────────────────────────────────
    ticket_row = bind.execute(
        sa.text("SELECT id, seat_number, showtime_id FROM tickets WHERE ticket_code = :code"),
        {"code": _TICKET_CODE},
    ).fetchone()

    if ticket_row is not None and (
        ticket_row[1] == _CORRECT_SEAT or ticket_row[2] == _CORRECT_SHOWTIME_ID
    ):
        bind.execute(
            sa.text(
                "UPDATE tickets "
                "SET seat_number = 'GENERAL-1', showtime_id = NULL "
                "WHERE ticket_code = :code"
            ),
            {"code": _TICKET_CODE},
        )

    # ── Revertir purchase ────────────────────────────────────────────────────────
    purchase_row = bind.execute(
        sa.text("SELECT id, show_date, showtime_id FROM purchases WHERE id = :pid"),
        {"pid": _PURCHASE_ID},
    ).fetchone()

    if purchase_row is not None and (
        str(purchase_row[1]) == _CORRECT_SHOW_DATE
        or purchase_row[2] == _CORRECT_SHOWTIME_ID
    ):
        bind.execute(
            sa.text(
                "UPDATE purchases "
                "SET show_date = '2026-04-15', showtime_id = NULL "
                "WHERE id = :pid"
            ),
            {"pid": _PURCHASE_ID},
        )
