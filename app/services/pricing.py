# app/services/pricing.py — el total de una compra, calculado solo en el backend.
#
# El frontend envía qué asientos y qué productos quiere; nunca montos. Este
# módulo arma las líneas (boletas General/Preferencial, comida, valor por
# servicio) con los precios del backend, y su suma es lo que se cobra en Wompi.
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.concession import ConcessionItem

GENERAL = "general"
PREFERENTIAL = "preferential"
SERVICE_FEE = "service_fee"

SEAT_LABELS = {GENERAL: "General", PREFERENTIAL: "Preferencial"}


@dataclass(frozen=True)
class PriceLine:
    kind: str          # ticket | concession | service_fee
    code: str
    description: str
    unit_price: int
    quantity: int

    @property
    def line_total(self) -> int:
        return self.unit_price * self.quantity


@dataclass(frozen=True)
class Quote:
    lines: tuple[PriceLine, ...]

    @property
    def total(self) -> int:
        return sum(line.line_total for line in self.lines)


def seat_category(seat_code: str, preferential_rows: frozenset[str]) -> str:
    """Una silla es preferencial si su fila (la letra) está en PREFERENTIAL_ROWS."""
    return PREFERENTIAL if seat_code[:1].upper() in preferential_rows else GENERAL


def ticket_prices(movie_price: float, surcharge: int) -> dict[str, int]:
    general = round(movie_price)
    return {GENERAL: general, PREFERENTIAL: general + surcharge}


def build_quote(
    *,
    movie_price: float,
    quantity: int,
    selected_seats: Sequence[str] | None,
    concessions: Iterable[tuple[ConcessionItem, int]],
    preferential_rows: frozenset[str],
    preferential_surcharge: int,
    service_fee: int,
) -> Quote:
    """
    Sin asientos elegidos todas las boletas son General. El valor por servicio
    se cobra una vez si la compra incluye comida.
    """
    prices = ticket_prices(movie_price, preferential_surcharge)
    counts = {GENERAL: 0, PREFERENTIAL: 0}
    if selected_seats:
        for seat in selected_seats:
            counts[seat_category(seat, preferential_rows)] += 1
    else:
        counts[GENERAL] = quantity

    lines: list[PriceLine] = [
        PriceLine("ticket", category, f"Boleta {SEAT_LABELS[category]}", prices[category], count)
        for category, count in counts.items()
        if count
    ]
    food = [
        PriceLine("concession", item.code, item.name, item.price, qty)
        for item, qty in concessions
        if qty
    ]
    lines.extend(food)
    if food and service_fee:
        lines.append(PriceLine("service_fee", SERVICE_FEE, "Valor por servicio", service_fee, 1))
    return Quote(tuple(lines))


def resolve_concessions(db: Session, selection: Mapping[str, int]) -> list[tuple[ConcessionItem, int]]:
    """Productos pedidos (código → cantidad). Rechaza códigos inexistentes o inactivos."""
    if not selection:
        return []
    items = (
        db.query(ConcessionItem)
        .filter(ConcessionItem.code.in_(list(selection)), ConcessionItem.is_active == True)  # noqa: E712
        .all()
    )
    by_code = {item.code: item for item in items}
    unknown = sorted(set(selection) - set(by_code))
    if unknown:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Productos no disponibles: {', '.join(unknown)}",
        )
    return [(by_code[code], qty) for code, qty in selection.items()]


def quote_for(
    db: Session,
    movie_price: float,
    quantity: int,
    selected_seats: Sequence[str] | None,
    selection: Mapping[str, int],
) -> Quote:
    return build_quote(
        movie_price=movie_price,
        quantity=quantity,
        selected_seats=selected_seats,
        concessions=resolve_concessions(db, selection),
        preferential_rows=settings.preferential_rows,
        preferential_surcharge=settings.PREFERENTIAL_SURCHARGE,
        service_fee=settings.CONCESSION_SERVICE_FEE,
    )
