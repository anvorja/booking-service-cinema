# app/schemas/purchase.py
import re
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, model_validator

_SEAT_CODE_RE = re.compile(r"^[A-Z]\d{1,3}$")


class ConcessionSelection(BaseModel):
    code: str = Field(..., min_length=1, max_length=20)
    quantity: int = Field(..., ge=1, le=20)


class PurchaseCreate(BaseModel):
    """
    Sin datos de pago ni montos: la persona paga en el Web Checkout de Wompi
    (tarjeta, PSE, Nequi…) y el total lo calcula el backend a partir de los
    asientos y los productos (ver app/services/pricing.py).
    """
    movie_id: int = Field(..., gt=0)
    quantity: int = Field(..., ge=1, le=10)
    # Comida que se compra con las boletas (códigos del menú de GET /pricing).
    concessions: List[ConcessionSelection] = Field(default_factory=list, max_length=30)
    show_date: Optional[date] = None
    show_time: Optional[str] = Field(None, pattern=r"^([0-1]?\d|2[0-3]):[0-5]\d$")
    showtime_id: Optional[int] = Field(None, gt=0)
    selected_seats: Optional[List[str]] = None  # ej. ["A1", "B3", "C7"]

    @model_validator(mode='after')
    def validate_purchase(self) -> 'PurchaseCreate':
        if self.selected_seats is not None:
            # Validar cantidad
            if len(self.selected_seats) != self.quantity:
                raise ValueError(
                    f'selected_seats debe tener exactamente {self.quantity} asiento(s), '
                    f'se recibieron {len(self.selected_seats)}.'
                )
            # Validar formato de cada código
            invalid = [s for s in self.selected_seats if not _SEAT_CODE_RE.match(s)]
            if invalid:
                raise ValueError(
                    f'Códigos de asiento inválidos: {invalid}. '
                    'El formato debe ser una letra mayúscula seguida de 1 a 3 dígitos (ej. A1, J10, K125).'
                )
            # Validar unicidad
            if len(set(self.selected_seats)) != len(self.selected_seats):
                raise ValueError('No se pueden seleccionar asientos duplicados.')

        codes = [c.code for c in self.concessions]
        if len(set(codes)) != len(codes):
            raise ValueError('Cada producto va una sola vez, con su cantidad.')

        return self

    @property
    def concession_selection(self) -> Dict[str, int]:
        return {c.code: c.quantity for c in self.concessions}


class PriceLineResponse(BaseModel):
    kind: str          # ticket | concession | service_fee
    code: str
    description: str
    unit_price: int
    quantity: int
    line_total: int


class QuoteResponse(BaseModel):
    lines: List[PriceLineResponse]
    total: int
    currency: str = "COP"


class ConcessionItemResponse(BaseModel):
    code: str
    category: str
    name: str
    description: str
    price: int


class PricingResponse(BaseModel):
    """Lo que la UI necesita para mostrar precios: todos salen del backend."""
    movie_id: int
    ticket_prices: Dict[str, int]          # {"general": …, "preferential": …}
    preferential_rows: List[str]
    service_fee_with_concessions: int
    concessions: List[ConcessionItemResponse]
    currency: str = "COP"


class TicketResponse(BaseModel):
    id: int
    ticket_code: str
    seat_number: str
    status: str
    created_at: datetime
    is_active: bool

    @classmethod
    def from_orm(cls, ticket):
        return cls(
            id=ticket.id,
            ticket_code=ticket.ticket_code,
            seat_number=ticket.seat_number,
            status=ticket.status.value,
            created_at=ticket.created_at,
            is_active=ticket.is_active,
        )


def _payment_summary(purchase) -> Dict[str, Any]:
    info = purchase.payment_info or {}
    return {
        "payment_method": info.get("payment_method", "wompi"),
        # Estado del flujo de pago: pending_inventory → payment_initiated →
        # awaiting_payment_result (la persona paga en Wompi) → approved | failed.
        "status": info.get("status"),
        # Hasta cuándo se puede pagar el enlace de Wompi.
        "payment_expires_at": info.get("payment_expires_at"),
        "payment_reference": info.get("payment_reference"),
        "payment_method_type": info.get("payment_method_type"),
        "last_four": info.get("last_four", "****"),
        "bank_name": info.get("bank_name"),
        "transaction_id": info.get("transaction_id"),
        "failure_reason": info.get("failure_reason"),
        "refund_status": info.get("refund_status"),
        "refund_detail": info.get("refund_detail"),
        "total_amount": purchase.total_amount,
        "currency": "COP",
    }


class PurchaseResponse(BaseModel):
    id: int
    user_id: int
    movie_id: int
    quantity: int
    total_amount: float
    status: str
    created_at: datetime
    is_confirmed: bool
    movie_title: str
    user_full_name: str
    tickets: List[TicketResponse]
    # Desglose del total (boletas, comida, valor por servicio).
    lines: List[PriceLineResponse] = Field(default_factory=list)
    payment_summary: Dict[str, Any]
    show_date: Optional[date] = None
    show_time: Optional[str] = None
    showtime_id: Optional[int] = None

    @classmethod
    def from_orm(cls, purchase):
        return cls(
            id=purchase.id,
            user_id=purchase.user_id,
            movie_id=purchase.movie_id,
            quantity=purchase.quantity,
            total_amount=purchase.total_amount,
            status=purchase.status.value,
            created_at=purchase.created_at,
            is_confirmed=purchase.is_confirmed,
            movie_title=purchase.movie.title,
            user_full_name=purchase.user.full_name,
            tickets=[TicketResponse.from_orm(t) for t in purchase.tickets],
            lines=[
                PriceLineResponse(
                    kind=line.kind.value, code=line.code, description=line.description,
                    unit_price=line.unit_price, quantity=line.quantity, line_total=line.line_total,
                )
                for line in purchase.lines
            ],
            payment_summary=_payment_summary(purchase),
            show_date=purchase.show_date,
            show_time=purchase.show_time,
            showtime_id=purchase.showtime_id,
        )


class PurchaseListResponse(BaseModel):
    id: int
    movie_title: str
    quantity: int
    total_amount: float
    status: str
    created_at: datetime
    tickets_count: int

    @classmethod
    def from_orm(cls, purchase):
        return cls(
            id=purchase.id,
            movie_title=purchase.movie.title,
            quantity=purchase.quantity,
            total_amount=purchase.total_amount,
            status=purchase.status.value,
            created_at=purchase.created_at,
            tickets_count=len(purchase.tickets),
        )


# ── Contrato interno para user-service (GET /internal/users/{user_id}/purchases) ──
# user-service ya no lee cinema_booking directamente (ver ARCHITECTURE.md,
# "Aislamiento de base de datos por servicio"). Este schema es un espejo
# deliberado de MyPurchaseResponse en user-service-cinema/app/schemas/user.py —
# cualquier cambio de forma acá debe reflejarse allá.

class InternalMovieInfo(BaseModel):
    id: int
    title: str
    genre: Optional[str] = None
    duration: Optional[int] = None
    poster_url: Optional[str] = None


class InternalTicketResponse(BaseModel):
    id: int
    ticket_code: str
    seat_number: str
    status: str
    created_at: datetime


class InternalUserPurchaseResponse(BaseModel):
    id: int
    movie_id: int
    movie: Optional[InternalMovieInfo]
    quantity: int
    total_amount: float
    status: str
    payment_info: Optional[Dict[str, Any]] = None
    tickets: List[InternalTicketResponse]
    created_at: datetime
    show_date: Optional[date] = None
    show_time: Optional[str] = None

    @classmethod
    def from_orm(cls, purchase):
        movie_info = None
        if purchase.movie:
            movie_info = InternalMovieInfo(
                id=purchase.movie.id,
                title=purchase.movie.title,
                genre=purchase.movie.genre,
                duration=purchase.movie.duration,
                poster_url=purchase.movie.poster_url,
            )
        return cls(
            id=purchase.id,
            movie_id=purchase.movie_id,
            movie=movie_info,
            quantity=purchase.quantity,
            total_amount=purchase.total_amount,
            status=purchase.status.value,
            payment_info=purchase.payment_info,
            tickets=[
                InternalTicketResponse(
                    id=t.id,
                    ticket_code=t.ticket_code,
                    seat_number=t.seat_number,
                    status=t.status.value,
                    created_at=t.created_at,
                )
                for t in purchase.tickets
            ],
            created_at=purchase.created_at,
            show_date=purchase.show_date,
            show_time=purchase.show_time,
        )


# ── Contrato interno para admin-service (panel de compras y reportes) ──────────
# admin-service ya no lee/escribe cinema_booking directamente (ver
# ARCHITECTURE.md, "Aislamiento de base de datos por servicio", caso 3).
# Estos schemas son un espejo deliberado de los de
# admin-service-cinema/app/schemas/admin.py — cualquier cambio de forma acá
# debe reflejarse allá.

class InternalAdminUserInfo(BaseModel):
    id: int
    first_name: str
    last_name: str
    email: str


class InternalAdminMovieInfo(BaseModel):
    id: int
    title: str
    genre: str


class InternalAdminPurchaseResponse(BaseModel):
    id: int
    user_id: int
    movie_id: int
    movie_title: str
    user_full_name: str
    user: InternalAdminUserInfo
    movie: InternalAdminMovieInfo
    quantity: int
    total_amount: float
    status: str
    is_confirmed: bool
    created_at: datetime
    tickets: List[TicketResponse]
    payment_summary: Dict[str, Any]

    @classmethod
    def from_orm(cls, p):
        return cls(
            id=p.id, user_id=p.user_id, movie_id=p.movie_id,
            movie_title=p.movie.title, user_full_name=p.user.full_name,
            user=InternalAdminUserInfo(
                id=p.user.id, first_name=p.user.first_name,
                last_name=p.user.last_name, email=p.user.email,
            ),
            movie=InternalAdminMovieInfo(
                id=p.movie.id, title=p.movie.title, genre=p.movie.genre,
            ),
            quantity=p.quantity, total_amount=p.total_amount,
            status=p.status.value, is_confirmed=p.is_confirmed,
            created_at=p.created_at,
            tickets=[TicketResponse.from_orm(t) for t in p.tickets],
            payment_summary={
                "last_four": p.payment_info.get("last_four", "****") if p.payment_info else "****",
                "total_amount": p.total_amount,
                "currency": "COP",
            },
        )


class SalesReport(BaseModel):
    total_purchases: int
    total_revenue: float
    total_tickets_sold: int
    average_purchase_amount: float
    total_refunds: int
    total_refunded_amount: float
    total_cancelled: int
    currency: str


class MovieSalesItem(BaseModel):
    movie_id: int
    movie_title: str
    purchases_count: int
    tickets_sold: int
    revenue: float
    refunded_amount: float
    net_revenue: float


class MovieSalesReport(BaseModel):
    items: List[MovieSalesItem]
    currency: str


class DateSalesItem(BaseModel):
    period: str
    purchases_count: int
    tickets_sold: int
    revenue: float


class DateSalesReport(BaseModel):
    items: List[DateSalesItem]
    period_type: str
    currency: str
