# app/schemas/purchase.py
import re
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, model_validator

_SEAT_CODE_RE = re.compile(r"^[A-Z]\d{1,3}$")


class PaymentInfo(BaseModel):
    card_number: str = Field(..., pattern=r"^\d{16}$")
    card_holder: str = Field(..., min_length=1, max_length=100)
    expiry_month: int = Field(..., ge=1, le=12)
    expiry_year: int = Field(..., ge=2024)
    cvv: str = Field(..., pattern=r"^\d{3,4}$")


class PseInfo(BaseModel):
    bank_code: str
    bank_name: str
    document_type: str = Field(..., pattern=r"^(CC|CE|NIT|PP|TI)$")
    document_number: str = Field(..., min_length=4, max_length=20)
    payer_email: str = Field(..., pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class PurchaseCreate(BaseModel):
    movie_id: int = Field(..., gt=0)
    quantity: int = Field(..., ge=1, le=10)
    payment_info: Optional[PaymentInfo] = None
    pse_info: Optional[PseInfo] = None
    show_date: Optional[date] = None
    show_time: Optional[str] = Field(None, pattern=r"^([0-1]?\d|2[0-3]):[0-5]\d$")
    showtime_id: Optional[int] = Field(None, gt=0)
    selected_seats: Optional[List[str]] = None  # ej. ["A1", "B3", "C7"]

    @model_validator(mode='after')
    def validate_purchase(self) -> 'PurchaseCreate':
        if self.payment_info is None and self.pse_info is None:
            raise ValueError('Se requiere payment_info (tarjeta) o pse_info (PSE)')

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

        return self


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
            payment_summary={
                "payment_method": purchase.payment_info.get("payment_method", "card") if purchase.payment_info else "card",
                "last_four": purchase.payment_info.get("last_four", "****") if purchase.payment_info else "****",
                "bank_name": purchase.payment_info.get("bank_name") if purchase.payment_info else None,
                "transaction_id": purchase.payment_info.get("transaction_id") if purchase.payment_info else None,
                "failure_reason": purchase.payment_info.get("failure_reason") if purchase.payment_info else None,
                "total_amount": purchase.total_amount,
                "currency": "COP",
            },
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
