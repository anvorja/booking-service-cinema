# app/models/purchase.py
import enum
from datetime import date as date_type
from typing import List, Optional, Dict, Any
from sqlalchemy import CheckConstraint, String, Integer, Float, ForeignKey, JSON, Enum, Date, Index, text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import BaseModel


class PurchaseStatus(str, enum.Enum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"


class TicketStatus(str, enum.Enum):
    ACTIVE = "active"
    USED = "used"
    CANCELLED = "cancelled"


class Purchase(BaseModel):
    __tablename__ = "purchases"

    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    movie_id: Mapped[int] = mapped_column(Integer, ForeignKey("movies.id"), nullable=False, index=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    total_amount: Mapped[float] = mapped_column(Float, nullable=False)
    payment_info: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    status: Mapped[PurchaseStatus] = mapped_column(
        Enum(PurchaseStatus, name="purchasestatus"),
        default=PurchaseStatus.CONFIRMED,
        nullable=False,
        index=True,
    )
    show_date: Mapped[Optional[date_type]] = mapped_column(Date, nullable=True)
    show_time: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    showtime_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)

    user: Mapped["User"] = relationship("User")  # type: ignore
    movie: Mapped["Movie"] = relationship("Movie")  # type: ignore
    tickets: Mapped[List["Ticket"]] = relationship(
        back_populates="purchase", cascade="all, delete-orphan"
    )
    # Desglose con que se calculó total_amount (boletas, comida, servicio).
    lines: Mapped[List["PurchaseLine"]] = relationship(
        back_populates="purchase", cascade="all, delete-orphan", order_by="PurchaseLine.id"
    )

    @property
    def is_confirmed(self) -> bool:
        return self.status == PurchaseStatus.CONFIRMED


class Ticket(BaseModel):
    __tablename__ = "tickets"

    purchase_id: Mapped[int] = mapped_column(Integer, ForeignKey("purchases.id"), nullable=False, index=True)
    ticket_code: Mapped[str] = mapped_column(String(20), unique=True, nullable=False, index=True)
    seat_number: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[TicketStatus] = mapped_column(
        Enum(TicketStatus, name="ticketstatus"),
        default=TicketStatus.ACTIVE,
        nullable=False,
        index=True,
    )
    showtime_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)

    purchase: Mapped["Purchase"] = relationship(back_populates="tickets")

    __table_args__ = (
        # Barrera de integridad final: un asiento no puede venderse dos veces en la
        # misma función. Los tickets cancelados (compra reembolsada) no cuentan:
        # la silla vuelve a estar a la venta.
        Index(
            "ix_ticket_showtime_seat",
            "showtime_id", "seat_number",
            unique=True,
            postgresql_where=text("showtime_id IS NOT NULL AND status <> 'CANCELLED'"),
        ),
    )

    @property
    def is_active(self) -> bool:
        return self.status == TicketStatus.ACTIVE


class PurchaseLineKind(str, enum.Enum):
    TICKET = "ticket"
    CONCESSION = "concession"
    SERVICE_FEE = "service_fee"


class PurchaseLine(BaseModel):
    """
    Una línea del total cobrado, con el precio del momento de la compra
    (si luego cambia el precio de la película o de un combo, la compra no
    cambia). total_amount de la compra es la suma de line_total.
    """
    __tablename__ = "purchase_lines"

    purchase_id: Mapped[int] = mapped_column(Integer, ForeignKey("purchases.id"), nullable=False, index=True)
    kind: Mapped[PurchaseLineKind] = mapped_column(
        Enum(PurchaseLineKind, name="purchaselinekind", values_callable=lambda e: [m.value for m in e]),
        nullable=False,
    )
    # ticket: general | preferential · concession: código del producto · service_fee: service_fee
    code: Mapped[str] = mapped_column(String(20), nullable=False)
    description: Mapped[str] = mapped_column(String(150), nullable=False)
    unit_price: Mapped[int] = mapped_column(Integer, nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    line_total: Mapped[int] = mapped_column(Integer, nullable=False)

    purchase: Mapped["Purchase"] = relationship(back_populates="lines")

    __table_args__ = (
        CheckConstraint("quantity > 0", name="ck_purchase_lines_quantity"),
        CheckConstraint("unit_price >= 0", name="ck_purchase_lines_unit_price"),
        CheckConstraint("line_total = unit_price * quantity", name="ck_purchase_lines_total"),
    )
