# app/models/purchase.py
import enum
from datetime import date as date_type
from typing import List, Optional, Dict, Any
from sqlalchemy import String, Integer, Float, ForeignKey, JSON, Enum, Date, Index, text
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
        # Barrera de integridad final: un asiento no puede venderse dos veces en la misma función
        Index(
            "ix_ticket_showtime_seat",
            "showtime_id", "seat_number",
            unique=True,
            postgresql_where=text("showtime_id IS NOT NULL"),
        ),
    )

    @property
    def is_active(self) -> bool:
        return self.status == TicketStatus.ACTIVE
