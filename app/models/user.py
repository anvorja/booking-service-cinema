# app/models/user.py — tabla de referencia local en cinema_booking
# Sincronizada vía Kafka (user.registered / user.deactivated).
# Sin password_hash — auth-service es la fuente de verdad para credenciales.
from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column
from app.models.base import BaseModel


class User(BaseModel):
    """Referencia local de usuarios en cinema_booking."""
    __tablename__ = "users"

    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    first_name: Mapped[str] = mapped_column(String(100), nullable=False)
    last_name: Mapped[str] = mapped_column(String(100), nullable=False)
    phone: Mapped[str] = mapped_column(String(20), nullable=False, default="")
    role: Mapped[str] = mapped_column(String(20), nullable=False, default="customer")

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}"
