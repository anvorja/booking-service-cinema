# app/models/movie.py — refleja la tabla movies de cinema_catalog (fuente de verdad: catalog-service)
from sqlalchemy import String, Integer, Float, Boolean
from sqlalchemy.orm import Mapped, mapped_column
from typing import Optional
from app.models.base import BaseModel


class Movie(BaseModel):
    """Refleja la tabla movies de cinema_catalog. Se mantiene sincronizada vía movie.updated."""
    __tablename__ = "movies"

    title: Mapped[str] = mapped_column(String(200), nullable=False)
    genre: Mapped[str] = mapped_column(String(100), nullable=True)
    duration: Mapped[int] = mapped_column(Integer, nullable=True)
    rating: Mapped[str] = mapped_column(String(10), nullable=True)
    price: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    available_tickets: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    max_capacity: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    poster_url: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)

    def can_purchase(self, quantity: int) -> bool:
        return self.is_active and quantity > 0 and self.available_tickets >= quantity
