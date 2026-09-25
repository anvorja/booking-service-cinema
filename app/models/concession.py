# app/models/concession.py
from sqlalchemy import CheckConstraint, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import BaseModel


class ConcessionItem(BaseModel):
    """
    Producto de comida que se compra junto con las boletas (confitería,
    sushi, Cinepolitana, Juan Valdez). El precio lo decide el backend: el
    frontend solo muestra este menú y envía códigos y cantidades.
    """
    __tablename__ = "concession_items"

    code: Mapped[str] = mapped_column(String(20), unique=True, nullable=False)
    category: Mapped[str] = mapped_column(String(40), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    # Pesos colombianos, sin decimales.
    price: Mapped[int] = mapped_column(Integer, nullable=False)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (CheckConstraint("price > 0", name="ck_concession_items_price"),)
