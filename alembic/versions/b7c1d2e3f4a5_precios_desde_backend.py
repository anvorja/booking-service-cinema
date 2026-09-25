"""Precios desde el backend: menú de comida y desglose de cada compra

- concession_items: productos de comida con su precio (sembrado con el menú
  que antes vivía fijo en el frontend).
- purchase_lines: líneas con que se calculó total_amount (boletas, comida,
  valor por servicio).

Idempotente: al arrancar, booking hace create_all ANTES de las migraciones,
así que las tablas pueden existir ya (vacías). Solo se crean si faltan y el
menú se siembra sin pisar productos existentes.

Revision ID: b7c1d2e3f4a5
Revises: a1b2c3d4e5f6
Create Date: 2026-09-25

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = 'b7c1d2e3f4a5'
down_revision: Union[str, Sequence[str], None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# (código, categoría, nombre, descripción, precio en pesos)
MENU = [
    ('f1', 'Confitería', 'Combo Fan Junior', '1 Caja crispetas de sal 55 g + 1 Gaseosa pequeña 640 ml', 19_900),
    ('f2', 'Confitería', 'Combo Fan', '1 Crispeta pequeña de sal 100 g + 1 Gaseosa mediana 960 ml', 24_900),
    ('f3', 'Confitería', 'Combo Pro', '1 Crispeta de sal 100 gr + 1 Gaseosa mediana 960 ml + 1 Perro caliente o sandwich', 39_900),
    ('f4', 'Confitería', 'Combo Deli', '1 Crispeta pequeña 100 g + 1 Gaseosa mediana 960 ml + 1 Queso cheddar + 1 galleta', 29_900),
    ('f5', 'Confitería', 'Combo Fan Para Dos', '1 Crispeta grande de sal 150 g + 2 Gaseosas medianas 960 ml', 43_900),
    ('f6', 'Confitería', 'Combo Pro Para Dos', '1 Crispeta mediana de sal 120 g + 2 Gaseosas medianas 960 ml + 2 Perros calientes o sandwich', 62_900),
    ('f7', 'Confitería', 'Crispeta Sal Grande 150 g', '', 26_900),
    ('f8', 'Confitería', 'Crispeta Sal Mediana 120 g', '', 24_900),
    ('f9', 'Confitería', 'Adición Completa Caramelo', 'Esta adición no incluye las crispetas', 4_700),
    ('f10', 'Confitería', 'Adición Media Caramelo', 'Esta adición no incluye las crispetas', 3_700),
    ('f11', 'Confitería', 'Porción Queso Cheddar 100 g', '', 9_500),
    ('f12', 'Confitería', 'Nachos Con Queso Cheddar', '', 17_900),
    ('s1', 'Sushi', 'Roll California (8 piezas)', 'Pepino, aguacate, cangrejo y sésamo', 32_900),
    ('s2', 'Sushi', 'Roll Spicy Tuna (8 piezas)', 'Atún, espinaca, aguacate, salsa picante', 36_900),
    ('s3', 'Sushi', 'Sashimi Mix (12 piezas)', 'Salmón, atún y pescado blanco', 48_900),
    ('s4', 'Sushi', 'Combo Sushi Dúo', '2 rolls a elección + 2 gaseosas', 64_900),
    ('c1', 'Cinepolitana', 'Pizza Personal Queso', 'Base de tomate, mozzarella', 22_900),
    ('c2', 'Cinepolitana', 'Pizza Personal Pepperoni', 'Base de tomate, mozzarella, pepperoni', 26_900),
    ('c3', 'Cinepolitana', 'Perro Caliente', 'Salchicha, papas de palillo, salsas', 14_900),
    ('c4', 'Cinepolitana', 'Sandwich Especial', 'Jamón, queso, lechuga, tomate', 18_900),
    ('jv1', 'Juan Valdez', 'Café Americano', 'Taza 250 ml', 7_900),
    ('jv2', 'Juan Valdez', 'Cappuccino', 'Espresso + leche espumada', 9_900),
    ('jv3', 'Juan Valdez', 'Latte Vainilla', 'Espresso + leche + sirope de vainilla', 10_900),
    ('jv4', 'Juan Valdez', 'Brownie Choco', 'Con chips de chocolate', 8_500),
]


def _timestamps():
    return [
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False),
    ]


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table('concession_items'):
        op.create_table(
            'concession_items',
            sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column('code', sa.String(20), nullable=False, unique=True),
            sa.Column('category', sa.String(40), nullable=False),
            sa.Column('name', sa.String(100), nullable=False),
            sa.Column('description', sa.String(255), nullable=False),
            sa.Column('price', sa.Integer(), nullable=False),
            sa.Column('sort_order', sa.Integer(), nullable=False),
            *_timestamps(),
            sa.CheckConstraint('price > 0', name='ck_concession_items_price'),
        )
        op.create_index('ix_concession_items_id', 'concession_items', ['id'])

    if not inspector.has_table('purchase_lines'):
        kind = postgresql.ENUM('ticket', 'concession', 'service_fee', name='purchaselinekind', create_type=False)
        kind.create(bind, checkfirst=True)
        op.create_table(
            'purchase_lines',
            sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column('purchase_id', sa.Integer(), sa.ForeignKey('purchases.id'), nullable=False),
            sa.Column('kind', kind, nullable=False),
            sa.Column('code', sa.String(20), nullable=False),
            sa.Column('description', sa.String(150), nullable=False),
            sa.Column('unit_price', sa.Integer(), nullable=False),
            sa.Column('quantity', sa.Integer(), nullable=False),
            sa.Column('line_total', sa.Integer(), nullable=False),
            *_timestamps(),
            sa.CheckConstraint('quantity > 0', name='ck_purchase_lines_quantity'),
            sa.CheckConstraint('unit_price >= 0', name='ck_purchase_lines_unit_price'),
            sa.CheckConstraint('line_total = unit_price * quantity', name='ck_purchase_lines_total'),
        )
        op.create_index('ix_purchase_lines_id', 'purchase_lines', ['id'])
        op.create_index('ix_purchase_lines_purchase_id', 'purchase_lines', ['purchase_id'])

    insert = sa.text(
        "INSERT INTO concession_items (code, category, name, description, price, sort_order, is_active) "
        "VALUES (:code, :category, :name, :description, :price, :sort_order, true) "
        "ON CONFLICT (code) DO NOTHING"
    )
    for order, (code, category, name, description, price) in enumerate(MENU):
        bind.execute(insert, {
            'code': code, 'category': category, 'name': name,
            'description': description, 'price': price, 'sort_order': order,
        })


def downgrade() -> None:
    op.drop_table('purchase_lines')
    op.drop_table('concession_items')
    sa.Enum(name='purchaselinekind').drop(op.get_bind(), checkfirst=True)
