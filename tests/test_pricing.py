from app.models.concession import ConcessionItem
from app.services.pricing import build_quote, seat_category, ticket_prices

ROWS = frozenset("KLMNOP")


def item(code, price, name=None):
    return ConcessionItem(code=code, category="Confitería", name=name or code, description="", price=price)


def quote(**kw):
    base = dict(
        movie_price=20_000, quantity=2, selected_seats=None, concessions=[],
        preferential_rows=ROWS, preferential_surcharge=5_700, service_fee=4_800,
    )
    return build_quote(**{**base, **kw})


def test_general_is_movie_price_and_preferential_adds_surcharge():
    assert ticket_prices(20_000, 5_700) == {"general": 20_000, "preferential": 25_700}


def test_seat_category_by_row():
    assert seat_category("K5", ROWS) == "preferential"
    assert seat_category("J10", ROWS) == "general"
    assert seat_category("p1", ROWS) == "preferential"


def test_seats_are_split_by_category():
    q = quote(selected_seats=["A1", "L3"])
    assert [(l.code, l.unit_price, l.quantity) for l in q.lines] == [
        ("general", 20_000, 1),
        ("preferential", 25_700, 1),
    ]
    assert q.total == 45_700


def test_without_seats_all_tickets_are_general():
    q = quote(quantity=3)
    assert [(l.code, l.quantity, l.line_total) for l in q.lines] == [("general", 3, 60_000)]


def test_food_adds_its_price_and_one_service_fee():
    # El caso de la captura: 2 General + 4 combos.
    combos = [(item("f1", 19_900), 1), (item("f2", 24_900), 1), (item("f3", 39_900), 1), (item("f4", 29_900), 1)]
    q = quote(selected_seats=["G12", "H12"], concessions=combos)
    kinds = [l.kind for l in q.lines]
    assert kinds == ["ticket", "concession", "concession", "concession", "concession", "service_fee"]
    assert q.total == 40_000 + 19_900 + 24_900 + 39_900 + 29_900 + 4_800


def test_quantities_multiply():
    q = quote(concessions=[(item("jv1", 7_900), 3)])
    food = next(l for l in q.lines if l.kind == "concession")
    assert food.line_total == 23_700


def test_no_food_no_service_fee():
    assert all(l.kind != "service_fee" for l in quote().lines)
