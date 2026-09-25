# app/api/purchases.py
import asyncio
import logging
from datetime import datetime, date as date_type, time as time_type, timedelta
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import or_, func, case
from sqlalchemy.orm import Session, selectinload, joinedload

from app.core.database import get_db
from app.api.dependencies import get_current_user, verify_internal_token
from app.kafka.producer import publish_event
from app.core.config import settings
from app.models.concession import ConcessionItem
from app.models.movie import Movie
from app.models.purchase import Purchase, PurchaseStatus, Ticket, TicketStatus
from app.models.user import User
from app.schemas.purchase import (
    ConcessionItemResponse,
    PriceLineResponse,
    PricingResponse,
    PurchaseCreate,
    PurchaseResponse,
    QuoteResponse,
    PurchaseListResponse,
    TicketResponse,
    InternalUserPurchaseResponse,
    InternalAdminPurchaseResponse,
    SalesReport,
    MovieSalesReport,
    DateSalesReport,
)
from app.services.booking import create_purchase as svc_create_purchase, call_refund_service
from app.services.pricing import quote_for, ticket_prices

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/purchases", tags=["purchases"])


@router.post("", response_model=PurchaseResponse, status_code=status.HTTP_201_CREATED)
async def create_purchase_endpoint(
    purchase_data: PurchaseCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Crear una nueva compra de boletos.
    Orquestada por booking-service: persiste PENDING → publica order.created
    → inventario decide → booking inicia el pago → payment-service emite el resultado.
    """
    purchase = await svc_create_purchase(
        db=db,
        user_id=current_user.id,
        purchase_data=purchase_data,
    )

    order_created_payload = {
        "order_id": purchase.id,
        "user_id": current_user.id,
        "user_email": current_user.email,
        "movie_id": purchase.movie_id,
        "quantity": purchase.quantity,
        "showtime_id": purchase.showtime_id,
    }

    # Publicar de forma directa (no fire-and-forget) para detectar fallos de Kafka
    # inmediatamente. Si falla, la compra quedó en PENDING y el reconciler la cancela
    # en 120s, pero al menos el error queda visible en los logs.
    try:
        await publish_event("order.created", order_created_payload)
    except Exception as kafka_exc:
        logger.error(
            "order.created publish failed for purchase_id=%s — saga will NOT start: %s",
            purchase.id, kafka_exc,
        )

    return PurchaseResponse.from_orm(purchase)


@router.get("", response_model=List[PurchaseListResponse])
async def get_my_purchases(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=10, ge=1, le=50),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Historial de compras del usuario autenticado."""
    purchases = (
        db.query(Purchase)
        .filter(Purchase.user_id == current_user.id)
        .order_by(Purchase.created_at.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )
    return [PurchaseListResponse.from_orm(p) for p in purchases]


def _movie_or_404(db: Session, movie_id: int) -> Movie:
    movie = db.query(Movie).filter(Movie.id == movie_id, Movie.is_active == True).first()  # noqa: E712
    if not movie:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Movie not found or not available")
    return movie


@router.get("/pricing", response_model=PricingResponse)
async def get_pricing(movie_id: int = Query(..., gt=0), db: Session = Depends(get_db)):
    """
    Precios para mostrar en la compra: boleta General (precio de la película),
    Preferencial (+ recargo), filas preferenciales, menú de comida y valor por
    servicio. Público. Son los mismos con que se calcula el cobro.
    """
    movie = _movie_or_404(db, movie_id)
    items = (
        db.query(ConcessionItem)
        .filter(ConcessionItem.is_active == True)  # noqa: E712
        .order_by(ConcessionItem.sort_order, ConcessionItem.id)
        .all()
    )
    return PricingResponse(
        movie_id=movie.id,
        ticket_prices=ticket_prices(movie.price, settings.PREFERENTIAL_SURCHARGE),
        preferential_rows=sorted(settings.preferential_rows),
        service_fee_with_concessions=settings.CONCESSION_SERVICE_FEE,
        concessions=[
            ConcessionItemResponse(
                code=i.code, category=i.category, name=i.name, description=i.description, price=i.price
            )
            for i in items
        ],
    )


@router.post("/quote", response_model=QuoteResponse)
async def quote_purchase(purchase_data: PurchaseCreate, db: Session = Depends(get_db)):
    """
    Desglose y total exactos de una compra antes de crearla (mismo cálculo que
    POST /purchases). No reserva nada.
    """
    movie = _movie_or_404(db, purchase_data.movie_id)
    quote = quote_for(
        db,
        movie_price=movie.price,
        quantity=purchase_data.quantity,
        selected_seats=purchase_data.selected_seats,
        selection=purchase_data.concession_selection,
    )
    return QuoteResponse(
        lines=[
            PriceLineResponse(
                kind=l.kind, code=l.code, description=l.description,
                unit_price=l.unit_price, quantity=l.quantity, line_total=l.line_total,
            )
            for l in quote.lines
        ],
        total=quote.total,
    )


@router.get("/{purchase_id}", response_model=PurchaseResponse)
async def get_purchase_detail(
    purchase_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Detalle de una compra. Solo el propietario puede verla."""
    purchase = (
        db.query(Purchase)
        .filter(Purchase.id == purchase_id, Purchase.user_id == current_user.id)
        .first()
    )
    if not purchase:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Compra no encontrada")
    return PurchaseResponse.from_orm(purchase)


@router.post("/{purchase_id}/cancel", response_model=PurchaseResponse)
async def cancel_purchase(
    purchase_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Cancelar y reembolsar una compra confirmada.
    Valida que la función no haya comenzado (o esté a menos de 30 min de comenzar).
    Pide a payment-service devolver el dinero (Wompi) y restaura los tickets disponibles.
    """
    purchase = (
        db.query(Purchase)
        .filter(Purchase.id == purchase_id, Purchase.user_id == current_user.id)
        .first()
    )
    if not purchase:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Compra no encontrada")

    if purchase.status != PurchaseStatus.CONFIRMED:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"No se puede cancelar una compra con estado '{purchase.status.value}'",
        )

    # Validar que la función no haya comenzado
    if purchase.show_date:
        today = date_type.today()
        if purchase.show_date < today:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No se puede cancelar una función que ya ocurrió.",
            )
        if purchase.show_date == today and purchase.show_time:
            try:
                h, m = map(int, purchase.show_time.split(':'))
                show_dt = datetime.combine(purchase.show_date, time_type(h, m))
                if datetime.now() >= show_dt - timedelta(minutes=30):
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="No se puede cancelar con menos de 30 minutos antes de la función.",
                    )
            except (ValueError, AttributeError):
                pass  # si no se puede parsear la hora, permitir cancelación

    # Devolver el dinero vía payment-service: Wompi anula las transacciones
    # con tarjeta; otros medios (PSE, Nequi…) quedan para devolver desde el
    # panel de Wompi (refund_status=manual_required).
    transaction_id = purchase.payment_info.get("transaction_id") if purchase.payment_info else None
    refund = await call_refund_service(purchase.id)
    payment_info = dict(purchase.payment_info or {})
    payment_info.update(
        refund_status=refund.get("refund_status"),
        refund_detail=refund.get("detail"),
    )
    purchase.payment_info = payment_info

    # Restaurar tickets disponibles en la película
    movie = db.query(Movie).filter(Movie.id == purchase.movie_id).first()
    if movie:
        movie.available_tickets += purchase.quantity

    # Cancelar tickets y marcar compra como reembolsada
    for ticket in purchase.tickets:
        ticket.status = TicketStatus.CANCELLED

    purchase.status = PurchaseStatus.REFUNDED
    db.commit()
    db.refresh(purchase)

    # Preparar respuesta y payload mientras la sesión está activa
    response = PurchaseResponse.from_orm(purchase)
    refund_payload = {
        "order_id":       purchase.id,
        "user_id":        current_user.id,
        "user_email":     current_user.email,
        "customer_name":  current_user.full_name,
        "movie_id":       purchase.movie_id,
        "movie_title":    purchase.movie.title if purchase.movie else "N/A",
        "quantity":       purchase.quantity,
        "total_amount":   float(purchase.total_amount),
        "transaction_id": transaction_id,
        "cancelled_at":   purchase.updated_at.isoformat() if purchase.updated_at else None,
        "showtime_id":    purchase.showtime_id,
    }

    # Publicar en background — no bloquea la respuesta HTTP
    asyncio.create_task(publish_event("order.refunded", refund_payload))

    return response


@router.get("/internal/movies/{movie_id}/used-ticket", dependencies=[Depends(verify_internal_token)])
async def check_user_used_ticket(
    movie_id: int,
    user_email: str = Query(...),
    db: Session = Depends(get_db),
):
    """
    Endpoint interno para catalog-service.
    Retorna si el usuario tiene al menos un ticket USED para la película dada
    (QR escaneado) y su primer nombre para mostrarlo en las reseñas.
    Protegido por X-Internal-Token (ver verify_internal_token) — no por JWT
    de usuario, ni por "nivel de red" (eso no existe realmente: Traefik no
    filtra /internal/*, y en Render cada servicio tiene su propia URL pública).
    """
    from app.models.user import User as UserModel
    user = db.query(UserModel).filter(UserModel.email == user_email).first()
    has_used = (
        db.query(Ticket)
        .join(Purchase, Ticket.purchase_id == Purchase.id)
        .filter(
            Purchase.movie_id == movie_id,
            Purchase.user_id == user.id if user else False,
            Ticket.status == TicketStatus.USED,
        )
        .first()
    ) is not None if user else False
    return {
        "has_used_ticket": has_used,
        "first_name": user.first_name if user else None,
    }


@router.get(
    "/internal/users/{user_id}/purchases",
    response_model=List[InternalUserPurchaseResponse],
    dependencies=[Depends(verify_internal_token)],
)
async def get_user_purchases_internal(
    user_id: int,
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=20, ge=1, le=100),
    purchase_status: str = Query(default=None, alias="status"),
    db: Session = Depends(get_db),
):
    """
    Endpoint interno para user-service (GET /api/v1/users/me/purchases).
    user-service ya validó el JWT y filtra por su propio user_id; esta ruta
    la protege X-Internal-Token (ver verify_internal_token), no JWT de
    usuario. Reemplaza la lectura directa que user-service hacía antes
    contra cinema_booking (ver ARCHITECTURE.md, "Aislamiento de base de
    datos por servicio").
    """
    q = (
        db.query(Purchase)
        .options(selectinload(Purchase.tickets), selectinload(Purchase.movie))
        .filter(Purchase.user_id == user_id)
    )
    if purchase_status:
        q = q.filter(Purchase.status == purchase_status)
    purchases = q.order_by(Purchase.id.desc()).offset(skip).limit(limit).all()
    return [InternalUserPurchaseResponse.from_orm(p) for p in purchases]


# ── Internas, para admin-service (panel de compras y reportes de ventas) ──────
# Reemplazan la lectura/escritura directa que admin-service hacía antes
# contra cinema_booking. Ver ARCHITECTURE.md, "Aislamiento de base de datos
# por servicio", caso 3.

@router.get(
    "/internal/admin/purchases",
    response_model=List[InternalAdminPurchaseResponse],
    dependencies=[Depends(verify_internal_token)],
)
async def list_purchases_internal(
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    movie_id: Optional[int] = Query(None),
    user_id: Optional[int] = Query(None),
    purchase_status: Optional[str] = Query(None, alias="status"),
    db: Session = Depends(get_db),
):
    q = db.query(Purchase).options(
        joinedload(Purchase.movie),
        joinedload(Purchase.user),
        joinedload(Purchase.tickets),
    )
    if movie_id:
        q = q.filter(Purchase.movie_id == movie_id)
    if user_id:
        q = q.filter(Purchase.user_id == user_id)
    if purchase_status:
        q = q.filter(Purchase.status == purchase_status.lower())
    purchases = q.order_by(Purchase.created_at.desc()).offset(skip).limit(limit).all()
    return [InternalAdminPurchaseResponse.from_orm(p) for p in purchases]


@router.get(
    "/internal/admin/reports/sales",
    response_model=SalesReport,
    dependencies=[Depends(verify_internal_token)],
)
async def sales_report_internal(db: Session = Depends(get_db)):
    total_purchases = db.query(Purchase).filter(Purchase.status == PurchaseStatus.CONFIRMED).count()
    total_revenue = db.query(func.sum(Purchase.total_amount)).filter(
        Purchase.status == PurchaseStatus.CONFIRMED
    ).scalar() or 0
    total_tickets = db.query(func.sum(Purchase.quantity)).filter(
        Purchase.status == PurchaseStatus.CONFIRMED
    ).scalar() or 0
    total_refunds = db.query(Purchase).filter(Purchase.status == PurchaseStatus.REFUNDED).count()
    total_refunded_amount = db.query(func.sum(Purchase.total_amount)).filter(
        Purchase.status == PurchaseStatus.REFUNDED
    ).scalar() or 0
    total_cancelled = db.query(Purchase).filter(Purchase.status == PurchaseStatus.CANCELLED).count()
    avg = total_revenue / total_purchases if total_purchases > 0 else 0

    return SalesReport(
        total_purchases=total_purchases,
        total_revenue=float(total_revenue),
        total_tickets_sold=int(total_tickets),
        average_purchase_amount=round(avg, 2),
        total_refunds=total_refunds,
        total_refunded_amount=float(total_refunded_amount),
        total_cancelled=total_cancelled,
        currency="COP",
    )


@router.get(
    "/internal/admin/reports/by-movie",
    response_model=MovieSalesReport,
    dependencies=[Depends(verify_internal_token)],
)
async def report_by_movie_internal(db: Session = Depends(get_db)):
    rows = (
        db.query(
            Movie.id.label("movie_id"),
            Movie.title.label("movie_title"),
            func.count(Purchase.id).label("purchases_count"),
            func.sum(Purchase.quantity).label("tickets_sold"),
            func.sum(
                case((Purchase.status == PurchaseStatus.CONFIRMED, Purchase.total_amount), else_=0)
            ).label("revenue"),
            func.sum(
                case((Purchase.status == PurchaseStatus.REFUNDED, Purchase.total_amount), else_=0)
            ).label("refunded_amount"),
        )
        .join(Movie, Purchase.movie_id == Movie.id)
        .filter(Purchase.status.in_([PurchaseStatus.CONFIRMED, PurchaseStatus.REFUNDED]))
        .group_by(Movie.id, Movie.title)
        .order_by(
            func.sum(
                case((Purchase.status == PurchaseStatus.CONFIRMED, Purchase.total_amount), else_=0)
            ).desc()
        )
        .all()
    )
    items = []
    for r in rows:
        revenue = float(r.revenue or 0)
        refunded = float(r.refunded_amount or 0)
        items.append({
            "movie_id": r.movie_id,
            "movie_title": r.movie_title,
            "purchases_count": r.purchases_count or 0,
            "tickets_sold": int(r.tickets_sold or 0),
            "revenue": revenue,
            "refunded_amount": refunded,
            "net_revenue": revenue - refunded,
        })
    return MovieSalesReport(items=items, currency="COP")


@router.get(
    "/internal/admin/reports/by-date",
    response_model=DateSalesReport,
    dependencies=[Depends(verify_internal_token)],
)
async def report_by_date_internal(
    period: str = Query("daily", pattern="^(daily|weekly|monthly)$"),
    db: Session = Depends(get_db),
):
    trunc_map = {"daily": "day", "weekly": "week", "monthly": "month"}
    trunc = trunc_map.get(period, "day")

    rows = (
        db.query(
            func.date_trunc(trunc, Purchase.created_at).label("period"),
            func.count(Purchase.id).label("purchases_count"),
            func.sum(Purchase.quantity).label("tickets_sold"),
            func.sum(Purchase.total_amount).label("revenue"),
        )
        .filter(Purchase.status == PurchaseStatus.CONFIRMED)
        .group_by(func.date_trunc(trunc, Purchase.created_at))
        .order_by(func.date_trunc(trunc, Purchase.created_at))
        .all()
    )
    items = [
        {
            "period": r.period.isoformat() if r.period else "",
            "purchases_count": r.purchases_count or 0,
            "tickets_sold": int(r.tickets_sold or 0),
            "revenue": float(r.revenue or 0),
        }
        for r in rows
    ]
    return DateSalesReport(items=items, period_type=period, currency="COP")


@router.get("/showtimes/{showtime_id}/occupied-seats")
async def get_occupied_seats(
    showtime_id: int,
    movie_id: Optional[int] = Query(default=None),
    show_date: Optional[date_type] = Query(default=None),
    show_time: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
):
    """
    Retorna los asientos ocupados (ACTIVE o USED) para una función confirmada.

    Estrategia de búsqueda (en orden):
    1. Tickets cuyo showtime_id coincide directamente.
    2. Tickets cuya compra tiene ese showtime_id (cubre el caso en que el ticket se
       creó sin showtime_id pero la compra sí lo tiene).
    3a. Fallback para datos históricos sin showtime_id: si se pasan movie_id + show_date
        + show_time, también se incluyen tickets de compras confirmadas que coincidan
        por esos tres campos (compras antiguas donde showtime_id era null).
    3b. Fallback adicional: compras donde show_time también es NULL pero coinciden en
        movie_id + show_date (cubre el caso en que el frontend no envió show_time,
        e.g. cuando el objeto showtime no tenía campo time).
    """
    occupied: set[str] = set()

    # ── 1 & 2: showtime_id en ticket o en la compra ─────────────────────────
    rows = (
        db.query(Ticket.seat_number)
        .join(Purchase, Ticket.purchase_id == Purchase.id)
        .filter(
            or_(
                Ticket.showtime_id == showtime_id,
                Purchase.showtime_id == showtime_id,
            ),
            Ticket.status.in_([TicketStatus.ACTIVE, TicketStatus.USED]),
            Purchase.status == PurchaseStatus.CONFIRMED,
        )
        .all()
    )
    occupied.update(row[0] for row in rows)

    if movie_id and show_date:
        base_filters = [
            Purchase.showtime_id.is_(None),
            Purchase.movie_id == movie_id,
            Purchase.show_date == show_date,
            Ticket.status.in_([TicketStatus.ACTIVE, TicketStatus.USED]),
            Purchase.status == PurchaseStatus.CONFIRMED,
        ]

        # ── 3a: Fallback con show_time exacto ───────────────────────────────
        if show_time:
            fallback_rows = (
                db.query(Ticket.seat_number)
                .join(Purchase, Ticket.purchase_id == Purchase.id)
                .filter(*base_filters, Purchase.show_time == show_time)
                .all()
            )
            occupied.update(row[0] for row in fallback_rows)

        # ── 3b: Fallback para compras donde show_time también es NULL ────────
        null_time_rows = (
            db.query(Ticket.seat_number)
            .join(Purchase, Ticket.purchase_id == Purchase.id)
            .filter(*base_filters, Purchase.show_time.is_(None))
            .all()
        )
        occupied.update(row[0] for row in null_time_rows)

    return {"showtime_id": showtime_id, "seats": list(occupied)}


@router.post("/tickets/{ticket_code}/validate", response_model=TicketResponse)
async def validate_ticket(
    ticket_code: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Valida un boleto en la entrada de la sala.
    Solo usuarios con rol 'admin' o 'staff' pueden validar boletos.
    Marca el boleto como USED si está ACTIVE y la compra está CONFIRMED.
    """
    if current_user.role not in ("admin", "scanner"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Solo administradores o empleados de sala pueden validar boletos.",
        )

    ticket = (
        db.query(Ticket)
        .filter(Ticket.ticket_code == ticket_code)
        .first()
    )
    if not ticket:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Boleto no encontrado.")

    if ticket.status == TicketStatus.USED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El boleto ya fue utilizado.",
        )
    if ticket.status == TicketStatus.CANCELLED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El boleto está cancelado y no es válido.",
        )

    purchase = db.query(Purchase).filter(Purchase.id == ticket.purchase_id).first()
    if not purchase or purchase.status != PurchaseStatus.CONFIRMED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El boleto pertenece a una compra que no está confirmada.",
        )

    # Verificar que ninguna otra boleta para el mismo asiento en la misma función
    # ya haya sido utilizada — protección contra boletas duplicadas por doble venta.
    # Funciona con o sin showtime_id.
    purchase_of_ticket = db.query(Purchase).filter(Purchase.id == ticket.purchase_id).first()
    if ticket.showtime_id:
        dup_q = db.query(Ticket).filter(
            Ticket.showtime_id == ticket.showtime_id,
            Ticket.seat_number == ticket.seat_number,
            Ticket.status == TicketStatus.USED,
            Ticket.id != ticket.id,
        )
    elif purchase_of_ticket and purchase_of_ticket.show_date and purchase_of_ticket.show_time:
        dup_q = db.query(Ticket).join(
            Purchase, Ticket.purchase_id == Purchase.id
        ).filter(
            Purchase.movie_id == purchase_of_ticket.movie_id,
            Purchase.show_date == purchase_of_ticket.show_date,
            Purchase.show_time == purchase_of_ticket.show_time,
            Ticket.seat_number == ticket.seat_number,
            Ticket.status == TicketStatus.USED,
            Ticket.id != ticket.id,
        )
    else:
        dup_q = None

    if dup_q is not None and dup_q.first():
        # Este ticket es consecuencia de una doble venta — invalidarlo
        ticket.status = TicketStatus.CANCELLED
        db.commit()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"El asiento {ticket.seat_number} ya fue utilizado con otra boleta. "
                   "Esta boleta ha sido invalidada por duplicidad.",
        )

    ticket.status = TicketStatus.USED
    db.commit()
    db.refresh(ticket)

    # Publicar evento para auditoría y notificaciones
    asyncio.create_task(publish_event("ticket.validated", {
        "ticket_code":  ticket.ticket_code,
        "seat_number":  ticket.seat_number,
        "purchase_id":  ticket.purchase_id,
        "scanned_at":   datetime.utcnow().isoformat(),
        "scanner_id":   current_user.id,
        "scanner_email": current_user.email,
    }))

    return TicketResponse.from_orm(ticket)
