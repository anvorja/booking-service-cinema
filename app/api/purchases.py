# app/api/purchases.py
import asyncio
from datetime import datetime, date as date_type, time as time_type, timedelta
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.api.dependencies import get_current_user
from app.kafka.producer import publish_event
from app.models.movie import Movie
from app.models.purchase import Purchase, PurchaseStatus, Ticket, TicketStatus
from app.models.user import User
from app.schemas.purchase import PurchaseCreate, PurchaseResponse, PurchaseListResponse, TicketResponse
from app.services.booking import create_purchase as svc_create_purchase, call_refund_service

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

    # Publicar en background — no bloquea la respuesta HTTP
    asyncio.create_task(publish_event("order.created", order_created_payload))

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
    Llama a payment-service para procesar el reembolso y restaura los tickets disponibles.
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

    # Procesar reembolso vía payment-service
    transaction_id = purchase.payment_info.get("transaction_id") if purchase.payment_info else None
    if transaction_id:
        await call_refund_service(transaction_id, float(purchase.total_amount))

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
    3. Fallback para datos históricos sin showtime_id: si se pasan movie_id + show_date
       + show_time, también se incluyen tickets de compras confirmadas que coincidan
       por esos tres campos (compras antiguas donde showtime_id era null).
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

    # ── 3: Fallback para compras históricas con showtime_id = NULL ───────────
    if movie_id and show_date and show_time:
        fallback_rows = (
            db.query(Ticket.seat_number)
            .join(Purchase, Ticket.purchase_id == Purchase.id)
            .filter(
                Purchase.showtime_id.is_(None),
                Purchase.movie_id == movie_id,
                Purchase.show_date == show_date,
                Purchase.show_time == show_time,
                Ticket.status.in_([TicketStatus.ACTIVE, TicketStatus.USED]),
                Purchase.status == PurchaseStatus.CONFIRMED,
            )
            .all()
        )
        occupied.update(row[0] for row in fallback_rows)

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
