# app/services/booking.py
import asyncio
import json
import logging
import secrets
import string
from datetime import date as date_type, datetime, timedelta, timezone
from typing import Any, List

import httpx
from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core import redis_client, circuit_breaker
from app.core.redis_client import hold_seat, release_seat, reassign_hold, release_seats_for_order
from app.kafka.producer import publish_event as _publish_kafka_event
from app.models.movie import Movie
from app.models.purchase import Purchase, Ticket, PurchaseStatus, TicketStatus
from app.models.user import User
from app.schemas.purchase import PurchaseCreate

_PENDING_PAYMENT_TTL_SECONDS = 15 * 60
_PENDING_PAYMENT_PREFIX = "booking:pending-payment:"
_PAYMENT_IN_FLIGHT_GUARD_SECONDS = 20

logger = logging.getLogger(__name__)


def _generate_ticket_code() -> str:
    chars = string.ascii_uppercase + string.digits
    random_part = "".join(secrets.choice(chars) for _ in range(7))
    return f"CINE-{random_part}"


async def _fetch_seat_map(movie_id: int, showtime_id: int) -> dict | None:
    """
    Consulta el mapa de asientos en catalog-service.
    Retorna el dict del seat map o None si el showtime no tiene template asignado.
    Lanza HTTPException en error de red o si el showtime no existe.
    """
    url = f"{settings.CATALOG_SERVICE_URL}/api/v1/movies/{movie_id}/showtimes/{showtime_id}/seats"
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(url)
            if resp.status_code == 404:
                # Puede ser showtime inexistente o sin template — devolver None
                return None
            resp.raise_for_status()
            return resp.json()
        except HTTPException:
            raise
        except httpx.TransportError as e:
            last_exc = e
            if attempt < 2:
                await asyncio.sleep(1.0 * (2 ** attempt))
            continue
        except Exception as e:
            last_exc = e
            break

    logger.error(
        "Error consultando seat map | movie_id=%s showtime_id=%s error=%s",
        movie_id, showtime_id, last_exc,
    )
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="No se pudo verificar el mapa de asientos de la función.",
    )


async def _validate_and_hold_seats(
    movie_id: int,
    showtime_id: int,
    order_id: int,
    selected_seats: list[str],
) -> None:
    """
    Valida que todos los asientos estén disponibles y adquiere sus holds Redis.
    Lanza HTTPException 409 si algún asiento no está disponible o ya fue tomado.
    """
    seat_map = await _fetch_seat_map(movie_id, showtime_id)

    if seat_map is not None:
        # Validar contra el template de sala si existe
        seat_status: dict[str, str] = {}
        for row in seat_map.get("rows", []):
            for s in row.get("seats", []):
                seat_status[s["code"]] = s["status"]

        for code in selected_seats:
            st = seat_status.get(code)
            if st is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"El asiento {code} no existe en esta sala.",
                )
            if st != "available":
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"El asiento {code} no está disponible (estado: {st}). Selecciona otro.",
                )
    # Si no hay template de sala, se omite la validación del mapa pero los holds
    # Redis se aplican igual para prevenir doble reserva concurrente.

    # Adquirir holds uno a uno — SETNX atómico
    acquired: list[str] = []
    for code in selected_seats:
        if hold_seat(showtime_id, code, order_id):
            acquired.append(code)
        else:
            # Otro usuario tomó el asiento — liberar los que ya adquirimos
            release_seats_for_order(showtime_id, acquired)
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"El asiento {code} acaba de ser reservado por otro usuario. Selecciona otro.",
            )


async def _fetch_showtime(movie_id: int, showtime_id: int) -> dict:
    """
    Valida que el showtime exista en catalog-service y pertenezca al movie_id indicado.
    Retorna el dict del showtime (id, show_date, show_time, available_tickets, capacity).
    Lanza HTTPException si no existe o no tiene disponibilidad.
    """
    url = f"{settings.CATALOG_SERVICE_URL}/api/v1/movies/{movie_id}/showtimes/{showtime_id}"
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(url)
            if resp.status_code == 404:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="La función seleccionada no existe o no está disponible.",
                )
            resp.raise_for_status()
            return resp.json()
        except HTTPException:
            raise
        except httpx.TransportError as e:
            last_exc = e
            if attempt < 2:
                await asyncio.sleep(1.0 * (2 ** attempt))
            continue
        except Exception as e:
            last_exc = e
            break

    logger.error(
        "Error validating showtime | movie_id=%s showtime_id=%s error=%s",
        movie_id, showtime_id, last_exc,
    )
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="No se pudo verificar la disponibilidad de la función.",
    )


async def _derive_showtime_id(
    movie_id: int,
    show_date: date_type,
    show_time: str,
) -> int | None:
    """
    Consulta el catálogo para encontrar el showtime_id que coincide con
    movie_id + show_date + show_time.  Útil como fallback cuando el cliente
    no envió showtime_id (e.g., por pérdida de estado de navegación).
    Retorna None si no se encuentra o si el catálogo no está disponible.
    """
    url = f"{settings.CATALOG_SERVICE_URL}/api/v1/movies/{movie_id}/showtimes"
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(url, params={
                "start_date": show_date.isoformat(),
                "end_date": show_date.isoformat(),
            })
        if resp.status_code != 200:
            return None
        showtimes = resp.json()
        matched = next(
            (st for st in showtimes if st.get("show_time") == show_time),
            None,
        )
        if matched:
            derived_id = matched.get("id")
            logger.info(
                "showtime_id auto-derivado | movie_id=%s show_date=%s show_time=%s → showtime_id=%s",
                movie_id, show_date, show_time, derived_id,
            )
            return derived_id
        logger.warning(
            "No se pudo auto-derivar showtime_id | movie_id=%s show_date=%s show_time=%s — "
            "showtimes disponibles: %s",
            movie_id, show_date, show_time,
            [(st.get("id"), st.get("show_time")) for st in showtimes],
        )
        return None
    except Exception as exc:
        logger.warning("Error en auto-derivación de showtime_id: %s", exc)
        return None


async def create_purchase(db: Session, user_id: int, purchase_data: PurchaseCreate) -> Purchase:
    """
    Crea una compra en estado PENDING y deja el contexto de pago
    en Redis para que el saga continúe por eventos.
    """
    payment_cache = redis_client._get_client()
    if payment_cache is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis no disponible para procesar compras asíncronas.",
        )

    movie = db.query(Movie).filter(
        Movie.id == purchase_data.movie_id,
        Movie.is_active == True,
    ).first()

    if not movie:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Movie not found or not available")

    # Resolver showtime_id: usar el que envió el cliente o derivarlo del catálogo
    # cuando el cliente no lo envió pero sí tiene show_date + show_time.
    effective_showtime_id: int | None = purchase_data.showtime_id
    if not effective_showtime_id and purchase_data.show_date and purchase_data.show_time:
        effective_showtime_id = await _derive_showtime_id(
            purchase_data.movie_id,
            purchase_data.show_date,
            purchase_data.show_time,
        )

    # Si se especifica un showtime, validar disponibilidad a nivel de función
    showtime_data: dict | None = None
    if effective_showtime_id:
        showtime_data = await _fetch_showtime(purchase_data.movie_id, effective_showtime_id)
        showtime_available = showtime_data.get("available_tickets", 0)
        if showtime_available < purchase_data.quantity:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Solo quedan {showtime_available} boletos disponibles para esa función.",
            )
    elif not movie.can_purchase(purchase_data.quantity):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Only {movie.available_tickets} tickets available",
        )

    total_amount = movie.price * purchase_data.quantity

    user = db.query(User).filter(User.id == user_id, User.is_active == True).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")

    preview_tickets = _build_ticket_preview(db, purchase_data.quantity, purchase_data.selected_seats)

    # Validar y retener asientos si se especificaron
    # IMPORTANTE: esto debe ocurrir ANTES de crear la compra en DB para que el order_id no
    # quede persistido si los holds fallan. Usamos un order_id temporal basado en user+time
    # y luego los confirmamos con el order_id real tras el flush.
    if purchase_data.selected_seats and effective_showtime_id:
        # Pre-validación: consultamos el mapa y adquirimos holds con order_id=0 (placeholder)
        # Los holds se reasignarán al order_id real si la DB confirma la compra.
        # Para atomicidad práctica usamos un ID temporal negativo que no puede chocar.
        import time as _time
        _tmp_order_id = -(int(_time.time() * 1000) % 2**30)
        await _validate_and_hold_seats(
            purchase_data.movie_id,
            effective_showtime_id,
            _tmp_order_id,
            purchase_data.selected_seats,
        )
    else:
        _tmp_order_id = None

    stored_payment_info = _build_pending_payment_info(purchase_data)

    # Resolve show_date/show_time/theater: prefer showtime entity values over free-form fields
    resolved_show_date = purchase_data.show_date
    resolved_show_time = purchase_data.show_time
    resolved_theater_name: str | None = None
    resolved_theater_location: str | None = None
    resolved_show_format: str | None = None
    if showtime_data:
        raw_date = showtime_data.get("show_date")
        if raw_date:
            resolved_show_date = (
                date_type.fromisoformat(raw_date) if isinstance(raw_date, str) else raw_date
            )
        resolved_show_time = showtime_data.get("show_time") or resolved_show_time
        resolved_theater_name = showtime_data.get("theater_name")
        resolved_theater_location = showtime_data.get("theater_location")
        raw_format = showtime_data.get("format")
        if raw_format:
            resolved_show_format = raw_format

    try:
        purchase = Purchase(
            user_id=user_id,
            movie_id=purchase_data.movie_id,
            quantity=purchase_data.quantity,
            total_amount=total_amount,
            status=PurchaseStatus.PENDING,
            payment_info=stored_payment_info,
            show_date=resolved_show_date,
            show_time=resolved_show_time,
            showtime_id=effective_showtime_id,
        )
        db.add(purchase)
        db.flush()

        # Reasignar holds al order_id real — usa SET atómico (sin soltar la clave)
        # para evitar la ventana de carrera que existía entre release_seat + hold_seat.
        if purchase_data.selected_seats and effective_showtime_id and _tmp_order_id is not None:
            for code in purchase_data.selected_seats:
                reassign_hold(effective_showtime_id, code, purchase.id)

        _store_pending_payment_context(
            order_id=purchase.id,
            purchase_data=purchase_data,
            movie=movie,
            user=user,
            total_amount=total_amount,
            created_at_iso=(
                purchase.created_at.isoformat()
                if purchase.created_at
                else _utcnow_iso()
            ),
            preview_tickets=preview_tickets,
            resolved_show_date=resolved_show_date,
            resolved_show_time=resolved_show_time,
            resolved_theater_name=resolved_theater_name,
            resolved_theater_location=resolved_theater_location,
            resolved_show_format=resolved_show_format,
            effective_showtime_id=effective_showtime_id,
        )
        db.commit()
        db.refresh(purchase)
        return purchase

    except HTTPException:
        db.rollback()
        _delete_pending_payment_context(purchase.id if "purchase" in locals() else None)
        # Liberar holds si la DB falló después de haberlos reasignado
        if purchase_data.selected_seats and effective_showtime_id:
            release_seats_for_order(effective_showtime_id, purchase_data.selected_seats)
        raise
    except Exception as e:
        db.rollback()
        _delete_pending_payment_context(purchase.id if "purchase" in locals() else None)
        # Liberar holds si la DB falló
        if purchase_data.selected_seats and effective_showtime_id:
            release_seats_for_order(effective_showtime_id, purchase_data.selected_seats)
        logger.error("DB error creating purchase: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error processing purchase",
        )


def _pending_payment_key(order_id: int) -> str:
    return f"{_PENDING_PAYMENT_PREFIX}{order_id}"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _utcnow_iso() -> str:
    return _utcnow().isoformat()


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _build_ticket_preview(
    db: Session,
    quantity: int,
    selected_seats: list[str] | None = None,
) -> List[dict[str, str]]:
    tickets: List[dict[str, str]] = []
    for i in range(quantity):
        code = _generate_ticket_code()
        while db.query(Ticket).filter(Ticket.ticket_code == code).first():
            code = _generate_ticket_code()
        seat = selected_seats[i] if selected_seats else f"GENERAL-{i + 1}"
        tickets.append({"code": code, "seat": seat, "status": "ACTIVE"})
    return tickets


def _build_pending_payment_info(purchase_data: PurchaseCreate) -> dict[str, Any]:
    if purchase_data.pse_info:
        return {
            "payment_method": "pse",
            "bank_name": purchase_data.pse_info.bank_name,
            "payer_email": purchase_data.pse_info.payer_email,
            "status": "pending_inventory",
        }

    return {
        "payment_method": "card",
        "last_four": purchase_data.payment_info.card_number[-4:],
        "card_holder": purchase_data.payment_info.card_holder,
        "status": "pending_inventory",
    }


def _store_pending_payment_context(
    order_id: int,
    purchase_data: PurchaseCreate,
    movie: Movie,
    user: User,
    total_amount: float,
    created_at_iso: str | None,
    preview_tickets: List[dict[str, str]],
    resolved_show_date=None,
    resolved_show_time: str | None = None,
    resolved_theater_name: str | None = None,
    resolved_theater_location: str | None = None,
    resolved_show_format: str | None = None,
    effective_showtime_id: int | None = None,
) -> None:
    client = redis_client._get_client()
    if client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis no disponible para guardar el contexto del pago.",
        )

    if purchase_data.pse_info:
        payment_request = {
            "flow": "pse",
            "payload": {
                "bank_code": purchase_data.pse_info.bank_code,
                "bank_name": purchase_data.pse_info.bank_name,
                "document_type": purchase_data.pse_info.document_type,
                "document_number": purchase_data.pse_info.document_number,
                "payer_email": purchase_data.pse_info.payer_email,
                "amount": total_amount,
            },
        }
    else:
        payment_request = {
            "flow": "card",
            "payload": {
                "card_number": purchase_data.payment_info.card_number,
                "card_holder": purchase_data.payment_info.card_holder,
                "expiry_month": purchase_data.payment_info.expiry_month,
                "expiry_year": purchase_data.payment_info.expiry_year,
                "cvv": purchase_data.payment_info.cvv,
                "amount": total_amount,
            },
        }

    context = {
        "state": "awaiting_inventory",
        "created_at": created_at_iso or _utcnow_iso(),
        "updated_at": _utcnow_iso(),
        "attempts": 0,
        "payment_request": payment_request,
        "event_context": {
            "order_id": order_id,
            "user_id": user.id,
            "user_email": user.email,
            "customer_name": user.full_name,
            "movie_id": movie.id,
            "movie_title": movie.title,
            "movie_genre": movie.genre,
            "movie_duration": movie.duration,
            "movie_rating": movie.rating,
            "quantity": purchase_data.quantity,
            "total_amount": total_amount,
            "purchase_created_at": created_at_iso,
            "show_date": (
                resolved_show_date.isoformat()
                if hasattr(resolved_show_date, "isoformat")
                else resolved_show_date
            ),
            "show_time": resolved_show_time,
            "showtime_id": effective_showtime_id if effective_showtime_id is not None else purchase_data.showtime_id,
            "theater_name": resolved_theater_name,
            "theater_location": resolved_theater_location,
            "show_format": resolved_show_format,
            "tickets": preview_tickets,
        },
    }
    client.setex(_pending_payment_key(order_id), _PENDING_PAYMENT_TTL_SECONDS, json.dumps(context))


def _load_pending_payment_context(order_id: int) -> dict[str, Any] | None:
    client = redis_client._get_client()
    if client is None:
        return None
    raw = client.get(_pending_payment_key(order_id))
    return json.loads(raw) if raw else None


def _save_pending_payment_context(order_id: int, context: dict[str, Any]) -> None:
    client = redis_client._get_client()
    if client is None:
        return
    context["updated_at"] = _utcnow_iso()
    client.setex(_pending_payment_key(order_id), _PENDING_PAYMENT_TTL_SECONDS, json.dumps(context))


def _update_pending_payment_context(order_id: int, **updates: Any) -> dict[str, Any] | None:
    context = _load_pending_payment_context(order_id)
    if context is None:
        return None
    context.update(updates)
    _save_pending_payment_context(order_id, context)
    return context


def _delete_pending_payment_context(order_id: int | None) -> None:
    if order_id is None:
        return
    client = redis_client._get_client()
    if client is None:
        return
    client.delete(_pending_payment_key(order_id))


def _set_purchase_payment_state(db: Session, purchase: Purchase, status_value: str, **extra: Any) -> None:
    payment_info = dict(purchase.payment_info or {})
    payment_info["status"] = status_value
    payment_info.update(extra)
    purchase.payment_info = payment_info
    db.add(purchase)


async def trigger_payment_for_order(db_factory, order_id: int) -> None:
    context = _load_pending_payment_context(order_id)
    if context is None:
        logger.error("Pending payment context not found | order_id=%s", order_id)
        await mark_purchase_cancelled(
            db_factory,
            order_id=order_id,
            reason="No se encontró el contexto temporal del pago.",
            release_inventory=True,
        )
        return

    attempts = int(context.get("attempts", 0))
    last_attempt_at = _parse_iso_datetime(context.get("last_attempt_at"))
    if last_attempt_at and (_utcnow() - last_attempt_at).total_seconds() < _PAYMENT_IN_FLIGHT_GUARD_SECONDS:
        logger.info("Payment initiation already in flight | order_id=%s", order_id)
        return

    next_attempt = attempts + 1
    _update_pending_payment_context(
        order_id,
        state="payment_initiated",
        attempts=next_attempt,
        last_attempt_at=_utcnow_iso(),
        last_error=None,
    )

    with db_factory() as db:
        purchase = db.query(Purchase).filter(Purchase.id == order_id).first()
        if purchase and purchase.status == PurchaseStatus.PENDING:
            _set_purchase_payment_state(
                db,
                purchase,
                "payment_initiated",
                payment_attempts=next_attempt,
            )
            db.commit()

    payment_request = context["payment_request"]
    event = {
        "flow": payment_request["flow"],
        **payment_request["payload"],
        "order_context": context["event_context"],
    }

    try:
        await _publish_kafka_event("payment.initiated", event)
        _update_pending_payment_context(order_id, state="awaiting_payment_result", last_error=None)
        with db_factory() as db:
            purchase = db.query(Purchase).filter(Purchase.id == order_id).first()
            if purchase and purchase.status == PurchaseStatus.PENDING:
                _set_purchase_payment_state(db, purchase, "awaiting_payment_result", payment_attempts=next_attempt)
                db.commit()
    except Exception as exc:
        logger.error("payment.initiated publish failed | order_id=%s | error=%s", order_id, exc)
        _update_pending_payment_context(
            order_id,
            state="payment_retry",
            last_error="No fue posible publicar el evento de pago.",
        )
        with db_factory() as db:
            purchase = db.query(Purchase).filter(Purchase.id == order_id).first()
            if purchase and purchase.status == PurchaseStatus.PENDING:
                _set_purchase_payment_state(
                    db, purchase, "payment_retry",
                    payment_attempts=next_attempt,
                    last_error="No fue posible publicar el evento de pago.",
                )
                db.commit()


async def mark_purchase_cancelled(
    db_factory,
    order_id: int,
    reason: str,
    release_inventory: bool,
) -> None:
    with db_factory() as db:
        purchase = db.query(Purchase).filter(Purchase.id == order_id).first()
        if not purchase or purchase.status != PurchaseStatus.PENDING:
            return

        _set_purchase_payment_state(db, purchase, "failed", failure_reason=reason)
        purchase.status = PurchaseStatus.CANCELLED
        db.commit()
        movie_id = purchase.movie_id
        quantity = purchase.quantity
        showtime_id = purchase.showtime_id

    # Liberar holds Redis si hay contexto de pago con asientos retenidos
    context = _load_pending_payment_context(order_id)
    if context:
        ec = context.get("event_context", {})
        ctx_showtime_id = ec.get("showtime_id") or showtime_id
        seats = [
            t["seat"] for t in ec.get("tickets", [])
            if t.get("seat", "").startswith(tuple("ABCDEFGHIJ"))
        ]
        if ctx_showtime_id and seats:
            release_seats_for_order(ctx_showtime_id, seats)

    _delete_pending_payment_context(order_id)

    if release_inventory:
        from app.kafka.producer import publish_event

        await publish_event(
            "inventory.release",
            {
                "order_id": order_id,
                "movie_id": movie_id,
                "quantity": quantity,
            },
        )


async def confirm_purchase_from_payment(db_factory, payload: dict[str, Any]) -> None:
    order_id = payload.get("order_id")
    if not order_id:
        logger.warning("payment.success sin order_id: %s", payload)
        return

    with db_factory() as db:
        purchase = db.query(Purchase).filter(Purchase.id == order_id).first()
        if not purchase:
            logger.warning("payment.success para compra inexistente | order_id=%s", order_id)
            return
        if purchase.status == PurchaseStatus.CONFIRMED:
            logger.info("payment.success duplicado ignorado | order_id=%s", order_id)
            return
        if purchase.status != PurchaseStatus.PENDING:
            logger.warning("payment.success fuera de secuencia | order_id=%s status=%s", order_id, purchase.status.value)
            return

        # Guard against double-booking: check for already-confirmed tickets on the same seats.
        # Works with or without showtime_id — falls back to (movie_id, show_date, show_time) match.
        incoming_seats = [
            t["seat"] for t in payload.get("tickets", [])
            if not t["seat"].startswith("GENERAL-")
        ]
        if incoming_seats:
            if purchase.showtime_id:
                conflict_q = db.query(Ticket).join(
                    Purchase, Ticket.purchase_id == Purchase.id
                ).filter(
                    Ticket.showtime_id == purchase.showtime_id,
                    Ticket.seat_number.in_(incoming_seats),
                    Ticket.status.in_([TicketStatus.ACTIVE, TicketStatus.USED]),
                    Ticket.purchase_id != purchase.id,
                )
            else:
                # sin showtime_id: comparar por película + fecha + hora
                conflict_q = db.query(Ticket).join(
                    Purchase, Ticket.purchase_id == Purchase.id
                ).filter(
                    Purchase.movie_id == purchase.movie_id,
                    Purchase.show_date == purchase.show_date,
                    Purchase.show_time == purchase.show_time,
                    Purchase.status == PurchaseStatus.CONFIRMED,
                    Ticket.seat_number.in_(incoming_seats),
                    Ticket.status.in_([TicketStatus.ACTIVE, TicketStatus.USED]),
                    Ticket.purchase_id != purchase.id,
                )
            conflict = conflict_q.first()
            if conflict:
                logger.error(
                    "DOUBLE-BOOKING DETECTADO | order_id=%s showtime_id=%s seat=%s "
                    "conflicting_purchase=%s — cancelando compra",
                    order_id, purchase.showtime_id, conflict.seat_number, conflict.purchase_id,
                )
                db.close()
                await mark_purchase_cancelled(
                    db_factory,
                    order_id=order_id,
                    reason=f"El asiento {conflict.seat_number} ya fue vendido a otra persona.",
                    release_inventory=True,
                )
                return

        existing_codes = {ticket.ticket_code for ticket in purchase.tickets}
        created_seats: list[str] = []
        for ticket_payload in payload.get("tickets", []):
            if ticket_payload["code"] in existing_codes:
                continue
            db.add(
                Ticket(
                    purchase_id=purchase.id,
                    ticket_code=ticket_payload["code"],
                    seat_number=ticket_payload["seat"],
                    showtime_id=purchase.showtime_id,
                    status=TicketStatus.ACTIVE,
                )
            )
            created_seats.append(ticket_payload["seat"])

        movie = db.query(Movie).filter(Movie.id == purchase.movie_id).first()
        if movie:
            movie.available_tickets = max(movie.available_tickets - purchase.quantity, 0)

        _set_purchase_payment_state(
            db,
            purchase,
            "approved",
            transaction_id=payload.get("transaction_id"),
            last_four=payload.get("payment_last_four", "****"),
            payment_attempts=(purchase.payment_info or {}).get("payment_attempts"),
        )
        purchase.status = PurchaseStatus.CONFIRMED
        showtime_id_for_release = purchase.showtime_id
        db.commit()

    # Liberar holds Redis tras confirmar los tickets en DB
    if showtime_id_for_release and created_seats:
        release_seats_for_order(showtime_id_for_release, created_seats)

    # Publicar purchase.confirmed con los datos reales del email.
    # Este evento se publica SOLO si la compra se confirmó sin conflictos.
    # La notification-service escucha purchase.confirmed (no payment.success) para
    # evitar enviar correos de compras que luego se cancelan por doble venta.
    context = _load_pending_payment_context(order_id)
    ec = (context or {}).get("event_context", {})

    with db_factory() as _db:
        real_tickets = [
            {"code": t.ticket_code, "seat": t.seat_number, "status": "ACTIVE"}
            for t in _db.query(Ticket).filter(Ticket.purchase_id == order_id).all()
        ]

    confirmed_event = {
        **ec,
        "order_id":          order_id,
        "transaction_id":    payload.get("transaction_id") or ec.get("transaction_id"),
        "payment_last_four": payload.get("payment_last_four", "****"),
        "tickets":           real_tickets,
    }
    await _publish_kafka_event("purchase.confirmed", confirmed_event)

    _update_pending_payment_context(order_id, state="payment_succeeded")
    _delete_pending_payment_context(order_id)


async def fail_purchase_from_payment(db_factory, payload: dict[str, Any]) -> None:
    order_id = payload.get("order_id")
    if not order_id:
        logger.warning("payment.failed sin order_id: %s", payload)
        return

    await mark_purchase_cancelled(
        db_factory,
        order_id=order_id,
        reason=payload.get("failure_reason", "Pago rechazado."),
        release_inventory=True,
    )


def _should_release_inventory_for_purchase(purchase: Purchase) -> bool:
    payment_status = (purchase.payment_info or {}).get("status")
    return payment_status in {"payment_initiated", "payment_retry", "awaiting_payment_result", "approved"}


async def reconcile_pending_purchases(db_factory) -> None:
    now = _utcnow()
    stale_before = now - timedelta(seconds=settings.PAYMENT_FLOW_STALE_SECONDS)
    inventory_timeout_before = now - timedelta(seconds=settings.INVENTORY_DECISION_TIMEOUT_SECONDS)
    retry_before = now - timedelta(seconds=settings.PAYMENT_RETRY_INTERVAL_SECONDS)

    with db_factory() as db:
        pending_purchases = (
            db.query(Purchase)
            .filter(Purchase.status == PurchaseStatus.PENDING)
            .order_by(Purchase.created_at.asc())
            .all()
        )

    for purchase in pending_purchases:
        context = _load_pending_payment_context(purchase.id)
        payment_status = (purchase.payment_info or {}).get("status")
        created_at = purchase.created_at
        if created_at is None:
            created_at_utc = now
        elif created_at.tzinfo is None:
            created_at_utc = created_at.replace(tzinfo=timezone.utc)
        else:
            created_at_utc = created_at.astimezone(timezone.utc)

        if context is None:
            await mark_purchase_cancelled(
                db_factory,
                order_id=purchase.id,
                reason="Se perdió el contexto temporal de la compra pendiente.",
                release_inventory=_should_release_inventory_for_purchase(purchase),
            )
            continue

        state = context.get("state")
        attempts = int(context.get("attempts", 0))
        last_attempt_at = _parse_iso_datetime(context.get("last_attempt_at"))

        if payment_status == "pending_inventory" and created_at_utc < inventory_timeout_before:
            await mark_purchase_cancelled(
                db_factory,
                order_id=purchase.id,
                reason="No llegó una decisión de inventario a tiempo.",
                release_inventory=False,
            )
            continue

        if state == "payment_retry":
            if attempts >= settings.PAYMENT_MAX_INIT_ATTEMPTS:
                await mark_purchase_cancelled(
                    db_factory,
                    order_id=purchase.id,
                    reason="No se logró iniciar el pago tras varios intentos.",
                    release_inventory=True,
                )
                continue

            if last_attempt_at is None or last_attempt_at < retry_before:
                logger.info(
                    "Retrying pending payment initiation | order_id=%s | attempt=%s",
                    purchase.id,
                    attempts + 1,
                )
                await trigger_payment_for_order(db_factory, purchase.id)
                continue

        if created_at_utc < stale_before:
            await mark_purchase_cancelled(
                db_factory,
                order_id=purchase.id,
                reason="La compra pendiente excedió el tiempo máximo de reconciliación.",
                release_inventory=_should_release_inventory_for_purchase(purchase),
            )


async def call_refund_service(transaction_id: str, amount: float) -> dict:
    """
    Llama a payment-service para procesar el reembolso de una transacción.
    Si el servicio falla, lanza HTTPException para que el cancelar se aborte.
    """
    if circuit_breaker.is_open():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de reembolsos no disponible temporalmente. Intenta de nuevo en unos minutos.",
        )

    payload = {"transaction_id": transaction_id, "amount": amount}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{settings.PAYMENT_SERVICE_URL}/payments/refund",
                json=payload,
            )
        resp.raise_for_status()
        circuit_breaker.record_success()
        return resp.json()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code >= 500:
            circuit_breaker.record_failure()
        logger.error("Refund HTTP error: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Error procesando el reembolso. Intenta de nuevo.",
        )
    except Exception as exc:
        circuit_breaker.record_failure()
        logger.error("Refund service unreachable: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de reembolsos no disponible. Intenta de nuevo en unos minutos.",
        )
