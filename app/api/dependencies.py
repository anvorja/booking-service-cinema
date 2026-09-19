# app/api/dependencies.py
from fastapi import Depends, Header, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.database import get_db
from app.core import redis_client
from app.models.user import User

security = HTTPBearer()


async def verify_internal_token(x_internal_token: str = Header(None)) -> None:
    """
    Guard para rutas /internal/* consumidas por otros microservicios
    (no por el usuario final). Ver INTERNAL_SERVICE_TOKEN en config.py.
    """
    if not settings.INTERNAL_SERVICE_TOKEN or x_internal_token != settings.INTERNAL_SERVICE_TOKEN:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid internal token")


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
) -> User:
    """
    Valida el JWT, verifica blacklist en Redis y devuelve el usuario.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    token = credentials.credentials

    # Check Redis blacklist — token invalidated by auth-service on logout
    if redis_client.is_blacklisted(token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has been invalidated. Please login again.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        payload = jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM])
        user_email: str = payload.get("sub")
        if not user_email:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = db.query(User).filter(User.email == user_email, User.is_active == True).first()
    if not user:
        raise credentials_exception

    return user
