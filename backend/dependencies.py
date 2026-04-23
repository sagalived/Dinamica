from fastapi import Header, HTTPException, status, Depends
from sqlalchemy.orm import Session
from sqlalchemy import select

from backend.database import get_db
from backend.models import AppUser
from backend.security import decode_access_token


async def get_current_user(
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> AppUser:
    """Dependency to get current user from JWT token."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token de acesso ausente.",
        )

    token = authorization.replace("Bearer ", "", 1)
    payload = decode_access_token(token)
    email = payload.get("sub")
    user = db.scalar(select(AppUser).where(AppUser.email == email))
    if user is None or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Usuario nao autorizado.")
    return user
