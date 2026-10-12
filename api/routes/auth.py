from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from fastapi import Request

from api.auth import require_viewer
from api.dependencies import get_session
from api.schemas import ErrorBody, Item
from auth.security import Principal
from services.auth_service import AuthService

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])
log = logging.getLogger("api.auth")


class LoginIn(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1, max_length=128)      # the policy is enforced at user creation, not here


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int                                          # seconds (900 = 15 minutes)


class MeOut(BaseModel):
    user_id: int
    role: str


@router.post("/login", response_model=TokenOut, responses={401: {"model": ErrorBody}})
def login(body: LoginIn, request: Request, session: Session = Depends(get_session)):
    """Exchange a username and password for a 15-minute bearer token. Public (no token needed)."""
    svc, metrics = AuthService(), getattr(request.app.state, "metrics", None)
    user = svc.authenticate(session, body.username, body.password)
    if user is None:
        if metrics:
            metrics.login_attempts.labels(result="failure").inc()
        log.warning("login failed")                          # the attempted username is deliberately NOT logged
        raise HTTPException(401, "invalid username or password", headers={"WWW-Authenticate": "Bearer"})
    token, expires_in = svc.issue_token(user, request.app.state.settings.jwt_secret)
    if metrics:
        metrics.login_attempts.labels(result="success").inc()
    log.info("login ok user_id=%s role=%s", user.id, user.role)
    return TokenOut(access_token=token, expires_in=expires_in)


@router.get("/me", response_model=Item[MeOut])
def me(principal: Principal = Depends(require_viewer)):
    """The identity and role carried by the presented token (any authenticated role)."""
    return Item[MeOut](data=MeOut(user_id=principal.user_id, role=principal.role.value))
