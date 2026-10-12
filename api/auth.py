"""FastAPI dependencies that enforce authentication and roles.

401 = no / invalid / expired token (with ``WWW-Authenticate: Bearer``).  403 = valid token, insufficient role.
"""
from __future__ import annotations

from typing import Callable

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from auth.security import Principal, Role, TokenError, decode_access_token

_bearer = HTTPBearer(auto_error=False, description="Access token from POST /api/v1/auth/login (valid 15 minutes)")


def get_principal(request: Request, creds: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> Principal:
    if creds is None or not creds.credentials:
        raise HTTPException(401, "authentication required", headers={"WWW-Authenticate": "Bearer"})
    try:
        return decode_access_token(creds.credentials, request.app.state.settings.jwt_secret)
    except TokenError:
        raise HTTPException(401, "invalid or expired token", headers={"WWW-Authenticate": "Bearer"}) from None


def require_role(minimum: Role) -> Callable[..., Principal]:
    def dependency(principal: Principal = Depends(get_principal)) -> Principal:
        if not principal.allows(minimum):
            raise HTTPException(403, f"requires role '{minimum.value}' or higher")
        return principal
    dependency.__name__ = f"require_{minimum.value}"
    return dependency


require_viewer = require_role(Role.VIEWER)
require_operator = require_role(Role.OPERATOR)
require_admin = require_role(Role.ADMIN)      # reserved: no admin-only endpoint exists yet
