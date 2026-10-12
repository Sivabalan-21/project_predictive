"""Password hashing and JWT access tokens. No database or web-framework code here.

Design
------
* Passwords: Argon2id via argon2-cffi (library defaults = RFC 9106 low-memory profile). Verification of an unknown user
  still performs one Argon2 verification against a dummy hash, so response time does not reveal which usernames exist.
* Tokens: PyJWT, HS256 ONLY (``algorithms=["HS256"]`` is pinned on decode, so ``alg: none`` / algorithm-confusion tokens
  are rejected). Claims: iss, sub (user id), role, iat, exp, jti. ``exp`` is required and the lifetime may not exceed
  15 minutes: a token minted with a longer lifetime is rejected even if its signature is valid.
* Tokens are STATELESS: a deactivated user or a changed role takes effect when the token expires (<= 15 minutes).
"""
from __future__ import annotations

import functools
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError

ALGORITHM = "HS256"
ISSUER = "predictive-maintenance-api"
ACCESS_TOKEN_TTL = timedelta(minutes=15)
_CLOCK_LEEWAY_S = 5          # tolerated clock skew between processes


class Role(str, Enum):
    VIEWER = "viewer"        # every GET endpoint
    OPERATOR = "operator"    # viewer + resolve alerts
    ADMIN = "admin"          # operator + reserved administrative privileges (no admin-only endpoints exist yet)


ROLE_RANK = {Role.VIEWER: 1, Role.OPERATOR: 2, Role.ADMIN: 3}


class TokenError(Exception):
    """The bearer token is missing required claims, expired, tampered with, or otherwise invalid."""


@dataclass(frozen=True)
class Principal:
    user_id: int
    role: Role

    def allows(self, minimum: Role) -> bool:
        return ROLE_RANK[self.role] >= ROLE_RANK[minimum]


# ------------------------------------------------------------------------------------------------ passwords
_hasher = PasswordHasher()


def hash_password(password: str) -> str:
    return _hasher.hash(password)


def verify_password(password_hash: str, password: str) -> bool:
    try:
        return _hasher.verify(password_hash, password)
    except (VerificationError, InvalidHashError):      # VerifyMismatchError is a VerificationError
        return False


@functools.lru_cache(maxsize=1)
def _dummy_hash() -> str:
    return _hasher.hash("timing-equaliser-not-a-real-password")


def verify_against_dummy(password: str) -> None:
    """Spend the same time as a real verification (used when the username does not exist or is inactive)."""
    verify_password(_dummy_hash(), password)


# ------------------------------------------------------------------------------------------------ tokens
def create_access_token(user_id: int, role: Role | str, secret: str, *, now: datetime | None = None,
                        ttl: timedelta = ACCESS_TOKEN_TTL) -> tuple[str, int]:
    """Returns ``(token, expires_in_seconds)``. ``now``/``ttl`` exist for tests; production code uses the defaults."""
    issued = now or datetime.now(timezone.utc)
    claims = {"iss": ISSUER, "sub": str(user_id), "role": Role(role).value, "iat": issued, "exp": issued + ttl,
              "jti": uuid.uuid4().hex}
    return jwt.encode(claims, secret, algorithm=ALGORITHM), int(ttl.total_seconds())


def decode_access_token(token: str, secret: str) -> Principal:
    try:
        claims = jwt.decode(token, secret, algorithms=[ALGORITHM], issuer=ISSUER, leeway=_CLOCK_LEEWAY_S,
                            options={"require": ["exp", "iat", "sub", "role", "iss"]})
        if claims["exp"] - claims["iat"] > ACCESS_TOKEN_TTL.total_seconds() + _CLOCK_LEEWAY_S:
            raise TokenError("token lifetime exceeds the 15 minute maximum")
        return Principal(user_id=int(claims["sub"]), role=Role(claims["role"]))
    except TokenError:
        raise
    except (jwt.PyJWTError, ValueError, TypeError, KeyError) as exc:     # incl. unknown role / non-numeric sub
        raise TokenError(exc.__class__.__name__) from exc
