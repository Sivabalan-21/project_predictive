"""Login and user creation. Passwords are only ever handled here and in auth.security."""
from __future__ import annotations

import re

from sqlalchemy.orm import Session

from auth.security import Role, create_access_token, hash_password, verify_against_dummy, verify_password
from core.settings import looks_like_placeholder
from db.models import User
from db.repositories import UserRepository
from services.errors import ConflictError

USERNAME_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{2,63}$")
MIN_PASSWORD_LENGTH, MAX_PASSWORD_LENGTH = 12, 128


class InvalidUserInput(ValueError):
    """Username / password / role rejected by policy (message never contains the password)."""


def normalise_username(username: str) -> str:
    return username.strip().lower()


def validate_new_user(username: str, password: str, role: str) -> None:
    if not USERNAME_RE.match(username):
        raise InvalidUserInput("username must be 3-64 characters: lower-case letters, digits, '.', '_' or '-', "
                               "starting with a letter or digit")
    if role not in {r.value for r in Role}:
        raise InvalidUserInput(f"role must be one of {', '.join(r.value for r in Role)}")
    if not MIN_PASSWORD_LENGTH <= len(password) <= MAX_PASSWORD_LENGTH:
        raise InvalidUserInput(f"password must be {MIN_PASSWORD_LENGTH}-{MAX_PASSWORD_LENGTH} characters long")
    if username in password.lower():
        raise InvalidUserInput("password must not contain the username")
    if looks_like_placeholder(password):
        raise InvalidUserInput("password is too weak (a common or placeholder value)")


class AuthService:
    def __init__(self, users: UserRepository | None = None):
        self._users = users or UserRepository()

    def create_user(self, session: Session, username: str, password: str, role: str) -> User:
        username = normalise_username(username)
        validate_new_user(username, password, role)
        with session.begin():
            user = self._users.create_if_absent(session, username=username, password_hash=hash_password(password), role=role)
        if user is None:
            raise ConflictError(f"user {username!r} already exists")
        return user

    def authenticate(self, session: Session, username: str, password: str) -> User | None:
        """The user for valid credentials, else None. Unknown / inactive / wrong password are indistinguishable."""
        user = self._users.get_by_username(session, normalise_username(username))
        if user is None or not user.is_active:
            verify_against_dummy(password)
            return None
        return user if verify_password(user.password_hash, password) else None

    def issue_token(self, user: User, secret: str) -> tuple[str, int]:
        return create_access_token(user.id, user.role, secret)
