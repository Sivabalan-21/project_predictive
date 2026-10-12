from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from db.models import User


class UserRepository:
    def get_by_username(self, session: Session, username: str) -> User | None:
        return session.execute(select(User).where(User.username == username)).scalar_one_or_none()

    def create_if_absent(self, session: Session, *, username: str, password_hash: str, role: str) -> User | None:
        """Insert a user; None if the username already exists (atomic: relies on the unique constraint)."""
        stmt = (insert(User).values(username=username, password_hash=password_hash, role=role)
                .on_conflict_do_nothing(index_elements=[User.username]).returning(User.id))
        user_id = session.execute(stmt).scalar_one_or_none()
        return session.get(User, user_id) if user_id is not None else None
