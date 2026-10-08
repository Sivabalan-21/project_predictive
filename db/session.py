from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from db.config import DatabaseSettings


def make_engine(settings: DatabaseSettings) -> Engine:
    return create_engine(settings.url, pool_size=settings.pool_size, pool_pre_ping=True, echo=settings.echo)


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False)
