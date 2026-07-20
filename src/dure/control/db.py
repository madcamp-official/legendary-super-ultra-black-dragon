from __future__ import annotations

import os
from collections.abc import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


DEFAULT_DATABASE_URL = "postgresql+psycopg://dure:dure@127.0.0.1/dure"


class Base(DeclarativeBase):
    pass


def database_url() -> str:
    return os.environ.get("DURE_DATABASE_URL", DEFAULT_DATABASE_URL)


def make_engine(url: str | None = None):
    value = url or database_url()
    kwargs = {"pool_pre_ping": True}
    if value.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    return create_engine(value, **kwargs)


def make_session_factory(engine=None):
    return sessionmaker(bind=engine or make_engine(), expire_on_commit=False, class_=Session)


def session_dependency(factory) -> Iterator[Session]:
    with factory() as session:
        yield session
