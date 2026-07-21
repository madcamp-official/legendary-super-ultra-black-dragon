from __future__ import annotations

import os
from collections.abc import Iterator

from sqlalchemy import create_engine, event
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


DEFAULT_DATABASE_URL = "postgresql+psycopg://dure:dure@127.0.0.1/dure"


class Base(DeclarativeBase):
    pass


def _configure_sqlite_connection(dbapi_connection, _connection_record) -> None:
    dbapi_connection.isolation_level = None
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA foreign_keys=ON")
    finally:
        cursor.close()


def _begin_sqlite_transaction(connection) -> None:
    connection.exec_driver_sql("BEGIN")


def database_url() -> str:
    return os.environ.get("DURE_DATABASE_URL", DEFAULT_DATABASE_URL)


def make_engine(url: str | None = None):
    value = url or database_url()
    kwargs = {"pool_pre_ping": True}
    if value.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    engine = create_engine(value, **kwargs)
    if engine.dialect.name == "sqlite":
        event.listen(engine, "connect", _configure_sqlite_connection)
        event.listen(engine, "begin", _begin_sqlite_transaction)
    return engine


def make_session_factory(engine=None):
    return sessionmaker(bind=engine or make_engine(), expire_on_commit=False, class_=Session)


def session_dependency(factory) -> Iterator[Session]:
    with factory() as session:
        yield session
