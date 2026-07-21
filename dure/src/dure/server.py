from __future__ import annotations

import argparse
import os
from pathlib import Path

from .envfile import parse_secure_env_file


SERVER_ENV_KEYS = frozenset({"DURE_DATABASE_URL", "DURE_ADMIN_TOKEN"})


def _server_env_values(explicit_path: Path | None) -> dict[str, str]:
    if explicit_path is not None:
        return parse_secure_env_file(
            explicit_path,
            keys=SERVER_ENV_KEYS,
            required=True,
            required_description="DURE_DATABASE_URL and DURE_ADMIN_TOKEN",
        )
    working_directory = Path.cwd()
    for candidate in (working_directory / "dure" / ".env", working_directory / ".env"):
        values = parse_secure_env_file(
            candidate,
            keys=SERVER_ENV_KEYS,
            required=False,
            required_description="DURE_DATABASE_URL and DURE_ADMIN_TOKEN",
        )
        if values:
            return values
    return {}


def _check_database_connection(database_url: str | None) -> None:
    from sqlalchemy import text
    from sqlalchemy.exc import SQLAlchemyError

    from .control.db import make_engine

    engine = None
    try:
        engine = make_engine(database_url)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        raise RuntimeError(
            "database connection failed; verify DURE_DATABASE_URL and PostgreSQL credentials"
        ) from exc
    finally:
        if engine is not None:
            engine.dispose()


def migrate(database_url: str | None = None) -> None:
    from alembic import command
    from alembic.config import Config
    from .control.db import database_url as configured_database_url

    config = Config()
    config.set_main_option("script_location", str(Path(__file__).parent / "control" / "migrations"))
    config.set_main_option("sqlalchemy.url", database_url or configured_database_url())
    command.upgrade(config, "head")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dure-server", description="Dure central control plane")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--database-url")
    parser.add_argument(
        "--env-file",
        type=Path,
        help=(
            "Read DURE_DATABASE_URL and DURE_ADMIN_TOKEN from this owner-only dotenv file"
        ),
    )
    parser.add_argument("--migrate", action="store_true", help="Apply database migrations and exit")
    parser.add_argument("--create-schema", action="store_true", help="Development only; use Alembic in production")
    args = parser.parse_args(argv)
    try:
        configured_server = _server_env_values(args.env_file)
        database_url = (
            args.database_url
            or configured_server.get("DURE_DATABASE_URL")
            or os.environ.get("DURE_DATABASE_URL")
        )
        admin_token = configured_server.get("DURE_ADMIN_TOKEN") or os.environ.get(
            "DURE_ADMIN_TOKEN"
        )
        if not args.migrate and not admin_token:
            raise ValueError(
                "DURE_ADMIN_TOKEN is required; configure a secure server env file"
            )
        _check_database_connection(database_url)
        if args.migrate:
            migrate(database_url)
            return 0
        import uvicorn
        from .control.api import create_app
    except ValueError as exc:
        parser.error(str(exc))
    except ImportError as exc:
        dependency = getattr(exc, "name", None) or str(exc)
        parser.error(
            f"control-plane dependency is missing ({dependency}); "
            "install Dure with the server extra: python3 -m pip install 'dure[server]'"
        )
    except RuntimeError as exc:
        parser.error(str(exc))

    uvicorn.run(
        create_app(
            database_url=database_url,
            admin_token=admin_token,
            create_schema=args.create_schema,
        ),
        host=args.host,
        port=args.port,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
