from __future__ import annotations

import argparse
from pathlib import Path


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
    parser.add_argument("--migrate", action="store_true", help="Apply database migrations and exit")
    parser.add_argument("--create-schema", action="store_true", help="Development only; use Alembic in production")
    args = parser.parse_args(argv)
    try:
        if args.migrate:
            migrate(args.database_url)
            return 0
        import uvicorn
        from .control.api import create_app
    except ImportError as exc:
        dependency = getattr(exc, "name", None) or str(exc)
        parser.error(
            f"control-plane dependency is missing ({dependency}); "
            "install Dure with the server extra: python3 -m pip install 'dure[server]'"
        )

    uvicorn.run(create_app(database_url=args.database_url, create_schema=args.create_schema), host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
