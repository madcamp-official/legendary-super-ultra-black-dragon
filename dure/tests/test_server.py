from __future__ import annotations

import io
import os
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import Mock, patch

from dure.server import _check_database_connection, main


class ServerEnvFileTests(unittest.TestCase):
    def _write_env(self, path: Path, content: str, mode: int = 0o600) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        path.chmod(mode)

    def test_migrate_automatically_uses_nested_dure_env_as_one_server_pair(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_env(
                root / "dure" / ".env",
                "DURE_SERVER=https://control.example\n"
                "DURE_DATABASE_URL=sqlite:////file-db\n"
                "DURE_ADMIN_TOKEN=file-token\n",
            )
            with patch.dict(
                os.environ,
                {
                    "DURE_DATABASE_URL": "sqlite:////stale-db",
                    "DURE_ADMIN_TOKEN": "stale-token",
                },
                clear=True,
            ), patch("dure.server.Path.cwd", return_value=root), patch(
                "dure.server._check_database_connection"
            ) as check_database, patch("dure.server.migrate") as migrate:
                result = main(["--migrate"])

        self.assertEqual(result, 0)
        check_database.assert_called_once_with("sqlite:////file-db")
        migrate.assert_called_once_with("sqlite:////file-db")

    def test_server_passes_same_explicit_file_pair_to_database_and_app(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "server.env"
            self._write_env(
                path,
                "DURE_DATABASE_URL=sqlite:////file-db\n"
                "DURE_ADMIN_TOKEN=file-token\n",
            )
            application = object()
            with patch.dict(os.environ, {}, clear=True), patch(
                "dure.server._check_database_connection"
            ) as check_database, patch(
                "dure.control.api.create_app", return_value=application
            ) as create_app, patch("uvicorn.run") as run:
                result = main(
                    [
                        "--env-file",
                        str(path),
                        "--host",
                        "0.0.0.0",
                        "--port",
                        "8081",
                    ]
                )

        self.assertEqual(result, 0)
        check_database.assert_called_once_with("sqlite:////file-db")
        create_app.assert_called_once_with(
            database_url="sqlite:////file-db",
            admin_token="file-token",
            create_schema=False,
        )
        run.assert_called_once_with(application, host="0.0.0.0", port=8081)

    def test_server_rejects_partial_or_group_readable_env_before_database_access(self):
        cases = (
            ("DURE_ADMIN_TOKEN=token-only\n", 0o600, "must define"),
            (
                "DURE_DATABASE_URL=sqlite:////db\nDURE_ADMIN_TOKEN=token\n",
                0o640,
                "group or others",
            ),
        )
        for content, mode, expected in cases:
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "server.env"
                self._write_env(path, content, mode)
                error = io.StringIO()
                with patch("dure.server._check_database_connection") as check_database, redirect_stderr(
                    error
                ), self.assertRaises(SystemExit) as raised:
                    main(["--env-file", str(path), "--migrate"])

                self.assertEqual(raised.exception.code, 2)
                self.assertIn(expected, error.getvalue())
                check_database.assert_not_called()

    def test_server_stops_before_listening_when_database_check_fails(self):
        error = io.StringIO()
        with patch.dict(
            os.environ,
            {
                "DURE_DATABASE_URL": "postgresql+psycopg://dure:secret@127.0.0.1/dure",
                "DURE_ADMIN_TOKEN": "admin-token",
            },
            clear=True,
        ), patch("dure.server._server_env_values", return_value={}), patch(
            "dure.server._check_database_connection",
            side_effect=RuntimeError(
                "database connection failed; verify DURE_DATABASE_URL and PostgreSQL credentials"
            ),
        ), patch("uvicorn.run") as run, redirect_stderr(error), self.assertRaises(
            SystemExit
        ) as raised:
            main([])

        self.assertEqual(raised.exception.code, 2)
        self.assertIn("database connection failed", error.getvalue())
        self.assertNotIn("secret", error.getvalue())
        run.assert_not_called()

    def test_server_rejects_missing_admin_token_before_database_access(self):
        error = io.StringIO()
        with patch.dict(
            os.environ,
            {"DURE_DATABASE_URL": "sqlite:////db"},
            clear=True,
        ), patch("dure.server._server_env_values", return_value={}), patch(
            "dure.server._check_database_connection"
        ) as check_database, patch("uvicorn.run") as run, redirect_stderr(
            error
        ), self.assertRaises(SystemExit) as raised:
            main([])

        self.assertEqual(raised.exception.code, 2)
        self.assertIn("DURE_ADMIN_TOKEN is required", error.getvalue())
        check_database.assert_not_called()
        run.assert_not_called()


class DatabaseConnectionCheckTests(unittest.TestCase):
    def test_database_error_is_redacted_and_engine_is_disposed(self):
        from sqlalchemy.exc import SQLAlchemyError

        engine = Mock()
        engine.connect.side_effect = SQLAlchemyError("driver detail with secret")
        with patch("dure.control.db.make_engine", return_value=engine), self.assertRaises(
            RuntimeError
        ) as raised:
            _check_database_connection("postgresql+psycopg://dure:secret@127.0.0.1/dure")

        self.assertEqual(
            str(raised.exception),
            "database connection failed; verify DURE_DATABASE_URL and PostgreSQL credentials",
        )
        engine.dispose.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
