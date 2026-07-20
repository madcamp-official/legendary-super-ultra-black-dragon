from __future__ import annotations

import unittest
from unittest.mock import patch

from dure import __version__
from dure.http import JSONClient


class FakeResponse:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self) -> bytes:
        return b'{"ok": true}'


class JSONClientTests(unittest.TestCase):
    def test_request_identifies_dure_instead_of_python_urllib(self):
        with patch("dure.http.urllib.request.urlopen", return_value=FakeResponse()) as urlopen:
            result = JSONClient("https://control.example").request("GET", "/health")

        request = urlopen.call_args.args[0]
        self.assertEqual(request.get_header("User-agent"), f"Dure/{__version__}")
        self.assertEqual(result, {"ok": True})


if __name__ == "__main__":
    unittest.main()
