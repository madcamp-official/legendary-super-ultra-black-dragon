import json
import unittest
from unittest.mock import MagicMock, patch

from dure.readiness import ReadinessVerifier


def _response(status=200, body=b""):
    value = MagicMock()
    value.status = status
    value.read.return_value = body
    value.__enter__.return_value = value
    value.__exit__.return_value = False
    return value


class ReadinessTests(unittest.TestCase):
    def test_api_requires_health_and_a_served_model(self):
        model_body = json.dumps({"data": [{"id": "qwen-test"}]}).encode()
        with patch(
            "dure.readiness.urllib.request.urlopen",
            side_effect=[_response(), _response(body=model_body)],
        ):
            result = ReadinessVerifier().api("http://127.0.0.1:8000")

        self.assertTrue(result.ok, result.detail)
        self.assertIn("qwen-test", result.detail)


if __name__ == "__main__":
    unittest.main()
