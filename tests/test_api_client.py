"""
Unit tests for API client.

Run with: pytest tests/test_api_client.py -v
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "collectors"))

import unittest
from unittest.mock import patch, MagicMock

from utils.api import OSRSAPIClient, APIRateLimitError


class TestOSRSAPIClient(unittest.TestCase):
    """Test cases for OSRSAPIClient class."""

    def setUp(self):
        """Set up test fixtures."""
        self.api_client = OSRSAPIClient()

    @patch("utils.api.requests.Session")
    def test_get_5m_data(self, mock_session_class):
        """Test fetching 5m data."""
        # Setup mock
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "timestamp": 1705312800,
            "data": {
                "4151": {
                    "avgHighPrice": 2850000,
                    "avgLowPrice": 2840000,
                    "highPriceVolume": 150,
                    "lowPriceVolume": 200,
                }
            },
        }
        mock_session.get.return_value = mock_response

        # Create new client with mocked session
        client = OSRSAPIClient()
        client.session = mock_session

        result = client.get_5m_data()

        self.assertIn("data", result)
        self.assertIn("4151", result["data"])

    @patch("utils.api.requests.Session")
    def test_rate_limit_handling(self, mock_session_class):
        """Test rate limit error handling."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"Retry-After": "60"}
        mock_response.text = "Rate limited"
        mock_session.get.return_value = mock_response

        client = OSRSAPIClient()
        client.session = mock_session

        with self.assertRaises(APIRateLimitError):
            client.get_5m_data()

    @patch("utils.api.requests.Session")
    def test_server_error_handling(self, mock_session_class):
        """Test server error handling."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_session.get.return_value = mock_response

        client = OSRSAPIClient()
        client.session = mock_session

        with self.assertRaises(Exception):
            client.get_5m_data()

    def test_parse_5m_record_with_valid_data(self):
        """Test parsing valid 5m record."""
        item_data = {
            "avgHighPrice": 2850000,
            "avgLowPrice": 2840000,
            "highPriceVolume": 150,
            "lowPriceVolume": 200,
        }

        result = self.api_client.parse_5m_record(4151, item_data, 1705312800)

        self.assertIsNotNone(result)
        self.assertEqual(result["item_id"], 4151)
        self.assertEqual(result["avg_high_price"], 2850000)

    def test_parse_5m_record_with_no_data(self):
        """Test parsing record with no trading activity."""
        item_data = {
            "avgHighPrice": None,
            "avgLowPrice": None,
            "highPriceVolume": None,
            "lowPriceVolume": None,
        }

        result = self.api_client.parse_5m_record(4151, item_data, 1705312800)

        self.assertIsNone(result)

    def test_parse_item_metadata(self):
        """Test parsing item metadata."""
        item_data = {
            "name": "Abyssal whip",
            "examine": "A weapon from the abyss.",
            "members": True,
            "tradeable": True,
            "limit": 70,
        }

        result = self.api_client.parse_item_metadata(4151, item_data)

        self.assertEqual(result["item_id"], 4151)
        self.assertEqual(result["name"], "Abyssal whip")
        self.assertEqual(result["buy_limit"], 70)


if __name__ == "__main__":
    unittest.main()
