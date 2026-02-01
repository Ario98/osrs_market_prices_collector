"""
OSRS Wiki API client with rate limiting and retry logic.

This module provides:
- API client for OSRS Wiki price endpoints
- Rate limiting (max 1 request per second)
- Exponential backoff retry logic
- Error handling for various API failure modes
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry  # type: ignore

from .timezone import ensure_utc, now_utc

logger = logging.getLogger(__name__)


class APIError(Exception):
    """Base exception for API operations."""

    pass


class APIRateLimitError(APIError):
    """Raised when API rate limit is exceeded."""

    pass


class APIServerError(APIError):
    """Raised when API server returns 5xx error."""

    pass


class OSRSAPIClient:
    """
    Client for OSRS Wiki Grand Exchange API.

    Endpoints:
    - /5m: 5-minute interval price/volume data
    - /latest: Most recent price snapshot
    - /item: Item metadata

    Rate Limit: Maximum 1 request per second
    """

    def __init__(self):
        """Initialize API client with configuration from environment."""
        self.base_url = os.getenv(
            "API_BASE_URL", "https://prices.runescape.wiki/api/v1/osrs"
        )
        self.user_agent = os.getenv("API_USER_AGENT", "OSRS-Market-Collector/1.0")
        self.rate_limit_seconds = float(os.getenv("API_RATE_LIMIT_SECONDS", "1.0"))

        self._last_request_time: Optional[float] = None
        self._timezone_logged = False

        # Setup session with retry strategy
        self.session = requests.Session()

        # Configure retries for specific error codes
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,  # 1s, 2s, 4s delays
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        # Set headers
        self.session.headers.update(
            {"User-Agent": self.user_agent, "Accept": "application/json"}
        )

        logger.info("[API] OSRSAPIClient initialized")
        logger.info(f"[API] Base URL: {self.base_url}")
        logger.info(f"[API] Rate limit: {self.rate_limit_seconds}s between requests")

    def _rate_limit(self) -> None:
        """
        Enforce rate limiting between requests.

        Ensures at least rate_limit_seconds between API calls.
        """
        if self._last_request_time is not None:
            elapsed = time.time() - self._last_request_time
            if elapsed < self.rate_limit_seconds:
                sleep_time = self.rate_limit_seconds - elapsed
                logger.debug(f"[API] Rate limiting: sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)

        self._last_request_time = time.time()

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make a rate-limited API request with error handling.

        Args:
            endpoint: API endpoint path (without base URL)
            params: Optional query parameters

        Returns:
            JSON response as dictionary

        Raises:
            APIError: On API failure
            APIRateLimitError: On 429 response
            APIServerError: On 5xx responses
        """
        self._rate_limit()

        url = f"{self.base_url}/{endpoint}"

        try:
            response = self.session.get(url, params=params, timeout=30)

            # Handle specific status codes
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"[API] Rate limited (429). Retry after {retry_after}s")
                raise APIRateLimitError(f"Rate limited. Retry after {retry_after}s")

            if response.status_code >= 500:
                logger.error(
                    f"[API] Server error {response.status_code}: {response.text[:200]}"
                )
                raise APIServerError(f"Server error {response.status_code}")

            response.raise_for_status()

            # Parse JSON response
            try:
                data = response.json()
            except ValueError as e:
                logger.error(f"[API] Invalid JSON response: {response.text[:200]}")
                raise APIError(f"Invalid JSON response: {e}") from e

            logger.debug(f"[API] Successfully fetched {endpoint}")
            return data

        except requests.exceptions.Timeout:
            logger.error(f"[API] Request timeout for {endpoint}")
            raise APIError(f"Request timeout for {endpoint}")

        except requests.exceptions.ConnectionError as e:
            logger.error(f"[API] Connection error: {e}")
            raise APIError(f"Connection error: {e}")

        except requests.exceptions.HTTPError as e:
            # Handle 404s silently - these are expected when probing endpoints
            if e.response.status_code == 404:
                raise APIError(f"Endpoint not found: {endpoint}")
            # Log other HTTP errors as actual errors
            logger.error(f"[API] Request failed: {e}")
            raise APIError(f"Request failed: {e}")

        except requests.exceptions.RequestException as e:
            logger.error(f"[API] Request failed: {e}")
            raise APIError(f"Request failed: {e}")

    def _log_timezone_info(self, first_timestamp: Any) -> None:
        """Log timezone information from first API response."""
        if not self._timezone_logged and first_timestamp:
            try:
                dt = ensure_utc(first_timestamp, "API")
                logger.info(f"[API] First API response timestamp: {dt.isoformat()}")
                logger.info("[API] API timezone verified: UTC")
                self._timezone_logged = True
            except Exception as e:
                logger.warning(
                    f"[API] Could not parse timestamp for timezone check: {e}"
                )

    def get_5m_data(self, timestamp: Optional[int] = None) -> Dict:
        """
        Fetch 5-minute interval data for all items.

        Args:
            timestamp: Optional Unix timestamp to get specific interval
                      (if omitted, gets most recent complete interval)

        Returns:
            Dictionary with 'data' key containing price data
            Format: {
                'timestamp': int,
                'data': {
                    'item_id': {
                        'avgHighPrice': int or None,
                        'avgLowPrice': int or None,
                        'highPriceVolume': int or None,
                        'lowPriceVolume': int or None
                    },
                    ...
                }
            }
        """
        params = {}
        if timestamp:
            params["timestamp"] = timestamp

        data = self._make_request("5m", params)

        # Log timezone on first successful call
        if "timestamp" in data:
            self._log_timezone_info(data["timestamp"])

        return data

    def get_latest_data(self) -> Dict:
        """
        Fetch latest price snapshot for all items.

        Returns:
            Dictionary with 'data' key containing latest prices
            Format: {
                'data': {
                    'item_id': {
                        'high': int or None,
                        'low': int or None,
                        'highTime': int or None,
                        'lowTime': int or None
                    },
                    ...
                }
            }
        """
        data = self._make_request("latest")

        # Log timezone on first successful call
        if data and "data" in data:
            first_item = next(iter(data["data"].values()))
            if first_item and "highTime" in first_item:
                self._log_timezone_info(first_item["highTime"])

        return data

    def get_item_metadata(self) -> Dict:
        """
        Fetch item metadata for all items.

        Returns:
            Dictionary with item metadata
            Format: {
                'item_id': {
                    'name': str,
                    'examine': str,
                    'members': bool,
                    'tradeable': bool,
                    ...
                },
                ...
            }
        """
        # Try /mapping endpoint first (this is the correct OSRS Wiki API endpoint)
        try:
            logger.debug("[API] Trying /mapping endpoint for item metadata...")
            return self._make_request("mapping")
        except APIError:
            # Fallback: try /items endpoint (alternative API pattern)
            logger.debug("[API] /mapping failed, trying /items endpoint...")
            try:
                return self._make_request("items")
            except APIError:
                # If both fail, return empty dict - we'll extract item IDs from price data
                logger.warning(
                    "[API] Item metadata endpoints not available, will extract from price data"
                )
                return {}

    def get_all_item_ids(self) -> List[int]:
        """
        Get list of all item IDs from metadata.

        Returns:
            List of item IDs as integers
        """
        metadata = self.get_item_metadata()
        return [int(item_id) for item_id in metadata.keys()]

    def parse_5m_record(
        self, item_id: int, data: Dict, timestamp: int
    ) -> Optional[Dict]:
        """
        Parse a 5m data record into standardized format.

        Args:
            item_id: Item ID
            data: Raw data dict from API
            timestamp: Unix timestamp from API

        Returns:
            Standardized record dict or None if no valid data
        """
        # Check if we have any valid price data
        has_high = data.get("avgHighPrice") is not None
        has_low = data.get("avgLowPrice") is not None

        if not has_high and not has_low:
            return None  # No trading activity for this item in this interval

        # Parse timestamp to UTC datetime
        dt = ensure_utc(timestamp, "API_5m")

        return {
            "item_id": item_id,
            "timestamp": dt,
            "avg_high_price": data.get("avgHighPrice"),
            "avg_low_price": data.get("avgLowPrice"),
            "high_price_volume": data.get("highPriceVolume"),
            "low_price_volume": data.get("lowPriceVolume"),
        }

    def parse_latest_record(self, item_id: int, data: Dict) -> Optional[Dict]:
        """
        Parse a latest data record into standardized format.

        Args:
            item_id: Item ID
            data: Raw data dict from API

        Returns:
            Standardized record dict or None if no valid data
        """
        # Check if we have any valid data
        has_high = data.get("high") is not None
        has_low = data.get("low") is not None

        if not has_high and not has_low:
            return None

        # Get timestamp (prefer high time if available)
        timestamp = data.get("highTime") or data.get("lowTime")
        if timestamp:
            dt = ensure_utc(timestamp, "API_latest")
        else:
            dt = now_utc()

        return {
            "item_id": item_id,
            "timestamp": dt,
            "avg_high_price": data.get("high"),
            "avg_low_price": data.get("low"),
            "high_price_volume": None,  # Not provided by /latest endpoint
            "low_price_volume": None,
        }

    def parse_item_metadata(self, item_id: int, data: Dict) -> Dict:
        """
        Parse item metadata into standardized format.

        Args:
            item_id: Item ID
            data: Raw metadata dict from API

        Returns:
            Standardized metadata dict
        """
        return {
            "item_id": item_id,
            "name": data.get("name"),
            "description": data.get("examine"),
            "members": data.get("members", False),
            "tradeable": data.get("tradeable", True),
            "tradeable_on_ge": data.get("tradeable_on_ge", True),
            "stackable": data.get("stackable", False),
            "noted": data.get("noted", False),
            "noteable": data.get("noteable", False),
            "equipable": data.get("equipable", False),
            "equipable_by_player": data.get("equipable_by_player", False),
            "equipable_weapon": data.get("equipable_weapon", False),
            "cost": data.get("cost"),
            "lowalch": data.get("lowalch"),
            "highalch": data.get("highalch"),
            "weight": data.get("weight"),
            "buy_limit": data.get("limit"),
            "quest_item": data.get("quest_item", False),
            "release_date": data.get("release_date"),
            "duplicate": data.get("duplicate", False),
            "placeholder": data.get("placeholder", False),
            "wiki_name": data.get("wiki_name"),
            "wiki_url": data.get("wiki_url"),
        }


# Global API client instance
api_client = OSRSAPIClient()
