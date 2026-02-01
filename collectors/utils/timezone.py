"""
Timezone utilities for OSRS Market Data Collector.

This module ensures all datetime operations use UTC timezone consistently.
It provides validation and conversion utilities with strict mode support.
"""

import logging
import os
from datetime import datetime
from typing import Optional, Union

import pytz

logger = logging.getLogger(__name__)


class TimezoneValidationError(Exception):
    """Raised when a naive datetime is detected in strict mode."""

    pass


class TimezoneManager:
    """
    Manages timezone operations and enforces UTC consistency.

    This class provides utilities for:
    - Converting naive datetimes to UTC
    - Validating timezone-aware datetimes
    - Enforcing strict mode (fail on naive datetimes)
    - Converting between different timezone representations
    """

    def __init__(self, strict_mode: Optional[bool] = None):
        """
        Initialize the timezone manager.

        Args:
            strict_mode: If True, raise exception on naive datetimes.
                        If None, read from TIMEZONE_STRICT_MODE env var.
        """
        if strict_mode is None:
            strict_mode = os.getenv("TIMEZONE_STRICT_MODE", "true").lower() == "true"

        self.strict_mode = strict_mode
        self.utc = pytz.UTC

        # Log initialization
        logger.info(
            f"[TIMEZONE] TimezoneManager initialized - Strict mode: {strict_mode}"
        )
        logger.info(
            f"[TIMEZONE] Python process timezone: {datetime.now().astimezone().tzinfo}"
        )

    def ensure_utc(
        self, dt: Union[datetime, str, int, float], from_source: str = "unknown"
    ) -> datetime:
        """
        Ensure a datetime is timezone-aware and in UTC.

        Args:
            dt: Datetime object, ISO string, or Unix timestamp
            from_source: Description of where this timestamp came from (for logging)

        Returns:
            Timezone-aware datetime in UTC

        Raises:
            TimezoneValidationError: If strict_mode is True and naive datetime detected
        """
        if dt is None:
            return None

        # Convert string to datetime
        if isinstance(dt, str):
            try:
                # Try ISO format with timezone
                dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
            except ValueError:
                try:
                    # Try without timezone and assume UTC
                    dt = datetime.fromisoformat(dt)
                    if dt.tzinfo is None:
                        if self.strict_mode:
                            raise TimezoneValidationError(
                                f"Naive datetime string from {from_source}: {dt}"
                            )
                        logger.warning(
                            f"[TIMEZONE] Naive datetime string from {from_source}, "
                            f"assuming UTC: {dt}"
                        )
                        dt = dt.replace(tzinfo=self.utc)
                except ValueError as e:
                    raise ValueError(f"Unable to parse datetime string: {dt}") from e

        # Convert Unix timestamp to datetime
        elif isinstance(dt, (int, float)):
            dt = datetime.fromtimestamp(dt, tz=self.utc)

        # Ensure it's a datetime object
        if not isinstance(dt, datetime):
            raise TypeError(f"Expected datetime, got {type(dt)}")

        # Check if naive
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            if self.strict_mode:
                raise TimezoneValidationError(
                    f"[STRICT MODE] Naive datetime detected from {from_source}: {dt}"
                )
            logger.warning(
                f"[TIMEZONE] Converting naive datetime from {from_source} to UTC: {dt}"
            )
            dt = dt.replace(tzinfo=self.utc)
        else:
            # Convert to UTC if it's in a different timezone
            if dt.tzinfo != self.utc:
                original_tz = str(dt.tzinfo)
                dt = dt.astimezone(self.utc)
                logger.debug(f"[TIMEZONE] Converted from {original_tz} to UTC: {dt}")

        return dt

    def validate_utc(self, dt: datetime, context: str = "") -> None:
        """
        Validate that a datetime is timezone-aware in UTC.

        Args:
            dt: Datetime to validate
            context: Additional context for error messages

        Raises:
            TimezoneValidationError: If datetime is naive or not in UTC
        """
        if dt is None:
            return

        if not isinstance(dt, datetime):
            raise TypeError(f"Expected datetime, got {type(dt)}")

        if dt.tzinfo is None:
            raise TimezoneValidationError(
                f"{context} - Naive datetime (no timezone info): {dt}"
            )

        if dt.tzinfo != self.utc:
            raise TimezoneValidationError(
                f"{context} - Datetime not in UTC (found {dt.tzinfo}): {dt}"
            )

    def now_utc(self) -> datetime:
        """Get current time in UTC."""
        return datetime.now(self.utc)

    def parse_api_timestamp(
        self, timestamp: Union[str, int], api_tz_info: Optional[str] = None
    ) -> datetime:
        """
        Parse timestamp from API response.

        The OSRS Wiki API returns timestamps in Unix epoch format (seconds).
        This method handles both integer timestamps and ISO strings.

        Args:
            timestamp: Unix timestamp (seconds) or ISO string
            api_tz_info: Optional timezone info from API (for logging)

        Returns:
            UTC datetime
        """
        if isinstance(timestamp, int):
            # Unix timestamp in seconds
            dt = datetime.fromtimestamp(timestamp, tz=self.utc)
            logger.debug(f"[TIMEZONE] Parsed API timestamp (Unix): {timestamp} -> {dt}")
            return dt
        elif isinstance(timestamp, str):
            # Try to parse as integer first
            try:
                ts_int = int(timestamp)
                dt = datetime.fromtimestamp(ts_int, tz=self.utc)
                logger.debug(
                    f"[TIMEZONE] Parsed API timestamp (string int): {timestamp} -> {dt}"
                )
                return dt
            except ValueError:
                # Parse as ISO string
                dt = self.ensure_utc(timestamp, from_source="API")
                return dt
        else:
            raise TypeError(f"Unexpected timestamp type: {type(timestamp)}")

    def format_for_db(self, dt: datetime) -> str:
        """
        Format datetime for database insertion.

        PostgreSQL TIMESTAMPTZ accepts ISO format strings.

        Args:
            dt: UTC datetime

        Returns:
            ISO format string
        """
        self.validate_utc(dt, "format_for_db")
        return dt.isoformat()

    def get_timezone_info(self) -> dict:
        """
        Get current timezone configuration information.

        Returns:
            Dictionary with timezone information
        """
        now = datetime.now()
        now_utc = self.now_utc()

        return {
            "python_process_tz": str(now.astimezone().tzinfo),
            "utc_now": now_utc.isoformat(),
            "local_now": now.isoformat(),
            "strict_mode": self.strict_mode,
            "pytz_version": pytz.__version__,  # type: ignore
        }


# Global timezone manager instance
tz_manager = TimezoneManager()


# Convenience functions for module-level access
def ensure_utc(
    dt: Union[datetime, str, int, float], from_source: str = "unknown"
) -> datetime:
    """Ensure datetime is UTC (uses global tz_manager)."""
    return tz_manager.ensure_utc(dt, from_source)


def validate_utc(dt: datetime, context: str = "") -> None:
    """Validate datetime is UTC (uses global tz_manager)."""
    tz_manager.validate_utc(dt, context)


def now_utc() -> datetime:
    """Get current UTC time (uses global tz_manager)."""
    return tz_manager.now_utc()


def parse_api_timestamp(
    timestamp: Union[str, int], api_tz_info: Optional[str] = None
) -> datetime:
    """Parse API timestamp (uses global tz_manager)."""
    return tz_manager.parse_api_timestamp(timestamp, api_tz_info)


def format_for_db(dt: datetime) -> str:
    """Format for database (uses global tz_manager)."""
    return tz_manager.format_for_db(dt)
