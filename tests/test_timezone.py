"""
Unit tests for timezone utilities.

Run with: pytest tests/test_timezone.py -v
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "collectors"))

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from utils.timezone import TimezoneManager, TimezoneValidationError, ensure_utc, now_utc


class TestTimezoneManager(unittest.TestCase):
    """Test cases for TimezoneManager class."""

    def setUp(self):
        """Set up test fixtures."""
        self.tz_manager = TimezoneManager(strict_mode=False)

    def test_ensure_utc_with_naive_datetime(self):
        """Test that naive datetime is converted to UTC."""
        naive_dt = datetime(2024, 1, 15, 10, 30, 0)
        utc_dt = self.tz_manager.ensure_utc(naive_dt, "test")

        self.assertIsNotNone(utc_dt.tzinfo)
        self.assertEqual(utc_dt.tzinfo.utcoffset(utc_dt).total_seconds(), 0)

    def test_ensure_utc_with_utc_datetime(self):
        """Test that UTC datetime passes through unchanged."""
        utc_dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        result = self.tz_manager.ensure_utc(utc_dt, "test")

        self.assertEqual(result, utc_dt)

    def test_ensure_utc_with_timestamp(self):
        """Test conversion of Unix timestamp."""
        timestamp = 1705312800  # 2024-01-15 10:00:00 UTC
        result = self.tz_manager.ensure_utc(timestamp, "test")

        self.assertIsNotNone(result.tzinfo)
        self.assertEqual(result.year, 2024)

    def test_strict_mode_raises_on_naive(self):
        """Test that strict mode raises exception on naive datetime."""
        strict_manager = TimezoneManager(strict_mode=True)
        naive_dt = datetime(2024, 1, 15, 10, 30, 0)

        with self.assertRaises(TimezoneValidationError):
            strict_manager.ensure_utc(naive_dt, "test")

    def test_now_utc(self):
        """Test that now_utc returns UTC datetime."""
        result = self.tz_manager.now_utc()

        self.assertIsNotNone(result.tzinfo)
        self.assertEqual(result.tzinfo.utcoffset(result).total_seconds(), 0)

    def test_validate_utc_with_valid_datetime(self):
        """Test validation passes for valid UTC datetime."""
        utc_dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

        # Should not raise
        try:
            self.tz_manager.validate_utc(utc_dt, "test")
        except TimezoneValidationError:
            self.fail("validate_utc raised exception for valid UTC datetime")

    def test_validate_utc_with_naive_datetime(self):
        """Test validation fails for naive datetime."""
        naive_dt = datetime(2024, 1, 15, 10, 30, 0)

        with self.assertRaises(TimezoneValidationError):
            self.tz_manager.validate_utc(naive_dt, "test")

    def test_ensure_utc_with_iso_string(self):
        """Test parsing of ISO format string."""
        iso_string = "2024-01-15T10:30:00+00:00"
        result = self.tz_manager.ensure_utc(iso_string, "test")

        self.assertIsNotNone(result.tzinfo)
        self.assertEqual(result.year, 2024)
        self.assertEqual(result.month, 1)
        self.assertEqual(result.day, 15)


class TestGlobalFunctions(unittest.TestCase):
    """Test cases for global convenience functions."""

    def test_global_ensure_utc(self):
        """Test global ensure_utc function."""
        naive_dt = datetime(2024, 1, 15, 10, 30, 0)
        result = ensure_utc(naive_dt, "test")

        self.assertIsNotNone(result.tzinfo)

    def test_global_now_utc(self):
        """Test global now_utc function."""
        result = now_utc()

        self.assertIsNotNone(result.tzinfo)
        self.assertEqual(result.tzinfo.utcoffset(result).total_seconds(), 0)


class TestEnvironmentConfig(unittest.TestCase):
    """Test cases for environment-based configuration."""

    @patch.dict(os.environ, {"TIMEZONE_STRICT_MODE": "true"})
    def test_strict_mode_from_env(self):
        """Test that strict mode is read from environment."""
        from utils.timezone import TimezoneManager

        manager = TimezoneManager()

        self.assertTrue(manager.strict_mode)

    @patch.dict(os.environ, {"TIMEZONE_STRICT_MODE": "false"})
    def test_non_strict_mode_from_env(self):
        """Test that non-strict mode is read from environment."""
        from utils.timezone import TimezoneManager

        manager = TimezoneManager()

        self.assertFalse(manager.strict_mode)


if __name__ == "__main__":
    unittest.main()
