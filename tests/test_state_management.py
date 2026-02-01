"""
Unit tests for state management.

Run with: pytest tests/test_state_management.py -v
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "collectors"))

import unittest
from datetime import timedelta
from unittest.mock import Mock

from utils.state import StateManager, BackfillProgressTracker
from utils.timezone import now_utc


class TestStateManager(unittest.TestCase):
    """Test cases for StateManager class."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_db = Mock()
        self.state_manager = StateManager("test_collector", self.mock_db)

    def test_load_state_no_previous_state(self):
        """Test loading state when no previous state exists."""
        self.mock_db.execute_dict.return_value = []

        result = self.state_manager.load_state()

        self.assertIsNone(result)

    def test_load_state_with_existing_state(self):
        """Test loading existing state."""
        ts = now_utc()
        self.mock_db.execute_dict.return_value = [
            {"last_processed_timestamp": ts, "status": "running", "metadata": "{}"}
        ]

        result = self.state_manager.load_state()

        self.assertIsNotNone(result)

    def test_save_state(self):
        """Test saving state."""
        ts = now_utc()

        self.state_manager.save_state(ts, status="running")

        # Verify database was called
        self.mock_db.execute.assert_called_once()

    def test_calculate_restart_timestamp_no_state(self):
        """Test restart calculation with no previous state."""
        self.mock_db.execute_dict.return_value = []
        self.mock_db.get_last_timestamp.return_value = None

        result = self.state_manager.calculate_restart_timestamp()

        self.assertIsNone(result)

    def test_record_error(self):
        """Test error recording."""
        self.state_manager.record_error("Test error message")

        # Verify database was called
        self.mock_db.execute.assert_called_once()

    def test_clear_error(self):
        """Test clearing error state."""
        self.state_manager.clear_error()

        # Verify database was called
        self.mock_db.execute.assert_called_once()


class TestBackfillProgressTracker(unittest.TestCase):
    """Test cases for BackfillProgressTracker class."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_db = Mock()
        self.tracker = BackfillProgressTracker(self.mock_db)

    def test_start_backfill(self):
        """Test starting backfill tracking."""
        target_ts = now_utc() - timedelta(days=30)
        self.mock_db.execute.return_value = [(1,)]

        result = self.tracker.start_backfill("test_backfill", target_ts)

        self.assertEqual(result, 1)
        self.mock_db.execute.assert_called_once()

    def test_update_progress(self):
        """Test updating progress."""
        current_ts = now_utc()

        self.tracker.update_progress(1, current_ts, 10, 100, 5)

        self.mock_db.execute.assert_called_once()

    def test_complete_backfill(self):
        """Test completing backfill."""
        self.tracker.complete_backfill(1)

        self.mock_db.execute.assert_called_once()


if __name__ == "__main__":
    unittest.main()
