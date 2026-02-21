"""
Tests for the realtime collector's future-wait shutdown fix.

Before the fix, the future-wait branch used raw time.sleep(up to 60s),
ignoring SIGTERM/SIGINT until the sleep completed.

After the fix, it uses _sleep_with_check(), which polls should_stop()
every second and exits within ~1s of a shutdown request.

Run with:
    pytest tests/test_realtime_future_wait.py -v
"""

import logging
import os
import sys
import time
import unittest
from unittest.mock import Mock, patch

import pytz

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Stub python-dotenv if not installed (it's a Docker-only dep; not needed for tests)
if "dotenv" not in sys.modules:
    dotenv_stub = Mock()
    dotenv_stub.load_dotenv = lambda *a, **kw: None
    sys.modules["dotenv"] = dotenv_stub


def _mock_setup_logging(self):
    """Minimal logging setup that avoids the hardcoded /app/logs path."""
    self.logger = logging.getLogger(f"test.{self.collector_name}")


def make_collector():
    """
    Create a RealtimeCollector with all I/O side effects mocked out.

    Patches:
    - _setup_logging   -> avoids /app/logs creation (fails locally on Windows)
    - db_manager       -> no real DB connection
    - api_client       -> no real HTTP calls
    - tz_manager       -> no real timezone checks
    - StateManager     -> no real DB state queries
    """
    import collectors.base_collector as base_mod
    from collectors.base_collector import BaseCollector

    with patch.object(BaseCollector, "_setup_logging", _mock_setup_logging), \
         patch.object(base_mod, "db_manager", Mock()), \
         patch.object(base_mod, "api_client", Mock()), \
         patch.object(base_mod, "tz_manager", Mock()), \
         patch.object(base_mod, "StateManager", Mock()):
        from collectors.realtime_collector import RealtimeCollector
        return RealtimeCollector()


class TestSleepWithCheckInterruptible(unittest.TestCase):
    """
    Verify that _sleep_with_check() is interruptible.

    This is the core mechanism behind graceful shutdown — it must exit
    within one check interval (~1s) when a stop is requested.
    """

    def test_exits_immediately_when_already_stopped(self):
        """Should return within ~1s when shutdown_requested is already True."""
        collector = make_collector()
        collector.shutdown_requested = True

        start = time.time()
        collector._sleep_with_check(60)  # Would block 60s with raw time.sleep
        elapsed = time.time() - start

        self.assertLess(elapsed, 1.5, "Should exit within one check interval (~1s)")

    def test_runs_to_completion_when_running_normally(self):
        """Should sleep the full duration when no stop is requested."""
        collector = make_collector()
        collector.shutdown_requested = False
        collector.running = True

        start = time.time()
        collector._sleep_with_check(2)
        elapsed = time.time() - start

        self.assertGreaterEqual(elapsed, 1.9, "Should sleep the full requested duration")
        self.assertLess(elapsed, 4.0, "Should not take significantly longer than requested")

    def test_stops_mid_sleep_when_flag_set(self):
        """Should exit early if shutdown_requested becomes True mid-sleep."""
        collector = make_collector()
        collector.shutdown_requested = False
        collector.running = True

        # Flip the flag after a short delay from another thread
        import threading
        def flip():
            time.sleep(0.5)
            collector.shutdown_requested = True

        threading.Thread(target=flip, daemon=True).start()

        start = time.time()
        collector._sleep_with_check(30)  # Would run 30s without stop
        elapsed = time.time() - start

        # Should have exited well before 30s — around 0.5s (flag flip) + 1s (check interval)
        self.assertLess(elapsed, 3.0, "Should have exited shortly after the flag was set")


class TestFutureWaitUsesInterruptibleSleep(unittest.TestCase):
    """
    Verify that when next_ts is in the future, _collection_cycle() calls
    _sleep_with_check() rather than the raw time.sleep() that was there before.
    """

    def test_future_timestamp_calls_sleep_with_check(self):
        """When next_ts is in the future, _sleep_with_check must be called."""
        from collectors.utils.timezone import now_utc

        collector = make_collector()
        now = now_utc()
        # last_processed = now  =>  next_ts = now + 5min  (in the future)
        collector.last_processed_timestamp = now

        with patch.object(collector, "_sleep_with_check") as mock_sleep, \
             patch("collectors.realtime_collector.now_utc", return_value=now):
            collector._collection_cycle()

        mock_sleep.assert_called_once()

    def test_future_wait_does_not_call_raw_time_sleep(self):
        """raw time.sleep() must NOT be called during a future-wait (was the pre-fix bug)."""
        from collectors.utils.timezone import now_utc

        collector = make_collector()
        now = now_utc()
        collector.last_processed_timestamp = now

        # Patch both: _sleep_with_check (so the cycle returns fast)
        # and time.sleep (so we can assert it was NOT used directly)
        with patch.object(collector, "_sleep_with_check"), \
             patch("time.sleep") as mock_raw_sleep, \
             patch("collectors.realtime_collector.now_utc", return_value=now):
            collector._collection_cycle()

        mock_raw_sleep.assert_not_called()

    def test_wait_seconds_capped_at_60(self):
        """
        Wait duration is capped at 60s regardless of how far in the future next_ts is.
        (next_ts = now + 5min => time_until_available = 300s, capped to 60s)
        """
        from collectors.utils.timezone import now_utc

        collector = make_collector()
        now = now_utc()
        collector.last_processed_timestamp = now

        with patch.object(collector, "_sleep_with_check") as mock_sleep, \
             patch("collectors.realtime_collector.now_utc", return_value=now):
            collector._collection_cycle()

        wait_seconds = mock_sleep.call_args[0][0]
        self.assertEqual(wait_seconds, 60, "Wait duration should be capped at 60s")

    def test_past_timestamp_skips_future_wait(self):
        """When next_ts is in the past (catching up), no sleep should occur before API call."""
        from collectors.utils.timezone import now_utc
        from datetime import timedelta

        collector = make_collector()
        # last_processed = 10 minutes ago => next_ts = 5 minutes ago (past)
        now = now_utc()
        collector.last_processed_timestamp = now - timedelta(minutes=10)

        # Return None from API so the cycle hits the "no data" retry path and returns cleanly
        collector.api.get_5m_data.return_value = None

        with patch.object(collector, "_sleep_with_check") as mock_sleep, \
             patch("collectors.realtime_collector.now_utc", return_value=now):
            collector._collection_cycle()

        # The 60s future-wait sleep must not have been requested
        for call in mock_sleep.call_args_list:
            self.assertNotEqual(call[0][0], 60, "60s future-wait sleep should not occur for past timestamps")


if __name__ == "__main__":
    unittest.main()
