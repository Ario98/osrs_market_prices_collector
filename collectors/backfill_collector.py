"""
Historical backfill collector for OSRS Grand Exchange prices.

This collector fetches historical 5-minute price data to populate
the database with past data. It works backwards from the current time
to the target timestamp (default: 6 months ago).

Features:
- Smart restart logic with database verification
- Progress tracking with ETA calculation
- Safe overlap on restart to prevent gaps
- Rate limiting compliance
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from collectors.utils.state import BackfillProgressTracker
from collectors.utils.timezone import ensure_utc

from collectors.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class BackfillCollector(BaseCollector):
    """
    Historical backfill collector for OSRS price data.

    Fetches historical 5-minute data working backwards from current time.
    Resumes safely from previous position after restarts.
    """

    def __init__(self):
        """Initialize backfill collector."""
        super().__init__("backfill_collector")

        # Configuration
        self.backfill_months = int(os.getenv("HISTORICAL_BACKFILL_MONTHS", "6"))
        # Allow separate overlap config for backfill (default 10 minutes)
        self.backfill_overlap_minutes = int(os.getenv("BACKFILL_OVERLAP_MINUTES", "10"))

        # State
        self.current_timestamp: Optional[datetime] = None
        self.target_timestamp: Optional[datetime] = None
        self.progress_id: Optional[int] = None
        self.intervals_processed = 0
        self.progress_tracker = BackfillProgressTracker(self.db)

        self.logger.info(f"[CONFIG] Backfill period: {self.backfill_months} months")
        self.logger.info(
            f"[CONFIG] Backfill overlap: {self.backfill_overlap_minutes} minutes"
        )

    def _check_retention_policy(self) -> None:
        """Check if retention policy matches backfill period and warn if not."""
        try:
            # Query TimescaleDB for retention policy on prices table
            query = """
                SELECT config->>'drop_after' as retention_interval
                FROM timescaledb_information.jobs
                WHERE hypertable_name = 'prices'
                AND proc_name = 'policy_retention'
                LIMIT 1
            """
            result = self.db.execute(query)

            if result:
                retention_str = result[0][0]  # e.g., "6 months"
                # Parse to extract months
                import re

                match = re.search(r"(\d+)\s*month", retention_str.lower())
                if match:
                    retention_months = int(match.group(1))

                    if self.backfill_months > retention_months:
                        self.logger.warning(
                            f"[RETENTION MISMATCH] Backfill: {self.backfill_months} months, "
                            f"Retention: {retention_months} months. "
                            f"Oldest {(self.backfill_months - retention_months)} months will be PURGED!"
                        )
                    else:
                        self.logger.info(
                            f"[RETENTION OK] Retention: {retention_months} months >= Backfill: {self.backfill_months} months"
                        )
        except Exception as e:
            # Don't fail if we can't check retention
            self.logger.debug(f"[RETENTION] Could not check retention policy: {e}")

    def run(self) -> None:
        """Main backfill loop."""
        self.log_startup_info()

        # Connect to database
        if not self.connect():
            self.logger.error("Failed to connect to database, exiting")
            sys.exit(1)

        # Check retention policy after DB connection
        self._check_retention_policy()

        # Wait for real-time collector if database is empty
        # This ensures we have some data before starting backfill
        if not self._wait_for_initial_data():
            self.logger.error(
                "Timeout waiting for initial data from real-time collector"
            )
            sys.exit(1)

        # Calculate restart parameters with backfill-specific overlap
        self.current_timestamp, self.target_timestamp = (
            self.state.calculate_backfill_restart(
                self.backfill_months, overlap_minutes=self.backfill_overlap_minutes
            )
        )

        # Check for completion with proper overlap logic
        db_oldest = self.db.get_oldest_timestamp("prices")
        if db_oldest:
            db_oldest = ensure_utc(db_oldest, "db_oldest")
            effective_target = self.target_timestamp + timedelta(
                minutes=self.backfill_overlap_minutes
            )

            if db_oldest <= effective_target:
                self.logger.info(
                    f"[BACKFILL] Already complete! Oldest data ({db_oldest.isoformat()}) at or before target ({effective_target.isoformat()})"
                )
                self.state.mark_backfill_complete()
                self._sleep_until_restart()
                return
            else:
                self.logger.info(
                    f"[BACKFILL] Need to fill gap: target {self.target_timestamp.isoformat()} to oldest {db_oldest.isoformat()}"
                )
                # Start from oldest data, not from before it
                self.current_timestamp = db_oldest
        else:
            self.logger.info("[BACKFILL] No existing data, starting fresh from now")

        # Validate timestamps are set
        if self.current_timestamp is None or self.target_timestamp is None:
            self.logger.error(
                "[BACKFILL] Failed to determine timestamps, cannot proceed"
            )
            sys.exit(1)

        self.logger.info(
            f"[BACKFILL] Starting from: {self.current_timestamp.isoformat()}"
        )
        self.logger.info(f"[BACKFILL] Target: {self.target_timestamp.isoformat()}")

        # Calculate total intervals for progress tracking
        total_minutes = (
            self.current_timestamp - self.target_timestamp
        ).total_seconds() / 60
        total_intervals = int(total_minutes / 5)
        self.logger.info(f"[BACKFILL] Total intervals to process: {total_intervals}")

        # Start progress tracking
        self.progress_id = self.progress_tracker.start_backfill(
            self.collector_name, self.target_timestamp
        )

        # Mark as running
        self.state.save_state(
            self.current_timestamp,
            status="running",
            metadata={
                "target_timestamp": self.target_timestamp.isoformat(),
                "total_intervals": total_intervals,
                "progress_id": self.progress_id,
            },
        )
        self.running = True

        try:
            while (
                not self.should_stop()
                and self.current_timestamp > self.target_timestamp
            ):
                try:
                    cycle_start = time.time()

                    # Process one 5-minute interval
                    success = self._process_interval()

                    if success:
                        # Move backwards 5 minutes
                        self.current_timestamp -= timedelta(minutes=5)
                        self.intervals_processed += 1

                        # Reset error count
                        self.reset_error_count()

                        # Update state periodically
                        if self.intervals_processed % 10 == 0:
                            self._update_progress(total_intervals)

                    # Calculate ETA
                    self._log_progress(total_intervals)

                    # Small delay to be nice to the API
                    cycle_duration = time.time() - cycle_start
                    sleep_time = max(
                        0, 1.0 - cycle_duration
                    )  # Aim for 1 second per interval
                    self._sleep_with_check(sleep_time)

                except Exception as e:
                    if not self.handle_error(e, "Backfill cycle"):
                        break

            # Backfill complete or stopped
            if self.current_timestamp <= self.target_timestamp:
                self.logger.info("[BACKFILL] Target reached! Backfill complete.")
                self.state.mark_backfill_complete()
                if self.progress_id:
                    self.progress_tracker.complete_backfill(self.progress_id)
            else:
                self.logger.info("[BACKFILL] Stopped before reaching target")

        finally:
            self.shutdown()

    def _wait_for_initial_data(self, timeout_seconds: int = 300) -> bool:
        """
        Wait for real-time collector to insert first record.

        Args:
            timeout_seconds: Maximum time to wait

        Returns:
            True if data found, False if timeout
        """
        self.logger.info(
            "[BACKFILL] Checking for initial data from real-time collector..."
        )

        start_time = time.time()
        check_interval = 10  # seconds

        while time.time() - start_time < timeout_seconds:
            # Check if real-time collector has inserted data
            last_ts = self.db.get_last_timestamp("prices")
            if last_ts:
                self.logger.info(f"[BACKFILL] Found initial data: {last_ts}")
                return True

            # Check real-time collector state
            rt_state = self.state.db.execute_dict(
                "SELECT status FROM collector_state WHERE collector_name = 'realtime_collector'"
            )
            if rt_state and rt_state[0].get("status") == "error":
                self.logger.error("[BACKFILL] Real-time collector in error state")
                return False

            self.logger.info(
                f"[BACKFILL] Waiting for real-time collector... ({int(time.time() - start_time)}s elapsed)"
            )
            time.sleep(check_interval)

        return False

    def _process_interval(self) -> bool:
        """
        Fetch and insert data for one 5-minute interval.

        Returns:
            True if successful, False otherwise
        """
        if self.current_timestamp is None:
            self.logger.error(
                "[ERROR] current_timestamp is None, cannot process interval"
            )
            return False

        timestamp_unix = int(self.current_timestamp.timestamp())

        try:
            # Fetch data for this timestamp
            self.logger.debug(
                f"[FETCH] Requesting /5m for timestamp {timestamp_unix}..."
            )
            data = self.api.get_5m_data(timestamp=timestamp_unix)

            if not data or "data" not in data:
                self.logger.warning(f"[FETCH] No data for timestamp {timestamp_unix}")
                return True  # Continue to next interval

            # Process records
            records = self._process_5m_data(data["data"], self.current_timestamp)

            if records:
                # Insert to database
                inserted = self.db.bulk_insert_prices(records)
                self.logger.info(
                    f"[INSERT] Timestamp {self.current_timestamp.isoformat()}: "
                    f"{inserted} records"
                )
            else:
                self.logger.debug(
                    f"[INSERT] No records for {self.current_timestamp.isoformat()}"
                )

            return True

        except Exception as e:
            self.logger.error(
                f"[ERROR] Failed to process interval {timestamp_unix}: {e}"
            )
            return False

    def _process_5m_data(self, data: Dict, timestamp: datetime) -> List[Dict]:
        """
        Process 5m API response data into standardized records.

        Args:
            data: API response data dict
            timestamp: Timestamp for these records

        Returns:
            List of standardized record dicts
        """
        records = []

        for item_id_str, item_data in data.items():
            item_id = int(item_id_str)

            # Skip if not in test whitelist (when in test mode)
            if self.test_mode and self.test_item_ids:
                if item_id not in self.test_item_ids:
                    continue

            # Parse record
            record = self.api.parse_5m_record(
                item_id, item_data, int(timestamp.timestamp())
            )
            if record:
                records.append(record)

        return records

    def _update_progress(self, total_intervals: int) -> None:
        """Update progress in database."""
        if self.current_timestamp is None or self.target_timestamp is None:
            return

        # Update collector state
        self.state.save_state(
            self.current_timestamp,
            status="running",
            metadata={
                "target_timestamp": self.target_timestamp.isoformat(),
                "intervals_processed": self.intervals_processed,
                "progress_percentage": (
                    self.intervals_processed / total_intervals * 100
                )
                if total_intervals > 0
                else 0,
            },
        )

        # Update progress tracker
        if self.progress_id:
            self.progress_tracker.update_progress(
                self.progress_id,
                self.current_timestamp,
                10,  # intervals since last update
                0,  # items processed (tracked separately)
                1,  # API calls
            )

    def _log_progress(self, total_intervals: int) -> None:
        """Log current progress and ETA."""
        if self.intervals_processed == 0 or self.current_timestamp is None:
            return

        percentage = (
            (self.intervals_processed / total_intervals * 100)
            if total_intervals > 0
            else 0
        )

        # Calculate ETA based on average time per interval
        # This is a rough estimate - actual time depends on API rate limiting
        remaining_intervals = total_intervals - self.intervals_processed
        eta_seconds = (
            remaining_intervals * 1.2
        )  # Assume 1.2s per interval (including overhead)
        eta_hours = eta_seconds / 3600

        # Current timestamp is guaranteed non-None by the check at function start
        current_ts_str = (
            self.current_timestamp.isoformat() if self.current_timestamp else "N/A"
        )

        self.logger.info(
            f"[PROGRESS] {percentage:.1f}% complete | "
            f"Processed: {self.intervals_processed}/{total_intervals} intervals | "
            f"Current: {current_ts_str} | "
            f"ETA: {eta_hours:.1f} hours"
        )

    def _sleep_with_check(self, seconds: float) -> None:
        """Sleep with periodic checks for shutdown signal."""
        check_interval = 0.5
        elapsed = 0.0

        while elapsed < seconds and not self.should_stop():
            time.sleep(min(check_interval, seconds - elapsed))
            elapsed += check_interval

    def _sleep_until_restart(self, check_interval_seconds: int = 60) -> None:
        """
        Sleep indefinitely until container is restarted.
        Used when backfill is complete to keep container alive but idle.

        Args:
            check_interval_seconds: How often to log a heartbeat
        """
        self.logger.info(
            "[BACKFILL] Entering sleep mode - backfill complete. Restart container to check for extension."
        )

        while not self.should_stop():
            time.sleep(check_interval_seconds)
            self.logger.debug("[BACKFILL] Sleeping... (complete)")


if __name__ == "__main__":
    collector = BackfillCollector()
    collector.run()
