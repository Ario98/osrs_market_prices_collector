"""
Real-time data collector for OSRS Grand Exchange prices.

This collector continuously fetches price data from the OSRS Wiki API:
- /5m endpoint: 5-minute interval price/volume data
- /latest endpoint: Most recent price snapshot

Features:
- Automatic mode switching (normal vs fast-catchup)
- Lag detection and recovery
- Graceful restart with overlap
- Comprehensive logging
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from collectors.utils.timezone import ensure_utc, now_utc

from collectors.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class RealtimeCollector(BaseCollector):
    """
    Real-time collector for OSRS price data.

    Fetches /5m and /latest endpoints continuously.
    Automatically switches to fast-catchup mode when lag exceeds threshold.
    """

    def __init__(self):
        """Initialize real-time collector."""
        super().__init__("realtime_collector")

        # Configuration
        self.fast_catchup_threshold = int(
            os.getenv("FAST_CATCHUP_THRESHOLD_MINUTES", "15")
        )
        self.fast_catchup_interval = float(
            os.getenv("FAST_CATCHUP_INTERVAL_MINUTES", "2.5")
        )
        self.normal_interval = 1.0  # 1 minute in normal mode for more frequent updates

        # State
        self.last_processed_timestamp: Optional[datetime] = None
        self.mode = "normal"  # 'normal' or 'fast-catchup'
        self.lag_minutes = 0.0

        self.logger.info(
            f"[CONFIG] Fast catchup threshold: {self.fast_catchup_threshold} minutes"
        )
        self.logger.info(
            f"[CONFIG] Fast catchup interval: {self.fast_catchup_interval} minutes"
        )

    def run(self) -> None:
        """Main collection loop."""
        self.log_startup_info()

        # Connect to database
        if not self.connect():
            self.logger.error("Failed to connect to database, exiting")
            sys.exit(1)

        # Calculate restart timestamp with overlap
        restart_ts = self.state.calculate_restart_timestamp()
        if restart_ts:
            self.last_processed_timestamp = restart_ts
            self.logger.info(f"[RESTART] Resuming from: {restart_ts.isoformat()}")
        else:
            self.logger.info("[RESTART] Starting fresh collection")

        # Mark as running (use the calculated restart timestamp, not current time!)
        self.state.save_state(
            self.last_processed_timestamp or now_utc(), status="running"
        )
        self.running = True

        try:
            while not self.should_stop():
                try:
                    cycle_start = time.time()

                    # Fetch and process data
                    self._collection_cycle()

                    # Reset error count on success
                    self.reset_error_count()

                    # Calculate sleep time based on mode
                    sleep_time = self._calculate_sleep_time()

                    cycle_duration = time.time() - cycle_start
                    self.logger.info(
                        f"[CYCLE] Duration: {cycle_duration:.2f}s, Sleep: {sleep_time:.2f}s, Mode: {self.mode}"
                    )

                    # Sleep with interruption check
                    self._sleep_with_check(sleep_time)

                except Exception as e:
                    if not self.handle_error(e, "Collection cycle"):
                        break

        finally:
            self.shutdown()

    def _collection_cycle(self) -> None:
        """Perform one collection cycle with specific timestamp tracking."""

        # Determine which timestamp to request
        if self.mode == "fast-catchup" and self.last_processed_timestamp:
            # In catch-up mode, request the NEXT timestamp after last processed
            next_ts = self.last_processed_timestamp + timedelta(minutes=5)
            target_timestamp = int(next_ts.timestamp())
            self.logger.debug(
                f"[FETCH] Catch-up mode: requesting timestamp {target_timestamp}"
            )
        else:
            # Normal mode: get most recent
            target_timestamp = None
            self.logger.debug("[FETCH] Normal mode: requesting most recent data")

        # Fetch 5m data (with specific timestamp if in catch-up)
        data_5m = self.api.get_5m_data(timestamp=target_timestamp)

        if not data_5m or "data" not in data_5m:
            self.logger.warning("[FETCH] No data returned from /5m endpoint")
            return

        # Get timestamp from API response
        api_timestamp = data_5m.get("timestamp")
        if api_timestamp:
            current_ts = ensure_utc(api_timestamp, "API_5m_cycle")
        else:
            current_ts = now_utc()

        # Process records
        records = self._process_5m_data(data_5m["data"], current_ts)

        if records:
            # Bulk insert to database
            inserted = self.db.bulk_insert_prices(records)
            self.logger.info(
                f"[INSERT] Inserted {inserted} price records for {current_ts.isoformat()}"
            )

            # Update latest prices
            self.db.update_latest_prices(records)

        # Always update state with the timestamp we processed (even if 0 records inserted)
        # This ensures we advance to the next timestamp on the next cycle
        self.last_processed_timestamp = current_ts
        self.state.update_state_if_needed(current_ts)

        # Calculate lag
        self._calculate_lag(current_ts)

        # Also fetch latest data (optional, for real-time monitoring)
        self._fetch_latest_data()

    def _process_5m_data(self, data: Dict, timestamp: datetime) -> List[Dict]:
        """
        Process 5m API response data into standardized records.

        Args:
            data: API response data dict (item_id -> price data)
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

    def _fetch_latest_data(self) -> None:
        """Fetch and store latest price snapshot."""
        try:
            self.logger.debug("[FETCH] Requesting /latest data...")
            data_latest = self.api.get_latest_data()

            if data_latest and "data" in data_latest:
                records = []
                for item_id_str, item_data in data_latest["data"].items():
                    item_id = int(item_id_str)

                    if self.test_mode and self.test_item_ids:
                        if item_id not in self.test_item_ids:
                            continue

                    record = self.api.parse_latest_record(item_id, item_data)
                    if record:
                        records.append(record)

                if records:
                    self.db.update_latest_prices(records)
                    self.logger.debug(
                        f"[LATEST] Updated {len(records)} latest price records"
                    )

        except Exception as e:
            self.logger.warning(f"[LATEST] Failed to fetch latest data: {e}")

    def _calculate_lag(self, current_ts: datetime) -> None:
        """
        Calculate how far behind real-time we are.

        Args:
            current_ts: Timestamp of current data
        """
        now = now_utc()
        lag = (now - current_ts).total_seconds() / 60  # minutes
        self.lag_minutes = lag

        # Determine mode
        if lag > self.fast_catchup_threshold:
            if self.mode != "fast-catchup":
                self.logger.warning(
                    f"[LAG] Lag detected: {lag:.1f} minutes. Switching to fast-catchup mode!"
                )
                self.mode = "fast-catchup"
        else:
            if self.mode == "fast-catchup" and lag < self.fast_catchup_threshold / 2:
                self.logger.info(
                    f"[LAG] Lag recovered: {lag:.1f} minutes. Returning to normal mode."
                )
                self.mode = "normal"

        if lag > 30:
            self.logger.warning(
                f"[LAG] Significant lag: {lag:.1f} minutes behind real-time"
            )

    def _calculate_sleep_time(self) -> float:
        """
        Calculate sleep time until next collection.

        Returns:
            Sleep time in seconds
        """
        if self.mode == "fast-catchup":
            return self.fast_catchup_interval * 60  # Convert minutes to seconds
        else:
            return self.normal_interval * 60

    def _sleep_with_check(self, seconds: float) -> None:
        """
        Sleep with periodic checks for shutdown signal.

        Args:
            seconds: Total seconds to sleep
        """
        check_interval = 1.0  # Check every second
        elapsed = 0.0

        while elapsed < seconds and not self.should_stop():
            time.sleep(min(check_interval, seconds - elapsed))
            elapsed += check_interval


if __name__ == "__main__":
    collector = RealtimeCollector()
    collector.run()
