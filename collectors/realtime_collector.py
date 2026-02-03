"""
Real-time data collector for OSRS Grand Exchange prices.

This collector continuously fetches price data from the OSRS Wiki API:
- /5m endpoint: 5-minute interval price/volume data
- /latest endpoint: Most recent price snapshot

Features:
- Sequential timestamp processing (never skips intervals)
- Graceful restart with overlap
- Comprehensive logging
- Robust gap handling
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pytz

from collectors.utils.timezone import ensure_utc, now_utc

from collectors.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class RealtimeCollector(BaseCollector):
    """
    Real-time collector for OSRS price data.

    Fetches /5m and /latest endpoints continuously with sequential timestamp processing.
    Ensures no data gaps by always requesting the next 5-minute interval.
    """

    def __init__(self):
        """Initialize real-time collector."""
        super().__init__("realtime_collector")

        # Configuration - simplified: always 1 second between calls
        self.collection_interval = 1.0  # 1 second between API calls

        # State
        self.last_processed_timestamp: Optional[datetime] = None
        self.lag_minutes = 0.0
        self.current_retry_count = 0  # Track retries for current timestamp
        self.max_retries = 900  # 15 minutes at 1 call per second

        self.logger.info(
            f"[CONFIG] Collection interval: {self.collection_interval} seconds"
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

                    # Sleep with interruption check
                    self._sleep_with_check(sleep_time)

                except Exception as e:
                    if not self.handle_error(e, "Collection cycle"):
                        break

        finally:
            self.shutdown()

    def _collection_cycle(self) -> None:
        """Perform one collection cycle with sequential timestamp tracking.

        CRITICAL: Always requests specific timestamps to ensure no gaps.
        Never skips intervals by requesting "latest".
        """

        cycle_start_time = time.time()

        # Determine which timestamp to request
        if self.last_processed_timestamp:
            # ALWAYS request the NEXT timestamp after last processed
            next_ts = self.last_processed_timestamp + timedelta(minutes=5)

            # Check if we're asking for future data (API won't have it yet)
            time_until_available = (next_ts - now_utc()).total_seconds()

            if time_until_available > 0:
                # Next timestamp is in the future, wait for it
                wait_seconds = min(
                    time_until_available + 5, 60
                )  # Wait + 5s buffer, max 60s
                self.logger.info(
                    f"[WAIT] Next data at {next_ts.strftime('%H:%M:%S')} "
                    f"(in {wait_seconds:.0f}s). Sleeping..."
                )
                time.sleep(wait_seconds)
                return  # Exit cycle, will retry on next iteration

            target_timestamp = int(next_ts.timestamp())

            # Calculate lag from real-time
            lag = (now_utc() - next_ts).total_seconds() / 60

            # Determine mode based on lag
            mode = "CATCH-UP" if lag > 2 else "LIVE"
        else:
            # First run only - get latest available
            target_timestamp = None
            next_ts = None
            lag = 0
            mode = "INIT"
            self.logger.info("[INIT] First run - requesting most recent data")

        # Fetch 5m data for specific timestamp
        api_start = time.time()
        data_5m = self.api.get_5m_data(timestamp=target_timestamp)
        api_duration = time.time() - api_start

        if not data_5m or "data" not in data_5m:
            # API returned no data - use retry logic
            ts_str = next_ts.strftime("%H:%M:%S") if next_ts else "latest"
            self.current_retry_count += 1

            if self.current_retry_count < self.max_retries:
                # Keep trying
                if self.current_retry_count % 10 == 0:
                    self.logger.warning(
                        f"[WAIT] {ts_str} - API returned no data, retrying ({self.current_retry_count}/{self.max_retries})"
                    )
                return  # Will retry same timestamp
            else:
                # Timeout - skip this timestamp
                self.logger.error(
                    f"[TIMEOUT] {ts_str} - API failed after {self.max_retries} attempts, skipping"
                )
                if target_timestamp:
                    self.current_retry_count = 0
                    self.last_processed_timestamp = datetime.fromtimestamp(
                        target_timestamp, tz=pytz.UTC
                    )
                    self.state.update_state_if_needed(self.last_processed_timestamp)
            return

        # Get timestamp from API response
        api_timestamp = data_5m.get("timestamp")
        if api_timestamp:
            received_ts = ensure_utc(api_timestamp, "API_5m_cycle")

            # Check if we got what we requested
            if target_timestamp:
                requested_ts = datetime.fromtimestamp(target_timestamp, tz=pytz.UTC)
                time_diff = abs((received_ts - requested_ts).total_seconds())

                if time_diff > 300:  # More than 5 minutes difference
                    self.logger.warning(
                        f"[{mode}] MISMATCH: Requested {requested_ts.strftime('%H:%M')}, "
                        f"got {received_ts.strftime('%H:%M')} | Using requested time"
                    )
                    current_ts = requested_ts
                else:
                    current_ts = received_ts
            else:
                current_ts = received_ts
        else:
            current_ts = now_utc()

        # Process records
        records = self._process_5m_data(data_5m["data"], current_ts)

        if records:
            # SUCCESS: Data received - reset retry counter and advance
            self.current_retry_count = 0

            # Bulk insert to database
            inserted = self.db.bulk_insert_prices(records)

            # Build status message based on context
            if lag > 10:
                status = f"{lag:.0f}min behind"
            elif lag > 2:
                status = f"{lag:.1f}min behind"
            else:
                status = "real-time"

            # Single concise log line
            self.logger.info(
                f"[{mode}] {status} | {inserted} records | {api_duration:.2f}s API"
            )

            # Update latest prices
            self.db.update_latest_prices(records)

            # Advance to next timestamp
            if target_timestamp:
                self.last_processed_timestamp = datetime.fromtimestamp(
                    target_timestamp, tz=pytz.UTC
                )
            else:
                self.last_processed_timestamp = current_ts

            self.state.update_state_if_needed(self.last_processed_timestamp)

        else:
            # NO DATA: Increment retry counter
            self.current_retry_count += 1

            if self.current_retry_count < self.max_retries:
                # Keep trying - log every 10th attempt to avoid spam
                if self.current_retry_count % 10 == 0:
                    self.logger.info(
                        f"[WAIT] {current_ts.strftime('%H:%M:%S')} - "
                        f"No data yet, retrying ({self.current_retry_count}/{self.max_retries})"
                    )
                # Don't advance - will retry same timestamp on next cycle
                return
            else:
                # TIMEOUT: 15 minutes elapsed with no data
                self.logger.warning(
                    f"[TIMEOUT] {current_ts.strftime('%H:%M:%S')} - "
                    f"No data after {self.max_retries} attempts (15min), skipping"
                )

                # Build status for timeout message
                if lag > 10:
                    status = f"{lag:.0f}min behind"
                elif lag > 2:
                    status = f"{lag:.1f}min behind"
                else:
                    status = "real-time"

                self.logger.info(
                    f"[{mode}] {status} | 0 records (timeout) | {api_duration:.2f}s API"
                )

                # Reset counter and advance
                self.current_retry_count = 0
                if target_timestamp:
                    self.last_processed_timestamp = datetime.fromtimestamp(
                        target_timestamp, tz=pytz.UTC
                    )
                else:
                    self.last_processed_timestamp = current_ts
                self.state.update_state_if_needed(self.last_processed_timestamp)

        # Calculate lag for next cycle
        self._calculate_lag(current_ts)

        # Fetch latest data
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

        # Log lag for monitoring
        if lag > 30:
            self.logger.warning(
                f"[LAG] Significant lag: {lag:.1f} minutes behind real-time"
            )
        elif lag > 10:
            self.logger.info(f"[LAG] Catching up: {lag:.1f} minutes behind")

    def _calculate_sleep_time(self) -> float:
        """
        Calculate sleep time until next collection.

        Simplified: always use fixed 1-second interval for consistent processing.

        Returns:
            Sleep time in seconds
        """
        return self.collection_interval

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
