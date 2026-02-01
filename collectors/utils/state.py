"""
State management for OSRS Market Data Collectors.

This module provides:
- Persistent state storage in database
- Safe restart logic with overlap handling
- Progress tracking for backfill operations
- State consistency verification
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

from .db import DatabaseManager, db_manager
from .timezone import ensure_utc, now_utc, validate_utc

logger = logging.getLogger(__name__)


class StateManager:
    """
    Manages collector state persistence and restart safety.

    Provides:
    - State persistence to database
    - Safe restart with overlap handling
    - Progress tracking
    - State verification against actual data
    """

    def __init__(self, collector_name: str, db: Optional[DatabaseManager] = None):
        """
        Initialize state manager.

        Args:
            collector_name: Unique name for this collector instance
            db: Database manager instance (uses global if not provided)
        """
        self.collector_name = collector_name
        self.db = db or db_manager

        # Configuration
        self.restart_overlap_minutes = int(os.getenv("RESTART_OVERLAP_MINUTES", "5"))
        self.state_sync_frequency = int(os.getenv("STATE_SYNC_FREQUENCY", "100"))

        # In-memory state
        self._last_timestamp: Optional[datetime] = None
        self._records_since_sync = 0

        logger.info(f"[STATE] StateManager initialized for {collector_name}")
        logger.info(f"[STATE] Restart overlap: {self.restart_overlap_minutes} minutes")

    def load_state(self) -> Optional[datetime]:
        """
        Load last processed timestamp from database.

        Returns:
            Last processed timestamp or None if no state exists
        """
        query = """
            SELECT last_processed_timestamp, status, metadata
            FROM collector_state
            WHERE collector_name = %s
        """
        results = self.db.execute_dict(query, (self.collector_name,))

        if results:
            state = results[0]
            ts = state.get("last_processed_timestamp")
            if ts:
                # Ensure it's a datetime object
                if isinstance(ts, str):
                    ts = datetime.fromisoformat(ts)
                ts = ensure_utc(ts, "load_state")
                logger.info(
                    f"[STATE] Loaded state for {self.collector_name}: {ts.isoformat()}"
                )
                return ts

        logger.info(f"[STATE] No previous state found for {self.collector_name}")
        return None

    def save_state(
        self,
        timestamp: datetime,
        status: str = "running",
        metadata: Optional[Dict] = None,
    ) -> None:
        """
        Save current state to database.

        Args:
            timestamp: Last successfully processed timestamp
            status: Collector status
            metadata: Additional metadata dict
        """
        validate_utc(timestamp, "save_state timestamp")

        query = """
            INSERT INTO collector_state (
                collector_name, last_processed_timestamp, status, 
                metadata, updated_at
            ) VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (collector_name) DO UPDATE SET
                last_processed_timestamp = EXCLUDED.last_processed_timestamp,
                status = EXCLUDED.status,
                metadata = EXCLUDED.metadata,
                updated_at = EXCLUDED.updated_at
        """

        # Convert metadata dict to JSON string
        metadata_json = json.dumps(metadata) if metadata else "{}"

        self.db.execute(
            query,
            (self.collector_name, timestamp.isoformat(), status, metadata_json),
        )

        self._last_timestamp = timestamp
        logger.debug(
            f"[STATE] Saved state for {self.collector_name}: {timestamp.isoformat()}"
        )

    def update_state_if_needed(self, timestamp: datetime) -> None:
        """
        Update state every cycle for reliable restart recovery.

        Args:
            timestamp: Current timestamp to save
        """
        # Save state every cycle to prevent data loss on crashes
        self.save_state(timestamp)
        self._records_since_sync += 1

    def calculate_restart_timestamp(self) -> Optional[datetime]:
        """
        Calculate safe restart timestamp with overlap.

        For real-time collector:
        1. Load state from state table
        2. Query actual max timestamp from database
        3. Use earlier of the two
        4. Subtract overlap period

        Returns:
            Safe restart timestamp or None for fresh start
        """
        logger.info(
            f"[RESTART] Calculating restart timestamp for {self.collector_name}"
        )

        # 1. Get state from table
        state_timestamp = self.load_state()

        # 2. Get actual max from database
        db_max = self.db.get_last_timestamp("prices")
        if db_max:
            db_max = ensure_utc(db_max, "db_max_timestamp")

        # 3. Compare and choose earlier
        if state_timestamp and db_max:
            discrepancy = abs((state_timestamp - db_max).total_seconds() / 60)

            if state_timestamp > db_max:
                # State is ahead of data (shouldn't happen often)
                logger.warning(
                    f"[RESTART] State table ahead of database by {discrepancy:.1f} minutes"
                )
                chosen = db_max
            elif db_max > state_timestamp:
                # Data has progressed past state
                logger.info(
                    f"[RESTART] Database ahead of state table by {discrepancy:.1f} minutes"
                )
                chosen = state_timestamp
            else:
                # They match
                chosen = state_timestamp

            logger.info(f"[RESTART] State table: {state_timestamp.isoformat()}")
            logger.info(f"[RESTART] Database max: {db_max.isoformat()}")
            logger.info(f"[RESTART] Chosen: {chosen.isoformat()}")

        elif state_timestamp:
            # Only state available
            chosen = state_timestamp
            logger.info(f"[RESTART] Using state table: {chosen.isoformat()}")

        elif db_max:
            # Only database available
            chosen = db_max
            logger.info(f"[RESTART] Using database max: {chosen.isoformat()}")

        else:
            # No state at all
            logger.info("[RESTART] No previous state found, starting fresh")
            return None

        # 4. Subtract overlap for safety
        overlap = timedelta(minutes=self.restart_overlap_minutes)
        restart_ts = chosen - overlap

        logger.info(f"[RESTART] Applied {self.restart_overlap_minutes} min overlap")
        logger.info(f"[RESTART] Resuming from: {restart_ts.isoformat()}")

        return restart_ts

    def calculate_backfill_restart(self, months: int = 6) -> Tuple[datetime, datetime]:
        """
        Calculate backfill restart parameters.

        For backfill collector:
        1. Check database for oldest existing timestamp
        2. Load backfill progress from state table
        3. Use database as source of truth
        4. Resume with safety overlap

        Args:
            months: Number of months to backfill

        Returns:
            Tuple of (current_position, target_timestamp)
        """
        logger.info(
            f"[BACKFILL-RESTART] Calculating restart for {months} month backfill"
        )

        # Calculate target timestamp
        target_ts = now_utc() - timedelta(days=30 * months)
        target_ts = target_ts.replace(hour=0, minute=0, second=0, microsecond=0)

        # Get oldest data in database
        db_oldest = self.db.get_oldest_timestamp("prices")
        if db_oldest:
            db_oldest = ensure_utc(db_oldest, "db_oldest_timestamp")
            logger.info(f"[BACKFILL-RESTART] Database oldest: {db_oldest.isoformat()}")

        # Load backfill progress
        query = """
            SELECT last_processed_timestamp, metadata
            FROM collector_state
            WHERE collector_name = %s
        """
        results = self.db.execute_dict(query, (self.collector_name,))

        state_ts = None
        if results and results[0].get("last_processed_timestamp"):
            state_ts = ensure_utc(
                results[0]["last_processed_timestamp"], "backfill_state"
            )
            logger.info(f"[BACKFILL-RESTART] State table: {state_ts.isoformat()}")

        # Use database as source of truth
        if db_oldest:
            # Add safety overlap (go back a bit further)
            overlap = timedelta(minutes=self.restart_overlap_minutes)
            current_ts = db_oldest - overlap

            if state_ts and state_ts != db_oldest:
                discrepancy = abs((state_ts - db_oldest).total_seconds() / 60)
                logger.warning(
                    f"[BACKFILL-RESTART] State/db mismatch: {discrepancy:.1f} minutes"
                )

            logger.info(f"[BACKFILL-RESTART] Resuming from: {current_ts.isoformat()}")
            logger.info(f"[BACKFILL-RESTART] Target: {target_ts.isoformat()}")

            return current_ts, target_ts
        else:
            # No data yet, start from now
            current_ts = now_utc()
            logger.info(
                f"[BACKFILL-RESTART] No data yet, starting from: {current_ts.isoformat()}"
            )
            return current_ts, target_ts

    def mark_backfill_complete(self) -> None:
        """Mark backfill collector as completed."""
        self.save_state(
            now_utc(),
            status="completed",
            metadata={
                "backfill_status": "completed",
                "completed_at": now_utc().isoformat(),
            },
        )
        logger.info(f"[STATE] Backfill marked as complete for {self.collector_name}")

    def record_error(self, error_message: str) -> None:
        """
        Record an error in the collector state.

        Args:
            error_message: Error message to record
        """
        query = """
            UPDATE collector_state
            SET last_error_message = %s,
                last_error_at = NOW(),
                consecutive_errors = COALESCE(consecutive_errors, 0) + 1,
                status = CASE 
                    WHEN COALESCE(consecutive_errors, 0) >= 5 THEN 'error'
                    ELSE status 
                END
            WHERE collector_name = %s
        """
        self.db.execute(query, (error_message, self.collector_name))
        logger.error(
            f"[STATE] Error recorded for {self.collector_name}: {error_message}"
        )

    def clear_error(self) -> None:
        """Clear error state and reset consecutive error counter."""
        query = """
            UPDATE collector_state
            SET consecutive_errors = 0,
                last_error_message = NULL,
                last_error_at = NULL
            WHERE collector_name = %s
        """
        self.db.execute(query, (self.collector_name,))
        logger.info(f"[STATE] Errors cleared for {self.collector_name}")

    def get_status(self) -> Optional[Dict]:
        """
        Get current collector status from database.

        Returns:
            Status dictionary or None
        """
        query = """
            SELECT collector_name, last_processed_timestamp, status,
                   metadata, started_at, updated_at, consecutive_errors
            FROM collector_state
            WHERE collector_name = %s
        """
        results = self.db.execute_dict(query, (self.collector_name,))
        return results[0] if results else None


class BackfillProgressTracker:
    """
    Tracks detailed backfill progress in separate table.

    Provides more granular progress information than collector_state.
    """

    def __init__(self, db: Optional[DatabaseManager] = None):
        """Initialize progress tracker."""
        self.db = db or db_manager

    def start_backfill(self, collector_name: str, target_ts: datetime) -> Optional[int]:
        """
        Record start of backfill operation.

        Args:
            collector_name: Name of backfill collector
            target_ts: Target timestamp (oldest point to reach)

        Returns:
            Progress record ID
        """
        query = """
            INSERT INTO backfill_progress (
                collector_name, target_timestamp, "current_timestamp",
                status, started_at, updated_at
            ) VALUES (%s, %s, NOW(), 'running', NOW(), NOW())
            RETURNING id
        """
        results = self.db.execute(query, (collector_name, target_ts.isoformat()))
        return results[0][0] if results else None

    def update_progress(
        self,
        progress_id: int,
        current_ts: datetime,
        intervals_processed: int,
        items_processed: int,
        api_calls: int,
    ) -> None:
        """
        Update backfill progress.

        Args:
            progress_id: Progress record ID
            current_ts: Current position in backfill
            intervals_processed: Number of 5-min intervals processed
            items_processed: Number of items processed
            api_calls: Number of API calls made
        """
        query = """
            UPDATE backfill_progress
            SET "current_timestamp" = %s,
                intervals_processed = %s,
                items_processed = items_processed + %s,
                api_calls_made = api_calls_made + %s,
                updated_at = NOW()
            WHERE id = %s
        """
        self.db.execute(
            query,
            (
                current_ts.isoformat(),
                intervals_processed,
                items_processed,
                api_calls,
                progress_id,
            ),
        )

    def complete_backfill(self, progress_id: int) -> None:
        """Mark backfill as completed."""
        query = """
            UPDATE backfill_progress
            SET status = 'completed',
                completed_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
        """
        self.db.execute(query, (progress_id,))

    def get_active_backfill(self, collector_name: str) -> Optional[Dict]:
        """Get currently active backfill record."""
        query = """
            SELECT *
            FROM backfill_progress
            WHERE collector_name = %s
            AND status IN ('running', 'paused')
            ORDER BY started_at DESC
            LIMIT 1
        """
        results = self.db.execute_dict(query, (collector_name,))
        return results[0] if results else None
