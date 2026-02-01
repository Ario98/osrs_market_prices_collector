"""
Base collector class for OSRS Market Data Collectors.

Provides shared functionality for all collector types:
- Signal handling for graceful shutdown
- Logging configuration
- Database and API client management
- State management
- Error handling and recovery
"""

import logging
import logging.handlers
import os
import signal
import sys
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv  # type: ignore

from .utils.api import api_client
from .utils.db import db_manager
from .utils.state import StateManager
from .utils.timezone import tz_manager

# Load environment variables
load_dotenv()


class BaseCollector(ABC):
    """
    Abstract base class for all OSRS data collectors.

    Provides:
    - Graceful shutdown handling (SIGTERM/SIGINT)
    - Structured logging
    - Database connection management
    - API client access
    - State persistence
    - Error recovery with exponential backoff
    """

    def __init__(self, collector_name: str):
        """
        Initialize base collector.

        Args:
            collector_name: Unique name for this collector instance
        """
        self.collector_name = collector_name
        self.running = False
        self.shutdown_requested = False
        self.last_processed_timestamp: Optional[datetime] = None

        # Configuration
        self.test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
        self.test_item_ids = self._parse_test_item_ids()
        self.log_level = os.getenv("LOG_LEVEL", "INFO")

        # Initialize components
        self._setup_logging()
        self.db = db_manager
        self.api = api_client
        self.tz = tz_manager
        self.state = StateManager(collector_name, self.db)

        # Error tracking
        self.consecutive_errors = 0
        self.max_consecutive_errors = 5

        # Setup signal handlers
        self._setup_signal_handlers()

        self.logger.info(f"[{self.collector_name}] Collector initialized")
        self.logger.info(f"[{self.collector_name}] Test mode: {self.test_mode}")

        if self.test_mode and self.test_item_ids:
            self.logger.info(
                f"[{self.collector_name}] Test items: {len(self.test_item_ids)} items"
            )

    def _parse_test_item_ids(self) -> Optional[list]:
        """Parse TEST_ITEM_IDS environment variable."""
        test_ids_str = os.getenv("TEST_ITEM_IDS", "")
        if not test_ids_str:
            return None
        try:
            return [int(x.strip()) for x in test_ids_str.split(",") if x.strip()]
        except ValueError:
            self.logger.warning("Invalid TEST_ITEM_IDS format, ignoring")
            return None

    def _setup_logging(self) -> None:
        """Configure structured logging."""
        # Create logs directory if needed
        log_dir = Path("/app/logs")
        log_dir.mkdir(exist_ok=True)

        # Setup logger
        self.logger = logging.getLogger(self.collector_name)
        self.logger.setLevel(getattr(logging, self.log_level.upper()))

        # Clear existing handlers
        self.logger.handlers.clear()

        # Console handler with UTC timestamps
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, self.log_level.upper()))
        console_format = logging.Formatter(
            "%(asctime)s UTC [%(name)s] %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        # Ensure UTC in formatter
        console_format.converter = time.gmtime
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)

        # File handler with rotation
        file_handler = logging.handlers.RotatingFileHandler(
            log_dir / f"{self.collector_name}.log",
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
        )
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            "%(asctime)s UTC [%(name)s] %(levelname)s [%(filename)s:%(lineno)d]: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_format.converter = time.gmtime
        file_handler.setFormatter(file_format)
        self.logger.addHandler(file_handler)

    def _setup_signal_handlers(self) -> None:
        """Setup handlers for graceful shutdown signals."""
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)

    def _handle_shutdown_signal(self, signum: int, frame: Any) -> None:
        """Handle shutdown signals gracefully."""
        self.logger.info(
            f"[{self.collector_name}] Received signal {signum}, initiating graceful shutdown..."
        )
        self.shutdown_requested = True
        self.running = False

    def connect(self) -> bool:
        """
        Connect to database and verify API access.

        Returns:
            True if successful, False otherwise
        """
        try:
            # Connect to database
            self.logger.info(f"[{self.collector_name}] Connecting to database...")
            self.db.connect()

            # Verify timezone
            tz_info = self.tz.get_timezone_info()
            self.logger.info(
                f"[{self.collector_name}] Python process timezone: {tz_info['python_process_tz']}"
            )
            self.logger.info(
                f"[{self.collector_name}] Strict mode: {tz_info['strict_mode']}"
            )

            return True

        except Exception as e:
            self.logger.error(f"[{self.collector_name}] Failed to connect: {e}")
            return False

    def log_startup_info(self) -> None:
        """Log timezone and startup information."""
        self.logger.info("=" * 60)
        self.logger.info(f"Starting {self.collector_name}")
        self.logger.info("=" * 60)

        # Timezone info
        tz_info = self.tz.get_timezone_info()
        self.logger.info(
            f"[STARTUP] Python process timezone: {tz_info['python_process_tz']}"
        )
        self.logger.info(f"[STARTUP] Current UTC time: {tz_info['utc_now']}")
        self.logger.info(f"[STARTUP] Timezone strict mode: {tz_info['strict_mode']}")

    def handle_error(self, error: Exception, context: str = "") -> bool:
        """
        Handle errors with exponential backoff.

        Args:
            error: The exception that occurred
            context: Additional context about where the error occurred

        Returns:
            True if should continue, False if max errors reached
        """
        self.consecutive_errors += 1

        error_msg = f"{context} - {str(error)}" if context else str(error)
        self.logger.error(
            f"[{self.collector_name}] Error ({self.consecutive_errors}/{self.max_consecutive_errors}): {error_msg}"
        )

        # Record in state
        self.state.record_error(error_msg)

        if self.consecutive_errors >= self.max_consecutive_errors:
            self.logger.error(
                f"[{self.collector_name}] Max consecutive errors reached, shutting down"
            )
            return False

        # Exponential backoff: 1s, 2s, 4s, 8s, max 60s
        backoff = min(2 ** (self.consecutive_errors - 1), 60)
        self.logger.info(
            f"[{self.collector_name}] Backing off for {backoff} seconds..."
        )
        time.sleep(backoff)

        return True

    def reset_error_count(self) -> None:
        """Reset consecutive error counter after successful operation."""
        if self.consecutive_errors > 0:
            self.consecutive_errors = 0
            self.state.clear_error()

    def should_stop(self) -> bool:
        """Check if collector should stop."""
        return self.shutdown_requested or not self.running

    @abstractmethod
    def run(self) -> None:
        """
        Main collector loop - must be implemented by subclasses.

        Should:
        1. Call self.log_startup_info()
        2. Connect to database
        3. Run collection loop checking self.should_stop()
        4. Handle errors gracefully
        5. Save state on shutdown
        """
        pass

    def shutdown(self) -> None:
        """Perform graceful shutdown."""
        self.logger.info(f"[{self.collector_name}] Shutting down...")

        try:
            # Save final state
            if (
                hasattr(self, "last_processed_timestamp")
                and self.last_processed_timestamp
            ):
                self.state.save_state(self.last_processed_timestamp, status="stopped")
                self.logger.info(
                    f"[{self.collector_name}] Final state saved: {self.last_processed_timestamp.isoformat()}"
                )

            # Close database connections
            self.db.close()
            self.logger.info(f"[{self.collector_name}] Database connections closed")

        except Exception as e:
            self.logger.error(f"[{self.collector_name}] Error during shutdown: {e}")

        self.logger.info(f"[{self.collector_name}] Shutdown complete")
