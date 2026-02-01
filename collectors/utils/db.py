"""
Database connection and query utilities for OSRS Market Data Collector.

This module provides:
- Connection pooling for TimescaleDB
- Transaction management
- Query execution with error handling
- Connection health checks and reconnection
"""

import logging
import os
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import OperationalError, pool
from psycopg2.extras import RealDictCursor, execute_values

from .timezone import ensure_utc, now_utc, validate_utc

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Base exception for database operations."""

    pass


class DatabaseConnectionError(DatabaseError):
    """Raised when database connection fails."""

    pass


class DatabaseQueryError(DatabaseError):
    """Raised when a query execution fails."""

    pass


class DatabaseManager:
    """
    Manages database connections and operations.

    Provides:
    - Connection pooling
    - Automatic reconnection
    - Transaction management
    - Query execution with parameter binding
    - Bulk insert operations optimized for TimescaleDB
    """

    def __init__(self):
        """Initialize database manager with connection parameters from environment."""
        self.host = os.getenv("POSTGRES_HOST", "timescaledb")
        self.port = int(os.getenv("POSTGRES_PORT", 5432))
        self.user = os.getenv("POSTGRES_USER", "osrs_trader")
        self.password = os.getenv("POSTGRES_PASSWORD", "change_me_in_env_file")
        self.dbname = os.getenv("POSTGRES_DB", "osrs_market")

        self.min_connections = 1
        self.max_connections = 5

        self._pool: Optional[pool.ThreadedConnectionPool] = None
        self._connection: Optional[psycopg2.extensions.connection] = None

        logger.info(
            f"[DB] DatabaseManager initialized for {self.host}:{self.port}/{self.dbname}"
        )

    def _get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters dictionary."""
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "dbname": self.dbname,
            "connect_timeout": 10,
            "options": "-c timezone=UTC",  # Force UTC timezone for session
        }

    def connect(self, max_retries: int = 5, retry_delay: float = 1.0) -> None:
        """
        Establish database connection with retry logic.

        Args:
            max_retries: Maximum number of connection attempts
            retry_delay: Initial delay between retries (exponential backoff)

        Raises:
            DatabaseConnectionError: If all retries fail
        """
        params = self._get_connection_params()

        for attempt in range(max_retries):
            try:
                if self._pool is None:
                    self._pool = pool.ThreadedConnectionPool(
                        minconn=self.min_connections,
                        maxconn=self.max_connections,
                        **params,
                    )

                # Test the connection
                conn = self._pool.getconn()
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
                    cur.execute("SHOW timezone")
                    tz = cur.fetchone()[0]
                    if tz != "UTC":
                        logger.warning(f"[DB] Database timezone is {tz}, expected UTC")
                    else:
                        logger.info(f"[DB] TimescaleDB timezone verified: {tz}")
                self._pool.putconn(conn)

                logger.info("[DB] Database connection pool established successfully")
                return

            except OperationalError as e:
                logger.warning(
                    f"[DB] Connection attempt {attempt + 1}/{max_retries} failed: {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (2**attempt))  # Exponential backoff
                else:
                    raise DatabaseConnectionError(
                        f"Failed to connect after {max_retries} attempts: {e}"
                    )

    def get_connection(self) -> psycopg2.extensions.connection:
        """
        Get a connection from the pool.

        Returns:
            Database connection

        Raises:
            DatabaseConnectionError: If pool is not initialized
        """
        if self._pool is None:
            self.connect()
            assert self._pool is not None, "Pool should be initialized after connect"

        try:
            conn = self._pool.getconn()
            # Ensure connection is alive
            if conn.closed:
                self._pool.putconn(conn)
                self.connect()
                assert self._pool is not None, (
                    "Pool should be initialized after reconnect"
                )
                conn = self._pool.getconn()
            return conn
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to get connection from pool: {e}")

    def release_connection(self, conn: psycopg2.extensions.connection) -> None:
        """Return a connection to the pool."""
        if self._pool and conn:
            self._pool.putconn(conn)

    @contextmanager
    def transaction(self):
        """
        Context manager for database transactions.

        Usage:
            with db.transaction() as cursor:
                cursor.execute("INSERT ...")
                cursor.execute("UPDATE ...")

        Automatically commits on success, rolls back on exception.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
            logger.debug("[DB] Transaction committed successfully")
        except Exception as e:
            conn.rollback()
            logger.error(f"[DB] Transaction rolled back due to error: {e}")
            raise DatabaseQueryError(f"Transaction failed: {e}") from e
        finally:
            cursor.close()
            self.release_connection(conn)

    def execute(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """
        Execute a query and return results.

        Args:
            query: SQL query string
            params: Query parameters (for parameterized queries)

        Returns:
            List of result tuples
        """
        with self.transaction() as cursor:
            cursor.execute(query, params)
            if cursor.description:
                return cursor.fetchall()
            return []

    def execute_dict(self, query: str, params: Optional[Tuple] = None) -> List[Dict]:
        """
        Execute a query and return results as dictionaries.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            List of result dictionaries
        """
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                if cursor.description:
                    return [dict(row) for row in cursor.fetchall()]
                return []
        finally:
            self.release_connection(conn)

    def execute_many(self, query: str, params_list: List[Tuple]) -> int:
        """
        Execute a query multiple times with different parameters.

        Args:
            query: SQL query string
            params_list: List of parameter tuples

        Returns:
            Number of rows affected
        """
        with self.transaction() as cursor:
            cursor.executemany(query, params_list)
            return cursor.rowcount

    def bulk_insert_prices(self, records: List[Dict]) -> int:
        """
        Bulk insert price records into TimescaleDB.

        Optimized for TimescaleDB hypertables with conflict handling.

        Args:
            records: List of price record dictionaries with keys:
                    item_id, timestamp, avg_high_price, avg_low_price,
                    high_price_volume, low_price_volume

        Returns:
            Number of records inserted
        """
        if not records:
            return 0

        # Validate and format timestamps
        formatted_records = []
        for record in records:
            # Ensure timestamp is UTC
            ts = record.get("timestamp")
            if ts:
                if isinstance(ts, str):
                    ts = ensure_utc(ts, "bulk_insert_prices")
                validate_utc(ts, "bulk_insert_prices timestamp")

            formatted_records.append(
                (
                    record.get("item_id"),
                    ts.isoformat() if ts else None,
                    record.get("avg_high_price"),
                    record.get("avg_low_price"),
                    record.get("high_price_volume"),
                    record.get("low_price_volume"),
                    now_utc().isoformat(),  # inserted_at
                )
            )

        query = """
            INSERT INTO prices (
                item_id, timestamp, avg_high_price, avg_low_price,
                high_price_volume, low_price_volume, inserted_at
            ) VALUES %s
            ON CONFLICT (timestamp, item_id) DO NOTHING
        """

        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                execute_values(cursor, query, formatted_records, page_size=1000)
                conn.commit()
                inserted = cursor.rowcount
                logger.debug(f"[DB] Bulk inserted {inserted} price records")
                return inserted
        except Exception as e:
            conn.rollback()
            logger.error(f"[DB] Bulk insert failed: {e}")
            raise DatabaseQueryError(f"Bulk insert failed: {e}") from e
        finally:
            self.release_connection(conn)

    def update_latest_prices(self, records: List[Dict]) -> int:
        """
        Update latest prices table with current data.

        Uses UPSERT (INSERT ON CONFLICT UPDATE) to maintain current snapshot.

        Args:
            records: List of price records

        Returns:
            Number of records upserted
        """
        if not records:
            return 0

        formatted_records = []
        for record in records:
            ts = record.get("timestamp")
            if ts and isinstance(ts, str):
                ts = ensure_utc(ts, "update_latest_prices")

            formatted_records.append(
                (
                    record.get("item_id"),
                    ts.isoformat() if ts else None,
                    record.get("avg_high_price"),
                    record.get("avg_low_price"),
                    now_utc().isoformat(),
                )
            )

        # First, get list of valid item_ids from items table to avoid FK violations
        try:
            valid_items_query = "SELECT item_id FROM items"
            valid_items_result = self.execute(valid_items_query)
            valid_item_ids = (
                set(row[0] for row in valid_items_result)
                if valid_items_result
                else set()
            )
        except Exception:
            valid_item_ids = set()

        # Filter records to only include items that exist in the database
        if valid_item_ids:
            filtered_records = [r for r in formatted_records if r[0] in valid_item_ids]
            skipped_count = len(formatted_records) - len(filtered_records)
            if skipped_count > 0:
                logger.debug(
                    f"[DB] Skipped {skipped_count} records for items not in items table"
                )
        else:
            # No items in database yet, skip all to avoid FK errors
            logger.debug("[DB] No items in database yet, skipping latest_prices update")
            return 0

        if not filtered_records:
            return 0

        query = """
            INSERT INTO latest_prices (
                item_id, timestamp, avg_high_price, avg_low_price, updated_at
            ) VALUES %s
            ON CONFLICT (item_id) DO UPDATE SET
                timestamp = EXCLUDED.timestamp,
                avg_high_price = EXCLUDED.avg_high_price,
                avg_low_price = EXCLUDED.avg_low_price,
                updated_at = EXCLUDED.updated_at
        """

        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                execute_values(cursor, query, filtered_records, page_size=1000)
                conn.commit()
                upserted = cursor.rowcount
                logger.debug(f"[DB] Upserted {upserted} latest price records")
                return upserted
        except Exception as e:
            conn.rollback()
            raise DatabaseQueryError(f"Latest prices update failed: {e}") from e
        finally:
            self.release_connection(conn)

    def get_last_timestamp(
        self, table: str = "prices", item_id: Optional[int] = None
    ) -> Optional[Any]:
        """
        Get the most recent timestamp from a table.

        Args:
            table: Table name ('prices' or 'latest_prices')
            item_id: Optional item ID to filter by

        Returns:
            Most recent timestamp or None
        """
        if item_id:
            query = f"""
                SELECT MAX(timestamp) as max_ts 
                FROM {table} 
                WHERE item_id = %s
            """
            params = (item_id,)
        else:
            query = f"SELECT MAX(timestamp) as max_ts FROM {table}"
            params = None

        results = self.execute(query, params)
        if results and results[0][0]:
            return results[0][0]
        return None

    def get_oldest_timestamp(self, table: str = "prices") -> Optional[Any]:
        """Get the oldest timestamp from prices table."""
        query = f"SELECT MIN(timestamp) as min_ts FROM {table}"
        results = self.execute(query)
        if results and results[0][0]:
            return results[0][0]
        return None

    def get_database_size(self) -> Dict[str, Any]:
        """Get database size information."""
        query = "SELECT * FROM get_database_size()"
        results = self.execute_dict(query)
        return results[0] if results else {}

    def get_prices_table_size(self) -> List[Dict]:
        """Get prices table chunk sizes."""
        query = "SELECT * FROM get_prices_table_size()"
        return self.execute_dict(query)

    def close(self) -> None:
        """Close all database connections."""
        if self._pool:
            self._pool.closeall()
            logger.info("[DB] Database connection pool closed")

    def health_check(self) -> bool:
        """Check if database connection is healthy."""
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            self.release_connection(conn)
            return True
        except Exception as e:
            logger.error(f"[DB] Health check failed: {e}")
            return False


# Global database manager instance
db_manager = DatabaseManager()
