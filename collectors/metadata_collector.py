"""
Item metadata collector for OSRS items.

This collector fetches and updates item metadata (names, descriptions, properties)
from the OSRS Wiki API. Item metadata changes infrequently, so this collector
runs periodically (default: every 24 hours).

Features:
- Daily scheduled updates
- Manual trigger support
- Upsert logic for item records
- Detection of new items
"""

import logging
import os
import sys
import time
from typing import Dict, List, Set

from collectors.utils.timezone import now_utc

from collectors.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class MetadataCollector(BaseCollector):
    """
    Item metadata collector for OSRS.

    Fetches item information (names, IDs, properties) from the Wiki API.
    Updates are infrequent - runs on schedule or manual trigger.
    """

    def __init__(self):
        """Initialize metadata collector."""
        super().__init__("metadata_collector")

        # Configuration
        self.update_interval_hours = int(
            os.getenv("METADATA_UPDATE_INTERVAL_HOURS", "24")
        )
        self.run_once = os.getenv("METADATA_RUN_ONCE", "false").lower() == "true"

        # State
        self.items_updated = 0
        self.items_added = 0

        self.logger.info(
            f"[CONFIG] Update interval: {self.update_interval_hours} hours"
        )
        self.logger.info(f"[CONFIG] Run once mode: {self.run_once}")

    def run(self) -> None:
        """Main metadata collection loop."""
        self.log_startup_info()

        # Connect to database
        if not self.connect():
            self.logger.error("Failed to connect to database, exiting")
            sys.exit(1)

        self.running = True

        try:
            while not self.should_stop():
                try:
                    self.logger.info("[METADATA] Starting item metadata update...")

                    # Perform update
                    self._update_metadata()

                    # Reset error count
                    self.reset_error_count()

                    # Save state
                    self.state.save_state(
                        now_utc(),
                        status="completed" if self.run_once else "running",
                        metadata={
                            "items_updated": self.items_updated,
                            "items_added": self.items_added,
                            "last_update": now_utc().isoformat(),
                        },
                    )

                    if self.run_once:
                        self.logger.info(
                            "[METADATA] Run-once mode, exiting after single update"
                        )
                        break

                    # Sleep until next update
                    sleep_hours = self.update_interval_hours
                    self.logger.info(
                        f"[METADATA] Sleeping for {sleep_hours} hours until next update..."
                    )
                    self._sleep_with_check(sleep_hours * 3600)

                except Exception as e:
                    if not self.handle_error(e, "Metadata update"):
                        break

        finally:
            self.shutdown()

    def _update_metadata(self) -> None:
        """Fetch and update item metadata."""
        self.logger.info("[METADATA] Fetching item list from API...")

        # Fetch all item metadata
        metadata = self.api.get_item_metadata()

        if not metadata:
            self.logger.warning("[METADATA] No metadata returned from API")
            return

        self.logger.info(f"[METADATA] Received {len(metadata)} items from API")

        # Get existing item IDs from database
        existing_ids = self._get_existing_item_ids()
        self.logger.info(
            f"[METADATA] Found {len(existing_ids)} existing items in database"
        )

        # Process items - handle both dict and list formats
        items_to_update = []
        items_to_insert = []

        if isinstance(metadata, dict):
            # Original format: {item_id: item_data}
            for item_id_str, item_data in metadata.items():
                item_id = int(item_id_str)
                parsed = self.api.parse_item_metadata(item_id, item_data)

                if item_id in existing_ids:
                    items_to_update.append(parsed)
                else:
                    items_to_insert.append(parsed)
        elif isinstance(metadata, list):
            # List format: [{id: item_id, ...}]
            for item_data in metadata:
                item_id = int(item_data.get("id", 0))
                if item_id == 0:
                    continue
                parsed = self.api.parse_item_metadata(item_id, item_data)

                if item_id in existing_ids:
                    items_to_update.append(parsed)
                else:
                    items_to_insert.append(parsed)

        # Insert new items
        if items_to_insert:
            self._insert_items(items_to_insert)
            self.items_added = len(items_to_insert)
            self.logger.info(f"[METADATA] Added {len(items_to_insert)} new items")

        # Update existing items
        if items_to_update:
            self._update_items(items_to_update)
            self.items_updated = len(items_to_update)
            self.logger.info(
                f"[METADATA] Updated {len(items_to_update)} existing items"
            )

        # Log summary
        new_item_names = [item["name"] for item in items_to_insert[:5]]
        if new_item_names:
            self.logger.info(
                f"[METADATA] New items include: {', '.join(new_item_names)}"
            )

    def _get_existing_item_ids(self) -> Set[int]:
        """
        Get set of existing item IDs from database.

        Returns:
            Set of item IDs
        """
        query = "SELECT item_id FROM items"
        results = self.db.execute(query)
        return set(row[0] for row in results) if results else set()

    def _insert_items(self, items: List[Dict]) -> int:
        """
        Insert new items into database.

        Args:
            items: List of item metadata dicts

        Returns:
            Number of items inserted
        """
        if not items:
            return 0

        # Build insert query
        columns = [
            "item_id",
            "name",
            "description",
            "members",
            "tradeable",
            "tradeable_on_ge",
            "stackable",
            "noted",
            "noteable",
            "equipable",
            "equipable_by_player",
            "equipable_weapon",
            "cost",
            "lowalch",
            "highalch",
            "weight",
            "buy_limit",
            "quest_item",
            "release_date",
            "duplicate",
            "placeholder",
            "wiki_name",
            "wiki_url",
        ]

        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join(columns)

        query = f"""
            INSERT INTO items ({columns_str}, created_at, updated_at)
            VALUES ({placeholders}, NOW(), NOW())
            ON CONFLICT (item_id) DO NOTHING
        """

        # Prepare values
        values = []
        for item in items:
            row = [item.get(col) for col in columns]
            values.append(tuple(row))

        # Execute inserts
        inserted = 0
        for batch in self._batch(values, 1000):
            with self.db.transaction() as cursor:
                cursor.executemany(query, batch)
                inserted += cursor.rowcount

        return inserted

    def _update_items(self, items: List[Dict]) -> int:
        """
        Update existing items in database.

        Args:
            items: List of item metadata dicts

        Returns:
            Number of items updated
        """
        if not items:
            return 0

        # Update query - only update mutable fields
        query = """
            UPDATE items SET
                name = %s,
                description = %s,
                members = %s,
                tradeable = %s,
                tradeable_on_ge = %s,
                stackable = %s,
                noted = %s,
                noteable = %s,
                equipable = %s,
                equipable_by_player = %s,
                equipable_weapon = %s,
                cost = %s,
                lowalch = %s,
                highalch = %s,
                weight = %s,
                buy_limit = %s,
                quest_item = %s,
                release_date = %s,
                duplicate = %s,
                placeholder = %s,
                wiki_name = %s,
                wiki_url = %s,
                updated_at = NOW()
            WHERE item_id = %s
        """

        # Prepare values
        values = []
        for item in items:
            row = [
                item.get("name"),
                item.get("description"),
                item.get("members"),
                item.get("tradeable"),
                item.get("tradeable_on_ge"),
                item.get("stackable"),
                item.get("noted"),
                item.get("noteable"),
                item.get("equipable"),
                item.get("equipable_by_player"),
                item.get("equipable_weapon"),
                item.get("cost"),
                item.get("lowalch"),
                item.get("highalch"),
                item.get("weight"),
                item.get("buy_limit"),
                item.get("quest_item"),
                item.get("release_date"),
                item.get("duplicate"),
                item.get("placeholder"),
                item.get("wiki_name"),
                item.get("wiki_url"),
                item.get("item_id"),
            ]
            values.append(tuple(row))

        # Execute updates
        updated = 0
        for batch in self._batch(values, 1000):
            with self.db.transaction() as cursor:
                cursor.executemany(query, batch)
                updated += cursor.rowcount

        return updated

    def _batch(self, iterable: List, batch_size: int):
        """Batch an iterable into chunks."""
        for i in range(0, len(iterable), batch_size):
            yield iterable[i : i + batch_size]

    def _sleep_with_check(self, seconds: float) -> None:
        """Sleep with periodic checks for shutdown signal."""
        check_interval = 60.0  # Check every minute for long sleeps
        elapsed = 0.0

        while elapsed < seconds and not self.should_stop():
            time.sleep(min(check_interval, seconds - elapsed))
            elapsed += check_interval

    def get_item_count(self) -> int:
        """Get total number of items in database."""
        query = "SELECT COUNT(*) FROM items"
        results = self.db.execute(query)
        return results[0][0] if results else 0


if __name__ == "__main__":
    collector = MetadataCollector()
    collector.run()
