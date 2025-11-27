"""
Trade processing service that consumes from Redis and broadcasts to WebSocket clients.
"""

from redis_client import redis
from app.db import (
    get_trader_classes_from_db_map,
    bulk_insert_current_trades,
)
from app.schema import TradeEntry
import time
import json
from typing import Callable, Optional
import asyncio

STREAM_KEY = "polymarket.trades"


class TradeService:
    """Service for processing trades from Redis stream."""

    def __init__(self, broadcast_callback: Optional[Callable[[dict], None]] = None):
        """
        Initialize the trade service.

        Args:
            broadcast_callback: Optional callback function to broadcast trade data
        """
        self.broadcast_callback = broadcast_callback
        self.running = False
        self.last_id = "0-0"

    def get_last_message_id(self, stream_key: str) -> str:
        """
        Get the last message ID from the stream.
        Returns "0-0" if stream is empty or doesn't exist.
        """
        try:
            # Get the most recent message (last entry in the stream)
            result = redis.xrevrange(stream_key, count=1)
            if result:
                # result is a list of tuples: [(message_id, {data})]
                return result[0][0]  # Return the message ID
            else:
                # Stream is empty, start from beginning
                return "0-0"
        except Exception as e:
            print(f"Error getting last message ID: {e}")
            # If stream doesn't exist or error, start from beginning
            return "0-0"

    def process_trade_entry(
        self, trade_entry: TradeEntry, trader_classes: dict
    ) -> dict:
        """
        Process a single trade entry and merge with trader class data.

        Args:
            trade_entry: The trade entry to process
            trader_classes: Dictionary of trader classes keyed by proxyWallet

        Returns:
            Flat JSON dictionary with merged trade and trader class data
        """
        trader_class = trader_classes.get(trade_entry.trade.proxyWallet.lower())
        # Create flat JSON by merging trade_entry and trader_class
        entry_data = {
            "id": trade_entry.id,
            **trade_entry.trade.model_dump(),
        }
        if trader_class:
            entry_data.update(trader_class.model_dump())
        return entry_data

    def process_batch(self, entries: list) -> list[dict]:
        """
        Process a batch of trade entries.

        Args:
            entries: List of (message_id, trade_dict) tuples

        Returns:
            List of processed trade data dictionaries
        """
        if not entries:
            return []

        trade_entries = TradeEntry.decode_batch(entries)
        flat_data = []

        if trade_entries:
            # Get unique proxy wallets
            proxy_wallets = [
                trade_entry.trade.proxyWallet for trade_entry in trade_entries
            ]
            # Fetch trader classes for all wallets
            trader_classes = get_trader_classes_from_db_map(proxy_wallets)

            # Process each trade entry
            for trade_entry in trade_entries:
                entry_data = self.process_trade_entry(trade_entry, trader_classes)
                flat_data.append(entry_data)

                # Broadcast to WebSocket clients if callback is set
                if self.broadcast_callback:
                    try:
                        self.broadcast_callback(entry_data)
                    except Exception as e:
                        print(f"Error broadcasting trade data: {e}")

        return flat_data

    async def start(self):
        """Start the trade processing service."""
        self.running = True
        print(f"Starting trade service from last_id: {self.last_id}")

        loop = asyncio.get_event_loop()

        while self.running:
            try:
                # Run blocking Redis operations in executor to avoid blocking event loop
                response = await loop.run_in_executor(
                    None,
                    lambda: redis.xread(
                        {STREAM_KEY: self.last_id},
                        count=100,
                        block=2000,  # read 100 messages at a time, wait 2 seconds if no messages
                    ),
                )

                for message in response:
                    stream_name, entries = message
                    if entries:
                        # Collect message IDs for deletion after successful insertion
                        message_ids = [entry[0] for entry in entries]
                        # Update last_id to the last message ID in this batch
                        self.last_id = entries[-1][0]

                        print(
                            f"Processed {len(entries)} trades from {stream_name}, last_id: {self.last_id}"
                        )

                        # Process the batch (runs in current thread, should be fast)
                        flat_data = self.process_batch(entries)

                        # Bulk insert into ClickHouse (run in executor to avoid blocking)
                        if flat_data:
                            try:
                                await loop.run_in_executor(
                                    None, bulk_insert_current_trades, flat_data
                                )
                                # Delete messages from Redis after successful insertion
                                if message_ids:
                                    deleted_count = await loop.run_in_executor(
                                        None,
                                        lambda: redis.xdel(STREAM_KEY, *message_ids),
                                    )
                                    print(
                                        f"✓ Deleted {deleted_count} messages from Redis stream"
                                    )
                            except Exception as insert_error:
                                print(
                                    f"Error during insertion, keeping messages in Redis: {insert_error}"
                                )
                                raise  # Re-raise to be caught by outer exception handler

            except Exception as e:
                print(f"Error in trade service: {e}")
                await asyncio.sleep(1)

    def stop(self):
        """Stop the trade processing service."""
        self.running = False
        print("Trade service stopped")
