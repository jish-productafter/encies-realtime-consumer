from clickhouse_driver import Client

import os
import json
from datetime import datetime, timezone
from typing import Any, List, Optional, Dict
from pathlib import Path
from dotenv import load_dotenv
from app.schema import TraderClass


# Find .env file by searching from current directory up to project root
def find_env_file():
    current = Path.cwd()
    # Search up to 3 levels up
    for _ in range(3):
        env_file = current / ".env"
        if env_file.exists():
            return env_file, current
        current = current.parent
    # If not found, use current directory
    return Path.cwd() / ".env", Path.cwd()


env_path, project_root = find_env_file()

# Load .env file with explicit path
load_dotenv(dotenv_path=env_path, override=True)

# Debug: Check if .env file exists
if env_path.exists():
    print(f"✓ Found .env file at: {env_path}")
else:
    print(f"⚠ Warning: .env file not found at: {env_path}")
    print(f"   Current working directory: {Path.cwd()}")
    print(f"   Searched from: {project_root}")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", 8123)


client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
)


def bulk_insert_current_trades(data: List[Dict[str, Any]]):
    """
    Bulk insert trade data into polymarket.current_trades table.

    Args:
        data: List of dictionaries containing trade data with trader_class fields
    """
    if not data:
        return

    try:
        # Prepare data for insertion - convert types and handle missing trader_class fields
        rows = []
        for entry in data:
            # Convert outcomeIndex from string to int, default to 0 if invalid
            outcome_index = 0
            try:
                outcome_index = int(entry.get("outcomeIndex", 0))
            except (ValueError, TypeError):
                outcome_index = 0

            # Convert price and size from string to float
            price = 0.0
            try:
                price = float(entry.get("price", 0))
            except (ValueError, TypeError):
                price = 0.0

            size = 0.0
            try:
                size = float(entry.get("size", 0))
            except (ValueError, TypeError):
                size = 0.0

            # Convert timestamp strings to datetime (timezone-naive for ClickHouse)
            timestamp = None
            if entry.get("timestamp"):
                try:
                    if isinstance(entry["timestamp"], str):
                        dt = datetime.fromisoformat(
                            entry["timestamp"].replace("Z", "+00:00")
                        )
                        # Make timezone-naive by converting to UTC then removing timezone
                        if dt.tzinfo is not None:
                            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                        timestamp = dt
                    else:
                        timestamp = entry["timestamp"]
                        # Ensure timezone-naive
                        if (
                            timestamp is not None
                            and hasattr(timestamp, "tzinfo")
                            and timestamp.tzinfo is not None
                        ):
                            timestamp = timestamp.astimezone(timezone.utc).replace(
                                tzinfo=None
                            )
                except (ValueError, AttributeError):
                    timestamp = datetime.now()
            else:
                timestamp = datetime.now()

            message_timestamp = None
            if entry.get("messageTimestamp"):
                try:
                    if isinstance(entry["messageTimestamp"], str):
                        dt = datetime.fromisoformat(
                            entry["messageTimestamp"].replace("Z", "+00:00")
                        )
                        # Make timezone-naive
                        if dt.tzinfo is not None:
                            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                        message_timestamp = dt
                    else:
                        message_timestamp = entry["messageTimestamp"]
                        # Ensure timezone-naive
                        if (
                            message_timestamp is not None
                            and hasattr(message_timestamp, "tzinfo")
                            and message_timestamp.tzinfo is not None
                        ):
                            message_timestamp = message_timestamp.astimezone(
                                timezone.utc
                            ).replace(tzinfo=None)
                except (ValueError, AttributeError):
                    message_timestamp = datetime.now()
            else:
                message_timestamp = datetime.now()

            # Handle calculated_at - convert from string if present, use default if None
            calculated_at = datetime(1970, 1, 1)  # Default value for ClickHouse
            calculated_at_value = entry.get("calculated_at")
            if calculated_at_value:
                try:
                    if isinstance(calculated_at_value, str):
                        dt = datetime.fromisoformat(
                            calculated_at_value.replace("Z", "+00:00")
                        )
                        # Make timezone-naive
                        if dt.tzinfo is not None:
                            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                        calculated_at = dt
                    elif isinstance(calculated_at_value, datetime):
                        calculated_at = calculated_at_value
                        # Ensure timezone-naive
                        if calculated_at.tzinfo is not None:
                            calculated_at = calculated_at.astimezone(
                                timezone.utc
                            ).replace(tzinfo=None)
                    # If it's None or other type, keep default
                except (ValueError, AttributeError, TypeError):
                    calculated_at = datetime(1970, 1, 1)

            # Prepare row with all fields in the correct order
            row = (
                entry.get("id", ""),
                entry.get("asset", ""),
                entry.get("bio", ""),
                entry.get("conditionId", ""),
                entry.get("eventSlug", ""),
                entry.get("icon", ""),
                entry.get("name", ""),
                entry.get("outcome", ""),
                outcome_index,
                price,
                entry.get("profileImage", ""),
                entry.get("proxyWallet", ""),
                entry.get("pseudonym", ""),
                entry.get("side", ""),
                size,
                entry.get("slug", ""),
                timestamp,
                entry.get("title", ""),
                entry.get("transactionHash", ""),
                entry.get("topic", ""),
                entry.get("type", ""),
                message_timestamp,
                entry.get("connection_id", ""),
                entry.get("rawType", ""),
                (
                    entry.get("global_roi_pct")
                    if entry.get("global_roi_pct") is not None
                    else 0.0
                ),
                (
                    entry.get("consistency_rating")
                    if entry.get("consistency_rating") is not None
                    else 0.0
                ),
                (
                    entry.get("recency_weighted_pnl")
                    if entry.get("recency_weighted_pnl") is not None
                    else 0.0
                ),
                entry.get("trader_class", ""),
                calculated_at,
            )
            rows.append(row)

        # Bulk insert
        client.execute(
            """
            INSERT INTO polymarket.current_trades (
                id, asset, bio, conditionId, eventSlug, icon, name, outcome, outcomeIndex,
                price, profileImage, proxyWallet, pseudonym, side, size, slug, timestamp,
                title, transactionHash, topic, type, messageTimestamp, connection_id, rawType,
                global_roi_pct, consistency_rating, recency_weighted_pnl, trader_class, calculated_at
            ) VALUES
            """,
            rows,
        )
        print(f"✓ Inserted {len(rows)} trades into polymarket.current_trades")
    except Exception as e:
        print(f"Error bulk inserting trades: {e}")


def get_trader_classes_from_db_map(proxy_wallets: List[str]) -> Dict[str, TraderClass]:
    # Convert input to lowercase and quote for SQL
    proxy_wallets_lower = [wallet.lower() for wallet in proxy_wallets]
    quoted_wallets = [f"'{wallet}'" for wallet in proxy_wallets_lower]
    query = f"SELECT proxyWallet, global_roi_pct, consistency_rating, recency_weighted_pnl, trader_class, calculated_at FROM polymarket.trader_alpha_scores final WHERE lower(proxyWallet) IN ({','.join(quoted_wallets)})"
    try:
        result = client.execute(query)
        # Convert tuples to dictionaries
        # Column order: proxyWallet, global_roi_pct, consistency_rating, recency_weighted_pnl, trader_class, calculated_at
        column_names = [
            "proxyWallet",
            "global_roi_pct",
            "consistency_rating",
            "recency_weighted_pnl",
            "trader_class",
            "calculated_at",
        ]
        result_dict = {}
        for row in result:
            # Convert tuple to dict, handling datetime conversion
            row_dict = dict(zip(column_names, row))
            # Convert datetime to string if needed
            if isinstance(row_dict["calculated_at"], datetime):
                row_dict["calculated_at"] = row_dict["calculated_at"].strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            result_dict[row[0].lower()] = TraderClass.model_validate(row_dict)
        return result_dict
    except Exception as e:
        print(f"Error getting trader classes from DB map: {e}")
        return {}
