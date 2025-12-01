from clickhouse_driver import Client

import os
import json
from datetime import datetime, timezone
from typing import Any, List, Optional, Dict
from pathlib import Path
from dotenv import load_dotenv
from app.schema import TraderClass
from redis_client import redis


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


def get_clickhouse_client() -> Client:
    """
    Create a new ClickHouse client instance.

    Note:
        The clickhouse-driver Client is not thread-safe. This function returns
        a fresh client so that each thread / executor task uses its own
        connection, avoiding "Simultaneous queries on single connection
        detected" errors when used from different threads.
    """
    return Client(
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

        # Bulk insert - use a fresh client/connection for this operation
        with get_clickhouse_client() as client:
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


def get_trades_by_slug(slug: str) -> List[Dict[str, Any]]:
    """
    Get trades from polymarket.current_trades table filtered by slug.

    Args:
        slug: The slug to filter trades by

    Returns:
        List of dictionaries containing trade data
    """
    # Escape the slug to prevent SQL injection
    # Replace single quotes with two single quotes (SQL escaping)
    escaped_slug = slug.replace("'", "''")
    query = f"""
        SELECT 
            id, asset, bio, conditionId, eventSlug, icon, name, outcome, outcomeIndex,
            price, profileImage, proxyWallet, pseudonym, side, size, slug, timestamp,
            title, transactionHash, topic, type, messageTimestamp, connection_id, rawType,
            global_roi_pct, consistency_rating, recency_weighted_pnl, trader_class, calculated_at
        FROM polymarket.current_trades
        WHERE slug = '{escaped_slug}'
        ORDER BY timestamp DESC
    """
    try:
        with get_clickhouse_client() as client:
            result = client.execute(query)
        # Column names matching the SELECT order
        column_names = [
            "id",
            "asset",
            "bio",
            "conditionId",
            "eventSlug",
            "icon",
            "name",
            "outcome",
            "outcomeIndex",
            "price",
            "profileImage",
            "proxyWallet",
            "pseudonym",
            "side",
            "size",
            "slug",
            "timestamp",
            "title",
            "transactionHash",
            "topic",
            "type",
            "messageTimestamp",
            "connection_id",
            "rawType",
            "global_roi_pct",
            "consistency_rating",
            "recency_weighted_pnl",
            "trader_class",
            "calculated_at",
        ]

        trades = []
        for row in result:
            # Convert tuple to dict
            trade_dict = dict(zip(column_names, row))

            # Convert datetime objects to ISO format strings
            for date_field in ["timestamp", "messageTimestamp", "calculated_at"]:
                if trade_dict.get(date_field) and isinstance(
                    trade_dict[date_field], datetime
                ):
                    trade_dict[date_field] = trade_dict[date_field].isoformat()

            # Convert numeric types to appropriate Python types
            if trade_dict.get("outcomeIndex") is not None:
                trade_dict["outcomeIndex"] = int(trade_dict["outcomeIndex"])
            if trade_dict.get("price") is not None:
                trade_dict["price"] = float(trade_dict["price"])
            if trade_dict.get("size") is not None:
                trade_dict["size"] = float(trade_dict["size"])
            if trade_dict.get("global_roi_pct") is not None:
                trade_dict["global_roi_pct"] = float(trade_dict["global_roi_pct"])
            if trade_dict.get("consistency_rating") is not None:
                trade_dict["consistency_rating"] = float(
                    trade_dict["consistency_rating"]
                )
            if trade_dict.get("recency_weighted_pnl") is not None:
                trade_dict["recency_weighted_pnl"] = float(
                    trade_dict["recency_weighted_pnl"]
                )

            trades.append(trade_dict)

        return trades
    except Exception as e:
        print(f"Error getting trades by slug: {e}")
        return []


def get_trader_classes_from_db_map(proxy_wallets: List[str]) -> Dict[str, TraderClass]:
    # Convert input to lowercase
    proxy_wallets_lower = [wallet.lower() for wallet in proxy_wallets]
    result_dict = {}
    cache_misses = []
    cache_hits = []

    print(
        f"[TraderClass Cache] Checking cache for {len(proxy_wallets_lower)} proxy wallets"
    )

    # Check Redis cache for each proxyWallet
    try:
        for wallet_lower in proxy_wallets_lower:
            cache_key = f"trader_class_{wallet_lower}"
            cached_data = redis.get(cache_key)
            if cached_data:
                # Cache hit - deserialize and add to result
                try:
                    trader_data = json.loads(cached_data)
                    result_dict[wallet_lower] = TraderClass.model_validate(trader_data)
                    cache_hits.append(wallet_lower)
                    print(f"[TraderClass Cache] ✓ Cache HIT for {wallet_lower}")
                except (json.JSONDecodeError, Exception) as e:
                    print(
                        f"[TraderClass Cache] ✗ Error parsing cached trader class for {wallet_lower}: {e}"
                    )
                    # If cache data is corrupted, treat as cache miss
                    cache_misses.append(wallet_lower)
            else:
                # Cache miss - need to query DB
                cache_misses.append(wallet_lower)
                print(f"[TraderClass Cache] ✗ Cache MISS for {wallet_lower}")
    except Exception as e:
        print(f"[TraderClass Cache] ✗ Error checking Redis cache: {e}")
        # If Redis fails, treat all as cache misses
        cache_misses = proxy_wallets_lower
        cache_hits = []

    print(
        f"[TraderClass Cache] Summary: {len(cache_hits)} hits, {len(cache_misses)} misses"
    )

    # Query DB only for cache misses
    if cache_misses:
        print(
            f"[TraderClass Cache] Querying DB for {len(cache_misses)} wallets: {', '.join(cache_misses[:5])}{'...' if len(cache_misses) > 5 else ''}"
        )
        quoted_wallets = [f"'{wallet}'" for wallet in cache_misses]
        query = f"SELECT proxyWallet, global_roi_pct, consistency_rating, recency_weighted_pnl, trader_class, calculated_at FROM polymarket.trader_alpha_scores final WHERE lower(proxyWallet) IN ({','.join(quoted_wallets)})"
        try:
            with get_clickhouse_client() as client:
                db_result = client.execute(query)
            db_rows_count = 0
            cached_count = 0

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
            for row in db_result:
                db_rows_count += 1
                # Convert tuple to dict, handling datetime conversion
                row_dict = dict(zip(column_names, row))
                # Convert datetime to string if needed
                if isinstance(row_dict["calculated_at"], datetime):
                    row_dict["calculated_at"] = row_dict["calculated_at"].strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                wallet_lower = row[0].lower()
                trader_class = TraderClass.model_validate(row_dict)
                result_dict[wallet_lower] = trader_class

                # Cache the result in Redis
                try:
                    cache_key = f"trader_class_{wallet_lower}"
                    # Serialize TraderClass to JSON for caching
                    trader_json = trader_class.model_dump_json()
                    redis.set(cache_key, trader_json)
                    cached_count += 1
                    print(
                        f"[TraderClass Cache] ✓ Cached trader class for {wallet_lower}"
                    )
                except Exception as e:
                    print(
                        f"[TraderClass Cache] ✗ Error caching trader class for {wallet_lower}: {e}"
                    )

            print(
                f"[TraderClass Cache] DB query returned {db_rows_count} rows, cached {cached_count} entries"
            )
        except Exception as e:
            print(
                f"[TraderClass Cache] ✗ Error getting trader classes from DB map: {e}"
            )

    print(f"[TraderClass Cache] Returning {len(result_dict)} trader classes total")
    return result_dict


def get_trade_summary_by_slug(slug: str) -> List[Dict[str, Any]]:
    """
    Get trade summary from polymarket.current_trades table filtered by slug.
    Returns token holdings by trader based on BUY and SELL sides.

    Args:
        slug: The slug to filter trades by

    Returns:
        List of dictionaries containing trade summary data for each trader/asset/outcome combination
    """
    # Escape the slug to prevent SQL injection
    escaped_slug = slug.replace("'", "''")

    query = f"""
        SELECT 
            proxyWallet,
            pseudonym,
            asset,
            conditionId,
            title,
            outcome,
            -- Total tokens bought
            sum(if(side = 'BUY', size, 0)) as total_buy_size,
            -- Total tokens sold
            sum(if(side = 'SELL', size, 0)) as total_sell_size,
            -- Net position (positive = holding, negative = short)
            sum(if(side = 'BUY', size, -size)) as net_position,
            -- Total value bought
            sum(if(side = 'BUY', size * price, 0)) as total_buy_value,
            -- Total value sold
            sum(if(side = 'SELL', size * price, 0)) as total_sell_value,
            -- Net cash flow (positive = net profit from trades, negative = net cost)
            sum(if(side = 'SELL', size * price, 0)) - sum(if(side = 'BUY', size * price, 0)) as net_cash_flow,
            -- Number of buy trades
            countIf(side = 'BUY') as buy_count,
            -- Number of sell trades
            countIf(side = 'SELL') as sell_count,
            -- Latest trade timestamp
            max(timestamp) as last_trade_time
        FROM polymarket.current_trades FINAL
        WHERE slug = '{escaped_slug}' AND side IN ('BUY', 'SELL')
        GROUP BY proxyWallet, pseudonym, asset, conditionId, title, outcome
        HAVING net_position != 0  -- Only show positions that are not zero
        ORDER BY proxyWallet, asset, outcome
    """

    try:
        with get_clickhouse_client() as client:
            result = client.execute(query)
        # Column names matching the SELECT order
        column_names = [
            "proxyWallet",
            "pseudonym",
            "asset",
            "conditionId",
            "title",
            "outcome",
            "total_buy_size",
            "total_sell_size",
            "net_position",
            "total_buy_value",
            "total_sell_value",
            "net_cash_flow",
            "buy_count",
            "sell_count",
            "last_trade_time",
        ]

        summaries = []
        for row in result:
            # Convert tuple to dict
            summary_dict = dict(zip(column_names, row))

            # Convert datetime objects to ISO format strings
            if summary_dict.get("last_trade_time") and isinstance(
                summary_dict["last_trade_time"], datetime
            ):
                summary_dict["last_trade_time"] = summary_dict[
                    "last_trade_time"
                ].isoformat()

            # Convert numeric types to appropriate Python types
            for numeric_field in [
                "total_buy_size",
                "total_sell_size",
                "net_position",
                "total_buy_value",
                "total_sell_value",
                "net_cash_flow",
            ]:
                if summary_dict.get(numeric_field) is not None:
                    summary_dict[numeric_field] = float(summary_dict[numeric_field])

            for int_field in ["buy_count", "sell_count"]:
                if summary_dict.get(int_field) is not None:
                    summary_dict[int_field] = int(summary_dict[int_field])

            summaries.append(summary_dict)

        return summaries
    except Exception as e:
        print(f"Error getting trade summary by slug: {e}")
        return []


def get_pro_trader_trade_summary_by_market_id(market_id: str) -> List[Dict[str, Any]]:
    # Escape the market_id to prevent SQL injection
    escaped_market_id = market_id.replace("'", "''")
    query = f"""
        SELECT 
            conditionId,
            asset,
            sum(if(side = 'BUY', size, 0)) - sum(if(side = 'SELL', size, 0)) as total_current_position,
            uniq(proxyWallet) as num_traders
    FROM polymarket.current_trades
    WHERE trader_class IN ('GOD-TIER', 'PRO') 
    AND conditionId = '{escaped_market_id}'
    GROUP BY conditionId, asset
    HAVING total_current_position > 0
    ORDER BY total_current_position DESC
    """
    with get_clickhouse_client() as client:
        data = client.execute(query)
    column_names = [
        "conditionId",
        "asset",
        "total_current_position",
        "num_traders",
    ]
    summaries = []
    for row in data:
        summary_dict = dict(zip(column_names, row))
        summaries.append(summary_dict)
    return summaries
