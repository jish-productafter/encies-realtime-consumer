import asyncio
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import httpx


class RateLimiter:
    def __init__(self, max_requests: int = 5, window_ms: int = 10000):
        self.request_timestamps: list[float] = []
        self.max_requests = max_requests
        self.window_ms = window_ms

    async def wait_for_slot(self) -> None:
        now = datetime.now().timestamp() * 1000  # Convert to milliseconds
        window_start = now - self.window_ms

        # Remove timestamps outside the current window
        self.request_timestamps = [
            timestamp
            for timestamp in self.request_timestamps
            if timestamp > window_start
        ]

        # If we're at the limit, wait until the oldest request expires
        if len(self.request_timestamps) >= self.max_requests:
            oldest_timestamp = self.request_timestamps[0]
            wait_time = oldest_timestamp + self.window_ms - now + 1  # +1ms buffer
            if wait_time > 0:
                print(
                    f"[{datetime.now().isoformat()}] RateLimiter: Rate limit reached, waiting {wait_time}ms"
                )
                await asyncio.sleep(wait_time / 1000)  # Convert ms to seconds
                # Recursively check again after waiting
                return await self.wait_for_slot()

        # Add current request timestamp
        self.request_timestamps.append(datetime.now().timestamp() * 1000)


# Singleton rate limiter instance
rate_limiter = RateLimiter()


# Wrapper for httpx that respects rate limits
async def rate_limited_fetch(
    url: str, client: httpx.AsyncClient, **kwargs: Any
) -> httpx.Response:
    await rate_limiter.wait_for_slot()
    return await client.get(url, **kwargs)


def format_date_for_api(date_string: str) -> str:
    try:
        formatted = date_string.strip()

        # If it's just a date (YYYY-MM-DD), add time
        if re.match(r"^\d{4}-\d{2}-\d{2}$", formatted):
            formatted = f"{formatted}T00:00:00.000"

        # Replace space with T if present (handles "YYYY-MM-DD HH:MM:SS" format)
        formatted = formatted.replace(" ", "T")

        # Ensure it ends with Z (UTC timezone)
        if not formatted.endswith("Z"):
            # Remove any existing timezone offset
            formatted = re.sub(r"[+-]\d{2}:\d{2}$", "", formatted)
            formatted = re.sub(r"[+-]\d{4}$", "", formatted)
            # Add Z if not present
            if "Z" not in formatted:
                formatted = formatted + "Z"

        # Validate by parsing
        try:
            # Handle Z suffix for UTC
            if formatted.endswith("Z"):
                dt_str = formatted.replace("Z", "+00:00")
            else:
                dt_str = formatted
            datetime.fromisoformat(dt_str)
        except ValueError:
            raise ValueError(f"Invalid date format: {date_string}")

        # Return URL-encoded
        return quote(formatted, safe="")
    except Exception as error:
        print(
            f'[{datetime.now().isoformat()}] formatDateForApi: Error formatting date "{date_string}": {error}'
        )
        raise


def get_value(obj: Any, key: str) -> Any:
    """
    Extract value from object or plain object
    """
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def normalize_date_to_datetime(value: Any) -> datetime:
    """
    Normalize date values to datetime objects with UTC timezone.
    Returns a datetime object suitable for ClickHouse DateTime64 columns.
    """
    default_date = datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)

    if value is None:
        return default_date

    try:
        date: datetime | None = None

        # If value is a number (timestamp in milliseconds), use it directly
        if isinstance(value, (int, float)):
            # Check if it's milliseconds (typically > 1e10) or seconds
            timestamp = value / 1000 if value > 1e10 else value
            date = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        else:
            # Check if it's a numeric string (timestamp)
            try:
                num_value = float(value)
                # Check if it's a timestamp (milliseconds if > 1e10, seconds otherwise)
                timestamp = num_value / 1000 if num_value > 1e10 else num_value
                date = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            except (ValueError, TypeError):
                # Otherwise, treat it as a date string
                value_str = str(value).strip()
                # Try parsing ISO format or common date formats
                try:
                    # Handle ISO format with Z or timezone
                    if value_str.endswith("Z"):
                        value_str = value_str[:-1] + "+00:00"
                    date = datetime.fromisoformat(value_str)
                    # Convert to UTC if timezone-aware, otherwise assume UTC
                    if isinstance(date, datetime) and date.tzinfo is not None:
                        date = date.astimezone(timezone.utc)
                    elif isinstance(date, datetime):
                        # Assume UTC if no timezone info
                        date = date.replace(tzinfo=timezone.utc)
                except ValueError:
                    # Try other common formats including milliseconds
                    for fmt in [
                        "%Y-%m-%d %H:%M:%S.%f",  # With milliseconds
                        "%Y-%m-%d %H:%M:%S",  # Without milliseconds
                        "%Y-%m-%d",  # Date only
                        "%Y-%m-%dT%H:%M:%S.%f",  # ISO with milliseconds
                        "%Y-%m-%dT%H:%M:%S",  # ISO without milliseconds
                    ]:
                        try:
                            date = datetime.strptime(value_str, fmt).replace(
                                tzinfo=timezone.utc
                            )
                            break
                        except ValueError:
                            continue
                    else:
                        return default_date

        # Ensure date is a datetime object before returning
        if not isinstance(date, datetime):
            return default_date

        return date
    except Exception:
        # If any error occurs, return default
        return default_date


def normalize_date(value: Any) -> str:
    """
    Normalize date strings to ClickHouse DateTime64 format (YYYY-MM-DD HH:MM:SS.mmm)
    Uses UTC timezone to match TypeScript implementation.
    """
    date = normalize_date_to_datetime(value)

    # Format as YYYY-MM-DD HH:MM:SS.mmm for ClickHouse DateTime64 (UTC)
    year = date.year
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"
    hours = f"{date.hour:02d}"
    minutes = f"{date.minute:02d}"
    seconds = f"{date.second:02d}"
    milliseconds = f"{date.microsecond // 1000:03d}"

    return f"{year}-{month}-{day} {hours}:{minutes}:{seconds}.{milliseconds}"
