from app.schema import Holding
from app.utils import rate_limited_fetch

# from schema import Holding
# from utils import rate_limited_fetch
import httpx
from datetime import datetime
import asyncio


async def get_polymarket_holders(
    market_id: str,
    limit: int = 1000,
) -> list[Holding]:
    print(
        f"[{datetime.now().isoformat()}] getPolymarketHolders: Starting (marketId={market_id}, limit={limit})"
    )

    url = f"https://data-api.polymarket.com/holders?limit={limit}&market={market_id}"
    print(f"[{datetime.now().isoformat()}] getPolymarketHolders: Fetching from {url}")

    try:
        async with httpx.AsyncClient() as client:
            response = await rate_limited_fetch(url, client)
            if response.status_code != 200:
                print(
                    f"[{datetime.now().isoformat()}] getPolymarketHolders: HTTP error! status: {response.status_code}"
                )
                raise Exception(f"HTTP error! status: {response.status_code}")

            data = response.json()

            # The response is a list of token objects, each containing a "holders" array
            # We need to flatten this into a single list of holdings
            holdings: list[Holding] = []
            for token_data in data:
                print(token_data)
                if isinstance(token_data, dict) and "holders" in token_data:
                    for holder_data in token_data["holders"]:
                        holdings.append(Holding(**holder_data))

            print(
                f"[{datetime.now().isoformat()}] getPolymarketHolders: Successfully fetched {len(holdings)} holdings"
            )
            return holdings
    except Exception as error:
        print(
            f"[{datetime.now().isoformat()}] getPolymarketHolders: Error fetching holders: {error}"
        )
        raise
