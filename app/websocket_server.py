"""
WebSocket server for streaming trade data.
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import json
import asyncio
from typing import Set, Dict, Any, List
from app.trade_service import TradeService
from app.db import (
    get_trades_by_slug,
    get_trader_classes_from_db_map,
    get_pro_trader_trade_summary_by_market_id,
)
from app.api import get_polymarket_holders
from app.schema import TraderClass
from firebase import get_user_config

app = FastAPI(title="Polymarket Trades WebSocket Server")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://localhost:3000",
        # Add other origins as needed
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store active WebSocket connections
active_connections: Set[WebSocket] = set()

# Optional per-connection filters (e.g. by user id)
connection_filters: Dict[WebSocket, Dict[str, Any]] = {}

# Trade service instance
trade_service: TradeService = None


def broadcast_trade(trade_data: Dict[str, Any]):
    """
    Broadcast trade data to all connected WebSocket clients.
    This is called by the trade service when new trades are processed.

    Args:
        trade_data: Dictionary containing trade data
    """
    if active_connections:
        # Schedule the broadcast in the event loop
        try:
            loop = asyncio.get_running_loop()
            # If we're in an async context, schedule the task
            asyncio.create_task(broadcast_to_all(trade_data))
        except RuntimeError:
            # No running event loop, try to get one
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(broadcast_to_all(trade_data))
                else:
                    loop.run_until_complete(broadcast_to_all(trade_data))
            except RuntimeError:
                # Fallback: create new event loop if needed
                asyncio.run(broadcast_to_all(trade_data))


async def broadcast_to_all(trade_data: Dict[str, Any]):
    """
    Broadcast trade data to all active WebSocket connections.

    Args:
        trade_data: Dictionary containing trade data
    """
    if not active_connections:
        return

    message = json.dumps(trade_data)
    disconnected = set()

    for connection in active_connections:
        try:
            # Apply optional per-connection filters
            filters = connection_filters.get(connection)
            user_config = filters.get("user_config")
            conditionId = trade_data.get("conditionId")
            summary = get_pro_trader_trade_summary_by_market_id(conditionId)
            print(user_config, conditionId, summary)
            if user_config:
                message = json.dumps({"type": "pro_trader_trade_summary", "summary": summary})

            await connection.send_text(message)
        except Exception as e:
            print(f"Error sending to WebSocket client: {e}")
            disconnected.add(connection)

    # Remove disconnected clients
    active_connections.difference_update(disconnected)


@app.get("/")
async def get_root():
    """Root endpoint with simple HTML page for testing WebSocket."""
    html = """
    <!DOCTYPE html>
    <html>
        <head>
            <title>Polymarket Trades WebSocket</title>
        </head>
        <body>
            <h1>Polymarket Trades WebSocket</h1>
            <p>Connect to <code>ws://localhost:8000/trades</code> to receive trade data.</p>
            <div id="messages"></div>
            <script>
                const ws = new WebSocket("ws://localhost:8000/trades");
                const messages = document.getElementById("messages");
                
                ws.onmessage = function(event) {
                    const trade = JSON.parse(event.data);
                    const div = document.createElement("div");
                    div.textContent = `Trade: ${trade.transactionHash} - ${trade.asset} - Price: ${trade.price}`;
                    messages.appendChild(div);
                };
                
                ws.onerror = function(error) {
                    console.error("WebSocket error:", error);
                };
                
                ws.onopen = function() {
                    console.log("WebSocket connected");
                };
            </script>
        </body>
    </html>
    """
    return HTMLResponse(content=html)


@app.get("/trades")
async def get_trades(
    slug: str = Query(..., description="The slug to filter trades by")
):
    """
    Get trades from polymarket.current_trades table filtered by slug.

    Args:
        slug: The slug to filter trades by

    Returns:
        List of trades matching the slug
    """
    trades = get_trades_by_slug(slug)
    return {"slug": slug, "count": len(trades), "trades": trades}


@app.get("/holders")
async def get_holders(
    marketId: str = Query(..., description="The marketId to filter holders by")
):
    """
    Get holders from polymarket.current_trades table filtered by marketId.
    """
    holders = await get_polymarket_holders(marketId)
    # get trader classes from db map
    trader_classes = get_trader_classes_from_db_map(
        [holder.proxy_wallet for holder in holders]
    )
    result = []
    for holder in holders:
        trader_class = trader_classes.get(
            holder.proxy_wallet.lower(),
            TraderClass(
                proxyWallet=holder.proxy_wallet,
                global_roi_pct=0.0,
                consistency_rating=0.0,
                recency_weighted_pnl=0.0,
                trader_class="",
                calculated_at="",
            ),
        )
        result.append(
            {
                **holder.model_dump(),
                **trader_class.model_dump(exclude={"proxyWallet": True}),
            }
        )
    return {"marketId": marketId, "count": len(result), "holders": result}


@app.websocket("/trades")
async def websocket_trades_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for streaming trade data.

    Clients connecting to this endpoint will receive real-time trade data
    as it is processed from the Redis stream.
    """
    await websocket.accept()
    active_connections.add(websocket)
    # No filters for this connection – receives all trades
    connection_filters[websocket] = {}
    print(f"WebSocket client connected. Total connections: {len(active_connections)}")

    try:
        # Send welcome message
        await websocket.send_json(
            {"type": "connected", "message": "Connected to trades stream"}
        )

        # Keep connection alive and handle incoming messages (if any)
        while True:
            try:
                # Wait for any message from client (ping/pong or close)
                data = await websocket.receive_text()
                # Echo back or handle client messages if needed
                if data == "ping":
                    await websocket.send_json({"type": "pong"})
            except WebSocketDisconnect:
                break
    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        active_connections.discard(websocket)
        connection_filters.pop(websocket, None)
        print(
            f"WebSocket client disconnected. Total connections: {len(active_connections)}"
        )


@app.websocket("/user_trades/{user_id}")
async def websocket_user_trades_endpoint(websocket: WebSocket, user_id: str):
    """
    WebSocket endpoint for streaming trade data for a specific user.

    The user id is taken as a path parameter and is matched against the
    `connection_id` field on incoming trade packets. The payload format is
    identical to the existing `/trades` websocket.
    """
    await websocket.accept()
    active_connections.add(websocket)
    # Store filter for this connection so broadcast only sends matching trades
    user_config = get_user_config(user_id)
    connection_filters[websocket] = {"user_config": user_config}
    print(
        f"User-specific WebSocket client connected (user_id={user_id}). "
        f"Total connections: {len(active_connections)}"
    )
    try:
        # Send welcome message
        await websocket.send_json(
            {
                "type": "connected",
                "message": "Connected to user trades stream",
                "user_id": user_id,
                "user_config": user_config,
            }
        )

        # Keep connection alive and handle incoming messages (if any)
        while True:
            try:
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_json({"type": "pong", "user_id": user_id})
            except WebSocketDisconnect:
                break
    except Exception as e:
        print(f"User trades WebSocket error (user_id={user_id}): {e}")
    finally:
        active_connections.discard(websocket)
        connection_filters.pop(websocket, None)
        print(
            f"User-specific WebSocket client disconnected (user_id={user_id}). "
            f"Total connections: {len(active_connections)}"
        )


@app.on_event("startup")
async def startup_event():
    """Initialize trade service on startup."""
    global trade_service
    print("Starting WebSocket server and trade service...")
    trade_service = TradeService(broadcast_callback=broadcast_trade)
    # Start trade service in background
    asyncio.create_task(trade_service.start())


@app.on_event("shutdown")
async def shutdown_event():
    """Stop trade service on shutdown."""
    global trade_service
    if trade_service:
        trade_service.stop()
    print("WebSocket server and trade service stopped")
