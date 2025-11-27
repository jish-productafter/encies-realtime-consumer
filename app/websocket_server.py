"""
WebSocket server for streaming trade data.
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import json
import asyncio
from typing import Set, Dict, Any
from app.trade_service import TradeService

app = FastAPI(title="Polymarket Trades WebSocket Server")

# Store active WebSocket connections
active_connections: Set[WebSocket] = set()

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


@app.websocket("/trades")
async def websocket_trades_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for streaming trade data.

    Clients connecting to this endpoint will receive real-time trade data
    as it is processed from the Redis stream.
    """
    await websocket.accept()
    active_connections.add(websocket)
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
        print(
            f"WebSocket client disconnected. Total connections: {len(active_connections)}"
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
