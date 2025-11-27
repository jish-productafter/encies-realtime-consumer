"""
Main entry point for the trade consumer application.
Runs the WebSocket server with integrated trade processing service.
"""

import uvicorn
from app.websocket_server import app

if __name__ == "__main__":
    # Run the FastAPI application with Uvicorn
    # The trade service will start automatically via the startup event
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
    )
