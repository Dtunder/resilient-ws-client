import asyncio
import logging
import websockets
from client import ResilientWebSocketClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def mock_server_handler(websocket):
    """A mock server that drops connection occasionally."""
    client_address = websocket.remote_address
    logger.info(f"Server: Client connected from {client_address}")
    
    # Send a welcome message
    await websocket.send("Welcome to the mock server!")
    
    try:
        # Loop to handle incoming messages
        while True:
            # We use wait_for to be able to drop connection occasionally
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                logger.info(f"Server: Received message: {message}")
                await websocket.send(f"Echo: {message}")
            except asyncio.TimeoutError:
                # Every 5 seconds of inactivity, randomly drop the connection to test resilience
                # Actually, let's just close the connection abruptly after receiving 3 messages
                pass
            except Exception as e:
                break
    except websockets.exceptions.ConnectionClosed:
        logger.info("Server: Connection closed by client.")
    finally:
        logger.info("Server: Connection ended.")

async def mock_server_handler_abrupt(websocket):
    client_address = websocket.remote_address
    logger.info(f"Server: Client connected from {client_address}")
    await websocket.send("Welcome to the mock server!")
    
    msg_count = 0
    try:
        async for message in websocket:
            logger.info(f"Server: Received message: {message}")
            await websocket.send(f"Echo: {message}")
            msg_count += 1
            if msg_count >= 3:
                logger.info("Server: Dropping connection to test client resilience...")
                break # breaking out of loop will close connection
    except websockets.exceptions.ConnectionClosed:
        logger.info("Server: Connection closed by client.")
    finally:
        logger.info("Server: Connection ended.")

async def start_mock_server():
    logger.info("Starting mock server on ws://localhost:8765")
    async with websockets.serve(mock_server_handler_abrupt, "localhost", 8765):
        await asyncio.Future()  # run forever

async def run_client():
    async def on_connect():
        logger.info("Client callback: Connected!")

    async def on_disconnect():
        logger.info("Client callback: Disconnected!")

    async def on_message(message):
        logger.info(f"Client callback: Received message: {message}")

    client = ResilientWebSocketClient(
        url="ws://localhost:8765",
        initial_backoff=2.0,
        max_backoff=10.0,  # Smaller for testing
        on_connect=on_connect,
        on_disconnect=on_disconnect,
        on_message=on_message
    )

    # Start the client connection in the background
    client_task = asyncio.create_task(client.connect())

    # Simulate sending messages
    try:
        count = 1
        while True:
            msg = f"Message {count}"
            logger.info(f"App: Queueing {msg}")
            await client.send(msg)
            count += 1
            await asyncio.sleep(2.0)
    except asyncio.CancelledError:
        pass
    finally:
        await client.stop()
        await client_task

async def main():
    # Start server and client concurrently
    server_task = asyncio.create_task(start_mock_server())
    
    # Wait a moment for server to start
    await asyncio.sleep(1)
    
    client_task = asyncio.create_task(run_client())
    
    # Let it run for 20 seconds to see reconnects, then exit
    await asyncio.sleep(20)
    
    logger.info("Shutting down...")
    client_task.cancel()
    server_task.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
