import asyncio
import pytest
import pytest_asyncio
import websockets
from client import ResilientWebSocketClient

@pytest.mark.asyncio
async def test_reconnect_message_ordering():
    received = []
    server = None

    async def handler(websocket):
        try:
            async for msg in websocket:
                received.append(msg)
        except websockets.exceptions.ConnectionClosed:
            pass

    server = await websockets.serve(handler, "localhost", 8790)
    client = ResilientWebSocketClient(f"ws://localhost:8790", initial_backoff=0.1, max_backoff=0.2)
    task = asyncio.create_task(client.connect())

    await asyncio.sleep(0.1)

    # Monkeypatch to simulate a drop during a send of message "2"
    real_ws_send = client._ws.send
    failed_once = False

    async def hooked_send(*args, **kwargs):
        nonlocal failed_once
        msg = args[0] if args else kwargs.get('message')
        if msg == "2" and not failed_once:
            failed_once = True
            raise websockets.exceptions.ConnectionClosed(None, None)
        return await real_ws_send(*args, **kwargs)

    client._ws.send = hooked_send

    await client.send("1")
    await client.send("2")
    await client.send("3")

    await asyncio.sleep(0.5) # Wait for processing and reconnect

    await client.stop()
    task.cancel()
    server.close()
    await server.wait_closed()

    assert received == ["1", "2", "3"]
