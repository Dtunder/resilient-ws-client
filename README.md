# resilient-ws-client

Resilient asyncio WebSocket client with queueing and auto-reconnect.

## Features

- **Auto-Reconnect**: Automatically attempts to reconnect when the connection drops, using an exponential backoff strategy (configurable initial and maximum backoff).
- **Message Queueing**: Messages are stored in an `asyncio.Queue` before sending, ensuring that no messages are lost while disconnected. Failed sends are automatically re-queued.
- **Callbacks**: State hooks are provided for connection (`on_connect`), disconnection (`on_disconnect`), and received messages (`on_message`).
- **Asynchronous**: Built on top of Python's standard `asyncio` and the popular `websockets` library.

## Installation

You need Python 3.7+ and the `websockets` package. Install requirements:

```bash
pip install -r requirements.txt
```

## Usage Example

`client.py` contains the `ResilientWebSocketClient` class.

`main.py` provides an example showing the client interacting with a mock server that occasionally drops connections to demonstrate resilience.

```python
import asyncio
from client import ResilientWebSocketClient

async def on_connect():
    print("Connected!")

async def on_disconnect():
    print("Disconnected!")

async def on_message(message):
    print(f"Received: {message}")

async def main():
    client = ResilientWebSocketClient(
        url="ws://localhost:8765",
        initial_backoff=2.0,
        max_backoff=60.0,
        on_connect=on_connect,
        on_disconnect=on_disconnect,
        on_message=on_message
    )

    # Start connection in background
    client_task = asyncio.create_task(client.connect())

    # Send messages
    await client.send("Hello World!")

    # ...
    # Stop client later
    await client.stop()
    await client_task

if __name__ == "__main__":
    asyncio.run(main())
```

Run the example with the mock server:
```bash
python main.py
```
