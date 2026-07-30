import asyncio
import collections
import logging
import websockets
from typing import Callable, Optional, Awaitable

logger = logging.getLogger(__name__)

class ResilientWebSocketClient:
    def __init__(
        self,
        url: str,
        initial_backoff: float = 2.0,
        max_backoff: float = 60.0,
        on_connect: Optional[Callable[[], Awaitable[None]]] = None,
        on_disconnect: Optional[Callable[[], Awaitable[None]]] = None,
        on_message: Optional[Callable[[str], Awaitable[None]]] = None,
    ):
        self.url = url
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect
        self.on_message = on_message

        self._queue = asyncio.Queue()
        self._failed_messages = collections.deque()
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._connected = asyncio.Event()
        self._stop_event = asyncio.Event()

    async def connect(self):
        backoff = self.initial_backoff
        
        while not self._stop_event.is_set():
            try:
                logger.info(f"Attempting to connect to {self.url}...")
                async with websockets.connect(self.url) as ws:
                    logger.info(f"Connected to {self.url}")
                    self._ws = ws
                    self._connected.set()
                    
                    if self.on_connect:
                        await self.on_connect()
                        
                    # Reset backoff on successful connection
                    backoff = self.initial_backoff
                    
                    # Create tasks for sending and receiving
                    receive_task = asyncio.create_task(self._receive_loop())
                    send_task = asyncio.create_task(self._send_loop())
                    
                    # Wait for either task to finish (e.g. connection close)
                    done, pending = await asyncio.wait(
                        [receive_task, send_task],
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    for task in pending:
                        task.cancel()
                        
            except (websockets.exceptions.ConnectionClosed, Exception) as e:
                logger.warning(f"Connection failed or lost: {e}")
            finally:
                self._connected.clear()
                self._ws = None
                if self.on_disconnect:
                    await self.on_disconnect()
                
            if not self._stop_event.is_set():
                logger.info(f"Reconnecting in {backoff} seconds...")
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, self.max_backoff)

    async def _receive_loop(self):
        try:
            async for message in self._ws:
                if self.on_message:
                    await self.on_message(message)
        except websockets.exceptions.ConnectionClosed:
            logger.info("WebSocket connection closed in receive loop")

    async def _send_loop(self):
        while True:
            try:
                # Wait until connected
                await self._connected.wait()

                # Check if there is a failed message to retry first
                if self._failed_messages:
                    message = self._failed_messages.popleft()
                else:
                    # Get message from queue
                    message = await self._queue.get()
            except asyncio.CancelledError:
                break
                
            try:
                await self._ws.send(message)
                self._queue.task_done()
            except websockets.exceptions.ConnectionClosed:
                logger.warning("Failed to send message, holding in failed_messages")
                self._failed_messages.appendleft(message)
                break
            except asyncio.CancelledError:
                logger.warning("Send cancelled, holding in failed_messages")
                self._failed_messages.appendleft(message)
                break
            except Exception as e:
                logger.error(f"Error sending message: {e}")
                self._failed_messages.appendleft(message)
                break

    async def send(self, message: str):
        """Enqueue a message to be sent."""
        await self._queue.put(message)

    async def stop(self):
        """Stop the client completely."""
        self._stop_event.set()
        if self._ws:
            await self._ws.close()

