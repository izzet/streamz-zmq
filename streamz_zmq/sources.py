import asyncio
import zmq
import zmq.asyncio
from streamz import Stream
from streamz.sources import Source


@Stream.register_api(staticmethod)
class from_zmq(Source):
    """Accepts messages from a ZMQ socket.

    This source connects to a ZMQ publisher and receives messages,
    which are then emitted into the stream.

    Requires the ``pyzmq`` library.

    Parameters
    ----------
    connect_str: str
        The ZMQ connection string, e.g., "tcp://localhost:5555".
    sock_type: int, optional
        The ZMQ socket type, like zmq.SUB or zmq.PULL.
        Defaults to zmq.SUB.
    subscribe: bytes, optional
        If using a SUB socket, this is the subscription topic.
        Defaults to b'', which subscribes to all messages.
    """

    def __init__(self, connect_str, sock_type=None, subscribe=b"", **kwargs):
        self.connect_str = connect_str
        self.sock_type = sock_type or zmq.SUB
        self.subscribe = subscribe
        self.socket = None
        self.context = None
        super().__init__(**kwargs)

    async def run(self):
        """
        The main coroutine that sets up the ZMQ connection and
        polls for messages.
        """
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(self.sock_type)

        if self.sock_type == zmq.SUB:
            self.socket.setsockopt(zmq.SUBSCRIBE, self.subscribe)

        self.socket.connect(self.connect_str)

        while not self.stopped:
            try:
                msg = await self.socket.recv_multipart()
                if len(msg) == 1:
                    await asyncio.gather(*self._emit(msg[0]))
                else:
                    await asyncio.gather(*self._emit(msg))
            except zmq.error.ZMQError:
                if self.stopped:
                    break
                else:
                    raise

    def stop(self):
        """
        Stops the source by closing the socket and terminating the context.
        """
        super().stop()
        if self.socket:
            self.socket.close()
        if self.context:
            self.context.term()
        self.socket = None
        self.context = None
