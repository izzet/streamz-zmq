import asyncio
import zmq
import zmq.asyncio
from streamz import Stream
from streamz.sources import Source


@Stream.register_api(staticmethod)
class from_zmq(Source):
    """Accepts messages from a ZMQ socket.

    This source connects to a ZMQ socket and receives messages,
    which are then emitted into the stream.

    Requires the ``pyzmq`` library.

    Parameters
    ----------
    connect_str: str
        The ZMQ connection string to connect to.
        Format: "tcp://hostname:port"

        Note: Sources typically connect to existing publishers/services.
        Use to_zmq with bind=True to create the service side.

    sock_type: int, optional
        The ZMQ socket type for receiving data:

        - zmq.SUB (default): Subscribe to broadcast messages (pairs with PUB)
        - zmq.PULL: Receive work items (pairs with PUSH)
        - zmq.REQ: Send requests (pairs with REP)

    subscribe: bytes, optional
        Subscription topic for SUB sockets. Use b"" to receive all messages.
        Defaults to b'', which subscribes to all messages.

    Examples
    --------
    Subscribe to broadcast data:

    >>> source = Stream.from_zmq("tcp://dataserver:5555", sock_type=zmq.SUB)
    >>> source = Stream.from_zmq("tcp://feeds:8080")  # SUB is default

    Receive work items for processing:

    >>> source = Stream.from_zmq("tcp://workqueue:6666", sock_type=zmq.PULL)

    Pipeline pattern (receive from one service, send to another):

    >>> # Receive data, process it, send results
    >>> source = Stream.from_zmq("tcp://input:5555", sock_type=zmq.SUB)
    >>> processed = source.map(transform_data)
    >>> processed.to_zmq("tcp://output:6666", sock_type=zmq.PUSH)
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
