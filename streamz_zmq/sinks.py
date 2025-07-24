import zmq
import zmq.asyncio
from streamz import Stream
from streamz.sinks import Sink


@Stream.register_api()
class to_zmq(Sink):
    """Sends elements from the stream to a ZMQ socket.

    This sink connects a ZMQ socket on the first element and sends each
    subsequent element as a multipart message.

    Requires the ``pyzmq`` library.

    Parameters
    ----------
    connect_str: str
        The ZMQ connection string, e.g., "tcp://localhost:5555".
        The sink will connect to this address.
    sock_type: int, optional
        The ZMQ socket type. For sending data, zmq.PUSH or zmq.PUB
        are common choices. Defaults to zmq.PUSH.
    """

    def __init__(self, upstream, connect_str, sock_type=zmq.PUSH, **kwargs):
        self.connect_str = connect_str
        self.sock_type = sock_type
        self.context = None
        self.socket = None

        # ensure_io_loop=True is important for network sinks
        super().__init__(upstream, ensure_io_loop=True, **kwargs)

    async def update(self, x, who=None, metadata=None):
        """
        Connect the socket if needed, then send the data.
        """
        # 1. Lazily create the context and socket on the first message
        if self.socket is None:
            self.context = zmq.asyncio.Context()
            self.socket = self.context.socket(self.sock_type)
            self.socket.connect(self.connect_str)

        # 2. Prepare the message for send_multipart (expects a list of bytes)
        if not isinstance(x, (list, tuple)):
            msg_parts = [x]
        else:
            msg_parts = x

        # Ensure all parts are bytes before sending
        encoded_parts = [
            part if isinstance(part, bytes) else str(part).encode("utf-8")
            for part in msg_parts
        ]

        # 3. Send the message asynchronously
        await self.socket.send_multipart(encoded_parts)

    def destroy(self):
        """
        Clean up the ZMQ socket and context.
        """
        super().destroy()
        if self.socket:
            self.socket.close()
        if self.context:
            self.context.term()
        self.socket = None
        self.context = None
