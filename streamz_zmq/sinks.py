import zmq
import zmq.asyncio
from streamz import Stream
from streamz.sinks import Sink


@Stream.register_api()
class to_zmq(Sink):
    """Sends elements from the stream to a ZMQ socket.

    This sink creates a ZMQ socket on the first element and sends each
    subsequent element as a multipart message.

    Requires the ``pyzmq`` library.

    Parameters
    ----------
    connect_str: str
        The ZMQ connection string.

        For connect mode (default): "tcp://hostname:port"
        For bind mode: "tcp://*:port" or "tcp://interface:port"

    sock_type: int, optional
        The ZMQ socket type. For sending data, zmq.PUSH or zmq.PUB
        are common choices. Defaults to zmq.PUSH.

    bind: bool, optional
        Socket connection mode:

        - False (default): Connect to existing service
          Use when sending data TO an established service/collector.
          Examples: sending logs to log server, metrics to monitoring system.

        - True: Create new service that others connect to
          Use when the stream acts as a data source/service.
          Examples: real-time feeds, processed data services.

    Examples
    --------
    Send data to existing service (most common):

    >>> stream.to_zmq("tcp://logserver:514")
    >>> stream.to_zmq("tcp://collector:5555", sock_type=zmq.PUSH)

    Create data service for others to consume:

    >>> stream.to_zmq("tcp://*:8080", sock_type=zmq.PUB, bind=True)
    >>> stream.to_zmq("tcp://*:9999", sock_type=zmq.PUSH, bind=True)
    """

    def __init__(self, upstream, connect_str, sock_type=zmq.PUSH, bind=False, **kwargs):
        self.connect_str = connect_str
        self.sock_type = sock_type
        self.bind = bind
        self.context = None
        self.socket = None

        # ensure_io_loop=True is important for network sinks
        super().__init__(upstream, ensure_io_loop=True, **kwargs)

    async def update(self, x, who=None, metadata=None):
        """
        Connect or bind the socket if needed, then send the data.
        """
        # 1. Lazily create the context and socket on the first message
        if self.socket is None:
            self.context = zmq.asyncio.Context()
            self.socket = self.context.socket(self.sock_type)
            if self.bind:
                self.socket.bind(self.connect_str)
            else:
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
