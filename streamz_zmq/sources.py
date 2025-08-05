import asyncio
import zmq
import zmq.asyncio
from streamz import Stream
from streamz.sources import Source


@Stream.register_api(staticmethod)
class from_zmq(Source):
    """Accepts messages from a ZMQ socket.

    This source can either connect to an existing ZMQ service (client mode)
    or bind to an address to create a new one (server mode).

    When binding (bind=True), the socket type defaults to zmq.PULL to create a
    reliable data ingestion service.

    Parameters
    ----------
    address: str
        The ZMQ connection string (e.g., "tcp://hostname:port").
    sock_type: int, optional
        ZMQ socket type.
        - If bind=False, defaults to zmq.SUB.
        - If bind=True, defaults to zmq.PULL.
    bind: bool, optional
        - False (default): Connect to an existing service (client mode).
        - True: Bind to the address to create a service (server mode).
    subscribe: bytes, optional
        For SUB sockets, the topic to subscribe to. Defaults to b"" (all).

    Examples
    --------
    Subscribe to broadcast data:

    >>> source = Stream.from_zmq("tcp://dataserver:5555", sock_type=zmq.SUB)
    >>> source = Stream.from_zmq("tcp://feeds:8080")  # SUB is default

    Receive work items for processing:

    >>> source = Stream.from_zmq("tcp://workqueue:6666", sock_type=zmq.PULL)

    Bind to a port and accept connections from publishers:

    >>> source = Stream.from_zmq("tcp://*:5555", bind=True)  # PULL is default for bind

    Pipeline pattern (receive from one service, send to another):

    >>> # Receive data, process it, send results
    >>> source = Stream.from_zmq("tcp://input:5555", sock_type=zmq.SUB)
    >>> processed = source.map(transform_data)
    >>> processed.to_zmq("tcp://output:6666", sock_type=zmq.PUSH)
    """

    def __init__(self, address, sock_type=None, subscribe=b"", bind=False, **kwargs):
        self.address = address
        self.subscribe = subscribe
        self.bind = bind
        self.socket = None
        self.context = None

        # 1. Determine the default socket type based on the 'bind' flag.
        if self.bind:
            default_sock_type = zmq.PULL
        else:
            default_sock_type = zmq.SUB

        # 2. Use the user-provided sock_type if it exists, otherwise use our smart default.
        self.sock_type = sock_type if sock_type is not None else default_sock_type

        # 3. Validate the final configuration.
        if self.bind:
            # If binding, the socket type MUST be PULL. No exceptions.
            if self.sock_type != zmq.PULL:
                raise ValueError(
                    "Configuration error: When bind=True, the socket type MUST be zmq.PULL. "
                    "You are trying to override it with an incompatible type. "
                    "Please remove the `sock_type` parameter to use the correct default."
                )
        elif self.sock_type not in [zmq.SUB, zmq.PULL]:
            # If connecting, only SUB and PULL are supported for this source.
            raise ValueError(
                f"Configuration error: When bind=False, sock_type must be zmq.SUB or zmq.PULL. "
                f"You provided {self.sock_type}."
            )

        # 4. Validate subscribe usage: only allowed for SUB sockets
        if self.subscribe != b"" and self.sock_type != zmq.SUB:
            raise ValueError(
                "Configuration error: The 'subscribe' parameter is only valid for zmq.SUB sockets. "
                f"You provided subscribe={self.subscribe!r} with sock_type={self.sock_type}."
            )

        super().__init__(**kwargs)

    async def run(self):
        """
        The main coroutine that sets up the ZMQ connection or binding and
        polls for messages.
        """
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(self.sock_type)

        if self.sock_type == zmq.SUB:
            self.socket.setsockopt(zmq.SUBSCRIBE, self.subscribe)

        if self.bind:
            self.socket.bind(self.address)
        else:
            self.socket.connect(self.address)

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
