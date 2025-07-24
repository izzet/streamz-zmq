import pytest
import asyncio
import threading
import time
import zmq
import zmq.asyncio
from streamz import Stream
import streamz_zmq  # noqa: F401  # This import registers the ZMQ extensions


def test_imports():
    """Test that the package imports correctly."""
    from streamz_zmq import from_zmq, to_zmq, __version__

    assert from_zmq is not None
    assert to_zmq is not None
    assert __version__ is not None
    assert isinstance(__version__, str)
    # In development, version should be either actual version or dev version
    assert "." in __version__ or "+dev" in __version__


def test_stream_registration():
    """Test that the ZMQ methods are registered with Stream."""
    # Verify the methods are available on Stream class
    assert hasattr(Stream, "from_zmq")
    assert hasattr(Stream, "to_zmq")


@pytest.mark.asyncio
async def test_zmq_sink_basic():
    """Test basic functionality of to_zmq sink."""
    # This is a minimal test - in practice you'd want to test with actual ZMQ sockets
    stream = Stream.from_iterable([b"test1", b"test2", b"test3"])

    # Create sink (won't actually connect in this test)
    sink = stream.to_zmq("tcp://localhost:5555")

    # Just verify the sink was created
    assert sink is not None


def zmq_publisher_thread(port, messages, delay=0.1):
    """Thread function that publishes messages via ZMQ."""
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind(f"tcp://*:{port}")

    # Give subscriber time to connect
    time.sleep(0.2)

    try:
        for msg in messages:
            if isinstance(msg, str):
                msg = msg.encode("utf-8")
            socket.send(msg)
            time.sleep(delay)
    finally:
        socket.close()
        context.term()


@pytest.mark.asyncio
async def test_zmq_integration():
    """Test actual ZMQ communication between publisher and subscriber."""
    port = 5556  # Use a different port to avoid conflicts
    test_messages = ["Hello", "World", "From", "ZMQ"]
    received_messages = []

    # Start publisher in a separate thread
    publisher_thread = threading.Thread(
        target=zmq_publisher_thread,
        args=(port, test_messages, 0.05),  # Faster for testing
    )

    # Create subscriber stream
    source = Stream.from_zmq(f"tcp://localhost:{port}", sock_type=zmq.SUB)

    # Collect received messages
    def collect_message(msg):
        # Handle both single bytes and multipart messages
        if isinstance(msg, list):
            decoded = [
                part.decode("utf-8") if isinstance(part, bytes) else str(part)
                for part in msg
            ]
        else:
            decoded = msg.decode("utf-8") if isinstance(msg, bytes) else str(msg)
        received_messages.append(decoded)

    source.sink(collect_message)

    # Start publisher and subscriber
    publisher_thread.start()

    # Start the stream (this is not async, it starts the event loop)
    source.start()

    # Wait for messages to be processed
    await asyncio.sleep(1.0)

    # Stop the stream
    source.stop()

    # Wait for publisher to finish
    publisher_thread.join(timeout=1.0)

    # Verify we received some messages
    assert len(received_messages) > 0, (
        f"Expected to receive messages, but got: {received_messages}"
    )

    # Check that we received the expected messages (order might vary due to async nature)
    received_strings = [
        msg if isinstance(msg, str) else str(msg) for msg in received_messages
    ]
    for expected_msg in test_messages:
        assert any(expected_msg in received for received in received_strings), (
            f"Expected '{expected_msg}' in received messages: {received_strings}"
        )
