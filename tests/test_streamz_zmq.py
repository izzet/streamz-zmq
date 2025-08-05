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


def test_from_zmq_bind_wrong_sock_type():
    """Test that from_zmq raises ValueError if bind=True and sock_type is not zmq.PULL."""
    with pytest.raises(ValueError) as excinfo:
        Stream.from_zmq("tcp://*:5555", sock_type=zmq.SUB, bind=True)
    assert "bind=True" in str(excinfo.value)


def test_from_zmq_invalid_sock_type_connect():
    """Test that from_zmq raises ValueError if bind=False and sock_type is not SUB or PULL."""
    with pytest.raises(ValueError) as excinfo:
        Stream.from_zmq("tcp://localhost:5555", sock_type=zmq.REQ, bind=False)
    assert "Configuration error" in str(excinfo.value)


def test_from_zmq_subscribe_wrong_sock_type():
    """Test that from_zmq raises ValueError if subscribe is set and sock_type is not zmq.SUB."""
    with pytest.raises(ValueError) as excinfo:
        Stream.from_zmq("tcp://localhost:5555", sock_type=zmq.PULL, subscribe=b"topic")
    assert "subscribe" in str(excinfo.value)


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


@pytest.mark.asyncio
async def test_zmq_pipeline_integration():
    """Test a complete pipeline: ZMQ → Stream1 → Processing → ZMQ → Stream2 → Results."""
    input_port = 5557
    intermediate_port = 5558
    test_numbers = [1, 2, 3, 4]
    final_results = []

    # Start publisher thread (publishes numbers)
    publisher_thread = threading.Thread(
        target=zmq_publisher_thread,
        args=(input_port, [str(n) for n in test_numbers], 0.15),
    )

    # === STREAM 1: Receive → Process → Send ===
    # Step 1: Receive from first ZMQ source
    stream1 = Stream.from_zmq(f"tcp://localhost:{input_port}", sock_type=zmq.SUB)

    # Step 2: Process the data (convert to int, square it, format as result)
    def process_message(msg):
        """Process incoming message: decode, convert to int, square it."""
        if isinstance(msg, bytes):
            msg = msg.decode("utf-8")
        elif isinstance(msg, list):
            msg = msg[0].decode("utf-8") if msg else "0"

        try:
            number = int(msg.strip())
            squared = number * number
            result = f"processed_{number}_squared_{squared}"
            print(f"Stream1 processed: {number} → {result}")  # Debug
            return result
        except (ValueError, AttributeError):
            return f"error_processing_{msg}"

    processed_stream = stream1.map(process_message)

    # Step 3: Send processed results via ZMQ (PUSH socket that binds)
    processed_stream.to_zmq(
        f"tcp://*:{intermediate_port}", sock_type=zmq.PUSH, bind=True
    )

    # === STREAM 2: Receive → Collect Results ===
    # Step 4: Second stream receives processed data (PULL socket that connects)
    stream2 = Stream.from_zmq(
        f"tcp://localhost:{intermediate_port}", sock_type=zmq.PULL
    )

    # Step 5: Collect final results
    def collect_result(msg):
        """Collect the final processed results."""
        if isinstance(msg, bytes):
            decoded = msg.decode("utf-8")
        elif isinstance(msg, list):
            decoded = msg[0].decode("utf-8") if msg else ""
        else:
            decoded = str(msg)

        print(f"Stream2 received: {decoded}")  # Debug
        final_results.append(decoded)

    stream2.sink(collect_result)

    # Start everything in the right order
    # 1. Start stream1 first (it will bind the intermediate socket)
    stream1.start()
    time.sleep(0.3)  # Give stream1 time to bind

    # 2. Start stream2 (it will connect to stream1's output)
    stream2.start()
    time.sleep(0.2)  # Give stream2 time to connect

    # 3. Start publisher last
    publisher_thread.start()

    # Wait for processing to complete
    await asyncio.sleep(3.0)

    # Stop both streams
    stream1.stop()
    stream2.stop()

    # Wait for publisher to finish
    publisher_thread.join(timeout=2.0)

    # Verify we got the expected results
    assert len(final_results) > 0, f"No results received: {final_results}"

    # Check that we got processed results for each input number
    expected_patterns = [
        "processed_1_squared_1",
        "processed_2_squared_4",
        "processed_3_squared_9",
        "processed_4_squared_16",
    ]

    print(f"Final results received: {final_results}")  # Debug output

    # Verify we got all expected processed messages
    matches = 0
    for expected in expected_patterns:
        if any(expected in result for result in final_results):
            matches += 1

    # We should get ALL 4 messages processed correctly
    assert matches == 4, (
        f"Expected all 4 processed messages, got {matches} matches from: {final_results}"
    )

    # Also verify we received exactly 4 results (no duplicates or extras)
    assert len(final_results) == 4, (
        f"Expected exactly 4 results, got {len(final_results)}: {final_results}"
    )

    # Verify all received messages have the expected format
    for result in final_results:
        assert "processed_" in result and "_squared_" in result, (
            f"Unexpected result format: {result}"
        )


def test_zmq_from_zmq_bind():
    """Test that from_zmq can bind and receive messages from a connecting publisher."""
    bind_port = 5560
    test_messages = ["alpha", "beta", "gamma", "delta"]
    received = []

    def publisher():
        ctx = zmq.Context()
        sock = ctx.socket(zmq.PUSH)
        # Publisher connects to the bound source
        sock.connect(f"tcp://localhost:{bind_port}")
        time.sleep(0.2)  # Give the source time to bind
        for msg in test_messages:
            sock.send_string(msg)
            time.sleep(0.05)
        sock.close()
        ctx.term()

    # Start the source stream (binds)
    source = Stream.from_zmq(f"tcp://*:{bind_port}", sock_type=zmq.PULL, bind=True)
    source.sink(received.append)
    source.start()

    # Start publisher thread
    pub_thread = threading.Thread(target=publisher)
    pub_thread.start()

    # Wait for messages
    asyncio.get_event_loop().run_until_complete(asyncio.sleep(1.0))
    source.stop()
    pub_thread.join(timeout=1.0)

    # Check all messages received
    received_strs = [
        msg if isinstance(msg, str) else msg.decode("utf-8") for msg in received
    ]
    assert len(received_strs) == len(test_messages), (
        f"Expected {len(test_messages)} messages, got {received_strs}"
    )
    for expected in test_messages:
        assert expected in received_strs, f"Missing message: {expected}"
