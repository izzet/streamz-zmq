"""
Simple ZMQ + streamz example

This demonstrates:
1. A ZMQ publisher thread that sends messages
2. A streamz subscriber that receives and processes them

Just run: python simple_example.py
"""

import asyncio
import threading
import time
import zmq
from streamz import Stream
import streamz_zmq  # noqa: F401  # This registers the ZMQ extensions


def zmq_publisher_thread(port, messages, delay=0.3):
    """ZMQ publisher running in a separate thread."""
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind(f"tcp://*:{port}")

    # Give subscriber time to connect
    time.sleep(0.5)

    print("📡 ZMQ Publisher starting...")

    try:
        for i, msg in enumerate(messages):
            full_msg = f"Message {i}: {msg}"
            socket.send(full_msg.encode("utf-8"))
            print(f"  📤 Sent: {full_msg}")
            time.sleep(delay)
    finally:
        socket.close()
        context.term()
        print("📡 ZMQ Publisher finished!")


async def main():
    """Main function demonstrating ZMQ + streamz integration."""
    print("🚀 Simple ZMQ + Streamz Example\n")

    port = 5555
    messages = ["Hello", "from", "ZMQ", "to", "streamz!"]
    received_messages = []

    # Set up streamz subscriber
    print("🔧 Setting up streamz subscriber...")
    source = Stream.from_zmq(f"tcp://localhost:{port}", sock_type=zmq.SUB)

    # Process received messages
    def process_message(msg):
        decoded = msg.decode("utf-8") if isinstance(msg, bytes) else str(msg)
        received_messages.append(decoded)
        print(f"  📥 Streamz received: {decoded}")

    source.sink(process_message)

    # Start streamz subscriber
    print("📻 Starting streamz subscriber...")
    source.start()

    # Start ZMQ publisher in background thread
    print("📡 Starting ZMQ publisher thread...")
    publisher_thread = threading.Thread(
        target=zmq_publisher_thread, args=(port, messages, 0.4)
    )
    publisher_thread.start()

    # Wait for everything to complete
    await asyncio.sleep(3.0)

    # Clean up
    source.stop()
    publisher_thread.join(timeout=1.0)

    print("\n✅ Example completed!")
    print(f"📊 Messages sent: {len(messages)}")
    print(f"📊 Messages received: {len(received_messages)}")

    if received_messages:
        print("\n📋 All received messages:")
        for msg in received_messages:
            print(f"    - {msg}")


if __name__ == "__main__":
    asyncio.run(main())
