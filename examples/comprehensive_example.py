"""
Comprehensive ZMQ examples showing different messaging patterns with streamz.

This demonstrates:
1. PUB/SUB pattern for broadcasting
2. PUSH/PULL pattern for load balancing
3. Processing pipeline
"""

import asyncio
import threading
import time
import zmq
from streamz import Stream
import streamz_zmq  # noqa: F401


def zmq_publisher_worker(port, messages, delay=0.1, sock_type=zmq.PUB, topic_prefix=""):
    """Worker function that publishes messages via ZMQ."""
    context = zmq.Context()
    socket = context.socket(sock_type)
    socket.bind(f"tcp://*:{port}")

    # Give subscribers time to connect
    time.sleep(0.3)

    print(f"Publisher on port {port} starting to send {len(messages)} messages...")

    try:
        for msg in messages:
            full_msg = f"{topic_prefix}{msg}" if topic_prefix else str(msg)
            if isinstance(full_msg, str):
                full_msg = full_msg.encode("utf-8")
            socket.send(full_msg)
            print(f"  Port {port} sent: {full_msg}")
            time.sleep(delay)
    finally:
        socket.close()
        context.term()
        print(f"Publisher on port {port} finished!")


async def pub_sub_example():
    """Demonstrate PUB/SUB pattern - one publisher, multiple subscribers."""
    print("=== PUB/SUB Pattern Example ===")

    port = 5557
    weather_data = [
        "NYC 25 sunny",
        "LA 30 cloudy",
        "CHI 15 snow",
        "NYC 26 sunny",
        "LA 28 rain",
    ]

    # Start publisher in background thread
    publisher_thread = threading.Thread(
        target=zmq_publisher_worker, args=(port, weather_data, 0.2, zmq.PUB, "WEATHER ")
    )

    # Subscriber: Listen for NYC weather only
    nyc_weather = []
    nyc_subscriber = Stream.from_zmq(
        f"tcp://localhost:{port}", sock_type=zmq.SUB, subscribe=b"WEATHER NYC"
    )
    (
        nyc_subscriber.map(
            lambda x: x.decode("utf-8") if isinstance(x, bytes) else str(x)
        ).sink(lambda x: nyc_weather.append(x))
    )

    # Subscriber: Listen for all weather
    all_weather = []
    all_subscriber = Stream.from_zmq(
        f"tcp://localhost:{port}", sock_type=zmq.SUB, subscribe=b"WEATHER"
    )
    (
        all_subscriber.map(
            lambda x: x.decode("utf-8") if isinstance(x, bytes) else str(x)
        ).sink(lambda x: all_weather.append(x))
    )

    # Start subscribers first
    nyc_subscriber.start()
    all_subscriber.start()

    # Start publisher
    publisher_thread.start()

    # Wait for completion
    await asyncio.sleep(2.0)

    # Stop everything
    nyc_subscriber.stop()
    all_subscriber.stop()
    publisher_thread.join(timeout=1.0)

    print(f"NYC weather updates: {nyc_weather}")
    print(f"All weather updates: {all_weather}")
    print()


async def push_pull_example():
    """Demonstrate PUSH/PULL pattern - load balancing work."""
    print("=== PUSH/PULL Pattern Example ===")

    port = 5558
    tasks = [f"task_{i}" for i in range(6)]

    # Start producer in background thread
    producer_thread = threading.Thread(
        target=zmq_publisher_worker, args=(port, tasks, 0.15, zmq.PUSH, "WORK ")
    )

    # Worker 1
    worker1_results = []
    worker1 = Stream.from_zmq(f"tcp://localhost:{port}", sock_type=zmq.PULL)
    (
        worker1.map(lambda x: x.decode("utf-8") if isinstance(x, bytes) else str(x))
        .map(lambda x: f"Worker1 processed: {x}")
        .sink(lambda x: worker1_results.append(x))
    )

    # Worker 2
    worker2_results = []
    worker2 = Stream.from_zmq(f"tcp://localhost:{port}", sock_type=zmq.PULL)
    (
        worker2.map(lambda x: x.decode("utf-8") if isinstance(x, bytes) else str(x))
        .map(lambda x: f"Worker2 processed: {x}")
        .sink(lambda x: worker2_results.append(x))
    )

    # Start workers first
    worker1.start()
    worker2.start()

    # Start producer
    producer_thread.start()

    # Wait for completion
    await asyncio.sleep(2.0)

    # Stop everything
    worker1.stop()
    worker2.stop()
    producer_thread.join(timeout=1.0)

    print(f"Worker 1 results: {worker1_results}")
    print(f"Worker 2 results: {worker2_results}")
    print(f"Total processed: {len(worker1_results) + len(worker2_results)}")
    print()


async def pipeline_example():
    """Demonstrate a processing pipeline using ZMQ."""
    print("=== Processing Pipeline Example ===")

    # Use different ports for each stage
    stage1_port = 5559
    stage2_port = 5560

    raw_data = [1, 2, 3, 4, 5]

    # Stage 1 producer thread
    stage1_thread = threading.Thread(
        target=zmq_publisher_worker, args=(stage1_port, raw_data, 0.2, zmq.PUSH)
    )

    # Stage 2: Receive from stage 1, square numbers, send to stage 3
    stage2_results = []

    def stage2_processor():
        """Stage 2 processor that squares numbers."""
        context = zmq.Context()

        # Input socket
        input_socket = context.socket(zmq.PULL)
        input_socket.connect(f"tcp://localhost:{stage1_port}")

        # Output socket
        output_socket = context.socket(zmq.PUSH)
        output_socket.bind(f"tcp://*:{stage2_port}")

        time.sleep(0.2)  # Let stage 3 connect

        try:
            for _ in range(len(raw_data)):
                msg = input_socket.recv()
                if isinstance(msg, bytes):
                    num = int(msg.decode("utf-8"))
                else:
                    num = int(msg)

                squared = num**2
                stage2_results.append(f"Stage2: {num} -> {squared}")

                output_socket.send(str(squared).encode("utf-8"))
                print(f"  Stage2: {num} -> {squared}")
                time.sleep(0.1)
        finally:
            input_socket.close()
            output_socket.close()
            context.term()

    stage2_thread = threading.Thread(target=stage2_processor)

    # Stage 3: Collect final results
    final_results = []
    stage3_input = Stream.from_zmq(f"tcp://localhost:{stage2_port}", sock_type=zmq.PULL)
    (
        stage3_input.map(
            lambda x: x.decode("utf-8") if isinstance(x, bytes) else str(x)
        )
        .map(int)
        .sink(lambda x: final_results.append(x))
    )

    # Start pipeline (reverse order - consumers first)
    stage3_input.start()
    stage2_thread.start()

    # Give time for connections
    await asyncio.sleep(0.3)

    # Start stage 1 producer
    stage1_thread.start()

    # Wait for completion
    await asyncio.sleep(3.0)

    # Stop everything
    stage3_input.stop()
    stage1_thread.join(timeout=1.0)
    stage2_thread.join(timeout=1.0)

    print(f"Original data: {raw_data}")
    print(f"Stage 2 processing: {stage2_results}")
    print(f"Final squared results: {sorted(final_results)}")
    print()


async def main():
    """Run all examples."""
    print("🚀 Starting Comprehensive ZMQ + Streamz Examples\n")

    await pub_sub_example()
    await asyncio.sleep(0.5)  # Brief pause between examples

    await push_pull_example()
    await asyncio.sleep(0.5)

    await pipeline_example()

    print("✅ All examples completed!")


if __name__ == "__main__":
    asyncio.run(main())
