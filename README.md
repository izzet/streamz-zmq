# streamz-zmq

[![PyPI version](https://badge.fury.io/py/streamz-zmq.svg)](https://badge.fury.io/py/streamz-zmq)
[![GitHub release](https://img.shields.io/github/v/release/izzet/streamz-zmq)](https://github.com/izzet/streamz-zmq/releases)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

ZeroMQ integration for [streamz](https://streamz.readthedocs.io/) - enabling high-performance streaming data processing with distributed messaging.

## Features

- **ZMQ Source (`from_zmq`)**: Receive data streams from ZeroMQ publishers
- **ZMQ Sink (`to_zmq`)**: Send processed data to ZeroMQ sockets
- **Async Support**: Built with asyncio for high-performance streaming
- **Multiple Patterns**: Support for PUB/SUB, PUSH/PULL, and other ZMQ patterns
- **Seamless Integration**: Extends streamz with familiar API patterns

## Installation

```bash
pip install streamz-zmq
```

Or with uv:

```bash
uv add streamz-zmq
```

## Quick Start

### Receiving data from ZMQ (Source)

```python
from streamz import Stream
import streamz_zmq  # Register the ZMQ extensions
import zmq

# Create a stream that receives from a ZMQ publisher (connect mode, default)
source = Stream.from_zmq("tcp://localhost:5555")
source.sink(print)  # Print received messages

# Start the stream
source.start()

# Or, act as a collector/server and accept connections from publishers:
collector = Stream.from_zmq("tcp://*:6000", sock_type=zmq.PULL, bind=True)
collector.sink(print)
collector.start()
```

### Sending data to ZMQ (Sink)

```python
from streamz import Stream
import streamz_zmq  # Register the ZMQ extensions

# Create a stream and send results to an existing ZMQ service (default: connect mode)
source = Stream.from_iterable([1, 2, 3, 4, 5])
source.map(lambda x: x * 2).to_zmq("tcp://localhost:5556")

# Or, act as a service and accept connections from ZMQ clients (bind mode)
source.map(...).to_zmq("tcp://*:5556", bind=True)

# Start the stream
source.start()
```

### Complete Example: Pipeline with ZMQ

```python
import asyncio
from streamz import Stream
import streamz_zmq

async def main():
    # Receive from one ZMQ socket, process, send to another
    source = Stream.from_zmq("tcp://localhost:5555")

    processed = (source
                .map(lambda x: x.decode('utf-8'))  # Decode bytes
                .map(str.upper)                    # Process data
                .map(str.encode))                  # Encode back to bytes

    processed.to_zmq("tcp://*:5556")

    # Start processing
    await source.start()

if __name__ == "__main__":
    asyncio.run(main())
```

## Examples

Check out the `examples/` directory for demonstrations:

- **`simple_example.py`**: Basic example showing ZMQ publisher thread + streamz subscriber
- **`comprehensive_example.py`**: Advanced demonstration showing multiple ZMQ patterns:
  - **PUB/SUB**: Publisher broadcasts weather updates to topic-specific subscribers
  - **PUSH/PULL**: Load balancing work distribution across multiple workers
  - **Pipeline**: Multi-stage data processing pipeline

Run the simple example:

```bash
uv run python examples/simple_example.py
```

Run the comprehensive example:

```bash
uv run python examples/comprehensive_example.py
```

## API Reference

### `Stream.from_zmq(connect_str, sock_type=zmq.SUB, subscribe=b"", bind=False)`

Creates a stream source that receives messages from a ZMQ socket.

**Parameters:**

- `connect_str` (str): ZMQ connection string (e.g., "tcp://localhost:5555" for connect, or "tcp://\*:5555" for bind)
- `sock_type` (int, optional): ZMQ socket type. Defaults to `zmq.SUB`
- `subscribe` (bytes, optional): Subscription topic for SUB sockets. Defaults to `b""` (all messages)
- `bind` (bool, optional): If True, bind the socket (act as a server/collector). If False (default), connect to the address.

### `stream.to_zmq(connect_str, sock_type=zmq.PUSH, bind=False)`

Sends stream elements to a ZMQ socket.

**Parameters:**

- `connect_str` (str): ZMQ connection string (e.g., "tcp://\*:5556" for bind, or "tcp://localhost:5556" for connect)
- `sock_type` (int, optional): ZMQ socket type. Defaults to `zmq.PUSH`
- `bind` (bool, optional): If True, bind the socket (act as a service). If False (default), connect to the address.

## ZMQ Patterns Supported

- **PUB/SUB**: Publisher broadcasts to multiple subscribers
- **PUSH/PULL**: Load balancing across workers
- **REQ/REP**: Request-response (less common for streaming)

## Requirements

- Python 3.9+
- streamz >= 0.6.4
- pyzmq >= 27.0.0

## Development

```bash
# Clone the repository
git clone https://github.com/izzet/streamz-zmq.git
cd streamz-zmq

# Install with uv (uses uv.lock for reproducible builds)
uv sync --dev

# Set up pre-commit hooks (recommended)
uv run pre-commit install

# Run tests
uv run pytest

# Format code
uv run ruff format .

# Check linting
uv run ruff check .

# Build package
uv build
```

**Note**: This project uses `uv.lock` for reproducible dependency management. The lock file is committed to ensure all developers and CI/CD use identical dependency versions.

**Pre-commit hooks**: The project includes pre-commit hooks that automatically format code, check linting, and run tests before each commit to maintain code quality.

## License

MIT License. See [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
