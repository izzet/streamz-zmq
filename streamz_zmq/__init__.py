"""
streamz-zmq: ZeroMQ integration for streamz

This package provides ZeroMQ sources and sinks for the streamz library,
enabling high-performance distributed streaming data processing.
"""

import importlib.metadata

try:
    __version__ = importlib.metadata.version("streamz-zmq")
except importlib.metadata.PackageNotFoundError:
    # Package not installed, fallback for development
    __version__ = "0.0.0+dev"

# Import the source and sink classes to register them with streamz
from .sources import from_zmq
from .sinks import to_zmq

__all__ = ["from_zmq", "to_zmq", "__version__"]
