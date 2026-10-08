"""One log format for all streaming processes (easy to read during a demo)."""
from __future__ import annotations

import logging
import sys

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(level=level.upper(), format=LOG_FORMAT, stream=sys.stderr, force=True)
    logging.getLogger("kafka").setLevel(logging.WARNING)   # librdkafka internals, routed via the logger= option
