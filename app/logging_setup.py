from __future__ import annotations

import logging
import logging.handlers
import socket
import sys
from pathlib import Path

from .config import Settings


_CONFIGURED = False


def _parse_level(raw: str) -> int:
    try:
        return int(getattr(logging, raw.strip().upper()))
    except Exception:
        return logging.INFO


def setup_logging(settings: Settings) -> None:
    """Configure standard Python logging.

    Supports:
    - Console (always)
    - Rotating file handler (LOG_FILE)
    - Syslog (LOG_SYSLOG_HOST/PORT or LOG_SYSLOG_PATH)

    This function is idempotent within a process.
    """

    global _CONFIGURED
    if _CONFIGURED:
        return

    level = _parse_level(settings.log_level)

    root = logging.getLogger()
    root.setLevel(level)

    fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    # Console handler
    console = logging.StreamHandler(stream=sys.stdout)
    console.setLevel(level)
    console.setFormatter(fmt)
    root.addHandler(console)

    # Optional file handler
    if settings.log_file is not None:
        log_path = Path(settings.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.handlers.RotatingFileHandler(
            filename=str(log_path),
            maxBytes=int(settings.log_file_max_bytes),
            backupCount=int(settings.log_file_backups),
            encoding="utf-8",
        )
        fh.setLevel(level)
        fh.setFormatter(fmt)
        root.addHandler(fh)

    # Optional syslog handler
    syslog_handler: logging.Handler | None = None
    if settings.log_syslog_path is not None:
        # Unix domain socket syslog (common on macOS/Linux)
        syslog_handler = logging.handlers.SysLogHandler(address=str(settings.log_syslog_path))
    elif settings.log_syslog_host:
        proto = (settings.log_syslog_protocol or "udp").strip().lower()
        socktype = socket.SOCK_DGRAM if proto != "tcp" else socket.SOCK_STREAM
        syslog_handler = logging.handlers.SysLogHandler(
            address=(settings.log_syslog_host, int(settings.log_syslog_port)),
            socktype=socktype,
        )

    if syslog_handler is not None:
        syslog_handler.setLevel(level)
        # Syslog already adds timestamps/hosts; keep it short.
        syslog_handler.setFormatter(logging.Formatter("phototank %(levelname)s %(name)s: %(message)s"))
        root.addHandler(syslog_handler)

    # Make uvicorn loggers follow the same level.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(name).setLevel(level)

    _CONFIGURED = True
