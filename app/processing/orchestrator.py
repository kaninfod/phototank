from __future__ import annotations

import threading
from typing import Any, Callable


def start_job_thread(target: Callable[..., Any], /, *args: Any, **kwargs: Any) -> None:
    """Start a background thread for a long-running job."""
    t = threading.Thread(target=target, args=args, kwargs=kwargs, daemon=True)
    t.start()
