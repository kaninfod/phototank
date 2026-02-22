from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.requests import Request

import json
import logging

from .core.config import get_settings
from .core.db import engine_for, init_db
from .core.logging_setup import setup_logging
from .core.routes import router


def create_app() -> FastAPI:
    try:
        setup_logging(get_settings())
    except Exception:
        # Keep app booting even if config is missing; endpoints already surface
        # config errors. Console logging will still work via uvicorn defaults.
        pass

    app = FastAPI(title="phototank")

    @app.on_event("startup")
    def _startup_init_db() -> None:
        # One-time init at process start; avoids doing DB setup per-request.
        settings = get_settings()
        engine = engine_for(settings.db_path)
        init_db(engine)

    validation_logger = logging.getLogger("phototank.validation")

    @app.exception_handler(RequestValidationError)
    async def _request_validation_handler(request: Request, exc: RequestValidationError):
        body_for_log = None
        try:
            raw = await request.body()
            if raw:
                try:
                    parsed = json.loads(raw)
                    if (
                        isinstance(parsed, dict)
                        and isinstance(parsed.get("guids"), list)
                        and all(isinstance(g, str) for g in parsed["guids"][:5])
                    ):
                        body_for_log = {
                            "guids_count": len(parsed["guids"]),
                            "guids_head": parsed["guids"][:5],
                        }
                    else:
                        body_for_log = parsed
                except Exception:
                    # Truncate to keep logs readable.
                    preview = raw[:8192]
                    body_for_log = preview.decode("utf-8", "replace") + ("…" if len(raw) > 8192 else "")
        except Exception:
            body_for_log = None

        validation_logger.warning(
            "Request validation failed: %s %s errors=%s body=%s",
            request.method,
            request.url.path,
            exc.errors(),
            body_for_log,
        )
        return JSONResponse(status_code=422, content={"detail": exc.errors()})

    static_dir = Path(__file__).resolve().parent / "static"
    app.mount("/phototank/static", StaticFiles(directory=str(static_dir)), name="static")

    app.include_router(router, prefix="/phototank")
    return app


app = create_app()
