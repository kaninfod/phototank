"""Backwards-compatible router aggregator.

Historically all endpoints lived in this module. We now split into:
- phototank.routers.api (JSON + FileResponse endpoints)
- phototank.routers.web (Jinja2 HTML pages)
"""

from __future__ import annotations

from fastapi import APIRouter

from ..routers.api import api_router
from ..routers.web import web_router

router = APIRouter()
router.include_router(web_router)
router.include_router(api_router)
