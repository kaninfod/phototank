from __future__ import annotations

from pathlib import Path
import base64

from fastapi import HTTPException


def resolve_under(base: Path, p: Path) -> Path:
    if p.is_absolute():
        return p
    return (base / p).resolve()


def resolve_relpath_under(base: Path, rel_path: str) -> Path:
    base_resolved = base.resolve()
    p = (base_resolved / rel_path).resolve()

    if p == base_resolved or base_resolved in p.parents:
        return p
    raise HTTPException(status_code=400, detail="path is outside PHOTO_ROOT")


def normalize_guid(raw: str) -> str:
    s = raw.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        s = s[1:-1].strip()
    s = s.strip().lower().replace("-", "")
    if len(s) != 32 or any(c not in "0123456789abcdef" for c in s):
        raise HTTPException(status_code=400, detail="guid must be a 32-character hex UUID (hyphens optional)")
    return s


def b64encode_cursor(dt: str, guid: str) -> str:
    raw = f"{dt}|{guid}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def b64decode_cursor(cursor: str) -> tuple[str, str]:
    s = cursor.strip()
    pad = "=" * ((4 - (len(s) % 4)) % 4)
    try:
        raw = base64.urlsafe_b64decode((s + pad).encode("ascii")).decode("utf-8")
        dt, guid = raw.split("|", 1)
        guid = normalize_guid(guid)
        if not dt:
            raise ValueError("empty datetime")
        return dt, guid
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=400, detail="invalid cursor")
