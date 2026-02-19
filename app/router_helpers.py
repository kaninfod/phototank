from __future__ import annotations

from pathlib import Path

from fastapi import HTTPException
from pydantic import ValidationError

from .config import get_settings


def settings_or_500():
    try:
        return get_settings()
    except ValidationError as e:
        raise HTTPException(
            status_code=500,
            detail=(
                "Invalid or missing configuration. Create phototank/.env (see app/.env.example) "
                "and set PHOTO_ROOT. "
                f"Details: {e.errors()}"
            ),
        )


def ensure_dirs_and_db(photo_root: Path, db_path: Path) -> None:
    if not photo_root.exists() or not photo_root.is_dir():
        raise HTTPException(status_code=400, detail=f"PHOTO_ROOT is not a directory: {photo_root}")
    db_path.parent.mkdir(parents=True, exist_ok=True)


def ensure_deriv_root(deriv_root: Path) -> None:
    deriv_root.mkdir(parents=True, exist_ok=True)


def ensure_import_dirs(import_root: Path, failed_root: Path) -> None:
    import_root.mkdir(parents=True, exist_ok=True)
    failed_root.mkdir(parents=True, exist_ok=True)
    if not import_root.is_dir():
        raise HTTPException(status_code=400, detail=f"IMPORT_ROOT is not a directory: {import_root}")
    if not failed_root.is_dir():
        raise HTTPException(status_code=400, detail=f"FAILED_ROOT is not a directory: {failed_root}")
