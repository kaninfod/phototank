from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .util import resolve_under


APP_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = APP_DIR.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        # Prefer project-root `.env` (phototank/.env). Keep `app/.env` as a
        # backward-compatible fallback.
        env_file=[str(PROJECT_ROOT / ".env"), str(APP_DIR / ".env")],
        env_file_encoding="utf-8",
        extra="ignore",
    )

    photo_root: Path
    db_path: Path = Path("data/phototank.sqlite")

    import_root: Path = Path("import")
    failed_root: Path = Path("failed")

    deriv_root: Path = Path("data/derivatives")
    thumb_max: int = 256
    mid_max: int = 2048
    thumb_quality: int = 75
    mid_quality: int = 85

    photo_exts: Optional[str] = None
    datetime_fallback: Optional[str] = None

    log_level: str = "INFO"
    log_file: Optional[Path] = None
    log_file_max_bytes: int = 10_000_000
    log_file_backups: int = 5
    log_syslog_host: Optional[str] = None
    log_syslog_port: int = 514
    log_syslog_protocol: str = "udp"  # udp|tcp
    log_syslog_path: Optional[Path] = None

    geocode_enabled: bool = True
    geocode_provider: str = "geonames"
    geocode_geonames_username: Optional[str] = None
    geocode_cache_cell_m: int = 100
    geocode_radius_km_primary: float = 0.2
    geocode_radius_km_fallback: float = 1.0
    geocode_timeout_s: float = 6.0
    geocode_min_interval_s: float = 0.25
    geocode_hourly_limit_cooldown_s: float = 3600.0

    phone_sync_ssh_user: str | None = None
    phone_sync_ip: str | None = None
    phone_sync_port: int = 22
    phone_sync_source_path: str | None = None
    phone_sync_dest_path: str | None = None
    phone_sync_ssh_key_path: Path = Path("~/.ssh/id_ed25519")

    @model_validator(mode="after")
    def _resolve_relative_paths(self):
        # Resolve relative paths from the project root so uvicorn cwd doesn't matter.
        repo_root = PROJECT_ROOT
        self.photo_root = resolve_under(repo_root, self.photo_root)
        self.db_path = resolve_under(repo_root, self.db_path)
        self.deriv_root = resolve_under(repo_root, self.deriv_root)
        self.import_root = resolve_under(repo_root, self.import_root)
        self.failed_root = resolve_under(repo_root, self.failed_root)

        if self.log_file is not None:
            self.log_file = resolve_under(repo_root, self.log_file)
        if self.log_syslog_path is not None:
            self.log_syslog_path = resolve_under(repo_root, self.log_syslog_path)
        if self.phone_sync_ssh_key_path is not None:
            self.phone_sync_ssh_key_path = self.phone_sync_ssh_key_path.expanduser()
        return self

    def extensions_set(self) -> set[str]:
        if not self.photo_exts:
            return {".jpg", ".jpeg", ".tif", ".tiff", ".png", ".heic", ".webp"}
        exts = set()
        for part in self.photo_exts.split(","):
            part = part.strip()
            if not part:
                continue
            if not part.startswith("."):
                part = "." + part
            exts.add(part.lower())
        return exts

    def datetime_fallback_order(self) -> list[str]:
        """Return fallback order for datetime_original when EXIF is missing.

        Supported values: json, filename, mtime. If unset, returns an empty list (EXIF-only).
        """
        if not self.datetime_fallback:
            return []
        order: list[str] = []
        for part in self.datetime_fallback.split(","):
            p = part.strip().lower()
            if p in ("json", "filename", "mtime"):
                order.append(p)
        return order


def get_settings() -> Settings:
    # Allow overriding env file so users can maintain multiple configs
    # without editing phototank/.env (e.g. PHOTOTANK_ENV_FILE=phototank/.env.extracted).
    env_file = os.environ.get("PHOTOTANK_ENV_FILE")
    if env_file:
        repo_root = PROJECT_ROOT
        p = Path(env_file)
        if not p.is_absolute():
            p = (repo_root / p).resolve()
        return Settings(_env_file=str(p))
    return Settings()
