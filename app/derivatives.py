from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from PIL import Image, ImageOps

try:
    from pillow_heif import register_heif_opener

    register_heif_opener()
except Exception:
    # HEIF/HEIC support is optional; generation will fail gracefully if Pillow can't open.
    pass


@dataclass(frozen=True)
class DerivResult:
    thumb_created: bool
    mid_created: bool


def _bucketed_path(deriv_root: Path, kind: str, guid: str, ext: str) -> Path:
    a = guid[0:2]
    b = guid[2:4]
    return deriv_root / kind / a / b / f"{guid}{ext}"


def thumb_path(deriv_root: Path, guid: str) -> Path:
    return _bucketed_path(deriv_root, "thumb", guid, ".webp")


def mid_path(deriv_root: Path, guid: str) -> Path:
    return _bucketed_path(deriv_root, "mid", guid, ".webp")


def _should_regen(out_path: Path, source_mtime: Optional[int]) -> bool:
    if not out_path.exists():
        return True
    if source_mtime is None:
        return False
    try:
        return int(out_path.stat().st_mtime) < int(source_mtime)
    except Exception:
        return True


def _save_webp(im: Image.Image, out_path: Path, *, quality: int) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # WebP prefers RGB/RGBA.
    if im.mode not in ("RGB", "RGBA"):
        im = im.convert("RGB")

    im.save(out_path, format="WEBP", quality=int(quality), method=6)


def ensure_derivatives(
    *,
    source_path: Path,
    deriv_root: Path,
    guid: str,
    source_mtime: Optional[int],
    thumb_max: int,
    mid_max: int,
    thumb_quality: int,
    mid_quality: int,
) -> DerivResult:
    tpath = thumb_path(deriv_root, guid)
    mpath = mid_path(deriv_root, guid)

    need_thumb = _should_regen(tpath, source_mtime)
    need_mid = _should_regen(mpath, source_mtime)

    if not (need_thumb or need_mid):
        return DerivResult(thumb_created=False, mid_created=False)

    with Image.open(source_path) as im:
        im = ImageOps.exif_transpose(im)

        thumb_created = False
        mid_created = False

        if need_thumb:
            thumb = im.copy()
            thumb.thumbnail((thumb_max, thumb_max), resample=Image.Resampling.LANCZOS)
            _save_webp(thumb, tpath, quality=thumb_quality)
            thumb_created = True

        if need_mid:
            mid = im.copy()
            mid.thumbnail((mid_max, mid_max), resample=Image.Resampling.LANCZOS)
            _save_webp(mid, mpath, quality=mid_quality)
            mid_created = True

        return DerivResult(thumb_created=thumb_created, mid_created=mid_created)
