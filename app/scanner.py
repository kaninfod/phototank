from __future__ import annotations

import os
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import exifread
from PIL import Image


@dataclass(frozen=True)
class PhotoRecord:
    guid: str
    rel_path: str
    datetime_original: Optional[str]
    gps_altitude: Optional[float]
    gps_latitude: Optional[float]
    gps_longitude: Optional[float]
    camera_make: Optional[str]
    file_size: int
    source_mtime: Optional[int]
    width: Optional[int]
    height: Optional[int]
    user_comment: Optional[str]
    indexed_at: str
    exif_error: Optional[str]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def iter_photo_files(photo_root: Path, exts: set[str]) -> Iterable[Path]:
    for root, _, files in os.walk(photo_root):
        for name in files:
            path = Path(root) / name
            if path.suffix.lower() in exts:
                yield path


def ratio_to_float(r) -> float:
    if hasattr(r, "num") and hasattr(r, "den"):
        return float(r.num) / float(r.den) if r.den else float("nan")
    return float(r)


def dms_to_decimal(dms_values) -> Optional[float]:
    try:
        degrees = ratio_to_float(dms_values[0])
        minutes = ratio_to_float(dms_values[1])
        seconds = ratio_to_float(dms_values[2])
        return degrees + (minutes / 60.0) + (seconds / 3600.0)
    except Exception:
        return None


def parse_exif_datetime(value: str) -> Optional[str]:
    try:
        dt = datetime.strptime(value.strip(), "%Y:%m:%d %H:%M:%S")
        return dt.isoformat()
    except Exception:
        return None


def _try_datetime_from_sidecar_json(media_path: Path) -> Optional[str]:
    """Try to derive datetime_original from a Google Takeout sidecar JSON.

    Common patterns live next to the media:
    - <file>.jpg.json
    - <file>.json
    """
    candidates = [
        Path(str(media_path) + ".json"),
        media_path.with_suffix(media_path.suffix + ".json"),
        media_path.with_suffix(".json"),
    ]

    for jp in candidates:
        if not jp.exists() or not jp.is_file():
            continue
        try:
            payload = json.loads(jp.read_text(encoding="utf-8"))
        except Exception:
            continue

        ts = None
        for key_path in [
            ("photoTakenTime", "timestamp"),
            ("creationTime", "timestamp"),
            ("takenTime", "timestamp"),
        ]:
            cur = payload
            ok = True
            for k in key_path:
                if not isinstance(cur, dict) or k not in cur:
                    ok = False
                    break
                cur = cur[k]
            if ok and cur is not None:
                ts = cur
                break
        if ts is None:
            continue

        try:
            dt = datetime.fromtimestamp(int(ts))
            return dt.replace(microsecond=0).isoformat()
        except Exception:
            continue

    return None


def _try_datetime_from_mtime(media_path: Path) -> Optional[str]:
    try:
        dt = datetime.fromtimestamp(media_path.stat().st_mtime)
        return dt.replace(microsecond=0).isoformat()
    except Exception:
        return None


def decode_user_comment(tag_value) -> Optional[str]:
    if tag_value is None:
        return None
    text = str(tag_value).strip()
    return text or None


def extract_exif_fields(path: Path) -> tuple[
    Optional[str],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[str],
    Optional[str],
    Optional[str],
]:
    try:
        with path.open("rb") as f:
            tags = exifread.process_file(f, details=False)

        dt_raw = None
        for key in ("EXIF DateTimeOriginal", "EXIF DateTimeDigitized", "Image DateTime"):
            if key in tags:
                dt_raw = str(tags[key])
                break
        datetime_original = parse_exif_datetime(dt_raw) if dt_raw else None

        camera_make = str(tags.get("Image Make")).strip() if tags.get("Image Make") else None
        user_comment = decode_user_comment(tags.get("EXIF UserComment"))

        lat = None
        lon = None
        alt = None

        lat_tag = tags.get("GPS GPSLatitude")
        lon_tag = tags.get("GPS GPSLongitude")
        lat_ref = str(tags.get("GPS GPSLatitudeRef")).strip() if tags.get("GPS GPSLatitudeRef") else None
        lon_ref = str(tags.get("GPS GPSLongitudeRef")).strip() if tags.get("GPS GPSLongitudeRef") else None

        if lat_tag and lon_tag:
            lat = dms_to_decimal(lat_tag.values)
            lon = dms_to_decimal(lon_tag.values)
            if lat is not None and lat_ref in ("S", "s"):
                lat = -lat
            if lon is not None and lon_ref in ("W", "w"):
                lon = -lon

        alt_tag = tags.get("GPS GPSAltitude")
        if alt_tag:
            try:
                alt = ratio_to_float(alt_tag.values[0] if isinstance(alt_tag.values, list) else alt_tag.values)
            except Exception:
                alt = None

            alt_ref = tags.get("GPS GPSAltitudeRef")
            try:
                if alt is not None and alt_ref is not None and int(str(alt_ref)) == 1:
                    alt = -alt
            except Exception:
                pass

        return datetime_original, alt, lat, lon, camera_make, user_comment, None

    except Exception as e:
        return None, None, None, None, None, None, f"{type(e).__name__}: {e}"


def get_image_dimensions(path: Path) -> tuple[Optional[int], Optional[int]]:
    try:
        with Image.open(path) as im:
            return int(im.size[0]), int(im.size[1])
    except Exception:
        return None, None


def build_record(photo_root: Path, path: Path, *, datetime_fallback_order: Optional[list[str]] = None) -> PhotoRecord:
    rel_path = path.relative_to(photo_root).as_posix()
    st = path.stat()

    datetime_original, alt, lat, lon, make, comment, exif_error = extract_exif_fields(path)
    if datetime_original is None and datetime_fallback_order:
        for fb in datetime_fallback_order:
            if fb == "json":
                datetime_original = _try_datetime_from_sidecar_json(path)
            elif fb == "mtime":
                datetime_original = _try_datetime_from_mtime(path)
            if datetime_original is not None:
                break
    width, height = get_image_dimensions(path)

    return PhotoRecord(
        guid=uuid.uuid4().hex,
        rel_path=rel_path,
        datetime_original=datetime_original,
        gps_altitude=alt,
        gps_latitude=lat,
        gps_longitude=lon,
        camera_make=make,
        file_size=int(st.st_size),
        source_mtime=int(st.st_mtime),
        width=width,
        height=height,
        user_comment=comment,
        indexed_at=utc_now_iso(),
        exif_error=exif_error,
    )
