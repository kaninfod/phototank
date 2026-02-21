from __future__ import annotations

import json
import logging
import math
import ssl
import threading
import time
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from sqlalchemy.orm import Session

from .config import Settings
from .models import Photo, ReverseGeocodeCache

try:
    import certifi  # type: ignore
except Exception:
    certifi = None


logger = logging.getLogger(__name__)


_GEONAMES_THROTTLE_LOCK = threading.Lock()
_GEONAMES_NEXT_REQUEST_AT_S = 0.0
_GEONAMES_HALT_UNTIL_S = 0.0


def _apply_geonames_throttle(settings: Settings) -> None:
    global _GEONAMES_NEXT_REQUEST_AT_S

    min_interval_s = max(0.0, float(getattr(settings, "geocode_min_interval_s", 0.25)))
    if min_interval_s <= 0:
        return

    with _GEONAMES_THROTTLE_LOCK:
        now = time.monotonic()
        wait_s = _GEONAMES_NEXT_REQUEST_AT_S - now
        if wait_s > 0:
            time.sleep(wait_s)
            now = time.monotonic()
        _GEONAMES_NEXT_REQUEST_AT_S = now + min_interval_s


def _mark_geonames_hourly_limit_hit(settings: Settings) -> None:
    global _GEONAMES_HALT_UNTIL_S
    cooldown_s = max(60.0, float(getattr(settings, "geocode_hourly_limit_cooldown_s", 3600.0)))
    with _GEONAMES_THROTTLE_LOCK:
        _GEONAMES_HALT_UNTIL_S = max(_GEONAMES_HALT_UNTIL_S, time.monotonic() + cooldown_s)


def _geonames_is_halted() -> bool:
    with _GEONAMES_THROTTLE_LOCK:
        return time.monotonic() < _GEONAMES_HALT_UNTIL_S


def _is_geonames_hourly_limit_error(err: Exception) -> bool:
    msg = str(err).casefold()
    return (
        "hourly limit" in msg
        or "credits" in msg and "exceeded" in msg
        or "please throttle your requests" in msg
    )


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    v = " ".join(value.split()).strip()
    return v or None


def _normalize_city(value: str | None) -> str | None:
    v = _normalize_text(value)
    return v.casefold() if v else None


def _snap_to_grid(lat: float, lon: float, cell_m: int) -> tuple[float, float, int, int]:
    safe_cell_m = max(10, int(cell_m))
    lat_step = safe_cell_m / 111_320.0

    cos_lat = math.cos(math.radians(lat))
    if abs(cos_lat) < 0.1:
        cos_lat = 0.1 if cos_lat >= 0 else -0.1
    lon_step = safe_cell_m / (111_320.0 * abs(cos_lat))

    lat_bucket = int(math.floor(lat / lat_step))
    lon_bucket = int(math.floor(lon / lon_step))
    snapped_lat = lat_bucket * lat_step
    snapped_lon = lon_bucket * lon_step
    return snapped_lat, snapped_lon, lat_bucket, lon_bucket


def _cache_key(provider: str, cell_m: int, lat_bucket: int, lon_bucket: int) -> str:
    return f"{provider}:{cell_m}:{lat_bucket}:{lon_bucket}"


def _geonames_lookup(
    *,
    lat: float,
    lon: float,
    username: str,
    radius_km: float,
    timeout_s: float,
) -> dict[str, Any] | None:
    params = {
        "lat": f"{lat:.7f}",
        "lng": f"{lon:.7f}",
        "radius": f"{radius_km:.3f}",
        "maxRows": "1",
        "username": username,
    }
    query = urlencode(params)
    https_url = f"https://api.geonames.org/findNearbyPlaceNameJSON?{query}"
    http_url = f"http://api.geonames.org/findNearbyPlaceNameJSON?{query}"

    timeout = max(1.0, float(timeout_s))

    def _is_cert_error(err: URLError) -> bool:
        reason = getattr(err, "reason", None)
        msg = str(reason or err).lower()
        return isinstance(reason, ssl.SSLCertVerificationError) or (
            "certificate verify failed" in msg
            or "certificate_verify_failed" in msg
            or "hostname mismatch" in msg
        )

    try:
        with urlopen(https_url, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except URLError as e:
        if not _is_cert_error(e):
            raise

        if certifi is not None:
            try:
                ctx = ssl.create_default_context(cafile=certifi.where())
                with urlopen(https_url, timeout=timeout, context=ctx) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
            except URLError as e2:
                if not _is_cert_error(e2):
                    raise
                logger.warning("GeoNames HTTPS TLS verification failed; falling back to HTTP for lookup")
                with urlopen(http_url, timeout=timeout) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
        else:
            logger.warning("GeoNames HTTPS TLS verification failed; falling back to HTTP for lookup")
            with urlopen(http_url, timeout=timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8"))

    # GeoNames may return an error payload even with HTTP 200.
    status = payload.get("status")
    if isinstance(status, dict) and status.get("message"):
        raise RuntimeError(f"GeoNames error: {status.get('message')}")

    rows = payload.get("geonames")
    if not isinstance(rows, list) or not rows:
        return None

    first = rows[0] if isinstance(rows[0], dict) else None
    if not first:
        return None

    city = _normalize_text(first.get("name"))
    region = _normalize_text(first.get("adminName1"))
    country = _normalize_text(first.get("countryName"))
    country_code = _normalize_text(first.get("countryCode"))

    parts = [p for p in [city, region, country] if p]
    display_name = ", ".join(parts) if parts else None

    return {
        "country_code": country_code,
        "country": country,
        "city": city,
        "city_norm": _normalize_city(city),
        "region": region,
        "postcode": _normalize_text(first.get("postalcode")),
        "display_name": display_name,
        "raw_json": json.dumps(first, ensure_ascii=False),
    }


def _lookup_provider_data(settings: Settings, *, lat: float, lon: float) -> dict[str, Any] | None:
    provider = (settings.geocode_provider or "").strip().lower()
    if provider != "geonames":
        raise RuntimeError(f"Unsupported geocode provider: {provider}")

    username = (settings.geocode_geonames_username or "").strip()
    if not username:
        return None

    primary = max(0.05, float(settings.geocode_radius_km_primary))
    fallback = max(primary, float(settings.geocode_radius_km_fallback))
    timeout_s = max(1.0, float(settings.geocode_timeout_s))

    first = _geonames_lookup(lat=lat, lon=lon, username=username, radius_km=primary, timeout_s=timeout_s)
    if first is not None:
        return first

    if fallback > primary:
        return _geonames_lookup(lat=lat, lon=lon, username=username, radius_km=fallback, timeout_s=timeout_s)

    return None


def _apply_cached_to_photo(photo: Photo, cached: ReverseGeocodeCache, *, provider: str, cache_key: str, now: str) -> None:
    photo.geo_country_code = cached.country_code
    photo.geo_country = cached.country
    photo.geo_city = cached.city
    photo.geo_city_norm = cached.city_norm
    photo.geo_region = cached.region
    photo.geo_postcode = cached.postcode
    photo.geo_display_name = cached.display_name
    photo.geo_provider = provider
    photo.geo_cache_key = cache_key
    photo.geo_lookup_at = now
    photo.geo_lookup_status = "ok"
    photo.geo_lookup_error = None


def _apply_result_to_photo(photo: Photo, data: dict[str, Any], *, provider: str, cache_key: str, now: str) -> None:
    photo.geo_country_code = _normalize_text(data.get("country_code"))
    photo.geo_country = _normalize_text(data.get("country"))
    photo.geo_city = _normalize_text(data.get("city"))
    photo.geo_city_norm = _normalize_city(data.get("city"))
    photo.geo_region = _normalize_text(data.get("region"))
    photo.geo_postcode = _normalize_text(data.get("postcode"))
    photo.geo_display_name = _normalize_text(data.get("display_name"))
    photo.geo_provider = provider
    photo.geo_cache_key = cache_key
    photo.geo_lookup_at = now
    photo.geo_lookup_status = "ok"
    photo.geo_lookup_error = None


def _should_lookup(photo: Photo) -> bool:
    if photo.gps_latitude is None or photo.gps_longitude is None:
        return False
    if photo.geo_lookup_status == "ok" and photo.geo_country and photo.geo_city:
        return False
    return True


def enrich_photo_location(session: Session, *, settings: Settings, photo: Photo) -> bool:
    if not settings.geocode_enabled:
        return False

    if not _should_lookup(photo):
        return False

    provider = (settings.geocode_provider or "").strip().lower()
    if provider != "geonames":
        return False

    if not (settings.geocode_geonames_username or "").strip():
        return False

    if _geonames_is_halted():
        photo.geo_provider = provider
        photo.geo_lookup_at = utc_now_iso()
        photo.geo_lookup_status = "error"
        photo.geo_lookup_error = "RuntimeError: GeoNames hourly limit previously exceeded; temporarily throttled"
        return True

    lat = float(photo.gps_latitude)
    lon = float(photo.gps_longitude)

    _, _, lat_bucket, lon_bucket = _snap_to_grid(lat, lon, int(settings.geocode_cache_cell_m))
    cache_key = _cache_key(provider, int(settings.geocode_cache_cell_m), lat_bucket, lon_bucket)
    now = utc_now_iso()

    cached = session.get(ReverseGeocodeCache, cache_key)
    if cached is not None:
        _apply_cached_to_photo(photo, cached, provider=provider, cache_key=cache_key, now=now)
        cached.last_used_at = now
        cached.hit_count = int(cached.hit_count or 0) + 1
        return True

    try:
        _apply_geonames_throttle(settings)
        result = _lookup_provider_data(settings, lat=lat, lon=lon)
    except HTTPError as e:
        detail = f"HTTPError: HTTP {getattr(e, 'code', '')}"
        if getattr(e, "code", None) in {401, 403}:
            detail = (
                "HTTPError: GeoNames rejected the request (401/403). "
                "Check GEOCODE_GEONAMES_USERNAME and confirm webservice is enabled on the GeoNames account."
            )
        photo.geo_provider = provider
        photo.geo_cache_key = cache_key
        photo.geo_lookup_at = now
        photo.geo_lookup_status = "error"
        photo.geo_lookup_error = detail
        logger.warning("reverse geocode failed guid=%s err=%s", getattr(photo, "guid", ""), detail)
        return True
    except (URLError, TimeoutError, RuntimeError) as e:
        if _is_geonames_hourly_limit_error(e):
            _mark_geonames_hourly_limit_hit(settings)
        photo.geo_provider = provider
        photo.geo_cache_key = cache_key
        photo.geo_lookup_at = now
        photo.geo_lookup_status = "error"
        photo.geo_lookup_error = f"{type(e).__name__}: {e}"
        logger.warning("reverse geocode failed guid=%s err=%s", getattr(photo, "guid", ""), e)
        return True
    except Exception as e:
        photo.geo_provider = provider
        photo.geo_cache_key = cache_key
        photo.geo_lookup_at = now
        photo.geo_lookup_status = "error"
        photo.geo_lookup_error = f"{type(e).__name__}: {e}"
        logger.exception("reverse geocode crashed guid=%s", getattr(photo, "guid", ""))
        return True

    if result is None:
        photo.geo_provider = provider
        photo.geo_cache_key = cache_key
        photo.geo_lookup_at = now
        photo.geo_lookup_status = "miss"
        photo.geo_lookup_error = None
        return True

    _apply_result_to_photo(photo, result, provider=provider, cache_key=cache_key, now=now)

    cache_row = ReverseGeocodeCache(
        cache_key=cache_key,
        provider=provider,
        cell_m=int(settings.geocode_cache_cell_m),
        lat_bucket=float(lat_bucket),
        lon_bucket=float(lon_bucket),
        country_code=photo.geo_country_code,
        country=photo.geo_country,
        city=photo.geo_city,
        city_norm=photo.geo_city_norm,
        region=photo.geo_region,
        postcode=photo.geo_postcode,
        display_name=photo.geo_display_name,
        raw_json=result.get("raw_json"),
        fetched_at=now,
        last_used_at=now,
        hit_count=1,
    )
    session.add(cache_row)
    return True
