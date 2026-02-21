from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path
import threading
from urllib.parse import parse_qs, urlencode, urlparse

from fastapi import APIRouter, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import and_, func, or_, select

from ..db import create_job, fetch_photo, get_job, list_tags, sessionmaker_for, tags_for_photo
from ..jobs import new_job_id, run_ingest_job, run_phone_reconcile_job, run_phone_sync_job, run_validate_job
from ..models import Photo, PhotoTag, ScanJob
from ..router_helpers import ensure_deriv_root, ensure_dirs_and_db, ensure_import_dirs, settings_or_500
from ..util import b64decode_cursor, b64encode_cursor, normalize_guid

web_router = APIRouter()

_templates_dir = Path(__file__).resolve().parents[1] / "templates"
templates = Jinja2Templates(directory=str(_templates_dir))


def _dt_min(value) -> str:
    """Format a datetime-ish value as 'YYYY-MM-DD HH:MM'.

    Accepts datetime objects or strings like '2025-10-23T15:11:52' / '2025-10-23 15:11:52'.
    Returns empty string for None.
    """
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.replace(microsecond=0).strftime("%Y-%m-%d %H:%M")

    s = str(value)
    if "T" in s:
        s = s.replace("T", " ")
    return s[:16]


templates.env.filters["dt_min"] = _dt_min


def _load_job_or_404(*, session, job_id: str) -> ScanJob:
    job = get_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


def _job_kind(job: ScanJob) -> str | None:
    jt = (job.job_type or "").strip().lower()
    if jt in {"ingest", "import"}:
        return "ingest"
    if jt == "validate":
        return "validate"
    if jt == "phone_sync":
        return "phone_sync"
    if jt == "phone_reconcile":
        return "phone_reconcile"

    # Backward compatibility for old rows written before job_type existed.
    msg = (job.message or "").strip().lower()
    if msg == "validate":
        return "validate"
    if msg.startswith("ingest:"):
        return "ingest"
    if msg.startswith("phase="):
        return "phone_sync"
    if msg.startswith("done reconciled_mids="):
        return "phone_reconcile"

    # Heuristic fallback for legacy/API rows where type metadata wasn't set.
    # Validate jobs usually carry a year scope; ingest jobs do not.
    if job.year is not None:
        return "validate"
    return None


def _job_sort_key(job: ScanJob) -> tuple[str, str, str]:
    return (
        str(job.started_at or ""),
        str(job.finished_at or ""),
        str(job.job_id or ""),
    )


def _start_job_thread(target, /, *args, **kwargs) -> None:
    t = threading.Thread(target=target, args=args, kwargs=kwargs, daemon=True)
    t.start()




def _safe_back_url(raw: str | None) -> str:
    if not raw:
        return "/phototank/"

    s = raw.strip()
    try:
        parsed = urlparse(s)
        candidate = parsed.path
        if parsed.query:
            candidate += "?" + parsed.query
    except Exception:
        candidate = s

    if not candidate.startswith("/phototank/"):
        return "/phototank/"
    return candidate


def _parse_jump_to_end_iso(raw: str | None) -> tuple[str, str]:
    """Return (jump_end_iso, jump_date_for_url).

    - Accepts YYYY-MM-DD or full ISO datetime.
    - For a date, the jump is interpreted as *end of that day*.
    - For a datetime, the date part is used for the date input.
    """
    if not raw:
        raw = date.today().isoformat()

    try:
        dt = datetime.fromisoformat(raw).replace(microsecond=0)
        return dt.isoformat(), dt.date().isoformat()
    except ValueError:
        try:
            d = date.fromisoformat(raw)
        except ValueError:
            raise HTTPException(status_code=400, detail="jump must be ISO date or datetime")
        dt = datetime.combine(d, time(23, 59, 59)).replace(microsecond=0)
        return dt.isoformat(), d.isoformat()


def _parse_rating_int(raw: str | None) -> int | None:
    if raw is None or raw == "":
        return None
    try:
        n = int(raw)
    except ValueError:
        raise HTTPException(status_code=400, detail="rating must be 0..3")
    if n < 0 or n > 3:
        raise HTTPException(status_code=400, detail="rating must be 0..3")
    return n


def _extract_filter_context(
    *,
    from_: str | None,
    jump: str | None,
    start: str | None,
    rating: str | None,
    tag: str | None,
    country: str | None,
    city: str | None,
) -> tuple[str, int | None, int | None, str | None, str | None]:
    """Return (jump_date_for_url, rating_int, tag_id, country, city).

    Preference order:
    1) explicit query params on the detail URL (jump preferred over start)
    2) parse them from the `from=` gallery URL
    3) defaults
    """
    jump_raw = jump or start
    rating_raw = rating
    tag_raw: str | None = tag
    country_raw: str | None = country
    city_raw: str | None = city

    if (
        (jump_raw is None or jump_raw == "")
        or (rating_raw is None)
        or (tag_raw is None)
        or (country_raw is None)
        or (city_raw is None)
    ):
        if from_:
            try:
                parsed = urlparse(from_)
                qs = parse_qs(parsed.query)
                if (jump_raw is None or jump_raw == ""):
                    if "jump" in qs and qs["jump"]:
                        jump_raw = qs["jump"][0]
                    elif "start" in qs and qs["start"]:
                        jump_raw = qs["start"][0]
                if rating_raw is None and "rating" in qs and qs["rating"]:
                    rating_raw = qs["rating"][0]
                if tag_raw is None and "tag" in qs and qs["tag"]:
                    tag_raw = qs["tag"][0]
                if country_raw is None and "country" in qs and qs["country"]:
                    country_raw = qs["country"][0]
                if city_raw is None and "city" in qs and qs["city"]:
                    city_raw = qs["city"][0]
            except Exception:
                pass

    _, jump_date_for_url = _parse_jump_to_end_iso(jump_raw)
    rating_int = _parse_rating_int(rating_raw)

    tag_id: int | None = None
    if tag_raw is not None and tag_raw != "":
        try:
            tag_id = int(tag_raw)
        except Exception:
            tag_id = None

    country_value = country_raw.strip() if country_raw else None
    if country_value == "":
        country_value = None

    city_value = city_raw.strip() if city_raw else None
    if city_value == "":
        city_value = None

    return jump_date_for_url, rating_int, tag_id, country_value, city_value


@web_router.get("/", response_class=HTMLResponse)
def gallery(
    request: Request,
    jump: str | None = Query(None, description="Jump date/datetime (ISO). e.g. 2010-01-01 or 2010-01-01T12:34:56"),
    start: str | None = Query(None, description="Compatibility alias for jump"),
    limit: int = Query(60, ge=1, le=200),
    older: str | None = Query(None, description="Keyset cursor for older page"),
    newer: str | None = Query(None, description="Keyset cursor for newer page"),
    rating: str | None = Query(None, description="Filter photos by rating (0..3)"),
    tag: str | None = Query(None, description="Filter photos by tag id"),
    country: str | None = Query(None, description="Filter photos by geo country"),
    city: str | None = Query(None, description="Filter photos by geo city"),
):
    settings = settings_or_500()

    SessionLocal = sessionmaker_for(settings.db_path)

    raw_jump = jump or start
    jump_end_iso, jump_date_value = _parse_jump_to_end_iso(raw_jump)

    direction: str
    cursor_value: str | None
    if older:
        direction = "older"
        cursor_value = older
    elif newer:
        direction = "newer"
        cursor_value = newer
    else:
        direction = "initial"
        cursor_value = None

    rating_int: int | None = None
    if rating is not None and rating != "":
        try:
            rating_int = int(rating)
        except ValueError:
            raise HTTPException(status_code=400, detail="rating must be 0..3")
        if rating_int < 0 or rating_int > 3:
            raise HTTPException(status_code=400, detail="rating must be 0..3")

    tag_id: int | None = None
    if tag is not None and tag != "":
        try:
            tag_id = int(tag)
        except Exception:
            raise HTTPException(status_code=400, detail="tag must be an integer")

    country_value = (country or "").strip() or None
    city_value = (city or "").strip() or None
    city_norm = city_value.casefold() if city_value else None

    with SessionLocal() as session:
        all_tags = list_tags(session)

        geo_country_rows = list(
            session.execute(
                select(Photo.geo_country)
                .where(Photo.geo_country.is_not(None))
                .where(Photo.geo_country != "")
                .distinct()
                .order_by(Photo.geo_country.asc())
            ).all()
        )
        geo_countries = [str(r[0]) for r in geo_country_rows if r and r[0]]

        geo_cities_q = (
            select(Photo.geo_city)
            .where(Photo.geo_city.is_not(None))
            .where(Photo.geo_city != "")
        )
        if country_value is not None:
            geo_cities_q = geo_cities_q.where(Photo.geo_country == country_value)
        geo_city_rows = list(
            session.execute(
                geo_cities_q.distinct().order_by(Photo.geo_city.asc())
            ).all()
        )
        geo_cities = [str(r[0]) for r in geo_city_rows if r and r[0]]

        base = select(Photo).where(Photo.datetime_original.is_not(None))
        if rating_int is not None:
            base = base.where(Photo.rating == rating_int)
        if tag_id is not None:
            base = base.join(PhotoTag, PhotoTag.photo_guid == Photo.guid).where(PhotoTag.tag_id == tag_id)
        if country_value is not None:
            base = base.where(Photo.geo_country == country_value)
        if city_norm is not None:
            base = base.where(Photo.geo_city_norm == city_norm)

        rows: list[Photo]
        if direction == "initial":
            q = base.where(Photo.datetime_original <= jump_end_iso).order_by(
                Photo.datetime_original.desc(), Photo.guid.desc()
            )
            rows = session.execute(q.limit(limit + 1)).scalars().all()
        else:
            if not cursor_value:
                raise HTTPException(status_code=400, detail="missing cursor")
            cursor_dt, cursor_guid = b64decode_cursor(cursor_value)
            if direction == "older":
                q = base.where(
                    or_(
                        Photo.datetime_original < cursor_dt,
                        and_(Photo.datetime_original == cursor_dt, Photo.guid < cursor_guid),
                    )
                ).order_by(Photo.datetime_original.desc(), Photo.guid.desc())
                rows = session.execute(q.limit(limit + 1)).scalars().all()
            else:
                q = base.where(
                    or_(
                        Photo.datetime_original > cursor_dt,
                        and_(Photo.datetime_original == cursor_dt, Photo.guid > cursor_guid),
                    )
                ).order_by(Photo.datetime_original.asc(), Photo.guid.asc())
                rows = session.execute(q.limit(limit + 1)).scalars().all()

        has_more_in_direction = len(rows) > limit
        rows = rows[:limit]

        if direction == "newer":
            # Display stays newest-first regardless of query direction.
            rows = list(reversed(rows))

        has_newer = False
        has_older = False
        newer_cursor = ""
        older_cursor = ""

        if rows:
            newest = rows[0]
            oldest = rows[-1]

            # Older = items strictly older than the oldest item on this page.
            older_exists = session.execute(
                base.where(
                    or_(
                        Photo.datetime_original < oldest.datetime_original,
                        and_(
                            Photo.datetime_original == oldest.datetime_original,
                            Photo.guid < oldest.guid,
                        ),
                    )
                )
                .limit(1)
            ).first()
            has_older = bool(older_exists) or (direction in {"initial", "older"} and has_more_in_direction)
            if has_older:
                older_cursor = b64encode_cursor(oldest.datetime_original, oldest.guid)

            # Newer = items strictly newer than the newest item on this page.
            newer_exists = session.execute(
                base.where(
                    or_(
                        Photo.datetime_original > newest.datetime_original,
                        and_(
                            Photo.datetime_original == newest.datetime_original,
                            Photo.guid > newest.guid,
                        ),
                    )
                )
                .limit(1)
            ).first()
            has_newer = bool(newer_exists) or (direction == "newer" and has_more_in_direction)
            if has_newer:
                newer_cursor = b64encode_cursor(newest.datetime_original, newest.guid)

    items = [
        {
            "guid": r.guid,
            "thumb_url": f"/phototank/thumb/{r.guid}",
            "date": r.datetime_original,
            "rating": r.rating,
        }
        for r in rows
    ]

    return templates.TemplateResponse(
        "gallery.html",
        {
            "request": request,
            "page_title": "Gallery",
            "items": items,
            "jump_date": jump_date_value,
            "limit": limit,
            "rating": rating_int,
            "tag_id": tag_id,
            "country": country_value,
            "city": city_value,
            "geo_countries": geo_countries,
            "geo_cities": geo_cities,
            "tags": all_tags,
            "has_newer": has_newer,
            "has_older": has_older,
            "newer_cursor": newer_cursor,
            "older_cursor": older_cursor,
        },
    )


@web_router.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)

    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        total_photos = int(session.execute(select(func.count()).select_from(Photo)).scalar_one())

        # Year counts based on rel_path prefix (YYYY/...). This matches the library layout.
        year_expr = func.substr(Photo.rel_path, 1, 4)
        year_rows = (
            session.execute(
                select(year_expr.label("year"), func.count().label("count"))
                .where(Photo.rel_path.op("GLOB")("[0-9][0-9][0-9][0-9]/*"))
                .group_by(year_expr)
                .order_by(year_expr.desc())
            )
            .all()
        )

        jobs = list(
            session.execute(
                select(ScanJob)
                .order_by(ScanJob.started_at.desc(), ScanJob.job_id.desc())
                .limit(200)
            ).scalars().all()
        )

    running_jobs: list[tuple[str, ScanJob]] = []
    recent_jobs: list[tuple[str, ScanJob]] = []

    for job in jobs:
        kind = _job_kind(job)
        if kind is None:
            continue
        is_running = job.state in {"queued", "running"}
        if is_running:
            running_jobs.append((kind, job))
        else:
            recent_jobs.append((kind, job))

    running_jobs.sort(key=lambda x: _job_sort_key(x[1]), reverse=True)
    recent_jobs.sort(key=lambda x: _job_sort_key(x[1]), reverse=True)

    photos_per_year = [(str(y), int(c)) for (y, c) in year_rows if y is not None]

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "title": "phototank dashboard",
            "page_title": "Dashboard",
            "total_photos": total_photos,
            "photos_per_year": photos_per_year,
            "running_jobs": running_jobs,
            "recent_jobs": recent_jobs[:20],
            "phone_sync_default_user": settings.phone_sync_ssh_user or "",
            "phone_sync_default_ip": settings.phone_sync_ip or "",
            "phone_sync_default_port": int(settings.phone_sync_port),
            "phone_sync_default_source": settings.phone_sync_source_path or "",
            "phone_sync_default_dest": settings.phone_sync_dest_path or "",
        },
    )


@web_router.post("/dashboard/import/start", response_class=HTMLResponse)
def dashboard_import_start(
    request: Request,
    ingest_mode: str | None = Form(None),
):
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)
    ensure_deriv_root(settings.deriv_root)
    ensure_import_dirs(settings.import_root, settings.failed_root)

    SessionLocal = sessionmaker_for(settings.db_path)

    mode = (ingest_mode or "move").strip().lower()
    if mode not in {"move", "copy"}:
        raise HTTPException(status_code=400, detail="ingest_mode must be 'move' or 'copy'")

    job_id = new_job_id()
    with SessionLocal() as session:
        with session.begin():
            create_job(session, job_id=job_id, year=None, job_type="ingest")
        session.commit()

    _start_job_thread(run_ingest_job, job_id, ingest_mode=mode)

    with SessionLocal() as session:
        job = _load_job_or_404(session=session, job_id=job_id)

    return templates.TemplateResponse(
        "partials/dashboard_job_status.html",
        {
            "request": request,
            "kind": "ingest",
            "job": job,
        },
    )


@web_router.get("/dashboard/import/status/{job_id}", response_class=HTMLResponse)
def dashboard_import_status(request: Request, job_id: str):
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)

    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        job = _load_job_or_404(session=session, job_id=job_id)

    return templates.TemplateResponse(
        "partials/dashboard_job_status.html",
        {
            "request": request,
            "kind": (_job_kind(job) or "ingest"),
            "job": job,
        },
    )


@web_router.post("/dashboard/validate/start", response_class=HTMLResponse)
def dashboard_validate_start(
    request: Request,
    year: str | None = Form(None),
    repair_mid_exif: bool = Form(False),
):
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)
    ensure_deriv_root(settings.deriv_root)

    year_int: int | None = None
    if year is not None:
        y = year.strip()
        if y != "":
            try:
                year_int = int(y)
            except ValueError:
                raise HTTPException(status_code=400, detail="year must be a number (YYYY)")

    if year_int is not None and (year_int < 1900 or year_int > 2100):
        raise HTTPException(status_code=400, detail="year must be between 1900 and 2100")

    SessionLocal = sessionmaker_for(settings.db_path)

    job_id = new_job_id()
    with SessionLocal() as session:
        with session.begin():
            create_job(session, job_id=job_id, year=year_int, job_type="validate")
        session.commit()

    _start_job_thread(run_validate_job, job_id, repair_mid_exif=bool(repair_mid_exif))

    with SessionLocal() as session:
        job = _load_job_or_404(session=session, job_id=job_id)

    return templates.TemplateResponse(
        "partials/dashboard_job_status.html",
        {
            "request": request,
            "kind": "validate",
            "job": job,
        },
    )


@web_router.get("/dashboard/validate/status/{job_id}", response_class=HTMLResponse)
def dashboard_validate_status(request: Request, job_id: str):
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)

    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        job = _load_job_or_404(session=session, job_id=job_id)

    return templates.TemplateResponse(
        "partials/dashboard_job_status.html",
        {
            "request": request,
            "kind": (_job_kind(job) or "validate"),
            "job": job,
        },
    )


@web_router.post("/dashboard/phone-sync/start", response_class=HTMLResponse)
def dashboard_phone_sync_start(
    request: Request,
    ip: str | None = Form(None),
    remote_source_path: str | None = Form(None),
    remote_dest_path: str | None = Form(None),
    ssh_user: str | None = Form(None),
    ssh_port: int | None = Form(None),
):
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)
    ensure_deriv_root(settings.deriv_root)

    ip_value = (ip or settings.phone_sync_ip or "").strip()
    src_value = (remote_source_path or settings.phone_sync_source_path or "").strip()
    dst_value = (remote_dest_path or settings.phone_sync_dest_path or "").strip()
    user_value = (ssh_user or settings.phone_sync_ssh_user or "").strip()
    port_value = int(ssh_port or settings.phone_sync_port)
    key_path = settings.phone_sync_ssh_key_path.expanduser()

    if not ip_value:
        raise HTTPException(status_code=400, detail="missing ip")
    if not src_value:
        raise HTTPException(status_code=400, detail="missing remote_source_path")
    if not dst_value:
        raise HTTPException(status_code=400, detail="missing remote_dest_path")
    if not user_value:
        raise HTTPException(status_code=400, detail="missing ssh_user")

    SessionLocal = sessionmaker_for(settings.db_path)

    job_id = new_job_id()
    with SessionLocal() as session:
        with session.begin():
            create_job(session, job_id=job_id, year=None, job_type="phone_sync")
        session.commit()

    _start_job_thread(
        run_phone_sync_job,
        job_id,
        ssh_user=user_value,
        phone_ip=ip_value,
        ssh_port=port_value,
        remote_source_path=src_value,
        remote_dest_path=dst_value,
        ssh_key_path=key_path,
    )

    with SessionLocal() as session:
        job = _load_job_or_404(session=session, job_id=job_id)

    return templates.TemplateResponse(
        "partials/dashboard_job_status.html",
        {
            "request": request,
            "kind": "phone_sync",
            "job": job,
        },
    )


@web_router.post("/dashboard/phone-reconcile/start", response_class=HTMLResponse)
def dashboard_phone_reconcile_start(
    request: Request,
    ip: str | None = Form(None),
    remote_dest_path: str | None = Form(None),
    ssh_user: str | None = Form(None),
    ssh_port: int | None = Form(None),
):
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)
    ensure_deriv_root(settings.deriv_root)

    ip_value = (ip or settings.phone_sync_ip or "").strip()
    dst_value = (remote_dest_path or settings.phone_sync_dest_path or "").strip()
    user_value = (ssh_user or settings.phone_sync_ssh_user or "").strip()
    port_value = int(ssh_port or settings.phone_sync_port)
    key_path = settings.phone_sync_ssh_key_path.expanduser()

    if not ip_value:
        raise HTTPException(status_code=400, detail="missing ip")
    if not dst_value:
        raise HTTPException(status_code=400, detail="missing remote_dest_path")
    if not user_value:
        raise HTTPException(status_code=400, detail="missing ssh_user")

    SessionLocal = sessionmaker_for(settings.db_path)

    job_id = new_job_id()
    with SessionLocal() as session:
        with session.begin():
            create_job(session, job_id=job_id, year=None, job_type="phone_reconcile")
        session.commit()

    _start_job_thread(
        run_phone_reconcile_job,
        job_id,
        ssh_user=user_value,
        phone_ip=ip_value,
        ssh_port=port_value,
        remote_dest_path=dst_value,
        ssh_key_path=key_path,
    )

    with SessionLocal() as session:
        job = _load_job_or_404(session=session, job_id=job_id)

    return templates.TemplateResponse(
        "partials/dashboard_job_status.html",
        {
            "request": request,
            "kind": "phone_reconcile",
            "job": job,
        },
    )


@web_router.get("/dashboard/job/status/{job_id}", response_class=HTMLResponse)
def dashboard_job_status(request: Request, job_id: str):
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)

    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        job = _load_job_or_404(session=session, job_id=job_id)

    return templates.TemplateResponse(
        "partials/dashboard_job_status.html",
        {
            "request": request,
            "kind": (_job_kind(job) or "ingest"),
            "job": job,
        },
    )


@web_router.get("/photo/{guid}", response_class=HTMLResponse)
def photo_detail(
    request: Request,
    guid: str,
    from_: str | None = Query(None, alias="from"),
    jump: str | None = Query(None, description="Filter context: ISO date/datetime"),
    start: str | None = Query(None, description="Compatibility alias for jump"),
    rating: str | None = Query(None, description="Filter context: rating 0..3"),
    tag: str | None = Query(None, description="Filter context: tag id"),
    country: str | None = Query(None, description="Filter context: geo country"),
    city: str | None = Query(None, description="Filter context: geo city"),
):
    settings = settings_or_500()
    guid = normalize_guid(guid)

    jump_for_url, rating_int, tag_ctx, country_ctx, city_ctx = _extract_filter_context(
        from_=from_,
        jump=jump,
        start=start,
        rating=rating,
        tag=tag,
        country=country,
        city=city,
    )

    tag_id: int | None = None
    if tag is not None and tag != "":
        try:
            tag_id = int(tag)
        except Exception:
            tag_id = None
    if tag_id is None:
        tag_id = tag_ctx

    country_value = (country.strip() if country is not None else None) or country_ctx
    city_value = (city.strip() if city is not None else None) or city_ctx
    city_norm = city_value.casefold() if city_value else None

    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        row = fetch_photo(session, guid)
        photo_tags = tags_for_photo(session, guid)
        all_tags = list_tags(session)

    if not row:
        raise HTTPException(status_code=404, detail="photo not found")

    # Prev/next within filter context (newest-first globally; no jump-based cutoff).
    prev_guid: str | None = None
    next_guid: str | None = None
    cur_dt = row.get("datetime_original")
    if cur_dt:
        with SessionLocal() as session:
            base = (
                select(Photo.guid, Photo.datetime_original)
                .where(Photo.datetime_original.is_not(None))
            )
            if rating_int is not None:
                base = base.where(Photo.rating == rating_int)
            if tag_id is not None:
                base = base.join(PhotoTag, PhotoTag.photo_guid == Photo.guid).where(PhotoTag.tag_id == tag_id)
            if country_value is not None:
                base = base.where(Photo.geo_country == country_value)
            if city_norm is not None:
                base = base.where(Photo.geo_city_norm == city_norm)

            next_q = base.where(
                or_(
                    Photo.datetime_original > cur_dt,
                    and_(Photo.datetime_original == cur_dt, Photo.guid > guid),
                )
            ).order_by(Photo.datetime_original.asc(), Photo.guid.asc()).limit(1)

            prev_q = base.where(
                or_(
                    Photo.datetime_original < cur_dt,
                    and_(Photo.datetime_original == cur_dt, Photo.guid < guid),
                )
            ).order_by(Photo.datetime_original.desc(), Photo.guid.desc()).limit(1)

            next_row = session.execute(next_q).first()
            prev_row = session.execute(prev_q).first()
            next_guid = str(next_row[0]) if next_row and next_row[0] else None
            prev_guid = str(prev_row[0]) if prev_row and prev_row[0] else None

    back_url = _safe_back_url(from_)

    def _detail_url(target_guid: str) -> str:
        q: dict[str, str] = {"jump": jump_for_url}
        if rating_int is not None:
            q["rating"] = str(rating_int)
        if tag_id is not None:
            q["tag"] = str(tag_id)
        if country_value is not None:
            q["country"] = country_value
        if city_value is not None:
            q["city"] = city_value
        if back_url:
            q["from"] = back_url
        return f"/phototank/photo/{target_guid}?{urlencode(q)}"

    prev_url = _detail_url(prev_guid) if prev_guid else ""
    next_url = _detail_url(next_guid) if next_guid else ""

    ctx = {
        "request": request,
        "page_title": "Detail",
        "photo": row,
        "thumb_url": f"/phototank/thumb/{guid}",
        "mid_url": f"/phototank/mid/{guid}",
        "original_url": f"/phototank/original/{guid}",
        "download_url": f"/phototank/download/original/{guid}",
        "photo_tags": photo_tags,
        "tags": all_tags,
        "tag_id": tag_id,
        "back_url": back_url,
        "prev_url": prev_url,
        "next_url": next_url,
    }

    # If HTMX is requesting, return only the panel fragment.
    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse("partials/photo_panel_htmx.html", ctx)

    # row is a dict from db.fetch_photo
    return templates.TemplateResponse("photo.html", ctx)
