from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

from fastapi import APIRouter, BackgroundTasks, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import ValidationError
from sqlalchemy import and_, func, or_, select

from ..config import get_settings
from ..db import create_job, engine_for, fetch_photo, get_job, init_db, sessionmaker_for
from ..jobs import new_job_id, run_import_job, run_scan_job
from ..models import Photo, ScanJob
from ..util import b64decode_cursor, b64encode_cursor, normalize_guid

web_router = APIRouter()

_templates_dir = Path(__file__).resolve().parents[1] / "templates"
templates = Jinja2Templates(directory=str(_templates_dir))


def _settings_or_500():
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


def _ensure_dirs_and_db(photo_root: Path, db_path: Path) -> None:
    if not photo_root.exists() or not photo_root.is_dir():
        raise HTTPException(status_code=400, detail=f"PHOTO_ROOT is not a directory: {photo_root}")
    db_path.parent.mkdir(parents=True, exist_ok=True)


def _ensure_deriv_root(deriv_root: Path) -> None:
    deriv_root.mkdir(parents=True, exist_ok=True)


def _ensure_import_dirs(import_root: Path, failed_root: Path) -> None:
    import_root.mkdir(parents=True, exist_ok=True)
    failed_root.mkdir(parents=True, exist_ok=True)
    if not import_root.is_dir():
        raise HTTPException(status_code=400, detail=f"IMPORT_ROOT is not a directory: {import_root}")
    if not failed_root.is_dir():
        raise HTTPException(status_code=400, detail=f"FAILED_ROOT is not a directory: {failed_root}")


def _load_job_or_404(*, session, job_id: str) -> ScanJob:
    job = get_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job




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
) -> tuple[str, int | None]:
    """Return (jump_date_for_url, rating_int).

    Preference order:
    1) explicit query params on the detail URL (jump preferred over start)
    2) parse them from the `from=` gallery URL
    3) defaults
    """
    jump_raw = jump or start
    rating_raw = rating

    if (jump_raw is None or jump_raw == "") or (rating_raw is None):
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
            except Exception:
                pass

    _, jump_date_for_url = _parse_jump_to_end_iso(jump_raw)
    rating_int = _parse_rating_int(rating_raw)
    return jump_date_for_url, rating_int


@web_router.get("/", response_class=HTMLResponse)
def gallery(
    request: Request,
    jump: str | None = Query(None, description="Jump date/datetime (ISO). e.g. 2010-01-01 or 2010-01-01T12:34:56"),
    start: str | None = Query(None, description="Compatibility alias for jump"),
    limit: int = Query(60, ge=1, le=200),
    older: str | None = Query(None, description="Keyset cursor for older page"),
    newer: str | None = Query(None, description="Keyset cursor for newer page"),
    rating: str | None = Query(None, description="Filter photos by rating (0..3)")
):
    settings = _settings_or_500()

    engine = engine_for(settings.db_path)
    init_db(engine)
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

    with SessionLocal() as session:
        base = select(Photo).where(Photo.datetime_original.is_not(None))
        if rating_int is not None:
            base = base.where(Photo.rating == rating_int)

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
            "has_newer": has_newer,
            "has_older": has_older,
            "newer_cursor": newer_cursor,
            "older_cursor": older_cursor,
        },
    )


@web_router.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    settings = _settings_or_500()
    _ensure_dirs_and_db(settings.photo_root, settings.db_path)

    engine = engine_for(settings.db_path)
    init_db(engine)
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

    photos_per_year = [(str(y), int(c)) for (y, c) in year_rows if y is not None]

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "title": "phototank dashboard",
            "page_title": "Dashboard",
            "total_photos": total_photos,
            "photos_per_year": photos_per_year,
        },
    )


@web_router.post("/dashboard/import/start", response_class=HTMLResponse)
def dashboard_import_start(request: Request, background_tasks: BackgroundTasks):
    settings = _settings_or_500()
    _ensure_dirs_and_db(settings.photo_root, settings.db_path)
    _ensure_deriv_root(settings.deriv_root)
    _ensure_import_dirs(settings.import_root, settings.failed_root)

    engine = engine_for(settings.db_path)
    init_db(engine)
    SessionLocal = sessionmaker_for(settings.db_path)

    job_id = new_job_id()
    with SessionLocal() as session:
        with session.begin():
            create_job(session, job_id=job_id, year=None)
        job = _load_job_or_404(session=session, job_id=job_id)
        job.message = "import"
        session.commit()

    background_tasks.add_task(run_import_job, job_id)

    with SessionLocal() as session:
        job = _load_job_or_404(session=session, job_id=job_id)

    return templates.TemplateResponse(
        "partials/dashboard_job_status.html",
        {
            "request": request,
            "kind": "import",
            "job": job,
        },
    )


@web_router.get("/dashboard/import/status/{job_id}", response_class=HTMLResponse)
def dashboard_import_status(request: Request, job_id: str):
    settings = _settings_or_500()
    _ensure_dirs_and_db(settings.photo_root, settings.db_path)

    engine = engine_for(settings.db_path)
    init_db(engine)
    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        job = _load_job_or_404(session=session, job_id=job_id)

    return templates.TemplateResponse(
        "partials/dashboard_job_status.html",
        {
            "request": request,
            "kind": "import",
            "job": job,
        },
    )


@web_router.post("/dashboard/scan/start", response_class=HTMLResponse)
def dashboard_scan_start(
    request: Request,
    background_tasks: BackgroundTasks,
    year: str | None = Form(None),
):
    settings = _settings_or_500()
    _ensure_dirs_and_db(settings.photo_root, settings.db_path)
    _ensure_deriv_root(settings.deriv_root)

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

    engine = engine_for(settings.db_path)
    init_db(engine)
    SessionLocal = sessionmaker_for(settings.db_path)

    job_id = new_job_id()
    with SessionLocal() as session:
        with session.begin():
            create_job(session, job_id=job_id, year=year_int)

    background_tasks.add_task(run_scan_job, job_id)

    with SessionLocal() as session:
        job = _load_job_or_404(session=session, job_id=job_id)

    return templates.TemplateResponse(
        "partials/dashboard_job_status.html",
        {
            "request": request,
            "kind": "scan",
            "job": job,
        },
    )


@web_router.get("/dashboard/scan/status/{job_id}", response_class=HTMLResponse)
def dashboard_scan_status(request: Request, job_id: str):
    settings = _settings_or_500()
    _ensure_dirs_and_db(settings.photo_root, settings.db_path)

    engine = engine_for(settings.db_path)
    init_db(engine)
    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        job = _load_job_or_404(session=session, job_id=job_id)

    return templates.TemplateResponse(
        "partials/dashboard_job_status.html",
        {
            "request": request,
            "kind": "scan",
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
):
    settings = _settings_or_500()
    guid = normalize_guid(guid)

    jump_for_url, rating_int = _extract_filter_context(from_=from_, jump=jump, start=start, rating=rating)

    engine = engine_for(settings.db_path)
    init_db(engine)
    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        row = fetch_photo(session, guid)

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
        "back_url": back_url,
        "prev_url": prev_url,
        "next_url": next_url,
    }

    # If HTMX is requesting, return only the panel fragment.
    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse("partials/photo_panel_htmx.html", ctx)

    # row is a dict from db.fetch_photo
    return templates.TemplateResponse("photo.html", ctx)
