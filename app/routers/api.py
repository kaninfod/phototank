from __future__ import annotations

from datetime import datetime
from pathlib import Path
import logging

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field
from pydantic import ValidationError
from sqlalchemy import and_, or_, select
from starlette.responses import FileResponse

from ..config import get_settings
from ..db import (
    count_by_prefix,
    create_job,
    engine_for,
    fetch_photo,
    get_job,
    init_db,
    sessionmaker_for,
)
from ..derivatives import mid_path, thumb_path
from ..jobs import new_job_id, run_import_job, run_scan_job
from ..models import Photo
from ..util import b64decode_cursor, b64encode_cursor, normalize_guid, resolve_relpath_under


class DeleteRequest(BaseModel):
    # Allow larger bulk deletes; UI selection can exceed 500.
    guids: list[str] = Field(..., min_length=1, max_length=2000)


class RateRequest(BaseModel):
    guid: str
    rating: int = Field(..., ge=0, le=3)

api_router = APIRouter()

logger = logging.getLogger(__name__)


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


@api_router.get("/count/{year}")
@api_router.get("/count/{year}/{month}")
@api_router.get("/count/{year}/{month}/{day}")
def count_photos(year: int, month: int | None = None, day: int | None = None):
    if month is not None and (month < 1 or month > 12):
        raise HTTPException(status_code=400, detail="month must be 1..12")
    if day is not None and (day < 1 or day > 31):
        raise HTTPException(status_code=400, detail="day must be 1..31")

    settings = _settings_or_500()
    _ensure_dirs_and_db(settings.photo_root, settings.db_path)

    engine = engine_for(settings.db_path)
    init_db(engine)
    SessionLocal = sessionmaker_for(settings.db_path)

    if month is None:
        prefix = f"{year}/"
    elif day is None:
        prefix = f"{year}/{month:02d}/"
    else:
        prefix = f"{year}/{month:02d}/{day:02d}/"

    with SessionLocal() as session:
        count = count_by_prefix(session, prefix)

    resp: dict[str, int] = {"year": year, "count": count}
    if month is not None:
        resp["month"] = month
    if day is not None:
        resp["day"] = day
    return resp


@api_router.post("/scan")
def scan_photos(background_tasks: BackgroundTasks, year: int | None = None):
    settings = _settings_or_500()
    _ensure_dirs_and_db(settings.photo_root, settings.db_path)
    _ensure_deriv_root(settings.deriv_root)

    if year is not None and (year < 1900 or year > 2100):
        raise HTTPException(status_code=400, detail="year must be between 1900 and 2100")

    engine = engine_for(settings.db_path)
    init_db(engine)
    SessionLocal = sessionmaker_for(settings.db_path)

    job_id = new_job_id()
    with SessionLocal() as session:
        with session.begin():
            create_job(session, job_id=job_id, year=year)

    background_tasks.add_task(run_scan_job, job_id)
    logger.info("queued scan job_id=%s year=%s", job_id, year)
    return {"job_id": job_id, "state": "queued", "year": year}


@api_router.post("/import")
def import_photos(background_tasks: BackgroundTasks):
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
        job = get_job(session, job_id)
        if job is not None:
            job.message = "import"
            session.commit()

    background_tasks.add_task(run_import_job, job_id)
    logger.info("queued import job_id=%s import_root=%s failed_root=%s", job_id, settings.import_root, settings.failed_root)
    return {"job_id": job_id, "state": "queued"}


@api_router.get("/scan/{job_id}")
def scan_status(job_id: str):
    settings = _settings_or_500()
    _ensure_dirs_and_db(settings.photo_root, settings.db_path)

    engine = engine_for(settings.db_path)
    init_db(engine)
    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        job = get_job(session, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")

        return {
            "job_id": job.job_id,
            "state": job.state,
            "year": job.year,
            "processed": job.processed,
            "upserted": job.upserted,
            "thumbs_done": job.thumbs_done,
            "mids_done": job.mids_done,
            "errors": job.errors,
            "started_at": job.started_at,
            "finished_at": job.finished_at,
            "message": job.message,
        }


@api_router.get("/import/{job_id}")
def import_status(job_id: str):
    settings = _settings_or_500()
    _ensure_dirs_and_db(settings.photo_root, settings.db_path)

    engine = engine_for(settings.db_path)
    init_db(engine)
    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        job = get_job(session, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")

        return {
            "job_id": job.job_id,
            "state": job.state,
            "processed": job.processed,
            "upserted": job.upserted,
            "thumbs_done": job.thumbs_done,
            "mids_done": job.mids_done,
            "errors": job.errors,
            "started_at": job.started_at,
            "finished_at": job.finished_at,
            "message": job.message,
        }


@api_router.get("/info/{guid}")
def photo_info(guid: str):
    settings = _settings_or_500()
    _ensure_dirs_and_db(settings.photo_root, settings.db_path)

    guid = normalize_guid(guid)

    engine = engine_for(settings.db_path)
    init_db(engine)
    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        row = fetch_photo(session, guid)

    if not row:
        raise HTTPException(status_code=404, detail="photo not found")

    return row


@api_router.get("/thumb/{guid}")
def get_thumb(guid: str):
    settings = _settings_or_500()
    guid = normalize_guid(guid)
    p = thumb_path(settings.deriv_root, guid)
    if not p.exists():
        raise HTTPException(status_code=404, detail="thumb not found")
    return FileResponse(p)


@api_router.get("/mid/{guid}")
def get_mid(guid: str):
    settings = _settings_or_500()
    guid = normalize_guid(guid)
    p = mid_path(settings.deriv_root, guid)
    if not p.exists():
        raise HTTPException(status_code=404, detail="mid not found")
    return FileResponse(p)


@api_router.get("/original/{guid}")
def get_original(guid: str):
    settings = _settings_or_500()
    _ensure_dirs_and_db(settings.photo_root, settings.db_path)

    guid = normalize_guid(guid)

    engine = engine_for(settings.db_path)
    init_db(engine)
    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        row = fetch_photo(session, guid)

    if not row:
        raise HTTPException(status_code=404, detail="photo not found")

    rel_path = row.get("rel_path")
    if not rel_path:
        raise HTTPException(status_code=404, detail="original path not found")

    source_path = resolve_relpath_under(settings.photo_root, rel_path)
    if not source_path.exists():
        raise HTTPException(status_code=404, detail="original not found")

    return FileResponse(source_path)


@api_router.get("/photos")
def get_photos(
    start: datetime = Query(..., description="Start datetime (ISO 8601). e.g. 2010-01-01 or 2010-01-01T00:00:00"),
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = Query(None, description="Opaque pagination cursor returned by the previous call"),
    rating: str | None = Query(None, description="If provided, only return photos with this rating (0..3)"),
):
    """Cursor pagination ordered by (datetime_original, guid) ascending.

    Returns only rows that have datetime_original.
    """

    settings = _settings_or_500()
    _ensure_dirs_and_db(settings.photo_root, settings.db_path)

    engine = engine_for(settings.db_path)
    init_db(engine)
    SessionLocal = sessionmaker_for(settings.db_path)

    start_iso = start.replace(microsecond=0).isoformat()

    cursor_dt = None
    cursor_guid = None
    if cursor:
        cursor_dt, cursor_guid = b64decode_cursor(cursor)

    rating_int: int | None = None
    if rating is not None and rating != "":
        try:
            rating_int = int(rating)
        except ValueError:
            raise HTTPException(status_code=400, detail="rating must be 0..3")
        if rating_int < 0 or rating_int > 3:
            raise HTTPException(status_code=400, detail="rating must be 0..3")

    with SessionLocal() as session:
        q = (
            select(Photo)
            .where(Photo.datetime_original.is_not(None))
            .where(Photo.datetime_original >= start_iso)
        )

        if rating_int is not None:
            q = q.where(Photo.rating == rating_int)

        if cursor_dt and cursor_guid:
            q = q.where(
                or_(
                    Photo.datetime_original > cursor_dt,
                    and_(Photo.datetime_original == cursor_dt, Photo.guid > cursor_guid),
                )
            )

        q = q.order_by(Photo.datetime_original.asc(), Photo.guid.asc()).limit(limit + 1)
        rows = session.execute(q).scalars().all()

    has_more = len(rows) > limit
    rows = rows[:limit]

    items = []
    for r in rows:
        items.append(
            {
                "guid": r.guid,
                "path": f"/phototank/thumb/{r.guid}",
                "date": r.datetime_original,
                "rating": r.rating,
                "location": (
                    {"lat": r.gps_latitude, "lon": r.gps_longitude}
                    if (r.gps_latitude is not None and r.gps_longitude is not None)
                    else None
                ),
            }
        )

    next_cursor = None
    if has_more and rows:
        last = rows[-1]
        next_cursor = b64encode_cursor(last.datetime_original, last.guid)

    return {"start": start_iso, "limit": limit, "items": items, "next_cursor": next_cursor}


@api_router.post("/rate")
def rate_photo(req: RateRequest):
    settings = _settings_or_500()
    _ensure_dirs_and_db(settings.photo_root, settings.db_path)

    guid = normalize_guid(req.guid)

    engine = engine_for(settings.db_path)
    init_db(engine)
    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        row = session.get(Photo, guid)
        if row is None:
            raise HTTPException(status_code=404, detail="photo not found")
        row.rating = int(req.rating)
        session.commit()

    return {"guid": guid, "rating": int(req.rating)}


@api_router.get("/get_photos/{startdate}/{count_per_page}")
def get_photos_legacy(
    startdate: str,
    count_per_page: int,
    cursor: str | None = Query(None, description="Opaque pagination cursor returned by the previous call"),
):
    # Legacy compatibility: startdate=YYYYMMDD
    if len(startdate) != 8 or not startdate.isdigit():
        raise HTTPException(status_code=400, detail="startdate must be YYYYMMDD")

    start = datetime(
        year=int(startdate[0:4]),
        month=int(startdate[4:6]),
        day=int(startdate[6:8]),
        hour=0,
        minute=0,
        second=0,
    )

    return get_photos(start=start, limit=count_per_page, cursor=cursor)


@api_router.post("/delete")
def delete_photos(req: DeleteRequest):
    """Delete photos everywhere: source file, derivatives, and DB record."""

    settings = _settings_or_500()
    _ensure_dirs_and_db(settings.photo_root, settings.db_path)
    _ensure_deriv_root(settings.deriv_root)

    engine = engine_for(settings.db_path)
    init_db(engine)
    SessionLocal = sessionmaker_for(settings.db_path)

    logger.info("delete requested: count=%d", len(req.guids))

    requested = [normalize_guid(g) for g in req.guids]

    deleted: list[str] = []
    not_found: list[str] = []
    errors: list[dict[str, str]] = []

    with SessionLocal() as session:
        for guid in requested:
            photo = session.get(Photo, guid)
            if photo is None:
                not_found.append(guid)
                continue

            # Resolve filesystem paths.
            source_path = resolve_relpath_under(settings.photo_root, photo.rel_path)
            tpath = thumb_path(settings.deriv_root, guid)
            mpath = mid_path(settings.deriv_root, guid)

            # Delete derivatives first.
            try:
                try:
                    tpath.unlink()
                except FileNotFoundError:
                    pass
                try:
                    mpath.unlink()
                except FileNotFoundError:
                    pass

                try:
                    source_path.unlink()
                except FileNotFoundError:
                    # Treat missing source as already-deleted; still remove DB row.
                    pass
            except Exception as e:
                errors.append({"guid": guid, "error": f"{type(e).__name__}: {e}"})
                session.rollback()
                continue

            try:
                session.delete(photo)
                session.commit()
                deleted.append(guid)
            except Exception as e:
                session.rollback()
                errors.append({"guid": guid, "error": f"DB delete failed: {type(e).__name__}: {e}"})

    logger.info(
        "delete finished: requested=%d deleted=%d not_found=%d errors=%d",
        len(requested),
        len(deleted),
        len(not_found),
        len(errors),
    )

    return {
        "requested": len(requested),
        "deleted": len(deleted),
        "deleted_guids": deleted,
        "not_found": not_found,
        "errors": errors,
    }
