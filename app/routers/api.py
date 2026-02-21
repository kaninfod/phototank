from __future__ import annotations

import logging
import threading
from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from starlette.responses import FileResponse

from ..db import (
    apply_tag_to_photos,
    create_job,
    create_or_get_tag,
    fetch_photo,
    get_job,
    list_tags,
    remove_tag_from_photos,
    sessionmaker_for,
    tags_for_photo,
)
from ..derivatives import mid_path, thumb_path
from ..jobs import new_job_id, run_phone_reconcile_job, run_phone_sync_job
from ..models import Photo
from ..router_helpers import ensure_deriv_root, ensure_dirs_and_db, settings_or_500
from ..util import normalize_guid, resolve_relpath_under


class DeleteRequest(BaseModel):
    # Allow larger bulk deletes; UI selection can exceed 500.
    guids: list[str] = Field(..., min_length=1, max_length=2000)


class RateRequest(BaseModel):
    guid: str
    rating: int = Field(..., ge=0, le=3)


class TagCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=80)
    description: str | None = Field(None, max_length=500)
    color: str = Field("primary")


class TagApplyRequest(BaseModel):
    guids: list[str] = Field(..., min_length=1, max_length=2000)


class PhoneSyncStartRequest(BaseModel):
    ip: str | None = None
    remote_source_path: str | None = None
    remote_dest_path: str | None = None
    ssh_user: str | None = None
    ssh_port: int | None = Field(None, ge=1, le=65535)
    ssh_key_path: str | None = None


class PhoneReconcileStartRequest(BaseModel):
    ip: str | None = None
    remote_dest_path: str | None = None
    ssh_user: str | None = None
    ssh_port: int | None = Field(None, ge=1, le=65535)
    ssh_key_path: str | None = None

api_router = APIRouter()

logger = logging.getLogger(__name__)


def _start_job_thread(target, /, *args, **kwargs) -> None:
    t = threading.Thread(target=target, args=args, kwargs=kwargs, daemon=True)
    t.start()


@api_router.get("/thumb/{guid}")
def get_thumb(guid: str):
    settings = settings_or_500()
    guid = normalize_guid(guid)
    p = thumb_path(settings.deriv_root, guid)
    if not p.exists():
        raise HTTPException(status_code=404, detail="thumb not found")
    return FileResponse(p, media_type="image/webp")


@api_router.get("/mid/{guid}")
def get_mid(guid: str):
    settings = settings_or_500()
    guid = normalize_guid(guid)
    p = mid_path(settings.deriv_root, guid)
    if not p.exists():
        raise HTTPException(status_code=404, detail="mid not found")
    return FileResponse(p, media_type="image/webp")


@api_router.get("/original/{guid}")
def get_original(guid: str):
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)

    guid = normalize_guid(guid)

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


@api_router.get("/download/original/{guid}")
def download_original(guid: str):
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)

    guid = normalize_guid(guid)

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

    # Force a download. Use GUID filename so the edited file can be re-imported
    # as a "replace this GUID" operation without needing to rename it.
    return FileResponse(source_path, filename=f"{guid}{source_path.suffix}")


@api_router.post("/rate")
def rate_photo(req: RateRequest):
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)

    guid = normalize_guid(req.guid)

    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        row = session.get(Photo, guid)
        if row is None:
            raise HTTPException(status_code=404, detail="photo not found")
        row.rating = int(req.rating)
        session.commit()

    return {"guid": guid, "rating": int(req.rating)}


@api_router.post("/delete")
def delete_photos(req: DeleteRequest):
    """Delete photos everywhere: source file, derivatives, and DB record."""

    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)
    ensure_deriv_root(settings.deriv_root)

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


@api_router.get("/tags")
def get_tags():
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)

    SessionLocal = sessionmaker_for(settings.db_path)
    with SessionLocal() as session:
        tags = list_tags(session)

    return {
        "tags": [
            {"id": int(t.id), "name": t.name, "description": t.description, "color": t.color}
            for t in tags
        ]
    }


@api_router.post("/tags")
def create_tag(req: TagCreateRequest):
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)

    SessionLocal = sessionmaker_for(settings.db_path)
    with SessionLocal() as session:
        try:
            tag = create_or_get_tag(
                session,
                name=req.name,
                description=req.description,
                color=req.color,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    return {"id": int(tag.id), "name": tag.name, "description": tag.description, "color": tag.color}


@api_router.get("/photo/{guid}/tags")
def get_photo_tags(guid: str):
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)

    guid = normalize_guid(guid)
    SessionLocal = sessionmaker_for(settings.db_path)
    with SessionLocal() as session:
        tags = tags_for_photo(session, guid)

    return {"guid": guid, "tags": [{"id": int(t.id), "name": t.name, "color": t.color} for t in tags]}


@api_router.post("/tags/{tag_id}/apply")
def apply_tag(tag_id: int, req: TagApplyRequest):
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)

    guids = [normalize_guid(g) for g in req.guids]
    SessionLocal = sessionmaker_for(settings.db_path)
    with SessionLocal() as session:
        try:
            applied = apply_tag_to_photos(session, tag_id=int(tag_id), guids=guids)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")

    return {"tag_id": int(tag_id), "requested": len(guids), "applied": int(applied)}


@api_router.post("/tags/{tag_id}/remove")
def remove_tag(tag_id: int, req: TagApplyRequest):
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)

    guids = [normalize_guid(g) for g in req.guids]
    SessionLocal = sessionmaker_for(settings.db_path)
    with SessionLocal() as session:
        try:
            removed = remove_tag_from_photos(session, tag_id=int(tag_id), guids=guids)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")

    return {"tag_id": int(tag_id), "requested": len(guids), "removed": int(removed)}


@api_router.post("/jobs/phone-sync/start")
def start_phone_sync(req: PhoneSyncStartRequest):
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)
    ensure_deriv_root(settings.deriv_root)

    ip = (req.ip or settings.phone_sync_ip or "").strip()
    remote_source_path = (req.remote_source_path or settings.phone_sync_source_path or "").strip()
    remote_dest_path = (req.remote_dest_path or settings.phone_sync_dest_path or "").strip()
    ssh_user = (req.ssh_user or settings.phone_sync_ssh_user or "").strip()
    ssh_port = int(req.ssh_port or settings.phone_sync_port)
    ssh_key_path_raw = (req.ssh_key_path or str(settings.phone_sync_ssh_key_path)).strip()

    if not ip:
        raise HTTPException(status_code=400, detail="missing ip")
    if not remote_source_path:
        raise HTTPException(status_code=400, detail="missing remote_source_path")
    if not remote_dest_path:
        raise HTTPException(status_code=400, detail="missing remote_dest_path")
    if not ssh_user:
        raise HTTPException(status_code=400, detail="missing ssh_user")

    ssh_key_path = Path(ssh_key_path_raw).expanduser()

    SessionLocal = sessionmaker_for(settings.db_path)
    job_id = new_job_id()
    with SessionLocal() as session:
        with session.begin():
            create_job(session, job_id=job_id, year=None, job_type="phone_sync")
        session.commit()

    _start_job_thread(
        run_phone_sync_job,
        job_id,
        ssh_user=ssh_user,
        phone_ip=ip,
        ssh_port=ssh_port,
        remote_source_path=remote_source_path,
        remote_dest_path=remote_dest_path,
        ssh_key_path=ssh_key_path,
    )

    return {
        "job_id": job_id,
        "job_type": "phone_sync",
        "state": "queued",
        "ip": ip,
        "remote_source_path": remote_source_path,
        "remote_dest_path": remote_dest_path,
        "ssh_user": ssh_user,
        "ssh_port": ssh_port,
        "ssh_key_path": str(ssh_key_path),
    }


@api_router.post("/jobs/phone-reconcile/start")
def start_phone_reconcile(req: PhoneReconcileStartRequest):
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)
    ensure_deriv_root(settings.deriv_root)

    ip = (req.ip or settings.phone_sync_ip or "").strip()
    remote_dest_path = (req.remote_dest_path or settings.phone_sync_dest_path or "").strip()
    ssh_user = (req.ssh_user or settings.phone_sync_ssh_user or "").strip()
    ssh_port = int(req.ssh_port or settings.phone_sync_port)
    ssh_key_path_raw = (req.ssh_key_path or str(settings.phone_sync_ssh_key_path)).strip()

    if not ip:
        raise HTTPException(status_code=400, detail="missing ip")
    if not remote_dest_path:
        raise HTTPException(status_code=400, detail="missing remote_dest_path")
    if not ssh_user:
        raise HTTPException(status_code=400, detail="missing ssh_user")

    ssh_key_path = Path(ssh_key_path_raw).expanduser()

    SessionLocal = sessionmaker_for(settings.db_path)
    job_id = new_job_id()
    with SessionLocal() as session:
        with session.begin():
            create_job(session, job_id=job_id, year=None, job_type="phone_reconcile")
        session.commit()

    _start_job_thread(
        run_phone_reconcile_job,
        job_id,
        ssh_user=ssh_user,
        phone_ip=ip,
        ssh_port=ssh_port,
        remote_dest_path=remote_dest_path,
        ssh_key_path=ssh_key_path,
    )

    return {
        "job_id": job_id,
        "job_type": "phone_reconcile",
        "state": "queued",
        "ip": ip,
        "remote_dest_path": remote_dest_path,
        "ssh_user": ssh_user,
        "ssh_port": ssh_port,
        "ssh_key_path": str(ssh_key_path),
    }


@api_router.get("/jobs/{job_id}")
def get_job_status(job_id: str):
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)

    SessionLocal = sessionmaker_for(settings.db_path)
    with SessionLocal() as session:
        job = get_job(session, job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="job not found")

    return {
        "job_id": job.job_id,
        "job_type": job.job_type,
        "state": job.state,
        "year": job.year,
        "processed": int(job.processed),
        "upserted": int(job.upserted),
        "thumbs_done": int(job.thumbs_done),
        "mids_done": int(job.mids_done),
        "errors": int(job.errors),
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "message": job.message,
    }
