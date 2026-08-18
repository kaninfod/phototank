from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from starlette.responses import FileResponse

from ..core.db import (
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
from ..services.derivatives import mid_path, thumb_path
from ..jobs import new_job_id
from ..core.models import Photo
from ..core.router_helpers import ensure_deriv_root, ensure_dirs_and_db, settings_or_500
from ..core.util import normalize_guid, resolve_relpath_under


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


class BackupStartRequest(BaseModel):
    dest: str | None = None
    ssh_user: str | None = None
    host: str | None = None
    ssh_port: int | None = Field(None, ge=1, le=65535)
    ssh_key_path: str | None = None
    dry_run: bool = False


api_router = APIRouter()

logger = logging.getLogger(__name__)


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
    """Delete photos via background job."""
    
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)
    ensure_deriv_root(settings.deriv_root)

    SessionLocal = sessionmaker_for(settings.db_path)

    logger.info("delete requested: count=%d", len(req.guids))

    # Create job
    job_id = new_job_id()
    normalized_guids = [normalize_guid(g) for g in req.guids]
    with SessionLocal() as session:
        with session.begin():
            create_job(
                session,
                job_id=job_id,
                year=None,
                job_type="delete_photos",
                params={"guids": normalized_guids},
            )
        session.commit()

    return {
        "job_id": job_id,
        "requested": len(normalized_guids),
        "status": "queued"
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
            tag = create_or_get_tag(session, name="", description="", color="", tag_id=tag_id)
            logger.info("Applied tag '%s' to %d photos", tag.name if tag else tag_id, applied)
        except Exception as e:
            logger.error("Error applying tag_id=%d to photos: %s: %s", tag_id, type(e).__name__, e)
            raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")

    return {"tag_id": int(tag_id), "requested": len(guids), "applied": int(applied), "tag": {"id": int(tag.id), "name": tag.name, "description": tag.description, "color": tag.color} if tag else None}


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
            create_job(
                session,
                job_id=job_id,
                year=None,
                job_type="phone_sync",
                params={
                    "ssh_user": ssh_user,
                    "phone_ip": ip,
                    "ssh_port": ssh_port,
                    "remote_source_path": remote_source_path,
                    "remote_dest_path": remote_dest_path,
                    "ssh_key_path": str(ssh_key_path),
                },
            )
        session.commit()

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
            create_job(
                session,
                job_id=job_id,
                year=None,
                job_type="phone_reconcile",
                params={
                    "ssh_user": ssh_user,
                    "phone_ip": ip,
                    "ssh_port": ssh_port,
                    "remote_dest_path": remote_dest_path,
                    "ssh_key_path": str(ssh_key_path),
                },
            )
        session.commit()

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


@api_router.post("/jobs/backup/start")
def start_backup(req: BackupStartRequest):
    settings = settings_or_500()
    ensure_dirs_and_db(settings.photo_root, settings.db_path)
    ensure_deriv_root(settings.deriv_root)

    dest_value = (req.dest or settings.backup_dest_path or "").strip()
    user_value = (req.ssh_user or settings.backup_ssh_user or "").strip() or None
    host_value = (req.host or settings.backup_host or "").strip() or None
    ssh_port = int(req.ssh_port or settings.backup_port)
    ssh_key_path_raw = (req.ssh_key_path or str(settings.backup_ssh_key_path)).strip()
    ssh_key_path = Path(ssh_key_path_raw).expanduser()

    if not dest_value:
        raise HTTPException(status_code=400, detail="missing backup destination")

    use_ssh = bool(user_value and host_value)
    if use_ssh and not ssh_key_path.exists():
        raise HTTPException(status_code=400, detail=f"backup ssh key not found: {ssh_key_path}")

    dest_value = dest_value.rstrip("/")
    if use_ssh:
        dest_value = f"{user_value}@{host_value}:{dest_value}"

    SessionLocal = sessionmaker_for(settings.db_path)

    job_id = new_job_id()
    with SessionLocal() as session:
        with session.begin():
            create_job(
                session,
                job_id=job_id,
                year=None,
                job_type="backup",
                params={
                    "backup_sources": [
                        str(settings.photo_root.resolve()),
                        str(settings.deriv_root.resolve()),
                        str(settings.db_path.resolve()),
                    ],
                    "dest": dest_value,
                    "ssh_user": user_value,
                    "ssh_host": host_value,
                    "ssh_port": ssh_port,
                    "ssh_key_path": str(ssh_key_path),
                    "use_ssh": use_ssh,
                    "delete": True,
                    "dry_run": bool(req.dry_run),
                },
            )
        session.commit()

    return {
        "job_id": job_id,
        "job_type": "backup",
        "state": "queued",
        "dest": dest_value,
        "use_ssh": use_ssh,
        "dry_run": bool(req.dry_run),
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
