from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from starlette.responses import FileResponse

from ..db import (
    apply_tag_to_photos,
    create_or_get_tag,
    fetch_photo,
    list_tags,
    remove_tag_from_photos,
    sessionmaker_for,
    tags_for_photo,
)
from ..derivatives import mid_path, thumb_path
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

api_router = APIRouter()

logger = logging.getLogger(__name__)


@api_router.get("/thumb/{guid}")
def get_thumb(guid: str):
    settings = settings_or_500()
    guid = normalize_guid(guid)
    p = thumb_path(settings.deriv_root, guid)
    if not p.exists():
        raise HTTPException(status_code=404, detail="thumb not found")
    return FileResponse(p)


@api_router.get("/mid/{guid}")
def get_mid(guid: str):
    settings = settings_or_500()
    guid = normalize_guid(guid)
    p = mid_path(settings.deriv_root, guid)
    if not p.exists():
        raise HTTPException(status_code=404, detail="mid not found")
    return FileResponse(p)


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
