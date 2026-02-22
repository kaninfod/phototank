from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
import threading
from typing import Any, Optional

from sqlalchemy import Engine, event, func, select, text
from sqlalchemy import delete
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine

from .models import Base, Photo, PhotoTag, ScanJob, Tag
from ..services.scanner import PhotoRecord


_ENGINES: dict[str, Engine] = {}
_SESSIONMAKERS: dict[str, sessionmaker] = {}
_INIT_LOCK = threading.Lock()
_INIT_DONE: set[str] = set()


def _db_key_for_engine(engine: Engine) -> str:
    # Use a stable key for the database backing this Engine.
    # For SQLite file URLs this will include the full path.
    try:
        return str(engine.url)
    except Exception:
        return f"engine:{id(engine)}"


def _sqlite_url(db_path: Path) -> str:
    # sqlite:////absolute/path on POSIX; sqlite:///relative/path also works.
    p = db_path.expanduser().resolve()
    return f"sqlite:///{p.as_posix()}"


def engine_for(db_path: Path) -> Engine:
    key = str(db_path.expanduser().resolve())
    engine = _ENGINES.get(key)
    if engine is not None:
        return engine

    db_path.parent.mkdir(parents=True, exist_ok=True)

    engine = create_engine(
        _sqlite_url(db_path),
        future=True,
    )

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:  # type: ignore[no-redef]
        cur = dbapi_connection.cursor()
        try:
            cur.execute("PRAGMA busy_timeout=5000;")
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA foreign_keys=ON;")
        finally:
            cur.close()

    _ENGINES[key] = engine
    return engine


def sessionmaker_for(db_path: Path) -> sessionmaker:
    key = str(db_path.expanduser().resolve())
    sm = _SESSIONMAKERS.get(key)
    if sm is not None:
        return sm

    engine = engine_for(db_path)
    sm = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    _SESSIONMAKERS[key] = sm
    return sm


def init_db(engine: Engine) -> None:
    key = _db_key_for_engine(engine)
    with _INIT_LOCK:
        if key in _INIT_DONE:
            return

        Base.metadata.create_all(engine)

        _INIT_DONE.add(key)


def upsert_photo(session: Session, rec: PhotoRecord) -> str:
    # Keep guid stable for an existing rel_path.
    values = asdict(rec)
    # New rows should default to rating=0 (and existing rows should retain their rating).
    values.setdefault("rating", 0)

    stmt = insert(Photo).values(**values)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Photo.rel_path],
        set_={
            "datetime_original": stmt.excluded.datetime_original,
            "gps_altitude": stmt.excluded.gps_altitude,
            "gps_latitude": stmt.excluded.gps_latitude,
            "gps_longitude": stmt.excluded.gps_longitude,
            "camera_make": stmt.excluded.camera_make,
            "file_size": stmt.excluded.file_size,
            "source_mtime": stmt.excluded.source_mtime,
            "width": stmt.excluded.width,
            "height": stmt.excluded.height,
            "user_comment": stmt.excluded.user_comment,
            "indexed_at": stmt.excluded.indexed_at,
            "exif_error": stmt.excluded.exif_error,
        },
    )
    # Return the guid actually stored for this rel_path (existing or newly inserted).
    try:
        guid_row = session.execute(stmt.returning(Photo.guid)).first()
        if guid_row and guid_row[0]:
            return str(guid_row[0])
    except Exception:
        pass

    session.execute(stmt)
    existing = session.execute(select(Photo.guid).where(Photo.rel_path == rec.rel_path)).scalar_one()
    return str(existing)


def create_job(session: Session, *, job_id: str, year: int | None, job_type: str | None = None) -> None:
    session.add(
        ScanJob(
            job_id=job_id,
            state="queued",
            job_type=(job_type.strip().lower() if job_type else None),
            year=year,
            processed=0,
            upserted=0,
            thumbs_done=0,
            mids_done=0,
            errors=0,
            started_at=None,
            finished_at=None,
            message=None,
        )
    )


def get_job(session: Session, job_id: str) -> ScanJob | None:
    return session.get(ScanJob, job_id)


def fetch_photo(session: Session, guid: str) -> Optional[dict[str, Any]]:
    row = session.execute(select(Photo).where(Photo.guid == guid)).scalar_one_or_none()
    if row is None:
        return None

    return {
        "guid": row.guid,
        "rel_path": row.rel_path,
        "datetime_original": row.datetime_original,
        "gps_altitude": row.gps_altitude,
        "gps_latitude": row.gps_latitude,
        "gps_longitude": row.gps_longitude,
        "camera_make": row.camera_make,
        "file_size": row.file_size,
        "width": row.width,
        "height": row.height,
        "user_comment": row.user_comment,
        "rating": row.rating,
        "indexed_at": row.indexed_at,
        "exif_error": row.exif_error,
        "geo_display_name": row.geo_display_name,
        "geo_lookup_status": row.geo_lookup_status,
        "geo_lookup_error": row.geo_lookup_error,
    }


def count_by_prefix(session: Session, prefix: str) -> int:
    q = select(func.count()).select_from(Photo).where(Photo.rel_path.like(prefix + "%"))
    return int(session.execute(q).scalar_one())


def normalize_tag_name(raw: str) -> tuple[str, str]:
    name = (raw or "").strip()
    if not name:
        raise ValueError("tag name cannot be empty")

    # Collapse internal whitespace.
    name = " ".join(name.split())

    # A conservative limit to keep UI tidy.
    if len(name) > 80:
        raise ValueError("tag name too long (max 80)")

    name_norm = name.casefold()
    return name, name_norm


def list_tags(session: Session) -> list[Tag]:
    return list(session.execute(select(Tag).order_by(Tag.name_norm.asc())).scalars().all())


def create_or_get_tag(
    session: Session,
    *,
    name: str,
    description: str | None,
    color: str,
) -> Tag:
    display, norm = normalize_tag_name(name)
    color = (color or "primary").strip().lower()

    if color not in {"primary", "secondary", "success", "danger", "warning", "info", "dark"}:
        raise ValueError("invalid tag color")

    existing = session.execute(select(Tag).where(Tag.name_norm == norm)).scalar_one_or_none()
    if existing is not None:
        return existing

    tag = Tag(
        name=display,
        name_norm=norm,
        description=(description.strip() if description and description.strip() else None),
        color=color,
    )
    session.add(tag)
    session.commit()
    session.refresh(tag)
    return tag


def tags_for_photo(session: Session, guid: str) -> list[Tag]:
    q = (
        select(Tag)
        .join(PhotoTag, PhotoTag.tag_id == Tag.id)
        .where(PhotoTag.photo_guid == guid)
        .order_by(Tag.name_norm.asc())
    )
    return list(session.execute(q).scalars().all())


def apply_tag_to_photos(session: Session, *, tag_id: int, guids: list[str]) -> int:
    if not guids:
        return 0

    values = [{"photo_guid": g, "tag_id": int(tag_id)} for g in guids]
    stmt = insert(PhotoTag).values(values).on_conflict_do_nothing(index_elements=[PhotoTag.photo_guid, PhotoTag.tag_id])
    res = session.execute(stmt)
    session.commit()
    try:
        return int(res.rowcount or 0)
    except Exception:
        return 0


def remove_tag_from_photos(session: Session, *, tag_id: int, guids: list[str]) -> int:
    if not guids:
        return 0
    try:
        stmt = delete(PhotoTag).where(PhotoTag.tag_id == int(tag_id)).where(PhotoTag.photo_guid.in_(guids))
        res = session.execute(stmt)
        n = int(res.rowcount or 0)
    except Exception:
        session.rollback()
        raise
    session.commit()
    return n
