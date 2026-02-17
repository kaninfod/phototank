from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import Engine, event, func, select, text
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine

from .models import Base, Photo, ScanJob
from .scanner import PhotoRecord


_ENGINES: dict[str, Engine] = {}
_SESSIONMAKERS: dict[str, sessionmaker] = {}


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
    Base.metadata.create_all(engine)

    # Minimal migrations for existing SQLite files (no Alembic yet).
    with engine.begin() as conn:
        cols = {
            row[1]
            for row in conn.execute(text("PRAGMA table_info('photos')")).fetchall()
        }
        if "source_mtime" not in cols:
            conn.execute(text("ALTER TABLE photos ADD COLUMN source_mtime INTEGER"))

        if "rating" not in cols:
            # Default rating for existing rows is 0.
            conn.execute(text("ALTER TABLE photos ADD COLUMN rating INTEGER NOT NULL DEFAULT 0"))

        # Indexes for fast timeline pagination and prev/next within filters.
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_photos_dt_guid "
                "ON photos(datetime_original, guid) "
                "WHERE datetime_original IS NOT NULL"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_photos_rating_dt_guid "
                "ON photos(rating, datetime_original, guid) "
                "WHERE datetime_original IS NOT NULL"
            )
        )


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


def create_job(session: Session, *, job_id: str, year: int | None) -> None:
    session.add(
        ScanJob(
            job_id=job_id,
            state="queued",
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
    }


def count_by_prefix(session: Session, prefix: str) -> int:
    q = select(func.count()).select_from(Photo).where(Photo.rel_path.like(prefix + "%"))
    return int(session.execute(q).scalar_one())
