from __future__ import annotations

import logging
import shutil
from datetime import datetime
import uuid
from pathlib import Path

from sqlalchemy.orm import Session

from .config import get_settings
from .db import engine_for, init_db, sessionmaker_for, upsert_photo
from .derivatives import ensure_derivatives, mid_path, thumb_path
from .models import ScanJob
from .scanner import build_record, extract_exif_fields, iter_photo_files


logger = logging.getLogger(__name__)


def utc_now_iso() -> str:
    # reuse scanner's format without importing it (keeps jobs module independent)
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def new_job_id() -> str:
    return uuid.uuid4().hex


def _safe_move(src: Path, dst: Path) -> Path:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dst.exists():
        shutil.move(str(src), str(dst))
        return dst

    stem = dst.stem
    suffix = dst.suffix
    for i in range(1, 10_000):
        cand = dst.with_name(f"{stem}__{i}{suffix}")
        if not cand.exists():
            shutil.move(str(src), str(cand))
            return cand
    raise RuntimeError(f"Too many name collisions for: {dst}")


def _infer_datetime_for_import(path: Path, datetime_fallback_order: list[str]) -> str | None:
    dt, *_rest = extract_exif_fields(path)
    if dt:
        return dt
    if "mtime" in datetime_fallback_order:
        try:
            return datetime.fromtimestamp(path.stat().st_mtime).replace(microsecond=0).isoformat()
        except Exception:
            return None
    return None


def _quarantine_failed(*, src_path: Path, failed_root: Path) -> Path:
    rel = src_path.name
    dst = failed_root / rel
    return _safe_move(src_path, dst)


def _cleanup_db_and_derivs(*, session: Session, guid: str, deriv_root: Path) -> None:
    # Best-effort cleanup used when an import fails after upsert.
    try:
        tpath = thumb_path(deriv_root, guid)
        mpath = mid_path(deriv_root, guid)
        try:
            tpath.unlink()
        except FileNotFoundError:
            pass
        except Exception:
            pass
        try:
            mpath.unlink()
        except FileNotFoundError:
            pass
        except Exception:
            pass
    except Exception:
        pass

    try:
        # Avoid importing Photo at module import time to keep this file lightweight.
        from .models import Photo

        photo = session.get(Photo, guid)
        if photo is not None:
            session.delete(photo)
            session.commit()
    except Exception:
        try:
            session.rollback()
        except Exception:
            pass


def run_scan_job(job_id: str) -> None:
    settings = get_settings()

    logger.info("scan job starting job_id=%s", job_id)

    engine = engine_for(settings.db_path)
    init_db(engine)
    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        job = session.get(ScanJob, job_id)
        if job is None:
            return
        job.state = "running"
        job.started_at = utc_now_iso()
        session.commit()

    exts = settings.extensions_set()
    scan_root = settings.photo_root

    year = None
    with SessionLocal() as session:
        job = session.get(ScanJob, job_id)
        if job is not None:
            year = job.year

    if year is not None:
        scan_root = (settings.photo_root / str(year)).resolve()
        if not scan_root.exists() or not scan_root.is_dir():
            with SessionLocal() as session:
                job = session.get(ScanJob, job_id)
                if job is not None:
                    job.state = "failed"
                    job.message = f"Year folder not found: {scan_root}"
                    job.finished_at = utc_now_iso()
                    session.commit()

                    logger.warning("scan job failed job_id=%s year=%s reason=missing_year_folder scan_root=%s", job_id, year, scan_root)
            return

    processed = 0
    upserted = 0
    thumbs_done = 0
    mids_done = 0
    errors = 0

    try:
        session = SessionLocal()
        try:
            job = session.get(ScanJob, job_id)
            if job is None:
                return

            for path in iter_photo_files(scan_root, exts):
                processed += 1

                try:
                    rec = build_record(
                        settings.photo_root,
                        path,
                        datetime_fallback_order=settings.datetime_fallback_order(),
                    )
                    guid = upsert_photo(session, rec)
                    upserted += 1

                    deriv = ensure_derivatives(
                        source_path=path,
                        deriv_root=settings.deriv_root,
                        guid=guid,
                        source_mtime=rec.source_mtime,
                        thumb_max=settings.thumb_max,
                        mid_max=settings.mid_max,
                        thumb_quality=settings.thumb_quality,
                        mid_quality=settings.mid_quality,
                    )
                    if deriv.thumb_created:
                        thumbs_done += 1
                    if deriv.mid_created:
                        mids_done += 1
                except Exception:
                    errors += 1
                    logger.exception("scan error job_id=%s path=%s", job_id, path)

                if processed == 1 or processed % 50 == 0:
                    job.processed = processed
                    job.upserted = upserted
                    job.thumbs_done = thumbs_done
                    job.mids_done = mids_done
                    job.errors = errors
                    session.commit()

            job.processed = processed
            job.upserted = upserted
            job.thumbs_done = thumbs_done
            job.mids_done = mids_done
            job.errors = errors
            job.state = "done"
            job.finished_at = utc_now_iso()
            session.commit()

            logger.info(
                "scan job done job_id=%s processed=%s upserted=%s thumbs_done=%s mids_done=%s errors=%s",
                job_id,
                processed,
                upserted,
                thumbs_done,
                mids_done,
                errors,
            )
        finally:
            session.close()

    except Exception as e:
        logger.exception("scan job crashed job_id=%s", job_id)
        with SessionLocal() as session:
            job = session.get(ScanJob, job_id)
            if job is not None:
                job.state = "failed"
                job.message = f"{type(e).__name__}: {e}"
                job.finished_at = utc_now_iso()
                job.processed = processed
                job.upserted = upserted
                job.thumbs_done = thumbs_done
                job.mids_done = mids_done
                job.errors = errors
                session.commit()


def run_import_job(job_id: str) -> None:
    settings = get_settings()

    logger.info(
        "import job starting job_id=%s import_root=%s failed_root=%s photo_root=%s",
        job_id,
        settings.import_root,
        settings.failed_root,
        settings.photo_root,
    )

    engine = engine_for(settings.db_path)
    init_db(engine)
    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        job = session.get(ScanJob, job_id)
        if job is None:
            return
        job.state = "running"
        job.started_at = utc_now_iso()
        session.commit()

    exts = settings.extensions_set()
    import_root = settings.import_root
    failed_root = settings.failed_root
    import_root.mkdir(parents=True, exist_ok=True)
    failed_root.mkdir(parents=True, exist_ok=True)
    settings.deriv_root.mkdir(parents=True, exist_ok=True)

    processed = 0
    upserted = 0
    thumbs_done = 0
    mids_done = 0
    errors = 0

    failed_root_resolved = failed_root.resolve()

    try:
        session = SessionLocal()
        try:
            job = session.get(ScanJob, job_id)
            if job is None:
                return

            for src_path in iter_photo_files(import_root, exts):
                try:
                    # If FAILED_ROOT lives under IMPORT_ROOT, skip quarantined items.
                    try:
                        sp = src_path.resolve()
                        if sp == failed_root_resolved or failed_root_resolved in sp.parents:
                            continue
                    except Exception:
                        pass

                    processed += 1

                    dt_iso = _infer_datetime_for_import(src_path, settings.datetime_fallback_order())
                    if not dt_iso:
                        _quarantine_failed(src_path=src_path, failed_root=failed_root)
                        errors += 1
                        logger.warning("import error job_id=%s path=%s reason=no_datetime", job_id, src_path)
                        continue

                    dt = datetime.fromisoformat(dt_iso)
                    dest_dir = settings.photo_root / f"{dt.year}" / f"{dt.month:02d}" / f"{dt.day:02d}"
                    dest_path = dest_dir / src_path.name

                    moved_path = _safe_move(src_path, dest_path)

                    guid: str | None = None
                    try:
                        rec = build_record(
                            settings.photo_root,
                            moved_path,
                            datetime_fallback_order=settings.datetime_fallback_order(),
                        )
                        guid = upsert_photo(session, rec)
                        upserted += 1

                        deriv = ensure_derivatives(
                            source_path=moved_path,
                            deriv_root=settings.deriv_root,
                            guid=guid,
                            source_mtime=rec.source_mtime,
                            thumb_max=settings.thumb_max,
                            mid_max=settings.mid_max,
                            thumb_quality=settings.thumb_quality,
                            mid_quality=settings.mid_quality,
                        )
                        if deriv.thumb_created:
                            thumbs_done += 1
                        if deriv.mid_created:
                            mids_done += 1

                        # Make each imported file durable; a later failure should not
                        # roll back earlier successful imports.
                        session.commit()

                    except Exception:
                        try:
                            session.rollback()
                        except Exception:
                            pass
                        if guid is not None:
                            _cleanup_db_and_derivs(session=session, guid=guid, deriv_root=settings.deriv_root)
                        try:
                            _safe_move(moved_path, failed_root / moved_path.name)
                        except Exception:
                            pass
                        errors += 1
                        logger.exception("import error job_id=%s src=%s moved=%s", job_id, src_path, moved_path)

                except Exception:
                    try:
                        session.rollback()
                    except Exception:
                        pass
                    try:
                        _quarantine_failed(src_path=src_path, failed_root=failed_root)
                    except Exception:
                        pass
                    errors += 1
                    logger.exception("import error job_id=%s path=%s", job_id, src_path)

                if processed == 1 or processed % 50 == 0:
                    job.processed = processed
                    job.upserted = upserted
                    job.thumbs_done = thumbs_done
                    job.mids_done = mids_done
                    job.errors = errors
                    session.commit()

            job.processed = processed
            job.upserted = upserted
            job.thumbs_done = thumbs_done
            job.mids_done = mids_done
            job.errors = errors
            job.state = "done"
            job.finished_at = utc_now_iso()
            session.commit()

            logger.info(
                "import job done job_id=%s processed=%s upserted=%s thumbs_done=%s mids_done=%s errors=%s",
                job_id,
                processed,
                upserted,
                thumbs_done,
                mids_done,
                errors,
            )

        finally:
            session.close()

    except Exception as e:
        logger.exception("import job crashed job_id=%s", job_id)
        with SessionLocal() as session:
            job = session.get(ScanJob, job_id)
            if job is not None:
                job.state = "failed"
                job.message = f"{type(e).__name__}: {e}"
                job.finished_at = utc_now_iso()
                job.processed = processed
                job.upserted = upserted
                job.thumbs_done = thumbs_done
                job.mids_done = mids_done
                job.errors = errors
                session.commit()
