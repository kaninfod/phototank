from __future__ import annotations

import logging
import os
import shutil
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from ...core.config import get_settings
from ...core.db import sessionmaker_for, upsert_photo
from ...services.derivatives import ensure_derivatives, mid_path, thumb_path
from ...services.geocode import enrich_photo_location
from ...core.models import ScanJob
from ...services.scanner import build_record, extract_exif_fields, iter_photo_files, try_datetime_from_filename
from ...core.util import normalize_guid, resolve_relpath_under
from ..job_helpers import commit_with_retry


logger = logging.getLogger(__name__)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


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


def _safe_copy(src: Path, dst: Path) -> Path:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dst.exists():
        shutil.copy2(str(src), str(dst))
        return dst

    stem = dst.stem
    suffix = dst.suffix
    for i in range(1, 10_000):
        cand = dst.with_name(f"{stem}__{i}{suffix}")
        if not cand.exists():
            shutil.copy2(str(src), str(cand))
            return cand
    raise RuntimeError(f"Too many name collisions for: {dst}")


def _place_into_library(*, src_path: Path, dest_path: Path, ingest_mode: str) -> Path:
    if ingest_mode == "copy":
        return _safe_copy(src_path, dest_path)
    if ingest_mode == "move":
        return _safe_move(src_path, dest_path)
    raise ValueError("ingest_mode must be 'move' or 'copy'")


def _maybe_guid_from_filename(path: Path) -> str | None:
    s = path.stem.strip().lower().replace("-", "")
    if len(s) != 32:
        return None
    if any(c not in "0123456789abcdef" for c in s):
        return None
    return s


def _replace_into_library(*, src_path: Path, dest_path: Path, ingest_mode: str) -> Path:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest_path.with_name(f".{dest_path.name}.incoming")

    try:
        tmp.unlink()
    except FileNotFoundError:
        pass
    except Exception:
        pass

    if ingest_mode == "copy":
        shutil.copy2(str(src_path), str(tmp))
    elif ingest_mode == "move":
        shutil.move(str(src_path), str(tmp))
    else:
        raise ValueError("ingest_mode must be 'move' or 'copy'")

    os.replace(str(tmp), str(dest_path))
    return dest_path


def _infer_datetime_for_import(path: Path, datetime_fallback_order: list[str]) -> str | None:
    dt, *_rest = extract_exif_fields(path)
    if dt:
        return dt
    if "filename" in datetime_fallback_order:
        dt = try_datetime_from_filename(path)
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


def _quarantine_failed_copy(*, src_path: Path, failed_root: Path) -> Path:
    rel = src_path.name
    dst = failed_root / rel
    return _safe_copy(src_path, dst)


def _cleanup_db_and_derivs(*, session: Session, guid: str, deriv_root: Path) -> None:
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
        from ...core.models import Photo

        photo = session.get(Photo, guid)
        if photo is not None:
            session.delete(photo)
            session.commit()
    except Exception:
        try:
            session.rollback()
        except Exception:
            pass


def run_ingest_job(
    job_id: str,
    *,
    ingest_mode: str = "move",
    import_root_override: Path | None = None,
    failed_root_override: Path | None = None,
    manage_job_state: bool = True,
) -> dict[str, object]:
    settings = get_settings()

    if ingest_mode not in {"move", "copy"}:
        raise ValueError("ingest_mode must be 'move' or 'copy'")

    logger.info(
        "ingest job starting job_id=%s ingest_mode=%s import_root=%s failed_root=%s photo_root=%s",
        job_id,
        ingest_mode,
        settings.import_root,
        settings.failed_root,
        settings.photo_root,
    )

    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        job = session.get(ScanJob, job_id)
        if job is None:
            return {
                "processed": 0,
                "upserted": 0,
                "thumbs_done": 0,
                "mids_done": 0,
                "errors": 0,
                "inserted_guids": [],
            }
        if manage_job_state:
            job.state = "running"
            job.started_at = utc_now_iso()
        commit_with_retry(session, label="ingest-start", logger=logger)

    exts = settings.extensions_set()
    import_root = (import_root_override or settings.import_root).resolve()
    failed_root = (failed_root_override or settings.failed_root).resolve()
    import_root.mkdir(parents=True, exist_ok=True)
    failed_root.mkdir(parents=True, exist_ok=True)
    settings.deriv_root.mkdir(parents=True, exist_ok=True)

    processed = 0
    upserted = 0
    thumbs_done = 0
    mids_done = 0
    errors = 0
    inserted_guids: set[str] = set()

    failed_root_resolved = failed_root.resolve()

    try:
        session = SessionLocal()
        try:
            job = session.get(ScanJob, job_id)
            if job is None:
                return {
                    "processed": 0,
                    "upserted": 0,
                    "thumbs_done": 0,
                    "mids_done": 0,
                    "errors": 0,
                    "inserted_guids": [],
                }

            for src_path in iter_photo_files(import_root, exts):
                try:
                    try:
                        sp = src_path.resolve()
                        if sp == failed_root_resolved or failed_root_resolved in sp.parents:
                            continue
                    except Exception:
                        pass

                    processed += 1

                    guid_in_name = _maybe_guid_from_filename(src_path)
                    if guid_in_name:
                        try:
                            guid_in_name = normalize_guid(guid_in_name)
                        except Exception:
                            guid_in_name = None

                    if guid_in_name:
                        from ...core.models import Photo

                        existing = session.get(Photo, guid_in_name)
                        if existing is not None and getattr(existing, "rel_path", None):
                            dest_path = resolve_relpath_under(settings.photo_root, existing.rel_path)
                            placed_path = _replace_into_library(
                                src_path=src_path,
                                dest_path=dest_path,
                                ingest_mode=ingest_mode,
                            )

                            try:
                                thumb_path(settings.deriv_root, guid_in_name).unlink()
                            except FileNotFoundError:
                                pass
                            except Exception:
                                pass
                            try:
                                mid_path(settings.deriv_root, guid_in_name).unlink()
                            except FileNotFoundError:
                                pass
                            except Exception:
                                pass

                            rec = build_record(
                                settings.photo_root,
                                placed_path,
                                datetime_fallback_order=settings.datetime_fallback_order(),
                            )

                            rec = replace(
                                rec,
                                datetime_original=existing.datetime_original or rec.datetime_original,
                                gps_altitude=rec.gps_altitude if rec.gps_altitude is not None else existing.gps_altitude,
                                gps_latitude=rec.gps_latitude if rec.gps_latitude is not None else existing.gps_latitude,
                                gps_longitude=rec.gps_longitude if rec.gps_longitude is not None else existing.gps_longitude,
                                camera_make=rec.camera_make or existing.camera_make,
                                user_comment=rec.user_comment or existing.user_comment,
                            )

                            guid = upsert_photo(session, rec)
                            upserted += 1

                            existing = session.get(Photo, guid)
                            if existing is not None:
                                enrich_photo_location(session, settings=settings, photo=existing)

                            deriv = ensure_derivatives(
                                source_path=placed_path,
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

                            session.commit()
                            continue

                    dt_iso = _infer_datetime_for_import(src_path, settings.datetime_fallback_order())
                    if not dt_iso:
                        if ingest_mode == "move":
                            _quarantine_failed(src_path=src_path, failed_root=failed_root)
                        else:
                            _quarantine_failed_copy(src_path=src_path, failed_root=failed_root)
                        errors += 1
                        logger.warning("ingest error job_id=%s path=%s reason=no_datetime", job_id, src_path)
                        continue

                    dt = datetime.fromisoformat(dt_iso)
                    dest_dir = settings.photo_root / f"{dt.year}" / f"{dt.month:02d}" / f"{dt.day:02d}"
                    dest_path = dest_dir / src_path.name

                    placed_path = _place_into_library(src_path=src_path, dest_path=dest_path, ingest_mode=ingest_mode)

                    guid: str | None = None
                    try:
                        rec = build_record(
                            settings.photo_root,
                            placed_path,
                            datetime_fallback_order=settings.datetime_fallback_order(),
                        )
                        from ...core.models import Photo

                        existing_guid = session.execute(
                            select(Photo.guid).where(Photo.rel_path == rec.rel_path)
                        ).scalar_one_or_none()

                        guid = upsert_photo(session, rec)
                        if existing_guid is None:
                            inserted_guids.add(guid)
                        upserted += 1

                        photo = session.get(Photo, guid)
                        if photo is not None:
                            enrich_photo_location(session, settings=settings, photo=photo)

                        deriv = ensure_derivatives(
                            source_path=placed_path,
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

                        session.commit()

                    except Exception:
                        try:
                            session.rollback()
                        except Exception:
                            pass
                        if guid is not None:
                            _cleanup_db_and_derivs(session=session, guid=guid, deriv_root=settings.deriv_root)
                        if ingest_mode == "move":
                            try:
                                _safe_move(placed_path, failed_root / placed_path.name)
                            except Exception:
                                pass
                        else:
                            try:
                                placed_path.unlink()
                            except Exception:
                                pass
                            try:
                                _quarantine_failed_copy(src_path=src_path, failed_root=failed_root)
                            except Exception:
                                pass
                        errors += 1
                        logger.exception("ingest error job_id=%s src=%s placed=%s", job_id, src_path, placed_path)

                except Exception:
                    try:
                        session.rollback()
                    except Exception:
                        pass
                    try:
                        if ingest_mode == "move":
                            _quarantine_failed(src_path=src_path, failed_root=failed_root)
                        else:
                            _quarantine_failed_copy(src_path=src_path, failed_root=failed_root)
                    except Exception:
                        pass
                    errors += 1
                    logger.exception("ingest error job_id=%s path=%s", job_id, src_path)

                if processed == 1 or processed % 50 == 0:
                    job.processed = processed
                    job.upserted = upserted
                    job.thumbs_done = thumbs_done
                    job.mids_done = mids_done
                    job.errors = errors
                    commit_with_retry(session, label="ingest-progress", logger=logger)

            job.processed = processed
            job.upserted = upserted
            job.thumbs_done = thumbs_done
            job.mids_done = mids_done
            job.errors = errors
            if manage_job_state:
                job.state = "done"
                job.finished_at = utc_now_iso()
            commit_with_retry(session, label="ingest-finish", logger=logger)

            logger.info(
                "ingest job done job_id=%s ingest_mode=%s processed=%s upserted=%s thumbs_done=%s mids_done=%s errors=%s",
                job_id,
                ingest_mode,
                processed,
                upserted,
                thumbs_done,
                mids_done,
                errors,
            )

            return {
                "processed": processed,
                "upserted": upserted,
                "thumbs_done": thumbs_done,
                "mids_done": mids_done,
                "errors": errors,
                "inserted_guids": sorted(inserted_guids),
            }

        finally:
            session.close()

    except Exception as e:
        logger.exception("ingest job crashed job_id=%s ingest_mode=%s", job_id, ingest_mode)
        if manage_job_state:
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
                    commit_with_retry(session, label="ingest-failed", logger=logger)
        return {
            "processed": processed,
            "upserted": upserted,
            "thumbs_done": thumbs_done,
            "mids_done": mids_done,
            "errors": errors,
            "inserted_guids": sorted(inserted_guids),
        }
