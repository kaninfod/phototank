from __future__ import annotations

import logging
import os
import shutil
import shlex
import subprocess
from datetime import datetime
from dataclasses import replace
import time
import uuid
from pathlib import Path

from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from .config import get_settings
from .db import sessionmaker_for, upsert_photo
from .derivatives import ensure_derivatives, mid_path, thumb_path
from .geocode import enrich_photo_location
from .models import ScanJob
from .scanner import build_record, extract_exif_fields, iter_photo_files, try_datetime_from_filename
from .util import normalize_guid, resolve_relpath_under


logger = logging.getLogger(__name__)


def _set_job_progress(
    SessionLocal,
    *,
    job_id: str,
    message: str | None = None,
    state: str | None = None,
    processed: int | None = None,
    upserted: int | None = None,
    thumbs_done: int | None = None,
    mids_done: int | None = None,
    errors: int | None = None,
    finished: bool = False,
) -> None:
    with SessionLocal() as session:
        job = session.get(ScanJob, job_id)
        if job is None:
            return
        if message is not None:
            job.message = message
        if state is not None:
            job.state = state
        if processed is not None:
            job.processed = processed
        if upserted is not None:
            job.upserted = upserted
        if thumbs_done is not None:
            job.thumbs_done = thumbs_done
        if mids_done is not None:
            job.mids_done = mids_done
        if errors is not None:
            job.errors = errors
        if finished:
            job.finished_at = utc_now_iso()
        _commit_with_retry(session, label="set-job-progress")


def _is_sqlite_lock_error(exc: Exception) -> bool:
    msg = str(getattr(exc, "orig", exc)).lower()
    return "database is locked" in msg or "database table is locked" in msg


def _commit_with_retry(
    session: Session,
    *,
    label: str,
    attempts: int = 6,
    base_sleep_s: float = 0.2,
) -> bool:
    for attempt in range(1, attempts + 1):
        try:
            session.commit()
            return True
        except OperationalError as e:
            if not _is_sqlite_lock_error(e):
                raise
            try:
                session.rollback()
            except Exception:
                pass
            if attempt >= attempts:
                logger.error("commit failed after retries label=%s err=%s", label, e)
                return False
            time.sleep(base_sleep_s * attempt)


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
    """Return normalized guid if the file stem looks like a guid, else None."""
    s = path.stem.strip().lower().replace("-", "")
    if len(s) != 32:
        return None
    if any(c not in "0123456789abcdef" for c in s):
        return None
    return s


def _replace_into_library(*, src_path: Path, dest_path: Path, ingest_mode: str) -> Path:
    """Overwrite dest_path with src_path content (atomic-ish), returning dest_path."""

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest_path.with_name(f".{dest_path.name}.incoming")

    # Ensure we don't leave an old temp file around.
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


def run_validate_job(job_id: str, *, repair_derivatives: bool = True, repair_mid_exif: bool = False) -> None:
    settings = get_settings()

    logger.info("validate job starting job_id=%s", job_id)

    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        job = session.get(ScanJob, job_id)
        if job is None:
            return
        job.state = "running"
        job.started_at = utc_now_iso()
        _commit_with_retry(session, label="validate-start")

    year: int | None = None
    with SessionLocal() as session:
        job = session.get(ScanJob, job_id)
        if job is not None:
            year = job.year

    prefix = f"{year}/%" if year is not None else None

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

            # Avoid importing Photo at module import time to keep this file lightweight.
            from .models import Photo

            q = select(Photo)
            if prefix is not None:
                q = q.where(Photo.rel_path.like(prefix))
            q = q.order_by(Photo.rel_path.asc())

            for photo in session.execute(q).scalars().yield_per(500):
                processed += 1

                try:
                    source_path = resolve_relpath_under(settings.photo_root, photo.rel_path)
                    if not source_path.exists():
                        errors += 1
                        continue

                    if repair_derivatives:
                        source_mtime: int | None
                        try:
                            source_mtime = int(source_path.stat().st_mtime)
                        except Exception:
                            source_mtime = None

                        deriv = ensure_derivatives(
                            source_path=source_path,
                            deriv_root=settings.deriv_root,
                            guid=photo.guid,
                            source_mtime=source_mtime,
                            thumb_max=settings.thumb_max,
                            mid_max=settings.mid_max,
                            thumb_quality=settings.thumb_quality,
                            mid_quality=settings.mid_quality,
                            repair_mid_exif=repair_mid_exif,
                        )
                        if deriv.thumb_created:
                            thumbs_done += 1
                        if deriv.mid_created:
                            mids_done += 1

                    changed = enrich_photo_location(session, settings=settings, photo=photo)
                    # Keep geocode writes in short transactions so we don't hold
                    # SQLite write locks while doing network requests.
                    if changed:
                        ok = _commit_with_retry(session, label="validate-photo-geocode")
                        if not ok:
                            errors += 1
                except Exception:
                    errors += 1
                    logger.exception("validate error job_id=%s rel_path=%s", job_id, getattr(photo, "rel_path", ""))

                if processed == 1 or processed % 200 == 0:
                    job.processed = processed
                    job.upserted = upserted
                    job.thumbs_done = thumbs_done
                    job.mids_done = mids_done
                    job.errors = errors
                    _commit_with_retry(session, label="validate-progress")

            job.processed = processed
            job.upserted = upserted
            job.thumbs_done = thumbs_done
            job.mids_done = mids_done
            job.errors = errors
            job.state = "done"
            job.finished_at = utc_now_iso()
            _commit_with_retry(session, label="validate-finish")

            logger.info(
                "validate job done job_id=%s processed=%s thumbs_done=%s mids_done=%s errors=%s",
                job_id,
                processed,
                thumbs_done,
                mids_done,
                errors,
            )
        finally:
            session.close()

    except Exception as e:
        logger.exception("validate job crashed job_id=%s", job_id)
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
                _commit_with_retry(session, label="validate-failed")


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
        _commit_with_retry(session, label="ingest-start")

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

                    # Special case: GUID-named file means "replace existing".
                    guid_in_name = _maybe_guid_from_filename(src_path)
                    if guid_in_name:
                        try:
                            guid_in_name = normalize_guid(guid_in_name)
                        except Exception:
                            guid_in_name = None

                    if guid_in_name:
                        # Avoid importing Photo at module import time to keep this file lightweight.
                        from .models import Photo

                        existing = session.get(Photo, guid_in_name)
                        if existing is not None and getattr(existing, "rel_path", None):
                            dest_path = resolve_relpath_under(settings.photo_root, existing.rel_path)
                            placed_path = _replace_into_library(
                                src_path=src_path,
                                dest_path=dest_path,
                                ingest_mode=ingest_mode,
                            )

                            # Force derivative regen regardless of mtimes.
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

                            # Preserve "date taken" and existing EXIF-derived fields if the
                            # edited file doesn't contain them.
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
                        from .models import Photo

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
                        if ingest_mode == "move":
                            try:
                                _safe_move(placed_path, failed_root / placed_path.name)
                            except Exception:
                                pass
                        else:
                            # Source is still present; remove the library copy and
                            # optionally copy the source into FAILED_ROOT for review.
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
                    _commit_with_retry(session, label="ingest-progress")

            job.processed = processed
            job.upserted = upserted
            job.thumbs_done = thumbs_done
            job.mids_done = mids_done
            job.errors = errors
            if manage_job_state:
                job.state = "done"
                job.finished_at = utc_now_iso()
            _commit_with_retry(session, label="ingest-finish")

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
                    _commit_with_retry(session, label="ingest-failed")
        return {
            "processed": processed,
            "upserted": upserted,
            "thumbs_done": thumbs_done,
            "mids_done": mids_done,
            "errors": errors,
            "inserted_guids": sorted(inserted_guids),
        }


def _run_command(args: list[str], *, label: str) -> subprocess.CompletedProcess[str]:
    logger.info("%s: %s", label, " ".join(args))
    proc = subprocess.run(args, capture_output=True, text=True)
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        tail = stderr[-800:] if stderr else stdout[-800:]
        raise RuntimeError(f"{label} failed (exit={proc.returncode}): {tail}")
    return proc


def run_phone_sync_job(
    job_id: str,
    *,
    ssh_user: str,
    phone_ip: str,
    ssh_port: int,
    remote_source_path: str,
    remote_dest_path: str,
    ssh_key_path: Path,
) -> None:
    settings = get_settings()
    SessionLocal = sessionmaker_for(settings.db_path)

    processed = 0
    upserted = 0
    thumbs_done = 0
    mids_done = 0
    errors = 0

    try:
        with SessionLocal() as session:
            job = session.get(ScanJob, job_id)
            if job is None:
                return
            job.state = "running"
            job.started_at = utc_now_iso()
            job.message = "phase=preflight"
            _commit_with_retry(session, label="phone-sync-start")

        if shutil.which("ssh") is None:
            raise RuntimeError("ssh not found in PATH")
        if shutil.which("rsync") is None:
            raise RuntimeError("rsync not found in PATH")

        key_path = ssh_key_path.expanduser()
        if not key_path.exists():
            raise RuntimeError(f"ssh key not found: {key_path}")

        ensure_import_root = settings.import_root.resolve()
        ensure_import_root.mkdir(parents=True, exist_ok=True)
        settings.deriv_root.mkdir(parents=True, exist_ok=True)

        stage_root = ensure_import_root / "_phone_sync" / job_id
        pull_root = stage_root / "pull"
        failed_root = stage_root / "failed"
        pull_root.mkdir(parents=True, exist_ok=True)
        failed_root.mkdir(parents=True, exist_ok=True)

        target = f"{ssh_user}@{phone_ip}"
        ssh_cmd = (
            "ssh"
            " -F /dev/null"
            f" -i {shlex.quote(str(key_path))}"
            f" -p {int(ssh_port)}"
            " -o IdentitiesOnly=yes"
            " -o BatchMode=yes"
            " -o StrictHostKeyChecking=accept-new"
        )

        _set_job_progress(SessionLocal, job_id=job_id, message="phase=preflight ssh")
        _run_command(
            [
                "ssh",
                "-F",
                "/dev/null",
                "-i",
                str(key_path),
                "-p",
                str(int(ssh_port)),
                "-o",
                "IdentitiesOnly=yes",
                "-o",
                "BatchMode=yes",
                "-o",
                "StrictHostKeyChecking=accept-new",
                target,
                "echo",
                "ok",
            ],
            label="phone-sync ssh check",
        )

        _set_job_progress(SessionLocal, job_id=job_id, message="phase=pull")
        _run_command(
            [
                "rsync",
                "-a",
                "-e",
                ssh_cmd,
                f"{target}:{remote_source_path.rstrip('/')}/",
                f"{pull_root.as_posix().rstrip('/')}/",
            ],
            label="phone-sync pull",
        )

        _set_job_progress(SessionLocal, job_id=job_id, message="phase=import")
        ingest_result = run_ingest_job(
            job_id,
            ingest_mode="move",
            import_root_override=pull_root,
            failed_root_override=failed_root,
            manage_job_state=False,
        )

        processed = int(ingest_result.get("processed", 0))
        upserted = int(ingest_result.get("upserted", 0))
        thumbs_done = int(ingest_result.get("thumbs_done", 0))
        mids_done = int(ingest_result.get("mids_done", 0))
        errors = int(ingest_result.get("errors", 0))

        inserted_guids = [str(g) for g in ingest_result.get("inserted_guids", [])]

        _set_job_progress(
            SessionLocal,
            job_id=job_id,
            message=f"phase=push preparing inserted={len(inserted_guids)}",
            processed=processed,
            upserted=upserted,
            thumbs_done=thumbs_done,
            mids_done=mids_done,
            errors=errors,
        )

        rel_mid_paths: list[str] = []
        for guid in inserted_guids:
            mp = mid_path(settings.deriv_root, guid)
            if mp.exists():
                rel_mid_paths.append(f"mid/{guid[:2]}/{guid[2:4]}/{guid}.webp")

        if rel_mid_paths:
            files_from_path = stage_root / "push_files.txt"
            files_from_path.write_text("\n".join(rel_mid_paths) + "\n", encoding="utf-8")

            _run_command(
                [
                    "ssh",
                    "-F",
                    "/dev/null",
                    "-i",
                    str(key_path),
                    "-p",
                    str(int(ssh_port)),
                    "-o",
                    "IdentitiesOnly=yes",
                    "-o",
                    "BatchMode=yes",
                    "-o",
                    "StrictHostKeyChecking=accept-new",
                    target,
                    "mkdir",
                    "-p",
                    f"{remote_dest_path.rstrip('/')}/mid",
                ],
                label="phone-sync remote mkdir",
            )

            _set_job_progress(
                SessionLocal,
                job_id=job_id,
                message=f"phase=push mids={len(rel_mid_paths)}",
            )
            _run_command(
                [
                    "rsync",
                    "-a",
                    "--files-from",
                    str(files_from_path),
                    "-e",
                    ssh_cmd,
                    f"{settings.deriv_root.as_posix().rstrip('/')}/",
                    f"{target}:{remote_dest_path.rstrip('/')}/",
                ],
                label="phone-sync push",
            )

        _set_job_progress(
            SessionLocal,
            job_id=job_id,
            state="done",
            message=f"done pulled={processed} imported={upserted} pushed_mids={len(rel_mid_paths)}",
            processed=processed,
            upserted=upserted,
            thumbs_done=thumbs_done,
            mids_done=mids_done,
            errors=errors,
            finished=True,
        )
    except Exception as e:
        logger.exception("phone sync job crashed job_id=%s", job_id)
        _set_job_progress(
            SessionLocal,
            job_id=job_id,
            state="failed",
            message=f"{type(e).__name__}: {e}",
            processed=processed,
            upserted=upserted,
            thumbs_done=thumbs_done,
            mids_done=mids_done,
            errors=errors + 1,
            finished=True,
        )


def run_phone_reconcile_job(
    job_id: str,
    *,
    ssh_user: str,
    phone_ip: str,
    ssh_port: int,
    remote_dest_path: str,
    ssh_key_path: Path,
) -> None:
    settings = get_settings()
    SessionLocal = sessionmaker_for(settings.db_path)

    processed = 0
    errors = 0

    try:
        with SessionLocal() as session:
            job = session.get(ScanJob, job_id)
            if job is None:
                return
            job.state = "running"
            job.started_at = utc_now_iso()
            job.message = "phase=preflight"
            _commit_with_retry(session, label="phone-reconcile-start")

        if shutil.which("ssh") is None:
            raise RuntimeError("ssh not found in PATH")
        if shutil.which("rsync") is None:
            raise RuntimeError("rsync not found in PATH")

        key_path = ssh_key_path.expanduser()
        if not key_path.exists():
            raise RuntimeError(f"ssh key not found: {key_path}")

        local_mid_root = (settings.deriv_root / "mid").resolve()
        local_mid_root.mkdir(parents=True, exist_ok=True)

        target = f"{ssh_user}@{phone_ip}"
        ssh_cmd = (
            "ssh"
            " -F /dev/null"
            f" -i {shlex.quote(str(key_path))}"
            f" -p {int(ssh_port)}"
            " -o IdentitiesOnly=yes"
            " -o BatchMode=yes"
            " -o StrictHostKeyChecking=accept-new"
        )

        _set_job_progress(SessionLocal, job_id=job_id, message="phase=preflight ssh")
        _run_command(
            [
                "ssh",
                "-F",
                "/dev/null",
                "-i",
                str(key_path),
                "-p",
                str(int(ssh_port)),
                "-o",
                "IdentitiesOnly=yes",
                "-o",
                "BatchMode=yes",
                "-o",
                "StrictHostKeyChecking=accept-new",
                target,
                "echo",
                "ok",
            ],
            label="phone-reconcile ssh check",
        )

        _set_job_progress(SessionLocal, job_id=job_id, message="phase=prepare remote")
        _run_command(
            [
                "ssh",
                "-F",
                "/dev/null",
                "-i",
                str(key_path),
                "-p",
                str(int(ssh_port)),
                "-o",
                "IdentitiesOnly=yes",
                "-o",
                "BatchMode=yes",
                "-o",
                "StrictHostKeyChecking=accept-new",
                target,
                "mkdir",
                "-p",
                f"{remote_dest_path.rstrip('/')}/mid",
            ],
            label="phone-reconcile remote mkdir",
        )

        processed = sum(1 for p in local_mid_root.rglob("*.webp") if p.is_file())
        _set_job_progress(
            SessionLocal,
            job_id=job_id,
            message=f"phase=push mids={processed}",
            processed=processed,
            mids_done=processed,
        )

        _run_command(
            [
                "rsync",
                "-a",
                "--delete",
                "-e",
                ssh_cmd,
                f"{local_mid_root.as_posix().rstrip('/')}/",
                f"{target}:{remote_dest_path.rstrip('/')}/mid/",
            ],
            label="phone-reconcile push",
        )

        _set_job_progress(
            SessionLocal,
            job_id=job_id,
            state="done",
            message=f"done reconciled_mids={processed}",
            processed=processed,
            mids_done=processed,
            errors=0,
            finished=True,
        )
    except Exception as e:
        logger.exception("phone reconcile job crashed job_id=%s", job_id)
        _set_job_progress(
            SessionLocal,
            job_id=job_id,
            state="failed",
            message=f"{type(e).__name__}: {e}",
            processed=processed,
            mids_done=processed,
            errors=errors + 1,
            finished=True,
        )
