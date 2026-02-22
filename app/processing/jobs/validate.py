from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import select

from ...core.config import get_settings
from ...core.db import sessionmaker_for
from ...services.derivatives import ensure_derivatives
from ...services.geocode import enrich_photo_location
from ...core.models import ScanJob
from ...core.util import resolve_relpath_under
from ..job_helpers import commit_with_retry


logger = logging.getLogger(__name__)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def run_validate_job(
    job_id: str,
    *,
    repair_derivatives: bool = True,
    repair_mid_exif: bool = False,
    do_geolookup: bool = True,
) -> None:
    settings = get_settings()

    logger.info("validate job starting job_id=%s", job_id)

    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        job = session.get(ScanJob, job_id)
        if job is None:
            return
        job.state = "running"
        job.started_at = utc_now_iso()
        commit_with_retry(session, label="validate-start", logger=logger)

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

            from ...core.models import Photo

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

                    if do_geolookup:
                        changed = enrich_photo_location(session, settings=settings, photo=photo)
                        if changed:
                            ok = commit_with_retry(session, label="validate-photo-geocode", logger=logger)
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
                    commit_with_retry(session, label="validate-progress", logger=logger)

            job.processed = processed
            job.upserted = upserted
            job.thumbs_done = thumbs_done
            job.mids_done = mids_done
            job.errors = errors
            job.state = "done"
            job.finished_at = utc_now_iso()
            commit_with_retry(session, label="validate-finish", logger=logger)

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
                commit_with_retry(session, label="validate-failed", logger=logger)
