from __future__ import annotations

import logging
from datetime import datetime, timezone

from ...core.config import get_settings
from ...core.db import sessionmaker_for
from ...services.derivatives import mid_path, thumb_path
from ...core.models import Photo, ScanJob
from ...core.util import normalize_guid, resolve_relpath_under
from ..job_helpers import commit_with_retry


logger = logging.getLogger(__name__)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def run_delete_photos_job(
    job_id: str,
    *,
    guids: list[str],
) -> None:
    """Delete photos: source file, derivatives, and DB record."""
    
    settings = get_settings()
    logger.info("delete job starting job_id=%s count=%d", job_id, len(guids))

    SessionLocal = sessionmaker_for(settings.db_path)

    # Mark job as running
    with SessionLocal() as session:
        job = session.get(ScanJob, job_id)
        if job is None:
            logger.warning("delete job not found job_id=%s", job_id)
            return
        job.state = "running"
        job.started_at = utc_now_iso()
        commit_with_retry(session, label="delete-start", logger=logger)

    deleted = 0
    not_found = 0
    errors = 0
    
    normalized_guids = [normalize_guid(g) for g in guids]

    try:
        session = SessionLocal()
        try:
            job = session.get(ScanJob, job_id)
            if job is None:
                return

            for guid in normalized_guids:
                photo = session.get(Photo, guid)
                if photo is None:
                    not_found += 1
                    continue

                try:
                    # Resolve filesystem paths
                    source_path = resolve_relpath_under(settings.photo_root, photo.rel_path)
                    tpath = thumb_path(settings.deriv_root, guid)
                    mpath = mid_path(settings.deriv_root, guid)

                    # Delete derivatives
                    try:
                        tpath.unlink()
                    except FileNotFoundError:
                        pass
                    except Exception as e:
                        logger.warning("Failed to delete thumb guid=%s: %s", guid, e)
                        errors += 1
                        continue

                    try:
                        mpath.unlink()
                    except FileNotFoundError:
                        pass
                    except Exception as e:
                        logger.warning("Failed to delete mid guid=%s: %s", guid, e)
                        errors += 1
                        continue

                    # Delete source file
                    try:
                        source_path.unlink()
                    except FileNotFoundError:
                        # Treat missing source as already-deleted; still remove DB row
                        pass
                    except Exception as e:
                        logger.warning("Failed to delete source guid=%s: %s", guid, e)
                        errors += 1
                        continue

                    # Delete from database
                    session.delete(photo)
                    deleted += 1

                    # Update progress periodically
                    if deleted % 10 == 0:
                        job.processed = deleted + not_found + errors
                        job.upserted = deleted
                        job.errors = errors
                        commit_with_retry(session, label="delete-progress", logger=logger)

                except Exception as e:
                    logger.exception("delete error job_id=%s guid=%s", job_id, guid)
                    errors += 1

            # Final commit
            session.commit()

            # Mark job as done
            job.processed = deleted + not_found + errors
            job.upserted = deleted
            job.errors = errors
            job.state = "done"
            job.finished_at = utc_now_iso()
            job.message = f"Deleted {deleted} photo(s), {not_found} not found, {errors} error(s)"
            commit_with_retry(session, label="delete-finish", logger=logger)

            logger.info(
                "delete job done job_id=%s deleted=%d not_found=%d errors=%d",
                job_id,
                deleted,
                not_found,
                errors,
            )

        finally:
            session.close()

    except Exception as e:
        logger.exception("delete job crashed job_id=%s", job_id)
        with SessionLocal() as session:
            job = session.get(ScanJob, job_id)
            if job is not None:
                job.state = "failed"
                job.message = f"{type(e).__name__}: {e}"
                job.finished_at = utc_now_iso()
                commit_with_retry(session, label="delete-crash", logger=logger)
