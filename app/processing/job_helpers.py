from __future__ import annotations

import logging
import subprocess
import time

from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from ..core.models import ScanJob
from .progress import utc_now_iso


def is_sqlite_lock_error(exc: Exception) -> bool:
    msg = str(getattr(exc, "orig", exc)).lower()
    return "database is locked" in msg or "database table is locked" in msg


def commit_with_retry(
    session: Session,
    *,
    label: str,
    logger: logging.Logger,
    attempts: int = 6,
    base_sleep_s: float = 0.2,
) -> bool:
    for attempt in range(1, attempts + 1):
        try:
            session.commit()
            return True
        except OperationalError as e:
            if not is_sqlite_lock_error(e):
                raise
            try:
                session.rollback()
            except Exception:
                pass
            if attempt >= attempts:
                logger.error("commit failed after retries label=%s err=%s", label, e)
                return False
            time.sleep(base_sleep_s * attempt)


def mark_job_started(SessionLocal, *, job_id: str, message: str, logger: logging.Logger) -> bool:
    with SessionLocal() as session:
        job = session.get(ScanJob, job_id)
        if job is None:
            return False
        job.state = "running"
        job.started_at = utc_now_iso()
        job.message = message
        commit_with_retry(session, label="set-job-started", logger=logger)
        return True


def set_job_progress(
    SessionLocal,
    *,
    job_id: str,
    logger: logging.Logger,
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
        commit_with_retry(session, label="set-job-progress", logger=logger)


def run_command(args: list[str], *, label: str, logger: logging.Logger) -> subprocess.CompletedProcess[str]:
    logger.info("%s: %s", label, " ".join(args))
    proc = subprocess.run(args, capture_output=True, text=True)
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        tail = stderr[-800:] if stderr else stdout[-800:]
        raise RuntimeError(f"{label} failed (exit={proc.returncode}): {tail}")
    return proc
