"""Background job worker.

Polls the scan_jobs SQLite table and runs one job at a time.
Started from a separate Docker service via: python -m app.worker
"""

from __future__ import annotations

from datetime import datetime
import logging
import signal
import sys
import time
from pathlib import Path
from types import FrameType
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from .core.config import get_settings
from .core.db import claim_next_queued_job, create_job, engine_for, init_db, mark_stale_running_jobs_failed, sessionmaker_for
from .core.logging_setup import setup_logging
from .core.models import ScanJob
from .jobs import new_job_id, run_backup_job, run_delete_photos_job, run_ingest_job, run_phone_reconcile_job, run_phone_sync_job, run_validate_job


logger = logging.getLogger(__name__)

POLL_INTERVAL_S = 5
SCHEDULER_CHECK_INTERVAL_S = 60  # re-evaluate daily schedule once a minute

_DISPATCH: dict[str, Any] = {
    "ingest": run_ingest_job,
    "import": run_ingest_job,
    "validate": run_validate_job,
    "phone_sync": run_phone_sync_job,
    "phone_reconcile": run_phone_reconcile_job,
    "delete_photos": run_delete_photos_job,
    "backup": run_backup_job,
}

_shutdown_requested = False


def _request_shutdown(signum: int, _frame: FrameType | None) -> None:
    global _shutdown_requested
    logger.info("received signal %s, finishing current job then exiting", signum)
    _shutdown_requested = True


def _prepare_params(kind: str, params: dict[str, object]) -> dict[str, object]:
    """Convert JSON-round-tripped params back to types expected by job functions."""
    if kind in ("phone_sync", "phone_reconcile", "backup"):
        if "ssh_key_path" in params:
            params = dict(params)
            params["ssh_key_path"] = Path(str(params["ssh_key_path"])).expanduser()
        if "ssh_port" in params:
            try:
                params["ssh_port"] = int(params["ssh_port"])
            except Exception:
                pass
    return params


def _mark_job_failed(session: Session, job_id: str, message: str) -> None:
    job = session.get(ScanJob, job_id)
    if job is None:
        return
    from .processing.progress import utc_now_iso

    job.state = "failed"
    job.message = message
    job.finished_at = utc_now_iso()
    session.commit()


def run_one_job(SessionLocal: Any, job_id: str, kind: str | None, params: dict[str, object]) -> None:
    if kind is None:
        logger.error("job has no type, cannot dispatch job_id=%s", job_id)
        with SessionLocal() as session:
            _mark_job_failed(session, job_id, "missing job_type")
        return

    fn = _DISPATCH.get(kind)
    if fn is None:
        logger.error("unknown job_type=%s job_id=%s", kind, job_id)
        with SessionLocal() as session:
            _mark_job_failed(session, job_id, f"unknown job_type: {kind}")
        return

    params = _prepare_params(kind, params)
    logger.info("starting job job_id=%s kind=%s", job_id, kind)
    try:
        fn(job_id, **params)
    except Exception:
        logger.exception("job crashed job_id=%s kind=%s", job_id, kind)
        # The job function usually catches exceptions and marks itself failed,
        # but if an unexpected error bubbles up we mark it here.
        try:
            with SessionLocal() as session:
                _mark_job_failed(session, job_id, f"worker caught exception: {sys.exc_info()[1]}")
        except Exception:
            logger.exception("failed to mark job as failed job_id=%s", job_id)
        return

    # Safety net: if the job function returned without updating state, mark done.
    try:
        with SessionLocal() as session:
            job = session.get(ScanJob, job_id)
            if job is not None and job.state == "running":
                from .processing.progress import utc_now_iso

                job.state = "done"
                job.finished_at = utc_now_iso()
                session.commit()
    except Exception:
        logger.exception("failed to finalize job state job_id=%s", job_id)


def _maybe_schedule_backup(SessionLocal: Any, settings: Any, last_run_day: str | None) -> str | None:
    """If daily backup is enabled and not yet queued today, enqueue a backup ScanJob.

    Returns the ISO date string (YYYY-MM-DD) for which a backup was scheduled, or
    ``last_run_day`` if nothing was scheduled this tick.
    """
    if not settings.backup_enabled:
        return last_run_day
    if not settings.backup_dest_path:
        return last_run_day

    run_time = (settings.backup_run_time or "02:00").strip()
    try:
        hour_str, minute_str = run_time.split(":")
        target_hour = int(hour_str)
        target_minute = int(minute_str)
    except Exception:
        logger.warning("invalid backup_run_time=%r, expected HH:MM", run_time)
        return last_run_day

    now = datetime.now().replace(microsecond=0)
    today = now.date().isoformat()

    # Already scheduled for today.
    if last_run_day == today:
        return last_run_day

    # Wait until the scheduled time of day.
    if now.hour < target_hour or (now.hour == target_hour and now.minute < target_minute):
        return last_run_day

    with SessionLocal() as session:
        existing = session.execute(
            select(ScanJob).where(
                ScanJob.job_type == "backup",
                ScanJob.state.in_(["queued", "running"]),
            )
        ).scalars().first()
        if existing is not None:
            logger.debug("backup already queued/running, skipping scheduler")
            return today

        job_id = new_job_id()
        ssh_user = (settings.backup_ssh_user or "").strip() or None
        ssh_host = (settings.backup_host or "").strip() or None
        ssh_key_path = settings.backup_ssh_key_path
        use_ssh = bool(ssh_user and ssh_host)
        dest = str(settings.backup_dest_path).rstrip("/")
        if use_ssh:
            dest = f"{ssh_user}@{ssh_host}:{dest}"

        sources = [
            str(settings.photo_root.resolve()),
            str(settings.deriv_root.resolve()),
            str(settings.db_path.resolve()),
        ]

        create_job(
            session,
            job_id=job_id,
            year=None,
            job_type="backup",
            params={
                "backup_sources": sources,
                "dest": dest,
                "ssh_user": ssh_user,
                "ssh_host": ssh_host,
                "ssh_port": int(settings.backup_port),
                "ssh_key_path": str(ssh_key_path),
                "use_ssh": use_ssh,
                "delete": True,
                "dry_run": False,
            },
        )
        session.commit()

    logger.info("scheduled daily backup job_id=%s dest=%s", job_id, dest)
    return today


def run_worker_loop() -> None:
    global _shutdown_requested
    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)

    settings = get_settings()
    setup_logging(settings)

    engine = engine_for(settings.db_path)
    init_db(engine)
    SessionLocal = sessionmaker_for(settings.db_path)

    with SessionLocal() as session:
        n = mark_stale_running_jobs_failed(session)
    if n:
        logger.warning("marked %d stale running job(s) as failed", n)

    logger.info("worker started, polling every %ss", POLL_INTERVAL_S)

    last_scheduler_check = 0.0
    last_backup_day: str | None = None

    while not _shutdown_requested:
        with SessionLocal() as session:
            next_job = claim_next_queued_job(session)
        if next_job is None:
            # Scheduler only needs to run once per minute.
            now = time.time()
            if now - last_scheduler_check >= SCHEDULER_CHECK_INTERVAL_S:
                last_scheduler_check = now
                last_backup_day = _maybe_schedule_backup(SessionLocal, settings, last_backup_day)
            time.sleep(POLL_INTERVAL_S)
            continue

        job_id, kind, params = next_job
        run_one_job(SessionLocal, job_id, kind, params)
        # Loop immediately to pick up any other queued jobs.

    logger.info("worker shutting down")


if __name__ == "__main__":
    run_worker_loop()
