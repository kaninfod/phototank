from __future__ import annotations

import logging
import re
import shlex
import shutil
import sqlite3
from pathlib import Path

from ...core.config import get_settings
from ...core.db import sessionmaker_for
from ..job_helpers import mark_job_started, run_command, set_job_progress


logger = logging.getLogger(__name__)


def _parse_rsync_stats(stdout: str) -> tuple[int, int]:
    """Return (number_of_files_transferred, total_bytes_transferred) from rsync --stats output."""
    files = 0
    bytes_transferred = 0

    # English: "Number of files transferred: 1234"
    files_match = re.search(r"[Nn]umber of (?:regular )?files transferred:?\s+(\d+)", stdout)
    if files_match:
        try:
            files = int(files_match.group(1))
        except Exception:
            pass

    # "Total bytes sent: 12345" or "Total bytes transferred: 12345"
    bytes_match = re.search(r"[Tt]otal bytes (?:transferred|sent):\s+(\d+)", stdout)
    if bytes_match:
        try:
            bytes_transferred = int(bytes_match.group(1))
        except Exception:
            pass

    return files, bytes_transferred


def _checkpoint_sqlite(db_path: Path) -> None:
    """Merge WAL contents into the main DB so the .sqlite file is complete for backup."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    finally:
        conn.close()


def run_backup_job(
    job_id: str,
    *,
    backup_sources: list[str],
    dest: str,
    ssh_user: str | None = None,
    ssh_host: str | None = None,
    ssh_port: int = 22,
    ssh_key_path: Path | None = None,
    use_ssh: bool = False,
    delete: bool = True,
    dry_run: bool = False,
) -> None:
    """Mirror a list of local source paths to a destination via rsync.

    Parameters
    ----------
    backup_sources:
        Absolute local paths to mirror (e.g. photo_root, deriv_root, db_path).
    dest:
        Destination folder. Either an absolute local path like ``/backup/phototank``
        or, when use_ssh is True, ``user@host:/backup/phototank``.
    ssh_user/ssh_host/ssh_port/ssh_key_path:
        Used when ``use_ssh`` is True to build the rsync ssh command.
    delete:
        If True, rsync deletes files at the destination that no longer exist at
        the source (mirror mode). Default True.
    dry_run:
        If True, passes ``--dry-run`` to rsync and marks the job done without
        actually changing the destination. Default False.
    """
    settings = get_settings()
    SessionLocal = sessionmaker_for(settings.db_path)

    total_files = 0
    total_bytes = 0
    sources_completed = 0

    try:
        started = mark_job_started(SessionLocal, job_id=job_id, message="phase=preflight", logger=logger)
        if not started:
            return

        if shutil.which("rsync") is None:
            raise RuntimeError("rsync not found in PATH")
        if use_ssh and shutil.which("ssh") is None:
            raise RuntimeError("ssh not found in PATH")
        if use_ssh:
            if not ssh_host:
                raise RuntimeError("backup_host is required for remote backup")
            if not ssh_user:
                raise RuntimeError("backup_ssh_user is required for remote backup")
            key_path = ssh_key_path.expanduser() if ssh_key_path else None
            if key_path is None or not key_path.exists():
                raise RuntimeError(f"backup ssh key not found: {ssh_key_path}")

        # Make sure the destination root exists locally if this is a local backup.
        if not use_ssh:
            dest_path = Path(dest).expanduser()
            dest_path.mkdir(parents=True, exist_ok=True)

        # Build a single ssh command string for rsync to use.
        ssh_cmd: str | None = None
        if use_ssh:
            key_path = ssh_key_path.expanduser()
            ssh_cmd = (
                "ssh"
                " -F /dev/null"
                f" -i {shlex.quote(str(key_path))}"
                f" -p {int(ssh_port)}"
                " -o IdentitiesOnly=yes"
                " -o BatchMode=yes"
                " -o StrictHostKeyChecking=accept-new"
            )

        for idx, source_raw in enumerate(backup_sources, start=1):
            source_path = Path(source_raw).expanduser().resolve()
            if not source_path.exists():
                logger.warning("backup source not found, skipping: %s", source_path)
                set_job_progress(
                    SessionLocal,
                    job_id=job_id,
                    logger=logger,
                    message=f"phase=skip source={source_path.name}",
                    processed=total_files,
                )
                continue

            # For directories, mirror the *contents* into a subfolder named after the source.
            # For a single file (the DB), copy it into the destination root.
            if source_path.is_dir():
                dest_sub = f"{dest.rstrip('/')}/{source_path.name}/"
                source_arg = f"{source_path}/"
            else:
                dest_sub = dest.rstrip("/")
                source_arg = str(source_path)

            set_job_progress(
                SessionLocal,
                job_id=job_id,
                logger=logger,
                message=f"phase=rsync source={source_path.name} ({idx}/{len(backup_sources)})",
                processed=total_files,
            )

            # Checkpoint the SQLite DB before backing it up.
            if source_path.is_file() and source_path.suffix == ".sqlite":
                try:
                    _checkpoint_sqlite(source_path)
                    logger.info("checkpointed sqlite before backup: %s", source_path)
                except Exception:
                    logger.warning("failed to checkpoint sqlite %s, continuing anyway", source_path, exc_info=True)

            args = [
                "rsync",
                "--archive",
                "--stats",
            ]
            if delete:
                args.append("--delete")
            if dry_run:
                args.append("--dry-run")
            if ssh_cmd:
                args.extend(["-e", ssh_cmd])
            args.extend([source_arg, dest_sub])

            proc = run_command(args, label=f"backup {source_path.name}", logger=logger)
            files, bytes_transferred = _parse_rsync_stats(proc.stdout or "")
            total_files += files
            total_bytes += bytes_transferred
            sources_completed += 1
            logger.info(
                "backup source=%s files=%d bytes=%d",
                source_path.name,
                files,
                bytes_transferred,
            )

            set_job_progress(
                SessionLocal,
                job_id=job_id,
                logger=logger,
                message=f"phase=done source={source_path.name} ({idx}/{len(backup_sources)})",
                processed=total_files,
            )

        msg = f"Backed up {sources_completed}/{len(backup_sources)} source(s), {total_files} file(s), {total_bytes} byte(s)"
        if dry_run:
            msg = "[DRY RUN] " + msg

        set_job_progress(
            SessionLocal,
            job_id=job_id,
            logger=logger,
            message=msg,
            state="done",
            processed=total_files,
            finished=True,
        )
    except Exception as e:
        logger.exception("backup job failed job_id=%s", job_id)
        set_job_progress(
            SessionLocal,
            job_id=job_id,
            logger=logger,
            message=f"backup failed: {e}",
            state="failed",
            finished=True,
        )
