from __future__ import annotations

import logging
import shlex
import shutil
from pathlib import Path

from ...core.config import get_settings
from ...core.db import sessionmaker_for
from ..job_helpers import mark_job_started, run_command, set_job_progress


logger = logging.getLogger(__name__)


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
        started = mark_job_started(SessionLocal, job_id=job_id, message="phase=preflight", logger=logger)
        if not started:
            return

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

        set_job_progress(SessionLocal, job_id=job_id, logger=logger, message="phase=preflight ssh")
        run_command(
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
            logger=logger,
        )

        set_job_progress(SessionLocal, job_id=job_id, logger=logger, message="phase=prepare remote")
        run_command(
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
            logger=logger,
        )

        processed = sum(1 for p in local_mid_root.rglob("*.webp") if p.is_file())
        set_job_progress(
            SessionLocal,
            job_id=job_id,
            logger=logger,
            message=f"phase=push mids={processed}",
            processed=processed,
            mids_done=processed,
        )

        run_command(
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
            logger=logger,
        )

        set_job_progress(
            SessionLocal,
            job_id=job_id,
            logger=logger,
            state="done",
            message=f"done reconciled_mids={processed}",
            processed=processed,
            mids_done=processed,
            errors=0,
            finished=True,
        )
    except Exception as e:
        logger.exception("phone reconcile job crashed job_id=%s", job_id)
        set_job_progress(
            SessionLocal,
            job_id=job_id,
            logger=logger,
            state="failed",
            message=f"{type(e).__name__}: {e}",
            processed=processed,
            mids_done=processed,
            errors=errors + 1,
            finished=True,
        )
