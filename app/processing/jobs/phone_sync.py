from __future__ import annotations

import logging
import shlex
import shutil
from pathlib import Path

from ...core.config import get_settings
from ...core.db import sessionmaker_for
from ..job_helpers import mark_job_started, run_command, set_job_progress


logger = logging.getLogger(__name__)


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
            label="phone-sync ssh check",
            logger=logger,
        )

        set_job_progress(SessionLocal, job_id=job_id, logger=logger, message="phase=pull")
        run_command(
            [
                "rsync",
                "-a",
                "-e",
                ssh_cmd,
                f"{target}:{remote_source_path.rstrip('/')}/",
                f"{pull_root.as_posix().rstrip('/')}/",
            ],
            label="phone-sync pull",
            logger=logger,
        )

        set_job_progress(SessionLocal, job_id=job_id, logger=logger, message="phase=import")
        from .ingest import run_ingest_job

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

        set_job_progress(
            SessionLocal,
            job_id=job_id,
            logger=logger,
            message=f"phase=push preparing inserted={len(inserted_guids)}",
            processed=processed,
            upserted=upserted,
            thumbs_done=thumbs_done,
            mids_done=mids_done,
            errors=errors,
        )

        from ...services.derivatives import mid_path

        rel_mid_paths: list[str] = []
        for guid in inserted_guids:
            mp = mid_path(settings.deriv_root, guid)
            if mp.exists():
                rel_mid_paths.append(f"mid/{guid[:2]}/{guid[2:4]}/{guid}.webp")

        if rel_mid_paths:
            files_from_path = stage_root / "push_files.txt"
            files_from_path.write_text("\n".join(rel_mid_paths) + "\n", encoding="utf-8")

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
                label="phone-sync remote mkdir",
                logger=logger,
            )

            set_job_progress(
                SessionLocal,
                job_id=job_id,
                logger=logger,
                message=f"phase=push mids={len(rel_mid_paths)}",
            )
            run_command(
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
                logger=logger,
            )

        set_job_progress(
            SessionLocal,
            job_id=job_id,
            logger=logger,
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
        set_job_progress(
            SessionLocal,
            job_id=job_id,
            logger=logger,
            state="failed",
            message=f"{type(e).__name__}: {e}",
            processed=processed,
            upserted=upserted,
            thumbs_done=thumbs_done,
            mids_done=mids_done,
            errors=errors + 1,
            finished=True,
        )
