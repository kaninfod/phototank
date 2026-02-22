from __future__ import annotations

import uuid
from pathlib import Path


def new_job_id() -> str:
    return uuid.uuid4().hex


def run_validate_job(
    job_id: str,
    *,
    repair_derivatives: bool = True,
    repair_mid_exif: bool = False,
    do_geolookup: bool = True,
) -> None:
    from .processing.jobs import run_validate_job as _run_validate_job

    _run_validate_job(
        job_id,
        repair_derivatives=repair_derivatives,
        repair_mid_exif=repair_mid_exif,
        do_geolookup=do_geolookup,
    )


def run_ingest_job(
    job_id: str,
    *,
    ingest_mode: str = "move",
    import_root_override: Path | None = None,
    failed_root_override: Path | None = None,
    manage_job_state: bool = True,
) -> dict[str, object]:
    from .processing.jobs import run_ingest_job as _run_ingest_job

    return _run_ingest_job(
        job_id,
        ingest_mode=ingest_mode,
        import_root_override=import_root_override,
        failed_root_override=failed_root_override,
        manage_job_state=manage_job_state,
    )


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
    from .processing.jobs import run_phone_sync_job as _run_phone_sync_job

    _run_phone_sync_job(
        job_id,
        ssh_user=ssh_user,
        phone_ip=phone_ip,
        ssh_port=ssh_port,
        remote_source_path=remote_source_path,
        remote_dest_path=remote_dest_path,
        ssh_key_path=ssh_key_path,
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
    from .processing.jobs import run_phone_reconcile_job as _run_phone_reconcile_job

    _run_phone_reconcile_job(
        job_id,
        ssh_user=ssh_user,
        phone_ip=phone_ip,
        ssh_port=ssh_port,
        remote_dest_path=remote_dest_path,
        ssh_key_path=ssh_key_path,
    )
