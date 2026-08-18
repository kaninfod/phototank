from __future__ import annotations

import uuid
from pathlib import Path


def new_job_id() -> str:
    return uuid.uuid4().hex


def run_delete_photos_job(
    job_id: str,
    *,
    guids: list[str],
) -> None:
    from .processing.jobs import run_delete_photos_job as _run_delete_photos_job

    _run_delete_photos_job(job_id, guids=guids)


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
    from .processing.jobs import run_backup_job as _run_backup_job

    _run_backup_job(
        job_id,
        backup_sources=backup_sources,
        dest=dest,
        ssh_user=ssh_user,
        ssh_host=ssh_host,
        ssh_port=ssh_port,
        ssh_key_path=ssh_key_path,
        use_ssh=use_ssh,
        delete=delete,
        dry_run=dry_run,
    )
