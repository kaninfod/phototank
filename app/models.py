from __future__ import annotations

from sqlalchemy import Float, Integer, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Photo(Base):
    __tablename__ = "photos"

    guid: Mapped[str] = mapped_column(Text, primary_key=True)
    rel_path: Mapped[str] = mapped_column(Text, unique=True, nullable=False)

    datetime_original: Mapped[str | None] = mapped_column(Text, nullable=True)
    gps_altitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    gps_latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    gps_longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    camera_make: Mapped[str | None] = mapped_column(Text, nullable=True)

    file_size: Mapped[int] = mapped_column(Integer, nullable=False)
    source_mtime: Mapped[int | None] = mapped_column(Integer, nullable=True)
    width: Mapped[int | None] = mapped_column(Integer, nullable=True)
    height: Mapped[int | None] = mapped_column(Integer, nullable=True)
    user_comment: Mapped[str | None] = mapped_column(Text, nullable=True)

    rating: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    indexed_at: Mapped[str] = mapped_column(Text, nullable=False)
    exif_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class ScanJob(Base):
    __tablename__ = "scan_jobs"

    job_id: Mapped[str] = mapped_column(Text, primary_key=True)
    state: Mapped[str] = mapped_column(Text, nullable=False)  # queued|running|done|failed
    year: Mapped[int | None] = mapped_column(Integer, nullable=True)

    processed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    upserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    thumbs_done: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    mids_done: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    errors: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    started_at: Mapped[str | None] = mapped_column(Text, nullable=True)
    finished_at: Mapped[str | None] = mapped_column(Text, nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
