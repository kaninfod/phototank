from __future__ import annotations

from sqlalchemy import Float, ForeignKey, Index, Integer, Text
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

    geo_country_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    geo_country: Mapped[str | None] = mapped_column(Text, nullable=True)
    geo_city: Mapped[str | None] = mapped_column(Text, nullable=True)
    geo_city_norm: Mapped[str | None] = mapped_column(Text, nullable=True)
    geo_region: Mapped[str | None] = mapped_column(Text, nullable=True)
    geo_postcode: Mapped[str | None] = mapped_column(Text, nullable=True)
    geo_display_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    geo_provider: Mapped[str | None] = mapped_column(Text, nullable=True)
    geo_cache_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    geo_lookup_at: Mapped[str | None] = mapped_column(Text, nullable=True)
    geo_lookup_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    geo_lookup_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class Tag(Base):
    __tablename__ = "tags"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    name_norm: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Bootstrap theme color keyword: primary|secondary|success|danger|warning|info|dark
    color: Mapped[str] = mapped_column(Text, nullable=False, default="primary")


class PhotoTag(Base):
    __tablename__ = "photo_tags"

    photo_guid: Mapped[str] = mapped_column(
        Text,
        ForeignKey("photos.guid", ondelete="CASCADE"),
        primary_key=True,
    )
    tag_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("tags.id", ondelete="CASCADE"),
        primary_key=True,
    )


class ReverseGeocodeCache(Base):
    __tablename__ = "reverse_geocode_cache"

    cache_key: Mapped[str] = mapped_column(Text, primary_key=True)
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    cell_m: Mapped[int] = mapped_column(Integer, nullable=False)
    lat_bucket: Mapped[float] = mapped_column(Float, nullable=False)
    lon_bucket: Mapped[float] = mapped_column(Float, nullable=False)

    country_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    country: Mapped[str | None] = mapped_column(Text, nullable=True)
    city: Mapped[str | None] = mapped_column(Text, nullable=True)
    city_norm: Mapped[str | None] = mapped_column(Text, nullable=True)
    region: Mapped[str | None] = mapped_column(Text, nullable=True)
    postcode: Mapped[str | None] = mapped_column(Text, nullable=True)
    display_name: Mapped[str | None] = mapped_column(Text, nullable=True)

    raw_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    fetched_at: Mapped[str] = mapped_column(Text, nullable=False)
    last_used_at: Mapped[str] = mapped_column(Text, nullable=False)
    hit_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)


# Indexes for fast timeline pagination and prev/next within filters.
Index(
    "idx_photos_dt_guid",
    Photo.datetime_original,
    Photo.guid,
    sqlite_where=Photo.datetime_original.is_not(None),
)
Index(
    "idx_photos_rating_dt_guid",
    Photo.rating,
    Photo.datetime_original,
    Photo.guid,
    sqlite_where=Photo.datetime_original.is_not(None),
)
Index("idx_photos_geo_country", Photo.geo_country)
Index("idx_photos_geo_city_norm", Photo.geo_city_norm)
Index("idx_photos_geo_country_city_norm", Photo.geo_country, Photo.geo_city_norm)
Index("idx_photos_geo_cache_key", Photo.geo_cache_key)

# Tag lookups.
Index("idx_tags_name_norm", Tag.name_norm, unique=True)
Index("idx_photo_tags_tag_photo", PhotoTag.tag_id, PhotoTag.photo_guid)
Index("idx_photo_tags_photo_tag", PhotoTag.photo_guid, PhotoTag.tag_id)
Index("idx_rgc_country_city_norm", ReverseGeocodeCache.country, ReverseGeocodeCache.city_norm)


class ScanJob(Base):
    __tablename__ = "scan_jobs"

    job_id: Mapped[str] = mapped_column(Text, primary_key=True)
    state: Mapped[str] = mapped_column(Text, nullable=False)  # queued|running|done|failed
    job_type: Mapped[str | None] = mapped_column(Text, nullable=True)  # ingest|validate
    year: Mapped[int | None] = mapped_column(Integer, nullable=True)

    processed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    upserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    thumbs_done: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    mids_done: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    errors: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    started_at: Mapped[str | None] = mapped_column(Text, nullable=True)
    finished_at: Mapped[str | None] = mapped_column(Text, nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
