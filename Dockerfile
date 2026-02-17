# syntax=docker/dockerfile:1.6

FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    VIRTUAL_ENV=/opt/venv

RUN python -m venv "$VIRTUAL_ENV"
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Build deps for Pillow / pillow-heif
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libjpeg62-turbo-dev \
    zlib1g-dev \
    libpng-dev \
    libtiff-dev \
    libwebp-dev \
    libheif-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /phototank

COPY requirements.txt requirements.lock.txt ./
RUN pip install --upgrade pip \
  && pip install --no-cache-dir -r requirements.txt

COPY app ./app


FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VIRTUAL_ENV=/opt/venv

ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Runtime libs for Pillow / pillow-heif
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    libjpeg62-turbo \
    zlib1g \
    libpng16-16 \
    libtiff6 \
    libwebp7 \
    libheif1 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /phototank

COPY --from=builder /opt/venv /opt/venv
COPY app ./app

RUN mkdir -p data import failed

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
