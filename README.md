# PhotoTank

This folder is the clean app project layout.

## Layout

- `app/` — FastAPI app package + templates + static assets
- `data/` — SQLite DB + generated derivatives
- `env/` — Python virtual environment for running the app

## Run (dev)

From this directory:

```bash
cd "/Users/martinhinge/projects/google takeout/phototank"
source env/bin/activate

uvicorn --reload --port 8000 app.main:app
```

## Run (Docker)

This repo includes a `Dockerfile` and `docker-compose.yml` so you can deploy via Portainer stacks.

### docker compose (local)

Edit `docker-compose.yml` and set:

- `PHOTO_ROOT` (required)
- volume mounts for your host paths

Then run:

```bash
docker compose up -d --build
```

### Portainer stack

- Use the provided `docker-compose.yml` as the stack definition.
- Configure environment variables in the stack UI (preferred) instead of baking a `.env` into the image.

## Configuration in containers

All settings can be provided via env vars (recommended for docker/Portainer). Common ones:

- `GEOCODE_ENABLED` (default: `true`)
- `GEOCODE_PROVIDER` (default: `geonames`)
- `GEOCODE_GEONAMES_USERNAME` (required to perform lookups)
- `GEOCODE_CACHE_CELL_M` (default: `100`; recommended `50..100`)
- `GEOCODE_RADIUS_KM_PRIMARY` (default: `0.2`)
- `GEOCODE_RADIUS_KM_FALLBACK` (default: `1.0`)

## GitHub Actions image build

- `ghcr.io/<owner>/<repo>/phototank:latest` (default branch)
- `ghcr.io/<owner>/<repo>/phototank:sha-...` (every push)

- `GEOCODE_ENABLED=true`
- `GEOCODE_PROVIDER=geonames`
- `GEOCODE_GEONAMES_USERNAME=<your_geonames_username>`
In Portainer, you can switch the compose service from `build: .` to `image: ghcr.io/<owner>/<repo>/phototank:latest`.

## Configure

Create `.env` in this directory (see `app/.env.example`).

Note: `app/.env` is also supported for backward-compat.

Common defaults:

- `DB_PATH=data/phototank.sqlite`
- `DERIV_ROOT=data/derivatives`

## Scan

Start a scan:

```bash
curl -s -X POST "http://127.0.0.1:8000/phototank/scan" | cat
```

Poll status:

```bash
curl -s "http://127.0.0.1:8000/phototank/scan/<job_id>" | cat
```

## Import

Drop new photos into `IMPORT_ROOT` (default: `./import`), then run:

```bash
curl -s -X POST "http://127.0.0.1:8000/phototank/import" | cat
```

Poll status:

```bash
curl -s "http://127.0.0.1:8000/phototank/import/<job_id>" | cat
```

Imported files are moved into `PHOTO_ROOT` and indexed + derivatives are generated.
Files that fail import are moved to `FAILED_ROOT` (default: `./failed`).
