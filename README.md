# ArisList

ArisList is a self-hosted media shelf for local comics, light novels, audio works, and image galleries. It provides scanning, tagging, search, reading progress, comic/EPUB/audio/gallery viewers, and optional qmediasync STRM support for 115 cloud comics.

## Features

- Local library scan for CBZ comics, EPUB novels, audio folders, and gallery folders.
- Fast gallery browsing with thumbnails, virtualized grids, and reading history.
- Fullscreen comic reader, EPUB reader, audio player, and progress resume.
- qmediasync STRM comic source support without recursively listing cloud drive folders.
- Docker-friendly read-only media mounts; app data is stored separately.

## Docker

Copy `.env.example` to `.env`, then edit passwords and optional qmediasync values.

```bash
docker compose up --build -d
```

Open:

```text
http://localhost:8787
```

For NAS deployment, edit `docker-compose.yml` volumes. The left side is the host/NAS path, and the right side is the container path used by ArisList:

```yaml
- /volume1/media/comics:/library/comics:ro
- /volume1/media/novels:/library/novels:ro
- /volume1/media/audio:/library/audio:ro
- /volume1/media/gallery:/library/gallery:ro
```

Keep media mounts read-only. `data/` and `generated/` must be writable because they store the database, indexes, thumbnails, and cache.

## Development

Backend:

```bash
cargo run -p media-shelf-server
```

Frontend:

```bash
npm install --prefix frontend
npm run dev --prefix frontend
```

Checks:

```bash
cargo test -p media-shelf-server
npm run build --prefix frontend
node scripts/validate-project.mjs
```
