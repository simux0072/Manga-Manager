# Project Context

## Architecture

Manga Manager v2 is the only runtime. FastAPI serves `frontend/dist` and `/api/v2`; PostgreSQL owns
catalog state, reading state, decisions, jobs, permits, provider telemetry, and storage reservations.
Workers use renewable leases and separate provider pools. `app/adapters/`, `app/domain.py`,
`app/settings.py`, and `app/kavita.py` are shared integration modules, not a second application.

Provider listing jobs are intentionally short. They inspect a bounded recent frontier, persist the
new sentinel set, and create deduplicated `source_refresh` jobs. Each refresh fetches detail and
chapters independently; malformed observations are quarantined and transient failures retry without
discarding the rest of a pull. Cover CDN traffic is attributed to its owning provider and cannot
create a new operational source.

Downloads prefer Asura, then MangaFire, then King of Shojo, while falling back when content is
missing, invalid, rate-limited, or temporarily unavailable. Provider-global scheduling, adaptive
cooldowns, pool permits, per-series exclusion, and an eight-job global ceiling apply across worker
processes. Page fetches use bounded ordered windows and a 256 MiB worker in-flight budget.

Storage uses content-addressed blobs plus Kavita-facing projections. PostgreSQL reservations are
tied to job leases. Below the configured free-space reserve, chapter work pauses and defers without
spending attempts; scheduler health checks clear the pause automatically. Production defaults to a
5 GiB reserve and local staging to 1 GiB.

Kavita sync scans the projected folder, maps series/chapters, updates Want to Read, and uploads the
canonical manga cover to both series and chapters. The scheduler and Operations endpoint enqueue
bounded pending sync work.

## Supported workflows

- Local service: `scripts/stage-local.sh serve --build`
- Local service plus Kavita: `scripts/kavita-local.sh up`
- Full rehearsal: `scripts/stage-local.sh`
- Deterministic small/scale environment: `scripts/test-environment.sh up|check|scale-check`
- Safe staging reset: `scripts/reset-local-data.sh preview|archive|apply`
- PostgreSQL migration: `manga-manager migrate`
- Legacy recovery: `audit-legacy`, `repair-legacy`, `validate-legacy`, `migrate-legacy-library`

The Raspberry Pi/Traefik switch remains a separate deployment action. Generated storage, databases,
logs, reports, backups, `.env`, and `.local/` credentials must never be committed.
Synthetic fixtures are generated at runtime; test databases, covers, and CBZs remain ignored.
