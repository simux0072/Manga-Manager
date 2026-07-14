# Raspberry Pi deployment and rollback

Target a 64-bit ARM OS and keep PostgreSQL, storage, and Kavita data on the external SSD. The Compose
limits are web 256 MiB, worker 1 GiB, PostgreSQL 384 MiB, and migration/maintenance 256 MiB. Only web
joins the routed edge; PostgreSQL remains on the internal network. Enable Traefik labels only after
the local ARM64 rehearsal passes, and keep Authelia in front of this private application.

Before cutover:

1. Run the local stage rehearsal with `STAGE_PLATFORM=linux/arm64`.
2. Create a paired PostgreSQL/storage backup and record image digests and environment files.
3. Mount Manga Manager's tracked-only `kavita-library/` into Kavita read-only and set
   `KAVITA_LIBRARY_ROOT` to Kavita's view (for example `/manga`). Do not mount the full `library/`.
4. Run migrations as a one-shot job, then start web and worker.
5. Verify `/healthz`, Operations, a pull, a chapter download, a Kavita scan, and series/chapter covers.
6. Only then switch the Traefik router.

For rollback, switch the router away, stop worker then web, restore the paired database/storage set,
start the previous web image, verify health, start the previous worker, and restore the router. Never
mix a newer artifact database with an older storage snapshot.

For migration 0018, preserve compatible queued `source_refresh` jobs: deleting them may lose work
already discovered beyond the saved homepage frontier. Back up PostgreSQL and create the storage
manifest, migrate, run provider identity repair and `reconcile-refresh-queue` in dry-run then apply
mode, and wait for downloads, repair, cover backfill, and Kavita synchronization to settle before
the final stage check. The reconciler upgrades compatible v1 payloads in place and defers a single
replacement behind any currently leased incompatible refresh rather than terminating live work.
