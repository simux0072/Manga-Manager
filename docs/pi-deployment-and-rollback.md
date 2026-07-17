# Raspberry Pi deployment and rollback

Target a 64-bit ARM OS and keep PostgreSQL, storage, and Kavita data on the external SSD. The Compose
limits are web 256 MiB, worker 1 GiB, PostgreSQL 384 MiB, and migration/maintenance 256 MiB. Only web
joins the routed edge; PostgreSQL remains on the internal network. Enable Traefik labels only after
the local ARM64 rehearsal passes, and keep Authelia in front of this private application.

Before cutover:

1. Clone the repository on the Pi and run `scripts/test-environment.sh up` followed by
   `TEST_ENV_SKIP_BROWSER=true scripts/test-environment.sh check`. This uses isolated PostgreSQL,
   storage, ports, and Kavita; browser automation remains covered by CI and can be checked manually
   from another machine against port 18001.
2. Run `scripts/test-environment.sh scale-check`; verify the first catalog and grouped-job pages are
   below one second and the worker remains below its 1 GiB limit.
3. Optionally enqueue one pull per provider and at most one chapter per provider. Do not use a large
   real catalog for initial acceptance.
4. Reset the test environment and run the local stage rehearsal with `STAGE_PLATFORM=linux/arm64`.
5. Create a paired PostgreSQL/storage backup and record image digests and environment files.
6. Mount Manga Manager's tracked-only `kavita-library/` into Kavita read-only and set
   `KAVITA_LIBRARY_ROOT` to Kavita's view (for example `/manga`). Do not mount the full `library/`.
7. Run migrations as a one-shot job, then start web and worker.
8. Verify `/healthz`, Operations, a pull, a chapter download, a Kavita scan, and series/chapter covers.
9. Track two real manga and observe them for 24 hours before expanding the catalog.
10. Only then switch the Traefik router.

The test environment never talks to the normal Kavita configuration. Its media is generated locally
and contains no copyrighted content. Use the production storage path only after isolated acceptance
passes.

For rollback, switch the router away, stop worker then web, restore the paired database/storage set,
start the previous web image, verify health, start the previous worker, and restore the router. Never
mix a newer artifact database with an older storage snapshot.

For migration 0018, preserve compatible queued `source_refresh` jobs: deleting them may lose work
already discovered beyond the saved homepage frontier. Back up PostgreSQL and create the storage
manifest, migrate, run provider identity repair and `reconcile-refresh-queue` in dry-run then apply
mode, and wait for downloads, repair, cover backfill, and Kavita synchronization to settle before
the final stage check. The reconciler upgrades compatible v1 payloads in place and defers a single
replacement behind any currently leased incompatible refresh rather than terminating live work.

Before migration `0019`, take the same paired PostgreSQL backup and storage manifest. The revision
recomputes denormalized latest-release fields and adds observation, cover-band, and telemetry
indexes without rewriting media. After upgrading, run `manga-manager database-audit --json`; do not
start a rollback from the newer database if that audit fails—restore the paired pre-migration set.
