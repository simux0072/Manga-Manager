# Raspberry Pi deployment and rollback

Target a 64-bit ARM OS and keep PostgreSQL, storage, and Kavita data on the external SSD. The Compose
limits are web 256 MiB, worker 1 GiB, PostgreSQL 384 MiB, and migration/maintenance 256 MiB. Only web
joins the routed edge; PostgreSQL remains on the internal network. Enable Traefik labels only after
the local ARM64 rehearsal passes, and keep Authelia in front of this private application.

Before cutover:

1. Run the local stage rehearsal with `STAGE_PLATFORM=linux/arm64`.
2. Create a paired PostgreSQL/storage backup and record image digests and environment files.
3. Mount Manga Manager's `library/` into Kavita and set `KAVITA_LIBRARY_ROOT` to Kavita's view.
4. Run migrations as a one-shot job, then start web and worker.
5. Verify `/healthz`, Operations, a pull, a chapter download, a Kavita scan, and series/chapter covers.
6. Only then switch the Traefik router.

For rollback, switch the router away, stop worker then web, restore the paired database/storage set,
start the previous web image, verify health, start the previous worker, and restore the router. Never
mix a newer artifact database with an older storage snapshot.
