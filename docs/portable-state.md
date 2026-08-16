# Portable metadata state

Portable state moves the useful catalog decisions between Manga Manager installations without
copying media or machine-specific runtime state. The JSON format is versioned, validated strictly,
and keyed by stable provider identities rather than PostgreSQL IDs.

The export includes:

- every tracked manga and every manga with an active downloaded artifact;
- canonical multi-provider groups created by merges;
- explicitly rejected match pairs created by keeping titles separate;
- provider source IDs and public title URLs, aliases, external identifiers, and descriptions;
- non-default chapter reading progress; and
- manual provider enable/disable preferences.

An untracked manga that has downloaded media is exported as `interested`. This deliberately asks a
new server to download it again. Other tracking states are preserved.

The export excludes CBZs, covers and cover URLs, fingerprints, blobs, projections, Kavita IDs,
credentials, jobs, errors, attempts, cooldowns, worker state, storage keys, filesystem paths,
provider response metadata, and learned concurrency limits.

## Private GitHub transport

The configured transport repository is private:
`simux0072/Manga-Manager-Data`. Clone it beside the application checkout, then export the current
state into it:

```bash
gh repo clone simux0072/Manga-Manager-Data ../Manga-Manager-Data
scripts/portable-state.sh export ../Manga-Manager-Data/manga-manager-state.json
cd ../Manga-Manager-Data
git add manga-manager-state.json
git commit -m "Update Manga Manager portable state"
git push
```

The file contains the library and reading history in plain text. Keep the data repository private
and grant access only to people who should see that information. Application and Kavita credentials
must remain in their local ignored environment files.

## Restore on another computer

Start Manga Manager on the destination and clone or pull the private data repository. Always preview
the import first:

```bash
scripts/portable-state.sh preview ../Manga-Manager-Data/manga-manager-state.json
scripts/portable-state.sh import ../Manga-Manager-Data/manga-manager-state.json --yes
```

Preview is read-only. It reports the number of canonical manga and provider identities that will be
created or merged, along with any conflicts. Apply runs in one database transaction, recreates merge
and separation state, and queues fresh provider refreshes. Tracked manga then flow through the normal
download planner, so no stale downloaded-state claim is imported.

Import is idempotent: importing the same file again updates the same provider identities and does not
duplicate manga, rejected matches, active refresh jobs, or download plans. Existing destination
tracking and reading progress are never downgraded. If destination data contradicts the snapshot,
the importer reports a conflict instead of guessing.

## Direct CLI usage

The helper targets the local staging worker by default. Other deployments can run the underlying CLI
inside any application container that has `V2_DATABASE_URL` configured:

```bash
manga-manager export-portable-state /tmp/manga-manager-state.json
manga-manager import-portable-state /tmp/manga-manager-state.json
manga-manager import-portable-state /tmp/manga-manager-state.json --apply
```

The importer accepts only the current `manga-manager-portable-state` schema and rejects unknown
fields. Keep snapshots from before an application upgrade until the new server has imported and
verified the latest one successfully.
