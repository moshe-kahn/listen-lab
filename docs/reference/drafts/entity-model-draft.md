# ListenLab Entity Model Draft

## Purpose
This document proposes the next data-model layer on top of raw ingest.

It is meant to solve:
- reuse of track, album, and artist entities across sources
- support for non-Spotify metadata
- separate identities for provider objects, display objects, and analysis groupings
- provenance and confidence tracking for inferred mappings
- album and track variant handling without collapsing important distinctions

## Why A Single Canonical Track Is Not Enough
ListenLab has multiple identity needs at once:

- provider identity
  - one exact object from one provider such as a Spotify track ID
- display identity
  - the specific release/version we may want to show separately in the UI
- analysis identity
  - the broader grouping we may want to count together for ranking and scoring

Examples:
- same song, different Spotify IDs after a rerelease
  - same analysis identity
  - often different display identity
- same Spotify-represented song with duplicate provider objects that should collapse
  - same display identity
  - same analysis identity
- live version
  - different display identity
  - usually different analysis identity
- remix
  - different display identity
  - usually different analysis identity
- cover
  - different display identity
  - different analysis identity

The same pattern applies to albums:
- same album, different Spotify album IDs
  - same display album in some cases
- deluxe, remaster, rerelease, anniversary edition
  - often different display album
  - sometimes same analysis album

## Current Implementation Notes

The current implementation has three important identity layers:
- provider/source identity: `source_*` tables and source map tables
- release/display identity: `release_album` and `release_track`
- analysis/grouping identity: `analysis_track` and album-family tables

Current album membership is represented through `album_track`.

Important schema point:
- `release_track` does not have a `release_album_id`.
- Album merge or repair work must move `album_track.release_album_id`.
- Album merge or repair work must not directly rewrite `release_track` rows.

Current artist representation is mixed:
- raw artist text exists on raw/source rows
- Spotify catalog tables store structured artist arrays as JSON
- normalized artist rows exist in `artist`
- album and track artists connect through `album_artist` and `track_artist`

### Known Limitation: Artist Groups vs Musicians

`artist` currently represents credited performing entities, not people/musicians or formal group membership. A durable group name such as `Crosby, Stills, Nash & Young`, `Earth, Wind & Fire`, or `Peter, Paul and Mary` should remain a valid artist identity even though its name contains member names and those members may also have solo artist identities. This is distinct from a raw composite credit such as `Dave Harrington, Tim Mislock`, where a history text value is only a comma-separated credit for multiple existing artists on a track or album.

That model is intentionally out of scope for current identity repair. Current repair/audit work should focus on preventing duplicate artist rows and cleaning up raw composite credits when album/track evidence proves they are separate credited artists. It should not infer group membership or split provider-backed group names just because a comma, ampersand, or member name appears in the artist display name. Low-priority future musician/member graph work is tracked in `docs/overview/roadmap.md`.

Fallback/history text paths can split identities from later provider-backed paths. History import may only know artist text, album text, and track text; Spotify API/catalog paths later provide stable Spotify IDs. If the later provider-backed source is inserted as a new canonical row instead of being reconciled to the text-backed row, the product can show duplicate artists or miss album/track metadata attached to the other row.

Current artist status:
- artist resolution is centralized in `backend/app/artist_resolution.py`
- resolver outcomes are explicit: `matched_existing`, `created_new_text_backed`, `created_new_provider_backed`, and `ambiguous_review`
- artist duplicate audit and evidence-gated repair exist and share the resolver evidence labels
- Spotify/provider artist ingest first checks exact `source_artist_map`, then can promote one safe text-only artist by attaching the provider source map
- artist name alone is not enough for promotion or repair
- safe automatic repair requires exact normalized name, exactly one provider-backed artist, text-only duplicates, and album/track identity evidence or strict shared-normalized-album-title evidence
- same-name-only, stylization, similar-name same-album, orphan placeholder, and multiple-provider groups remain review-only
- composite history credits are classified separately and excluded from automatic repair

Current artist evidence labels shared by ingest prevention and repair:
- `shared_release_album_id`
- `shared_release_track_id`
- `reconciled_source_album`
- `reconciled_source_track`
- `shared_normalized_album_title_with_provider_context`
- `composite_credit_context`

The same class applies to albums and tracks:
- text-only `release_album` rows such as `Kid A` or `Amnesiac` can exist without Spotify album IDs, while Spotify catalog metadata is only attached to source/catalog rows
- `release_track` has Spotify ID vs Spotify URI reconciliation, but not general safe history-text track to Spotify track reconciliation by title/artist/album/duration/ISRC

Fallback/history text paths previously used raw artist strings in stable keys. This could also split identities such as:
- `Telekinesis`
- `Telekinesis, Telekinesis`

Future fallback key creation now normalizes only fallback/history artist text through `_normalize_fallback_artist_text(...)`.

That normalization:
- is used for `history_raw_artist`, `history_raw_album`, and `history_raw_track` keys
- picks a canonical primary token from comma-delimited artist text
- preserves original raw artist text for display/raw fields
- does not affect Spotify-ID provider identity paths
- does not repair existing rows

Remaining reconciliation work:
- extend source/text reconciliation beyond artists into release albums and tracks
- audit duplicate `release_album` candidates by normalized album name + primary artist + year/catalog evidence; names alone are not enough
- audit duplicate `release_track` candidates by title + artist + album + duration/ISRC/context; names alone are not enough
- keep catalog backfill enrichment-only; identity mutation should live in explicit dry-run/apply repair or promotion flows

Current artist repair endpoints:
- `GET /debug/artists/duplicate-audit`
- `POST /debug/artists/duplicate-repair?dry_run=true|false`
- `POST /debug/artists/composite-credit-cleanup?dry_run=true|false`

Current artist repair invariant:
- frontend de-duping and comma-display preservation are defensive display behavior only; backend identity repair owns durable artist identity.
- raw ingest still exposes compatibility helpers from `backend/app/db.py`, but durable artist decisions route through `resolve_artist(...)`.

## Release Duplicate Diagnostics

Current read-only diagnostics include:
- duplicate release albums by same resolved Spotify album ID
- duplicate release albums by normalized album name + normalized primary artist
- duplicate release tracks by same resolved Spotify track ID

The Spotify-ID diagnostics are stronger evidence because Spotify ID is the highest-confidence identity signal.

The name + primary artist diagnostic is weaker but catches fallback/text duplicates when Spotify IDs differ or are missing.

These diagnostics are read-only:
- no Spotify API calls
- no writes
- no mapping mutation
- no merge behavior

## Release Album Merge Preview And Dry Run

Current read-only endpoints:
- `POST /debug/identity/release-albums/merge-preview`
- `POST /debug/identity/release-albums/merge-dry-run`

Preview chooses a deterministic survivor:
- accepted/direct Spotify `source_album_map`
- Spotify catalog match
- most associated tracks/listens
- lowest `release_album_id`

Preview returns:
- survivor recommendation
- duplicate album IDs
- warnings
- affected counts
- proposed operations
- `merge_readiness`
- `readiness_reasons`

Readiness values:
- `safe_candidate`
- `needs_review`
- `unsafe`

Unsafe cases:
- missing requested IDs
- different normalized album names
- different normalized primary artists

Needs-review cases:
- multiple distinct Spotify album IDs
- album-track conflicts
- no strong single Spotify evidence

`album_track_conflicts` means duplicate album-track rows would collide on:
- survivor `release_album_id`
- same `release_track_id`

Dry run:
- reuses preview/readiness
- blocks `unsafe`
- blocks survivor mismatch
- allows `safe_candidate` and `needs_review`
- returns exact row-level plans for:
  - `source_album_map` repoints
  - `album_artist` inserts/deletes/dedupes
  - `album_track` repoints
  - `album_track` conflicts
  - release albums that would be retired later

There is no apply/merge endpoint yet.

Any future apply path should:
- start with `safe_candidate` only
- run in one transaction
- be idempotent
- return explicit `rows_affected`
- prove `release_track` rows are not changed directly
- prove `analysis_track_map` is not mutated

## Proposed Identity Layers

### Artists
Artists are simpler than tracks and albums, but still need source mapping.

- `artist`
  - canonical artist entity used by the product
- `source_artist`
  - one provider-specific artist object

### Tracks
- `release_track`
  - the display-level track
  - distinguishes version and release context when we care
- `recording_track`
  - proposed normal-listening track identity between `release_track` and `track_family`
  - groups release tracks that represent the same core recorded performance or master lineage across albums, singles, compilations, soundtracks, rereleases, remasters, and compatible mono/stereo cases
  - preserves release appearances and source provenance while allowing one representative release/playback choice
- `track_family`
  - the broader grouping-level track used for related variants, scoring, and rollups
  - contains variants that should not collapse into one `recording_track`, such as meaningful radio edits, alternate takes, demos, live versions, acoustic versions, remixes, and rerecordings
  - current schema/code term: `analysis_track`
- `source_track`
  - one provider-specific track object

Mapping direction:
- many `source_track` -> one `release_track`
- many `release_track` -> one `recording_track` (proposed; not current schema)
- many `recording_track` -> one `track_family` (proposed)
- current schema maps many `release_track` -> one `analysis_track`
- generated cache tables currently store read-only recording/track-family candidate clusters for lookup, but they are not canonical `recording_track` or `track_family` mappings

Current implementation note:
- `source_track -> release_track` dedupe is intentionally conservative and rerunnable
- play-event projection now ensures `release_track` identity for music facts:
  - exact Spotify track IDs map through `source_track(source_name='spotify')`
  - no-Spotify-ID music rows with title text use the existing `history_raw` fallback key
  - Spotify podcast episodes are not mapped into `release_track`
- `release_track -> track_family` is now driven by a policy/config layer plus variant-title interpretation, not by one hardcoded title-heuristic block
- future `recording_track` should split the current broad jump from release-level identity to family-level identity into a normal-listening same-recording layer and a broader related-version layer
- `track_family` should still be treated as a conservative grouping layer, not as the final universal canonical track identity for every downstream analysis
- current recording-track support is read-only/debug only: candidate endpoints, generated candidate cache tables, summary evidence buckets, CLI inspection, and saved review decisions exist, but no canonical `recording_track` table or promotion/apply path exists
- saved recording-track candidate reviews preserve the reviewed candidate snapshot and manual judgment metadata, but they are not active identity mappings

UI identity direction:
- `release_track_id` is the preferred UI identity for same-song state.
- Spotify track ID remains source/version/playback identity.
- Current frontend liked stars are release-track-aware.
- Activity `Listened` grouping now prefers `release_track_id`.
- Track preview payloads expose release-track, recording-candidate, and family-candidate metadata so overlays can show relation transparency without changing playback identity.
- Activity `Liked` grouping should remain a separate phase because displayed internal liked-release-track count can differ from Spotify liked-track count.
- Future `recording_track_id` can become the default normal-song aggregation identity after read-only evidence, review UI, and representative playback rules exist.
- Current `recording_track` review decisions should be treated as evidence for future classifier tuning and schema design, not as product identity.

### Albums
- `release_album`
  - the display-level album
- `analysis_album`
  - the grouping-level album used for rollups
- `source_album`
  - one provider-specific album object

Mapping direction:
- many `source_album` -> one `release_album`
- many `release_album` -> one `analysis_album`

## Provenance On Mapping Rows
Every inferred mapping should record how it was made.

Recommended fields on mapping tables:
- `match_method`
- `confidence`
- `status`
- `is_user_confirmed`
- `explanation`
- `created_at`
- `updated_at`

Suggested `status` values:
- `suggested`
- `accepted`
- `rejected`
- `superseded`

Suggested `match_method` examples:
- `provider_identity`
- `spotify_same_track_uri_family`
- `exact_metadata`
- `title_artist_duration_heuristic`
- `manual`
- `imported_curated_mapping`

Notes:
- confidence belongs on the mapping row, not on the entity row
- manual mappings should generally be treated as `confidence = 1.0`
- rejected mappings are worth keeping so we do not repeatedly resuggest them

## Relationship Tables
We also need semantic relationships between display-level tracks and albums.

### Track relationships
`track_relationship` links one `release_track` to another.

Suggested relationship types:
- `duplicate_provider_representation_of`
- `rerelease_of`
- `remaster_of`
- `live_version_of`
- `acoustic_version_of`
- `demo_version_of`
- `edit_of`
- `extended_mix_of`
- `remix_of`
- `cover_of`

### Album relationships
`album_relationship` links one `release_album` to another.

Suggested relationship types:
- `duplicate_provider_representation_of`
- `rerelease_of`
- `remaster_of`
- `deluxe_edition_of`
- `expanded_edition_of`
- `anniversary_edition_of`

These relationships should not be used as the only grouping mechanism. They provide semantic links in addition to the explicit analysis mapping tables.

## First-Pass SQLite Schema

```sql
CREATE TABLE artist (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  canonical_name TEXT NOT NULL,
  sort_name TEXT,
  country_code TEXT,
  start_year INTEGER,
  end_year INTEGER,
  is_person INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE source_artist (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_name TEXT NOT NULL,
  external_id TEXT NOT NULL,
  external_uri TEXT,
  artist_id INTEGER NOT NULL REFERENCES artist(id),
  source_name_raw TEXT,
  raw_payload_json TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(source_name, external_id)
);

CREATE TABLE release_album (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  primary_name TEXT NOT NULL,
  normalized_name TEXT,
  release_date TEXT,
  release_year INTEGER,
  album_type TEXT,
  version_label TEXT,
  explicit INTEGER,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE analysis_album (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  primary_name TEXT NOT NULL,
  grouping_note TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE source_album (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_name TEXT NOT NULL,
  external_id TEXT NOT NULL,
  external_uri TEXT,
  source_name_raw TEXT,
  raw_payload_json TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(source_name, external_id)
);

CREATE TABLE source_album_map (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_album_id INTEGER NOT NULL REFERENCES source_album(id),
  release_album_id INTEGER NOT NULL REFERENCES release_album(id),
  match_method TEXT NOT NULL,
  confidence REAL NOT NULL,
  status TEXT NOT NULL,
  is_user_confirmed INTEGER NOT NULL DEFAULT 0,
  explanation TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(source_album_id, release_album_id)
);

CREATE TABLE analysis_album_map (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  release_album_id INTEGER NOT NULL REFERENCES release_album(id),
  analysis_album_id INTEGER NOT NULL REFERENCES analysis_album(id),
  match_method TEXT NOT NULL,
  confidence REAL NOT NULL,
  status TEXT NOT NULL,
  is_user_confirmed INTEGER NOT NULL DEFAULT 0,
  explanation TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(release_album_id, analysis_album_id)
);

CREATE TABLE release_track (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  primary_name TEXT NOT NULL,
  normalized_name TEXT,
  duration_ms INTEGER,
  disc_number INTEGER,
  track_number INTEGER,
  version_label TEXT,
  is_explicit INTEGER,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- Proposed future layer; not current schema.
CREATE TABLE recording_track (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  primary_name TEXT NOT NULL,
  normalized_name TEXT,
  representative_release_track_id INTEGER REFERENCES release_track(id),
  representative_source_track_id INTEGER REFERENCES source_track(id),
  representative_reason TEXT,
  grouping_note TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE analysis_track (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  primary_name TEXT NOT NULL,
  grouping_note TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE source_track (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_name TEXT NOT NULL,
  external_id TEXT NOT NULL,
  external_uri TEXT,
  isrc TEXT,
  source_name_raw TEXT,
  raw_payload_json TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(source_name, external_id)
);

CREATE TABLE source_track_map (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_track_id INTEGER NOT NULL REFERENCES source_track(id),
  release_track_id INTEGER NOT NULL REFERENCES release_track(id),
  match_method TEXT NOT NULL,
  confidence REAL NOT NULL,
  status TEXT NOT NULL,
  is_user_confirmed INTEGER NOT NULL DEFAULT 0,
  explanation TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(source_track_id, release_track_id)
);

-- Proposed future layer; not current schema.
CREATE TABLE recording_track_map (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  release_track_id INTEGER NOT NULL REFERENCES release_track(id),
  recording_track_id INTEGER NOT NULL REFERENCES recording_track(id),
  relationship_kind TEXT NOT NULL,
  match_method TEXT NOT NULL,
  confidence REAL NOT NULL,
  status TEXT NOT NULL,
  is_user_confirmed INTEGER NOT NULL DEFAULT 0,
  explanation TEXT,
  evidence_json TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(release_track_id, recording_track_id)
);

CREATE TABLE analysis_track_map (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  release_track_id INTEGER NOT NULL REFERENCES release_track(id),
  analysis_track_id INTEGER NOT NULL REFERENCES analysis_track(id),
  match_method TEXT NOT NULL,
  confidence REAL NOT NULL,
  status TEXT NOT NULL,
  is_user_confirmed INTEGER NOT NULL DEFAULT 0,
  explanation TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(release_track_id, analysis_track_id)
);

CREATE TABLE album_artist (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  release_album_id INTEGER NOT NULL REFERENCES release_album(id),
  artist_id INTEGER NOT NULL REFERENCES artist(id),
  role TEXT NOT NULL DEFAULT 'primary',
  billing_index INTEGER,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(release_album_id, artist_id, role)
);

CREATE TABLE track_artist (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  release_track_id INTEGER NOT NULL REFERENCES release_track(id),
  artist_id INTEGER NOT NULL REFERENCES artist(id),
  role TEXT NOT NULL DEFAULT 'primary',
  billing_index INTEGER,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(release_track_id, artist_id, role)
);

CREATE TABLE album_track (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  release_album_id INTEGER NOT NULL REFERENCES release_album(id),
  release_track_id INTEGER NOT NULL REFERENCES release_track(id),
  disc_number INTEGER,
  track_number INTEGER,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(release_album_id, release_track_id, disc_number, track_number)
);

CREATE TABLE track_relationship (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  from_release_track_id INTEGER NOT NULL REFERENCES release_track(id),
  to_release_track_id INTEGER NOT NULL REFERENCES release_track(id),
  relationship_type TEXT NOT NULL,
  match_method TEXT NOT NULL,
  confidence REAL NOT NULL,
  status TEXT NOT NULL,
  is_user_confirmed INTEGER NOT NULL DEFAULT 0,
  explanation TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE album_relationship (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  from_release_album_id INTEGER NOT NULL REFERENCES release_album(id),
  to_release_album_id INTEGER NOT NULL REFERENCES release_album(id),
  relationship_type TEXT NOT NULL,
  match_method TEXT NOT NULL,
  confidence REAL NOT NULL,
  status TEXT NOT NULL,
  is_user_confirmed INTEGER NOT NULL DEFAULT 0,
  explanation TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE image_asset (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_name TEXT,
  source_url TEXT NOT NULL,
  local_cache_path TEXT,
  mime_type TEXT,
  width INTEGER,
  height INTEGER,
  byte_size INTEGER,
  sha256_hex TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE entity_image (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entity_type TEXT NOT NULL,
  entity_id INTEGER NOT NULL,
  image_asset_id INTEGER NOT NULL REFERENCES image_asset(id),
  usage_type TEXT NOT NULL,
  source_name TEXT,
  rank_order INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
```

## Recommended Guardrails
- Keep raw ingest source-faithful and separate from this layer.
- Do not write canonical IDs directly into `raw_play_event` until backfill logic is stable.
- Start with conservative automatic mappings only.
- Treat manual confirmations as first-class data.
- Keep relationship types explicit instead of overloading analysis grouping.

## Recommended Implementation Order
1. Create artist, release, analysis, source, and join tables.
2. Backfill exact provider rows from current raw Spotify columns.
3. Auto-create only high-confidence source-to-release mappings first.
4. Add release-to-analysis mappings for clearly safe cases.
5. Add relationship rows for rereleases, remasters, and other variants.
6. Later, add review workflows for low-confidence suggestions.

## Conservative Default Rules

### Safe to auto-merge into the same release track
- same provider object repeated across imports
- obvious duplicate provider representations already known to be identical

### Safe to keep as separate release tracks but maybe same analysis track
- rerelease
- remaster
- anniversary edition placement

### Usually keep separate at both levels
- live version
- acoustic version
- remix
- cover
- demo

These defaults should be overrideable because edge cases will appear quickly.
