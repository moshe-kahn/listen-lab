# Release Album Merge Validation

This workflow validates the existing read-only release-album merge contract before any apply endpoint exists.

Scope:
- use real duplicate `release_album` groups from the local SQLite database
- inspect `merge-preview`
- inspect `merge-dry-run`
- confirm the plan is safe and stable
- do not write data
- do not involve Spotify API calls

## Routes And Helpers

Current routes:
- `POST /debug/identity/release-albums/merge-preview`
- `POST /debug/identity/release-albums/merge-dry-run`

Current helper functions:
- `preview_release_album_merge(...)`
- `dry_run_release_album_merge(...)`

Files:
- `backend/app/main.py`
- `backend/app/spotify_catalog_backfill.py`

## How Preview Chooses A Survivor

Preview sorts candidates by:
1. accepted/direct Spotify album mapping
2. Spotify catalog match
3. more associated tracks plus raw listens
4. lower `release_album_id`

Preview returns:
- `survivor_release_album_id`
- `merge_release_album_ids`
- `merge_readiness`
- `readiness_reasons`
- `warnings`
- `affected`
- `proposed_operations`

Dry run reuses preview and returns:
- `blocked`
- `blocked_reasons`
- `rows_affected`
- exact row-level `plan`
- explicit non-mutation `statements`

## Local DB

Default local DB:

```bash
DB=/Users/kahntra/Documents/ListenLab/listen-lab-main/backend/data/listenlabs.sqlite3
```

## List Candidate Duplicate Groups

Duplicate groups by resolved Spotify album ID:

```bash
sqlite3 "$DB" "
WITH primary_artists AS (
  SELECT ordered.release_album_id, group_concat(ordered.artist_name, ', ') AS artist_name
  FROM (
    SELECT aa.release_album_id, a.canonical_name AS artist_name
    FROM album_artist aa
    JOIN artist a ON a.id = aa.artist_id
    WHERE aa.role = 'primary'
    ORDER BY aa.release_album_id, COALESCE(aa.billing_index, 999999), aa.id, a.canonical_name
  ) ordered
  GROUP BY ordered.release_album_id
),
primary_artist_first AS (
  SELECT release_album_id, artist_name
  FROM (
    SELECT
      aa.release_album_id,
      a.canonical_name AS artist_name,
      row_number() OVER (
        PARTITION BY aa.release_album_id
        ORDER BY COALESCE(aa.billing_index, 999999), aa.id, a.canonical_name
      ) AS rn
    FROM album_artist aa
    JOIN artist a ON a.id = aa.artist_id
    WHERE aa.role = 'primary'
  )
  WHERE rn = 1
),
mapped_album_candidates AS (
  SELECT sam.release_album_id, sa.external_id AS spotify_album_id, 1 AS source_priority, 0 AS listen_count
  FROM source_album sa
  JOIN source_album_map sam ON sam.source_album_id = sa.id
  WHERE sa.source_name = 'spotify' AND sa.external_id IS NOT NULL AND sa.external_id != '' AND sam.status = 'accepted'
  UNION ALL
  SELECT sam.release_album_id, sa.external_id AS spotify_album_id, 2 AS source_priority, 0 AS listen_count
  FROM source_album sa
  JOIN source_album_map sam ON sam.source_album_id = sa.id
  WHERE sa.source_name = 'spotify' AND sa.external_id IS NOT NULL AND sa.external_id != ''
),
raw_album_candidates AS (
  SELECT at.release_album_id, rpe.spotify_album_id, 3 AS source_priority, count(*) AS listen_count
  FROM raw_play_event rpe
  JOIN source_track st ON st.source_name = 'spotify' AND st.external_id = rpe.spotify_track_id
  JOIN source_track_map stm ON stm.source_track_id = st.id AND stm.status = 'accepted'
  JOIN album_track at ON at.release_track_id = stm.release_track_id
  WHERE rpe.spotify_album_id IS NOT NULL AND rpe.spotify_album_id != ''
  GROUP BY at.release_album_id, rpe.spotify_album_id
),
fallback_local_album_candidates AS (
  SELECT ra.id AS release_album_id, sac.spotify_album_id, 4 AS source_priority, 0 AS listen_count
  FROM release_album ra
  JOIN primary_artist_first paf ON paf.release_album_id = ra.id
  JOIN spotify_album_catalog sac ON lower(trim(COALESCE(sac.name, ''))) = lower(trim(COALESCE(ra.primary_name, '')))
  JOIN json_each(COALESCE(sac.artists_json, '[]')) artist_json
  WHERE lower(trim(COALESCE(json_extract(artist_json.value, '$.name'), ''))) = lower(trim(COALESCE(paf.artist_name, '')))
),
all_album_candidates AS (
  SELECT * FROM mapped_album_candidates
  UNION ALL SELECT * FROM raw_album_candidates
  UNION ALL SELECT * FROM fallback_local_album_candidates
),
representative_spotify_album AS (
  SELECT release_album_id, spotify_album_id
  FROM (
    SELECT
      release_album_id,
      spotify_album_id,
      row_number() OVER (
        PARTITION BY release_album_id
        ORDER BY source_priority ASC, listen_count DESC, spotify_album_id ASC
      ) AS rn
    FROM all_album_candidates
  )
  WHERE rn = 1
),
base AS (
  SELECT
    ra.id AS release_album_id,
    ra.primary_name AS release_album_name,
    COALESCE(pa.artist_name, 'Unknown artist') AS artist_name,
    rsa.spotify_album_id
  FROM release_album ra
  LEFT JOIN primary_artists pa ON pa.release_album_id = ra.id
  LEFT JOIN representative_spotify_album rsa ON rsa.release_album_id = ra.id
  WHERE rsa.spotify_album_id IS NOT NULL AND rsa.spotify_album_id != ''
)
SELECT
  spotify_album_id,
  count(*) AS duplicate_count,
  group_concat(release_album_id, ',') AS release_album_ids,
  group_concat(release_album_name || ' — ' || artist_name, ' | ') AS members
FROM base
GROUP BY spotify_album_id
HAVING count(*) > 1
ORDER BY duplicate_count DESC, spotify_album_id ASC
LIMIT 25;
"
```

Duplicate groups by normalized album name plus normalized primary artist:

```bash
python3 - <<'PY'
import json
import sqlite3

DB = "/Users/kahntra/Documents/ListenLab/listen-lab-main/backend/data/listenlabs.sqlite3"

def norm(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.strip().lower().split())

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
rows = conn.execute("""
WITH primary_artists AS (
  SELECT ordered.release_album_id, group_concat(ordered.artist_name, ', ') AS artist_name
  FROM (
    SELECT aa.release_album_id, a.canonical_name AS artist_name
    FROM album_artist aa
    JOIN artist a ON a.id = aa.artist_id
    WHERE aa.role = 'primary'
    ORDER BY aa.release_album_id, COALESCE(aa.billing_index, 999999), aa.id, a.canonical_name
  ) ordered
  GROUP BY ordered.release_album_id
)
SELECT
  ra.id AS release_album_id,
  ra.primary_name AS release_album_name,
  COALESCE(pa.artist_name, 'Unknown artist') AS artist_name
FROM release_album ra
LEFT JOIN primary_artists pa ON pa.release_album_id = ra.id
ORDER BY ra.id
""").fetchall()

groups = {}
for row in rows:
    album_key = norm(row["release_album_name"])
    artist_key = norm((row["artist_name"] or "").split(",")[0])
    if not album_key or not artist_key:
        continue
    key = (album_key, artist_key)
    groups.setdefault(key, []).append(
        {
            "release_album_id": row["release_album_id"],
            "release_album_name": row["release_album_name"],
            "artist_name": row["artist_name"],
        }
    )

items = [
    {
        "normalized_album_name": album_key,
        "normalized_primary_artist": artist_key,
        "duplicate_count": len(members),
        "release_albums": members,
    }
    for (album_key, artist_key), members in groups.items()
    if len(members) > 1
]
items.sort(key=lambda item: (-item["duplicate_count"], item["normalized_primary_artist"], item["normalized_album_name"]))
print(json.dumps(items[:25], indent=2))
PY
```

## Run Preview And Dry Run

Python helper path:

```bash
cd /Users/kahntra/Documents/ListenLab/listen-lab-main
python3 - <<'PY'
import json
import sys
from pathlib import Path

repo = Path("/Users/kahntra/Documents/ListenLab/listen-lab-main")
sys.path.insert(0, str(repo))

from backend.app.spotify_catalog_backfill import preview_release_album_merge, dry_run_release_album_merge

release_album_ids = [7337, 16151]

preview = preview_release_album_merge(release_album_ids)
print("PREVIEW")
print(json.dumps(preview, indent=2))

survivor_id = preview["survivor_release_album_id"]
dry_run = dry_run_release_album_merge(release_album_ids, survivor_release_album_id=survivor_id)
print("DRY RUN")
print(json.dumps(dry_run, indent=2))
PY
```

If the local API server is running and the browser session is already authenticated, equivalent curl commands are:

```bash
curl -sS -X POST http://127.0.0.1:8765/debug/identity/release-albums/merge-preview \
  -H 'Content-Type: application/json' \
  -d '{"release_album_ids":[7337,16151]}'

curl -sS -X POST http://127.0.0.1:8765/debug/identity/release-albums/merge-dry-run \
  -H 'Content-Type: application/json' \
  -d '{"release_album_ids":[7337,16151],"survivor_release_album_id":16151}'
```

## Evaluation Checklist

For each group:

- survivor choice
  - confirm the recommended survivor matches the documented ordering
  - check whether the survivor looks reasonable given accepted Spotify mapping, catalog match, associated tracks/listens, then lowest ID
- readiness class
  - confirm `safe_candidate` only when names match, primary artists match, a single Spotify album signal exists, and `album_track_conflicts == 0`
  - confirm `unsafe` when normalized album names differ or normalized primary artists differ
- warnings
  - confirm warnings are narrow and explain the risky condition
- row move plan
  - inspect `source_album_map_repoints`
  - inspect `album_artist_inserts` and `album_artist_deletes`
  - inspect `album_track_repoints`
  - inspect `album_track_conflicts`
  - inspect `release_album_retirements`
- forbidden table mutations
  - confirm the payload explicitly says `release_track rows are not changed directly`
  - confirm the payload explicitly says `analysis_track_map is not changed`
  - confirm no plan section proposes direct `release_track` or `analysis_track_map` writes
- idempotency and stability
  - run preview twice and compare payloads
  - run dry run twice with the same survivor and compare payloads
  - confirm blocked dry runs stay blocked with zero `rows_affected`

## Representative Local Cases

Validated against the local dev DB on `2026-05-01`:

- `safe_candidate`
  - IDs: `7337,16151`
  - album: `Boys by Girls`
  - artist: `Instupendo`
  - survivor: `16151`
- `needs_review`
  - IDs: `19,135`
  - album: `Shvat`
  - artist: `TATRAN`
  - survivor: `19`
  - reason: `album_track_conflicts > 0`
- `unsafe`
  - IDs: `8483,16159`
  - albums: `Runaway` and `Runaway (Deluxe)`
  - survivor recommendation: `16159`
  - blocked reason: normalized album names differ

Useful local one-liners:

```bash
cd /Users/kahntra/Documents/ListenLab/listen-lab-main
python3 - <<'PY'
import json
import sys
from pathlib import Path

repo = Path("/Users/kahntra/Documents/ListenLab/listen-lab-main")
sys.path.insert(0, str(repo))

from backend.app.spotify_catalog_backfill import preview_release_album_merge, dry_run_release_album_merge

for label, ids in {
    "safe_candidate": [7337, 16151],
    "needs_review": [19, 135],
    "unsafe": [8483, 16159],
}.items():
    p1 = preview_release_album_merge(ids)
    p2 = preview_release_album_merge(ids)
    d1 = dry_run_release_album_merge(ids, survivor_release_album_id=p1["survivor_release_album_id"])
    d2 = dry_run_release_album_merge(ids, survivor_release_album_id=p1["survivor_release_album_id"])
    print(label)
    print(json.dumps({
        "ids": ids,
        "preview_same": p1 == p2,
        "dry_run_same": d1 == d2,
        "merge_readiness": p1["merge_readiness"],
        "survivor_release_album_id": p1["survivor_release_album_id"],
        "blocked": d1["blocked"],
        "rows_affected": d1["rows_affected"],
        "statements": d1["statements"],
    }, indent=2))
PY
```
