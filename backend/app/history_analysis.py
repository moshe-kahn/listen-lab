from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

RECENT_WINDOW_DAYS = 90
MIN_PLAY_MS = 30_000
_CACHE: dict[str, Any] = {"signature": None, "summary": None}


@dataclass
class _Aggregate:
    name: str
    artist_name: str | None = None
    total_ms: int = 0
    recent_ms: int = 0
    play_count: int = 0
    recent_play_count: int = 0
    weighted_plays: float = 0.0
    recent_weighted_plays: float = 0.0
    distinct_tracks: set[str] | None = None
    recent_distinct_tracks: set[str] | None = None
    top_tracks: dict[str, int] | None = None

    def __post_init__(self) -> None:
        self.distinct_tracks = set()
        self.recent_distinct_tracks = set()
        self.top_tracks = defaultdict(int)


def _play_weight(ms_played: int, skipped: bool) -> float:
    base_weight = min(ms_played / 180_000, 1.0)
    return base_weight * (0.2 if skipped else 1.0)


def _score_artist(aggregate: _Aggregate, recent: bool) -> float:
    if recent:
        return (
            aggregate.recent_ms
            + len(aggregate.recent_distinct_tracks or set()) * 180_000
            + aggregate.recent_weighted_plays * 60_000
        )
    return (
        aggregate.total_ms
        + len(aggregate.distinct_tracks or set()) * 180_000
        + aggregate.weighted_plays * 60_000
    )


def _score_album(aggregate: _Aggregate, recent: bool) -> float:
    if recent:
        return (
            aggregate.recent_ms
            + len(aggregate.recent_distinct_tracks or set()) * 260_000
            + aggregate.recent_weighted_plays * 40_000
        )
    return (
        aggregate.total_ms
        + len(aggregate.distinct_tracks or set()) * 260_000
        + aggregate.weighted_plays * 40_000
    )


def _history_signature(history_dir: Path) -> tuple[tuple[str, int, int], ...]:
    files = sorted(history_dir.glob("Streaming_History_Audio_*.json"))
    return tuple((file.name, int(file.stat().st_mtime), file.stat().st_size) for file in files)


def get_history_signature(history_dir: str) -> tuple[tuple[str, int, int], ...] | None:
    path = Path(history_dir)
    if not path.exists() or not path.is_dir():
        return None

    signature = _history_signature(path)
    return signature or None


def clear_history_insights_cache() -> None:
    _CACHE["signature"] = None
    _CACHE["summary"] = None


def _iter_history_rows(history_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for file_path in sorted(history_dir.glob("Streaming_History_Audio_*.json")):
        with file_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, list):
            rows.extend(payload)
    return rows


def _normalize_artist_results(aggregates: dict[str, _Aggregate], limit: int, recent: bool) -> list[dict[str, Any]]:
    ranked = sorted(
        aggregates.values(),
        key=lambda aggregate: (
            -_score_artist(aggregate, recent),
            -(len(aggregate.recent_distinct_tracks if recent else aggregate.distinct_tracks) or 0),
            -aggregate.recent_ms if recent else -aggregate.total_ms,
            aggregate.name.lower(),
        ),
    )[:limit]

    return [
        {
            "artist_id": None,
            "name": aggregate.name,
            "followers_total": None,
            "genres": [],
            "popularity": None,
            "url": None,
            "image_url": None,
            "debug": {
                "source": "history",
                "score": round(_score_artist(aggregate, recent), 2),
                "total_ms": aggregate.recent_ms if recent else aggregate.total_ms,
                "play_count": aggregate.recent_play_count if recent else aggregate.play_count,
                "distinct_tracks": len(aggregate.recent_distinct_tracks if recent else aggregate.distinct_tracks),
            },
        }
        for aggregate in ranked
    ]


def _normalize_album_results(aggregates: dict[tuple[str, str], _Aggregate], limit: int, recent: bool) -> list[dict[str, Any]]:
    ranked = sorted(
        aggregates.values(),
        key=lambda aggregate: (
            -_score_album(aggregate, recent),
            -(len(aggregate.recent_distinct_tracks if recent else aggregate.distinct_tracks) or 0),
            -aggregate.recent_ms if recent else -aggregate.total_ms,
            aggregate.name.lower(),
        ),
    )[:limit]

    return [
        {
            "album_id": None,
            "name": aggregate.name,
            "artist_name": aggregate.artist_name,
            "url": None,
            "image_url": None,
            "track_representation_count": len(
                aggregate.recent_distinct_tracks if recent else aggregate.distinct_tracks
            ),
            "rank_score": round(_score_album(aggregate, recent), 2),
            "album_score": round(_score_album(aggregate, recent), 2),
            "represented_track_names": [
                track_name
                for track_name, _ms in sorted(
                    (aggregate.top_tracks or {}).items(),
                    key=lambda item: (-item[1], item[0].lower()),
                )[:3]
            ],
            "debug": {
                "source": "history",
                "score": round(_score_album(aggregate, recent), 2),
                "total_ms": aggregate.recent_ms if recent else aggregate.total_ms,
                "play_count": aggregate.recent_play_count if recent else aggregate.play_count,
                "distinct_tracks": len(aggregate.recent_distinct_tracks if recent else aggregate.distinct_tracks),
            },
        }
        for aggregate in ranked
    ]


def load_history_insights(history_dir: str, limit: int) -> dict[str, Any] | None:
    path = Path(history_dir)
    if not path.exists() or not path.is_dir():
        return None

    signature = _history_signature(path)
    if not signature:
        return None

    cached_signature = _CACHE.get("signature")
    cached_summary = _CACHE.get("summary")
    if cached_signature == signature and cached_summary is not None:
        return cached_summary

    rows = _iter_history_rows(path)
    latest_played_at: datetime | None = None
    parsed_timestamps: list[datetime | None] = []
    for row in rows:
        ts_value = row.get("ts")
        try:
            parsed = datetime.fromisoformat(str(ts_value).replace("Z", "+00:00"))
        except ValueError:
            parsed = None
        parsed_timestamps.append(parsed)
        if parsed and (latest_played_at is None or parsed > latest_played_at):
            latest_played_at = parsed

    if latest_played_at is None:
        return None

    cutoff = latest_played_at - timedelta(days=RECENT_WINDOW_DAYS)
    artists: dict[str, _Aggregate] = {}
    albums: dict[tuple[str, str], _Aggregate] = {}

    for row, played_at in zip(rows, parsed_timestamps):
        track_name = row.get("master_metadata_track_name")
        artist_name = row.get("master_metadata_album_artist_name")
        album_name = row.get("master_metadata_album_album_name")
        track_uri = row.get("spotify_track_uri")
        ms_played = int(row.get("ms_played") or 0)
        skipped = bool(row.get("skipped"))

        if (
            not track_name
            or not artist_name
            or not album_name
            or not track_uri
            or ms_played < MIN_PLAY_MS
            or played_at is None
        ):
            continue

        is_recent = played_at >= cutoff
        weight = _play_weight(ms_played, skipped)
        if weight <= 0:
            continue

        artist_entry = artists.setdefault(artist_name, _Aggregate(name=artist_name))
        artist_entry.total_ms += ms_played
        artist_entry.play_count += 1
        artist_entry.weighted_plays += weight
        artist_entry.distinct_tracks.add(track_uri)
        artist_entry.top_tracks[track_name] += ms_played
        if is_recent:
            artist_entry.recent_ms += ms_played
            artist_entry.recent_play_count += 1
            artist_entry.recent_weighted_plays += weight
            artist_entry.recent_distinct_tracks.add(track_uri)

        album_key = (album_name, artist_name)
        album_entry = albums.setdefault(album_key, _Aggregate(name=album_name, artist_name=artist_name))
        album_entry.total_ms += ms_played
        album_entry.play_count += 1
        album_entry.weighted_plays += weight
        album_entry.distinct_tracks.add(track_uri)
        album_entry.top_tracks[track_name] += ms_played
        if is_recent:
            album_entry.recent_ms += ms_played
            album_entry.recent_play_count += 1
            album_entry.recent_weighted_plays += weight
            album_entry.recent_distinct_tracks.add(track_uri)

    summary = {
        "artists_all_time": _normalize_artist_results(artists, limit, recent=False),
        "artists_recent": _normalize_artist_results(artists, limit, recent=True),
        "albums_all_time": _normalize_album_results(albums, limit, recent=False),
        "albums_recent": _normalize_album_results(albums, limit, recent=True),
        "source": str(path),
    }
    _CACHE["signature"] = signature
    _CACHE["summary"] = summary
    return summary
