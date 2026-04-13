from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

DEFAULT_RECENT_WINDOW_DAYS = 28
MIN_PLAY_MS = 30_000
MIN_ALBUM_DISTINCT_TRACKS = 3
MIN_RECENT_ALBUM_DISTINCT_TRACKS = 2
_CACHE: dict[str, Any] = {"signature": None, "recent_window_days": None, "summary": None}


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


def _track_identity_key(track_name: str, artist_name: str) -> str:
    track_part = " ".join(track_name.strip().lower().split())
    artist_part = " ".join(artist_name.strip().lower().split())
    return f"{track_part}|||{artist_part}"


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


def _track_longevity_score(
    *,
    span_days: int,
    active_months_count: int,
    span_months_count: int,
    play_count: int,
) -> float:
    if span_days <= 0 or active_months_count <= 0 or span_months_count <= 0 or play_count <= 0:
        return 0.0

    span_years = span_days / 365.25
    consistency_ratio = min(1.0, active_months_count / span_months_count)

    # Longevity should clearly differ from simple play-count ranking:
    # favor tracks that stay alive across many years, while still penalizing
    # sparse one-off gaps via monthly consistency.
    # Age-first weighting: span dominates; consistency and play count only refine.
    active_months_factor = active_months_count ** 0.75
    consistency_factor = consistency_ratio ** 1.15
    span_factor = max(span_years, 0.0) ** 2.7
    play_factor = max(play_count, 1) ** 0.02

    return active_months_factor * consistency_factor * span_factor * play_factor


def _history_files(history_dir: Path) -> list[Path]:
    patterns = (
        "Streaming_History_Audio_*.json",
        "endsong_*.json",
        "StreamingHistory*.json",
    )
    files: dict[str, Path] = {}
    for pattern in patterns:
        for file_path in history_dir.glob(pattern):
            files[str(file_path.resolve())] = file_path
    return sorted(files.values(), key=lambda item: item.name.lower())


def _history_signature(history_dir: Path) -> tuple[tuple[str, int, int], ...]:
    files = _history_files(history_dir)
    return tuple((file.name, int(file.stat().st_mtime), file.stat().st_size) for file in files)


def get_history_signature(history_dir: str) -> tuple[tuple[str, int, int], ...] | None:
    path = Path(history_dir)
    if not path.exists() or not path.is_dir():
        return None

    signature = _history_signature(path)
    return signature or None


def clear_history_insights_cache() -> None:
    _CACHE["signature"] = None
    _CACHE["recent_window_days"] = None
    _CACHE["summary"] = None


def _iter_history_rows(history_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for file_path in _history_files(history_dir):
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
    primary_minimum_distinct_tracks = MIN_RECENT_ALBUM_DISTINCT_TRACKS if recent else MIN_ALBUM_DISTINCT_TRACKS

    def _rank_with_threshold(minimum_distinct_tracks: int) -> list[_Aggregate]:
        return sorted(
            [
                aggregate
                for aggregate in aggregates.values()
                if len(aggregate.recent_distinct_tracks if recent else aggregate.distinct_tracks) >= minimum_distinct_tracks
            ],
            key=lambda aggregate: (
                -_score_album(aggregate, recent),
                -(len(aggregate.recent_distinct_tracks if recent else aggregate.distinct_tracks) or 0),
                -aggregate.recent_ms if recent else -aggregate.total_ms,
                aggregate.name.lower(),
            ),
        )[:limit]

    ranked = _rank_with_threshold(primary_minimum_distinct_tracks)
    if recent and len(ranked) < min(limit, 3):
        ranked = _rank_with_threshold(1)

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


def load_history_insights(history_dir: str, limit: int, recent_window_days: int = DEFAULT_RECENT_WINDOW_DAYS) -> dict[str, Any] | None:
    path = Path(history_dir)
    if not path.exists() or not path.is_dir():
        return None

    signature = _history_signature(path)
    if not signature:
        return None

    cached_signature = _CACHE.get("signature")
    track_limit = max(limit, 50)
    cached_summary = _CACHE.get("summary")
    if (
        cached_signature == signature
        and _CACHE.get("recent_window_days") == recent_window_days
        and _CACHE.get("track_limit") == track_limit
        and cached_summary is not None
    ):
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

    cutoff = latest_played_at - timedelta(days=recent_window_days)
    artists: dict[str, _Aggregate] = {}
    albums: dict[tuple[str, str], _Aggregate] = {}
    track_play_counts_all_time: dict[str, int] = defaultdict(int)
    track_play_counts_recent: dict[str, int] = defaultdict(int)
    track_first_played_at: dict[str, datetime] = {}
    track_last_played_at: dict[str, datetime] = {}
    track_active_months: dict[str, set[str]] = defaultdict(set)
    track_key_play_counts_all_time: dict[str, int] = defaultdict(int)
    track_key_play_counts_recent: dict[str, int] = defaultdict(int)
    track_key_total_ms: dict[str, int] = defaultdict(int)
    track_key_recent_ms: dict[str, int] = defaultdict(int)
    track_key_first_played_at: dict[str, datetime] = {}
    track_key_last_played_at: dict[str, datetime] = {}
    track_key_active_months: dict[str, set[str]] = defaultdict(set)
    track_key_latest_meta: dict[str, dict[str, Any]] = {}

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

        track_play_counts_all_time[track_uri] += 1
        if is_recent:
            track_play_counts_recent[track_uri] += 1
        track_active_months[track_uri].add(played_at.strftime("%Y-%m"))
        track_key = _track_identity_key(str(track_name), str(artist_name))
        track_key_play_counts_all_time[track_key] += 1
        track_key_total_ms[track_key] += ms_played
        if is_recent:
            track_key_play_counts_recent[track_key] += 1
            track_key_recent_ms[track_key] += ms_played
        track_key_active_months[track_key].add(played_at.strftime("%Y-%m"))
        track_key_latest = track_key_latest_meta.get(track_key)
        if track_key_latest is None or played_at >= track_key_latest.get("played_at", datetime.min.replace(tzinfo=UTC)):
            track_key_latest_meta[track_key] = {
                "played_at": played_at,
                "track_name": track_name,
                "artist_name": artist_name,
                "album_name": album_name,
                "spotify_track_uri": track_uri,
            }
        first_seen = track_first_played_at.get(track_uri)
        if first_seen is None or played_at < first_seen:
            track_first_played_at[track_uri] = played_at
        last_seen = track_last_played_at.get(track_uri)
        if last_seen is None or played_at > last_seen:
            track_last_played_at[track_uri] = played_at
        track_key_first_seen = track_key_first_played_at.get(track_key)
        if track_key_first_seen is None or played_at < track_key_first_seen:
            track_key_first_played_at[track_key] = played_at
        track_key_last_seen = track_key_last_played_at.get(track_key)
        if track_key_last_seen is None or played_at > track_key_last_seen:
            track_key_last_played_at[track_key] = played_at

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
        album_track_key = " ".join(str(track_name).strip().lower().split())
        album_entry = albums.setdefault(album_key, _Aggregate(name=album_name, artist_name=artist_name))
        album_entry.total_ms += ms_played
        album_entry.play_count += 1
        album_entry.weighted_plays += weight
        album_entry.distinct_tracks.add(album_track_key)
        album_entry.top_tracks[track_name] += ms_played
        if is_recent:
            album_entry.recent_ms += ms_played
            album_entry.recent_play_count += 1
            album_entry.recent_weighted_plays += weight
            album_entry.recent_distinct_tracks.add(album_track_key)

    track_history_metrics: dict[str, dict[str, Any]] = {}
    for track_uri, play_count in track_play_counts_all_time.items():
        first_seen = track_first_played_at.get(track_uri)
        last_seen = track_last_played_at.get(track_uri)
        span_days = (
            max(0, int((last_seen - first_seen).total_seconds() // 86_400))
            if first_seen and last_seen
            else 0
        )
        span_months = max(1, int(span_days // 30) + 1)
        active_months_count = len(track_active_months.get(track_uri, set()))
        consistency_ratio = min(1.0, active_months_count / span_months) if span_months > 0 else 0.0
        longevity_score = _track_longevity_score(
            span_days=span_days,
            active_months_count=active_months_count,
            span_months_count=span_months,
            play_count=int(play_count),
        )
        track_history_metrics[track_uri] = {
            "play_count": int(play_count),
            "recent_play_count": int(track_play_counts_recent.get(track_uri, 0)),
            "first_played_at": first_seen.isoformat() if first_seen else None,
            "last_played_at": last_seen.isoformat() if last_seen else None,
            "listening_span_days": span_days,
            "listening_span_years": round(span_days / 365.25, 3),
            "active_months_count": active_months_count,
            "span_months_count": span_months,
            "consistency_ratio": round(consistency_ratio, 4),
            "longevity_score": round(longevity_score, 4),
        }

    track_history_metrics_by_key: dict[str, dict[str, Any]] = {}
    for track_key, play_count in track_key_play_counts_all_time.items():
        first_seen = track_key_first_played_at.get(track_key)
        last_seen = track_key_last_played_at.get(track_key)
        span_days = (
            max(0, int((last_seen - first_seen).total_seconds() // 86_400))
            if first_seen and last_seen
            else 0
        )
        span_months = max(1, int(span_days // 30) + 1)
        active_months_count = len(track_key_active_months.get(track_key, set()))
        consistency_ratio = min(1.0, active_months_count / span_months) if span_months > 0 else 0.0
        longevity_score = _track_longevity_score(
            span_days=span_days,
            active_months_count=active_months_count,
            span_months_count=span_months,
            play_count=int(play_count),
        )
        track_history_metrics_by_key[track_key] = {
            "play_count": int(play_count),
            "recent_play_count": int(track_key_play_counts_recent.get(track_key, 0)),
            "first_played_at": first_seen.isoformat() if first_seen else None,
            "last_played_at": last_seen.isoformat() if last_seen else None,
            "listening_span_days": span_days,
            "listening_span_years": round(span_days / 365.25, 3),
            "active_months_count": active_months_count,
            "span_months_count": span_months,
            "consistency_ratio": round(consistency_ratio, 4),
            "longevity_score": round(longevity_score, 4),
        }

    all_keys = list(track_history_metrics_by_key.keys())
    top_by_plays = sorted(
        all_keys,
        key=lambda key: (
            -int(track_history_metrics_by_key[key].get("play_count", 0)),
            -int(track_key_total_ms.get(key, 0)),
            str(track_key_latest_meta.get(key, {}).get("track_name", "")).lower(),
        ),
    )
    top_by_longevity = sorted(
        all_keys,
        key=lambda key: (
            -float(track_history_metrics_by_key[key].get("longevity_score", 0.0)),
            -int(track_history_metrics_by_key[key].get("active_months_count", 0)),
            -int(track_key_total_ms.get(key, 0)),
            str(track_key_latest_meta.get(key, {}).get("track_name", "")).lower(),
        ),
    )
    candidate_keys: list[str] = []
    seen_keys: set[str] = set()
    index = 0
    while len(candidate_keys) < track_limit and (index < len(top_by_plays) or index < len(top_by_longevity)):
        if index < len(top_by_plays):
            key = top_by_plays[index]
            if key not in seen_keys:
                seen_keys.add(key)
                candidate_keys.append(key)
                if len(candidate_keys) >= track_limit:
                    break
        if index < len(top_by_longevity):
            key = top_by_longevity[index]
            if key not in seen_keys:
                seen_keys.add(key)
                candidate_keys.append(key)
                if len(candidate_keys) >= track_limit:
                    break
        index += 1

    tracks_all_time: list[dict[str, Any]] = []
    for key in candidate_keys:
        metrics = track_history_metrics_by_key.get(key, {})
        meta = track_key_latest_meta.get(key, {})
        uri = meta.get("spotify_track_uri")
        track_id = uri.split(":")[-1] if isinstance(uri, str) and uri.startswith("spotify:track:") else None
        tracks_all_time.append(
            {
                "track_id": track_id,
                "track_name": meta.get("track_name"),
                "artist_name": meta.get("artist_name"),
                "album_name": meta.get("album_name"),
                "uri": uri,
                "url": f"https://open.spotify.com/track/{track_id}" if track_id else None,
                "play_count": int(metrics.get("play_count", 0) or 0),
                "all_time_play_count": int(metrics.get("play_count", 0) or 0),
                "recent_play_count": int(metrics.get("recent_play_count", 0) or 0),
                "first_played_at": metrics.get("first_played_at"),
                "last_played_at": metrics.get("last_played_at"),
                "listening_span_days": int(metrics.get("listening_span_days", 0) or 0),
                "listening_span_years": float(metrics.get("listening_span_years", 0.0) or 0.0),
                "active_months_count": int(metrics.get("active_months_count", 0) or 0),
                "span_months_count": int(metrics.get("span_months_count", 0) or 0),
                "consistency_ratio": float(metrics.get("consistency_ratio", 0.0) or 0.0),
                "longevity_score": float(metrics.get("longevity_score", 0.0) or 0.0),
            }
        )

    recent_keys = sorted(
        all_keys,
        key=lambda key: (
            -int(track_history_metrics_by_key[key].get("recent_play_count", 0)),
            -int(track_key_recent_ms.get(key, 0)),
            str(track_key_latest_meta.get(key, {}).get("track_name", "")).lower(),
        ),
    )[:limit]
    tracks_recent: list[dict[str, Any]] = []
    for key in recent_keys:
        metrics = track_history_metrics_by_key.get(key, {})
        meta = track_key_latest_meta.get(key, {})
        uri = meta.get("spotify_track_uri")
        track_id = uri.split(":")[-1] if isinstance(uri, str) and uri.startswith("spotify:track:") else None
        tracks_recent.append(
            {
                "track_id": track_id,
                "track_name": meta.get("track_name"),
                "artist_name": meta.get("artist_name"),
                "album_name": meta.get("album_name"),
                "uri": uri,
                "url": f"https://open.spotify.com/track/{track_id}" if track_id else None,
                "play_count": int(metrics.get("recent_play_count", 0) or 0),
                "all_time_play_count": int(metrics.get("play_count", 0) or 0),
                "recent_play_count": int(metrics.get("recent_play_count", 0) or 0),
                "first_played_at": metrics.get("first_played_at"),
                "last_played_at": metrics.get("last_played_at"),
                "listening_span_days": int(metrics.get("listening_span_days", 0) or 0),
                "listening_span_years": float(metrics.get("listening_span_years", 0.0) or 0.0),
                "active_months_count": int(metrics.get("active_months_count", 0) or 0),
                "span_months_count": int(metrics.get("span_months_count", 0) or 0),
                "consistency_ratio": float(metrics.get("consistency_ratio", 0.0) or 0.0),
                "longevity_score": float(metrics.get("longevity_score", 0.0) or 0.0),
            }
        )

    summary = {
        "artists_all_time": _normalize_artist_results(artists, limit, recent=False),
        "artists_recent": _normalize_artist_results(artists, limit, recent=True),
        "albums_all_time": _normalize_album_results(albums, limit, recent=False),
        "albums_recent": _normalize_album_results(albums, limit, recent=True),
        "first_played_at": min(
            (played_at for played_at in parsed_timestamps if played_at is not None),
            default=latest_played_at,
        ).isoformat(),
        "last_played_at": latest_played_at.isoformat(),
        "total_listen_ms": sum(aggregate.total_ms for aggregate in artists.values()),
        "total_play_count": sum(aggregate.play_count for aggregate in artists.values()),
        "track_play_counts_all_time": dict(track_play_counts_all_time),
        "track_play_counts_recent": dict(track_play_counts_recent),
        "track_history_metrics": track_history_metrics,
        "track_history_metrics_by_key": track_history_metrics_by_key,
        "tracks_all_time": tracks_all_time,
        "tracks_recent": tracks_recent,
        "recent_window_days": recent_window_days,
        "source": str(path),
    }
    _CACHE["signature"] = signature
    _CACHE["recent_window_days"] = recent_window_days
    _CACHE["track_limit"] = track_limit
    _CACHE["summary"] = summary
    return summary
