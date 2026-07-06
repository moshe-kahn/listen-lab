from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
from dataclasses import dataclass
from typing import Literal

logger = logging.getLogger("listenlabs.artist_resolution")

ArtistResolutionOutcome = Literal[
    "matched_existing",
    "created_new_text_backed",
    "created_new_provider_backed",
    "ambiguous_review",
]

TEXT_ONLY_SOURCE_NAMES = {"history_raw"}

EVIDENCE_PROVIDER_SOURCE_ARTIST_MAP = "provider_source_artist_map"
EVIDENCE_TEXT_SOURCE_ARTIST_MAP = "text_source_artist_map"
EVIDENCE_PROVIDER_SOURCE_ARTIST_ID = "provider_source_artist_id"
EVIDENCE_TEXT_ARTIST_NAME = "text_artist_name"
EVIDENCE_SINGLE_PROVIDER_ARTIST_SAME_NORMALIZED_NAME = "single_provider_artist_same_normalized_name"
EVIDENCE_SHARED_RELEASE_ALBUM_ID = "shared_release_album_id"
EVIDENCE_SHARED_RELEASE_TRACK_ID = "shared_release_track_id"
EVIDENCE_RECONCILED_SOURCE_ALBUM = "reconciled_source_album"
EVIDENCE_RECONCILED_SOURCE_TRACK = "reconciled_source_track"
EVIDENCE_SHARED_NORMALIZED_ALBUM_TITLE_WITH_PROVIDER_CONTEXT = "shared_normalized_album_title_with_provider_context"
EVIDENCE_COMPOSITE_CREDIT_CONTEXT = "composite_credit_context"

IDENTITY_EVIDENCE_TYPES = {
    EVIDENCE_SHARED_RELEASE_ALBUM_ID,
    EVIDENCE_SHARED_RELEASE_TRACK_ID,
    EVIDENCE_RECONCILED_SOURCE_ALBUM,
    EVIDENCE_RECONCILED_SOURCE_TRACK,
}

REVIEW_REASON_MISSING_PROVIDER_ARTIST_ID = "missing_provider_artist_id"
REVIEW_REASON_EVIDENCED_COMPOSITE_ARTIST_CREDIT = "evidenced_composite_artist_credit"
REVIEW_REASON_MISSING_TEXT_ARTIST_NAME = "missing_text_artist_name"
REVIEW_REASON_MISSING_ARTIST_NAME = "missing_artist_name"
REVIEW_REASON_MISSING_NORMALIZED_ARTIST_NAME = "missing_normalized_artist_name"
REVIEW_REASON_PROVIDER_BACKED_NAME_COLLISION = "provider_backed_name_collision"
REVIEW_REASON_MISSING_ALBUM_TRACK_EVIDENCE = "missing_album_track_evidence"
REVIEW_REASON_AMBIGUOUS_TEXT_ONLY_ARTIST = "ambiguous_text_only_artist"
REVIEW_REASON_NO_TEXT_ONLY_ARTIST = "no_text_only_artist"


@dataclass(frozen=True)
class ArtistResolutionResult:
    artist_id: int | None
    outcome: ArtistResolutionOutcome
    source_artist_id: int | None = None
    normalized_name: str | None = None
    match_method: str | None = None
    confidence: float | None = None
    evidence: tuple[str, ...] = ()
    review_reason: str | None = None


def resolve_artist(
    connection: sqlite3.Connection,
    *,
    source_name: str,
    external_id: str | None = None,
    external_uri: str | None = None,
    artist_name: str | None = None,
    raw_payload_json: str | None = None,
    release_album_id: int | None = None,
    release_track_id: int | None = None,
) -> ArtistResolutionResult:
    if source_name in TEXT_ONLY_SOURCE_NAMES:
        return _resolve_text_artist(
            connection,
            source_name=source_name,
            artist_name_raw=artist_name,
            release_album_id=release_album_id,
            release_track_id=release_track_id,
        )
    if not external_id:
        return ArtistResolutionResult(
            artist_id=None,
            outcome="ambiguous_review",
            normalized_name=_normalize_name(artist_name),
            review_reason=REVIEW_REASON_MISSING_PROVIDER_ARTIST_ID,
        )
    return _resolve_provider_artist(
        connection,
        source_name=source_name,
        external_id=external_id,
        external_uri=external_uri,
        artist_name=artist_name,
        raw_payload_json=raw_payload_json,
        release_album_id=release_album_id,
        release_track_id=release_track_id,
    )


def _resolve_provider_artist(
    connection: sqlite3.Connection,
    *,
    source_name: str,
    external_id: str,
    external_uri: str | None,
    artist_name: str | None,
    raw_payload_json: str | None,
    release_album_id: int | None,
    release_track_id: int | None,
) -> ArtistResolutionResult:
    existing = connection.execute(
        """
        SELECT
          sa.id AS source_artist_id,
          sam.artist_id AS artist_id
        FROM source_artist sa
        LEFT JOIN source_artist_map sam
          ON sam.source_artist_id = sa.id
        WHERE sa.source_name = ?
          AND sa.external_id = ?
        ORDER BY sam.id ASC, sa.id ASC
        LIMIT 1
        """,
        (source_name, external_id),
    ).fetchone()
    if existing is not None and existing["artist_id"] is not None:
        return ArtistResolutionResult(
            artist_id=int(existing["artist_id"]),
            outcome="matched_existing",
            source_artist_id=int(existing["source_artist_id"]),
            normalized_name=_normalize_name(artist_name),
            match_method="provider_identity",
            confidence=1.0,
            evidence=(EVIDENCE_PROVIDER_SOURCE_ARTIST_MAP,),
        )

    if existing is None:
        cursor = connection.execute(
            """
            INSERT INTO source_artist (
              source_name,
              external_id,
              external_uri,
              source_name_raw,
              raw_payload_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (source_name, external_id, external_uri, artist_name, raw_payload_json),
        )
        source_artist_id = int(cursor.lastrowid)
    else:
        source_artist_id = int(existing["source_artist_id"])
        connection.execute(
            """
            UPDATE source_artist
            SET
              external_uri = COALESCE(external_uri, ?),
              source_name_raw = COALESCE(source_name_raw, ?),
              raw_payload_json = COALESCE(raw_payload_json, ?),
              updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ?
            """,
            (external_uri, artist_name, raw_payload_json, source_artist_id),
        )

    promotion = _find_safe_text_only_artist_for_provider_promotion(
        connection,
        provider_source_name=source_name,
        artist_name=artist_name,
        release_album_id=release_album_id,
        release_track_id=release_track_id,
    )
    if promotion.artist_id is not None:
        artist_id = promotion.artist_id
        connection.execute(
            """
            UPDATE artist
            SET
              canonical_name = COALESCE(NULLIF(?, ''), canonical_name),
              sort_name = COALESCE(NULLIF(?, ''), sort_name),
              updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ?
            """,
            (
                artist_name.strip() if artist_name and artist_name.strip() else None,
                _normalize_name(artist_name) if artist_name else None,
                artist_id,
            ),
        )
        outcome: ArtistResolutionOutcome = "matched_existing"
        evidence = promotion.evidence
    else:
        artist_id = _create_artist_with_connection(connection, artist_name=artist_name)
        outcome = "created_new_provider_backed"
        evidence = (EVIDENCE_PROVIDER_SOURCE_ARTIST_ID,)

    connection.execute(
        """
        INSERT OR IGNORE INTO source_artist_map (
          source_artist_id,
          artist_id,
          match_method,
          confidence,
          status,
          is_user_confirmed,
          explanation
        )
        VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, ?)
        """,
        (source_artist_id, artist_id, _provider_identity_explanation(source_name)),
    )
    return ArtistResolutionResult(
        artist_id=artist_id,
        outcome=outcome,
        source_artist_id=source_artist_id,
        normalized_name=_normalize_name(artist_name),
        match_method="provider_identity",
        confidence=1.0,
        evidence=evidence,
        review_reason=promotion.review_reason,
    )


def _resolve_text_artist(
    connection: sqlite3.Connection,
    *,
    source_name: str,
    artist_name_raw: str | None,
    release_album_id: int | None,
    release_track_id: int | None,
) -> ArtistResolutionResult:
    if _is_evidenced_composite_history_artist_credit(
        connection,
        artist_name_raw=artist_name_raw,
        release_album_id=release_album_id,
        release_track_id=release_track_id,
    ):
        logger.warning(
            "event=history_text_artist_mapping_skipped reason=evidenced_composite_artist_credit artist_name_raw=%s release_album_id=%s release_track_id=%s",
            artist_name_raw,
            release_album_id,
            release_track_id,
        )
        return ArtistResolutionResult(
            artist_id=None,
            outcome="ambiguous_review",
            normalized_name=_normalize_name(artist_name_raw),
            review_reason=REVIEW_REASON_EVIDENCED_COMPOSITE_ARTIST_CREDIT,
            evidence=(EVIDENCE_COMPOSITE_CREDIT_CONTEXT,),
        )

    artist_label = _normalized_history_artist_label(artist_name_raw)
    if artist_label is None:
        return ArtistResolutionResult(
            artist_id=None,
            outcome="ambiguous_review",
            review_reason=REVIEW_REASON_MISSING_TEXT_ARTIST_NAME,
        )

    normalized_name = _normalize_name(artist_label)
    external_id = _stable_text_key(f"{source_name}_artist", artist_label)
    existing = connection.execute(
        """
        SELECT
          sa.id AS source_artist_id,
          sam.artist_id AS artist_id
        FROM source_artist sa
        LEFT JOIN source_artist_map sam
          ON sam.source_artist_id = sa.id
        WHERE sa.source_name = ?
          AND sa.external_id = ?
        ORDER BY sam.id ASC, sa.id ASC
        LIMIT 1
        """,
        (source_name, external_id),
    ).fetchone()
    if existing is not None and existing["artist_id"] is not None:
        return ArtistResolutionResult(
            artist_id=int(existing["artist_id"]),
            outcome="matched_existing",
            source_artist_id=int(existing["source_artist_id"]),
            normalized_name=normalized_name,
            match_method="history_raw_text",
            confidence=0.6,
            evidence=(EVIDENCE_TEXT_SOURCE_ARTIST_MAP,),
        )

    provider_artist_id = _single_provider_artist_for_normalized_name(connection, normalized_name)

    if existing is None:
        cursor = connection.execute(
            """
            INSERT INTO source_artist (
              source_name,
              external_id,
              external_uri,
              source_name_raw,
              raw_payload_json
            )
            VALUES (?, ?, NULL, ?, NULL)
            """,
            (source_name, external_id, artist_label),
        )
        source_artist_id = int(cursor.lastrowid)
    else:
        source_artist_id = int(existing["source_artist_id"])

    if provider_artist_id is not None:
        artist_id = provider_artist_id
        outcome: ArtistResolutionOutcome = "matched_existing"
        evidence = (EVIDENCE_SINGLE_PROVIDER_ARTIST_SAME_NORMALIZED_NAME,)
    else:
        cursor = connection.execute(
            """
            INSERT INTO artist (
              canonical_name,
              sort_name
            )
            VALUES (?, ?)
            """,
            (artist_label, normalized_name),
        )
        artist_id = int(cursor.lastrowid)
        outcome = "created_new_text_backed"
        evidence = (EVIDENCE_TEXT_ARTIST_NAME,)
    connection.execute(
        """
        INSERT OR IGNORE INTO source_artist_map (
          source_artist_id,
          artist_id,
          match_method,
          confidence,
          status,
          is_user_confirmed,
          explanation
        )
        VALUES (?, ?, 'history_raw_text', 0.6, 'accepted', 0, 'Backfilled from raw artist_name_raw')
        """,
        (source_artist_id, artist_id),
    )
    return ArtistResolutionResult(
        artist_id=artist_id,
        outcome=outcome,
        source_artist_id=source_artist_id,
        normalized_name=normalized_name,
        match_method="history_raw_text",
        confidence=0.6,
        evidence=evidence,
    )


def _provider_identity_explanation(source_name: str) -> str:
    if source_name == "spotify":
        return "Exact Spotify artist ID backfill"
    return f"Exact {source_name} artist ID backfill"


def _normalize_name(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(str(value).strip().lower().split())
    return normalized or None


def _comma_artist_credit_parts(raw: str | None) -> list[tuple[str, str]]:
    if raw is None or "," not in str(raw):
        return []
    seen: set[str] = set()
    parts: list[tuple[str, str]] = []
    raw_parts = str(raw).split(",")
    for index, raw_part in enumerate(raw_parts):
        display = " ".join(raw_part.strip().split())
        if index == len(raw_parts) - 1:
            display = re.sub(r"\s+(?:&|and)\s+friends\.?$", "", display, flags=re.IGNORECASE).strip()
        normalized = _normalize_name(display)
        if not display or not normalized or normalized in seen:
            continue
        seen.add(normalized)
        parts.append((display, normalized))
    return parts


def _normalized_history_artist_label(raw: str | None) -> str | None:
    if raw is None:
        return None
    parts = _comma_artist_credit_parts(raw)
    if len(parts) == 1:
        return parts[0][0]
    artist_label = " ".join(str(raw).strip().split())
    return artist_label or None


def _single_provider_artist_for_normalized_name(connection: sqlite3.Connection, normalized_name: str | None) -> int | None:
    if not normalized_name:
        return None
    rows = connection.execute(
        """
        SELECT DISTINCT a.id
        FROM artist a
        JOIN source_artist_map sam
          ON sam.artist_id = a.id
        JOIN source_artist sa
          ON sa.id = sam.source_artist_id
         AND sa.source_name NOT IN ('history_raw')
        WHERE COALESCE(a.sort_name, lower(trim(a.canonical_name))) = ?
        ORDER BY a.id ASC
        """,
        (normalized_name,),
    ).fetchall()
    return int(rows[0][0]) if len(rows) == 1 else None


def _has_provider_artist_linked_to_context(
    connection: sqlite3.Connection,
    *,
    normalized_name: str,
    release_album_id: int | None,
    release_track_id: int | None,
) -> bool:
    if release_track_id is not None:
        row = connection.execute(
            """
            SELECT 1
            FROM track_artist ta
            JOIN artist a
              ON a.id = ta.artist_id
            JOIN source_artist_map sam
              ON sam.artist_id = a.id
            JOIN source_artist sa
              ON sa.id = sam.source_artist_id
             AND sa.source_name NOT IN ('history_raw')
            WHERE ta.release_track_id = ?
              AND COALESCE(a.sort_name, lower(trim(a.canonical_name))) = ?
            LIMIT 1
            """,
            (release_track_id, normalized_name),
        ).fetchone()
        if row is not None:
            return True
    if release_album_id is not None:
        row = connection.execute(
            """
            SELECT 1
            FROM album_artist aa
            JOIN artist a
              ON a.id = aa.artist_id
            JOIN source_artist_map sam
              ON sam.artist_id = a.id
            JOIN source_artist sa
              ON sa.id = sam.source_artist_id
             AND sa.source_name NOT IN ('history_raw')
            WHERE aa.release_album_id = ?
              AND COALESCE(a.sort_name, lower(trim(a.canonical_name))) = ?
            LIMIT 1
            """,
            (release_album_id, normalized_name),
        ).fetchone()
        if row is not None:
            return True
    return False


def _is_evidenced_composite_history_artist_credit(
    connection: sqlite3.Connection,
    *,
    artist_name_raw: str | None,
    release_album_id: int | None,
    release_track_id: int | None,
) -> bool:
    parts = _comma_artist_credit_parts(artist_name_raw)
    if len(parts) <= 1:
        return False
    full_normalized_name = _normalize_name(artist_name_raw)
    if _single_provider_artist_for_normalized_name(connection, full_normalized_name) is not None:
        return False
    return all(
        _has_provider_artist_linked_to_context(
            connection,
            normalized_name=normalized_part,
            release_album_id=release_album_id,
            release_track_id=release_track_id,
        )
        for _display_part, normalized_part in parts
    )


def _stable_text_key(*parts: str | None) -> str:
    payload = "|".join("" if part is None else str(part).strip() for part in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _create_artist_with_connection(
    connection: sqlite3.Connection,
    *,
    artist_name: str | None,
) -> int:
    canonical_name = artist_name.strip() if artist_name and artist_name.strip() else "Unknown artist"
    normalized_name = _normalize_name(canonical_name)
    cursor = connection.execute(
        """
        INSERT INTO artist (
          canonical_name,
          sort_name
        )
        VALUES (?, ?)
        """,
        (canonical_name, normalized_name),
    )
    return int(cursor.lastrowid)


@dataclass(frozen=True)
class _PromotionResolution:
    artist_id: int | None
    evidence: tuple[str, ...] = ()
    review_reason: str | None = None


def _find_safe_text_only_artist_for_provider_promotion(
    connection: sqlite3.Connection,
    *,
    provider_source_name: str,
    artist_name: str | None,
    release_album_id: int | None,
    release_track_id: int | None,
) -> _PromotionResolution:
    canonical_name = artist_name.strip() if artist_name and artist_name.strip() else None
    if canonical_name is None:
        return _PromotionResolution(None, review_reason=REVIEW_REASON_MISSING_ARTIST_NAME)
    normalized_name = _normalize_name(canonical_name)
    if not normalized_name:
        return _PromotionResolution(None, review_reason=REVIEW_REASON_MISSING_NORMALIZED_ARTIST_NAME)

    rows = connection.execute(
        """
        SELECT
          a.id AS artist_id,
          count(sam.id) AS source_map_count,
          sum(CASE WHEN sa.source_name NOT IN ('history_raw') THEN 1 ELSE 0 END) AS provider_map_count
        FROM artist a
        LEFT JOIN source_artist_map sam
          ON sam.artist_id = a.id
        LEFT JOIN source_artist sa
          ON sa.id = sam.source_artist_id
        WHERE COALESCE(a.sort_name, lower(trim(a.canonical_name))) = ?
        GROUP BY a.id
        ORDER BY a.id ASC
        """,
        (normalized_name,),
    ).fetchall()
    text_only_artist_ids = [
        int(row["artist_id"])
        for row in rows
        if int(row["source_map_count"] or 0) > 0 and int(row["provider_map_count"] or 0) == 0
    ]
    provider_artist_ids = [
        int(row["artist_id"])
        for row in rows
        if int(row["provider_map_count"] or 0) > 0
    ]
    if provider_artist_ids:
        logger.warning(
            "event=%s_artist_text_promotion_skipped reason=provider_backed_name_collision normalized_name=%s provider_artist_ids=%s text_only_artist_ids=%s",
            provider_source_name,
            normalized_name,
            provider_artist_ids,
            text_only_artist_ids,
        )
        _record_artist_promotion_skip(
            connection,
            reason=REVIEW_REASON_PROVIDER_BACKED_NAME_COLLISION,
            normalized_name=normalized_name,
            release_album_id=release_album_id,
            release_track_id=release_track_id,
            provider_artist_ids=provider_artist_ids,
            text_only_artist_ids=text_only_artist_ids,
        )
        return _PromotionResolution(None, review_reason=REVIEW_REASON_PROVIDER_BACKED_NAME_COLLISION)
    if len(text_only_artist_ids) == 1:
        artist_id = text_only_artist_ids[0]
        has_track_evidence = release_track_id is not None and connection.execute(
            """
            SELECT 1
            FROM track_artist
            WHERE artist_id = ?
              AND release_track_id = ?
            LIMIT 1
            """,
            (artist_id, release_track_id),
        ).fetchone() is not None
        if has_track_evidence:
            return _PromotionResolution(artist_id, evidence=(EVIDENCE_SHARED_RELEASE_TRACK_ID,))

        has_album_evidence = release_album_id is not None and connection.execute(
            """
            SELECT 1
            FROM album_artist
            WHERE artist_id = ?
              AND release_album_id = ?
            LIMIT 1
            """,
            (artist_id, release_album_id),
        ).fetchone() is not None
        if has_album_evidence:
            return _PromotionResolution(artist_id, evidence=(EVIDENCE_SHARED_RELEASE_ALBUM_ID,))

        has_album_title_provider_context = release_album_id is not None and connection.execute(
            """
            SELECT 1
            FROM release_album provider_album
            JOIN release_album text_album
              ON text_album.id != provider_album.id
             AND text_album.normalized_name = provider_album.normalized_name
            JOIN album_artist text_album_artist
              ON text_album_artist.release_album_id = text_album.id
             AND text_album_artist.artist_id = ?
            WHERE provider_album.id = ?
              AND provider_album.normalized_name IS NOT NULL
              AND (
                EXISTS (
                  SELECT 1
                  FROM source_album_map provider_album_map
                  JOIN source_album provider_source_album
                    ON provider_source_album.id = provider_album_map.source_album_id
                   AND provider_source_album.source_name NOT IN ('history_raw')
                  WHERE provider_album_map.release_album_id = provider_album.id
                )
                OR EXISTS (
                  SELECT 1
                  FROM album_track provider_album_track
                  JOIN source_track_map provider_track_map
                    ON provider_track_map.release_track_id = provider_album_track.release_track_id
                  JOIN source_track provider_source_track
                    ON provider_source_track.id = provider_track_map.source_track_id
                   AND provider_source_track.source_name NOT IN ('history_raw')
                  WHERE provider_album_track.release_album_id = provider_album.id
                )
              )
            LIMIT 1
            """,
            (artist_id, release_album_id),
        ).fetchone() is not None
        if has_album_title_provider_context:
            return _PromotionResolution(artist_id, evidence=(EVIDENCE_SHARED_NORMALIZED_ALBUM_TITLE_WITH_PROVIDER_CONTEXT,))
        logger.info(
            "event=%s_artist_text_promotion_skipped reason=missing_album_track_evidence normalized_name=%s artist_id=%s release_album_id=%s release_track_id=%s",
            provider_source_name,
            normalized_name,
            artist_id,
            release_album_id,
            release_track_id,
        )
        _record_artist_promotion_skip(
            connection,
            reason=REVIEW_REASON_MISSING_ALBUM_TRACK_EVIDENCE,
            normalized_name=normalized_name,
            artist_id=artist_id,
            release_album_id=release_album_id,
            release_track_id=release_track_id,
            text_only_artist_ids=[artist_id],
        )
        return _PromotionResolution(None, review_reason=REVIEW_REASON_MISSING_ALBUM_TRACK_EVIDENCE)
    if len(text_only_artist_ids) > 1:
        logger.warning(
            "event=%s_artist_text_promotion_skipped reason=ambiguous_text_only_artist normalized_name=%s artist_ids=%s",
            provider_source_name,
            normalized_name,
            text_only_artist_ids,
        )
        _record_artist_promotion_skip(
            connection,
            reason=REVIEW_REASON_AMBIGUOUS_TEXT_ONLY_ARTIST,
            normalized_name=normalized_name,
            release_album_id=release_album_id,
            release_track_id=release_track_id,
            text_only_artist_ids=text_only_artist_ids,
        )
        return _PromotionResolution(None, review_reason=REVIEW_REASON_AMBIGUOUS_TEXT_ONLY_ARTIST)
    return _PromotionResolution(None, review_reason=REVIEW_REASON_NO_TEXT_ONLY_ARTIST)


def _artist_promotion_skip_signature(
    *,
    reason: str,
    normalized_name: str,
    artist_id: int | None,
    release_album_id: int | None,
    release_track_id: int | None,
    provider_artist_ids: list[int],
    text_only_artist_ids: list[int],
) -> str:
    payload = {
        "artist_id": artist_id,
        "normalized_name": normalized_name,
        "provider_artist_ids": provider_artist_ids,
        "reason": reason,
        "release_album_id": release_album_id,
        "release_track_id": release_track_id,
        "text_only_artist_ids": text_only_artist_ids,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _sorted_int_list(values: list[int] | tuple[int, ...] | None) -> list[int]:
    return sorted({int(value) for value in values or [] if value is not None})


def _record_artist_promotion_skip(
    connection: sqlite3.Connection,
    *,
    reason: str,
    normalized_name: str,
    artist_id: int | None = None,
    release_album_id: int | None = None,
    release_track_id: int | None = None,
    provider_artist_ids: list[int] | tuple[int, ...] | None = None,
    text_only_artist_ids: list[int] | tuple[int, ...] | None = None,
) -> None:
    normalized_provider_ids = _sorted_int_list(provider_artist_ids)
    normalized_text_ids = _sorted_int_list(text_only_artist_ids)
    signature_key = _artist_promotion_skip_signature(
        reason=reason,
        normalized_name=normalized_name,
        artist_id=artist_id,
        release_album_id=release_album_id,
        release_track_id=release_track_id,
        provider_artist_ids=normalized_provider_ids,
        text_only_artist_ids=normalized_text_ids,
    )
    connection.execute(
        """
        INSERT INTO artist_promotion_skip_log (
          signature_key,
          reason,
          normalized_name,
          artist_id,
          release_album_id,
          release_track_id,
          provider_artist_ids_json,
          text_only_artist_ids_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(signature_key) DO UPDATE SET
          occurrence_count = artist_promotion_skip_log.occurrence_count + 1,
          last_seen_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        """,
        (
            signature_key,
            reason,
            normalized_name,
            artist_id,
            release_album_id,
            release_track_id,
            json.dumps(normalized_provider_ids),
            json.dumps(normalized_text_ids),
        ),
    )
