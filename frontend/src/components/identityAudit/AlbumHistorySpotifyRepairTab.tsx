import { useMemo, useState } from "react";

import { repairReleaseAlbumsHistorySpotify } from "../../api/appApi";
import type {
  ReleaseAlbumHistorySpotifyRepairCandidate,
  ReleaseAlbumHistorySpotifyRepairResponse,
} from "../../types/appTypes";

type RepairFilter = "all" | "safe" | "blocked";

function rowCount(candidate: ReleaseAlbumHistorySpotifyRepairCandidate, key: string) {
  return Number(candidate.rows_affected?.[key] ?? 0);
}

function candidateChangeSummary(candidate: ReleaseAlbumHistorySpotifyRepairCandidate) {
  const sourceMaps = rowCount(candidate, "source_album_map");
  const artistDeletes = rowCount(candidate, "album_artist_delete");
  const artistInserts = rowCount(candidate, "album_artist_insert");
  const trackRepoints = rowCount(candidate, "album_track_repoint");
  const trackConflicts = rowCount(candidate, "album_track_conflict_delete");
  const retiredAlbums = rowCount(candidate, "release_album_retire");
  return [
    sourceMaps ? `${sourceMaps} source map${sourceMaps === 1 ? "" : "s"}` : null,
    artistInserts || artistDeletes ? `${artistInserts} artist insert · ${artistDeletes} delete` : null,
    trackRepoints ? `${trackRepoints} track repoint${trackRepoints === 1 ? "" : "s"}` : null,
    trackConflicts ? `${trackConflicts} track conflict${trackConflicts === 1 ? "" : "s"}` : null,
    retiredAlbums ? `${retiredAlbums} album retire${retiredAlbums === 1 ? "" : "s"}` : null,
  ].filter(Boolean).join(" · ") || "No row changes";
}

function spotifyAlbumUrl(spotifyAlbumId: string | null | undefined) {
  return spotifyAlbumId ? `https://open.spotify.com/album/${spotifyAlbumId}` : "";
}

function formatLoadedAt(value: string | null) {
  return value ? new Date(value).toLocaleTimeString() : null;
}

function CandidateCard({ candidate }: { candidate: ReleaseAlbumHistorySpotifyRepairCandidate }) {
  const sourceMapCount = candidate.plan.source_album_map_repoints.length;
  const trackRepointCount = candidate.plan.album_track_repoints.length;
  const conflictCount = candidate.plan.album_track_conflicts.length;
  const firstRepoints = candidate.plan.album_track_repoints.slice(0, 4);
  const firstConflicts = candidate.plan.album_track_conflicts.slice(0, 3);
  const title = `${candidate.release_album_name || "Untitled album"} · ${candidate.normalized_artist || "unknown artist"}`;

  return (
    <article className="identity-audit-example album-repair-card">
      <div className="identity-audit-example-header">
        <div className="identity-audit-card-title-button">
          <span>
            <strong>{candidate.release_album_name || "Untitled album"}</strong>
            <small>{candidate.normalized_artist || "unknown artist"}</small>
          </span>
        </div>
        <span className={`identity-audit-type-badge ${candidate.safe ? "recording-track-good" : "recording-track-warn"}`}>
          {candidate.safe ? "ready to apply" : "needs review"}
        </span>
      </div>

      <div className="identity-audit-card-summary">
        <div>
          <span>What happened</span>
          <strong>History-only album matches a Spotify-backed album.</strong>
        </div>
        <div>
          <span>{candidate.safe ? "Why safe" : "Why blocked"}</span>
          <strong>{candidate.safe ? candidateChangeSummary(candidate) : candidate.blocked_reasons.join(", ") || "Repair evidence is incomplete."}</strong>
        </div>
        <div>
          <span>Action</span>
          <strong>{candidate.safe ? "Can be applied from the latest dry run." : "Blocked candidates are not applied."}</strong>
        </div>
      </div>

      <details className="identity-audit-details">
        <summary>Details</summary>
        <div className="identity-audit-stats">
          <span className="identity-audit-stat"><span>survivor</span><strong>{candidate.survivor_release_album_id}</strong></span>
          <span className="identity-audit-stat"><span>merge</span><strong>{candidate.merge_release_album_ids.join(", ")}</strong></span>
          <span className="identity-audit-stat"><span>readiness</span><strong>{candidate.merge_readiness}</strong></span>
          <span className="identity-audit-stat"><span>source maps</span><strong>{sourceMapCount}</strong></span>
          <span className="identity-audit-stat"><span>track repoints</span><strong>{trackRepointCount}</strong></span>
          <span className="identity-audit-stat"><span>conflicts</span><strong>{conflictCount}</strong></span>
        </div>

        {candidate.spotify_album_id ? (
          <a className="identity-audit-note" href={spotifyAlbumUrl(candidate.spotify_album_id)} rel="noreferrer" target="_blank">
            Spotify album {candidate.spotify_album_id}
          </a>
        ) : null}

        {candidate.blocked_reasons.length > 0 ? (
          <div className="album-repair-reasons" aria-label={`${title} blocked reasons`}>
            {candidate.blocked_reasons.map((reason) => (
              <span className="identity-audit-stat recording-track-warn" key={`${candidate.release_album_ids.join("-")}-${reason}`}>
                <strong>{reason}</strong>
              </span>
            ))}
          </div>
        ) : null}

        {firstRepoints.length > 0 || firstConflicts.length > 0 ? (
          <div className="album-repair-plan-list">
            {firstRepoints.map((row, index) => (
              <span className="identity-audit-stat" key={`album-repair-repoint-${candidate.release_album_ids.join("-")}-${index}`}>
                <span>move</span>
                <strong>{String(row.release_track_name ?? row.release_track_id ?? "release track")}</strong>
              </span>
            ))}
            {firstConflicts.map((row, index) => (
              <span className="identity-audit-stat recording-track-warn" key={`album-repair-conflict-${candidate.release_album_ids.join("-")}-${index}`}>
                <span>conflict</span>
                <strong>{String(row.release_track_name ?? row.release_track_id ?? "release track")}</strong>
              </span>
            ))}
            {trackRepointCount + conflictCount > firstRepoints.length + firstConflicts.length ? (
              <span className="identity-audit-stat">
                <span>more</span>
                <strong>{trackRepointCount + conflictCount - firstRepoints.length - firstConflicts.length}</strong>
              </span>
            ) : null}
          </div>
        ) : null}
      </details>
    </article>
  );
}

export function AlbumHistorySpotifyRepairTab() {
  const [plan, setPlan] = useState<ReleaseAlbumHistorySpotifyRepairResponse | null>(null);
  const [filter, setFilter] = useState<RepairFilter>("all");
  const [limit, setLimit] = useState(50);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastRunAt, setLastRunAt] = useState<string | null>(null);

  async function runRepair(dryRun: boolean) {
    if (!dryRun) {
      const safeCount = plan?.safe_candidate_count ?? 0;
      const ok = window.confirm(
        `Apply safe release-album repair?\n\nThis will merge only the ${safeCount} safe history/Spotify album candidates from the latest dry run. Blocked candidates are not applied.`,
      );
      if (!ok) {
        return;
      }
    }

    setLoading(true);
    setError(null);
    try {
      const payload = await repairReleaseAlbumsHistorySpotify(dryRun, limit);
      setPlan(payload);
      setLastRunAt(new Date().toISOString());
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : "Failed to run release-album repair.");
    } finally {
      setLoading(false);
    }
  }

  const filteredItems = useMemo(() => {
    const items = plan?.items ?? [];
    if (filter === "safe") {
      return items.filter((item) => item.safe);
    }
    if (filter === "blocked") {
      return items.filter((item) => !item.safe);
    }
    return items;
  }, [filter, plan]);

  const blockedCount = (plan?.candidate_count ?? 0) - (plan?.safe_candidate_count ?? 0);
  const canApply = Boolean(plan && plan.mode === "dry_run" && plan.safe_candidate_count > 0 && !loading);
  const lastRunLabel = formatLoadedAt(lastRunAt);

  return (
    <div className="identity-audit-grid">
      <div className="identity-audit-group">
        <div className="section-column-header">
          <div>
            <h3>History / Spotify Album Repair</h3>
            <p className="identity-audit-tab-copy">
              Finds duplicate release albums where history-only rows can safely move into a Spotify-backed album. Dry run is read-only; apply only writes safe candidates.
            </p>
          </div>
          <div className="section-column-header-actions album-repair-actions">
            <label className="album-repair-limit">
              <span>Limit</span>
              <input
                min={1}
                max={200}
                onChange={(event) => setLimit(Math.max(1, Math.min(200, Number(event.target.value) || 50)))}
                type="number"
                value={limit}
              />
            </label>
            <button className="secondary-button" disabled={loading} onClick={() => void runRepair(true)} type="button">
              {loading ? "Running..." : "Dry Run"}
            </button>
            <button className="secondary-button" disabled={!canApply} onClick={() => void runRepair(false)} type="button">
              Apply Safe
            </button>
          </div>
        </div>

        <div className="identity-audit-ambiguous-summary">
          <span className="identity-audit-pill">Candidates: {plan?.candidate_count ?? 0}</span>
          <span className="identity-audit-pill">Safe: {plan?.safe_candidate_count ?? 0}</span>
          <span className="identity-audit-pill">Blocked: {blockedCount}</span>
          <span className="identity-audit-pill">Applied: {plan?.applied_count ?? 0}</span>
          <span className="identity-audit-pill">Mode: {plan?.mode ?? "not run"}</span>
          {lastRunLabel ? <span className="identity-audit-pill">Checked {lastRunLabel}</span> : null}
        </div>

        <div className="track-ranking-toggle identity-audit-tabs" role="group" aria-label="Release album repair filter">
          {([
            ["all", "All"],
            ["safe", "Safe"],
            ["blocked", "Blocked"],
          ] as Array<[RepairFilter, string]>).map(([value, label]) => (
            <button
              className={`track-ranking-chip${filter === value ? " track-ranking-chip-active" : ""}`}
              key={`album-repair-filter-${value}`}
              onClick={() => setFilter(value)}
              type="button"
            >
              {label}
            </button>
          ))}
        </div>

        {error ? <p className="empty-copy">{error}</p> : null}
        {!plan && !error ? <p className="empty-copy">Run a dry run to review safe and blocked candidates before applying anything.</p> : null}
        {plan && filteredItems.length === 0 ? <p className="empty-copy">No candidates match this filter.</p> : null}

        <div className="album-repair-card-grid">
          {filteredItems.map((candidate) => (
            <CandidateCard candidate={candidate} key={`album-repair-${candidate.release_album_ids.join("-")}`} />
          ))}
        </div>
      </div>
    </div>
  );
}
