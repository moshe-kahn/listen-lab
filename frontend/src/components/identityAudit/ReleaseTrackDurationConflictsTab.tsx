import { useEffect, useMemo, useState } from "react";

import { fetchReleaseTrackDurationConflicts } from "../../api/appApi";
import type { ReleaseTrackDurationConflictItem } from "../../types/appTypes";
import { formatDurationMs } from "../../utils/dashboardUtils";

function formatDelta(ms: number) {
  return formatDurationMs(ms) ?? `${ms}ms`;
}

function sourceDurationClass(item: ReleaseTrackDurationConflictItem, durationMs: number) {
  if (durationMs === item.min_duration_ms) {
    return "recording-track-good";
  }
  if (durationMs === item.max_duration_ms) {
    return "recording-track-risk";
  }
  return "recording-track-warn";
}

export function ReleaseTrackDurationConflictsTab() {
  const [items, setItems] = useState<ReleaseTrackDurationConflictItem[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastLoadedAt, setLastLoadedAt] = useState<string | null>(null);

  const sortedItems = useMemo(
    () => [...items].sort((left, right) => right.duration_delta_ms - left.duration_delta_ms || left.release_track_id - right.release_track_id),
    [items],
  );

  async function loadConflicts() {
    setLoading(true);
    setError(null);
    try {
      const payload = await fetchReleaseTrackDurationConflicts(100, 0, 2000);
      setItems(payload.items);
      setTotal(payload.total);
      setLastLoadedAt(new Date().toISOString());
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : "Failed to load duration conflicts.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadConflicts();
  }, []);

  return (
    <div className="identity-audit-grid">
      <div className="identity-audit-group">
        <div className="section-column-header">
          <div>
            <h3>Duration Conflicts</h3>
            <p className="identity-audit-tab-copy">
              Release tracks skipped by duration repair because accepted Spotify source durations differ by more than 2 seconds.
            </p>
          </div>
          <button className="secondary-button" disabled={loading} onClick={() => void loadConflicts()} type="button">
            {loading ? "Loading..." : "Reload"}
          </button>
        </div>
        <div className="identity-audit-ambiguous-summary">
          <span className="identity-audit-pill">Conflicts: {total}</span>
          <span className="identity-audit-pill">Shown: {sortedItems.length}</span>
          <span className="identity-audit-pill">Read-only</span>
          {lastLoadedAt ? <span className="identity-audit-pill">Loaded {new Date(lastLoadedAt).toLocaleTimeString()}</span> : null}
        </div>
        {error ? <p className="empty-copy">{error}</p> : null}
        {!loading && !error && sortedItems.length === 0 ? <p className="empty-copy">No duration conflicts found.</p> : null}
        <div className="identity-audit-examples">
          {sortedItems.map((item) => (
            <article className="recording-track-candidate-card" key={`duration-conflict-${item.release_track_id}`}>
              <div className="identity-audit-example-header">
                <div>
                  <h4>{item.release_track_name}</h4>
                  <p>
                    release {item.release_track_id} · {item.source_track_count} source tracks · delta {formatDelta(item.duration_delta_ms)}
                  </p>
                </div>
                <span className="identity-audit-type-badge recording-track-risk">
                  {formatDelta(item.min_duration_ms)} - {formatDelta(item.max_duration_ms)}
                </span>
              </div>
              <div className="identity-audit-variant-list">
                {item.source_tracks.map((source) => (
                  <div className="identity-audit-variant duration-conflict-source" key={`${item.release_track_id}-${source.source_track_db_id}`}>
                    <div className="identity-audit-variant-main">
                      {source.spotify_url ? (
                        <a href={source.spotify_url} rel="noreferrer" target="_blank">
                          {source.spotify_track_name || source.spotify_track_id}
                        </a>
                      ) : (
                        <strong>{source.spotify_track_name || source.spotify_track_id}</strong>
                      )}
                      <span>{source.album_name || "Album unknown"}</span>
                      <code>{source.spotify_track_id}</code>
                    </div>
                    <div className="identity-audit-variant-stats">
                      <span className={sourceDurationClass(item, source.duration_ms)}>{formatDelta(source.duration_ms)}</span>
                      <span>{source.explicit == null ? "explicit unknown" : source.explicit ? "explicit" : "not explicit"}</span>
                      {source.album_release_date ? <span>{source.album_release_date}</span> : null}
                      {source.album_type ? <span>{source.album_type}</span> : null}
                      {source.isrc ? <span>ISRC {source.isrc}</span> : null}
                      <span>{source.match_method}</span>
                    </div>
                  </div>
                ))}
              </div>
            </article>
          ))}
        </div>
      </div>
    </div>
  );
}
