import { useEffect, useState } from "react";

import { fetchArtistPromotionSkips } from "../../api/appApi";
import type { ArtistPromotionSkipLogResponse } from "../../types/appTypes";

const reasonLabels: Record<string, string> = {
  ambiguous_text_only_artist: "Ambiguous text-only artist",
  missing_album_track_evidence: "Missing album/track evidence",
  provider_backed_name_collision: "Provider-backed name collision",
};

function readableReason(value: string) {
  return reasonLabels[value] ?? value.split("_").join(" ");
}

function shortDateTime(value: string | null | undefined) {
  if (!value) {
    return "unknown";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

export function ArtistPromotionSkipsTab() {
  const [data, setData] = useState<ArtistPromotionSkipLogResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function load() {
    setLoading(true);
    setError(null);
    try {
      setData(await fetchArtistPromotionSkips());
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : "Failed to load artist promotion skips.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void load();
  }, []);

  return (
    <div className="identity-audit-grid">
      <div className="identity-audit-group">
        <div className="section-column-header">
          <div>
            <h3>Promotion Skips</h3>
            <p className="identity-audit-tab-copy">Spotify artist matches blocked because the available evidence was not safe enough.</p>
          </div>
          <button className="secondary-button" disabled={loading} onClick={() => void load()} type="button">
            {loading ? "Loading..." : "Reload"}
          </button>
        </div>

        {error ? (
          <div className="identity-audit-notice identity-audit-notice-error">
            <strong>Promotion skips could not be loaded.</strong>
            <span>{error}</span>
            <span>Restart the backend once if it has not loaded database migration 35.</span>
          </div>
        ) : null}

        {!error ? (
          <div className="identity-audit-ambiguous-summary">
            <span className="identity-audit-pill">Skipped matches: {data?.summary.total ?? 0}</span>
            {Object.entries(data?.summary.reason_counts ?? {}).map(([reason, count]) => (
              <span className="identity-audit-pill" key={`promotion-skip-summary-${reason}`}>
                {readableReason(reason)}: {count}
              </span>
            ))}
          </div>
        ) : null}

        {!loading && !error && !data?.items.length ? (
          <div className="identity-audit-notice">
            <strong>No promotion skips recorded yet.</strong>
            <span>Only new skipped promotions after migration 35 are saved here. Existing terminal log lines are not backfilled.</span>
          </div>
        ) : null}

        {data?.items.length ? (
          <div className="identity-audit-examples">
            {data.items.map((item) => (
              <article className="recording-track-candidate-card artist-promotion-skip-card" key={`artist-promotion-skip-${item.id}`}>
                <div className="identity-audit-example-header">
                  <div className="identity-audit-card-title-button">
                    <span>
                      <strong>{item.normalized_name || "Unknown artist"}</strong>
                      <small>{readableReason(item.reason)}</small>
                    </span>
                  </div>
                  <span className="identity-audit-type-badge recording-track-muted">{item.occurrence_count}x</span>
                </div>
                <div className="artist-audit-inline-inventory">
                  <div className="artist-audit-inline-inventory-row">
                    <span className="artist-audit-inventory-label">Context</span>
                    {item.artist_id ? <span className="artist-audit-inventory-chip">artist {item.artist_id}</span> : null}
                    {item.release_album_id ? <span className="artist-audit-inventory-chip">album {item.release_album_id}</span> : null}
                    {item.release_track_id ? <span className="artist-audit-inventory-chip">track {item.release_track_id}</span> : null}
                    {item.provider_artist_ids.length ? <span className="artist-audit-inventory-chip">provider {item.provider_artist_ids.join(", ")}</span> : null}
                    {item.text_only_artist_ids.length ? <span className="artist-audit-inventory-chip">text-only {item.text_only_artist_ids.join(", ")}</span> : null}
                  </div>
                  <div className="artist-audit-inline-inventory-row">
                    <span className="artist-audit-inventory-label">Seen</span>
                    <span className="artist-audit-inventory-empty">{shortDateTime(item.first_seen_at)} to {shortDateTime(item.last_seen_at)}</span>
                  </div>
                </div>
              </article>
            ))}
          </div>
        ) : null}
      </div>
    </div>
  );
}
