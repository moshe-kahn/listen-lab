import { useState, type Dispatch, type SetStateAction } from "react";
import type { PreviewItem, RecentDebugSourceFilter, RecentPlayFilter, RecentTrack } from "../../types/appTypes";
import {
  DEBUG_GAP_MARKER_MAX_MS,
  DEBUG_GAP_MARKER_MIN_MS,
  RECENT_DEBUG_SOURCE_FILTER_OPTIONS,
} from "../../constants/appConstants";
import {
  buildDebugSessions,
  debugTrackKey,
  formatDebugLabel,
  formatDebugTimestamp,
  formatDebugValue,
  formatDurationMs,
  formatSessionRange,
  formatTimeOnly,
  isComputedField,
  isLinkOrUriField,
  trackEstimatedMs,
  trackPlayedAtMs,
} from "../../utils/dashboardUtils";
import { recentTrackCompletionRatio, trackUriWithFallback } from "../../utils/playbackUtils";

type RecentDebugPageProps = {
  hasProfile: boolean;
  listeningLogTracks: RecentTrack[];
  listeningLogLoading: boolean;
  listeningLogError: string;
  listeningLogOffset: number;
  listeningLogHasMore: boolean;
  listeningLogLastLoadedAt: number | null;
  recentDebugSourceFilter: RecentDebugSourceFilter;
  setRecentDebugSourceFilter: Dispatch<SetStateAction<RecentDebugSourceFilter>>;
  setListeningLogTracks: Dispatch<SetStateAction<RecentTrack[]>>;
  setListeningLogHasMore: Dispatch<SetStateAction<boolean>>;
  setListeningLogOffset: Dispatch<SetStateAction<number>>;
  setListeningLogLoaded: Dispatch<SetStateAction<boolean>>;
  setListeningLogLastLoadedAt: Dispatch<SetStateAction<number | null>>;
  setListeningLogError: Dispatch<SetStateAction<string>>;
  showDebugLinkFields: boolean;
  setShowDebugLinkFields: Dispatch<SetStateAction<boolean>>;
  openDebugSessions: Record<string, boolean>;
  setOpenDebugSessions: Dispatch<SetStateAction<Record<string, boolean>>>;
  openDebugTracks: Record<string, boolean>;
  setOpenDebugTracks: Dispatch<SetStateAction<Record<string, boolean>>>;
  loadListeningLogBatch: (reset?: boolean, forceRecentSync?: boolean) => void | Promise<void>;
  onBack: () => void;
  onSelectPreview: (preview: PreviewItem) => void;
};

export function RecentDebugPage({
  hasProfile,
  listeningLogTracks,
  listeningLogLoading,
  listeningLogError,
  listeningLogOffset,
  listeningLogHasMore,
  listeningLogLastLoadedAt,
  recentDebugSourceFilter,
  setRecentDebugSourceFilter,
  setListeningLogTracks,
  setListeningLogHasMore,
  setListeningLogOffset,
  setListeningLogLoaded,
  setListeningLogLastLoadedAt,
  setListeningLogError,
  showDebugLinkFields,
  setShowDebugLinkFields,
  openDebugSessions,
  setOpenDebugSessions,
  openDebugTracks,
  setOpenDebugTracks,
  loadListeningLogBatch,
  onBack,
  onSelectPreview,
}: RecentDebugPageProps) {
  const [listenLogPlayFilter, setListenLogPlayFilter] = useState<RecentPlayFilter>("listened");

  if (!hasProfile) {
    return null;
  }

  const allSortedTracks = [...listeningLogTracks].sort((a, b) => {
    const aMs = trackPlayedAtMs(a) ?? -1;
    const bMs = trackPlayedAtMs(b) ?? -1;
    return bMs - aMs;
  });
  const visibleTracks = allSortedTracks.filter((track) => {
    if (listenLogPlayFilter === "all") {
      return true;
    }
    const ratio = recentTrackCompletionRatio(track);
    if (ratio === null) {
      return false;
    }
    return listenLogPlayFilter === "listened"
      ? ratio >= 0.65
      : ratio < 0.65;
  });
  const sessions = buildDebugSessions(visibleTracks);
  const canTryLoadMore = listeningLogOffset === 0 || listeningLogHasMore;
  const buildSpotifyUrl = (kind: "track" | "artist" | "album", id: string | null): string =>
    id ? `https://open.spotify.com/${kind}/${id}` : "";
  const firstArtist = (track: RecentTrack) => track.artists?.find((artist) => Boolean(artist?.name || artist?.id || artist?.artist_id)) ?? null;
  const openDebugPreview = (track: RecentTrack, kind: PreviewItem["kind"]) => {
    const artist = firstArtist(track);
    const artistLabel = artist?.name ?? track.artist_name ?? "Unknown artist";
    const albumLabel = track.album_name ?? "Unknown album";
    const releaseYear = track.album_release_year ?? null;

    if (kind === "track") {
      const fallbackTrackUrl = buildSpotifyUrl("track", track.track_id ?? null);
      onSelectPreview({
        image: track.image_url ?? null,
        fallbackLabel: "T",
        label: track.track_name ?? "Unknown track",
        meta: track.artist_name ?? null,
        detail: track.album_name ?? null,
        kind: "track",
        entityId: track.track_id ?? null,
        trackUri: trackUriWithFallback(track.uri, track.track_id),
        url: track.url ?? fallbackTrackUrl,
        trackId: track.track_id ?? null,
        albumId: track.album_id ?? null,
        artistName: track.artist_name ?? null,
        sourceTrack: track,
      });
      return;
    }

    if (kind === "artist") {
      const artistId = artist?.artist_id ?? artist?.id ?? null;
      const fallbackArtistUrl = buildSpotifyUrl("artist", artistId);
      onSelectPreview({
        image: track.image_url ?? null,
        fallbackLabel: "A",
        label: artistLabel,
        meta: null,
        detail: null,
        kind: "artist",
        entityId: artistId,
        trackUri: null,
        url: artist?.url ?? fallbackArtistUrl,
        trackId: null,
        albumId: null,
        artistName: artistLabel,
        sourceTrack: track,
      });
      return;
    }

    const fallbackAlbumUrl = buildSpotifyUrl("album", track.album_id ?? null);
    onSelectPreview({
      image: track.image_url ?? null,
      fallbackLabel: "L",
      label: albumLabel,
      meta: track.artist_name ?? null,
      detail: releaseYear,
      kind: "album",
      entityId: track.album_id ?? null,
      trackUri: null,
      url: track.album_url ?? fallbackAlbumUrl,
      trackId: null,
      albumId: track.album_id ?? null,
      artistName: track.artist_name ?? null,
      sourceTrack: track,
    });
  };

  const debugFieldOrder = [
    "event_id",
    "source_label",
    "raw_spotify_recent_id",
    "raw_spotify_history_id",
    "timing_source",
    "matched_state",
    "track_id",
    "track_name",
    "artist_name",
    "album_name",
    "album_release_year",
    "album_id",
    "artists",
    "spotify_played_at",
    "duration_ms",
    "estimated_played_ms",
    "estimated_completion_ratio",
    "spotify_context_type",
    "spotify_context_uri",
    "spotify_context_url",
    "spotify_context_href",
  ];

  const renderRecentDebugSourceFilterToggle = () => (
    <div className="track-ranking-toggle" role="group" aria-label="Recent debug source filter">
      {RECENT_DEBUG_SOURCE_FILTER_OPTIONS.map((option) => (
        <button
          className={`track-ranking-chip${recentDebugSourceFilter === option.value ? " track-ranking-chip-active" : ""}`}
          key={option.value}
          onClick={() => {
            if (recentDebugSourceFilter === option.value) {
              return;
            }
            setRecentDebugSourceFilter(option.value);
            setListeningLogTracks([]);
            setListeningLogHasMore(false);
            setListeningLogOffset(0);
            setListeningLogLoaded(false);
            setListeningLogLastLoadedAt(null);
            setListeningLogError("");
            setOpenDebugSessions({});
            setOpenDebugTracks({});
          }}
          type="button"
        >
          {option.label}
        </button>
      ))}
    </div>
  );

  const renderListenLogPlayFilterToggle = () => (
    <div className="track-ranking-toggle" role="group" aria-label="Listen Log play amount filter">
      {[
        ["listened", "Listened"],
        ["all", "All"],
        ["skipped", "Skipped"],
      ].map(([value, label]) => (
        <button
          className={`track-ranking-chip${listenLogPlayFilter === value ? " track-ranking-chip-active" : ""}`}
          key={value}
          onClick={() => {
            setListenLogPlayFilter(value as RecentPlayFilter);
            setOpenDebugSessions({});
            setOpenDebugTracks({});
          }}
          type="button"
        >
          {label}
        </button>
      ))}
    </div>
  );

  return (
    <section className="info-card info-card-wide tracks-only-card" id="recent-debug-page">
      <div className="tracks-only-header">
        <div className="section-column-header">
          <div>
            <h2>Listen Log</h2>
            <p className="tracks-only-subtitle">
              Canonical play events from the merged fact layer.
            </p>
          </div>
          <div className="recent-debug-controls">
            {renderListenLogPlayFilterToggle()}
            {renderRecentDebugSourceFilterToggle()}
            <button
              className="secondary-button"
              disabled={listeningLogLoading}
              onClick={() => void loadListeningLogBatch(true, true)}
              type="button"
            >
              {listeningLogLoading ? "Loading..." : "Reload"}
            </button>
            <label className="recent-debug-filter">
              <input
                checked={showDebugLinkFields}
                onChange={(event) => setShowDebugLinkFields(event.currentTarget.checked)}
                type="checkbox"
              />
              Show raw data
            </label>
          </div>
        </div>
        <button
          className="secondary-button tracks-only-back-button"
          onClick={onBack}
          type="button"
        >
          Back to activity
        </button>
      </div>
      <div className="tracks-only-diagnostics">
        <span>{visibleTracks.length} visible play events</span>
        {listeningLogLastLoadedAt ? (
          <span>Loaded {new Date(listeningLogLastLoadedAt).toLocaleTimeString()}</span>
        ) : null}
      </div>
      {listeningLogError ? (
        <p className="empty-copy">
          {listeningLogError}
          {" "}
          Refresh this page after confirming the frontend is using the same backend where `/auth/session` is authenticated.
        </p>
      ) : null}
      {visibleTracks.length === 0 && listeningLogLoading ? (
        <p className="empty-copy">Loading listening log...</p>
      ) : null}
      {visibleTracks.length === 0 && !listeningLogLoading && !listeningLogError ? (
        <p className="empty-copy">No play events are currently available.</p>
      ) : (
        <div className="recent-debug-list">
          {sessions.map((session, sessionIndex) => {
            const isSessionOpen = showDebugLinkFields || (openDebugSessions[session.id] ?? sessionIndex === 0);
            return (
              <section className="recent-debug-session" key={session.id}>
                <button
                  className="recent-debug-session-toggle"
                  onClick={() =>
                    setOpenDebugSessions((current) => ({
                      ...current,
                      [session.id]: !isSessionOpen,
                    }))
                  }
                  type="button"
                >
                  <span className="recent-debug-session-title">
                    Session {sessionIndex + 1}: {formatSessionRange(session)} ({session.tracks.length} {session.tracks.length === 1 ? "play" : "plays"})
                  </span>
                  <span>{isSessionOpen ? "^" : "v"}</span>
                </button>
                {isSessionOpen ? (
                  <div className="recent-debug-session-list">
                    {session.tracks.map((track, index) => {
                      const trackKey = debugTrackKey(session.id, track, index);
                      const isTrackOpen = showDebugLinkFields || Boolean(openDebugTracks[trackKey]);
                      const albumSummary = track.album_name ?? "Unknown album";
                      const albumWithYear = track.album_release_year
                        ? `${track.album_release_year} - ${albumSummary}`
                        : albumSummary;
                      const playedAtSummary = formatDebugTimestamp(track.spotify_played_at ?? null);
                      const durationSummary = formatDurationMs(track.duration_ms ?? null);
                      const estimatedSummary = formatDurationMs(track.estimated_played_ms ?? null);
                      const endMs = trackPlayedAtMs(track);
                      const estimatedMs = trackEstimatedMs(track);
                      const startMs = endMs != null && estimatedMs != null ? Math.max(0, endMs - estimatedMs) : null;
                      const playedGapMsValue = typeof track.played_at_gap_ms === "number"
                        ? Math.max(0, Math.round(track.played_at_gap_ms))
                        : null;
                      const timeRangeSummary =
                        startMs != null && endMs != null
                          ? `${new Date(startMs).toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" })} - ${new Date(endMs).toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" })}`
                          : formatTimeOnly(track.spotify_played_at ?? null);
                      const completionRatio =
                        typeof track.estimated_completion_ratio === "number"
                          ? Math.max(0, Math.min(1, track.estimated_completion_ratio))
                          : typeof track.duration_ms === "number" && track.duration_ms > 0 && typeof track.estimated_played_ms === "number"
                            ? Math.max(0, Math.min(1, track.estimated_played_ms / track.duration_ms))
                            : 0;
                      const nextOlderTrack = index + 1 < session.tracks.length ? session.tracks[index + 1] : null;
                      const nextOlderEndMs = nextOlderTrack ? trackPlayedAtMs(nextOlderTrack) : null;
                      const interTrackGapMs =
                        startMs != null && nextOlderEndMs != null
                          ? Math.max(0, startMs - nextOlderEndMs)
                          : null;
                      const showGapMarker = Boolean(
                        interTrackGapMs != null
                        && interTrackGapMs >= DEBUG_GAP_MARKER_MIN_MS
                        && interTrackGapMs <= DEBUG_GAP_MARKER_MAX_MS,
                      );
                      const rowEntries = Object.entries(track)
                        .filter(([key, value]) => {
                          if (!showDebugLinkFields && isLinkOrUriField(key, value)) {
                            return false;
                          }
                          if (key === "duration_seconds" || key === "estimated_played_seconds") {
                            return false;
                          }
                          if (key === "duration_ms" || key === "estimated_played_ms") {
                            return false;
                          }
                          return true;
                        })
                        .sort(([keyA], [keyB]) => {
                          const indexA = debugFieldOrder.indexOf(keyA);
                          const indexB = debugFieldOrder.indexOf(keyB);
                          const rankA = indexA === -1 ? 10_000 : indexA;
                          const rankB = indexB === -1 ? 10_000 : indexB;
                          if (rankA !== rankB) {
                            return rankA - rankB;
                          }
                          return keyA.localeCompare(keyB);
                        });

                      return (
                        <div className="recent-debug-item-wrap" key={trackKey}>
                          <article className="recent-debug-item">
                          <div className="recent-debug-item-top">
                            {track.image_url ? (
                              <img
                                alt=""
                                className="recent-debug-cover"
                                src={track.image_url}
                              />
                            ) : (
                              <span className="recent-debug-cover recent-debug-cover-fallback" aria-hidden="true">
                                {(track.track_name ?? "?").slice(0, 1).toUpperCase()}
                              </span>
                            )}
                            <div className="recent-debug-item-summary">
                              <p className="recent-debug-item-time" title={playedAtSummary}>
                                {timeRangeSummary}
                              </p>
                              <div className="recent-debug-title-row">
                                <button
                                  className="recent-debug-link recent-debug-item-title"
                                  onClick={() => openDebugPreview(track, "track")}
                                  title={track.track_name ?? "Unknown track"}
                                  type="button"
                                >
                                  {track.track_name ?? "Unknown track"}
                                </button>
                                <span className="card-inline-badge">
                                  {track.source_label === "both"
                                    ? "Both"
                                    : track.source_label === "history"
                                      ? "History"
                                      : "API"}
                                </span>
                              </div>
                              <button
                                className="empty-copy recent-debug-link recent-debug-item-meta"
                                onClick={() => openDebugPreview(track, "artist")}
                                title={track.artist_name ?? "Unknown artist"}
                                type="button"
                              >
                                {track.artist_name ?? "Unknown artist"}
                              </button>
                              <button
                                className="empty-copy recent-debug-link recent-debug-item-album"
                                onClick={() => openDebugPreview(track, "album")}
                                title={albumWithYear}
                                type="button"
                              >
                                {albumWithYear}
                              </button>
                              <div
                                className="recent-debug-completion"
                                title={`Estimated completion*: ${(completionRatio * 100).toFixed(1)}%`}
                              >
                                <div
                                  className="recent-debug-completion-fill"
                                  style={{ width: `${completionRatio * 100}%` }}
                                />
                              </div>
                              <div className="recent-debug-times">
                                <span className="recent-debug-time-chip">
                                  Gap to previous play*: {formatDurationMs(playedGapMsValue)}
                                </span>
                                <span className="recent-debug-time-chip">Length: {durationSummary}</span>
                                <span className="recent-debug-time-chip">Estimated played*: {estimatedSummary}</span>
                              </div>
                            </div>
                            {!showDebugLinkFields ? (
                              <button
                                className="secondary-button recent-debug-expand-button"
                                onClick={() =>
                                  setOpenDebugTracks((current) => ({
                                    ...current,
                                    [trackKey]: !isTrackOpen,
                                  }))
                                }
                                type="button"
                              >
                                {isTrackOpen ? "Hide data" : "Show data"}
                              </button>
                            ) : null}
                          </div>
                          {isTrackOpen ? (
                            <div className="recent-debug-grid">
                              {rowEntries.map(([key, value]) => (
                                <div className="recent-debug-row" key={`${trackKey}-${key}`}>
                                  <span className="recent-debug-key">
                                    {formatDebugLabel(key)}
                                    {isComputedField(key) ? "*" : ""}
                                  </span>
                                  <span className="recent-debug-value">{formatDebugValue(key, value)}</span>
                                </div>
                              ))}
                            </div>
                          ) : null}
                          </article>
                          {showGapMarker ? (
                            <div
                              className="recent-debug-gap"
                              title={`Gap of ${formatDurationMs(interTrackGapMs ?? null)} before this play`}
                            >
                              <span className="recent-debug-gap-line" />
                              <span className="recent-debug-gap-text">
                                {interTrackGapMs != null ? `${Math.max(0, Math.round(interTrackGapMs / 1000))}s gap` : "gap"}
                              </span>
                              <span className="recent-debug-gap-line" />
                            </div>
                          ) : null}
                        </div>
                      );
                    })}
                  </div>
                ) : null}
              </section>
            );
          })}
          <div className="recent-debug-footer">
            <button
              className="secondary-button"
              disabled={!canTryLoadMore || listeningLogLoading}
              onClick={() => void loadListeningLogBatch(false)}
              title={
                listeningLogLoading
                  ? "Loading older play events..."
                  : canTryLoadMore
                    ? "Load 50 more play events from the listening log"
                    : "No additional play events in the listening log"
              }
              type="button"
            >
              {listeningLogLoading ? "Loading..." : canTryLoadMore ? "Show 50 more" : "No more yet"}
            </button>
          </div>
        </div>
      )}
    </section>
  );
}
