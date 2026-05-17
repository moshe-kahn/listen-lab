import { Fragment, type Dispatch, type SetStateAction } from "react";
import type {
  AlbumCatalogLookupItem,
  AlbumCatalogLookupResponse,
  CatalogBackfillEnqueueResponse,
  TrackCatalogLookupItem,
  TrackCatalogLookupResponse,
} from "../../types/appTypes";
import {
  albumLookupRowCanBulkPrioritize,
  albumLookupRowIsIncompleteForEnqueue,
  albumLookupStatusLabel,
  formatDebugTimestamp,
  queueStatusLabel,
  rowIsPendingQueue,
  trackLookupRowCanBulkPrioritize,
  trackLookupRowIsIncompleteForEnqueue,
  trackLookupStatusLabel,
} from "../../utils/dashboardUtils";

type SearchLookupEntityType = "albums" | "tracks";
type AlbumCatalogLookupStatus = "all" | "backfilled" | "not_backfilled" | "tracklist_complete" | "tracklist_incomplete" | "error";
type TrackCatalogLookupStatus = "all" | "backfilled" | "not_backfilled" | "duration_missing" | "error";
type SearchLookupQueueStatus = "all" | "not_queued" | "pending" | "done" | "error";
type SearchLookupSort = "default" | "recently_backfilled" | "name" | "incomplete_first";

type SearchLookupPageProps = {
  hasProfile: boolean;
  searchLookupEntityType: SearchLookupEntityType;
  setSearchLookupEntityType: Dispatch<SetStateAction<SearchLookupEntityType>>;
  albumCatalogLookupQ: string;
  setAlbumCatalogLookupQ: Dispatch<SetStateAction<string>>;
  albumCatalogLookupStatus: AlbumCatalogLookupStatus;
  setAlbumCatalogLookupStatus: Dispatch<SetStateAction<AlbumCatalogLookupStatus>>;
  trackCatalogLookupStatus: TrackCatalogLookupStatus;
  setTrackCatalogLookupStatus: Dispatch<SetStateAction<TrackCatalogLookupStatus>>;
  searchLookupQueueStatus: SearchLookupQueueStatus;
  setSearchLookupQueueStatus: Dispatch<SetStateAction<SearchLookupQueueStatus>>;
  searchLookupSort: SearchLookupSort;
  setSearchLookupSort: Dispatch<SetStateAction<SearchLookupSort>>;
  albumCatalogLookupResult: AlbumCatalogLookupResponse | null;
  albumCatalogLookupLoading: boolean;
  albumCatalogLookupError: string;
  albumCatalogLookupLastLoadedAt: number | null;
  trackCatalogLookupResult: TrackCatalogLookupResponse | null;
  trackCatalogLookupLoading: boolean;
  trackCatalogLookupError: string;
  trackCatalogLookupLastLoadedAt: number | null;
  albumCatalogLookupEnqueueLoading: boolean;
  albumCatalogLookupEnqueueError: string;
  setAlbumCatalogLookupEnqueueError: Dispatch<SetStateAction<string>>;
  albumCatalogLookupEnqueueResult: CatalogBackfillEnqueueResponse | null;
  setAlbumCatalogLookupEnqueueResult: Dispatch<SetStateAction<CatalogBackfillEnqueueResponse | null>>;
  loadActiveSearchLookup: (reset?: boolean) => void | Promise<void>;
  enqueueVisibleIncompleteLookupAlbums: (items?: AlbumCatalogLookupItem[]) => void | Promise<void>;
  enqueueVisibleIncompleteLookupTracks: (items?: TrackCatalogLookupItem[]) => void | Promise<void>;
  openAlbumLookupPreview: (item: AlbumCatalogLookupItem) => void;
  openTrackLookupPreview: (item: TrackCatalogLookupItem) => void;
  onBack: () => void;
};

export function SearchLookupPage({
  hasProfile,
  searchLookupEntityType,
  setSearchLookupEntityType,
  albumCatalogLookupQ,
  setAlbumCatalogLookupQ,
  albumCatalogLookupStatus,
  setAlbumCatalogLookupStatus,
  trackCatalogLookupStatus,
  setTrackCatalogLookupStatus,
  searchLookupQueueStatus,
  setSearchLookupQueueStatus,
  searchLookupSort,
  setSearchLookupSort,
  albumCatalogLookupResult,
  albumCatalogLookupLoading,
  albumCatalogLookupError,
  albumCatalogLookupLastLoadedAt,
  trackCatalogLookupResult,
  trackCatalogLookupLoading,
  trackCatalogLookupError,
  trackCatalogLookupLastLoadedAt,
  albumCatalogLookupEnqueueLoading,
  albumCatalogLookupEnqueueError,
  setAlbumCatalogLookupEnqueueError,
  albumCatalogLookupEnqueueResult,
  setAlbumCatalogLookupEnqueueResult,
  loadActiveSearchLookup,
  enqueueVisibleIncompleteLookupAlbums,
  enqueueVisibleIncompleteLookupTracks,
  openAlbumLookupPreview,
  openTrackLookupPreview,
  onBack,
}: SearchLookupPageProps) {
  if (!hasProfile) {
    return null;
  }

  const isAlbumsLookup = searchLookupEntityType === "albums";
  const visibleAlbumItems = albumCatalogLookupResult?.items ?? [];
  const visibleTrackItems = trackCatalogLookupResult?.items ?? [];
  const visibleIncompleteAlbumIds = Array.from(
    new Set(
      visibleAlbumItems
        .filter((item) => albumLookupRowCanBulkPrioritize(item))
        .map((item) => item.spotify_album_id)
        .filter((spotifyAlbumId): spotifyAlbumId is string => Boolean(spotifyAlbumId)),
    ),
  );
  const visibleIncompleteTrackIds = Array.from(
    new Set(
      visibleTrackItems
        .filter((item) => trackLookupRowCanBulkPrioritize(item))
        .map((item) => item.spotify_track_id)
        .filter((spotifyTrackId): spotifyTrackId is string => Boolean(spotifyTrackId)),
    ),
  );
  const statusBadgeColors: Record<string, { background: string; color: string; border: string }> = {
    Complete: { background: "#e8f7ee", color: "#1c6b3d", border: "#bfe7cf" },
    "Missing metadata": { background: "#fff7e6", color: "#8a5b00", border: "#f1ddb0" },
    "Missing duration": { background: "#fff7e6", color: "#8a5b00", border: "#f1ddb0" },
    "Tracklist incomplete": { background: "#fff3e8", color: "#8a4a1f", border: "#efd0b9" },
    "Not queued": { background: "#f2f3f5", color: "#4f5663", border: "#d8dbe1" },
    Pending: { background: "#e7f0ff", color: "#2252a3", border: "#c1d5ff" },
    Done: { background: "#e9f7ee", color: "#1f6f40", border: "#c4e9d2" },
    Error: { background: "#fdecec", color: "#9a1f1f", border: "#f2c3c3" },
  };
  const renderLookupStatusBadge = (label: string) => {
    const colors = statusBadgeColors[label] ?? { background: "#f2f2f2", color: "#3a3a3a", border: "#d9d9d9" };
    return (
      <span
        style={{
          display: "inline-block",
          padding: "2px 8px",
          borderRadius: "999px",
          fontSize: "12px",
          fontWeight: 600,
          whiteSpace: "nowrap",
          background: colors.background,
          color: colors.color,
          border: `1px solid ${colors.border}`,
        }}
      >
        {label}
      </span>
    );
  };

  return (
    <section className="info-card info-card-wide tracks-only-card" id="search-lookup-page">
      <div className="tracks-only-header">
        <div>
          <h2>Search / Lookup</h2>
          <p className="tracks-only-subtitle">Read-only lookup tools for catalog and enrichment status.</p>
        </div>
        <div className="section-column-header-actions">
          <button className="secondary-button tracks-only-back-button" onClick={onBack} type="button">
            Back to dashboard
          </button>
        </div>
      </div>

      <div className="info-card-body">
        <h3>{searchLookupEntityType === "albums" ? "Album Catalog Lookup" : "Track Catalog Lookup"}</h3>
        <div className="identity-audit-ambiguous-toolbar">
          <div className="track-ranking-toggle" role="group" aria-label="Lookup type">
            <button
              className={`track-ranking-chip${isAlbumsLookup ? " track-ranking-chip-active" : ""}`}
              onClick={() => {
                setSearchLookupEntityType("albums");
                setAlbumCatalogLookupEnqueueError("");
                setAlbumCatalogLookupEnqueueResult(null);
              }}
              type="button"
            >
              Albums
            </button>
            <button
              className={`track-ranking-chip${searchLookupEntityType === "tracks" ? " track-ranking-chip-active" : ""}`}
              onClick={() => {
                setSearchLookupEntityType("tracks");
                setAlbumCatalogLookupEnqueueError("");
                setAlbumCatalogLookupEnqueueResult(null);
              }}
              type="button"
            >
              Tracks
            </button>
          </div>
          <label>
            Query
            <input
              onChange={(event) => setAlbumCatalogLookupQ(event.target.value)}
              placeholder={isAlbumsLookup ? "Album, artist, or Spotify album id" : "Track, artist, album, or Spotify track id"}
              type="text"
              value={albumCatalogLookupQ}
            />
          </label>
          <label>
            Catalog status
            {isAlbumsLookup ? (
              <select
                onChange={(event) => setAlbumCatalogLookupStatus(event.target.value as AlbumCatalogLookupStatus)}
                value={albumCatalogLookupStatus}
              >
                <option value="all">all</option>
                <option value="backfilled">backfilled</option>
                <option value="not_backfilled">not_backfilled</option>
                <option value="tracklist_complete">tracklist_complete</option>
                <option value="tracklist_incomplete">tracklist_incomplete</option>
                <option value="error">error</option>
              </select>
            ) : (
              <select
                onChange={(event) => setTrackCatalogLookupStatus(event.target.value as TrackCatalogLookupStatus)}
                value={trackCatalogLookupStatus}
              >
                <option value="all">all</option>
                <option value="backfilled">backfilled</option>
                <option value="not_backfilled">not_backfilled</option>
                <option value="duration_missing">duration_missing</option>
                <option value="error">error</option>
              </select>
            )}
          </label>
          <label>
            Queue status
            <select
              onChange={(event) => setSearchLookupQueueStatus(event.target.value as SearchLookupQueueStatus)}
              value={searchLookupQueueStatus}
            >
              <option value="all">All queue states</option>
              <option value="not_queued">Not queued</option>
              <option value="pending">Pending</option>
              <option value="done">Done</option>
              <option value="error">Error</option>
            </select>
          </label>
          <label>
            Sort
            <select
              onChange={(event) => setSearchLookupSort(event.target.value as SearchLookupSort)}
              value={searchLookupSort}
            >
              <option value="default">Default</option>
              <option value="recently_backfilled">Recently backfilled</option>
              <option value="name">Name</option>
              <option value="incomplete_first">Incomplete first</option>
            </select>
          </label>
          <button
            className="primary-button"
            disabled={isAlbumsLookup ? albumCatalogLookupLoading : trackCatalogLookupLoading}
            onClick={() => {
              void loadActiveSearchLookup(true);
            }}
            type="button"
          >
            {isAlbumsLookup ? (albumCatalogLookupLoading ? "Searching..." : "Search") : (trackCatalogLookupLoading ? "Searching..." : "Search")}
          </button>
          <button
            className="secondary-button"
            disabled={albumCatalogLookupEnqueueLoading || (isAlbumsLookup ? visibleIncompleteAlbumIds.length === 0 : visibleIncompleteTrackIds.length === 0)}
            onClick={() => {
              if (isAlbumsLookup) {
                void enqueueVisibleIncompleteLookupAlbums();
              } else {
                void enqueueVisibleIncompleteLookupTracks();
              }
            }}
            type="button"
          >
            {albumCatalogLookupEnqueueLoading
              ? "Prioritizing..."
              : isAlbumsLookup
                ? "Prioritize visible incomplete albums"
                : "Prioritize visible incomplete tracks"}
          </button>
        </div>
        {isAlbumsLookup && albumCatalogLookupResult ? (
          <p className="empty-copy">Visible incomplete albums with Spotify IDs: {visibleIncompleteAlbumIds.length}</p>
        ) : null}
        {searchLookupEntityType === "tracks" && trackCatalogLookupResult ? (
          <p className="empty-copy">Visible incomplete tracks with Spotify IDs: {visibleIncompleteTrackIds.length}</p>
        ) : null}
        <p className="empty-copy">Catalog status shows what data exists. Queue status shows whether backfill work is scheduled.</p>
        <p className="empty-copy">Prioritized items are added to the catalog backfill queue. Run and monitor them from Catalog Backfill.</p>
        {albumCatalogLookupError ? <p className="empty-copy">{albumCatalogLookupError}</p> : null}
        {trackCatalogLookupError ? <p className="empty-copy">{trackCatalogLookupError}</p> : null}
        {albumCatalogLookupEnqueueError ? <p className="empty-copy">{albumCatalogLookupEnqueueError}</p> : null}
        {albumCatalogLookupEnqueueResult ? (
          <p className="empty-copy">
            Added {albumCatalogLookupEnqueueResult.enqueued}, updated {albumCatalogLookupEnqueueResult.updated}, already complete {albumCatalogLookupEnqueueResult.already_complete}, invalid {albumCatalogLookupEnqueueResult.invalid}.
          </p>
        ) : null}
        {isAlbumsLookup && !albumCatalogLookupResult && albumCatalogLookupLoading ? <p className="empty-copy">Loading albums...</p> : null}
        {searchLookupEntityType === "tracks" && !trackCatalogLookupResult && trackCatalogLookupLoading ? <p className="empty-copy">Loading tracks...</p> : null}
        {isAlbumsLookup && albumCatalogLookupLastLoadedAt ? (
          <p className="empty-copy">Albums loaded {new Date(albumCatalogLookupLastLoadedAt).toLocaleTimeString()}</p>
        ) : null}
        {searchLookupEntityType === "tracks" && trackCatalogLookupLastLoadedAt ? (
          <p className="empty-copy">Tracks loaded {new Date(trackCatalogLookupLastLoadedAt).toLocaleTimeString()}</p>
        ) : null}
        {isAlbumsLookup && (!albumCatalogLookupResult || albumCatalogLookupResult.items.length === 0) ? (
          <p className="empty-copy">No matching albums.</p>
        ) : null}
        {isAlbumsLookup && albumCatalogLookupResult && albumCatalogLookupResult.items.length > 0 ? (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Album</th>
                  <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Artist</th>
                  <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Spotify Album</th>
                  <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Tracklist</th>
                  <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Status</th>
                  <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Queue</th>
                  <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Last Updated</th>
                  <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Action</th>
                </tr>
              </thead>
              <tbody>
                {albumCatalogLookupResult.items.map((item) => {
                  const tracklistText = `${item.album_track_rows} / ${item.total_tracks ?? "?"} tracks`;
                  const statusLabel = albumLookupStatusLabel(item);
                  const queueLabel = queueStatusLabel(item.queue_status);
                  const canPrioritize = albumLookupRowIsIncompleteForEnqueue(item);
                  const isPendingQueue = rowIsPendingQueue(item.queue_status);
                  const actionLabel = isPendingQueue ? "Prioritized" : (String(item.queue_status).toLowerCase() === "error" ? "Retry priority" : "Prioritize");
                  return (
                    <Fragment key={`album-lookup-fragment-${item.release_album_id}`}>
                      <tr key={`album-lookup-${item.release_album_id}`}>
                        <td style={{ padding: "8px", verticalAlign: "top", fontWeight: 600 }}>
                          {item.spotify_album_id ? (
                            <button
                              className="jump-link"
                              onClick={() => openAlbumLookupPreview(item)}
                              type="button"
                            >
                              {item.release_album_name}
                            </button>
                          ) : (
                            item.release_album_name
                          )}
                        </td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{item.artist_name}</td>
                        <td style={{ padding: "8px", verticalAlign: "top", wordBreak: "break-word" }}>{item.spotify_album_id ?? "None"}</td>
                        <td style={{ padding: "8px", verticalAlign: "top", whiteSpace: "nowrap" }}>{tracklistText}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{renderLookupStatusBadge(statusLabel)}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{renderLookupStatusBadge(queueLabel)}</td>
                        <td style={{ padding: "8px", verticalAlign: "top", whiteSpace: "nowrap" }}>{formatDebugTimestamp(item.catalog_fetched_at)}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>
                          {canPrioritize ? (
                            <button
                              className="secondary-button"
                              disabled={albumCatalogLookupEnqueueLoading || isPendingQueue}
                              onClick={() => {
                                void enqueueVisibleIncompleteLookupAlbums([item]);
                              }}
                              type="button"
                            >
                              {albumCatalogLookupEnqueueLoading ? "Prioritizing..." : actionLabel}
                            </button>
                          ) : null}
                        </td>
                      </tr>
                      {item.catalog_last_error ? (
                        <tr key={`album-lookup-error-${item.release_album_id}`}>
                          <td colSpan={8} style={{ padding: "0 8px 8px 8px", color: "rgba(0, 0, 0, 0.65)", fontSize: "12px", wordBreak: "break-word" }}>
                            Error detail: {item.catalog_last_error}
                          </td>
                        </tr>
                      ) : null}
                    </Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : null}
        {searchLookupEntityType === "tracks" && (!trackCatalogLookupResult || trackCatalogLookupResult.items.length === 0) ? (
          <p className="empty-copy">No matching tracks.</p>
        ) : null}
        {searchLookupEntityType === "tracks" && trackCatalogLookupResult && trackCatalogLookupResult.items.length > 0 ? (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Track</th>
                  <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Artist</th>
                  <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Album</th>
                  <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Spotify Track</th>
                  <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Duration</th>
                  <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Status</th>
                  <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Queue</th>
                  <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Last Updated</th>
                  <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Action</th>
                </tr>
              </thead>
              <tbody>
                {trackCatalogLookupResult.items.map((item) => {
                  const statusLabel = trackLookupStatusLabel(item);
                  const queueLabel = queueStatusLabel(item.queue_status);
                  const canPrioritize = trackLookupRowIsIncompleteForEnqueue(item);
                  const isPendingQueue = rowIsPendingQueue(item.queue_status);
                  const actionLabel = isPendingQueue ? "Prioritized" : (String(item.queue_status).toLowerCase() === "error" ? "Retry priority" : "Prioritize");
                  return (
                    <Fragment key={`track-lookup-fragment-${item.release_track_id}`}>
                      <tr key={`track-lookup-${item.release_track_id}`}>
                        <td style={{ padding: "8px", verticalAlign: "top", fontWeight: 600 }}>
                          <button
                            className="detail-modal-inline-link"
                            onClick={() => openTrackLookupPreview(item)}
                            type="button"
                          >
                            {item.release_track_name}
                          </button>
                        </td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{item.artist_name}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{item.release_album_name}</td>
                        <td style={{ padding: "8px", verticalAlign: "top", wordBreak: "break-word" }}>{item.spotify_track_id ?? "None"}</td>
                        <td style={{ padding: "8px", verticalAlign: "top", whiteSpace: "nowrap" }}>{item.duration_display ?? "Unknown"}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{renderLookupStatusBadge(statusLabel)}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{renderLookupStatusBadge(queueLabel)}</td>
                        <td style={{ padding: "8px", verticalAlign: "top", whiteSpace: "nowrap" }}>{formatDebugTimestamp(item.catalog_fetched_at)}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>
                          {canPrioritize ? (
                            <button
                              className="secondary-button"
                              disabled={albumCatalogLookupEnqueueLoading || isPendingQueue}
                              onClick={() => {
                                void enqueueVisibleIncompleteLookupTracks([item]);
                              }}
                              type="button"
                            >
                              {albumCatalogLookupEnqueueLoading ? "Prioritizing..." : actionLabel}
                            </button>
                          ) : null}
                        </td>
                      </tr>
                      {item.catalog_last_error ? (
                        <tr key={`track-lookup-error-${item.release_track_id}`}>
                          <td colSpan={9} style={{ padding: "0 8px 8px 8px", color: "rgba(0, 0, 0, 0.65)", fontSize: "12px", wordBreak: "break-word" }}>
                            Error detail: {item.catalog_last_error}
                          </td>
                        </tr>
                      ) : null}
                    </Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : null}
      </div>
    </section>
  );
}
