import type { PlayerQueueTrack, PlayerTrackSummary } from "../../types/appTypes";

export type PlayerSavedPanelTab = "queues" | "bookmarks" | "playlists" | "likes";

export type SavedPlayerQueueGroup = {
  id: string;
  label: string;
  url?: string | null;
  imageUrl?: string | null;
  cursor?: number | null;
  tracks: PlayerQueueTrack[];
};

export type SavedPlayerQueueSnapshot = {
  id: string;
  savedAt: string;
  context?: {
    label?: string | null;
    url?: string | null;
  } | null;
  source?: "listenlab" | "spotify" | null;
  activeCursor?: number | null;
  playedKeys?: string[];
  groups: SavedPlayerQueueGroup[];
  currentTrack?: PlayerTrackSummary | null;
};

export type SavedTrackBookmark = {
  id: string;
  bookmarkedAt: string;
  track: PlayerQueueTrack;
  context?: {
    type: "playlist" | "album" | "artist" | "track" | "queue" | "player";
    label: string;
    url?: string | null;
    imageUrl?: string | null;
    entityId?: string | null;
    position?: number | null;
  } | null;
};

export type SavedEntityBookmark = {
  id: string;
  bookmarkedAt: string;
  type: "playlist" | "album" | "artist";
  label: string;
  url?: string | null;
  imageUrl?: string | null;
  entityId?: string | null;
  meta?: string | null;
  detail?: string | null;
};

type PlayerSavedPanelProps = {
  activeTab: PlayerSavedPanelTab;
  savedQueues: SavedPlayerQueueSnapshot[];
  trackBookmarks: SavedTrackBookmark[];
  entityBookmarks: SavedEntityBookmark[];
  onTabChange: (tab: PlayerSavedPanelTab) => void;
  onRestoreSavedQueue: (snapshot: SavedPlayerQueueSnapshot) => void;
  onDeleteSavedQueue: (snapshotId: string) => void;
  onPlayBookmark: (action: "play_now" | "play_next", bookmark: SavedTrackBookmark) => void;
  onOpenBookmark: (bookmark: SavedTrackBookmark) => void;
  onDeleteBookmark: (bookmarkId: string) => void;
  onOpenEntityBookmark: (bookmark: SavedEntityBookmark) => void;
  onDeleteEntityBookmark: (bookmarkId: string) => void;
};

const tabs: Array<{ value: PlayerSavedPanelTab; label: string; empty: string }> = [
  { value: "queues", label: "Queues", empty: "Saved queue history will appear here." },
  { value: "bookmarks", label: "Bookmarks", empty: "Bookmarked tracks, albums, artists, and playlists will appear here." },
  { value: "playlists", label: "Playlists", empty: "Playlist shortcuts will appear here." },
  { value: "likes", label: "Likes", empty: "Liked tracks and albums will appear here." },
];

function formatSavedAt(value: string) {
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) {
    return "Saved recently";
  }
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date(timestamp));
}

function savedQueueTitle(snapshot: SavedPlayerQueueSnapshot) {
  return snapshot.context?.label || snapshot.groups[0]?.label || "Saved queue";
}

function savedQueueImage(snapshot: SavedPlayerQueueSnapshot) {
  return snapshot.groups.find((group) => group.imageUrl)?.imageUrl
    ?? snapshot.groups.flatMap((group) => group.tracks).find((track) => track.image)?.image
    ?? null;
}

function savedQueueTrackCount(snapshot: SavedPlayerQueueSnapshot) {
  return snapshot.groups.reduce((total, group) => total + group.tracks.length, 0);
}

function bookmarkContextLabel(bookmark: SavedTrackBookmark) {
  if (!bookmark.context?.label) {
    return null;
  }
  const typeLabel = bookmark.context.type === "player"
    ? "Player"
    : bookmark.context.type.slice(0, 1).toUpperCase() + bookmark.context.type.slice(1);
  return `${typeLabel}: ${bookmark.context.label}`;
}

function tabCount(tab: PlayerSavedPanelTab, savedQueues: SavedPlayerQueueSnapshot[], trackBookmarks: SavedTrackBookmark[], entityBookmarks: SavedEntityBookmark[]) {
  if (tab === "queues") {
    return savedQueues.length;
  }
  if (tab === "bookmarks") {
    return trackBookmarks.length + entityBookmarks.length;
  }
  return 0;
}

export function PlayerSavedPanel({
  activeTab,
  savedQueues,
  trackBookmarks,
  entityBookmarks,
  onTabChange,
  onRestoreSavedQueue,
  onDeleteSavedQueue,
  onPlayBookmark,
  onOpenBookmark,
  onDeleteBookmark,
  onOpenEntityBookmark,
  onDeleteEntityBookmark,
}: PlayerSavedPanelProps) {
  const active = tabs.find((tab) => tab.value === activeTab) ?? tabs[0];

  return (
    <section className="player-saved-panel" id="saved" aria-labelledby="player-saved-heading">
      <div className="player-saved-panel-header">
        <h3 id="player-saved-heading">Saved</h3>
      </div>
      <div className="player-saved-panel-layout">
        <aside className="player-saved-panel-sidebar" aria-label="Saved navigation and filters">
          <div className="player-saved-panel-tabs" role="tablist" aria-label="Saved">
            {tabs.map((tab) => (
              <button
                aria-selected={activeTab === tab.value}
                className={activeTab === tab.value ? "player-saved-panel-tab player-saved-panel-tab-active" : "player-saved-panel-tab"}
                key={tab.value}
                onClick={() => onTabChange(tab.value)}
                role="tab"
                type="button"
              >
                <span>{tab.label}</span>
                <span className="player-saved-panel-tab-count">{tabCount(tab.value, savedQueues, trackBookmarks, entityBookmarks)}</span>
              </button>
            ))}
          </div>
          <div className="player-saved-panel-controls">
            <label>
              <span>Filter</span>
              <select value="all" onChange={() => undefined}>
                <option value="all">All</option>
              </select>
            </label>
            <label>
              <span>Sort</span>
              <select value="recent" onChange={() => undefined}>
                <option value="recent">Recently saved</option>
                <option value="name">Name</option>
              </select>
            </label>
          </div>
        </aside>
        <div className="player-saved-panel-body" role="tabpanel">
          {activeTab === "queues" ? (
            savedQueues.length > 0 ? (
              <div className="player-saved-queue-list">
                {savedQueues.map((snapshot) => {
                  const imageUrl = savedQueueImage(snapshot);
                  const trackCount = savedQueueTrackCount(snapshot);
                  const activeCursor = snapshot.activeCursor != null && snapshot.activeCursor >= 0 ? snapshot.activeCursor : null;
                  return (
                    <div className="player-saved-queue-row" key={snapshot.id}>
                      <div className="player-saved-queue-image" aria-hidden="true">
                        {imageUrl ? <img alt="" src={imageUrl} /> : <span>{savedQueueTitle(snapshot).slice(0, 1).toUpperCase()}</span>}
                      </div>
                      <div className="player-saved-queue-copy">
                        <span className="player-saved-queue-title single-line-ellipsis">{savedQueueTitle(snapshot)}</span>
                        <span className="player-saved-queue-meta single-line-ellipsis">
                          {formatSavedAt(snapshot.savedAt)} · {snapshot.groups.length} context{snapshot.groups.length === 1 ? "" : "s"} · {trackCount} track{trackCount === 1 ? "" : "s"}
                          {activeCursor != null && trackCount > 0 ? ` · position ${Math.min(activeCursor + 1, trackCount)}/${trackCount}` : ""}
                        </span>
                      </div>
                      <div className="player-saved-queue-actions">
                        <button onClick={() => onRestoreSavedQueue(snapshot)} type="button">
                          Restore
                        </button>
                        <button aria-label={`Delete saved queue ${savedQueueTitle(snapshot)}`} onClick={() => onDeleteSavedQueue(snapshot.id)} type="button">
                          Delete
                        </button>
                      </div>
                    </div>
                  );
                })}
              </div>
            ) : (
              <p>{active.empty}</p>
            )
          ) : activeTab === "bookmarks" ? (
            trackBookmarks.length > 0 || entityBookmarks.length > 0 ? (
              <div className="player-saved-queue-list">
                {entityBookmarks.map((bookmark) => (
                  <div className="player-saved-queue-row player-track-bookmark-row" key={bookmark.id}>
                    <div className="player-saved-queue-image" aria-hidden="true">
                      {bookmark.imageUrl ? <img alt="" src={bookmark.imageUrl} /> : <span>{bookmark.label.slice(0, 1).toUpperCase()}</span>}
                    </div>
                    <div className="player-saved-queue-copy">
                      <span className="player-saved-queue-title single-line-ellipsis">{bookmark.label}</span>
                      <span className="player-saved-queue-meta single-line-ellipsis">
                        {bookmark.type.slice(0, 1).toUpperCase() + bookmark.type.slice(1)}
                        {bookmark.meta ? ` · ${bookmark.meta}` : ""}
                        {bookmark.detail ? ` · ${bookmark.detail}` : ""}
                        {" · "}
                        {formatSavedAt(bookmark.bookmarkedAt)}
                      </span>
                    </div>
                    <div className="player-saved-queue-actions">
                      <button onClick={() => onOpenEntityBookmark(bookmark)} type="button">
                        Open
                      </button>
                      <button aria-label={`Remove bookmark ${bookmark.label}`} onClick={() => onDeleteEntityBookmark(bookmark.id)} type="button">
                        Remove
                      </button>
                    </div>
                  </div>
                ))}
                {trackBookmarks.map((bookmark) => (
                  <div className="player-saved-queue-row player-track-bookmark-row" key={bookmark.id}>
                    <div className="player-saved-queue-image" aria-hidden="true">
                      {bookmark.track.image ? <img alt="" src={bookmark.track.image} /> : <span>{bookmark.track.name.slice(0, 1).toUpperCase()}</span>}
                    </div>
                    <div className="player-saved-queue-copy">
                      <span className="player-saved-queue-title single-line-ellipsis">{bookmark.track.name}</span>
                      <span className="player-saved-queue-meta single-line-ellipsis">
                        {bookmark.track.artists || "Unknown artist"} · {bookmark.track.album || "Unknown album"} · {formatSavedAt(bookmark.bookmarkedAt)}
                      </span>
                      {bookmarkContextLabel(bookmark) ? (
                        <span className="player-saved-queue-meta player-track-bookmark-context single-line-ellipsis">
                          {bookmarkContextLabel(bookmark)}
                          {bookmark.context?.position != null ? ` · #${bookmark.context.position + 1}` : ""}
                        </span>
                      ) : null}
                    </div>
                    <div className="player-saved-queue-actions">
                      <button disabled={!bookmark.track.uri} onClick={() => onPlayBookmark("play_now", bookmark)} type="button">
                        Play
                      </button>
                      <button disabled={!bookmark.track.uri} onClick={() => onPlayBookmark("play_next", bookmark)} type="button">
                        Next
                      </button>
                      <button onClick={() => onOpenBookmark(bookmark)} type="button">
                        Open
                      </button>
                      <button aria-label={`Remove bookmark ${bookmark.track.name}`} onClick={() => onDeleteBookmark(bookmark.id)} type="button">
                        Remove
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <p>{active.empty}</p>
            )
          ) : (
            <p>{active.empty}</p>
          )}
        </div>
      </div>
    </section>
  );
}
