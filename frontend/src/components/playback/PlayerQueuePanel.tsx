import { useState, type ReactNode } from "react";
import type { PlayerQueueTrack } from "../../types/appTypes";
import { LikedBadge } from "../common/LikedBadge";
import { ReleaseSiblingBadge } from "../common/ReleaseSiblingBadge";

export type PlayerQueueContext = {
  label: string;
  url?: string | null;
  playlistId?: string | null;
  playlistName?: string | null;
};

export type PlayerQueueGroup = {
  id: string;
  label: string;
  url?: string | null;
  imageUrl?: string | null;
  tracks: PlayerQueueTrack[];
};

export type PlayerQueueBookmarkContext = {
  type: "playlist" | "album" | "artist" | "track" | "queue" | "player";
  label: string;
  url?: string | null | undefined;
  imageUrl?: string | null | undefined;
  entityId?: string | null | undefined;
  position?: number | null | undefined;
};

type PlayerQueuePanelProps = {
  activeQueueCursor: number | null;
  bookmarkContextFromQueueGroup: (group: PlayerQueueGroup | null | undefined, position?: number | null) => PlayerQueueBookmarkContext | null;
  dragIndex: number | null;
  groupMode: "custom" | "artist" | "album";
  groups: PlayerQueueGroup[];
  hasActiveQueueCursor: boolean;
  headerMenuOpen: boolean;
  liveReadOnlyMode: boolean;
  loading: boolean;
  loopEnabled: boolean;
  onClearQueue: () => void | Promise<void>;
  onClose?: () => void;
  onDragEnd: () => void;
  onDragStart: (index: number) => void;
  onGroupModeChange: (mode: "custom" | "artist" | "album") => void;
  onJumpToGroup: (groupStartIndex: number, groupId: string) => void | Promise<void>;
  onMoveTrack: (fromIndex: number, toIndex: number) => void;
  onMoveGroup: (fromGroupIndex: number, toGroupIndex: number) => void;
  onOpenTrack: (track: PlayerQueueTrack) => void;
  onOpenGroupIdsChange: (updater: (current: Set<string>) => Set<string>) => void;
  onPlayTrack: (index: number) => void | Promise<void>;
  onRemoveTrack: (index: number) => void;
  onSaveQueue: (name?: string) => void;
  onSettingsOpenChange: (open: boolean | ((current: boolean) => boolean)) => void;
  onHeaderMenuOpenChange: (open: boolean | ((current: boolean) => boolean)) => void;
  onPauseMenuOpenChange: (open: boolean | ((current: boolean) => boolean)) => void;
  onOrganizeModeChange: (open: boolean | ((current: boolean) => boolean)) => void;
  onShuffle: () => void | Promise<void>;
  onSortModeChange: (mode: "custom" | "length" | "az" | "recent") => void;
  onToggleBookmark: (track: PlayerQueueTrack, context: PlayerQueueBookmarkContext | null) => void;
  onToggleEdit: () => void;
  onCancelEdit: () => void;
  onToggleLoop: () => void | Promise<void>;
  onUnloopCurrentTrack: () => void;
  openGroupIds: Set<string>;
  organizeMode: boolean;
  pausedCursor: number | null;
  pauseAfterCurrentEnabled: boolean;
  playedKeys: Set<string>;
  queueDelayControl: ReactNode;
  queueError: string | null;
  queueSource: "listenlab" | "spotify" | null;
  queueTrackHasRelationTags: (track: PlayerQueueTrack | null | undefined) => boolean;
  queueTrackIdentity: (track: PlayerQueueTrack | null | undefined) => string | null;
  queueTrackIsKnownLiked: (track: PlayerQueueTrack) => boolean;
  releaseSiblingSourceCountForTrackId: (trackId: string | null | undefined) => number;
  settingsOpen: boolean;
  shuffleAvailable: boolean;
  shuffleEnabled: boolean;
  sortMode: "custom" | "length" | "az" | "recent";
  title?: string;
  trackIsBookmarked: (track: PlayerQueueTrack | null | undefined) => boolean;
  trackLoopEnabled: boolean;
  tracks: PlayerQueueTrack[];
  transportPaused: boolean;
  variant?: "inline" | "overlay";
};

export function playerQueueGroupMeta(
  group: PlayerQueueGroup,
  groupStartIndex: number,
  activeQueueCursor: number | null,
  hasActiveQueueCursor: boolean,
  queueSource: "listenlab" | "spotify" | null,
) {
  const groupEndIndex = groupStartIndex + group.tracks.length - 1;
  const activeInGroup = hasActiveQueueCursor && activeQueueCursor != null && activeQueueCursor >= groupStartIndex && activeQueueCursor <= groupEndIndex;
  return [
    group.url ? "Context" : queueSource === "spotify" ? "Spotify" : "ListenLab",
    `${group.tracks.length} ${group.tracks.length === 1 ? "track" : "tracks"}`,
    activeInGroup ? `track ${activeQueueCursor - groupStartIndex + 1}` : null,
  ].filter(Boolean).join(" · ");
}

function groupStartIndexFor(groups: PlayerQueueGroup[], groupIndex: number) {
  return groups.slice(0, groupIndex).reduce((total, item) => total + item.tracks.length, 0);
}

export function PlayerQueuePanel(props: PlayerQueuePanelProps) {
  const {
    activeQueueCursor,
    bookmarkContextFromQueueGroup,
    dragIndex,
    groupMode,
    groups,
    hasActiveQueueCursor,
    headerMenuOpen,
    liveReadOnlyMode,
    loading,
    loopEnabled,
    onClearQueue,
    onClose,
    onDragEnd,
    onDragStart,
    onGroupModeChange,
    onHeaderMenuOpenChange,
    onJumpToGroup,
    onMoveGroup,
    onMoveTrack,
    onOpenGroupIdsChange,
    onOpenTrack,
    onOrganizeModeChange,
    onPauseMenuOpenChange,
    onPlayTrack,
    onRemoveTrack,
    onSaveQueue,
    onSettingsOpenChange,
    onShuffle,
    onSortModeChange,
    onToggleBookmark,
    onToggleEdit,
    onCancelEdit,
    onToggleLoop,
    onUnloopCurrentTrack,
    openGroupIds,
    organizeMode,
    pausedCursor,
    playedKeys,
    queueDelayControl,
    queueError,
    queueSource,
    queueTrackHasRelationTags,
    queueTrackIdentity,
    queueTrackIsKnownLiked,
    releaseSiblingSourceCountForTrackId,
    settingsOpen,
    shuffleAvailable,
    shuffleEnabled,
    sortMode,
    title = "Queue",
    trackIsBookmarked,
    trackLoopEnabled,
    tracks,
    transportPaused,
    variant = "inline",
  } = props;
  const [dragGroupIndex, setDragGroupIndex] = useState<number | null>(null);
  const [savePopoverOpen, setSavePopoverOpen] = useState(false);
  const [saveName, setSaveName] = useState("");

  const renderTrackRow = (track: PlayerQueueTrack, index: number, bookmarkContext: PlayerQueueBookmarkContext | null) => {
    const isCurrentQueueTrack = hasActiveQueueCursor && index === activeQueueCursor;
    const isLoopedQueueTrack = trackLoopEnabled && isCurrentQueueTrack;
    const isPausedQueueTrack = pausedCursor === index && isCurrentQueueTrack && transportPaused;
    const isUpNextQueueTrack = !trackLoopEnabled && hasActiveQueueCursor && index === Number(activeQueueCursor) + 1;
    const isQueueDimmedByTrackLoop = trackLoopEnabled && !isCurrentQueueTrack;
    const isPlayedQueueTrack = queueSource === "listenlab" && playedKeys.has(queueTrackIdentity(track) ?? "");
    const isBookmarkedQueueTrack = trackIsBookmarked(track);

    return (
      <div
        className={`player-recent-row player-queue-row${organizeMode ? " player-queue-row-organizing" : ""}${dragIndex === index ? " player-queue-row-dragging" : ""}${isCurrentQueueTrack ? " player-queue-row-current" : ""}${isUpNextQueueTrack || isLoopedQueueTrack || isPausedQueueTrack ? " player-queue-row-up-next" : ""}${isQueueDimmedByTrackLoop ? " player-queue-row-muted" : ""}`}
        data-player-queue-role={isUpNextQueueTrack ? "up-next" : (isCurrentQueueTrack ? "current" : undefined)}
        draggable={organizeMode}
        key={`${track.uri ?? track.trackId ?? track.name}-${index}`}
        onDragEnd={onDragEnd}
        onDragOver={(event) => {
          if (organizeMode) {
            event.preventDefault();
          }
        }}
        onDragStart={() => onDragStart(index)}
        onDrop={(event) => {
          event.preventDefault();
          if (dragIndex != null) {
            onMoveTrack(dragIndex, index);
            onDragEnd();
          }
        }}
      >
        {organizeMode ? (
          <button aria-label={`Remove ${track.name} from queue`} className="player-queue-remove-button" onClick={() => onRemoveTrack(index)} type="button">X</button>
        ) : null}
        {organizeMode ? (
          <span className="player-queue-cover-button player-queue-cover-static" aria-hidden="true">
            {track.image ? (
              <img alt="" className="player-recent-cover" src={track.image} />
            ) : (
              <span className="player-recent-cover player-recent-cover-fallback">{track.name.slice(0, 1).toUpperCase()}</span>
            )}
          </span>
        ) : (
          <button aria-label={`Play ${track.name}`} className="player-queue-cover-button" disabled={!track.uri} onClick={() => void onPlayTrack(index)} type="button">
            {track.image ? (
              <img alt="" className="player-recent-cover" src={track.image} />
            ) : (
              <span className="player-recent-cover player-recent-cover-fallback" aria-hidden="true">{track.name.slice(0, 1).toUpperCase()}</span>
            )}
            <span className="player-queue-cover-play" aria-hidden="true">
              <svg viewBox="0 0 24 24">
                <path d="M8 5.5v13l10-6.5-10-6.5Z" />
              </svg>
            </span>
          </button>
        )}
        {organizeMode ? (
          <div className="player-recent-copy player-queue-drag-copy">
            <span className="player-recent-track single-line-ellipsis">
              {queueTrackIsKnownLiked(track) ? <LikedBadge className="player-liked-badge" /> : null}
              {queueTrackHasRelationTags(track) ? (
                <ReleaseSiblingBadge
                  className="player-release-sibling-badge"
                  sourceCount={track.releaseTrackSourceCount ?? releaseSiblingSourceCountForTrackId(track.trackId)}
                  duplicateSourceCount={track.releaseTrackDuplicateSourceCount ?? null}
                  clusterCandidateType={track.releaseTrackClusterCandidateType ?? null}
                  clusterRelationshipKind={track.releaseTrackClusterRelationshipKind ?? null}
                />
              ) : null}
              {track.name}
            </span>
            <span className="player-recent-artist single-line-ellipsis">{track.artists}</span>
          </div>
        ) : (
          <button className="player-recent-copy player-queue-copy-button" onClick={() => onOpenTrack(track)} type="button">
            <span className="player-recent-track single-line-ellipsis">{track.name}</span>
            <span className="player-recent-artist single-line-ellipsis">{track.artists}</span>
          </button>
        )}
        <span className="player-queue-row-actions">
          {!organizeMode ? (
            <button
              aria-label={isBookmarkedQueueTrack ? `Bookmarked ${track.name}` : `Bookmark ${track.name}`}
              aria-pressed={isBookmarkedQueueTrack}
              className={`player-queue-bookmark-button${isBookmarkedQueueTrack ? " player-queue-bookmark-button-active" : ""}`}
              onClick={() => onToggleBookmark(track, bookmarkContext)}
              title={isBookmarkedQueueTrack ? "Bookmarked" : "Bookmark"}
              type="button"
            >
              <svg aria-hidden="true" viewBox="0 0 20 20">
                <path d="M5 3.5h10v13l-5-3.2-5 3.2v-13Z" />
              </svg>
            </button>
          ) : null}
          {isPausedQueueTrack ? <span className="player-queue-status player-queue-status-next">Paused</span> : null}
          {isCurrentQueueTrack && !isLoopedQueueTrack && !isPausedQueueTrack ? <span className="player-queue-status">Current</span> : null}
          {isLoopedQueueTrack ? (
            <button aria-label="Unloop current song" className="player-queue-status player-queue-status-next player-queue-loop-status" onClick={onUnloopCurrentTrack} title="Unloop" type="button">
              <span className="player-queue-loop-status-default">Looped</span>
              <span className="player-queue-loop-status-hover">Unloop</span>
            </button>
          ) : null}
          {isUpNextQueueTrack ? <span className="player-queue-status player-queue-status-next">{props.pauseAfterCurrentEnabled ? "Paused" : "Up next"}</span> : null}
          {isPlayedQueueTrack && !isCurrentQueueTrack && !isUpNextQueueTrack ? <span className="player-queue-status player-queue-status-played">Played</span> : null}
        </span>
      </div>
    );
  };

  if (variant === "overlay") {
    const defaultSaveName = groups[0]?.label || title || "Saved queue";
    const openSavePopover = () => {
      setSaveName(defaultSaveName);
      setSavePopoverOpen(true);
    };
    const confirmSave = () => {
      onSaveQueue(saveName.trim() || defaultSaveName);
      setSavePopoverOpen(false);
    };

    return (
      <div className="player-queue-overlay" onMouseDown={onClose} role="presentation">
        <section
          aria-label="Queue overlay"
          aria-modal="true"
          className="player-queue-overlay-panel"
          onMouseDown={(event) => event.stopPropagation()}
          role="dialog"
        >
          <div className="player-queue-overlay-layout">
            <nav className="player-queue-overlay-nav" aria-label="Queue contexts">
              <div className="player-queue-overlay-nav-header">
                <h3>Contexts</h3>
                <div className="player-queue-overlay-save">
                  <button className="player-queue-overlay-action-button" disabled={tracks.length === 0} onClick={openSavePopover} type="button">Save</button>
                  {savePopoverOpen ? (
                    <form
                      className="player-queue-save-popover"
                      onSubmit={(event) => {
                        event.preventDefault();
                        confirmSave();
                      }}
                    >
                      <label>
                        <span>Name</span>
                        <input
                          autoFocus
                          onChange={(event) => setSaveName(event.currentTarget.value)}
                          value={saveName}
                        />
                      </label>
                      <div className="player-queue-save-actions">
                        <button type="button" onClick={() => setSavePopoverOpen(false)}>Cancel</button>
                        <button type="submit">Save</button>
                      </div>
                    </form>
                  ) : null}
                </div>
                {loading ? <span className="player-queue-overlay-loading">Loading</span> : null}
              </div>
              <div className="player-queue-overlay-context-list">
                {groups.length > 0 ? groups.map((group, groupIndex) => {
                  const groupStartIndex = groupStartIndexFor(groups, groupIndex);
                  const activeInGroup = hasActiveQueueCursor && activeQueueCursor != null && activeQueueCursor >= groupStartIndex && activeQueueCursor < groupStartIndex + group.tracks.length;
                  return (
                    <button
                      className={`player-queue-context-item${activeInGroup ? " player-queue-context-item-active" : ""}${organizeMode ? " player-queue-context-item-draggable" : ""}${dragGroupIndex === groupIndex ? " player-queue-context-item-dragging" : ""}`}
                      draggable={organizeMode}
                      key={group.id}
                      onDragEnd={() => setDragGroupIndex(null)}
                      onDragOver={(event) => {
                        if (organizeMode) {
                          event.preventDefault();
                        }
                      }}
                      onDragStart={() => setDragGroupIndex(groupIndex)}
                      onDrop={(event) => {
                        event.preventDefault();
                        if (dragGroupIndex != null) {
                          onMoveGroup(dragGroupIndex, groupIndex);
                          setDragGroupIndex(null);
                        }
                      }}
                      onClick={() => void onJumpToGroup(groupStartIndex, group.id)}
                      type="button"
                    >
                      {group.imageUrl ? (
                        <img alt="" className="player-queue-context-image" src={group.imageUrl} />
                      ) : (
                        <span className="player-queue-context-image player-queue-context-image-fallback" aria-hidden="true">{group.label.slice(0, 1).toUpperCase()}</span>
                      )}
                      <span className="player-queue-context-copy">
                        <span className="single-line-ellipsis">{group.label}</span>
                        <span className="single-line-ellipsis">{playerQueueGroupMeta(group, groupStartIndex, activeQueueCursor, hasActiveQueueCursor, queueSource)}</span>
                      </span>
                      {activeInGroup ? <span className="player-queue-current-dot" aria-label="Current queue context" /> : null}
                    </button>
                  );
                }) : (
                  <span className="player-queue-context-empty">No queue contexts</span>
                )}
              </div>
              <div className="player-queue-overlay-nav-actions" aria-label="Queue controls">
                <div className="player-queue-overlay-nav-divider" aria-hidden="true" />
                <div className="player-queue-overlay-button-row">
                  <button
                    aria-label={loopEnabled ? "Stop looping queue" : "Loop queue"}
                    aria-pressed={loopEnabled}
                    className={`player-queue-header-button${loopEnabled ? " player-queue-header-button-active player-queue-header-toggle-active" : ""}${liveReadOnlyMode ? " player-control-readonly" : ""}`}
                    disabled={tracks.length === 0}
                    onClick={() => void onToggleLoop()}
                    title={loopEnabled ? "Stop looping queue" : "Loop queue"}
                    type="button"
                  >
                    <svg aria-hidden="true" viewBox="0 0 24 24">
                      <path d="M7 7h9.2l-1.8-1.8L15.8 3.8 20 8l-4.2 4.2-1.4-1.4L16.2 9H7a3 3 0 0 0 0 6h1v2H7A5 5 0 0 1 7 7Zm10 10H7.8l1.8 1.8-1.4 1.4L4 16l4.2-4.2 1.4 1.4L7.8 15H17a3 3 0 0 0 0-6h-1V7h1a5 5 0 0 1 0 10Z" />
                    </svg>
                  </button>
                  {queueDelayControl}
                  <button
                    aria-label={shuffleEnabled ? "Restore queue order" : "Shuffle unplayed queue"}
                    aria-pressed={shuffleEnabled}
                    className={`player-queue-header-button${shuffleEnabled ? " player-queue-header-button-active player-queue-shuffle-active" : ""}${liveReadOnlyMode ? " player-control-readonly" : ""}`}
                    disabled={!shuffleAvailable && !shuffleEnabled}
                    onClick={() => void onShuffle()}
                    title={shuffleEnabled ? "Restore queue order" : "Shuffle unplayed queue"}
                    type="button"
                  >
                    <svg aria-hidden="true" viewBox="0 0 24 24">
                      <path d="M16.8 3.9 21 8.1l-4.2 4.2-1.4-1.4 1.8-1.8h-1.6c-2 0-3.4.8-4.5 2.4l-1.2 1.8c-1.4 2.1-3.4 3.2-5.9 3.2H3v-2h1c1.9 0 3.3-.8 4.3-2.4l1.2-1.8c1.5-2.1 3.5-3.2 6.1-3.2h1.6l-1.8-1.8 1.4-1.4ZM3 7.5h1c2.1 0 3.7.8 5 2.5l-1.2 1.8C6.8 10.3 5.6 9.5 4 9.5H3v-2Zm9.7 5.9c.8 1 1.8 1.6 3.1 1.6h1.4l-1.8-1.8 1.4-1.4L21 16l-4.2 4.2-1.4-1.4 1.8-1.8h-1.4c-2 0-3.6-.8-4.8-2.3l1.1-1.7.6.4Z" />
                    </svg>
                  </button>
                </div>
              </div>
            </nav>
            <aside className="player-recent-column player-queue-column player-home-queue-column player-queue-overlay-column" aria-label={queueSource === "listenlab" ? "ListenLab queue" : "Spotify queue"}>
              <div className="player-recent-header">
                <h3>Tracks</h3>
                <div className="player-queue-header-actions">
                  <button className={`player-queue-overlay-action-button${liveReadOnlyMode ? " player-control-readonly" : ""}`} disabled={tracks.length === 0} onClick={() => void onClearQueue()} type="button">Clear</button>
                  {organizeMode ? (
                    <button className="player-queue-overlay-action-button" onClick={onCancelEdit} type="button">Cancel</button>
                  ) : null}
                  <button
                    aria-label={organizeMode ? "Done editing queue" : "Edit queue"}
                    aria-pressed={organizeMode}
                    className={`player-queue-header-button${organizeMode ? " player-queue-header-button-active" : ""}`}
                    onClick={onToggleEdit}
                    title={organizeMode ? "Done editing queue" : "Edit queue"}
                    type="button"
                  >
                    {organizeMode ? (
                      <svg aria-hidden="true" viewBox="0 0 24 24">
                        <path d="M9.2 16.6 4.9 12.3 3.5 13.7l5.7 5.7L21 7.6l-1.4-1.4L9.2 16.6Z" />
                      </svg>
                    ) : (
                      <svg aria-hidden="true" viewBox="0 0 24 24">
                        <path d="m4 16.8-.8 4 4-.8L18.6 8.6l-3.2-3.2L4 16.8Zm13.2-12.8 2.8 2.8 1.3-1.3a1.9 1.9 0 0 0 0-2.7l-.1-.1a1.9 1.9 0 0 0-2.7 0L17.2 4Z" />
                      </svg>
                    )}
                  </button>
                </div>
              </div>
              {organizeMode ? (
                <div className="player-queue-organize-bar">
                  <label>
                    <span>Sort</span>
                    <select value={sortMode} onChange={(event) => onSortModeChange(event.currentTarget.value as typeof sortMode)}>
                      <option value="custom">Custom</option>
                      <option value="length">Length</option>
                      <option value="az">A-Z</option>
                      <option value="recent">Recently played</option>
                    </select>
                  </label>
                  <label>
                    <span>Group by</span>
                    <select value={groupMode} onChange={(event) => onGroupModeChange(event.currentTarget.value as typeof groupMode)}>
                      <option value="custom">Custom</option>
                      <option value="artist">Artist</option>
                      <option value="album">Album</option>
                    </select>
                  </label>
                </div>
              ) : null}
              <div className="player-recent-list">
                {groups.map((group, groupIndex) => {
                  const groupStartIndex = groupStartIndexFor(groups, groupIndex);
                  const groupEndIndex = groupStartIndex + group.tracks.length - 1;
                  const groupIsOpen = openGroupIds.has(group.id);
                  const activeInGroup = hasActiveQueueCursor && activeQueueCursor != null && activeQueueCursor >= groupStartIndex && activeQueueCursor <= groupEndIndex;
                  return (
                    <div className="player-queue-group-wrap" key={group.id}>
                      <div className="player-queue-group">
                        <button
                          aria-expanded={groupIsOpen}
                          className="player-queue-group-header"
                          onClick={() => onOpenGroupIdsChange((current) => {
                            const next = new Set(current);
                            if (next.has(group.id)) {
                              next.delete(group.id);
                            } else {
                              next.add(group.id);
                            }
                            return next;
                          })}
                          type="button"
                        >
                          <span className="player-queue-group-toggle" aria-hidden="true">{groupIsOpen ? "⌄" : "›"}</span>
                          <span className="player-queue-group-copy">
                            <span className="single-line-ellipsis">{group.label}</span>
                            <span className="player-queue-group-meta single-line-ellipsis">{playerQueueGroupMeta(group, groupStartIndex, activeQueueCursor, hasActiveQueueCursor, queueSource)}</span>
                          </span>
                          {activeInGroup ? <span className="player-queue-current-dot" aria-label="Current queue context" /> : null}
                        </button>
                      </div>
                      {groupIsOpen ? group.tracks.map((track, groupTrackIndex) => (
                        renderTrackRow(track, groupStartIndex + groupTrackIndex, bookmarkContextFromQueueGroup(group, groupTrackIndex))
                      )) : null}
                    </div>
                  );
                })}
                {!loading && tracks.length === 0 ? <p className="empty-copy player-recent-empty">No queued songs were returned.</p> : null}
                {queueError ? <p className="empty-copy player-recent-empty">{queueError}</p> : null}
              </div>
            </aside>
          </div>
        </section>
      </div>
    );
  }

  const panel = (
    <aside className="player-recent-column player-queue-column player-home-queue-column" aria-label={queueSource === "listenlab" ? "ListenLab queue" : "Spotify queue"}>
      <div className="player-recent-header">
        <div className="player-queue-heading-menu">
          <button
            aria-expanded={headerMenuOpen}
            className="player-queue-heading-button"
            onClick={() => {
              onHeaderMenuOpenChange((current) => !current);
              onSettingsOpenChange(false);
              onPauseMenuOpenChange(false);
            }}
            type="button"
          >
            {title}
          </button>
          {headerMenuOpen ? (
            <div className="player-queue-settings-menu player-queue-context-menu">
              <div className="player-queue-context-actions" aria-label="Queue controls">
                <div className="player-queue-settings">
                  <button
                    aria-expanded={settingsOpen}
                    aria-label="Queue settings"
                    className={`player-queue-header-button${liveReadOnlyMode ? " player-control-readonly" : ""}`}
                    onClick={() => {
                      onSettingsOpenChange((current) => !current);
                      onPauseMenuOpenChange(false);
                    }}
                    title="Queue settings"
                    type="button"
                  >
                    <svg aria-hidden="true" viewBox="0 0 24 24">
                      <path d="M19.4 13.5c.1-.5.1-1 .1-1.5s0-1-.1-1.5l2-1.5-2-3.5-2.4 1a7.8 7.8 0 0 0-2.6-1.5L14 2h-4l-.4 3a7.8 7.8 0 0 0-2.6 1.5l-2.4-1-2 3.5 2 1.5c-.1.5-.1 1-.1 1.5s0 1 .1 1.5l-2 1.5 2 3.5 2.4-1a7.8 7.8 0 0 0 2.6 1.5l.4 3h4l.4-3a7.8 7.8 0 0 0 2.6-1.5l2.4 1 2-3.5-2-1.5ZM12 15.5a3.5 3.5 0 1 1 0-7 3.5 3.5 0 0 1 0 7Z" />
                    </svg>
                  </button>
                  {settingsOpen ? (
                    <div className="player-queue-settings-menu player-queue-context-settings-menu">
                      <button onClick={() => {
                        onOrganizeModeChange((current) => !current);
                        onSettingsOpenChange(false);
                      }} type="button">
                        {organizeMode ? "Done organizing" : "Organize"}
                      </button>
                      <button disabled={tracks.length === 0} onClick={() => onSaveQueue()} type="button">
                        Save current queue
                      </button>
                      <button className={liveReadOnlyMode ? "player-control-readonly" : undefined} disabled={tracks.length === 0} onClick={() => void onClearQueue()} type="button">
                        Clear queue
                      </button>
                    </div>
                  ) : null}
                </div>
              </div>
              {groups.length > 0 ? groups.map((group, groupIndex) => {
                const groupStartIndex = groupStartIndexFor(groups, groupIndex);
                const activeInGroup = hasActiveQueueCursor && activeQueueCursor != null && activeQueueCursor >= groupStartIndex && activeQueueCursor < groupStartIndex + group.tracks.length;
                return (
                  <button
                    className={`player-queue-context-item${activeInGroup ? " player-queue-context-item-active" : ""}`}
                    key={group.id}
                    onClick={() => void onJumpToGroup(groupStartIndex, group.id)}
                    type="button"
                  >
                    {group.imageUrl ? (
                      <img alt="" className="player-queue-context-image" src={group.imageUrl} />
                    ) : (
                      <span className="player-queue-context-image player-queue-context-image-fallback" aria-hidden="true">{group.label.slice(0, 1).toUpperCase()}</span>
                    )}
                    <span className="player-queue-context-copy">
                      <span className="single-line-ellipsis">{group.label}</span>
                      <span className="single-line-ellipsis">{playerQueueGroupMeta(group, groupStartIndex, activeQueueCursor, hasActiveQueueCursor, queueSource)}</span>
                    </span>
                    {activeInGroup ? <span className="player-queue-current-dot" aria-label="Current queue context" /> : null}
                  </button>
                );
              }) : (
                <span className="player-queue-context-empty">No queue contexts</span>
              )}
            </div>
          ) : null}
        </div>
        <div className="player-queue-header-actions">
          {loading ? <span>Loading</span> : null}
          <button
            aria-label={loopEnabled ? "Stop looping queue" : "Loop queue"}
            aria-pressed={loopEnabled}
            className={`player-queue-header-button${loopEnabled ? " player-queue-header-button-active player-queue-header-toggle-active" : ""}${liveReadOnlyMode ? " player-control-readonly" : ""}`}
            disabled={tracks.length === 0}
            onClick={() => void onToggleLoop()}
            title={loopEnabled ? "Stop looping queue" : "Loop queue"}
            type="button"
          >
            <svg aria-hidden="true" viewBox="0 0 24 24">
              <path d="M7 7h9.2l-1.8-1.8L15.8 3.8 20 8l-4.2 4.2-1.4-1.4L16.2 9H7a3 3 0 0 0 0 6h1v2H7A5 5 0 0 1 7 7Zm10 10H7.8l1.8 1.8-1.4 1.4L4 16l4.2-4.2 1.4 1.4L7.8 15H17a3 3 0 0 0 0-6h-1V7h1a5 5 0 0 1 0 10Z" />
            </svg>
          </button>
          {queueDelayControl}
          <button
            aria-label={shuffleEnabled ? "Restore queue order" : "Shuffle unplayed queue"}
            aria-pressed={shuffleEnabled}
            className={`player-queue-header-button${shuffleEnabled ? " player-queue-header-button-active player-queue-shuffle-active" : ""}${liveReadOnlyMode ? " player-control-readonly" : ""}`}
            disabled={!shuffleAvailable && !shuffleEnabled}
            onClick={() => void onShuffle()}
            title={shuffleEnabled ? "Restore queue order" : "Shuffle unplayed queue"}
            type="button"
          >
            <svg aria-hidden="true" viewBox="0 0 24 24">
              <path d="M16.8 3.9 21 8.1l-4.2 4.2-1.4-1.4 1.8-1.8h-1.6c-2 0-3.4.8-4.5 2.4l-1.2 1.8c-1.4 2.1-3.4 3.2-5.9 3.2H3v-2h1c1.9 0 3.3-.8 4.3-2.4l1.2-1.8c1.5-2.1 3.5-3.2 6.1-3.2h1.6l-1.8-1.8 1.4-1.4ZM3 7.5h1c2.1 0 3.7.8 5 2.5l-1.2 1.8C6.8 10.3 5.6 9.5 4 9.5H3v-2Zm9.7 5.9c.8 1 1.8 1.6 3.1 1.6h1.4l-1.8-1.8 1.4-1.4L21 16l-4.2 4.2-1.4-1.4 1.8-1.8h-1.4c-2 0-3.6-.8-4.8-2.3l1.1-1.7.6.4Z" />
            </svg>
          </button>
        </div>
      </div>
      {organizeMode ? (
        <div className="player-queue-organize-bar">
          <label>
            <span>Sort</span>
            <select value={sortMode} onChange={(event) => onSortModeChange(event.currentTarget.value as typeof sortMode)}>
              <option value="custom">Custom</option>
              <option value="length">Length</option>
              <option value="az">A-Z</option>
              <option value="recent">Recently played</option>
            </select>
          </label>
          <label>
            <span>Group by</span>
            <select value={groupMode} onChange={(event) => onGroupModeChange(event.currentTarget.value as typeof groupMode)}>
              <option value="custom">Custom</option>
              <option value="artist">Artist</option>
              <option value="album">Album</option>
            </select>
          </label>
        </div>
      ) : null}
      <div className="player-recent-list">
        {groups.map((group, groupIndex) => {
          const groupStartIndex = groupStartIndexFor(groups, groupIndex);
          const groupEndIndex = groupStartIndex + group.tracks.length - 1;
          const groupIsOpen = openGroupIds.has(group.id);
          const activeInGroup = hasActiveQueueCursor && activeQueueCursor != null && activeQueueCursor >= groupStartIndex && activeQueueCursor <= groupEndIndex;
          return (
            <div className="player-queue-group-wrap" key={group.id}>
              <div className="player-queue-group">
                <button
                  aria-expanded={groupIsOpen}
                  className="player-queue-group-header"
                  onClick={() => onOpenGroupIdsChange((current) => {
                    const next = new Set(current);
                    if (next.has(group.id)) {
                      next.delete(group.id);
                    } else {
                      next.add(group.id);
                    }
                    return next;
                  })}
                  type="button"
                >
                  <span className="player-queue-group-toggle" aria-hidden="true">{groupIsOpen ? "⌄" : "›"}</span>
                  <span className="player-queue-group-copy">
                    <span className="single-line-ellipsis">{group.label}</span>
                    <span className="player-queue-group-meta single-line-ellipsis">{playerQueueGroupMeta(group, groupStartIndex, activeQueueCursor, hasActiveQueueCursor, queueSource)}</span>
                  </span>
                  {activeInGroup ? <span className="player-queue-current-dot" aria-label="Current queue context" /> : null}
                </button>
              </div>
              {groupIsOpen ? group.tracks.map((track, groupTrackIndex) => (
                renderTrackRow(track, groupStartIndex + groupTrackIndex, bookmarkContextFromQueueGroup(group, groupTrackIndex))
              )) : null}
            </div>
          );
        })}
        {!loading && tracks.length === 0 ? <p className="empty-copy player-recent-empty">No queued songs were returned.</p> : null}
        {queueError ? <p className="empty-copy player-recent-empty">{queueError}</p> : null}
      </div>
    </aside>
  );

  return panel;
}
