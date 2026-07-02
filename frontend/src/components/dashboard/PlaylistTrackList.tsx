import { Fragment, useEffect, useMemo, useRef, useState } from "react";

import type { RecentTrack } from "../../types/appTypes";
import { LikedBadge } from "../common/LikedBadge";
import { NewTrackBadge } from "../common/NewTrackBadge";
import { PlaybackActionMenu, type PlaybackAction } from "../playback/PlaybackActionMenu";
import { trackRelationTags } from "../../utils/trackRelationTags";

type PlaylistTrackListProps = {
  currentTrackUri: string | null;
  entries: RecentTrack[];
  error: string | null;
  focusPlaylistPosition: number | null;
  focusSpotifyTrackId: string | null;
  formatPlaybackClock: (positionMs: number) => string;
  formatCompactRelativeAge: (value: string | null | undefined) => string | null;
  hasMore: boolean;
  hasPremiumPlayback: boolean;
  isTrackPlaying: (trackUri: string | null) => boolean;
  isTrackLiked: (track: RecentTrack, fallbackTrackId?: string | null) => boolean;
  loading: boolean;
  onPreviewTrack: (track: RecentTrack, rowTrackUri: string | null) => Promise<void>;
  rowOffset: number;
  showCollaborativeColumns: boolean;
  total: number | null;
  onPlayAll: (action: PlaybackAction) => Promise<void>;
  onPlayTrack: (track: RecentTrack, action: PlaybackAction) => Promise<void>;
  onSelectTrack: (track: RecentTrack) => void;
  playbackDurationMs: number;
  playbackPaused: boolean;
  playbackPositionMs: number;
  previewingTrackUri: string | null;
  previewPlayedTrackKeys: Set<string>;
  trackUriWithFallback: (uri: string | null | undefined, trackId: string | null | undefined) => string | null;
};

function playlistTrackArtists(track: RecentTrack) {
  return track.artists?.map((artist) => artist.name).filter(Boolean).join(", ") || track.artist_name || "Unknown artist";
}

function playlistTrackPreviewKey(track: RecentTrack, rowTrackUri: string | null) {
  return `playlist:${track.track_id ?? rowTrackUri ?? track.track_name ?? "track"}`;
}

function playlistTrackAddedBy(track: RecentTrack) {
  return track.playlist_added_by?.display_name
    || track.playlist_added_by?.user_id
    || track.playlist_added_by?.id
    || "-";
}

type PlaylistGroupMode = "none" | "artist" | "album";

function playlistTrackListenCount(track: RecentTrack) {
  return Number(track.recording_play_count ?? track.source_play_count ?? track.play_count ?? 0);
}

function playlistTrackGroupLabel(track: RecentTrack, mode: PlaylistGroupMode) {
  if (mode === "artist") {
    return playlistTrackArtists(track);
  }
  if (mode === "album") {
    return track.album_name || "Unknown album";
  }
  return "";
}

export function PlaylistTrackList({
  currentTrackUri,
  entries,
  error,
  focusPlaylistPosition,
  focusSpotifyTrackId,
  formatPlaybackClock,
  formatCompactRelativeAge,
  hasMore,
  hasPremiumPlayback,
  isTrackPlaying,
  isTrackLiked,
  loading,
  onPreviewTrack,
  rowOffset,
  showCollaborativeColumns,
  total,
  onPlayAll,
  onPlayTrack,
  onSelectTrack,
  playbackDurationMs,
  playbackPaused,
  playbackPositionMs,
  previewingTrackUri,
  previewPlayedTrackKeys,
  trackUriWithFallback,
}: PlaylistTrackListProps) {
  const [groupMode, setGroupMode] = useState<PlaylistGroupMode>("none");
  const focusedRowRef = useRef<HTMLLIElement | null>(null);
  const playableEntries = entries.filter((track) => trackUriWithFallback(track.uri, track.track_id));
  const loadedTrackCount = entries.length;
  const totalTrackCount = total != null ? total : loadedTrackCount;
  const countsArePartial = totalTrackCount > loadedTrackCount;
  const likedTrackCount = entries.filter((track) => isTrackLiked(track, track.track_id ?? track.uri ?? null)).length;
  const unlistenedTrackCount = entries.filter((track) => playlistTrackListenCount(track) <= 0).length;
  const summaryScopeLabel = countsArePartial ? "loaded" : "";
  const trackCountLabel = total != null && total > loadedTrackCount
    ? `${loadedTrackCount.toLocaleString()} of ${total.toLocaleString()} tracks`
      : loadedTrackCount > 0
      ? `${loadedTrackCount.toLocaleString()} tracks`
      : "Tracks";
  const displayGroups = useMemo(() => {
    if (groupMode === "none") {
      return [{
        key: "playlist-order",
        label: "",
        items: entries.map((track, index) => ({ track, originalIndex: index })),
      }];
    }
    const groups: Array<{
      key: string;
      label: string;
      items: Array<{ track: RecentTrack; originalIndex: number }>;
    }> = [];
    const groupByKey = new Map<string, (typeof groups)[number]>();
    entries.forEach((track, index) => {
      const label = playlistTrackGroupLabel(track, groupMode);
      const key = label.toLocaleLowerCase();
      let group = groupByKey.get(key);
      if (!group) {
        group = { key, label, items: [] };
        groupByKey.set(key, group);
        groups.push(group);
      }
      group.items.push({ track, originalIndex: index });
    });
    return groups;
  }, [entries, groupMode]);

  useEffect(() => {
    if (!focusedRowRef.current) {
      return;
    }
    focusedRowRef.current.scrollIntoView({ block: "center", behavior: "smooth" });
  }, [entries, focusPlaylistPosition, focusSpotifyTrackId]);

  return (
    <div className={`detail-modal-album-tracks detail-modal-album-tracks-full detail-modal-album-tracks-no-with${showCollaborativeColumns ? " detail-modal-playlist-tracks-collaborative" : ""}`}>
      <div className="detail-modal-playlist-summary">
        <div className="detail-modal-playlist-summary-counts" aria-label="Playlist summary">
          <span>{totalTrackCount.toLocaleString()} {totalTrackCount === 1 ? "track" : "tracks"}</span>
          <span>{unlistenedTrackCount.toLocaleString()} {summaryScopeLabel ? `${summaryScopeLabel} ` : ""}unlistened</span>
          <span>{likedTrackCount.toLocaleString()} {summaryScopeLabel ? `${summaryScopeLabel} ` : ""}liked</span>
        </div>
        <label className="detail-modal-playlist-group-control">
          <span>Group</span>
          <select
            value={groupMode}
            onChange={(event) => setGroupMode(event.target.value as PlaylistGroupMode)}
          >
            <option value="none">Playlist order</option>
            <option value="artist">Artist</option>
            <option value="album">Album</option>
          </select>
        </label>
      </div>
      <div className="detail-modal-album-header">
        <span className="detail-modal-album-number-header">#</span>
        <span className="detail-modal-album-preview-header">Preview</span>
        {hasPremiumPlayback ? (
          <PlaybackActionMenu
            ariaLabel="Playlist playback options"
            buttonClassName="detail-album-play-all-button"
            disabled={playableEntries.length === 0}
            placement="adjacent"
            onAction={onPlayAll}
          >
            Play all
          </PlaybackActionMenu>
        ) : (
          <span aria-hidden="true" />
        )}
        <span className="detail-modal-album-liked-header">Liked</span>
        <span className="detail-modal-album-title-header">
          {trackCountLabel}
        </span>
        <span className="detail-modal-album-with-header">Artist</span>
        {showCollaborativeColumns ? <span className="detail-modal-album-added-by-header">Added by</span> : null}
        <span className="detail-modal-album-liked-header">Tags</span>
        <span className="detail-modal-album-listens-header">Listens</span>
        <span className="detail-modal-album-last-played-header">Last</span>
        {showCollaborativeColumns ? <span className="detail-modal-album-added-at-header">Added</span> : null}
      </div>
      {loading && entries.length === 0 ? <p className="detail-modal-preview-missing">Loading playlist...</p> : null}
      {!loading && error ? <p className="detail-modal-preview-missing">{error}</p> : null}
      {!error && entries.length > 0 ? (
        <>
          <ul className={`detail-album-track-list${loading ? " detail-album-track-list-updating" : ""}`}>
            {displayGroups.map((group) => (
              <Fragment key={group.key}>
                {groupMode !== "none" ? (
                  <li className="detail-album-track-group-header">
                    <span>{group.label}</span>
                    <span>{group.items.length.toLocaleString()} {group.items.length === 1 ? "track" : "tracks"}</span>
                  </li>
                ) : null}
                {group.items.map(({ track, originalIndex }) => {
              const rowTrackUri = trackUriWithFallback(track.uri, track.track_id);
              const rowPlaying = isTrackPlaying(rowTrackUri);
              const rowPreviewPlaying = Boolean(rowTrackUri && previewingTrackUri === rowTrackUri);
              const rowPreviewActive = Boolean(rowPreviewPlaying && rowPlaying);
              const rowPreviewPlayed = previewPlayedTrackKeys.has(playlistTrackPreviewKey(track, rowTrackUri));
              const rowIsCurrentTrack = Boolean(rowTrackUri && currentTrackUri === rowTrackUri);
              const rowPosition = typeof track.playlist_position === "number" ? track.playlist_position : rowOffset + originalIndex;
              const rowIsFocused = (
                (typeof focusPlaylistPosition === "number" && rowPosition === focusPlaylistPosition)
                || Boolean(focusSpotifyTrackId && track.track_id === focusSpotifyTrackId && rowPosition >= rowOffset && rowPosition < rowOffset + entries.length)
              );
              const rowBaseDurationMs = track.duration_ms ?? (rowIsCurrentTrack ? playbackDurationMs : null);
              const rowButtonTimeMs = rowIsCurrentTrack
                ? rowPlaying
                  ? Math.min(Math.max(0, playbackPositionMs), rowBaseDurationMs ?? playbackPositionMs)
                  : playbackPaused
                    ? Math.max(0, playbackPositionMs)
                    : rowBaseDurationMs
                : rowBaseDurationMs;
              return (
                <li
                  className={`detail-album-track-row${rowIsCurrentTrack ? " detail-album-track-row-selected" : ""}${rowIsFocused ? " detail-album-track-row-focused" : ""}`}
                  key={`${track.track_id ?? track.uri ?? track.track_name ?? "track"}-${rowPosition}-${originalIndex}`}
                  ref={rowIsFocused ? focusedRowRef : null}
                >
                  <span className="detail-album-track-number">{rowPosition + 1}</span>
                  {hasPremiumPlayback ? (
                    <button
                      aria-label={rowPreviewPlaying ? `Stop preview for ${track.track_name ?? "track"}` : `Preview ${track.track_name ?? "track"}`}
                      className={`detail-album-track-preview-button${rowPreviewActive ? " detail-album-track-preview-button-active" : ""}${rowPreviewPlayed ? " detail-album-track-preview-button-played" : ""}`}
                      disabled={!rowTrackUri}
                      onClick={() => {
                        void onPreviewTrack(track, rowTrackUri);
                      }}
                      type="button"
                    />
                  ) : (
                    <span className="detail-album-track-preview-placeholder" aria-hidden="true" />
                  )}
                  {hasPremiumPlayback ? (
                    <PlaybackActionMenu
                      ariaLabel={rowPlaying ? "Currently playing in ListenLab" : rowTrackUri ? `Play ${track.track_name ?? "track"} in ListenLab` : `${track.track_name ?? "Track"} is not playable`}
                      buttonClassName={`secondary-button detail-album-track-play-button${rowPlaying ? " detail-icon-button-playing" : ""}`}
                      disabled={!rowTrackUri}
                      isPlaying={rowPlaying}
                      placement="overlay-trigger"
                      onAction={(action) => onPlayTrack(track, action)}
                    >
                      {rowPlaying ? (
                        <span className="detail-wave-icon" aria-hidden="true">
                          <span />
                          <span />
                          <span />
                        </span>
                      ) : (
                        <span className="detail-play-icon" aria-hidden="true">{"\u25B6"}</span>
                      )}
                      <span className={`detail-album-track-play-time${rowIsCurrentTrack && playbackPaused ? " detail-album-track-play-time-flash" : ""}`}>
                        {rowButtonTimeMs != null ? formatPlaybackClock(rowButtonTimeMs) : "?:??"}
                      </span>
                    </PlaybackActionMenu>
                  ) : <span aria-hidden="true" />}
                  <span className="detail-album-track-liked-cell">
                    {isTrackLiked(track, track.track_id ?? rowTrackUri) ? (
                      <LikedBadge className="detail-album-track-liked-badge" />
                    ) : (
                      <span className="detail-album-track-liked-empty" aria-label="Not liked">-</span>
                    )}
                  </span>
                  <button
                    className="detail-album-track-name-button single-line-ellipsis"
                    onClick={() => onSelectTrack(track)}
                    type="button"
                  >
                    {track.track_name ?? "Unknown track"}
                  </button>
                  <span className="detail-album-track-with single-line-ellipsis">{playlistTrackArtists(track)}</span>
                  {showCollaborativeColumns ? (
                    <span className="detail-album-track-added-by single-line-ellipsis">
                      {playlistTrackAddedBy(track)}
                    </span>
                  ) : null}
                  <span className="detail-album-track-tags-cell">
                    <span className="detail-album-track-badges">
                      {(() => {
                        const rowRelationTagsResult = trackRelationTags({
                          releaseTrackDuplicateSourceCount: track.release_track_duplicate_source_count,
                          releaseTrackSourceCount: track.release_track_source_count,
                          hasReleaseTrackSiblings: track.has_release_track_siblings,
                          releaseTrackClusterCandidateType: track.release_track_cluster_candidate_type,
                          releaseTrackClusterRelationshipKind: track.release_track_cluster_relationship_kind,
                        });
                        return rowRelationTagsResult.text ? (
                          <span
                            className="relation-tags-badge detail-album-track-relation-badge"
                            title={rowRelationTagsResult.title}
                            aria-label={rowRelationTagsResult.title}
                          >
                            {rowRelationTagsResult.text}
                          </span>
                        ) : <span className="detail-album-track-tags-empty">-</span>;
                      })()}
                    </span>
                  </span>
                  <span className="detail-album-track-listen-count">
                    {playlistTrackListenCount(track) > 0
                      ? playlistTrackListenCount(track).toLocaleString()
                      : "-"}
                  </span>
                  <span className="detail-album-track-last-played">
                    {formatCompactRelativeAge(track.recording_last_played_at ?? track.source_last_played_at ?? track.last_played_at) ?? (
                      Number(track.recording_play_count ?? track.source_play_count ?? track.play_count ?? 0) <= 0
                        ? <NewTrackBadge className="detail-album-track-played-new-badge" />
                        : "-"
                      )}
                  </span>
                  {showCollaborativeColumns ? (
                    <span className="detail-album-track-added-at">
                      {formatCompactRelativeAge(track.playlist_added_at) ?? "-"}
                    </span>
                  ) : null}
                </li>
              );
                })}
              </Fragment>
            ))}
          </ul>
          {hasMore ? <p className="detail-modal-preview-missing">Showing first {entries.length} tracks.</p> : null}
        </>
      ) : null}
      {!loading && !error && entries.length === 0 ? <p className="detail-modal-preview-missing">No tracks were returned for this playlist.</p> : null}
    </div>
  );
}
