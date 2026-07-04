import { Fragment, useEffect, useMemo, useRef, useState } from "react";

import type { RecentTrack } from "../../types/appTypes";
import { LikedBadge } from "../common/LikedBadge";
import { NewTrackBadge } from "../common/NewTrackBadge";
import { PlaybackActionMenu, type PlaybackAction } from "../playback/PlaybackActionMenu";
import { mergeRowsBySharedRecordingIdentity, recordingIdentityTokens } from "../../utils/recordingIdentity";
import { trackRelationTags } from "../../utils/trackRelationTags";
import { displayTrackArtistName, displayTrackName } from "../../utils/trackDisplayName";

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
  onShownCountChange?: (shownCount: number) => void;
  onTableOptionsChange: (options: PlaylistTableOptions) => void;
  onPreviewTrack: (track: RecentTrack, rowTrackUri: string | null) => Promise<void>;
  rowOffset: number;
  mergeRecordingTracks: boolean;
  showReleaseTrackStats: boolean;
  showCollaborativeColumns: boolean;
  total: number | null;
  onPlayTrack: (track: RecentTrack, action: PlaybackAction) => Promise<void>;
  onSelectAlbum: (track: RecentTrack) => void;
  onSelectArtist: (track: RecentTrack) => void;
  onSelectTrack: (track: RecentTrack) => void;
  playbackDurationMs: number;
  playbackPaused: boolean;
  playbackPositionMs: number;
  previewingTrackUri: string | null;
  previewPlayedTrackKeys: Set<string>;
  tableOptions: PlaylistTableOptions;
  trackUriWithFallback: (uri: string | null | undefined, trackId: string | null | undefined) => string | null;
};

function playlistTrackArtists(track: RecentTrack) {
  const artistName = track.artists?.map((artist) => artist.name).filter(Boolean).join(", ") || track.artist_name || "";
  return displayTrackArtistName(track.track_name, artistName) || "Unknown artist";
}

function playlistTrackAlbum(track: RecentTrack) {
  return track.album_name || "Unknown album";
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

export type PlaylistGroupMode = "none" | "artist" | "album" | "liked" | "listened" | "added_by";
export type PlaylistSortKey = "playlist" | "liked" | "title" | "album" | "artist" | "added_by" | "tags" | "listens" | "last" | "added" | "duration";
export type PlaylistFilterMode = "all" | "liked" | "unliked" | "has_tags" | "no_tags" | "listened" | "unlistened" | "played" | "never_played" | "has_added_at" | "duration_short" | "duration_medium" | "duration_long";
export type PlaylistTableOptions = {
  filterMode: PlaylistFilterMode;
  groupMode: PlaylistGroupMode;
  sortMode: { key: PlaylistSortKey; direction: "asc" | "desc" };
};
type PlaylistColumnKey = Exclude<PlaylistSortKey, "playlist" | "duration"> | "play";
type PlaylistDisplayTrack = RecentTrack & {
  playlist_merged_count?: number;
  playlist_merged_album_count?: number;
  playlist_merged_album_label?: string | null;
};

function playlistTrackListenCount(track: RecentTrack, showReleaseTrackStats: boolean) {
  return Number(
    showReleaseTrackStats
      ? track.source_play_count ?? 0
      : track.recording_play_count ?? track.play_count ?? track.source_play_count ?? 0,
  );
}

function playlistTrackLastPlayedAt(track: RecentTrack, showReleaseTrackStats: boolean) {
  return showReleaseTrackStats
    ? track.source_last_played_at ?? null
    : track.recording_last_played_at ?? track.last_played_at ?? track.source_last_played_at ?? null;
}

function playlistTrackDurationMs(track: RecentTrack) {
  const duration = Number(track.duration_ms ?? 0);
  return Number.isFinite(duration) ? Math.max(0, duration) : 0;
}

function playlistTrackGroupLabel(track: RecentTrack, mode: PlaylistGroupMode) {
  if (mode === "artist") {
    return playlistTrackArtists(track);
  }
  if (mode === "album") {
    return playlistTrackAlbum(track);
  }
  if (mode === "liked") {
    return "";
  }
  if (mode === "listened") {
    return "";
  }
  if (mode === "added_by") {
    return playlistTrackAddedBy(track);
  }
  return "";
}

function playlistTrackHasTags(track: RecentTrack) {
  return Boolean(trackRelationTags({
    releaseTrackDuplicateSourceCount: track.release_track_duplicate_source_count,
    releaseTrackSourceCount: track.release_track_source_count,
    hasReleaseTrackSiblings: track.has_release_track_siblings,
    releaseTrackClusterCandidateType: track.release_track_cluster_candidate_type,
    releaseTrackClusterRelationshipKind: track.release_track_cluster_relationship_kind,
  }).text);
}

function parseTimestamp(value: string | null | undefined) {
  if (!value) return 0;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizePlaylistMergeText(value: string | null | undefined) {
  return String(value ?? "")
    .toLocaleLowerCase()
    .replace(/\bfeat(?:uring)?\.?\b.*$/i, "")
    .replace(/\([^)]*\bfeat(?:uring)?\.?[^)]*\)/gi, "")
    .replace(/\[[^\]]*\bfeat(?:uring)?\.?[^\]]*\]/gi, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function playlistTrackMergeTokens(track: RecentTrack) {
  const titleKey = normalizePlaylistMergeText(track.track_name);
  const artistNames = [
    ...(track.artists?.map((artist) => artist.name ?? "") ?? []),
    ...(track.artist_name ?? "").split(","),
  ]
    .map(normalizePlaylistMergeText)
    .filter(Boolean)
    .sort();
  const textToken = `text:${titleKey}:${Array.from(new Set(artistNames)).join(",")}`;
  return [...recordingIdentityTokens(track), textToken];
}

function mergePlaylistTrackRows(rows: Array<{ track: RecentTrack; originalIndex: number }>) {
  return mergeRowsBySharedRecordingIdentity(rows, (row) => playlistTrackMergeTokens(row.track)).map((group) => {
    const sortedGroup = group.slice().sort((left, right) => left.originalIndex - right.originalIndex);
    const representative = sortedGroup[0];
    const albums = Array.from(new Set(sortedGroup.map(({ track }) => playlistTrackAlbum(track)).filter(Boolean)));
    const albumLabel = albums[0] ?? null;
    const mergedTrack: PlaylistDisplayTrack = {
      ...representative.track,
      playlist_merged_count: sortedGroup.length,
      playlist_merged_album_count: albums.length,
      playlist_merged_album_label: albumLabel,
      is_liked: sortedGroup.some(({ track }) => Boolean(track.is_liked)) || representative.track.is_liked,
      liked_at: sortedGroup.find(({ track }) => track.liked_at)?.track.liked_at ?? representative.track.liked_at ?? null,
    };
    return {
      track: mergedTrack,
      originalIndex: representative.originalIndex,
    };
  });
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
  onShownCountChange,
  onTableOptionsChange,
  onPreviewTrack,
  rowOffset,
  mergeRecordingTracks,
  showReleaseTrackStats,
  showCollaborativeColumns,
  total,
  onPlayTrack,
  onSelectAlbum,
  onSelectArtist,
  onSelectTrack,
  playbackDurationMs,
  playbackPaused,
  playbackPositionMs,
  previewingTrackUri,
  previewPlayedTrackKeys,
  tableOptions,
  trackUriWithFallback,
}: PlaylistTrackListProps) {
  const [openColumnMenu, setOpenColumnMenu] = useState<PlaylistColumnKey | null>(null);
  const [playColumnPreviewMode, setPlayColumnPreviewMode] = useState(false);
  const focusedRowRef = useRef<HTMLLIElement | null>(null);
  const { filterMode, groupMode, sortMode } = tableOptions;
  const transformedEntries = useMemo(() => {
    const filteredRows = entries
      .map((track, index) => ({ track, originalIndex: index }))
      .filter(({ track }) => {
        const liked = isTrackLiked(track, track.track_id ?? track.uri ?? null);
        const listenCount = playlistTrackListenCount(track, showReleaseTrackStats);
        const lastPlayedAt = playlistTrackLastPlayedAt(track, showReleaseTrackStats);
        const durationMs = playlistTrackDurationMs(track);
        switch (filterMode) {
          case "liked": return liked;
          case "unliked": return !liked;
          case "has_tags": return playlistTrackHasTags(track);
          case "no_tags": return !playlistTrackHasTags(track);
          case "listened": return listenCount > 0;
          case "unlistened": return listenCount <= 0;
          case "played": return Boolean(lastPlayedAt);
          case "never_played": return !lastPlayedAt;
          case "has_added_at": return Boolean(track.playlist_added_at);
          case "duration_short": return durationMs > 0 && durationMs < 180_000;
          case "duration_medium": return durationMs >= 180_000 && durationMs <= 300_000;
          case "duration_long": return durationMs > 300_000;
          case "all":
          default:
          return true;
        }
      });
    const rows = mergeRecordingTracks ? mergePlaylistTrackRows(filteredRows) : filteredRows;
    const directionMultiplier = sortMode.direction === "asc" ? 1 : -1;
    return rows.slice().sort((left, right) => {
      if (sortMode.key === "playlist") {
        return (left.originalIndex - right.originalIndex) * directionMultiplier;
      }
      let comparison = 0;
      if (sortMode.key === "liked") {
        comparison = Number(isTrackLiked(left.track, left.track.track_id ?? left.track.uri ?? null)) - Number(isTrackLiked(right.track, right.track.track_id ?? right.track.uri ?? null));
      } else if (sortMode.key === "title") {
        comparison = String(left.track.track_name ?? "").localeCompare(String(right.track.track_name ?? ""));
      } else if (sortMode.key === "album") {
        comparison = playlistTrackAlbum(left.track).localeCompare(playlistTrackAlbum(right.track));
      } else if (sortMode.key === "artist") {
        comparison = playlistTrackArtists(left.track).localeCompare(playlistTrackArtists(right.track));
      } else if (sortMode.key === "added_by") {
        comparison = playlistTrackAddedBy(left.track).localeCompare(playlistTrackAddedBy(right.track));
      } else if (sortMode.key === "tags") {
        comparison = Number(playlistTrackHasTags(left.track)) - Number(playlistTrackHasTags(right.track));
      } else if (sortMode.key === "listens") {
        comparison = playlistTrackListenCount(left.track, showReleaseTrackStats) - playlistTrackListenCount(right.track, showReleaseTrackStats);
      } else if (sortMode.key === "last") {
        comparison = parseTimestamp(playlistTrackLastPlayedAt(left.track, showReleaseTrackStats)) - parseTimestamp(playlistTrackLastPlayedAt(right.track, showReleaseTrackStats));
      } else if (sortMode.key === "added") {
        comparison = parseTimestamp(left.track.playlist_added_at) - parseTimestamp(right.track.playlist_added_at);
      } else if (sortMode.key === "duration") {
        comparison = playlistTrackDurationMs(left.track) - playlistTrackDurationMs(right.track);
      }
      if (comparison === 0) {
        return left.originalIndex - right.originalIndex;
      }
      return comparison * directionMultiplier;
    });
  }, [entries, filterMode, isTrackLiked, mergeRecordingTracks, showReleaseTrackStats, sortMode]);

  useEffect(() => {
    onShownCountChange?.(transformedEntries.length);
  }, [onShownCountChange, transformedEntries.length]);

  const displayGroups = useMemo(() => {
    if (groupMode === "none") {
      return [{
        key: "playlist-order",
        label: "",
        items: transformedEntries,
      }];
    }
    const groups: Array<{
      key: string;
      label: string;
      items: Array<{ track: RecentTrack; originalIndex: number }>;
    }> = [];
    const groupByKey = new Map<string, (typeof groups)[number]>();
    transformedEntries.forEach(({ track, originalIndex }) => {
      const label = groupMode === "liked"
        ? isTrackLiked(track, track.track_id ?? track.uri ?? null) ? "Liked" : "Not liked"
        : groupMode === "listened"
          ? playlistTrackListenCount(track, showReleaseTrackStats) > 0 ? "Listened" : "Unlistened"
          : playlistTrackGroupLabel(track, groupMode);
      const key = label.toLocaleLowerCase();
      let group = groupByKey.get(key);
      if (!group) {
        group = { key, label, items: [] };
        groupByKey.set(key, group);
        groups.push(group);
      }
      group.items.push({ track, originalIndex });
    });
    return groups;
  }, [groupMode, isTrackLiked, showReleaseTrackStats, transformedEntries]);

  useEffect(() => {
    if (!focusedRowRef.current) {
      return;
    }
    focusedRowRef.current.scrollIntoView({ block: "center", behavior: "smooth" });
  }, [entries, focusPlaylistPosition, focusSpotifyTrackId]);

  useEffect(() => {
    if (!openColumnMenu) {
      return;
    }
    function handlePointerDown(event: MouseEvent) {
      const target = event.target;
      if (target instanceof Element && target.closest(".playlist-column-menu-wrap")) {
        return;
      }
      setOpenColumnMenu(null);
    }
    document.addEventListener("mousedown", handlePointerDown);
    return () => document.removeEventListener("mousedown", handlePointerDown);
  }, [openColumnMenu]);

  const applySort = (key: PlaylistSortKey, direction: "asc" | "desc") => {
    onTableOptionsChange({ ...tableOptions, sortMode: { key, direction } });
    setOpenColumnMenu(null);
  };
  const applyFilter = (mode: PlaylistFilterMode) => {
    onTableOptionsChange({ ...tableOptions, filterMode: mode });
    setOpenColumnMenu(null);
  };
  const applyGroup = (mode: PlaylistGroupMode) => {
    onTableOptionsChange({ ...tableOptions, groupMode: mode });
    setOpenColumnMenu(null);
  };
  const columnHasActiveTableOption = (column: PlaylistColumnKey) => {
    const sortActive = sortMode.key === column;
    const groupActive = (
      (column === "artist" && groupMode === "artist")
      || (column === "album" && groupMode === "album")
      || (column === "liked" && groupMode === "liked")
      || (column === "listens" && groupMode === "listened")
      || (column === "added_by" && groupMode === "added_by")
    );
    const filterActive = (
      (column === "liked" && (filterMode === "liked" || filterMode === "unliked"))
      || (column === "tags" && (filterMode === "has_tags" || filterMode === "no_tags"))
      || (column === "listens" && (filterMode === "listened" || filterMode === "unlistened"))
      || (column === "last" && (filterMode === "played" || filterMode === "never_played"))
      || (column === "added" && filterMode === "has_added_at")
      || (column === "play" && (filterMode === "duration_short" || filterMode === "duration_medium" || filterMode === "duration_long"))
    );
    const playActive = column === "play" && (playColumnPreviewMode || sortMode.key === "duration");
    return sortActive || groupActive || filterActive || playActive;
  };
  const headerMenu = (
    column: PlaylistColumnKey,
    label: string,
    options: {
      sort?: Array<{ label: string; key: PlaylistSortKey; direction: "asc" | "desc" }>;
      filters?: Array<{ label: string; mode: PlaylistFilterMode }>;
      groups?: Array<{ label: string; mode: PlaylistGroupMode }>;
      actions?: Array<{ label: string; active: boolean; onClick: () => void }>;
    },
  ) => (
    <span className="playlist-column-menu-wrap">
      <button
        aria-expanded={openColumnMenu === column}
        className={`playlist-column-header-button${openColumnMenu === column ? " playlist-column-header-button-active" : ""}${columnHasActiveTableOption(column) ? " playlist-column-header-button-has-option" : ""}`}
        onClick={() => setOpenColumnMenu((current) => current === column ? null : column)}
        type="button"
      >
        <span>{label}</span>
        {columnHasActiveTableOption(column) ? <span className="playlist-column-active-dot" aria-hidden="true" /> : null}
      </button>
      {openColumnMenu === column ? (
        <span className="playlist-column-menu">
          {options.sort?.length ? <span className="playlist-column-menu-label">Sort</span> : null}
          {options.sort?.map((option) => (
            <button
              className={sortMode.key === option.key && sortMode.direction === option.direction ? "playlist-column-menu-item-active" : undefined}
              key={`sort-${option.label}`}
              onClick={() => applySort(option.key, option.direction)}
              type="button"
            >
              {option.label}
            </button>
          ))}
          {options.filters?.length ? <span className="playlist-column-menu-label">Filter</span> : null}
          {options.filters?.map((option) => (
            <button
              className={filterMode === option.mode ? "playlist-column-menu-item-active" : undefined}
              key={`filter-${option.label}`}
              onClick={() => applyFilter(option.mode)}
              type="button"
            >
              {option.label}
            </button>
          ))}
          {options.groups?.length ? <span className="playlist-column-menu-label">Group</span> : null}
          {options.groups?.map((option) => (
            <button
              className={groupMode === option.mode ? "playlist-column-menu-item-active" : undefined}
              key={`group-${option.label}`}
              onClick={() => applyGroup(option.mode)}
              type="button"
            >
              {option.label}
            </button>
          ))}
          {options.actions?.length ? <span className="playlist-column-menu-label">Display</span> : null}
          {options.actions?.map((option) => (
            <button
              className={option.active ? "playlist-column-menu-item-active" : undefined}
              key={`action-${option.label}`}
              onClick={option.onClick}
              type="button"
            >
              {option.active ? "✓ " : ""}{option.label}
            </button>
          ))}
          <button
            onClick={() => {
              onTableOptionsChange({
                filterMode: "all",
                groupMode: "none",
                sortMode: { key: "playlist", direction: "asc" },
              });
              setOpenColumnMenu(null);
            }}
            type="button"
          >
            Clear table options
          </button>
        </span>
      ) : null}
    </span>
  );

  return (
    <div className={`detail-modal-album-tracks detail-modal-album-tracks-full detail-modal-album-tracks-no-with${showCollaborativeColumns ? " detail-modal-playlist-tracks-collaborative" : ""}`}>
      <div className="detail-modal-album-header">
        <span className="detail-modal-album-number-header">#</span>
        <span className="detail-modal-album-preview-header">Pre</span>
        <span className="detail-modal-album-play-header">
          {headerMenu("play", "Play", {
            sort: [
              { label: "Shortest first", key: "duration", direction: "asc" },
              { label: "Longest first", key: "duration", direction: "desc" },
            ],
            filters: [
              { label: "All lengths", mode: "all" },
              { label: "Under 3 min", mode: "duration_short" },
              { label: "3-5 min", mode: "duration_medium" },
              { label: "Over 5 min", mode: "duration_long" },
            ],
            actions: [{
              label: "Preview",
              active: playColumnPreviewMode,
              onClick: () => setPlayColumnPreviewMode((current) => !current),
            }],
          })}
        </span>
        <span className="detail-modal-album-liked-header">
          {headerMenu("liked", "★", {
            sort: [
              { label: "Liked first", key: "liked", direction: "desc" },
              { label: "Unliked first", key: "liked", direction: "asc" },
            ],
            filters: [
              { label: "All tracks", mode: "all" },
              { label: "Liked only", mode: "liked" },
              { label: "Unliked only", mode: "unliked" },
            ],
            groups: [{ label: "Group by liked", mode: "liked" }],
          })}
        </span>
        <span className="detail-modal-album-title-header">
          {headerMenu("title", "Track", {
            sort: [
              { label: "Title A-Z", key: "title", direction: "asc" },
              { label: "Title Z-A", key: "title", direction: "desc" },
              { label: "Playlist order", key: "playlist", direction: "asc" },
            ],
          })}
        </span>
        <span className="detail-modal-playlist-album-header">
          {headerMenu("album", "Album", {
            sort: [
              { label: "Album A-Z", key: "album", direction: "asc" },
              { label: "Album Z-A", key: "album", direction: "desc" },
            ],
            groups: [{ label: "Group by album", mode: "album" }],
          })}
        </span>
        <span className="detail-modal-album-with-header">
          {headerMenu("artist", "Artist", {
            sort: [
              { label: "Artist A-Z", key: "artist", direction: "asc" },
              { label: "Artist Z-A", key: "artist", direction: "desc" },
            ],
            groups: [
              { label: "Group by artist", mode: "artist" },
            ],
          })}
        </span>
        {showCollaborativeColumns ? (
          <span className="detail-modal-album-added-by-header">
            {headerMenu("added_by", "Added by", {
              sort: [
                { label: "Added by A-Z", key: "added_by", direction: "asc" },
                { label: "Added by Z-A", key: "added_by", direction: "desc" },
              ],
              groups: [{ label: "Group by added by", mode: "added_by" }],
            })}
          </span>
        ) : null}
        <span className="detail-modal-album-added-at-header">
          {headerMenu("added", "Added", {
            sort: [
              { label: "Newest added", key: "added", direction: "desc" },
              { label: "Oldest added", key: "added", direction: "asc" },
            ],
            filters: [
              { label: "All tracks", mode: "all" },
              { label: "Has added date", mode: "has_added_at" },
            ],
          })}
        </span>
        <span className="detail-modal-album-liked-header">
          {headerMenu("tags", "Tags", {
            sort: [
              { label: "Tagged first", key: "tags", direction: "desc" },
              { label: "No tags first", key: "tags", direction: "asc" },
            ],
            filters: [
              { label: "All tracks", mode: "all" },
              { label: "Has tags", mode: "has_tags" },
              { label: "No tags", mode: "no_tags" },
            ],
          })}
        </span>
        <span className="detail-modal-album-listens-header">
          {headerMenu("listens", "Count", {
            sort: [
              { label: "Most listens", key: "listens", direction: "desc" },
              { label: "Fewest listens", key: "listens", direction: "asc" },
            ],
            filters: [
              { label: "All tracks", mode: "all" },
              { label: "Listened", mode: "listened" },
              { label: "Unlistened", mode: "unlistened" },
            ],
            groups: [{ label: "Group by listened", mode: "listened" }],
          })}
        </span>
        <span className="detail-modal-album-last-played-header">
          {headerMenu("last", "Last", {
            sort: [
              { label: "Newest first", key: "last", direction: "desc" },
              { label: "Oldest first", key: "last", direction: "asc" },
            ],
            filters: [
              { label: "All tracks", mode: "all" },
              { label: "Played", mode: "played" },
              { label: "Never played", mode: "never_played" },
            ],
          })}
        </span>
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
              const rowPausedAtEnd = Boolean(
                rowIsCurrentTrack
                && playbackPaused
                && rowBaseDurationMs != null
                && rowBaseDurationMs > 0
                && playbackPositionMs >= rowBaseDurationMs - 750,
              );
              const rowButtonTimeMs = rowIsCurrentTrack
                ? rowPlaying
                  ? Math.min(Math.max(0, playbackPositionMs), rowBaseDurationMs ?? playbackPositionMs)
                  : playbackPaused
                    ? rowPausedAtEnd ? rowBaseDurationMs : Math.max(0, playbackPositionMs)
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
                  {hasPremiumPlayback && playColumnPreviewMode ? (
                    <button
                      aria-label={rowPreviewPlaying ? `Stop preview for ${track.track_name ?? "track"}` : `Preview ${track.track_name ?? "track"}`}
                      className={`secondary-button detail-album-track-play-button detail-album-track-play-preview-button${rowPreviewActive ? " detail-icon-button-playing" : ""}`}
                      disabled={!rowTrackUri}
                      onClick={() => {
                        void onPreviewTrack(track, rowTrackUri);
                      }}
                      type="button"
                    >
                      <span
                        className={`detail-album-track-preview-button${rowPreviewActive ? " detail-album-track-preview-button-active" : ""}${rowPreviewPlayed ? " detail-album-track-preview-button-played" : ""}`}
                        aria-hidden="true"
                      />
                      <span className={`detail-album-track-play-time${rowPreviewActive ? " detail-album-track-play-time-flash" : ""}`}>
                        {rowButtonTimeMs != null ? formatPlaybackClock(rowButtonTimeMs) : "?:??"}
                      </span>
                    </button>
                  ) : hasPremiumPlayback ? (
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
                      <span className={`detail-album-track-play-time${rowIsCurrentTrack && playbackPaused && !rowPausedAtEnd ? " detail-album-track-play-time-flash" : ""}`}>
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
                    className="detail-album-track-name-button"
                    onClick={() => onSelectTrack(track)}
                    type="button"
                  >
                    <span className="detail-playlist-cell-text single-line-ellipsis">{displayTrackName(track.track_name ?? "Unknown track")}</span>
                    {(track as PlaylistDisplayTrack).playlist_merged_count && (track as PlaylistDisplayTrack).playlist_merged_count! > 1 ? (
                      <span className="detail-playlist-merged-count">+{(track as PlaylistDisplayTrack).playlist_merged_count! - 1}</span>
                    ) : null}
                  </button>
                  <button
                    className="detail-album-track-album detail-playlist-cell-button"
                    onClick={() => onSelectAlbum(track)}
                    type="button"
                  >
                    <span className="detail-playlist-cell-text single-line-ellipsis">
                      {(track as PlaylistDisplayTrack).playlist_merged_album_label ?? playlistTrackAlbum(track)}
                    </span>
                    {(track as PlaylistDisplayTrack).playlist_merged_album_count && (track as PlaylistDisplayTrack).playlist_merged_album_count! > 1 ? (
                      <span className="detail-playlist-merged-count">+{(track as PlaylistDisplayTrack).playlist_merged_album_count! - 1}</span>
                    ) : null}
                  </button>
                  <button
                    className="detail-album-track-with detail-playlist-cell-button single-line-ellipsis"
                    onClick={() => onSelectArtist(track)}
                    type="button"
                  >
                    {playlistTrackArtists(track)}
                  </button>
                  {showCollaborativeColumns ? (
                    <span className="detail-album-track-added-by single-line-ellipsis">
                      {playlistTrackAddedBy(track)}
                    </span>
                  ) : null}
                  <span className="detail-album-track-added-at">
                    {formatCompactRelativeAge(track.playlist_added_at) ?? "-"}
                  </span>
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
                    {playlistTrackListenCount(track, showReleaseTrackStats) > 0
                      ? playlistTrackListenCount(track, showReleaseTrackStats).toLocaleString()
                      : "-"}
                  </span>
                  <span className="detail-album-track-last-played">
                    {formatCompactRelativeAge(playlistTrackLastPlayedAt(track, showReleaseTrackStats)) ?? (
                      playlistTrackListenCount(track, showReleaseTrackStats) <= 0
                        ? <NewTrackBadge className="detail-album-track-played-new-badge" />
                        : "-"
                      )}
                  </span>
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
