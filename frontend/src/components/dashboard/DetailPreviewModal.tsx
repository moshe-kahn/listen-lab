import { Fragment, useEffect, useMemo, useState, type CSSProperties, type Dispatch, type FormEvent, type MouseEvent, type ReactNode, type Ref, type RefObject, type SetStateAction } from "react";

import type {
  AlbumFamilyContext,
  AlbumTrackEntry,
  ArtistAlbumEntry,
  PlaybackActionRequest,
  PlaylistIndexStatus,
  PlaylistMembership,
  OwnedPlaylist,
  PlayerQueueTrack,
  PlayerTrackSummary,
  PreviewItem,
  RecentTrack,
  RecordingRelationRows,
  RecordingTrackCandidateMember,
  ReleaseTrackDetailResponse,
  ReleaseTrackDetailSourceVersion,
  TrackArtistEntry,
} from "../../types/appTypes";
import { LikedBadge } from "../common/LikedBadge";
import { NewTrackBadge } from "../common/NewTrackBadge";
import { ReleaseSiblingBadge } from "../common/ReleaseSiblingBadge";
import { PlaybackActionMenu, type PlaybackAction } from "../playback/PlaybackActionMenu";
import { trackRelationTags } from "../../utils/trackRelationTags";
import { TrackRelationList } from "./TrackRelationList";
import { AlbumVersionSelector } from "./AlbumVersionSelector";
import { displayTrackName, remasterYearFromTrackName } from "../../utils/trackDisplayName";
import { AlbumVersionTrackSummary } from "./AlbumVersionTrackSummary";
import { RelatedAlbumsSection } from "./RelatedAlbumsSection";
import { PlaylistTrackList, type PlaylistFilterMode, type PlaylistGroupMode, type PlaylistSortKey, type PlaylistTableOptions } from "./PlaylistTrackList";

type AlbumPlaybackQueue = {
  playlistUris: string[];
  queueTracks: PlayerQueueTrack[];
  queueCursor: number;
  queueContext: {
    label: string;
    url?: string | null;
  };
};

type AppearsOnReleaseType = "Single" | "Soundtrack" | "Compilation" | "Album" | "Release";
type PlaylistAddSortMode = "recent" | "name" | "tracks" | "membership";
type PlaylistAddCategoryFilter = "active" | "all" | "none" | string;

const PLAYLIST_TABLE_OPTIONS_STORAGE_KEY = "listenlab.playlistTableOptions.v1";
const DEFAULT_PLAYLIST_TABLE_OPTIONS: PlaylistTableOptions = {
  filterMode: "all",
  groupMode: "none",
  sortMode: { key: "playlist", direction: "asc" },
};

function spotifyPlaylistUrl(preview: PreviewItem | null | undefined) {
  if (!preview || preview.kind !== "playlist") {
    return null;
  }
  if (preview.url) {
    return preview.url;
  }
  return preview.entityId ? `https://open.spotify.com/playlist/${encodeURIComponent(preview.entityId)}` : null;
}

function isPlaylistTrackAccessDenied(error: string | null) {
  if (!error) {
    return false;
  }
  const normalized = error.toLocaleLowerCase();
  return normalized.includes("spotify denied access to this playlist's tracks")
    || normalized.includes("spotify denied access to this resource: forbidden")
    || normalized.includes("failed to load playlist tracks (403)");
}
const PLAYLIST_FILTER_LABELS: Record<PlaylistFilterMode, string> = {
  all: "All tracks",
  liked: "Liked only",
  unliked: "Unliked only",
  has_tags: "Has tags",
  no_tags: "No tags",
  listened: "Listened",
  unlistened: "Unlistened",
  played: "Played",
  never_played: "Never played",
  has_added_at: "Has added date",
  duration_short: "Under 3 min",
  duration_medium: "3-5 min",
  duration_long: "Over 5 min",
};
const PLAYLIST_GROUP_LABELS: Record<PlaylistGroupMode, string> = {
  none: "No grouping",
  artist: "Artist",
  album: "Album",
  liked: "Liked",
  listened: "Listened",
  added_by: "Added by",
};
const PLAYLIST_SORT_LABELS: Record<PlaylistSortKey, string> = {
  playlist: "Playlist order",
  liked: "Liked",
  title: "Title",
  album: "Album",
  artist: "Artist",
  added_by: "Added by",
  tags: "Tags",
  listens: "Count",
  last: "Last",
  added: "Added",
  duration: "Length",
};

function playlistTableOptionsEqual(left: PlaylistTableOptions, right: PlaylistTableOptions) {
  return left.filterMode === right.filterMode
    && left.groupMode === right.groupMode
    && left.sortMode.key === right.sortMode.key
    && left.sortMode.direction === right.sortMode.direction;
}

function coercePlaylistTableOptions(value: unknown): PlaylistTableOptions {
  const candidate = value as Partial<PlaylistTableOptions> | null;
  const filterModes = new Set<PlaylistFilterMode>(["all", "liked", "unliked", "has_tags", "no_tags", "listened", "unlistened", "played", "never_played", "has_added_at", "duration_short", "duration_medium", "duration_long"]);
  const groupModes = new Set<PlaylistGroupMode>(["none", "artist", "album", "liked", "listened", "added_by"]);
  const sortKeys = new Set<PlaylistSortKey>(["playlist", "liked", "title", "album", "artist", "added_by", "tags", "listens", "last", "added", "duration"]);
  const filterMode = filterModes.has(candidate?.filterMode as PlaylistFilterMode) ? candidate?.filterMode as PlaylistFilterMode : DEFAULT_PLAYLIST_TABLE_OPTIONS.filterMode;
  const groupMode = groupModes.has(candidate?.groupMode as PlaylistGroupMode) ? candidate?.groupMode as PlaylistGroupMode : DEFAULT_PLAYLIST_TABLE_OPTIONS.groupMode;
  const sortModeValue = candidate?.sortMode;
  const sortKey = sortKeys.has(sortModeValue?.key as PlaylistSortKey) ? sortModeValue?.key as PlaylistSortKey : DEFAULT_PLAYLIST_TABLE_OPTIONS.sortMode.key;
  const sortDirection = sortModeValue?.direction === "desc" ? "desc" : "asc";
  return { filterMode, groupMode, sortMode: { key: sortKey, direction: sortDirection } };
}

function readSavedPlaylistTableOptions() {
  if (typeof window === "undefined") {
    return DEFAULT_PLAYLIST_TABLE_OPTIONS;
  }
  try {
    return coercePlaylistTableOptions(JSON.parse(window.localStorage.getItem(PLAYLIST_TABLE_OPTIONS_STORAGE_KEY) ?? "null"));
  } catch {
    return DEFAULT_PLAYLIST_TABLE_OPTIONS;
  }
}
type PlaylistCategory = {
  id: string;
  name: string;
  playlistIds: string[];
};

const appearsOnReleaseTypeOrder: AppearsOnReleaseType[] = [
  "Single",
  "Soundtrack",
  "Compilation",
  "Album",
  "Release",
];

function appearsOnReleaseType(member: RecordingTrackCandidateMember): AppearsOnReleaseType {
  const preferredAlbum = member.album_versions?.find((version) => Boolean(version.spotify_album_id && version.is_direct_source_album))
    ?? member.album_versions?.find((version) => Boolean(version.spotify_album_id))
    ?? member.album_versions?.[0]
    ?? null;
  const albumType = String(preferredAlbum?.album_type ?? member.album_types?.[0] ?? "").trim().toLocaleLowerCase();
  const albumName = String(preferredAlbum?.name ?? member.album ?? "").trim().toLocaleLowerCase();
  if (albumType === "single") {
    return "Single";
  }
  if (/\b(soundtrack|ost|original score|motion picture|bande originale|bo du film)\b/.test(albumName)) {
    return "Soundtrack";
  }
  if (albumType === "compilation") {
    return "Compilation";
  }
  if (albumType === "album") {
    return "Album";
  }
  return "Release";
}

function appearsOnSummary(members: RecordingTrackCandidateMember[]) {
  const counts = new Map<AppearsOnReleaseType, number>();
  members.forEach((member) => {
    const releaseType = appearsOnReleaseType(member);
    counts.set(releaseType, (counts.get(releaseType) ?? 0) + 1);
  });
  return appearsOnReleaseTypeOrder
    .flatMap((releaseType) => {
      const count = counts.get(releaseType) ?? 0;
      if (count === 0) {
        return [];
      }
      return [`${count} ${releaseType.toLocaleLowerCase()}${count === 1 ? "" : "s"}`];
    })
    .join(" · ");
}

function recordingVersionTagLabel(version: ReleaseTrackDetailSourceVersion | null) {
  const rawName = String(version?.name ?? "").trim();
  const suffix = versionDescriptorFromText(rawName);
  if (suffix) {
    return suffix;
  }
  const albumName = String(version?.album_name ?? "").trim();
  const albumMatch = versionDescriptorFromText(albumName);
  if (albumMatch) return albumMatch;
  return version?.is_representative_choice ? "Representative" : "Version";
}

function versionDescriptorFromText(value: string | null | undefined) {
  const raw = String(value ?? "").trim();
  const match = raw.match(/\s[-\u2013\u2014]\s((?:(?:\d{4}\s+)?remaster(?:ed)?|deluxe|expanded|anniversary|mono|stereo|live|demo|acoustic|instrumental|edit|mix|version).*)$/i)
    ?? raw.match(/\(([^)]*(?:remaster(?:ed)?|deluxe|expanded|anniversary|mono|stereo|live|demo|acoustic|instrumental|edit|mix|version)[^)]*)\)\s*$/i)
    ?? raw.match(/\[([^\]]*(?:remaster(?:ed)?|deluxe|expanded|anniversary|mono|stereo|live|demo|acoustic|instrumental|edit|mix|version)[^\]]*)\]\s*$/i);
  return match?.[1]?.trim().replace(/\s+/g, " ") ?? null;
}

function playlistOverlayTrackListenCount(track: RecentTrack, view: "recording" | "release") {
  return Number(
    view === "release"
      ? track.source_play_count ?? 0
      : track.recording_play_count ?? track.play_count ?? track.source_play_count ?? 0,
  );
}

function playlistOverlayTrackIsComplete(track: RecentTrack, view: "recording" | "release") {
  if (typeof track.estimated_completion_ratio === "number" && Number.isFinite(track.estimated_completion_ratio)) {
    return track.estimated_completion_ratio >= 0.98;
  }
  return playlistOverlayTrackListenCount(track, view) > 0;
}

type DetailPreviewModalProps = {
  apiBaseUrl: string;
  albumFamilyContext: AlbumFamilyContext | null;
  albumTrackEntries: AlbumTrackEntry[];
  albumTrackEntriesError: string | null;
  albumTrackEntriesLoading: boolean;
  albumTrackEntriesPartial: boolean;
  albumFamilyDiscScrollTarget: number | null;
  albumTrackFetchFromSpotifyLoading: boolean;
  albumTrackMoreOnSpotifyUrl: string | null;
  albumTrackIsExactKnownLiked: (track: AlbumTrackEntry) => boolean;
  albumTrackIsKnownLiked: (track: AlbumTrackEntry) => boolean;
  albumTrackLastSortMode: "recent" | "oldest" | null;
  albumTrackListRef: RefObject<HTMLUListElement>;
  albumTrackPreviewKey: (track: AlbumTrackEntry, rowTrackUri: string | null) => string;
  albumTracklistSummaryLabel: (entries: AlbumTrackEntry[]) => string;
  artistEntriesForAlbumTrack: (track: AlbumTrackEntry) => TrackArtistEntry[];
  artistNameMatches: (candidate: string | null | undefined, target: string | null | undefined) => boolean;
  backendSelectedPreviewArtistAlbums: ArtistAlbumEntry[] | null;
  buildAlbumPlaybackQueue: (selectedTrackUri: string | null, entries?: AlbumTrackEntry[], contextPreview?: PreviewItem | null) => AlbumPlaybackQueue | null;
  clearAlbumWithArtistHighlight: () => void;
  clearAlbumFamilyDiscScrollTarget: () => void;
  currentTrack: PlayerTrackSummary | null;
  detailOptionsOpen: boolean;
  displayAlbumTrackEntries: AlbumTrackEntry[];
  formatCompactRelativeAge: (value: string | null | undefined) => string | null;
  formatPlaybackClock: (positionMs: number) => string;
  handleAlbumPlayAll: (action?: PlaybackAction, entries?: AlbumTrackEntry[]) => Promise<void>;
  handlePlaybackAction: (action: PlaybackAction, request: PlaybackActionRequest) => Promise<void>;
  includeAlbumFamilyTracks: (spotifyAlbumId: string) => void;
  hasPremiumPlayback: boolean;
  hoveredAlbumWithArtistName: string | null;
  isTrackPlaying: (trackUri: string | null) => boolean;
  localStarredTrackById: Record<string, boolean>;
  nextLastPlayedSortMode: (current: "recent" | "oldest" | null) => "recent" | "oldest" | null;
  openAlbumTrackPreview: (track: AlbumTrackEntry) => void;
  openAlbumWithArtistPreview: (artist: TrackArtistEntry) => void;
  openArtistAlbumPreview: (album: ArtistAlbumEntry) => void;
  openRecordingCandidateReleaseTrack: (member: RecordingTrackCandidateMember, detailView?: "recording" | "release") => void;
  openRecentTrackAlbumPreview: (track: RecentTrack) => void;
  openRecentTrackArtistPreview: (track: RecentTrack) => void;
  openRecentPlayerTrackDetails: (track: RecentTrack) => void;
  openReleaseSourceVersion: (version: ReleaseTrackDetailSourceVersion, detailView?: "recording" | "release") => void;
  openSelectedAlbumArtistPreview: (artist?: TrackArtistEntry) => void;
  openSelectedArtistMemberPreview: (artist: TrackArtistEntry) => void;
  openSelectedTrackAlbumPreview: () => void;
  openSelectedTrackArtistPreview: (artist?: TrackArtistEntry) => void;
  openPlaylistMembershipPreview: (membership: PlaylistMembership) => void;
  onAddSelectedTrackToPlaylists: (playlistIds: string[], removePlaylistIds: string[], newPlaylistName: string | null) => Promise<void>;
  playlistAddRequestNonce: number;
  pausedTimeFlashOn: boolean;
  playbackDurationMs: number;
  playbackPaused: boolean;
  playbackPositionMs: number;
  playlistTrackEntries: RecentTrack[];
  playlistTrackEntriesError: string | null;
  playlistTrackEntriesHasMore: boolean;
  playlistTrackEntriesLoading: boolean;
  playlistTrackEntriesOffset: number;
  playlistTrackEntriesShowCollaborativeColumns: boolean;
  playlistTrackEntriesTotal: number | null;
  loadMorePlaylistTrackEntries: () => Promise<void>;
  recentTrackIsKnownLiked: (track: RecentTrack | null | undefined, fallbackTrackId?: string | null) => boolean;
  togglePlaylistTrackPreview: (track: RecentTrack, rowTrackUri: string | null) => Promise<void>;
  handlePlaylistPlayAll: (action: PlaybackAction) => Promise<void>;
  handlePlaylistTrackPlayback: (track: RecentTrack, action: PlaybackAction) => Promise<void>;
  playerSummaryFromAlbumTrack: (track: AlbumTrackEntry) => PlayerTrackSummary;
  previewAlbumHeading: (preview: PreviewItem) => string | null;
  previewPlayedTrackKeys: Set<string>;
  previewingTrackUri: string | null;
  recordingMemberAlbumImageUrl: (member: RecordingTrackCandidateMember) => string | null;
  recordingMemberAlbumName: (member: RecordingTrackCandidateMember) => string | null;
  recordingMemberReleaseYear: (member: RecordingTrackCandidateMember) => string | null;
  recordingVariationStripRef: Ref<HTMLDivElement>;
  releaseSourceVersionAlbumImageUrl: (version: ReleaseTrackDetailSourceVersion) => string | null;
  releaseSourceVersionPlayCountLabel: (version: ReleaseTrackDetailSourceVersion) => string | null;
  renderSelectedPreviewArtistAlbumSection: (title: string, albums: ArtistAlbumEntry[]) => ReactNode;
  renderSelectedPreviewArtistTrackSection: () => ReactNode;
  scheduleAlbumWithArtistHighlight: (artistName: string) => void;
  scrollRecordingVariationStrip: (direction: -1 | 1) => void;
  selectedAlbumTrackMarkerTop: (entries: AlbumTrackEntry[], minScrollableTrackCount?: number) => string | null;
  selectedPreview: PreviewItem | null;
  selectedPreviewAlbumGuestArtists: TrackArtistEntry[];
  selectedPreviewAlbumHasGuestArtists: boolean;
  selectedPreviewAlbumIsSpotifyLiked: boolean;
  selectedPreviewAlbumMainArtists: TrackArtistEntry[];
  selectedPreviewAlbumSummary: string | null;
  selectedPreviewAlbumSpotifyId: string | null;
  selectedPreviewAlbumContextTagLabel: string | null;
  selectedPreviewAppearsOnAlbums: ArtistAlbumEntry[];
  selectedPreviewArtistAlbumsForDisplay: ArtistAlbumEntry[];
  selectedPreviewArtistFollowStatusKnown: boolean;
  selectedPreviewArtistImageUrl: string | null;
  selectedPreviewArtistIsSpotifyFollowed: boolean;
  selectedPreviewArtists: TrackArtistEntry[];
  selectedPreviewCanOpenAlbum: boolean;
  selectedPreviewCanOpenArtist: boolean;
  selectedPreviewCanonicalTrackTitle: string | null;
  selectedPreviewCurrentSpotifyTrackId: string | null;
  selectedPreviewCurrentVersionIsSpotifyLiked: boolean;
  selectedPreviewDetailView: "recording" | "release";
  selectedPreviewDisplayRelationRows: RecordingRelationRows;
  selectedPreviewHasReleaseSibling: boolean;
  selectedPreviewListenedBreakdown: {
    thisAlbumFirstLabel: string | null;
    thisAlbumLastLabel: string | null;
    otherAlbumsFirstLabel: string | null;
    otherAlbumsLastLabel: string | null;
  } | null;
  selectedPreviewListenedRangeLabel: string | null;
  selectedPreviewIsEntityBookmarked: boolean;
  selectedPreviewIsBookmarked: boolean;
  selectedPreviewIsKnownLiked: boolean;
  selectedPreviewIsSharedArtistPage: boolean;
  selectedPreviewListenBreakdown: { thisAlbumCount: number; otherAlbumsCount: number } | null;
  selectedPreviewListenCountLabel: string | null;
  selectedPreviewOtherRecordingMembers: RecordingTrackCandidateMember[];
  selectedPreviewPlaybackTrackUri: string | null;
  selectedPreviewPlaylistIndexStatus: PlaylistIndexStatus | null;
  selectedPreviewPlaylistMemberships: PlaylistMembership[];
  selectedPreviewPlaylistMembershipsLoading: boolean;
  selectedPreviewPlaylistOwnerFollowedByYou: boolean;
  selectedPreviewAvailablePlaylists: OwnedPlaylist[];
  selectedPreviewPrimaryArtistAlbums: ArtistAlbumEntry[];
  selectedPreviewRecordingCandidateError: string | null;
  selectedPreviewRecordingMembers: RecordingTrackCandidateMember[];
  selectedPreviewReleaseAlbumVariationCount: number;
  selectedPreviewReleaseSiblingSourceCount: number | null;
  selectedPreviewReleaseSourceVersionNeedsArrows: boolean;
  selectedPreviewReleaseSourceVersions: ReleaseTrackDetailSourceVersion[];
  selectedPreviewReleaseTrackDetailError: string | null;
  selectedPreviewReleaseTrackDetailReady: ReleaseTrackDetailResponse | null;
  selectedPreviewRelatedAlbums: ArtistAlbumEntry[];
  selectedPreviewStarTrackId: string | null;
  selectedPreviewTrackDurationLabel: string | null;
  selectedPreviewTrackGuestArtists: TrackArtistEntry[];
  selectedPreviewTrackMainArtists: TrackArtistEntry[];
  selectedPreviewTrackOptimisticSummary: PlayerTrackSummary | null;
  toggleSelectedPreviewEntityBookmark: () => void;
  toggleSelectedPreviewTrackBookmark: () => void;
  setAlbumTrackLastSortMode: Dispatch<SetStateAction<"recent" | "oldest" | null>>;
  setDetailOptionsOpen: Dispatch<SetStateAction<boolean>>;
  setLocalStarredAlbumById: Dispatch<SetStateAction<Record<string, boolean>>>;
  setLocalStarredTrackById: Dispatch<SetStateAction<Record<string, boolean>>>;
  setSelectedPreview: Dispatch<SetStateAction<PreviewItem | null>>;
  setSelectedPreviewDetailView: Dispatch<SetStateAction<"recording" | "release">>;
  spotifyTrackIdFromUri: (uri: string | null) => string | null;
  switchSelectedTrackAlbumVersion: (spotifyAlbumId: string) => void;
  toggleAlbumTrackPreview: (track: AlbumTrackEntry, rowTrackUri: string | null) => Promise<void>;
  trackUriWithFallback: (uri: string | null | undefined, trackId: string | null | undefined) => string | null;
  variationSubtitleFromTitle: (
    title: string | null | undefined,
    options?: { allowRemasterOnly?: boolean },
  ) => string | null;
};

export function DetailPreviewModal(props: DetailPreviewModalProps) {
  const [playlistTrackStatsView, setPlaylistTrackStatsView] = useState<"recording" | "release">("recording");
  const [playlistTrackMergeView, setPlaylistTrackMergeView] = useState<"merged" | "individual">("merged");
  const [savedPlaylistTableOptions, setSavedPlaylistTableOptions] = useState<PlaylistTableOptions>(() => readSavedPlaylistTableOptions());
  const [playlistTableOptions, setPlaylistTableOptions] = useState<PlaylistTableOptions>(() => readSavedPlaylistTableOptions());
  const [playlistTableShownCount, setPlaylistTableShownCount] = useState<number | null>(null);
  const {
    apiBaseUrl,
    albumFamilyContext,
    albumTrackEntries,
    albumTrackEntriesError,
    albumTrackEntriesLoading,
    albumTrackEntriesPartial,
    albumFamilyDiscScrollTarget,
    albumTrackFetchFromSpotifyLoading,
    albumTrackMoreOnSpotifyUrl,
    albumTrackIsExactKnownLiked,
    albumTrackIsKnownLiked,
    albumTrackLastSortMode,
    albumTrackListRef,
    albumTrackPreviewKey,
    albumTracklistSummaryLabel,
    artistEntriesForAlbumTrack,
    artistNameMatches,
    backendSelectedPreviewArtistAlbums,
    buildAlbumPlaybackQueue,
    clearAlbumWithArtistHighlight,
    clearAlbumFamilyDiscScrollTarget,
    currentTrack,
    detailOptionsOpen,
    displayAlbumTrackEntries,
    formatCompactRelativeAge,
    formatPlaybackClock,
    handleAlbumPlayAll,
    handlePlaybackAction,
    includeAlbumFamilyTracks,
    hasPremiumPlayback,
    hoveredAlbumWithArtistName,
    isTrackPlaying,
    localStarredTrackById,
    nextLastPlayedSortMode,
    openAlbumTrackPreview,
    openAlbumWithArtistPreview,
    openArtistAlbumPreview,
    openRecordingCandidateReleaseTrack,
    openRecentTrackAlbumPreview,
    openRecentTrackArtistPreview,
    openRecentPlayerTrackDetails,
    openReleaseSourceVersion,
    openSelectedAlbumArtistPreview,
    openSelectedArtistMemberPreview,
    openSelectedTrackAlbumPreview,
    openSelectedTrackArtistPreview,
    openPlaylistMembershipPreview,
    onAddSelectedTrackToPlaylists,
    playlistAddRequestNonce,
    pausedTimeFlashOn,
    playbackDurationMs,
    playbackPaused,
    playbackPositionMs,
    playlistTrackEntries,
    playlistTrackEntriesError,
    playlistTrackEntriesHasMore,
    playlistTrackEntriesLoading,
    playlistTrackEntriesOffset,
    playlistTrackEntriesShowCollaborativeColumns,
    playlistTrackEntriesTotal,
    loadMorePlaylistTrackEntries,
    recentTrackIsKnownLiked,
    togglePlaylistTrackPreview,
    handlePlaylistPlayAll,
    handlePlaylistTrackPlayback,
    playerSummaryFromAlbumTrack,
    previewAlbumHeading,
    previewPlayedTrackKeys,
    previewingTrackUri,
    recordingMemberAlbumImageUrl,
    recordingMemberAlbumName,
    recordingMemberReleaseYear,
    recordingVariationStripRef,
    releaseSourceVersionAlbumImageUrl,
    releaseSourceVersionPlayCountLabel,
    renderSelectedPreviewArtistAlbumSection,
    renderSelectedPreviewArtistTrackSection,
    scheduleAlbumWithArtistHighlight,
    scrollRecordingVariationStrip,
    selectedAlbumTrackMarkerTop,
    selectedPreview,
    selectedPreviewAlbumGuestArtists,
    selectedPreviewAlbumHasGuestArtists,
    selectedPreviewAlbumIsSpotifyLiked,
    selectedPreviewAlbumMainArtists,
    selectedPreviewAlbumSummary,
    selectedPreviewAlbumSpotifyId,
    selectedPreviewAlbumContextTagLabel,
    selectedPreviewAppearsOnAlbums,
    selectedPreviewArtistAlbumsForDisplay,
    selectedPreviewArtistFollowStatusKnown,
    selectedPreviewArtistImageUrl,
    selectedPreviewArtistIsSpotifyFollowed,
    selectedPreviewArtists,
    selectedPreviewCanOpenAlbum,
    selectedPreviewCanOpenArtist,
    selectedPreviewCanonicalTrackTitle,
    selectedPreviewCurrentSpotifyTrackId,
    selectedPreviewCurrentVersionIsSpotifyLiked,
    selectedPreviewDetailView,
    selectedPreviewDisplayRelationRows,
    selectedPreviewHasReleaseSibling,
    selectedPreviewListenedBreakdown,
    selectedPreviewListenedRangeLabel,
    selectedPreviewIsEntityBookmarked,
    selectedPreviewIsBookmarked,
    selectedPreviewIsKnownLiked,
    selectedPreviewIsSharedArtistPage,
    selectedPreviewListenBreakdown,
    selectedPreviewListenCountLabel,
    selectedPreviewOtherRecordingMembers,
    selectedPreviewPlaybackTrackUri,
    selectedPreviewPlaylistIndexStatus,
    selectedPreviewPlaylistMemberships,
    selectedPreviewPlaylistMembershipsLoading,
    selectedPreviewPlaylistOwnerFollowedByYou,
    selectedPreviewAvailablePlaylists,
    selectedPreviewPrimaryArtistAlbums,
    selectedPreviewRecordingCandidateError,
    selectedPreviewRecordingMembers,
    selectedPreviewReleaseAlbumVariationCount,
    selectedPreviewReleaseSiblingSourceCount,
    selectedPreviewReleaseSourceVersionNeedsArrows,
    selectedPreviewReleaseSourceVersions,
    selectedPreviewReleaseTrackDetailError,
    selectedPreviewReleaseTrackDetailReady,
    selectedPreviewRelatedAlbums,
    selectedPreviewStarTrackId,
    selectedPreviewTrackDurationLabel,
    selectedPreviewTrackGuestArtists,
    selectedPreviewTrackMainArtists,
    selectedPreviewTrackOptimisticSummary,
    toggleSelectedPreviewEntityBookmark,
    toggleSelectedPreviewTrackBookmark,
    setAlbumTrackLastSortMode,
    setDetailOptionsOpen,
    setLocalStarredAlbumById,
    setLocalStarredTrackById,
    setSelectedPreview,
    setSelectedPreviewDetailView,
    spotifyTrackIdFromUri,
    switchSelectedTrackAlbumVersion,
    toggleAlbumTrackPreview,
    trackUriWithFallback,
    variationSubtitleFromTitle,
  } = props;

  useEffect(() => {
    if (selectedPreview?.kind === "playlist") {
      setPlaylistTrackStatsView("recording");
      setPlaylistTrackMergeView("merged");
      const savedOptions = readSavedPlaylistTableOptions();
      setSavedPlaylistTableOptions(savedOptions);
      setPlaylistTableOptions(savedOptions);
      setPlaylistTableShownCount(null);
    }
  }, [selectedPreview?.kind, selectedPreview?.entityId, selectedPreview?.url]);
  const [hiddenAlbumDiscNumbers, setHiddenAlbumDiscNumbers] = useState<Set<number>>(() => new Set());

  useEffect(() => {
    setHiddenAlbumDiscNumbers(new Set());
  }, [albumFamilyContext?.album_family_id]);

  const selectedPreviewIsTrack = selectedPreview?.kind === "track";
  const playlistHeaderSummary = useMemo(() => {
    const loadedCount = playlistTrackEntries.length;
    const totalCount = playlistTrackEntriesTotal != null ? playlistTrackEntriesTotal : loadedCount;
    const albumKeys = new Set<string>();
    const artistKeys = new Set<string>();
    playlistTrackEntries.forEach((track) => {
      const albumKey = String(track.album_id ?? track.album_name ?? "").trim().toLocaleLowerCase();
      if (albumKey) {
        albumKeys.add(albumKey);
      }
      (track.artists ?? []).forEach((artist) => {
        const artistKey = String(artist.id ?? artist.artist_id ?? artist.name ?? "").trim().toLocaleLowerCase();
        if (artistKey) {
          artistKeys.add(artistKey);
        }
      });
    });
    const likedCount = playlistTrackEntries.filter((track) => recentTrackIsKnownLiked(track, track.track_id ?? track.uri ?? null)).length;
    const unlistenedCount = playlistTrackEntries.filter((track) => playlistOverlayTrackListenCount(track, playlistTrackStatsView) <= 0).length;
    const listenedCount = Math.max(0, loadedCount - unlistenedCount);
    const completeCount = playlistTrackEntries.filter((track) => playlistOverlayTrackIsComplete(track, playlistTrackStatsView)).length;
    const likedBarCount = Math.min(likedCount, loadedCount);
    const listenedBarCount = Math.max(0, listenedCount - likedBarCount);
    const denominator = Math.max(loadedCount, 1);
    const completePercent = loadedCount > 0 ? Math.round((completeCount / loadedCount) * 100) : 0;
    return {
      loadedCount,
      totalCount,
      albumCount: albumKeys.size,
      artistCount: artistKeys.size,
      likedCount,
      unlistenedCount,
      listenedCount,
      completeCount,
      completePercent,
      likedPercent: (likedBarCount / denominator) * 100,
      listenedPercent: (listenedBarCount / denominator) * 100,
      partial: totalCount > loadedCount,
    };
  }, [playlistTrackEntries, playlistTrackEntriesTotal, playlistTrackStatsView, recentTrackIsKnownLiked]);
  const playlistTableHasActiveOptions = !playlistTableOptionsEqual(playlistTableOptions, DEFAULT_PLAYLIST_TABLE_OPTIONS);
  const playlistTracksCanMerge = playlistTrackStatsView !== "release" && playlistTableOptions.groupMode !== "album";
  const playlistTracksAreMerged = playlistTracksCanMerge && playlistTrackMergeView === "merged";
  const playlistTrackAccessDenied = selectedPreview?.kind === "playlist" && isPlaylistTrackAccessDenied(playlistTrackEntriesError);
  const selectedPreviewSpotifyPlaylistUrl = spotifyPlaylistUrl(selectedPreview);
  const playlistTableHasUnsavedOptions = !playlistTableOptionsEqual(playlistTableOptions, savedPlaylistTableOptions);
  const playlistTableOptionLabels = [
    playlistTracksAreMerged ? "Merged recordings" : "Individual releases",
    playlistTableOptions.filterMode !== "all" ? `Filter: ${PLAYLIST_FILTER_LABELS[playlistTableOptions.filterMode]}` : null,
    playlistTableOptions.sortMode.key !== "playlist" || playlistTableOptions.sortMode.direction !== "asc"
      ? `Sort: ${PLAYLIST_SORT_LABELS[playlistTableOptions.sortMode.key]} ${playlistTableOptions.sortMode.direction === "desc" ? "desc" : "asc"}`
      : null,
    playlistTableOptions.groupMode !== "none" ? `Group: ${PLAYLIST_GROUP_LABELS[playlistTableOptions.groupMode]}` : null,
  ].filter((label): label is string => Boolean(label));
  const savePlaylistTableOptions = () => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(PLAYLIST_TABLE_OPTIONS_STORAGE_KEY, JSON.stringify(playlistTableOptions));
    }
    setSavedPlaylistTableOptions(playlistTableOptions);
  };
  const clearPlaylistTableOptions = () => {
    setPlaylistTableOptions(DEFAULT_PLAYLIST_TABLE_OPTIONS);
  };
  const visibleTrackPanelAlbumEntries = displayAlbumTrackEntries;
  const trackIsHiddenByDisc = (track: AlbumTrackEntry) => (
    track.discNumber != null && hiddenAlbumDiscNumbers.has(track.discNumber)
  );
  const playableTrackPanelAlbumEntries = visibleTrackPanelAlbumEntries.filter(
    (track) => !track.familyExclusive && !trackIsHiddenByDisc(track),
  );
  const toggleAlbumDisc = (discNumber: number) => {
    setHiddenAlbumDiscNumbers((current) => {
      const next = new Set(current);
      if (next.has(discNumber)) {
        next.delete(discNumber);
      } else {
        next.add(discNumber);
      }
      return next;
    });
  };
  const selectedReleaseSourceVersion = selectedPreviewReleaseSourceVersions.find(
    (version) => version.spotify_track_id === selectedPreviewCurrentSpotifyTrackId,
  ) ?? selectedPreviewReleaseSourceVersions[0] ?? null;
  const recordingMemberVersionTagLabel = (member: RecordingTrackCandidateMember | null) => {
    if (!member) {
      return "Version";
    }
    const subtitle = variationSubtitleFromTitle(member.title, { allowRemasterOnly: selectedPreviewDetailView === "release" });
    if (subtitle) {
      return subtitle;
    }
    const evidenceTokens = member.evidence?.version_tokens?.filter(Boolean).join(" ");
    if (evidenceTokens) {
      return evidenceTokens;
    }
    const albumDescriptor = versionDescriptorFromText(recordingMemberAlbumName(member));
    if (albumDescriptor) {
      return albumDescriptor;
    }
    return member.release_track_id === selectedPreview?.releaseTrackId ? "Original" : "Version";
  };
  const selectedRecordingVersionMember = selectedPreviewRecordingMembers.find(
    (member) => member.release_track_id === selectedPreview?.releaseTrackId,
  ) ?? selectedPreviewRecordingMembers[0] ?? null;
  const selectedRecordingVersionMembersHaveVariants = selectedPreviewRecordingMembers.some((member) => {
    const label = recordingMemberVersionTagLabel(member).trim().toLocaleLowerCase();
    return Boolean(label && label !== "version" && label !== "original");
  });
  const selectedRecordingVersionMembersForDropdown = selectedRecordingVersionMembersHaveVariants
    ? selectedPreviewRecordingMembers
    : [];
  const trackPanelDiscNumbers = new Set(
    visibleTrackPanelAlbumEntries
      .map((track) => track.discNumber)
      .filter((discNumber): discNumber is number => discNumber != null && discNumber > 0),
  );
  const trackPanelHasMultipleDiscs = trackPanelDiscNumbers.size > 1;
  const albumPanelDiscNumbers = new Set(
    displayAlbumTrackEntries
      .map((track) => track.discNumber)
      .filter((discNumber): discNumber is number => discNumber != null && discNumber > 0),
  );
  const albumPanelHasMultipleDiscs = albumPanelDiscNumbers.size > 1;
  const playableAlbumEntries = displayAlbumTrackEntries.filter(
    (track) => !track.familyExclusive && !trackIsHiddenByDisc(track),
  );
  useEffect(() => {
    if (albumFamilyDiscScrollTarget == null || albumTrackEntriesLoading) {
      return;
    }
    const discHeader = albumTrackListRef.current?.querySelector<HTMLElement>(
      `.detail-album-track-disc-row[data-disc-number="${albumFamilyDiscScrollTarget}"]`,
    );
    if (!discHeader) {
      clearAlbumFamilyDiscScrollTarget();
      return;
    }
    const frameId = window.requestAnimationFrame(() => {
      discHeader.scrollIntoView({ behavior: "smooth", block: "start" });
      clearAlbumFamilyDiscScrollTarget();
    });
    return () => window.cancelAnimationFrame(frameId);
  }, [
    albumFamilyContext?.selected_spotify_album_id,
    albumFamilyDiscScrollTarget,
    albumTrackEntriesLoading,
    albumTrackListRef,
    clearAlbumFamilyDiscScrollTarget,
    visibleTrackPanelAlbumEntries.length,
  ]);
  const discEditionLabel = (discNumber: number) => {
    const explicitLabel = albumFamilyContext?.disc_labels?.[String(discNumber)];
    if (explicitLabel) {
      return explicitLabel;
    }
    const versions = visibleTrackPanelAlbumEntries
      .filter((track) => track.discNumber === discNumber)
      .flatMap((track) => track.familyAvailableVersions)
      .filter((version, index, all) => all.findIndex((candidate) => candidate.spotify_album_id === version.spotify_album_id) === index)
      .sort((left, right) => (left.total_tracks ?? Number.MAX_SAFE_INTEGER) - (right.total_tracks ?? Number.MAX_SAFE_INTEGER));
    const firstLabel = versions[0]?.label ?? null;
    if (firstLabel && new RegExp(`^(?:disc|disk)\\s+${discNumber}$`, "i").test(firstLabel.trim())) {
      return null;
    }
    return firstLabel && firstLabel !== "Original" ? firstLabel : null;
  };
  const albumVersionSelectorVersions = albumFamilyContext?.versions.map((version) => {
    if (!version.is_selected) {
      return version;
    }
    const selectedRows = displayAlbumTrackEntries.filter((track) => !track.familyExclusive);
    const knownDurationMs = selectedRows.reduce((total, track) => total + Math.max(0, track.durationMs ?? 0), 0);
    const sourceYear = Number(
      selectedReleaseSourceVersion?.album_release_year
      ?? selectedPreview?.sourceTrack?.album_release_year
      ?? selectedPreview?.sourceAlbumYear
      ?? remasterYearFromTrackName(selectedPreviewCanonicalTrackTitle),
    );
    return {
      ...version,
      image_url: version.image_url
        ?? selectedReleaseSourceVersion?.album_image_url
        ?? selectedPreview?.sourceAlbumImage
        ?? selectedPreview?.image
        ?? selectedPreview?.sourceTrack?.image_url
        ?? null,
      release_year: version.release_year ?? (Number.isInteger(sourceYear) && sourceYear > 0 ? sourceYear : null),
      total_tracks: version.total_tracks ?? (selectedRows.length || null),
      total_duration_ms: version.total_duration_ms ?? (knownDurationMs > 0 ? knownDurationMs : null),
    };
  }) ?? [];
  const canShowListenBreakdown = Boolean(
    selectedPreviewDetailView === "recording"
    && selectedPreviewOtherRecordingMembers.length > 0
    && selectedPreviewListenBreakdown
    && selectedPreviewListenBreakdown.otherAlbumsCount > 0,
  );
  const selectedPreviewTrackArtistHeading = selectedPreview?.kind === "track" && selectedPreviewCanOpenArtist ? (
    <div className="detail-modal-track-artist-heading detail-modal-meta-with-image">
      <span className="detail-modal-artist-links">
        {selectedPreviewTrackMainArtists.map((artist, index) => {
          const artistName = artist.name?.trim();
          if (!artistName) {
            return null;
          }
          const artistImageUrl = artist.image_url ?? (index === 0 ? selectedPreviewArtistImageUrl : null);
          return (
            <span className="detail-modal-artist-link-wrap" key={`${artist.artist_id ?? artist.id ?? artistName}-${index}`}>
              {index > 0 ? <span className="detail-modal-artist-separator">, </span> : null}
              {artistImageUrl ? (
                <img alt="" className="detail-modal-inline-artist-image detail-modal-track-heading-artist-image" src={artistImageUrl} />
              ) : (
                <span className="detail-modal-inline-artist-image detail-modal-track-heading-artist-image detail-modal-inline-artist-image-fallback" aria-hidden="true">
                  <span />
                </span>
              )}
              <button
                className="detail-modal-inline-link"
                onClick={() => openSelectedTrackArtistPreview(artist)}
                type="button"
              >
                {artistName}
              </button>
            </span>
          );
        })}
      </span>
    </div>
  ) : null;
  const [listenBreakdownOpen, setListenBreakdownOpen] = useState(false);
  const [dateBreakdownOpen, setDateBreakdownOpen] = useState(false);
  const [recordingVersionDropdownOpen, setRecordingVersionDropdownOpen] = useState(false);
  const [playlistAddOverlayOpen, setPlaylistAddOverlayOpen] = useState(false);
  const [playlistAddSelectedIds, setPlaylistAddSelectedIds] = useState<Set<string>>(new Set());
  const [playlistAddInitialSelectedIds, setPlaylistAddInitialSelectedIds] = useState<Set<string>>(new Set());
  const [playlistAddNewName, setPlaylistAddNewName] = useState("");
  const [playlistAddSearch, setPlaylistAddSearch] = useState("");
  const [playlistAddSortMode, setPlaylistAddSortMode] = useState<PlaylistAddSortMode>("recent");
  const [playlistAddCategoryFilter, setPlaylistAddCategoryFilter] = useState<PlaylistAddCategoryFilter>("active");
  const [playlistCategories, setPlaylistCategories] = useState<PlaylistCategory[]>([]);
  const [playlistAddSaving, setPlaylistAddSaving] = useState(false);
  const [playlistAddError, setPlaylistAddError] = useState<string | null>(null);
  useEffect(() => {
    setRecordingVersionDropdownOpen(false);
    setPlaylistAddOverlayOpen(false);
    setPlaylistAddSelectedIds(new Set());
    setPlaylistAddInitialSelectedIds(new Set());
    setPlaylistAddNewName("");
    setPlaylistAddSearch("");
    setPlaylistAddSortMode("recent");
    setPlaylistAddCategoryFilter("active");
    setPlaylistAddError(null);
  }, [selectedPreview]);
  useEffect(() => {
    if (selectedPreview?.kind !== "track") {
      return;
    }
    const membershipIds = new Set(selectedPreviewPlaylistMemberships.map((membership) => membership.playlist_id));
    setPlaylistAddSelectedIds(membershipIds);
    setPlaylistAddInitialSelectedIds(membershipIds);
    setPlaylistAddError(null);
  }, [playlistAddOverlayOpen, selectedPreviewPlaylistMemberships]);
  useEffect(() => {
    if (selectedPreview?.kind !== "track" || playlistAddRequestNonce <= 0) {
      return;
    }
    setPlaylistAddSortMode("recent");
    setPlaylistAddOverlayOpen(true);
  }, [playlistAddRequestNonce, selectedPreview]);
  useEffect(() => {
    if (!playlistAddOverlayOpen) {
      return;
    }
    let cancelled = false;
    async function loadPlaylistCategories() {
      try {
        const response = await fetch(`${apiBaseUrl}/playlists/categories`, { credentials: "include" });
        if (!response.ok) {
          throw new Error(`Playlist categories failed to load (${response.status}).`);
        }
        const payload = await response.json() as { items?: PlaylistCategory[] };
        if (!cancelled) {
          setPlaylistCategories(Array.isArray(payload.items) ? payload.items : []);
        }
      } catch (error) {
        console.error(error);
        if (!cancelled) {
          setPlaylistCategories([]);
        }
      }
    }
    void loadPlaylistCategories();
    return () => {
      cancelled = true;
    };
  }, [apiBaseUrl, selectedPreview?.kind]);
  const handleModalClick = (event: MouseEvent<HTMLElement>) => {
    event.stopPropagation();
    if (!detailOptionsOpen && !listenBreakdownOpen && !dateBreakdownOpen && !recordingVersionDropdownOpen && !playlistAddOverlayOpen) {
      return;
    }
    const target = event.target;
    if (target instanceof Element && target.closest(".detail-modal-options")) {
      return;
    }
    if (target instanceof Element && target.closest(".detail-modal-recording-version-selector")) {
      return;
    }
    if (target instanceof Element && target.closest(".detail-modal-playlist-add-overlay")) {
      return;
    }
    if (target instanceof Element && target.closest(".detail-track-action-listen-popover-wrap")) {
      return;
    }
    setDetailOptionsOpen(false);
    setListenBreakdownOpen(false);
    setDateBreakdownOpen(false);
    setRecordingVersionDropdownOpen(false);
    setPlaylistAddOverlayOpen(false);
  };
  const existingPlaylistMembershipIds = new Set(selectedPreviewPlaylistMemberships.map((membership) => membership.playlist_id));
  const archiveCategoryIds = playlistCategories
    .filter((category) => category.name.trim().toLocaleLowerCase() === "archive")
    .map((category) => category.id);
  const playlistCategoryNames = (playlistId: string | null | undefined) => {
    if (!playlistId) {
      return [];
    }
    return playlistCategories
      .filter((category) => category.playlistIds.includes(playlistId))
      .map((category) => category.name)
      .filter(Boolean);
  };
  const playlistAddSearchQuery = playlistAddSearch.trim().toLocaleLowerCase();
  const availablePlaylistOptions = selectedPreviewAvailablePlaylists
    .filter((playlist) => Boolean(playlist.playlist_id))
    .filter((playlist) => Boolean(playlist.is_owned || playlist.is_collaborative))
    .filter((playlist) => {
      const playlistId = playlist.playlist_id ?? "";
      const categoryIds = playlistCategories
        .filter((category) => category.playlistIds.includes(playlistId))
        .map((category) => category.id);
      if (playlistAddCategoryFilter === "all") {
        return true;
      }
      if (playlistAddCategoryFilter === "none") {
        return categoryIds.length === 0;
      }
      if (playlistAddCategoryFilter === "active") {
        return !archiveCategoryIds.some((categoryId) => categoryIds.includes(categoryId));
      }
      return categoryIds.includes(playlistAddCategoryFilter);
    })
    .filter((playlist) => {
      if (!playlistAddSearchQuery) {
        return true;
      }
      return [
        playlist.name,
        playlist.owner_name,
        playlist.description,
      ].some((value) => String(value ?? "").toLocaleLowerCase().includes(playlistAddSearchQuery));
    })
    .map((playlist, index) => ({ playlist, index }))
    .sort((a, b) => {
      if (playlistAddSortMode === "name") {
        return String(a.playlist.name ?? "").localeCompare(String(b.playlist.name ?? ""));
      }
      if (playlistAddSortMode === "tracks") {
        return (b.playlist.track_count ?? -1) - (a.playlist.track_count ?? -1)
          || String(a.playlist.name ?? "").localeCompare(String(b.playlist.name ?? ""));
      }
      if (playlistAddSortMode === "membership") {
        return Number(playlistAddInitialSelectedIds.has(b.playlist.playlist_id ?? "")) - Number(playlistAddInitialSelectedIds.has(a.playlist.playlist_id ?? ""))
          || String(a.playlist.name ?? "").localeCompare(String(b.playlist.name ?? ""));
      }
      return a.index - b.index;
    })
    .map(({ playlist }) => playlist);
  const handlePlaylistAddSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (playlistAddSaving) {
      return;
    }
    const playlistIds = Array.from(playlistAddSelectedIds).filter((playlistId) => !existingPlaylistMembershipIds.has(playlistId));
    const removePlaylistIds = Array.from(existingPlaylistMembershipIds).filter((playlistId) => !playlistAddSelectedIds.has(playlistId));
    const newPlaylistName = playlistAddNewName.trim();
    if (playlistIds.length === 0 && removePlaylistIds.length === 0 && !newPlaylistName) {
      setPlaylistAddError("Choose a playlist or enter a new playlist name.");
      return;
    }
    setPlaylistAddSaving(true);
    setPlaylistAddError(null);
    try {
      await onAddSelectedTrackToPlaylists(playlistIds, removePlaylistIds, newPlaylistName || null);
      setPlaylistAddOverlayOpen(false);
      setPlaylistAddNewName("");
    } catch (error) {
      setPlaylistAddError(error instanceof Error ? error.message : "Could not add this track to playlists.");
    } finally {
      setPlaylistAddSaving(false);
    }
  };
  const selectedPreviewPlaylistMembershipStatusMessage = selectedPreview?.kind === "track" && !selectedPreviewPlaylistMembershipsLoading && selectedPreviewPlaylistMemberships.length === 0
    ? !selectedPreviewPlaylistIndexStatus?.has_playlist_metadata
      ? "Playlist cache is still starting. It will appear here after playlist metadata loads."
      : !selectedPreviewPlaylistIndexStatus.has_track_cache
        ? "Playlist tracks are still being cached. This song will appear here once that background index finishes."
        : !selectedPreviewPlaylistIndexStatus.has_identity_index
          ? "Playlist tracks are cached, but their track identities are still being indexed."
          : "No cached playlist contains this song yet."
    : null;
  return selectedPreview ? (
        <div
          aria-modal="true"
          className="detail-modal-backdrop"
          onClick={() => setSelectedPreview(null)}
          role="dialog"
        >
          <section className={`detail-modal${selectedPreview.kind === "track" ? " detail-modal-track-view" : ""}${selectedPreview.kind === "album" ? " detail-modal-album-view" : ""}${selectedPreview.kind === "playlist" ? " detail-modal-playlist-view" : ""}`} onClick={handleModalClick}>
            {selectedPreview.kind !== "track" && selectedPreview.kind !== "playlist" ? (
              <div className="detail-modal-options">
                <button
                  aria-expanded={detailOptionsOpen}
                  aria-label="Track options"
                  className="detail-modal-options-button"
                  onClick={() => setDetailOptionsOpen((current) => !current)}
                  type="button"
                >
                  <span aria-hidden="true">⚙</span>
                </button>
                {detailOptionsOpen ? (
                  <div className="detail-modal-options-menu">
                    {selectedPreview.url ? (
                      <a
                        className="detail-modal-options-item"
                        href={selectedPreview.url}
                        rel="noreferrer"
                        target="_blank"
                      >
                        Open in Spotify
                      </a>
                    ) : null}
                  </div>
                ) : null}
              </div>
            ) : null}
            {selectedPreview.kind !== "track" && selectedPreview.kind !== "artist" ? (
              <div className="detail-modal-left">
                {selectedPreview.image ? (
                  <img alt={selectedPreview.label} className="detail-modal-image" src={selectedPreview.image} />
                ) : (
                  <div className="detail-modal-image detail-modal-image-fallback" aria-hidden="true">
                    {selectedPreview.fallbackLabel ?? selectedPreview.label.slice(0, 1).toUpperCase()}
                  </div>
                )}
                {selectedPreview.kind === "album" ? (
                  <p className="detail-modal-cover-album-title">{previewAlbumHeading(selectedPreview)}</p>
                ) : null}
              </div>
            ) : null}
            <div className="detail-modal-copy">
              {selectedPreview.kind === "track" && selectedRecordingVersionMembersForDropdown.length > 1 ? (
                <div className="detail-modal-recording-version-selector">
                  <button
                    aria-expanded={recordingVersionDropdownOpen}
                    className="detail-modal-recording-version-button"
                    onClick={() => setRecordingVersionDropdownOpen((current) => !current)}
                    type="button"
                  >
                    <span>{recordingMemberVersionTagLabel(selectedRecordingVersionMember)}</span>
                    <span aria-hidden="true">⌄</span>
                  </button>
                  {recordingVersionDropdownOpen ? (
                    <div className="detail-modal-recording-version-menu">
                      {selectedRecordingVersionMembersForDropdown.map((member) => {
                        const isSelectedRecordingMember = member.release_track_id === selectedPreview.releaseTrackId;
                        const albumImageUrl = recordingMemberAlbumImageUrl(member);
                        return (
                          <button
                            aria-current={isSelectedRecordingMember}
                            className={`detail-modal-recording-version-option${isSelectedRecordingMember ? " detail-modal-recording-version-option-active" : ""}`}
                            key={`recording-version-option-${member.release_track_id}`}
                            onClick={() => {
                              openRecordingCandidateReleaseTrack(member, "recording");
                              setRecordingVersionDropdownOpen(false);
                            }}
                            type="button"
                          >
                            {albumImageUrl ? (
                              <img alt="" src={albumImageUrl} />
                            ) : (
                              <span className="detail-modal-recording-version-fallback" aria-hidden="true">
                                {(recordingMemberAlbumName(member) || member.title || "?").slice(0, 1).toUpperCase()}
                              </span>
                            )}
                            <span className="detail-modal-recording-version-copy">
                              <strong>{recordingMemberVersionTagLabel(member)}</strong>
                              <span>{[recordingMemberReleaseYear(member), recordingMemberAlbumName(member), member.play_count != null ? `${member.play_count} ${member.play_count === 1 ? "listen" : "listens"}` : null].filter(Boolean).join(" · ")}</span>
                            </span>
                          </button>
                        );
                      })}
                    </div>
                  ) : null}
                </div>
              ) : null}
              <h2 className={selectedPreview.kind === "track" ? "detail-modal-track-title" : undefined}>
                {selectedPreview.kind === "artist" ? (
                  selectedPreview.image ? (
                    <img alt="" className="detail-modal-title-artist-image" src={selectedPreview.image} />
                  ) : (
                    <span className="detail-modal-title-artist-image detail-modal-title-artist-image-fallback" aria-hidden="true">
                      {selectedPreview.label.slice(0, 1).toUpperCase()}
                    </span>
                  )
                ) : null}
                {(
                  (selectedPreview.kind !== "track" && selectedPreview.kind !== "playlist" && selectedPreviewIsKnownLiked)
                  || (selectedPreview.kind === "playlist" && selectedPreviewPlaylistOwnerFollowedByYou)
                ) ? <LikedBadge className="detail-liked-badge" /> : null}
                {selectedPreview.kind !== "track" && selectedPreviewHasReleaseSibling ? (
                  <ReleaseSiblingBadge
                    className="detail-release-sibling-badge"
                    sourceCount={selectedPreviewReleaseSiblingSourceCount}
                    duplicateSourceCount={selectedPreview.releaseTrackDuplicateSourceCount}
                    clusterCandidateType={selectedPreview.releaseTrackClusterCandidateType}
                    clusterRelationshipKind={selectedPreview.releaseTrackClusterRelationshipKind}
                  />
                ) : null}
                <span className={selectedPreview.kind === "track" ? "detail-modal-title-scroll" : undefined}>
                {selectedPreviewIsSharedArtistPage ? (
                  <span className="detail-modal-artist-links">
                    {selectedPreviewArtists.map((artist, index) => {
                      const artistName = artist.name?.trim();
                      if (!artistName) {
                        return null;
                      }
                      return (
                        <span className="detail-modal-artist-link-wrap" key={`${artist.artist_id ?? artist.id ?? artistName}-${index}`}>
                          {index > 0 ? <span className="detail-modal-artist-separator">, </span> : null}
                          <button
                            className="detail-modal-inline-link"
                            onClick={() => openSelectedArtistMemberPreview(artist)}
                            type="button"
                          >
                            {artistName}
                          </button>
                        </span>
                      );
                    })}
                  </span>
                ) : selectedPreview.kind === "track" && selectedPreviewCanonicalTrackTitle
                  ? displayTrackName(selectedPreviewCanonicalTrackTitle)
                  : selectedPreview.label}
                </span>
              </h2>
              {selectedPreview.kind === "playlist" ? (
                <div className="detail-modal-playlist-info">
                  {selectedPreview.detail ? (
                    <p className="detail-modal-playlist-description">{selectedPreview.detail}</p>
                  ) : null}
                  <div className="detail-modal-playlist-info-stats" aria-label="Playlist totals">
                    <span>{playlistHeaderSummary.totalCount.toLocaleString()} tracks</span>
                    <span>
                      {playlistHeaderSummary.albumCount.toLocaleString()} {playlistHeaderSummary.partial ? "loaded " : ""}albums
                    </span>
                    <span>
                      {playlistHeaderSummary.artistCount.toLocaleString()} {playlistHeaderSummary.partial ? "loaded " : ""}artists
                    </span>
                  </div>
                </div>
              ) : null}
              {selectedPreview.kind === "playlist" && !playlistTrackAccessDenied ? (
                <div className="detail-modal-playlist-header-summary" aria-label="Playlist summary">
                  {hasPremiumPlayback ? (
                    <PlaybackActionMenu
                      ariaLabel="Playlist playback options"
                      buttonClassName="detail-album-play-all-button detail-playlist-header-play-all-button"
                      disabled={playlistTrackEntries.every((track) => !trackUriWithFallback(track.uri, track.track_id))}
                      placement="adjacent"
                      onAction={handlePlaylistPlayAll}
                    >
                      Play all
                    </PlaybackActionMenu>
                  ) : null}
                  <div className="detail-modal-playlist-table-status" aria-label="Playlist table options">
                    <span className="detail-modal-playlist-table-shown">
                      {(playlistTableShownCount ?? playlistTrackEntries.length).toLocaleString()} shown
                    </span>
                    {playlistTableOptionLabels.length > 1 ? (
                      <span className="detail-modal-playlist-table-options">
                        {playlistTableOptionLabels.filter((label) => label !== "Merged recordings").join(" · ")}
                      </span>
                    ) : <span aria-hidden="true" />}
                    <span className="detail-modal-playlist-table-actions">
                      <button
                        className={`detail-modal-playlist-table-action${playlistTracksAreMerged ? " detail-modal-playlist-table-action-active" : ""}`}
                        disabled={!playlistTracksCanMerge}
                        onClick={() => setPlaylistTrackMergeView((current) => current === "merged" ? "individual" : "merged")}
                        title={playlistTracksCanMerge ? undefined : "Release-track view and album grouping show individual playlist rows."}
                        type="button"
                      >
                        {playlistTracksAreMerged ? "Show individual" : "Show merged"}
                      </button>
                      <button
                        className="detail-modal-playlist-table-action"
                        disabled={!playlistTableHasUnsavedOptions}
                        onClick={savePlaylistTableOptions}
                        type="button"
                      >
                        Save settings
                      </button>
                      <button
                        className="detail-modal-playlist-table-action"
                        disabled={!playlistTableHasActiveOptions}
                        onClick={clearPlaylistTableOptions}
                        type="button"
                      >
                        Show all
                      </button>
                    </span>
                  </div>
                </div>
              ) : null}
              {selectedPreview.kind === "artist" && selectedPreviewArtistFollowStatusKnown && selectedPreviewArtistIsSpotifyFollowed ? (
                <button
                  aria-label="Followed artist on Spotify"
                  aria-pressed="true"
                  className="secondary-button detail-track-action-button detail-track-star-button detail-artist-star-button detail-track-action-button-active"
                  title="Followed on Spotify."
                  type="button"
                >
                  <span aria-hidden="true">★</span>
                </button>
              ) : null}
              {hasPremiumPlayback && selectedPreview.kind === "track" && selectedPreviewPlaybackTrackUri ? (
                <div className="detail-modal-play-menu detail-modal-play-menu-inline">
                  <PlaybackActionMenu
                    ariaLabel={isTrackPlaying(selectedPreviewPlaybackTrackUri) ? "Currently playing in ListenLab" : `Play ${selectedPreview.label} in ListenLab`}
                    buttonClassName={`secondary-button detail-track-play-menu-button${isTrackPlaying(selectedPreviewPlaybackTrackUri) ? " detail-icon-button-playing" : ""}`}
                    isPlaying={isTrackPlaying(selectedPreviewPlaybackTrackUri)}
                    placement="overlay-trigger"
                    onAction={(action) => {
                      const albumQueue = buildAlbumPlaybackQueue(selectedPreviewPlaybackTrackUri, playableTrackPanelAlbumEntries);
                      return handlePlaybackAction(action, {
                        trackUri: selectedPreviewPlaybackTrackUri,
                        optimisticTrack: selectedPreviewTrackOptimisticSummary,
                        queueCursor: albumQueue?.queueCursor,
                        queueContext: albumQueue?.queueContext,
                        queuePlaylistUris: albumQueue?.playlistUris,
                        queueTracks: albumQueue?.queueTracks,
                        sourceTrack: selectedPreview?.sourceTrack ?? null,
                      });
                    }}
                  >
                    {isTrackPlaying(selectedPreviewPlaybackTrackUri) ? (
                      <span className="detail-wave-icon" aria-hidden="true">
                        <span />
                        <span />
                        <span />
                      </span>
                    ) : (
                      <span className="detail-play-icon" aria-hidden="true">{"\u25B6"}</span>
                    )}
                    <span>{selectedPreviewTrackDurationLabel ?? "?:??"}</span>
                  </PlaybackActionMenu>
                  <button
                    aria-label={selectedPreviewIsBookmarked ? "Remove bookmark" : "Bookmark"}
                    aria-pressed={selectedPreviewIsBookmarked}
                    className={`secondary-button detail-track-action-button detail-track-bookmark-button${selectedPreviewIsBookmarked ? " detail-track-action-button-active" : ""}`}
                    onClick={toggleSelectedPreviewTrackBookmark}
                    title={selectedPreviewIsBookmarked ? "Saved for later locally. Click to remove bookmark." : "Save for later locally."}
                    type="button"
                  >
                    <svg aria-hidden="true" viewBox="0 0 20 20">
                      <path d="M5 3.5h10v13l-5-3.2-5 3.2v-13Z" />
                    </svg>
                  </button>
                  <button
                    aria-label={selectedPreviewIsKnownLiked ? "Liked song" : "Not liked"}
                    aria-pressed={selectedPreviewIsKnownLiked}
                    className={`secondary-button detail-track-action-button detail-track-star-button${selectedPreviewIsKnownLiked ? " detail-track-action-button-active" : ""}`}
                    onClick={() => {
                      if (!selectedPreviewStarTrackId) {
                        return;
                      }
                      setLocalStarredTrackById((current) => ({
                        ...current,
                        [selectedPreviewStarTrackId]: !selectedPreviewIsKnownLiked,
                      }));
                    }}
                    title={selectedPreviewIsKnownLiked ? "Liked locally. Click to unstar." : "Not liked locally. Click to star."}
                    type="button"
                  >
                    <span aria-hidden="true">{selectedPreviewIsKnownLiked ? "★" : "☆"}</span>
                  </button>
                </div>
              ) : null}
              {selectedPreview.kind === "track" ? selectedPreviewTrackArtistHeading : null}
              {selectedPreview.kind !== "track" ? (
                <button
                  aria-label={selectedPreviewIsEntityBookmarked ? `Remove ${selectedPreview.kind} bookmark` : `Bookmark ${selectedPreview.kind}`}
                  aria-pressed={selectedPreviewIsEntityBookmarked}
                  className={`secondary-button detail-track-action-button detail-track-bookmark-button detail-entity-bookmark-button${selectedPreviewIsEntityBookmarked ? " detail-track-action-button-active" : ""}`}
                  onClick={toggleSelectedPreviewEntityBookmark}
                  title={selectedPreviewIsEntityBookmarked ? "Saved for later locally. Click to remove bookmark." : "Save for later locally."}
                  type="button"
                >
                  <svg aria-hidden="true" viewBox="0 0 20 20">
                    <path d="M5 3.5h10v13l-5-3.2-5 3.2v-13Z" />
                  </svg>
                </button>
              ) : null}
              {selectedPreview.kind === "album" ? (
                <div className="detail-modal-album-meta-block">
                  {selectedPreview.detail ? (
                    <span className="detail-modal-meta-text detail-modal-album-release-year">{selectedPreview.detail}</span>
                  ) : null}
                  {selectedPreviewAlbumSummary ? (
                    <span className="detail-modal-meta-text detail-modal-album-summary">{selectedPreviewAlbumSummary}</span>
                  ) : null}
                  {selectedPreviewAlbumMainArtists.length > 0 ? (
                    <div className="detail-modal-meta detail-modal-meta-with-image">
                      <span className="detail-modal-artist-links detail-modal-meta-text">
                        {selectedPreviewAlbumMainArtists.map((artist, index) => {
                          const artistName = artist.name?.trim();
                          if (!artistName) {
                            return null;
                          }
                          const artistImageUrl = artist.image_url ?? (index === 0 ? selectedPreviewArtistImageUrl : null);
                          return (
                            <span className="detail-modal-artist-link-wrap" key={`${artist.artist_id ?? artist.id ?? artistName}-${index}`}>
                              {index > 0 ? <span className="detail-modal-artist-separator">, </span> : null}
                              {artistImageUrl ? <img alt="" className="detail-modal-inline-artist-image" src={artistImageUrl} /> : null}
                              <button
                                className="detail-modal-inline-link"
                                onClick={() => openSelectedAlbumArtistPreview(artist)}
                                type="button"
                              >
                                {artistName}
                              </button>
                            </span>
                          );
                        })}
                      </span>
                    </div>
                  ) : null}
                  {selectedPreviewAlbumSpotifyId ? (
                    <button
                      aria-label={selectedPreviewAlbumIsSpotifyLiked ? "Saved album on Spotify" : "Album not saved on Spotify"}
                      aria-pressed={selectedPreviewAlbumIsSpotifyLiked}
                      className={`secondary-button detail-track-action-button detail-track-star-button detail-album-star-button${selectedPreviewAlbumIsSpotifyLiked ? " detail-track-action-button-active" : ""}`}
                      onClick={() => {
                        setLocalStarredAlbumById((current) => ({
                          ...current,
                          [selectedPreviewAlbumSpotifyId]: !selectedPreviewAlbumIsSpotifyLiked,
                        }));
                      }}
                      title={selectedPreviewAlbumIsSpotifyLiked ? "Saved in Spotify library." : "Not saved in Spotify library."}
                      type="button"
                    >
                      <span aria-hidden="true">{selectedPreviewAlbumIsSpotifyLiked ? "★" : "☆"}</span>
                    </button>
                  ) : null}
                  {selectedPreviewAlbumGuestArtists.length > 0 ? (
                    <p className="detail-modal-with-artists">
                      <span>with </span>
                      <span className="detail-modal-artist-links">
                        {selectedPreviewAlbumGuestArtists.map((artist, index) => {
                          const artistName = artist.name?.trim();
                          if (!artistName) {
                            return null;
                          }
                          return (
                            <span className="detail-modal-artist-link-wrap" key={`${artist.artist_id ?? artist.id ?? artistName}-${index}`}>
                              {index > 0 ? <span className="detail-modal-artist-separator">, </span> : null}
                              {artist.image_url ? <img alt="" className="detail-modal-inline-artist-image" src={artist.image_url} /> : null}
                              <button
                                className="detail-modal-inline-link"
                                onClick={() => openSelectedAlbumArtistPreview(artist)}
                                onMouseEnter={() => scheduleAlbumWithArtistHighlight(artistName)}
                                onMouseLeave={clearAlbumWithArtistHighlight}
                                onFocus={() => scheduleAlbumWithArtistHighlight(artistName)}
                                onBlur={clearAlbumWithArtistHighlight}
                                type="button"
                              >
                                {artistName}
                              </button>
                            </span>
                          );
                        })}
                      </span>
                    </p>
                  ) : null}
                </div>
              ) : selectedPreview.kind === "track" && selectedPreviewCanOpenArtist && selectedPreviewTrackGuestArtists.length > 0 ? (
                <div className="detail-modal-album-meta-block">
                  <p className="detail-modal-with-artists">
                    <span>with </span>
                    <span className="detail-modal-artist-links">
                      {selectedPreviewTrackGuestArtists.map((artist, index) => {
                        const artistName = artist.name?.trim();
                        if (!artistName) {
                          return null;
                        }
                        return (
                          <span className="detail-modal-artist-link-wrap" key={`${artist.artist_id ?? artist.id ?? artistName}-${index}`}>
                            {index > 0 ? <span className="detail-modal-artist-separator">, </span> : null}
                            {artist.image_url ? <img alt="" className="detail-modal-inline-artist-image" src={artist.image_url} /> : null}
                            <button
                              className="detail-modal-inline-link"
                              onClick={() => openSelectedTrackArtistPreview(artist)}
                              type="button"
                            >
                              {artistName}
                            </button>
                          </span>
                        );
                      })}
                    </span>
                  </p>
                </div>
              ) : selectedPreview.meta && selectedPreview.kind !== "playlist" && !(selectedPreview.kind === "track" && selectedPreviewCanOpenArtist) ? (
                <div className="detail-modal-meta detail-modal-meta-with-image">
                  {selectedPreviewCanOpenArtist ? (
                    <span className="detail-modal-artist-links detail-modal-meta-text">
                      {selectedPreviewArtists.map((artist, index) => {
                        const artistName = artist.name?.trim();
                        if (!artistName) {
                          return null;
                        }
                        return (
                          <span className="detail-modal-artist-link-wrap" key={`${artist.artist_id ?? artist.id ?? artistName}-${index}`}>
                            {index > 0 ? <span className="detail-modal-artist-separator">, </span> : null}
                            <button
                              className="detail-modal-inline-link"
                              onClick={() => {
                                if (selectedPreview.kind === "album") {
                                  openSelectedAlbumArtistPreview(artist);
                                  return;
                                }
                                openSelectedTrackArtistPreview(artist);
                              }}
                              type="button"
                            >
                              {artistName}
                            </button>
                          </span>
                        );
                      })}
                    </span>
                  ) : (
                    <span className="detail-modal-meta-text">{selectedPreview.meta}</span>
                  )}
                </div>
              ) : null}
              {selectedPreview.kind === "track" && !selectedPreviewReleaseTrackDetailReady && selectedPreviewReleaseTrackDetailError ? (
                <p className="detail-modal-release-note">{selectedPreviewReleaseTrackDetailError}</p>
              ) : null}
              {selectedPreview.kind === "track" && selectedPreviewRecordingCandidateError ? (
                <p className="detail-modal-release-note">{selectedPreviewRecordingCandidateError}</p>
              ) : null}
              {selectedPreview.detail && selectedPreview.kind !== "track" && selectedPreview.kind !== "album" && selectedPreview.kind !== "playlist" ? <p className="detail-modal-detail">{selectedPreview.detail}</p> : null}
              {selectedPreview.kind === "track" && !selectedPreviewPlaybackTrackUri ? (
                <p className="detail-modal-preview-missing">This track does not have a playable Spotify URI.</p>
              ) : null}
              {selectedPreview.kind === "artist" ? (
                <>
                  {renderSelectedPreviewArtistTrackSection()}
                  {selectedPreviewIsSharedArtistPage || !backendSelectedPreviewArtistAlbums ? (
                    renderSelectedPreviewArtistAlbumSection(
                      "Albums",
                      selectedPreviewArtistAlbumsForDisplay,
                    )
                  ) : (
                    <>
                      {renderSelectedPreviewArtistAlbumSection(
                        "Albums",
                        selectedPreviewPrimaryArtistAlbums,
                      )}
                      {renderSelectedPreviewArtistAlbumSection(
                        "Appears on",
                        selectedPreviewAppearsOnAlbums,
                      )}
                    </>
                  )}
                </>
              ) : null}
            </div>
            {selectedPreviewIsTrack || selectedPreview.kind === "album" || selectedPreview.kind === "playlist" ? (
              <div className="detail-modal-track-scroll-area">
            {selectedPreview.kind === "track" && selectedPreviewDetailView === "recording" && selectedPreviewDisplayRelationRows.songFamily.length > 0 ? (
              <div className="detail-modal-recording-variations">
                <div className="detail-modal-recording-variations-header">
                  <span>Song Family</span>
                </div>
                <TrackRelationList
                  albumNameForMember={recordingMemberAlbumName}
                  albumImageForMember={recordingMemberAlbumImageUrl}
                  onOpenRelatedTrack={(member) => openRecordingCandidateReleaseTrack(member, "recording")}
                  relatedTracks={selectedPreviewDisplayRelationRows.songFamily}
                  releaseYearForMember={recordingMemberReleaseYear}
                />
              </div>
            ) : null}
            {selectedPreview.kind === "track" ? (
              <div className="detail-modal-track-album-panel">
                <div className="detail-modal-track-album-panel-heading">
                  {selectedPreviewCanOpenAlbum ? (
                    <button
                      className="detail-modal-inline-link detail-modal-cover-album-title detail-modal-track-album-panel-title"
                      onClick={openSelectedTrackAlbumPreview}
                      type="button"
                    >
                      {albumFamilyContext
                        ? `${albumFamilyContext.versions.find((version) => version.is_selected)?.release_year ?? ""} - ${albumFamilyContext.core_name}`.replace(/^\s*-\s*/, "")
                        : previewAlbumHeading(selectedPreview)}
                    </button>
                  ) : (
                    <p className="detail-modal-cover-album-title detail-modal-track-album-panel-title">
                      {albumFamilyContext
                        ? `${albumFamilyContext.versions.find((version) => version.is_selected)?.release_year ?? ""} - ${albumFamilyContext.core_name}`.replace(/^\s*-\s*/, "")
                        : previewAlbumHeading(selectedPreview)}
                    </p>
                  )}
                  {albumFamilyContext && (
                    albumFamilyContext.versions.length > 1
                    || albumFamilyContext.versions.some((version) => version.is_selected && version.label !== "Original")
                  ) ? (
                    <AlbumVersionSelector
                      onSelect={switchSelectedTrackAlbumVersion}
                      selectedSpotifyAlbumId={albumFamilyContext.selected_spotify_album_id}
                      versions={albumVersionSelectorVersions}
                    />
                  ) : null}
                </div>
                              <div className={`detail-modal-album-tracks detail-modal-album-tracks-full${selectedPreview.kind === "track" ? " detail-modal-album-tracks-track detail-modal-album-tracks-no-with" : ""}${selectedPreview.kind !== "track" && selectedPreviewAlbumHasGuestArtists ? "" : " detail-modal-album-tracks-no-with"}`}>
                                <div className="detail-modal-album-header">
                                  {hasPremiumPlayback ? (
                                    <PlaybackActionMenu
                                      ariaLabel="Album playback options"
                                      buttonClassName="detail-album-play-all-button"
                                      placement={selectedPreview.kind === "track" ? "overlay-trigger" : "adjacent"}
                                      onAction={(action) => handleAlbumPlayAll(action, playableTrackPanelAlbumEntries)}
                                    >
                                      Play all
                                    </PlaybackActionMenu>
                                  ) : (
                                    <span aria-hidden="true" />
                                  )}
                                  {albumFamilyContext ? (
                                    <AlbumVersionTrackSummary
                                      context={albumFamilyContext}
                                      entries={albumTrackEntries}
                                    />
                                  ) : (
                                    <span className="detail-modal-album-title-header">{albumTracklistSummaryLabel(albumTrackEntries)}</span>
                                  )}
                                  {selectedPreview.kind !== "track" && selectedPreviewAlbumHasGuestArtists ? <span className="detail-modal-album-with-header">With</span> : null}
                                  <span className="detail-modal-album-liked-header">Tags</span>
                                  {selectedPreview.kind === "track" ? <span className="detail-modal-album-listens-header">Listens</span> : null}
                                  {selectedPreview.kind === "track" ? <span className="detail-modal-album-playlists-header">Lists</span> : null}
                                  <span className="detail-modal-album-actions-header">
                                    {selectedPreview.kind !== "track" ? <span className="detail-modal-album-preview-header">Pre</span> : null}
                                    <button
                                      className={`detail-modal-album-last-played-header detail-modal-album-sort-header${albumTrackLastSortMode ? " detail-modal-album-sort-header-active" : ""}`}
                                      onClick={() => setAlbumTrackLastSortMode((current) => nextLastPlayedSortMode(current))}
                                      type="button"
                                    >
                                      Last
                                      {albumTrackLastSortMode ? (
                                        <span aria-hidden="true">{albumTrackLastSortMode === "recent" ? "↓" : "↑"}</span>
                                      ) : null}
                                    </button>
                                  </span>
                                </div>
                                {albumTrackEntriesLoading && albumTrackEntries.length === 0 ? (
                                  <p className="detail-modal-preview-missing">{albumTrackFetchFromSpotifyLoading ? "Fetching Album..." : "Loading Album..."}</p>
                                ) : null}
                                {!albumTrackEntriesLoading && albumTrackEntriesError && !albumTrackMoreOnSpotifyUrl ? (
                                  <p className="detail-modal-preview-missing">{albumTrackEntriesError}</p>
                                ) : null}
                                {!albumTrackEntriesLoading && albumTrackEntriesError && albumTrackMoreOnSpotifyUrl ? (
                                  <p className="detail-modal-preview-missing">
                                    <a href={albumTrackMoreOnSpotifyUrl} rel="noreferrer" target="_blank">More tracks on Spotify</a>
                                  </p>
                                ) : null}
                                {albumTrackEntriesPartial && albumTrackEntries.length > 0 ? (
                                  <p className="detail-modal-album-enrichment-note">
                                    Tracklist is shown from partial local cache. Album details are still being completed.
                                  </p>
                                ) : null}
                                {!albumTrackEntriesError && albumTrackEntries.length > 0 ? (
                                  <div className="detail-album-track-list-wrap">
                                    {selectedAlbumTrackMarkerTop(visibleTrackPanelAlbumEntries) ? (
                                      <span
                                        className="detail-album-track-scroll-marker"
                                        style={{ "--detail-album-track-marker-top": selectedAlbumTrackMarkerTop(visibleTrackPanelAlbumEntries) } as CSSProperties}
                                        aria-hidden="true"
                                      />
                                    ) : null}
                                    <ul className={`detail-album-track-list${albumTrackEntriesLoading ? " detail-album-track-list-updating" : ""}`} ref={albumTrackListRef}>
                                      {visibleTrackPanelAlbumEntries.map((track, trackIndex) => {
                                      const rowTrackUri = track.uri ?? (track.id ? `spotify:track:${track.id}` : null);
                                      const rowIsCurrentTrack = Boolean(rowTrackUri && currentTrack?.uri === rowTrackUri);
                                      const rowPlaying = isTrackPlaying(rowTrackUri);
                                      const rowPreviewPlaying = Boolean(rowTrackUri && previewingTrackUri === rowTrackUri);
                                      const rowPreviewActive = Boolean(rowPreviewPlaying && rowPlaying);
                                      const rowPreviewKey = albumTrackPreviewKey(track, rowTrackUri);
                                      const rowPreviewPlayed = previewPlayedTrackKeys.has(rowPreviewKey);
                                      const rowPausedCurrent = Boolean(rowIsCurrentTrack && playbackPaused);
                                      const rowHistoryLastPlayedAt = selectedPreviewDetailView === "release"
                                        ? track.sourceLastPlayedAt === undefined ? track.lastPlayedAt : track.sourceLastPlayedAt
                                        : track.recordingLastPlayedAt === undefined ? track.lastPlayedAt : track.recordingLastPlayedAt;
                                      const rowHistoryPlayCount = selectedPreviewDetailView === "release"
                                        ? track.sourcePlayCount === undefined ? track.playCount : track.sourcePlayCount
                                        : track.recordingPlayCount === undefined ? track.playCount : track.recordingPlayCount;
                                      const rowPlaylistCount = selectedPreviewDetailView === "release"
                                        ? track.sourcePlaylistCount === undefined ? 0 : track.sourcePlaylistCount
                                        : track.recordingPlaylistCount === undefined ? (track.sourcePlaylistCount ?? 0) : track.recordingPlaylistCount;
                                      const rowLastPlayed = formatCompactRelativeAge(rowHistoryLastPlayedAt);
                                      const rowIsUnlistened = !rowHistoryLastPlayedAt && rowHistoryPlayCount <= 0;
                                      const rowRelationTagsResult = trackRelationTags({
                                        releaseTrackDuplicateSourceCount: track.releaseTrackDuplicateSourceCount,
                                        releaseTrackSourceCount: track.releaseTrackSourceCount,
                                        hasReleaseTrackSiblings: track.hasReleaseTrackSiblings,
                                        releaseTrackClusterCandidateType: track.releaseTrackClusterCandidateType,
                                        releaseTrackClusterRelationshipKind: track.releaseTrackClusterRelationshipKind,
                                        hasEditionRelation: track.familyHasEditionRelation,
                                        hasExternalRecordingRelation: track.familyHasExternalRecordingRelation,
                                      });
                                      const rowRelationTags = rowRelationTagsResult.text;
                                      const rowRelationTagsTitle = rowRelationTagsResult.title;
                                      const rowStarTrackId = track.id ?? spotifyTrackIdFromUri(rowTrackUri);
                                      const rowMatchesSelectedReleaseTrack = Boolean(
                                        selectedPreview.kind === "track"
                                        && (
                                          (track.releaseTrackId != null && track.releaseTrackId === selectedPreview.releaseTrackId)
                                          || (rowStarTrackId && selectedPreviewReleaseTrackDetailReady?.source_versions.some((version) => version.spotify_track_id === rowStarTrackId))
                                        ),
                                      );
                                      const rowIsLiked = rowStarTrackId && rowStarTrackId in localStarredTrackById
                                        ? localStarredTrackById[rowStarTrackId]
                                        : selectedPreview.kind === "track"
                                          ? selectedPreviewDetailView === "release"
                                            ? albumTrackIsExactKnownLiked(track)
                                            : (rowMatchesSelectedReleaseTrack && selectedPreviewIsKnownLiked) || albumTrackIsExactKnownLiked(track)
                                          : albumTrackIsKnownLiked(track);
                                      const mainArtistNames = new Set(
                                        selectedPreview.kind === "album" || selectedPreview.kind === "track"
                                          ? selectedPreviewAlbumMainArtists.map((artist) => artist.name?.trim().toLocaleLowerCase()).filter(Boolean)
                                          : [],
                                      );
                                      const rowWithArtists = selectedPreview.kind === "album" || selectedPreview.kind === "track"
                                        ? artistEntriesForAlbumTrack(track).filter((artist) => {
                                          const artistName = artist.name?.trim().toLocaleLowerCase();
                                          return Boolean(artistName && !mainArtistNames.has(artistName));
                                        })
                                        : [];
                                      const rowHasDuplicateTitle = displayAlbumTrackEntries.some((candidate) => (
                                        candidate !== track
                                        && candidate.name.trim().toLocaleLowerCase() === track.name.trim().toLocaleLowerCase()
                                      ));
                                      const rowGuestArtistNames = rowWithArtists
                                        .map((artist) => artist.name?.trim())
                                        .filter((name): name is string => Boolean(name));
                                      const trackDisplayName = displayTrackName(track.name);
                                      const rowDisplayName = selectedPreview.kind === "track"
                                        && rowHasDuplicateTitle
                                        && rowGuestArtistNames.length > 0
                                        ? `${trackDisplayName} - with ${rowGuestArtistNames.join(", ")}`
                                        : trackDisplayName;
                                      const rowMatchesHighlightedArtist = Boolean(
                                        selectedPreview.kind === "album"
                                        && selectedPreview.albumHighlightArtistNames?.some((artistName) => artistNameMatches(track.artistName, artistName)),
                                      );
                                      const rowMatchesHoveredWithArtist = Boolean(
                                        hoveredAlbumWithArtistName && artistNameMatches(track.artistName, hoveredAlbumWithArtistName),
                                      );
                                      const rowBaseDurationMs = (
                                        track.durationMs
                                        ?? (rowIsCurrentTrack
                                          ? (playbackDurationMs > 0 ? playbackDurationMs : currentTrack?.durationMs ?? null)
                                          : null)
                                      );
                                      const rowElapsedMs = rowIsCurrentTrack
                                        ? (
                                          rowBaseDurationMs != null
                                            ? Math.min(Math.max(0, playbackPositionMs), rowBaseDurationMs)
                                            : Math.max(0, playbackPositionMs)
                                        )
                                        : null;
                                      const rowButtonTimeMs = rowIsCurrentTrack
                                        ? (
                                          rowPlaying
                                            ? rowElapsedMs
                                            : (rowPausedCurrent ? (pausedTimeFlashOn ? rowElapsedMs : rowBaseDurationMs) : rowBaseDurationMs)
                                        )
                                        : rowBaseDurationMs;
                                      const rowFamilyAvailabilityLabel = track.familySwitchLabel
                                        ?? track.familyAvailableVersions[0]?.label
                                        ?? "another edition";
                                      const rowFamilyTitle = track.familyExclusive
                                        ? `Switch to include these tracks from ${rowFamilyAvailabilityLabel}.`
                                        : trackIsHiddenByDisc(track) && track.discNumber != null
                                          ? `Disc ${track.discNumber} is hidden from playback.`
                                          : undefined;
                                      const rowExcludedFromPlayback = track.familyExclusive || trackIsHiddenByDisc(track);
                                      return (
                                        <Fragment key={track.id ?? track.name}>
                                          {trackPanelHasMultipleDiscs
                                            && track.discNumber != null
                                            && visibleTrackPanelAlbumEntries.findIndex((candidate) => candidate.discNumber === track.discNumber) === trackIndex ? (
                                              <li className="detail-album-track-disc-row" data-disc-number={track.discNumber}>
                                                <span>
                                                  Disc {track.discNumber}
                                                  {discEditionLabel(track.discNumber) ? ` - ${discEditionLabel(track.discNumber)}` : ""}
                                                </span>
                                                <button
                                                  className="detail-album-track-disc-toggle"
                                                  onClick={() => toggleAlbumDisc(track.discNumber!)}
                                                  type="button"
                                                >
                                                  {hiddenAlbumDiscNumbers.has(track.discNumber) ? "Show" : "Hide"}
                                                </button>
                                              </li>
                                            ) : null}
                                          <li
                                            className={`detail-album-track-row${track.isSelected ? " detail-album-track-row-selected" : ""}${rowMatchesHighlightedArtist || rowMatchesHoveredWithArtist ? " detail-album-track-row-artist-highlighted" : ""}${rowExcludedFromPlayback ? " detail-album-track-row-family-exclusive" : ""}`}
                                            title={rowFamilyTitle}
                                          >
                                            {hasPremiumPlayback ? (
                                              <PlaybackActionMenu
                                              ariaLabel={rowPlaying ? "Currently playing in ListenLab" : rowTrackUri ? `Play ${track.name} in ListenLab` : `${track.name} is not playable`}
                                              buttonClassName={`secondary-button detail-album-track-play-button${rowPlaying ? " detail-icon-button-playing" : ""}`}
                                              disabled={!rowTrackUri || rowExcludedFromPlayback}
                                              isPlaying={rowPlaying}
                                              placement="overlay-trigger"
                                              onAction={(action) => {
                                                const albumQueue = buildAlbumPlaybackQueue(rowTrackUri, playableTrackPanelAlbumEntries);
                                                return handlePlaybackAction(action, {
                                                  trackUri: rowTrackUri,
                                                  optimisticTrack: playerSummaryFromAlbumTrack(track),
                                                  queueCursor: albumQueue?.queueCursor,
                                                  queueContext: albumQueue?.queueContext,
                                                  queuePlaylistUris: albumQueue?.playlistUris,
                                                  queueTracks: albumQueue?.queueTracks,
                                                  sourceTrack: track.sourceTrack,
                                                }).then(() => {
                                                  if (action === "play_now") {
                                                    openAlbumTrackPreview(track);
                                                  }
                                                });
                                              }}
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
                                              <span className={`detail-album-track-play-time${rowPausedCurrent ? " detail-album-track-play-time-flash" : ""}`}>
                                                {rowButtonTimeMs != null ? formatPlaybackClock(rowButtonTimeMs) : "?:??"}
                                              </span>
                                            </PlaybackActionMenu>
                                          ) : <span aria-hidden="true" />}
                                          <button
                                            className="detail-album-track-name-button single-line-ellipsis"
                                            onClick={() => {
                                              if (track.familyExclusive && track.familySwitchAlbumId) {
                                                includeAlbumFamilyTracks(track.familySwitchAlbumId);
                                                return;
                                              }
                                              if (trackIsHiddenByDisc(track) && track.discNumber != null) {
                                                toggleAlbumDisc(track.discNumber);
                                                return;
                                              }
                                              openAlbumTrackPreview(track);
                                            }}
                                            type="button"
                                          >
                                            <span className="single-line-ellipsis">{rowDisplayName}</span>
                                          </button>
                                          {selectedPreview.kind !== "track" && selectedPreviewAlbumHasGuestArtists ? (
                                            <span className="detail-album-track-with single-line-ellipsis">
                                              {rowWithArtists.map((artist, index) => {
                                                const artistName = artist.name?.trim();
                                                if (!artistName) {
                                                  return null;
                                                }
                                                return (
                                                  <span className="detail-modal-artist-link-wrap" key={`${artist.artist_id ?? artist.id ?? artistName}-${index}`}>
                                                    {index > 0 ? <span className="detail-modal-artist-separator">, </span> : null}
                                                    <button
                                                      className="detail-modal-inline-link"
                                                      onClick={() => openAlbumWithArtistPreview(artist)}
                                                      type="button"
                                                    >
                                                      {artistName}
                                                    </button>
                                                  </span>
                                                );
                                              })}
                                            </span>
                                          ) : null}
                                          <span className="detail-album-track-liked-cell">
                                            <span className="detail-album-track-badges">
                                              {rowIsLiked ? <LikedBadge className="detail-album-track-liked-badge" /> : null}
                                              {rowRelationTags ? (
                                                <span
                                                  className="relation-tags-badge detail-album-track-relation-badge"
                                                  title={rowRelationTagsTitle}
                                                  aria-label={rowRelationTagsTitle}
                                                >
                                                  {rowRelationTags}
                                                </span>
                                              ) : null}
                                            </span>
                                          </span>
                                          {selectedPreview.kind === "track" ? (
                                            <span className="detail-album-track-listen-count">
                                              {rowHistoryPlayCount > 0 ? rowHistoryPlayCount.toLocaleString() : "-"}
                                            </span>
                                          ) : null}
                                          {selectedPreview.kind === "track" ? (
                                            <span className="detail-album-track-playlist-count">
                                              {rowPlaylistCount > 0 ? rowPlaylistCount.toLocaleString() : "-"}
                                            </span>
                                          ) : null}
                                          <div className="detail-album-track-actions">
                                            {selectedPreview.kind !== "track" && hasPremiumPlayback ? (
                                              <button
                                                aria-label={rowPreviewPlaying ? `Stop preview for ${track.name}` : `Preview ${track.name}`}
                                                className={`detail-album-track-preview-button${rowPreviewActive ? " detail-album-track-preview-button-active" : ""}${rowPreviewPlayed ? " detail-album-track-preview-button-played" : ""}`}
                                                disabled={!rowTrackUri}
                                                onClick={() => {
                                                  void toggleAlbumTrackPreview(track, rowTrackUri);
                                                }}
                                                type="button"
                                              />
                                            ) : selectedPreview.kind !== "track" ? (
                                              <span className="detail-album-track-preview-placeholder" aria-hidden="true" />
                                            ) : null}
                                            {rowLastPlayed ? (
                                              <span className="detail-album-track-last-played">{rowLastPlayed}</span>
                                            ) : rowIsUnlistened ? (
                                              <span className="detail-album-track-last-played">
                                                <NewTrackBadge className="detail-album-track-played-new-badge" />
                                              </span>
                                            ) : (
                                              <span className="detail-album-track-last-played">-</span>
                                            )}
                                          </div>
                                        </li>
                                        {selectedPreview.kind === "track" && rowMatchesSelectedReleaseTrack && (albumTrackFetchFromSpotifyLoading || albumTrackMoreOnSpotifyUrl) ? (
                                          <li className="detail-album-track-fetch-row">
                                            {albumTrackFetchFromSpotifyLoading ? (
                                              <span className="detail-album-track-fetch-status">Fetching Album...</span>
                                            ) : albumTrackMoreOnSpotifyUrl ? (
                                              <a className="detail-album-track-fetch-link" href={albumTrackMoreOnSpotifyUrl} rel="noreferrer" target="_blank">More tracks on Spotify</a>
                                            ) : null}
                                          </li>
                                        ) : null}
                                        </Fragment>
                                      );
                                      })}
                                    </ul>
                                  </div>
                                ) : null}
                              </div>
                              {selectedPreviewDetailView === "recording" && selectedPreviewDisplayRelationRows.recording.length > 0 ? (
                                <details
                                  className="detail-modal-also-appears"
                                  key={`also-appears-${selectedPreview.releaseTrackId ?? selectedPreview.trackId ?? selectedPreview.label}`}
                                >
                                  <summary className="detail-modal-also-appears-bar">
                                    <span>Also Appears On</span>
                                    <span className="detail-modal-also-appears-count">
                                      {appearsOnSummary(selectedPreviewDisplayRelationRows.recording)}
                                    </span>
                                    <span className="detail-modal-also-appears-chevron" aria-hidden="true">⌄</span>
                                  </summary>
                                  <div className="detail-modal-also-appears-content">
                                    <div className="detail-modal-recording-variation-strip">
                                      {selectedPreviewDisplayRelationRows.recording.map((member) => {
                                        const albumImageUrl = recordingMemberAlbumImageUrl(member);
                                        const title = [recordingMemberReleaseYear(member), member.album || "Unknown album"].filter(Boolean).join(" · ");
                                        const subtitle = variationSubtitleFromTitle(member.title);
                                        const releaseType = appearsOnReleaseType(member);
                                        return (
                                          <button
                                            className="detail-modal-recording-variation-cover"
                                            key={`recording-cover-${member.release_track_id}`}
                                            onClick={() => openRecordingCandidateReleaseTrack(member, "recording")}
                                            title={title}
                                            type="button"
                                          >
                                            <span className="detail-modal-recording-variation-art">
                                              {albumImageUrl ? (
                                                <img alt="" src={albumImageUrl} />
                                              ) : (
                                                <span className="detail-modal-recording-variation-fallback" aria-hidden="true">{(member.album || member.title || "?").slice(0, 1).toUpperCase()}</span>
                                              )}
                                            </span>
                                            <span className="detail-modal-recording-variation-copy">
                                              <span className="detail-modal-recording-variation-heading single-line-ellipsis">
                                                <span className="detail-relation-badge">{releaseType}</span>
                                                {subtitle ? <span className="detail-modal-recording-variation-subtitle">{subtitle}</span> : null}
                                              </span>
                                              <span className="detail-modal-recording-variation-album">{member.album || "Unknown album"}</span>
                                              <span className="detail-modal-recording-variation-year">{recordingMemberReleaseYear(member)}</span>
                                            </span>
                                          </button>
                                        );
                                      })}
                                    </div>
                                  </div>
                                </details>
                              ) : null}
                              </div>
            ) : null}
            {selectedPreview.kind === "album" ? (
              <div className={`detail-modal-album-tracks detail-modal-album-tracks-full${selectedPreviewAlbumHasGuestArtists ? "" : " detail-modal-album-tracks-no-with"}`}>
                {albumFamilyContext && (
                  albumFamilyContext.versions.length > 1
                  || albumFamilyContext.versions.some((version) => version.is_selected && version.label !== "Original")
                ) ? (
                  <div className="detail-modal-album-version-heading">
                    <AlbumVersionSelector
                      onSelect={switchSelectedTrackAlbumVersion}
                      selectedSpotifyAlbumId={albumFamilyContext.selected_spotify_album_id}
                      versions={albumFamilyContext.versions}
                    />
                  </div>
                ) : null}
                <div className="detail-modal-album-track-summary-row">
                  {albumFamilyContext ? (
                    <AlbumVersionTrackSummary
                      context={albumFamilyContext}
                      entries={albumTrackEntries}
                    />
                  ) : (
                    <span className="detail-modal-album-title-header">{albumTracklistSummaryLabel(albumTrackEntries)}</span>
                  )}
                </div>
                <div className="detail-modal-album-header">
                  <span className="detail-modal-album-number-header">#</span>
                  <span className="detail-modal-album-preview-header">Pre</span>
                  {hasPremiumPlayback ? (
                    <PlaybackActionMenu
                      ariaLabel="Album playback options"
                      buttonClassName="detail-album-play-all-button"
                      placement="adjacent"
                      onAction={(action) => handleAlbumPlayAll(action, playableAlbumEntries)}
                    >
                      Play all
                    </PlaybackActionMenu>
                  ) : (
                    <span aria-hidden="true" />
                  )}
                  <span className="detail-modal-album-liked-header">★</span>
                  <span className="detail-modal-album-title-header">Track</span>
                  {selectedPreviewAlbumHasGuestArtists ? <span className="detail-modal-album-with-header">With</span> : null}
                  <span className="detail-modal-album-liked-header">Tags</span>
                  <span className="detail-modal-album-listens-header">Listens</span>
                  <button
                    className={`detail-modal-album-last-played-header detail-modal-album-sort-header${albumTrackLastSortMode ? " detail-modal-album-sort-header-active" : ""}`}
                    onClick={() => setAlbumTrackLastSortMode((current) => nextLastPlayedSortMode(current))}
                    type="button"
                  >
                    Last
                    {albumTrackLastSortMode ? (
                      <span aria-hidden="true">{albumTrackLastSortMode === "recent" ? "↓" : "↑"}</span>
                    ) : null}
                  </button>
                </div>
                {albumTrackEntriesLoading && albumTrackEntries.length === 0 ? (
                  <p className="detail-modal-preview-missing">{albumTrackFetchFromSpotifyLoading ? "Fetching Album..." : "Loading Album..."}</p>
                ) : null}
                {!albumTrackEntriesLoading && albumTrackEntriesError && !albumTrackMoreOnSpotifyUrl ? (
                  <p className="detail-modal-preview-missing">{albumTrackEntriesError}</p>
                ) : null}
                {!albumTrackEntriesLoading && albumTrackEntriesError && albumTrackMoreOnSpotifyUrl ? (
                  <p className="detail-modal-preview-missing">
                    <a href={albumTrackMoreOnSpotifyUrl} rel="noreferrer" target="_blank">More tracks on Spotify</a>
                  </p>
                ) : null}
                {albumTrackEntriesPartial && albumTrackEntries.length > 0 ? (
                  <p className="detail-modal-album-enrichment-note">
                    Tracklist is shown from partial local cache. Album details are still being completed.
                  </p>
                ) : null}
                {!albumTrackEntriesError && albumTrackEntries.length > 0 ? (
                  <div className="detail-album-track-list-wrap">
                    {selectedAlbumTrackMarkerTop(displayAlbumTrackEntries) ? (
                      <span
                        className="detail-album-track-scroll-marker"
                        style={{ "--detail-album-track-marker-top": selectedAlbumTrackMarkerTop(displayAlbumTrackEntries) } as CSSProperties}
                        aria-hidden="true"
                      />
                    ) : null}
                    <ul className={`detail-album-track-list${albumTrackEntriesLoading ? " detail-album-track-list-updating" : ""}`} ref={albumTrackListRef}>
                      {displayAlbumTrackEntries.map((track, trackIndex) => {
                      const rowTrackUri = track.uri ?? (track.id ? `spotify:track:${track.id}` : null);
                      const rowIsCurrentTrack = Boolean(rowTrackUri && currentTrack?.uri === rowTrackUri);
                      const rowPlaying = isTrackPlaying(rowTrackUri);
                      const rowPreviewPlaying = Boolean(rowTrackUri && previewingTrackUri === rowTrackUri);
                      const rowPreviewActive = Boolean(rowPreviewPlaying && rowPlaying);
                      const rowPreviewKey = albumTrackPreviewKey(track, rowTrackUri);
                      const rowPreviewPlayed = previewPlayedTrackKeys.has(rowPreviewKey);
                      const rowPausedCurrent = Boolean(rowIsCurrentTrack && playbackPaused);
                      const rowHistoryLastPlayedAt = selectedPreviewDetailView === "release"
                        ? track.sourceLastPlayedAt === undefined ? track.lastPlayedAt : track.sourceLastPlayedAt
                        : track.recordingLastPlayedAt === undefined ? track.lastPlayedAt : track.recordingLastPlayedAt;
                      const rowHistoryPlayCount = selectedPreviewDetailView === "release"
                        ? track.sourcePlayCount === undefined ? track.playCount : track.sourcePlayCount
                        : track.recordingPlayCount === undefined ? track.playCount : track.recordingPlayCount;
                      const rowLastPlayed = formatCompactRelativeAge(rowHistoryLastPlayedAt);
                      const rowIsUnlistened = !rowHistoryLastPlayedAt && rowHistoryPlayCount <= 0;
                      const rowRelationTagsResult = trackRelationTags({
                        releaseTrackDuplicateSourceCount: track.releaseTrackDuplicateSourceCount,
                        releaseTrackSourceCount: track.releaseTrackSourceCount,
                        hasReleaseTrackSiblings: track.hasReleaseTrackSiblings,
                        releaseTrackClusterCandidateType: track.releaseTrackClusterCandidateType,
                        releaseTrackClusterRelationshipKind: track.releaseTrackClusterRelationshipKind,
                      });
                      const rowRelationTags = rowRelationTagsResult.text;
                      const rowRelationTagsTitle = rowRelationTagsResult.title;
                      const rowStarTrackId = track.id ?? spotifyTrackIdFromUri(rowTrackUri);
                      const rowMatchesSelectedReleaseTrack = Boolean(
                        selectedPreview.kind === "track"
                        && (
                          (track.releaseTrackId != null && track.releaseTrackId === selectedPreview.releaseTrackId)
                          || (rowStarTrackId && selectedPreviewReleaseTrackDetailReady?.source_versions.some((version) => version.spotify_track_id === rowStarTrackId))
                        ),
                      );
                      const rowIsLiked = rowStarTrackId && rowStarTrackId in localStarredTrackById
                        ? localStarredTrackById[rowStarTrackId]
                        : selectedPreview.kind === "track"
                          ? selectedPreviewDetailView === "release"
                            ? albumTrackIsExactKnownLiked(track)
                            : (rowMatchesSelectedReleaseTrack && selectedPreviewIsKnownLiked) || albumTrackIsExactKnownLiked(track)
                          : albumTrackIsKnownLiked(track);
                      const mainArtistNames = new Set(
                        selectedPreview.kind === "album" || selectedPreview.kind === "track"
                          ? selectedPreviewAlbumMainArtists.map((artist) => artist.name?.trim().toLocaleLowerCase()).filter(Boolean)
                          : [],
                      );
                      const rowWithArtists = selectedPreview.kind === "album" || selectedPreview.kind === "track"
                        ? artistEntriesForAlbumTrack(track).filter((artist) => {
                          const artistName = artist.name?.trim().toLocaleLowerCase();
                          return Boolean(artistName && !mainArtistNames.has(artistName));
                        })
                        : [];
                      const rowMatchesHighlightedArtist = Boolean(
                        selectedPreview.kind === "album"
                        && selectedPreview.albumHighlightArtistNames?.some((artistName) => artistNameMatches(track.artistName, artistName)),
                      );
                      const rowMatchesHoveredWithArtist = Boolean(
                        hoveredAlbumWithArtistName && artistNameMatches(track.artistName, hoveredAlbumWithArtistName),
                      );
                      const rowBaseDurationMs = (
                        track.durationMs
                        ?? (rowIsCurrentTrack
                          ? (playbackDurationMs > 0 ? playbackDurationMs : currentTrack?.durationMs ?? null)
                          : null)
                      );
                      const rowElapsedMs = rowIsCurrentTrack
                        ? (
                          rowBaseDurationMs != null
                            ? Math.min(Math.max(0, playbackPositionMs), rowBaseDurationMs)
                            : Math.max(0, playbackPositionMs)
                        )
                        : null;
                      const rowButtonTimeMs = rowIsCurrentTrack
                        ? (
                          rowPlaying
                            ? rowElapsedMs
                            : (rowPausedCurrent ? (pausedTimeFlashOn ? rowElapsedMs : rowBaseDurationMs) : rowBaseDurationMs)
                        )
                        : rowBaseDurationMs;
                      const rowFamilyAvailabilityLabel = track.familySwitchLabel
                        ?? track.familyAvailableVersions[0]?.label
                        ?? "another edition";
                      const rowFamilyTitle = track.familyExclusive
                        ? `Switch to include these tracks from ${rowFamilyAvailabilityLabel}.`
                        : trackIsHiddenByDisc(track) && track.discNumber != null
                          ? `Disc ${track.discNumber} is hidden from playback.`
                          : undefined;
                      const rowExcludedFromPlayback = track.familyExclusive || trackIsHiddenByDisc(track);
                      return (
                        <Fragment key={track.id ?? track.name}>
                          {albumPanelHasMultipleDiscs
                            && track.discNumber != null
                            && displayAlbumTrackEntries.findIndex((candidate) => candidate.discNumber === track.discNumber) === trackIndex ? (
                              <li className="detail-album-track-disc-row" data-disc-number={track.discNumber}>
                                <span>
                                  Disc {track.discNumber}
                                  {discEditionLabel(track.discNumber) ? ` - ${discEditionLabel(track.discNumber)}` : ""}
                                </span>
                                <button
                                  className="detail-album-track-disc-toggle"
                                  onClick={() => toggleAlbumDisc(track.discNumber!)}
                                  type="button"
                                >
                                  {hiddenAlbumDiscNumbers.has(track.discNumber) ? "Show" : "Hide"}
                                </button>
                              </li>
                            ) : null}
                          <li
                            className={`detail-album-track-row${track.isSelected ? " detail-album-track-row-selected" : ""}${rowMatchesHighlightedArtist || rowMatchesHoveredWithArtist ? " detail-album-track-row-artist-highlighted" : ""}${rowExcludedFromPlayback ? " detail-album-track-row-family-exclusive" : ""}`}
                            title={rowFamilyTitle}
                          >
                            <span className="detail-album-track-number">
                              {track.trackNumber ?? trackIndex + 1}
                            </span>
                            {selectedPreview.kind !== "track" && hasPremiumPlayback ? (
                              <button
                                aria-label={rowPreviewPlaying ? `Stop preview for ${track.name}` : `Preview ${track.name}`}
                                className={`detail-album-track-preview-button${rowPreviewActive ? " detail-album-track-preview-button-active" : ""}${rowPreviewPlayed ? " detail-album-track-preview-button-played" : ""}`}
                                disabled={!rowTrackUri}
                                onClick={() => {
                                  void toggleAlbumTrackPreview(track, rowTrackUri);
                                }}
                                type="button"
                              />
                            ) : selectedPreview.kind !== "track" ? (
                              <span className="detail-album-track-preview-placeholder" aria-hidden="true" />
                            ) : null}
                            {hasPremiumPlayback ? (
                              <PlaybackActionMenu
                              ariaLabel={rowPlaying ? "Currently playing in ListenLab" : rowTrackUri ? `Play ${track.name} in ListenLab` : `${track.name} is not playable`}
                              buttonClassName={`secondary-button detail-album-track-play-button${rowPlaying ? " detail-icon-button-playing" : ""}`}
                              disabled={!rowTrackUri || rowExcludedFromPlayback}
                              isPlaying={rowPlaying}
                              placement="overlay-trigger"
                              onAction={(action) => {
                                const albumQueue = buildAlbumPlaybackQueue(rowTrackUri, playableAlbumEntries);
                                return handlePlaybackAction(action, {
                                  trackUri: rowTrackUri,
                                  optimisticTrack: playerSummaryFromAlbumTrack(track),
                                  queueCursor: albumQueue?.queueCursor,
                                  queueContext: albumQueue?.queueContext,
                                  queuePlaylistUris: albumQueue?.playlistUris,
                                  queueTracks: albumQueue?.queueTracks,
                                  sourceTrack: track.sourceTrack,
                                }).then(() => {
                                  if (action === "play_now") {
                                    openAlbumTrackPreview(track);
                                  }
                                });
                              }}
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
                              <span className={`detail-album-track-play-time${rowPausedCurrent ? " detail-album-track-play-time-flash" : ""}`}>
                                {rowButtonTimeMs != null ? formatPlaybackClock(rowButtonTimeMs) : "?:??"}
                              </span>
                            </PlaybackActionMenu>
                          ) : <span aria-hidden="true" />}
                          <span className="detail-album-track-liked-cell">
                            {rowIsLiked ? (
                              <LikedBadge className="detail-album-track-liked-badge" />
                            ) : (
                              <span className="detail-album-track-liked-empty" aria-label="Not liked">-</span>
                            )}
                          </span>
                          <button
                            className="detail-album-track-name-button single-line-ellipsis"
                            onClick={() => {
                              if (track.familyExclusive && track.familySwitchAlbumId) {
                                includeAlbumFamilyTracks(track.familySwitchAlbumId);
                                return;
                              }
                              if (trackIsHiddenByDisc(track) && track.discNumber != null) {
                                toggleAlbumDisc(track.discNumber);
                                return;
                              }
                              openAlbumTrackPreview(track);
                            }}
                            type="button"
                          >
                            <span className="single-line-ellipsis">{track.name}</span>
                          </button>
                          {selectedPreview.kind !== "track" && selectedPreviewAlbumHasGuestArtists ? (
                            <span className="detail-album-track-with single-line-ellipsis">
                              {rowWithArtists.map((artist, index) => {
                                const artistName = artist.name?.trim();
                                if (!artistName) {
                                  return null;
                                }
                                return (
                                  <span className="detail-modal-artist-link-wrap" key={`${artist.artist_id ?? artist.id ?? artistName}-${index}`}>
                                    {index > 0 ? <span className="detail-modal-artist-separator">, </span> : null}
                                    <button
                                      className="detail-modal-inline-link"
                                      onClick={() => openAlbumWithArtistPreview(artist)}
                                      type="button"
                                    >
                                      {artistName}
                                    </button>
                                  </span>
                                );
                              })}
                            </span>
                          ) : null}
                          <span className="detail-album-track-tags-cell">
                            <span className="detail-album-track-badges">
                              {rowRelationTags ? (
                                <span
                                  className="relation-tags-badge detail-album-track-relation-badge"
                                  title={rowRelationTagsTitle}
                                  aria-label={rowRelationTagsTitle}
                                >
                                  {rowRelationTags}
                                </span>
                              ) : <span className="detail-album-track-tags-empty">-</span>}
                            </span>
                          </span>
                          <span className="detail-album-track-listen-count">
                            {rowHistoryPlayCount > 0 ? rowHistoryPlayCount.toLocaleString() : "-"}
                          </span>
                          <div className="detail-album-track-actions">
                            {rowLastPlayed ? (
                              <span className="detail-album-track-last-played">{rowLastPlayed}</span>
                            ) : rowIsUnlistened ? (
                              <span className="detail-album-track-last-played">
                                <NewTrackBadge className="detail-album-track-played-new-badge" />
                              </span>
                            ) : (
                              <span className="detail-album-track-last-played">-</span>
                            )}
                          </div>
                        </li>
                        {selectedPreview.kind === "track" && rowMatchesSelectedReleaseTrack && (albumTrackFetchFromSpotifyLoading || albumTrackMoreOnSpotifyUrl) ? (
                          <li className="detail-album-track-fetch-row">
                            {albumTrackFetchFromSpotifyLoading ? (
                              <span className="detail-album-track-fetch-status">Fetching Album...</span>
                            ) : albumTrackMoreOnSpotifyUrl ? (
                              <a className="detail-album-track-fetch-link" href={albumTrackMoreOnSpotifyUrl} rel="noreferrer" target="_blank">More tracks on Spotify</a>
                            ) : null}
                          </li>
                        ) : null}
                        </Fragment>
                      );
                      })}
                    </ul>
                  </div>
                ) : null}
                <RelatedAlbumsSection
                  context={albumFamilyContext}
                  relatedAlbums={selectedPreviewRelatedAlbums}
                  onSelect={switchSelectedTrackAlbumVersion}
                  onSelectAlbum={openArtistAlbumPreview}
                />
              </div>
            ) : null}
            {selectedPreview.kind === "playlist" && playlistTrackAccessDenied ? (
              <div className="detail-modal-playlist-access-denied" role="status">
                {selectedPreviewSpotifyPlaylistUrl ? (
                  <a href={selectedPreviewSpotifyPlaylistUrl} rel="noreferrer" target="_blank">
                    View playlist on Spotify
                  </a>
                ) : (
                  <span>Playlist tracks are only available in Spotify.</span>
                )}
              </div>
            ) : null}
            {selectedPreview.kind === "playlist" && !playlistTrackAccessDenied ? (
              <PlaylistTrackList
                currentTrackUri={currentTrack?.uri ?? null}
                entries={playlistTrackEntries}
                error={playlistTrackEntriesError}
                formatCompactRelativeAge={formatCompactRelativeAge}
                formatPlaybackClock={formatPlaybackClock}
                hasMore={playlistTrackEntriesHasMore}
                hasPremiumPlayback={hasPremiumPlayback}
                isTrackPlaying={isTrackPlaying}
                isTrackLiked={recentTrackIsKnownLiked}
                loading={playlistTrackEntriesLoading}
                onShownCountChange={setPlaylistTableShownCount}
                onTableOptionsChange={setPlaylistTableOptions}
                onPreviewTrack={togglePlaylistTrackPreview}
                rowOffset={playlistTrackEntriesOffset}
                mergeRecordingTracks={playlistTracksAreMerged}
                showReleaseTrackStats={playlistTrackStatsView === "release"}
                focusPlaylistPosition={selectedPreview.focusPlaylistPosition ?? null}
                focusSpotifyTrackId={selectedPreview.focusSpotifyTrackId ?? null}
                showCollaborativeColumns={playlistTrackEntriesShowCollaborativeColumns}
                total={playlistTrackEntriesTotal}
                onPlayTrack={handlePlaylistTrackPlayback}
                onSelectAlbum={openRecentTrackAlbumPreview}
                onSelectArtist={openRecentTrackArtistPreview}
                onSelectTrack={openRecentPlayerTrackDetails}
                playbackDurationMs={playbackDurationMs}
                playbackPaused={playbackPaused}
                playbackPositionMs={playbackPositionMs}
                previewingTrackUri={previewingTrackUri}
                previewPlayedTrackKeys={previewPlayedTrackKeys}
                tableOptions={playlistTableOptions}
                trackUriWithFallback={trackUriWithFallback}
              />
            ) : null}
            {selectedPreview.kind === "track" ? (
              <div className="detail-modal-recording-variations detail-modal-playlist-memberships">
                <div className="detail-modal-recording-variations-header">
                  <span>Playlists</span>
                  {selectedPreviewPlaylistMemberships.length > 0 ? (
                    <button
                      className="detail-modal-playlist-edit-button"
                      onClick={() => {
                        setPlaylistAddSortMode("membership");
                        setPlaylistAddOverlayOpen(true);
                      }}
                      type="button"
                    >
                      Edit
                    </button>
                  ) : null}
                </div>
                {selectedPreviewPlaylistMembershipsLoading && selectedPreviewPlaylistMemberships.length === 0 ? (
                  <p className="detail-modal-preview-missing">Checking cached playlists...</p>
                ) : (
                  <>
                    {selectedPreviewPlaylistMembershipStatusMessage ? (
                      <p className="detail-modal-preview-missing">{selectedPreviewPlaylistMembershipStatusMessage}</p>
                    ) : null}
                    <div className="detail-modal-playlist-membership-list">
                      {selectedPreviewPlaylistMemberships.map((membership) => (
                        <button
                          className="detail-modal-playlist-membership"
                          key={`${membership.playlist_id}-${membership.position}-${membership.spotify_track_id ?? "track"}`}
                          onClick={() => openPlaylistMembershipPreview(membership)}
                          type="button"
                        >
                          {membership.playlist_image_url ? (
                            <img alt="" src={membership.playlist_image_url} />
                          ) : (
                            <span className="detail-modal-playlist-membership-fallback" aria-hidden="true">P</span>
                          )}
                          <span className="detail-modal-playlist-membership-copy">
                            <strong className="single-line-ellipsis">{membership.playlist_name ?? "Untitled playlist"}</strong>
                            <span className="single-line-ellipsis">
                              {playlistCategoryNames(membership.playlist_id).join(" · ") || "Playlist"}
                            </span>
                          </span>
                        </button>
                      ))}
                      {selectedPreviewPlaylistMemberships.length === 0 ? (
                        <button
                          className="detail-modal-playlist-membership detail-modal-playlist-add-card"
                          onClick={() => {
                            setPlaylistAddSortMode("recent");
                            setPlaylistAddOverlayOpen(true);
                          }}
                          type="button"
                        >
                          <span className="detail-modal-playlist-add-plus" aria-hidden="true">+</span>
                          <span className="detail-modal-playlist-membership-copy">
                            <strong>Add to playlist</strong>
                            <span>{availablePlaylistOptions.length > 0 ? `${availablePlaylistOptions.length} playlists available` : "Create a new playlist"}</span>
                          </span>
                        </button>
                      ) : null}
                    </div>
                    {playlistAddOverlayOpen ? (
                      <div className="detail-modal-playlist-add-overlay" role="dialog" aria-modal="false" aria-label="Add to playlist">
                        <form className="detail-modal-playlist-add-panel" onSubmit={handlePlaylistAddSubmit}>
                          <div className="detail-modal-playlist-add-header">
                            <span>Add to playlist</span>
                            <button
                              aria-label="Close add to playlist"
                              className="detail-modal-playlist-add-close"
                              onClick={() => setPlaylistAddOverlayOpen(false)}
                              type="button"
                            >
                              ×
                            </button>
                          </div>
                          <div className="detail-modal-playlist-add-toolbar">
                            <input
                              disabled={playlistAddSaving}
                              onChange={(event) => setPlaylistAddSearch(event.target.value)}
                              placeholder="Search playlists"
                              type="search"
                              value={playlistAddSearch}
                            />
                            <select
                              disabled={playlistAddSaving}
                              onChange={(event) => setPlaylistAddSortMode(event.target.value as PlaylistAddSortMode)}
                              value={playlistAddSortMode}
                            >
                              <option value="recent">Recently edited</option>
                              <option value="membership">In playlist</option>
                              <option value="tracks">Track count</option>
                              <option value="name">Name</option>
                            </select>
                            <select
                              disabled={playlistAddSaving}
                              onChange={(event) => setPlaylistAddCategoryFilter(event.target.value)}
                              value={playlistAddCategoryFilter}
                            >
                              <option value="active">Active categories</option>
                              <option value="all">All categories</option>
                              <option value="none">None</option>
                              {playlistCategories.map((category) => (
                                <option key={category.id} value={category.id}>{category.name}</option>
                              ))}
                            </select>
                          </div>
                          <div className="detail-modal-playlist-add-list detail-modal-playlist-add-grid">
                            {availablePlaylistOptions.length > 0 ? availablePlaylistOptions.map((playlist) => {
                              const playlistId = playlist.playlist_id ?? "";
                              const alreadyContainsTrack = existingPlaylistMembershipIds.has(playlistId);
                              const isChecked = playlistAddSelectedIds.has(playlistId);
                              const categoryNames = playlistCategoryNames(playlistId);
                              return (
                                <label className="detail-modal-playlist-add-option" key={playlistId}>
                                  <input
                                    checked={isChecked}
                                    disabled={playlistAddSaving}
                                    onChange={(event) => {
                                      setPlaylistAddSelectedIds((current) => {
                                        const next = new Set(current);
                                        if (event.target.checked) {
                                          next.add(playlistId);
                                        } else {
                                          next.delete(playlistId);
                                        }
                                        return next;
                                      });
                                    }}
                                    type="checkbox"
                                  />
                                  <span className="detail-modal-playlist-add-checkbox" aria-hidden="true" />
                                  {playlist.image_url ? (
                                    <img className="detail-modal-playlist-add-image" alt="" src={playlist.image_url} />
                                  ) : (
                                    <span className="detail-modal-playlist-add-image detail-modal-playlist-add-image-fallback" aria-hidden="true">P</span>
                                  )}
                                  <span className="detail-modal-playlist-add-option-copy">
                                    <strong className="single-line-ellipsis">{playlist.name ?? "Untitled playlist"}</strong>
                                    <span className="single-line-ellipsis">
                                      {[
                                        categoryNames.length > 0 ? categoryNames.join(", ") : null,
                                        alreadyContainsTrack ? "In playlist" : null,
                                        playlist.is_collaborative ? "Collaborative" : null,
                                        playlist.track_count != null ? `${playlist.track_count.toLocaleString()} tracks` : null,
                                      ].filter(Boolean).join(" · ")}
                                    </span>
                                  </span>
                                </label>
                              );
                            }) : (
                              <p className="detail-modal-preview-missing">No cached playlists are available yet.</p>
                            )}
                          </div>
                          <label className="detail-modal-playlist-new-field">
                            <span>New playlist</span>
                            <input
                              disabled={playlistAddSaving}
                              onChange={(event) => setPlaylistAddNewName(event.target.value)}
                              placeholder="Playlist name"
                              type="text"
                              value={playlistAddNewName}
                            />
                          </label>
                          {playlistAddError ? <p className="detail-modal-playlist-add-error">{playlistAddError}</p> : null}
                          <div className="detail-modal-playlist-add-actions">
                            <button
                              className="detail-modal-options-item"
                              disabled={playlistAddSaving}
                              onClick={() => setPlaylistAddOverlayOpen(false)}
                              type="button"
                            >
                              Cancel
                            </button>
                            <button className="detail-modal-playlist-add-submit" disabled={playlistAddSaving} type="submit">
                              {playlistAddSaving ? "Saving..." : "Save"}
                            </button>
                          </div>
                        </form>
                      </div>
                    ) : null}
                  </>
                )}
              </div>
            ) : null}
            {selectedPreview.kind === "track" && selectedPreviewDetailView === "release" && selectedPreviewReleaseSourceVersions.length > 1 ? (
              <div className="detail-modal-recording-variations detail-modal-release-source-albums">
                <div className="detail-modal-recording-variations-header">
                  <span>Release albums</span>
                  {selectedPreviewReleaseSourceVersionNeedsArrows ? (
                    <span className="detail-modal-recording-variation-controls">
                      <button aria-label="Previous source album covers" onClick={() => scrollRecordingVariationStrip(-1)} type="button">{"<"}</button>
                      <button aria-label="Next source album covers" onClick={() => scrollRecordingVariationStrip(1)} type="button">{">"}</button>
                    </span>
                  ) : null}
                </div>
                <div className="detail-modal-recording-variation-strip" ref={recordingVariationStripRef}>
                  {selectedPreviewReleaseSourceVersions.map((version) => {
                    const isSelectedSourceVersion = version.spotify_track_id === selectedPreviewCurrentSpotifyTrackId;
                    const albumImageUrl = releaseSourceVersionAlbumImageUrl(version);
                    const subtitle = variationSubtitleFromTitle(version.name, { allowRemasterOnly: true });
                    const title = [
                      version.album_release_year,
                      version.album_name || "Unknown album",
                      releaseSourceVersionPlayCountLabel(version),
                    ].filter(Boolean).join(" · ");
                    return (
                      <button
                        className={`detail-modal-recording-variation-cover${isSelectedSourceVersion ? " detail-modal-recording-variation-cover-selected" : ""}`}
                        key={`release-source-cover-${version.source_track_id}`}
                        onClick={() => openReleaseSourceVersion(version, "release")}
                        title={title}
                        type="button"
                      >
                        <span className="detail-modal-recording-variation-art">
                          <span className="detail-modal-recording-variation-kind">Source</span>
                          {albumImageUrl ? (
                            <img alt="" src={albumImageUrl} />
                          ) : (
                            <span className="detail-modal-recording-variation-fallback" aria-hidden="true">{(version.album_name || version.name || "?").slice(0, 1).toUpperCase()}</span>
                          )}
                          {version.is_representative_choice ? (
                            <span className="detail-modal-recording-variation-badge">Rep</span>
                          ) : null}
                        </span>
                        <span className="detail-modal-recording-variation-copy">
                          {subtitle ? <span className="detail-modal-recording-variation-subtitle">{subtitle}</span> : null}
                          <span className="detail-modal-recording-variation-album">{version.album_name || "Unknown album"}</span>
                          <span className="detail-modal-recording-variation-year">{version.album_release_year || releaseSourceVersionPlayCountLabel(version)}</span>
                        </span>
                      </button>
                    );
                  })}
                </div>
              </div>
            ) : null}
              </div>
            ) : null}
            {selectedPreview.kind === "track" ? (
              <div className="detail-modal-bottom-tags" aria-label="Track summary">
                <div className="detail-modal-options detail-modal-options-bottom">
                  <button
                    aria-expanded={detailOptionsOpen}
                    aria-label="Track options"
                    className="detail-modal-options-button"
                    onClick={() => setDetailOptionsOpen((current) => !current)}
                    type="button"
                  >
                    <span aria-hidden="true">⚙</span>
                  </button>
                  {detailOptionsOpen ? (
                    <div className="detail-modal-options-menu">
                      {selectedPreview.url ? (
                        <a
                          className="detail-modal-options-item"
                          href={selectedPreview.url}
                          rel="noreferrer"
                          target="_blank"
                        >
                          Open in Spotify
                        </a>
                      ) : null}
                      <button
                        className="detail-modal-options-item"
                        onClick={() => {
                          setSelectedPreviewDetailView((current) => current === "release" ? "recording" : "release");
                          setDetailOptionsOpen(false);
                        }}
                        type="button"
                      >
                        {selectedPreviewDetailView === "release" ? "Show representative" : "Show individual"}
                      </button>
                    </div>
                  ) : null}
                </div>
                {selectedPreviewCurrentVersionIsSpotifyLiked ? (
                  <span className="detail-track-bottom-liked-version-badge" title="This Spotify version is liked">
                    Liked
                  </span>
                ) : null}
                {selectedPreviewAlbumContextTagLabel ? <span>{selectedPreviewAlbumContextTagLabel}</span> : null}
                {selectedPreviewListenedRangeLabel && selectedPreviewListenedBreakdown ? (
                  <span className="detail-track-action-listen-popover-wrap">
                    <button
                      aria-expanded={dateBreakdownOpen}
                      aria-label="Show listened date breakdown"
                      className="detail-track-action-meta-listens"
                      onClick={() => {
                        setListenBreakdownOpen(false);
                        setDateBreakdownOpen((current) => !current);
                      }}
                      type="button"
                    >
                      Listened {selectedPreviewListenedRangeLabel}
                    </button>
                    {dateBreakdownOpen ? (
                      <span className="detail-track-action-listen-popover" role="status">
                        <span>
                          <span>This album:</span>
                          <strong>{selectedPreviewListenedBreakdown.thisAlbumFirstLabel ?? "-"} - {selectedPreviewListenedBreakdown.thisAlbumLastLabel ?? "-"}</strong>
                        </span>
                        <span>
                          <span>Other albums:</span>
                          <strong>{selectedPreviewListenedBreakdown.otherAlbumsFirstLabel ?? "-"} - {selectedPreviewListenedBreakdown.otherAlbumsLastLabel ?? "-"}</strong>
                        </span>
                      </span>
                    ) : null}
                  </span>
                ) : selectedPreviewListenedRangeLabel ? <span>Listened {selectedPreviewListenedRangeLabel}</span> : null}
                {selectedPreviewListenCountLabel && canShowListenBreakdown && selectedPreviewListenBreakdown ? (
                  <span className="detail-track-action-listen-popover-wrap">
                    <button
                      aria-expanded={listenBreakdownOpen}
                      aria-label="Show listen breakdown"
                      className="detail-track-action-meta-listens"
                      onClick={() => {
                        setDateBreakdownOpen(false);
                        setListenBreakdownOpen((current) => !current);
                      }}
                      type="button"
                    >
                      {selectedPreviewListenCountLabel}
                    </button>
                    {listenBreakdownOpen ? (
                      <span className="detail-track-action-listen-popover" role="status">
                        <span>
                          <span>This album:</span>
                          <strong>{selectedPreviewListenBreakdown.thisAlbumCount.toLocaleString()}</strong>
                        </span>
                        <span>
                          <span>Other albums:</span>
                          <strong>{selectedPreviewListenBreakdown.otherAlbumsCount.toLocaleString()}</strong>
                        </span>
                      </span>
                    ) : null}
                  </span>
                ) : selectedPreviewListenCountLabel ? <span>{selectedPreviewListenCountLabel}</span> : null}
              </div>
            ) : null}
            {selectedPreview.kind === "playlist" ? (
              <div className="detail-modal-bottom-tags detail-modal-playlist-bottom-tags" aria-label="Playlist options">
                <div className="detail-modal-options detail-modal-options-bottom">
                  <button
                    aria-expanded={detailOptionsOpen}
                    aria-label="Playlist options"
                    className="detail-modal-options-button"
                    onClick={() => setDetailOptionsOpen((current) => !current)}
                    type="button"
                  >
                    <span aria-hidden="true">⚙</span>
                  </button>
                  {detailOptionsOpen ? (
                    <div className="detail-modal-options-menu">
                      {playlistTrackEntriesHasMore ? (
                        <button
                          className="detail-modal-options-item"
                          disabled={playlistTrackEntriesLoading}
                          onClick={() => {
                            setDetailOptionsOpen(false);
                            void loadMorePlaylistTrackEntries();
                          }}
                          type="button"
                        >
                          {playlistTrackEntriesLoading ? "Fetching..." : "Fetch next 500 tracks"}
                        </button>
                      ) : null}
                      <button
                        className="detail-modal-options-item"
                        onClick={() => {
                          setPlaylistTrackStatsView((current) => current === "release" ? "recording" : "release");
                          setDetailOptionsOpen(false);
                        }}
                        type="button"
                      >
                        {playlistTrackStatsView === "release" ? "Show recordings" : "Show release tracks"}
                      </button>
                      {selectedPreview.url ? (
                        <a
                          className="detail-modal-options-item"
                          href={selectedPreview.url}
                          rel="noreferrer"
                          target="_blank"
                        >
                          Open in Spotify
                        </a>
                      ) : null}
                    </div>
                  ) : null}
                </div>
                <div className="detail-modal-playlist-footer-counts">
                  <span>
                    {playlistHeaderSummary.likedCount.toLocaleString()} {playlistHeaderSummary.partial ? "loaded " : ""}liked
                  </span>
                  <div className="detail-modal-playlist-footer-progress" aria-hidden="true">
                    <span
                      className="detail-modal-playlist-progress-liked"
                      style={{ width: `${Math.max(0, Math.min(100, playlistHeaderSummary.likedPercent))}%` }}
                    />
                    <span
                      className="detail-modal-playlist-progress-listened"
                      style={{ width: `${Math.max(0, Math.min(100, playlistHeaderSummary.listenedPercent))}%` }}
                    />
                  </div>
                  <span>
                    {playlistHeaderSummary.completePercent}% complete
                  </span>
                  {playlistHeaderSummary.unlistenedCount > 0 ? (
                    <span>
                      {playlistHeaderSummary.unlistenedCount.toLocaleString()} {playlistHeaderSummary.partial ? "loaded " : ""}unlistened
                    </span>
                  ) : null}
                </div>
              </div>
            ) : null}
          </section>
        </div>
      ) : null;
}
