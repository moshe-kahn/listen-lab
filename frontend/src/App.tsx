import { lazy, Suspense, type CSSProperties, type ReactNode, useEffect, useMemo, useRef, useState } from "react";
import type {
  SessionResponse,
  ProfileProgressResponse,
  RecentTrack,
  ArtistAlbumEvidenceItem,
  ArtistTrackEvidenceItem,
  MatchCounts,
  TopPlaylist,
  ProfileResponse,
  RecentSectionResponse,
  LikedTracksResponse,
  ReleaseTrackMetadataItem,
  RecentArchiveResponse,
  RecentCompletionFilter,
  ListeningLogResponse,
  MergedTrackSourceFilter,
  RecentDebugSourceFilter,
  MergedTrackAggregateResponse,
  TrackIdentityAuditResponse,
  AmbiguousReviewComponent,
  AmbiguousReviewItem,
  AmbiguousReviewResponse,
  SuggestedGroupReleaseTrack,
  SuggestedAnalysisGroup,
  SuggestedGroupsResponse,
  LocalReviewVerdict,
  LocalGroupingTarget,
  LocalReviewDecision,
  SubmissionPreviewValidationResponse,
  IdentityAuditSubmissionSaveResponse,
  IdentityAuditSavedSubmissionListItem,
  IdentityAuditSavedSubmissionListResponse,
  IdentityAuditSavedSubmissionReadResponse,
  IdentityAuditSubmissionDryRunResponse,
  CatalogBackfillRunItem,
  CatalogBackfillRunsResponse,
  CatalogBackfillQueueItem,
  CatalogBackfillQueueResponse,
  CatalogBackfillQueueRepairResponse,
  AlbumCatalogLookupItem,
  AlbumCatalogLookupResponse,
  AlbumDuplicateReleaseItem,
  AlbumDuplicateGroupItem,
  AlbumDuplicateLookupResponse,
  AlbumNameDuplicateGroupItem,
  AlbumNameDuplicateLookupResponse,
  AlbumMergeReviewTarget,
  ReleaseAlbumMergePreviewResponse,
  ReleaseAlbumMergeDryRunResponse,
  TrackDuplicateReleaseItem,
  TrackDuplicateGroupItem,
  TrackDuplicateLookupResponse,
  TrackCatalogLookupItem,
  TrackCatalogLookupResponse,
  TrackMappingSourceItem,
  TrackMappingConfirmationPreview,
  TrackMappingSourceReleaseGroup,
  TrackMappingReleaseItem,
  TrackMappingReleaseFamilyGroup,
  TrackMappingLineageResponse,
  CatalogBackfillCoverageResponse,
  CatalogBackfillRunResponse,
  CatalogBackfillEnqueueResponse,
  UnifiedReviewItem,
  RecentRange,
  AnalysisMode,
  ExperienceMode,
  ExperienceVisualMode,
  TrackRankingMode,
  RankMovementFilter,
  AppPage,
  CatalogBackfillTab,
  CatalogBackfillRunMode,
  CatalogBackfillQueueReasonFilter,
  SectionKey,
  DashboardListCardProps,
  PreviewItem,
  AuthTokenResponse,
  RecentIngestResultResponse,
  FullAvailabilityResponse,
  CurrentPlaybackSnapshot,
  CurrentPlaybackResponse,
  ReleaseTrackDetailResponse,
  ReleaseTrackDetailSourceVersion,
  RecordingTrackCandidateItem,
  RecordingTrackCandidateMember,
  TrackArtistEntry,
  ArtistAlbumEntry,
  PlayerTrackSummary,
  PlayerQueueTrack,
  SpotifyPlayerState,
  AlbumTrackEntry,
  AlbumFamilyContext,
  SpotifyPlayerInstance,
  PopupTrackPlaybackOptions,
  PlaybackActionRequest,
  PlaylistMembership,
  PlaylistIndexStatus,
  OwnedPlaylist
} from "./types/appTypes";
import {
  DEFAULT_PLAYER_VOLUME,
  EXPERIENCE_MODE_STORAGE_KEY,
  SPOTIFY_COOLDOWN_DURATION_STORAGE_KEY,
  SPOTIFY_COOLDOWN_UNTIL_STORAGE_KEY,
  IDENTITY_AUDIT_AMBIGUOUS_VISIBLE_STEP,
  INITIAL_OPEN_SECTIONS,
  INITIAL_SECTION_PAGES,
  LIKED_TRACKS_FETCH_LIMIT,
  LIKED_TRACKS_RECENT_DISPLAY_LIMIT,
  LIKED_TRACKS_SHUFFLE_POOL_LIMIT,
  LIVE_PLAYBACK_POLL_INTERVAL_MS,
  LIVE_PLAYBACK_PROGRESS_TICK_MS,
  PAGE_SIZE,
  PLAYER_RECENT_FETCH_LIMIT,
  PREVIEW_RAMP_DURATION_MS,
  PREVIEW_RAMP_START_VOLUME,
  PREVIEW_RAMP_STEP_MS,
  RECENT_RANGE_OPTIONS,
  RECENT_SECTION_FETCH_LIMIT,
  githubRepoUrl,
  spotifyAppsUrl,
} from "./constants/appConstants";
import {
  fetchCatalogBackfillCoverage,
  fetchCatalogBackfillRuns,
  fetchCatalogBackfillQueue,
  postCatalogBackfillQueueRepair,
  fetchAlbumCatalogLookup,
  fetchTrackCatalogLookup,
  fetchAlbumDuplicateLookup,
  fetchTrackDuplicateLookup,
  fetchTrackMappingLineage,
  fetchAlbumNameDuplicateLookup,
  postReleaseAlbumMergePreview,
  postReleaseAlbumMergeDryRun,
  enqueueCatalogBackfillItems,
  fetchMergedTrackAggregate,
  fetchIdentityAudit,
  fetchIdentityAuditSuggestedGroups,
  fetchIdentityAuditAmbiguousReview,
  fetchIdentityAuditSavedSubmissions,
  fetchIdentityAuditSavedSubmissionById,
  fetchIdentityAuditSavedSubmissionDryRun,
  fetchAllLikedTracks,
  fetchActivityListeningLog,
  fetchArtistAlbumEvidence,
  fetchLikedAlbumContains,
  fetchLikedTracksContains,
  fetchRecordingTrackCandidateByReleaseTrack,
  fetchReleaseTrackDetail,
  fetchReleaseTrackMetadata,
  postLikedTracksSync
} from "./api/appApi";
import { syncQueuePlaylist } from "./api/playbackApi";
import { CatalogBackfillPage } from "./components/catalogBackfill/CatalogBackfillPage";
import { LikedBadge } from "./components/common/LikedBadge";
import { NewTrackBadge } from "./components/common/NewTrackBadge";
import { ReleaseSiblingBadge } from "./components/common/ReleaseSiblingBadge";
import { DashboardAlbumColumn, DashboardArtistColumn } from "./components/dashboard/DashboardColumns";
import { MergedTrackSourceFilterToggle, RankMovementFilterToggle, TrackRankingToggle } from "./components/dashboard/DashboardControls";
import { DashboardListCard } from "./components/dashboard/DashboardListCard";
import { DashboardPlaylistsSection } from "./components/dashboard/DashboardPlaylistsSection";
import { DashboardSections } from "./components/dashboard/DashboardSections";
import { DashboardTrackColumn } from "./components/dashboard/DashboardTrackColumn";
import { DualSectionCard } from "./components/dashboard/DualSectionCard";
import { LoginHeroPanel } from "./components/dashboard/LoginHeroPanel";
import {
  auditList,
  auditNumber,
  identityAuditMeta,
  identityAuditTitle,
  renderIdentityAuditExample,
  renderIdentityAuditGroup,
  type TrackIdentityAuditExample,
} from "./components/identityAudit/IdentityAuditDiagnostics";
import { AlbumIdentityAuditCatalogTab } from "./components/identityAudit/AlbumIdentityAuditCatalogTab";
import { AlbumIdentityAuditMergeReviewTab } from "./components/identityAudit/AlbumIdentityAuditMergeReviewTab";
import { AlbumDuplicateMergeCard } from "./components/identityAudit/AlbumDuplicateMergeCard";
import { IdentityAuditPage } from "./components/identityAudit/IdentityAuditPage";
import {
  AlbumIdentityAuditOverviewCards,
  TrackIdentityAuditOverviewCards,
} from "./components/identityAudit/IdentityAuditOverviewCards";
import { TrackIdentityAuditAmbiguousTab } from "./components/identityAudit/TrackIdentityAuditAmbiguousTab";
import { FormulaLabPage } from "./components/formulaLab/FormulaLabPage";
import {
  IssueFeed,
  issueSeverityForCount,
  type NormalizedAuditIssue,
} from "./components/identityAudit/IssueFeed";
import { FullAnalysisOverlay, LoadingScreen } from "./components/loading/LoadingScreens";
import { HomeAlbumAppearanceStrip } from "./components/playback/HomeAlbumAppearanceStrip";
import { PlayerBottomDrawer, type PlayerBottomDrawerTab, type SavedEntityBookmark, type SavedPlayerQueueSnapshot, type SavedTrackBookmark } from "./components/playback/PlayerBottomDrawer";
import { PlaybackActionMenu, type PlaybackAction } from "./components/playback/PlaybackActionMenu";
import { SpotifyCooldownPanel } from "./components/profile/SpotifyCooldownPanel";
import { RecentDebugPage } from "./components/recentDebug/RecentDebugPage";
import { SearchLookupPage } from "./components/searchLookup/SearchLookupPage";
import {
  loadIdentityAuditPersistedPrefs,
  saveIdentityAuditPersistedPrefs,
  type AlbumIdentityAuditTab,
  type IdentityAuditEntityTab,
  type IdentityAuditIssueReviewState,
  type IdentityAuditIssueSort,
  type TrackIdentityAuditTab,
} from "./utils/identityAuditPrefs";
import {
  collapseRepeatedQueueCycle,
  currentTrackFromState,
  dedupeRecentTracksForPlayer,
  filterAndDedupeRecentTracksForActivity,
  formatPlaybackClock,
  queuePlaylistTrackUris,
  queueRepeatsTrack,
  recentTracksToPlayerQueueTracks,
  spotifyTrackIdFromUri,
  spotifyTrackUrl,
  trackUriWithFallback,
} from "./utils/playbackUtils";
import { recordingIdentityMatchesAnyReleaseTrackId } from "./utils/recordingIdentity";
import {
  albumLookupRowCanBulkPrioritize,
  clampProgress,
  collapseRecentPreviewTracks,
  collapseTrackPreviewAlbums,
  firstArtistFromRecentTrack,
  formatCooldownCopy,
  formatCompactRelativeAge,
  formatDebugTimestamp,
  formatDurationMs,
  formatListeningSince,
  formatLoadingStatusDetailed,
  formatLoadingStatusUi,
  formatMonthDay,
  derivedAlbumDisplayLabel,
  formatPlaylistSummary,
  formatRelativeSyncTime,
  formatUiErrorMessage,
  mergeExtendedProfile,
  normalizedTrackArtistKey,
  parseCooldownSeconds,
  parseTimestampMs,
  previewAlbumHeading,
  previewImages,
  previewItems,
  primaryArtistName,
  recentRangeLabel,
  sortedTracksForView,
  spotifyPlaylistIdFromUrl,
  trackLookupRowCanBulkPrioritize,
} from "./utils/dashboardUtils";

const DetailPreviewModal = lazy(() => import("./components/dashboard/DetailPreviewModal").then((module) => ({
  default: module.DetailPreviewModal,
})));

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "/api";
const ALBUM_TRACKS_FETCH_TIMEOUT_MS = 15_000;
let recentSectionsRequestInFlight: Promise<RecentSectionResponse> | null = null;
let recentSectionsRequestBlockedUntilMs = 0;
function artistEntriesFromText(value: string | null | undefined): TrackArtistEntry[] {
  return String(value ?? "")
    .split(",")
    .map((name) => name.trim())
    .filter(Boolean)
    .map((name) => ({ name }));
}

function artistEntryFromDisplayText(value: string | null | undefined): TrackArtistEntry[] {
  const name = String(value ?? "").trim();
  return name ? [{ name }] : [];
}

function recordingMemberArtistEntries(member: RecordingTrackCandidateMember): TrackArtistEntry[] {
  if (member.artists?.length) {
    return uniqueArtistEntries(member.artists);
  }
  return String(member.artist ?? "")
    .split("|")
    .map((name) => name.trim())
    .filter(Boolean)
    .map((name) => ({ name }));
}

function recordingMemberArtistDisplay(member: RecordingTrackCandidateMember) {
  return recordingMemberArtistEntries(member)
    .map((artist) => artist.name?.trim())
    .filter(Boolean)
    .join(", ");
}

function uniqueArtistEntries(...groups: Array<RecentTrack["artists"] | null | undefined>): TrackArtistEntry[] {
  const entries: TrackArtistEntry[] = [];
  const seenIds = new Set<string>();
  const seenNames = new Set<string>();
  for (const group of groups) {
    for (const artist of group ?? []) {
      const name = artist?.name?.trim() ?? "";
      const artistId = artist?.artist_id?.trim() ?? artist?.id?.trim() ?? "";
      if (!name && !artistId) {
        continue;
      }
      const nameKey = name.toLocaleLowerCase();
      if ((artistId && seenIds.has(artistId)) || (nameKey && seenNames.has(nameKey))) {
        continue;
      }
      if (artistId) {
        seenIds.add(artistId);
      }
      if (nameKey) {
        seenNames.add(nameKey);
      }
      entries.push(artist);
    }
  }
  return entries;
}

function artistEntryIdentityKey(artist: TrackArtistEntry): string | null {
  const artistId = artist.artist_id?.trim() || artist.id?.trim();
  if (artistId) {
    return `id:${artistId}`;
  }
  const artistName = artist.name?.trim().toLocaleLowerCase();
  return artistName ? `name:${artistName}` : null;
}

function artistEntriesForAlbumTrack(track: AlbumTrackEntry): TrackArtistEntry[] {
  return track.sourceTrack?.artists?.length
    ? uniqueArtistEntries(track.sourceTrack.artists)
    : track.artists?.length
      ? uniqueArtistEntries(track.artists)
      : uniqueArtistEntries(artistEntriesFromText(track.artistName));
}

function artistNameMatches(candidate: string | null | undefined, target: string | null | undefined) {
  const normalizedTarget = target?.trim().toLocaleLowerCase();
  if (!normalizedTarget) {
    return false;
  }
  return String(candidate ?? "")
    .split(",")
    .map((name) => name.trim().toLocaleLowerCase())
    .some((name) => name === normalizedTarget);
}

function nonYearArtistText(value: string | null | undefined) {
  const text = String(value ?? "").trim();
  return /^\d{4}$/.test(text) ? null : text || null;
}

function collaboratorLabel(artistNames: string | null | undefined, selectedArtistName: string | null | undefined) {
  const selectedKey = selectedArtistName?.trim().toLocaleLowerCase();
  if (!selectedKey) {
    return null;
  }
  const collaborators = String(artistNames ?? "")
    .split(",")
    .map((name) => name.trim())
    .filter((name) => name && name.toLocaleLowerCase() !== selectedKey);
  return collaborators.length > 0 ? `with ${collaborators.join(", ")}` : null;
}

function albumContextTagLabel(albumType: string | null | undefined, albumName: string | null | undefined) {
  const type = String(albumType ?? "").trim().toLocaleLowerCase();
  const name = String(albumName ?? "").trim().toLocaleLowerCase();
  if (type === "single") {
    return "Single";
  }
  if (/\b(soundtrack|ost|original score|motion picture|bande originale|bo du film)\b/.test(name)) {
    return "Soundtrack";
  }
  if (type === "compilation") {
    return "Compilation";
  }
  if (/\bremaster(?:ed)?\b/.test(name)) {
    return "Remaster";
  }
  return null;
}

type PlayerQueueContext = {
  label: string;
  url?: string | null;
  playlistId?: string | null;
  playlistName?: string | null;
};

type PlayerQueueGroup = {
  id: string;
  label: string;
  url?: string | null;
  imageUrl?: string | null;
  tracks: PlayerQueueTrack[];
};

const SAVED_PLAYER_QUEUES_STORAGE_KEY = "listenlab.savedQueues";
const TRACK_BOOKMARKS_STORAGE_KEY = "listenlab.trackBookmarks";
const ENTITY_BOOKMARKS_STORAGE_KEY = "listenlab.entityBookmarks";
type TrackBookmarkContext = NonNullable<SavedTrackBookmark["context"]>;

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}

function normalizeSavedPlayerQueueSnapshot(value: unknown): SavedPlayerQueueSnapshot | null {
  if (!isRecord(value)) {
    return null;
  }
  const groupsValue = value.groups;
  if (!Array.isArray(groupsValue)) {
    return null;
  }
  const groups = groupsValue.flatMap((groupValue) => {
    if (!isRecord(groupValue) || !Array.isArray(groupValue.tracks)) {
      return [];
    }
    const tracks = groupValue.tracks.filter((track): track is PlayerQueueTrack => isRecord(track) && typeof track.name === "string");
    if (tracks.length === 0) {
      return [];
    }
    return [{
      id: typeof groupValue.id === "string" && groupValue.id ? groupValue.id : `saved-group-${Math.random().toString(36).slice(2)}`,
      label: typeof groupValue.label === "string" && groupValue.label ? groupValue.label : "Saved context",
      url: typeof groupValue.url === "string" ? groupValue.url : null,
      imageUrl: typeof groupValue.imageUrl === "string" ? groupValue.imageUrl : null,
      cursor: typeof groupValue.cursor === "number" && Number.isFinite(groupValue.cursor) ? groupValue.cursor : null,
      tracks,
    }];
  });
  if (groups.length === 0) {
    return null;
  }
  const contextValue = value.context;
  const sourceValue = value.source;
  return {
    id: typeof value.id === "string" && value.id ? value.id : `saved-queue-${Math.random().toString(36).slice(2)}`,
    savedAt: typeof value.savedAt === "string" ? value.savedAt : new Date().toISOString(),
    context: isRecord(contextValue)
      ? {
        label: typeof contextValue.label === "string" ? contextValue.label : null,
        url: typeof contextValue.url === "string" ? contextValue.url : null,
      }
      : null,
    source: sourceValue === "spotify" || sourceValue === "listenlab" ? sourceValue : null,
    activeCursor: typeof value.activeCursor === "number" && Number.isFinite(value.activeCursor) ? value.activeCursor : null,
    playedKeys: Array.isArray(value.playedKeys) ? value.playedKeys.filter((key): key is string => typeof key === "string") : [],
    groups,
    currentTrack: isRecord(value.currentTrack) && typeof value.currentTrack.name === "string"
      ? value.currentTrack as PlayerTrackSummary
      : null,
  };
}

function playerTrackToQueueTrack(track: PlayerTrackSummary | PlayerQueueTrack): PlayerQueueTrack {
  const queueTrack = track as Partial<PlayerQueueTrack>;
  return {
    name: track.name,
    artists: track.artists,
    album: track.album,
    image: track.image,
    uri: track.uri,
    durationMs: track.durationMs,
    trackId: queueTrack.trackId ?? spotifyTrackIdFromUri(track.uri) ?? null,
    albumId: queueTrack.albumId ?? null,
    releaseTrackId: queueTrack.releaseTrackId ?? null,
    releaseTrackName: queueTrack.releaseTrackName ?? null,
    releaseTrackSourceCount: queueTrack.releaseTrackSourceCount ?? null,
    releaseTrackDuplicateSourceCount: queueTrack.releaseTrackDuplicateSourceCount ?? null,
    hasReleaseTrackSiblings: queueTrack.hasReleaseTrackSiblings ?? null,
    releaseTrackClusterCandidateType: queueTrack.releaseTrackClusterCandidateType ?? null,
    releaseTrackClusterRelationshipKind: queueTrack.releaseTrackClusterRelationshipKind ?? null,
    isLiked: queueTrack.isLiked ?? null,
    likedAt: queueTrack.likedAt ?? null,
    artistItems: queueTrack.artistItems,
  };
}

function bookmarkIdentityForTrack(track: PlayerTrackSummary | PlayerQueueTrack | null | undefined) {
  if (!track) {
    return null;
  }
  const queueTrack = track as Partial<PlayerQueueTrack>;
  return track.uri
    ?? queueTrack.trackId
    ?? [track.name, track.artists, track.album].map((value) => value.trim().toLocaleLowerCase()).join("|");
}

function normalizeTrackBookmark(value: unknown): SavedTrackBookmark | null {
  if (!isRecord(value) || !isRecord(value.track) || typeof value.track.name !== "string") {
    return null;
  }
  const track = playerTrackToQueueTrack(value.track as PlayerTrackSummary | PlayerQueueTrack);
  const fallbackId = bookmarkIdentityForTrack(track);
  if (!fallbackId) {
    return null;
  }
  const contextValue = value.context;
  const contextType = isRecord(contextValue) && (
    contextValue.type === "playlist"
    || contextValue.type === "album"
    || contextValue.type === "artist"
    || contextValue.type === "track"
    || contextValue.type === "queue"
    || contextValue.type === "player"
  )
    ? contextValue.type
    : null;
  return {
    id: typeof value.id === "string" && value.id ? value.id : fallbackId,
    bookmarkedAt: typeof value.bookmarkedAt === "string" ? value.bookmarkedAt : new Date().toISOString(),
    track,
    context: contextType && isRecord(contextValue)
      ? {
        type: contextType,
        label: typeof contextValue.label === "string" && contextValue.label ? contextValue.label : "Unknown context",
        url: typeof contextValue.url === "string" ? contextValue.url : null,
        imageUrl: typeof contextValue.imageUrl === "string" ? contextValue.imageUrl : null,
        entityId: typeof contextValue.entityId === "string" ? contextValue.entityId : null,
        position: typeof contextValue.position === "number" && Number.isFinite(contextValue.position) ? contextValue.position : null,
      }
      : null,
  };
}

function entityBookmarkIdentity(bookmark: Pick<SavedEntityBookmark, "type" | "label" | "entityId" | "url"> | null | undefined) {
  if (!bookmark) {
    return null;
  }
  const stableKey = bookmark.entityId ?? bookmark.url ?? bookmark.label;
  return `${bookmark.type}:${String(stableKey).trim().toLocaleLowerCase()}`;
}

function normalizeEntityBookmark(value: unknown): SavedEntityBookmark | null {
  if (!isRecord(value)) {
    return null;
  }
  const type = value.type === "playlist" || value.type === "album" || value.type === "artist" ? value.type : null;
  if (!type || typeof value.label !== "string" || !value.label.trim()) {
    return null;
  }
  const bookmark: SavedEntityBookmark = {
    id: typeof value.id === "string" && value.id ? value.id : `${type}:${value.label.trim().toLocaleLowerCase()}`,
    bookmarkedAt: typeof value.bookmarkedAt === "string" ? value.bookmarkedAt : new Date().toISOString(),
    type,
    label: value.label,
    url: typeof value.url === "string" ? value.url : null,
    imageUrl: typeof value.imageUrl === "string" ? value.imageUrl : null,
    entityId: typeof value.entityId === "string" ? value.entityId : null,
    meta: typeof value.meta === "string" ? value.meta : null,
    detail: typeof value.detail === "string" ? value.detail : null,
  };
  return { ...bookmark, id: entityBookmarkIdentity(bookmark) ?? bookmark.id };
}

function editionAlbumCoreName(name: string | null | undefined) {
  return String(name ?? "")
    .replace(/\s*[\[(]\s*(?:expanded\s+deluxe(?:\s+edition)?|deluxe(?:\s+edition)?|expanded(?:\s+edition)?|(?:\d+(?:st|nd|rd|th)\s+)?anniversary(?:\s+remaster(?:ed)?)?(?:\s+edition)?|remaster(?:ed)?(?:\s+edition)?|mono|stereo|rework)\s*[\])]\s*$/i, "")
    .trim();
}

function normalizedEditionAlbumCoreName(name: string | null | undefined) {
  return editionAlbumCoreName(name).toLocaleLowerCase();
}

type LastPlayedSortMode = "recent" | "oldest" | null;
type ArtistViewMode = "core" | "all";
type ArtistTrackSortKey = "year" | "duration" | "plays" | "last";
type SortDirection = "asc" | "desc";

declare global {
  interface Window {
    onSpotifyWebPlaybackSDKReady?: () => void;
    Spotify?: {
      Player: new (options: {
        name: string;
        getOAuthToken: (callback: (token: string) => void) => void;
        volume?: number;
      }) => SpotifyPlayerInstance;
    };
  }
}

export function App() {
  const [session, setSession] = useState<SessionResponse | null>(null);
  const [profile, setProfile] = useState<ProfileResponse | null>(null);
  const [statusMessage, setStatusMessage] = useState("Checking authentication state...");
  const [statusHistory, setStatusHistory] = useState<string[]>([]);
  const [recentIngestCallbackPending, setRecentIngestCallbackPending] = useState(false);
  const [authTransitioning, setAuthTransitioning] = useState(false);
  const [loadingProfile, setLoadingProfile] = useState(false);
  const [loadingExtendedProfile, setLoadingExtendedProfile] = useState(false);
  const [loadingRecentSection, setLoadingRecentSection] = useState(false);
  const [recentSectionLoadAttempted, setRecentSectionLoadAttempted] = useState(false);
  const [recentSectionStartupError, setRecentSectionStartupError] = useState(false);
  const [startupDashboardReleased, setStartupDashboardReleased] = useState(false);
  const [likedTracksCache, setLikedTracksCache] = useState<LikedTracksResponse | null>(null);
  const [likedTracksLoading, setLikedTracksLoading] = useState(false);
  const [likedTracksSyncing, setLikedTracksSyncing] = useState(false);
  const [likedTracksError, setLikedTracksError] = useState<string | null>(null);
  const [likedTracksLoadAttempted, setLikedTracksLoadAttempted] = useState(false);
  const [targetedLikedTrackById, setTargetedLikedTrackById] = useState<Record<string, boolean>>({});
  const [targetedLikedTrackCheckedById, setTargetedLikedTrackCheckedById] = useState<Record<string, boolean>>({});
  const [localBookmarkedTrackById, setLocalBookmarkedTrackById] = useState<Record<string, boolean>>({});
  const [localStarredTrackById, setLocalStarredTrackById] = useState<Record<string, boolean>>({});
  const [localStarredAlbumById, setLocalStarredAlbumById] = useState<Record<string, boolean>>({});
  const [targetedLikedAlbumById, setTargetedLikedAlbumById] = useState<Record<string, boolean>>({});
  const [targetedLikedAlbumCheckedById, setTargetedLikedAlbumCheckedById] = useState<Record<string, boolean>>({});
  const [trackSummaryChipCache, setTrackSummaryChipCache] = useState<Record<string, {
    durationLabel?: string | null;
    lastListenedLabel?: string | null;
    listenCountLabel?: string | null;
  }>>({});
  const [likedTracksCountMode, setLikedTracksCountMode] = useState<"100" | "all">("100");
  const [likedTracksSortMode, setLikedTracksSortMode] = useState<"recent" | "older">("recent");
  const [likedTracksShuffleEnabled, setLikedTracksShuffleEnabled] = useState(false);
  const [likedTracksShuffleNonce, setLikedTracksShuffleNonce] = useState(0);
  const [releaseTrackMetadataById, setReleaseTrackMetadataById] = useState<Record<string, ReleaseTrackMetadataItem>>({});
  const [releaseTrackMetadataCheckedIds, setReleaseTrackMetadataCheckedIds] = useState<Record<string, true>>({});
  const [loadingHistoryRecompute, setLoadingHistoryRecompute] = useState(false);
  const [profileLoadAttempted, setProfileLoadAttempted] = useState(false);
  const [reloadCooldownUntil, setReloadCooldownUntil] = useState<number | null>(() => {
    const stored = Number(window.localStorage.getItem(SPOTIFY_COOLDOWN_UNTIL_STORAGE_KEY));
    return Number.isFinite(stored) && stored > 0 ? stored : null;
  });
  const [reloadCooldownDurationMs, setReloadCooldownDurationMs] = useState<number>(() => {
    const stored = Number(window.localStorage.getItem(SPOTIFY_COOLDOWN_DURATION_STORAGE_KEY));
    return Number.isFinite(stored) && stored > 0 ? stored : 60_000;
  });
  const [reloadCountdownNow, setReloadCountdownNow] = useState(Date.now());
  const [openSections, setOpenSections] = useState<Record<SectionKey, boolean>>(INITIAL_OPEN_SECTIONS);
  const [sectionPages, setSectionPages] = useState<Record<SectionKey, number>>(INITIAL_SECTION_PAGES);
  const [profileMenuOpen, setProfileMenuOpen] = useState(false);
  const [brandMenuOpen, setBrandMenuOpen] = useState(false);
  const [experimentalMenuOpen, setExperimentalMenuOpen] = useState(false);
  const [profileSettingsOpen, setProfileSettingsOpen] = useState(false);
  const [playerMenuOpen, setPlayerMenuOpen] = useState(false);
  const [selectedPreview, setSelectedPreview] = useState<PreviewItem | null>(null);
  const [selectedPreviewReleaseTrackDetail, setSelectedPreviewReleaseTrackDetail] = useState<ReleaseTrackDetailResponse | null>(null);
  const [selectedPreviewReleaseTrackDetailLoading, setSelectedPreviewReleaseTrackDetailLoading] = useState(false);
  const [selectedPreviewReleaseTrackDetailError, setSelectedPreviewReleaseTrackDetailError] = useState<string | null>(null);
  const [selectedPreviewRecordingCandidate, setSelectedPreviewRecordingCandidate] = useState<RecordingTrackCandidateItem | null>(null);
  const [selectedPreviewRelatedCandidates, setSelectedPreviewRelatedCandidates] = useState<RecordingTrackCandidateItem[]>([]);
  const [selectedPreviewRecordingCandidateError, setSelectedPreviewRecordingCandidateError] = useState<string | null>(null);
  const [selectedPreviewDetailView, setSelectedPreviewDetailView] = useState<"recording" | "release">("recording");
  const [recordingAlbumTracklistOpen, setRecordingAlbumTracklistOpen] = useState(false);
  const [detailOptionsOpen, setDetailOptionsOpen] = useState(false);
  const [artistAlbumEvidenceItems, setArtistAlbumEvidenceItems] = useState<ArtistAlbumEvidenceItem[] | null>(null);
  const [artistTrackEvidenceItems, setArtistTrackEvidenceItems] = useState<ArtistTrackEvidenceItem[] | null>(null);
  const [artistAlbumEvidenceLoading, setArtistAlbumEvidenceLoading] = useState(false);
  const [artistViewMode, setArtistViewMode] = useState<ArtistViewMode>("core");
  const [artistIncludeSingles, setArtistIncludeSingles] = useState(false);
  const [artistTrackSort, setArtistTrackSort] = useState<{ key: ArtistTrackSortKey; direction: SortDirection }>({
    key: "last",
    direction: "desc",
  });
  const [albumTrackEntries, setAlbumTrackEntries] = useState<AlbumTrackEntry[]>([]);
  const [albumTrackEntriesLoading, setAlbumTrackEntriesLoading] = useState(false);
  const [albumTrackEntriesError, setAlbumTrackEntriesError] = useState<string | null>(null);
  const [albumTrackEntriesPartial, setAlbumTrackEntriesPartial] = useState(false);
  const [selectedAlbumFamilyContext, setSelectedAlbumFamilyContext] = useState<AlbumFamilyContext | null>(null);
  const [albumFamilyDiscScrollTarget, setAlbumFamilyDiscScrollTarget] = useState<number | null>(null);
  const [albumTrackSpotifyFetchRequest, setAlbumTrackSpotifyFetchRequest] = useState<{
    albumId: string | null;
    trackId: string | null;
    nonce: number;
  } | null>(null);
  const [albumTrackSpotifyFetchPending, setAlbumTrackSpotifyFetchPending] = useState(false);
  const [albumTrackLastSortMode, setAlbumTrackLastSortMode] = useState<LastPlayedSortMode>(null);
  const [playlistTrackEntries, setPlaylistTrackEntries] = useState<RecentTrack[]>([]);
  const [playlistTrackEntriesLoading, setPlaylistTrackEntriesLoading] = useState(false);
  const [playlistTrackEntriesError, setPlaylistTrackEntriesError] = useState<string | null>(null);
  const [playlistTrackEntriesHasMore, setPlaylistTrackEntriesHasMore] = useState(false);
  const [playlistTrackEntriesTotal, setPlaylistTrackEntriesTotal] = useState<number | null>(null);
  const [playlistTrackEntriesNextOffset, setPlaylistTrackEntriesNextOffset] = useState<number | null>(null);
  const [playlistTrackEntriesOffset, setPlaylistTrackEntriesOffset] = useState(0);
  const playlistTrackEntriesCacheRef = useRef<Record<string, { items: RecentTrack[]; hasMore: boolean; total: number | null; nextOffset: number | null }>>({});
  const [selectedPreviewPlaylistMemberships, setSelectedPreviewPlaylistMemberships] = useState<PlaylistMembership[]>([]);
  const [selectedPreviewPlaylistMembershipsLoading, setSelectedPreviewPlaylistMembershipsLoading] = useState(false);
  const [selectedPreviewPlaylistIndexStatus, setSelectedPreviewPlaylistIndexStatus] = useState<PlaylistIndexStatus | null>(null);
  const [hoveredAlbumWithArtistName, setHoveredAlbumWithArtistName] = useState<string | null>(null);
  const [homeAlbumExpanded, setHomeAlbumExpanded] = useState(false);
  const [homeAlbumTrackEntries, setHomeAlbumTrackEntries] = useState<AlbumTrackEntry[]>([]);
  const [homeAlbumTrackEntriesLoading, setHomeAlbumTrackEntriesLoading] = useState(false);
  const [homeAlbumTrackEntriesError, setHomeAlbumTrackEntriesError] = useState<string | null>(null);
  const [homeAlbumTrackLastSortMode, setHomeAlbumTrackLastSortMode] = useState<LastPlayedSortMode>(null);
  const [homeRecordingCandidate, setHomeRecordingCandidate] = useState<RecordingTrackCandidateItem | null>(null);
  const [playerDrawerExpanded, setPlayerDrawerExpanded] = useState(false);
  const [playerDrawerActiveTab, setPlayerDrawerActiveTab] = useState<PlayerBottomDrawerTab>("previousQueues");
  const [savedPlayerQueues, setSavedPlayerQueues] = useState<SavedPlayerQueueSnapshot[]>([]);
  const [trackBookmarks, setTrackBookmarks] = useState<SavedTrackBookmark[]>([]);
  const [entityBookmarks, setEntityBookmarks] = useState<SavedEntityBookmark[]>([]);
  const [playerReady, setPlayerReady] = useState(false);
  const [playerError, setPlayerError] = useState<string | null>(null);
  const [playerRecentTracks, setPlayerRecentTracks] = useState<RecentTrack[]>([]);
  const [playerRecentTracksLoading, setPlayerRecentTracksLoading] = useState(false);
  const [playerRecentTracksError, setPlayerRecentTracksError] = useState<string | null>(null);
  const [playerRecentTracksLoadAttempted, setPlayerRecentTracksLoadAttempted] = useState(false);
  const [playerQueueTracks, setPlayerQueueTracks] = useState<PlayerQueueTrack[]>([]);
  const [playerQueueGroups, setPlayerQueueGroups] = useState<PlayerQueueGroup[]>([]);
  const [playerQueueGroupCursors, setPlayerQueueGroupCursors] = useState<Record<string, number>>({});
  const [playerQueueCursor, setPlayerQueueCursor] = useState<number | null>(null);
  const [playerQueueSource, setPlayerQueueSource] = useState<"listenlab" | "spotify" | null>(null);
  const [playerQueueShuffleEnabled, setPlayerQueueShuffleEnabled] = useState(false);
  const [playerQueueShuffleBaseTracks, setPlayerQueueShuffleBaseTracks] = useState<PlayerQueueTrack[] | null>(null);
  const [playerQueueSettingsOpen, setPlayerQueueSettingsOpen] = useState(false);
  const [playerQueueOrganizeMode, setPlayerQueueOrganizeMode] = useState(false);
  const [homeQueueOpenGroupIds, setHomeQueueOpenGroupIds] = useState<Set<string>>(() => new Set());
  const [homeQueueHeaderMenuOpen, setHomeQueueHeaderMenuOpen] = useState(false);
  const [playerQueueSortMode, setPlayerQueueSortMode] = useState<"custom" | "length" | "az" | "recent">("custom");
  const [playerQueueGroupMode, setPlayerQueueGroupMode] = useState<"custom" | "artist" | "album">("custom");
  const [playerQueueDragIndex, setPlayerQueueDragIndex] = useState<number | null>(null);
  const [playerQueueCleared, setPlayerQueueCleared] = useState(false);
  const [playerQueueLoopEnabled, setPlayerQueueLoopEnabled] = useState(false);
  const [playerTrackLoopEnabled, setPlayerTrackLoopEnabled] = useState(false);
  const [playerQueueContext, setPlayerQueueContext] = useState<PlayerQueueContext | null>(null);
  const [playerQueuePlayedKeys, setPlayerQueuePlayedKeys] = useState<Set<string>>(() => new Set());
  const [playerQueuePauseMenuOpen, setPlayerQueuePauseMenuOpen] = useState(false);
  const [queuePauseAfterCurrentEnabled, setQueuePauseAfterCurrentEnabled] = useState(false);
  const [queueSleepTimerUntilMs, setQueueSleepTimerUntilMs] = useState<number | null>(null);
  const [queuePausedCursor, setQueuePausedCursor] = useState<number | null>(null);
  const [queuePlaylistUri, setQueuePlaylistUri] = useState<string | null>(null);
  const [playerQueueLoading, setPlayerQueueLoading] = useState(false);
  const [playerQueueError, setPlayerQueueError] = useState<string | null>(null);
  const [playerQueueLoadAttempted, setPlayerQueueLoadAttempted] = useState(false);
  const [activePlayerListenEventId, setActivePlayerListenEventId] = useState<number | null>(null);
  const [currentTrack, setCurrentTrack] = useState<PlayerTrackSummary | null>(null);
  const [playbackPaused, setPlaybackPaused] = useState(true);
  const [playbackPositionMs, setPlaybackPositionMs] = useState(0);
  const [playbackDurationMs, setPlaybackDurationMs] = useState(0);
  const [overlayTrackPlaybackExpanded, setOverlayTrackPlaybackExpanded] = useState(false);
  const [overlaySeekMs, setOverlaySeekMs] = useState<number | null>(null);
  const [pausedTimeFlashOn, setPausedTimeFlashOn] = useState(true);
  const [previewingTrackUri, setPreviewingTrackUri] = useState<string | null>(null);
  const [previewPlaybackSession, setPreviewPlaybackSessionState] = useState<{
    baseTrack: PlayerTrackSummary | null;
    basePositionMs: number;
    baseDurationMs: number;
    basePaused: boolean;
    previewTrack: PlayerTrackSummary;
  } | null>(null);
  const [previewPlayedTrackKeys, setPreviewPlayedTrackKeys] = useState<Set<string>>(new Set());
  const [livePlaybackSnapshot, setLivePlaybackSnapshot] = useState<CurrentPlaybackSnapshot | null>(null);
  const [liveDerivedProgressMs, setLiveDerivedProgressMs] = useState(0);
  const [liveAwaitingNextTrack, setLiveAwaitingNextTrack] = useState(false);
  const [livePlaybackProbeComplete, setLivePlaybackProbeComplete] = useState(false);
  const [pendingSeekMs, setPendingSeekMs] = useState<number | null>(null);
  const [liveControlOverrideUntilMs, setLiveControlOverrideUntilMs] = useState<number | null>(null);
  const [recentRange, setRecentRange] = useState<RecentRange>("short_term");
  const [recentCompletionFilter, setRecentCompletionFilter] = useState<RecentCompletionFilter>("listened");
  const [recentLikedOnly, setRecentLikedOnly] = useState(false);
  const [likedActivityTracks, setLikedActivityTracks] = useState<RecentTrack[] | null>(null);
  const [recentTaggedOnly, setRecentTaggedOnly] = useState(false);
  const [trackRankingMode, setTrackRankingMode] = useState<TrackRankingMode>("plays");
  const [trackRankingRefreshPending, setTrackRankingRefreshPending] = useState(false);
  const [appPage, setAppPage] = useState<AppPage>("dashboard");
  const [showDebugLinkFields, setShowDebugLinkFields] = useState(false);
  const [openDebugSessions, setOpenDebugSessions] = useState<Record<string, boolean>>({});
  const [openDebugTracks, setOpenDebugTracks] = useState<Record<string, boolean>>({});
  const [listeningLogTracks, setListeningLogTracks] = useState<RecentTrack[]>([]);
  const [listeningLogHasMore, setListeningLogHasMore] = useState(false);
  const [listeningLogOffset, setListeningLogOffset] = useState(0);
  const [listeningLogLoading, setListeningLogLoading] = useState(false);
  const [listeningLogLoaded, setListeningLogLoaded] = useState(false);
  const [listeningLogError, setListeningLogError] = useState("");
  const [listeningLogLastLoadedAt, setListeningLogLastLoadedAt] = useState<number | null>(null);
  const [recentDebugSourceFilter, setRecentDebugSourceFilter] = useState<RecentDebugSourceFilter>("all");
  const [catalogBackfillCoverage, setCatalogBackfillCoverage] = useState<CatalogBackfillCoverageResponse | null>(null);
  const [catalogBackfillCoverageLoading, setCatalogBackfillCoverageLoading] = useState(false);
  const [catalogBackfillCoverageLoaded, setCatalogBackfillCoverageLoaded] = useState(false);
  const [catalogBackfillCoverageError, setCatalogBackfillCoverageError] = useState("");
  const [catalogBackfillCoverageLastLoadedAt, setCatalogBackfillCoverageLastLoadedAt] = useState<number | null>(null);
  const [catalogBackfillRuns, setCatalogBackfillRuns] = useState<CatalogBackfillRunsResponse | null>(null);
  const [catalogBackfillRunsLoading, setCatalogBackfillRunsLoading] = useState(false);
  const [catalogBackfillRunsLoaded, setCatalogBackfillRunsLoaded] = useState(false);
  const [catalogBackfillRunsError, setCatalogBackfillRunsError] = useState("");
  const [catalogBackfillRunsLastLoadedAt, setCatalogBackfillRunsLastLoadedAt] = useState<number | null>(null);
  const [catalogBackfillQueue, setCatalogBackfillQueue] = useState<CatalogBackfillQueueResponse | null>(null);
  const [catalogBackfillQueueLoading, setCatalogBackfillQueueLoading] = useState(false);
  const [catalogBackfillQueueLoaded, setCatalogBackfillQueueLoaded] = useState(false);
  const [catalogBackfillQueueError, setCatalogBackfillQueueError] = useState("");
  const [catalogBackfillQueueLastLoadedAt, setCatalogBackfillQueueLastLoadedAt] = useState<number | null>(null);
  const [catalogBackfillQueueStatusFilter, setCatalogBackfillQueueStatusFilter] = useState<"all" | "pending" | "done" | "error">("all");
  const [catalogBackfillQueueReasonFilter, setCatalogBackfillQueueReasonFilter] = useState<CatalogBackfillQueueReasonFilter>("all");
  const [catalogBackfillTab, setCatalogBackfillTab] = useState<CatalogBackfillTab>("priorityMetadata");
  const [catalogBackfillQueueRepairLoading, setCatalogBackfillQueueRepairLoading] = useState(false);
  const [catalogBackfillQueueRepairMessage, setCatalogBackfillQueueRepairMessage] = useState("");
  const [catalogBackfillRunLoading, setCatalogBackfillRunLoading] = useState(false);
  const [catalogBackfillRunError, setCatalogBackfillRunError] = useState("");
  const [catalogBackfillLatestResult, setCatalogBackfillLatestResult] = useState<CatalogBackfillRunResponse | null>(null);
  const [catalogBackfillLimit, setCatalogBackfillLimit] = useState(25);
  const [catalogBackfillOffset, setCatalogBackfillOffset] = useState(0);
  const [catalogBackfillMarket, setCatalogBackfillMarket] = useState("US");
  const [catalogBackfillIncludeAlbums, setCatalogBackfillIncludeAlbums] = useState(true);
  const [catalogBackfillForceRefresh, setCatalogBackfillForceRefresh] = useState(false);
  const [catalogBackfillRequestDelaySeconds, setCatalogBackfillRequestDelaySeconds] = useState(2.0);
  const [catalogBackfillMaxRuntimeSeconds, setCatalogBackfillMaxRuntimeSeconds] = useState(60);
  const [catalogBackfillMaxRequests, setCatalogBackfillMaxRequests] = useState(150);
  const [catalogBackfillMaxErrors, setCatalogBackfillMaxErrors] = useState(10);
  const [catalogBackfillMaxAlbumTracksPagesPerAlbum, setCatalogBackfillMaxAlbumTracksPagesPerAlbum] = useState(10);
  const [catalogBackfillMax429, setCatalogBackfillMax429] = useState(3);
  const [catalogBackfillAlbumTracklistPolicy, setCatalogBackfillAlbumTracklistPolicy] = useState<"all" | "priority_only" | "relevant_albums" | "none">("relevant_albums");
  const [catalogBackfillFullRunMode, setCatalogBackfillFullRunMode] = useState<"tracklists_relevant" | "full_catalog">("tracklists_relevant");
  const [searchLookupEntityType, setSearchLookupEntityType] = useState<"albums" | "tracks">("albums");
  const [searchLookupQueueStatus, setSearchLookupQueueStatus] = useState<"all" | "not_queued" | "pending" | "done" | "error">("all");
  const [searchLookupSort, setSearchLookupSort] = useState<"default" | "recently_backfilled" | "name" | "incomplete_first">("default");
  const [albumCatalogLookupQ, setAlbumCatalogLookupQ] = useState("");
  const [albumCatalogLookupStatus, setAlbumCatalogLookupStatus] = useState<"all" | "backfilled" | "not_backfilled" | "tracklist_complete" | "tracklist_incomplete" | "error">("all");
  const [albumCatalogLookupResult, setAlbumCatalogLookupResult] = useState<AlbumCatalogLookupResponse | null>(null);
  const [albumCatalogLookupLoading, setAlbumCatalogLookupLoading] = useState(false);
  const [albumCatalogLookupLoaded, setAlbumCatalogLookupLoaded] = useState(false);
  const [albumCatalogLookupError, setAlbumCatalogLookupError] = useState("");
  const [albumCatalogLookupLastLoadedAt, setAlbumCatalogLookupLastLoadedAt] = useState<number | null>(null);
  const [trackCatalogLookupStatus, setTrackCatalogLookupStatus] = useState<"all" | "backfilled" | "not_backfilled" | "duration_missing" | "error">("all");
  const [trackCatalogLookupResult, setTrackCatalogLookupResult] = useState<TrackCatalogLookupResponse | null>(null);
  const [trackCatalogLookupLoading, setTrackCatalogLookupLoading] = useState(false);
  const [trackCatalogLookupLoaded, setTrackCatalogLookupLoaded] = useState(false);
  const [trackCatalogLookupError, setTrackCatalogLookupError] = useState("");
  const [trackCatalogLookupLastLoadedAt, setTrackCatalogLookupLastLoadedAt] = useState<number | null>(null);
  const [trackMappingLineageResult, setTrackMappingLineageResult] = useState<TrackMappingLineageResponse | null>(null);
  const [trackMappingLineageLoading, setTrackMappingLineageLoading] = useState(false);
  const [trackMappingLineageLoaded, setTrackMappingLineageLoaded] = useState(false);
  const [trackMappingLineageError, setTrackMappingLineageError] = useState("");
  const [trackMappingLineageLastLoadedAt, setTrackMappingLineageLastLoadedAt] = useState<number | null>(null);
  const [trackMappingKindFilter, setTrackMappingKindFilter] = useState<"all" | "source_release" | "release_family">("source_release");
  const [trackMappingConfirmationFilter, setTrackMappingConfirmationFilter] = useState<"all" | "confirmed" | "unconfirmed">("all");
  const [trackMappingSourceMetadataFilter, setTrackMappingSourceMetadataFilter] = useState<"all" | "complete" | "incomplete">("all");
  const [trackMappingCertaintyFilter, setTrackMappingCertaintyFilter] = useState<"all" | "certain" | "uncertain">("all");
  const [albumDuplicateLookupResult, setAlbumDuplicateLookupResult] = useState<AlbumDuplicateLookupResponse | null>(null);
  const [albumDuplicateLookupLoading, setAlbumDuplicateLookupLoading] = useState(false);
  const [albumDuplicateLookupLoaded, setAlbumDuplicateLookupLoaded] = useState(false);
  const [albumDuplicateLookupError, setAlbumDuplicateLookupError] = useState("");
  const [albumDuplicateLookupLastLoadedAt, setAlbumDuplicateLookupLastLoadedAt] = useState<number | null>(null);
  const [albumNameDuplicateLookupResult, setAlbumNameDuplicateLookupResult] = useState<AlbumNameDuplicateLookupResponse | null>(null);
  const [albumNameDuplicateLookupLoading, setAlbumNameDuplicateLookupLoading] = useState(false);
  const [albumNameDuplicateLookupLoaded, setAlbumNameDuplicateLookupLoaded] = useState(false);
  const [albumNameDuplicateLookupError, setAlbumNameDuplicateLookupError] = useState("");
  const [albumNameDuplicateLookupLastLoadedAt, setAlbumNameDuplicateLookupLastLoadedAt] = useState<number | null>(null);
  const [releaseAlbumMergePreviewByKey, setReleaseAlbumMergePreviewByKey] = useState<Record<string, ReleaseAlbumMergePreviewResponse>>({});
  const [releaseAlbumMergePreviewLoadingKey, setReleaseAlbumMergePreviewLoadingKey] = useState<string | null>(null);
  const [releaseAlbumMergePreviewErrorByKey, setReleaseAlbumMergePreviewErrorByKey] = useState<Record<string, string>>({});
  const [releaseAlbumMergeDryRunByKey, setReleaseAlbumMergeDryRunByKey] = useState<Record<string, ReleaseAlbumMergeDryRunResponse>>({});
  const [releaseAlbumMergeDryRunLoadingKey, setReleaseAlbumMergeDryRunLoadingKey] = useState<string | null>(null);
  const [releaseAlbumMergeDryRunErrorByKey, setReleaseAlbumMergeDryRunErrorByKey] = useState<Record<string, string>>({});
  const [selectedAlbumMergeReviewKey, setSelectedAlbumMergeReviewKey] = useState<string | null>(null);
  const [albumSpotifyDuplicateFilter, setAlbumSpotifyDuplicateFilter] = useState<"all" | "not_previewed" | "safe_candidate" | "needs_review" | "unsafe">("all");
  const [albumSpotifyDuplicateReasonFilter, setAlbumSpotifyDuplicateReasonFilter] = useState("all");
  const [albumSpotifyDuplicateBulkPreviewLoading, setAlbumSpotifyDuplicateBulkPreviewLoading] = useState(false);
  const [albumNameDuplicateGroupFilter, setAlbumNameDuplicateGroupFilter] = useState<"all" | "single_spotify_id" | "multiple_spotify_ids" | "no_spotify_id">("all");
  const [albumNameDuplicatePreviewFilter, setAlbumNameDuplicatePreviewFilter] = useState<"all" | "not_previewed" | "safe_candidate" | "needs_review" | "unsafe">("all");
  const [albumNameDuplicateReasonFilter, setAlbumNameDuplicateReasonFilter] = useState("all");
  const [albumNameDuplicateBulkPreviewLoading, setAlbumNameDuplicateBulkPreviewLoading] = useState(false);
  const [trackDuplicateLookupResult, setTrackDuplicateLookupResult] = useState<TrackDuplicateLookupResponse | null>(null);
  const [trackDuplicateLookupLoading, setTrackDuplicateLookupLoading] = useState(false);
  const [trackDuplicateLookupLoaded, setTrackDuplicateLookupLoaded] = useState(false);
  const [trackDuplicateLookupError, setTrackDuplicateLookupError] = useState("");
  const [trackDuplicateLookupLastLoadedAt, setTrackDuplicateLookupLastLoadedAt] = useState<number | null>(null);
  const [albumCatalogLookupEnqueueLoading, setAlbumCatalogLookupEnqueueLoading] = useState(false);
  const [albumCatalogLookupEnqueueError, setAlbumCatalogLookupEnqueueError] = useState("");
  const [albumCatalogLookupEnqueueResult, setAlbumCatalogLookupEnqueueResult] = useState<CatalogBackfillEnqueueResponse | null>(null);
  const [mergedTracks, setMergedTracks] = useState<RecentTrack[]>([]);
  const [mergedTracksLoading, setMergedTracksLoading] = useState(false);
  const [mergedTracksLoaded, setMergedTracksLoaded] = useState(false);
  const [mergedTracksError, setMergedTracksError] = useState("");
  const [mergedTracksExcludedUnknownCount, setMergedTracksExcludedUnknownCount] = useState(0);
  const [mergedTracksLastLoadedAt, setMergedTracksLastLoadedAt] = useState<number | null>(null);
  const [recentComputedTracks, setRecentComputedTracks] = useState<RecentTrack[]>([]);
  const [recentComputedTracksLoading, setRecentComputedTracksLoading] = useState(false);
  const [recentComputedTracksLoaded, setRecentComputedTracksLoaded] = useState(false);
  const [recentComputedTracksError, setRecentComputedTracksError] = useState("");
  const [mergedTrackSourceFilter, setMergedTrackSourceFilter] = useState<MergedTrackSourceFilter>("all");
  const [recentTopTracksUseSpotify, setRecentTopTracksUseSpotify] = useState(false);
  const [rankMovementFilter, setRankMovementFilter] = useState<RankMovementFilter>("all");
  const [identityAudit, setIdentityAudit] = useState<TrackIdentityAuditResponse | null>(null);
  const [identityAuditLoading, setIdentityAuditLoading] = useState(false);
  const [identityAuditLoaded, setIdentityAuditLoaded] = useState(false);
  const [identityAuditError, setIdentityAuditError] = useState("");
  const [identityAuditLastLoadedAt, setIdentityAuditLastLoadedAt] = useState<number | null>(null);
  const [identityAuditEntityTab, setIdentityAuditEntityTab] = useState<IdentityAuditEntityTab>(() => loadIdentityAuditPersistedPrefs().entityTab ?? "tracks");
  const [trackIdentityAuditTab, setTrackIdentityAuditTab] = useState<TrackIdentityAuditTab>(() => loadIdentityAuditPersistedPrefs().trackTab ?? "problems");
  const [albumIdentityAuditTab, setAlbumIdentityAuditTab] = useState<AlbumIdentityAuditTab>(() => loadIdentityAuditPersistedPrefs().albumTab ?? "problems");
  const [trackIdentityAuditIssueSort, setTrackIdentityAuditIssueSort] = useState<IdentityAuditIssueSort>(() => loadIdentityAuditPersistedPrefs().trackIssueSort ?? "severity");
  const [albumIdentityAuditIssueSort, setAlbumIdentityAuditIssueSort] = useState<IdentityAuditIssueSort>(() => loadIdentityAuditPersistedPrefs().albumIssueSort ?? "severity");
  const [identityAuditIssueReviewState, setIdentityAuditIssueReviewState] = useState<Record<string, IdentityAuditIssueReviewState>>(() => loadIdentityAuditPersistedPrefs().issueReviewState ?? {});
  const [identityAuditExpandedIssueKeys, setIdentityAuditExpandedIssueKeys] = useState<Record<string, boolean>>(() => loadIdentityAuditPersistedPrefs().expandedIssueKeys ?? {});
  const [identityAuditSuggestedGroups, setIdentityAuditSuggestedGroups] = useState<SuggestedGroupsResponse | null>(null);
  const [identityAuditSuggestedLoading, setIdentityAuditSuggestedLoading] = useState(false);
  const [identityAuditSuggestedLoaded, setIdentityAuditSuggestedLoaded] = useState(false);
  const [identityAuditSuggestedError, setIdentityAuditSuggestedError] = useState("");
  const [identityAuditSuggestedLastLoadedAt, setIdentityAuditSuggestedLastLoadedAt] = useState<number | null>(null);
  const [identityAuditAmbiguous, setIdentityAuditAmbiguous] = useState<AmbiguousReviewResponse | null>(null);
  const [identityAuditAmbiguousLoading, setIdentityAuditAmbiguousLoading] = useState(false);
  const [identityAuditAmbiguousLoaded, setIdentityAuditAmbiguousLoaded] = useState(false);
  const [identityAuditAmbiguousError, setIdentityAuditAmbiguousError] = useState("");
  const [identityAuditAmbiguousLastLoadedAt, setIdentityAuditAmbiguousLastLoadedAt] = useState<number | null>(null);
  const [identityAuditAmbiguousFamilyFilter, setIdentityAuditAmbiguousFamilyFilter] = useState("all");
  const [identityAuditAmbiguousBucketFilter, setIdentityAuditAmbiguousBucketFilter] = useState<"all" | "grouped" | "ungrouped">("all");
  const [identityAuditAmbiguousVisibleCount, setIdentityAuditAmbiguousVisibleCount] = useState(IDENTITY_AUDIT_AMBIGUOUS_VISIBLE_STEP);
  const [identityAuditLocalDecisions, setIdentityAuditLocalDecisions] = useState<Record<string, LocalReviewDecision>>({});
  const [identityAuditFocusedReviewKey, setIdentityAuditFocusedReviewKey] = useState<string | null>(null);
  const [identityAuditPreviewCopyStatus, setIdentityAuditPreviewCopyStatus] = useState("");
  const [identityAuditPreviewValidationLoading, setIdentityAuditPreviewValidationLoading] = useState(false);
  const [identityAuditPreviewValidationError, setIdentityAuditPreviewValidationError] = useState("");
  const [identityAuditPreviewValidationResult, setIdentityAuditPreviewValidationResult] = useState<SubmissionPreviewValidationResponse | null>(null);
  const [identityAuditPreviewValidatedAt, setIdentityAuditPreviewValidatedAt] = useState<number | null>(null);
  const [identityAuditSubmissionSaveLoading, setIdentityAuditSubmissionSaveLoading] = useState(false);
  const [identityAuditSubmissionSaveError, setIdentityAuditSubmissionSaveError] = useState("");
  const [identityAuditSubmissionSaveResult, setIdentityAuditSubmissionSaveResult] = useState<IdentityAuditSubmissionSaveResponse | null>(null);
  const [identityAuditSavedSubmissions, setIdentityAuditSavedSubmissions] = useState<IdentityAuditSavedSubmissionListResponse | null>(null);
  const [identityAuditSavedSubmissionsLoading, setIdentityAuditSavedSubmissionsLoading] = useState(false);
  const [identityAuditSavedSubmissionsError, setIdentityAuditSavedSubmissionsError] = useState("");
  const [identityAuditSavedSubmissionDetail, setIdentityAuditSavedSubmissionDetail] = useState<IdentityAuditSavedSubmissionReadResponse | null>(null);
  const [identityAuditSavedSubmissionDetailLoading, setIdentityAuditSavedSubmissionDetailLoading] = useState(false);
  const [identityAuditSavedSubmissionDetailError, setIdentityAuditSavedSubmissionDetailError] = useState("");
  const [identityAuditSavedSubmissionDryRun, setIdentityAuditSavedSubmissionDryRun] = useState<IdentityAuditSubmissionDryRunResponse | null>(null);
  const [identityAuditSavedSubmissionDryRunLoading, setIdentityAuditSavedSubmissionDryRunLoading] = useState(false);
  const [identityAuditSavedSubmissionDryRunError, setIdentityAuditSavedSubmissionDryRunError] = useState("");
  const [identityAuditSavedSubmissionDryRunAt, setIdentityAuditSavedSubmissionDryRunAt] = useState<number | null>(null);
  const [recentRangeRefreshPending, setRecentRangeRefreshPending] = useState(false);
  const [analysisMode, setAnalysisMode] = useState<AnalysisMode>("quick");
  const [experienceMode, setExperienceMode] = useState<ExperienceMode>(() => {
    const stored = window.localStorage.getItem(EXPERIENCE_MODE_STORAGE_KEY);
    return stored === "local" ? "local" : "full";
  });
  const [testingFullExperience, setTestingFullExperience] = useState(false);
  const [testFullSuccessPinned, setTestFullSuccessPinned] = useState(false);
  const [testProbeModeVisual, setTestProbeModeVisual] = useState<ExperienceVisualMode | null>(null);
  const profileMenuRef = useRef<HTMLDivElement | null>(null);
  const brandMenuRef = useRef<HTMLDivElement | null>(null);
  const experimentalMenuRef = useRef<HTMLDivElement | null>(null);
  const playerMenuRef = useRef<HTMLDivElement | null>(null);
  const homeQueueListRef = useRef<HTMLDivElement | null>(null);
  const spotifyPlayerRef = useRef<SpotifyPlayerInstance | null>(null);
  const spotifyDeviceIdRef = useRef<string | null>(null);
  const previewStopTimerRef = useRef<number | null>(null);
  const previewVolumeRampTimerRef = useRef<number | null>(null);
  const previewingTrackUriRef = useRef<string | null>(null);
  const previewPlaybackSessionRef = useRef<{
    baseTrack: PlayerTrackSummary | null;
    basePositionMs: number;
    baseDurationMs: number;
    basePaused: boolean;
    previewTrack: PlayerTrackSummary;
  } | null>(null);
  const currentPlayerVolumeRef = useRef(DEFAULT_PLAYER_VOLUME);
  const loadedAlbumTracksAlbumIdRef = useRef<string | null>(null);
  const albumTrackRowsCacheRef = useRef<Map<string, { albumId: string; rows: AlbumTrackEntry[]; partial: boolean; family: AlbumFamilyContext | null }>>(new Map());
  const albumTrackSpotifyAutoFetchAttemptedRef = useRef<Set<string>>(new Set());
  const loadedHomeAlbumTracksAlbumIdRef = useRef<string | null>(null);
  const albumTrackListRef = useRef<HTMLUListElement>(null);
  const homeAlbumTrackListRef = useRef<HTMLUListElement | null>(null);
  const autoScrolledAlbumTracklistKeyRef = useRef<string | null>(null);
  const preserveRecordingAlbumTracklistOpenRef = useRef(false);
  const recordingVariationStripRef = useRef<HTMLDivElement | null>(null);
  const albumWithHoverDelayRef = useRef<number | null>(null);
  const trackMappingLineageRequestIdRef = useRef(0);
  const playbackPositionMsRef = useRef(0);
  const autoAdvanceTrackUriRef = useRef<string | null>(null);
  const queueSkipHoldTimerRef = useRef<number | null>(null);
  const queueSkipHoldHandledRef = useRef(false);
  const optimisticQualifiedListenEventIdsRef = useRef<Set<number>>(new Set());
  const liveProgressAnchorRef = useRef<{ baseProgressMs: number; receivedAtMs: number; durationMs: number } | null>(null);
  const liveEndRefreshRequestedRef = useRef(false);
  const liveListenQualificationRef = useRef<{
    trackId: string | null;
    lastProgressMs: number;
    eventId: string | null;
    submitted: boolean;
  }>({ trackId: null, lastProgressMs: 0, eventId: null, submitted: false });
  const profileLoadInFlightRef = useRef(false);
  const extendedLoadInFlightRef = useRef(false);
  const quickProfileLoadInFlightRef = useRef(false);
  const quickRecentAutoAttemptRef = useRef<string | null>(null);
  const recentSectionLoadInFlightRef = useRef(false);
  const listeningLogLoadInFlightRef = useRef(false);
  const releaseTrackMetadataInFlightIdsRef = useRef<Set<string>>(new Set());
  const targetedLikedInFlightIdsRef = useRef<Set<string>>(new Set());
  const targetedLikedAlbumInFlightIdsRef = useRef<Set<string>>(new Set());
  const hasPremiumPlayback = profile?.product?.toLowerCase() === "premium";
  const usingLivePlaybackSnapshot = Boolean(livePlaybackSnapshot);
  const livePlaybackTrackSummary: PlayerTrackSummary | null = useMemo(() => (livePlaybackSnapshot
    ? {
      name: livePlaybackSnapshot.name ?? "Spotify Playback",
      artists: (livePlaybackSnapshot.artist_names ?? []).join(", ") || "Unknown artist",
      album: livePlaybackSnapshot.album_name ?? "Unknown album",
      image: livePlaybackSnapshot.image_url ?? null,
      uri: livePlaybackSnapshot.uri ?? null,
      durationMs: Math.max(0, Number(livePlaybackSnapshot.duration_ms ?? 0)),
    }
    : null), [livePlaybackSnapshot]);
  const liveControlOverrideActive = Boolean(
    liveControlOverrideUntilMs != null
    && liveControlOverrideUntilMs > Date.now()
    && playerReady,
  );
  const livePlaybackOnListenLabDevice = Boolean(
    usingLivePlaybackSnapshot
    && (
      (
        livePlaybackSnapshot?.device_id
        && spotifyDeviceIdRef.current
        && livePlaybackSnapshot.device_id === spotifyDeviceIdRef.current
      )
      || ((livePlaybackSnapshot?.device_name ?? "").toLocaleLowerCase().includes("listenlab"))
    ),
  );
  const liveSpotifyPlaybackShouldOwnQueue = usingLivePlaybackSnapshot && !livePlaybackOnListenLabDevice;
  const liveReadOnlyMode = usingLivePlaybackSnapshot && !livePlaybackOnListenLabDevice && !liveControlOverrideActive;
  const startupPlaybackReady = experienceMode === "local"
    || !hasPremiumPlayback
    || livePlaybackProbeComplete;
  const startupRecentReady = experienceMode === "local"
    || analysisMode !== "quick"
    || Boolean(profile?.recent_tracks_available)
    || (Boolean(profile) && recentSectionLoadAttempted && !loadingRecentSection);
  const startupReadyForDashboard = Boolean(profile && startupPlaybackReady && startupRecentReady);
  useEffect(() => {
    if (startupReadyForDashboard && !startupDashboardReleased) {
      setStartupDashboardReleased(true);
    }
  }, [startupDashboardReleased, startupReadyForDashboard]);
  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(SAVED_PLAYER_QUEUES_STORAGE_KEY);
      const parsed: unknown = raw ? JSON.parse(raw) : [];
      const snapshots = Array.isArray(parsed)
        ? parsed.flatMap((item) => {
          const snapshot = normalizeSavedPlayerQueueSnapshot(item);
          return snapshot ? [snapshot] : [];
        }).slice(0, 25)
        : [];
      setSavedPlayerQueues(snapshots);
      if (raw) {
        window.localStorage.setItem(SAVED_PLAYER_QUEUES_STORAGE_KEY, JSON.stringify(snapshots));
      }
    } catch {
      setSavedPlayerQueues([]);
    }
  }, []);
  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(TRACK_BOOKMARKS_STORAGE_KEY);
      const parsed: unknown = raw ? JSON.parse(raw) : [];
      const bookmarks = Array.isArray(parsed)
        ? parsed.flatMap((item) => {
          const bookmark = normalizeTrackBookmark(item);
          return bookmark ? [bookmark] : [];
        }).slice(0, 100)
        : [];
      setTrackBookmarks(bookmarks);
      if (raw) {
        window.localStorage.setItem(TRACK_BOOKMARKS_STORAGE_KEY, JSON.stringify(bookmarks));
      }
    } catch {
      setTrackBookmarks([]);
    }
  }, []);
  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(ENTITY_BOOKMARKS_STORAGE_KEY);
      const parsed: unknown = raw ? JSON.parse(raw) : [];
      const bookmarks = Array.isArray(parsed)
        ? parsed.flatMap((item) => {
          const bookmark = normalizeEntityBookmark(item);
          return bookmark ? [bookmark] : [];
        }).slice(0, 100)
        : [];
      setEntityBookmarks(bookmarks);
      if (raw) {
        window.localStorage.setItem(ENTITY_BOOKMARKS_STORAGE_KEY, JSON.stringify(bookmarks));
      }
    } catch {
      setEntityBookmarks([]);
    }
  }, []);
  const liveSnapshotTrackUri = livePlaybackTrackSummary?.uri ?? null;
  const currentTrackUri = currentTrack?.uri ?? null;
  const liveSnapshotIsDifferentTrack = Boolean(
    liveSnapshotTrackUri
    && currentTrackUri
    && liveSnapshotTrackUri !== currentTrackUri,
  );
  const shouldUseLiveSnapshotDisplay = liveReadOnlyMode
    || (usingLivePlaybackSnapshot && !currentTrack)
    || (usingLivePlaybackSnapshot && liveSnapshotIsDifferentTrack && !liveControlOverrideActive);
  const basePlayerDisplayTrack: PlayerTrackSummary | null = shouldUseLiveSnapshotDisplay
    ? livePlaybackTrackSummary
    : currentTrack;
  const basePlayerDisplayPaused = shouldUseLiveSnapshotDisplay
    ? !Boolean(livePlaybackSnapshot?.is_playing)
    : playbackPaused;
  const basePlayerDisplayPositionMs = shouldUseLiveSnapshotDisplay
    ? Math.max(0, liveDerivedProgressMs)
    : playbackPositionMs;
  const basePlayerDisplayDurationMs = shouldUseLiveSnapshotDisplay
    ? Math.max(0, Number(livePlaybackSnapshot?.duration_ms ?? 0))
    : playbackDurationMs;
  const previewInProgress = Boolean(previewingTrackUri && previewPlaybackSession);
  const previewStatusTooltip = previewInProgress
    ? `preview of ${previewPlaybackSession?.previewTrack.name ?? "song"} in progress`
    : undefined;
  const playerDisplayTrack: PlayerTrackSummary | null = previewInProgress
    ? previewPlaybackSession?.baseTrack ?? basePlayerDisplayTrack
    : basePlayerDisplayTrack;
  const playerDisplayPaused = previewInProgress
    ? previewPlaybackSession?.basePaused ?? basePlayerDisplayPaused
    : basePlayerDisplayPaused;
  const playerDisplayPositionMs = previewInProgress
    ? previewPlaybackSession?.basePositionMs ?? basePlayerDisplayPositionMs
    : basePlayerDisplayPositionMs;
  const playerDisplayDurationMs = previewInProgress
    ? previewPlaybackSession?.baseDurationMs ?? basePlayerDisplayDurationMs
    : basePlayerDisplayDurationMs;
  const cachedLikedTracks = likedTracksCache?.items ?? [];
  const usingLikedTracksFallback = cachedLikedTracks.length === 0 && Boolean(profile?.recent_likes_tracks.length);
  const likedTracksForActivitySource = cachedLikedTracks.length > 0 ? cachedLikedTracks : (profile?.recent_likes_tracks ?? []);
  const likedTrackIdsForDisplay = useMemo(() => {
    const ids = new Set<string>();
    for (const track of likedTracksForActivitySource) {
      if (track.track_id) {
        ids.add(track.track_id);
      }
    }
    return ids;
  }, [likedTracksForActivitySource]);
  const likedReleaseTrackIdsForDisplay = useMemo(() => {
    const ids = new Set<number>();
    for (const track of likedTracksForActivitySource) {
      if (typeof track.release_track_id === "number") {
        ids.add(track.release_track_id);
      }
      for (const releaseTrackId of track.recording_release_track_ids ?? []) {
        if (typeof releaseTrackId === "number") {
          ids.add(releaseTrackId);
        }
      }
    }
    return ids;
  }, [likedTracksForActivitySource]);
  const activityRecentTracks = recentLikedOnly && likedActivityTracks !== null
    ? likedActivityTracks
    : (profile?.recent_tracks ?? []);
  useEffect(() => {
    if (!recentLikedOnly) {
      setLikedActivityTracks(null);
      return undefined;
    }
    let cancelled = false;
    setLikedActivityTracks(null);
    void fetchActivityListeningLog(50, 0, true)
      .then((payload) => {
        if (!cancelled) {
          setLikedActivityTracks(payload.items ?? []);
        }
      })
      .catch((error) => {
        if (!cancelled) {
          setStatusMessage(formatUiErrorMessage(error, "Failed to load liked listening history."));
        }
      });
    return () => {
      cancelled = true;
    };
  }, [recentLikedOnly]);
  const releaseTrackIdForSpotifyTrackId = (trackId: string | null | undefined) => {
    if (!trackId) {
      return null;
    }
    const metadata = releaseTrackMetadataById[trackId];
    return typeof metadata?.release_track_id === "number" ? metadata.release_track_id : null;
  };
  const recentTrackIsKnownLiked = (track: RecentTrack | null | undefined, fallbackTrackId?: string | null) => {
    if (track?.is_liked === true) {
      return true;
    }
    if (track?.source_label === "liked_cache") {
      return true;
    }
    const fallbackSpotifyTrackId = fallbackTrackId?.startsWith("spotify:track:")
      ? spotifyTrackIdFromUri(fallbackTrackId)
      : fallbackTrackId;
    const releaseTrackId = typeof track?.release_track_id === "number"
      ? track.release_track_id
      : releaseTrackIdForSpotifyTrackId(track?.track_id ?? fallbackSpotifyTrackId);
    if (typeof releaseTrackId === "number" && likedReleaseTrackIdsForDisplay.has(releaseTrackId)) {
      return true;
    }
    if (track && recordingIdentityMatchesAnyReleaseTrackId(track, likedReleaseTrackIdsForDisplay)) {
      return true;
    }
    const spotifyTrackId = track?.track_id ?? fallbackSpotifyTrackId;
    return Boolean(spotifyTrackId && (likedTrackIdsForDisplay.has(spotifyTrackId) || targetedLikedTrackById[spotifyTrackId]));
  };
  const albumTrackIsKnownLiked = (track: AlbumTrackEntry) => {
    const spotifyTrackId = track.id ?? spotifyTrackIdFromUri(track.uri);
    if (spotifyTrackId && localStarredTrackById[spotifyTrackId] === true) {
      return true;
    }
    if (recentTrackIsKnownLiked(track.sourceTrack, track.id)) {
      return true;
    }
    const releaseTrackId = track.releaseTrackId ?? releaseTrackIdForSpotifyTrackId(track.id);
    if (typeof releaseTrackId === "number" && likedReleaseTrackIdsForDisplay.has(releaseTrackId)) {
      return true;
    }
    return Boolean(track.id && (likedTrackIdsForDisplay.has(track.id) || targetedLikedTrackById[track.id]));
  };
  const albumTrackIsExactKnownLiked = (track: AlbumTrackEntry) => {
    const spotifyTrackId = track.id ?? spotifyTrackIdFromUri(track.uri);
    if (spotifyTrackId && localStarredTrackById[spotifyTrackId] === true) {
      return true;
    }
    if (track.sourceTrack?.is_liked === true && (!spotifyTrackId || track.sourceTrack.track_id === spotifyTrackId)) {
      return true;
    }
    if (track.sourceTrack?.source_label === "liked_cache" && (!spotifyTrackId || track.sourceTrack.track_id === spotifyTrackId)) {
      return true;
    }
    return Boolean(spotifyTrackId && (likedTrackIdsForDisplay.has(spotifyTrackId) || targetedLikedTrackById[spotifyTrackId]));
  };
  const queueTrackIsKnownLiked = (track: PlayerQueueTrack) => {
    if (track.isLiked === true) {
      return true;
    }
    const releaseTrackId = track.releaseTrackId ?? releaseTrackIdForSpotifyTrackId(track.trackId);
    if (typeof releaseTrackId === "number" && likedReleaseTrackIdsForDisplay.has(releaseTrackId)) {
      return true;
    }
    return Boolean(track.trackId && (likedTrackIdsForDisplay.has(track.trackId) || targetedLikedTrackById[track.trackId]));
  };
  const selectedPreviewArtists = selectedPreview?.kind === "artist"
    ? (
      selectedPreview.targetArtists?.length
        ? uniqueArtistEntries(selectedPreview.targetArtists)
        : selectedPreview.artists?.length
          ? uniqueArtistEntries(selectedPreview.artists)
        : uniqueArtistEntries(artistEntriesFromText(selectedPreview.artistName ?? selectedPreview.label))
    )
    : selectedPreview?.kind === "track" || selectedPreview?.kind === "album"
      ? (() => {
        const structuredArtists = uniqueArtistEntries(selectedPreview.artists, selectedPreview.sourceTrack?.artists);
        const displayArtistText = selectedPreview.artistName ?? selectedPreview.meta;
        return structuredArtists.length > 0
          ? structuredArtists
          : uniqueArtistEntries(artistEntriesFromText(displayArtistText));
      })()
      : [];
  const selectedPreviewPrimaryArtistName = selectedPreview?.kind === "track" || selectedPreview?.kind === "album" || selectedPreview?.kind === "artist"
    ? (
      selectedPreviewArtists[0]?.name
      ?? firstArtistFromRecentTrack(selectedPreview.sourceTrack)?.name
      ?? selectedPreview.artistName
      ?? primaryArtistName(selectedPreview.meta)
      ?? null
    )
    : null;
  const selectedPreviewPrimaryArtist = selectedPreview?.kind === "track" || selectedPreview?.kind === "album" || selectedPreview?.kind === "artist"
    ? (selectedPreviewArtists[0] ?? firstArtistFromRecentTrack(selectedPreview.sourceTrack))
    : null;
  const selectedPreviewArtistFollowStatusKnown = Boolean(
    selectedPreview?.kind === "artist"
    && profile?.experience_mode === "full"
    && profile.followed_artists_list_available,
  );
  const selectedPreviewArtistIsSpotifyFollowed = useMemo(() => {
    if (selectedPreview?.kind !== "artist" || profile?.experience_mode !== "full" || !profile.followed_artists_list_available) {
      return false;
    }
    const artistIds = new Set(
      selectedPreviewArtists
        .flatMap((artist) => [artist.artist_id, artist.id])
        .map((value) => value?.trim())
        .filter((value): value is string => Boolean(value)),
    );
    const artistNames = new Set(
      selectedPreviewArtists
        .map((artist) => artist.name?.trim().toLocaleLowerCase())
        .filter((value): value is string => Boolean(value)),
    );
    const primaryName = selectedPreviewPrimaryArtistName?.trim().toLocaleLowerCase();
    if (primaryName) {
      artistNames.add(primaryName);
    }
    return (profile.followed_artists ?? []).some((artist) => {
      const followedId = artist.artist_id?.trim();
      if (followedId && artistIds.has(followedId)) {
        return true;
      }
      const followedName = artist.name?.trim().toLocaleLowerCase();
      return Boolean(followedName && artistNames.has(followedName));
    });
  }, [profile?.experience_mode, profile?.followed_artists, profile?.followed_artists_list_available, selectedPreview, selectedPreviewArtists, selectedPreviewPrimaryArtistName]);
  const selectedPreviewAlbumTrackCount = selectedPreview?.kind === "album" && albumTrackEntries.length > 0
    ? albumTrackEntries.length
    : null;
  const selectedPreviewAlbumDurationMs = selectedPreview?.kind === "album"
    ? albumTrackEntries.reduce((total, track) => total + Math.max(0, track.durationMs ?? 0), 0)
    : 0;
  const selectedPreviewAlbumSummary = selectedPreview?.kind === "album"
    ? [
      derivedAlbumDisplayLabel(selectedPreview),
      selectedPreviewAlbumTrackCount != null ? `${selectedPreviewAlbumTrackCount} tracks` : null,
      selectedPreviewAlbumDurationMs > 0 ? formatDurationMs(selectedPreviewAlbumDurationMs) : null,
    ].filter(Boolean).join(" | ")
    : null;
  const selectedPreviewAlbumMainArtists = useMemo<TrackArtistEntry[]>(() => {
    if (selectedPreview?.kind !== "album" && selectedPreview?.kind !== "track") {
      return [];
    }
    const baseArtists = selectedPreview.kind === "album" ? selectedPreviewArtists : [];
    if (albumTrackEntries.length === 0) {
      return baseArtists;
    }
    const knownArtists = uniqueArtistEntries(
      selectedPreviewArtists,
    );
    const hydrateArtist = (artist: TrackArtistEntry): TrackArtistEntry => {
      if (artist.image_url) {
        return artist;
      }
      const artistKey = artistEntryIdentityKey(artist);
      const nameKey = artist.name?.trim().toLocaleLowerCase();
      const knownArtist = knownArtists.find((candidate) => (
        (artistKey && artistEntryIdentityKey(candidate) === artistKey)
        || (nameKey && candidate.name?.trim().toLocaleLowerCase() === nameKey)
      ));
      const imageUrl = knownArtist?.image_url ?? findArtistImageUrl(artist.name);
      return imageUrl ? { ...artist, image_url: imageUrl } : artist;
    };
    const artistCounts = new Map<string, { artist: TrackArtistEntry; count: number }>();
    let tracksWithArtists = 0;
    for (const track of albumTrackEntries) {
      const trackArtists = artistEntriesForAlbumTrack(track);
      if (trackArtists.length === 0) {
        continue;
      }
      tracksWithArtists += 1;
      for (const artist of trackArtists) {
        const artistName = artist.name?.trim();
        if (!artistName) {
          continue;
        }
        const key = artistEntryIdentityKey(artist) ?? artistName.toLocaleLowerCase();
        const current = artistCounts.get(key);
        artistCounts.set(key, { artist: current?.artist ?? hydrateArtist(artist), count: (current?.count ?? 0) + 1 });
      }
    }
    const albumWideArtists = tracksWithArtists > 1
      ? [...artistCounts.values()]
        .filter((entry) => entry.count === tracksWithArtists)
        .map((entry) => entry.artist)
      : [];
    if (albumWideArtists.length > 0) {
      return uniqueArtistEntries(albumWideArtists);
    }
    const majorityArtists = [...artistCounts.values()]
      .filter((entry) => entry.count > albumTrackEntries.length / 2)
      .map((entry) => entry.artist);
    return majorityArtists.length > 0 ? uniqueArtistEntries(majorityArtists) : baseArtists.map(hydrateArtist);
  }, [albumTrackEntries, profile, selectedPreview, selectedPreviewArtists]);
  const selectedPreviewAlbumGuestArtists = useMemo<TrackArtistEntry[]>(() => {
    if (selectedPreview?.kind !== "album" && selectedPreview?.kind !== "track") {
      return [];
    }
    const knownArtists = uniqueArtistEntries(
      selectedPreviewArtists,
      selectedPreviewAlbumMainArtists,
    );
    const hydrateArtist = (artist: TrackArtistEntry): TrackArtistEntry => {
      if (artist.image_url) {
        return artist;
      }
      const artistKey = artistEntryIdentityKey(artist);
      const nameKey = artist.name?.trim().toLocaleLowerCase();
      const knownArtist = knownArtists.find((candidate) => (
        (artistKey && artistEntryIdentityKey(candidate) === artistKey)
        || (nameKey && candidate.name?.trim().toLocaleLowerCase() === nameKey)
      ));
      const imageUrl = knownArtist?.image_url ?? findArtistImageUrl(artist.name);
      return imageUrl ? { ...artist, image_url: imageUrl } : artist;
    };
    const primaryNames = new Set(
      selectedPreviewAlbumMainArtists.map((artist) => artist.name?.trim().toLocaleLowerCase()).filter(Boolean),
    );
    const guestArtists = albumTrackEntries.flatMap((track) => artistEntriesForAlbumTrack(track));
    return uniqueArtistEntries(guestArtists).filter((artist) => {
      const artistName = artist.name?.trim().toLocaleLowerCase();
      return Boolean(artistName && !primaryNames.has(artistName));
    }).map(hydrateArtist);
  }, [albumTrackEntries, profile, selectedPreview, selectedPreviewAlbumMainArtists, selectedPreviewArtists]);
  const selectedPreviewAlbumHasGuestArtists = useMemo(() => {
    if (selectedPreview?.kind !== "album" && selectedPreview?.kind !== "track") {
      return false;
    }
    const mainArtistNames = new Set(
      selectedPreviewAlbumMainArtists.map((artist) => artist.name?.trim().toLocaleLowerCase()).filter(Boolean),
    );
    return albumTrackEntries.some((track) => (
      artistEntriesForAlbumTrack(track).some((artist) => {
        const artistName = artist.name?.trim().toLocaleLowerCase();
        return Boolean(artistName && !mainArtistNames.has(artistName));
      })
    ));
  }, [albumTrackEntries, selectedPreview, selectedPreviewAlbumMainArtists]);
  const selectedPreviewTrackMainArtists = useMemo<TrackArtistEntry[]>(() => {
    if (selectedPreview?.kind !== "track") {
      return [];
    }
    const mainNames = new Set(
      selectedPreviewAlbumMainArtists.map((artist) => artist.name?.trim().toLocaleLowerCase()).filter(Boolean),
    );
    const matchingArtists = selectedPreviewArtists.filter((artist) => {
      const artistName = artist.name?.trim().toLocaleLowerCase();
      return Boolean(artistName && mainNames.has(artistName));
    });
    return matchingArtists.length > 0 ? matchingArtists : selectedPreviewArtists.slice(0, 1);
  }, [selectedPreview, selectedPreviewAlbumMainArtists, selectedPreviewArtists]);
  const selectedPreviewTrackGuestArtists = useMemo<TrackArtistEntry[]>(() => {
    if (selectedPreview?.kind !== "track") {
      return [];
    }
    const mainNames = new Set(
      selectedPreviewTrackMainArtists.map((artist) => artist.name?.trim().toLocaleLowerCase()).filter(Boolean),
    );
    return selectedPreviewArtists.filter((artist) => {
      const artistName = artist.name?.trim().toLocaleLowerCase();
      return Boolean(artistName && !mainNames.has(artistName));
    });
  }, [selectedPreview, selectedPreviewArtists, selectedPreviewTrackMainArtists]);
  const selectedPreviewArtistImageUrl = selectedPreview && (selectedPreview.kind === "track" || selectedPreview.kind === "album" || selectedPreview.kind === "artist")
    ? (
      selectedPreviewPrimaryArtist?.image_url
      ?? findArtistImageUrl(selectedPreviewPrimaryArtistName ?? selectedPreview.artistName ?? selectedPreview.meta)
    )
    : null;
  const selectedPreviewCanOpenArtist = (selectedPreview?.kind === "track" || selectedPreview?.kind === "album") && selectedPreviewArtists.length > 0;
  const selectedPreviewIsSharedArtistPage = selectedPreview?.kind === "artist" && selectedPreviewArtists.length > 1;
  const selectedPreviewArtistEvidenceArtists = selectedPreview?.kind === "album"
    ? selectedPreviewArtists.slice(0, 1)
    : selectedPreviewArtists;
  const selectedPreviewArtistAlbumRequestKey = selectedPreview?.kind === "artist" || selectedPreview?.kind === "album"
    ? JSON.stringify({
      artistNames: selectedPreviewArtistEvidenceArtists.map((artist) => artist.name?.trim()).filter(Boolean),
      sourceAlbumId: selectedPreview.sourceAlbumId ?? selectedPreview.sourceTrack?.album_id ?? null,
      sourceAlbumName: selectedPreview.sourceAlbumName ?? selectedPreview.sourceTrack?.album_name ?? null,
    })
    : null;
  const selectedPreviewCanOpenAlbum = Boolean(
    selectedPreview?.kind === "track"
    && (selectedPreview.albumId || selectedPreview.sourceTrack?.album_id || selectedPreview.sourceTrack?.album_name || selectedPreview.detail),
  );
  const selectedPreviewAlbumSpotifyId = selectedPreview?.kind === "album"
    ? selectedPreview.albumId ?? selectedPreview.sourceAlbumId ?? selectedPreview.entityId
    : null;
  const selectedPreviewAlbumIsSpotifyLiked = Boolean(
    selectedPreviewAlbumSpotifyId
    && (
      localStarredAlbumById[selectedPreviewAlbumSpotifyId] === true
      || (
        localStarredAlbumById[selectedPreviewAlbumSpotifyId] !== false
        && targetedLikedAlbumById[selectedPreviewAlbumSpotifyId] === true
      )
    ),
  );
  const selectedPreviewPlaylistMetadata = useMemo(() => {
    if (selectedPreview?.kind !== "playlist") {
      return null;
    }
    const playlistId = selectedPreview.entityId ?? spotifyPlaylistIdFromUrl(selectedPreview.url);
    const normalizedPlaylistId = playlistId?.trim();
    if (!normalizedPlaylistId) {
      return null;
    }
    return profile?.owned_playlists.find((playlist) => {
      const candidateId = playlist.playlist_id ?? spotifyPlaylistIdFromUrl(playlist.url);
      return candidateId?.trim() === normalizedPlaylistId;
    }) ?? null;
  }, [profile?.owned_playlists, selectedPreview]);
  const selectedPreviewPlaylistIsCollaborative = Boolean(selectedPreviewPlaylistMetadata?.is_collaborative);
  const selectedPreviewPlaylistOwnerFollowedByYou = Boolean(
    selectedPreview?.kind === "playlist"
    && (
      selectedPreview.playlistOwnerFollowedByYou === true
      || selectedPreviewPlaylistMetadata?.owner_followed_by_you === true
    ),
  );
  const selectedPreviewMatchedAlbumTrack = selectedPreview?.kind === "track"
    ? (
      albumTrackEntries.find((row) => {
        const rowTrackUri = trackUriWithFallback(row.uri, row.id);
        if (selectedPreview.trackId && row.id && selectedPreview.trackId === row.id) {
          return true;
        }
        if (selectedPreview.trackUri && rowTrackUri && selectedPreview.trackUri === rowTrackUri) {
          return true;
        }
        return false;
      }) ?? null
    )
    : null;
  const selectedPreviewEffectiveTrackUri = selectedPreview?.kind === "track"
    ? trackUriWithFallback(
      selectedPreviewMatchedAlbumTrack?.uri ?? selectedPreview.trackUri,
      selectedPreview.trackId ?? selectedPreviewMatchedAlbumTrack?.id ?? null,
    )
    : null;
  const selectedPreviewSummaryCacheKey = selectedPreview?.kind === "track"
    ? selectedPreview.trackId ?? spotifyTrackIdFromUri(selectedPreviewEffectiveTrackUri) ?? selectedPreviewEffectiveTrackUri ?? selectedPreview.label
    : null;
  const selectedPreviewCachedSummary = selectedPreviewSummaryCacheKey ? trackSummaryChipCache[selectedPreviewSummaryCacheKey] : null;
  const selectedPreviewReleaseTrackDetailReady = selectedPreview?.kind === "track"
    && selectedPreviewReleaseTrackDetail
    && selectedPreviewReleaseTrackDetail.release_track.id === selectedPreview.releaseTrackId
    ? selectedPreviewReleaseTrackDetail
    : null;
  const selectedPreviewReleaseSourceVersionsRaw = selectedPreviewReleaseTrackDetailReady?.source_versions ?? [];
  const selectedPreviewReleaseSourceVersions = selectedPreviewReleaseSourceVersionsRaw;
  const selectedPreviewReleasePlaybackSourceVersion = selectedPreviewReleaseSourceVersions.find((version) => version.is_playback_choice) ?? null;
  const selectedPreviewReleaseDetailPlaybackUri = selectedPreviewReleaseTrackDetailReady?.playback.reason !== "unavailable"
    ? trackUriWithFallback(
      selectedPreviewReleaseTrackDetailReady?.playback.uri ?? null,
      selectedPreviewReleaseTrackDetailReady?.playback.spotify_track_id ?? null,
    )
    : null;
  const selectedPreviewPlaybackTrackUri = selectedPreviewReleaseDetailPlaybackUri ?? selectedPreviewEffectiveTrackUri;
  const selectedPreviewTrackIsCurrent = Boolean(
    selectedPreview?.kind === "track"
    && selectedPreviewPlaybackTrackUri
    && currentTrack?.uri === selectedPreviewPlaybackTrackUri,
  );
  const selectedPreviewTrackBaseDurationMs = selectedPreview?.kind === "track"
    ? (
      selectedPreviewReleasePlaybackSourceVersion?.duration_ms
      ?? selectedPreviewMatchedAlbumTrack?.durationMs
      ?? selectedPreview.sourceTrack?.duration_ms
      ?? (selectedPreviewTrackIsCurrent
        ? (playbackDurationMs > 0 ? playbackDurationMs : currentTrack?.durationMs ?? null)
        : null)
    )
    : null;
  const selectedPreviewTrackElapsedMs = selectedPreviewTrackIsCurrent
    ? (
      selectedPreviewTrackBaseDurationMs != null
        ? Math.min(Math.max(0, playbackPositionMs), selectedPreviewTrackBaseDurationMs)
        : Math.max(0, playbackPositionMs)
    )
    : 0;
  const selectedPreviewTrackElapsedDisplayMs = selectedPreviewTrackIsCurrent ? selectedPreviewTrackElapsedMs : 0;
  const selectedPreviewTrackTotalDisplayMs = selectedPreviewTrackBaseDurationMs ?? 0;
  const selectedPreviewTrackProgressPercent = selectedPreviewTrackBaseDurationMs != null && selectedPreviewTrackBaseDurationMs > 0
    ? Math.max(0, Math.min(100, (selectedPreviewTrackElapsedMs / selectedPreviewTrackBaseDurationMs) * 100))
    : 0;
  const selectedPreviewStarTrackId = selectedPreview?.kind === "track"
    ? selectedPreviewDetailView === "release"
      ? selectedPreviewReleasePlaybackSourceVersion?.spotify_track_id ?? null
      : (
      selectedPreview.trackId
      ?? spotifyTrackIdFromUri(selectedPreviewPlaybackTrackUri ?? selectedPreview.trackUri)
      ?? selectedPreview.sourceTrack?.track_id
      ?? null
    )
    : null;
  const selectedPreviewBookmarkIdentity = selectedPreview?.kind === "track"
    ? selectedPreviewPlaybackTrackUri ?? selectedPreviewStarTrackId ?? selectedPreview.trackId ?? selectedPreview.label
    : null;
  const selectedPreviewIsBookmarked = Boolean(
    (selectedPreviewStarTrackId && localBookmarkedTrackById[selectedPreviewStarTrackId])
    || (selectedPreviewBookmarkIdentity && trackBookmarks.some((bookmark) => bookmarkIdentityForTrack(bookmark.track) === selectedPreviewBookmarkIdentity)),
  );
  const selectedPreviewEntityBookmarkValue = selectedPreviewEntityBookmark();
  const selectedPreviewIsEntityBookmarked = entityIsBookmarked(selectedPreviewEntityBookmarkValue);
  const selectedPreviewTrackDurationLabel = selectedPreviewTrackTotalDisplayMs > 0
    ? formatPlaybackClock(selectedPreviewTrackTotalDisplayMs)
    : selectedPreviewCachedSummary?.durationLabel ?? null;
  useEffect(() => {
    if (!selectedPreviewReleaseTrackDetailReady || selectedPreview?.kind !== "track") {
      return;
    }
    const detailReleaseTrackId = selectedPreviewReleaseTrackDetailReady.release_track.id;
    const sourceTrackIds = new Set(
      selectedPreviewReleaseTrackDetailReady.source_versions
        .map((version) => version.spotify_track_id)
        .filter((trackId): trackId is string => Boolean(trackId)),
    );
    const detailPlayCount = selectedPreviewReleaseTrackDetailReady.source_versions.reduce(
      (total, version) => total + Math.max(0, Number(version.play_count ?? 0) || 0),
      0,
    );
    const detailLastPlayedAt = selectedPreviewReleaseTrackDetailReady.source_versions
      .map((version) => version.last_played_at ?? null)
      .filter((value): value is string => Boolean(value))
      .sort((left, right) => (parseTimestampMs(right) ?? 0) - (parseTimestampMs(left) ?? 0))[0] ?? null;
    if (detailPlayCount <= 0 && !detailLastPlayedAt) {
      return;
    }
    const patchRows = (rows: AlbumTrackEntry[]) => rows.map((row) => {
      const rowMatchesDetail = (
        row.releaseTrackId === detailReleaseTrackId
        || Boolean(row.id && sourceTrackIds.has(row.id))
        || Boolean(row.id && selectedPreview.trackId && row.id === selectedPreview.trackId)
      );
      if (!rowMatchesDetail) {
        return row;
      }
      return {
        ...row,
        playCount: Math.max(row.playCount, detailPlayCount),
        lastPlayedAt: row.lastPlayedAt ?? detailLastPlayedAt,
      };
    });
    setAlbumTrackEntries(patchRows);
    setHomeAlbumTrackEntries(patchRows);
  }, [selectedPreview, selectedPreviewReleaseTrackDetailReady]);
  const knownPlayerTracks = profile
    ? [
      ...(profile.recent_tracks ?? []),
      ...(profile.top_tracks ?? []),
      ...(profile.recent_top_tracks ?? []),
      ...(profile.recent_likes_tracks ?? []),
    ]
    : [];
  useEffect(() => {
    if (!selectedPreviewArtistAlbumRequestKey || (selectedPreview?.kind !== "artist" && selectedPreview?.kind !== "album")) {
      setArtistAlbumEvidenceItems(null);
      setArtistTrackEvidenceItems(null);
      setArtistAlbumEvidenceLoading(false);
      return;
    }
    const artistNames = selectedPreviewArtistEvidenceArtists
      .map((artist) => artist.name?.trim())
      .filter((name): name is string => Boolean(name));
    if (artistNames.length === 0) {
      setArtistAlbumEvidenceItems(null);
      setArtistTrackEvidenceItems(null);
      setArtistAlbumEvidenceLoading(false);
      return;
    }
    let cancelled = false;
    setArtistAlbumEvidenceLoading(true);
    fetchArtistAlbumEvidence(
      artistNames,
      selectedPreview.sourceAlbumId ?? selectedPreview.sourceTrack?.album_id ?? null,
      selectedPreview.sourceAlbumName ?? selectedPreview.sourceTrack?.album_name ?? null,
      selectedPreviewArtistEvidenceArtists.map((artist) => artist.artist_id ?? artist.id ?? null),
    )
      .then((payload) => {
        if (!cancelled) {
          setArtistAlbumEvidenceItems(payload.items);
          setArtistTrackEvidenceItems(payload.tracks ?? []);
          const enrichedArtists = uniqueArtistEntries(payload.artists, selectedPreviewArtists);
          const enrichedImage = enrichedArtists.find((artist) => Boolean(artist.image_url))?.image_url ?? null;
          if (selectedPreview.kind === "artist" && enrichedImage && (!selectedPreview.image || selectedPreview.image === selectedPreview.sourceAlbumImage)) {
            setSelectedPreview((currentPreview) => {
              if (
                !currentPreview
                || currentPreview.kind !== "artist"
                || (currentPreview.image && currentPreview.image !== currentPreview.sourceAlbumImage)
              ) {
                return currentPreview;
              }
              return {
                ...currentPreview,
                image: enrichedImage,
                artists: enrichedArtists,
                targetArtists: enrichedArtists,
              };
            });
          }
        }
      })
      .catch(() => {
        if (!cancelled) {
          setArtistAlbumEvidenceItems(null);
          setArtistTrackEvidenceItems(null);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setArtistAlbumEvidenceLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [selectedPreviewArtistAlbumRequestKey, selectedPreview]);
  const selectedPreviewArtistAlbums = useMemo<ArtistAlbumEntry[]>(() => {
    if (!profile || selectedPreview?.kind !== "artist") {
      return [];
    }
    const targetArtistNames = uniqueArtistEntries(
      selectedPreview.targetArtists,
      selectedPreview.artists,
      artistEntriesFromText((selectedPreview.targetArtists?.length || selectedPreview.artists?.length) ? selectedPreview.artistName : selectedPreview.artistName ?? selectedPreview.label),
    ).map((artist) => artist.name).filter(Boolean);
    const artistMatchesAllTargets = (candidate: string | null | undefined) => (
      targetArtistNames.length > 0 && targetArtistNames.every((targetName) => artistNameMatches(candidate, targetName))
    );
    const highlightedAlbumId = selectedPreview.sourceAlbumId ?? selectedPreview.sourceTrack?.album_id ?? null;
    const highlightedAlbumName = selectedPreview.sourceAlbumName ?? selectedPreview.sourceTrack?.album_name ?? null;
    const albumEntries = new Map<string, ArtistAlbumEntry>();
    const appendAlbum = (entry: Omit<ArtistAlbumEntry, "isHighlighted">) => {
      if (!entry.name.trim()) {
        return;
      }
      const key = entry.albumId ? `id:${entry.albumId}` : `name:${entry.name.trim().toLocaleLowerCase()}::${entry.artistName?.trim().toLocaleLowerCase() ?? ""}`;
      const existing = albumEntries.get(key);
      const isHighlighted = Boolean(
        (highlightedAlbumId && entry.albumId && highlightedAlbumId === entry.albumId)
        || (!highlightedAlbumId && highlightedAlbumName && entry.name.trim().toLocaleLowerCase() === highlightedAlbumName.trim().toLocaleLowerCase())
      );
      if (existing) {
        albumEntries.set(key, {
          ...existing,
          imageUrl: existing.imageUrl ?? entry.imageUrl,
          url: existing.url || entry.url,
          releaseYear: existing.releaseYear ?? entry.releaseYear,
          trackCount: existing.trackCount ?? entry.trackCount,
          isHighlighted: existing.isHighlighted || isHighlighted,
        });
        return;
      }
      albumEntries.set(key, { ...entry, isHighlighted });
    };

    for (const album of [...(profile.top_albums ?? []), ...(profile.recent_top_albums ?? [])]) {
      if (!artistMatchesAllTargets(album.artist_name)) {
        continue;
      }
      appendAlbum({
        albumId: album.album_id ?? null,
        name: album.name ?? "Unknown album",
        artistName: album.artist_name ?? null,
        imageUrl: album.image_url ?? null,
        url: album.url ?? "",
        releaseYear: album.release_year ?? null,
        trackCount: album.track_representation_count ?? null,
        albumType: album.album_type ?? null,
        source: "album",
      });
    }

    const knownTracksByAlbum = new Map<string, { representative: RecentTrack; artistNames: Set<string> }>();
    for (const track of knownPlayerTracks) {
      if (!track.album_name) {
        continue;
      }
      const albumKey = track.album_id
        ? `id:${track.album_id}`
        : `name:${track.album_name.trim().toLocaleLowerCase()}`;
      const group = knownTracksByAlbum.get(albumKey) ?? {
        representative: track,
        artistNames: new Set<string>(),
      };
      const trackArtists = uniqueArtistEntries(track.artists, artistEntriesFromText(track.artist_name));
      for (const artist of trackArtists) {
        const artistName = artist.name?.trim();
        if (artistName) {
          group.artistNames.add(artistName);
        }
      }
      knownTracksByAlbum.set(albumKey, group);
    }

    for (const group of knownTracksByAlbum.values()) {
      const track = group.representative;
      const albumArtistNames = [...group.artistNames].join(", ");
      if (!artistMatchesAllTargets(albumArtistNames)) {
        continue;
      }
      appendAlbum({
        albumId: track.album_id ?? null,
        name: track.album_name ?? "Unknown album",
        artistName: (albumArtistNames || track.artist_name) ?? null,
        imageUrl: track.image_url ?? null,
        url: track.album_url ?? spotifyEntityUrl("album", track.album_id),
        releaseYear: track.album_release_year ?? null,
        trackCount: null,
        albumType: track.spotify_album_type ?? null,
        source: "track",
      });
    }

    return [...albumEntries.values()].sort((left, right) => {
      if (left.isHighlighted !== right.isHighlighted) {
        return left.isHighlighted ? -1 : 1;
      }
      if (left.source !== right.source) {
        return left.source === "album" ? -1 : 1;
      }
      return left.name.localeCompare(right.name);
    });
  }, [knownPlayerTracks, profile, selectedPreview]);
  const backendSelectedPreviewArtistAlbums = useMemo<ArtistAlbumEntry[] | null>(() => {
    if (!artistAlbumEvidenceItems || selectedPreview?.kind !== "artist") {
      return null;
    }
    return artistAlbumEvidenceItems.map((item) => ({
      albumId: item.album_id,
      name: item.album_name,
      artistName: item.album_artist_names.join(", ") || null,
      imageUrl: item.image_url,
      url: item.url ?? "",
      releaseYear: item.release_year,
      trackCount: item.total_tracks ?? item.cached_track_count ?? null,
      albumType: item.album_type ?? null,
      source: item.relationship === "album" ? "album" : "track",
      isHighlighted: Boolean(
        (selectedPreview.sourceAlbumId && item.album_id && selectedPreview.sourceAlbumId === item.album_id)
        || (
          selectedPreview.sourceAlbumName
          && item.album_name.trim().toLocaleLowerCase() === selectedPreview.sourceAlbumName.trim().toLocaleLowerCase()
        )
      ),
      relationship: item.relationship,
      evidence: item.evidence,
    }));
  }, [artistAlbumEvidenceItems, selectedPreview]);
  const selectedPreviewArtistTracks = useMemo<ArtistTrackEvidenceItem[]>(() => {
    if (selectedPreview?.kind !== "artist") {
      return [];
    }
    if (artistTrackEvidenceItems && artistTrackEvidenceItems.length > 0) {
      return artistTrackEvidenceItems;
    }
    const targetNames = selectedPreviewArtists
      .map((artist) => artist.name?.trim())
      .filter((name): name is string => Boolean(name));
    if (targetNames.length === 0) {
      return [];
    }
    const seen = new Set<string>();
    return knownPlayerTracks
      .filter((track) => {
        const trackArtists = uniqueArtistEntries(track.artists, artistEntriesFromText(track.artist_name));
        return targetNames.some((artistName) => (
          artistNameMatches(track.artist_name, artistName)
          || trackArtists.some((artist) => artistNameMatches(artist.name, artistName))
        ));
      })
      .map((track): ArtistTrackEvidenceItem => ({
        track_id: track.track_id,
        track_name: track.track_name ?? "Unknown track",
        artist_name: track.artist_name,
        artists: track.artists ?? null,
        album_id: track.album_id ?? null,
        album_name: track.album_name ?? null,
        album_image_url: track.image_url ?? null,
      album_release_year: track.album_release_year ?? null,
      album_type: track.spotify_album_type ?? null,
      album_total_tracks: track.spotify_album_total_tracks ?? null,
        duration_ms: track.duration_ms ?? null,
        disc_number: track.spotify_disc_number ?? null,
        track_number: track.spotify_track_number ?? null,
        play_count: track.play_count ?? track.all_time_play_count ?? 0,
        first_played_at: track.first_played_at ?? null,
        last_played_at: track.last_played_at ?? track.spotify_played_at ?? null,
        url: track.url ?? null,
        album_url: track.album_url ?? null,
        uri: track.uri ?? null,
      }))
      .filter((track) => {
        const key = track.track_id ?? `${track.track_name}:${track.album_id ?? track.album_name ?? ""}`;
        if (seen.has(key)) {
          return false;
        }
        seen.add(key);
        return true;
      });
  }, [artistTrackEvidenceItems, knownPlayerTracks, selectedPreview, selectedPreviewArtists]);
  const selectedPreviewSourceAlbumEntry = useMemo<ArtistAlbumEntry | null>(() => {
    if (selectedPreview?.kind !== "artist" || !selectedPreview.sourceAlbumName) {
      return null;
    }
    const sourceEntry: ArtistAlbumEntry = {
      albumId: selectedPreview.sourceAlbumId ?? null,
      name: selectedPreview.sourceAlbumName,
      artistName: selectedPreview.sourceTrack?.artist_name ?? selectedPreview.artistName ?? selectedPreview.label,
      imageUrl: selectedPreview.sourceAlbumImage ?? selectedPreview.sourceTrack?.image_url ?? null,
      url: selectedPreview.sourceAlbumUrl ?? selectedPreview.sourceTrack?.album_url ?? spotifyEntityUrl("album", selectedPreview.sourceAlbumId),
      releaseYear: selectedPreview.sourceAlbumYear ?? selectedPreview.sourceTrack?.album_release_year ?? null,
      trackCount: null,
      albumType: selectedPreview.sourceTrack?.spotify_album_type ?? null,
      source: "track",
      isHighlighted: true,
      relationship: "appears_on",
      evidence: "selected track context",
    };
    return sourceEntry;
  }, [selectedPreview]);
  const selectedPreviewArtistAlbumsForDisplay = useMemo<ArtistAlbumEntry[]>(() => {
    const entries = backendSelectedPreviewArtistAlbums ?? selectedPreviewArtistAlbums;
    if (!selectedPreviewSourceAlbumEntry) {
      return entries;
    }
    const sourceAlbumId = selectedPreviewSourceAlbumEntry.albumId?.trim() ?? "";
    const sourceAlbumNameKey = selectedPreviewSourceAlbumEntry.name.trim().toLocaleLowerCase();
    const hasSourceAlbum = entries.some((entry) => (
      (sourceAlbumId && entry.albumId === sourceAlbumId)
      || (sourceAlbumNameKey && entry.name.trim().toLocaleLowerCase() === sourceAlbumNameKey)
    ));
    return hasSourceAlbum ? entries : [selectedPreviewSourceAlbumEntry, ...entries];
  }, [backendSelectedPreviewArtistAlbums, selectedPreviewArtistAlbums, selectedPreviewSourceAlbumEntry]);
  const selectedPreviewPrimaryArtistAlbums = backendSelectedPreviewArtistAlbums && !selectedPreviewIsSharedArtistPage
    ? backendSelectedPreviewArtistAlbums.filter((album) => album.relationship === "album")
    : selectedPreviewArtistAlbumsForDisplay;
  const selectedPreviewAppearsOnAlbums = backendSelectedPreviewArtistAlbums && !selectedPreviewIsSharedArtistPage
    ? selectedPreviewArtistAlbumsForDisplay.filter((album) => album.relationship === "appears_on" || album.relationship === "unknown")
    : [];
  const selectedPreviewRelatedAlbums = useMemo<ArtistAlbumEntry[]>(() => {
    if (selectedPreview?.kind !== "album") {
      return [];
    }
    const selectedAlbumId = selectedPreview.albumId ?? selectedPreview.sourceAlbumId ?? selectedPreview.entityId ?? null;
    const selectedAlbumNameKey = selectedPreview.sourceAlbumName?.trim().toLocaleLowerCase()
      ?? selectedPreview.label.trim().toLocaleLowerCase();
    const selectedCoreName = normalizedEditionAlbumCoreName(selectedPreview.label) || selectedAlbumNameKey;
    const seen = new Set<string>();
    return selectedPreviewArtistAlbumsForDisplay
      .filter((album) => {
        const albumNameKey = album.name.trim().toLocaleLowerCase();
        if ((selectedAlbumId && album.albumId === selectedAlbumId) || albumNameKey === selectedAlbumNameKey) {
          return false;
        }
        const key = album.albumId ? `id:${album.albumId}` : `name:${albumNameKey}`;
        if (seen.has(key)) {
          return false;
        }
        seen.add(key);
        return true;
      })
      .sort((left, right) => {
        const leftCoreMatch = normalizedEditionAlbumCoreName(left.name) === selectedCoreName;
        const rightCoreMatch = normalizedEditionAlbumCoreName(right.name) === selectedCoreName;
        if (leftCoreMatch !== rightCoreMatch) {
          return leftCoreMatch ? -1 : 1;
        }
        const leftSingle = artistAlbumIsSingle(left);
        const rightSingle = artistAlbumIsSingle(right);
        if (leftSingle !== rightSingle) {
          return leftSingle ? 1 : -1;
        }
        return left.name.localeCompare(right.name);
      })
      .slice(0, 16);
  }, [selectedPreview, selectedPreviewArtistAlbumsForDisplay]);
  const selectedPreviewTrackOptimisticSummary: PlayerTrackSummary | null = selectedPreview?.kind === "track"
    ? {
      name: selectedPreview.label,
      artists: selectedPreviewArtists.map((artist) => artist.name).filter(Boolean).join(", ") || selectedPreview.meta || "Unknown artist",
      album: selectedPreviewReleasePlaybackSourceVersion?.album_name ?? selectedPreview.sourceTrack?.album_name ?? selectedPreview.detail ?? "Unknown album",
      image: selectedPreviewReleasePlaybackSourceVersion?.album_image_url ?? selectedPreview.image ?? null,
      uri: selectedPreviewPlaybackTrackUri,
      durationMs: selectedPreviewTrackBaseDurationMs ?? 0,
    }
    : null;
  const playerDisplayTrackId = playerDisplayTrack ? spotifyTrackIdFromUri(playerDisplayTrack.uri) : null;
  const playerDisplayPrimaryArtistFromDisplay = primaryArtistName(playerDisplayTrack?.artists ?? null);
  const playerDisplayKnownTrack = playerDisplayTrack && knownPlayerTracks.length > 0
    ? (
      knownPlayerTracks.find((track) => {
        if (playerDisplayTrackId && track.track_id && track.track_id === playerDisplayTrackId) {
          return true;
        }
        return normalizedTrackArtistKey(track.track_name, track.artist_name) === normalizedTrackArtistKey(
          playerDisplayTrack.name,
          playerDisplayTrack.artists,
        );
      }) ?? null
    )
    : null;
  const playerDisplayArtists = uniqueArtistEntries(
    playerDisplayKnownTrack?.artists,
    artistEntriesFromText(playerDisplayTrack?.artists),
  );
  const playerDisplayArtist = playerDisplayArtists[0] ?? firstArtistFromRecentTrack(playerDisplayKnownTrack);
  const playerDisplayKnownLiked = Boolean(
    recentTrackIsKnownLiked(playerDisplayKnownTrack, playerDisplayTrackId),
  );
  const playerDisplayArtistName = playerDisplayArtist?.name ?? playerDisplayPrimaryArtistFromDisplay ?? null;
  const playerDisplayArtistNames = playerDisplayArtists.map((artist) => artist.name?.trim()).filter(Boolean);
  const playerDisplayArtistLabel = playerDisplayArtistNames.join(", ") || playerDisplayArtistName;
  const playerDisplayArtistId = playerDisplayArtist?.artist_id ?? playerDisplayArtist?.id ?? null;
  const playerDisplayArtistImageUrl = playerDisplayArtist?.image_url ?? (playerDisplayArtistName ? findArtistImageUrl(playerDisplayArtistName) : null);
  const playerDisplayAlbumName = playerDisplayKnownTrack?.album_name ?? playerDisplayTrack?.album ?? null;
  const playerDisplayAlbumId = playerDisplayKnownTrack?.album_id ?? null;
  const playerDisplayAlbumYear = playerDisplayKnownTrack?.album_release_year ?? null;
  const playerDisplayAlbumLabel = playerDisplayAlbumName
    ? (playerDisplayAlbumYear ? `${playerDisplayAlbumYear} - ${playerDisplayAlbumName}` : playerDisplayAlbumName)
    : "Choose something to play";
  const playerDisplayReleaseTrackId = playerDisplayKnownTrack?.release_track_id ?? releaseTrackIdForSpotifyTrackId(playerDisplayTrackId);
  useEffect(() => {
    if (!playerDisplayReleaseTrackId) {
      setHomeRecordingCandidate(null);
      return;
    }
    let cancelled = false;
    setHomeRecordingCandidate((current) => (
      current?.members.some((member) => member.release_track_id === playerDisplayReleaseTrackId) ? current : null
    ));
    fetchRecordingTrackCandidateByReleaseTrack(playerDisplayReleaseTrackId)
      .then((payload) => {
        if (cancelled) {
          return;
        }
        const items = payload.items ?? (payload.item ? [payload.item] : []);
        const recordingCandidate = items.find((item) => item.candidate_type === "recording_track_candidate") ?? payload.item ?? null;
        setHomeRecordingCandidate(recordingCandidate);
      })
      .catch(() => {
        if (!cancelled) {
          setHomeRecordingCandidate(null);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [playerDisplayReleaseTrackId]);
  const recentLikedStartupTrack = profile?.recent_likes_tracks[0] ?? null;
  const recentLikedStartupTrackUri = recentLikedStartupTrack
    ? trackUriWithFallback(recentLikedStartupTrack.uri, recentLikedStartupTrack.track_id)
    : null;
  const recentLikedStartupTrackId = recentLikedStartupTrack?.track_id ?? spotifyTrackIdFromUri(recentLikedStartupTrackUri);
  const currentTrackId = spotifyTrackIdFromUri(currentTrack?.uri ?? null);
  const usingRecentLikedStartupFallback = Boolean(
    !usingLivePlaybackSnapshot
    && currentTrack
    && recentLikedStartupTrack
    && playbackPaused
    && playbackPositionMs === 0
    && playbackDurationMs === 0
    && (
      (currentTrackId && recentLikedStartupTrackId && currentTrackId === recentLikedStartupTrackId)
      || (currentTrack.uri && recentLikedStartupTrackUri && currentTrack.uri === recentLikedStartupTrackUri)
    ),
  );
  const canControlPlayback = !previewInProgress && (!liveReadOnlyMode || livePlaybackOnListenLabDevice);
  const playerTransportReadOnly = liveReadOnlyMode || previewInProgress;
  const canSeekSelectedPreview = Boolean(
    canControlPlayback
    && selectedPreviewTrackIsCurrent
    && selectedPreviewTrackBaseDurationMs != null
    && selectedPreviewTrackBaseDurationMs > 0,
  );
  const livePlaybackControlTooltip = liveReadOnlyMode
    ? `Playing on ${livePlaybackSnapshot?.device_name ?? "another device"}. Click to control on ListenLab.`
    : undefined;
  const playerConnectingTooltip = !usingLivePlaybackSnapshot && !playerReady && !playerError
    ? "Connecting to Spotify player..."
    : undefined;
  const playerTransportTooltip = previewStatusTooltip ?? livePlaybackControlTooltip ?? playerConnectingTooltip;
  const listenLabQueueCursor = playerQueueSource === "listenlab" ? playerQueueCursor : null;
  const listenLabQueueHasCursor = listenLabQueueCursor != null && listenLabQueueCursor >= 0;
  const spotifyQueueCurrentIndex = playerQueueSource === "spotify" && playerDisplayTrack
    ? playerQueueTracks.findIndex((track) => {
      const queueTrackUri = trackUriWithFallback(track.uri, track.trackId);
      const displayTrackUri = playerDisplayTrack.uri;
      const queueTrackId = track.trackId ?? spotifyTrackIdFromUri(queueTrackUri);
      const displayTrackId = spotifyTrackIdFromUri(displayTrackUri);
      return Boolean(
        (queueTrackId && displayTrackId && queueTrackId === displayTrackId)
        || (queueTrackUri && displayTrackUri && queueTrackUri === displayTrackUri),
      );
    })
    : -1;
  const activeQueueCursor = playerQueueSource === "spotify" ? spotifyQueueCurrentIndex : listenLabQueueCursor;
  const hasActiveQueueCursor = activeQueueCursor != null && activeQueueCursor >= 0;
  const queueHasUnplayedTracks = playerQueueTracks.length > (hasActiveQueueCursor ? activeQueueCursor + 1 : 0);
  const queueHasLoopShuffleTracks = playerQueueLoopEnabled && playerQueueTracks.length > 1 && hasActiveQueueCursor;
  const queueShuffleAvailable = queueHasUnplayedTracks || queueHasLoopShuffleTracks;
  const livePlaylistContextId = livePlaybackSnapshot?.context_uri?.startsWith("spotify:playlist:")
    ? livePlaybackSnapshot.context_uri.split(":")[2] ?? null
    : null;
  const activePlaylistPlayback = playerQueueSource === "listenlab" && playerQueueContext?.playlistId
    ? {
      playlistId: playerQueueContext.playlistId,
      playlistName: playerQueueContext.playlistName ?? playerQueueContext.label,
      trackId: playerDisplayTrack?.uri ? spotifyTrackIdFromUri(playerDisplayTrack.uri) : null,
      trackUri: playerDisplayTrack?.uri ?? null,
      position: hasActiveQueueCursor ? activeQueueCursor : null,
      isPlaying: Boolean(playerDisplayTrack && !playerDisplayPaused && !previewingTrackUri),
    }
    : livePlaylistContextId
      ? {
        playlistId: livePlaylistContextId,
        playlistName: null,
        trackId: playerDisplayTrack?.uri ? spotifyTrackIdFromUri(playerDisplayTrack.uri) : null,
        trackUri: playerDisplayTrack?.uri ?? null,
        position: null,
        isPlaying: Boolean(playerDisplayTrack && !playerDisplayPaused && !previewingTrackUri),
      }
    : null;
  const playerUpNextTrack = hasActiveQueueCursor
    ? (
        playerQueueTracks[activeQueueCursor + 1]
        ?? (playerQueueLoopEnabled ? playerQueueTracks[0] : null)
      )
    : null;
  const canMoveListenLabQueuePrevious = Boolean(
    playerQueueSource === "listenlab"
    && listenLabQueueHasCursor
    && listenLabQueueCursor > 0,
  );
  const canMoveListenLabQueueNext = Boolean(
    playerQueueSource === "listenlab"
    && listenLabQueueHasCursor
    && listenLabQueueCursor < playerQueueTracks.length - 1,
  );
  const playerTransportControlsAvailable = Boolean(
    playerDisplayTrack
    && (playerReady || usingLivePlaybackSnapshot),
  );
  const playerPreviousDisabled = playerQueueSource === "listenlab"
    ? (previewInProgress || !canMoveListenLabQueuePrevious)
    : (previewInProgress || !playerTransportControlsAvailable);
  const playerNextDisabled = playerQueueSource === "listenlab"
    ? (previewInProgress || !canMoveListenLabQueueNext)
    : (previewInProgress || !playerTransportControlsAvailable);
  const playerPanelVisible = Boolean(profile && (playerMenuOpen || appPage === "dashboard" || !startupReadyForDashboard));
  const queueSleepTimerActive = queueSleepTimerUntilMs != null && queueSleepTimerUntilMs > Date.now();
  const queueDelayActive = queuePauseAfterCurrentEnabled || queueSleepTimerActive;

  useEffect(() => {
    setHomeQueueOpenGroupIds(new Set(playerQueueGroups.map((group) => group.id)));
  }, [playerQueueContext?.label, playerQueueContext?.url, playerQueueContext?.playlistId, playerQueueGroups]);

  useEffect(() => {
    saveIdentityAuditPersistedPrefs({
      entityTab: identityAuditEntityTab,
      trackTab: trackIdentityAuditTab,
      albumTab: albumIdentityAuditTab,
      trackIssueSort: trackIdentityAuditIssueSort,
      albumIssueSort: albumIdentityAuditIssueSort,
      issueReviewState: identityAuditIssueReviewState,
      expandedIssueKeys: identityAuditExpandedIssueKeys,
    });
  }, [
    albumIdentityAuditIssueSort,
    albumIdentityAuditTab,
    identityAuditExpandedIssueKeys,
    identityAuditEntityTab,
    identityAuditIssueReviewState,
    trackIdentityAuditIssueSort,
    trackIdentityAuditTab,
  ]);

  const reloadSecondsRemaining =
    reloadCooldownUntil == null ? 0 : Math.max(0, Math.ceil((reloadCooldownUntil - reloadCountdownNow) / 1000));
  const reloadReady = reloadSecondsRemaining <= 0;
  const spotifyCooldownActive = reloadCooldownUntil != null && !reloadReady;
  const showRateLimitReload = reloadCooldownUntil != null || Boolean(statusMessage && statusMessage.includes("rate-limiting"));
  const reloadProgress =
    reloadCooldownUntil == null
      ? 1
      : Math.max(0, Math.min(1, 1 - (reloadCooldownUntil - reloadCountdownNow) / Math.max(reloadCooldownDurationMs, 1)));

  function groupDecisionKey(group: SuggestedAnalysisGroup): string {
    return `group:${group.analysis_track_id}`;
  }

  function trackDecisionKey(track: AmbiguousReviewItem): string {
    return `track:${track.entry_id}`;
  }

  function isReviewedDecision(decision: LocalReviewDecision | undefined): boolean {
    return Boolean(decision && decision.verdict !== "unsure");
  }

  function computeAmbiguousTrackItems(): AmbiguousReviewItem[] {
    const allItems = identityAuditAmbiguous?.items ?? [];
    return allItems.filter((item) => {
      if (identityAuditAmbiguousFamilyFilter !== "all") {
        if (!item.review_families.map((name) => name.toLowerCase()).includes(identityAuditAmbiguousFamilyFilter.toLowerCase())) {
          return false;
        }
      }
      if (identityAuditAmbiguousBucketFilter !== "all" && item.bucket !== identityAuditAmbiguousBucketFilter) {
        return false;
      }
      return true;
    });
  }

  function computeUnifiedReviewItems(): UnifiedReviewItem[] {
    const suggestedItems = identityAuditSuggestedGroups?.items ?? [];
    const trackItems = computeAmbiguousTrackItems();
    const suggested = suggestedItems.map((group): UnifiedReviewItem => ({
      decision_key: groupDecisionKey(group),
      item_type: "group",
      title: group.analysis_track_name || `Track Family ${group.analysis_track_id}`,
      subtitle: `${group.release_track_count} release tracks | ${Math.round(group.confidence * 100)}% confidence`,
      bucket_label: "Suggested groups",
      family_label: group.song_family_key || "Suggested groups",
      group,
      track: null,
    }));
    const tracks = trackItems.map((track): UnifiedReviewItem => ({
      decision_key: trackDecisionKey(track),
      item_type: "track",
      title: track.release_track_name,
      subtitle: `${track.artist_name} | ${track.bucket}`,
      bucket_label: "Ambiguous tracks",
      family_label: track.dominant_family || track.review_families[0] || track.bucket || "Ambiguous tracks",
      group: null,
      track,
    }));
    return [...suggested, ...tracks];
  }

  function findNextUnreviewedDecisionKey(
    items: UnifiedReviewItem[],
    afterKey: string | null = null,
    decisions: Record<string, LocalReviewDecision> = identityAuditLocalDecisions,
  ): string | null {
    if (items.length === 0) {
      return null;
    }
    const startIndex = afterKey == null ? -1 : items.findIndex((item) => item.decision_key === afterKey);
    for (let index = startIndex + 1; index < items.length; index += 1) {
      if (!isReviewedDecision(decisions[items[index].decision_key])) {
        return items[index].decision_key;
      }
    }
    for (let index = 0; index <= startIndex; index += 1) {
      if (!isReviewedDecision(decisions[items[index].decision_key])) {
        return items[index].decision_key;
      }
    }
    return null;
  }

  useEffect(() => {
    if (appPage !== "recentDebug" || !profile || listeningLogLoaded || listeningLogLoading) {
      return;
    }
    void loadListeningLogBatch(true, false);
  }, [appPage, listeningLogLoaded, listeningLogLoading, profile, recentDebugSourceFilter]);

  useEffect(() => {
    if (!["dashboard", "formulaLab"].includes(appPage) || !profile || !startupDashboardReleased) {
      return;
    }
    if (!mergedTracksLoaded && !mergedTracksLoading) {
      void loadMergedTrackRankings();
    }
  }, [
    appPage,
    mergedTracksLoaded,
    mergedTracksLoading,
    profile,
    startupDashboardReleased,
  ]);

  useEffect(() => {
    if (recentTopTracksUseSpotify || !profile) {
      return;
    }
    if (!recentComputedTracksLoaded && !recentComputedTracksLoading) {
      void loadRecentComputedTrackRankings();
    }
  }, [
    profile,
    recentComputedTracksLoaded,
    recentComputedTracksLoading,
    recentTopTracksUseSpotify,
  ]);

  useEffect(() => {
    if (appPage !== "searchLookup" || !profile) {
      return;
    }
    if (searchLookupEntityType === "tracks" && !trackCatalogLookupLoaded && !trackCatalogLookupLoading) {
      void loadTrackCatalogLookup(true);
      return;
    }
    if (searchLookupEntityType === "albums" && !albumCatalogLookupLoaded && !albumCatalogLookupLoading) {
      void loadAlbumCatalogLookup(true);
    }
  }, [
    appPage,
    profile,
    searchLookupEntityType,
    albumCatalogLookupLoaded,
    albumCatalogLookupLoading,
    trackCatalogLookupLoaded,
    trackCatalogLookupLoading,
  ]);

  useEffect(() => {
    if (appPage !== "catalogBackfill" || !profile) {
      return;
    }
    if (!catalogBackfillCoverageLoaded && !catalogBackfillCoverageLoading) {
      void loadCatalogBackfillCoverage();
    }
    if (!catalogBackfillRunsLoaded && !catalogBackfillRunsLoading) {
      void loadCatalogBackfillRuns();
    }
    if (!catalogBackfillQueueLoaded && !catalogBackfillQueueLoading) {
      void loadCatalogBackfillQueue();
    }
  }, [
    appPage,
    profile,
    catalogBackfillCoverageLoaded,
    catalogBackfillCoverageLoading,
    catalogBackfillRunsLoaded,
    catalogBackfillRunsLoading,
    catalogBackfillQueueLoaded,
    catalogBackfillQueueLoading,
  ]);

  useEffect(() => {
    if (appPage !== "identityAudit" || !profile) {
      return;
    }
    if (!identityAuditLoaded && !identityAuditLoading) {
      void loadIdentityAudit();
    }
    if (!identityAuditSuggestedLoaded && !identityAuditSuggestedLoading) {
      void loadIdentityAuditSuggestedGroups();
    }
    if (!identityAuditAmbiguousLoaded && !identityAuditAmbiguousLoading) {
      void loadIdentityAuditAmbiguousReview();
    }
    if (!identityAuditSavedSubmissions && !identityAuditSavedSubmissionsLoading) {
      void loadIdentityAuditSavedSubmissions();
    }
  }, [
    appPage,
    identityAuditAmbiguousLoaded,
    identityAuditAmbiguousLoading,
    identityAuditLoaded,
    identityAuditLoading,
    identityAuditSuggestedLoaded,
    identityAuditSuggestedLoading,
    identityAuditSavedSubmissions,
    identityAuditSavedSubmissionsLoading,
    profile,
  ]);

  useEffect(() => {
    setIdentityAuditSavedSubmissionDryRun(null);
    setIdentityAuditSavedSubmissionDryRunError("");
    setIdentityAuditSavedSubmissionDryRunLoading(false);
    setIdentityAuditSavedSubmissionDryRunAt(null);
  }, [identityAuditSavedSubmissionDetail?.item.id]);

  useEffect(() => {
    setIdentityAuditAmbiguousVisibleCount(IDENTITY_AUDIT_AMBIGUOUS_VISIBLE_STEP);
  }, [identityAuditAmbiguousFamilyFilter, identityAuditAmbiguousBucketFilter]);

  useEffect(() => {
    setIdentityAuditPreviewValidationResult(null);
    setIdentityAuditPreviewValidationError("");
    setIdentityAuditPreviewValidatedAt(null);
    setIdentityAuditSubmissionSaveLoading(false);
    setIdentityAuditSubmissionSaveError("");
    setIdentityAuditSubmissionSaveResult(null);
  }, [identityAuditLocalDecisions]);

  useEffect(() => {
    if (appPage !== "identityAudit" || identityAuditEntityTab !== "tracks" || trackIdentityAuditTab !== "review_queue") {
      return;
    }
    const unifiedItems = computeUnifiedReviewItems();
    if (unifiedItems.length === 0) {
      if (identityAuditFocusedReviewKey != null) {
        setIdentityAuditFocusedReviewKey(null);
      }
      return;
    }
    const hasCurrentFocus = identityAuditFocusedReviewKey != null
      && unifiedItems.some((item) => item.decision_key === identityAuditFocusedReviewKey);
    const focusedReviewed = hasCurrentFocus
      ? isReviewedDecision(identityAuditLocalDecisions[identityAuditFocusedReviewKey as string])
      : false;
    if (!hasCurrentFocus || focusedReviewed) {
      const nextKey = findNextUnreviewedDecisionKey(unifiedItems, identityAuditFocusedReviewKey);
      setIdentityAuditFocusedReviewKey(nextKey);
    }
  }, [
    appPage,
    identityAuditEntityTab,
    trackIdentityAuditTab,
    identityAuditFocusedReviewKey,
    identityAuditLocalDecisions,
    identityAuditSuggestedGroups,
    identityAuditAmbiguous,
    identityAuditAmbiguousFamilyFilter,
    identityAuditAmbiguousBucketFilter,
  ]);

  useEffect(() => {
    if (appPage !== "identityAudit" || identityAuditEntityTab !== "tracks" || trackIdentityAuditTab !== "review_queue") {
      return;
    }

    function onKeydown(event: KeyboardEvent) {
      const target = event.target as HTMLElement | null;
      const isTypingTarget = Boolean(
        target
        && (
          target.tagName === "INPUT"
          || target.tagName === "TEXTAREA"
          || target.tagName === "SELECT"
          || target.isContentEditable
        )
      );
      if (isTypingTarget) {
        return;
      }
      if (event.metaKey || event.ctrlKey || event.altKey) {
        return;
      }

      const unifiedItems = computeUnifiedReviewItems();
      if (unifiedItems.length === 0) {
        return;
      }
      const focusedKey = identityAuditFocusedReviewKey ?? findNextUnreviewedDecisionKey(unifiedItems);

      if (event.key.toLowerCase() === "n") {
        event.preventDefault();
        setIdentityAuditFocusedReviewKey(findNextUnreviewedDecisionKey(unifiedItems, focusedKey));
        return;
      }

      if (!focusedKey) {
        return;
      }

      if (event.key.toLowerCase() === "a") {
        event.preventDefault();
        const nextDecisions = {
          ...identityAuditLocalDecisions,
          [focusedKey]: {
            verdict: "good_to_group" as LocalReviewVerdict,
            grouping_target: identityAuditLocalDecisions[focusedKey]?.grouping_target ?? "same_composition",
            note: identityAuditLocalDecisions[focusedKey]?.note ?? "",
            updated_at_ms: Date.now(),
          },
        };
        updateLocalReviewDecision(focusedKey, {
          verdict: "good_to_group",
          grouping_target: identityAuditLocalDecisions[focusedKey]?.grouping_target ?? "same_composition",
        });
        setIdentityAuditFocusedReviewKey(findNextUnreviewedDecisionKey(unifiedItems, focusedKey, nextDecisions));
        return;
      }

      if (event.key.toLowerCase() === "r") {
        event.preventDefault();
        const nextDecisions = {
          ...identityAuditLocalDecisions,
          [focusedKey]: {
            verdict: "not_good" as LocalReviewVerdict,
            grouping_target: null,
            note: identityAuditLocalDecisions[focusedKey]?.note ?? "",
            updated_at_ms: Date.now(),
          },
        };
        updateLocalReviewDecision(focusedKey, {
          verdict: "not_good",
          grouping_target: null,
        });
        setIdentityAuditFocusedReviewKey(findNextUnreviewedDecisionKey(unifiedItems, focusedKey, nextDecisions));
        return;
      }

      if (event.key.toLowerCase() === "s") {
        event.preventDefault();
        const nextDecisions = {
          ...identityAuditLocalDecisions,
          [focusedKey]: {
            verdict: "skipped" as LocalReviewVerdict,
            grouping_target: null,
            note: identityAuditLocalDecisions[focusedKey]?.note ?? "",
            updated_at_ms: Date.now(),
          },
        };
        updateLocalReviewDecision(focusedKey, {
          verdict: "skipped",
          grouping_target: null,
        });
        setIdentityAuditFocusedReviewKey(findNextUnreviewedDecisionKey(unifiedItems, focusedKey, nextDecisions));
      }
    }

    window.addEventListener("keydown", onKeydown);
    return () => window.removeEventListener("keydown", onKeydown);
  }, [
    appPage,
    identityAuditEntityTab,
    trackIdentityAuditTab,
    identityAuditFocusedReviewKey,
    identityAuditLocalDecisions,
    identityAuditSuggestedGroups,
    identityAuditAmbiguous,
    identityAuditAmbiguousFamilyFilter,
    identityAuditAmbiguousBucketFilter,
  ]);

  useEffect(() => {
    if (appPage !== "identityAudit") {
      return;
    }
    if (identityAuditEntityTab === "tracks" && trackIdentityAuditTab === "problems" && !trackDuplicateLookupLoaded && !trackDuplicateLookupLoading) {
      void loadTrackDuplicateLookup(true);
    }
    if (identityAuditEntityTab === "tracks" && trackIdentityAuditTab === "mapping" && !trackMappingLineageLoaded && !trackMappingLineageLoading) {
      void loadTrackMappingLineage(true);
    }
    if (identityAuditEntityTab !== "albums") {
      return;
    }
    if (!albumDuplicateLookupLoaded && !albumDuplicateLookupLoading) {
      void loadAlbumDuplicateLookup(true);
    }
    if (!albumNameDuplicateLookupLoaded && !albumNameDuplicateLookupLoading) {
      void loadAlbumNameDuplicateLookup(true);
    }
  }, [
    appPage,
    identityAuditEntityTab,
    trackIdentityAuditTab,
    albumDuplicateLookupLoaded,
    albumDuplicateLookupLoading,
    albumNameDuplicateLookupLoaded,
    albumNameDuplicateLookupLoading,
    trackDuplicateLookupLoaded,
    trackDuplicateLookupLoading,
    trackMappingLineageLoaded,
    trackMappingLineageLoading,
  ]);

  useEffect(() => {
    if (
      usingLivePlaybackSnapshot
      && livePlaybackOnListenLabDevice
      && playerError
      && playerError.includes("Spotify player could not connect")
    ) {
      setPlayerError(null);
    }
  }, [livePlaybackOnListenLabDevice, playerError, usingLivePlaybackSnapshot]);

  useEffect(() => {
    if (!selectedPreview) {
      return;
    }
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = previousOverflow;
    };
  }, [selectedPreview]);

  useEffect(() => {
    const releaseTrackId = selectedPreview?.kind === "track" ? selectedPreview.releaseTrackId : null;
    if (typeof releaseTrackId !== "number" || !Number.isFinite(releaseTrackId) || releaseTrackId <= 0) {
      setSelectedPreviewReleaseTrackDetail(null);
      setSelectedPreviewReleaseTrackDetailLoading(false);
      setSelectedPreviewReleaseTrackDetailError(null);
      return;
    }
    const contextTrackId = selectedPreview?.trackId ?? spotifyTrackIdFromUri(selectedPreview?.trackUri ?? null);
    let cancelled = false;
    setSelectedPreviewReleaseTrackDetailLoading(true);
    setSelectedPreviewReleaseTrackDetailError(null);
    fetchReleaseTrackDetail(releaseTrackId, contextTrackId)
      .then((payload) => {
        if (cancelled) {
          return;
        }
        setSelectedPreviewReleaseTrackDetail(payload);
        setSelectedPreviewReleaseTrackDetailLoading(false);
      })
      .catch((error) => {
        if (cancelled) {
          return;
        }
        setSelectedPreviewReleaseTrackDetail(null);
        setSelectedPreviewReleaseTrackDetailLoading(false);
        setSelectedPreviewReleaseTrackDetailError(formatUiErrorMessage(error, "Release-track detail could not be loaded."));
      });
    return () => {
      cancelled = true;
    };
  }, [
    selectedPreview?.kind === "track" ? selectedPreview.releaseTrackId : null,
    selectedPreview?.kind === "track" ? selectedPreview.trackId ?? spotifyTrackIdFromUri(selectedPreview.trackUri) : null,
  ]);

  useEffect(() => {
    if (!selectedPreview || selectedPreview.kind !== "track") {
      setSelectedPreviewDetailView("recording");
      return;
    }
    const backendPage = appPage === "identityAudit" || appPage === "searchLookup" || appPage === "recentDebug" || appPage === "catalogBackfill";
    setSelectedPreviewDetailView(selectedPreview.preferredDetailView ?? (backendPage ? "release" : "recording"));
  }, [appPage, selectedPreview]);

  const selectedPreviewReleaseTrackId = selectedPreview?.kind === "track" ? selectedPreview.releaseTrackId : null;

  useEffect(() => {
    if (!selectedPreview || selectedPreview.kind !== "track" || selectedPreviewDetailView !== "recording") {
      return;
    }
    if (preserveRecordingAlbumTracklistOpenRef.current) {
      preserveRecordingAlbumTracklistOpenRef.current = false;
      return;
    }
    setRecordingAlbumTracklistOpen(false);
  }, [selectedPreviewReleaseTrackId, selectedPreviewDetailView]);

  useEffect(() => {
    const releaseTrackId = selectedPreviewReleaseTrackId;
    if (typeof releaseTrackId !== "number" || !Number.isFinite(releaseTrackId) || releaseTrackId <= 0) {
      setSelectedPreviewRecordingCandidate(null);
      setSelectedPreviewRelatedCandidates([]);
      setSelectedPreviewRecordingCandidateError(null);
      return;
    }
    let cancelled = false;
    setSelectedPreviewRecordingCandidate((current) => (
      current?.members.some((member) => member.release_track_id === releaseTrackId) ? current : null
    ));
    setSelectedPreviewRelatedCandidates((current) => (
      current.some((item) => item.members.some((member) => member.release_track_id === releaseTrackId)) ? current : []
    ));
    setSelectedPreviewRecordingCandidateError(null);
    const candidateTimer = window.setTimeout(() => {
      fetchRecordingTrackCandidateByReleaseTrack(releaseTrackId)
        .then((payload) => {
          if (cancelled) {
            return;
          }
          const baseItems = payload.items ?? (payload.item ? [payload.item] : []);
          const recordingCandidate = baseItems.find((item) => item.candidate_type === "recording_track_candidate") ?? null;
          const itemByKey = new Map<string, RecordingTrackCandidateItem>();
          for (const payloadItems of [baseItems]) {
            for (const item of payloadItems) {
              itemByKey.set(item.candidate_key, item);
            }
          }
          const items = Array.from(itemByKey.values());
          const hydratedRecordingCandidate = recordingCandidate
            ? itemByKey.get(recordingCandidate.candidate_key) ?? recordingCandidate
            : null;
          setSelectedPreviewRelatedCandidates(items);
          setSelectedPreviewRecordingCandidate(hydratedRecordingCandidate);
        })
        .catch((error) => {
          if (cancelled) {
            return;
          }
          setSelectedPreviewRecordingCandidate(null);
          setSelectedPreviewRelatedCandidates([]);
          setSelectedPreviewRecordingCandidateError(formatUiErrorMessage(error, "Recording view could not be loaded."));
        });
    }, 120);
    return () => {
      cancelled = true;
      window.clearTimeout(candidateTimer);
    };
  }, [selectedPreviewReleaseTrackId]);

  useEffect(() => {
    if (!selectedPreview || selectedPreview.kind !== "track") {
      setOverlayTrackPlaybackExpanded(false);
    }
  }, [selectedPreview]);

  useEffect(() => {
    if (selectedPreview?.kind === "track" && selectedPreviewTrackIsCurrent) {
      setOverlayTrackPlaybackExpanded(true);
    }
  }, [selectedPreview?.kind, selectedPreviewTrackIsCurrent]);

  useEffect(() => {
    if (!currentTrack?.uri || !playbackPaused) {
      setPausedTimeFlashOn(true);
      return;
    }
    const timer = window.setInterval(() => {
      setPausedTimeFlashOn((current) => !current);
    }, 1400);
    return () => {
      window.clearInterval(timer);
    };
  }, [currentTrack?.uri, playbackPaused]);

  useEffect(() => {
    return () => {
      if (previewStopTimerRef.current != null) {
        window.clearTimeout(previewStopTimerRef.current);
      }
      if (previewVolumeRampTimerRef.current != null) {
        window.clearInterval(previewVolumeRampTimerRef.current);
      }
      if (queueSkipHoldTimerRef.current != null) {
        window.clearTimeout(queueSkipHoldTimerRef.current);
      }
    };
  }, []);

  useEffect(() => {
    const url = new URL(window.location.href);
    if (url.pathname === "/auth/callback") {
      const status = url.searchParams.get("status");
      const flow = url.searchParams.get("flow");
      setAuthTransitioning(status === "success");
      setStatusMessage(
        status === "success"
          ? "Spotify login succeeded. Session restored."
          : "Spotify login did not complete successfully.",
      );
      if (flow === "recent_ingest") {
        setRecentIngestCallbackPending(true);
      }
      window.history.replaceState({}, "", "/");
    }
  }, []);

  useEffect(() => {
    void loadSession();
  }, []);

  useEffect(() => {
    if (!session?.authenticated || experienceMode === "local" || !hasPremiumPlayback) {
      setLivePlaybackSnapshot(null);
      setLiveAwaitingNextTrack(false);
      setLiveControlOverrideUntilMs(null);
      setLivePlaybackProbeComplete(false);
      return;
    }

    let cancelled = false;
    let pollTimer: number | null = null;
    const refresh = async () => {
      await loadCurrentPlaybackSnapshot();
      if (!cancelled) {
        setLivePlaybackProbeComplete(true);
      }
    };

    void refresh();
    pollTimer = window.setInterval(() => {
      if (!cancelled) {
        void refresh();
      }
    }, LIVE_PLAYBACK_POLL_INTERVAL_MS);

    return () => {
      cancelled = true;
      if (pollTimer != null) {
        window.clearInterval(pollTimer);
      }
    };
  }, [experienceMode, hasPremiumPlayback, session?.authenticated, session?.spotify_user_id]);

  useEffect(() => {
    if (!livePlaybackSnapshot) {
      liveProgressAnchorRef.current = null;
      liveEndRefreshRequestedRef.current = false;
      setLiveDerivedProgressMs(0);
      return;
    }
    const receivedAtMs = Date.now();
    const durationMs = Math.max(0, Number(livePlaybackSnapshot.duration_ms ?? 0));
    const progressMs = Math.max(0, Number(livePlaybackSnapshot.progress_ms ?? 0));
    const correctedBaseProgressMs = clampProgress(progressMs, durationMs);
    liveProgressAnchorRef.current = {
      baseProgressMs: correctedBaseProgressMs,
      receivedAtMs,
      durationMs,
    };
    liveEndRefreshRequestedRef.current = false;
    setLiveDerivedProgressMs(correctedBaseProgressMs);
  }, [livePlaybackSnapshot]);

  useEffect(() => {
    if (!usingLivePlaybackSnapshot || !livePlaybackSnapshot?.is_playing) {
      return;
    }
    const timer = window.setInterval(() => {
      const anchor = liveProgressAnchorRef.current;
      if (!anchor) {
        return;
      }
      const elapsedSinceReceiptMs = Math.max(0, Date.now() - anchor.receivedAtMs);
      const next = clampProgress(anchor.baseProgressMs + elapsedSinceReceiptMs, anchor.durationMs);
      setLiveDerivedProgressMs((current) => (current === next ? current : next));
      if (
        anchor.durationMs > 0
        && next >= anchor.durationMs
        && !liveEndRefreshRequestedRef.current
      ) {
        liveEndRefreshRequestedRef.current = true;
        setLiveAwaitingNextTrack(true);
        void loadCurrentPlaybackSnapshot();
      }
    }, LIVE_PLAYBACK_PROGRESS_TICK_MS);

    return () => {
      window.clearInterval(timer);
    };
  }, [livePlaybackSnapshot?.is_playing, usingLivePlaybackSnapshot]);

  useEffect(() => {
    const snapshot = livePlaybackSnapshot;
    const trackId = snapshot?.item_id ?? spotifyTrackIdFromUri(snapshot?.uri ?? null);
    const durationMs = Math.max(0, Number(snapshot?.duration_ms ?? 0));
    const progressMs = Math.max(0, Number(liveDerivedProgressMs));
    if (
      !snapshot
      || !trackId
      || durationMs <= 0
      || !snapshot.is_playing
      || livePlaybackOnListenLabDevice
      || activePlayerListenEventId != null
    ) {
      return;
    }

    const state = liveListenQualificationRef.current;
    if (state.trackId !== trackId || progressMs + 10_000 < state.lastProgressMs) {
      state.trackId = trackId;
      state.lastProgressMs = progressMs;
      state.eventId = crypto.randomUUID();
      state.submitted = false;
    } else {
      state.lastProgressMs = progressMs;
    }
    if (state.submitted || progressMs < durationMs * 0.65) {
      return;
    }

    state.submitted = true;
    void (async () => {
      const verificationResponse = await fetch(`${apiBaseUrl}/auth/current-playback`, { credentials: "include" });
      if (!verificationResponse.ok) {
        throw new Error(`Playback verification failed (${verificationResponse.status}).`);
      }
      const verification = (await verificationResponse.json()) as CurrentPlaybackResponse;
      const verifiedSnapshot = verification.status === "ok" && verification.has_playback ? verification.snapshot ?? null : null;
      const verifiedTrackId = verifiedSnapshot?.item_id ?? spotifyTrackIdFromUri(verifiedSnapshot?.uri ?? null);
      const verifiedDurationMs = Math.max(0, Number(verifiedSnapshot?.duration_ms ?? 0));
      const verifiedProgressMs = Math.max(0, Number(verifiedSnapshot?.progress_ms ?? 0));
      if (
        !verifiedSnapshot
        || verifiedTrackId !== trackId
        || !verifiedSnapshot.is_playing
        || verifiedDurationMs <= 0
        || verifiedProgressMs < verifiedDurationMs * 0.65
      ) {
        state.submitted = false;
        if (verifiedSnapshot) {
          setLivePlaybackSnapshot(verifiedSnapshot);
        }
        return;
      }
      setLivePlaybackSnapshot(verifiedSnapshot);
      const qualifiedAt = new Date().toISOString();
      const playedAt = new Date(Date.now() - verifiedProgressMs).toISOString();
      const response = await fetch(`${apiBaseUrl}/auth/player-listen-event`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          event_id: state.eventId,
          track_uri: verifiedSnapshot.uri,
          track_id: trackId,
          track_name: verifiedSnapshot.name,
          artist_name: verifiedSnapshot.artist_names.join(", "),
          album_name: verifiedSnapshot.album_name,
          album_id: verifiedSnapshot.album_id,
          duration_ms: verifiedDurationMs,
          progress_ms: verifiedProgressMs,
          ms_played_confidence: "listened",
          played_at: playedAt,
        }),
      });
      if (!response.ok) {
        throw new Error(`Listen qualification failed (${response.status}).`);
      }
      const payload = (await response.json()) as { listen_qualified?: boolean };
      if (payload.listen_qualified) {
        await refreshQualifiedListenState(
          trackId,
          verifiedSnapshot.release_track_id ?? releaseTrackIdForSpotifyTrackId(trackId),
          qualifiedAt,
          verifiedProgressMs,
          verifiedDurationMs,
        );
      }
    })().catch(() => {
      state.submitted = false;
    });
  }, [activePlayerListenEventId, liveDerivedProgressMs, livePlaybackOnListenLabDevice, livePlaybackSnapshot]);

  useEffect(() => {
    if (!recentIngestCallbackPending) {
      return;
    }
    void loadRecentIngestResult();
  }, [recentIngestCallbackPending]);

  useEffect(() => {
    window.localStorage.setItem(EXPERIENCE_MODE_STORAGE_KEY, experienceMode);
  }, [experienceMode]);

  useEffect(() => {
    if (reloadCooldownUntil == null) {
      window.localStorage.removeItem(SPOTIFY_COOLDOWN_UNTIL_STORAGE_KEY);
      window.localStorage.removeItem(SPOTIFY_COOLDOWN_DURATION_STORAGE_KEY);
      return;
    }
    window.localStorage.setItem(SPOTIFY_COOLDOWN_UNTIL_STORAGE_KEY, String(reloadCooldownUntil));
    window.localStorage.setItem(SPOTIFY_COOLDOWN_DURATION_STORAGE_KEY, String(reloadCooldownDurationMs));
  }, [reloadCooldownDurationMs, reloadCooldownUntil]);

  useEffect(() => {
    if (
      experienceMode === "local"
      && session != null
      && !profile
      && !loadingProfile
      && !profileLoadAttempted
      && !profileLoadInFlightRef.current
    ) {
      void loadProfile();
      return;
    }
    if (
      session?.authenticated
      && !profile
      && !loadingProfile
      && !profileLoadAttempted
      && !profileLoadInFlightRef.current
    ) {
      void loadProfile();
    }
  }, [experienceMode, loadingProfile, profile, profileLoadAttempted, session]);

  useEffect(() => {
    const hasRecentDataLoaded = Boolean(
      profile
      && (
        profile.recent_tracks_available
        || profile.recent_likes_available
        || profile.recent_top_tracks_available
        || profile.recent_top_artists_available
        || profile.recent_top_albums_available
      ),
    );
    if (
      (experienceMode !== "local" && !session?.authenticated)
      || experienceMode === "local"
      || !profile
      || analysisMode !== "quick"
      || loadingProfile
      || loadingExtendedProfile
      || loadingRecentSection
      || spotifyCooldownActive
    ) {
      return;
    }
    const currentRange = profile.recent_range ?? "short_term";
    const shouldFetchRecent = currentRange !== recentRange || !hasRecentDataLoaded;
    if (!shouldFetchRecent) {
      return;
    }
    const attemptKey = `${profile.id}:${recentRange}`;
    if (quickRecentAutoAttemptRef.current === attemptKey) {
      return;
    }
    quickRecentAutoAttemptRef.current = attemptKey;
    void refreshRecentSection(recentRange);
  }, [
    analysisMode,
    experienceMode,
    loadingExtendedProfile,
    loadingProfile,
    loadingRecentSection,
    profile,
    recentRange,
    session,
    spotifyCooldownActive,
  ]);

  useEffect(() => {
    setPlayerRecentTracksLoadAttempted(false);
    setPlayerQueueLoadAttempted(false);
    setRecentSectionLoadAttempted(false);
    setRecentSectionStartupError(false);
    setStartupDashboardReleased(false);
  }, [session?.spotify_user_id]);

  useEffect(() => {
    if (!profile) {
      quickRecentAutoAttemptRef.current = null;
      recentSectionLoadInFlightRef.current = false;
    }
  }, [profile]);

  useEffect(() => {
    if (
      experienceMode !== "full"
      || !session?.authenticated
      || !profile
      || analysisMode !== "quick"
      || !startupPlaybackReady
      || loadingProfile
      || loadingExtendedProfile
      || loadingRecentSection
      || spotifyCooldownActive
      || quickProfileLoadInFlightRef.current
      || !startupDashboardReleased
    ) {
      return;
    }
    if (!startupRecentReady) {
      return;
    }
    const hasTopDataLoaded = Boolean(
      profile.top_tracks_available
      || profile.followed_artists_list_available
      || profile.top_albums_available
      || profile.owned_playlists_available
    );
    if (hasTopDataLoaded) {
      return;
    }
    void loadQuickProfileSections(profile.recent_range ?? recentRange);
  }, [
    analysisMode,
    experienceMode,
    loadingExtendedProfile,
    loadingProfile,
    loadingRecentSection,
    profile,
    recentRange,
    session?.authenticated,
    spotifyCooldownActive,
    startupPlaybackReady,
    startupDashboardReleased,
    startupRecentReady,
  ]);

  useEffect(() => {
    if (!recentRangeRefreshPending) {
      return;
    }
    const currentRange = profile?.recent_range ?? null;
    if (
      currentRange === recentRange
      && !loadingRecentSection
      && !loadingExtendedProfile
    ) {
      setRecentRangeRefreshPending(false);
    }
  }, [loadingExtendedProfile, loadingRecentSection, profile?.recent_range, recentRange, recentRangeRefreshPending]);

  useEffect(() => {
    if (!trackRankingRefreshPending) {
      return;
    }
    const timer = window.setTimeout(() => {
      setTrackRankingRefreshPending(false);
    }, 450);
    return () => {
      window.clearTimeout(timer);
    };
  }, [trackRankingRefreshPending]);

  useEffect(() => {
    if (reloadCooldownUntil == null) {
      return;
    }

    setReloadCountdownNow(Date.now());
    const timer = window.setInterval(() => {
      setReloadCountdownNow(Date.now());
    }, 1000);

    return () => {
      window.clearInterval(timer);
    };
  }, [reloadCooldownUntil]);

  useEffect(() => {
    function handlePointerDown(event: MouseEvent) {
      if (!profileMenuRef.current?.contains(event.target as Node)) {
        setProfileMenuOpen(false);
        setProfileSettingsOpen(false);
      }
      if (!brandMenuRef.current?.contains(event.target as Node)) {
        setBrandMenuOpen(false);
      }
      if (!experimentalMenuRef.current?.contains(event.target as Node)) {
        setExperimentalMenuOpen(false);
      }
      if (!playerMenuRef.current?.contains(event.target as Node)) {
        setPlayerMenuOpen(false);
      }
    }

    document.addEventListener("mousedown", handlePointerDown);
    return () => {
      document.removeEventListener("mousedown", handlePointerDown);
    };
  }, []);

  useEffect(() => {
    function handlePointerDown(event: MouseEvent) {
      const target = event.target;
      if (!(target instanceof Element) || !target.closest(".player-queue-settings")) {
        setPlayerQueueSettingsOpen(false);
        setPlayerQueuePauseMenuOpen(false);
      }
    }

    document.addEventListener("mousedown", handlePointerDown);
    return () => {
      document.removeEventListener("mousedown", handlePointerDown);
    };
  }, []);

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setSelectedPreview(null);
        setExperimentalMenuOpen(false);
      }
    }

    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, []);

  useEffect(() => {
    let cancelled = false;

    if (
      experienceMode === "local"
      || !selectedPreview
      || (selectedPreview.kind !== "track" && selectedPreview.kind !== "album")
    ) {
      loadedAlbumTracksAlbumIdRef.current = null;
      setAlbumTrackEntries([]);
      setAlbumTrackEntriesLoading(false);
      setAlbumTrackEntriesError(null);
      setAlbumTrackEntriesPartial(false);
      setSelectedAlbumFamilyContext(null);
      setAlbumTrackSpotifyFetchPending(false);
      return () => {
        cancelled = true;
      };
    }

    const activePreview = selectedPreview;
    const selectedTrackId = selectedPreview.kind === "track"
      ? (
        selectedPreview.trackId
        ?? selectedPreview.entityId
        ?? spotifyTrackIdFromUri(selectedPreview.trackUri)
        ?? selectedPreviewReleasePlaybackSourceVersion?.spotify_track_id
        ?? null
      )
      : null;
    const selectedReleaseTrackId = selectedPreview.kind === "track" ? selectedPreview.releaseTrackId ?? null : null;
    const initialAlbumId = (
      selectedPreview.kind === "track"
        ? selectedPreview.albumId
          ?? selectedPreview.sourceAlbumId
          ?? selectedPreviewReleasePlaybackSourceVersion?.album_id
          ?? albumIdFromPreview(selectedPreview)
        : albumIdFromPreview(selectedPreview)
    );
    if (!initialAlbumId && !selectedTrackId) {
      if (selectedPreview.kind === "track" && selectedPreviewReleaseTrackDetailLoading) {
        setAlbumTrackEntriesLoading(true);
        setAlbumTrackEntriesError(null);
        return () => {
          cancelled = true;
        };
      }
      loadedAlbumTracksAlbumIdRef.current = null;
      setAlbumTrackEntries([]);
      setAlbumTrackEntriesLoading(false);
      setAlbumTrackEntriesError("Album track list is unavailable for this item.");
      setAlbumTrackEntriesPartial(false);
      setSelectedAlbumFamilyContext(null);
      setAlbumTrackSpotifyFetchPending(false);
      return () => {
        cancelled = true;
      };
    }
    const shouldForceSpotifyFetch = Boolean(
      albumTrackSpotifyFetchRequest
      && !spotifyCooldownActive
      && (
        (initialAlbumId && albumTrackSpotifyFetchRequest.albumId === initialAlbumId)
        || (!initialAlbumId && selectedTrackId && albumTrackSpotifyFetchRequest.trackId === selectedTrackId)
      ),
    );
    const albumCompletionKey = initialAlbumId
      ? `album:${initialAlbumId}`
      : selectedTrackId ? `track:${selectedTrackId}` : null;
    const cachedAlbumTrackRowsCandidate = !shouldForceSpotifyFetch
      ? (
        (initialAlbumId
          ? albumTrackRowsCacheRef.current.get(`album:${initialAlbumId}`)
          : selectedTrackId ? albumTrackRowsCacheRef.current.get(`track:${selectedTrackId}`) : null)
        ?? null
      )
      : null;
    const cachedAlbumTrackRows = cachedAlbumTrackRowsCandidate?.rows.every((row) => (
      row.recordingHistoryAvailable === true
      && (activePreview.kind !== "track" || !row.id || row.releaseTrackId != null)
    ))
      ? cachedAlbumTrackRowsCandidate
      : null;
    if (cachedAlbumTrackRows && cachedAlbumTrackRows.rows.length > 0) {
      setAlbumTrackEntries(cachedAlbumTrackRows.rows.map((row) => ({
        ...row,
        isSelected: Boolean(
          (selectedTrackId && row.id && selectedTrackId === row.id)
          || (selectedReleaseTrackId && row.releaseTrackId === selectedReleaseTrackId),
        ),
      })));
      loadedAlbumTracksAlbumIdRef.current = cachedAlbumTrackRows.albumId;
      setAlbumTrackEntriesLoading(false);
      setAlbumTrackEntriesError(null);
      setAlbumTrackEntriesPartial(cachedAlbumTrackRows.partial);
      setSelectedAlbumFamilyContext(cachedAlbumTrackRows.family);
      setAlbumTrackSpotifyFetchPending(false);
      return () => {
        cancelled = true;
      };
    }
    const albumAlreadyLoaded = initialAlbumId
      && loadedAlbumTracksAlbumIdRef.current === initialAlbumId
      && albumTrackEntries.length > 0
      && albumTrackEntries.every((row) => row.recordingHistoryAvailable === true)
      && !shouldForceSpotifyFetch;
    if (albumAlreadyLoaded) {
      setAlbumTrackEntries((current) => current.map((row) => ({
        ...row,
        isSelected: Boolean(
          (selectedTrackId && row.id && selectedTrackId === row.id)
          || (selectedReleaseTrackId && row.releaseTrackId === selectedReleaseTrackId),
        ),
      })));
      setAlbumTrackEntriesLoading(false);
      setAlbumTrackEntriesError(null);
      return () => {
        cancelled = true;
      };
    }

    async function loadAlbumTrackEntries() {
      let scheduledSpotifyCompletion = false;
      setAlbumTrackEntriesLoading(true);
      setAlbumTrackEntriesError(null);
      const controller = new AbortController();
      const timeoutId = window.setTimeout(() => controller.abort(), ALBUM_TRACKS_FETCH_TIMEOUT_MS);
      try {
        const params = new URLSearchParams();
        if (initialAlbumId) {
          params.set("album_id", initialAlbumId);
        }
        if (selectedTrackId) {
          params.set("track_id", selectedTrackId);
        }
        params.set("promote_identity", "true");
        if (activePreview.kind === "track") {
          params.set("include_family", "true");
        }
        const activeTrackUri = activePreview.trackUri ?? selectedPreviewReleasePlaybackSourceVersion?.uri ?? null;
        if (activeTrackUri) {
          params.set("track_uri", activeTrackUri);
        }
        if (shouldForceSpotifyFetch) {
          params.set("force_spotify", "true");
        }
        if (spotifyCooldownActive && !shouldForceSpotifyFetch) {
          params.set("local_only", "true");
        }
        const response = await fetch(`${apiBaseUrl}/auth/playback/album-tracks?${params.toString()}`, {
          credentials: "include",
          signal: controller.signal,
        });
        if (!response.ok) {
          throw new Error(`Failed to load album tracks (${response.status}).`);
        }
        const payload = (await response.json()) as {
          album_id?: string | null;
          items?: Array<{
            id?: string | null;
            name?: string | null;
            uri?: string | null;
            duration_ms?: number | null;
            disc_number?: number | null;
            track_number?: number | null;
            artists?: TrackArtistEntry[];
            release_track_id?: number | null;
            release_track_name?: string | null;
            release_track_source_count?: number | null;
            release_track_duplicate_source_count?: number | null;
            has_release_track_siblings?: boolean | null;
            release_track_cluster_candidate_type?: string | null;
            release_track_cluster_relationship_kind?: string | null;
            play_count?: number | null;
            first_played_at?: string | null;
            last_played_at?: string | null;
            source_play_count?: number | null;
            source_first_played_at?: string | null;
            source_last_played_at?: string | null;
            recording_play_count?: number | null;
            recording_first_played_at?: string | null;
            recording_last_played_at?: string | null;
            family_exclusive?: boolean | null;
            family_available_versions?: AlbumFamilyContext["versions"] | null;
            family_switch_album_id?: string | null;
            family_switch_label?: string | null;
            family_has_edition_relation?: boolean | null;
            family_has_external_recording_relation?: boolean | null;
          }>;
          partial?: boolean | null;
          album_family?: AlbumFamilyContext | null;
        };
        const resolvedAlbumId = payload.album_id ?? initialAlbumId;
        if (!resolvedAlbumId) {
          throw new Error("Album track list is unavailable for this item.");
        }
        const rows = albumTrackRowsFromItems(payload.items ?? [], selectedTrackId, selectedReleaseTrackId);

        if (!cancelled) {
          const isPartial = Boolean(payload.partial);
          setAlbumTrackEntries(rows);
          setSelectedAlbumFamilyContext(payload.album_family ?? null);
          loadedAlbumTracksAlbumIdRef.current = resolvedAlbumId;
          albumTrackRowsCacheRef.current.set(`album:${resolvedAlbumId}`, {
            albumId: resolvedAlbumId,
            rows,
            partial: isPartial,
            family: payload.album_family ?? null,
          });
          if (selectedTrackId) {
            albumTrackRowsCacheRef.current.set(`track:${selectedTrackId}`, {
              albumId: resolvedAlbumId,
              rows,
              partial: isPartial,
              family: payload.album_family ?? null,
            });
          }
          setAlbumTrackEntriesError(rows.length === 0 ? "No tracks were returned for this album." : null);
          setAlbumTrackEntriesPartial(isPartial);
          const completionKey = resolvedAlbumId ? `album:${resolvedAlbumId}` : albumCompletionKey;
          if (
            isPartial
            && activePreview.kind === "track"
            && !spotifyCooldownActive
            && !shouldForceSpotifyFetch
            && completionKey
            && !albumTrackSpotifyAutoFetchAttemptedRef.current.has(completionKey)
          ) {
            albumTrackSpotifyAutoFetchAttemptedRef.current.add(completionKey);
            scheduledSpotifyCompletion = true;
            setAlbumTrackSpotifyFetchPending(true);
            setAlbumTrackSpotifyFetchRequest({
              albumId: resolvedAlbumId,
              trackId: selectedTrackId,
              nonce: Date.now(),
            });
          }
        }
      } catch (error) {
        if (!cancelled) {
          if (error instanceof DOMException && error.name === "AbortError" && albumTrackEntries.length > 0) {
            setAlbumTrackEntriesError(null);
            setAlbumTrackEntriesPartial(true);
          } else {
            setAlbumTrackEntriesError(
              error instanceof DOMException && error.name === "AbortError"
                ? "Album track list took too long to load."
                : error instanceof Error ? error.message : "Album track list could not be loaded.",
            );
          }
        }
      } finally {
        window.clearTimeout(timeoutId);
        if (!cancelled) {
          setAlbumTrackEntriesLoading(false);
          if (shouldForceSpotifyFetch) {
            setAlbumTrackSpotifyFetchRequest(null);
          }
          if (!scheduledSpotifyCompletion) {
            setAlbumTrackSpotifyFetchPending(false);
          }
        }
      }
    }

    const albumTrackLoadDelayMs = activePreview.kind === "track" ? 120 : 0;
    const albumTrackLoadTimer = window.setTimeout(() => {
      void loadAlbumTrackEntries();
    }, albumTrackLoadDelayMs);
    return () => {
      cancelled = true;
      window.clearTimeout(albumTrackLoadTimer);
    };
  }, [
    experienceMode,
    profile?.recent_likes_tracks,
    profile?.recent_top_tracks,
    profile?.recent_tracks,
    profile?.top_tracks,
    selectedPreview,
    selectedPreviewReleasePlaybackSourceVersion?.album_id,
    selectedPreviewReleasePlaybackSourceVersion?.spotify_track_id,
    selectedPreviewReleasePlaybackSourceVersion?.uri,
    selectedPreviewReleaseTrackDetailLoading,
    selectedPreviewDetailView,
    recordingAlbumTracklistOpen,
    spotifyCooldownActive,
    albumTrackSpotifyFetchRequest,
  ]);

  useEffect(() => {
    setHomeAlbumExpanded(false);
    setHomeAlbumTrackEntries([]);
    setHomeAlbumTrackEntriesError(null);
    setHomeAlbumTrackEntriesLoading(false);
    loadedHomeAlbumTracksAlbumIdRef.current = null;
  }, [playerDisplayTrack?.uri, playerDisplayAlbumId]);

  useEffect(() => {
    setHoveredAlbumWithArtistName(null);
    setDetailOptionsOpen(false);
    if (albumWithHoverDelayRef.current != null) {
      window.clearTimeout(albumWithHoverDelayRef.current);
      albumWithHoverDelayRef.current = null;
    }
  }, [selectedPreview]);

  useEffect(() => {
    const ids = new Set<string>();
    if (selectedPreview?.kind === "track") {
      for (const trackId of [
        selectedPreview.trackId,
        selectedPreview.entityId,
        spotifyTrackIdFromUri(selectedPreview.trackUri),
        selectedPreview.sourceTrack?.track_id,
      ]) {
        if (trackId) {
          ids.add(trackId);
        }
      }
      for (const version of selectedPreviewReleaseTrackDetailReady?.source_versions ?? []) {
        if (version.spotify_track_id) {
          ids.add(version.spotify_track_id);
        }
      }
    }
    for (const track of [...albumTrackEntries, ...homeAlbumTrackEntries]) {
      if (track.id) {
        ids.add(track.id);
      }
      if (track.sourceTrack?.track_id) {
        ids.add(track.sourceTrack.track_id);
      }
    }
    const missingIds = [...ids].filter((trackId) => (
      !likedTrackIdsForDisplay.has(trackId)
      && !targetedLikedTrackCheckedById[trackId]
      && !targetedLikedInFlightIdsRef.current.has(trackId)
    )).slice(0, 50);
    if (missingIds.length === 0) {
      return;
    }
    missingIds.forEach((trackId) => targetedLikedInFlightIdsRef.current.add(trackId));
    fetchLikedTracksContains(missingIds).then((payload) => {
      setTargetedLikedTrackById((current) => {
        const next = { ...current };
        for (const trackId of missingIds) {
          next[trackId] = Boolean(payload.items[trackId]);
        }
        return next;
      });
      setTargetedLikedTrackCheckedById((current) => {
        const next = { ...current };
        for (const trackId of missingIds) {
          next[trackId] = true;
        }
        return next;
      });
    }).catch(() => {
      // Star enrichment is best-effort; unchecked tracks can retry later.
    }).finally(() => {
      missingIds.forEach((trackId) => targetedLikedInFlightIdsRef.current.delete(trackId));
    });
  }, [
    albumTrackEntries,
    homeAlbumTrackEntries,
    likedTrackIdsForDisplay,
    selectedPreview,
    selectedPreviewReleaseTrackDetailReady,
    targetedLikedTrackCheckedById,
  ]);

  useEffect(() => {
    const albumId = selectedPreviewAlbumSpotifyId;
    if (
      !albumId
      || targetedLikedAlbumCheckedById[albumId]
      || targetedLikedAlbumInFlightIdsRef.current.has(albumId)
    ) {
      return;
    }
    targetedLikedAlbumInFlightIdsRef.current.add(albumId);
    fetchLikedAlbumContains(albumId).then((payload) => {
      setTargetedLikedAlbumById((current) => ({
        ...current,
        [albumId]: Boolean(payload.is_liked),
      }));
      setTargetedLikedAlbumCheckedById((current) => ({
        ...current,
        [albumId]: true,
      }));
    }).catch(() => {
      // Album star enrichment is best-effort.
    }).finally(() => {
      targetedLikedAlbumInFlightIdsRef.current.delete(albumId);
    });
  }, [selectedPreviewAlbumSpotifyId, targetedLikedAlbumCheckedById]);

  useEffect(() => {
    if (!hoveredAlbumWithArtistName || !albumTrackListRef.current) {
      return;
    }
    const visibleTrackEntries = sortedAlbumTrackEntries(albumTrackEntries, albumTrackLastSortMode, selectedPreviewDetailView);
    const firstMatchIndex = visibleTrackEntries.findIndex((track) => artistNameMatches(track.artistName, hoveredAlbumWithArtistName));
    if (firstMatchIndex < 0) {
      return;
    }
    const row = albumTrackListRef.current.children.item(firstMatchIndex);
    row?.scrollIntoView({ block: "nearest" });
  }, [albumTrackEntries, albumTrackLastSortMode, hoveredAlbumWithArtistName, selectedPreviewDetailView]);

  useEffect(() => {
    const activePreview = selectedPreview;
    const albumTracklistVisible = Boolean(
      activePreview
      && (activePreview.kind === "track" || activePreview.kind === "album")
    );
    if (!albumTracklistVisible || albumTrackEntriesLoading || albumTrackEntries.length === 0) {
      if (!albumTracklistVisible || albumTrackEntries.length === 0) {
        autoScrolledAlbumTracklistKeyRef.current = null;
      }
      return;
    }
    if (albumFamilyDiscScrollTarget != null) {
      return;
    }
    if (!activePreview) {
      return;
    }
    const visibleEntries = sortedAlbumTrackEntries(albumTrackEntries, albumTrackLastSortMode, selectedPreviewDetailView);
    const selectedEntry = visibleEntries.find((track) => track.isSelected);
    if (!selectedEntry) {
      autoScrolledAlbumTracklistKeyRef.current = null;
      return;
    }
    const albumKey = loadedAlbumTracksAlbumIdRef.current
      ?? albumIdFromPreview(activePreview)
      ?? `${activePreview.kind}:${activePreview.entityId}`;
    const albumTracklistKey = `${albumKey}|${selectedEntry.id ?? selectedEntry.releaseTrackId ?? selectedEntry.name}`;
    if (autoScrolledAlbumTracklistKeyRef.current === albumTracklistKey) {
      return;
    }
    let cancelled = false;
    let retryTimer: number | null = null;
    let attemptCount = 0;
    const attemptScroll = () => {
      if (cancelled) {
        return;
      }
      if (scrollSelectedAlbumTrackToMiddle(albumTrackListRef.current, visibleEntries)) {
        autoScrolledAlbumTracklistKeyRef.current = albumTracklistKey;
        return;
      }
      attemptCount += 1;
      if (attemptCount < 4) {
        retryTimer = window.setTimeout(attemptScroll, 75 * attemptCount);
      }
    };
    const frameId = window.requestAnimationFrame(attemptScroll);
    return () => {
      cancelled = true;
      window.cancelAnimationFrame(frameId);
      if (retryTimer != null) {
        window.clearTimeout(retryTimer);
      }
    };
  }, [
    albumTrackEntries,
    albumTrackEntriesLoading,
    albumTrackLastSortMode,
    albumFamilyDiscScrollTarget,
    selectedPreview,
    selectedPreviewDetailView,
  ]);

  useEffect(() => {
    if (!homeAlbumExpanded || homeAlbumTrackEntriesLoading || homeAlbumTrackEntries.length === 0) {
      return;
    }
    const frameId = window.requestAnimationFrame(() => {
      scrollSelectedAlbumTrackToMiddle(homeAlbumTrackListRef.current, sortedAlbumTrackEntries(homeAlbumTrackEntries, homeAlbumTrackLastSortMode));
    });
    return () => {
      window.cancelAnimationFrame(frameId);
    };
  }, [homeAlbumExpanded, homeAlbumTrackEntries, homeAlbumTrackEntriesLoading, homeAlbumTrackLastSortMode]);

  useEffect(() => () => {
    if (albumWithHoverDelayRef.current != null) {
      window.clearTimeout(albumWithHoverDelayRef.current);
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    if (
      !homeAlbumExpanded
      || experienceMode === "local"
      || !playerDisplayTrack
      || spotifyCooldownActive
    ) {
      return () => {
        cancelled = true;
      };
    }

    const activeDisplayTrack = playerDisplayTrack;
    const selectedTrackId = spotifyTrackIdFromUri(activeDisplayTrack.uri);
    const initialAlbumId = playerDisplayAlbumId;
    if (!initialAlbumId && !selectedTrackId) {
      setHomeAlbumTrackEntries([]);
      setHomeAlbumTrackEntriesLoading(false);
      setHomeAlbumTrackEntriesError("Album track list is unavailable for this item.");
      return () => {
        cancelled = true;
      };
    }
    if (initialAlbumId && loadedHomeAlbumTracksAlbumIdRef.current === initialAlbumId) {
      setHomeAlbumTrackEntries((current) => current.map((row) => ({
        ...row,
        isSelected: Boolean(selectedTrackId && row.id && selectedTrackId === row.id),
      })));
      setHomeAlbumTrackEntriesLoading(false);
      setHomeAlbumTrackEntriesError(null);
      return () => {
        cancelled = true;
      };
    }

    async function loadHomeAlbumTrackEntries() {
      setHomeAlbumTrackEntriesLoading(true);
      setHomeAlbumTrackEntriesError(null);
      try {
        const params = new URLSearchParams();
        if (initialAlbumId) {
          params.set("album_id", initialAlbumId);
        }
        if (selectedTrackId) {
          params.set("track_id", selectedTrackId);
        }
        if (activeDisplayTrack.uri) {
          params.set("track_uri", activeDisplayTrack.uri);
        }
        const response = await fetch(`${apiBaseUrl}/auth/playback/album-tracks?${params.toString()}`, {
          credentials: "include",
        });
        if (!response.ok) {
          throw new Error(`Failed to load album tracks (${response.status}).`);
        }
        const payload = (await response.json()) as {
          album_id?: string | null;
          items?: Array<{
            id?: string | null;
            name?: string | null;
            uri?: string | null;
            duration_ms?: number | null;
            artists?: TrackArtistEntry[];
            release_track_id?: number | null;
            release_track_name?: string | null;
            release_track_source_count?: number | null;
            release_track_duplicate_source_count?: number | null;
            has_release_track_siblings?: boolean | null;
            release_track_cluster_candidate_type?: string | null;
            release_track_cluster_relationship_kind?: string | null;
            play_count?: number | null;
            last_played_at?: string | null;
          }>;
        };
        const resolvedAlbumId = payload.album_id ?? initialAlbumId;
        if (!resolvedAlbumId) {
          throw new Error("Album track list is unavailable for this item.");
        }
        if (!cancelled) {
          const rows = albumTrackRowsFromItems(payload.items ?? [], selectedTrackId);
          setHomeAlbumTrackEntries(rows);
          loadedHomeAlbumTracksAlbumIdRef.current = resolvedAlbumId;
          setHomeAlbumTrackEntriesError(rows.length === 0 ? "No tracks were returned for this album." : null);
        }
      } catch (error) {
        if (!cancelled) {
          setHomeAlbumTrackEntriesError(error instanceof Error ? error.message : "Album track list could not be loaded.");
        }
      } finally {
        if (!cancelled) {
          setHomeAlbumTrackEntriesLoading(false);
        }
      }
    }

    void loadHomeAlbumTrackEntries();
    return () => {
      cancelled = true;
    };
  }, [experienceMode, homeAlbumExpanded, playerDisplayAlbumId, playerDisplayTrack, profile?.recent_likes_tracks, profile?.recent_top_tracks, profile?.recent_tracks, profile?.top_tracks, spotifyCooldownActive]);

  useEffect(() => {
    let cancelled = false;
    const playlistId = selectedPreview?.kind === "playlist"
      ? selectedPreview.entityId ?? spotifyPlaylistIdFromUrl(selectedPreview.url)
      : null;
    if (!playlistId) {
      setPlaylistTrackEntries([]);
      setPlaylistTrackEntriesLoading(false);
      setPlaylistTrackEntriesError(selectedPreview?.kind === "playlist" ? "Playlist tracks cannot be loaded because this playlist is missing a Spotify id." : null);
      setPlaylistTrackEntriesHasMore(false);
      setPlaylistTrackEntriesTotal(null);
      setPlaylistTrackEntriesNextOffset(null);
      setPlaylistTrackEntriesOffset(0);
      return () => {
        cancelled = true;
      };
    }
    const normalizedPlaylistId = playlistId;
    const initialOffset = selectedPreview?.kind === "playlist" && typeof selectedPreview.focusPlaylistPosition === "number"
      ? Math.max(0, Math.floor(selectedPreview.focusPlaylistPosition / 500) * 500)
      : 0;
    const playlistCacheKey = `${normalizedPlaylistId}:${initialOffset}`;
    const cachedPlaylistTrackEntries = playlistTrackEntriesCacheRef.current[playlistCacheKey];
    if (cachedPlaylistTrackEntries) {
      setPlaylistTrackEntries(cachedPlaylistTrackEntries.items);
      setPlaylistTrackEntriesHasMore(cachedPlaylistTrackEntries.hasMore);
      setPlaylistTrackEntriesTotal(cachedPlaylistTrackEntries.total);
      setPlaylistTrackEntriesNextOffset(cachedPlaylistTrackEntries.nextOffset);
      setPlaylistTrackEntriesOffset(initialOffset);
      setPlaylistTrackEntriesLoading(false);
      setPlaylistTrackEntriesError(cachedPlaylistTrackEntries.items.length === 0 ? "No tracks were returned for this playlist." : null);
      return () => {
        cancelled = true;
      };
    }

    async function loadPlaylistTrackEntries() {
      setPlaylistTrackEntriesLoading(true);
      setPlaylistTrackEntriesError(null);
      setPlaylistTrackEntriesHasMore(false);
      setPlaylistTrackEntriesTotal(null);
      setPlaylistTrackEntriesNextOffset(null);
      setPlaylistTrackEntriesOffset(initialOffset);
      try {
        const params = new URLSearchParams({
          playlist_id: normalizedPlaylistId,
          limit: "500",
          offset: String(initialOffset),
        });
        const response = await fetch(`${apiBaseUrl}/auth/playback/playlist-tracks?${params.toString()}`, {
          credentials: "include",
        });
        if (!response.ok) {
          let detail = `Failed to load playlist tracks (${response.status}).`;
          try {
            const payload = (await response.json()) as { detail?: string };
            if (payload.detail) {
              detail = payload.detail;
            }
          } catch {
            // Keep status fallback.
          }
          throw new Error(detail);
        }
        const payload = (await response.json()) as {
          items?: RecentTrack[];
          has_more?: boolean | null;
          total?: number | null;
          next_offset?: number | null;
        };
        if (!cancelled) {
          const items = payload.items ?? [];
          const hasMore = Boolean(payload.has_more);
          const total = typeof payload.total === "number" ? payload.total : null;
          const nextOffset = typeof payload.next_offset === "number" ? payload.next_offset : initialOffset + items.length;
          playlistTrackEntriesCacheRef.current[playlistCacheKey] = { items, hasMore, total, nextOffset };
          setPlaylistTrackEntries(items);
          setPlaylistTrackEntriesHasMore(hasMore);
          setPlaylistTrackEntriesTotal(total);
          setPlaylistTrackEntriesNextOffset(nextOffset);
          setPlaylistTrackEntriesOffset(initialOffset);
          setPlaylistTrackEntriesError(items.length === 0 ? "No tracks were returned for this playlist." : null);
        }
      } catch (error) {
        if (!cancelled) {
          setPlaylistTrackEntriesError(error instanceof Error ? error.message : "Playlist tracks could not be loaded.");
          setPlaylistTrackEntries([]);
          setPlaylistTrackEntriesHasMore(false);
          setPlaylistTrackEntriesTotal(null);
          setPlaylistTrackEntriesNextOffset(null);
          setPlaylistTrackEntriesOffset(initialOffset);
        }
      } finally {
        if (!cancelled) {
          setPlaylistTrackEntriesLoading(false);
        }
      }
    }

    void loadPlaylistTrackEntries();
    return () => {
      cancelled = true;
    };
  }, [selectedPreview?.entityId, selectedPreview?.focusPlaylistPosition, selectedPreview?.kind, selectedPreview?.url]);

  useEffect(() => {
    let cancelled = false;
    if (selectedPreview?.kind !== "track") {
      setSelectedPreviewPlaylistMemberships([]);
      setSelectedPreviewPlaylistMembershipsLoading(false);
      setSelectedPreviewPlaylistIndexStatus(null);
      return () => {
        cancelled = true;
      };
    }
    const trackId = selectedPreview.trackId ?? spotifyTrackIdFromUri(selectedPreview.trackUri) ?? selectedPreview.sourceTrack?.track_id ?? null;
    const currentReleaseTrackId = selectedPreview.releaseTrackId ?? selectedPreview.sourceTrack?.release_track_id ?? null;
    const releaseTrackIds = selectedPreviewDetailView === "release"
      ? [currentReleaseTrackId].filter((value): value is number => typeof value === "number" && value > 0)
      : [
        currentReleaseTrackId,
        ...(selectedPreview.sourceTrack?.recording_release_track_ids ?? []),
        ...selectedPreviewRecordingMembers.map((member) => member.release_track_id),
      ].filter((value): value is number => typeof value === "number" && value > 0);
    if (!trackId && releaseTrackIds.length === 0) {
      setSelectedPreviewPlaylistMemberships([]);
      setSelectedPreviewPlaylistMembershipsLoading(false);
      setSelectedPreviewPlaylistIndexStatus(null);
      return () => {
        cancelled = true;
      };
    }
    async function loadPlaylistMemberships() {
      setSelectedPreviewPlaylistMembershipsLoading(true);
      try {
        const params = new URLSearchParams();
        if (trackId) {
          params.set("track_id", trackId);
        }
        if (releaseTrackIds[0]) {
          params.set("release_track_id", String(releaseTrackIds[0]));
        }
        if (releaseTrackIds.length > 0) {
          params.set("recording_release_track_ids", Array.from(new Set(releaseTrackIds)).join(","));
        }
        params.set("mode", selectedPreviewDetailView === "release" ? "individual" : "representative");
        const response = await fetch(`${apiBaseUrl}/tracks/playlist-memberships?${params.toString()}`, {
          credentials: "include",
        });
        if (!response.ok) {
          throw new Error(`Playlist memberships failed (${response.status}).`);
        }
        const payload = (await response.json()) as {
          items?: PlaylistMembership[];
          playlist_index_status?: PlaylistIndexStatus | null;
        };
        if (!cancelled) {
          setSelectedPreviewPlaylistMemberships(payload.items ?? []);
          setSelectedPreviewPlaylistIndexStatus(payload.playlist_index_status ?? null);
        }
      } catch {
        if (!cancelled) {
          setSelectedPreviewPlaylistMemberships([]);
          setSelectedPreviewPlaylistIndexStatus(null);
        }
      } finally {
        if (!cancelled) {
          setSelectedPreviewPlaylistMembershipsLoading(false);
        }
      }
    }
    void loadPlaylistMemberships();
    return () => {
      cancelled = true;
    };
  }, [selectedPreview, selectedPreviewDetailView]);

  async function loadMorePlaylistTrackEntries() {
    const playlistId = selectedPreview?.kind === "playlist"
      ? selectedPreview.entityId ?? spotifyPlaylistIdFromUrl(selectedPreview.url)
      : null;
    const normalizedPlaylistId = playlistId?.trim();
    if (!normalizedPlaylistId || playlistTrackEntriesLoading || !playlistTrackEntriesHasMore) {
      return;
    }
    const offset = playlistTrackEntriesNextOffset ?? playlistTrackEntries.length;
    setPlaylistTrackEntriesLoading(true);
    setPlaylistTrackEntriesError(null);
    try {
      const params = new URLSearchParams({
        playlist_id: normalizedPlaylistId,
        limit: "500",
        offset: String(offset),
      });
      const response = await fetch(`${apiBaseUrl}/auth/playback/playlist-tracks?${params.toString()}`, {
        credentials: "include",
      });
      if (!response.ok) {
        let detail = `Failed to load playlist tracks (${response.status}).`;
        try {
          const payload = (await response.json()) as { detail?: string };
          if (payload.detail) {
            detail = payload.detail;
          }
        } catch {
          // Keep status fallback.
        }
        throw new Error(detail);
      }
      const payload = (await response.json()) as {
        items?: RecentTrack[];
        has_more?: boolean | null;
        total?: number | null;
        next_offset?: number | null;
      };
      const incomingItems = payload.items ?? [];
      const total = typeof payload.total === "number" ? payload.total : playlistTrackEntriesTotal;
      const nextOffset = typeof payload.next_offset === "number" ? payload.next_offset : offset + incomingItems.length;
      const hasMore = Boolean(payload.has_more);
      setPlaylistTrackEntries((current) => {
        const items = [...current, ...incomingItems];
        playlistTrackEntriesCacheRef.current[`${normalizedPlaylistId}:${playlistTrackEntriesOffset}`] = { items, hasMore, total, nextOffset };
        return items;
      });
      setPlaylistTrackEntriesHasMore(hasMore);
      setPlaylistTrackEntriesTotal(total);
      setPlaylistTrackEntriesNextOffset(nextOffset);
    } catch (error) {
      setPlaylistTrackEntriesError(error instanceof Error ? error.message : "More playlist tracks could not be loaded.");
    } finally {
      setPlaylistTrackEntriesLoading(false);
    }
  }

  async function fetchPlaybackToken() {
    if (experienceMode === "local") {
      throw new Error("Playback is unavailable in restricted local mode.");
    }
    if (spotifyCooldownActive) {
      throw new Error(formatCooldownCopy(reloadSecondsRemaining));
    }
    const response = await fetch(`${apiBaseUrl}/auth/token`, {
      credentials: "include",
    });
    if (!response.ok) {
      throw new Error("Spotify playback authorization is not available.");
    }
    const data = (await response.json()) as AuthTokenResponse;
    return data.access_token;
  }

  async function spotifyApiRequest(path: string, init: RequestInit) {
    const token = await fetchPlaybackToken();
    const response = await fetch(`https://api.spotify.com/v1${path}`, {
      ...init,
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
        ...(init.headers ?? {}),
      },
    });
    if (!response.ok && response.status !== 204) {
      throw new Error(`Spotify playback request failed (${response.status}).`);
    }
  }

  async function activatePlayerElement() {
    const player = spotifyPlayerRef.current;
    if (!player?.activateElement) {
      return;
    }
    try {
      await player.activateElement();
    } catch {
      // Browser activation is best-effort; playback calls still provide the real error.
    }
  }

  async function saveListenLabPlayerEvent(track: PlayerTrackSummary, sourceTrack?: RecentTrack | null, progressMs = 0) {
    if (experienceMode === "local" || !track.uri) {
      return null;
    }
    try {
      const response = await fetch(`${apiBaseUrl}/auth/player-listen-event`, {
        method: "POST",
        credentials: "include",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          track_uri: track.uri,
          track_id: spotifyTrackIdFromUri(track.uri) ?? sourceTrack?.track_id ?? null,
          track_name: sourceTrack?.track_name ?? track.name,
          artist_name: sourceTrack?.artist_name ?? track.artists,
          album_name: sourceTrack?.album_name ?? track.album,
          album_id: sourceTrack?.album_id ?? null,
          duration_ms: track.durationMs || sourceTrack?.duration_ms || null,
          progress_ms: Math.max(0, Math.floor(progressMs)),
          ms_played_confidence: "in_progress",
          played_at: new Date().toISOString(),
        }),
      });
      if (!response.ok) {
        return null;
      }
      const data = (await response.json()) as { row_id?: number | null };
      return typeof data.row_id === "number" ? data.row_id : null;
    } catch {
      return null;
    }
  }

  async function updateListenLabPlayerEventProgress(progressMs: number, confidence: "in_progress" | "paused" | "complete" = "in_progress") {
    if (experienceMode === "local" || activePlayerListenEventId == null) {
      return;
    }
    try {
      const response = await fetch(`${apiBaseUrl}/auth/player-listen-event`, {
        method: "POST",
        credentials: "include",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          row_id: activePlayerListenEventId,
          progress_ms: Math.max(0, Math.floor(progressMs)),
          ms_played_confidence: confidence,
        }),
      });
      if (response.ok) {
        const payload = (await response.json()) as { listen_qualified?: boolean };
        const shouldRefreshListenState = Boolean(payload.listen_qualified || confidence === "complete");
        if (shouldRefreshListenState && currentTrack && !optimisticQualifiedListenEventIdsRef.current.has(activePlayerListenEventId)) {
          optimisticQualifiedListenEventIdsRef.current.add(activePlayerListenEventId);
          const trackId = spotifyTrackIdFromUri(currentTrack.uri);
          if (trackId) {
            await refreshQualifiedListenState(
              trackId,
              playerDisplayKnownTrack?.release_track_id ?? null,
              new Date().toISOString(),
              progressMs,
              playbackDurationMs || currentTrack.durationMs,
            );
          }
        }
      }
    } catch {
      // Player progress persistence is best-effort and should not interrupt playback.
    }
  }

  async function refreshQualifiedListenState(
    trackId: string,
    releaseTrackId: number | null,
    playedAt: string,
    progressMs: number,
    durationMs: number,
  ) {
    const patchRows = (rows: AlbumTrackEntry[]) => rows.map((row) => {
      if (row.id !== trackId && (!releaseTrackId || row.releaseTrackId !== releaseTrackId)) {
        return row;
      }
      return {
        ...row,
        playCount: Math.max(0, row.playCount) + 1,
        lastPlayedAt: playedAt,
        sourceTrack: row.sourceTrack
          ? {
              ...row.sourceTrack,
              play_count: Math.max(0, Number(row.sourceTrack.play_count ?? 0)) + 1,
              last_played_at: playedAt,
              spotify_played_at: playedAt,
            }
          : row.sourceTrack,
      };
    });
    setAlbumTrackEntries(patchRows);
    setHomeAlbumTrackEntries(patchRows);
    const incrementCount = (value: number | null | undefined) => Math.max(0, Number(value ?? 0) || 0) + 1;
    const patchPlaylistRows = (rows: RecentTrack[]) => rows.map((row) => {
      const rowTrackId = row.track_id ?? spotifyTrackIdFromUri(row.uri ?? null);
      const releaseMatches = releaseTrackId != null && row.release_track_id === releaseTrackId;
      if (rowTrackId !== trackId && !releaseMatches) {
        return row;
      }
      return {
        ...row,
        track_id: row.track_id ?? trackId,
        release_track_id: row.release_track_id ?? releaseTrackId ?? null,
        spotify_played_at: playedAt,
        last_played_at: playedAt,
        source_last_played_at: playedAt,
        recording_last_played_at: playedAt,
        play_count: incrementCount(row.play_count),
        source_play_count: incrementCount(row.source_play_count ?? row.play_count),
        recording_play_count: incrementCount(row.recording_play_count ?? row.play_count ?? row.source_play_count),
        estimated_played_ms: progressMs,
        estimated_completion_ratio: durationMs > 0 ? Math.min(1, progressMs / durationMs) : 0.65,
      };
    });
    setPlaylistTrackEntries(patchPlaylistRows);
    for (const [cacheKey, cachedRows] of Object.entries(playlistTrackEntriesCacheRef.current)) {
      playlistTrackEntriesCacheRef.current[cacheKey] = {
        ...cachedRows,
        items: patchPlaylistRows(cachedRows.items),
      };
    }
    setProfile((current) => {
      if (!current) {
        return current;
      }
      const existing = current.recent_tracks.find((track) => track.track_id === trackId) ?? null;
      const optimistic: RecentTrack = {
        ...(existing ?? {
          track_id: trackId,
          track_name: livePlaybackSnapshot?.name ?? currentTrack?.name ?? "Unknown track",
          artist_name: livePlaybackSnapshot?.artist_names.join(", ") ?? currentTrack?.artists ?? "Unknown artist",
          album_name: livePlaybackSnapshot?.album_name ?? currentTrack?.album ?? "Unknown album",
        }),
        track_id: trackId,
        release_track_id: releaseTrackId ?? existing?.release_track_id ?? null,
        spotify_played_at: playedAt,
        last_played_at: playedAt,
        duration_ms: durationMs,
        estimated_played_ms: progressMs,
        estimated_completion_ratio: durationMs > 0 ? Math.min(1, progressMs / durationMs) : 0.65,
        source_label: "api",
      };
      return {
        ...current,
        recent_tracks: [optimistic, ...current.recent_tracks.filter((track) => track.track_id !== trackId)],
        recent_tracks_available: true,
      };
    });

    const selectedTrackMatches = selectedPreview?.kind === "track" && Boolean(
      selectedPreview.trackId === trackId
      || selectedPreview.entityId === trackId
      || selectedPreview.sourceTrack?.track_id === trackId
      || (releaseTrackId && selectedPreview.releaseTrackId === releaseTrackId),
    );
    if (!selectedTrackMatches || !releaseTrackId) {
      return;
    }
    const [detail, candidatePayload] = await Promise.all([
      fetchReleaseTrackDetail(releaseTrackId, trackId).catch(() => null),
      fetchRecordingTrackCandidateByReleaseTrack(releaseTrackId).catch(() => null),
    ]);
    if (detail) {
      setSelectedPreviewReleaseTrackDetail(detail);
    }
    if (candidatePayload) {
      setSelectedPreviewRecordingCandidate(candidatePayload.item);
      setSelectedPreviewRelatedCandidates(candidatePayload.items ?? (candidatePayload.item ? [candidatePayload.item] : []));
    }
  }

  function spotifyEntityUrl(kind: "track" | "artist" | "album", id: string | null | undefined) {
    return id ? `https://open.spotify.com/${kind}/${id}` : "";
  }

  function albumIdFromPreview(preview: PreviewItem | null | undefined) {
    if (!preview) {
      return null;
    }
    return preview.albumId ?? preview.sourceAlbumId ?? preview.sourceTrack?.album_id ?? (preview.kind === "album" ? preview.entityId : null);
  }

  function albumTrackRowsFromItems(
    items: Array<{
      id?: string | null;
      name?: string | null;
      uri?: string | null;
      duration_ms?: number | null;
      disc_number?: number | null;
      track_number?: number | null;
      artists?: TrackArtistEntry[];
      release_track_id?: number | null;
      release_track_name?: string | null;
      release_track_source_count?: number | null;
      release_track_duplicate_source_count?: number | null;
      has_release_track_siblings?: boolean | null;
      release_track_cluster_candidate_type?: string | null;
      release_track_cluster_relationship_kind?: string | null;
      play_count?: number | null;
      first_played_at?: string | null;
      last_played_at?: string | null;
      source_play_count?: number | null;
      source_playlist_count?: number | null;
      source_first_played_at?: string | null;
      source_last_played_at?: string | null;
      recording_play_count?: number | null;
      recording_playlist_count?: number | null;
      recording_first_played_at?: string | null;
      recording_last_played_at?: string | null;
      family_exclusive?: boolean | null;
      family_available_versions?: AlbumFamilyContext["versions"] | null;
      family_switch_album_id?: string | null;
      family_switch_label?: string | null;
      family_has_edition_relation?: boolean | null;
      family_has_external_recording_relation?: boolean | null;
    }>,
    selectedTrackId: string | null,
    selectedReleaseTrackId: number | null = null,
  ) {
    const normalizedTopTrackKeys = new Set(
      [
        ...(profile?.top_tracks ?? []),
        ...(profile?.recent_top_tracks ?? []),
      ].map((track) => normalizedTrackArtistKey(track.track_name, track.artist_name)),
    );
    const topTrackIds = new Set(
      [
        ...(profile?.top_tracks ?? []),
        ...(profile?.recent_top_tracks ?? []),
      ].map((track) => track.track_id).filter((value): value is string => Boolean(value)),
    );
    const knownTracksById = new Map<string, RecentTrack>();
    const latestPlayedAtByTrackId = new Map<string, string>();
    const knownTrackRows = [
      ...(profile?.recent_tracks ?? []),
      ...(profile?.top_tracks ?? []),
      ...(profile?.recent_top_tracks ?? []),
      ...(profile?.recent_likes_tracks ?? []),
    ];
    for (const knownTrack of knownTrackRows) {
      const knownTrackId = knownTrack.track_id;
      if (!knownTrackId) {
        continue;
      }
      if (!knownTracksById.has(knownTrackId)) {
        knownTracksById.set(knownTrackId, knownTrack);
      }
      for (const candidatePlayedAt of [knownTrack.spotify_played_at, knownTrack.last_played_at]) {
        if (!candidatePlayedAt) {
          continue;
        }
        const candidateMs = parseTimestampMs(candidatePlayedAt);
        if (candidateMs == null) {
          continue;
        }
        const existingPlayedAt = latestPlayedAtByTrackId.get(knownTrackId);
        const existingMs = parseTimestampMs(existingPlayedAt);
        if (existingMs == null || candidateMs > existingMs) {
          latestPlayedAtByTrackId.set(knownTrackId, candidatePlayedAt);
        }
      }
    }

    return items.map((item) => {
      const id = item.id ?? null;
      const itemArtists = uniqueArtistEntries(item.artists);
      const artistNames = itemArtists.map((artist) => artist.name ?? "").filter(Boolean).join(", ");
      const normalizedKey = normalizedTrackArtistKey(item.name ?? null, artistNames || null);
      const isTopTrack = Boolean((id && topTrackIds.has(id)) || normalizedTopTrackKeys.has(normalizedKey));
      const sourceTrack = id ? (knownTracksById.get(id) ?? null) : null;
      const backendFirstPlayedAt = typeof item.first_played_at === "string" && item.first_played_at.trim()
        ? item.first_played_at
        : null;
      const backendLastPlayedAt = typeof item.last_played_at === "string" && item.last_played_at.trim()
        ? item.last_played_at
        : null;
      const hasSourceLastPlayedAt = Object.prototype.hasOwnProperty.call(item, "source_last_played_at");
      const hasSourcePlayCount = Object.prototype.hasOwnProperty.call(item, "source_play_count");
      const sourceLastPlayedAt = hasSourceLastPlayedAt
        ? typeof item.source_last_played_at === "string" && item.source_last_played_at.trim()
          ? item.source_last_played_at
          : null
        : undefined;
      const sourcePlayCount = hasSourcePlayCount
        ? typeof item.source_play_count === "number" && Number.isFinite(item.source_play_count)
          ? Math.max(0, item.source_play_count)
          : (sourceLastPlayedAt ? 1 : 0)
        : undefined;
      const sourcePlaylistCount = typeof item.source_playlist_count === "number" && Number.isFinite(item.source_playlist_count)
        ? Math.max(0, item.source_playlist_count)
        : undefined;
      const lastPlayedAt = backendLastPlayedAt ?? (id ? (latestPlayedAtByTrackId.get(id) ?? null) : null);
      const playCount = typeof item.play_count === "number" && Number.isFinite(item.play_count) ? Math.max(0, item.play_count) : (lastPlayedAt ? 1 : 0);
      const recordingLastPlayedAt = typeof item.recording_last_played_at === "string" && item.recording_last_played_at.trim()
        ? item.recording_last_played_at
        : lastPlayedAt;
      const recordingPlayCount = typeof item.recording_play_count === "number" && Number.isFinite(item.recording_play_count)
        ? Math.max(0, item.recording_play_count)
        : playCount;
      const recordingPlaylistCount = typeof item.recording_playlist_count === "number" && Number.isFinite(item.recording_playlist_count)
        ? Math.max(0, item.recording_playlist_count)
        : (sourcePlaylistCount ?? 0);
      const recordingHistoryAvailable = Object.prototype.hasOwnProperty.call(item, "recording_last_played_at")
        && Object.prototype.hasOwnProperty.call(item, "recording_play_count");
      const sourceTrackWithHistory = sourceTrack
        ? {
          ...sourceTrack,
          first_played_at: backendFirstPlayedAt ?? sourceTrack.first_played_at ?? null,
          last_played_at: lastPlayedAt ?? sourceTrack.last_played_at ?? null,
          play_count: playCount || sourceTrack.play_count || null,
        }
        : (backendFirstPlayedAt || lastPlayedAt || playCount > 0)
          ? {
            track_id: id,
            track_name: item.name ?? "Unknown track",
            artist_name: artistNames || null,
            album_name: null,
            duration_ms: typeof item.duration_ms === "number" && Number.isFinite(item.duration_ms) ? Math.max(0, item.duration_ms) : null,
            uri: item.uri ?? (id ? `spotify:track:${id}` : null),
            first_played_at: backendFirstPlayedAt,
            last_played_at: lastPlayedAt,
            play_count: playCount,
          } satisfies RecentTrack
          : null;
      return {
        id,
        name: item.name ?? "Unknown track",
        uri: item.uri ?? null,
        durationMs: typeof item.duration_ms === "number" && Number.isFinite(item.duration_ms) ? Math.max(0, item.duration_ms) : null,
        discNumber: typeof item.disc_number === "number" && Number.isFinite(item.disc_number) ? item.disc_number : null,
        trackNumber: typeof item.track_number === "number" && Number.isFinite(item.track_number) ? item.track_number : null,
        artistName: artistNames || null,
        artists: itemArtists,
        sourceTrack: sourceTrackWithHistory,
        lastPlayedAt,
        sourceLastPlayedAt,
        sourcePlayCount,
        sourcePlaylistCount,
        recordingLastPlayedAt,
        recordingPlayCount,
        recordingPlaylistCount,
        recordingHistoryAvailable,
        familyExclusive: Boolean(item.family_exclusive),
        familyAvailableVersions: item.family_available_versions ?? [],
        familySwitchAlbumId: item.family_switch_album_id ?? null,
        familySwitchLabel: item.family_switch_label ?? null,
        familyHasEditionRelation: Boolean(item.family_has_edition_relation),
        familyHasExternalRecordingRelation: Boolean(item.family_has_external_recording_relation),
        playCount,
        isSelected: Boolean(
          (selectedTrackId && id && selectedTrackId === id)
          || (selectedReleaseTrackId && typeof item.release_track_id === "number" && item.release_track_id === selectedReleaseTrackId),
        ),
        isTopTrack,
        releaseTrackId: typeof item.release_track_id === "number" ? item.release_track_id : null,
        releaseTrackName: item.release_track_name ?? null,
        releaseTrackSourceCount: typeof item.release_track_source_count === "number" ? item.release_track_source_count : 0,
        releaseTrackDuplicateSourceCount: typeof item.release_track_duplicate_source_count === "number" ? item.release_track_duplicate_source_count : 0,
        hasReleaseTrackSiblings: Boolean(item.has_release_track_siblings),
        releaseTrackClusterCandidateType: item.release_track_cluster_candidate_type ?? null,
        releaseTrackClusterRelationshipKind: item.release_track_cluster_relationship_kind ?? null,
      } satisfies AlbumTrackEntry;
    });
  }

  function openPlayerTrackDetails() {
    if (!playerDisplayTrack) {
      return;
    }
    const trackId = spotifyTrackIdFromUri(playerDisplayTrack.uri) ?? livePlaybackSnapshot?.item_id ?? null;
    const trackUrl = spotifyTrackUrl(playerDisplayTrack.uri) ?? (trackId ? `https://open.spotify.com/track/${trackId}` : "");
    setSelectedPreview({
      image: playerDisplayTrack.image ?? playerDisplayKnownTrack?.image_url ?? null,
      label: playerDisplayTrack.name,
      meta: playerDisplayTrack.artists || null,
      detail: (playerDisplayKnownTrack?.album_name ?? playerDisplayTrack.album) || null,
      kind: "track",
      entityId: trackId,
      trackUri: playerDisplayTrack.uri,
      url: trackUrl,
      trackId,
      releaseTrackId: releaseTrackIdForSpotifyTrackId(trackId),
      albumId: playerDisplayKnownTrack?.album_id ?? null,
      artistName: playerDisplayTrack.artists || null,
      artists: playerDisplayArtists,
      sourceTrack: playerDisplayKnownTrack ?? null,
    });
  }

  function openPlayerArtistDetails(artist?: TrackArtistEntry) {
    const targetArtist = artist ?? playerDisplayArtist;
    const artistName = targetArtist?.name?.trim() || playerDisplayArtistName;
    if (!artistName) {
      return;
    }
    const artistId = targetArtist?.artist_id ?? targetArtist?.id ?? playerDisplayArtistId;
    const artistUrl = targetArtist?.url ?? spotifyEntityUrl("artist", artistId);
    setSelectedPreview({
      image: targetArtist?.image_url ?? findArtistImageUrl(artistName) ?? null,
      fallbackLabel: "A",
      label: artistName,
      meta: null,
      detail: null,
      kind: "artist",
      entityId: artistId,
      trackUri: null,
      url: artistUrl,
      trackId: null,
      albumId: null,
      artistName,
      artists: targetArtist ? [targetArtist] : artistEntriesFromText(artistName),
      targetArtists: targetArtist ? [targetArtist] : artistEntriesFromText(artistName),
      sourceAlbumId: playerDisplayAlbumId,
      sourceAlbumName: playerDisplayAlbumName,
      sourceAlbumImage: playerDisplayTrack?.image ?? playerDisplayKnownTrack?.image_url ?? null,
      sourceAlbumUrl: playerDisplayKnownTrack?.album_url ?? spotifyEntityUrl("album", playerDisplayAlbumId),
      sourceAlbumYear: playerDisplayAlbumYear,
      sourceTrack: playerDisplayKnownTrack ?? null,
    });
  }

  function openPlayerArtistsDetails() {
    if (playerDisplayArtists.length <= 1) {
      openPlayerArtistDetails(playerDisplayArtists[0]);
      return;
    }
    const artistLabel = playerDisplayArtistLabel ?? "Artists";
    setSelectedPreview({
      image: playerDisplayArtistImageUrl ?? playerDisplayTrack?.image ?? playerDisplayKnownTrack?.image_url ?? null,
      fallbackLabel: "A",
      label: artistLabel,
      meta: null,
      detail: null,
      kind: "artist",
      entityId: null,
      trackUri: null,
      url: "",
      trackId: null,
      albumId: null,
      artistName: artistLabel,
      artists: playerDisplayArtists,
      targetArtists: playerDisplayArtists,
      sourceAlbumId: playerDisplayAlbumId,
      sourceAlbumName: playerDisplayAlbumName,
      sourceAlbumImage: playerDisplayTrack?.image ?? playerDisplayKnownTrack?.image_url ?? null,
      sourceAlbumUrl: playerDisplayKnownTrack?.album_url ?? spotifyEntityUrl("album", playerDisplayAlbumId),
      sourceAlbumYear: playerDisplayAlbumYear,
      sourceTrack: playerDisplayKnownTrack ?? null,
    });
  }

  function openPlayerAlbumDetails() {
    if (!playerDisplayAlbumName) {
      return;
    }
    const albumUrl = playerDisplayKnownTrack?.album_url ?? spotifyEntityUrl("album", playerDisplayAlbumId);
    setSelectedPreview({
      image: playerDisplayTrack?.image ?? playerDisplayKnownTrack?.image_url ?? null,
      fallbackLabel: "L",
      label: playerDisplayAlbumName,
      meta: playerDisplayTrack?.artists ?? playerDisplayKnownTrack?.artist_name ?? null,
      detail: playerDisplayAlbumYear,
      kind: "album",
      entityId: playerDisplayAlbumId,
      trackUri: null,
      url: albumUrl,
      trackId: null,
      albumId: playerDisplayAlbumId,
      artistName: playerDisplayArtistName,
      artists: playerDisplayArtists,
      sourceAlbumId: playerDisplayAlbumId,
      sourceAlbumName: playerDisplayAlbumName,
      sourceAlbumImage: playerDisplayTrack?.image ?? playerDisplayKnownTrack?.image_url ?? null,
      sourceAlbumUrl: albumUrl,
      sourceAlbumYear: playerDisplayAlbumYear,
      sourceTrack: playerDisplayKnownTrack ?? null,
    });
  }

  function playerAlbumPreviewContext(): PreviewItem | null {
    if (!playerDisplayAlbumName) {
      return null;
    }
    const albumUrl = playerDisplayKnownTrack?.album_url ?? spotifyEntityUrl("album", playerDisplayAlbumId);
    return {
      image: playerDisplayTrack?.image ?? playerDisplayKnownTrack?.image_url ?? null,
      fallbackLabel: "L",
      label: playerDisplayAlbumName,
      meta: playerDisplayTrack?.artists ?? playerDisplayKnownTrack?.artist_name ?? null,
      detail: playerDisplayAlbumYear,
      kind: "album",
      entityId: playerDisplayAlbumId,
      trackUri: null,
      url: albumUrl,
      trackId: null,
      albumId: playerDisplayAlbumId,
      artistName: playerDisplayArtistName,
      artists: playerDisplayArtists,
      sourceAlbumId: playerDisplayAlbumId,
      sourceAlbumName: playerDisplayAlbumName,
      sourceAlbumImage: playerDisplayTrack?.image ?? playerDisplayKnownTrack?.image_url ?? null,
      sourceAlbumUrl: albumUrl,
      sourceAlbumYear: playerDisplayAlbumYear,
      sourceTrack: playerDisplayKnownTrack ?? null,
    };
  }

  function openRecentPlayerTrackDetails(track: RecentTrack) {
    const trackUri = trackUriWithFallback(track.uri, track.track_id);
    setSelectedPreview({
      image: track.image_url ?? null,
      fallbackLabel: "T",
      label: track.track_name ?? "Unknown track",
      meta: track.artist_name ?? null,
      detail: track.album_name ?? null,
      kind: "track",
      entityId: track.track_id ?? null,
      trackUri,
      url: track.url ?? spotifyTrackUrl(trackUri) ?? "",
      trackId: track.track_id ?? null,
      releaseTrackId: typeof track.release_track_id === "number" ? track.release_track_id : releaseTrackIdForSpotifyTrackId(track.track_id),
      albumId: track.album_id ?? null,
      artistName: track.artist_name ?? null,
      artists: track.artists ?? null,
      sourceTrack: track,
    });
  }

  function openRecentTrackAlbumPreview(track: RecentTrack) {
    const albumName = track.album_name?.trim() || "Unknown album";
    const albumId = track.album_id ?? null;
    const albumUrl = track.album_url ?? spotifyEntityUrl("album", albumId);
    const albumArtistEntries = uniqueArtistEntries(track.artists, artistEntriesFromText(track.artist_name));
    const albumArtistName = nonYearArtistText(track.artist_name)
      ?? albumArtistEntries.map((artist) => artist.name?.trim()).filter(Boolean).join(", ")
      ?? null;
    setSelectedPreview({
      image: track.image_url ?? null,
      fallbackLabel: "L",
      label: albumName,
      meta: albumArtistName,
      detail: track.album_release_year ?? null,
      kind: "album",
      entityId: albumId,
      trackUri: null,
      url: albumUrl,
      trackId: null,
      albumId,
      artistName: albumArtistName,
      artists: albumArtistEntries,
      targetArtists: null,
      sourceAlbumId: albumId,
      sourceAlbumName: albumName,
      sourceAlbumImage: track.image_url ?? null,
      sourceAlbumUrl: albumUrl,
      sourceAlbumYear: track.album_release_year ?? null,
      sourceTrack: track,
    });
  }

  function openRecentTrackArtistPreview(track: RecentTrack) {
    const artists = uniqueArtistEntries(track.artists, artistEntriesFromText(track.artist_name));
    const artistName = artists.map((artist) => artist.name?.trim()).filter(Boolean).join(", ")
      || nonYearArtistText(track.artist_name)
      || "Unknown artist";
    const firstArtist = artists[0] ?? null;
    const firstArtistId = firstArtist?.artist_id ?? firstArtist?.id ?? null;
    setSelectedPreview({
      image: firstArtist?.image_url ?? findArtistImageUrl(artistName) ?? null,
      fallbackLabel: "A",
      label: artistName,
      meta: null,
      detail: null,
      kind: "artist",
      entityId: firstArtistId,
      trackUri: null,
      url: firstArtist?.url ?? spotifyEntityUrl("artist", firstArtistId),
      trackId: null,
      albumId: null,
      artistName,
      artists,
      targetArtists: artists,
      sourceAlbumId: track.album_id ?? null,
      sourceAlbumName: track.album_name ?? null,
      sourceAlbumImage: track.image_url ?? null,
      sourceAlbumUrl: track.album_url ?? spotifyEntityUrl("album", track.album_id),
      sourceAlbumYear: track.album_release_year ?? null,
      sourceTrack: track,
    });
  }

  function openRecordingCandidateReleaseTrack(member: RecordingTrackCandidateMember, detailView: "recording" | "release" = "release") {
    preserveRecordingAlbumTracklistOpenRef.current = detailView === "recording" && recordingAlbumTracklistOpen;
    const sourceTrackIds = member.source_track_ids ?? [];
    const sourceTrackUris = member.source_track_uris ?? [];
    const sourceTrackDbIds = member.source_track_db_ids ?? [];
    const preferredAlbumVersion = recordingMemberPreferredAlbumVersion(member);
    const spotifyAlbumIds = member.spotify_album_ids ?? [];
    const albumReleaseDates = member.album_release_dates ?? [];
    const spotifyTrackId = sourceTrackIds[0] ?? null;
    const trackUri = sourceTrackUris[0] ?? (spotifyTrackId ? `spotify:track:${spotifyTrackId}` : null);
    const albumId = preferredAlbumVersion?.spotify_album_id ?? spotifyAlbumIds[0] ?? null;
    const albumImageUrl = preferredAlbumVersion?.image_url ?? recordingMemberAlbumImageUrl(member);
    const releaseDate = preferredAlbumVersion?.release_date
      ?? albumReleaseDates.find((value) => /^\d{4}/.test(String(value ?? "")))
      ?? null;
    const releaseYear = releaseDate ? String(releaseDate).slice(0, 4) : null;
    const artistName = recordingMemberArtistDisplay(member);
    const artists = recordingMemberArtistEntries(member);
    setSelectedPreview({
      image: albumImageUrl,
      fallbackLabel: "T",
      label: member.title || `Release track ${member.release_track_id}`,
      meta: artistName || null,
      detail: preferredAlbumVersion?.name ?? member.album ?? null,
      kind: "track",
      entityId: spotifyTrackId,
      trackUri,
      url: spotifyTrackUrl(trackUri) ?? (spotifyTrackId ? `https://open.spotify.com/track/${spotifyTrackId}` : ""),
      trackId: spotifyTrackId,
      releaseTrackId: member.release_track_id,
      releaseTrackName: member.title || null,
      releaseTrackSourceCount: sourceTrackDbIds.length || sourceTrackIds.length || null,
      hasReleaseTrackSiblings: sourceTrackDbIds.length > 1 || sourceTrackIds.length > 1,
      albumId,
      artistName: artistName || null,
      artists,
      sourceAlbumId: albumId,
      sourceAlbumName: preferredAlbumVersion?.name ?? member.album ?? null,
      sourceAlbumImage: albumImageUrl,
      sourceAlbumYear: releaseYear,
      sourceTrack: null,
      preferredDetailView: detailView,
    });
  }

  function openQueuePlayerTrackDetails(track: PlayerQueueTrack) {
    const queueKnownTrack = knownPlayerTracks.find((candidate) => {
      if (track.trackId && candidate.track_id && track.trackId === candidate.track_id) {
        return true;
      }
      return normalizedTrackArtistKey(candidate.track_name, candidate.artist_name) === normalizedTrackArtistKey(track.name, track.artists);
    }) ?? null;
    const resolvedTrackId = track.trackId ?? queueKnownTrack?.track_id ?? spotifyTrackIdFromUri(track.uri) ?? null;
    const resolvedTrackUri = track.uri ?? queueKnownTrack?.uri ?? (resolvedTrackId ? `spotify:track:${resolvedTrackId}` : null);
    const resolvedAlbumId = track.albumId ?? queueKnownTrack?.album_id ?? null;
    const resolvedAlbumName = track.album ?? queueKnownTrack?.album_name ?? null;
    const resolvedImage = track.image ?? queueKnownTrack?.image_url ?? null;
    const resolvedArtistName = track.artists ?? queueKnownTrack?.artist_name ?? null;
    const resolvedArtists = uniqueArtistEntries(track.artistItems, queueKnownTrack?.artists, artistEntriesFromText(resolvedArtistName));
    setSelectedPreview({
      image: resolvedImage,
      fallbackLabel: "T",
      label: track.name || queueKnownTrack?.track_name || "Track",
      meta: resolvedArtistName,
      detail: resolvedAlbumName,
      kind: "track",
      entityId: resolvedTrackId,
      trackUri: resolvedTrackUri,
      url: spotifyTrackUrl(resolvedTrackUri) ?? (queueKnownTrack?.url ?? ""),
      trackId: resolvedTrackId,
      releaseTrackId: queueKnownTrack?.release_track_id ?? releaseTrackIdForSpotifyTrackId(resolvedTrackId),
      releaseTrackName: queueKnownTrack?.release_track_name ?? null,
      releaseTrackSourceCount: queueKnownTrack?.release_track_source_count ?? null,
      releaseTrackDuplicateSourceCount: queueKnownTrack?.release_track_duplicate_source_count ?? null,
      hasReleaseTrackSiblings: queueKnownTrack?.has_release_track_siblings ?? null,
      releaseTrackClusterCandidateType: queueKnownTrack?.release_track_cluster_candidate_type ?? null,
      releaseTrackClusterRelationshipKind: queueKnownTrack?.release_track_cluster_relationship_kind ?? null,
      albumId: resolvedAlbumId,
      artistName: resolvedArtistName,
      artists: resolvedArtists,
      sourceAlbumId: resolvedAlbumId,
      sourceAlbumName: resolvedAlbumName,
      sourceAlbumImage: resolvedImage,
      sourceAlbumYear: queueKnownTrack?.album_release_year ?? null,
      sourceTrack: queueKnownTrack,
    });
  }

  function findArtistImageUrl(artistName: string | null | undefined): string | null {
    const target = primaryArtistName(artistName)?.toLocaleLowerCase() ?? null;
    if (!target || !profile) {
      return null;
    }
    const artistPools = [
      ...(profile.followed_artists ?? []),
      ...(profile.recent_top_artists ?? []),
    ];
    for (const artist of artistPools) {
      const candidate = artist.name?.trim().toLocaleLowerCase() ?? null;
      if (!candidate || !artist.image_url) {
        continue;
      }
      if (candidate === target) {
        return artist.image_url;
      }
    }
    return null;
  }

  useEffect(() => {
    if (!session?.authenticated || !profile || profile.product?.toLowerCase() !== "premium") {
      setPlayerReady(false);
      setPlayerError(profile && profile.product?.toLowerCase() !== "premium" ? "Spotify Premium is required for full playback." : null);
      setPlayerMenuOpen(false);
      spotifyPlayerRef.current?.disconnect();
      spotifyPlayerRef.current = null;
      spotifyDeviceIdRef.current = null;
      setCurrentTrack(null);
      setPlaybackPositionMs(0);
      setPlaybackDurationMs(0);
      return;
    }

    let cancelled = false;
    let connectTimeout: number | null = null;

    async function initializePlayer() {
      try {
        await fetchPlaybackToken();
      } catch (error) {
        if (!cancelled) {
          setPlayerError(error instanceof Error ? error.message : "Spotify playback authorization is not available.");
        }
        return;
      }

      const createPlayer = () => {
        if (cancelled || !window.Spotify || spotifyPlayerRef.current) {
          return;
        }

        setPlayerError(null);
        const player = new window.Spotify.Player({
          name: "ListenLab Player",
          getOAuthToken: (callback) => {
            void fetchPlaybackToken()
              .then((token) => callback(token))
              .catch(() => {
                setPlayerError("Spotify playback authorization expired. Reconnect Spotify.");
              });
          },
          volume: DEFAULT_PLAYER_VOLUME,
        });

        player.addListener("ready", ({ device_id }: { device_id: string }) => {
          spotifyDeviceIdRef.current = device_id;
          setPlayerReady(true);
          setPlayerError(null);
          if (connectTimeout != null) {
            window.clearTimeout(connectTimeout);
            connectTimeout = null;
          }
        });
        player.addListener("not_ready", () => {
          spotifyDeviceIdRef.current = null;
          setPlayerReady(false);
        });
        player.addListener("player_state_changed", (state: SpotifyPlayerState | null) => {
          if (!state) {
            return;
          }
          setLiveControlOverrideUntilMs(Date.now() + LIVE_PLAYBACK_POLL_INTERVAL_MS);
          if (previewingTrackUriRef.current) {
            return;
          }
          setCurrentTrack(currentTrackFromState(state));
          setPlaybackPaused(state.paused);
          setPlaybackPositionMs(state.position ?? 0);
          setPlaybackDurationMs(state.duration ?? state.track_window.current_track.duration_ms ?? 0);
        });
        player.addListener("initialization_error", ({ message }: { message: string }) => setPlayerError(message));
        player.addListener("authentication_error", ({ message }: { message: string }) => setPlayerError(message));
        player.addListener("account_error", ({ message }: { message: string }) => setPlayerError(message));
        player.addListener("playback_error", ({ message }: { message: string }) => {
          const normalized = message.toLocaleLowerCase();
          if (normalized.includes("no list was loaded")) {
            // SDK can emit this while API fallback playback control is still valid.
            setPlayerError(null);
            void loadCurrentPlaybackSnapshot();
            return;
          }
          setPlayerError(message);
        });

        spotifyPlayerRef.current = player;
        connectTimeout = window.setTimeout(() => {
          if (!cancelled && !spotifyDeviceIdRef.current) {
            setPlayerError("Spotify player could not connect. Open Spotify on a device and try again.");
          }
        }, 12000);
        void player.connect().then((connected) => {
          if (!connected && !cancelled) {
            setPlayerError("Spotify player connection was rejected. Reconnect Spotify and try again.");
          }
        });
      };

      if (window.Spotify) {
        createPlayer();
        return;
      }

      const script = document.querySelector<HTMLScriptElement>('script[data-spotify-sdk="true"]');
      if (!script) {
        const spotifyScript = document.createElement("script");
        spotifyScript.src = "https://sdk.scdn.co/spotify-player.js";
        spotifyScript.async = true;
        spotifyScript.dataset.spotifySdk = "true";
        document.body.appendChild(spotifyScript);
      }
      window.onSpotifyWebPlaybackSDKReady = createPlayer;
    }

    void initializePlayer();
    return () => {
      cancelled = true;
      if (connectTimeout != null) {
        window.clearTimeout(connectTimeout);
      }
    };
  }, [profile, session]);

  useEffect(() => {
    if (!currentTrack || playbackPaused || previewingTrackUri) {
      return;
    }

    const timer = window.setInterval(() => {
      setPlaybackPositionMs((current) => {
        const ceiling = playbackDurationMs || currentTrack.durationMs || 0;
        return ceiling > 0 ? Math.min(current + 1000, ceiling) : current + 1000;
      });
    }, 1000);

    return () => {
      window.clearInterval(timer);
    };
  }, [currentTrack, playbackDurationMs, playbackPaused, previewingTrackUri]);

  useEffect(() => {
    if (queueSleepTimerUntilMs == null) {
      return;
    }

    const remainingMs = queueSleepTimerUntilMs - Date.now();
    if (remainingMs <= 0) {
      setQueueSleepTimerUntilMs(null);
      void pausePlayback();
      return;
    }

    const timer = window.setTimeout(() => {
      setQueueSleepTimerUntilMs(null);
      void pausePlayback();
    }, remainingMs);
    return () => {
      window.clearTimeout(timer);
    };
  }, [queueSleepTimerUntilMs]);

  function scrollQueueListToRole(queueList: HTMLElement | null, role: "current" | "up-next") {
    const anchorRow = queueList?.querySelector<HTMLElement>(`[data-player-queue-role='${role}']`) ?? null;
    if (!queueList || !anchorRow) {
      return;
    }
    const listRect = queueList.getBoundingClientRect();
    const rowRect = anchorRow.getBoundingClientRect();
    queueList.scrollTop += rowRect.top - listRect.top;
  }

  function scrollQueueListToAutoAnchor(queueList: HTMLElement | null) {
    const anchorRole = queueList?.querySelector<HTMLElement>("[data-player-queue-role='up-next']")
      ? "up-next"
      : "current";
    scrollQueueListToRole(queueList, anchorRole);
  }

  function scrollQueueToAutoAnchor() {
    const menu = playerMenuRef.current;
    const popupQueueList = menu?.querySelector<HTMLElement>(".player-queue-column .player-recent-list") ?? null;
    scrollQueueListToAutoAnchor(popupQueueList);
    scrollQueueListToAutoAnchor(homeQueueListRef.current);
  }

  useEffect(() => {
    if (playerQueueSource !== "listenlab" || playerQueueCursor == null) {
      return;
    }

    const frame = window.requestAnimationFrame(() => {
      scrollQueueToAutoAnchor();
    });
    return () => {
      window.cancelAnimationFrame(frame);
    };
  }, [appPage, playerMenuOpen, playerQueueCursor, playerQueueSource, playerQueueTracks.length]);

  useEffect(() => {
    playbackPositionMsRef.current = playbackPositionMs;
  }, [playbackPositionMs]);

  useEffect(() => {
    if (
      !currentTrack?.uri
      || previewingTrackUri
      || playerQueueSource !== "listenlab"
      || playerQueueCursor == null
    ) {
      autoAdvanceTrackUriRef.current = null;
      return;
    }
    const durationMs = playbackDurationMs || currentTrack.durationMs || 0;
    const trackAtEnd = durationMs > 0 && playbackPositionMs >= durationMs - 500;
    if (playbackPaused && !trackAtEnd) {
      autoAdvanceTrackUriRef.current = null;
      return;
    }
    const queueAtEnd = playerQueueCursor >= playerQueueTracks.length - 1;
    if (queueAtEnd && !playerQueueLoopEnabled && !playerTrackLoopEnabled && !queuePauseAfterCurrentEnabled) {
      autoAdvanceTrackUriRef.current = null;
      return;
    }

    if (!trackAtEnd) {
      autoAdvanceTrackUriRef.current = null;
      return;
    }

    if (autoAdvanceTrackUriRef.current === currentTrack.uri) {
      return;
    }
    autoAdvanceTrackUriRef.current = currentTrack.uri;
    if (queuePauseAfterCurrentEnabled) {
      void pauseAtNextListenLabQueueTrack();
    } else if (playerTrackLoopEnabled) {
      void playQueueTrackAtIndex(playerQueueCursor, { markPreviousComplete: true });
    } else if (queueAtEnd && playerQueueLoopEnabled) {
      void playQueueTrackAtIndex(0, { markPreviousComplete: true });
    } else {
      void startNextListenLabQueueTrack();
    }
  }, [currentTrack, playbackDurationMs, playbackPaused, playbackPositionMs, playerQueueCursor, playerQueueSource, playerQueueTracks, playerQueueLoopEnabled, playerTrackLoopEnabled, queuePauseAfterCurrentEnabled, previewingTrackUri]);

  useEffect(() => {
    if (!currentTrack || playbackPaused || previewingTrackUri || activePlayerListenEventId == null) {
      return;
    }

    const timer = window.setInterval(() => {
      const durationMs = playbackDurationMs || currentTrack.durationMs || 0;
      const currentPositionMs = playbackPositionMsRef.current;
      const confidence = durationMs > 0 && currentPositionMs >= durationMs * 0.98 ? "complete" : "in_progress";
      void updateListenLabPlayerEventProgress(currentPositionMs, confidence);
    }, 5000);

    return () => {
      window.clearInterval(timer);
    };
  }, [activePlayerListenEventId, currentTrack, playbackDurationMs, playbackPaused, previewingTrackUri]);

  useEffect(() => {
    setPlayerRecentTracks(dedupeRecentTracksForPlayer(profile?.recent_tracks ?? []));
    setPlayerRecentTracksError(null);
    if (profile?.recent_tracks_available) {
      setPlayerRecentTracksLoadAttempted(true);
    }
  }, [profile?.recent_tracks, profile?.recent_tracks_available]);

  useEffect(() => {
    if (!startupDashboardReleased || !playerPanelVisible || !profile || playerRecentTracksLoading) {
      return;
    }
    if ((!playbackPaused && currentTrack) || (playerRecentTracks.length > 0 && !playerRecentTracksError)) {
      return;
    }

    void loadPlayerRecentTracks();
  }, [currentTrack, experienceMode, playbackPaused, playerPanelVisible, playerRecentTracks.length, playerRecentTracksError, playerRecentTracksLoading, profile, recentRange, startupDashboardReleased]);

  useEffect(() => {
    if (!startupDashboardReleased || !playerPanelVisible || !profile) {
      return;
    }

    if (playerQueueCleared) {
      return;
    }

    if (usingRecentLikedStartupFallback) {
      resetQueueControls();
      setPlayerQueueCleared(false);
      replacePlayerQueueTracks(recentTracksToPlayerQueueTracks(profile.recent_likes_tracks), { label: "Recent Likes" });
      setPlayerQueueCursor(0);
      setPlayerQueueSource("listenlab");
      setPlayerQueueContext({ label: "Recent Likes" });
      setPlayerQueuePlayedKeys(new Set());
      setPlayerQueueError(null);
      setPlayerQueueLoadAttempted(true);
      return;
    }

    if (!liveSpotifyPlaybackShouldOwnQueue && playerQueueSource === "listenlab") {
      return;
    }

    if (playerQueueLoading) {
      return;
    }

    void loadPlayerQueueTracks();
  }, [playerPanelVisible, profile, experienceMode, usingRecentLikedStartupFallback, liveSpotifyPlaybackShouldOwnQueue, livePlaybackSnapshot?.item_id, playerQueueSource, playerQueueCleared, startupDashboardReleased]);

  useEffect(() => {
    if (pendingSeekMs == null) {
      return;
    }
    setPendingSeekMs(null);
  }, [currentTrack?.uri]);

  useEffect(() => {
    if (overlaySeekMs == null) {
      return;
    }
    setOverlaySeekMs(null);
  }, [currentTrack?.uri, selectedPreview?.trackUri]);

  useEffect(() => {
    if (
      !hasPremiumPlayback
      || currentTrack
      || !profile
      || !livePlaybackProbeComplete
      || usingLivePlaybackSnapshot
    ) {
      return;
    }

    const seedTrack = profile.recent_likes_tracks[0] ?? null;
    if (!seedTrack?.track_name) {
      return;
    }

    setCurrentTrack({
      name: seedTrack.track_name,
      artists: seedTrack.artist_name ?? "Unknown artist",
      album: seedTrack.album_name ?? "Unknown album",
      image: seedTrack.image_url ?? null,
      uri: seedTrack.uri ?? null,
      durationMs: 0,
    });
    setPlaybackPaused(true);
    setPlaybackPositionMs(0);
    setPlaybackDurationMs(0);
  }, [currentTrack, hasPremiumPlayback, livePlaybackProbeComplete, profile, usingLivePlaybackSnapshot]);

  useEffect(() => {
    if (!livePlaybackSnapshot || !livePlaybackTrackSummary || liveControlOverrideActive) {
      return;
    }
    if (liveSpotifyPlaybackShouldOwnQueue) {
      setPlayerQueueSource("spotify");
    }
    setCurrentTrack(livePlaybackTrackSummary);
    setPlaybackPaused(!Boolean(livePlaybackSnapshot.is_playing));
    setPlaybackPositionMs(Math.max(0, Number(livePlaybackSnapshot.progress_ms ?? 0)));
    setPlaybackDurationMs(Math.max(0, Number(livePlaybackSnapshot.duration_ms ?? 0)));
  }, [liveControlOverrideActive, livePlaybackSnapshot, livePlaybackTrackSummary, liveSpotifyPlaybackShouldOwnQueue]);

  async function playTrackUri(trackUri: string | null, positionMs = 0, options?: { syncQueuePlaylist?: boolean; queuePlaylistUris?: string[] | null }) {
    await activatePlayerElement();
    if (!trackUri) {
      setPlayerError("This item does not have a playable Spotify track.");
      return false;
    }
    let syncedPlaylistUri: string | null = null;
    const explicitQueuePlaylistUris = options?.queuePlaylistUris?.filter((uri) => uri.startsWith("spotify:track:")) ?? null;
    if (explicitQueuePlaylistUris || (options?.syncQueuePlaylist && playerQueueSource === "listenlab")) {
      const listenLabUpcomingTracks = playerQueueCursor == null
        ? playerQueueTracks
        : playerQueueTracks.slice(playerQueueCursor + 1);
      const queuePlaylistUris = explicitQueuePlaylistUris ?? queuePlaylistTrackUris(trackUri, listenLabUpcomingTracks);
      if (queuePlaylistUris.length > 0) {
        try {
          const playlist = await syncQueuePlaylist(queuePlaylistUris);
          syncedPlaylistUri = playlist.playlist_uri?.startsWith("spotify:playlist:") ? playlist.playlist_uri : null;
          setQueuePlaylistUri(syncedPlaylistUri);
        } catch (error) {
          setQueuePlaylistUri(null);
          console.warn("ListenLab queue playlist sync failed; falling back to single-track playback.", error);
        }
      }
    }
    const deviceId = spotifyDeviceIdRef.current;

    const safePositionMs = Math.max(0, Math.floor(positionMs));
    const singleTrackPayload = JSON.stringify({
      uris: [trackUri],
      position_ms: safePositionMs,
    });
    const playlistContextPayload = syncedPlaylistUri
      ? JSON.stringify({
        context_uri: syncedPlaylistUri,
        offset: { uri: trackUri },
        position_ms: safePositionMs,
      })
      : null;
    const preferredPayload = playlistContextPayload ?? singleTrackPayload;
    try {
      if (deviceId) {
        await spotifyApiRequest(`/me/player/play?device_id=${encodeURIComponent(deviceId)}`, {
          method: "PUT",
          body: preferredPayload,
        });
        setPlayerError(null);
        return true;
      }
    } catch (primaryError) {
      try {
        if (deviceId && playlistContextPayload) {
          await spotifyApiRequest(`/me/player/play?device_id=${encodeURIComponent(deviceId)}`, {
            method: "PUT",
            body: singleTrackPayload,
          });
          setPlayerError(null);
          return true;
        }
      } catch {
        // Fall through to global play endpoint.
      }
      setPlayerError(primaryError instanceof Error ? primaryError.message : "Spotify playback could not be started.");
    }
    try {
      await spotifyApiRequest("/me/player/play", {
        method: "PUT",
        body: preferredPayload,
      });
      setPlayerError(null);
      return true;
    } catch (fallbackError) {
      if (playlistContextPayload) {
        try {
          await spotifyApiRequest("/me/player/play", {
            method: "PUT",
            body: singleTrackPayload,
          });
          setPlayerError(null);
          return true;
        } catch {
          // Report the original fallback error below.
        }
      }
      setPlayerError(
        fallbackError instanceof Error
          ? fallbackError.message
          : "Spotify playback could not be started.",
      );
      return false;
    }
  }

  async function pausePlayback() {
    void updateListenLabPlayerEventProgress(playbackPositionMs, "paused");
    const player = spotifyPlayerRef.current;
    if (player) {
      try {
        await player.pause();
        setPlaybackPaused(true);
        setPlayerError(null);
        return true;
      } catch {
        // Fall back to web API pause.
      }
    }
    try {
      await spotifyApiRequest("/me/player/pause", {
        method: "PUT",
      });
      setPlaybackPaused(true);
      setPlayerError(null);
      return true;
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback could not be paused.");
      return false;
    }
  }

  async function resumePlayback() {
    await activatePlayerElement();
    if (currentTrack?.uri && (playbackDurationMs <= 0 || currentTrack.durationMs <= 0)) {
      const resumed = await playTrackUri(currentTrack.uri, Math.max(0, playbackPositionMs), {
        syncQueuePlaylist: playerQueueSource === "listenlab",
      });
      if (resumed) {
        setQueuePausedCursor(null);
      }
      return resumed;
    }
    const player = spotifyPlayerRef.current;
    if (player) {
      try {
        await player.resume();
        setPlaybackPaused(false);
        setQueuePausedCursor(null);
        setPlayerError(null);
        return true;
      } catch {
        // Fall back to web API play.
      }
    }
    const deviceId = spotifyDeviceIdRef.current;
    try {
      if (deviceId) {
        await spotifyApiRequest(`/me/player/play?device_id=${encodeURIComponent(deviceId)}`, {
          method: "PUT",
        });
      } else {
        await spotifyApiRequest("/me/player/play", {
          method: "PUT",
        });
      }
      setPlaybackPaused(false);
      setQueuePausedCursor(null);
      setPlayerError(null);
      return true;
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback could not be resumed.");
      return false;
    }
  }

  async function togglePlayerPlayback() {
    await activatePlayerElement();
    try {
      let updated = false;
      if (playbackPaused) {
        updated = await resumePlayback();
      } else {
        updated = await pausePlayback();
      }
      if (!updated) {
        await loadCurrentPlaybackSnapshot();
      }
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback could not be updated.");
      await loadCurrentPlaybackSnapshot();
    }
  }

  async function takeOverPlaybackFromLiveSnapshot() {
    await activatePlayerElement();
    const deviceId = spotifyDeviceIdRef.current;

    try {
      if (deviceId) {
        await spotifyApiRequest("/me/player", {
          method: "PUT",
          body: JSON.stringify({ device_ids: [deviceId], play: true }),
        });
      }
      if (playerDisplayTrack?.uri) {
        if (deviceId) {
          await spotifyApiRequest(`/me/player/play?device_id=${encodeURIComponent(deviceId)}`, {
            method: "PUT",
            body: JSON.stringify({
              uris: [playerDisplayTrack.uri],
              position_ms: Math.max(0, Math.floor(playerDisplayPositionMs)),
            }),
          });
        } else {
          await spotifyApiRequest("/me/player/play", {
            method: "PUT",
            body: JSON.stringify({
              uris: [playerDisplayTrack.uri],
              position_ms: Math.max(0, Math.floor(playerDisplayPositionMs)),
            }),
          });
        }
      }
      setPlayerError(null);
      setLiveControlOverrideUntilMs(Date.now() + LIVE_PLAYBACK_POLL_INTERVAL_MS);
      await loadCurrentPlaybackSnapshot();
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback could not be switched.");
    }
  }

  async function takeOverAndPausePlayback() {
    await activatePlayerElement();
    const deviceId = spotifyDeviceIdRef.current;
    try {
      if (deviceId) {
        await spotifyApiRequest("/me/player", {
          method: "PUT",
          body: JSON.stringify({ device_ids: [deviceId], play: false }),
        });
      }
      await pausePlayback();
      setPlayerError(null);
      setLiveControlOverrideUntilMs(Date.now() + LIVE_PLAYBACK_POLL_INTERVAL_MS);
      await loadCurrentPlaybackSnapshot();
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback could not be paused on ListenLab.");
    }
  }

  async function handlePlayerPrimaryButtonClick() {
    await activatePlayerElement();
    if (previewingTrackUriRef.current) {
      await stopTrackPreviewPlayback(true);
      return;
    }
    if (!liveReadOnlyMode) {
      await togglePlayerPlayback();
      return;
    }
    if (!playerDisplayPaused) {
      await takeOverAndPausePlayback();
      return;
    }
    await takeOverPlaybackFromLiveSnapshot();
  }

  async function handlePopupTrackPlayback(trackUri: string | null, options?: PopupTrackPlaybackOptions) {
    await activatePlayerElement();
    clearPreviewPlaybackState();
    if (!trackUri) {
      setPlayerError("This item does not have a playable Spotify track.");
      return false;
    }

    const player = spotifyPlayerRef.current;
    const isCurrent = currentTrack?.uri === trackUri;

    try {
      if (isCurrent && !playbackPaused && player) {
        await player.pause();
        setPlaybackPaused(true);
        return true;
      }

      if (isCurrent && playbackPaused && player) {
        await player.resume();
        setPlaybackPaused(false);
        return true;
      }

      const playbackStarted = await playTrackUri(trackUri, 0, {
        queuePlaylistUris: options?.queuePlaylistUris,
        syncQueuePlaylist: true,
      });
      if (!playbackStarted) {
        return false;
      }
      const optimisticTrack = options?.optimisticTrack ?? null;
      let nextCurrentTrack: PlayerTrackSummary;
      if (optimisticTrack) {
        nextCurrentTrack = {
          ...optimisticTrack,
          uri: trackUri,
        };
        setCurrentTrack(nextCurrentTrack);
      } else {
        nextCurrentTrack = {
          name: "Spotify Playback",
          artists: "Unknown artist",
          album: "Unknown album",
          image: null,
          uri: trackUri,
          durationMs: 0,
        };
        setCurrentTrack((current) => (
          current && current.uri === trackUri
            ? current
            : nextCurrentTrack
        ));
      }
      setPlaybackPaused(false);
      setPlaybackPositionMs(0);
      setPlaybackDurationMs(Math.max(0, options?.optimisticTrack?.durationMs ?? 0));
      if (options?.queueTracks) {
        resetQueueControls();
        setPlayerQueueCleared(false);
        replacePlayerQueueTracks(options.queueTracks, options.queueContext ?? null);
        setPlayerQueueCursor(options.queueCursor ?? 0);
        setPlayerQueueSource("listenlab");
        setPlayerQueueContext(options.queueContext ?? null);
        resetQueuePlayedKeys(nextCurrentTrack);
        setPlayerQueueError(null);
      }
      const listenEventId = await saveListenLabPlayerEvent(nextCurrentTrack, options?.sourceTrack ?? null, 0);
      setActivePlayerListenEventId(listenEventId);
      setPlayerRecentTracks((current) => {
        const sourceTrack = options?.sourceTrack ?? null;
        const optimisticRecentTrack: RecentTrack = {
          ...(sourceTrack ?? {
            track_id: spotifyTrackIdFromUri(trackUri),
            track_name: nextCurrentTrack.name,
            artist_name: nextCurrentTrack.artists,
            album_name: nextCurrentTrack.album,
            duration_ms: nextCurrentTrack.durationMs,
            uri: trackUri,
            image_url: nextCurrentTrack.image,
            album_id: null,
            url: spotifyTrackUrl(trackUri),
          }),
          event_id: listenEventId,
          spotify_played_at: new Date().toISOString(),
          estimated_played_ms: 0,
          estimated_completion_ratio: 0,
          source_label: "api",
        };
        return dedupeRecentTracksForPlayer([
          optimisticRecentTrack,
          ...current.filter((track) => trackUriWithFallback(track.uri, track.track_id) !== trackUri),
        ]);
      });
      return true;
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback could not be updated.");
      return false;
    }
  }

  function playerQueueTrackFromPlaybackRequest(request: PlaybackActionRequest): PlayerQueueTrack | null {
    const trackUri = request.trackUri;
    if (!trackUri) {
      return null;
    }
    const optimisticTrack = request.optimisticTrack;
    const sourceTrack = request.sourceTrack;
    return {
      name: optimisticTrack?.name ?? sourceTrack?.track_name ?? "Spotify Playback",
      artists: optimisticTrack?.artists ?? sourceTrack?.artist_name ?? "Unknown artist",
      album: optimisticTrack?.album ?? sourceTrack?.album_name ?? "Unknown album",
      image: optimisticTrack?.image ?? sourceTrack?.image_url ?? null,
      uri: trackUri,
      durationMs: Math.max(0, optimisticTrack?.durationMs ?? sourceTrack?.duration_ms ?? 0),
      trackId: spotifyTrackIdFromUri(trackUri) ?? sourceTrack?.track_id ?? null,
      albumId: sourceTrack?.album_id ?? null,
      artistItems: sourceTrack?.artists ?? artistEntriesFromText(optimisticTrack?.artists ?? sourceTrack?.artist_name),
      hasReleaseTrackSiblings: sourceTrack
        ? Boolean(
          sourceTrack.has_release_track_siblings
          || hasReleaseSiblingForTrackId(sourceTrack.track_id)
          || Number(sourceTrack.release_track_duplicate_source_count ?? 0) > 1
          || sourceTrack.release_track_cluster_candidate_type,
        )
        : null,
      isLiked: sourceTrack ? recentTrackIsKnownLiked(sourceTrack, spotifyTrackIdFromUri(trackUri)) : null,
      likedAt: sourceTrack?.liked_at ?? null,
      releaseTrackId: sourceTrack?.release_track_id ?? null,
      releaseTrackName: sourceTrack?.release_track_name ?? null,
      releaseTrackSourceCount: sourceTrack?.release_track_source_count ?? null,
      releaseTrackDuplicateSourceCount: sourceTrack?.release_track_duplicate_source_count ?? null,
      releaseTrackClusterCandidateType: sourceTrack?.release_track_cluster_candidate_type ?? null,
      releaseTrackClusterRelationshipKind: sourceTrack?.release_track_cluster_relationship_kind ?? null,
    };
  }

  function currentPlayerQueueTrack(): PlayerQueueTrack | null {
    const displayTrack = currentTrack ?? playerDisplayTrack;
    if (!displayTrack?.uri) {
      return null;
    }
    const knownTrack = playerDisplayKnownTrack;
    return {
      ...displayTrack,
      trackId: spotifyTrackIdFromUri(displayTrack.uri) ?? knownTrack?.track_id ?? null,
      albumId: knownTrack?.album_id ?? playerDisplayAlbumId ?? null,
      artistItems: knownTrack?.artists ?? artistEntriesFromText(displayTrack.artists),
      hasReleaseTrackSiblings: knownTrack
        ? Boolean(
          knownTrack.has_release_track_siblings
          || hasReleaseSiblingForTrackId(knownTrack.track_id)
          || Number(knownTrack.release_track_duplicate_source_count ?? 0) > 1
          || knownTrack.release_track_cluster_candidate_type,
        )
        : null,
      isLiked: knownTrack ? recentTrackIsKnownLiked(knownTrack, spotifyTrackIdFromUri(displayTrack.uri)) : null,
      likedAt: knownTrack?.liked_at ?? null,
      releaseTrackId: knownTrack?.release_track_id ?? null,
      releaseTrackName: knownTrack?.release_track_name ?? null,
      releaseTrackSourceCount: knownTrack?.release_track_source_count ?? null,
      releaseTrackDuplicateSourceCount: knownTrack?.release_track_duplicate_source_count ?? null,
      releaseTrackClusterCandidateType: knownTrack?.release_track_cluster_candidate_type ?? null,
      releaseTrackClusterRelationshipKind: knownTrack?.release_track_cluster_relationship_kind ?? null,
    };
  }

  function playerQueueGroupFromTracks(
    tracks: PlayerQueueTrack[],
    context: PlayerQueueContext | null | undefined,
    fallbackLabel: string = "Current queue",
  ): PlayerQueueGroup | null {
    if (tracks.length === 0) {
      return null;
    }
    const label = context?.playlistName ?? context?.label ?? fallbackLabel;
    const idBase = context?.playlistId ?? context?.url ?? label;
    return {
      id: `${idBase}:${tracks.map((track) => queueTrackIdentity(track) ?? track.name).join("|")}`,
      label,
      url: context?.url ?? null,
      imageUrl: tracks.find((track) => Boolean(track.image))?.image ?? null,
      tracks,
    };
  }

  function setSinglePlayerQueueGroup(
    tracks: PlayerQueueTrack[],
    context: PlayerQueueContext | null | undefined,
    fallbackLabel?: string,
  ) {
    const group = playerQueueGroupFromTracks(tracks, context, fallbackLabel);
    setPlayerQueueGroups(group ? [group] : []);
    setHomeQueueOpenGroupIds(group ? new Set([group.id]) : new Set());
    setPlayerQueueGroupCursors(group ? { [group.id]: 0 } : {});
  }

  function replacePlayerQueueTracks(
    tracks: PlayerQueueTrack[],
    context: PlayerQueueContext | null | undefined = playerQueueContext,
    fallbackLabel?: string,
  ) {
    setPlayerQueueTracks(tracks);
    setSinglePlayerQueueGroup(tracks, context, fallbackLabel);
  }

  function playerQueueGroupsForNavigation() {
    const fallbackGroup = playerQueueGroupFromTracks(playerQueueTracks, playerQueueContext, playerQueueSource === "spotify" ? "Spotify queue" : "Current queue");
    return playerQueueGroups.length > 0
      ? playerQueueGroups
      : [fallbackGroup].filter((group): group is PlayerQueueGroup => Boolean(group));
  }

  function queueGroupBounds(groups: PlayerQueueGroup[]) {
    let start = 0;
    return groups.map((group) => {
      const bounds = {
        group,
        start,
        end: start + group.tracks.length - 1,
      };
      start += group.tracks.length;
      return bounds;
    });
  }

  function queueGroupBoundsForIndex(index: number) {
    return queueGroupBounds(playerQueueGroupsForNavigation()).find((bounds) => index >= bounds.start && index <= bounds.end) ?? null;
  }

  async function jumpToAdjacentQueueGroup(direction: "previous" | "next") {
    if (playerQueueSource !== "listenlab") {
      await movePlaybackQueue(direction);
      return;
    }
    const currentIndex = playerQueueCursor ?? activeQueueCursor;
    if (currentIndex == null || currentIndex < 0) {
      setPlayerError("ListenLab queue position could not be found.");
      return;
    }
    const bounds = queueGroupBounds(playerQueueGroupsForNavigation());
    const currentGroupIndex = bounds.findIndex((item) => currentIndex >= item.start && currentIndex <= item.end);
    const targetGroup = bounds[direction === "next" ? currentGroupIndex + 1 : currentGroupIndex - 1] ?? null;
    if (!targetGroup) {
      setPlayerError(direction === "next" ? "No next queue context is available." : "No previous queue context is available.");
      return;
    }
    const savedGroupCursor = playerQueueGroupCursors[targetGroup.group.id];
    const targetRelativeIndex = direction === "next"
      ? 0
      : Math.min(Math.max(0, savedGroupCursor ?? targetGroup.group.tracks.length - 1), targetGroup.group.tracks.length - 1);
    setHomeQueueOpenGroupIds((current) => {
      const next = new Set(current);
      next.add(targetGroup.group.id);
      return next;
    });
    await playQueueTrackAtIndex(targetGroup.start + targetRelativeIndex, { markPreviousComplete: true });
  }

  function insertPlaybackActionTracks(request: PlaybackActionRequest, placement: "next" | "end") {
    const requestedTracks = request.insertTracks?.length
      ? request.insertTracks
      : [playerQueueTrackFromPlaybackRequest(request)].filter((track): track is PlayerQueueTrack => Boolean(track));
    if (requestedTracks.length === 0) {
      setPlayerError("This item does not have a playable Spotify track.");
      return;
    }
    const baseTracks = playerQueueTracks.length > 0
      ? playerQueueTracks
      : [currentPlayerQueueTrack()].filter((track): track is PlayerQueueTrack => Boolean(track));
    const currentIdentity = queueTrackIdentity(currentTrack);
    const fallbackCursor = currentIdentity
      ? baseTracks.findIndex((track) => queueTrackIdentity(track) === currentIdentity)
      : -1;
    const baseCursor = playerQueueSource === "listenlab" && playerQueueCursor != null
      ? playerQueueCursor
      : hasActiveQueueCursor
        ? activeQueueCursor
      : (fallbackCursor >= 0 ? fallbackCursor : null);
    const insertAt = placement === "next" && baseCursor != null && baseCursor >= 0
      ? baseCursor + 1
      : baseTracks.length;
    const nextTracks = [
      ...baseTracks.slice(0, insertAt),
      ...requestedTracks,
      ...baseTracks.slice(insertAt),
    ];
    setPlayerQueueTracks(nextTracks);
    const insertedGroup = playerQueueGroupFromTracks(
      requestedTracks,
      request.queueContext,
      request.optimisticTrack?.album ?? request.optimisticTrack?.name ?? "Queued list",
    );
    const baseGroups = playerQueueGroups.length > 0
      ? playerQueueGroups
      : [playerQueueGroupFromTracks(baseTracks, playerQueueContext)].filter((group): group is PlayerQueueGroup => Boolean(group));
    const nextGroups = insertedGroup
      ? (() => {
        const groups: PlayerQueueGroup[] = [];
        let seen = 0;
        let inserted = false;
        for (const group of baseGroups) {
          const groupStart = seen;
          const groupEnd = groupStart + group.tracks.length;
          if (!inserted && insertAt <= groupEnd) {
            const splitAt = Math.max(0, Math.min(group.tracks.length, insertAt - groupStart));
            const beforeTracks = group.tracks.slice(0, splitAt);
            const afterTracks = group.tracks.slice(splitAt);
            if (beforeTracks.length > 0) {
              groups.push({ ...group, id: `${group.id}:before:${insertAt}`, tracks: beforeTracks });
            }
            groups.push(insertedGroup);
            if (afterTracks.length > 0) {
              groups.push({ ...group, id: `${group.id}:after:${insertAt}`, tracks: afterTracks });
            }
            inserted = true;
          } else {
            groups.push(group);
          }
          seen = groupEnd;
        }
        if (!inserted) {
          groups.push(insertedGroup);
        }
        return groups;
      })()
      : baseGroups;
    setPlayerQueueGroups(nextGroups);
    setPlayerQueueGroupCursors((current) => ({
      ...current,
      ...(insertedGroup ? { [insertedGroup.id]: 0 } : {}),
    }));
    setHomeQueueOpenGroupIds((current) => {
      const next = new Set(current);
      if (insertedGroup) {
        next.add(insertedGroup.id);
      }
      return next;
    });
    setPlayerQueueCursor(baseCursor);
    setPlayerQueueSource("listenlab");
    setPlayerQueueCleared(false);
    setPlayerQueueError(null);
    setPlayerQueueSortMode("custom");
    setPlayerQueueGroupMode("custom");
    setPlayerQueueShuffleEnabled(false);
    setPlayerQueueShuffleBaseTracks(null);
    if (!playerQueueContext && request.queueContext) {
      setPlayerQueueContext(request.queueContext);
    }
  }

  async function handlePlaybackAction(action: PlaybackAction, request: PlaybackActionRequest) {
    clearPreviewPlaybackState();
    if (action === "play_now") {
      setOverlayTrackPlaybackExpanded(true);
      await handlePopupTrackPlayback(request.trackUri, request);
      return;
    }
    await prepareQueueControlAction();
    insertPlaybackActionTracks(request, action === "play_next" ? "next" : "end");
  }

  function buildAlbumPlaybackQueue(
    selectedTrackUri: string | null,
    entries: AlbumTrackEntry[] = albumTrackEntries,
    contextPreview: PreviewItem | null = selectedPreview,
  ) {
    if (!selectedTrackUri || entries.length <= 1) {
      return null;
    }
    const playableTracks = entries
      .filter((track) => !track.familyExclusive)
      .map((track) => {
        const uri = trackUriWithFallback(track.uri, track.id);
        if (!uri) {
          return null;
        }
        return {
          track,
          uri,
        };
      })
      .filter((item): item is { track: AlbumTrackEntry; uri: string } => Boolean(item));
    const selectedIndex = playableTracks.findIndex(({ uri }) => uri === selectedTrackUri);
    if (selectedIndex < 0) {
      return null;
    }
    const playlistUris = playableTracks.map(({ uri }) => uri);
    const queueTracks = playableTracks.map(({ track, uri }) => {
      const familySourceVersion = selectedAlbumFamilyContext?.versions.find((version) => (
        version.spotify_album_id === track.familySwitchAlbumId
        || track.familyAvailableVersions.some((available) => available.spotify_album_id === version.spotify_album_id)
      )) ?? null;
      return {
        ...playerSummaryFromAlbumTrack(track, contextPreview),
        uri,
        durationMs: Math.max(0, track.durationMs ?? 0),
        trackId: track.id ?? spotifyTrackIdFromUri(uri),
        albumId: familySourceVersion?.spotify_album_id ?? albumIdFromPreview(contextPreview),
        artistItems: uniqueArtistEntries(track.sourceTrack?.artists, contextPreview?.artists, artistEntriesFromText(track.artistName)),
      };
    });
    const albumLabel = contextPreview?.kind === "album"
      ? contextPreview.label
      : (contextPreview?.sourceTrack?.album_name ?? contextPreview?.sourceAlbumName ?? contextPreview?.detail ?? "Album");
    const albumUrl = contextPreview?.kind === "album"
      ? contextPreview.url
      : (contextPreview?.sourceTrack?.album_url ?? contextPreview?.sourceAlbumUrl ?? spotifyEntityUrl("album", albumIdFromPreview(contextPreview)));
    return {
      playlistUris,
      queueTracks,
      queueCursor: selectedIndex,
      queueContext: {
        label: albumLabel,
        url: albumUrl || null,
      },
    };
  }

  async function seekPlayer(positionMs: number) {
    await activatePlayerElement();
    const safePositionMs = Math.max(0, Math.floor(positionMs));
    const player = spotifyPlayerRef.current;
    if (player) {
      try {
        await player.seek(safePositionMs);
        setPlaybackPositionMs(safePositionMs);
        setPendingSeekMs(null);
        setOverlaySeekMs(null);
        setPlayerError(null);
        return;
      } catch {
        // Fall back to Web API seek.
      }
    }
    const deviceId = spotifyDeviceIdRef.current;
    try {
      const query = deviceId
        ? `/me/player/seek?position_ms=${encodeURIComponent(String(safePositionMs))}&device_id=${encodeURIComponent(deviceId)}`
        : `/me/player/seek?position_ms=${encodeURIComponent(String(safePositionMs))}`;
      await spotifyApiRequest(query, {
        method: "PUT",
      });
      setPlaybackPositionMs(safePositionMs);
      setPendingSeekMs(null);
      setOverlaySeekMs(null);
      setPlayerError(null);
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback position could not be updated.");
    }
  }

  async function movePlaybackQueue(direction: "previous" | "next") {
    await activatePlayerElement();
    if (playerQueueSource === "listenlab") {
      await moveListenLabQueue(direction);
      return;
    }
    const player = spotifyPlayerRef.current;
    const deviceId = spotifyDeviceIdRef.current;
    const query = deviceId
      ? `/me/player/${direction}?device_id=${encodeURIComponent(deviceId)}`
      : `/me/player/${direction}`;
    try {
      await spotifyApiRequest(query, { method: "POST" });
      setPlayerError(null);
      setLiveControlOverrideUntilMs(Date.now() + LIVE_PLAYBACK_POLL_INTERVAL_MS);
      schedulePlaybackSnapshotRefresh();
      return;
    } catch (error) {
      const sdkMove = direction === "next" ? player?.nextTrack : player?.previousTrack;
      if (sdkMove) {
        try {
          await sdkMove.call(player);
          setPlayerError(null);
          setLiveControlOverrideUntilMs(Date.now() + LIVE_PLAYBACK_POLL_INTERVAL_MS);
          schedulePlaybackSnapshotRefresh();
          return;
        } catch {
          // Report the Web API error below because it is usually more specific.
        }
      }
      setPlayerError(error instanceof Error ? error.message : `Spotify playback could not skip ${direction}.`);
    }
  }

  function startQueueSkipHold(direction: "previous" | "next") {
    queueSkipHoldHandledRef.current = false;
    if (queueSkipHoldTimerRef.current != null) {
      window.clearTimeout(queueSkipHoldTimerRef.current);
    }
    queueSkipHoldTimerRef.current = window.setTimeout(() => {
      queueSkipHoldHandledRef.current = true;
      queueSkipHoldTimerRef.current = null;
      void jumpToAdjacentQueueGroup(direction);
    }, 520);
  }

  function cancelQueueSkipHold() {
    if (queueSkipHoldTimerRef.current != null) {
      window.clearTimeout(queueSkipHoldTimerRef.current);
      queueSkipHoldTimerRef.current = null;
    }
  }

  function consumeQueueSkipHold() {
    const handled = queueSkipHoldHandledRef.current;
    queueSkipHoldHandledRef.current = false;
    return handled;
  }

  async function moveListenLabQueue(direction: "previous" | "next", options?: { markPreviousComplete?: boolean; startPaused?: boolean }) {
    if (playerQueueSource !== "listenlab") {
      return;
    }
    const currentIndex = playerQueueCursor ?? playerQueueTracks.findIndex((track) => (
      Boolean(track.uri && currentTrack?.uri && track.uri === currentTrack.uri)
    ));
    if (currentIndex < 0) {
      setPlayerError("ListenLab queue position could not be found.");
      return;
    }
    const targetIndex = direction === "next" ? currentIndex + 1 : currentIndex - 1;
    const targetTrack = playerQueueTracks[targetIndex] ?? null;
    if (!targetTrack?.uri) {
      setPlayerError(direction === "next" ? "No next song is available in this queue." : "No previous song is available in this queue.");
      return;
    }
    await playQueueTrackAtIndex(targetIndex, {
      markPreviousComplete: options?.markPreviousComplete,
      startPaused: options?.startPaused,
    });
  }

  async function playQueueTrackAtIndex(targetIndex: number, options?: { markPreviousComplete?: boolean; startPaused?: boolean }) {
    const targetTrack = playerQueueTracks[targetIndex] ?? null;
    if (!targetTrack?.uri) {
      setPlayerError("This queued song does not have a playable Spotify track.");
      return;
    }
    const currentIndex = playerQueueCursor ?? playerQueueTracks.findIndex((track) => (
      Boolean(track.uri && currentTrack?.uri && track.uri === currentTrack.uri)
    ));
    const previousProgressMs = playbackPositionMsRef.current;
    const previousDurationMs = Math.max(
      0,
      playbackDurationMs
        || currentTrack?.durationMs
        || (currentIndex >= 0 ? playerQueueTracks[currentIndex]?.durationMs : 0)
        || 0,
    );
    const previousCompletionProgressMs = options?.markPreviousComplete
      ? Math.max(previousProgressMs, previousDurationMs)
      : previousProgressMs;
    const upcomingTracks = playerQueueTracks.slice(targetIndex + 1);
    const playbackStarted = await playTrackUri(targetTrack.uri, 0, {
      queuePlaylistUris: queuePlaylistTrackUris(targetTrack.uri, upcomingTracks),
      syncQueuePlaylist: true,
    });
    if (!playbackStarted) {
      autoAdvanceTrackUriRef.current = null;
      return;
    }
    if (currentIndex !== targetIndex) {
      const previousStatus: "complete" | "in_progress" = options?.markPreviousComplete ? "complete" : "in_progress";
      void updateListenLabPlayerEventProgress(previousCompletionProgressMs, previousStatus);
      if (options?.markPreviousComplete) {
        markQueueTrackPlayed(playerQueueTracks[currentIndex] ?? currentTrack);
      }
    }
    setCurrentTrack(targetTrack);
    if (options?.startPaused) {
      await pausePlayback();
      setPlaybackPaused(true);
      setQueuePausedCursor(targetIndex);
    } else {
      setPlaybackPaused(false);
      setQueuePausedCursor(null);
    }
    setPlaybackPositionMs(0);
    setPlaybackDurationMs(Math.max(0, targetTrack.durationMs ?? 0));
    setPlayerQueueCursor(targetIndex);
    const targetGroupBounds = queueGroupBoundsForIndex(targetIndex);
    if (targetGroupBounds) {
      setPlayerQueueGroupCursors((current) => ({
        ...current,
        [targetGroupBounds.group.id]: targetIndex - targetGroupBounds.start,
      }));
    }
    setPlayerQueueSource("listenlab");
    setPlayerQueueCleared(false);
    markQueueTrackPlayed(targetTrack);
    setPlayerQueueError(null);
    setPlayerError(null);
    setLiveControlOverrideUntilMs(Date.now() + LIVE_PLAYBACK_POLL_INTERVAL_MS);
    const listenEventId = await saveListenLabPlayerEvent(targetTrack, null, 0);
    setActivePlayerListenEventId(listenEventId);
  }

  async function startNextListenLabQueueTrack() {
    await moveListenLabQueue("next", { markPreviousComplete: true });
  }

  async function pauseAtNextListenLabQueueTrack() {
    if (playerQueueSource !== "listenlab" || playerQueueCursor == null) {
      return;
    }
    const queueAtEnd = playerQueueCursor >= playerQueueTracks.length - 1;
    const targetIndex = queueAtEnd && playerQueueLoopEnabled ? 0 : playerQueueCursor + 1;
    if (!playerQueueTracks[targetIndex]?.uri) {
      setQueuePauseAfterCurrentEnabled(false);
      void pausePlayback();
      return;
    }
    setQueuePauseAfterCurrentEnabled(false);
    await playQueueTrackAtIndex(targetIndex, { markPreviousComplete: true, startPaused: true });
  }

  function schedulePlaybackSnapshotRefresh() {
    window.setTimeout(() => {
      void loadCurrentPlaybackSnapshot();
      if (playerMenuOpen && playerQueueSource !== "listenlab") {
        void loadPlayerQueueTracks();
      }
    }, 500);
  }

  function isTrackPlaying(trackUri: string | null) {
    return Boolean(
      trackUri
      && (
        (previewingTrackUriRef.current && previewingTrackUriRef.current === trackUri)
        || (currentTrack?.uri === trackUri && !playbackPaused)
      ),
    );
  }

  function resetQueueControls() {
    setPlayerQueueShuffleEnabled(false);
    setPlayerQueueShuffleBaseTracks(null);
    setPlayerQueueSettingsOpen(false);
    setPlayerQueuePauseMenuOpen(false);
    setPlayerQueueOrganizeMode(false);
    setPlayerQueueSortMode("custom");
    setPlayerQueueGroupMode("custom");
    setPlayerQueueDragIndex(null);
    setPlayerQueueLoopEnabled(false);
    setPlayerTrackLoopEnabled(false);
    setQueuePauseAfterCurrentEnabled(false);
    setQueueSleepTimerUntilMs(null);
    setQueuePausedCursor(null);
  }

  function clearQueueContext() {
    setPlayerQueueContext(null);
  }

  function queueTrackIdentity(track: (Pick<PlayerQueueTrack, "uri" | "trackId"> | PlayerTrackSummary) | null | undefined) {
    if (!track) {
      return null;
    }
    if (track.uri) {
      return track.uri;
    }
    return "trackId" in track ? track.trackId : null;
  }

  function resetQueuePlayedKeys(track?: PlayerQueueTrack | PlayerTrackSummary | null) {
    const identity = queueTrackIdentity(track);
    setPlayerQueuePlayedKeys(identity ? new Set([identity]) : new Set());
  }

  function markQueueTrackPlayed(track: PlayerQueueTrack | PlayerTrackSummary | null | undefined) {
    const identity = queueTrackIdentity(track);
    if (!identity) {
      return;
    }
    setPlayerQueuePlayedKeys((current) => {
      if (current.has(identity)) {
        return current;
      }
      const next = new Set(current);
      next.add(identity);
      return next;
    });
  }

  function queueTrackRecentTime(track: PlayerQueueTrack) {
    const identity = queueTrackIdentity(track);
    const trackKey = normalizedTrackArtistKey(track.name, track.artists);
    const match = knownPlayerTracks.find((candidate) => {
      const candidateUri = trackUriWithFallback(candidate.uri, candidate.track_id);
      if (identity && candidateUri === identity) {
        return true;
      }
      if (track.trackId && candidate.track_id === track.trackId) {
        return true;
      }
      return normalizedTrackArtistKey(candidate.track_name, candidate.artist_name) === trackKey;
    });
    const timeText = match?.last_played_at ?? match?.spotify_played_at ?? null;
    return timeText ? Date.parse(timeText) || 0 : 0;
  }

  function setQueueTracksPreservingCurrent(nextTracks: PlayerQueueTrack[]) {
    const currentIdentity = queueTrackIdentity(currentTrack);
    const nextCursor = currentIdentity
      ? nextTracks.findIndex((track) => queueTrackIdentity(track) === currentIdentity)
      : -1;
    setPlayerQueueTracks(nextTracks);
    setSinglePlayerQueueGroup(nextTracks, playerQueueContext);
    if (playerQueueSource === "listenlab") {
      setPlayerQueueCursor(nextCursor >= 0 ? nextCursor : null);
    }
    setPlayerQueueShuffleEnabled(false);
    setPlayerQueueShuffleBaseTracks(null);
  }

  function removeQueueTrackAtIndex(index: number) {
    const removedTrack = playerQueueTracks[index] ?? null;
    const removedIsCurrent = playerQueueSource === "listenlab" && index === playerQueueCursor;
    const nextTracks = playerQueueTracks.filter((_track, trackIndex) => trackIndex !== index);
    setPlayerQueueTracks(nextTracks);
    setPlayerQueueGroups((currentGroups) => {
      let seen = 0;
      const nextGroups = currentGroups.flatMap((group) => {
        const groupStart = seen;
        seen += group.tracks.length;
        if (index < groupStart || index >= groupStart + group.tracks.length) {
          return [group];
        }
        const nextGroupTracks = group.tracks.filter((_track, trackIndex) => groupStart + trackIndex !== index);
        return nextGroupTracks.length > 0 ? [{ ...group, tracks: nextGroupTracks }] : [];
      });
      setPlayerQueueGroupCursors((currentCursors) => {
        const nextCursors: Record<string, number> = {};
        for (const group of nextGroups) {
          const previousCursor = currentCursors[group.id] ?? 0;
          nextCursors[group.id] = Math.min(Math.max(0, previousCursor), group.tracks.length - 1);
        }
        return nextCursors;
      });
      return nextGroups;
    });
    if (playerQueueSource === "listenlab") {
      if (removedIsCurrent) {
        setPlayerQueueCursor(null);
      } else if (playerQueueCursor != null && index < playerQueueCursor) {
        setPlayerQueueCursor(playerQueueCursor - 1);
      }
    }
    const removedIdentity = queueTrackIdentity(removedTrack);
    if (removedIdentity) {
      setPlayerQueuePlayedKeys((current) => {
        if (!current.has(removedIdentity)) {
          return current;
        }
        const next = new Set(current);
        next.delete(removedIdentity);
        return next;
      });
    }
    setPlayerQueueSortMode("custom");
    setPlayerQueueGroupMode("custom");
    setPlayerQueueShuffleEnabled(false);
    setPlayerQueueShuffleBaseTracks(null);
  }

  function moveQueueTrack(fromIndex: number, toIndex: number) {
    if (fromIndex === toIndex || fromIndex < 0 || toIndex < 0 || fromIndex >= playerQueueTracks.length || toIndex >= playerQueueTracks.length) {
      return;
    }
    const nextTracks = [...playerQueueTracks];
    const [movedTrack] = nextTracks.splice(fromIndex, 1);
    if (!movedTrack) {
      return;
    }
    nextTracks.splice(toIndex, 0, movedTrack);
    setQueueTracksPreservingCurrent(nextTracks);
    setPlayerQueueSortMode("custom");
    setPlayerQueueGroupMode("custom");
  }

  function sortPlayerQueue(mode: "custom" | "length" | "az" | "recent") {
    setPlayerQueueSortMode(mode);
    if (mode === "custom") {
      return;
    }
    setPlayerQueueGroupMode("custom");
    const nextTracks = [...playerQueueTracks].sort((a, b) => {
      if (mode === "length") {
        return (a.durationMs ?? 0) - (b.durationMs ?? 0);
      }
      if (mode === "recent") {
        return queueTrackRecentTime(b) - queueTrackRecentTime(a);
      }
      return a.name.localeCompare(b.name, undefined, { sensitivity: "base" });
    });
    setQueueTracksPreservingCurrent(nextTracks);
  }

  function groupPlayerQueue(mode: "custom" | "artist" | "album") {
    setPlayerQueueGroupMode(mode);
    if (mode === "custom") {
      return;
    }
    setPlayerQueueSortMode("custom");
    const keyForTrack = (track: PlayerQueueTrack) => (
      mode === "artist" ? track.artists : track.album
    ).toLocaleLowerCase();
    const nextTracks = [...playerQueueTracks].sort((a, b) => {
      const groupCompare = keyForTrack(a).localeCompare(keyForTrack(b), undefined, { sensitivity: "base" });
      return groupCompare || a.name.localeCompare(b.name, undefined, { sensitivity: "base" });
    });
    setQueueTracksPreservingCurrent(nextTracks);
  }

  function shuffleTracks(tracks: PlayerQueueTrack[]) {
    const shuffledTracks = [...tracks];
    for (let index = shuffledTracks.length - 1; index > 0; index -= 1) {
      const swapIndex = Math.floor(Math.random() * (index + 1));
      [shuffledTracks[index], shuffledTracks[swapIndex]] = [shuffledTracks[swapIndex], shuffledTracks[index]];
    }
    return shuffledTracks;
  }

  function shuffleQueueTail(tracks: PlayerQueueTrack[], cursor: number | null) {
    if (playerQueueLoopEnabled && cursor != null && cursor >= 0) {
      const current = tracks[cursor];
      const shuffledTracks = shuffleTracks(tracks.filter((_track, index) => index !== cursor));
      return current ? [current, ...shuffledTracks] : shuffledTracks;
    }
    const stableEnd = cursor != null && cursor >= 0 ? cursor + 1 : 0;
    const stableTracks = tracks.slice(0, stableEnd);
    const shuffledTracks = shuffleTracks(tracks.slice(stableEnd));
    return [...stableTracks, ...shuffledTracks];
  }

  function restoreQueueOrder() {
    const baseTracks = playerQueueShuffleBaseTracks;
    if (!baseTracks) {
      setPlayerQueueShuffleEnabled(false);
      return;
    }
    setPlayerQueueTracks(baseTracks);
    setSinglePlayerQueueGroup(baseTracks, playerQueueContext);
    if (playerQueueSource === "listenlab") {
      const currentIdentity = queueTrackIdentity(currentTrack);
      const restoredCursor = currentIdentity
        ? baseTracks.findIndex((track) => queueTrackIdentity(track) === currentIdentity)
        : playerQueueCursor;
      setPlayerQueueCursor(restoredCursor != null && restoredCursor >= 0 ? restoredCursor : playerQueueCursor);
    }
    setPlayerQueueShuffleEnabled(false);
    setPlayerQueueShuffleBaseTracks(null);
  }

  function toggleQueueShuffle() {
    if (playerQueueShuffleEnabled) {
      restoreQueueOrder();
      return;
    }
    if (!queueShuffleAvailable) {
      return;
    }
    setPlayerQueueShuffleBaseTracks(playerQueueTracks);
    const nextTracks = shuffleQueueTail(playerQueueTracks, hasActiveQueueCursor ? activeQueueCursor : null);
    setPlayerQueueTracks(nextTracks);
    setSinglePlayerQueueGroup(nextTracks, playerQueueContext);
    if (playerQueueLoopEnabled && hasActiveQueueCursor && playerQueueSource === "listenlab") {
      setPlayerQueueCursor(0);
    }
    setPlayerQueueShuffleEnabled(true);
  }

  function toggleQueueLoop() {
    setPlayerQueueLoopEnabled((current) => !current);
  }

  function toggleTrackLoop() {
    setPlayerTrackLoopEnabled((current) => !current);
    setQueuePauseAfterCurrentEnabled(false);
    setQueuePausedCursor(null);
  }

  function clearPlayerQueue() {
    setPlayerQueueTracks([]);
    setPlayerQueueGroups([]);
    setPlayerQueueGroupCursors({});
    setHomeQueueOpenGroupIds(new Set());
    setPlayerQueueCursor(null);
    setPlayerQueueSource(null);
    clearQueueContext();
    setPlayerQueuePlayedKeys(new Set());
    setPlayerQueueError(null);
    resetQueueControls();
    setPlayerQueueCleared(true);
  }

  async function prepareQueueControlAction() {
    if (!liveReadOnlyMode) {
      return;
    }
    await takeOverPlaybackFromLiveSnapshot();
    if (playerQueueSource === "spotify" && hasActiveQueueCursor) {
      setPlayerQueueSource("listenlab");
      setPlayerQueueCursor(activeQueueCursor);
      setPlayerQueueCleared(false);
      markQueueTrackPlayed(playerQueueTracks[activeQueueCursor] ?? currentTrack);
    }
  }

  async function handleQueueShuffleClick() {
    await prepareQueueControlAction();
    toggleQueueShuffle();
  }

  async function handleQueueLoopClick() {
    await prepareQueueControlAction();
    toggleQueueLoop();
  }

  async function handleQueuePauseAfterCurrentClick() {
    await prepareQueueControlAction();
    setQueueSleepTimerUntilMs(null);
    setQueuePausedCursor(null);
    setPlayerTrackLoopEnabled(false);
    setQueuePauseAfterCurrentEnabled((current) => !current);
    setPlayerQueuePauseMenuOpen(false);
  }

  async function handleQueueSleepTimerClick() {
    await prepareQueueControlAction();
    setQueuePauseAfterCurrentEnabled(false);
    setQueuePausedCursor(null);
    setQueueSleepTimerUntilMs(Date.now() + 15 * 60 * 1000);
    setPlayerQueuePauseMenuOpen(false);
  }

  function cancelQueueDelay() {
    setQueuePauseAfterCurrentEnabled(false);
    setQueueSleepTimerUntilMs(null);
    setQueuePausedCursor(null);
    setPlayerQueuePauseMenuOpen(false);
  }

  async function handleTrackLoopClick() {
    await prepareQueueControlAction();
    toggleTrackLoop();
  }

  function unloopCurrentTrack() {
    setPlayerTrackLoopEnabled(false);
  }

  async function handleClearPlayerQueueClick() {
    await prepareQueueControlAction();
    clearPlayerQueue();
  }

  async function jumpToQueueGroup(groupStartIndex: number, groupId: string) {
    setHomeQueueHeaderMenuOpen(false);
    setHomeQueueOpenGroupIds((current) => {
      const next = new Set(current);
      next.add(groupId);
      return next;
    });
    await playQueueTrackAtIndex(groupStartIndex, { markPreviousComplete: true });
  }

  function persistSavedPlayerQueues(snapshots: SavedPlayerQueueSnapshot[]) {
    const next = snapshots.slice(0, 25);
    setSavedPlayerQueues(next);
    window.localStorage.setItem(SAVED_PLAYER_QUEUES_STORAGE_KEY, JSON.stringify(next));
    return next;
  }

  function persistTrackBookmarks(bookmarks: SavedTrackBookmark[]) {
    const seen = new Set<string>();
    const next = bookmarks.flatMap((bookmark) => {
      const identity = bookmarkIdentityForTrack(bookmark.track);
      if (!identity || seen.has(identity)) {
        return [];
      }
      seen.add(identity);
      return [bookmark];
    }).slice(0, 100);
    setTrackBookmarks(next);
    window.localStorage.setItem(TRACK_BOOKMARKS_STORAGE_KEY, JSON.stringify(next));
    return next;
  }

  function persistEntityBookmarks(bookmarks: SavedEntityBookmark[]) {
    const seen = new Set<string>();
    const next = bookmarks.flatMap((bookmark) => {
      const identity = entityBookmarkIdentity(bookmark);
      if (!identity || seen.has(identity)) {
        return [];
      }
      seen.add(identity);
      return [{ ...bookmark, id: identity }];
    }).slice(0, 100);
    setEntityBookmarks(next);
    window.localStorage.setItem(ENTITY_BOOKMARKS_STORAGE_KEY, JSON.stringify(next));
    return next;
  }

  function selectedPreviewEntityBookmark(): SavedEntityBookmark | null {
    if (!selectedPreview || selectedPreview.kind === "track") {
      return null;
    }
    const entityId = selectedPreview.kind === "playlist"
      ? selectedPreview.entityId ?? spotifyPlaylistIdFromUrl(selectedPreview.url)
      : selectedPreview.kind === "album"
        ? selectedPreview.albumId ?? selectedPreview.sourceAlbumId ?? selectedPreview.entityId
        : selectedPreview.entityId;
    const bookmark: SavedEntityBookmark = {
      id: "",
      bookmarkedAt: new Date().toISOString(),
      type: selectedPreview.kind,
      label: selectedPreview.label,
      url: selectedPreview.url || (
        selectedPreview.kind === "playlist"
          ? (entityId ? `https://open.spotify.com/playlist/${entityId}` : null)
          : spotifyEntityUrl(selectedPreview.kind, entityId)
      ),
      imageUrl: selectedPreview.kind === "artist"
        ? selectedPreview.image ?? selectedPreviewArtistImageUrl ?? null
        : selectedPreview.image ?? selectedPreview.sourceAlbumImage ?? null,
      entityId,
      meta: selectedPreview.kind === "playlist"
        ? selectedPreview.meta ?? "Playlist"
        : selectedPreview.kind === "album"
          ? selectedPreview.meta ?? selectedPreview.artistName ?? null
          : null,
      detail: selectedPreview.kind === "album" ? selectedPreview.detail : selectedPreview.kind === "playlist" ? selectedPreview.detail : null,
    };
    return { ...bookmark, id: entityBookmarkIdentity(bookmark) ?? `${bookmark.type}:${bookmark.label}` };
  }

  function entityIsBookmarked(bookmark: SavedEntityBookmark | null | undefined) {
    const identity = entityBookmarkIdentity(bookmark);
    return Boolean(identity && entityBookmarks.some((item) => entityBookmarkIdentity(item) === identity));
  }

  function removeEntityBookmark(bookmarkId: string) {
    persistEntityBookmarks(entityBookmarks.filter((bookmark) => bookmark.id !== bookmarkId));
    setStatusMessage("Removed bookmark.");
  }

  function toggleSelectedPreviewEntityBookmark() {
    const bookmark = selectedPreviewEntityBookmark();
    const identity = entityBookmarkIdentity(bookmark);
    if (!bookmark || !identity) {
      return;
    }
    if (entityIsBookmarked(bookmark)) {
      persistEntityBookmarks(entityBookmarks.filter((item) => entityBookmarkIdentity(item) !== identity));
      setStatusMessage(`Removed bookmark "${bookmark.label}".`);
      return;
    }
    persistEntityBookmarks([bookmark, ...entityBookmarks]);
    setStatusMessage(`Bookmarked "${bookmark.label}".`);
  }

  function openEntityBookmark(bookmark: SavedEntityBookmark) {
    if (bookmark.type === "playlist") {
      setSelectedPreview({
        image: bookmark.imageUrl ?? null,
        fallbackLabel: "P",
        label: bookmark.label,
        meta: bookmark.meta ?? "Playlist",
        detail: bookmark.detail ?? null,
        kind: "playlist",
        entityId: bookmark.entityId ?? spotifyPlaylistIdFromUrl(bookmark.url),
        trackUri: null,
        url: bookmark.url ?? "",
      });
      setPlayerDrawerExpanded(false);
      return;
    }
    if (bookmark.type === "album") {
      setSelectedPreview({
        image: bookmark.imageUrl ?? null,
        fallbackLabel: "L",
        label: bookmark.label,
        meta: bookmark.meta ?? null,
        detail: bookmark.detail ?? null,
        kind: "album",
        entityId: bookmark.entityId ?? spotifyEntityIdFromUrl("album", bookmark.url),
        trackUri: null,
        url: bookmark.url ?? spotifyEntityUrl("album", bookmark.entityId),
        trackId: null,
        albumId: bookmark.entityId ?? spotifyEntityIdFromUrl("album", bookmark.url),
        artistName: bookmark.meta ?? null,
        artists: artistEntriesFromText(bookmark.meta),
        sourceAlbumId: bookmark.entityId ?? spotifyEntityIdFromUrl("album", bookmark.url),
        sourceAlbumName: bookmark.label,
        sourceAlbumImage: bookmark.imageUrl ?? null,
        sourceAlbumUrl: bookmark.url ?? spotifyEntityUrl("album", bookmark.entityId),
        sourceAlbumYear: bookmark.detail ?? null,
      });
      setPlayerDrawerExpanded(false);
      return;
    }
    const artistId = bookmark.entityId ?? spotifyEntityIdFromUrl("artist", bookmark.url);
    setSelectedPreview({
      image: bookmark.imageUrl ?? null,
      fallbackLabel: "A",
      label: bookmark.label,
      meta: null,
      detail: bookmark.detail ?? null,
      kind: "artist",
      entityId: artistId,
      trackUri: null,
      url: bookmark.url ?? spotifyEntityUrl("artist", artistId),
      trackId: null,
      albumId: null,
      artistName: bookmark.label,
      artists: [{ artist_id: artistId, id: artistId, name: bookmark.label, url: bookmark.url ?? spotifyEntityUrl("artist", artistId), image_url: bookmark.imageUrl ?? null }],
      targetArtists: [{ artist_id: artistId, id: artistId, name: bookmark.label, url: bookmark.url ?? spotifyEntityUrl("artist", artistId), image_url: bookmark.imageUrl ?? null }],
    });
    setPlayerDrawerExpanded(false);
  }

  function bookmarkContextTypeFromUrl(url: string | null | undefined): TrackBookmarkContext["type"] | null {
    if (!url) {
      return null;
    }
    if (url.includes("/playlist/")) {
      return "playlist";
    }
    if (url.includes("/album/")) {
      return "album";
    }
    if (url.includes("/artist/")) {
      return "artist";
    }
    if (url.includes("/track/")) {
      return "track";
    }
    return null;
  }

  function bookmarkContextFromQueueGroup(group: PlayerQueueGroup | null | undefined, position: number | null = null): TrackBookmarkContext | null {
    if (!group) {
      return null;
    }
    return {
      type: bookmarkContextTypeFromUrl(group.url) ?? "queue",
      label: group.label,
      url: group.url ?? null,
      imageUrl: group.imageUrl ?? null,
      entityId: null,
      position,
    };
  }

  function activePlayerBookmarkContext(): TrackBookmarkContext {
    const activeGroupMatch = hasActiveQueueCursor
      ? homeQueueGroupForCursor(activeQueueCursor)
      : null;
    const activeGroupContext = bookmarkContextFromQueueGroup(
      activeGroupMatch?.group,
      activeGroupMatch && hasActiveQueueCursor ? activeQueueCursor - activeGroupMatch.startIndex : null,
    );
    if (activeGroupContext) {
      return activeGroupContext;
    }
    return {
      type: playerQueueContext?.playlistId ? "playlist" : "player",
      label: playerQueueContext?.label ?? "Homepage player",
      url: playerQueueContext?.url ?? null,
      imageUrl: playerDisplayTrack?.image ?? null,
      entityId: playerQueueContext?.playlistId ?? null,
      position: hasActiveQueueCursor ? activeQueueCursor : null,
    };
  }

  function selectedPreviewBookmarkContext(): TrackBookmarkContext {
    if (selectedPreview?.kind === "track") {
      return {
        type: "track",
        label: selectedPreview.label,
        url: selectedPreview.url ?? spotifyTrackUrl(selectedPreviewPlaybackTrackUri) ?? null,
        imageUrl: selectedPreview.image ?? selectedPreview.sourceAlbumImage ?? null,
        entityId: selectedPreview.trackId ?? spotifyTrackIdFromUri(selectedPreviewPlaybackTrackUri) ?? null,
        position: null,
      };
    }
    if (selectedPreview?.kind === "playlist") {
      return {
        type: "playlist",
        label: selectedPreview.label,
        url: selectedPreview.url ?? null,
        imageUrl: selectedPreview.image ?? null,
        entityId: selectedPreview.entityId ?? spotifyPlaylistIdFromUrl(selectedPreview.url),
        position: null,
      };
    }
    if (selectedPreview?.kind === "album") {
      return {
        type: "album",
        label: selectedPreview.label,
        url: selectedPreview.url ?? selectedPreview.sourceAlbumUrl ?? null,
        imageUrl: selectedPreview.image ?? selectedPreview.sourceAlbumImage ?? null,
        entityId: selectedPreview.albumId ?? selectedPreview.sourceAlbumId ?? selectedPreview.entityId,
        position: null,
      };
    }
    if (selectedPreview?.kind === "artist") {
      return {
        type: "artist",
        label: selectedPreview.label,
        url: selectedPreview.url ?? null,
        imageUrl: selectedPreview.image ?? selectedPreviewArtistImageUrl ?? null,
        entityId: selectedPreview.entityId ?? null,
        position: null,
      };
    }
    return {
      type: "player",
      label: "ListenLab",
      url: null,
      imageUrl: null,
      entityId: null,
      position: null,
    };
  }

  function spotifyEntityIdFromUrl(kind: "track" | "artist" | "album" | "playlist", url: string | null | undefined) {
    if (!url) {
      return null;
    }
    const match = url.match(new RegExp(`/(${kind})/([^/?#]+)`));
    return match?.[2] ? decodeURIComponent(match[2]) : null;
  }

  function recentTrackFromBookmark(bookmark: SavedTrackBookmark): RecentTrack {
    return {
      track_id: bookmark.track.trackId ?? spotifyTrackIdFromUri(bookmark.track.uri),
      release_track_id: bookmark.track.releaseTrackId ?? null,
      release_track_name: bookmark.track.releaseTrackName ?? null,
      release_track_source_count: bookmark.track.releaseTrackSourceCount ?? null,
      release_track_duplicate_source_count: bookmark.track.releaseTrackDuplicateSourceCount ?? null,
      has_release_track_siblings: bookmark.track.hasReleaseTrackSiblings ?? null,
      release_track_cluster_candidate_type: bookmark.track.releaseTrackClusterCandidateType ?? null,
      release_track_cluster_relationship_kind: bookmark.track.releaseTrackClusterRelationshipKind ?? null,
      track_name: bookmark.track.name,
      artist_name: bookmark.track.artists,
      album_name: bookmark.track.album,
      artists: bookmark.track.artistItems ?? artistEntriesFromText(bookmark.track.artists),
      duration_ms: bookmark.track.durationMs,
      uri: bookmark.track.uri,
      url: spotifyTrackUrl(bookmark.track.uri) ?? (bookmark.track.trackId ? spotifyEntityUrl("track", bookmark.track.trackId) : null),
      image_url: bookmark.track.image,
      album_id: bookmark.track.albumId ?? null,
      is_liked: bookmark.track.isLiked ?? null,
      liked_at: bookmark.track.likedAt ?? null,
    };
  }

  function openTrackBookmarkContext(bookmark: SavedTrackBookmark) {
    const context = bookmark.context ?? null;
    const trackId = bookmark.track.trackId ?? spotifyTrackIdFromUri(bookmark.track.uri) ?? spotifyEntityIdFromUrl("track", context?.url);
    const trackUrl = spotifyTrackUrl(bookmark.track.uri) ?? context?.url ?? spotifyEntityUrl("track", trackId);
    const sourceTrack = recentTrackFromBookmark(bookmark);
    if (context?.type === "playlist") {
      const playlistId = context.entityId ?? spotifyPlaylistIdFromUrl(context.url);
      setSelectedPreview({
        image: context.imageUrl ?? bookmark.track.image ?? null,
        fallbackLabel: "P",
        label: context.label,
        meta: "Playlist",
        detail: context.position != null ? `${context.position + 1}` : null,
        kind: "playlist",
        entityId: playlistId,
        trackUri: null,
        url: context.url ?? (playlistId ? `https://open.spotify.com/playlist/${playlistId}` : ""),
        focusPlaylistPosition: context.position ?? null,
        focusSpotifyTrackId: trackId,
      });
      setPlayerDrawerExpanded(false);
      return;
    }
    if (context?.type === "album") {
      const albumId = context.entityId ?? bookmark.track.albumId ?? spotifyEntityIdFromUrl("album", context.url);
      setSelectedPreview({
        image: context.imageUrl ?? bookmark.track.image ?? null,
        fallbackLabel: "L",
        label: context.label,
        meta: bookmark.track.artists || null,
        detail: null,
        kind: "album",
        entityId: albumId,
        trackUri: null,
        url: context.url ?? spotifyEntityUrl("album", albumId),
        trackId: null,
        albumId,
        artistName: bookmark.track.artists || null,
        artists: bookmark.track.artistItems ?? artistEntriesFromText(bookmark.track.artists),
        sourceAlbumId: albumId,
        sourceAlbumName: context.label,
        sourceAlbumImage: context.imageUrl ?? bookmark.track.image ?? null,
        sourceAlbumUrl: context.url ?? spotifyEntityUrl("album", albumId),
        sourceTrack,
      });
      setPlayerDrawerExpanded(false);
      return;
    }
    if (context?.type === "artist") {
      const artistId = context.entityId ?? spotifyEntityIdFromUrl("artist", context.url);
      setSelectedPreview({
        image: context.imageUrl ?? null,
        fallbackLabel: "A",
        label: context.label,
        meta: null,
        detail: null,
        kind: "artist",
        entityId: artistId,
        trackUri: null,
        url: context.url ?? spotifyEntityUrl("artist", artistId),
        trackId: null,
        albumId: bookmark.track.albumId ?? null,
        artistName: context.label,
        artists: [{ artist_id: artistId, id: artistId, name: context.label, url: context.url ?? spotifyEntityUrl("artist", artistId), image_url: context.imageUrl ?? null }],
        targetArtists: [{ artist_id: artistId, id: artistId, name: context.label, url: context.url ?? spotifyEntityUrl("artist", artistId), image_url: context.imageUrl ?? null }],
        sourceAlbumId: bookmark.track.albumId ?? null,
        sourceAlbumName: bookmark.track.album,
        sourceAlbumImage: bookmark.track.image ?? null,
        sourceTrack,
      });
      setPlayerDrawerExpanded(false);
      return;
    }
    if (context?.type === "queue" || context?.type === "player") {
      setPlayerDrawerExpanded(false);
      setStatusMessage(`Bookmark source is ${context.label}.`);
      return;
    }
    setSelectedPreview({
      image: bookmark.track.image ?? null,
      fallbackLabel: "T",
      label: bookmark.track.name,
      meta: bookmark.track.artists || null,
      detail: bookmark.track.album || context?.label || null,
      kind: "track",
      entityId: trackId,
      trackUri: bookmark.track.uri,
      url: trackUrl,
      trackId,
      releaseTrackId: bookmark.track.releaseTrackId ?? releaseTrackIdForSpotifyTrackId(trackId),
      releaseTrackName: bookmark.track.releaseTrackName ?? null,
      releaseTrackSourceCount: bookmark.track.releaseTrackSourceCount ?? null,
      releaseTrackDuplicateSourceCount: bookmark.track.releaseTrackDuplicateSourceCount ?? null,
      hasReleaseTrackSiblings: bookmark.track.hasReleaseTrackSiblings ?? null,
      releaseTrackClusterCandidateType: bookmark.track.releaseTrackClusterCandidateType ?? null,
      releaseTrackClusterRelationshipKind: bookmark.track.releaseTrackClusterRelationshipKind ?? null,
      albumId: bookmark.track.albumId ?? null,
      artistName: bookmark.track.artists || null,
      artists: bookmark.track.artistItems ?? artistEntriesFromText(bookmark.track.artists),
      sourceAlbumId: bookmark.track.albumId ?? null,
      sourceAlbumName: bookmark.track.album,
      sourceAlbumImage: bookmark.track.image ?? null,
      sourceTrack,
    });
    setPlayerDrawerExpanded(false);
  }

  function homeQueueGroupForCursor(cursor: number) {
    const groups = playerQueueGroups.length > 0
      ? playerQueueGroups
      : [playerQueueGroupFromTracks(playerQueueTracks, playerQueueContext, playerQueueSource === "spotify" ? "Spotify queue" : "Current queue")]
        .filter((group): group is PlayerQueueGroup => Boolean(group));
    let seen = 0;
    for (const group of groups) {
      const end = seen + group.tracks.length;
      if (cursor >= seen && cursor < end) {
        return { group, startIndex: seen };
      }
      seen = end;
    }
    return null;
  }

  function addTrackBookmark(track: PlayerTrackSummary | PlayerQueueTrack | null | undefined, context: TrackBookmarkContext | null = null) {
    const identity = bookmarkIdentityForTrack(track);
    if (!track || !identity) {
      setStatusMessage("No playable track to bookmark.");
      return;
    }
    const bookmark: SavedTrackBookmark = {
      id: identity,
      bookmarkedAt: new Date().toISOString(),
      track: playerTrackToQueueTrack(track),
      context,
    };
    persistTrackBookmarks([bookmark, ...trackBookmarks.filter((item) => bookmarkIdentityForTrack(item.track) !== identity)]);
    setStatusMessage(`Bookmarked "${track.name}".`);
  }

  function removeTrackBookmark(bookmarkId: string) {
    const removed = trackBookmarks.find((bookmark) => bookmark.id === bookmarkId);
    persistTrackBookmarks(trackBookmarks.filter((bookmark) => bookmark.id !== bookmarkId));
    const removedTrackId = removed?.track.trackId ?? spotifyTrackIdFromUri(removed?.track.uri ?? null);
    if (removedTrackId) {
      setLocalBookmarkedTrackById((current) => ({
        ...current,
        [removedTrackId]: false,
      }));
    }
    setStatusMessage("Removed bookmark.");
  }

  function removeTrackBookmarkByIdentity(identity: string) {
    const removed = trackBookmarks.find((bookmark) => bookmarkIdentityForTrack(bookmark.track) === identity);
    persistTrackBookmarks(trackBookmarks.filter((bookmark) => bookmarkIdentityForTrack(bookmark.track) !== identity));
    if (removed) {
      const removedTrackId = removed.track.trackId ?? spotifyTrackIdFromUri(removed.track.uri);
      if (removedTrackId) {
        setLocalBookmarkedTrackById((current) => ({
          ...current,
          [removedTrackId]: false,
        }));
      }
      setStatusMessage(`Removed bookmark "${removed.track.name}".`);
    }
  }

  function trackIsBookmarked(track: PlayerTrackSummary | PlayerQueueTrack | null | undefined) {
    const identity = bookmarkIdentityForTrack(track);
    return Boolean(identity && trackBookmarks.some((bookmark) => bookmarkIdentityForTrack(bookmark.track) === identity));
  }

  function toggleTrackBookmark(track: PlayerTrackSummary | PlayerQueueTrack | null | undefined, context: TrackBookmarkContext | null = null) {
    const identity = bookmarkIdentityForTrack(track);
    if (identity && trackIsBookmarked(track)) {
      removeTrackBookmarkByIdentity(identity);
      return;
    }
    addTrackBookmark(track, context);
  }

  function toggleSelectedPreviewTrackBookmark() {
    if (!selectedPreviewStarTrackId || !selectedPreviewTrackOptimisticSummary) {
      return;
    }
    const nextBookmarked = !selectedPreviewIsBookmarked;
    setLocalBookmarkedTrackById((current) => ({
      ...current,
      [selectedPreviewStarTrackId]: nextBookmarked,
    }));
    if (nextBookmarked) {
      addTrackBookmark(selectedPreviewTrackOptimisticSummary, selectedPreviewBookmarkContext());
      return;
    }
    const identity = bookmarkIdentityForTrack(selectedPreviewTrackOptimisticSummary);
    if (identity) {
      removeTrackBookmarkByIdentity(identity);
    }
  }

  async function playTrackBookmark(action: "play_now" | "play_next", bookmark: SavedTrackBookmark) {
    await handlePlaybackAction(action, {
      trackUri: bookmark.track.uri,
      optimisticTrack: bookmark.track,
      queueContext: {
        label: bookmark.context?.label ? `Bookmark: ${bookmark.context.label}` : "Bookmarks",
        url: bookmark.context?.url ?? null,
        playlistId: bookmark.context?.type === "playlist" ? bookmark.context.entityId ?? null : null,
        playlistName: bookmark.context?.type === "playlist" ? bookmark.context.label : null,
      },
      queueCursor: 0,
      queueTracks: [bookmark.track],
    });
  }

  function saveCurrentQueueSnapshot() {
    const snapshot: SavedPlayerQueueSnapshot = {
      id: crypto.randomUUID(),
      savedAt: new Date().toISOString(),
      context: playerQueueContext,
      source: playerQueueSource,
      activeCursor: hasActiveQueueCursor ? activeQueueCursor : null,
      playedKeys: Array.from(playerQueuePlayedKeys),
      groups: (playerQueueGroups.length > 0 ? playerQueueGroups : [playerQueueGroupFromTracks(playerQueueTracks, playerQueueContext)])
        .filter((group): group is PlayerQueueGroup => Boolean(group))
        .map((group) => ({
          id: group.id,
          label: group.label,
          url: group.url ?? null,
          imageUrl: group.imageUrl ?? null,
          cursor: playerQueueGroupCursors[group.id] ?? null,
          tracks: group.tracks,
        })),
      currentTrack: playerDisplayTrack,
    };
    let existing: SavedPlayerQueueSnapshot[] = [];
    try {
      const existingRaw = window.localStorage.getItem(SAVED_PLAYER_QUEUES_STORAGE_KEY);
      const parsed: unknown = existingRaw ? JSON.parse(existingRaw) : [];
      existing = Array.isArray(parsed)
        ? parsed.flatMap((item) => {
          const savedSnapshot = normalizeSavedPlayerQueueSnapshot(item);
          return savedSnapshot ? [savedSnapshot] : [];
        })
        : [];
    } catch {
      existing = [];
    }
    persistSavedPlayerQueues([snapshot, ...existing]);
    setHomeQueueHeaderMenuOpen(false);
    setStatusMessage(`Saved queue "${snapshot.groups[0]?.label ?? "Current queue"}".`);
  }

  function deleteSavedPlayerQueue(snapshotId: string) {
    persistSavedPlayerQueues(savedPlayerQueues.filter((snapshot) => snapshot.id !== snapshotId));
    setStatusMessage("Deleted saved queue.");
  }

  function restoreSavedPlayerQueue(snapshot: SavedPlayerQueueSnapshot) {
    const groups: PlayerQueueGroup[] = snapshot.groups
      .map((group) => ({
        id: group.id,
        label: group.label,
        url: group.url ?? null,
        imageUrl: group.imageUrl ?? null,
        tracks: group.tracks,
      }))
      .filter((group) => group.tracks.length > 0);
    const tracks = groups.flatMap((group) => group.tracks);
    if (tracks.length === 0) {
      setStatusMessage("That saved queue has no tracks to restore.");
      return;
    }
    const savedCursor = snapshot.activeCursor != null && snapshot.activeCursor >= 0 ? snapshot.activeCursor : 0;
    const nextCursor = Math.min(savedCursor, tracks.length - 1);
    const restoredTrack = snapshot.currentTrack ?? tracks[nextCursor] ?? null;
    setPlayerQueueTracks(tracks);
    setPlayerQueueGroups(groups);
    setPlayerQueueGroupCursors(Object.fromEntries(snapshot.groups.map((group) => [group.id, group.cursor ?? 0])));
    setHomeQueueOpenGroupIds(new Set(groups.map((group) => group.id)));
    setPlayerQueueCursor(nextCursor);
    setPlayerQueueSource("listenlab");
    setPlayerQueueContext({
      label: snapshot.context?.label || groups[0]?.label || "Restored queue",
      url: snapshot.context?.url ?? groups[0]?.url ?? null,
    });
    setPlayerQueuePlayedKeys(new Set(snapshot.playedKeys ?? []));
    setPlayerQueueCleared(false);
    setPlayerQueueError(null);
    resetQueueControls();
    setQueuePausedCursor(nextCursor);
    setCurrentTrack(restoredTrack);
    setPlaybackPaused(true);
    setPlaybackPositionMs(0);
    setHomeQueueHeaderMenuOpen(false);
    setStatusMessage(`Restored queue "${snapshot.context?.label || groups[0]?.label || "Saved queue"}".`);
  }

  function openAlbumTrackPreview(
    track: AlbumTrackEntry,
    contextPreview: PreviewItem | null = selectedPreview,
  ) {
    if (!contextPreview || (contextPreview.kind !== "track" && contextPreview.kind !== "album")) {
      return;
    }
    const previewTrackUri = trackUriWithFallback(track.uri, track.id);
    const previewTrackUrl = spotifyTrackUrl(previewTrackUri) ?? (track.id ? `https://open.spotify.com/track/${track.id}` : contextPreview.url);
    const summaryCacheKey = track.id ?? spotifyTrackIdFromUri(previewTrackUri) ?? previewTrackUri ?? track.name;
    setTrackSummaryChipCache((current) => ({
      ...current,
      [summaryCacheKey]: {
        ...current[summaryCacheKey],
        durationLabel: track.durationMs != null && track.durationMs > 0 ? formatPlaybackClock(track.durationMs) : current[summaryCacheKey]?.durationLabel ?? null,
        lastListenedLabel: formatMonthDay(track.lastPlayedAt, true) ?? current[summaryCacheKey]?.lastListenedLabel ?? null,
      },
    }));
    const contextReleaseSourceVersion = contextPreview === selectedPreview ? selectedPreviewReleasePlaybackSourceVersion : null;
    const contextAlbumId = contextReleaseSourceVersion?.album_id ?? albumIdFromPreview(contextPreview);
    const contextAlbumName = contextPreview.kind === "album"
      ? contextPreview.label
      : contextReleaseSourceVersion?.album_name ?? contextPreview.sourceAlbumName ?? contextPreview.sourceTrack?.album_name ?? contextPreview.detail ?? null;
    const contextAlbumImage = contextPreview.kind === "album"
      ? contextPreview.image ?? null
      : contextReleaseSourceVersion?.album_image_url ?? contextPreview.sourceAlbumImage ?? contextPreview.sourceTrack?.image_url ?? contextPreview.image ?? null;
    const contextAlbumUrl = contextPreview.kind === "album"
      ? contextPreview.url ?? null
      : contextPreview.sourceAlbumUrl ?? contextPreview.sourceTrack?.album_url ?? spotifyEntityUrl("album", contextAlbumId);
    const contextAlbumYear = contextPreview.kind === "album"
      ? contextPreview.detail ?? null
      : contextReleaseSourceVersion?.album_release_year ?? contextPreview.sourceAlbumYear ?? contextPreview.sourceTrack?.album_release_year ?? null;
    preserveRecordingAlbumTracklistOpenRef.current = contextPreview.kind === "track" && selectedPreviewDetailView === "recording" && recordingAlbumTracklistOpen;
    setSelectedPreview({
      image: track.sourceTrack?.image_url ?? contextAlbumImage,
      fallbackLabel: contextPreview.fallbackLabel,
      label: track.name,
      meta: track.artistName ?? track.sourceTrack?.artist_name ?? contextPreview.meta ?? null,
      detail: track.sourceTrack?.album_name ?? contextAlbumName,
      kind: "track",
      entityId: track.id ?? null,
      trackUri: previewTrackUri,
      url: previewTrackUrl,
      trackId: track.id ?? null,
      releaseTrackId: track.releaseTrackId ?? null,
      releaseTrackName: track.releaseTrackName ?? null,
      releaseTrackSourceCount: track.releaseTrackSourceCount ?? null,
      releaseTrackDuplicateSourceCount: track.releaseTrackDuplicateSourceCount ?? null,
      releaseTrackClusterCandidateType: track.releaseTrackClusterCandidateType ?? null,
      releaseTrackClusterRelationshipKind: track.releaseTrackClusterRelationshipKind ?? null,
      hasReleaseTrackSiblings: track.hasReleaseTrackSiblings,
      albumId: contextAlbumId,
      artistName: track.artistName ?? track.sourceTrack?.artist_name ?? contextPreview.artistName ?? null,
      artists: artistEntriesForAlbumTrack(track),
      targetArtists: null,
      sourceAlbumId: contextAlbumId,
      sourceAlbumName: track.sourceTrack?.album_name ?? contextAlbumName,
      sourceAlbumImage: track.sourceTrack?.image_url ?? contextAlbumImage,
      sourceAlbumUrl: track.sourceTrack?.album_url ?? contextAlbumUrl,
      sourceAlbumYear: track.sourceTrack?.album_release_year ?? contextAlbumYear,
      sourceTrack: track.sourceTrack ?? null,
      preferredDetailView: "recording",
    });
  }

  function openSelectedTrackArtistPreview(artist?: TrackArtistEntry) {
    const targetArtist = artist ?? selectedPreviewArtists[0];
    const artistName = targetArtist?.name?.trim() || selectedPreviewPrimaryArtistName;
    if (!selectedPreview || selectedPreview.kind !== "track" || !artistName) {
      return;
    }
    const sourceTrack = selectedPreview.sourceTrack ?? null;
    const artistId = targetArtist?.artist_id ?? targetArtist?.id ?? null;
    setSelectedPreview({
      image: targetArtist?.image_url ?? selectedPreviewArtistImageUrl ?? findArtistImageUrl(artistName) ?? null,
      fallbackLabel: "A",
      label: artistName,
      meta: null,
      detail: null,
      kind: "artist",
      entityId: artistId,
      trackUri: null,
      url: targetArtist?.url ?? spotifyEntityUrl("artist", artistId),
      trackId: null,
      albumId: null,
      artistName,
      artists: targetArtist ? [targetArtist] : artistEntriesFromText(artistName),
      targetArtists: targetArtist ? [targetArtist] : artistEntriesFromText(artistName),
      sourceAlbumId: sourceTrack?.album_id ?? selectedPreview.albumId ?? selectedPreview.sourceAlbumId ?? null,
      sourceAlbumName: sourceTrack?.album_name ?? selectedPreview.detail ?? selectedPreview.sourceAlbumName ?? null,
      sourceAlbumImage: sourceTrack?.image_url ?? selectedPreview.image ?? selectedPreview.sourceAlbumImage ?? null,
      sourceAlbumUrl: sourceTrack?.album_url ?? selectedPreview.sourceAlbumUrl ?? spotifyEntityUrl("album", sourceTrack?.album_id ?? selectedPreview.albumId ?? selectedPreview.sourceAlbumId ?? null),
      sourceAlbumYear: sourceTrack?.album_release_year ?? selectedPreview.sourceAlbumYear ?? null,
      sourceTrack,
    });
  }

  function openSelectedAlbumArtistPreview(artist?: TrackArtistEntry) {
    if (!selectedPreview || selectedPreview.kind !== "album") {
      return;
    }
    const targetArtist = artist ?? selectedPreviewArtists[0];
    const artistName = targetArtist?.name?.trim() || selectedPreview.artistName || selectedPreview.meta || null;
    if (!artistName) {
      return;
    }
    const artistId = targetArtist?.artist_id ?? targetArtist?.id ?? null;
    setSelectedPreview({
      image: targetArtist?.image_url ?? findArtistImageUrl(artistName) ?? null,
      fallbackLabel: "A",
      label: artistName,
      meta: null,
      detail: null,
      kind: "artist",
      entityId: artistId,
      trackUri: null,
      url: targetArtist?.url ?? spotifyEntityUrl("artist", artistId),
      trackId: null,
      albumId: null,
      artistName,
      artists: targetArtist ? [targetArtist] : artistEntriesFromText(artistName),
      targetArtists: targetArtist ? [targetArtist] : artistEntriesFromText(artistName),
      sourceAlbumId: selectedPreview.albumId ?? selectedPreview.entityId ?? null,
      sourceAlbumName: selectedPreview.label,
      sourceAlbumImage: selectedPreview.image ?? null,
      sourceAlbumUrl: selectedPreview.url ?? null,
      sourceAlbumYear: selectedPreview.detail ?? null,
      sourceTrack: selectedPreview.sourceTrack ?? null,
    });
  }

  function openAlbumWithArtistPreview(artist: TrackArtistEntry) {
    if (!selectedPreview || (selectedPreview.kind !== "album" && selectedPreview.kind !== "track")) {
      return;
    }
    const artists = uniqueArtistEntries(selectedPreviewAlbumMainArtists, [artist]);
    const artistNames = artists.map((entry) => entry.name?.trim()).filter((name): name is string => Boolean(name));
    if (artistNames.length === 0) {
      return;
    }
    const firstArtist = artists[0] ?? artist;
    const firstArtistId = firstArtist.artist_id ?? firstArtist.id ?? null;
    setSelectedPreview({
      image: firstArtist.image_url ?? findArtistImageUrl(artistNames[0]) ?? null,
      fallbackLabel: "A",
      label: artistNames.join(", "),
      meta: null,
      detail: null,
      kind: "artist",
      entityId: firstArtistId,
      trackUri: null,
      url: firstArtist.url ?? spotifyEntityUrl("artist", firstArtistId),
      trackId: null,
      albumId: null,
      artistName: artistNames.join(", "),
      artists,
      targetArtists: artists,
      sourceAlbumId: selectedPreview.albumId ?? (selectedPreview.kind === "album" ? selectedPreview.entityId : null) ?? selectedPreview.sourceTrack?.album_id ?? null,
      sourceAlbumName: selectedPreview.kind === "album" ? selectedPreview.label : selectedPreview.sourceTrack?.album_name ?? selectedPreview.detail ?? null,
      sourceAlbumImage: selectedPreview.kind === "album" ? selectedPreview.image ?? null : selectedPreview.sourceTrack?.image_url ?? selectedPreview.image ?? null,
      sourceAlbumUrl: selectedPreview.kind === "album" ? selectedPreview.url ?? null : selectedPreview.sourceTrack?.album_url ?? null,
      sourceAlbumYear: selectedPreview.kind === "album" ? selectedPreview.detail ?? null : selectedPreview.sourceTrack?.album_release_year ?? null,
      sourceTrack: selectedPreview.sourceTrack ?? null,
    });
  }

  function openSelectedArtistMemberPreview(artist: TrackArtistEntry) {
    if (!selectedPreview || selectedPreview.kind !== "artist") {
      return;
    }
    const artistName = artist.name?.trim();
    if (!artistName) {
      return;
    }
    const artistId = artist.artist_id ?? artist.id ?? null;
    setSelectedPreview({
      image: artist.image_url ?? findArtistImageUrl(artistName) ?? null,
      fallbackLabel: "A",
      label: artistName,
      meta: null,
      detail: null,
      kind: "artist",
      entityId: artistId,
      trackUri: null,
      url: artist.url ?? spotifyEntityUrl("artist", artistId),
      trackId: null,
      albumId: null,
      artistName,
      artists: [artist],
      targetArtists: [artist],
      sourceAlbumId: selectedPreview.sourceAlbumId ?? null,
      sourceAlbumName: selectedPreview.sourceAlbumName ?? null,
      sourceAlbumImage: selectedPreview.sourceAlbumImage ?? null,
      sourceAlbumUrl: selectedPreview.sourceAlbumUrl ?? null,
      sourceAlbumYear: selectedPreview.sourceAlbumYear ?? null,
      sourceTrack: selectedPreview.sourceTrack ?? null,
    });
  }

  function openArtistAlbumPreview(album: ArtistAlbumEntry) {
    const highlightArtistNames = selectedPreview?.kind === "artist" && album.relationship && album.relationship !== "album"
      ? selectedPreviewArtists.map((artist) => artist.name?.trim()).filter((name): name is string => Boolean(name))
      : null;
    setSelectedPreview({
      image: album.imageUrl,
      fallbackLabel: "L",
      label: album.name,
      meta: album.artistName,
      detail: album.releaseYear,
      kind: "album",
      entityId: album.albumId,
      trackUri: null,
      url: album.url,
      trackId: null,
      albumId: album.albumId,
      artistName: album.artistName,
      artists: artistEntryFromDisplayText(album.artistName),
      targetArtists: null,
      sourceAlbumId: album.albumId,
      sourceAlbumName: album.name,
      sourceAlbumImage: album.imageUrl,
      sourceAlbumUrl: album.url,
      sourceAlbumYear: album.releaseYear,
      albumHighlightArtistNames: highlightArtistNames,
      sourceTrack: null,
    });
  }

  function openSelectedTrackAlbumPreview() {
    if (!selectedPreview || selectedPreview.kind !== "track") {
      return;
    }
    const sourceTrack = selectedPreview.sourceTrack ?? null;
    const albumId = selectedAlbumFamilyContext
      ? selectedPreview.albumId ?? selectedPreview.sourceAlbumId ?? null
      : selectedPreviewReleasePlaybackSourceVersion?.album_id ?? sourceTrack?.album_id ?? selectedPreview.albumId ?? selectedPreview.sourceAlbumId ?? null;
    const albumName = selectedAlbumFamilyContext
      ? selectedPreview.sourceAlbumName ?? selectedAlbumFamilyContext.core_name
      : selectedPreviewReleasePlaybackSourceVersion?.album_name ?? sourceTrack?.album_name ?? selectedPreview.sourceAlbumName ?? selectedPreview.detail ?? "Unknown album";
    const albumYear = selectedAlbumFamilyContext
      ? selectedPreview.sourceAlbumYear ?? null
      : selectedPreviewReleasePlaybackSourceVersion?.album_release_year ?? sourceTrack?.album_release_year ?? selectedPreview.sourceAlbumYear ?? null;
    const albumImage = selectedAlbumFamilyContext
      ? selectedPreview.sourceAlbumImage ?? selectedPreview.image ?? null
      : selectedPreviewReleasePlaybackSourceVersion?.album_image_url ?? selectedPreview.sourceAlbumImage ?? selectedPreview.image ?? sourceTrack?.image_url ?? null;
    const albumUrl = selectedAlbumFamilyContext
      ? selectedPreview.sourceAlbumUrl || spotifyEntityUrl("album", albumId)
      : spotifyEntityUrl("album", selectedPreviewReleasePlaybackSourceVersion?.album_id) || sourceTrack?.album_url || selectedPreview.sourceAlbumUrl || spotifyEntityUrl("album", albumId);
    const albumArtistEntries = uniqueArtistEntries(
      sourceTrack?.artists,
      selectedPreview.artists,
      artistEntriesFromText(nonYearArtistText(sourceTrack?.artist_name ?? selectedPreview.artistName ?? selectedPreview.meta)),
    );
    const albumArtistEntryText = albumArtistEntries.map((artist) => artist.name?.trim()).filter(Boolean).join(", ");
    const albumArtistName = nonYearArtistText(sourceTrack?.artist_name ?? selectedPreview.artistName ?? selectedPreview.meta)
      ?? (albumArtistEntryText || null)
      ?? null;
    setSelectedPreview({
      image: albumImage,
      fallbackLabel: "L",
      label: albumName,
      meta: albumArtistName,
      detail: albumYear,
      kind: "album",
      entityId: albumId,
      trackUri: null,
      url: albumUrl,
      trackId: null,
      albumId,
      artistName: albumArtistName,
      artists: albumArtistEntries,
      targetArtists: null,
      sourceAlbumId: albumId,
      sourceAlbumName: albumName,
      sourceAlbumImage: albumImage,
      sourceAlbumUrl: albumUrl,
      sourceAlbumYear: albumYear,
      sourceTrack,
    });
  }

  function switchSelectedTrackAlbumVersion(spotifyAlbumId: string) {
    if (!selectedPreview || !selectedAlbumFamilyContext || (selectedPreview.kind !== "track" && selectedPreview.kind !== "album")) {
      return;
    }
    const version = selectedAlbumFamilyContext.versions.find((candidate) => candidate.spotify_album_id === spotifyAlbumId);
    if (!version) {
      return;
    }
    const versionDiscNumbers = selectedAlbumFamilyContext.version_disc_numbers ?? {};
    const currentDiscNumber = versionDiscNumbers[selectedAlbumFamilyContext.selected_spotify_album_id] ?? null;
    const targetDiscNumber = versionDiscNumbers[spotifyAlbumId] == null ? currentDiscNumber : null;
    setAlbumFamilyDiscScrollTarget(targetDiscNumber);
    setAlbumTrackLastSortMode(null);
    setSelectedPreview((current) => {
      if (!current || (current.kind !== "track" && current.kind !== "album")) {
        return current;
      }
      if (current.kind === "album") {
        return {
          ...current,
          image: version.image_url ?? current.image,
          label: version.name,
          detail: version.release_year == null ? current.detail : String(version.release_year),
          entityId: version.spotify_album_id,
          albumId: version.spotify_album_id,
          sourceAlbumId: version.spotify_album_id,
          sourceAlbumName: version.name,
          sourceAlbumImage: version.image_url,
          sourceAlbumUrl: spotifyEntityUrl("album", version.spotify_album_id),
          sourceAlbumYear: version.release_year == null ? null : String(version.release_year),
        };
      }
      return {
        ...current,
        image: version.image_url ?? current.image,
        albumId: version.spotify_album_id,
        sourceAlbumId: version.spotify_album_id,
        sourceAlbumName: version.name,
        sourceAlbumImage: version.image_url,
        sourceAlbumUrl: spotifyEntityUrl("album", version.spotify_album_id),
        sourceAlbumYear: version.release_year == null ? null : String(version.release_year),
      };
    });
  }

  function playerSummaryFromAlbumTrack(track: AlbumTrackEntry, contextPreview: PreviewItem | null = selectedPreview): PlayerTrackSummary {
    const previewTrackUri = trackUriWithFallback(track.uri, track.id);
    const familySourceVersion = selectedAlbumFamilyContext?.versions.find((version) => (
      version.spotify_album_id === track.familySwitchAlbumId
      || track.familyAvailableVersions.some((available) => available.spotify_album_id === version.spotify_album_id)
    )) ?? null;
    return {
      name: track.name,
      artists: track.artistName ?? track.sourceTrack?.artist_name ?? contextPreview?.artistName ?? "Unknown artist",
      album: familySourceVersion?.name ?? track.sourceTrack?.album_name ?? contextPreview?.sourceTrack?.album_name ?? contextPreview?.sourceAlbumName ?? contextPreview?.detail ?? "Unknown album",
      image: familySourceVersion?.image_url ?? track.sourceTrack?.image_url ?? contextPreview?.sourceAlbumImage ?? contextPreview?.image ?? null,
      uri: previewTrackUri,
      durationMs: Math.max(0, track.durationMs ?? track.sourceTrack?.duration_ms ?? 0),
    };
  }

  function includeAlbumFamilyTracks(spotifyAlbumId: string) {
    setAlbumTrackEntries((current) => current.map((track) => (
      track.familyExclusive
      && (
        track.familySwitchAlbumId === spotifyAlbumId
        || track.familyAvailableVersions.some((version) => version.spotify_album_id === spotifyAlbumId)
      )
        ? { ...track, familyExclusive: false }
        : track
    )));
  }

  function selectedAlbumTrackMarkerTop(entries: AlbumTrackEntry[], minScrollableTrackCount = 5) {
    const selectedIndex = entries.findIndex((track) => track.isSelected);
    if (selectedIndex < 0 || entries.length <= minScrollableTrackCount) {
      return null;
    }
    return `${(selectedIndex / (entries.length - 1)) * 100}%`;
  }

  function albumTracklistSummaryLabel(entries: AlbumTrackEntry[]) {
    if (entries.length === 0) {
      return "Tracks";
    }
    const trackLabel = `${entries.length} ${entries.length === 1 ? "Track" : "Tracks"}`;
    const durationMs = entries.reduce((total, track) => total + Math.max(0, track.durationMs ?? 0), 0);
    return durationMs > 0 ? `${trackLabel} (${formatDurationMs(durationMs)})` : trackLabel;
  }

  function nextLastPlayedSortMode(current: LastPlayedSortMode): LastPlayedSortMode {
    if (current === null) {
      return "recent";
    }
    if (current === "recent") {
      return "oldest";
    }
    return null;
  }

  function sortedAlbumTrackEntries(
    entries: AlbumTrackEntry[],
    mode: LastPlayedSortMode,
    historyMode: "recording" | "release" | "default" = "default",
  ) {
    if (mode === null) {
      return entries;
    }
    return entries
      .map((track, index) => ({ track, index }))
      .sort((left, right) => {
        const editionGroupDelta = Number(left.track.familyExclusive) - Number(right.track.familyExclusive);
        if (editionGroupDelta !== 0) {
          return editionGroupDelta;
        }
        const historyTimestamp = (track: AlbumTrackEntry) => historyMode === "recording"
          ? track.recordingLastPlayedAt === undefined ? track.lastPlayedAt : track.recordingLastPlayedAt
          : historyMode === "release"
            ? track.sourceLastPlayedAt === undefined ? track.lastPlayedAt : track.sourceLastPlayedAt
            : track.lastPlayedAt;
        const leftMs = parseTimestampMs(historyTimestamp(left.track));
        const rightMs = parseTimestampMs(historyTimestamp(right.track));
        const leftSortValue = leftMs ?? -Infinity;
        const rightSortValue = rightMs ?? -Infinity;
        const delta = mode === "recent"
          ? rightSortValue - leftSortValue
          : leftSortValue - rightSortValue;
        return delta || left.index - right.index;
      })
      .map((entry) => entry.track);
  }

  function scrollSelectedAlbumTrackToMiddle(listElement: HTMLUListElement | null, entries: AlbumTrackEntry[]) {
    if (!listElement) {
      return false;
    }
    const selectedIndex = entries.findIndex((track) => track.isSelected);
    if (selectedIndex < 0) {
      return false;
    }
    const row = listElement.querySelector(".detail-album-track-row-selected");
    if (!(row instanceof HTMLElement)) {
      return false;
    }
    const targetTop = row.offsetTop - ((listElement.clientHeight - row.offsetHeight) / 2);
    const maxScrollTop = Math.max(0, listElement.scrollHeight - listElement.clientHeight);
    listElement.scrollTop = Math.max(0, Math.min(maxScrollTop, targetTop));
    return true;
  }

  function albumTrackPreviewKey(track: AlbumTrackEntry, rowTrackUri: string | null) {
    return track.id ?? rowTrackUri ?? normalizedTrackArtistKey(track.name, track.artistName);
  }

  function clearPreviewVolumeRamp() {
    if (previewVolumeRampTimerRef.current != null) {
      window.clearInterval(previewVolumeRampTimerRef.current);
      previewVolumeRampTimerRef.current = null;
    }
  }

  async function setPlayerVolumeSafe(nextVolume: number) {
    const player = spotifyPlayerRef.current;
    if (!player?.setVolume) {
      return;
    }
    const safeVolume = Math.max(0, Math.min(1, nextVolume));
    try {
      await player.setVolume(safeVolume);
      currentPlayerVolumeRef.current = safeVolume;
    } catch {
      // Ignore volume updates when SDK volume is unavailable.
    }
  }

  function restoreDefaultPlayerVolume() {
    clearPreviewVolumeRamp();
    void setPlayerVolumeSafe(DEFAULT_PLAYER_VOLUME);
  }

  function startPreviewVolumeRamp() {
    clearPreviewVolumeRamp();
    const steps = Math.max(1, Math.floor(PREVIEW_RAMP_DURATION_MS / PREVIEW_RAMP_STEP_MS));
    let step = 0;
    previewVolumeRampTimerRef.current = window.setInterval(() => {
      step += 1;
      const progress = Math.min(1, step / steps);
      const nextVolume = PREVIEW_RAMP_START_VOLUME + ((DEFAULT_PLAYER_VOLUME - PREVIEW_RAMP_START_VOLUME) * progress);
      void setPlayerVolumeSafe(nextVolume);
      if (progress >= 1) {
        clearPreviewVolumeRamp();
      }
    }, PREVIEW_RAMP_STEP_MS);
  }

  function setPreviewingTrackUriState(nextUri: string | null) {
    previewingTrackUriRef.current = nextUri;
    setPreviewingTrackUri(nextUri);
  }

  function setPreviewPlaybackSession(nextSession: typeof previewPlaybackSessionRef.current) {
    previewPlaybackSessionRef.current = nextSession;
    setPreviewPlaybackSessionState(nextSession);
  }

  function clearPreviewPlaybackState() {
    if (previewStopTimerRef.current != null) {
      window.clearTimeout(previewStopTimerRef.current);
      previewStopTimerRef.current = null;
    }
    setPreviewingTrackUriState(null);
    setPreviewPlaybackSession(null);
    restoreDefaultPlayerVolume();
  }

  async function resumeFromPreviewSession(session: typeof previewPlaybackSessionRef.current) {
    if (!session?.baseTrack?.uri) {
      setPlaybackPaused(true);
      return;
    }
    const resumed = await playTrackUri(session.baseTrack.uri, Math.max(0, session.basePositionMs), {
      syncQueuePlaylist: playerQueueSource === "listenlab",
    });
    if (!resumed) {
      return;
    }
    setCurrentTrack(session.baseTrack);
    setPlaybackPaused(false);
    setPlaybackPositionMs(Math.max(0, session.basePositionMs));
    setPlaybackDurationMs(Math.max(0, session.baseDurationMs || session.baseTrack.durationMs || 0));
    setQueuePausedCursor(null);
  }

  async function stopTrackPreviewPlayback(resumeBase = true) {
    const hasActivePreview = Boolean(previewingTrackUriRef.current);
    const session = previewPlaybackSessionRef.current;
    clearPreviewPlaybackState();
    if (!hasActivePreview) {
      return;
    }
    if (resumeBase) {
      await resumeFromPreviewSession(session);
      return;
    }
    const player = spotifyPlayerRef.current;
    let paused = false;
    if (player) {
      try {
        await player.pause();
        paused = true;
      } catch {
        paused = false;
      }
    }
    if (!paused) {
      try {
        await spotifyApiRequest("/me/player/pause", {
          method: "PUT",
        });
        paused = true;
      } catch {
        // Ignore pause errors for preview stop fallback.
      }
    }
    if (paused) {
      setPlaybackPaused(true);
    }
  }

  async function toggleAlbumTrackPreview(track: AlbumTrackEntry, rowTrackUri: string | null) {
    if (!rowTrackUri) {
      return;
    }
    const previewIsActiveForRow = Boolean(previewingTrackUriRef.current && previewingTrackUriRef.current === rowTrackUri);
    if (previewIsActiveForRow) {
      await stopTrackPreviewPlayback();
      return;
    }
    const existingPreviewSession = previewPlaybackSessionRef.current;
    const baseTrack = existingPreviewSession?.baseTrack ?? playerDisplayTrack;
    const basePositionMs = existingPreviewSession?.basePositionMs ?? playerDisplayPositionMs;
    const baseDurationMs = existingPreviewSession?.baseDurationMs ?? (playerDisplayDurationMs || playerDisplayTrack?.durationMs || 0);
    const basePaused = existingPreviewSession?.basePaused ?? playerDisplayPaused;
    clearPreviewPlaybackState();
    const durationMs = Math.max(0, track.durationMs ?? track.sourceTrack?.duration_ms ?? 0);
    if (durationMs < 60_000) {
      setPlayerError("Preview is only available for tracks longer than 60 seconds.");
      return;
    }
    const minStartMs = 20_000;
    const maxStartMs = durationMs - 40_000;
    if (maxStartMs < minStartMs) {
      setPlayerError("Preview window could not be generated for this track.");
      return;
    }
    const randomStartMs = Math.floor(Math.random() * (maxStartMs - minStartMs + 1)) + minStartMs;
    await setPlayerVolumeSafe(PREVIEW_RAMP_START_VOLUME);
    const previewTrack = {
      ...playerSummaryFromAlbumTrack(track),
      uri: rowTrackUri,
      durationMs,
    };
    const playbackStarted = await playTrackUri(rowTrackUri, randomStartMs);
    if (!playbackStarted) {
      restoreDefaultPlayerVolume();
      return;
    }
    const previewKey = albumTrackPreviewKey(track, rowTrackUri);
    setPreviewPlayedTrackKeys((current) => {
      const next = new Set(current);
      next.add(previewKey);
      return next;
    });
    setPreviewPlaybackSession({
      baseTrack,
      basePositionMs: Math.max(0, basePositionMs),
      baseDurationMs: Math.max(0, baseDurationMs),
      basePaused,
      previewTrack,
    });
    setPreviewingTrackUriState(rowTrackUri);
    startPreviewVolumeRamp();
    previewStopTimerRef.current = window.setTimeout(() => {
      void stopTrackPreviewPlayback(true);
    }, 20_000);
  }

  async function handleHomeAlbumTrackPlay(track: AlbumTrackEntry, trackUri: string | null) {
    if (!trackUri) {
      return;
    }
    const contextPreview = playerAlbumPreviewContext();
    clearPreviewPlaybackState();
    const albumQueue = buildAlbumPlaybackQueue(trackUri, homeAlbumTrackEntries, contextPreview);
    const playbackStarted = await handlePopupTrackPlayback(trackUri, {
      optimisticTrack: playerSummaryFromAlbumTrack(track, contextPreview),
      queueCursor: albumQueue?.queueCursor,
      queueContext: albumQueue?.queueContext,
      queuePlaylistUris: albumQueue?.playlistUris,
      queueTracks: albumQueue?.queueTracks,
      sourceTrack: track.sourceTrack,
    });
    if (playbackStarted) {
      setHomeAlbumExpanded(true);
    }
  }

  async function handleHomeAlbumPlayAll(action: PlaybackAction = "play_now") {
    const contextPreview = playerAlbumPreviewContext();
    const firstPlayableTrack = homeAlbumTrackEntries
      .map((track) => ({ track, uri: trackUriWithFallback(track.uri, track.id) }))
      .find((item) => Boolean(item.uri));
    if (!firstPlayableTrack) {
      setPlayerError("This album does not have a playable first song.");
      return;
    }
    const albumQueue = buildAlbumPlaybackQueue(firstPlayableTrack.uri, homeAlbumTrackEntries, contextPreview);
    await handlePlaybackAction(action, {
      trackUri: firstPlayableTrack.uri,
      optimisticTrack: playerSummaryFromAlbumTrack(firstPlayableTrack.track, contextPreview),
      insertTracks: albumQueue?.queueTracks,
      queueCursor: albumQueue?.queueCursor,
      queueContext: albumQueue?.queueContext,
      queuePlaylistUris: albumQueue?.playlistUris,
      queueTracks: albumQueue?.queueTracks,
      sourceTrack: firstPlayableTrack.track.sourceTrack,
    });
    if (action === "play_now") {
      setHomeAlbumExpanded(true);
    }
  }

  function playerSummaryFromPlaylistTrack(track: RecentTrack): PlayerTrackSummary {
    const trackUri = trackUriWithFallback(track.uri, track.track_id);
    return {
      name: track.track_name ?? "Unknown track",
      artists: track.artist_name ?? "Unknown artist",
      album: track.album_name ?? selectedPreview?.label ?? "Unknown album",
      image: track.image_url ?? selectedPreview?.image ?? null,
      uri: trackUri,
      durationMs: Math.max(0, track.duration_ms ?? 0),
    };
  }

  async function togglePlaylistTrackPreview(track: RecentTrack, rowTrackUri: string | null) {
    if (!rowTrackUri) {
      return;
    }
    const previewIsActiveForRow = Boolean(previewingTrackUriRef.current && previewingTrackUriRef.current === rowTrackUri);
    if (previewIsActiveForRow) {
      await stopTrackPreviewPlayback();
      return;
    }
    const existingPreviewSession = previewPlaybackSessionRef.current;
    const baseTrack = existingPreviewSession?.baseTrack ?? playerDisplayTrack;
    const basePositionMs = existingPreviewSession?.basePositionMs ?? playerDisplayPositionMs;
    const baseDurationMs = existingPreviewSession?.baseDurationMs ?? (playerDisplayDurationMs || playerDisplayTrack?.durationMs || 0);
    const basePaused = existingPreviewSession?.basePaused ?? playerDisplayPaused;
    clearPreviewPlaybackState();
    const durationMs = Math.max(0, track.duration_ms ?? 0);
    if (durationMs < 60_000) {
      setPlayerError("Preview is only available for tracks longer than 60 seconds.");
      return;
    }
    const minStartMs = 20_000;
    const maxStartMs = durationMs - 40_000;
    if (maxStartMs < minStartMs) {
      setPlayerError("Preview window could not be generated for this track.");
      return;
    }
    const randomStartMs = Math.floor(Math.random() * (maxStartMs - minStartMs + 1)) + minStartMs;
    await setPlayerVolumeSafe(PREVIEW_RAMP_START_VOLUME);
    const previewTrack = {
      ...playerSummaryFromPlaylistTrack(track),
      uri: rowTrackUri,
      durationMs,
    };
    const playbackStarted = await playTrackUri(rowTrackUri, randomStartMs);
    if (!playbackStarted) {
      restoreDefaultPlayerVolume();
      return;
    }
    setPreviewPlayedTrackKeys((current) => {
      const next = new Set(current);
      next.add(`playlist:${track.track_id ?? rowTrackUri ?? track.track_name ?? "track"}`);
      return next;
    });
    setPreviewPlaybackSession({
      baseTrack,
      basePositionMs: Math.max(0, basePositionMs),
      baseDurationMs: Math.max(0, baseDurationMs),
      basePaused,
      previewTrack,
    });
    setPreviewingTrackUriState(rowTrackUri);
    startPreviewVolumeRamp();
    previewStopTimerRef.current = window.setTimeout(() => {
      void stopTrackPreviewPlayback(true);
    }, 20_000);
  }

  function buildPlaylistPlaybackQueue(selectedTrackUri: string | null) {
    const playableTracks = playlistTrackEntries
      .map((track) => {
        const uri = trackUriWithFallback(track.uri, track.track_id);
        return uri
          ? {
            track,
            uri,
          }
          : null;
      })
      .filter((item): item is { track: RecentTrack; uri: string } => Boolean(item));
    if (!selectedTrackUri || playableTracks.length === 0) {
      return null;
    }
    const queueCursor = Math.max(0, playableTracks.findIndex((item) => item.uri === selectedTrackUri));
    return {
      playlistUris: playableTracks.map((item) => item.uri),
      queueTracks: playableTracks.map(({ track, uri }) => ({
        ...playerSummaryFromPlaylistTrack(track),
        uri,
        durationMs: Math.max(0, track.duration_ms ?? 0),
        trackId: track.track_id ?? spotifyTrackIdFromUri(uri),
        albumId: track.album_id ?? null,
        isLiked: recentTrackIsKnownLiked(track, track.track_id ?? spotifyTrackIdFromUri(uri)),
        likedAt: track.liked_at ?? null,
        artistItems: track.artists ?? null,
        releaseTrackId: track.release_track_id ?? null,
        releaseTrackName: track.release_track_name ?? null,
        releaseTrackSourceCount: track.release_track_source_count ?? null,
        releaseTrackDuplicateSourceCount: track.release_track_duplicate_source_count ?? null,
        releaseTrackClusterCandidateType: track.release_track_cluster_candidate_type ?? null,
        releaseTrackClusterRelationshipKind: track.release_track_cluster_relationship_kind ?? null,
        hasReleaseTrackSiblings: track.has_release_track_siblings ?? null,
      })),
      queueCursor,
      queueContext: {
        label: selectedPreview?.label ?? "Playlist",
        url: selectedPreview?.url ?? null,
        playlistId: selectedPreview?.kind === "playlist" ? selectedPreview.entityId ?? spotifyPlaylistIdFromUrl(selectedPreview.url) : null,
        playlistName: selectedPreview?.kind === "playlist" ? selectedPreview.label : null,
      },
    };
  }

  function openPlaylistMembershipPreview(membership: PlaylistMembership) {
    setSelectedPreview({
      image: membership.playlist_image_url ?? null,
      fallbackLabel: "P",
      label: membership.playlist_name ?? "Untitled playlist",
      meta: membership.owner_name ? `By ${membership.owner_name}` : "Playlist",
      detail: `${membership.position + 1}`,
      kind: "playlist",
      entityId: membership.playlist_id,
      trackUri: null,
      url: membership.playlist_url ?? "",
      focusPlaylistPosition: membership.position,
      focusSpotifyTrackId: membership.spotify_track_id,
    });
  }

  async function hidePlaylistFromListenLab(playlist: OwnedPlaylist) {
    const playlistId = playlist.playlist_id ?? spotifyPlaylistIdFromUrl(playlist.url);
    if (!playlistId) {
      setPlayerError("This playlist is missing a Spotify id.");
      return;
    }
    setProfile((current) => current
      ? {
        ...current,
        owned_playlists: current.owned_playlists.map((item) => (
          (item.playlist_id ?? spotifyPlaylistIdFromUrl(item.url)) === playlistId
            ? { ...item, hidden_by_user: true }
            : item
        )),
      }
      : current);
    try {
      const response = await fetch(`${apiBaseUrl}/playlists/${encodeURIComponent(playlistId)}/hidden`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ hidden: true }),
      });
      if (!response.ok) {
        throw new Error(`Hide playlist failed (${response.status}).`);
      }
      setSelectedPreviewPlaylistMemberships((current) => current.filter((membership) => membership.playlist_id !== playlistId));
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Playlist could not be hidden.");
      setProfile((current) => current
        ? {
          ...current,
          owned_playlists: current.owned_playlists.map((item) => (
            (item.playlist_id ?? spotifyPlaylistIdFromUrl(item.url)) === playlistId
              ? { ...item, hidden_by_user: false }
              : item
          )),
        }
        : current);
    }
  }

  async function unhidePlaylistInListenLab(playlist: OwnedPlaylist) {
    const playlistId = playlist.playlist_id ?? spotifyPlaylistIdFromUrl(playlist.url);
    if (!playlistId) {
      setPlayerError("This playlist is missing a Spotify id.");
      return;
    }
    setProfile((current) => current
      ? {
        ...current,
        owned_playlists: current.owned_playlists.map((item) => (
          (item.playlist_id ?? spotifyPlaylistIdFromUrl(item.url)) === playlistId
            ? { ...item, hidden_by_user: false }
            : item
        )),
      }
      : current);
    try {
      const response = await fetch(`${apiBaseUrl}/playlists/${encodeURIComponent(playlistId)}/hidden`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ hidden: false }),
      });
      if (!response.ok) {
        throw new Error(`Unhide playlist failed (${response.status}).`);
      }
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Playlist could not be unhidden.");
      setProfile((current) => current
        ? {
          ...current,
          owned_playlists: current.owned_playlists.map((item) => (
            (item.playlist_id ?? spotifyPlaylistIdFromUrl(item.url)) === playlistId
              ? { ...item, hidden_by_user: true }
              : item
          )),
        }
        : current);
    }
  }

  async function deletePlaylistFromSpotify(playlist: OwnedPlaylist) {
    const playlistId = playlist.playlist_id ?? spotifyPlaylistIdFromUrl(playlist.url);
    if (!playlistId) {
      setPlayerError("This playlist is missing a Spotify id.");
      return;
    }
    const playlistName = playlist.name ?? "this playlist";
    const confirmed = window.confirm(`Delete ${playlistName} from Spotify? This cannot be undone here.`);
    if (!confirmed) {
      return;
    }
    const previousProfile = profile;
    setProfile((current) => current
      ? {
        ...current,
        owned_playlists: current.owned_playlists.filter((item) => (item.playlist_id ?? spotifyPlaylistIdFromUrl(item.url)) !== playlistId),
      }
      : current);
    try {
      const response = await fetch(`${apiBaseUrl}/auth/playback/playlists/${encodeURIComponent(playlistId)}`, {
        method: "DELETE",
        credentials: "include",
      });
      if (!response.ok) {
        throw new Error(`Delete playlist failed (${response.status}).`);
      }
      setSelectedPreviewPlaylistMemberships((current) => current.filter((membership) => membership.playlist_id !== playlistId));
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Playlist could not be deleted.");
      setProfile(previousProfile);
    }
  }

  async function addSelectedTrackToPlaylists(playlistIds: string[], removePlaylistIds: string[], newPlaylistName: string | null) {
    if (selectedPreview?.kind !== "track") {
      throw new Error("Open a track before adding it to a playlist.");
    }
    const trackUri = selectedPreviewPlaybackTrackUri ?? trackUriWithFallback(selectedPreview.trackUri, selectedPreview.trackId);
    if (!trackUri) {
      throw new Error("This track does not have a playable Spotify URI.");
    }
    const selectedPlaylistIds = Array.from(new Set(playlistIds.map((playlistId) => playlistId.trim()).filter(Boolean)));
    const createdPlaylists: OwnedPlaylist[] = [];
    async function readError(response: Response, fallback: string) {
      try {
        const payload = await response.json() as { detail?: string };
        return payload.detail || fallback;
      } catch {
        return fallback;
      }
    }
    if (newPlaylistName?.trim()) {
      const response = await fetch(`${apiBaseUrl}/auth/playback/playlists`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: newPlaylistName.trim(),
          track_uri: trackUri,
        }),
      });
      if (!response.ok) {
        throw new Error(await readError(response, `Playlist creation failed (${response.status}).`));
      }
      const payload = await response.json() as { playlist?: OwnedPlaylist };
      if (payload.playlist?.playlist_id) {
        createdPlaylists.push(payload.playlist);
      }
    }
    let addedPlaylistIds = selectedPlaylistIds;
    const selectedRemovePlaylistIds = Array.from(new Set(removePlaylistIds.map((playlistId) => playlistId.trim()).filter(Boolean)));
    if (selectedPlaylistIds.length > 0) {
      const response = await fetch(`${apiBaseUrl}/auth/playback/playlist-tracks`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          playlist_ids: selectedPlaylistIds,
          track_uri: trackUri,
        }),
      });
      if (!response.ok) {
        throw new Error(await readError(response, `Playlist update failed (${response.status}).`));
      }
      const payload = await response.json() as { added_playlist_ids?: string[]; errors?: { playlist_id: string; error: string }[] };
      addedPlaylistIds = payload.added_playlist_ids ?? selectedPlaylistIds;
      if (payload.errors?.length) {
        setPlayerError(payload.errors.map((error) => error.error).join(" "));
      }
    }
    let removedPlaylistIds = selectedRemovePlaylistIds;
    if (selectedRemovePlaylistIds.length > 0) {
      const response = await fetch(`${apiBaseUrl}/auth/playback/playlist-tracks`, {
        method: "DELETE",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          playlist_ids: selectedRemovePlaylistIds,
          track_uri: trackUri,
        }),
      });
      if (!response.ok) {
        throw new Error(await readError(response, `Playlist update failed (${response.status}).`));
      }
      const payload = await response.json() as { removed_playlist_ids?: string[]; errors?: { playlist_id: string; error: string }[] };
      removedPlaylistIds = payload.removed_playlist_ids ?? selectedRemovePlaylistIds;
      if (payload.errors?.length) {
        setPlayerError(payload.errors.map((error) => error.error).join(" "));
      }
    }
    const playlistById = new Map((profile?.owned_playlists ?? []).map((playlist) => [playlist.playlist_id, playlist]));
    for (const playlist of createdPlaylists) {
      playlistById.set(playlist.playlist_id, playlist);
    }
    const trackId = spotifyTrackIdFromUri(trackUri);
    const previewArtistName = selectedPreview.artists
      ?.map((artist) => typeof artist === "string" ? artist : artist.name)
      .find((name) => Boolean(name?.trim()))
      ?? selectedPreview.sourceTrack?.artist_name
      ?? selectedPreview.meta
      ?? null;
    const newMemberships: PlaylistMembership[] = [
      ...addedPlaylistIds,
      ...createdPlaylists.map((playlist) => playlist.playlist_id).filter((playlistId): playlistId is string => Boolean(playlistId)),
    ]
      .filter((playlistId, index, all) => all.indexOf(playlistId) === index)
      .map((playlistId) => {
        const playlist = playlistById.get(playlistId);
        return {
          playlist_id: playlistId,
          playlist_name: playlist?.name ?? "Untitled playlist",
          playlist_url: playlist?.url ?? null,
          playlist_image_url: playlist?.image_url ?? null,
          owner_name: playlist?.owner_name ?? null,
          owner_id: playlist?.owner_id ?? null,
          is_collaborative: playlist?.is_collaborative ?? null,
          is_owned: playlist?.is_owned ?? null,
          position: Math.max(0, playlist?.track_count ?? 1) - 1,
          spotify_track_id: trackId,
          track_name: selectedPreview.label,
          artist_name: previewArtistName,
          added_at: new Date().toISOString(),
          release_track_id: selectedPreview.releaseTrackId ?? null,
          recording_cluster_id: null,
          representative_release_track_id: null,
        };
      });
    if (newMemberships.length > 0) {
      setSelectedPreviewPlaylistMemberships((current) => {
        const membershipByPlaylistId = new Map(current.map((membership) => [membership.playlist_id, membership]));
        for (const playlistId of removedPlaylistIds) {
          membershipByPlaylistId.delete(playlistId);
        }
        for (const membership of newMemberships) {
          membershipByPlaylistId.set(membership.playlist_id, membership);
        }
        return Array.from(membershipByPlaylistId.values());
      });
    } else if (removedPlaylistIds.length > 0) {
      setSelectedPreviewPlaylistMemberships((current) => current.filter((membership) => !removedPlaylistIds.includes(membership.playlist_id)));
    }
    if (createdPlaylists.length > 0) {
      setProfile((current) => current
        ? {
          ...current,
          owned_playlists: [
            ...createdPlaylists,
            ...current.owned_playlists.filter((playlist) => !createdPlaylists.some((created) => created.playlist_id === playlist.playlist_id)),
          ],
          owned_playlists_available: true,
        }
        : current);
    }
  }

  async function handlePlaylistTrackPlayback(track: RecentTrack, action: PlaybackAction) {
    const trackUri = trackUriWithFallback(track.uri, track.track_id);
    if (!trackUri) {
      setPlayerError("This playlist track is not playable.");
      return;
    }
    const playlistQueue = buildPlaylistPlaybackQueue(trackUri);
    await handlePlaybackAction(action, {
      trackUri,
      optimisticTrack: playerSummaryFromPlaylistTrack(track),
      insertTracks: playlistQueue?.queueTracks,
      queueCursor: playlistQueue?.queueCursor,
      queueContext: playlistQueue?.queueContext,
      queuePlaylistUris: playlistQueue?.playlistUris,
      queueTracks: playlistQueue?.queueTracks,
      sourceTrack: track,
    });
  }

  async function handlePlaylistPlayAll(action: PlaybackAction = "play_now") {
    const firstPlayableTrack = playlistTrackEntries
      .map((track) => ({ track, uri: trackUriWithFallback(track.uri, track.track_id) }))
      .find((item) => Boolean(item.uri));
    if (!firstPlayableTrack) {
      setPlayerError("This playlist does not have a playable first song.");
      return;
    }
    await handlePlaylistTrackPlayback(firstPlayableTrack.track, action);
  }

  async function handleAlbumPlayAll(
    action: PlaybackAction = "play_now",
    entries: AlbumTrackEntry[] = albumTrackEntries,
  ) {
    const firstPlayableTrack = entries
      .filter((track) => !track.familyExclusive)
      .map((track) => ({ track, uri: trackUriWithFallback(track.uri, track.id) }))
      .find((item) => Boolean(item.uri));
    if (!firstPlayableTrack) {
      setPlayerError("This album does not have a playable first song.");
      return;
    }
    const albumQueue = buildAlbumPlaybackQueue(firstPlayableTrack.uri, entries);
    await handlePlaybackAction(action, {
      trackUri: firstPlayableTrack.uri,
      optimisticTrack: playerSummaryFromAlbumTrack(firstPlayableTrack.track),
      insertTracks: albumQueue?.queueTracks,
      queueCursor: albumQueue?.queueCursor,
      queueContext: albumQueue?.queueContext,
      queuePlaylistUris: albumQueue?.playlistUris,
      queueTracks: albumQueue?.queueTracks,
      sourceTrack: firstPlayableTrack.track.sourceTrack,
    });
    if (action === "play_now") {
      openAlbumTrackPreview(firstPlayableTrack.track);
    }
  }

  function handleExperienceModeChange(nextMode: ExperienceMode) {
    if (nextMode === experienceMode) {
      return;
    }
    setTestFullSuccessPinned(false);
    setTestProbeModeVisual(null);
    setExperienceMode(nextMode);
    setProfileLoadAttempted(false);
    setProfile(null);
    setOpenSections(INITIAL_OPEN_SECTIONS);
    setSectionPages(INITIAL_SECTION_PAGES);
    setPlayerQueueTracks([]);
    setPlayerQueueGroups([]);
    setPlayerQueueGroupCursors({});
    setHomeQueueOpenGroupIds(new Set());
    setPlayerQueueCursor(null);
    setPlayerQueueSource(null);
    resetQueueControls();
    setQueuePlaylistUri(null);
    if (nextMode === "local") {
      setStatusMessage("Loading local-only experience...");
      setAuthTransitioning(true);
      return;
    }
    if (session?.authenticated) {
      setStatusMessage("Loading full Spotify experience...");
      setAuthTransitioning(true);
    } else {
      setStatusMessage("Not connected yet. Use Spotify login to start the auth flow.");
      setAuthTransitioning(false);
    }
  }

  function renderExperienceModeToggle() {
    const visualMode: ExperienceVisualMode =
      testProbeModeVisual ?? (testFullSuccessPinned ? "test" : experienceMode);
    const localActive = visualMode === "local" || (testingFullExperience && experienceMode === "local");
    const testActive = visualMode === "test" || testingFullExperience;
    return (
      <div className="experience-toggle" role="group" aria-label="Experience mode">
        <span
          className={`experience-toggle-slider experience-toggle-slider-${visualMode}${testingFullExperience ? " experience-toggle-slider-testing" : ""}${testFullSuccessPinned ? " experience-toggle-slider-flash" : ""}`}
          aria-hidden="true"
        />
        <button
          className={`experience-chip${localActive ? " experience-chip-active" : ""}`}
          onClick={() => handleExperienceModeChange("local")}
          type="button"
        >
          Local
        </button>
        <button
          className={`experience-chip experience-chip-test${testActive ? " experience-chip-active" : ""}${testingFullExperience ? " experience-chip-test-running" : ""}${testFullSuccessPinned ? " experience-chip-test-success" : ""}`}
          disabled={testingFullExperience}
          onClick={() => void testFullExperienceAvailability()}
          type="button"
        >
          {testingFullExperience ? "Testing..." : "Test"}
        </button>
        <button
          className={`experience-chip${visualMode === "full" ? " experience-chip-active" : ""}`}
          onClick={() => handleExperienceModeChange("full")}
          type="button"
        >
          Full
        </button>
      </div>
    );
  }

  async function testFullExperienceAvailability() {
    if (testingFullExperience) {
      return;
    }
    setTestFullSuccessPinned(false);
    setTestProbeModeVisual("test");
    setTestingFullExperience(true);
    try {
      const response = await fetch(`${apiBaseUrl}/auth/full-availability`, {
        credentials: "include",
      });
      if (!response.ok) {
        throw new Error("Could not test full experience availability.");
      }
      const data = (await response.json()) as FullAvailabilityResponse;
      if (data.available) {
        setTestFullSuccessPinned(true);
        setTestProbeModeVisual("test");
        setStatusMessage("Full experience is available. You can switch modes anytime.");
        setStatusHistory((current) => [...current, "Full experience test: available."]);
        return;
      }
      const retryAfter = Math.max(0, Number(data.retry_after_seconds ?? 0));
      if (data.blocked && retryAfter > 0) {
        setTestFullSuccessPinned(false);
        setTestProbeModeVisual("local");
        setReloadCooldownDurationMs(retryAfter * 1000);
        setReloadCooldownUntil(Date.now() + retryAfter * 1000);
        const message = formatCooldownCopy(retryAfter);
        setStatusMessage(message);
        setStatusHistory((current) => [...current, `Full experience test: blocked (${message})`]);
        return;
      }
      setTestFullSuccessPinned(false);
      setTestProbeModeVisual("local");
      const detail = data.detail || "Full experience is not currently available.";
      setStatusMessage(detail);
      setStatusHistory((current) => [...current, `Full experience test: ${detail}`]);
    } catch (error) {
      setTestFullSuccessPinned(false);
      setTestProbeModeVisual("local");
      const message = error instanceof Error ? error.message : "Could not test full experience availability.";
      setStatusMessage(message);
      setStatusHistory((current) => [...current, `Full experience test error: ${message}`]);
    } finally {
      setTestingFullExperience(false);
    }
  }

  function renderRecentRangeHeader() {
    const showRangeRefreshSpinner = recentRangeRefreshPending || loadingRecentSection;
    return (
      <div className="section-column-header">
        <h3>Recents</h3>
        <div className="recent-header-controls">
          <div className="recent-range-toggle" role="group" aria-label="Recent range">
            {showRangeRefreshSpinner ? (
              <span className="recent-range-vinyl-spinner" aria-hidden="true">
                <span className="recent-range-vinyl-center" />
              </span>
            ) : null}
            {RECENT_RANGE_OPTIONS.map((option) => (
              <button
                key={option.value}
                className={`recent-range-chip${recentRange === option.value ? " recent-range-chip-active" : ""}`}
                onClick={() => {
                  if (option.value === recentRange || loadingRecentSection) {
                    return;
                  }
                  setRecentRangeRefreshPending(true);
                  void refreshRecentSection(option.value);
                }}
                type="button"
              >
                {option.label}
              </button>
            ))}
          </div>
          <div className="recent-source-toggle" role="group" aria-label="Recent top tracks source">
            <button
              aria-pressed={recentTopTracksUseSpotify}
              className={`recent-range-chip${recentTopTracksUseSpotify ? " recent-range-chip-active" : ""}`}
              onClick={() => setRecentTopTracksUseSpotify((current) => !current)}
              type="button"
            >
              Spotify
            </button>
          </div>
        </div>
      </div>
    );
  }

  async function loadSession() {
    try {
      const response = await fetch(`${apiBaseUrl}/auth/session`, {
        credentials: "include",
      });

      if (!response.ok) {
        throw new Error("Failed to load auth session.");
      }

      const data = (await response.json()) as SessionResponse;
      setSession(data);
      const sessionCooldownSeconds = Math.max(0, Number(data.spotify_cooldown_seconds_remaining ?? 0));
      if (sessionCooldownSeconds > 0) {
        setReloadCooldownDurationMs(sessionCooldownSeconds * 1000);
        setReloadCooldownUntil(Date.now() + sessionCooldownSeconds * 1000);
      }
      setProfileLoadAttempted(false);
      setOpenSections(INITIAL_OPEN_SECTIONS);
      setSectionPages(INITIAL_SECTION_PAGES);

      if (data.authenticated) {
        setStatusMessage("");
        setStatusHistory([]);
        setAuthTransitioning(true);
      } else {
        setProfile(null);
        setProfileMenuOpen(false);
        setProfileSettingsOpen(false);
        setBrandMenuOpen(false);
        setPlayerMenuOpen(false);
        setLivePlaybackSnapshot(null);
        setLivePlaybackProbeComplete(false);
        setLiveControlOverrideUntilMs(null);
        setCurrentTrack(null);
        setPlayerQueueTracks([]);
        setPlayerQueueGroups([]);
        setPlayerQueueGroupCursors({});
        setHomeQueueOpenGroupIds(new Set());
        setPlayerQueueCursor(null);
        setPlayerQueueSource(null);
        clearQueueContext();
        resetQueueControls();
        setQueuePlaylistUri(null);
        setPlayerReady(false);
        setStatusMessage("Not connected yet. Use Spotify login to start the auth flow.");
        setStatusHistory([]);
        setAuthTransitioning(false);
      }
    } catch (error) {
      setStatusMessage(formatUiErrorMessage(error, "Failed to load session."));
    }
  }

  async function loadCurrentPlaybackSnapshot() {
    try {
      const response = await fetch(`${apiBaseUrl}/auth/current-playback`, {
        credentials: "include",
      });
      if (!response.ok) {
        setLivePlaybackSnapshot(null);
        setLiveAwaitingNextTrack(false);
        return;
      }
      const data = (await response.json()) as CurrentPlaybackResponse;
      if (data.status === "ok" && data.has_playback && data.snapshot) {
        setLivePlaybackSnapshot(data.snapshot);
        setLiveAwaitingNextTrack(false);
        return;
      }
      setLivePlaybackSnapshot(null);
      setLiveAwaitingNextTrack(false);
    } catch {
      setLivePlaybackSnapshot(null);
      setLiveAwaitingNextTrack(false);
    }
  }

  function startLogin() {
    window.location.href = `${apiBaseUrl}/auth/login`;
  }

  async function loadRecentIngestResult() {
    try {
      const response = await fetch(`${apiBaseUrl}/auth/recent-ingest/result`, {
        credentials: "include",
      });
      if (!response.ok) {
        throw new Error(`Recent ingest result failed (${response.status})`);
      }
      const data = (await response.json()) as RecentIngestResultResponse;
      if (!data.has_result) {
        setStatusMessage("Spotify auth succeeded, but no ingest result was returned.");
        return;
      }
      if (data.auth_succeeded && data.ingest_succeeded) {
        const earliest = data.earliest_api_played_at ?? "n/a";
        const latest = data.latest_api_played_at ?? "n/a";
        setStatusMessage(
          `Recent ingest succeeded: ${data.row_count ?? 0} rows (${earliest} to ${latest}).`,
        );
      } else {
        setStatusMessage(`Recent ingest failed: ${data.error ?? "unknown error"}`);
      }
    } catch (error) {
      setStatusMessage(
        error instanceof Error ? error.message : "Failed to load recent ingest result.",
      );
    } finally {
      setRecentIngestCallbackPending(false);
    }
  }

  function handleAuthAction() {
    if (experienceMode === "local") {
      setProfile(null);
      setProfileLoadAttempted(false);
      setAuthTransitioning(true);
      setStatusMessage("Loading local-only experience...");
      return;
    }
    if (session?.authenticated) {
      startLogin();
      return;
    }
    startLogin();
  }

  function toggleSection(section: SectionKey, anchorId?: string) {
    setExperimentalMenuOpen(false);
    const isCurrentlyOpen = openSections[section];
    if (isCurrentlyOpen && anchorId) {
      const element = document.getElementById(anchorId);
      if (element) {
        element.scrollIntoView({ behavior: "smooth", block: "start" });
      }
      window.setTimeout(() => {
        setOpenSections((current) => ({
          ...current,
          [section]: false,
        }));
      }, 180);
      return;
    }

    setOpenSections((current) => ({
      ...current,
      [section]: !current[section],
    }));
  }

  function openListeningLogPage() {
    setListeningLogTracks([]);
    setListeningLogHasMore(false);
    setListeningLogOffset(0);
    setListeningLogLoaded(false);
    setListeningLogLoading(false);
    setListeningLogError("");
    setListeningLogLastLoadedAt(null);
    setOpenDebugSessions({});
    setOpenDebugTracks({});
    setExperimentalMenuOpen(false);
    setAppPage("recentDebug");
  }

  function openFormulaLabPage() {
    setMergedTracks([]);
    setMergedTracksLoaded(false);
    setMergedTracksLoading(false);
    setMergedTracksError("");
    setMergedTracksExcludedUnknownCount(0);
    setMergedTracksLastLoadedAt(null);
    setExperimentalMenuOpen(false);
    setAppPage("formulaLab");
  }

  function openIdentityAuditPage() {
    setIdentityAudit(null);
    setIdentityAuditLoaded(false);
    setIdentityAuditLoading(false);
    setIdentityAuditError("");
    setIdentityAuditLastLoadedAt(null);
    setIdentityAuditSuggestedGroups(null);
    setIdentityAuditSuggestedLoaded(false);
    setIdentityAuditSuggestedLoading(false);
    setIdentityAuditSuggestedError("");
    setIdentityAuditSuggestedLastLoadedAt(null);
    setIdentityAuditAmbiguous(null);
    setIdentityAuditAmbiguousLoaded(false);
    setIdentityAuditAmbiguousLoading(false);
    setIdentityAuditAmbiguousError("");
    setIdentityAuditAmbiguousLastLoadedAt(null);
    setAlbumDuplicateLookupResult(null);
    setAlbumDuplicateLookupLoading(false);
    setAlbumDuplicateLookupLoaded(false);
    setAlbumDuplicateLookupError("");
    setAlbumDuplicateLookupLastLoadedAt(null);
    setAlbumNameDuplicateLookupResult(null);
    setAlbumNameDuplicateLookupLoading(false);
    setAlbumNameDuplicateLookupLoaded(false);
    setAlbumNameDuplicateLookupError("");
    setAlbumNameDuplicateLookupLastLoadedAt(null);
    setTrackDuplicateLookupResult(null);
    setTrackDuplicateLookupLoading(false);
    setTrackDuplicateLookupLoaded(false);
    setTrackDuplicateLookupError("");
    setTrackDuplicateLookupLastLoadedAt(null);
    setSelectedAlbumMergeReviewKey(null);
    const savedPrefs = loadIdentityAuditPersistedPrefs();
    setIdentityAuditEntityTab(savedPrefs.entityTab ?? "tracks");
    setTrackIdentityAuditTab(savedPrefs.trackTab ?? "problems");
    setAlbumIdentityAuditTab(savedPrefs.albumTab ?? "problems");
    setExperimentalMenuOpen(false);
    setAppPage("identityAudit");
  }

  function openCatalogBackfillPage() {
    setCatalogBackfillCoverage(null);
    setCatalogBackfillCoverageLoading(false);
    setCatalogBackfillCoverageLoaded(false);
    setCatalogBackfillCoverageError("");
    setCatalogBackfillCoverageLastLoadedAt(null);
    setCatalogBackfillRuns(null);
    setCatalogBackfillRunsLoading(false);
    setCatalogBackfillRunsLoaded(false);
    setCatalogBackfillRunsError("");
    setCatalogBackfillRunsLastLoadedAt(null);
    setCatalogBackfillQueue(null);
    setCatalogBackfillQueueLoading(false);
    setCatalogBackfillQueueLoaded(false);
    setCatalogBackfillQueueError("");
    setCatalogBackfillQueueLastLoadedAt(null);
    setCatalogBackfillQueueStatusFilter("all");
    setCatalogBackfillQueueReasonFilter("all");
    setCatalogBackfillTab("priorityMetadata");
    setCatalogBackfillQueueRepairLoading(false);
    setCatalogBackfillQueueRepairMessage("");
    setCatalogBackfillRunLoading(false);
    setCatalogBackfillRunError("");
    setCatalogBackfillLatestResult(null);
    setCatalogBackfillLimit(25);
    setCatalogBackfillOffset(0);
    setCatalogBackfillMarket("US");
    setCatalogBackfillIncludeAlbums(true);
    setCatalogBackfillForceRefresh(false);
    setCatalogBackfillRequestDelaySeconds(2.0);
    setCatalogBackfillMaxRuntimeSeconds(60);
    setCatalogBackfillMaxRequests(150);
    setCatalogBackfillMaxErrors(10);
    setCatalogBackfillMaxAlbumTracksPagesPerAlbum(10);
    setCatalogBackfillMax429(3);
    setCatalogBackfillAlbumTracklistPolicy("relevant_albums");
    setCatalogBackfillFullRunMode("tracklists_relevant");
    setExperimentalMenuOpen(false);
    setAppPage("catalogBackfill");
  }

  function openSearchLookupPage() {
    setSearchLookupEntityType("albums");
    setSearchLookupQueueStatus("all");
    setSearchLookupSort("default");
    setAlbumCatalogLookupQ("");
    setAlbumCatalogLookupStatus("all");
    setAlbumCatalogLookupResult(null);
    setAlbumCatalogLookupLoading(false);
    setAlbumCatalogLookupLoaded(false);
    setAlbumCatalogLookupError("");
    setAlbumCatalogLookupLastLoadedAt(null);
    setTrackCatalogLookupStatus("all");
    setTrackCatalogLookupResult(null);
    setTrackCatalogLookupLoading(false);
    setTrackCatalogLookupLoaded(false);
    setTrackCatalogLookupError("");
    setTrackCatalogLookupLastLoadedAt(null);
    setAlbumDuplicateLookupResult(null);
    setAlbumDuplicateLookupLoading(false);
    setAlbumDuplicateLookupLoaded(false);
    setAlbumDuplicateLookupError("");
    setAlbumDuplicateLookupLastLoadedAt(null);
    setAlbumCatalogLookupEnqueueLoading(false);
    setAlbumCatalogLookupEnqueueError("");
    setAlbumCatalogLookupEnqueueResult(null);
    setExperimentalMenuOpen(false);
    setAppPage("searchLookup");
  }

  function openAlbumLookupPreview(item: AlbumCatalogLookupItem) {
    const spotifyAlbumId = item.spotify_album_id ?? null;
    setSelectedPreview({
      image: null,
      fallbackLabel: "L",
      label: item.release_album_name,
      meta: item.artist_name ?? null,
      detail: item.release_date ?? null,
      kind: "album",
      entityId: spotifyAlbumId,
      trackUri: null,
      url: spotifyAlbumId ? `https://open.spotify.com/album/${spotifyAlbumId}` : "",
      trackId: null,
      albumId: spotifyAlbumId,
      artistName: item.artist_name ?? null,
      artists: artistEntryFromDisplayText(item.artist_name),
      sourceAlbumId: spotifyAlbumId,
      sourceAlbumName: item.release_album_name,
      sourceAlbumImage: null,
      sourceAlbumUrl: spotifyAlbumId ? `https://open.spotify.com/album/${spotifyAlbumId}` : "",
      sourceAlbumYear: item.release_date ?? null,
      sourceTrack: null,
    });
  }

  function openTrackLookupPreview(item: TrackCatalogLookupItem) {
    const spotifyTrackId = item.spotify_track_id ?? null;
    const spotifyAlbumId = item.album_id ?? null;
    const sourceTrack: RecentTrack = {
      track_id: spotifyTrackId,
      track_name: item.spotify_track_name ?? item.release_track_name,
      artist_name: item.artist_name,
      album_name: item.release_album_name,
      duration_ms: item.duration_ms,
      uri: spotifyTrackId ? `spotify:track:${spotifyTrackId}` : null,
      url: spotifyEntityUrl("track", spotifyTrackId),
      album_id: spotifyAlbumId,
      album_url: spotifyEntityUrl("album", spotifyAlbumId),
    };
    setSelectedPreview({
      image: null,
      fallbackLabel: "T",
      label: item.spotify_track_name ?? item.release_track_name,
      meta: item.artist_name ?? null,
      detail: item.release_album_name ?? null,
      kind: "track",
      entityId: spotifyTrackId,
      trackUri: trackUriWithFallback(sourceTrack.uri, spotifyTrackId),
      url: sourceTrack.url ?? "",
      trackId: spotifyTrackId,
      albumId: spotifyAlbumId,
      artistName: item.artist_name ?? null,
      sourceTrack,
    });
  }

  function openTrackMappingSourcePreview(group: TrackMappingSourceReleaseGroup, source: TrackMappingSourceItem) {
    const spotifyTrackId = source.source_name === "spotify" ? source.external_id : null;
    const spotifyAlbumId = source.album_id ?? null;
    const trackName = source.spotify_track_name ?? source.source_name_raw ?? group.release_track_name;
    const albumName = source.album_name_display ?? source.album_name ?? group.release_album_name;
    const sourceTrack: RecentTrack = {
      track_id: spotifyTrackId,
      track_name: trackName,
      artist_name: group.artist_name,
      album_name: albumName,
      duration_ms: source.duration_ms,
      uri: spotifyTrackId ? `spotify:track:${spotifyTrackId}` : null,
      url: spotifyEntityUrl("track", spotifyTrackId),
      album_id: spotifyAlbumId,
      album_url: spotifyEntityUrl("album", spotifyAlbumId),
      spotify_track_number: source.track_number,
      spotify_disc_number: source.disc_number,
      spotify_album_total_tracks: source.album_total_tracks,
      play_count: source.play_count,
    };
    setSelectedPreview({
      image: null,
      fallbackLabel: "T",
      label: trackName,
      meta: group.artist_name ?? null,
      detail: albumName ?? null,
      kind: "track",
      entityId: spotifyTrackId,
      trackUri: trackUriWithFallback(sourceTrack.uri, spotifyTrackId),
      url: sourceTrack.url ?? "",
      trackId: spotifyTrackId,
      albumId: spotifyAlbumId,
      artistName: group.artist_name ?? null,
      sourceTrack,
    });
  }

  function openTrackMappingSourceReleasePreview(group: TrackMappingSourceReleaseGroup) {
    const sourceTrack: RecentTrack = {
      track_id: null,
      track_name: group.release_track_name,
      artist_name: group.artist_name,
      album_name: group.release_album_name,
      duration_ms: null,
      uri: null,
      url: null,
      album_id: null,
      album_url: null,
      play_count: group.sources.reduce((sum, source) => sum + source.play_count, 0),
    };
    setSelectedPreview({
      image: null,
      fallbackLabel: "T",
      label: group.release_track_name,
      meta: group.artist_name ?? null,
      detail: group.release_album_name ?? null,
      kind: "track",
      entityId: null,
      trackUri: null,
      url: "",
      trackId: null,
      albumId: null,
      artistName: group.artist_name ?? null,
      sourceTrack,
    });
  }

  function openTrackMappingReleasePreview(track: TrackMappingReleaseItem) {
    const sourceTrack: RecentTrack = {
      track_id: null,
      track_name: track.release_track_name,
      artist_name: track.artist_name,
      album_name: track.release_album_name,
      duration_ms: null,
      uri: null,
      url: null,
      album_id: null,
      album_url: null,
      play_count: track.play_count,
    };
    setSelectedPreview({
      image: null,
      fallbackLabel: "T",
      label: track.release_track_name,
      meta: track.artist_name ?? null,
      detail: track.release_album_name ?? null,
      kind: "track",
      entityId: null,
      trackUri: null,
      url: "",
      trackId: null,
      albumId: null,
      artistName: track.artist_name ?? null,
      sourceTrack,
    });
  }

  function renderTrackRankingToggle() {
    const showTrackRankingSpinner = trackRankingRefreshPending;
    return (
      <TrackRankingToggle
        trackRankingMode={trackRankingMode}
        showTrackRankingSpinner={showTrackRankingSpinner}
        onSelectTrackRankingMode={(nextMode) => {
          if (nextMode === trackRankingMode) {
            return;
          }
          setTrackRankingRefreshPending(true);
          setSectionPages((current) => ({
            ...current,
            tracksAllTime: 0,
          }));
          setTrackRankingMode(nextMode);
        }}
      />
    );
  }

  function renderMergedTrackSourceFilterToggle() {
    return (
      <MergedTrackSourceFilterToggle
        mergedTrackSourceFilter={mergedTrackSourceFilter}
        setMergedTrackSourceFilter={setMergedTrackSourceFilter}
      />
    );
  }

  function renderRankMovementFilterToggle() {
    return (
      <RankMovementFilterToggle
        rankMovementFilter={rankMovementFilter}
        setRankMovementFilter={setRankMovementFilter}
      />
    );
  }

  function openAndScrollToSection(section: SectionKey, anchorId: string) {
    setExperimentalMenuOpen(false);
    setAppPage("dashboard");
    setOpenSections((current) => ({
      ...current,
      artists: false,
      tracks: false,
      albums: false,
      playlists: false,
      recent: false,
      [section]: true,
    }));

    window.setTimeout(() => {
      const element = document.getElementById(anchorId);
      if (element) {
        element.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    }, 0);
  }

  function moveSectionPage(section: SectionKey, direction: -1 | 1, itemCount: number, pageSize: number = PAGE_SIZE) {
    const maxPage = Math.max(0, Math.ceil(itemCount / pageSize) - 1);
    setSectionPages((current) => ({
      ...current,
      [section]: Math.min(maxPage, Math.max(0, current[section] + direction)),
    }));
  }

  function visibleItems<T>(section: SectionKey, items: T[]) {
    const start = sectionPages[section] * PAGE_SIZE;
    return items.slice(start, start + PAGE_SIZE);
  }

  function visibleItemsWithPageSize<T>(section: SectionKey, items: T[], pageSize: number) {
    const start = sectionPages[section] * pageSize;
    return items.slice(start, start + pageSize);
  }

  function recentUnavailableCopy(defaultCopy: string) {
    if (experienceMode === "local") {
      return "Recent sections in restricted local mode come from local history data only.";
    }
    if (loadingRecentSection) {
      return "Loading recent sections...";
    }
    const hasRecentDataLoaded = Boolean(
      profile
      && (
        profile.recent_tracks_available
        || profile.recent_likes_available
        || profile.recent_top_tracks_available
        || profile.recent_top_artists_available
        || profile.recent_top_albums_available
      ),
    );
    if (analysisMode === "quick" && !hasRecentDataLoaded) {
      return "Recent data is off in quick load. Open settings and choose Load full analysis.";
    }
    return defaultCopy;
  }

  function quickUnavailableCopy(defaultCopy: string) {
    if (experienceMode === "local") {
      return "This section is unavailable in restricted local mode until we have a saved Spotify snapshot for it.";
    }
    if (analysisMode === "quick") {
      return "This section is limited in quick load. Open settings and choose Load full analysis.";
    }
    return defaultCopy;
  }

  function hasStaleSection(section: string) {
    return Boolean(profile?.stale_sections?.includes(section));
  }

  function renderSectionTitle(title: string, staleSection?: string) {
    const showStale = Boolean(staleSection && experienceMode === "local" && hasStaleSection(staleSection));
    const syncedLabel = formatRelativeSyncTime(profile?.local_last_synced_at);
    return (
      <span className="section-title-row">
        <span>{title}</span>
        {showStale ? (
          <span
            className="section-stale-badge"
            title={`Cached from Spotify${syncedLabel ? `, last synced ${syncedLabel}` : ""}. Open settings to reload.`}
          >
            Cached
          </span>
        ) : null}
      </span>
    );
  }

  async function loadProfile() {
    if (profileLoadInFlightRef.current) {
      return;
    }
    const fellBackForCooldown = experienceMode === "full" && spotifyCooldownActive;
    const loadExperienceMode = fellBackForCooldown ? "local" : experienceMode;
    if (loadExperienceMode === "local" && experienceMode !== "local") {
      setExperienceMode("local");
    }
    profileLoadInFlightRef.current = true;
    setLoadingProfile(true);
    setProfileLoadAttempted(true);
    setStatusMessage(loadExperienceMode === "local" ? "Loading local history..." : "Loading your Spotify data...");
    setStatusHistory(["Initial load started."]);
    let pollingActive = true;
    let progressTimer: number | null = null;
    const startedAt = performance.now();

    const updateProgress = async () => {
      const fallbackElapsed = (performance.now() - startedAt) / 1000;
      try {
        const response = await fetch(`${apiBaseUrl}/me/progress`, {
          credentials: "include",
        });
        if (!response.ok) {
          if (pollingActive) {
            setStatusMessage(formatLoadingStatusUi(null));
          }
          return;
        }
        const data = (await response.json()) as ProfileProgressResponse;
        if (!pollingActive) {
          return;
        }
        setStatusMessage(formatLoadingStatusUi(data.active ? data.phase : null));
        if (data.events?.length) {
          setStatusHistory(
            [
              "Initial load started.",
              ...data.events.map((event) => `initial ${event.at_seconds.toFixed(1)}s: ${event.phase}`),
            ],
          );
        } else {
          setStatusHistory(["Initial load started.", `initial ${formatLoadingStatusDetailed(null, fallbackElapsed)}`]);
        }
      } catch {
        if (pollingActive) {
          setStatusMessage(formatLoadingStatusUi(null));
          setStatusHistory(["Initial load started.", `initial ${formatLoadingStatusDetailed(null, fallbackElapsed)}`]);
        }
      }
    };

    await updateProgress();
    progressTimer = window.setInterval(() => {
      void updateProgress();
    }, 500);
    try {
      const endpoint = loadExperienceMode === "local" ? "/me/local" : "/me";
      const profileParams = new URLSearchParams({
        recent_range: recentRange,
        analysis_mode: analysisMode,
      });
      if (loadExperienceMode === "local") {
        profileParams.set("mode", "extended");
      } else if (analysisMode === "quick") {
        profileParams.set("mode", "shell");
      }
      const response = await fetch(
        `${apiBaseUrl}${endpoint}?${profileParams.toString()}`,
        {
        method: "GET",
        credentials: "include",
        },
      );

      if (!response.ok) {
        let detail = "Failed to load Spotify profile.";
        try {
          const payload = (await response.json()) as { detail?: string };
          if (payload.detail) {
            detail = payload.detail;
          }
        } catch {
          // ignore invalid error payloads
        }
        if (response.status === 403) {
          detail = "Spotify permission missing. Log out and log back in to grant the latest scopes.";
        }
        if (response.status === 429) {
          const cooldownSeconds = parseCooldownSeconds(detail) ?? 60;
          setReloadCooldownDurationMs(cooldownSeconds * 1000);
          setReloadCooldownUntil(Date.now() + cooldownSeconds * 1000);
          const localResponse = await fetch(
            `${apiBaseUrl}/me/local?${new URLSearchParams({
              recent_range: recentRange,
              analysis_mode: analysisMode,
              mode: "extended",
            }).toString()}`,
            { method: "GET", credentials: "include" },
          );
          if (localResponse.ok) {
            const localData = (await localResponse.json()) as ProfileResponse;
            const hydratedLocalProfile = {
              ...localData,
              username: session?.spotify_user_id ?? localData.username,
              display_name: session?.display_name ?? localData.display_name,
              email: session?.email ?? localData.email,
            };
            setExperienceMode("local");
            setProfile(hydratedLocalProfile);
            setAnalysisMode(hydratedLocalProfile.analysis_mode ?? analysisMode);
            setAuthTransitioning(false);
            setSectionPages(INITIAL_SECTION_PAGES);
            setStatusMessage(`Spotify is rate-limiting requests. Loaded offline data; Full mode will be available after the cooldown.`);
            setStatusHistory((current) => [...current, `Spotify 429: switched to Local mode for ${cooldownSeconds}s cooldown.`]);
            if (hydratedLocalProfile.recent_range) {
              setRecentRange(hydratedLocalProfile.recent_range);
            }
            return;
          }
          detail = `${formatCooldownCopy(cooldownSeconds)} Offline data could not be loaded.`;
        }
        throw new Error(detail);
      }

      const data = (await response.json()) as ProfileResponse;
      let hydratedProfile = data;
      if (loadExperienceMode === "local") {
        hydratedProfile = {
          ...hydratedProfile,
          username: session?.spotify_user_id ?? hydratedProfile.username,
          display_name: session?.display_name ?? hydratedProfile.display_name,
          email: session?.email ?? hydratedProfile.email,
        };
      }

      setProfile(hydratedProfile);
      setAnalysisMode(hydratedProfile.analysis_mode ?? analysisMode);
      setAuthTransitioning(false);
      setSectionPages(INITIAL_SECTION_PAGES);
      setStatusMessage(fellBackForCooldown
        ? "Spotify is rate-limiting requests. Loaded offline data; Full mode will be available after the cooldown."
        : "");
      if (hydratedProfile.recent_range) {
        setRecentRange(hydratedProfile.recent_range);
      }
    } catch (error) {
      const message = formatUiErrorMessage(error, "Failed to load Spotify profile.");
      setStatusMessage(message);
      setAuthTransitioning(false);
      setStatusHistory((current) => (current.length > 0 ? [...current, `Error: ${message}`] : [message]));
    } finally {
      profileLoadInFlightRef.current = false;
      pollingActive = false;
      if (progressTimer != null) {
        window.clearInterval(progressTimer);
      }
      setLoadingProfile(false);
    }
  }

  async function loadQuickProfileSections(targetRange: RecentRange = recentRange) {
    if (experienceMode !== "full" || spotifyCooldownActive || quickProfileLoadInFlightRef.current) {
      return;
    }
    quickProfileLoadInFlightRef.current = true;
    try {
      const response = await fetch(
        `${apiBaseUrl}/me?recent_range=${encodeURIComponent(targetRange)}&analysis_mode=quick`,
        {
          method: "GET",
          credentials: "include",
        },
      );

      if (!response.ok) {
        let detail = "Failed to load quick profile sections.";
        try {
          const payload = (await response.json()) as { detail?: string };
          if (payload.detail) {
            detail = payload.detail;
          }
        } catch {
          // ignore invalid error payloads
        }
        throw new Error(detail);
      }

      const data = (await response.json()) as ProfileResponse;
      setProfile((current) => current
        ? {
            ...current,
            followers_total: data.followers_total,
            followed_artists_total: data.followed_artists_total,
            followed_artists_available: data.followed_artists_available,
            followed_artists: data.followed_artists,
            followed_artists_list_available: data.followed_artists_list_available,
            top_tracks: data.top_tracks,
            top_tracks_available: data.top_tracks_available,
            top_albums: data.top_albums,
            top_albums_available: data.top_albums_available,
            top_playlists_recent: data.top_playlists_recent,
            top_playlists_all_time: data.top_playlists_all_time,
            top_playlists_available: data.top_playlists_available,
            owned_playlists: data.owned_playlists,
            owned_playlists_available: data.owned_playlists_available,
            history_insights_available: data.history_insights_available,
            history_first_played_at: data.history_first_played_at,
            history_last_played_at: data.history_last_played_at,
            history_total_listen_ms: data.history_total_listen_ms,
            history_total_play_count: data.history_total_play_count,
            extended_loaded: data.extended_loaded,
          }
        : current);
    } catch (error) {
      const message = formatUiErrorMessage(error, "Failed to load quick profile sections.");
      setStatusHistory((current) => [...current, `Quick profile warning: ${message}`]);
    } finally {
      quickProfileLoadInFlightRef.current = false;
    }
  }

  async function loadExtendedProfile(targetRange: RecentRange = recentRange, targetAnalysisMode: AnalysisMode = "full") {
    if (experienceMode === "full" && spotifyCooldownActive) {
      setStatusMessage(formatCooldownCopy(reloadSecondsRemaining));
      setStatusHistory((current) => [...current, "Spotify cooldown active. Full analysis paused."]);
      return;
    }
    if (extendedLoadInFlightRef.current) {
      return;
    }
    extendedLoadInFlightRef.current = true;
    setLoadingExtendedProfile(true);
    setStatusMessage("Starting full analysis...");
    setStatusHistory((current) => [...current, "Background expansion started."]);
    let pollingActive = true;
    let progressTimer: number | null = null;
    const startedAt = performance.now();

    const updateProgress = async () => {
      const fallbackElapsed = (performance.now() - startedAt) / 1000;
      try {
        const response = await fetch(`${apiBaseUrl}/me/progress`, {
          credentials: "include",
        });
        if (!response.ok) {
          return;
        }
        const data = (await response.json()) as ProfileProgressResponse;
        if (!pollingActive) {
          return;
        }
        if (data.events?.length) {
          setStatusMessage(
            formatLoadingStatusUi(data.active ? data.phase : "Analyzing your music..."),
          );
          setStatusHistory((current) => {
            const prefix = current.filter((entry) => !entry.startsWith("background "));
            const extensionEvents = (data.events ?? []).map(
              (event) => `background ${event.at_seconds.toFixed(1)}s: ${event.phase}`,
            );
            return [...prefix, ...extensionEvents];
          });
        } else {
          setStatusMessage(formatLoadingStatusUi(null));
          setStatusHistory((current) => {
            const prefix = current.filter((entry) => !entry.startsWith("background "));
            return [...prefix, `background ${formatLoadingStatusDetailed(null, fallbackElapsed)}`];
          });
        }
      } catch {
        // ignore background progress failures
      }
    };

    progressTimer = window.setInterval(() => {
      void updateProgress();
    }, 500);
    try {
      const endpoint = experienceMode === "local" ? "/me/local" : "/me";
      const response = await fetch(
        `${apiBaseUrl}${endpoint}?mode=extended&recent_range=${encodeURIComponent(targetRange)}&analysis_mode=${encodeURIComponent(targetAnalysisMode)}`,
        {
          credentials: "include",
        },
      );

      if (!response.ok) {
        let detail = "Failed to load Spotify profile.";
        try {
          const payload = (await response.json()) as { detail?: string };
          if (payload.detail) {
            detail = payload.detail;
          }
        } catch {
          // ignore invalid error payloads
        }
        if (response.status === 403) {
          detail = "Spotify permission missing. Log out and log back in to grant the latest scopes.";
        }
        if (response.status === 429) {
          const cooldownSeconds = parseCooldownSeconds(detail) ?? 60;
          setReloadCooldownDurationMs(cooldownSeconds * 1000);
          setReloadCooldownUntil(Date.now() + cooldownSeconds * 1000);
          detail = formatCooldownCopy(cooldownSeconds);
        }
        throw new Error(detail);
      }

      const data = (await response.json()) as ProfileResponse;
      setProfile((current) => mergeExtendedProfile(current, data));
      setAnalysisMode(data.analysis_mode ?? targetAnalysisMode);
      if (data.recent_range) {
        setRecentRange(data.recent_range);
      }
      setStatusMessage("");
      setStatusHistory((current) => {
        const filtered = current.filter((entry) => !entry.startsWith("background "));
        return [...filtered, "Background expansion complete."];
      });
    } catch (error) {
      const message = formatUiErrorMessage(error, "Failed to load extended Spotify profile.");
      setStatusMessage(message);
      setStatusHistory((current) => {
        const filtered = current.filter((entry) => !entry.startsWith("background "));
        return [...filtered, `Background expansion error: ${message}`];
      });
    } finally {
      extendedLoadInFlightRef.current = false;
      pollingActive = false;
      if (progressTimer != null) {
        window.clearInterval(progressTimer);
      }
      setLoadingExtendedProfile(false);
    }
  }

  async function logout() {
    await fetch(`${apiBaseUrl}/auth/logout`, {
      method: "POST",
      credentials: "include",
    });
    setSession({
      authenticated: false,
      display_name: null,
      spotify_user_id: null,
      email: null,
    });
    setProfile(null);
    setProfileLoadAttempted(false);
    setOpenSections(INITIAL_OPEN_SECTIONS);
    setSectionPages(INITIAL_SECTION_PAGES);
    setStatusMessage("Signed out.");
    setStatusHistory([]);
    setAuthTransitioning(false);
    setProfileMenuOpen(false);
    setProfileSettingsOpen(false);
    setBrandMenuOpen(false);
    setPlayerMenuOpen(false);
    setCurrentTrack(null);
    setPlayerQueueTracks([]);
    setPlayerQueueGroups([]);
    setPlayerQueueGroupCursors({});
    setHomeQueueOpenGroupIds(new Set());
    setPlayerQueueCursor(null);
    setPlayerQueueSource(null);
    clearQueueContext();
    resetQueueControls();
    setQueuePlaylistUri(null);
    setPlayerReady(false);
    setListeningLogTracks([]);
    setListeningLogHasMore(false);
    setListeningLogOffset(0);
    setListeningLogLoading(false);
    setListeningLogLoaded(false);
    setListeningLogError("");
    setListeningLogLastLoadedAt(null);
    setRecentDebugSourceFilter("all");
    setMergedTracks([]);
    setMergedTracksLoaded(false);
    setMergedTracksLoading(false);
    setMergedTracksError("");
    setMergedTracksExcludedUnknownCount(0);
    setMergedTracksLastLoadedAt(null);
    setRecentComputedTracks([]);
    setRecentComputedTracksLoaded(false);
    setRecentComputedTracksLoading(false);
    setRecentComputedTracksError("");
    setMergedTrackSourceFilter("all");
    setRecentTopTracksUseSpotify(false);
    setIdentityAudit(null);
    setIdentityAuditLoading(false);
    setIdentityAuditLoaded(false);
    setIdentityAuditError("");
    setIdentityAuditLastLoadedAt(null);
  }

  function renderDashboardListCard(props: DashboardListCardProps, key: string) {
    const previewKind: PreviewItem["kind"] = props.fallbackLabel === "T"
      ? "track"
      : props.fallbackLabel === "A"
        ? "artist"
        : props.fallbackLabel === "P"
          ? "playlist"
          : "album";

    return (
      <DashboardListCard
        key={key}
        {...props}
        previewKind={previewKind}
        previewTrackUri={trackUriWithFallback(props.trackUri, props.previewTrack?.track_id ?? props.entityId ?? null)}
        onSelectPreview={setSelectedPreview}
      />
    );
  }

  function renderTrackColumn(
    section: SectionKey,
    items: RecentTrack[],
    available: boolean,
    emptyCopy: string,
    unavailableCopy: string,
    unavailableAction?: ReactNode,
    paged: boolean = true,
    presorted: boolean = false,
  ) {
    return (
      <DashboardTrackColumn
        section={section}
        items={section === "recent" ? filterAndDedupeRecentTracksForActivity(items, recentCompletionFilter, items.length, likedTrackIdsForDisplay, likedReleaseTrackIdsForDisplay, { likedOnly: recentLikedOnly, taggedOnly: recentTaggedOnly }) : items}
        available={available}
        emptyCopy={section === "recent" && (recentCompletionFilter !== "all" || recentLikedOnly || recentTaggedOnly) ? "No songs match these activity filters." : emptyCopy}
        unavailableCopy={unavailableCopy}
        unavailableAction={unavailableAction}
        paged={section === "recent" ? false : paged}
        presorted={presorted}
        trackRankingMode={trackRankingMode}
        likedTrackIds={likedTrackIdsForDisplay}
        likedReleaseTrackIds={likedReleaseTrackIdsForDisplay}
        releaseTrackSiblingById={releaseTrackSiblingById}
        sectionPage={sectionPages[section]}
        moveSectionPage={moveSectionPage}
        visibleItems={visibleItems}
        renderDashboardListCard={renderDashboardListCard}
      />
    );
  }

  function setIssueReviewState(issueKey: string, state: IdentityAuditIssueReviewState | null) {
    setIdentityAuditIssueReviewState((current) => {
      const next = { ...current };
      if (state) {
        next[issueKey] = state;
      } else {
        delete next[issueKey];
      }
      return next;
    });
  }

  function setIssueExpanded(issueKey: string, expanded: boolean) {
    setIdentityAuditExpandedIssueKeys((current) => {
      if (expanded) {
        return { ...current, [issueKey]: true };
      }
      const next = { ...current };
      delete next[issueKey];
      return next;
    });
  }

  function trackDiagnosticIssue(
    kind: "possible_duplicate" | "ambiguous_mapping" | "suspicious_split" | "family_concern",
    example: TrackIdentityAuditExample,
    index: number,
  ): NormalizedAuditIssue {
    const variantItems = kind === "suspicious_split"
      ? auditList(example.source_tracks)
      : kind === "family_concern"
        ? auditList(example.release_tracks)
        : auditList(example.variants);
    const confidence = auditNumber(example.confidence);
    const affected = auditNumber(example.spotify_track_id_count)
      ?? auditNumber(example.source_track_count)
      ?? auditNumber(example.release_track_count)
      ?? variantItems.length;
    const typeLabel = kind === "possible_duplicate"
      ? "Possible duplicate"
      : kind === "ambiguous_mapping"
        ? "Ambiguous mapping"
        : kind === "suspicious_split"
          ? "Suspicious split"
          : "Grouping concern";
    const whyFlagged = kind === "possible_duplicate"
      ? "Same normalized track/artist appears across multiple Spotify identities."
      : kind === "ambiguous_mapping"
        ? "Current source evidence can point at more than one mapping outcome."
        : kind === "suspicious_split"
          ? "Multiple source rows are folded into one release track."
          : "Multiple release tracks are grouped for analysis and need review context.";
    return {
      key: `${kind}-${identityAuditTitle(example)}-${index}`,
      typeLabel,
      entityLabel: identityAuditTitle(example),
      whyFlagged,
      evidenceSummary: identityAuditMeta(example) || `${variantItems.length} evidence rows`,
      confidenceLabel: confidence != null ? `${Math.round(confidence * 100)}%` : "needs review",
      confidenceScore: confidence,
      severityLabel: issueSeverityForCount(affected),
      affectedCount: affected,
      affectedScore: typeof affected === "number" ? affected : 0,
      reviewStatus: "unreviewed",
      isResolved: false,
      isBlocked: false,
      suggestedAction: kind === "family_concern" ? "Review queue" : "Inspect evidence",
      onOpenMapping: () => {
        setAlbumCatalogLookupQ(identityAuditTitle(example));
        setTrackIdentityAuditTab("mapping");
      },
      onReview: kind === "family_concern" ? () => setTrackIdentityAuditTab("review_queue") : undefined,
      details: renderIdentityAuditExample(example, index),
    };
  }

  function updateLocalReviewDecision(
    entryId: string,
    patch: Partial<LocalReviewDecision>,
  ) {
    setIdentityAuditLocalDecisions((current) => {
      const existing = current[entryId] ?? {
        verdict: "unsure" as LocalReviewVerdict,
        grouping_target: null,
        note: "",
        updated_at_ms: Date.now(),
      };
      return {
        ...current,
        [entryId]: {
          ...existing,
          ...patch,
          updated_at_ms: Date.now(),
        },
      };
    });
  }

  function releaseAlbumMergeReadinessLabel(value: string | null | undefined): string {
    return String(value ?? "unknown").replace(/_/g, " ");
  }

  function albumMergeReadinessTone(value: string | null | undefined): string {
    if (value === "safe_candidate") {
      return "rgba(46, 204, 113, 0.18)";
    }
    if (value === "needs_review") {
      return "rgba(241, 196, 15, 0.18)";
    }
    return "rgba(231, 76, 60, 0.18)";
  }

  function renderAlbumMergeReadinessBadge(value: string | null | undefined) {
    return (
      <span
        style={{
          alignSelf: "flex-start",
          background: albumMergeReadinessTone(value),
          borderRadius: "999px",
          display: "inline-flex",
          fontSize: "12px",
          fontWeight: 700,
          padding: "4px 10px",
          textTransform: "capitalize",
        }}
      >
        {releaseAlbumMergeReadinessLabel(value)}
      </span>
    );
  }

  function summarizeAlbumMergeWarnings(preview: ReleaseAlbumMergePreviewResponse | undefined): string | null {
    if (!preview) {
      return null;
    }
    if (preview.warnings.length > 0) {
      return preview.warnings[0];
    }
    if (preview.readiness_reasons.length > 0 && preview.merge_readiness !== "safe_candidate") {
      return preview.readiness_reasons[0];
    }
    return null;
  }

  function spotifyAlbumUrl(spotifyAlbumId: string | null | undefined): string {
    return spotifyAlbumId ? `https://open.spotify.com/album/${spotifyAlbumId}` : "";
  }

  function plainEnglishAlbumMergeExplanation(preview: ReleaseAlbumMergePreviewResponse | undefined): string | null {
    if (!preview) {
      return null;
    }
    const reasons = preview.readiness_reasons;
    if (preview.merge_readiness === "safe_candidate") {
      return "Safe candidate because the album name and primary artist align, there is strong single Spotify album evidence, and no album-track conflicts were found.";
    }
    if (preview.merge_readiness === "unsafe") {
      if (reasons.some((reason) => reason.includes("different normalized album names"))) {
        return "Unsafe because the normalized album names do not match, so these rows may represent different releases even if Spotify evidence overlaps.";
      }
      if (reasons.some((reason) => reason.includes("different normalized primary artists"))) {
        return "Unsafe because the normalized primary artists do not match.";
      }
      if (reasons.some((reason) => reason.includes("not found"))) {
        return "Unsafe because one or more of the requested release albums could not be found.";
      }
      return `Unsafe because ${reasons[0] ?? "the local release identity signals do not agree enough for a safe merge."}`;
    }
    if (reasons.some((reason) => reason.includes("collide"))) {
      return "Needs review because the album rows overlap but some album-track rows would collide and need deduping.";
    }
    if (reasons.some((reason) => reason.includes("Multiple distinct Spotify album IDs"))) {
      return "Needs review because the local rows share name or artist signals, but multiple Spotify album IDs are involved.";
    }
    if (reasons.some((reason) => reason.includes("No strong single Spotify album evidence"))) {
      return "Needs review because there is not enough strong single-Spotify evidence to treat this as a low-risk merge.";
    }
    return `Needs review because ${reasons[0] ?? "the duplicate rows still need manual inspection."}`;
  }

  function albumMergeReasonKey(preview: ReleaseAlbumMergePreviewResponse | undefined): string {
    if (!preview) {
      return "not_previewed";
    }
    const reasons = preview.readiness_reasons;
    if (preview.merge_readiness === "safe_candidate") {
      return "safe_clean";
    }
    if (reasons.some((reason) => reason.includes("different normalized album names"))) {
      return "name_mismatch";
    }
    if (reasons.some((reason) => reason.includes("different normalized primary artists"))) {
      return "artist_mismatch";
    }
    if (reasons.some((reason) => reason.includes("not found"))) {
      return "missing_album";
    }
    if (reasons.some((reason) => reason.includes("collide"))) {
      return "album_track_conflicts";
    }
    if (reasons.some((reason) => reason.includes("Multiple distinct Spotify album IDs"))) {
      return "multiple_spotify_ids";
    }
    if (reasons.some((reason) => reason.includes("No strong single Spotify album evidence"))) {
      return "weak_spotify_evidence";
    }
    return "other";
  }

  function albumMergeReasonLabel(reasonKey: string): string {
    const labels: Record<string, string> = {
      album_track_conflicts: "Album-track conflicts",
      artist_mismatch: "Artist mismatch",
      missing_album: "Missing album row",
      multiple_spotify_ids: "Multiple Spotify IDs",
      name_mismatch: "Name mismatch",
      not_previewed: "Not previewed",
      other: "Other reason",
      safe_clean: "Clean safety checks",
      weak_spotify_evidence: "Weak Spotify evidence",
    };
    return labels[reasonKey] ?? reasonKey.replace(/_/g, " ");
  }

  function albumReasonFilterMatch(preview: ReleaseAlbumMergePreviewResponse | undefined, reasonFilter: string): boolean {
    if (reasonFilter === "all") {
      return true;
    }
    return albumMergeReasonKey(preview) === reasonFilter;
  }

  function albumReasonOptionsForTargets(
    targets: Array<{ target: AlbumMergeReviewTarget }>,
    readinessFilter: "all" | "not_previewed" | "safe_candidate" | "needs_review" | "unsafe",
  ): Array<{ key: string; label: string; count: number }> {
    const counts = new Map<string, number>();
    for (const { target } of targets) {
      const preview = releaseAlbumMergePreviewByKey[target.key];
      if (!albumPreviewFilterMatch(preview, readinessFilter)) {
        continue;
      }
      const reasonKey = albumMergeReasonKey(preview);
      counts.set(reasonKey, (counts.get(reasonKey) ?? 0) + 1);
    }
    return Array.from(counts.entries())
      .map(([key, count]) => ({ key, label: albumMergeReasonLabel(key), count }))
      .sort((left, right) => left.label.localeCompare(right.label));
  }

  function albumPreviewFilterMatch(
    preview: ReleaseAlbumMergePreviewResponse | undefined,
    filter: "all" | "not_previewed" | "safe_candidate" | "needs_review" | "unsafe",
  ): boolean {
    if (filter === "all") {
      return true;
    }
    if (filter === "not_previewed") {
      return !preview;
    }
    return preview?.merge_readiness === filter;
  }

  async function previewAlbumMergeTargetsInSession(targets: AlbumMergeReviewTarget[]) {
    const nextPreviews: Record<string, ReleaseAlbumMergePreviewResponse> = {};
    const nextErrors: Record<string, string> = {};
    for (const target of targets) {
      if (releaseAlbumMergePreviewByKey[target.key] || nextPreviews[target.key]) {
        continue;
      }
      try {
        nextPreviews[target.key] = await postReleaseAlbumMergePreview(target.releaseAlbumIds);
      } catch (error) {
        nextErrors[target.key] = formatUiErrorMessage(error, "Failed to preview release album merge.");
      }
    }
    if (Object.keys(nextPreviews).length > 0) {
      setReleaseAlbumMergePreviewByKey((current) => ({ ...current, ...nextPreviews }));
    }
    if (Object.keys(nextErrors).length > 0) {
      setReleaseAlbumMergePreviewErrorByKey((current) => ({ ...current, ...nextErrors }));
    }
  }

  function renderAlbumDuplicateCard(
    target: AlbumMergeReviewTarget,
    rows: Array<{
      release_album_id: number;
      release_album_name: string;
      artist_name: string;
      spotify_album_id?: string | null;
      spotify_album_name?: string | null;
    }>,
    extraMeta: ReactNode,
  ) {
    const preview = releaseAlbumMergePreviewByKey[target.key];
    const dryRun = releaseAlbumMergeDryRunByKey[target.key];
    const previewError = releaseAlbumMergePreviewErrorByKey[target.key];
    const dryRunError = releaseAlbumMergeDryRunErrorByKey[target.key];
    const warningSummary = summarizeAlbumMergeWarnings(preview) ?? target.warningSummary ?? null;
    return (
      <AlbumDuplicateMergeCard
        target={target}
        rows={rows}
        extraMeta={extraMeta}
        preview={preview}
        dryRun={dryRun}
        previewError={previewError}
        dryRunError={dryRunError}
        warningSummary={warningSummary}
        previewLoadingKey={releaseAlbumMergePreviewLoadingKey}
        dryRunLoadingKey={releaseAlbumMergeDryRunLoadingKey}
        spotifyAlbumUrl={spotifyAlbumUrl}
        albumMergeReasonLabel={albumMergeReasonLabel}
        albumMergeReasonKey={albumMergeReasonKey}
        plainEnglishAlbumMergeExplanation={plainEnglishAlbumMergeExplanation}
        renderAlbumMergeReadinessBadge={renderAlbumMergeReadinessBadge}
        renderReleaseAlbumMergePreview={renderReleaseAlbumMergePreview}
        onPreviewMerge={(reviewTarget) => {
          setSelectedAlbumMergeReviewKey(reviewTarget.key);
          setAlbumIdentityAuditTab("merge_review");
          void loadReleaseAlbumMergePreview(reviewTarget.key, reviewTarget.releaseAlbumIds);
        }}
        onDryRunMerge={(reviewTarget, survivorReleaseAlbumId) => {
          setSelectedAlbumMergeReviewKey(reviewTarget.key);
          setAlbumIdentityAuditTab("merge_review");
          void loadReleaseAlbumMergeDryRun(reviewTarget.key, reviewTarget.releaseAlbumIds, survivorReleaseAlbumId);
        }}
      />
    );
  }

  function collectAlbumMergeReviewTargets(): AlbumMergeReviewTarget[] {
    const spotifyTargets = (albumDuplicateLookupResult?.items ?? []).map((group) => ({
      key: releaseAlbumMergePreviewKey(`spotify:${group.spotify_album_id}`, group.release_albums),
      title: group.spotify_album_name ?? group.release_albums[0]?.release_album_name ?? "Duplicate album group",
      subtitle: `Spotify album ${group.spotify_album_id}`,
      releaseAlbumIds: group.release_albums.map((item) => item.release_album_id),
      duplicateCount: group.duplicate_count,
      sourceLabel: "Duplicate Spotify IDs",
      spotifyAlbumId: group.spotify_album_id,
      spotifyAlbumName: group.spotify_album_name,
    }));
    const nameTargets = (albumNameDuplicateLookupResult?.items ?? []).map((group) => ({
      key: releaseAlbumMergePreviewKey(`name:${group.normalized_album_name}:${group.normalized_primary_artist}`, group.release_albums),
      title: group.release_albums[0]?.release_album_name ?? group.normalized_album_name,
      subtitle: `${group.normalized_primary_artist} · ${group.spotify_album_ids.length > 0 ? group.spotify_album_ids.join(", ") : "No Spotify album ID"}`,
      releaseAlbumIds: group.release_albums.map((item) => item.release_album_id),
      duplicateCount: group.duplicate_count,
      sourceLabel: "Duplicate Name + Artist",
      spotifyAlbumId: group.spotify_album_ids.length === 1 ? group.spotify_album_ids[0] : null,
      spotifyAlbumName: group.release_albums[0]?.spotify_album_name ?? null,
    }));
    return [...spotifyTargets, ...nameTargets];
  }

  function renderTrackIdentityAuditOverviewTab() {
    const canonicalCount = identityAudit?.same_name_canonical_splits.length ?? 0;
    const releaseCount = identityAudit?.release_track_source_splits.length ?? 0;
    const compositionCount = identityAudit?.analysis_track_groups.length ?? 0;
    const suggestedCount = identityAuditSuggestedGroups?.summary.total_groups ?? 0;
    const ambiguousCount = identityAuditAmbiguous?.summary.total_review_entries ?? 0;
    return (
      <TrackIdentityAuditOverviewCards
        canonicalCount={canonicalCount}
        releaseCount={releaseCount}
        compositionCount={compositionCount}
        suggestedCount={suggestedCount}
        ambiguousCount={ambiguousCount}
      />
    );
  }

  function renderTrackIdentityAuditCanonicalTab() {
    if (identityAuditError) {
      return <p className="empty-copy">{identityAuditError}</p>;
    }
    if (!identityAudit) {
      return <p className="empty-copy">{identityAuditLoading ? "Loading canonical splits..." : "Canonical splits are not loaded yet."}</p>;
    }
    return (
      <div className="identity-audit-grid">
        <p className="identity-audit-tab-copy">Canonical checks show where same-name entities still split across Spotify track IDs.</p>
        {renderIdentityAuditGroup("Canonical Splits", identityAudit.same_name_canonical_splits)}
      </div>
    );
  }

  function releaseAlbumMergePreviewKey(prefix: string, releaseAlbums: Array<{ release_album_id: number }>) {
    return `${prefix}:${releaseAlbums.map((item) => item.release_album_id).sort((a, b) => a - b).join(",")}`;
  }

  async function loadReleaseAlbumMergePreview(key: string, releaseAlbumIds: number[]) {
    if (releaseAlbumMergePreviewLoadingKey) {
      return;
    }
    setSelectedAlbumMergeReviewKey(key);
    setReleaseAlbumMergePreviewLoadingKey(key);
    setReleaseAlbumMergePreviewErrorByKey((current) => ({ ...current, [key]: "" }));
    try {
      const preview = await postReleaseAlbumMergePreview(releaseAlbumIds);
      setReleaseAlbumMergePreviewByKey((current) => ({ ...current, [key]: preview }));
    } catch (error) {
      setReleaseAlbumMergePreviewErrorByKey((current) => ({
        ...current,
        [key]: formatUiErrorMessage(error, "Failed to preview release album merge."),
      }));
    } finally {
      setReleaseAlbumMergePreviewLoadingKey(null);
    }
  }

  async function loadReleaseAlbumMergeDryRun(key: string, releaseAlbumIds: number[], survivorReleaseAlbumId: number) {
    if (releaseAlbumMergeDryRunLoadingKey) {
      return;
    }
    setSelectedAlbumMergeReviewKey(key);
    setReleaseAlbumMergeDryRunLoadingKey(key);
    setReleaseAlbumMergeDryRunErrorByKey((current) => ({ ...current, [key]: "" }));
    try {
      const dryRun = await postReleaseAlbumMergeDryRun(releaseAlbumIds, survivorReleaseAlbumId);
      setReleaseAlbumMergeDryRunByKey((current) => ({ ...current, [key]: dryRun }));
    } catch (error) {
      setReleaseAlbumMergeDryRunErrorByKey((current) => ({
        ...current,
        [key]: formatUiErrorMessage(error, "Failed to dry run release album merge."),
      }));
    } finally {
      setReleaseAlbumMergeDryRunLoadingKey(null);
    }
  }

  function renderReleaseAlbumMergeDryRun(key: string) {
    const dryRun = releaseAlbumMergeDryRunByKey[key];
    const error = releaseAlbumMergeDryRunErrorByKey[key];
    if (error) {
      return <p className="empty-copy">{error}</p>;
    }
    if (!dryRun) {
      return null;
    }
    return (
      <div style={{ marginTop: "12px" }}>
        <div className="tracks-formula-heading">
          <h3>Dry Run Plan</h3>
          <span>{dryRun.blocked ? "blocked" : "ready"}</span>
        </div>
        {dryRun.blocked_reasons.map((reason) => (
          <p className="empty-copy" key={`release-album-dry-run-blocked-${key}-${reason}`}>{reason}</p>
        ))}
        <p className="empty-copy">
          Rows: source maps {dryRun.rows_affected.source_album_map ?? 0}, artist inserts {dryRun.rows_affected.album_artist_insert ?? 0}, artist deletes {dryRun.rows_affected.album_artist_delete ?? 0}, track repoints {dryRun.rows_affected.album_track_repoint ?? 0}, track conflicts {dryRun.rows_affected.album_track_conflict_delete ?? 0}, retired albums {dryRun.rows_affected.release_album_retire ?? 0}
        </p>
        {dryRun.statements.map((statement) => (
          <p className="empty-copy" key={`release-album-dry-run-statement-${key}-${statement}`}>{statement}</p>
        ))}
        {Object.entries(dryRun.plan).map(([planKey, rows]) => (
          rows.length > 0 ? (
            <details key={`release-album-dry-run-plan-${key}-${planKey}`} open={planKey === "album_track_conflicts"}>
              <summary>{planKey.replace(/_/g, " ")} ({rows.length})</summary>
              <pre style={{ whiteSpace: "pre-wrap", overflowX: "auto" }}>{JSON.stringify(rows, null, 2)}</pre>
            </details>
          ) : null
        ))}
        <details>
          <summary>Raw dry run JSON</summary>
          <pre style={{ whiteSpace: "pre-wrap", overflowX: "auto" }}>{JSON.stringify(dryRun, null, 2)}</pre>
        </details>
      </div>
    );
  }

  function renderReleaseAlbumMergePreview(key: string) {
    const preview = releaseAlbumMergePreviewByKey[key];
    const error = releaseAlbumMergePreviewErrorByKey[key];
    if (error) {
      return <p className="empty-copy">{error}</p>;
    }
    if (!preview) {
      return null;
    }
    const readinessLabel = preview.merge_readiness.replace(/_/g, " ");
    return (
      <div className="identity-audit-group" style={{ marginTop: "12px" }}>
        <div className="tracks-formula-heading">
          <h3>Merge Preview</h3>
          <span>{readinessLabel} · Survivor {preview.survivor_release_album_id ?? "None"}</span>
        </div>
        {preview.readiness_reasons.length > 0 ? (
          <div>
            {preview.readiness_reasons.map((reason) => (
              <p className="empty-copy" key={`release-album-merge-readiness-${key}-${reason}`}>{reason}</p>
            ))}
          </div>
        ) : null}
        {preview.warnings.length > 0 ? (
          <div>
            {preview.warnings.map((warning) => (
              <p className="empty-copy" key={`release-album-merge-warning-${key}-${warning}`}>{warning}</p>
            ))}
          </div>
        ) : null}
        {plainEnglishAlbumMergeExplanation(preview) ? (
          <p className="identity-audit-tab-copy" style={{ marginTop: 0 }}>{plainEnglishAlbumMergeExplanation(preview)}</p>
        ) : null}
        <p className="empty-copy">
          Affected rows: source album maps {preview.affected.source_album_map_rows}, album artists {preview.affected.album_artist_rows}, release tracks {preview.affected.release_track_rows}, album tracks {preview.affected.album_track_rows}, album-track conflicts {preview.affected.album_track_conflicts}, raw listens {preview.affected.raw_play_event_rows}
        </p>
        <ul>
          {preview.proposed_operations.map((operation) => (
            <li key={`release-album-merge-operation-${key}-${operation}`}>{operation}</li>
          ))}
        </ul>
        {preview.survivor_release_album_id !== null ? (() => {
          const survivorReleaseAlbumId = preview.survivor_release_album_id;
          return (
            <button
              className="track-ranking-chip"
              type="button"
              disabled={releaseAlbumMergeDryRunLoadingKey !== null}
              onClick={() => void loadReleaseAlbumMergeDryRun(
                key,
                [survivorReleaseAlbumId, ...preview.merge_release_album_ids],
                survivorReleaseAlbumId,
              )}
            >
              {releaseAlbumMergeDryRunLoadingKey === key ? "Loading..." : "Dry run"}
            </button>
          );
        })() : null}
        <details>
          <summary>Raw preview JSON</summary>
          <pre style={{ whiteSpace: "pre-wrap", overflowX: "auto" }}>{JSON.stringify(preview, null, 2)}</pre>
        </details>
        {renderReleaseAlbumMergeDryRun(key)}
      </div>
    );
  }

  function renderTrackIdentityAuditReleaseTab() {
    const releaseTrackSplits = identityAudit?.release_track_source_splits ?? [];
    return (
      <div className="identity-audit-grid">
        <p className="identity-audit-tab-copy">
          Release review highlights candidates that likely belong to one release identity but are split today.
        </p>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Duplicate Tracks (Same Spotify Track ID)</h3>
            <span>{trackDuplicateLookupResult?.total ?? 0} groups</span>
          </div>
          {trackDuplicateLookupError ? <p className="empty-copy">{trackDuplicateLookupError}</p> : null}
          {!trackDuplicateLookupResult && trackDuplicateLookupLoading ? (
            <p className="empty-copy">Loading duplicate track groups...</p>
          ) : null}
          {!trackDuplicateLookupLoading && (!trackDuplicateLookupResult || trackDuplicateLookupResult.items.length === 0) ? (
            <p className="empty-copy">No duplicate tracks found.</p>
          ) : null}
          {trackDuplicateLookupResult && trackDuplicateLookupResult.items.length > 0 ? (
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Spotify Track</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Spotify Name</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Duration</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Release Track</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Artist</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Release Album</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Spotify Album</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Catalog</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Queue</th>
                    <th style={{ textAlign: "left", padding: "8px", fontSize: "12px" }}>Dup Count</th>
                  </tr>
                </thead>
                <tbody>
                  {trackDuplicateLookupResult.items.map((group) =>
                    group.release_tracks.map((item, index) => (
                      <tr key={`identity-release-dup-track-${group.spotify_track_id}-${item.release_track_id}`}>
                        <td style={{ padding: "8px", verticalAlign: "top", wordBreak: "break-word" }}>{index === 0 ? group.spotify_track_id : ""}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{index === 0 ? (group.spotify_track_name ?? "Unknown") : ""}</td>
                        <td style={{ padding: "8px", verticalAlign: "top", whiteSpace: "nowrap" }}>{index === 0 ? (group.duration_display ?? "Unknown") : ""}</td>
                        <td style={{ padding: "8px", verticalAlign: "top", fontWeight: 600 }}>{item.release_track_name}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{item.artist_name}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{item.release_album_name}</td>
                        <td style={{ padding: "8px", verticalAlign: "top", wordBreak: "break-word" }}>{item.spotify_album_id ?? "Unknown"}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{item.catalog_status ?? "unknown"}</td>
                        <td style={{ padding: "8px", verticalAlign: "top" }}>{item.queue_status}</td>
                        <td style={{ padding: "8px", verticalAlign: "top", whiteSpace: "nowrap" }}>{index === 0 ? group.duplicate_count : ""}</td>
                      </tr>
                    )),
                  )}
                </tbody>
              </table>
            </div>
          ) : null}
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Release Track Split Signals</h3>
            <span>{releaseTrackSplits.length} examples</span>
          </div>
          <p className="identity-audit-tab-copy">
            These examples show source-track rows collapsing into one release track and can indicate additional release-level cleanup opportunities.
          </p>
          {identityAuditError ? <p className="empty-copy">{identityAuditError}</p> : null}
          {!identityAudit && identityAuditLoading ? <p className="empty-copy">Loading release track splits...</p> : null}
          {identityAudit && releaseTrackSplits.length > 0 ? renderIdentityAuditGroup("Release Track Splits", releaseTrackSplits) : null}
          {identityAudit && releaseTrackSplits.length === 0 ? <p className="empty-copy">No release track split examples returned.</p> : null}
        </div>
      </div>
    );
  }

  function renderTrackIdentityAuditMappingTab() {
    const sourceMapCounts = trackMappingLineageResult?.source_release.map_counts ?? [];
    const familyMapCounts = trackMappingLineageResult?.release_family.map_counts ?? [];
    const sourceReleaseGroups = (trackMappingLineageResult?.source_release.groups ?? [])
      .map((group) => ({
        ...group,
        sources: group.sources.filter((source) => (
          trackMappingConfirmationFilter === "all"
            || (trackMappingConfirmationFilter === "confirmed" && source.is_user_confirmed)
            || (trackMappingConfirmationFilter === "unconfirmed" && !source.is_user_confirmed)
        )),
      }))
      .filter((group) => group.sources.length > 0);
    const releaseFamilyGroups = (trackMappingLineageResult?.release_family.groups ?? [])
      .map((group) => ({
        ...group,
        release_tracks: group.release_tracks.filter((track) => (
          trackMappingConfirmationFilter === "all"
            || (trackMappingConfirmationFilter === "confirmed" && track.is_user_confirmed)
            || (trackMappingConfirmationFilter === "unconfirmed" && !track.is_user_confirmed)
        )),
      }))
      .filter((group) => group.release_tracks.length > 0);
    const showSourceReleaseGroups = trackMappingKindFilter === "all" || trackMappingKindFilter === "source_release";
    const showReleaseFamilyGroups = trackMappingKindFilter === "all" || trackMappingKindFilter === "release_family";
    const formatMappingCounts = (items: Array<{ status: string; is_user_confirmed: boolean; count: number }>) => (
      items.length > 0
        ? items.map((item) => `${item.status || "unknown"} ${item.is_user_confirmed ? "confirmed" : "unconfirmed"}: ${item.count}`).join(" · ")
        : "No map rows"
    );
    const confirmationPreviewLabel = (preview: TrackMappingConfirmationPreview) => {
      if (preview.readiness === "safe_candidate") {
        return "confirm candidate";
      }
      if (preview.readiness === "needs_review") {
        return "needs review";
      }
      if (preview.readiness === "unsafe") {
        return "not confirmable";
      }
      return preview.readiness;
    };

    return (
      <div className="identity-audit-grid">
        <p className="identity-audit-tab-copy">
          Inspect current track lineage from track family to local release track to Spotify source track. This view is read-only.
        </p>
        <div className="identity-audit-ambiguous-toolbar">
          <label>
            Query
            <input
              onChange={(event) => setAlbumCatalogLookupQ(event.target.value)}
              placeholder="Track, artist, album, family, or Spotify track id"
              type="text"
              value={albumCatalogLookupQ}
            />
          </label>
          <label>
            Mapping type
            <select
              onChange={(event) => {
                const nextValue = event.target.value as "all" | "source_release" | "release_family";
                setTrackMappingKindFilter(nextValue);
                void loadTrackMappingLineage(true, nextValue, trackMappingSourceMetadataFilter, trackMappingCertaintyFilter);
              }}
              value={trackMappingKindFilter}
            >
              <option value="all">All mapping types</option>
              <option value="source_release">Source to release</option>
              <option value="release_family">Release to family</option>
            </select>
          </label>
          <label>
            Confirmation
            <select
              onChange={(event) => setTrackMappingConfirmationFilter(event.target.value as "all" | "confirmed" | "unconfirmed")}
              value={trackMappingConfirmationFilter}
            >
              <option value="all">All confirmations</option>
              <option value="confirmed">Confirmed only</option>
              <option value="unconfirmed">Unconfirmed only</option>
            </select>
          </label>
          <label>
            Source metadata
            <select
              onChange={(event) => {
                const nextValue = event.target.value as "all" | "complete" | "incomplete";
                setTrackMappingSourceMetadataFilter(nextValue);
                void loadTrackMappingLineage(true, trackMappingKindFilter, nextValue, trackMappingCertaintyFilter);
              }}
              value={trackMappingSourceMetadataFilter}
            >
              <option value="all">All source rows</option>
              <option value="complete">All complete</option>
              <option value="incomplete">Needs metadata</option>
            </select>
          </label>
          <label>
            Certainty
            <select
              onChange={(event) => {
                const nextValue = event.target.value as "all" | "certain" | "uncertain";
                setTrackMappingCertaintyFilter(nextValue);
                void loadTrackMappingLineage(true, trackMappingKindFilter, trackMappingSourceMetadataFilter, nextValue);
              }}
              value={trackMappingCertaintyFilter}
            >
              <option value="all">All certainty</option>
              <option value="certain">Certain</option>
              <option value="uncertain">Uncertain</option>
            </select>
          </label>
          <button
            className="primary-button"
            disabled={trackMappingLineageLoading}
            onClick={() => void loadTrackMappingLineage(true)}
            type="button"
          >
            {trackMappingLineageLoading ? "Loading..." : "Search"}
          </button>
        </div>
        {trackMappingLineageError ? <p className="empty-copy">{trackMappingLineageError}</p> : null}
        {!trackMappingLineageResult && trackMappingLineageLoading ? <p className="empty-copy">Loading track mapping...</p> : null}
        {trackMappingLineageResult ? (
          <div className="tracks-only-summary">
            <span>Source to release groups: {trackMappingLineageResult.source_release.total_is_exact === false ? `${trackMappingLineageResult.source_release.groups.length}${trackMappingLineageResult.source_release.has_more ? "+" : ""} loaded` : trackMappingLineageResult.source_release.total}</span>
            <span>Release to family groups: {trackMappingLineageResult.release_family.total}</span>
            <span>Source map counts: {formatMappingCounts(sourceMapCounts)}</span>
            <span>Family map counts: {formatMappingCounts(familyMapCounts)}</span>
            {trackMappingLineageLastLoadedAt ? <span>Loaded {new Date(trackMappingLineageLastLoadedAt).toLocaleTimeString()}</span> : null}
          </div>
        ) : null}
        {trackMappingLineageResult && showSourceReleaseGroups && sourceReleaseGroups.length > 0 ? (
          <div className="identity-audit-group">
            <div className="tracks-formula-heading">
              <h3>Lineage: Release Track With Source Evidence</h3>
              <span>{sourceReleaseGroups.length}</span>
            </div>
            <div className="identity-audit-review-list">
              {sourceReleaseGroups.map((group) => (
                <details className="source-release-track-group" key={`identity-source-release-${group.release_track_id}`} open>
                  <summary className="source-release-track-header">
                    <div>
                      <span className="identity-audit-type-badge">Release Track</span>
                      <h4>
                        <button
                          className="detail-modal-inline-link"
                          onClick={() => openTrackMappingSourceReleasePreview(group)}
                          type="button"
                        >
                          {group.release_track_name}
                        </button>
                      </h4>
                      <p className="empty-copy">{group.artist_name} · {group.release_album_name}</p>
                      <p className="empty-copy">Track Family: not loaded in this Source to release slice</p>
                    </div>
                    <div style={{ display: "flex", gap: "8px", flexWrap: "wrap", justifyContent: "flex-end" }}>
                      <span className="track-ranking-chip">{group.source_count} sources</span>
                      <span className="track-ranking-chip">
                        {group.all_source_metadata_complete ? "metadata complete" : `${group.source_metadata_incomplete_count} need metadata`}
                      </span>
                      <span className={`track-ranking-chip source-release-confirmation-chip source-release-confirmation-${group.confirmation_preview.readiness}`}>
                        {confirmationPreviewLabel(group.confirmation_preview)}
                      </span>
                      <span className="track-ranking-chip">{group.all_source_metadata_complete ? "catalog ready" : "unresolved catalog data"}</span>
                    </div>
                  </summary>
                  <div className="source-release-source-list">
                    {group.sources.map((source) => (
                      <div className="source-release-source-row" key={`identity-source-release-${group.release_track_id}-${source.source_track_id}`}>
                        <div className="source-release-source-main">
                          <span className="source-release-source-kind">Spotify Source Track</span>
                          <button
                            className="detail-modal-inline-link source-release-track-name-button"
                            onClick={() => openTrackMappingSourcePreview(group, source)}
                            type="button"
                          >
                            <strong>{source.spotify_track_name ?? source.source_name_raw ?? "Unknown track"}</strong>
                          </button>
                          <span className="source-release-id">{source.external_id ?? "No external ID"}</span>
                        </div>
                        <div className="source-release-source-stats">
                          <span>{source.metadata_complete ? "metadata complete" : "metadata incomplete"}</span>
                          <span>{source.is_user_confirmed ? "manually reviewed" : "unconfirmed"}</span>
                          <span>{source.play_count} plays</span>
                        </div>
                        <details className="source-release-source-album">
                          <summary>
                            <strong>{source.album_name_display ?? source.album_name ?? "Unknown album"}</strong>
                            <span>{source.duration_display ?? "Unknown"} · pos {source.disc_number ?? "?"}.{source.track_number ?? "?"}</span>
                          </summary>
                          <div className="identity-audit-stats" style={{ marginTop: "8px" }}>
                            <span className="identity-audit-stat"><span>Album ID</span><strong>{source.album_id ?? "No album ID"}</strong></span>
                            <span className="identity-audit-stat"><span>Release date</span><strong>{source.album_release_date ?? "No date"}</strong></span>
                            <span className="identity-audit-stat"><span>Total tracks</span><strong>{source.album_total_tracks ?? "Unknown"}</strong></span>
                            <span className="identity-audit-stat"><span>Metadata gaps</span><strong>{source.metadata_gaps.join(", ") || "None"}</strong></span>
                          </div>
                        </details>
                      </div>
                    ))}
                  </div>
                </details>
              ))}
            </div>
          </div>
        ) : null}
        {trackMappingLineageResult && showReleaseFamilyGroups && releaseFamilyGroups.length > 0 ? (
          <div className="identity-audit-group">
            <div className="tracks-formula-heading">
              <h3>Lineage: Track Family With Release Tracks</h3>
              <span>{releaseFamilyGroups.length}</span>
            </div>
            <div className="identity-audit-review-list">
              {releaseFamilyGroups.map((group) => (
                <details className="identity-audit-review-card" key={`identity-release-family-${group.analysis_track_id}`} open>
                  <summary className="identity-audit-review-card-header">
                    <div>
                      <span className="identity-audit-type-badge">Track Family</span>
                      <h4>{group.track_family_name}</h4>
                      {group.grouping_note ? <p className="empty-copy">{group.grouping_note}</p> : null}
                    </div>
                    <span className="track-ranking-chip">{group.release_count} releases</span>
                  </summary>
                  <div className="source-release-source-list">
                    {group.release_tracks.map((track) => (
                      <div className="source-release-source-row" key={`identity-release-family-${group.analysis_track_id}-${track.release_track_id}`}>
                        <div className="source-release-source-main">
                          <button
                            className="detail-modal-inline-link source-release-track-name-button"
                            onClick={() => openTrackMappingReleasePreview(track)}
                            type="button"
                          >
                            <strong>{track.release_track_name}</strong>
                          </button>
                          <span>{track.artist_name}</span>
                        </div>
                        <div className="source-release-source-album">
                          <strong>{track.release_album_name}</strong>
                          <span>{track.source_count} sources · {track.play_count} plays</span>
                        </div>
                        <div className="source-release-source-map">
                          <span>{track.match_method}</span>
                          <span>{track.confidence !== null ? track.confidence.toFixed(2) : "no confidence"}</span>
                          <span>{track.is_user_confirmed ? "manually reviewed" : "unconfirmed"}</span>
                          <span>{track.status || "review status unknown"}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </details>
              ))}
            </div>
          </div>
        ) : null}
        {trackMappingLineageResult && (
          (!showSourceReleaseGroups || sourceReleaseGroups.length === 0)
          && (!showReleaseFamilyGroups || releaseFamilyGroups.length === 0)
        ) ? (
          <p className="empty-copy">No track mapping groups match the active filters.</p>
        ) : null}
      </div>
    );
  }

  function renderAlbumIdentityAuditOverviewTab() {
    const mergeTargets = collectAlbumMergeReviewTargets();
    const previewedCount = mergeTargets.filter((target) => releaseAlbumMergePreviewByKey[target.key]).length;
    const dryRunCount = mergeTargets.filter((target) => releaseAlbumMergeDryRunByKey[target.key]).length;
    return (
      <AlbumIdentityAuditOverviewCards
        duplicateAlbumCount={albumDuplicateLookupResult?.total ?? 0}
        duplicateNameArtistCount={albumNameDuplicateLookupResult?.total ?? 0}
        previewedCount={previewedCount}
        dryRunCount={dryRunCount}
      />
    );
  }

  function renderAlbumIdentityAuditSpotifyDuplicatesTab() {
    const allTargets = (albumDuplicateLookupResult?.items ?? []).map((group) => ({
      group,
      target: {
        key: releaseAlbumMergePreviewKey(`spotify:${group.spotify_album_id}`, group.release_albums),
        title: group.spotify_album_name ?? group.release_albums[0]?.release_album_name ?? "Duplicate album group",
        subtitle: `Spotify album ${group.spotify_album_id}`,
        releaseAlbumIds: group.release_albums.map((item) => item.release_album_id),
        duplicateCount: group.duplicate_count,
        sourceLabel: "Duplicate Spotify IDs",
        spotifyAlbumId: group.spotify_album_id,
        spotifyAlbumName: group.spotify_album_name,
      } satisfies AlbumMergeReviewTarget,
    }));
    const reasonOptions = albumReasonOptionsForTargets(allTargets, albumSpotifyDuplicateFilter);
    const targets = allTargets.filter(({ target }) => {
      const preview = releaseAlbumMergePreviewByKey[target.key];
      return albumPreviewFilterMatch(preview, albumSpotifyDuplicateFilter)
        && albumReasonFilterMatch(preview, albumSpotifyDuplicateReasonFilter);
    });
    return (
      <div className="identity-audit-grid">
        <p className="identity-audit-tab-copy">
          Review duplicate albums that already resolve to the same Spotify album ID.
        </p>
        <div style={{ alignItems: "center", display: "flex", flexWrap: "wrap", gap: "8px" }}>
          {[
            ["all", "All"],
            ["not_previewed", "Not previewed"],
            ["safe_candidate", "Safe candidate"],
            ["needs_review", "Needs review"],
            ["unsafe", "Unsafe"],
          ].map(([value, label]) => (
            <button
              className={`track-ranking-chip${albumSpotifyDuplicateFilter === value ? " track-ranking-chip-active" : ""}`}
              key={`album-spotify-filter-${value}`}
              onClick={() => {
                setAlbumSpotifyDuplicateFilter(value as typeof albumSpotifyDuplicateFilter);
                setAlbumSpotifyDuplicateReasonFilter("all");
              }}
              type="button"
            >
              {label}
            </button>
          ))}
          <button
            className="secondary-button"
            disabled={albumSpotifyDuplicateBulkPreviewLoading || !targets.some(({ target }) => !releaseAlbumMergePreviewByKey[target.key])}
            onClick={() => {
              void (async () => {
                setAlbumSpotifyDuplicateBulkPreviewLoading(true);
                try {
                  await previewAlbumMergeTargetsInSession(targets.map((item) => item.target));
                } finally {
                  setAlbumSpotifyDuplicateBulkPreviewLoading(false);
                }
              })();
            }}
            type="button"
          >
            {albumSpotifyDuplicateBulkPreviewLoading ? "Previewing..." : "Preview listed groups"}
          </button>
        </div>
        {reasonOptions.length > 0 ? (
          <div style={{ alignItems: "center", display: "flex", flexWrap: "wrap", gap: "8px" }}>
            <span className="empty-copy" style={{ margin: 0 }}>Reasons</span>
            <button
              className={`track-ranking-chip${albumSpotifyDuplicateReasonFilter === "all" ? " track-ranking-chip-active" : ""}`}
              onClick={() => setAlbumSpotifyDuplicateReasonFilter("all")}
              type="button"
            >
              All reasons
            </button>
            {reasonOptions.map((option) => (
              <button
                className={`track-ranking-chip${albumSpotifyDuplicateReasonFilter === option.key ? " track-ranking-chip-active" : ""}`}
                key={`album-spotify-reason-filter-${option.key}`}
                onClick={() => setAlbumSpotifyDuplicateReasonFilter(option.key)}
                type="button"
              >
                {option.label} ({option.count})
              </button>
            ))}
          </div>
        ) : null}
        {albumDuplicateLookupError ? <p className="empty-copy">{albumDuplicateLookupError}</p> : null}
        {!albumDuplicateLookupResult && albumDuplicateLookupLoading ? <p className="empty-copy">Loading duplicate album groups...</p> : null}
        {!albumDuplicateLookupLoading && (!albumDuplicateLookupResult || targets.length === 0) ? <p className="empty-copy">No duplicate albums found for this filter.</p> : null}
        {targets.length ? (
          <div style={{ display: "grid", gap: "16px" }}>
            {targets.map(({ group, target }) => {
              return renderAlbumDuplicateCard(
                target,
                group.release_albums,
                <div style={{ display: "flex", flexWrap: "wrap", gap: "8px" }}>
                  <span className="identity-audit-stat">
                    <span>Spotify album ID</span>
                    <strong style={{ fontFamily: "monospace" }}>
                      <a href={spotifyAlbumUrl(group.spotify_album_id)} rel="noreferrer" target="_blank">{group.spotify_album_id}</a>
                    </strong>
                  </span>
                  <span className="identity-audit-stat">
                    <span>Spotify name</span>
                    <strong>{group.spotify_album_name ?? "Unknown"}</strong>
                  </span>
                </div>,
              );
            })}
          </div>
        ) : null}
      </div>
    );
  }

  function renderAlbumIdentityAuditNameDuplicatesTab() {
    const allTargets = (albumNameDuplicateLookupResult?.items ?? []).map((group) => ({
      group,
      target: {
        key: releaseAlbumMergePreviewKey(`name:${group.normalized_album_name}:${group.normalized_primary_artist}`, group.release_albums),
        title: group.release_albums[0]?.release_album_name ?? group.normalized_album_name,
        subtitle: `${group.normalized_primary_artist} · ${group.spotify_album_ids.length > 0 ? group.spotify_album_ids.join(", ") : "No Spotify album ID"}`,
        releaseAlbumIds: group.release_albums.map((item) => item.release_album_id),
        duplicateCount: group.duplicate_count,
        sourceLabel: "Duplicate Name + Artist",
        spotifyAlbumId: group.spotify_album_ids.length === 1 ? group.spotify_album_ids[0] : null,
        spotifyAlbumName: group.release_albums[0]?.spotify_album_name ?? null,
      } satisfies AlbumMergeReviewTarget,
    }));
    const subgroupTargets = allTargets.filter(({ group }) => {
      const subgroupMatch = albumNameDuplicateGroupFilter === "all"
        || (albumNameDuplicateGroupFilter === "single_spotify_id" && group.spotify_album_ids.length === 1)
        || (albumNameDuplicateGroupFilter === "multiple_spotify_ids" && group.spotify_album_ids.length > 1)
        || (albumNameDuplicateGroupFilter === "no_spotify_id" && group.spotify_album_ids.length === 0);
      return subgroupMatch;
    });
    const reasonOptions = albumReasonOptionsForTargets(subgroupTargets, albumNameDuplicatePreviewFilter);
    const targets = subgroupTargets.filter(({ target }) => {
      const preview = releaseAlbumMergePreviewByKey[target.key];
      return albumPreviewFilterMatch(preview, albumNameDuplicatePreviewFilter)
        && albumReasonFilterMatch(preview, albumNameDuplicateReasonFilter);
    });
    return (
      <div className="identity-audit-grid">
        <p className="identity-audit-tab-copy">
          Review album duplicates grouped by normalized album name and normalized primary artist.
        </p>
        <div style={{ alignItems: "center", display: "flex", flexWrap: "wrap", gap: "8px" }}>
          {[
            ["all", "All"],
            ["single_spotify_id", "Single Spotify ID"],
            ["multiple_spotify_ids", "Multiple Spotify IDs"],
            ["no_spotify_id", "No Spotify ID"],
          ].map(([value, label]) => (
            <button
              className={`track-ranking-chip${albumNameDuplicateGroupFilter === value ? " track-ranking-chip-active" : ""}`}
              key={`album-name-group-filter-${value}`}
              onClick={() => {
                setAlbumNameDuplicateGroupFilter(value as typeof albumNameDuplicateGroupFilter);
                setAlbumNameDuplicateReasonFilter("all");
              }}
              type="button"
            >
              {label}
            </button>
          ))}
        </div>
        <div style={{ alignItems: "center", display: "flex", flexWrap: "wrap", gap: "8px" }}>
          {[
            ["all", "All previews"],
            ["not_previewed", "Not previewed"],
            ["safe_candidate", "Safe candidate"],
            ["needs_review", "Needs review"],
            ["unsafe", "Unsafe"],
          ].map(([value, label]) => (
            <button
              className={`track-ranking-chip${albumNameDuplicatePreviewFilter === value ? " track-ranking-chip-active" : ""}`}
              key={`album-name-preview-filter-${value}`}
              onClick={() => {
                setAlbumNameDuplicatePreviewFilter(value as typeof albumNameDuplicatePreviewFilter);
                setAlbumNameDuplicateReasonFilter("all");
              }}
              type="button"
            >
              {label}
            </button>
          ))}
          <button
            className="secondary-button"
            disabled={albumNameDuplicateBulkPreviewLoading || !targets.some(({ target }) => !releaseAlbumMergePreviewByKey[target.key])}
            onClick={() => {
              void (async () => {
                setAlbumNameDuplicateBulkPreviewLoading(true);
                try {
                  await previewAlbumMergeTargetsInSession(targets.map((item) => item.target));
                } finally {
                  setAlbumNameDuplicateBulkPreviewLoading(false);
                }
              })();
            }}
            type="button"
          >
            {albumNameDuplicateBulkPreviewLoading ? "Previewing..." : "Preview listed groups"}
          </button>
        </div>
        {reasonOptions.length > 0 ? (
          <div style={{ alignItems: "center", display: "flex", flexWrap: "wrap", gap: "8px" }}>
            <span className="empty-copy" style={{ margin: 0 }}>Reasons</span>
            <button
              className={`track-ranking-chip${albumNameDuplicateReasonFilter === "all" ? " track-ranking-chip-active" : ""}`}
              onClick={() => setAlbumNameDuplicateReasonFilter("all")}
              type="button"
            >
              All reasons
            </button>
            {reasonOptions.map((option) => (
              <button
                className={`track-ranking-chip${albumNameDuplicateReasonFilter === option.key ? " track-ranking-chip-active" : ""}`}
                key={`album-name-reason-filter-${option.key}`}
                onClick={() => setAlbumNameDuplicateReasonFilter(option.key)}
                type="button"
              >
                {option.label} ({option.count})
              </button>
            ))}
          </div>
        ) : null}
        {albumNameDuplicateLookupError ? <p className="empty-copy">{albumNameDuplicateLookupError}</p> : null}
        {!albumNameDuplicateLookupResult && albumNameDuplicateLookupLoading ? <p className="empty-copy">Loading duplicate album name groups...</p> : null}
        {!albumNameDuplicateLookupLoading && (!albumNameDuplicateLookupResult || targets.length === 0) ? <p className="empty-copy">No duplicate album name groups found for this filter.</p> : null}
        {targets.length ? (
          <div style={{ display: "grid", gap: "16px" }}>
            {targets.map(({ group, target }) => {
              return renderAlbumDuplicateCard(
                target,
                group.release_albums,
                <div style={{ display: "flex", flexWrap: "wrap", gap: "8px" }}>
                  <span className="identity-audit-stat">
                    <span>Normalized album</span>
                    <strong>{group.normalized_album_name}</strong>
                  </span>
                  <span className="identity-audit-stat">
                    <span>Normalized artist</span>
                    <strong>{group.normalized_primary_artist}</strong>
                  </span>
                  <span className="identity-audit-stat">
                    <span>Spotify album IDs</span>
                    <strong>{group.spotify_album_ids.length > 0 ? group.spotify_album_ids.join(", ") : "None"}</strong>
                  </span>
                </div>,
              );
            })}
          </div>
        ) : null}
      </div>
    );
  }

  function renderAlbumIdentityAuditMergeReviewTab() {
    const targets = collectAlbumMergeReviewTargets();
    return (
      <AlbumIdentityAuditMergeReviewTab
        targets={targets}
        selectedAlbumMergeReviewKey={selectedAlbumMergeReviewKey}
        releaseAlbumMergePreviewByKey={releaseAlbumMergePreviewByKey}
        releaseAlbumMergeDryRunByKey={releaseAlbumMergeDryRunByKey}
        renderAlbumMergeReadinessBadge={renderAlbumMergeReadinessBadge}
        renderReleaseAlbumMergePreview={renderReleaseAlbumMergePreview}
        onSelectTarget={setSelectedAlbumMergeReviewKey}
      />
    );
  }

  function renderTrackIdentityAuditCompositionTab() {
    if (identityAuditError) {
      return <p className="empty-copy">{identityAuditError}</p>;
    }
    if (!identityAudit) {
      return <p className="empty-copy">{identityAuditLoading ? "Loading composition examples..." : "Composition examples are not loaded yet."}</p>;
    }
    return (
      <div className="identity-audit-grid">
        <p className="identity-audit-tab-copy">
          Composition checks show currently grouped release tracks.
        </p>
        {renderIdentityAuditGroup("Current Composition Groups", identityAudit.analysis_track_groups)}
      </div>
    );
  }

  function renderTrackIdentityAuditAmbiguousTab() {
    return (
      <TrackIdentityAuditAmbiguousTab
        computeAmbiguousTrackItems={computeAmbiguousTrackItems}
        computeUnifiedReviewItems={computeUnifiedReviewItems}
        dryRunIdentityAuditSavedSubmission={dryRunIdentityAuditSavedSubmission}
        findNextUnreviewedDecisionKey={findNextUnreviewedDecisionKey}
        groupDecisionKey={groupDecisionKey}
        identityAuditAmbiguous={identityAuditAmbiguous}
        identityAuditAmbiguousBucketFilter={identityAuditAmbiguousBucketFilter}
        identityAuditAmbiguousError={identityAuditAmbiguousError}
        identityAuditAmbiguousFamilyFilter={identityAuditAmbiguousFamilyFilter}
        identityAuditAmbiguousLoading={identityAuditAmbiguousLoading}
        identityAuditAmbiguousVisibleCount={identityAuditAmbiguousVisibleCount}
        identityAuditFocusedReviewKey={identityAuditFocusedReviewKey}
        identityAuditLocalDecisions={identityAuditLocalDecisions}
        identityAuditPreviewCopyStatus={identityAuditPreviewCopyStatus}
        identityAuditPreviewValidatedAt={identityAuditPreviewValidatedAt}
        identityAuditPreviewValidationError={identityAuditPreviewValidationError}
        identityAuditPreviewValidationLoading={identityAuditPreviewValidationLoading}
        identityAuditPreviewValidationResult={identityAuditPreviewValidationResult}
        identityAuditSavedSubmissionDetail={identityAuditSavedSubmissionDetail}
        identityAuditSavedSubmissionDetailError={identityAuditSavedSubmissionDetailError}
        identityAuditSavedSubmissionDetailLoading={identityAuditSavedSubmissionDetailLoading}
        identityAuditSavedSubmissionDryRun={identityAuditSavedSubmissionDryRun}
        identityAuditSavedSubmissionDryRunAt={identityAuditSavedSubmissionDryRunAt}
        identityAuditSavedSubmissionDryRunError={identityAuditSavedSubmissionDryRunError}
        identityAuditSavedSubmissionDryRunLoading={identityAuditSavedSubmissionDryRunLoading}
        identityAuditSavedSubmissions={identityAuditSavedSubmissions}
        identityAuditSavedSubmissionsError={identityAuditSavedSubmissionsError}
        identityAuditSavedSubmissionsLoading={identityAuditSavedSubmissionsLoading}
        identityAuditSubmissionSaveError={identityAuditSubmissionSaveError}
        identityAuditSubmissionSaveLoading={identityAuditSubmissionSaveLoading}
        identityAuditSubmissionSaveResult={identityAuditSubmissionSaveResult}
        identityAuditSuggestedError={identityAuditSuggestedError}
        identityAuditSuggestedGroups={identityAuditSuggestedGroups}
        identityAuditSuggestedLoading={identityAuditSuggestedLoading}
        isReviewedDecision={isReviewedDecision}
        loadIdentityAuditSavedSubmissions={loadIdentityAuditSavedSubmissions}
        setIdentityAuditAmbiguousBucketFilter={setIdentityAuditAmbiguousBucketFilter}
        setIdentityAuditAmbiguousFamilyFilter={setIdentityAuditAmbiguousFamilyFilter}
        setIdentityAuditAmbiguousVisibleCount={setIdentityAuditAmbiguousVisibleCount}
        setIdentityAuditFocusedReviewKey={setIdentityAuditFocusedReviewKey}
        setIdentityAuditLocalDecisions={setIdentityAuditLocalDecisions}
        setIdentityAuditPreviewCopyStatus={setIdentityAuditPreviewCopyStatus}
        setIdentityAuditPreviewValidatedAt={setIdentityAuditPreviewValidatedAt}
        setIdentityAuditPreviewValidationError={setIdentityAuditPreviewValidationError}
        setIdentityAuditPreviewValidationLoading={setIdentityAuditPreviewValidationLoading}
        setIdentityAuditPreviewValidationResult={setIdentityAuditPreviewValidationResult}
        setIdentityAuditSubmissionSaveError={setIdentityAuditSubmissionSaveError}
        setIdentityAuditSubmissionSaveLoading={setIdentityAuditSubmissionSaveLoading}
        setIdentityAuditSubmissionSaveResult={setIdentityAuditSubmissionSaveResult}
        trackDecisionKey={trackDecisionKey}
        updateLocalReviewDecision={updateLocalReviewDecision}
        viewIdentityAuditSavedSubmission={viewIdentityAuditSavedSubmission}
      />
    );
  }

  function renderTrackIdentityAuditProblemsTab() {
    const canonicalIssues = (identityAudit?.same_name_canonical_splits ?? [])
      .map((example, index) => trackDiagnosticIssue("possible_duplicate", example, index));
    const releaseIssues = (identityAudit?.release_track_source_splits ?? [])
      .map((example, index) => trackDiagnosticIssue("suspicious_split", example, index));
    const familyIssues = (identityAudit?.analysis_track_groups ?? [])
      .map((example, index) => trackDiagnosticIssue("family_concern", example, index));
    const duplicateIssues: NormalizedAuditIssue[] = (trackDuplicateLookupResult?.items ?? []).map((group, index) => ({
      key: `duplicate-track-${group.spotify_track_id}-${index}`,
      typeLabel: "Possible duplicate",
      entityLabel: group.spotify_track_name ?? group.release_tracks[0]?.release_track_name ?? "Duplicate track",
      whyFlagged: "One Spotify track is connected to multiple local release tracks.",
      evidenceSummary: `${group.duplicate_count} release tracks share ${group.spotify_track_id}`,
      confidenceLabel: "high",
      confidenceScore: 1,
      severityLabel: issueSeverityForCount(group.duplicate_count, "high"),
      affectedCount: group.duplicate_count,
      affectedScore: group.duplicate_count,
      reviewStatus: "needs review",
      isResolved: false,
      isBlocked: group.release_tracks.some((track) => (track.catalog_status ?? "").toLowerCase() !== "backfilled"),
      suggestedAction: "Inspect release rows",
      onOpenMapping: () => {
        setAlbumCatalogLookupQ(group.spotify_track_name ?? group.release_tracks[0]?.release_track_name ?? group.spotify_track_id);
        setTrackIdentityAuditTab("mapping");
      },
      onReview: () => setTrackIdentityAuditTab("review_queue"),
      details: (
        <div className="identity-audit-variant-list">
          {group.release_tracks.map((track) => (
            <div className="identity-audit-variant" key={`duplicate-track-issue-${group.spotify_track_id}-${track.release_track_id}`}>
              <div className="identity-audit-variant-main">
                <strong>{track.release_track_name}</strong>
                <span>{track.artist_name} | {track.release_album_name}</span>
                <code>release {track.release_track_id}</code>
              </div>
              <div className="identity-audit-variant-stats">
                <span>{track.catalog_status ?? "catalog unknown"}</span>
                <span>{track.queue_status}</span>
              </div>
            </div>
          ))}
        </div>
      ),
    }));
    const metadataIssue: NormalizedAuditIssue | null = trackMappingLineageResult
      ? {
        key: "metadata-blockers",
        typeLabel: "Missing metadata",
        entityLabel: "Source mapping evidence",
        whyFlagged: "Some mapping rows may need source album or track metadata before review.",
        evidenceSummary: `${trackMappingLineageResult.source_release.total} source mapping groups loaded`,
        confidenceLabel: trackMappingSourceMetadataFilter === "incomplete" ? "focused" : "unknown",
        confidenceScore: null,
        severityLabel: "medium",
        affectedCount: trackMappingLineageResult.source_release.total,
        affectedScore: trackMappingLineageResult.source_release.total,
        reviewStatus: "inspect mapping",
        isResolved: false,
        isBlocked: trackMappingSourceMetadataFilter === "incomplete",
        suggestedAction: "Open Mapping",
        onOpenMapping: () => setTrackIdentityAuditTab("mapping"),
        details: (
          <button className="secondary-button" onClick={() => setTrackIdentityAuditTab("mapping")} type="button">
            Open Mapping
          </button>
        ),
      }
      : null;
    const issues = [
      ...duplicateIssues,
      ...canonicalIssues,
      ...releaseIssues,
      ...familyIssues,
      ...(metadataIssue ? [metadataIssue] : []),
    ];
    return (
      <div className="identity-audit-grid">
        <p className="identity-audit-tab-copy">
          Find track identity problems first. Each issue uses the same review shape, with full diagnostics tucked into expandable evidence.
        </p>
        {renderTrackIdentityAuditOverviewTab()}
        {identityAuditError ? <p className="empty-copy">{identityAuditError}</p> : null}
        {!identityAudit && identityAuditLoading ? <p className="empty-copy">Loading issue examples...</p> : null}
        {trackDuplicateLookupError ? <p className="empty-copy">{trackDuplicateLookupError}</p> : null}
        {!trackDuplicateLookupResult && trackDuplicateLookupLoading ? <p className="empty-copy">Loading duplicate track issues...</p> : null}
        <IssueFeed
          emptyCopy="No track identity issues returned."
          expandedIssueKeys={identityAuditExpandedIssueKeys}
          issues={issues}
          resetIssueState={() => {
            setIdentityAuditIssueReviewState({});
            setIdentityAuditExpandedIssueKeys({});
          }}
          reviewState={identityAuditIssueReviewState}
          setIssueExpanded={setIssueExpanded}
          setIssueReviewState={setIssueReviewState}
          setSort={setTrackIdentityAuditIssueSort}
          sort={trackIdentityAuditIssueSort}
          title="Track Issue Feed"
        />
        <details className="identity-audit-group">
          <summary>
            <strong>Advanced diagnostic tables</strong>
          </summary>
          {renderTrackIdentityAuditCanonicalTab()}
          {renderTrackIdentityAuditReleaseTab()}
          {renderTrackIdentityAuditCompositionTab()}
        </details>
      </div>
    );
  }

  function renderAlbumIdentityAuditProblemsTab() {
    const spotifyDuplicateIssues: NormalizedAuditIssue[] = (albumDuplicateLookupResult?.items ?? []).map((group, index) => {
      const target: AlbumMergeReviewTarget = {
        key: releaseAlbumMergePreviewKey(`spotify:${group.spotify_album_id}`, group.release_albums),
        title: group.spotify_album_name ?? group.release_albums[0]?.release_album_name ?? "Duplicate album group",
        subtitle: `Spotify album ${group.spotify_album_id}`,
        releaseAlbumIds: group.release_albums.map((item) => item.release_album_id),
        duplicateCount: group.duplicate_count,
        sourceLabel: "Duplicate Spotify IDs",
        spotifyAlbumId: group.spotify_album_id,
        spotifyAlbumName: group.spotify_album_name,
      };
      const preview = releaseAlbumMergePreviewByKey[target.key];
      return {
        key: `album-spotify-issue-${group.spotify_album_id}-${index}`,
        typeLabel: "Possible duplicate",
        entityLabel: target.title,
        whyFlagged: "Multiple local albums resolve to one Spotify album.",
        evidenceSummary: `${group.duplicate_count} local albums share ${group.spotify_album_id}`,
        confidenceLabel: preview ? releaseAlbumMergeReadinessLabel(preview.merge_readiness) : "needs preview",
        confidenceScore: preview?.merge_readiness === "safe_candidate" ? 1 : preview?.merge_readiness === "needs_review" ? 0.6 : preview ? 0.2 : null,
        severityLabel: issueSeverityForCount(group.duplicate_count, "high"),
        affectedCount: group.duplicate_count,
        affectedScore: group.duplicate_count,
        reviewStatus: preview ? "previewed" : "unreviewed",
        isResolved: Boolean(preview),
        isBlocked: Boolean(preview && preview.merge_readiness === "unsafe"),
        suggestedAction: preview?.merge_readiness === "safe_candidate" ? "Dry run merge" : "Preview merge",
        onReview: () => {
          setSelectedAlbumMergeReviewKey(target.key);
          setAlbumIdentityAuditTab("merge_review");
        },
        details: renderAlbumDuplicateCard(
          target,
          group.release_albums,
          <div className="identity-audit-stats">
            <span className="identity-audit-stat"><span>Spotify album</span><strong>{group.spotify_album_id}</strong></span>
            <span className="identity-audit-stat"><span>Spotify name</span><strong>{group.spotify_album_name ?? "Unknown"}</strong></span>
          </div>,
        ),
      };
    });
    const nameDuplicateIssues: NormalizedAuditIssue[] = (albumNameDuplicateLookupResult?.items ?? []).map((group, index) => {
      const target: AlbumMergeReviewTarget = {
        key: releaseAlbumMergePreviewKey(`name:${group.normalized_album_name}:${group.normalized_primary_artist}`, group.release_albums),
        title: group.release_albums[0]?.release_album_name ?? group.normalized_album_name,
        subtitle: `${group.normalized_primary_artist} · ${group.spotify_album_ids.length > 0 ? group.spotify_album_ids.join(", ") : "No Spotify album ID"}`,
        releaseAlbumIds: group.release_albums.map((item) => item.release_album_id),
        duplicateCount: group.duplicate_count,
        sourceLabel: "Duplicate Name + Artist",
        spotifyAlbumId: group.spotify_album_ids.length === 1 ? group.spotify_album_ids[0] : null,
        spotifyAlbumName: group.release_albums[0]?.spotify_album_name ?? null,
      };
      const preview = releaseAlbumMergePreviewByKey[target.key];
      return {
        key: `album-name-issue-${group.normalized_album_name}-${group.normalized_primary_artist}-${index}`,
        typeLabel: group.spotify_album_ids.length > 1 ? "Conflicting Spotify mappings" : "Possible duplicate",
        entityLabel: target.title,
        whyFlagged: "Albums share normalized title and primary artist.",
        evidenceSummary: `${group.duplicate_count} albums | ${group.spotify_album_ids.length || "no"} Spotify IDs`,
        confidenceLabel: preview ? releaseAlbumMergeReadinessLabel(preview.merge_readiness) : "needs preview",
        confidenceScore: preview?.merge_readiness === "safe_candidate" ? 1 : preview?.merge_readiness === "needs_review" ? 0.6 : preview ? 0.2 : null,
        severityLabel: group.spotify_album_ids.length > 1 ? "high" : issueSeverityForCount(group.duplicate_count),
        affectedCount: group.duplicate_count,
        affectedScore: group.duplicate_count,
        reviewStatus: preview ? "previewed" : "unreviewed",
        isResolved: Boolean(preview),
        isBlocked: group.spotify_album_ids.length > 1 || Boolean(preview && preview.merge_readiness === "unsafe"),
        suggestedAction: "Preview merge",
        onReview: () => {
          setSelectedAlbumMergeReviewKey(target.key);
          setAlbumIdentityAuditTab("merge_review");
        },
        details: renderAlbumDuplicateCard(
          target,
          group.release_albums,
          <div className="identity-audit-stats">
            <span className="identity-audit-stat"><span>Normalized album</span><strong>{group.normalized_album_name}</strong></span>
            <span className="identity-audit-stat"><span>Normalized artist</span><strong>{group.normalized_primary_artist}</strong></span>
          </div>,
        ),
      };
    });
    const issues = [...spotifyDuplicateIssues, ...nameDuplicateIssues];
    return (
      <div className="identity-audit-grid">
        <p className="identity-audit-tab-copy">
          Find album identity problems first. Duplicate ID and duplicate name signals share one issue feed; merge actions stay in Merge Review.
        </p>
        {renderAlbumIdentityAuditOverviewTab()}
        {albumDuplicateLookupError ? <p className="empty-copy">{albumDuplicateLookupError}</p> : null}
        {albumNameDuplicateLookupError ? <p className="empty-copy">{albumNameDuplicateLookupError}</p> : null}
        {albumDuplicateLookupLoading || albumNameDuplicateLookupLoading ? <p className="empty-copy">Loading album issues...</p> : null}
        <IssueFeed
          emptyCopy="No album identity issues returned."
          expandedIssueKeys={identityAuditExpandedIssueKeys}
          issues={issues}
          resetIssueState={() => {
            setIdentityAuditIssueReviewState({});
            setIdentityAuditExpandedIssueKeys({});
          }}
          reviewState={identityAuditIssueReviewState}
          setIssueExpanded={setIssueExpanded}
          setIssueReviewState={setIssueReviewState}
          setSort={setAlbumIdentityAuditIssueSort}
          sort={albumIdentityAuditIssueSort}
          title="Album Issue Feed"
        />
        <details className="identity-audit-group">
          <summary>
            <strong>Advanced duplicate views</strong>
          </summary>
          {renderAlbumIdentityAuditSpotifyDuplicatesTab()}
          {renderAlbumIdentityAuditNameDuplicatesTab()}
        </details>
        <details className="identity-audit-group">
          <summary>
            <strong>Catalog blockers and operational status</strong>
          </summary>
          <p className="identity-audit-tab-copy">
            Catalog completeness and queue state now live under Albums {"->"} Catalog so duplicate review is not mixed with operational lookup.
          </p>
          <button
            className="secondary-button"
            onClick={() => setAlbumIdentityAuditTab("catalog")}
            type="button"
          >
            Open Catalog
          </button>
        </details>
      </div>
    );
  }

  function renderAlbumIdentityAuditCatalogTab() {
    return (
      <AlbumIdentityAuditCatalogTab
        catalogBackfillCoverage={catalogBackfillCoverage}
        catalogBackfillCoverageLoading={catalogBackfillCoverageLoading}
        catalogBackfillCoverageError={catalogBackfillCoverageError}
        catalogBackfillCoverageLastLoadedAt={catalogBackfillCoverageLastLoadedAt}
        onOpenAlbumLookup={() => {
          setSearchLookupEntityType("albums");
          setAppPage("searchLookup");
        }}
        onOpenCatalogBackfill={() => setAppPage("catalogBackfill")}
        onRefreshCatalogSummary={() => void loadCatalogBackfillCoverage(true)}
      />
    );
  }

  function renderIdentityAuditPage() {
    if (!profile) {
      return null;
    }

    return (
      <IdentityAuditPage
        identityAuditLoading={identityAuditLoading}
        identityAuditSuggestedLoading={identityAuditSuggestedLoading}
        identityAuditAmbiguousLoading={identityAuditAmbiguousLoading}
        identityAuditLimit={identityAudit?.limit ?? null}
        suggestedGroupTotal={identityAuditSuggestedGroups?.summary.total_groups ?? 0}
        ambiguousReviewTotal={identityAuditAmbiguous?.summary.total_review_entries ?? 0}
        albumDuplicateLookupLoaded={albumDuplicateLookupLoaded}
        albumDuplicateTotal={albumDuplicateLookupResult?.total ?? 0}
        albumNameDuplicateLookupLoaded={albumNameDuplicateLookupLoaded}
        albumNameDuplicateTotal={albumNameDuplicateLookupResult?.total ?? 0}
        identityAuditLastLoadedAt={identityAuditLastLoadedAt}
        identityAuditSuggestedLastLoadedAt={identityAuditSuggestedLastLoadedAt}
        identityAuditAmbiguousLastLoadedAt={identityAuditAmbiguousLastLoadedAt}
        albumDuplicateLookupLastLoadedAt={albumDuplicateLookupLastLoadedAt}
        albumNameDuplicateLookupLastLoadedAt={albumNameDuplicateLookupLastLoadedAt}
        identityAuditEntityTab={identityAuditEntityTab}
        trackIdentityAuditTab={trackIdentityAuditTab}
        albumIdentityAuditTab={albumIdentityAuditTab}
        onReloadAll={() => {
          void loadIdentityAudit(true);
          void loadIdentityAuditSuggestedGroups(true);
          void loadIdentityAuditAmbiguousReview(true);
        }}
        onBackToDashboard={() => setAppPage("dashboard")}
        setIdentityAuditEntityTab={setIdentityAuditEntityTab}
        setTrackIdentityAuditTab={setTrackIdentityAuditTab}
        setAlbumIdentityAuditTab={setAlbumIdentityAuditTab}
        onOpenRecordingCandidateReleaseTrack={openRecordingCandidateReleaseTrack}
        renderTrackProblemsTab={renderTrackIdentityAuditProblemsTab}
        renderTrackMappingTab={renderTrackIdentityAuditMappingTab}
        renderTrackReviewQueueTab={renderTrackIdentityAuditAmbiguousTab}
        renderAlbumProblemsTab={renderAlbumIdentityAuditProblemsTab}
        renderAlbumMergeReviewTab={renderAlbumIdentityAuditMergeReviewTab}
        renderAlbumCatalogTab={renderAlbumIdentityAuditCatalogTab}
      />
    );
  }

  function handleCooldownRetry() {
    setReloadCooldownUntil(null);
    setReloadCooldownDurationMs(60_000);
    if (experienceMode === "local") {
      handleExperienceModeChange("full");
      return;
    }
    if (!profile) {
      void loadProfile();
      return;
    }
    void refreshRecentSection(recentRange);
  }

  async function loadFullAnalysis() {
    setAnalysisMode("full");
    await loadExtendedProfile(recentRange, "full");
  }

  async function recomputeHistoryFromLocal() {
    if (loadingHistoryRecompute || loadingExtendedProfile) {
      return;
    }
    setLoadingHistoryRecompute(true);
    setStatusMessage("Recomputing from full history...");
    try {
      const response = await fetch(`${apiBaseUrl}/cache/rebuild`, {
        method: "POST",
        credentials: "include",
      });
      if (!response.ok) {
        let detail = "Failed to recompute history.";
        try {
          const payload = (await response.json()) as { detail?: string };
          if (payload.detail) {
            detail = payload.detail;
          }
        } catch {
          // Keep fallback detail.
        }
        throw new Error(detail);
      }
      setAnalysisMode("full");
      await loadExtendedProfile(recentRange, "full");
      setStatusHistory((current) => [...current, "History recompute complete."]);
    } catch (error) {
      const message = formatUiErrorMessage(error, "Failed to recompute history.");
      setStatusMessage(message);
      setStatusHistory((current) => [...current, `History recompute error: ${message}`]);
    } finally {
      setLoadingHistoryRecompute(false);
    }
  }

  async function fetchRecentSections(targetRange: RecentRange, forceRecentSync = false): Promise<RecentSectionResponse> {
    if (Date.now() < recentSectionsRequestBlockedUntilMs) {
      const seconds = Math.max(1, Math.ceil((recentSectionsRequestBlockedUntilMs - Date.now()) / 1000));
      throw new Error(formatCooldownCopy(seconds));
    }
    if (recentSectionsRequestInFlight) {
      return recentSectionsRequestInFlight;
    }
    const endpoint = experienceMode === "local" ? "/me/local/recent" : "/me/recent";
    const params = new URLSearchParams({
      recent_range: targetRange,
      limit: String(RECENT_SECTION_FETCH_LIMIT),
    });
    if (forceRecentSync && experienceMode !== "local") {
      params.set("force_recent_sync", "true");
    }
    const request = (async () => {
      const response = await fetch(
        `${apiBaseUrl}${endpoint}?${params.toString()}`,
        {
          credentials: "include",
        },
      );
      if (!response.ok) {
        let detail = "Failed to refresh recent sections.";
        try {
          const payload = (await response.json()) as { detail?: string };
          if (payload.detail) {
            detail = payload.detail;
          }
        } catch {
          // ignore invalid error payloads
        }
        if (response.status === 429 && experienceMode === "full") {
          const cooldownSeconds = parseCooldownSeconds(detail) ?? 60;
          recentSectionsRequestBlockedUntilMs = Date.now() + cooldownSeconds * 1000;
          setReloadCooldownDurationMs(cooldownSeconds * 1000);
          setReloadCooldownUntil(recentSectionsRequestBlockedUntilMs);
          detail = formatCooldownCopy(cooldownSeconds);
        }
        throw new Error(detail);
      }
      return (await response.json()) as RecentSectionResponse;
    })();
    recentSectionsRequestInFlight = request;
    try {
      return await request;
    } finally {
      if (recentSectionsRequestInFlight === request) {
        recentSectionsRequestInFlight = null;
      }
    }
  }

  async function loadLikedTracksCache() {
    if (experienceMode !== "full") {
      return;
    }
    setLikedTracksLoadAttempted(true);
    setLikedTracksLoading(true);
    setLikedTracksError(null);
    try {
      const payload = await fetchAllLikedTracks(LIKED_TRACKS_FETCH_LIMIT);
      setLikedTracksCache(payload);
      if (payload.items.length > 0) {
        setProfile((current) => current
          ? {
              ...current,
              recent_likes_tracks: payload.items,
              recent_likes_available: true,
            }
          : current);
      }
    } catch (error) {
      setLikedTracksError(formatUiErrorMessage(error, "Failed to load liked tracks cache."));
    } finally {
      setLikedTracksLoading(false);
    }
  }

  function likedTracksSyncFailureMessage(result: { stopped_reason: string; errors?: string[] }) {
    const detail = result.errors?.find((entry) => entry.trim().length > 0) ?? "";
    switch (result.stopped_reason) {
      case "auth_error":
        return "Liked tracks refresh needs a valid Spotify session. Log out and log back in to reconnect Spotify.";
      case "forbidden":
      case "missing_scope":
        return "Liked tracks refresh needs Spotify library access. Log out and log back in to grant library access.";
      case "rate_limited":
        return "Spotify is rate-limiting liked tracks refresh right now. Try again later.";
      case "network_error":
        return detail || "Liked tracks refresh could not reach Spotify. Try again later.";
      case "parse_error":
      case "unexpected_response":
        return detail || "Liked tracks refresh received an unexpected Spotify response. Try again later.";
      default:
        return null;
    }
  }

  async function syncLikedTracks() {
    if (likedTracksSyncing) {
      return;
    }
    setLikedTracksSyncing(true);
    setLikedTracksError(null);
    setStatusMessage("Refreshing liked tracks...");
    try {
      const result = await postLikedTracksSync("full");
      const failureMessage = likedTracksSyncFailureMessage(result);
      if (failureMessage) {
        setLikedTracksError(failureMessage);
        setStatusMessage(failureMessage);
        setStatusHistory((current) => [...current, `Liked tracks refresh ${result.stopped_reason}: ${failureMessage}`]);
        return;
      }
      const payload = await fetchAllLikedTracks(LIKED_TRACKS_FETCH_LIMIT);
      setLikedTracksCache(payload);
      setProfile((current) => current
        ? {
            ...current,
            recent_likes_tracks: payload.items,
            recent_likes_available: payload.items.length > 0,
          }
        : current);
      setStatusMessage("");
      setStatusHistory((current) => [
        ...current,
        `Liked tracks refresh ${result.stopped_reason}: ${result.tracks_seen} seen, ${result.active_likes} active.`,
      ]);
    } catch (error) {
      const message = formatUiErrorMessage(error, "Failed to refresh liked tracks.");
      setLikedTracksError(message);
      setStatusMessage(message);
      setStatusHistory((current) => [...current, `Liked tracks refresh error: ${message}`]);
    } finally {
      setLikedTracksSyncing(false);
    }
  }

  async function loadPlayerRecentTracks() {
    setPlayerRecentTracksLoading(true);
    setPlayerRecentTracksError(null);
    try {
      const endpoint = experienceMode === "local" ? "/me/local/recent" : "/me/recent";
      const response = await fetch(
        `${apiBaseUrl}${endpoint}?recent_range=${encodeURIComponent(recentRange)}&limit=${encodeURIComponent(String(PLAYER_RECENT_FETCH_LIMIT))}`,
        {
          credentials: "include",
        },
      );
      if (!response.ok) {
        let detail = "Failed to load recently played songs.";
        try {
          const payload = (await response.json()) as { detail?: string };
          if (payload.detail) {
            detail = payload.detail;
          }
        } catch {
          // Keep fallback detail.
        }
        throw new Error(detail);
      }
      const data = (await response.json()) as RecentSectionResponse;
      let uniqueTracks = dedupeRecentTracksForPlayer(data.recent_tracks ?? []);
      let offset = 0;
      while (uniqueTracks.length < PLAYER_RECENT_FETCH_LIMIT && offset < 500) {
        const archive = await fetchListeningLog(50, offset, "all");
        if (archive.items.length === 0) {
          break;
        }
        uniqueTracks = dedupeRecentTracksForPlayer([...uniqueTracks, ...archive.items]);
        offset += archive.items.length;
        if (!archive.has_more) {
          break;
        }
      }
      setPlayerRecentTracks(uniqueTracks);
    } catch (error) {
      setPlayerRecentTracksError(formatUiErrorMessage(error, "Failed to load recently played songs."));
    } finally {
      setPlayerRecentTracksLoadAttempted(true);
      setPlayerRecentTracksLoading(false);
    }
  }

  async function loadPlayerQueueTracks() {
    if (experienceMode === "local") {
      setPlayerQueueTracks([]);
      setPlayerQueueGroups([]);
      setPlayerQueueGroupCursors({});
      setHomeQueueOpenGroupIds(new Set());
      setPlayerQueueCursor(null);
      setPlayerQueueSource(null);
      clearQueueContext();
      resetQueueControls();
      setQueuePlaylistUri(null);
      setPlayerQueueError(null);
      setPlayerQueueLoadAttempted(true);
      return;
    }
    setPlayerQueueLoading(true);
    setPlayerQueueError(null);
    try {
      const token = await fetchPlaybackToken();
      const response = await fetch("https://api.spotify.com/v1/me/player/queue", {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });
      if (!response.ok) {
        throw new Error(`Spotify queue request failed (${response.status}).`);
      }
      type SpotifyQueueItem = {
        type?: string | null;
        id?: string | null;
        name?: string | null;
        uri?: string | null;
        duration_ms?: number | null;
        album?: {
          id?: string | null;
          name?: string | null;
          images?: Array<{ url?: string | null }>;
        } | null;
        artists?: Array<{ name?: string | null }>;
      };
      const spotifyQueueItemToTrack = (item: SpotifyQueueItem | null | undefined): PlayerQueueTrack | null => {
        if (!item || (item.type !== "track" && !item.uri?.startsWith("spotify:track:"))) {
          return null;
        }
        const uri = trackUriWithFallback(item.uri, item.id ?? null);
        return {
          name: item.name ?? "Unknown track",
          artists: (item.artists ?? []).map((artist) => artist.name ?? "").filter(Boolean).join(", ") || "Unknown artist",
          album: item.album?.name ?? "Unknown album",
          image: item.album?.images?.find((image) => image.url)?.url ?? null,
          uri,
          durationMs: Math.max(0, Number(item.duration_ms ?? 0)),
          trackId: item.id ?? spotifyTrackIdFromUri(uri),
          albumId: item.album?.id ?? null,
        };
      };
      const displayTrackAsQueueTrack = playerDisplayTrack
        ? {
          ...playerDisplayTrack,
          trackId: spotifyTrackIdFromUri(playerDisplayTrack.uri),
          albumId: null,
        }
        : null;
      const payload = (await response.json()) as {
        currently_playing?: SpotifyQueueItem | null;
        queue?: SpotifyQueueItem[];
      };
      const currentQueueTrack = spotifyQueueItemToTrack(payload.currently_playing) ?? displayTrackAsQueueTrack;
      const queuedTracks = (payload.queue ?? [])
        .map(spotifyQueueItemToTrack)
        .filter((track): track is PlayerQueueTrack => Boolean(track));
      const collapsedQueuedTracks = collapseRepeatedQueueCycle(queuedTracks);
      let liveQueueTracks = [
        ...(currentQueueTrack ? [currentQueueTrack] : []),
        ...(queueRepeatsTrack(collapsedQueuedTracks, currentQueueTrack?.uri ?? playerDisplayTrack?.uri) ? [] : collapsedQueuedTracks),
      ]
        .slice(0, PLAYER_RECENT_FETCH_LIMIT);
      let expandedAlbumContext: { label: string; url?: string | null } | null = null;
      const queuePlaybackSnapshot = livePlaybackSnapshot;
      const contextAlbumId = queuePlaybackSnapshot?.context_type === "album"
        ? (
          queuePlaybackSnapshot.album_id
          ?? (queuePlaybackSnapshot.context_uri?.startsWith("spotify:album:")
            ? queuePlaybackSnapshot.context_uri.split(":")[2] ?? null
            : null)
        )
        : null;
      if (queuePlaybackSnapshot && contextAlbumId && currentQueueTrack?.trackId) {
        try {
          const params = new URLSearchParams({
            album_id: contextAlbumId,
            track_id: currentQueueTrack.trackId,
          });
          if (!spotifyCooldownActive) {
            params.set("force_spotify", "true");
          } else {
            params.set("local_only", "true");
          }
          const albumResponse = await fetch(`${apiBaseUrl}/auth/playback/album-tracks?${params.toString()}`, {
            credentials: "include",
          });
          if (albumResponse.ok) {
            const albumPayload = (await albumResponse.json()) as {
              items?: Array<{
                id?: string | null;
                name?: string | null;
                uri?: string | null;
                duration_ms?: number | null;
                artists?: TrackArtistEntry[];
              }>;
            };
            const albumQueueTracks = (albumPayload.items ?? []).map((item): PlayerQueueTrack | null => {
              const uri = trackUriWithFallback(item.uri ?? null, item.id ?? null);
              if (!uri) {
                return null;
              }
              return {
                name: item.name ?? "Unknown track",
                artists: (item.artists ?? []).map((artist) => artist.name ?? "").filter(Boolean).join(", ") || "Unknown artist",
                album: queuePlaybackSnapshot.album_name ?? currentQueueTrack.album,
                image: queuePlaybackSnapshot.image_url ?? currentQueueTrack.image,
                uri,
                durationMs: Math.max(0, Number(item.duration_ms ?? 0)),
                trackId: item.id ?? spotifyTrackIdFromUri(uri),
                albumId: contextAlbumId,
              };
            }).filter((track): track is PlayerQueueTrack => Boolean(track));
            const currentAlbumTrackIndex = albumQueueTracks.findIndex((track) => {
              const albumTrackId = track.trackId ?? spotifyTrackIdFromUri(track.uri);
              return Boolean(albumTrackId && albumTrackId === currentQueueTrack.trackId);
            });
            if (albumQueueTracks.length > 0 && currentAlbumTrackIndex >= 0) {
              // Album context is a fixed tracklist, not Spotify's rotated/wrapped queue.
              liveQueueTracks = albumQueueTracks;
              expandedAlbumContext = {
                label: queuePlaybackSnapshot.album_name ?? currentQueueTrack.album,
                url: queuePlaybackSnapshot.context_url ?? spotifyEntityUrl("album", contextAlbumId),
              };
            }
          }
        } catch {
          // Spotify's explicit queue remains usable when album expansion is unavailable.
        }
      }
      replacePlayerQueueTracks(liveQueueTracks, expandedAlbumContext ?? null, expandedAlbumContext ? undefined : "Spotify queue");
      setPlayerQueueCursor(currentQueueTrack ? 0 : null);
      setPlayerQueueSource("spotify");
      if (expandedAlbumContext) {
        setPlayerQueueContext(expandedAlbumContext);
      } else {
        clearQueueContext();
      }
      setPlayerQueueCleared(false);
      resetQueueControls();
    } catch (error) {
      setPlayerQueueTracks([]);
      setPlayerQueueGroups([]);
      setPlayerQueueGroupCursors({});
      setHomeQueueOpenGroupIds(new Set());
      setPlayerQueueCursor(null);
      setPlayerQueueSource(null);
      clearQueueContext();
      resetQueueControls();
      setPlayerQueueError(formatUiErrorMessage(error, "Failed to load Spotify queue."));
    } finally {
      setPlayerQueueLoadAttempted(true);
      setPlayerQueueLoading(false);
    }
  }

  async function fetchListeningLog(
    limit: number,
    offset: number,
    sourceFilter: RecentDebugSourceFilter,
    forceRecentSync = false,
  ): Promise<ListeningLogResponse> {
    const endpoint = "/debug/listening-log";
    const params = new URLSearchParams({
      limit: String(limit),
      offset: String(offset),
      source_filter: sourceFilter,
    });
    if (forceRecentSync) {
      params.set("force_recent_sync", "true");
    }
    const response = await fetch(
      `${apiBaseUrl}${endpoint}?${params.toString()}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load listening log.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Listening Log (${response.status}): ${detail}`);
    }
    return (await response.json()) as ListeningLogResponse;
  }

  async function postCatalogBackfillRun(runMode: CatalogBackfillRunMode): Promise<CatalogBackfillRunResponse> {
    const isPriorityMetadata = runMode === "metadata_only";
    const runReason = isPriorityMetadata
      ? "identity_metadata"
      : runMode === "tracklists_relevant"
        ? "tracklist_completion"
        : "full_backfill";
    const effectiveAlbumTracklistPolicy = isPriorityMetadata ? "none" : catalogBackfillAlbumTracklistPolicy;
    const response = await fetch(`${apiBaseUrl}/debug/spotify/catalog-backfill`, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        run_mode: runMode,
        reason: runReason,
        limit: Math.max(1, Math.round(catalogBackfillLimit)),
        offset: Math.max(0, Math.round(catalogBackfillOffset)),
        market: catalogBackfillMarket.trim() || "US",
        include_albums: isPriorityMetadata ? true : catalogBackfillIncludeAlbums,
        force_refresh: catalogBackfillForceRefresh,
        request_delay_seconds: Math.max(0.2, catalogBackfillRequestDelaySeconds),
        max_runtime_seconds: Math.min(300, Math.max(5, Math.round(catalogBackfillMaxRuntimeSeconds))),
        max_requests: Math.min(1000, Math.max(1, Math.round(catalogBackfillMaxRequests))),
        max_errors: Math.min(100, Math.max(1, Math.round(catalogBackfillMaxErrors))),
        max_album_tracks_pages_per_album: Math.min(50, Math.max(1, Math.round(catalogBackfillMaxAlbumTracksPagesPerAlbum))),
        max_429: Math.min(20, Math.max(1, Math.round(catalogBackfillMax429))),
        album_tracklist_policy: effectiveAlbumTracklistPolicy,
        priority_scope: isPriorityMetadata ? "identity_and_top_listened" : "all",
      }),
    });
    const payload = (await response.json()) as
      | CatalogBackfillRunResponse
      | { detail?: string; error?: { message?: string }; status?: string; last_error?: string | null };
    if (!response.ok || !("ok" in payload && payload.ok)) {
      let detail = "Catalog backfill failed.";
      if ("error" in payload && payload.error?.message) {
        detail = payload.error.message;
      } else if ("detail" in payload && payload.detail) {
        detail = payload.detail;
      } else if ("last_error" in payload && payload.last_error) {
        detail = payload.last_error;
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Catalog Backfill Run (${response.status}): ${detail}`);
    }
    return payload as CatalogBackfillRunResponse;
  }

  async function loadMergedTrackRankings(reset: boolean = false) {
    if (mergedTracksLoading) {
      return;
    }
    if (reset) {
      setMergedTracks([]);
      setMergedTracksExcludedUnknownCount(0);
      setMergedTracksLoaded(false);
      setMergedTracksLastLoadedAt(null);
    }
    setMergedTracksLoading(true);
    setMergedTracksError("");
    try {
      const data = await fetchMergedTrackAggregate();
      setMergedTracks(data.items);
      setMergedTracksExcludedUnknownCount(Math.max(0, data.excluded_unknown_identity_count ?? 0));
      setMergedTracksLoaded(true);
      setMergedTracksLastLoadedAt(Date.now());
    } catch (error) {
      setMergedTracksError(formatUiErrorMessage(error, "Failed to load merged tracks."));
    } finally {
      setMergedTracksLoading(false);
    }
  }

  function reloadTrackRankings() {
    void loadMergedTrackRankings(true);
  }

  async function loadRecentComputedTrackRankings(reset: boolean = false) {
    if (recentComputedTracksLoading) {
      return;
    }
    if (reset) {
      setRecentComputedTracks([]);
      setRecentComputedTracksLoaded(false);
    }
    setRecentComputedTracksLoading(true);
    setRecentComputedTracksError("");
    try {
      const data = await fetchMergedTrackAggregate("recent");
      setRecentComputedTracks(data.items);
      setRecentComputedTracksLoaded(true);
    } catch (error) {
      setRecentComputedTracksError(formatUiErrorMessage(error, "Failed to load computed recent tracks."));
    } finally {
      setRecentComputedTracksLoading(false);
    }
  }

  async function loadIdentityAudit(reset: boolean = false) {
    if (identityAuditLoading) {
      return;
    }
    if (reset) {
      setIdentityAudit(null);
      setIdentityAuditLoaded(false);
      setIdentityAuditLastLoadedAt(null);
    }
    setIdentityAuditLoading(true);
    setIdentityAuditError("");
    try {
      const data = await fetchIdentityAudit();
      setIdentityAudit(data);
      setIdentityAuditLoaded(true);
      setIdentityAuditLastLoadedAt(Date.now());
    } catch (error) {
      setIdentityAuditError(formatUiErrorMessage(error, "Failed to load identity audit."));
    } finally {
      setIdentityAuditLoading(false);
    }
  }

  async function loadIdentityAuditSuggestedGroups(reset: boolean = false) {
    if (identityAuditSuggestedLoading) {
      return;
    }
    if (reset) {
      setIdentityAuditSuggestedGroups(null);
      setIdentityAuditSuggestedLoaded(false);
      setIdentityAuditSuggestedLastLoadedAt(null);
    }
    setIdentityAuditSuggestedLoading(true);
    setIdentityAuditSuggestedError("");
    try {
      const data = await fetchIdentityAuditSuggestedGroups();
      setIdentityAuditSuggestedGroups(data);
      setIdentityAuditSuggestedLoaded(true);
      setIdentityAuditSuggestedLastLoadedAt(Date.now());
    } catch (error) {
      setIdentityAuditSuggestedError(formatUiErrorMessage(error, "Failed to load suggested composition groups."));
    } finally {
      setIdentityAuditSuggestedLoading(false);
    }
  }

  async function loadIdentityAuditAmbiguousReview(reset: boolean = false) {
    if (identityAuditAmbiguousLoading) {
      return;
    }
    if (reset) {
      setIdentityAuditAmbiguous(null);
      setIdentityAuditAmbiguousLoaded(false);
      setIdentityAuditAmbiguousLastLoadedAt(null);
    }
    setIdentityAuditAmbiguousLoading(true);
    setIdentityAuditAmbiguousError("");
    try {
      const data = await fetchIdentityAuditAmbiguousReview();
      setIdentityAuditAmbiguous(data);
      setIdentityAuditAmbiguousLoaded(true);
      setIdentityAuditAmbiguousLastLoadedAt(Date.now());
    } catch (error) {
      setIdentityAuditAmbiguousError(formatUiErrorMessage(error, "Failed to load ambiguous review queue."));
    } finally {
      setIdentityAuditAmbiguousLoading(false);
    }
  }

  async function loadIdentityAuditSavedSubmissions(reset: boolean = false) {
    if (identityAuditSavedSubmissionsLoading) {
      return;
    }
    if (reset) {
      setIdentityAuditSavedSubmissions(null);
      setIdentityAuditSavedSubmissionDetail(null);
      setIdentityAuditSavedSubmissionDetailError("");
      setIdentityAuditSavedSubmissionDryRun(null);
      setIdentityAuditSavedSubmissionDryRunError("");
      setIdentityAuditSavedSubmissionDryRunLoading(false);
      setIdentityAuditSavedSubmissionDryRunAt(null);
    }
    setIdentityAuditSavedSubmissionsLoading(true);
    setIdentityAuditSavedSubmissionsError("");
    try {
      const data = await fetchIdentityAuditSavedSubmissions(20, 0);
      setIdentityAuditSavedSubmissions(data);
    } catch (error) {
      setIdentityAuditSavedSubmissionsError(formatUiErrorMessage(error, "Failed to load saved submissions."));
    } finally {
      setIdentityAuditSavedSubmissionsLoading(false);
    }
  }

  async function viewIdentityAuditSavedSubmission(submissionId: number) {
    if (identityAuditSavedSubmissionDetailLoading) {
      return;
    }
    setIdentityAuditSavedSubmissionDetailLoading(true);
    setIdentityAuditSavedSubmissionDetailError("");
    setIdentityAuditSavedSubmissionDryRun(null);
    setIdentityAuditSavedSubmissionDryRunError("");
    setIdentityAuditSavedSubmissionDryRunLoading(false);
    setIdentityAuditSavedSubmissionDryRunAt(null);
    try {
      const payload = await fetchIdentityAuditSavedSubmissionById(submissionId);
      setIdentityAuditSavedSubmissionDetail(payload);
    } catch (error) {
      setIdentityAuditSavedSubmissionDetailError(formatUiErrorMessage(error, "Failed to load saved submission details."));
      setIdentityAuditSavedSubmissionDetail(null);
    } finally {
      setIdentityAuditSavedSubmissionDetailLoading(false);
    }
  }

  async function dryRunIdentityAuditSavedSubmission(submissionId: number) {
    if (identityAuditSavedSubmissionDryRunLoading) {
      return;
    }
    setIdentityAuditSavedSubmissionDryRunLoading(true);
    setIdentityAuditSavedSubmissionDryRunError("");
    try {
      const payload = await fetchIdentityAuditSavedSubmissionDryRun(submissionId);
      setIdentityAuditSavedSubmissionDryRun(payload);
      setIdentityAuditSavedSubmissionDryRunAt(Date.now());
    } catch (error) {
      setIdentityAuditSavedSubmissionDryRunError(formatUiErrorMessage(error, "Failed to run dry run."));
      setIdentityAuditSavedSubmissionDryRun(null);
      setIdentityAuditSavedSubmissionDryRunAt(null);
    } finally {
      setIdentityAuditSavedSubmissionDryRunLoading(false);
    }
  }

  async function loadListeningLogBatch(reset: boolean = false, forceRecentSync = false) {
    if (listeningLogLoadInFlightRef.current) {
      return;
    }
    listeningLogLoadInFlightRef.current = true;
    setListeningLogLoading(true);
    setListeningLogError("");
    try {
      const targetOffset = reset ? 0 : listeningLogOffset;
      const payload = await fetchListeningLog(50, targetOffset, recentDebugSourceFilter, forceRecentSync);
      setListeningLogTracks((current) => (reset ? payload.items : [...current, ...payload.items]));
      setListeningLogOffset(targetOffset + payload.items.length);
      setListeningLogHasMore(Boolean(payload.has_more));
      setListeningLogLoaded(true);
      setListeningLogLastLoadedAt(Date.now());
    } catch (error) {
      const message = formatUiErrorMessage(error, "Failed to load listening log.");
      setStatusHistory((current) => [...current, `Listening log error: ${message}`]);
      setListeningLogError(message);
      if (reset) {
        setListeningLogTracks([]);
        setListeningLogOffset(0);
        setListeningLogHasMore(false);
      }
    } finally {
      listeningLogLoadInFlightRef.current = false;
      setListeningLogLoading(false);
    }
  }

  async function loadCatalogBackfillCoverage(reset: boolean = false) {
    if (catalogBackfillCoverageLoading) {
      return;
    }
    if (reset) {
      setCatalogBackfillCoverage(null);
      setCatalogBackfillCoverageLoaded(false);
      setCatalogBackfillCoverageLastLoadedAt(null);
    }
    setCatalogBackfillCoverageLoading(true);
    setCatalogBackfillCoverageError("");
    try {
      const payload = await fetchCatalogBackfillCoverage();
      setCatalogBackfillCoverage(payload);
      setCatalogBackfillCoverageLoaded(true);
      setCatalogBackfillCoverageLastLoadedAt(Date.now());
    } catch (error) {
      setCatalogBackfillCoverageError(formatUiErrorMessage(error, "Failed to load catalog coverage."));
    } finally {
      setCatalogBackfillCoverageLoading(false);
    }
  }

  async function loadCatalogBackfillRuns(reset: boolean = false) {
    if (catalogBackfillRunsLoading) {
      return;
    }
    if (reset) {
      setCatalogBackfillRuns(null);
      setCatalogBackfillRunsLoaded(false);
      setCatalogBackfillRunsLastLoadedAt(null);
    }
    setCatalogBackfillRunsLoading(true);
    setCatalogBackfillRunsError("");
    try {
      const payload = await fetchCatalogBackfillRuns(20, 0);
      setCatalogBackfillRuns(payload);
      setCatalogBackfillRunsLoaded(true);
      setCatalogBackfillRunsLastLoadedAt(Date.now());
    } catch (error) {
      setCatalogBackfillRunsError(formatUiErrorMessage(error, "Failed to load backfill runs."));
    } finally {
      setCatalogBackfillRunsLoading(false);
    }
  }

  async function loadCatalogBackfillQueue(
    reset: boolean = false,
    explicitFilter?: "all" | "pending" | "done" | "error",
    explicitReasonFilter?: CatalogBackfillQueueReasonFilter
  ) {
    if (catalogBackfillQueueLoading) {
      return;
    }
    if (reset) {
      setCatalogBackfillQueue(null);
      setCatalogBackfillQueueLoaded(false);
      setCatalogBackfillQueueLastLoadedAt(null);
    }
    const activeFilter = explicitFilter ?? catalogBackfillQueueStatusFilter;
    const activeReasonFilter = explicitReasonFilter ?? catalogBackfillQueueReasonFilter;
    if (explicitFilter && explicitFilter !== catalogBackfillQueueStatusFilter) {
      setCatalogBackfillQueueStatusFilter(explicitFilter);
    }
    if (explicitReasonFilter && explicitReasonFilter !== catalogBackfillQueueReasonFilter) {
      setCatalogBackfillQueueReasonFilter(explicitReasonFilter);
    }
    setCatalogBackfillQueueLoading(true);
    setCatalogBackfillQueueError("");
    try {
      const payload = await fetchCatalogBackfillQueue(activeFilter, activeReasonFilter, 50, 0);
      setCatalogBackfillQueue(payload);
      setCatalogBackfillQueueLoaded(true);
      setCatalogBackfillQueueLastLoadedAt(Date.now());
    } catch (error) {
      setCatalogBackfillQueueError(formatUiErrorMessage(error, "Failed to load backfill queue."));
    } finally {
      setCatalogBackfillQueueLoading(false);
    }
  }

  async function repairCatalogBackfillQueueStatuses() {
    if (catalogBackfillQueueRepairLoading) {
      return;
    }
    setCatalogBackfillQueueRepairLoading(true);
    setCatalogBackfillQueueRepairMessage("");
    try {
      const payload = await postCatalogBackfillQueueRepair();
      setCatalogBackfillQueueRepairMessage(`Repaired ${payload.repaired} queue item status values.`);
      await loadCatalogBackfillQueue(true);
    } catch (error) {
      setCatalogBackfillQueueRepairMessage(formatUiErrorMessage(error, "Failed to repair queue statuses."));
    } finally {
      setCatalogBackfillQueueRepairLoading(false);
    }
  }

  async function loadAlbumCatalogLookup(reset: boolean = false) {
    if (albumCatalogLookupLoading) {
      return;
    }
    if (reset) {
      setAlbumCatalogLookupResult(null);
      setAlbumCatalogLookupLoaded(false);
      setAlbumCatalogLookupLastLoadedAt(null);
    }
    setAlbumCatalogLookupLoading(true);
    setAlbumCatalogLookupError("");
    try {
      const payload = await fetchAlbumCatalogLookup(
        albumCatalogLookupQ,
        albumCatalogLookupStatus,
        searchLookupQueueStatus,
        searchLookupSort,
        50,
        0,
      );
      setAlbumCatalogLookupResult(payload);
      setAlbumCatalogLookupLoaded(true);
      setAlbumCatalogLookupLastLoadedAt(Date.now());
    } catch (error) {
      setAlbumCatalogLookupError(formatUiErrorMessage(error, "Failed to search albums."));
    } finally {
      setAlbumCatalogLookupLoading(false);
    }
  }

  async function loadTrackCatalogLookup(reset: boolean = false) {
    if (trackCatalogLookupLoading) {
      return;
    }
    if (reset) {
      setTrackCatalogLookupResult(null);
      setTrackCatalogLookupLoaded(false);
      setTrackCatalogLookupLastLoadedAt(null);
    }
    setTrackCatalogLookupLoading(true);
    setTrackCatalogLookupError("");
    try {
      const payload = await fetchTrackCatalogLookup(
        albumCatalogLookupQ,
        trackCatalogLookupStatus,
        searchLookupQueueStatus,
        searchLookupSort,
        50,
        0,
      );
      setTrackCatalogLookupResult(payload);
      setTrackCatalogLookupLoaded(true);
      setTrackCatalogLookupLastLoadedAt(Date.now());
    } catch (error) {
      setTrackCatalogLookupError(formatUiErrorMessage(error, "Failed to search tracks."));
    } finally {
      setTrackCatalogLookupLoading(false);
    }
  }

  async function loadAlbumDuplicateLookup(reset: boolean = false) {
    if (albumDuplicateLookupLoading) {
      return;
    }
    if (reset) {
      setAlbumDuplicateLookupResult(null);
      setAlbumDuplicateLookupLoaded(false);
      setAlbumDuplicateLookupLastLoadedAt(null);
    }
    setAlbumDuplicateLookupLoading(true);
    setAlbumDuplicateLookupError("");
    try {
      const payload = await fetchAlbumDuplicateLookup(200, 0);
      setAlbumDuplicateLookupResult(payload);
      setAlbumDuplicateLookupLoaded(true);
      setAlbumDuplicateLookupLastLoadedAt(Date.now());
    } catch (error) {
      setAlbumDuplicateLookupError(formatUiErrorMessage(error, "Failed to load duplicate albums."));
    } finally {
      setAlbumDuplicateLookupLoading(false);
    }
  }

  async function loadTrackDuplicateLookup(reset: boolean = false) {
    if (trackDuplicateLookupLoading) {
      return;
    }
    if (reset) {
      setTrackDuplicateLookupResult(null);
      setTrackDuplicateLookupLoaded(false);
      setTrackDuplicateLookupLastLoadedAt(null);
    }
    setTrackDuplicateLookupLoading(true);
    setTrackDuplicateLookupError("");
    try {
      const payload = await fetchTrackDuplicateLookup(200, 0);
      setTrackDuplicateLookupResult(payload);
      setTrackDuplicateLookupLoaded(true);
      setTrackDuplicateLookupLastLoadedAt(Date.now());
    } catch (error) {
      setTrackDuplicateLookupError(formatUiErrorMessage(error, "Failed to load duplicate tracks."));
    } finally {
      setTrackDuplicateLookupLoading(false);
    }
  }

  async function loadTrackMappingLineage(
    reset: boolean = false,
    mappingKind: "all" | "source_release" | "release_family" = trackMappingKindFilter,
    sourceMetadata: "all" | "complete" | "incomplete" = trackMappingSourceMetadataFilter,
    confirmationCertainty: "all" | "certain" | "uncertain" = trackMappingCertaintyFilter,
  ) {
    const requestId = trackMappingLineageRequestIdRef.current + 1;
    trackMappingLineageRequestIdRef.current = requestId;
    if (reset) {
      setTrackMappingLineageResult(null);
      setTrackMappingLineageLoaded(false);
      setTrackMappingLineageLastLoadedAt(null);
    }
    setTrackMappingLineageLoading(true);
    setTrackMappingLineageError("");
    try {
      const payload = await fetchTrackMappingLineage(albumCatalogLookupQ, mappingKind, sourceMetadata, confirmationCertainty, 10, 0);
      if (trackMappingLineageRequestIdRef.current !== requestId) {
        return;
      }
      setTrackMappingLineageResult(payload);
      setTrackMappingLineageLoaded(true);
      setTrackMappingLineageLastLoadedAt(Date.now());
    } catch (error) {
      if (trackMappingLineageRequestIdRef.current !== requestId) {
        return;
      }
      setTrackMappingLineageLoaded(true);
      setTrackMappingLineageError(formatUiErrorMessage(error, "Failed to load track mapping."));
    } finally {
      if (trackMappingLineageRequestIdRef.current === requestId) {
        setTrackMappingLineageLoading(false);
      }
    }
  }

  async function loadAlbumNameDuplicateLookup(reset: boolean = false) {
    if (albumNameDuplicateLookupLoading) {
      return;
    }
    if (reset) {
      setAlbumNameDuplicateLookupResult(null);
      setAlbumNameDuplicateLookupLoaded(false);
      setAlbumNameDuplicateLookupLastLoadedAt(null);
    }
    setAlbumNameDuplicateLookupLoading(true);
    setAlbumNameDuplicateLookupError("");
    try {
      const payload = await fetchAlbumNameDuplicateLookup(200, 0);
      setAlbumNameDuplicateLookupResult(payload);
      setAlbumNameDuplicateLookupLoaded(true);
      setAlbumNameDuplicateLookupLastLoadedAt(Date.now());
    } catch (error) {
      setAlbumNameDuplicateLookupError(formatUiErrorMessage(error, "Failed to load duplicate albums by name."));
    } finally {
      setAlbumNameDuplicateLookupLoading(false);
    }
  }

  async function loadActiveSearchLookup(reset: boolean = false) {
    if (searchLookupEntityType === "tracks") {
      await loadTrackCatalogLookup(reset);
      return;
    }
    await loadAlbumCatalogLookup(reset);
  }

  async function enqueueVisibleIncompleteLookupAlbums(items?: AlbumCatalogLookupItem[]) {
    if (albumCatalogLookupEnqueueLoading) {
      return;
    }
    const sourceItems = items ?? (albumCatalogLookupResult?.items ?? []);
    const spotifyAlbumIds = Array.from(
      new Set(
        sourceItems
          .filter((item) => albumLookupRowCanBulkPrioritize(item))
          .map((item) => item.spotify_album_id)
          .filter((spotifyAlbumId): spotifyAlbumId is string => Boolean(spotifyAlbumId)),
      ),
    );
    if (spotifyAlbumIds.length === 0) {
      setAlbumCatalogLookupEnqueueResult(null);
      setAlbumCatalogLookupEnqueueError("No visible incomplete albums with Spotify IDs to enqueue.");
      return;
    }

    setAlbumCatalogLookupEnqueueLoading(true);
    setAlbumCatalogLookupEnqueueError("");
    setAlbumCatalogLookupEnqueueResult(null);
    try {
      const payload = await enqueueCatalogBackfillItems(
        spotifyAlbumIds.map((spotifyId) => ({
          entity_type: "album",
          spotify_id: spotifyId,
          reason: "manual_priority",
          priority: 80,
        })),
      );
      setAlbumCatalogLookupEnqueueResult(payload);
      await loadCatalogBackfillQueue(true);
    } catch (error) {
      setAlbumCatalogLookupEnqueueError(formatUiErrorMessage(error, "Failed to enqueue albums."));
    } finally {
      setAlbumCatalogLookupEnqueueLoading(false);
    }
  }

  async function enqueueVisibleIncompleteLookupTracks(items?: TrackCatalogLookupItem[]) {
    if (albumCatalogLookupEnqueueLoading) {
      return;
    }
    const sourceItems = items ?? (trackCatalogLookupResult?.items ?? []);
    const spotifyTrackIds = Array.from(
      new Set(
        sourceItems
          .filter((item) => trackLookupRowCanBulkPrioritize(item))
          .map((item) => item.spotify_track_id)
          .filter((spotifyTrackId): spotifyTrackId is string => Boolean(spotifyTrackId)),
      ),
    );
    if (spotifyTrackIds.length === 0) {
      setAlbumCatalogLookupEnqueueResult(null);
      setAlbumCatalogLookupEnqueueError("No visible incomplete tracks with Spotify IDs to prioritize.");
      return;
    }

    setAlbumCatalogLookupEnqueueLoading(true);
    setAlbumCatalogLookupEnqueueError("");
    setAlbumCatalogLookupEnqueueResult(null);
    try {
      const payload = await enqueueCatalogBackfillItems(
        spotifyTrackIds.map((spotifyId) => ({
          entity_type: "track",
          spotify_id: spotifyId,
          reason: "manual_priority",
          priority: 80,
        })),
      );
      setAlbumCatalogLookupEnqueueResult(payload);
      await loadCatalogBackfillQueue(true);
    } catch (error) {
      setAlbumCatalogLookupEnqueueError(formatUiErrorMessage(error, "Failed to prioritize tracks."));
    } finally {
      setAlbumCatalogLookupEnqueueLoading(false);
    }
  }

  async function runCatalogBackfill(runMode: CatalogBackfillRunMode) {
    if (catalogBackfillRunLoading) {
      return;
    }
    setCatalogBackfillRunLoading(true);
    setCatalogBackfillRunError("");
    try {
      const result = await postCatalogBackfillRun(runMode);
      setCatalogBackfillLatestResult(result);
      await Promise.all([loadCatalogBackfillCoverage(true), loadCatalogBackfillRuns(true), loadCatalogBackfillQueue(true)]);
    } catch (error) {
      setCatalogBackfillRunError(formatUiErrorMessage(error, "Catalog backfill failed."));
    } finally {
      setCatalogBackfillRunLoading(false);
    }
  }

  async function refreshRecentSection(targetRange: RecentRange = recentRange, forceRecentSync = false) {
    if (experienceMode === "full" && spotifyCooldownActive) {
      setStatusMessage(formatCooldownCopy(reloadSecondsRemaining));
      setStatusHistory((current) => [...current, "Spotify cooldown active. Recent refresh paused."]);
      return;
    }
    if (recentSectionLoadInFlightRef.current) {
      return;
    }
    recentSectionLoadInFlightRef.current = true;
    setRecentSectionStartupError(false);
    setLoadingRecentSection(true);
    setStatusMessage("Refreshing recent sections...");
    try {
      const data = await fetchRecentSections(targetRange, forceRecentSync);
      setProfile((current) =>
        current
          ? {
              ...current,
              recent_range: data.recent_range,
              recent_window_days: data.recent_window_days,
              recent_top_artists: data.recent_top_artists,
              recent_top_artists_available: data.recent_top_artists_available,
              recent_top_tracks: data.recent_top_tracks,
              recent_top_tracks_available: data.recent_top_tracks_available,
              recent_top_albums: data.recent_top_albums,
              recent_top_albums_available: data.recent_top_albums_available,
              recent_tracks: data.recent_tracks,
              recent_tracks_available: data.recent_tracks_available,
              recent_likes_tracks: likedTracksCache?.items.length ? likedTracksCache.items : data.recent_likes_tracks,
              recent_likes_available: likedTracksCache?.items.length ? true : data.recent_likes_available,
            }
          : current,
      );
      if (targetRange !== recentRange) {
        setRecentRange(targetRange);
      }
      setStatusMessage("");
    } catch (error) {
      const message = formatUiErrorMessage(error, "Failed to refresh recent sections.");
      setStatusMessage(message);
      setRecentSectionStartupError(true);
      setStatusHistory((current) => [...current, `Recent refresh error: ${message}`]);
    } finally {
      setRecentSectionLoadAttempted(true);
      setLoadingRecentSection(false);
      recentSectionLoadInFlightRef.current = false;
    }
  }

  function renderQueueDelayControl() {
    return (
      <div className="player-queue-settings">
        <button
          aria-expanded={playerQueuePauseMenuOpen}
          aria-label="Stop queue timer"
          aria-pressed={queueDelayActive}
          className={`player-queue-header-button${queueDelayActive ? " player-queue-header-button-active player-queue-header-toggle-active" : ""}${liveReadOnlyMode ? " player-control-readonly" : ""}`}
          disabled={!playerDisplayTrack && playerQueueTracks.length === 0}
          onClick={() => {
            setPlayerQueuePauseMenuOpen((current) => !current);
            setPlayerQueueSettingsOpen(false);
          }}
          title={queueDelayActive ? "Stop queue active" : "Stop queue"}
          type="button"
        >
          <svg aria-hidden="true" viewBox="0 0 24 24">
            <path d="M10 2h4v2h-4V2Zm1 11V7h2v7h-5v-2h3Zm1-8a8 8 0 1 0 0 16 8 8 0 0 0 0-16Zm0 18a10 10 0 1 1 0-20 10 10 0 0 1 0 20Zm6.6-17.2 1.4 1.4-1.6 1.6L17 7.4l1.6-1.6Z" />
          </svg>
        </button>
        {playerQueuePauseMenuOpen ? (
          <div className="player-queue-settings-menu player-queue-delay-menu">
            <button
              className={queuePauseAfterCurrentEnabled ? "player-queue-settings-active" : undefined}
              disabled={playerQueueTracks.length === 0}
              onClick={() => void handleQueuePauseAfterCurrentClick()}
              type="button"
            >
              After this song
            </button>
            <button
              className={queueSleepTimerActive ? "player-queue-settings-active" : undefined}
              disabled={!playerDisplayTrack}
              onClick={() => void handleQueueSleepTimerClick()}
              type="button"
            >
              15 minutes
            </button>
            {queueDelayActive ? (
              <button onClick={cancelQueueDelay} type="button">
                Cancel stop
              </button>
            ) : null}
          </div>
        ) : null}
      </div>
    );
  }

  function renderHomePlayerPanel() {
    if (!profile || appPage !== "dashboard" || experienceMode === "local") {
      return null;
    }

    const fallbackHomeQueueGroup = playerQueueGroupFromTracks(
      playerQueueTracks,
      playerQueueContext,
      playerQueueSource === "spotify" ? "Spotify queue" : "Current queue",
    );
    const homeQueueGroups = playerQueueGroups.length > 0
      ? playerQueueGroups
      : [fallbackHomeQueueGroup].filter((group): group is PlayerQueueGroup => Boolean(group));
    const homeQueueGroupMeta = (group: PlayerQueueGroup, groupStartIndex: number) => {
      const groupEndIndex = groupStartIndex + group.tracks.length - 1;
      const activeInGroup = hasActiveQueueCursor && activeQueueCursor >= groupStartIndex && activeQueueCursor <= groupEndIndex;
      return [
        group.url ? "Context" : playerQueueSource === "spotify" ? "Spotify" : "ListenLab",
        `${group.tracks.length} ${group.tracks.length === 1 ? "track" : "tracks"}`,
        activeInGroup ? `track ${activeQueueCursor - groupStartIndex + 1}` : null,
      ].filter(Boolean).join(" · ");
    };
    const renderHomeQueueTrackRow = (track: PlayerQueueTrack, index: number, bookmarkContext: TrackBookmarkContext | null = null) => {
      const isCurrentQueueTrack = hasActiveQueueCursor && index === activeQueueCursor;
      const isLoopedQueueTrack = playerTrackLoopEnabled && isCurrentQueueTrack;
      const isPausedQueueTrack = queuePausedCursor === index && isCurrentQueueTrack && playerDisplayPaused;
      const isUpNextQueueTrack = !playerTrackLoopEnabled && hasActiveQueueCursor && index === activeQueueCursor + 1;
      const isQueueDimmedByTrackLoop = playerTrackLoopEnabled && !isCurrentQueueTrack;
      const isPlayedQueueTrack = playerQueueSource === "listenlab" && playerQueuePlayedKeys.has(queueTrackIdentity(track) ?? "");
      const isBookmarkedQueueTrack = trackIsBookmarked(track);
      return (
        <div
          className={`player-recent-row player-queue-row${playerQueueOrganizeMode ? " player-queue-row-organizing" : ""}${playerQueueDragIndex === index ? " player-queue-row-dragging" : ""}${isCurrentQueueTrack ? " player-queue-row-current" : ""}${isUpNextQueueTrack || isLoopedQueueTrack || isPausedQueueTrack ? " player-queue-row-up-next" : ""}${isQueueDimmedByTrackLoop ? " player-queue-row-muted" : ""}`}
          data-player-queue-role={isUpNextQueueTrack ? "up-next" : (isCurrentQueueTrack ? "current" : undefined)}
          draggable={playerQueueOrganizeMode}
          key={`${track.uri ?? track.trackId ?? track.name}-${index}`}
          onDragEnd={() => setPlayerQueueDragIndex(null)}
          onDragOver={(event) => {
            if (playerQueueOrganizeMode) {
              event.preventDefault();
            }
          }}
          onDragStart={() => setPlayerQueueDragIndex(index)}
          onDrop={(event) => {
            event.preventDefault();
            if (playerQueueDragIndex != null) {
              moveQueueTrack(playerQueueDragIndex, index);
              setPlayerQueueDragIndex(null);
            }
          }}
        >
          {playerQueueOrganizeMode ? (
            <button aria-label={`Remove ${track.name} from queue`} className="player-queue-remove-button" onClick={() => removeQueueTrackAtIndex(index)} type="button">X</button>
          ) : null}
          {playerQueueOrganizeMode ? (
            <span className="player-queue-cover-button player-queue-cover-static" aria-hidden="true">
              {track.image ? (
                <img alt="" className="player-recent-cover" src={track.image} />
              ) : (
                <span className="player-recent-cover player-recent-cover-fallback">{track.name.slice(0, 1).toUpperCase()}</span>
              )}
            </span>
          ) : (
            <button aria-label={`Play ${track.name}`} className="player-queue-cover-button" disabled={!track.uri} onClick={() => void playQueueTrackAtIndex(index)} type="button">
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
          {playerQueueOrganizeMode ? (
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
            <button className="player-recent-copy player-queue-copy-button" onClick={() => openQueuePlayerTrackDetails(track)} type="button">
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
            </button>
          )}
          <span className="player-queue-row-actions">
            {!playerQueueOrganizeMode ? (
              <button
                aria-label={isBookmarkedQueueTrack ? `Bookmarked ${track.name}` : `Bookmark ${track.name}`}
                aria-pressed={isBookmarkedQueueTrack}
                className={`player-queue-bookmark-button${isBookmarkedQueueTrack ? " player-queue-bookmark-button-active" : ""}`}
                onClick={() => toggleTrackBookmark(track, bookmarkContext)}
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
              <button aria-label="Unloop current song" className="player-queue-status player-queue-status-next player-queue-loop-status" onClick={unloopCurrentTrack} title="Unloop" type="button">
                <span className="player-queue-loop-status-default">Looped</span>
                <span className="player-queue-loop-status-hover">Unloop</span>
              </button>
            ) : null}
            {isUpNextQueueTrack ? <span className="player-queue-status player-queue-status-next">{queuePauseAfterCurrentEnabled ? "Paused" : "Up next"}</span> : null}
            {isPlayedQueueTrack && !isCurrentQueueTrack && !isUpNextQueueTrack ? <span className="player-queue-status player-queue-status-played">Played</span> : null}
          </span>
        </div>
      );
    };

    return (
      <section className="info-card info-card-wide player-home-panel" aria-label="Playback controls">
        <div className="player-home-layout">
          <div className="player-current-column">
            <div className="player-menu-summary">
              <div className="player-menu-copy">
                <div className="player-menu-copy-top">
                  <h2>
                    {playerDisplayTrack ? (
                      <button
                        className="player-menu-title-button player-menu-title-scroll"
                        onClick={() => openPlayerTrackDetails()}
                        type="button"
                      >
                        <span>
                          {recentTrackHasRelationTags(playerDisplayKnownTrack) || hasReleaseSiblingForTrackId(spotifyTrackIdFromUri(playerDisplayTrack.uri)) ? (
                            <ReleaseSiblingBadge
                              className="player-release-sibling-badge"
                              sourceCount={releaseSiblingSourceCountForTrackId(playerDisplayKnownTrack?.track_id ?? spotifyTrackIdFromUri(playerDisplayTrack.uri))}
                              duplicateSourceCount={playerDisplayKnownTrack?.release_track_duplicate_source_count ?? null}
                              clusterCandidateType={playerDisplayKnownTrack?.release_track_cluster_candidate_type ?? null}
                              clusterRelationshipKind={playerDisplayKnownTrack?.release_track_cluster_relationship_kind ?? null}
                            />
                          ) : null}
                          {playerDisplayTrack.name ?? "ListenLab Player"}
                        </span>
                      </button>
                    ) : (
                      <span className="player-menu-title-scroll">
                        <span>ListenLab Player</span>
                      </span>
                    )}
                  </h2>
                </div>
                {playerDisplayArtists.length > 0 ? (
                  <div className="player-menu-artist-row">
                    <button className="player-menu-meta-button player-menu-line player-menu-artist-button single-line-ellipsis" onClick={openPlayerArtistsDetails} type="button">
                      {playerDisplayArtistImageUrl ? <img alt="" className="player-menu-artist-image" src={playerDisplayArtistImageUrl} /> : null}
                      <span className="single-line-ellipsis">{playerDisplayArtistLabel}</span>
                    </button>
                    {playerDisplayTrack?.uri ? (
                      <a
                        aria-label="Open in Spotify"
                        className="player-menu-external player-menu-external-inline"
                        href={spotifyTrackUrl(playerDisplayTrack.uri) ?? undefined}
                        rel="noreferrer"
                        target="_blank"
                      >
                        <svg aria-hidden="true" viewBox="0 0 24 24">
                          <path d="M12 2.2a9.8 9.8 0 1 0 0 19.6 9.8 9.8 0 0 0 0-19.6Zm4.49 14.13a.72.72 0 0 1-.99.24c-2.7-1.65-6.1-2.02-10.1-1.11a.72.72 0 1 1-.32-1.4c4.38-1 8.14-.57 11.17 1.28.34.2.44.65.24.99Zm1.2-2.68a.9.9 0 0 1-1.24.3c-3.09-1.9-7.8-2.45-11.46-1.34a.9.9 0 1 1-.52-1.72c4.18-1.27 9.37-.66 12.92 1.52.42.26.56.82.3 1.24Zm.1-2.8c-3.7-2.2-9.8-2.4-13.34-1.33a1.08 1.08 0 1 1-.63-2.07c4.06-1.23 10.8-.99 15.07 1.55a1.08 1.08 0 0 1-1.1 1.85Z" />
                        </svg>
                      </a>
                    ) : null}
                  </div>
                ) : (
                  <p className="player-menu-line single-line-ellipsis">{playerDisplayTrack?.artists ?? "Spotify Premium playback"}</p>
                )}
              </div>
            </div>

            {playerDisplayTrack ? (
              <div className="player-progress" aria-label="Playback progress">
                <input
                  aria-label="Seek playback"
                  className="player-progress-slider"
                  disabled={!canControlPlayback}
                  max={Math.max(playerDisplayDurationMs || playerDisplayTrack.durationMs || 0, 1)}
                  min={0}
                  onChange={(event) => setPendingSeekMs(Number(event.currentTarget.value))}
                  onMouseUp={(event) => {
                    if (canControlPlayback) {
                      void seekPlayer(Number(event.currentTarget.value));
                    }
                  }}
                  onTouchEnd={(event) => {
                    if (canControlPlayback) {
                      void seekPlayer(Number(event.currentTarget.value));
                    }
                  }}
                  step={1000}
                  title={playerTransportTooltip}
                  type="range"
                  value={pendingSeekMs ?? playerDisplayPositionMs}
                />
                <div className="player-progress-times">
                  <span>{formatPlaybackClock(pendingSeekMs ?? playerDisplayPositionMs)}</span>
                  <span>{formatPlaybackClock(playerDisplayDurationMs || playerDisplayTrack.durationMs || 0)}</span>
                </div>
              </div>
            ) : null}

            <div className="actions actions-centered actions-in-card player-transport-controls">
              <button
                aria-label={playerDisplayKnownLiked ? "Liked song" : "Not liked"}
                aria-pressed={playerDisplayKnownLiked}
                className={`secondary-button player-icon-button player-liked-control-button${playerDisplayKnownLiked ? " player-liked-control-button-active" : ""}`}
                disabled={!playerDisplayTrack}
                title={playerDisplayKnownLiked ? "Liked" : "Not liked"}
                type="button"
              >
                <span aria-hidden="true">{playerDisplayKnownLiked ? "★" : "☆"}</span>
              </button>
              <button
                aria-label={trackIsBookmarked(playerDisplayTrack) ? "Bookmarked track" : "Bookmark track"}
                aria-pressed={trackIsBookmarked(playerDisplayTrack)}
                className={`secondary-button player-icon-button player-bookmark-control-button${trackIsBookmarked(playerDisplayTrack) ? " player-bookmark-control-button-active" : ""}`}
                disabled={!playerDisplayTrack}
                onClick={() => toggleTrackBookmark(playerDisplayTrack, activePlayerBookmarkContext())}
                title={trackIsBookmarked(playerDisplayTrack) ? "Bookmarked" : "Bookmark"}
                type="button"
              >
                <svg aria-hidden="true" viewBox="0 0 20 20">
                  <path d="M5 3.5h10v13l-5-3.2-5 3.2v-13Z" />
                </svg>
              </button>
              <button
                aria-label="Previous track"
                className={`secondary-button player-icon-button${playerTransportReadOnly ? " player-control-readonly" : ""}`}
                disabled={playerPreviousDisabled}
                onClick={() => {
                  if (consumeQueueSkipHold()) {
                    return;
                  }
                  void movePlaybackQueue("previous");
                }}
                onPointerCancel={cancelQueueSkipHold}
                onPointerDown={() => startQueueSkipHold("previous")}
                onPointerLeave={cancelQueueSkipHold}
                onPointerUp={cancelQueueSkipHold}
                title={playerTransportTooltip ?? "Previous track"}
                type="button"
              >
                <svg aria-hidden="true" viewBox="0 0 24 24">
                  <path d="M7 5h2v14H7V5Zm3.7 7L19 5.8v12.4L10.7 12Z" />
                </svg>
              </button>
              <span title={playerTransportTooltip}>
                <button
                  className={`primary-button${playerTransportReadOnly ? " primary-button-readonly" : ""}`}
                  disabled={!previewInProgress && (!playerDisplayTrack || (!playerReady && !usingLivePlaybackSnapshot))}
                  onClick={() => void handlePlayerPrimaryButtonClick()}
                  type="button"
                >
                  {previewInProgress ? "Play" : (playerDisplayPaused ? "Play" : "Pause")}
                </button>
              </span>
              <button
                aria-label="Next track"
                className={`secondary-button player-icon-button${playerTransportReadOnly ? " player-control-readonly" : ""}`}
                disabled={playerNextDisabled}
                onClick={() => {
                  if (consumeQueueSkipHold()) {
                    return;
                  }
                  void movePlaybackQueue("next");
                }}
                onPointerCancel={cancelQueueSkipHold}
                onPointerDown={() => startQueueSkipHold("next")}
                onPointerLeave={cancelQueueSkipHold}
                onPointerUp={cancelQueueSkipHold}
                title={playerTransportTooltip ?? "Next track"}
                type="button"
              >
                <svg aria-hidden="true" viewBox="0 0 24 24">
                  <path d="M15 5h2v14h-2V5ZM5 5.8 13.3 12 5 18.2V5.8Z" />
                </svg>
              </button>
              <button
                aria-label={playerTrackLoopEnabled ? "Unloop current song" : "Loop current song"}
                aria-pressed={playerTrackLoopEnabled}
                className={`secondary-button player-icon-button player-track-loop-button${playerTrackLoopEnabled ? " player-icon-button-active player-icon-button-toggle-active" : ""}${playerTransportReadOnly ? " player-control-readonly" : ""}`}
                disabled={previewInProgress || !playerDisplayTrack}
                onClick={() => void handleTrackLoopClick()}
                title={playerTrackLoopEnabled ? "Unloop current song" : "Loop current song"}
                type="button"
              >
                <svg aria-hidden="true" viewBox="0 0 24 24">
                  <path d="M7 7h8.8L14 5.2l1.4-1.4L19.6 8l-4.2 4.2-1.4-1.4L15.8 9H7a3 3 0 0 0 0 6h1v2H7A5 5 0 0 1 7 7Zm10 10h-4v-2h4a3 3 0 0 0 0-6h-1V7h1a5 5 0 0 1 0 10Zm-6-1h2v2h-2v-2Z" />
                </svg>
              </button>
            </div>

            <div className="player-home-album">
              <HomeAlbumAppearanceStrip
                currentAlbum={{
                  name: playerDisplayAlbumName,
                  imageUrl: playerDisplayTrack?.image ?? playerDisplayKnownTrack?.image_url ?? null,
                  year: playerDisplayAlbumYear,
                  albumType: playerDisplayKnownTrack?.spotify_album_type ?? null,
                  onClick: openPlayerTrackDetails,
                }}
                recordingMembers={(homeRecordingCandidate?.members ?? []).filter((member) => member.release_track_id !== playerDisplayReleaseTrackId)}
                recordingMemberAlbumImageUrl={recordingMemberAlbumImageUrl}
                recordingMemberAlbumName={recordingMemberAlbumName}
                recordingMemberReleaseYear={recordingMemberReleaseYear}
                onMemberClick={(member) => openRecordingCandidateReleaseTrack(member, "recording")}
                sourceTrack={playerDisplayKnownTrack}
              />
              {false && homeAlbumExpanded ? (
                <div className="player-home-album-tracks detail-modal-album-tracks detail-modal-album-tracks-full">
                  <div className="detail-modal-album-header">
                    {hasPremiumPlayback ? (
                      <PlaybackActionMenu
                        ariaLabel="Album playback options"
                        buttonClassName="detail-album-play-all-button"
                        onAction={(action) => handleHomeAlbumPlayAll(action)}
                      >
                        Play all
                      </PlaybackActionMenu>
                    ) : (
                      <span aria-hidden="true" />
                    )}
                    <span className="detail-modal-album-title-header">{albumTracklistSummaryLabel(homeAlbumTrackEntries)}</span>
                    <button
                      className={`detail-modal-album-last-played-header detail-modal-album-sort-header${homeAlbumTrackLastSortMode ? " detail-modal-album-sort-header-active" : ""}`}
                      onClick={() => setHomeAlbumTrackLastSortMode((current) => nextLastPlayedSortMode(current))}
                      type="button"
                    >
                      Last
                      {homeAlbumTrackLastSortMode ? (
                        <span aria-hidden="true">{homeAlbumTrackLastSortMode === "recent" ? "↓" : "↑"}</span>
                      ) : null}
                    </button>
                  </div>
                  {homeAlbumTrackEntriesLoading ? (
                    <p className="detail-modal-preview-missing">Loading album songs...</p>
                  ) : null}
                  {!homeAlbumTrackEntriesLoading && homeAlbumTrackEntriesError ? (
                    <p className="detail-modal-preview-missing">{homeAlbumTrackEntriesError}</p>
                  ) : null}
                  {!homeAlbumTrackEntriesLoading && !homeAlbumTrackEntriesError && homeAlbumTrackEntries.length > 0 ? (
                    <div className="detail-album-track-list-wrap">
                      {selectedAlbumTrackMarkerTop(displayHomeAlbumTrackEntries, 7) ? (
                        <span
                          className="detail-album-track-scroll-marker"
                          style={{ "--detail-album-track-marker-top": selectedAlbumTrackMarkerTop(displayHomeAlbumTrackEntries, 7) } as CSSProperties}
                          aria-hidden="true"
                        />
                      ) : null}
                      <ul className="detail-album-track-list" ref={homeAlbumTrackListRef}>
                        {displayHomeAlbumTrackEntries.map((track) => {
                        const rowTrackUri = trackUriWithFallback(track.uri, track.id);
                        const rowIsCurrentTrack = Boolean(rowTrackUri && currentTrack?.uri === rowTrackUri);
                        const rowPlaying = isTrackPlaying(rowTrackUri);
                        const rowPreviewPlaying = Boolean(rowTrackUri && previewingTrackUri === rowTrackUri);
                        const rowPreviewActive = Boolean(rowPreviewPlaying && rowPlaying);
                        const rowPreviewKey = albumTrackPreviewKey(track, rowTrackUri);
                        const rowPreviewPlayed = previewPlayedTrackKeys.has(rowPreviewKey);
                        const rowPausedCurrent = Boolean(rowIsCurrentTrack && playbackPaused);
                        const rowLastPlayed = formatCompactRelativeAge(track.lastPlayedAt);
                        const rowIsUnlistened = !track.lastPlayedAt && track.playCount <= 0;
                        const rowIsLiked = albumTrackIsKnownLiked(track);
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
                        return (
                          <li className={`detail-album-track-row${track.isSelected ? " detail-album-track-row-selected" : ""}`} key={track.id ?? track.name}>
                            {hasPremiumPlayback ? (
                              <PlaybackActionMenu
                                ariaLabel={rowPlaying ? "Currently playing in ListenLab" : rowTrackUri ? `Play ${track.name} in ListenLab` : `${track.name} is not playable`}
                                buttonClassName={`secondary-button detail-album-track-play-button${rowPlaying ? " detail-icon-button-playing" : ""}`}
                                disabled={!rowTrackUri}
                                isPlaying={rowPlaying}
                                onAction={(action) => {
                                  const contextPreview = playerAlbumPreviewContext();
                                  const albumQueue = buildAlbumPlaybackQueue(rowTrackUri, homeAlbumTrackEntries, contextPreview);
                                  return handlePlaybackAction(action, {
                                    trackUri: rowTrackUri,
                                    optimisticTrack: playerSummaryFromAlbumTrack(track, contextPreview),
                                    queueCursor: albumQueue?.queueCursor,
                                    queueContext: albumQueue?.queueContext,
                                    queuePlaylistUris: albumQueue?.playlistUris,
                                    queueTracks: albumQueue?.queueTracks,
                                    sourceTrack: track.sourceTrack,
                                  }).then(() => {
                                    if (action === "play_now") {
                                      setHomeAlbumExpanded(true);
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
                            ) : null}
                            <button
                              className="detail-album-track-name-button single-line-ellipsis"
                              disabled={!rowTrackUri}
                              onClick={() => {
                                void handleHomeAlbumTrackPlay(track, rowTrackUri);
                              }}
                              type="button"
                            >
                              {rowIsLiked ? <LikedBadge className="detail-album-track-liked-badge" /> : null}
                              <span className="single-line-ellipsis">{track.name}</span>
                            </button>
                            {rowLastPlayed ? <span className="detail-album-track-last-played">{rowLastPlayed}</span> : <span className="detail-album-track-last-played">-</span>}
                          </li>
                        );
                        })}
                      </ul>
                    </div>
                  ) : null}
                </div>
              ) : null}
            </div>

            {usingLivePlaybackSnapshot && liveAwaitingNextTrack ? <p className="empty-copy">Track ended. Checking for the next song...</p> : null}
            {playerError ? <p className="empty-copy">{playerError}</p> : null}
          </div>

          <aside className="player-recent-column player-queue-column player-home-queue-column" aria-label={playerQueueSource === "listenlab" ? "ListenLab queue" : "Spotify queue"}>
            <div className="player-recent-header">
              <div className="player-queue-heading-menu">
                <button
                  aria-expanded={homeQueueHeaderMenuOpen}
                  className="player-queue-heading-button"
                  onClick={() => {
                    setHomeQueueHeaderMenuOpen((current) => !current);
                    setPlayerQueueSettingsOpen(false);
                    setPlayerQueuePauseMenuOpen(false);
                  }}
                  type="button"
                >
                  Queue
                </button>
                {homeQueueHeaderMenuOpen ? (
                  <div className="player-queue-settings-menu player-queue-context-menu">
                    <div className="player-queue-context-actions" aria-label="Queue controls">
                      <div className="player-queue-settings">
                        <button
                          aria-expanded={playerQueueSettingsOpen}
                          aria-label="Queue settings"
                          className={`player-queue-header-button${liveReadOnlyMode ? " player-control-readonly" : ""}`}
                          onClick={() => {
                            setPlayerQueueSettingsOpen((current) => !current);
                            setPlayerQueuePauseMenuOpen(false);
                          }}
                          title="Queue settings"
                          type="button"
                        >
                          <svg aria-hidden="true" viewBox="0 0 24 24">
                            <path d="M19.4 13.5c.1-.5.1-1 .1-1.5s0-1-.1-1.5l2-1.5-2-3.5-2.4 1a7.8 7.8 0 0 0-2.6-1.5L14 2h-4l-.4 3a7.8 7.8 0 0 0-2.6 1.5l-2.4-1-2 3.5 2 1.5c-.1.5-.1 1-.1 1.5s0 1 .1 1.5l-2 1.5 2 3.5 2.4-1a7.8 7.8 0 0 0 2.6 1.5l.4 3h4l.4-3a7.8 7.8 0 0 0 2.6-1.5l2.4 1 2-3.5-2-1.5ZM12 15.5a3.5 3.5 0 1 1 0-7 3.5 3.5 0 0 1 0 7Z" />
                          </svg>
                        </button>
                        {playerQueueSettingsOpen ? (
                          <div className="player-queue-settings-menu player-queue-context-settings-menu">
                            <button onClick={() => {
                              setPlayerQueueOrganizeMode((current) => !current);
                              setPlayerQueueSettingsOpen(false);
                            }} type="button">
                              {playerQueueOrganizeMode ? "Done organizing" : "Organize"}
                            </button>
                            <button disabled={playerQueueTracks.length === 0} onClick={saveCurrentQueueSnapshot} type="button">
                              Save current queue
                            </button>
                            <button className={liveReadOnlyMode ? "player-control-readonly" : undefined} disabled={playerQueueTracks.length === 0} onClick={() => void handleClearPlayerQueueClick()} type="button">
                              Clear queue
                            </button>
                          </div>
                        ) : null}
                      </div>
                    </div>
                    {homeQueueGroups.length > 0 ? homeQueueGroups.map((group, groupIndex) => {
                      const groupStartIndex = homeQueueGroups.slice(0, groupIndex).reduce((total, item) => total + item.tracks.length, 0);
                      const activeInGroup = hasActiveQueueCursor && activeQueueCursor >= groupStartIndex && activeQueueCursor < groupStartIndex + group.tracks.length;
                      return (
                        <button
                          className={`player-queue-context-item${activeInGroup ? " player-queue-context-item-active" : ""}`}
                          key={group.id}
                          onClick={() => void jumpToQueueGroup(groupStartIndex, group.id)}
                          type="button"
                        >
                          {group.imageUrl ? (
                            <img alt="" className="player-queue-context-image" src={group.imageUrl} />
                          ) : (
                            <span className="player-queue-context-image player-queue-context-image-fallback" aria-hidden="true">{group.label.slice(0, 1).toUpperCase()}</span>
                          )}
                          <span className="player-queue-context-copy">
                            <span className="single-line-ellipsis">{group.label}</span>
                            <span className="single-line-ellipsis">{homeQueueGroupMeta(group, groupStartIndex)}</span>
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
                {playerQueueLoading ? <span>Loading</span> : null}
                <button
                  aria-label={playerQueueLoopEnabled ? "Stop looping queue" : "Loop queue"}
                  aria-pressed={playerQueueLoopEnabled}
                  className={`player-queue-header-button${playerQueueLoopEnabled ? " player-queue-header-button-active player-queue-header-toggle-active" : ""}${liveReadOnlyMode ? " player-control-readonly" : ""}`}
                  disabled={playerQueueTracks.length === 0}
                  onClick={() => void handleQueueLoopClick()}
                  title={playerQueueLoopEnabled ? "Stop looping queue" : "Loop queue"}
                  type="button"
                >
                  <svg aria-hidden="true" viewBox="0 0 24 24">
                    <path d="M7 7h9.2l-1.8-1.8L15.8 3.8 20 8l-4.2 4.2-1.4-1.4L16.2 9H7a3 3 0 0 0 0 6h1v2H7A5 5 0 0 1 7 7Zm10 10H7.8l1.8 1.8-1.4 1.4L4 16l4.2-4.2 1.4 1.4L7.8 15H17a3 3 0 0 0 0-6h-1V7h1a5 5 0 0 1 0 10Z" />
                  </svg>
                </button>
                {renderQueueDelayControl()}
                <button
                  aria-label={playerQueueShuffleEnabled ? "Restore queue order" : "Shuffle unplayed queue"}
                  aria-pressed={playerQueueShuffleEnabled}
                  className={`player-queue-header-button${playerQueueShuffleEnabled ? " player-queue-header-button-active player-queue-shuffle-active" : ""}${liveReadOnlyMode ? " player-control-readonly" : ""}`}
                  disabled={!queueShuffleAvailable && !playerQueueShuffleEnabled}
                  onClick={() => void handleQueueShuffleClick()}
                  title={playerQueueShuffleEnabled ? "Restore queue order" : "Shuffle unplayed queue"}
                  type="button"
                >
                  <svg aria-hidden="true" viewBox="0 0 24 24">
                    <path d="M16.8 3.9 21 8.1l-4.2 4.2-1.4-1.4 1.8-1.8h-1.6c-2 0-3.4.8-4.5 2.4l-1.2 1.8c-1.4 2.1-3.4 3.2-5.9 3.2H3v-2h1c1.9 0 3.3-.8 4.3-2.4l1.2-1.8c1.5-2.1 3.5-3.2 6.1-3.2h1.6l-1.8-1.8 1.4-1.4ZM3 7.5h1c2.1 0 3.7.8 5 2.5l-1.2 1.8C6.8 10.3 5.6 9.5 4 9.5H3v-2Zm9.7 5.9c.8 1 1.8 1.6 3.1 1.6h1.4l-1.8-1.8 1.4-1.4L21 16l-4.2 4.2-1.4-1.4 1.8-1.8h-1.4c-2 0-3.6-.8-4.8-2.3l1.1-1.7.6.4Z" />
                  </svg>
                </button>
              </div>
            </div>
            {playerQueueOrganizeMode ? (
              <div className="player-queue-organize-bar">
                <label>
                  <span>Sort</span>
                  <select value={playerQueueSortMode} onChange={(event) => sortPlayerQueue(event.currentTarget.value as typeof playerQueueSortMode)}>
                    <option value="custom">Custom</option>
                    <option value="length">Length</option>
                    <option value="az">A-Z</option>
                    <option value="recent">Recently played</option>
                  </select>
                </label>
                <label>
                  <span>Group by</span>
                  <select value={playerQueueGroupMode} onChange={(event) => groupPlayerQueue(event.currentTarget.value as typeof playerQueueGroupMode)}>
                    <option value="custom">Custom</option>
                    <option value="artist">Artist</option>
                    <option value="album">Album</option>
                  </select>
                </label>
              </div>
            ) : null}
            <div className="player-recent-list" ref={homeQueueListRef}>
              {homeQueueGroups.map((group, groupIndex) => {
                const groupStartIndex = homeQueueGroups.slice(0, groupIndex).reduce((total, item) => total + item.tracks.length, 0);
                const groupEndIndex = groupStartIndex + group.tracks.length - 1;
                const groupIsOpen = homeQueueOpenGroupIds.has(group.id);
                const activeInGroup = hasActiveQueueCursor && activeQueueCursor >= groupStartIndex && activeQueueCursor <= groupEndIndex;
                return (
                  <div className="player-queue-group-wrap" key={group.id}>
                    <div className="player-queue-group">
                      <button
                        aria-expanded={groupIsOpen}
                        className="player-queue-group-header"
                        onClick={() => setHomeQueueOpenGroupIds((current) => {
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
                          <span className="player-queue-group-meta single-line-ellipsis">{homeQueueGroupMeta(group, groupStartIndex)}</span>
                        </span>
                        {activeInGroup ? <span className="player-queue-current-dot" aria-label="Current queue context" /> : null}
                      </button>
                    </div>
                    {groupIsOpen ? group.tracks.map((track, groupTrackIndex) => (
                      renderHomeQueueTrackRow(track, groupStartIndex + groupTrackIndex, bookmarkContextFromQueueGroup(group, groupTrackIndex))
                    )) : null}
                  </div>
                );
              })}
              {!playerQueueLoading && playerQueueTracks.length === 0 ? <p className="empty-copy player-recent-empty">No queued songs were returned.</p> : null}
              {playerQueueError ? <p className="empty-copy player-recent-empty">{playerQueueError}</p> : null}
            </div>
          </aside>
        </div>
        <PlayerBottomDrawer
          activeTab={playerDrawerActiveTab}
          expanded={playerDrawerExpanded}
          savedQueues={savedPlayerQueues}
          trackBookmarks={trackBookmarks}
          entityBookmarks={entityBookmarks}
          onDeleteSavedQueue={deleteSavedPlayerQueue}
          onDeleteBookmark={removeTrackBookmark}
          onDeleteEntityBookmark={removeEntityBookmark}
          onOpenBookmark={openTrackBookmarkContext}
          onOpenEntityBookmark={openEntityBookmark}
          onPlayBookmark={(action, bookmark) => void playTrackBookmark(action, bookmark)}
          onRestoreSavedQueue={restoreSavedPlayerQueue}
          onTabChange={setPlayerDrawerActiveTab}
          onToggle={() => setPlayerDrawerExpanded((current) => !current)}
        />
      </section>
    );
  }

  const likedTracksForActivity = useMemo(() => {
    const likedAtMs = (track: RecentTrack) => {
      const timestamp = parseTimestampMs(track.liked_at ?? track.spotify_played_at ?? null);
      return timestamp ?? 0;
    };
    const ordered = [...likedTracksForActivitySource].sort((left, right) => {
      const delta = likedAtMs(right) - likedAtMs(left);
      return likedTracksSortMode === "recent" ? delta : -delta;
    });
    const baseRows = likedTracksCountMode === "all"
      ? ordered
      : ordered.slice(0, LIKED_TRACKS_RECENT_DISPLAY_LIMIT);
    if (!likedTracksShuffleEnabled) {
      return baseRows;
    }
    const shufflePool = likedTracksCountMode === "all"
      ? ordered
      : ordered.slice(0, LIKED_TRACKS_SHUFFLE_POOL_LIMIT);
    const shuffled = [...shufflePool];
    for (let index = shuffled.length - 1; index > 0; index -= 1) {
      const swapIndex = Math.floor(Math.random() * (index + 1));
      [shuffled[index], shuffled[swapIndex]] = [shuffled[swapIndex], shuffled[index]];
    }
    return likedTracksCountMode === "all"
      ? shuffled
      : shuffled.slice(0, LIKED_TRACKS_RECENT_DISPLAY_LIMIT);
  }, [
    likedTracksCountMode,
    likedTracksForActivitySource,
    likedTracksShuffleEnabled,
    likedTracksShuffleNonce,
    likedTracksSortMode,
  ]);
  const likedTracksAvailableForActivity = likedTracksForActivitySource.length > 0 || Boolean(profile?.recent_likes_available);
  const releaseTrackMetadataIds = useMemo(() => {
    const ids = new Set<string>();
    const addRecentTrack = (track: RecentTrack | null | undefined) => {
      if (track?.track_id) {
        ids.add(track.track_id);
      }
    };
    [
      ...(profile?.recent_tracks ?? []),
      ...(profile?.top_tracks ?? []),
      ...(profile?.recent_top_tracks ?? []),
      ...(profile?.recent_likes_tracks ?? []),
      ...mergedTracks,
    ].forEach(addRecentTrack);
    albumTrackEntries.forEach((track) => {
      if (track.id) {
        ids.add(track.id);
      }
    });
    homeAlbumTrackEntries.forEach((track) => {
      if (track.id) {
        ids.add(track.id);
      }
    });
    playerQueueTracks.forEach((track) => {
      if (track.trackId) {
        ids.add(track.trackId);
      }
    });
    if (playerDisplayTrackId) {
      ids.add(playerDisplayTrackId);
    }
    if (selectedPreview?.kind === "track" && selectedPreview.trackId) {
      ids.add(selectedPreview.trackId);
    }
    addRecentTrack(playerDisplayKnownTrack);
    return Array.from(ids).sort();
  }, [
    albumTrackEntries,
    homeAlbumTrackEntries,
    mergedTracks,
    playerDisplayKnownTrack,
    playerDisplayTrackId,
    playerQueueTracks,
    profile?.recent_likes_tracks,
    profile?.recent_top_tracks,
    profile?.recent_tracks,
    profile?.top_tracks,
    selectedPreview,
  ]);
  const releaseTrackMetadataKey = releaseTrackMetadataIds.join("|");
  useEffect(() => {
    if (!releaseTrackMetadataIds.length || !profile) {
      return;
    }
    const missingIds = releaseTrackMetadataIds.filter((trackId) => (
      !releaseTrackMetadataCheckedIds[trackId]
      && !releaseTrackMetadataInFlightIdsRef.current.has(trackId)
    ));
    if (!missingIds.length) {
      return;
    }
    const requestIds = missingIds.slice(0, 500);
    requestIds.forEach((trackId) => releaseTrackMetadataInFlightIdsRef.current.add(trackId));
    fetchReleaseTrackMetadata(requestIds)
      .then((payload) => {
        setReleaseTrackMetadataById((current) => ({
          ...current,
          ...payload.items,
        }));
        setReleaseTrackMetadataCheckedIds((current) => {
          const next = { ...current };
          for (const trackId of requestIds) {
            next[trackId] = true;
          }
          return next;
        });
      })
      .catch(() => {
        console.warn("Release-track metadata lookup failed.");
        setReleaseTrackMetadataCheckedIds((current) => {
          const next = { ...current };
          for (const trackId of requestIds) {
            next[trackId] = true;
          }
          return next;
        });
      })
      .finally(() => {
        requestIds.forEach((trackId) => releaseTrackMetadataInFlightIdsRef.current.delete(trackId));
      });
  }, [profile, releaseTrackMetadataCheckedIds, releaseTrackMetadataIds, releaseTrackMetadataKey]);
  const releaseTrackSiblingById = useMemo(() => {
    const byId = new Map<string, number>();
    for (const [trackId, metadata] of Object.entries(releaseTrackMetadataById)) {
      if (metadata.has_release_track_siblings) {
        byId.set(trackId, metadata.release_track_source_count);
      }
    }
    for (const track of [...albumTrackEntries, ...homeAlbumTrackEntries]) {
      if (track.id && track.hasReleaseTrackSiblings) {
        byId.set(track.id, track.releaseTrackSourceCount);
      }
    }
    return byId;
  }, [albumTrackEntries, homeAlbumTrackEntries, releaseTrackMetadataById]);
  const releaseSiblingSourceCountForTrackId = (trackId: string | null | undefined) =>
    trackId ? (releaseTrackSiblingById.get(trackId) ?? 0) : 0;
  const hasReleaseSiblingForTrackId = (trackId: string | null | undefined) =>
    releaseSiblingSourceCountForTrackId(trackId) > 1;
  const mergeReleaseMetadataIntoTrack = (track: RecentTrack): RecentTrack => {
    const trackId = track.track_id;
    const metadata = trackId ? releaseTrackMetadataById[trackId] : null;
    if (!metadata) {
      return track;
    }
    return {
      ...track,
      release_track_id: track.release_track_id ?? metadata.release_track_id,
      release_track_name: track.release_track_name ?? metadata.release_track_name,
      release_track_source_count: track.release_track_source_count ?? metadata.release_track_source_count,
      release_track_duplicate_source_count: track.release_track_duplicate_source_count ?? metadata.release_track_duplicate_source_count,
      has_release_track_siblings: track.has_release_track_siblings ?? metadata.has_release_track_siblings,
      release_track_cluster_candidate_type: track.release_track_cluster_candidate_type ?? metadata.release_track_cluster_candidate_type,
      release_track_cluster_relationship_kind: track.release_track_cluster_relationship_kind ?? metadata.release_track_cluster_relationship_kind,
      recording_release_track_ids: track.recording_release_track_ids ?? metadata.recording_release_track_ids,
    };
  };
  const allTimeTopTrackSource = mergedTracksLoaded && mergedTracks.length > 0
    ? mergedTracks
    : (profile?.top_tracks ?? []);
  const allTimeTopTracks = useMemo(() => (
    sortedTracksForView(
      "tracksAllTime",
      allTimeTopTrackSource.map(mergeReleaseMetadataIntoTrack),
      trackRankingMode,
    )
  ), [allTimeTopTrackSource, releaseTrackMetadataById, trackRankingMode]);
  const allTimeTopTracksAvailableForDisplay = allTimeTopTracks.length > 0 || mergedTracksLoading || Boolean(profile?.top_tracks_available);
  const recentTopTracksAreSpotify = (tracks: RecentTrack[]) => {
    if (tracks.length === 0) {
      return false;
    }
    const sourceLabels = tracks
      .map((track) => track.top_tracks_source)
      .filter((source): source is "spotify" | "history" | "db" => Boolean(source));
    const hasLocalEvidence = tracks.some((track) => (
      track.top_tracks_source === "history"
      || track.top_tracks_source === "db"
      || track.source_label === "history"
      || track.source_label === "both"
      || Boolean(track.first_played_at || track.last_played_at)
      || typeof track.recent_play_count === "number"
      || typeof track.all_time_play_count === "number"
    ));
    return (
      (sourceLabels.length > 0 && sourceLabels.every((source) => source === "spotify"))
      || (sourceLabels.length === 0 && !hasLocalEvidence)
    );
  };
  const computedRecentTopTracks = useMemo(() => {
    const knownTrackByKey = new Map<string, RecentTrack>();
    [
      ...allTimeTopTracks,
      ...(profile?.recent_top_tracks ?? []),
      ...(profile?.recent_tracks ?? []),
      ...(profile?.recent_likes_tracks ?? []),
    ].forEach((track) => {
      knownTrackByKey.set(normalizedTrackArtistKey(track.track_name, track.artist_name), track);
      if (track.track_id) {
        knownTrackByKey.set(track.track_id, track);
      }
    });
    return [...recentComputedTracks]
      .filter((track) => Number(track.recent_play_count ?? 0) > 0)
      .map((track) => {
        const knownTrack = knownTrackByKey.get(track.track_id ?? "") ?? knownTrackByKey.get(normalizedTrackArtistKey(track.track_name, track.artist_name));
        return {
          ...track,
          ...mergeReleaseMetadataIntoTrack(track),
          top_tracks_source: "db" as const,
          album_name: track.album_name ?? knownTrack?.album_name ?? null,
          album_id: track.album_id ?? knownTrack?.album_id ?? null,
          album_url: track.album_url ?? knownTrack?.album_url ?? null,
          image_url: track.image_url ?? knownTrack?.image_url ?? null,
          url: track.url ?? knownTrack?.url ?? null,
          uri: track.uri ?? knownTrack?.uri ?? null,
          artists: track.artists ?? knownTrack?.artists ?? null,
        };
      })
      .sort((left, right) => {
        const recentDelta = Number(right.recent_play_count ?? 0) - Number(left.recent_play_count ?? 0);
        if (recentDelta !== 0) {
          return recentDelta;
        }
        const rightLast = parseTimestampMs(right.last_played_at) ?? 0;
        const leftLast = parseTimestampMs(left.last_played_at) ?? 0;
        if (rightLast !== leftLast) {
          return rightLast - leftLast;
        }
        return String(left.track_name ?? "").localeCompare(String(right.track_name ?? ""));
      })
      .slice(0, Math.max(10, profile?.recent_top_tracks?.length ?? 10));
  }, [allTimeTopTracks, profile?.recent_likes_tracks, profile?.recent_top_tracks, profile?.recent_tracks, recentComputedTracks, releaseTrackMetadataById]);
  const profileRecentTopTracksAreSpotify = recentTopTracksAreSpotify(profile?.recent_top_tracks ?? []);
  const recentTopTracksForDisplay = recentTopTracksUseSpotify
    ? (profile?.recent_top_tracks ?? []).map(mergeReleaseMetadataIntoTrack)
    : (
        profileRecentTopTracksAreSpotify
          ? computedRecentTopTracks
          : (profile?.recent_top_tracks ?? []).map(mergeReleaseMetadataIntoTrack)
      );
  const recentTopTracksAvailableForDisplay = recentTopTracksUseSpotify
    ? Boolean(profile?.recent_top_tracks_available)
    : recentTopTracksForDisplay.length > 0 || recentComputedTracksLoading;
  const recentTrackHasRelationTags = (track: RecentTrack | null | undefined) =>
    Boolean(
      track
      && (
        track.has_release_track_siblings
        || hasReleaseSiblingForTrackId(track.track_id)
        || Number(track.release_track_duplicate_source_count ?? 0) > 1
        || track.release_track_cluster_candidate_type
      ),
    );
  const queueTrackHasRelationTags = (track: PlayerQueueTrack | null | undefined) =>
    Boolean(
      track
      && (
        track.hasReleaseTrackSiblings
        || hasReleaseSiblingForTrackId(track.trackId)
        || Number(track.releaseTrackDuplicateSourceCount ?? 0) > 1
        || track.releaseTrackClusterCandidateType
      ),
    );
  const selectedPreviewReleaseSiblingSourceCount = selectedPreview?.hasReleaseTrackSiblings
    ? selectedPreview.releaseTrackSourceCount ?? 0
    : releaseSiblingSourceCountForTrackId(selectedPreview?.trackId ?? selectedPreview?.sourceTrack?.track_id);
  const selectedPreviewHasReleaseSibling = selectedPreviewReleaseSiblingSourceCount > 1
    || Number(selectedPreview?.releaseTrackDuplicateSourceCount ?? 0) > 1
    || Boolean(selectedPreview?.releaseTrackClusterCandidateType);
  const selectedPreviewReleaseSiblingNote = selectedPreviewHasReleaseSibling
    ? `Grouped with ${selectedPreviewReleaseSiblingSourceCount} source ${selectedPreviewReleaseSiblingSourceCount === 1 ? "version" : "versions"}`
    : null;
  const selectedPreviewCanonicalTrackTitle = selectedPreviewReleaseTrackDetailReady?.release_track.name?.trim() || null;
  const selectedPreviewCurrentSpotifyTrackId = selectedPreview?.kind === "track"
    ? selectedPreview.trackId ?? spotifyTrackIdFromUri(selectedPreview.trackUri)
    : null;
  const selectedPreviewRecordingCandidateForCurrent = selectedPreview?.kind === "track"
    && selectedPreviewRecordingCandidate?.members.some((member) => member.release_track_id === selectedPreview.releaseTrackId)
    ? selectedPreviewRecordingCandidate
    : null;
  const selectedPreviewRecordingMembers = selectedPreviewRecordingCandidateForCurrent?.members ?? [];
  const selectedPreviewRelationAnchorReleaseTrackIds = new Set([
    ...(selectedPreview?.kind === "track" && selectedPreview.releaseTrackId ? [selectedPreview.releaseTrackId] : []),
    ...selectedPreviewRecordingMembers.map((member) => member.release_track_id),
  ]);
  const selectedPreviewRelatedCandidatesForCurrent = selectedPreview?.kind === "track"
    ? selectedPreviewRelatedCandidates.filter((candidate) => (
      candidate.members.some((member) => selectedPreviewRelationAnchorReleaseTrackIds.has(member.release_track_id))
    ))
    : [];
  const selectedPreviewRecordingRepresentative = selectedPreviewRecordingCandidateForCurrent
    ? selectedPreviewRecordingMembers.find((member) => member.release_track_id === selectedPreviewRecordingCandidateForCurrent.representative.release_track_id) ?? selectedPreviewRecordingMembers[0] ?? null
    : null;
  const selectedPreviewHasRecordingView = selectedPreviewRecordingMembers.length > 1;
  const selectedPreviewOtherRecordingMembers = selectedPreview && selectedPreview.kind === "track"
    ? selectedPreviewRecordingMembers.filter((member) => member.release_track_id !== selectedPreview.releaseTrackId)
    : [];
  const selectedPreviewAlsoAppearsOnRecordingMembers = selectedAlbumFamilyContext
    ? selectedPreviewOtherRecordingMembers.filter((member) => !member.release_album_ids?.some(
      (releaseAlbumId) => selectedAlbumFamilyContext.release_album_ids.includes(releaseAlbumId),
    ))
    : selectedPreviewOtherRecordingMembers;
  const selectedPreviewCurrentRecordingMember = selectedPreview?.kind === "track"
    ? selectedPreviewRecordingMembers.find((member) => member.release_track_id === selectedPreview.releaseTrackId) ?? null
    : null;
  const selectedPreviewIsReleaseDetailView = selectedPreview?.kind === "track" && selectedPreviewDetailView === "release";
  const selectedPreviewRecordingStarTrackIds = selectedPreview?.kind === "track"
    ? Array.from(new Set([
      selectedPreviewStarTrackId,
      selectedPreview.trackId,
      spotifyTrackIdFromUri(selectedPreview.trackUri),
      selectedPreview.sourceTrack?.track_id,
      ...selectedPreviewReleaseSourceVersionsRaw.map((version) => version.spotify_track_id ?? null),
      ...selectedPreviewRecordingMembers.flatMap((member) => member.source_track_ids ?? []),
      ...selectedPreviewRecordingMembers.flatMap((member) => (member.source_track_uris ?? []).map((uri) => spotifyTrackIdFromUri(uri))),
    ].map((trackId) => String(trackId ?? "").trim()).filter(Boolean)))
    : [];
  const selectedPreviewBaseKnownLiked = Boolean(
    selectedPreview?.kind === "track"
    && (
      (
        (!selectedPreviewStarTrackId || localStarredTrackById[selectedPreviewStarTrackId] !== false)
        && recentTrackIsKnownLiked(selectedPreview.sourceTrack, selectedPreview.trackId)
      )
      || selectedPreviewRecordingStarTrackIds.some((trackId) => {
        const localStarred = localStarredTrackById[trackId];
        if (localStarred != null) {
          return localStarred;
        }
        return likedTrackIdsForDisplay.has(trackId) || Boolean(targetedLikedTrackById[trackId]);
      })
    ),
  );
  const selectedPreviewCurrentVersionTrackIds = selectedPreview?.kind === "track"
    ? selectedPreviewIsReleaseDetailView
      ? Array.from(new Set([
        selectedPreviewReleasePlaybackSourceVersion?.spotify_track_id ?? null,
      ].map((trackId) => String(trackId ?? "").trim()).filter(Boolean)))
      : Array.from(new Set([
      selectedPreviewStarTrackId,
      selectedPreviewCurrentSpotifyTrackId,
      selectedPreview.trackId,
      spotifyTrackIdFromUri(selectedPreview.trackUri),
      spotifyTrackIdFromUri(selectedPreviewPlaybackTrackUri),
      selectedPreviewReleasePlaybackSourceVersion?.spotify_track_id ?? null,
      selectedPreviewMatchedAlbumTrack?.id ?? null,
    ].map((trackId) => String(trackId ?? "").trim()).filter(Boolean)))
    : [];
  const selectedPreviewCurrentVersionIsSpotifyLiked = selectedPreview?.kind === "track"
    ? selectedPreviewCurrentVersionTrackIds.some((trackId) => (
      likedTrackIdsForDisplay.has(trackId)
      || Boolean(targetedLikedTrackById[trackId])
      || Boolean(
        selectedPreview.sourceTrack
        && selectedPreview.sourceTrack.track_id === trackId
        && (
          selectedPreview.sourceTrack.is_liked === true
          || selectedPreview.sourceTrack.source_label === "liked_cache"
        ),
      )
    ))
    : false;
  const selectedPreviewCurrentVersionIsLocallyStarred = selectedPreview?.kind === "track"
    ? selectedPreviewCurrentVersionTrackIds.some((trackId) => localStarredTrackById[trackId] === true)
    : false;
  const selectedPreviewCurrentVersionIsKnownLiked = selectedPreviewCurrentVersionIsLocallyStarred || selectedPreviewCurrentVersionIsSpotifyLiked;
  const selectedPreviewIsKnownLiked = selectedPreviewIsReleaseDetailView
    ? selectedPreviewCurrentVersionIsKnownLiked
    : selectedPreviewBaseKnownLiked;
  const selectedPreviewAlbumContextTagLabel = selectedPreview?.kind === "track"
    ? albumContextTagLabel(
      selectedPreviewReleasePlaybackSourceVersion?.album_type
        ?? selectedPreview.sourceTrack?.spotify_album_type
        ?? selectedPreviewCurrentRecordingMember?.album_types?.[0]
        ?? null,
      selectedPreviewReleasePlaybackSourceVersion?.album_name
        ?? selectedPreview.sourceAlbumName
        ?? selectedPreview.detail
        ?? selectedPreviewCurrentRecordingMember?.album
        ?? null,
    )
    : null;
  const selectedPreviewReleaseSourceListenCount = selectedPreview?.kind === "track"
    ? Math.max(
      0,
      Number(
        selectedPreviewReleaseTrackDetailReady?.listen_counts?.release_track_play_count
        ?? selectedPreviewReleaseTrackDetailReady?.listen_counts?.source_versions_play_count
        ?? selectedPreviewReleaseSourceVersions.reduce((total, version) => total + Math.max(0, Number(version.play_count ?? 0) || 0), 0),
      ) || 0,
    )
    : 0;
  const selectedPreviewRecordingBackendListenCount = selectedPreview?.kind === "track"
    ? Math.max(0, Number(selectedPreviewRecordingCandidateForCurrent?.listen_counts?.recording_total_play_count ?? 0) || 0)
    : 0;
  const selectedPreviewRecordingMemberListenCount = selectedPreview?.kind === "track"
    ? selectedPreviewRecordingMembers.reduce((total, member) => total + Math.max(0, Number(member.play_count ?? 0) || 0), 0)
    : 0;
  const selectedPreviewRecordingListenCount = selectedPreviewRecordingBackendListenCount > 0
    ? selectedPreviewRecordingBackendListenCount
    : selectedPreviewRecordingMemberListenCount > 0
      ? selectedPreviewRecordingMemberListenCount
      : selectedPreviewReleaseSourceListenCount;
  const selectedPreviewCurrentSourceVersionListenCount = Math.max(
    0,
    Number(
      selectedPreviewReleasePlaybackSourceVersion?.play_count
      ?? selectedPreviewReleaseTrackDetailReady?.listen_counts?.playback_source_play_count
      ?? 0,
    ) || 0,
  );
  const selectedPreviewListenCount = selectedPreviewIsReleaseDetailView
    ? selectedPreviewCurrentSourceVersionListenCount
    : selectedPreviewRecordingListenCount;
  const selectedPreviewThisAlbumListenCount = selectedPreviewIsReleaseDetailView
    ? selectedPreviewCurrentSourceVersionListenCount
    : selectedPreviewCurrentSourceVersionListenCount > 0
      ? selectedPreviewCurrentSourceVersionListenCount
      : selectedPreviewCurrentRecordingMember
        ? Math.max(0, Number(selectedPreviewCurrentRecordingMember.play_count ?? 0) || 0)
        : selectedPreviewReleaseSourceListenCount;
  const selectedPreviewListenCountLabel = selectedPreviewListenCount > 0
    ? `${selectedPreviewListenCount.toLocaleString()} ${selectedPreviewListenCount === 1 ? "listen" : "listens"}`
    : selectedPreviewCachedSummary?.listenCountLabel ?? null;
  const selectedPreviewListenBreakdown = selectedPreview?.kind === "track" && selectedPreviewListenCount > 0
    ? selectedPreviewIsReleaseDetailView
      ? null
      : {
      thisAlbumCount: selectedPreviewThisAlbumListenCount,
      otherAlbumsCount: Math.max(0, selectedPreviewListenCount - selectedPreviewThisAlbumListenCount),
    }
    : null;
  const firstTimestamp = (values: Array<string | null | undefined>) => values.reduce<string | null>((earliest, value) => {
    const valueMs = parseTimestampMs(value);
    if (valueMs == null) {
      return earliest;
    }
    const earliestMs = parseTimestampMs(earliest);
    return earliestMs == null || valueMs < earliestMs ? value ?? null : earliest;
  }, null);
  const lastTimestamp = (values: Array<string | null | undefined>) => values.reduce<string | null>((latest, value) => {
    const valueMs = parseTimestampMs(value);
    if (valueMs == null) {
      return latest;
    }
    const latestMs = parseTimestampMs(latest);
    return latestMs == null || valueMs > latestMs ? value ?? null : latest;
  }, null);
  const selectedPreviewOtherRecordingMembersForBreakdown = selectedPreview?.kind === "track"
    ? selectedPreviewRecordingMembers.filter((member) => member.release_track_id !== selectedPreview.releaseTrackId)
    : [];
  const selectedPreviewOtherReleaseSourceVersionsForBreakdown = selectedPreview?.kind === "track"
    ? selectedPreviewReleaseSourceVersions.filter((version) => (
      version.spotify_track_id
      && version.spotify_track_id !== selectedPreviewReleasePlaybackSourceVersion?.spotify_track_id
      && version.spotify_track_id !== selectedPreviewCurrentSpotifyTrackId
    ))
    : [];
  const selectedPreviewThisAlbumFirstListenedAt = selectedPreview?.kind === "track"
    ? selectedPreviewIsReleaseDetailView
      ? selectedPreviewReleasePlaybackSourceVersion?.first_played_at ?? null
      : firstTimestamp([
        selectedPreviewReleasePlaybackSourceVersion?.first_played_at,
        selectedPreviewMatchedAlbumTrack?.sourceTrack?.first_played_at,
        selectedPreview.sourceTrack?.first_played_at,
        selectedPreviewCurrentRecordingMember?.first_played_at,
      ])
    : null;
  const selectedPreviewThisAlbumLastListenedAt = selectedPreview?.kind === "track"
    ? selectedPreviewIsReleaseDetailView
      ? selectedPreviewReleasePlaybackSourceVersion?.last_played_at ?? null
      : lastTimestamp([
        selectedPreviewReleasePlaybackSourceVersion?.last_played_at,
        selectedPreviewMatchedAlbumTrack?.lastPlayedAt,
        selectedPreviewMatchedAlbumTrack?.sourceTrack?.last_played_at,
        selectedPreview.sourceTrack?.last_played_at,
        selectedPreview.sourceTrack?.spotify_played_at,
        selectedPreviewCurrentRecordingMember?.last_played_at,
      ])
    : null;
  const selectedPreviewOtherAlbumFirstListenedAt = selectedPreviewIsReleaseDetailView
    ? firstTimestamp(selectedPreviewOtherReleaseSourceVersionsForBreakdown.map((version) => version.first_played_at))
    : firstTimestamp(selectedPreviewOtherRecordingMembersForBreakdown.map((member) => member.first_played_at));
  const selectedPreviewOtherAlbumLastListenedAt = selectedPreviewIsReleaseDetailView
    ? lastTimestamp(selectedPreviewOtherReleaseSourceVersionsForBreakdown.map((version) => version.last_played_at))
    : lastTimestamp(selectedPreviewOtherRecordingMembersForBreakdown.map((member) => member.last_played_at));
  const selectedPreviewHasOtherAlbumsForBottomBreakdown = selectedPreviewIsReleaseDetailView
    ? false
    : selectedPreviewOtherRecordingMembersForBreakdown.length > 0;
  const selectedPreviewAllFirstListenedAt = selectedPreviewIsReleaseDetailView
    ? selectedPreviewThisAlbumFirstListenedAt
    : firstTimestamp([
      selectedPreviewThisAlbumFirstListenedAt,
      selectedPreviewOtherAlbumFirstListenedAt,
    ]);
  const selectedPreviewAllLastListenedAt = selectedPreviewIsReleaseDetailView
    ? selectedPreviewThisAlbumLastListenedAt
    : lastTimestamp([
      selectedPreviewThisAlbumLastListenedAt,
      selectedPreviewOtherAlbumLastListenedAt,
    ]);
  const selectedPreviewFirstListenedLabel = selectedPreview?.kind === "track"
    ? formatMonthDay(selectedPreviewAllFirstListenedAt, true)
    : null;
  const selectedPreviewLastListenedLabel = selectedPreview?.kind === "track"
    ? (formatMonthDay(selectedPreviewAllLastListenedAt, true) ?? selectedPreviewCachedSummary?.lastListenedLabel ?? null)
    : null;
  const selectedPreviewListenedRangeLabel = selectedPreviewFirstListenedLabel && selectedPreviewLastListenedLabel
    ? `${selectedPreviewFirstListenedLabel} - ${selectedPreviewLastListenedLabel}`
    : selectedPreviewFirstListenedLabel ?? selectedPreviewLastListenedLabel;
  const selectedPreviewListenedBreakdown = selectedPreview?.kind === "track" && selectedPreviewHasOtherAlbumsForBottomBreakdown && (selectedPreviewOtherAlbumFirstListenedAt || selectedPreviewOtherAlbumLastListenedAt)
    ? {
      thisAlbumFirstLabel: formatMonthDay(selectedPreviewThisAlbumFirstListenedAt, true),
      thisAlbumLastLabel: formatMonthDay(selectedPreviewThisAlbumLastListenedAt, true),
      otherAlbumsFirstLabel: formatMonthDay(selectedPreviewOtherAlbumFirstListenedAt, true),
      otherAlbumsLastLabel: formatMonthDay(selectedPreviewOtherAlbumLastListenedAt, true),
    }
    : null;

  useEffect(() => {
    if (!selectedPreviewSummaryCacheKey || selectedPreview?.kind !== "track") {
      return;
    }
    const nextSummary = {
      durationLabel: selectedPreviewTrackDurationLabel,
      lastListenedLabel: selectedPreviewLastListenedLabel,
      listenCountLabel: selectedPreviewListenCountLabel ?? selectedPreviewCachedSummary?.listenCountLabel ?? null,
    };
    if (!nextSummary.durationLabel && !nextSummary.lastListenedLabel && !nextSummary.listenCountLabel) {
      return;
    }
    setTrackSummaryChipCache((current) => {
      const existing = current[selectedPreviewSummaryCacheKey] ?? {};
      if (
        existing.durationLabel === nextSummary.durationLabel
        && existing.lastListenedLabel === nextSummary.lastListenedLabel
        && existing.listenCountLabel === nextSummary.listenCountLabel
      ) {
        return current;
      }
      return {
        ...current,
        [selectedPreviewSummaryCacheKey]: {
          ...existing,
          ...nextSummary,
        },
      };
    });
  }, [
    selectedPreview?.kind,
    selectedPreviewCachedSummary?.listenCountLabel,
    selectedPreviewLastListenedLabel,
    selectedPreviewListenCountLabel,
    selectedPreviewSummaryCacheKey,
    selectedPreviewTrackDurationLabel,
  ]);

  const selectedPreviewRecordingVariationCount = selectedPreviewOtherRecordingMembers.length;
  const selectedPreviewReleaseAlbumVariationCount = selectedPreviewReleaseSourceVersions.length + selectedPreviewOtherRecordingMembers.length;
  const selectedPreviewReleaseSourceVersionNeedsArrows = selectedPreviewReleaseAlbumVariationCount > 3;
  const selectedPreviewAlreadyShownRelationReleaseTrackIds = new Set([
    ...(selectedPreview?.kind === "track" && selectedPreview.releaseTrackId ? [selectedPreview.releaseTrackId] : []),
    ...selectedPreviewOtherRecordingMembers.map((member) => member.release_track_id),
  ]);
  const selectedPreviewRecordingRepresentativeByReleaseTrackId = new Map<number, RecordingTrackCandidateMember>();
  for (const candidate of selectedPreviewRelatedCandidates) {
    if (candidate.candidate_type !== "recording_track_candidate") {
      continue;
    }
    const representative = candidate.members.find(
      (member) => member.release_track_id === candidate.representative.release_track_id,
    ) ?? candidate.members[0];
    if (!representative) {
      continue;
    }
    for (const member of candidate.members) {
      selectedPreviewRecordingRepresentativeByReleaseTrackId.set(member.release_track_id, representative);
    }
  }
  type SongFamilyKind = "Original" | "Cover" | "Remix" | "Version" | "Rework" | "Live" | "Demo" | "Acoustic" | "Instrumental";
  const songFamilyKind = (
    member: RecordingTrackCandidateMember,
    original: RecordingTrackCandidateMember,
  ): SongFamilyKind => {
    if (member.release_track_id === original.release_track_id) {
      return "Original";
    }
    const normalizedTitle = member.title.toLocaleLowerCase();
    if (/\blive\b/.test(normalizedTitle)) {
      return "Live";
    }
    if (/\bdemo\b/.test(normalizedTitle)) {
      return "Demo";
    }
    if (/\bacoustic\b/.test(normalizedTitle)) {
      return "Acoustic";
    }
    if (/\binstrumental\b/.test(normalizedTitle)) {
      return "Instrumental";
    }
    if (/\b(?:remix|rmx)\b/.test(normalizedTitle)) {
      return "Remix";
    }
    if (/\brework\b/.test(normalizedTitle)) {
      return "Rework";
    }
    if (/\bcover\b/.test(normalizedTitle)) {
      return "Cover";
    }
    const originalArtistNames = new Set(
      (original.artists ?? []).map((artist) => artist.name?.trim().toLocaleLowerCase()).filter(Boolean),
    );
    const memberArtistNames = new Set(
      (member.artists ?? []).map((artist) => artist.name?.trim().toLocaleLowerCase()).filter(Boolean),
    );
    const keepsOriginalArtist = Array.from(originalArtistNames).some((name) => memberArtistNames.has(name));
    const addsArtist = Array.from(memberArtistNames).some((name) => !originalArtistNames.has(name));
    if (keepsOriginalArtist && addsArtist) {
      return "Rework";
    }
    if (!keepsOriginalArtist && memberArtistNames.size > 0) {
      return "Cover";
    }
    return "Version";
  };
  const songFamilyQualifier = (
    member: RecordingTrackCandidateMember,
    original: RecordingTrackCandidateMember,
    kind: SongFamilyKind,
  ) => {
    if (kind === "Original") {
      return null;
    }
    const rawTitle = member.title.trim();
    const originalTitle = original.title.trim();
    let qualifier = rawTitle.toLocaleLowerCase().startsWith(originalTitle.toLocaleLowerCase())
      ? rawTitle.slice(originalTitle.length)
      : rawTitle.match(/\s[-–—]\s(.+)$/)?.[1] ?? rawTitle.match(/\(([^)]+)\)\s*$/)?.[1] ?? "";
    qualifier = qualifier.replace(/^[\s:–—-]+/, "").replace(/^\((.*)\)$/, "$1").trim();
    if (["Live", "Demo", "Acoustic", "Instrumental", "Remix", "Cover"].includes(kind)) {
      qualifier = qualifier
        .replace(new RegExp(`^${kind}\\b[\\s:–—-]*`, "i"), "")
        .replace(new RegExp(`[\\s:–—-]*${kind}$`, "i"), "")
        .trim();
    }
    if (/^version$/i.test(qualifier) || /^(live|demo|acoustic|instrumental) version$/i.test(qualifier)) {
      return null;
    }
    return qualifier || null;
  };
  const selectedPreviewFamilyRelationMembers = selectedPreview && selectedPreview.kind === "track"
    ? selectedPreviewRelatedCandidatesForCurrent
      .filter((candidate) => candidate.candidate_type === "track_family_candidate")
      .flatMap((candidate) => {
        const rawOriginal = candidate.members.find(
          (member) => member.release_track_id === candidate.representative.release_track_id,
        ) ?? candidate.members[0];
        if (!rawOriginal) {
          return [];
        }
        const original = selectedPreviewRecordingRepresentativeByReleaseTrackId.get(rawOriginal.release_track_id) ?? rawOriginal;
        const currentRawMember = candidate.members.find((member) => selectedPreviewRelationAnchorReleaseTrackIds.has(member.release_track_id));
        const currentMember = currentRawMember
          ? selectedPreviewRecordingRepresentativeByReleaseTrackId.get(currentRawMember.release_track_id) ?? currentRawMember
          : null;
        const currentKind = currentMember ? songFamilyKind(currentMember, original) : null;
        return candidate.members.map((rawMember) => {
          const member = selectedPreviewRecordingRepresentativeByReleaseTrackId.get(rawMember.release_track_id) ?? rawMember;
          const kind = songFamilyKind(member, original);
          const badge = kind === "Cover" && currentKind === "Cover"
            ? "Sibling Cover" as const
            : kind === "Remix" && currentKind === "Remix"
              ? "Sibling Remix" as const
              : kind;
          return {
            member,
            badge,
            qualifier: songFamilyQualifier(member, original, kind),
            originalArtists: original.artists ?? [],
          };
        });
      })
      .filter((item) => !selectedPreviewAlreadyShownRelationReleaseTrackIds.has(item.member.release_track_id))
      .filter((item, index, members) => members.findIndex((candidate) => candidate.member.release_track_id === item.member.release_track_id) === index)
    : [];
  const selectedPreviewRelationRows = {
    recording: selectedPreviewAlsoAppearsOnRecordingMembers,
    songFamily: selectedPreviewFamilyRelationMembers,
  };
  const selectedPreviewDisplayRelationRows = selectedPreviewRelationRows;
  const releaseSourceVersionArtistText = (version: ReleaseTrackDetailSourceVersion) => {
    const names = version.artists.map((artist) => artist.name?.trim()).filter(Boolean);
    return names.length > 0 ? names.join(", ") : null;
  };
  const releaseSourceVersionPlayCountLabel = (version: ReleaseTrackDetailSourceVersion) => {
    const playCount = Number(version.play_count ?? 0);
    return `${playCount} ${playCount === 1 ? "listen" : "listens"}`;
  };
  const releaseSourceVersionAlbumImageUrl = (version: ReleaseTrackDetailSourceVersion) => {
    if (version.album_image_url) {
      return version.album_image_url;
    }
    if (selectedPreview?.kind === "track" && version.album_id && version.album_id === selectedPreview.sourceAlbumId) {
      return selectedPreview.sourceAlbumImage ?? selectedPreview.image ?? null;
    }
    return null;
  };
  const openReleaseSourceVersion = (version: ReleaseTrackDetailSourceVersion, detailView: "recording" | "release" = "release") => {
    if (!selectedPreview || selectedPreview.kind !== "track") {
      return;
    }
    preserveRecordingAlbumTracklistOpenRef.current = detailView === "recording" && recordingAlbumTracklistOpen;
    const artistText = releaseSourceVersionArtistText(version);
    const trackUri = trackUriWithFallback(version.uri, version.spotify_track_id);
    const albumImageUrl = releaseSourceVersionAlbumImageUrl(version);
    setSelectedPreview({
      ...selectedPreview,
      image: albumImageUrl ?? selectedPreview.image ?? null,
      label: version.name ?? selectedPreview.label,
      meta: artistText ?? selectedPreview.meta,
      detail: version.album_name ?? selectedPreview.detail,
      entityId: version.spotify_track_id,
      trackUri,
      url: version.spotify_url ?? spotifyTrackUrl(trackUri) ?? selectedPreview.url,
      trackId: version.spotify_track_id,
      albumId: version.album_id,
      artistName: artistText ?? selectedPreview.artistName,
      artists: version.artists.length > 0 ? version.artists : selectedPreview.artists,
      sourceAlbumId: version.album_id,
      sourceAlbumName: version.album_name,
      sourceAlbumImage: albumImageUrl,
      sourceAlbumUrl: version.album_id ? spotifyEntityUrl("album", version.album_id) : selectedPreview.sourceAlbumUrl,
      sourceAlbumYear: version.album_release_year ?? selectedPreview.sourceAlbumYear ?? null,
      sourceTrack: null,
      preferredDetailView: detailView,
    });
  };
  function recordingMemberPreferredAlbumVersion(member: RecordingTrackCandidateMember) {
    return member.album_versions?.find((version) => Boolean(version.spotify_album_id && version.is_direct_source_album))
      ?? member.album_versions?.find((version) => Boolean(version.spotify_album_id))
      ?? member.album_versions?.[0]
      ?? null;
  }
  const recordingMemberAlbumName = (member: RecordingTrackCandidateMember) => (
    recordingMemberPreferredAlbumVersion(member)?.name ?? member.album
  );
  const recordingMemberReleaseYear = (member: RecordingTrackCandidateMember) => {
    const rawDate = recordingMemberPreferredAlbumVersion(member)?.release_date
      ?? member.album_release_dates?.find((value) => /^\d{4}/.test(String(value ?? "")));
    return rawDate ? String(rawDate).slice(0, 4) : null;
  };
  const recordingMemberDurationLabel = (member: RecordingTrackCandidateMember) => {
    const durations = member.duration_values_ms?.length ? member.duration_values_ms : member.duration_ms ? [member.duration_ms] : [];
    return durations.length > 0 ? durations.map((duration) => formatDurationMs(duration)).join(", ") : "No duration";
  };
  const recordingMemberAlbumImageUrl = (member: RecordingTrackCandidateMember) => (
    recordingMemberPreferredAlbumVersion(member)?.image_url ?? member.album_image_urls?.find(Boolean) ?? null
  );
  const variationSubtitleIsDisplayWorthy = (subtitle: string, options: { allowRemasterOnly?: boolean } = {}) => {
    const normalized = subtitle.trim().replace(/\s+/g, " ");
    return Boolean(
      normalized
      && (
        options.allowRemasterOnly
        || !/^(?:(?:\d{4}\s+)?remaster(?:ed)?|remaster(?:ed)?\s+\d{4})$/i.test(normalized)
      ),
    );
  };
  const variationSubtitleFromTitle = (
    title: string | null | undefined,
    options: { allowRemasterOnly?: boolean } = {},
  ) => {
    const rawTitle = title?.trim();
    if (!rawTitle) {
      return null;
    }
    const baseTitles = [
      selectedPreviewCanonicalTrackTitle,
      selectedPreview?.kind === "track" ? selectedPreview.releaseTrackName : null,
      selectedPreview?.kind === "track" ? selectedPreview.label : null,
    ].map((value) => value?.trim()).filter((value): value is string => Boolean(value));
    for (const baseTitle of baseTitles) {
      if (rawTitle.toLocaleLowerCase() !== baseTitle.toLocaleLowerCase() && rawTitle.toLocaleLowerCase().startsWith(baseTitle.toLocaleLowerCase())) {
        const subtitle = rawTitle.slice(baseTitle.length).replace(/^[\s:–—-]+/, "").replace(/^\((.*)\)$/, "$1").trim();
        if (subtitle && variationSubtitleIsDisplayWorthy(subtitle, options)) {
          return subtitle;
        }
      }
    }
    return null;
  };
  const scrollRecordingVariationStrip = (direction: -1 | 1) => {
    recordingVariationStripRef.current?.scrollBy({ left: direction * 224, behavior: "smooth" });
  };
  const allTimeLikedMatchCount = allTimeTopTracks.filter((track) => recentTrackIsKnownLiked(track)).length;
  const allTimeTrackIdCount = allTimeTopTracks.filter((track) => Boolean(track.track_id)).length;
  const likedTracksTotalCount = Number(likedTracksCache?.metadata?.last_active_count ?? (cachedLikedTracks.length > 0 ? cachedLikedTracks.length : likedTracksForActivitySource.length));
  const likedTracksTotalLabel = likedTracksTotalCount > 0 ? likedTracksTotalCount.toLocaleString() : "All";
  const likedTracksCacheStatus = (() => {
    const metadata = likedTracksCache?.metadata ?? null;
    const cachedCount = cachedLikedTracks.length;
    if (!metadata) {
      return cachedCount > 0 ? `${cachedCount.toLocaleString()} cached liked songs` : null;
    }
    const activeCount = Number(metadata.last_active_count ?? cachedCount);
    const countLabel = `${activeCount.toLocaleString()} cached liked songs`;
    if (metadata.last_full_completed) {
      const completedLabel = formatRelativeSyncTime(parseTimestampMs(metadata.last_completed_full_sync_at));
      return `${countLabel} · full refresh complete${completedLabel ? ` ${completedLabel}` : ""}`;
    }
    if (metadata.last_stopped_reason) {
      const attemptedLabel = formatRelativeSyncTime(parseTimestampMs(metadata.last_attempted_sync_at));
      return `${countLabel} · partial cache, stopped: ${metadata.last_stopped_reason}${attemptedLabel ? ` ${attemptedLabel}` : ""}`;
    }
    return countLabel;
  })();
  const showLoadingScreen = (authTransitioning || session?.authenticated || experienceMode === "local")
    && (!profile || !startupDashboardReleased);
  const heroTitle = "ListenLab";
  const heroCopy =
    "Connect your account and browse the listening, library, and profile details Spotify already makes available to ListenLab.";
  const artistTargetNameKeys = selectedPreview?.kind === "artist"
    ? selectedPreviewArtists
      .map((artist) => artist.name?.trim().toLocaleLowerCase())
      .filter((name): name is string => Boolean(name))
    : [];
  const artistAlbumIsEdition = (album: ArtistAlbumEntry) => {
    const coreName = normalizedEditionAlbumCoreName(album.name);
    return Boolean(coreName && coreName !== album.name.trim().toLocaleLowerCase());
  };
  const artistAlbumIsSingle = (album: ArtistAlbumEntry) => {
    const type = String(album.albumType ?? "").trim().toLocaleLowerCase();
    const name = album.name.trim().toLocaleLowerCase();
    return type === "single" || /\b(single|single version)\b/.test(name);
  };
  const artistAlbumIsEp = (album: ArtistAlbumEntry) => {
    const name = album.name.trim().toLocaleLowerCase();
    return /\bep\b/.test(name);
  };
  const artistAlbumIsSpecialRelease = (album: ArtistAlbumEntry) => {
    const type = String(album.albumType ?? "").trim().toLocaleLowerCase();
    const name = album.name.trim().toLocaleLowerCase();
    return (
      type === "compilation"
      || artistAlbumIsSingle(album)
      || artistAlbumIsEp(album)
      || /\b(deluxe|expanded|anniversary|remaster(?:ed)?|remix|rmx|live|soundtrack|ost|score|compilation|greatest hits|best of)\b/.test(name)
    );
  };
  const artistAlbumIsCoreAlbum = (album: ArtistAlbumEntry) => (
    (!album.relationship || album.relationship === "album")
    && !artistAlbumIsSpecialRelease(album)
  );
  const artistTrackIsSingle = (track: ArtistTrackEvidenceItem) => {
    const type = String(track.album_type ?? "").trim().toLocaleLowerCase();
    const albumName = String(track.album_name ?? "").trim().toLocaleLowerCase();
    return type === "single" || /\b(single|single version)\b/.test(albumName);
  };
  const artistTrackIsMainArtist = (track: ArtistTrackEvidenceItem) => {
    if (artistTargetNameKeys.length === 0) {
      return true;
    }
    const firstStructuredArtist = track.artists?.find((artist) => artist.name?.trim())?.name?.trim().toLocaleLowerCase() ?? null;
    const firstTextArtist = String(track.artist_name ?? "").split(",")[0]?.trim().toLocaleLowerCase() || null;
    return artistTargetNameKeys.some((targetName) => firstStructuredArtist === targetName || firstTextArtist === targetName);
  };
  const artistTrackCoreKey = (track: ArtistTrackEvidenceItem) => (
    `${String(track.track_name ?? "").trim().toLocaleLowerCase()}::${artistTargetNameKeys.join(",")}`
  );
  const chooseCoreArtistTrack = (current: ArtistTrackEvidenceItem, candidate: ArtistTrackEvidenceItem) => {
    const currentYear = Number.parseInt(current.album_release_year ?? "", 10);
    const candidateYear = Number.parseInt(candidate.album_release_year ?? "", 10);
    const currentSinglePenalty = artistTrackIsSingle(current) ? 1 : 0;
    const candidateSinglePenalty = artistTrackIsSingle(candidate) ? 1 : 0;
    if (currentSinglePenalty !== candidateSinglePenalty) {
      return candidateSinglePenalty < currentSinglePenalty ? candidate : current;
    }
    if (Number.isFinite(currentYear) && Number.isFinite(candidateYear) && currentYear !== candidateYear) {
      return candidateYear < currentYear ? candidate : current;
    }
    if (!current.track_id && candidate.track_id) {
      return candidate;
    }
    return current;
  };
  const sortArtistTracks = (tracks: ArtistTrackEvidenceItem[]) => {
    const valueForTrack = (track: ArtistTrackEvidenceItem) => {
      if (artistTrackSort.key === "year") {
        return Number.parseInt(track.album_release_year ?? "", 10);
      }
      if (artistTrackSort.key === "duration") {
        return track.duration_ms ?? Number.NaN;
      }
      if (artistTrackSort.key === "plays") {
        return track.play_count;
      }
      return parseTimestampMs(track.last_played_at) ?? Number.NaN;
    };
    return [...tracks].sort((left, right) => {
      const leftValue = valueForTrack(left);
      const rightValue = valueForTrack(right);
      const leftMissing = !Number.isFinite(leftValue);
      const rightMissing = !Number.isFinite(rightValue);
      if (leftMissing !== rightMissing) {
        return leftMissing ? 1 : -1;
      }
      if (leftValue !== rightValue) {
        return artistTrackSort.direction === "asc" ? leftValue - rightValue : rightValue - leftValue;
      }
      return left.track_name.localeCompare(right.track_name);
    });
  };
  const nextArtistTrackSort = (key: ArtistTrackSortKey) => {
    setArtistTrackSort((current) => (
      current.key === key
        ? { key, direction: current.direction === "asc" ? "desc" : "asc" }
        : { key, direction: key === "year" ? "asc" : "desc" }
    ));
  };
  const artistTrackSortIndicator = (key: ArtistTrackSortKey) => (
    artistTrackSort.key === key ? (artistTrackSort.direction === "asc" ? "↑" : "↓") : ""
  );
  const selectedPreviewArtistTracksForDisplay = useMemo(() => {
    let tracks = selectedPreviewArtistTracks;
    if (!artistIncludeSingles) {
      tracks = tracks.filter((track) => !artistTrackIsSingle(track));
    }
    if (artistViewMode === "core") {
      const grouped = new Map<string, ArtistTrackEvidenceItem>();
      for (const track of tracks.filter(artistTrackIsMainArtist)) {
        const key = artistTrackCoreKey(track);
        const existing = grouped.get(key);
        grouped.set(key, existing ? chooseCoreArtistTrack(existing, track) : track);
      }
      tracks = [...grouped.values()];
    }
    return sortArtistTracks(tracks);
  }, [artistIncludeSingles, artistTrackSort, artistViewMode, selectedPreviewArtistTracks, selectedPreviewArtists]);
  const coreArtistAlbumsForDisplay = useMemo(() => {
    const grouped = new Map<string, ArtistAlbumEntry[]>();
    for (const album of selectedPreviewArtistAlbumsForDisplay) {
      if (album.relationship && album.relationship !== "album") {
        continue;
      }
      const coreName = normalizedEditionAlbumCoreName(album.name) || album.name.trim().toLocaleLowerCase();
      const key = coreName;
      grouped.set(key, [...(grouped.get(key) ?? []), album]);
    }
    const coreAlbums: ArtistAlbumEntry[] = [];
    for (const albums of grouped.values()) {
      const preferred = albums.find(artistAlbumIsCoreAlbum);
      if (!preferred) {
        continue;
      }
      const coreAlbum: ArtistAlbumEntry = { ...preferred, editionCount: albums.length > 1 ? albums.length : null };
      coreAlbums.push(coreAlbum);
    }
    return coreAlbums.sort((left, right) => {
        if (left.isHighlighted !== right.isHighlighted) {
          return left.isHighlighted ? -1 : 1;
        }
        return left.name.localeCompare(right.name);
      });
  }, [selectedPreviewArtistAlbumsForDisplay]);
  const selectedPreviewArtistAlbumsForMode = artistViewMode === "core"
    ? coreArtistAlbumsForDisplay
    : selectedPreviewArtistAlbumsForDisplay.filter((album) => artistIncludeSingles || !artistAlbumIsSingle(album));
  const selectedPreviewPrimaryArtistAlbumsForMode = artistViewMode === "core"
    ? coreArtistAlbumsForDisplay
    : selectedPreviewPrimaryArtistAlbums.filter((album) => artistIncludeSingles || !artistAlbumIsSingle(album));
  const selectedPreviewAppearsOnAlbumsForMode = artistViewMode === "core"
    ? []
    : selectedPreviewAppearsOnAlbums.filter((album) => artistIncludeSingles || !artistAlbumIsSingle(album));
  const openArtistTrackPreview = (track: ArtistTrackEvidenceItem) => {
    const sourceTrack: RecentTrack = {
      track_id: track.track_id,
      track_name: track.track_name,
      artist_name: track.artist_name,
      album_name: track.album_name,
      album_release_year: track.album_release_year,
      artists: track.artists ?? null,
      duration_ms: track.duration_ms,
      uri: track.uri,
      url: track.url,
      image_url: track.album_image_url,
      album_id: track.album_id,
      album_url: track.album_url,
      spotify_album_type: track.album_type,
      spotify_track_number: track.track_number,
      spotify_disc_number: track.disc_number,
      spotify_album_total_tracks: track.album_total_tracks,
      play_count: track.play_count,
      first_played_at: track.first_played_at,
      last_played_at: track.last_played_at,
    };
    setSelectedPreview({
      image: track.album_image_url,
      fallbackLabel: "T",
      label: track.track_name,
      meta: track.artist_name,
      detail: track.album_name,
      kind: "track",
      entityId: track.track_id,
      trackUri: track.uri,
      url: track.url ?? "",
      trackId: track.track_id,
      albumId: track.album_id,
      artistName: track.artist_name,
      artists: track.artists ?? null,
      targetArtists: null,
      sourceAlbumId: track.album_id,
      sourceAlbumName: track.album_name,
      sourceAlbumImage: track.album_image_url,
      sourceAlbumUrl: track.album_url,
      sourceAlbumYear: track.album_release_year,
      sourceTrack,
      preferredDetailView: "recording",
    });
  };
  const renderSelectedPreviewArtistTrackSection = () => {
    if (artistAlbumEvidenceLoading && selectedPreviewArtistTracks.length === 0) {
      return <p className="empty-copy">Loading artist tracks...</p>;
    }
    if (selectedPreviewArtistTracksForDisplay.length === 0) {
      return (
        <p className="empty-copy">
          {selectedPreviewArtistTracks.length > 0
            ? "No tracks match the current artist filters."
            : "No cached tracklist rows are available for this artist yet."}
        </p>
      );
    }
    return (
      <div className="detail-modal-artist-tracks">
        <div className="detail-artist-view-controls">
          <div className="detail-artist-view-mode-toggle" role="group" aria-label="Artist catalog scope">
            <button
              className={`recent-range-chip${artistViewMode === "core" ? " recent-range-chip-active" : ""}`}
              onClick={() => setArtistViewMode("core")}
              type="button"
            >
              Core
            </button>
            <button
              className={`recent-range-chip${artistViewMode === "all" ? " recent-range-chip-active" : ""}`}
              onClick={() => setArtistViewMode("all")}
              type="button"
            >
              All
            </button>
          </div>
          <label className="detail-artist-singles-toggle">
            <input
              checked={artistIncludeSingles}
              onChange={(event) => setArtistIncludeSingles(event.target.checked)}
              type="checkbox"
            />
            <span>Singles</span>
          </label>
        </div>
        <p className="detail-modal-album-title">All Tracks</p>
        <div className="detail-artist-track-grid" role="table" aria-label="Artist tracks">
          <div className="detail-artist-track-grid-header" role="row">
            <span role="columnheader">Track</span>
            <button className={`detail-artist-track-sort${artistTrackSort.key === "year" ? " detail-artist-track-sort-active" : ""}`} onClick={() => nextArtistTrackSort("year")} role="columnheader" type="button">
              Year {artistTrackSortIndicator("year")}
            </button>
            <button className={`detail-artist-track-sort${artistTrackSort.key === "duration" ? " detail-artist-track-sort-active" : ""}`} onClick={() => nextArtistTrackSort("duration")} role="columnheader" type="button">
              Length {artistTrackSortIndicator("duration")}
            </button>
            <button className={`detail-artist-track-sort${artistTrackSort.key === "plays" ? " detail-artist-track-sort-active" : ""}`} onClick={() => nextArtistTrackSort("plays")} role="columnheader" type="button">
              Plays {artistTrackSortIndicator("plays")}
            </button>
            <button className={`detail-artist-track-sort${artistTrackSort.key === "last" ? " detail-artist-track-sort-active" : ""}`} onClick={() => nextArtistTrackSort("last")} role="columnheader" type="button">
              Last {artistTrackSortIndicator("last")}
            </button>
          </div>
          <ul className="detail-artist-track-list">
          {selectedPreviewArtistTracksForDisplay.map((track) => (
            <li className="detail-artist-track-row" key={`${track.track_id ?? track.track_name}-${track.album_id ?? track.album_name ?? ""}`}>
              <button className="detail-artist-track-button" onClick={() => openArtistTrackPreview(track)} type="button">
                {track.album_image_url ? (
                  <img alt="" className="detail-artist-track-cover" src={track.album_image_url} />
                ) : (
                  <span className="detail-artist-track-cover detail-artist-track-cover-fallback" aria-hidden="true">
                    {track.track_name.slice(0, 1).toUpperCase()}
                  </span>
                )}
                <span className="detail-artist-track-copy">
                  <span className="detail-artist-track-name single-line-ellipsis">{track.track_name}</span>
                  <span className="detail-artist-track-meta single-line-ellipsis">
                    {[
                      track.album_name,
                    ].filter(Boolean).join(" | ")}
                  </span>
                </span>
              </button>
              <span className="detail-artist-track-cell">{track.album_release_year ?? "-"}</span>
              <span className="detail-artist-track-cell">{track.duration_ms != null ? formatPlaybackClock(track.duration_ms) : "-"}</span>
              <span className="detail-artist-track-cell">{track.play_count > 0 ? track.play_count.toLocaleString() : "-"}</span>
              <span className="detail-artist-track-cell">{formatMonthDay(track.last_played_at, true) ?? "-"}</span>
            </li>
          ))}
        </ul>
        </div>
      </div>
    );
  };
  const renderSelectedPreviewArtistAlbumSection = (
    title: string,
    albums: ArtistAlbumEntry[],
  ) => {
    if (albums.length === 0) {
      return null;
    }
    return (
      <div className="detail-modal-artist-albums">
        <p className="detail-modal-album-title">{title}</p>
        <ul className="detail-artist-album-list">
          {albums.map((album) => (
          <li
            className={`detail-artist-album-row${album.isHighlighted ? " detail-artist-album-row-highlighted" : ""}`}
            key={`${title}-${album.albumId ?? album.name}-${album.artistName ?? ""}`}
            >
              <button
                className="detail-artist-album-button"
                onClick={() => openArtistAlbumPreview(album)}
                type="button"
              >
                {album.imageUrl ? (
                  <img alt="" className="detail-artist-album-cover" src={album.imageUrl} />
                ) : (
                  <span className="detail-artist-album-cover detail-artist-album-cover-fallback" aria-hidden="true">
                    {album.name.slice(0, 1).toUpperCase()}
                  </span>
                )}
                <span className="detail-artist-album-copy">
                  <span className="detail-artist-album-name single-line-ellipsis">{album.name}</span>
                  <span className="detail-artist-album-meta single-line-ellipsis">
                    {[
                      album.releaseYear,
                      selectedPreviewArtists.length === 1 && album.artistName
                        ? collaboratorLabel(album.artistName, selectedPreview?.label)
                        : null,
                      album.evidence ?? null,
                      album.trackCount != null ? `${album.trackCount} tracks represented` : null,
                    ]
                      .filter(Boolean)
                      .join(" | ")}
                  </span>
                </span>
                <span className="detail-artist-album-status">
                  {album.editionCount && album.editionCount > 1 ? (
                    <span className="detail-artist-album-editions">{album.editionCount} editions</span>
                  ) : null}
                  {album.isHighlighted ? <span className="detail-artist-album-current">Selected</span> : null}
                </span>
              </button>
            </li>
          ))}
        </ul>
      </div>
    );
  };
  const scheduleAlbumWithArtistHighlight = (artistName: string) => {
    if (albumWithHoverDelayRef.current != null) {
      window.clearTimeout(albumWithHoverDelayRef.current);
    }
    albumWithHoverDelayRef.current = window.setTimeout(() => {
      setHoveredAlbumWithArtistName(artistName);
      albumWithHoverDelayRef.current = null;
    }, 250);
  };
  const clearAlbumWithArtistHighlight = () => {
    if (albumWithHoverDelayRef.current != null) {
      window.clearTimeout(albumWithHoverDelayRef.current);
      albumWithHoverDelayRef.current = null;
    }
    setHoveredAlbumWithArtistName(null);
  };

  if (showLoadingScreen) {
    return (
      <LoadingScreen
        statusHistory={statusHistory}
        statusMessage={statusMessage}
        analysisMode={analysisMode}
        showRateLimitReload={showRateLimitReload}
        reloadReady={reloadReady}
        reloadProgress={reloadProgress}
        loadingProfile={loadingProfile}
        loadingRecentSection={loadingRecentSection}
        loadingExtendedProfile={loadingExtendedProfile}
        onCooldownRetry={handleCooldownRetry}
      />
    );
  }

  const displayHomeAlbumTrackEntries = sortedAlbumTrackEntries(homeAlbumTrackEntries, homeAlbumTrackLastSortMode);
  const displayAlbumTrackEntries = sortedAlbumTrackEntries(albumTrackEntries, albumTrackLastSortMode, selectedPreviewDetailView);

  return (
    <>
      {profile && loadingExtendedProfile ? (
        <FullAnalysisOverlay
          statusHistory={statusHistory}
          statusMessage={statusMessage}
          analysisMode={analysisMode}
          showRateLimitReload={showRateLimitReload}
          reloadReady={reloadReady}
          reloadProgress={reloadProgress}
          loadingProfile={loadingProfile}
          loadingRecentSection={loadingRecentSection}
          loadingExtendedProfile={loadingExtendedProfile}
          onCooldownRetry={handleCooldownRetry}
        />
      ) : null}
      <main className="app-shell">
        <section className="hero-card">
        {!profile ? (
          <LoginHeroPanel
            heroTitle={heroTitle}
            heroCopy={heroCopy}
            experienceMode={experienceMode}
            renderExperienceModeToggle={renderExperienceModeToggle}
            handleAuthAction={handleAuthAction}
          />
        ) : null}

        {!profile ? null : (
          <>
            <nav className="jump-links jump-links-sticky" aria-label="Dashboard sections">
              <div className="sticky-bar-left">
                <div className="profile-menu-shell profile-menu-shell-inline" ref={brandMenuRef}>
                  <button
                    aria-expanded={brandMenuOpen}
                    className="bar-trigger bar-trigger-brand"
                    onClick={() => {
                      setBrandMenuOpen((current) => !current);
                      setExperimentalMenuOpen(false);
                      setProfileMenuOpen(false);
                    }}
                    type="button"
                  >
                    <span className="brand-trigger-text" data-text="ListenLab">
                      ListenLab
                    </span>
                  </button>

                  {brandMenuOpen ? (
                    <section className="profile-card top-profile-card profile-menu-card brand-menu-card">
                      <div className="profile-panel-top">
                        <div>
                          <h2>ListenLab</h2>
                          <p className="empty-copy">
                            A local Spotify listening dashboard for exploring recent activity, favorites, albums, artists,
                            and playlists.
                          </p>
                        </div>
                      </div>

                      <div className="actions actions-right actions-in-card">
                        <a className="secondary-button bar-link-button" href={githubRepoUrl} rel="noreferrer" target="_blank">
                          View on GitHub
                        </a>
                      </div>
                    </section>
                  ) : null}
                </div>
              </div>

              <div className="sticky-bar-center">
                <div className="profile-menu-shell profile-menu-shell-inline experimental-menu-shell" ref={experimentalMenuRef}>
                  <button
                    aria-expanded={experimentalMenuOpen}
                    aria-label="Experimental tools"
                    className="jump-link jump-link-icon"
                    onClick={() => {
                      setExperimentalMenuOpen((current) => !current);
                      setBrandMenuOpen(false);
                      setPlayerMenuOpen(false);
                      setProfileMenuOpen(false);
                    }}
                    type="button"
                  >
                    {"🧪"}
                  </button>
                  {experimentalMenuOpen ? (
                    <section className="profile-card top-profile-card profile-menu-card experimental-menu-card">
                      <div className="profile-panel-top">
                        <div>
                          <h2>Experimental</h2>
                          <p className="empty-copy">Tools for inspecting the ranking and identity work in progress.</p>
                        </div>
                        <button
                          aria-label="Force refresh Spotify data"
                          className="secondary-button experimental-refresh-button"
                          disabled={spotifyCooldownActive || loadingRecentSection}
                          onClick={() => void refreshRecentSection(recentRange, true)}
                          title={spotifyCooldownActive ? "Spotify requests are paused by cooldown." : "Force refresh Spotify data"}
                          type="button"
                        >
                          <svg aria-hidden="true" viewBox="0 0 24 24">
                            <path d="M17.7 6.3A7.9 7.9 0 0 0 4.4 10H2.3A10 10 0 0 1 19.1 4.9L21 3v6h-6l2.7-2.7ZM6.3 17.7A7.9 7.9 0 0 0 19.6 14h2.1A10 10 0 0 1 4.9 19.1L3 21v-6h6l-2.7 2.7Z" />
                          </svg>
                        </button>
                      </div>
                      <div className="experimental-menu-actions">
                        <button className="secondary-button" onClick={openListeningLogPage} type="button">
                          Listen Log
                        </button>
                        <button className="secondary-button" onClick={openFormulaLabPage} type="button">
                          Formula Lab
                        </button>
                        <button className="secondary-button" onClick={openIdentityAuditPage} type="button">
                          Identity Audit
                        </button>
                        <button className="secondary-button" onClick={openCatalogBackfillPage} type="button">
                          Catalog Backfill
                        </button>
                        <button className="secondary-button" onClick={openSearchLookupPage} type="button">
                          Search / Lookup
                        </button>
                      </div>
                    </section>
                  ) : null}
                </div>
                <button className="jump-link" onClick={() => openAndScrollToSection("recent", "activity")} type="button">
                  Activity
                </button>
                <button className="jump-link" onClick={() => openAndScrollToSection("tracks", "tracks")} type="button">
                  Tracks
                </button>
                <button className="jump-link" onClick={() => openAndScrollToSection("artists", "artists")} type="button">
                  Artists
                </button>
                <button className="jump-link" onClick={() => openAndScrollToSection("albums", "albums")} type="button">
                  Albums
                </button>
                <button
                  className="jump-link"
                  onClick={() => openAndScrollToSection("playlists", "playlists")}
                  type="button"
                >
                  Playlists
                </button>
              </div>

              <div className="sticky-bar-right">
                {experienceMode === "full" && hasPremiumPlayback ? (
                <div className="profile-menu-shell profile-menu-shell-inline" ref={playerMenuRef}>
                  <button
                    aria-expanded={playerMenuOpen}
                    className="bar-trigger bar-trigger-player"
                    onClick={() => {
                      setPlayerMenuOpen((current) => !current);
                      setProfileMenuOpen(false);
                      setBrandMenuOpen(false);
                      setExperimentalMenuOpen(false);
                    }}
                    type="button"
                  >
                    <span className="toolbar-player-icon" aria-hidden="true">
                      {playerDisplayTrack?.image ? <img alt="" className="toolbar-player-cover" src={playerDisplayTrack.image} /> : null}
                      {playerDisplayTrack && !playerDisplayPaused ? (
                        <span className="detail-wave-icon">
                          <span />
                          <span />
                          <span />
                        </span>
                      ) : (
                        <span className="detail-play-icon">▶</span>
                      )}
                    </span>
                  </button>

                  {playerMenuOpen ? (
                    <section className="profile-card top-profile-card profile-menu-card player-menu-card player-menu-card-basic">
                      <div className="player-menu-layout player-menu-layout-basic">
                        <aside className="player-recent-column" aria-label="Recently played songs">
                          <div className="player-recent-header">
                            <h3>Recently played</h3>
                            {playerRecentTracksLoading ? <span>Loading</span> : null}
                          </div>
                          <div className="player-recent-list">
                            {playerRecentTracks.map((track, index) => {
                              const isDisplayedTrack = Boolean(
                                playerDisplayTrack?.uri
                                && trackUriWithFallback(track.uri, track.track_id) === playerDisplayTrack.uri,
                              );
                              const durationMs = track.duration_ms ?? (isDisplayedTrack ? playerDisplayDurationMs || playerDisplayTrack?.durationMs : null);
                              const progressRatio = isDisplayedTrack && durationMs
                                ? Math.max(0, Math.min(1, playerDisplayPositionMs / durationMs))
                                : (
                                  typeof track.estimated_completion_ratio === "number"
                                    ? Math.max(0, Math.min(1, track.estimated_completion_ratio))
                                    : null
                                );
                              return (
                                <button
                                  className="player-recent-row"
                                  key={`${track.spotify_played_at ?? "recent"}-${track.track_id ?? track.uri ?? index}`}
                                  onClick={() => openRecentPlayerTrackDetails(track)}
                                  type="button"
                                >
                                  {track.image_url ? (
                                    <img alt="" className="player-recent-cover" src={track.image_url} />
                                  ) : (
                                    <span className="player-recent-cover player-recent-cover-fallback" aria-hidden="true">
                                      {(track.track_name ?? "?").slice(0, 1).toUpperCase()}
                                    </span>
                                  )}
                                  <span className="player-recent-copy">
                                    <span className="player-recent-track single-line-ellipsis">
                                      {recentTrackIsKnownLiked(track) ? <LikedBadge className="player-liked-badge" /> : null}
                                      {recentTrackHasRelationTags(track) ? (
                                        <ReleaseSiblingBadge
                                          className="player-release-sibling-badge"
                                          sourceCount={releaseSiblingSourceCountForTrackId(track.track_id)}
                                          duplicateSourceCount={track.release_track_duplicate_source_count ?? null}
                                          clusterCandidateType={track.release_track_cluster_candidate_type ?? null}
                                          clusterRelationshipKind={track.release_track_cluster_relationship_kind ?? null}
                                        />
                                      ) : null}
                                      {track.track_name ?? "Unknown track"}
                                      {Number(track.completed_play_count ?? 0) > 1 ? (
                                        <span className="player-recent-repeat-count">x{track.completed_play_count}</span>
                                      ) : null}
                                    </span>
                                    <span className="player-recent-artist single-line-ellipsis">
                                      {track.artist_name ?? "Unknown artist"}
                                    </span>
                                    <span className="player-recent-completion" aria-hidden="true">
                                      <span
                                        className="player-recent-completion-fill"
                                        style={{ width: `${Math.round((progressRatio ?? 0) * 100)}%` }}
                                      />
                                    </span>
                                  </span>
                                </button>
                              );
                            })}
                            {!playerRecentTracksLoading && playerRecentTracks.length === 0 ? (
                              <p className="empty-copy player-recent-empty">No recently played songs yet.</p>
                            ) : null}
                            {playerRecentTracksError ? (
                              <p className="empty-copy player-recent-empty">{playerRecentTracksError}</p>
                            ) : null}
                          </div>
                          <button
                            className="secondary-button player-menu-footer-button"
                            onClick={() => {
                              setPlayerMenuOpen(false);
                              openListeningLogPage();
                            }}
                            type="button"
                          >
                            complete listen log
                          </button>
                        </aside>

                        <div className="player-current-column">
                          <div className="player-menu-summary">
                            {playerDisplayTrack?.image ? (
                              <img alt={`${playerDisplayTrack.album} cover`} className="player-menu-image" src={playerDisplayTrack.image} />
                            ) : null}

                            <div className="player-menu-copy">
                              <div className="player-menu-copy-top">
                                <h2>
                                  {usingLivePlaybackSnapshot && playerDisplayTrack ? (
                                    <button
                                      className="player-menu-title-button player-menu-title-scroll"
                                      onClick={() => openPlayerTrackDetails()}
                                      type="button"
                                    >
                                      <span>
                                        {playerDisplayKnownLiked ? <LikedBadge className="player-liked-badge" /> : null}
                                        {recentTrackHasRelationTags(playerDisplayKnownTrack) || hasReleaseSiblingForTrackId(spotifyTrackIdFromUri(playerDisplayTrack.uri)) ? (
                                          <ReleaseSiblingBadge
                                            className="player-release-sibling-badge"
                                            sourceCount={releaseSiblingSourceCountForTrackId(playerDisplayKnownTrack?.track_id ?? spotifyTrackIdFromUri(playerDisplayTrack.uri))}
                                            duplicateSourceCount={playerDisplayKnownTrack?.release_track_duplicate_source_count ?? null}
                                            clusterCandidateType={playerDisplayKnownTrack?.release_track_cluster_candidate_type ?? null}
                                            clusterRelationshipKind={playerDisplayKnownTrack?.release_track_cluster_relationship_kind ?? null}
                                          />
                                        ) : null}
                                        {playerDisplayTrack.name ?? "ListenLab Player"}
                                      </span>
                                    </button>
                                  ) : (
                                    <span className="player-menu-title-scroll">
                                      <span>
                                        {playerDisplayKnownLiked ? <LikedBadge className="player-liked-badge" /> : null}
                                        {recentTrackHasRelationTags(playerDisplayKnownTrack) || hasReleaseSiblingForTrackId(spotifyTrackIdFromUri(playerDisplayTrack?.uri ?? null)) ? (
                                          <ReleaseSiblingBadge
                                            className="player-release-sibling-badge"
                                            sourceCount={releaseSiblingSourceCountForTrackId(playerDisplayKnownTrack?.track_id ?? spotifyTrackIdFromUri(playerDisplayTrack?.uri ?? null))}
                                            duplicateSourceCount={playerDisplayKnownTrack?.release_track_duplicate_source_count ?? null}
                                            clusterCandidateType={playerDisplayKnownTrack?.release_track_cluster_candidate_type ?? null}
                                            clusterRelationshipKind={playerDisplayKnownTrack?.release_track_cluster_relationship_kind ?? null}
                                          />
                                        ) : null}
                                        {playerDisplayTrack?.name ?? "ListenLab Player"}
                                      </span>
                                    </span>
                                  )}
                                </h2>
                              </div>
                              {playerDisplayArtists.length > 0 ? (
                                <div className="player-menu-artist-row">
                                  <button
                                    className="player-menu-meta-button player-menu-line player-menu-artist-button single-line-ellipsis"
                                    onClick={openPlayerArtistsDetails}
                                    type="button"
                                  >
                                    {playerDisplayArtistImageUrl ? <img alt="" className="player-menu-artist-image" src={playerDisplayArtistImageUrl} /> : null}
                                    <span className="single-line-ellipsis">{playerDisplayArtistLabel}</span>
                                  </button>
                                  {playerDisplayTrack?.uri ? (
                                    <a
                                      aria-label="Open in Spotify"
                                      className="player-menu-external player-menu-external-inline"
                                      href={spotifyTrackUrl(playerDisplayTrack.uri) ?? undefined}
                                      rel="noreferrer"
                                      target="_blank"
                                    >
                                      <svg aria-hidden="true" viewBox="0 0 24 24">
                                        <path d="M12 2.2a9.8 9.8 0 1 0 0 19.6 9.8 9.8 0 0 0 0-19.6Zm4.49 14.13a.72.72 0 0 1-.99.24c-2.7-1.65-6.1-2.02-10.1-1.11a.72.72 0 1 1-.32-1.4c4.38-1 8.14-.57 11.17 1.28.34.2.44.65.24.99Zm1.2-2.68a.9.9 0 0 1-1.24.3c-3.09-1.9-7.8-2.45-11.46-1.34a.9.9 0 1 1-.52-1.72c4.18-1.27 9.37-.66 12.92 1.52.42.26.56.82.3 1.24Zm.1-2.8c-3.7-2.2-9.8-2.4-13.34-1.33a1.08 1.08 0 1 1-.63-2.07c4.06-1.23 10.8-.99 15.07 1.55a1.08 1.08 0 0 1-1.1 1.85Z" />
                                      </svg>
                                    </a>
                                  ) : null}
                                </div>
                              ) : (
                                <p className="player-menu-line single-line-ellipsis">
                                  {playerDisplayTrack?.artists ?? "Spotify Premium playback"}
                                </p>
                              )}
                              {playerDisplayAlbumName ? (
                                <button
                                  className="player-menu-meta-button player-menu-line player-menu-line-muted single-line-ellipsis"
                                  onClick={() => openPlayerAlbumDetails()}
                                  type="button"
                                >
                                  {playerDisplayAlbumLabel}
                                </button>
                              ) : (
                                <p className="player-menu-line player-menu-line-muted single-line-ellipsis">
                                  {playerDisplayAlbumLabel}
                                </p>
                              )}
                            </div>
                          </div>

                          {playerDisplayTrack ? (
                            <div className="player-progress" aria-label="Playback progress">
                              <input
                                aria-label="Seek playback"
                                className="player-progress-slider"
                                disabled={!canControlPlayback}
                                max={Math.max(playerDisplayDurationMs || playerDisplayTrack.durationMs || 0, 1)}
                                min={0}
                                onChange={(event) => setPendingSeekMs(Number(event.currentTarget.value))}
                                onMouseUp={(event) => {
                                  if (canControlPlayback) {
                                    void seekPlayer(Number(event.currentTarget.value));
                                  }
                                }}
                                onTouchEnd={(event) => {
                                  if (canControlPlayback) {
                                    void seekPlayer(Number(event.currentTarget.value));
                                  }
                                }}
                                step={1000}
                                title={playerTransportTooltip}
                                type="range"
                                value={pendingSeekMs ?? playerDisplayPositionMs}
                              />
                              <div className="player-progress-times">
                                <span>{formatPlaybackClock(pendingSeekMs ?? playerDisplayPositionMs)}</span>
                                <span>{formatPlaybackClock(playerDisplayDurationMs || playerDisplayTrack.durationMs || 0)}</span>
                              </div>
                            </div>
                          ) : null}

                          <div className="actions actions-centered actions-in-card player-transport-controls">
                            <button
                              aria-label="Previous track"
                              className={`secondary-button player-icon-button${playerTransportReadOnly ? " player-control-readonly" : ""}`}
                              disabled={playerPreviousDisabled}
                              onClick={() => void movePlaybackQueue("previous")}
                              title={playerTransportTooltip ?? "Previous track"}
                              type="button"
                            >
                              <svg aria-hidden="true" viewBox="0 0 24 24">
                                <path d="M7 5h2v14H7V5Zm3.7 7L19 5.8v12.4L10.7 12Z" />
                              </svg>
                            </button>
                            <span title={playerTransportTooltip}>
                              <button
                                className={`primary-button${playerTransportReadOnly ? " primary-button-readonly" : ""}`}
                                disabled={!previewInProgress && (!playerDisplayTrack || (!playerReady && !usingLivePlaybackSnapshot))}
                                onClick={() => void handlePlayerPrimaryButtonClick()}
                                type="button"
                              >
                                {previewInProgress ? "Play" : (playerDisplayPaused ? "Play" : "Pause")}
                              </button>
                            </span>
                            <button
                              aria-label="Next track"
                              className={`secondary-button player-icon-button${playerTransportReadOnly ? " player-control-readonly" : ""}`}
                              disabled={playerNextDisabled}
                              onClick={() => void movePlaybackQueue("next")}
                              title={playerTransportTooltip ?? "Next track"}
                              type="button"
                            >
                              <svg aria-hidden="true" viewBox="0 0 24 24">
                                <path d="M15 5h2v14h-2V5ZM5 5.8 13.3 12 5 18.2V5.8Z" />
                              </svg>
                            </button>
                            <button
                              aria-label={playerTrackLoopEnabled ? "Unloop current song" : "Loop current song"}
                              aria-pressed={playerTrackLoopEnabled}
                              className={`secondary-button player-icon-button player-track-loop-button${playerTrackLoopEnabled ? " player-icon-button-active player-icon-button-toggle-active" : ""}${playerTransportReadOnly ? " player-control-readonly" : ""}`}
                              disabled={previewInProgress || !playerDisplayTrack}
                              onClick={() => void handleTrackLoopClick()}
                              title={playerTrackLoopEnabled ? "Unloop current song" : "Loop current song"}
                              type="button"
                            >
                              <svg aria-hidden="true" viewBox="0 0 24 24">
                                <path d="M7 7h8.8L14 5.2l1.4-1.4L19.6 8l-4.2 4.2-1.4-1.4L15.8 9H7a3 3 0 0 0 0 6h1v2H7A5 5 0 0 1 7 7Zm10 10h-4v-2h4a3 3 0 0 0 0-6h-1V7h1a5 5 0 0 1 0 10Zm-6-1h2v2h-2v-2Z" />
                              </svg>
                            </button>
                          </div>

                          <div className="player-overlay-up-next-panel">
                            <div className="player-overlay-up-next-copy">
                              <span className="player-overlay-up-next-label">Up next</span>
                              {playerUpNextTrack ? (
                                <button
                                  className="player-overlay-up-next-title single-line-ellipsis"
                                  onClick={() => openQueuePlayerTrackDetails(playerUpNextTrack)}
                                  type="button"
                                >
                                  {playerUpNextTrack.name}
                                </button>
                              ) : (
                                <span className="player-overlay-up-next-title single-line-ellipsis">No next song</span>
                              )}
                              {playerUpNextTrack ? (
                                <button
                                  className="player-overlay-up-next-artist single-line-ellipsis"
                                  onClick={() => openQueuePlayerTrackDetails(playerUpNextTrack)}
                                  type="button"
                                >
                                  {playerUpNextTrack.artists}
                                </button>
                              ) : null}
                            </div>
                            <div className="player-overlay-up-next-actions">
                              <button
                                aria-label={playerTrackLoopEnabled ? "Unloop current song" : "Loop current song"}
                                aria-pressed={playerTrackLoopEnabled}
                                className={`player-queue-header-button${playerTrackLoopEnabled ? " player-queue-header-button-active player-queue-header-toggle-active" : ""}${liveReadOnlyMode ? " player-control-readonly" : ""}`}
                                disabled={!playerDisplayTrack}
                                onClick={() => void handleTrackLoopClick()}
                                title={playerTrackLoopEnabled ? "Unloop current song" : "Loop current song"}
                                type="button"
                              >
                                <svg aria-hidden="true" viewBox="0 0 24 24">
                                  <path d="M7 7h8.8L14 5.2l1.4-1.4L19.6 8l-4.2 4.2-1.4-1.4L15.8 9H7a3 3 0 0 0 0 6h1v2H7A5 5 0 0 1 7 7Zm10 10h-4v-2h4a3 3 0 0 0 0-6h-1V7h1a5 5 0 0 1 0 10Zm-6-1h2v2h-2v-2Z" />
                                </svg>
                              </button>
                              {renderQueueDelayControl()}
                              <button
                                aria-label={playerQueueShuffleEnabled ? "Restore queue order" : "Shuffle unplayed queue"}
                                aria-pressed={playerQueueShuffleEnabled}
                                className={`player-queue-header-button${playerQueueShuffleEnabled ? " player-queue-header-button-active player-queue-shuffle-active" : ""}${liveReadOnlyMode ? " player-control-readonly" : ""}`}
                                disabled={!queueShuffleAvailable && !playerQueueShuffleEnabled}
                                onClick={() => void handleQueueShuffleClick()}
                                title={playerQueueShuffleEnabled ? "Restore queue order" : "Shuffle unplayed queue"}
                                type="button"
                              >
                                <svg aria-hidden="true" viewBox="0 0 24 24">
                                  <path d="M16.8 3.9 21 8.1l-4.2 4.2-1.4-1.4 1.8-1.8h-1.6c-2 0-3.4.8-4.5 2.4l-1.2 1.8c-1.4 2.1-3.4 3.2-5.9 3.2H3v-2h1c1.9 0 3.3-.8 4.3-2.4l1.2-1.8c1.5-2.1 3.5-3.2 6.1-3.2h1.6l-1.8-1.8 1.4-1.4ZM3 7.5h1c2.1 0 3.7.8 5 2.5l-1.2 1.8C6.8 10.3 5.6 9.5 4 9.5H3v-2Zm9.7 5.9c.8 1 1.8 1.6 3.1 1.6h1.4l-1.8-1.8 1.4-1.4L21 16l-4.2 4.2-1.4-1.4 1.8-1.8h-1.4c-2 0-3.6-.8-4.8-2.3l1.1-1.7.6.4Z" />
                                </svg>
                              </button>
                            </div>
                          </div>

                          {usingLivePlaybackSnapshot && liveAwaitingNextTrack ? (
                            <p className="empty-copy">Track ended. Checking for the next song...</p>
                          ) : null}
                          {playerError ? <p className="empty-copy">{playerError}</p> : null}
                        </div>

                        <aside className="player-recent-column player-queue-column" aria-label={playerQueueSource === "listenlab" ? "ListenLab queue" : "Spotify queue"}>
                          <div className="player-recent-header">
                            {playerQueueContext?.url ? (
                              <h3>
                                <a className="player-queue-title-link" href={playerQueueContext.url} rel="noreferrer" target="_blank">
                                  {playerQueueContext.label}
                                </a>
                              </h3>
                            ) : (
                              <h3>{playerQueueContext?.label ?? "Queue"}</h3>
                            )}
                            <div className="player-queue-header-actions">
                              {playerQueueLoading ? <span>Loading</span> : null}
                              <button
                                aria-label={playerQueueLoopEnabled ? "Stop looping queue" : "Loop queue"}
                                aria-pressed={playerQueueLoopEnabled}
                                className={`player-queue-header-button${playerQueueLoopEnabled ? " player-queue-header-button-active player-queue-header-toggle-active" : ""}${liveReadOnlyMode ? " player-control-readonly" : ""}`}
                                disabled={playerQueueTracks.length === 0}
                                onClick={() => void handleQueueLoopClick()}
                                title={playerQueueLoopEnabled ? "Stop looping queue" : "Loop queue"}
                                type="button"
                              >
                                <svg aria-hidden="true" viewBox="0 0 24 24">
                                  <path d="M7 7h9.2l-1.8-1.8L15.8 3.8 20 8l-4.2 4.2-1.4-1.4L16.2 9H7a3 3 0 0 0 0 6h1v2H7A5 5 0 0 1 7 7Zm10 10H7.8l1.8 1.8-1.4 1.4L4 16l4.2-4.2 1.4 1.4L7.8 15H17a3 3 0 0 0 0-6h-1V7h1a5 5 0 0 1 0 10Z" />
                                </svg>
                              </button>
                              {renderQueueDelayControl()}
                              <button
                                aria-label={playerQueueShuffleEnabled ? "Restore queue order" : "Shuffle unplayed queue"}
                                aria-pressed={playerQueueShuffleEnabled}
                                className={`player-queue-header-button${playerQueueShuffleEnabled ? " player-queue-header-button-active player-queue-shuffle-active" : ""}${liveReadOnlyMode ? " player-control-readonly" : ""}`}
                                disabled={!queueShuffleAvailable && !playerQueueShuffleEnabled}
                                onClick={() => void handleQueueShuffleClick()}
                                title={playerQueueShuffleEnabled ? "Restore queue order" : "Shuffle unplayed queue"}
                                type="button"
                              >
                                <svg aria-hidden="true" viewBox="0 0 24 24">
                                  <path d="M16.8 3.9 21 8.1l-4.2 4.2-1.4-1.4 1.8-1.8h-1.6c-2 0-3.4.8-4.5 2.4l-1.2 1.8c-1.4 2.1-3.4 3.2-5.9 3.2H3v-2h1c1.9 0 3.3-.8 4.3-2.4l1.2-1.8c1.5-2.1 3.5-3.2 6.1-3.2h1.6l-1.8-1.8 1.4-1.4ZM3 7.5h1c2.1 0 3.7.8 5 2.5l-1.2 1.8C6.8 10.3 5.6 9.5 4 9.5H3v-2Zm9.7 5.9c.8 1 1.8 1.6 3.1 1.6h1.4l-1.8-1.8 1.4-1.4L21 16l-4.2 4.2-1.4-1.4 1.8-1.8h-1.4c-2 0-3.6-.8-4.8-2.3l1.1-1.7.6.4Z" />
                                </svg>
                              </button>
                              <div className="player-queue-settings">
                                <button
                                  aria-expanded={playerQueueSettingsOpen}
                                  aria-label="Queue settings"
                                  className={`player-queue-header-button${liveReadOnlyMode ? " player-control-readonly" : ""}`}
                                  onClick={() => {
                                    setPlayerQueueSettingsOpen((current) => !current);
                                    setPlayerQueuePauseMenuOpen(false);
                                  }}
                                  title="Queue settings"
                                  type="button"
                                >
                                  <svg aria-hidden="true" viewBox="0 0 24 24">
                                    <path d="M19.4 13.5c.1-.5.1-1 .1-1.5s0-1-.1-1.5l2-1.5-2-3.5-2.4 1a7.8 7.8 0 0 0-2.6-1.5L14 2h-4l-.4 3a7.8 7.8 0 0 0-2.6 1.5l-2.4-1-2 3.5 2 1.5c-.1.5-.1 1-.1 1.5s0 1 .1 1.5l-2 1.5 2 3.5 2.4-1a7.8 7.8 0 0 0 2.6 1.5l.4 3h4l.4-3a7.8 7.8 0 0 0 2.6-1.5l2.4 1 2-3.5-2-1.5ZM12 15.5a3.5 3.5 0 1 1 0-7 3.5 3.5 0 0 1 0 7Z" />
                                  </svg>
                                </button>
                                {playerQueueSettingsOpen ? (
                                  <div className="player-queue-settings-menu">
                                    <button
                                      onClick={() => {
                                        setPlayerQueueOrganizeMode((current) => !current);
                                        setPlayerQueueSettingsOpen(false);
                                      }}
                                      type="button"
                                    >
                                      {playerQueueOrganizeMode ? "Done organizing" : "Organize"}
                                    </button>
                                    <button
                                      className={liveReadOnlyMode ? "player-control-readonly" : undefined}
                                      disabled={playerQueueTracks.length === 0}
                                      onClick={() => void handleClearPlayerQueueClick()}
                                      type="button"
                                    >
                                      Clear queue
                                    </button>
                                  </div>
                                ) : null}
                              </div>
                            </div>
                          </div>
                          {playerQueueOrganizeMode ? (
                            <div className="player-queue-organize-bar">
                              <label>
                                <span>Sort</span>
                                <select value={playerQueueSortMode} onChange={(event) => sortPlayerQueue(event.currentTarget.value as typeof playerQueueSortMode)}>
                                  <option value="custom">Custom</option>
                                  <option value="length">Length</option>
                                  <option value="az">A-Z</option>
                                  <option value="recent">Recently played</option>
                                </select>
                              </label>
                              <label>
                                <span>Group by</span>
                                <select value={playerQueueGroupMode} onChange={(event) => groupPlayerQueue(event.currentTarget.value as typeof playerQueueGroupMode)}>
                                  <option value="custom">Custom</option>
                                  <option value="artist">Artist</option>
                                  <option value="album">Album</option>
                                </select>
                              </label>
                            </div>
                          ) : null}
                          <div className="player-recent-list">
                            {playerQueueTracks.map((track, index) => {
                              const isCurrentQueueTrack = hasActiveQueueCursor && index === activeQueueCursor;
                              const isLoopedQueueTrack = playerTrackLoopEnabled && isCurrentQueueTrack;
                              const isUpNextQueueTrack = !playerTrackLoopEnabled && hasActiveQueueCursor && index === activeQueueCursor + 1;
                              const isQueueDimmedByTrackLoop = playerTrackLoopEnabled && !isCurrentQueueTrack;
                              const isPlayedQueueTrack = playerQueueSource === "listenlab" && playerQueuePlayedKeys.has(queueTrackIdentity(track) ?? "");
                              return (
                                <div
                                  className={`player-recent-row player-queue-row${playerQueueOrganizeMode ? " player-queue-row-organizing" : ""}${playerQueueDragIndex === index ? " player-queue-row-dragging" : ""}${isCurrentQueueTrack ? " player-queue-row-current" : ""}${isUpNextQueueTrack || isLoopedQueueTrack ? " player-queue-row-up-next" : ""}${isQueueDimmedByTrackLoop ? " player-queue-row-muted" : ""}`}
                                  data-player-queue-role={isUpNextQueueTrack ? "up-next" : (isCurrentQueueTrack ? "current" : undefined)}
                                  draggable={playerQueueOrganizeMode}
                                  key={`${track.uri ?? track.trackId ?? track.name}-${index}`}
                                  onDragEnd={() => setPlayerQueueDragIndex(null)}
                                  onDragOver={(event) => {
                                    if (playerQueueOrganizeMode) {
                                      event.preventDefault();
                                    }
                                  }}
                                  onDragStart={() => setPlayerQueueDragIndex(index)}
                                  onDrop={(event) => {
                                    event.preventDefault();
                                    if (playerQueueDragIndex != null) {
                                      moveQueueTrack(playerQueueDragIndex, index);
                                      setPlayerQueueDragIndex(null);
                                    }
                                  }}
                                >
                                  {playerQueueOrganizeMode ? (
                                    <button aria-label={`Remove ${track.name} from queue`} className="player-queue-remove-button" onClick={() => removeQueueTrackAtIndex(index)} type="button">X</button>
                                  ) : null}
                                  {playerQueueOrganizeMode ? (
                                    <span className="player-queue-cover-button player-queue-cover-static" aria-hidden="true">
                                      {track.image ? (
                                        <img alt="" className="player-recent-cover" src={track.image} />
                                      ) : (
                                        <span className="player-recent-cover player-recent-cover-fallback">{track.name.slice(0, 1).toUpperCase()}</span>
                                      )}
                                    </span>
                                  ) : (
                                    <button
                                      aria-label={`Play ${track.name}`}
                                      className="player-queue-cover-button"
                                      disabled={!track.uri}
                                      onClick={() => void playQueueTrackAtIndex(index)}
                                      type="button"
                                    >
                                      {track.image ? (
                                        <img alt="" className="player-recent-cover" src={track.image} />
                                      ) : (
                                        <span className="player-recent-cover player-recent-cover-fallback" aria-hidden="true">
                                          {track.name.slice(0, 1).toUpperCase()}
                                        </span>
                                      )}
                                      <span className="player-queue-cover-play" aria-hidden="true">
                                        <svg viewBox="0 0 24 24">
                                          <path d="M8 5.5v13l10-6.5-10-6.5Z" />
                                        </svg>
                                      </span>
                                    </button>
                                  )}
                                  {playerQueueOrganizeMode ? (
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
                                      <span className="player-recent-artist single-line-ellipsis">
                                        {track.artists}
                                      </span>
                                    </div>
                                  ) : (
                                    <button
                                      className="player-recent-copy player-queue-copy-button"
                                      onClick={() => openQueuePlayerTrackDetails(track)}
                                      type="button"
                                    >
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
                                      <span className="player-recent-artist single-line-ellipsis">
                                        {track.artists}
                                      </span>
                                    </button>
                                  )}
                                  {isCurrentQueueTrack && !isLoopedQueueTrack ? <span className="player-queue-status">Current</span> : null}
                                  {isLoopedQueueTrack ? (
                                    <button
                                      aria-label="Unloop current song"
                                      className="player-queue-status player-queue-status-next player-queue-loop-status"
                                      onClick={unloopCurrentTrack}
                                      title="Unloop"
                                      type="button"
                                    >
                                      <span className="player-queue-loop-status-default">Looped</span>
                                      <span className="player-queue-loop-status-hover">Unloop</span>
                                    </button>
                                  ) : null}
                                  {isUpNextQueueTrack ? <span className="player-queue-status player-queue-status-next">Up next</span> : null}
                                  {isPlayedQueueTrack && !isCurrentQueueTrack && !isUpNextQueueTrack ? <span className="player-queue-status player-queue-status-played">Played</span> : null}
                                </div>
                              );
                            })}
                            {!playerQueueLoading && playerQueueTracks.length === 0 ? (
                              <p className="empty-copy player-recent-empty">No queued songs were returned.</p>
                            ) : null}
                            {playerQueueError ? (
                              <p className="empty-copy player-recent-empty">{playerQueueError}</p>
                            ) : null}
                          </div>
                        </aside>
                      </div>
                    </section>
                  ) : null}
                </div>
                ) : null}

                <div className="profile-menu-shell profile-menu-shell-inline" ref={profileMenuRef}>
                  <button
                    aria-expanded={profileMenuOpen}
                    className="bar-trigger bar-trigger-user"
                    onClick={() => {
                      setProfileMenuOpen((current) => !current);
                      setBrandMenuOpen(false);
                      setExperimentalMenuOpen(false);
                      setPlayerMenuOpen(false);
                    }}
                    type="button"
                  >
                    <span className="profile-username profile-username-nav single-line-ellipsis">
                      @{profile.username ?? "spotify-user"}
                      <span
                        aria-label={experienceMode === "local" ? "Restricted local mode" : "Full Spotify experience"}
                        className={`experience-mode-indicator${experienceMode === "local" ? " experience-mode-indicator-local" : ""}`}
                        title={experienceMode === "local" ? "Restricted local mode" : "Full Spotify experience"}
                      />
                    </span>
                  </button>

                  {profileMenuOpen ? (
                    <section className="profile-card top-profile-card profile-menu-card">
                      <div className="profile-panel-top">
                        <a
                          className="profile-identity"
                          href={profile.profile_url ?? undefined}
                          rel="noreferrer"
                          target="_blank"
                        >
                          {profile.image_url ? (
                            <img
                              alt={`${profile.display_name ?? "Spotify user"} profile`}
                              className="profile-image"
                              src={profile.image_url}
                            />
                          ) : (
                            <div className="profile-image profile-image-fallback" aria-hidden="true">
                              {(profile.display_name ?? "S").slice(0, 1).toUpperCase()}
                            </div>
                          )}

                          <div>
                            <h2 className="single-line-ellipsis">{profile.display_name ?? "Spotify user"}</h2>
                            <p className="profile-username single-line-ellipsis">@{profile.username ?? "spotify-user"}</p>
                            {formatListeningSince(profile.history_first_played_at) ? (
                              <p className="profile-history-line">{formatListeningSince(profile.history_first_played_at)}</p>
                            ) : null}
                            {experienceMode === "local" && formatRelativeSyncTime(profile.local_last_synced_at) ? (
                              <p className="profile-history-line">Last synced {formatRelativeSyncTime(profile.local_last_synced_at)}</p>
                            ) : null}
                          </div>
                        </a>
                      </div>
                      {showRateLimitReload ? (
                        <SpotifyCooldownPanel
                          loading={loadingProfile || loadingRecentSection || loadingExtendedProfile}
                          onRetry={handleCooldownRetry}
                          ready={reloadReady}
                          secondsRemaining={reloadSecondsRemaining}
                        />
                      ) : null}
                      <div className="profile-mode-row">
                        {renderExperienceModeToggle()}
                        <button
                          aria-expanded={profileSettingsOpen}
                          className={`profile-settings-button${profileSettingsOpen ? " profile-settings-button-active" : ""}`}
                          onClick={() => setProfileSettingsOpen((current) => !current)}
                          type="button"
                        >
                          {"\u2699"}
                        </button>
                      </div>

                      {profileSettingsOpen ? (
                        <>
                          <div className="profile-settings-divider" aria-hidden="true" />
                          <div className="actions actions-right actions-in-card profile-settings-actions">
                            {analysisMode !== "full" && experienceMode === "full" ? (
                              <button
                                className="primary-button"
                                disabled={loadingExtendedProfile}
                                onClick={() => void loadFullAnalysis()}
                                type="button"
                              >
                                {loadingExtendedProfile ? "Loading full analysis..." : "Load full analysis"}
                              </button>
                            ) : null}
                            <button
                              className="secondary-button"
                              disabled={loadingHistoryRecompute || loadingExtendedProfile}
                              onClick={() => void recomputeHistoryFromLocal()}
                              type="button"
                            >
                              {loadingHistoryRecompute ? "Recomputing history..." : "Recompute from history"}
                            </button>
                            {experienceMode === "full" ? (
                              <button
                                className="primary-button"
                                onClick={handleAuthAction}
                                type="button"
                              >
                                Reconnect Spotify
                              </button>
                            ) : null}
                            <button className="secondary-button" onClick={() => void logout()} type="button">
                              Log out
                            </button>
                            {experienceMode === "full" ? (
                              <a
                                className="secondary-button bar-link-button"
                                href={spotifyAppsUrl}
                                rel="noreferrer"
                                target="_blank"
                              >
                                Revoke permissions
                              </a>
                            ) : null}
                          </div>
                        </>
                      ) : null}
                    </section>
                  ) : null}
                </div>
              </div>
            </nav>
            <DashboardSections
              activityRecentTracks={activityRecentTracks}
              activityPreviewTracks={filterAndDedupeRecentTracksForActivity(activityRecentTracks, recentCompletionFilter, activityRecentTracks.length, likedTrackIdsForDisplay, likedReleaseTrackIdsForDisplay, { likedOnly: recentLikedOnly, taggedOnly: recentTaggedOnly })}
              albumCatalogLookupEnqueueError={albumCatalogLookupEnqueueError}
              albumCatalogLookupEnqueueLoading={albumCatalogLookupEnqueueLoading}
              albumCatalogLookupEnqueueResult={albumCatalogLookupEnqueueResult}
              albumCatalogLookupError={albumCatalogLookupError}
              albumCatalogLookupLastLoadedAt={albumCatalogLookupLastLoadedAt}
              albumCatalogLookupLoading={albumCatalogLookupLoading}
              albumCatalogLookupQ={albumCatalogLookupQ}
              albumCatalogLookupResult={albumCatalogLookupResult}
              albumCatalogLookupStatus={albumCatalogLookupStatus}
              allTimeLikedMatchCount={allTimeLikedMatchCount}
              allTimeTopTracks={allTimeTopTracks}
              allTimeTopTracksAvailableForDisplay={allTimeTopTracksAvailableForDisplay}
              allTimeTrackIdCount={allTimeTrackIdCount}
              analysisMode={analysisMode}
              appPage={appPage}
              activePlaylistPlayback={activePlaylistPlayback}
              cachedLikedTracks={cachedLikedTracks}
              catalogBackfillAlbumTracklistPolicy={catalogBackfillAlbumTracklistPolicy}
              catalogBackfillCoverage={catalogBackfillCoverage}
              catalogBackfillCoverageError={catalogBackfillCoverageError}
              catalogBackfillCoverageLastLoadedAt={catalogBackfillCoverageLastLoadedAt}
              catalogBackfillCoverageLoading={catalogBackfillCoverageLoading}
              catalogBackfillForceRefresh={catalogBackfillForceRefresh}
              catalogBackfillFullRunMode={catalogBackfillFullRunMode}
              catalogBackfillIncludeAlbums={catalogBackfillIncludeAlbums}
              catalogBackfillLatestResult={catalogBackfillLatestResult}
              catalogBackfillLimit={catalogBackfillLimit}
              catalogBackfillMarket={catalogBackfillMarket}
              catalogBackfillMaxAlbumTracksPagesPerAlbum={catalogBackfillMaxAlbumTracksPagesPerAlbum}
              catalogBackfillMaxRequests={catalogBackfillMaxRequests}
              catalogBackfillMaxRuntimeSeconds={catalogBackfillMaxRuntimeSeconds}
              catalogBackfillOffset={catalogBackfillOffset}
              catalogBackfillQueue={catalogBackfillQueue}
              catalogBackfillQueueError={catalogBackfillQueueError}
              catalogBackfillQueueLastLoadedAt={catalogBackfillQueueLastLoadedAt}
              catalogBackfillQueueLoading={catalogBackfillQueueLoading}
              catalogBackfillQueueReasonFilter={catalogBackfillQueueReasonFilter}
              catalogBackfillQueueRepairLoading={catalogBackfillQueueRepairLoading}
              catalogBackfillQueueRepairMessage={catalogBackfillQueueRepairMessage}
              catalogBackfillQueueStatusFilter={catalogBackfillQueueStatusFilter}
              catalogBackfillRunError={catalogBackfillRunError}
              catalogBackfillRunLoading={catalogBackfillRunLoading}
              catalogBackfillRuns={catalogBackfillRuns}
              catalogBackfillRunsError={catalogBackfillRunsError}
              catalogBackfillRunsLastLoadedAt={catalogBackfillRunsLastLoadedAt}
              catalogBackfillRunsLoading={catalogBackfillRunsLoading}
              catalogBackfillTab={catalogBackfillTab}
              collapseRecentPreviewTracks={collapseRecentPreviewTracks}
              collapseTrackPreviewAlbums={collapseTrackPreviewAlbums}
              enqueueVisibleIncompleteLookupAlbums={enqueueVisibleIncompleteLookupAlbums}
              enqueueVisibleIncompleteLookupTracks={enqueueVisibleIncompleteLookupTracks}
              experienceMode={experienceMode}
              apiBaseUrl={apiBaseUrl}
              likedTracksAvailableForActivity={likedTracksAvailableForActivity}
              likedTracksCacheStatus={likedTracksCacheStatus}
              likedTracksCountMode={likedTracksCountMode}
              likedTracksError={likedTracksError}
              likedTracksForActivity={likedTracksForActivity}
              likedTracksForActivitySource={likedTracksForActivitySource}
              likedTracksLoading={likedTracksLoading}
              likedTracksShuffleEnabled={likedTracksShuffleEnabled}
              likedTracksSortMode={likedTracksSortMode}
              likedTracksSyncing={likedTracksSyncing}
              likedTracksTotalLabel={likedTracksTotalLabel}
              listeningLogError={listeningLogError}
              listeningLogHasMore={listeningLogHasMore}
              listeningLogLastLoadedAt={listeningLogLastLoadedAt}
              listeningLogLoading={listeningLogLoading}
              listeningLogOffset={listeningLogOffset}
              listeningLogTracks={listeningLogTracks}
              loadActiveSearchLookup={loadActiveSearchLookup}
              loadCatalogBackfillCoverage={loadCatalogBackfillCoverage}
              loadCatalogBackfillQueue={loadCatalogBackfillQueue}
              loadCatalogBackfillRuns={loadCatalogBackfillRuns}
              loadListeningLogBatch={loadListeningLogBatch}
              loadingRecentSection={loadingRecentSection}
              mergedTrackSourceFilter={mergedTrackSourceFilter}
              mergedTracks={mergedTracks}
              mergedTracksError={mergedTracksError}
              mergedTracksLastLoadedAt={mergedTracksLastLoadedAt}
              mergedTracksLoaded={mergedTracksLoaded}
              mergedTracksLoading={mergedTracksLoading}
              moveSectionPage={moveSectionPage}
              openAlbumLookupPreview={openAlbumLookupPreview}
              openDebugSessions={openDebugSessions}
              openDebugTracks={openDebugTracks}
              openListeningLogPage={openListeningLogPage}
              openSections={openSections}
              openTrackLookupPreview={openTrackLookupPreview}
              previewItems={previewItems}
              profile={profile}
              quickUnavailableCopy={quickUnavailableCopy}
              rankMovementFilter={rankMovementFilter}
              recentDebugSourceFilter={recentDebugSourceFilter}
              recentCompletionFilter={recentCompletionFilter}
              recentLikedOnly={recentLikedOnly}
              recentTaggedOnly={recentTaggedOnly}
              recentRange={recentRange}
              recentTopTracksAvailableForDisplay={recentTopTracksAvailableForDisplay}
              recentTopTracksForDisplay={recentTopTracksForDisplay}
              recentUnavailableCopy={recentUnavailableCopy}
              refreshRecentSection={refreshRecentSection}
              reloadTrackRankings={reloadTrackRankings}
              renderHomePlayerPanel={renderHomePlayerPanel}
              renderIdentityAuditPage={renderIdentityAuditPage}
              renderMergedTrackSourceFilterToggle={renderMergedTrackSourceFilterToggle}
              renderRankMovementFilterToggle={renderRankMovementFilterToggle}
              renderRecentRangeHeader={renderRecentRangeHeader}
              renderSectionTitle={renderSectionTitle}
              renderTrackColumn={renderTrackColumn}
              renderTrackRankingToggle={renderTrackRankingToggle}
              repairCatalogBackfillQueueStatuses={repairCatalogBackfillQueueStatuses}
              runCatalogBackfill={runCatalogBackfill}
              searchLookupEntityType={searchLookupEntityType}
              searchLookupQueueStatus={searchLookupQueueStatus}
              searchLookupSort={searchLookupSort}
              sectionPages={sectionPages}
              setAlbumCatalogLookupEnqueueError={setAlbumCatalogLookupEnqueueError}
              setAlbumCatalogLookupEnqueueResult={setAlbumCatalogLookupEnqueueResult}
              setAlbumCatalogLookupQ={setAlbumCatalogLookupQ}
              setAlbumCatalogLookupStatus={setAlbumCatalogLookupStatus}
              setAppPage={setAppPage}
              setCatalogBackfillAlbumTracklistPolicy={setCatalogBackfillAlbumTracklistPolicy}
              setCatalogBackfillForceRefresh={setCatalogBackfillForceRefresh}
              setCatalogBackfillFullRunMode={setCatalogBackfillFullRunMode}
              setCatalogBackfillIncludeAlbums={setCatalogBackfillIncludeAlbums}
              setCatalogBackfillLimit={setCatalogBackfillLimit}
              setCatalogBackfillMarket={setCatalogBackfillMarket}
              setCatalogBackfillMaxAlbumTracksPagesPerAlbum={setCatalogBackfillMaxAlbumTracksPagesPerAlbum}
              setCatalogBackfillMaxRequests={setCatalogBackfillMaxRequests}
              setCatalogBackfillMaxRuntimeSeconds={setCatalogBackfillMaxRuntimeSeconds}
              setCatalogBackfillOffset={setCatalogBackfillOffset}
              setCatalogBackfillTab={setCatalogBackfillTab}
              setLikedTracksCountMode={setLikedTracksCountMode}
              setLikedTracksShuffleEnabled={setLikedTracksShuffleEnabled}
              setLikedTracksShuffleNonce={setLikedTracksShuffleNonce}
              setLikedTracksSortMode={setLikedTracksSortMode}
              setListeningLogError={setListeningLogError}
              setListeningLogHasMore={setListeningLogHasMore}
              setListeningLogLastLoadedAt={setListeningLogLastLoadedAt}
              setListeningLogLoaded={setListeningLogLoaded}
              setListeningLogOffset={setListeningLogOffset}
              setListeningLogTracks={setListeningLogTracks}
              setOpenDebugSessions={setOpenDebugSessions}
              setOpenDebugTracks={setOpenDebugTracks}
              setRecentDebugSourceFilter={setRecentDebugSourceFilter}
              setRecentCompletionFilter={setRecentCompletionFilter}
              setRecentLikedOnly={setRecentLikedOnly}
              setRecentTaggedOnly={setRecentTaggedOnly}
              setSearchLookupEntityType={setSearchLookupEntityType}
              setSearchLookupQueueStatus={setSearchLookupQueueStatus}
              setSearchLookupSort={setSearchLookupSort}
              setSelectedPreview={setSelectedPreview}
              hidePlaylistFromListenLab={hidePlaylistFromListenLab}
              unhidePlaylistInListenLab={unhidePlaylistInListenLab}
              deletePlaylistFromSpotify={deletePlaylistFromSpotify}
              setShowDebugLinkFields={setShowDebugLinkFields}
              setTrackCatalogLookupStatus={setTrackCatalogLookupStatus}
              showDebugLinkFields={showDebugLinkFields}
              spotifyCooldownActive={spotifyCooldownActive}
              syncLikedTracks={syncLikedTracks}
              toggleSection={toggleSection}
              trackCatalogLookupError={trackCatalogLookupError}
              trackCatalogLookupLastLoadedAt={trackCatalogLookupLastLoadedAt}
              trackCatalogLookupLoading={trackCatalogLookupLoading}
              trackCatalogLookupResult={trackCatalogLookupResult}
              trackCatalogLookupStatus={trackCatalogLookupStatus}
              trackRankingMode={trackRankingMode}
              usingLikedTracksFallback={usingLikedTracksFallback}
              visibleItemsWithPageSize={visibleItemsWithPageSize}
            />
          </>
        )}
        </section>
      </main>
      {selectedPreview ? (
        <Suspense fallback={null}>
          <DetailPreviewModal
            apiBaseUrl={apiBaseUrl}
            albumTrackEntries={albumTrackEntries}
            albumTrackEntriesError={albumTrackEntriesError}
            albumTrackEntriesLoading={albumTrackEntriesLoading}
            albumTrackEntriesPartial={albumTrackEntriesPartial}
            albumFamilyDiscScrollTarget={albumFamilyDiscScrollTarget}
            albumTrackFetchFromSpotifyLoading={albumTrackSpotifyFetchPending}
            albumTrackMoreOnSpotifyUrl={
              selectedPreview.kind === "track" && spotifyCooldownActive && (albumTrackEntriesPartial || Boolean(albumTrackEntriesError))
                ? (
                  selectedPreview.sourceAlbumUrl
                  || selectedPreview.sourceTrack?.album_url
                  || spotifyEntityUrl("album", selectedPreviewReleasePlaybackSourceVersion?.album_id ?? albumIdFromPreview(selectedPreview))
                  || null
                )
                : null
            }
            albumTrackIsExactKnownLiked={albumTrackIsExactKnownLiked}
            albumTrackIsKnownLiked={albumTrackIsKnownLiked}
            albumTrackLastSortMode={albumTrackLastSortMode}
            albumTrackListRef={albumTrackListRef}
            albumTrackPreviewKey={albumTrackPreviewKey}
            albumTracklistSummaryLabel={albumTracklistSummaryLabel}
            albumFamilyContext={selectedAlbumFamilyContext}
            artistEntriesForAlbumTrack={artistEntriesForAlbumTrack}
            artistNameMatches={artistNameMatches}
            backendSelectedPreviewArtistAlbums={backendSelectedPreviewArtistAlbums}
            buildAlbumPlaybackQueue={buildAlbumPlaybackQueue}
            clearAlbumWithArtistHighlight={clearAlbumWithArtistHighlight}
            clearAlbumFamilyDiscScrollTarget={() => setAlbumFamilyDiscScrollTarget(null)}
            currentTrack={currentTrack}
            detailOptionsOpen={detailOptionsOpen}
            displayAlbumTrackEntries={displayAlbumTrackEntries}
            formatCompactRelativeAge={formatCompactRelativeAge}
            formatPlaybackClock={formatPlaybackClock}
            handleAlbumPlayAll={handleAlbumPlayAll}
            handlePlaybackAction={handlePlaybackAction}
            includeAlbumFamilyTracks={includeAlbumFamilyTracks}
            hasPremiumPlayback={hasPremiumPlayback}
            hoveredAlbumWithArtistName={hoveredAlbumWithArtistName}
            isTrackPlaying={isTrackPlaying}
            localStarredTrackById={localStarredTrackById}
            nextLastPlayedSortMode={nextLastPlayedSortMode}
            openAlbumTrackPreview={openAlbumTrackPreview}
            openAlbumWithArtistPreview={openAlbumWithArtistPreview}
            openArtistAlbumPreview={openArtistAlbumPreview}
            openRecordingCandidateReleaseTrack={openRecordingCandidateReleaseTrack}
            openRecentTrackAlbumPreview={openRecentTrackAlbumPreview}
            openRecentTrackArtistPreview={openRecentTrackArtistPreview}
            openRecentPlayerTrackDetails={openRecentPlayerTrackDetails}
            openReleaseSourceVersion={openReleaseSourceVersion}
            openSelectedAlbumArtistPreview={openSelectedAlbumArtistPreview}
            openSelectedArtistMemberPreview={openSelectedArtistMemberPreview}
            openSelectedTrackAlbumPreview={openSelectedTrackAlbumPreview}
            openSelectedTrackArtistPreview={openSelectedTrackArtistPreview}
            openPlaylistMembershipPreview={openPlaylistMembershipPreview}
            onAddSelectedTrackToPlaylists={addSelectedTrackToPlaylists}
            pausedTimeFlashOn={pausedTimeFlashOn}
            playbackDurationMs={playbackDurationMs}
            playbackPaused={playbackPaused}
            playbackPositionMs={playbackPositionMs}
            playlistTrackEntries={playlistTrackEntries}
            playlistTrackEntriesError={playlistTrackEntriesError}
            playlistTrackEntriesHasMore={playlistTrackEntriesHasMore}
            playlistTrackEntriesLoading={playlistTrackEntriesLoading}
            playlistTrackEntriesOffset={playlistTrackEntriesOffset}
            playlistTrackEntriesShowCollaborativeColumns={selectedPreviewPlaylistIsCollaborative}
            playlistTrackEntriesTotal={playlistTrackEntriesTotal}
            loadMorePlaylistTrackEntries={loadMorePlaylistTrackEntries}
            recentTrackIsKnownLiked={recentTrackIsKnownLiked}
            togglePlaylistTrackPreview={togglePlaylistTrackPreview}
            handlePlaylistPlayAll={handlePlaylistPlayAll}
            handlePlaylistTrackPlayback={handlePlaylistTrackPlayback}
            playerSummaryFromAlbumTrack={playerSummaryFromAlbumTrack}
            previewAlbumHeading={previewAlbumHeading}
            previewPlayedTrackKeys={previewPlayedTrackKeys}
            previewingTrackUri={previewingTrackUri}
            recordingMemberAlbumImageUrl={recordingMemberAlbumImageUrl}
            recordingMemberAlbumName={recordingMemberAlbumName}
            recordingMemberReleaseYear={recordingMemberReleaseYear}
            recordingVariationStripRef={recordingVariationStripRef}
            releaseSourceVersionAlbumImageUrl={releaseSourceVersionAlbumImageUrl}
            releaseSourceVersionPlayCountLabel={releaseSourceVersionPlayCountLabel}
            renderSelectedPreviewArtistAlbumSection={renderSelectedPreviewArtistAlbumSection}
            renderSelectedPreviewArtistTrackSection={renderSelectedPreviewArtistTrackSection}
            scheduleAlbumWithArtistHighlight={scheduleAlbumWithArtistHighlight}
            scrollRecordingVariationStrip={scrollRecordingVariationStrip}
            selectedAlbumTrackMarkerTop={selectedAlbumTrackMarkerTop}
            selectedPreview={selectedPreview}
            selectedPreviewAlbumGuestArtists={selectedPreviewAlbumGuestArtists}
            selectedPreviewAlbumHasGuestArtists={selectedPreviewAlbumHasGuestArtists}
            selectedPreviewAlbumIsSpotifyLiked={selectedPreviewAlbumIsSpotifyLiked}
            selectedPreviewAlbumMainArtists={selectedPreviewAlbumMainArtists}
            selectedPreviewAlbumSummary={selectedPreviewAlbumSummary}
            selectedPreviewAlbumSpotifyId={selectedPreviewAlbumSpotifyId}
            selectedPreviewAlbumContextTagLabel={selectedPreviewAlbumContextTagLabel}
            selectedPreviewAppearsOnAlbums={selectedPreviewAppearsOnAlbumsForMode}
            selectedPreviewArtistAlbumsForDisplay={selectedPreviewArtistAlbumsForMode}
            selectedPreviewArtistFollowStatusKnown={selectedPreviewArtistFollowStatusKnown}
            selectedPreviewArtistImageUrl={selectedPreviewArtistImageUrl}
            selectedPreviewArtistIsSpotifyFollowed={selectedPreviewArtistIsSpotifyFollowed}
            selectedPreviewArtists={selectedPreviewArtists}
            selectedPreviewCanOpenAlbum={selectedPreviewCanOpenAlbum}
            selectedPreviewCanOpenArtist={selectedPreviewCanOpenArtist}
            selectedPreviewCanonicalTrackTitle={selectedPreviewCanonicalTrackTitle}
            selectedPreviewCurrentSpotifyTrackId={selectedPreviewCurrentSpotifyTrackId}
            selectedPreviewCurrentVersionIsSpotifyLiked={selectedPreviewCurrentVersionIsSpotifyLiked}
            selectedPreviewDetailView={selectedPreviewDetailView}
            selectedPreviewDisplayRelationRows={selectedPreviewDisplayRelationRows}
            selectedPreviewHasReleaseSibling={selectedPreviewHasReleaseSibling}
            selectedPreviewListenedBreakdown={selectedPreviewListenedBreakdown}
            selectedPreviewListenedRangeLabel={selectedPreviewListenedRangeLabel}
            selectedPreviewIsEntityBookmarked={selectedPreviewIsEntityBookmarked}
            selectedPreviewIsBookmarked={selectedPreviewIsBookmarked}
            selectedPreviewIsKnownLiked={selectedPreviewIsKnownLiked}
            selectedPreviewIsSharedArtistPage={selectedPreviewIsSharedArtistPage}
            selectedPreviewListenBreakdown={selectedPreviewListenBreakdown}
            selectedPreviewListenCountLabel={selectedPreviewListenCountLabel}
            selectedPreviewOtherRecordingMembers={selectedPreviewOtherRecordingMembers}
            selectedPreviewPlaybackTrackUri={selectedPreviewPlaybackTrackUri}
            selectedPreviewPlaylistMemberships={selectedPreviewPlaylistMemberships}
            selectedPreviewPlaylistMembershipsLoading={selectedPreviewPlaylistMembershipsLoading}
            selectedPreviewPlaylistIndexStatus={selectedPreviewPlaylistIndexStatus}
            selectedPreviewPlaylistOwnerFollowedByYou={selectedPreviewPlaylistOwnerFollowedByYou}
            selectedPreviewAvailablePlaylists={(profile?.owned_playlists ?? []).filter((playlist) => !playlist.hidden_by_user)}
            selectedPreviewPrimaryArtistAlbums={selectedPreviewPrimaryArtistAlbumsForMode}
            selectedPreviewRecordingCandidateError={selectedPreviewRecordingCandidateError}
            selectedPreviewRecordingMembers={selectedPreviewRecordingMembers}
            selectedPreviewReleaseAlbumVariationCount={selectedPreviewReleaseAlbumVariationCount}
            selectedPreviewReleaseSiblingSourceCount={selectedPreviewReleaseSiblingSourceCount}
            selectedPreviewReleaseSourceVersionNeedsArrows={selectedPreviewReleaseSourceVersionNeedsArrows}
            selectedPreviewReleaseSourceVersions={selectedPreviewReleaseSourceVersions}
            selectedPreviewReleaseTrackDetailError={selectedPreviewReleaseTrackDetailError}
            selectedPreviewReleaseTrackDetailReady={selectedPreviewReleaseTrackDetailReady}
            selectedPreviewRelatedAlbums={selectedPreviewRelatedAlbums}
            selectedPreviewStarTrackId={selectedPreviewStarTrackId}
            selectedPreviewTrackDurationLabel={selectedPreviewTrackDurationLabel}
            selectedPreviewTrackGuestArtists={selectedPreviewTrackGuestArtists}
            selectedPreviewTrackMainArtists={selectedPreviewTrackMainArtists}
            selectedPreviewTrackOptimisticSummary={selectedPreviewTrackOptimisticSummary}
            toggleSelectedPreviewEntityBookmark={toggleSelectedPreviewEntityBookmark}
            toggleSelectedPreviewTrackBookmark={toggleSelectedPreviewTrackBookmark}
            setAlbumTrackLastSortMode={setAlbumTrackLastSortMode}
            setDetailOptionsOpen={setDetailOptionsOpen}
            setLocalStarredAlbumById={setLocalStarredAlbumById}
            setLocalStarredTrackById={setLocalStarredTrackById}
            setSelectedPreview={setSelectedPreview}
            setSelectedPreviewDetailView={setSelectedPreviewDetailView}
            spotifyTrackIdFromUri={spotifyTrackIdFromUri}
            switchSelectedTrackAlbumVersion={switchSelectedTrackAlbumVersion}
            toggleAlbumTrackPreview={toggleAlbumTrackPreview}
            trackUriWithFallback={trackUriWithFallback}
            variationSubtitleFromTitle={variationSubtitleFromTitle}
          />
        </Suspense>
      ) : null}
    </>
  );
}
