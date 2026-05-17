import { type ReactNode, useEffect, useMemo, useRef, useState } from "react";
import type {
  SessionResponse,
  ProfileProgressResponse,
  RecentTrack,
  MatchCounts,
  TopPlaylist,
  OwnedPlaylist,
  FollowedArtist,
  TopAlbum,
  ProfileResponse,
  RecentSectionResponse,
  RecentArchiveResponse,
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
  RepresentativePreviewResponse,
  AuthTokenResponse,
  RecentIngestResultResponse,
  RecentBeforeProbeResponse,
  RecentBackfillProbeResponse,
  FullAvailabilityResponse,
  CurrentPlaybackSnapshot,
  CurrentPlaybackResponse,
  PlayerTrackSummary,
  PlayerQueueTrack,
  SpotifyPlayerState,
  AlbumTrackEntry,
  SpotifyPlayerInstance,
  PopupTrackPlaybackOptions
} from "./types/appTypes";
import {
  DEFAULT_PLAYER_VOLUME,
  EXPERIENCE_MODE_STORAGE_KEY,
  IDENTITY_AUDIT_AMBIGUOUS_VISIBLE_STEP,
  INITIAL_OPEN_SECTIONS,
  INITIAL_SECTION_PAGES,
  LIVE_PLAYBACK_POLL_INTERVAL_MS,
  LIVE_PLAYBACK_PROGRESS_TICK_MS,
  LIVE_TRACK_END_RECENT_POLL_DELAY_MS,
  MERGED_TRACK_SOURCE_FILTER_OPTIONS,
  PAGE_SIZE,
  PLAYER_RECENT_FETCH_LIMIT,
  PLAYLISTS_PAGE_SIZE,
  PREVIEW_RAMP_DURATION_MS,
  PREVIEW_RAMP_START_VOLUME,
  PREVIEW_RAMP_STEP_MS,
  RANK_MOVEMENT_FILTER_OPTIONS,
  RECENT_RANGE_OPTIONS,
  RECENT_SECTION_FETCH_LIMIT,
  githubRepoUrl,
  spotifyAppsUrl,
  spotifyLogoDataUrl,
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
  fetchIdentityAuditSavedSubmissionDryRun
} from "./api/appApi";
import { syncQueuePlaylist } from "./api/playbackApi";
import { CatalogBackfillPage } from "./components/catalogBackfill/CatalogBackfillPage";
import { DashboardListCard } from "./components/dashboard/DashboardListCard";
import { DashboardPaging } from "./components/dashboard/DashboardPaging";
import {
  auditList,
  auditNumber,
  identityAuditMeta,
  identityAuditTitle,
  renderIdentityAuditExample,
  renderIdentityAuditGroup,
  type TrackIdentityAuditExample,
} from "./components/identityAudit/IdentityAuditDiagnostics";
import { FormulaLabPage } from "./components/formulaLab/FormulaLabPage";
import {
  IssueFeed,
  issueSeverityForCount,
  type NormalizedAuditIssue,
} from "./components/identityAudit/IssueFeed";
import { FullAnalysisOverlay, LoadingScreen } from "./components/loading/LoadingScreens";
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
  currentTrackFromState,
  dedupeRecentTracksForPlayer,
  formatPlaybackClock,
  queuePlaylistTrackUris,
  queueRepeatsTrack,
  recentTracksToPlayerQueueTracks,
  spotifyTrackIdFromUri,
  spotifyTrackUrl,
  trackUriWithFallback,
} from "./utils/playbackUtils";
import {
  albumLookupRowCanBulkPrioritize,
  capTracksPerAlbum,
  clampProgress,
  collapseRecentPreviewTracks,
  collapseTrackPreviewAlbums,
  emptySlots,
  firstArtistFromRecentTrack,
  formatAlbumBreadth,
  formatAlbumSummary,
  formatCooldownCopy,
  formatCooldownTimerLabel,
  formatDebugTimestamp,
  formatDurationMs,
  formatFormulaRankDelta,
  formatHistoryDebugLine,
  formatListeningSince,
  formatLoadingStatusDetailed,
  formatLoadingStatusUi,
  formatMonthDay,
  formatPlaylistSummary,
  formatRelativeSyncTime,
  formatTrackLongevity,
  formatTrackRankingMetric,
  formatTrackSourceBadge,
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
  representativeReasonMessage,
  splitItems,
  sortedTracksForView,
  trackLookupRowCanBulkPrioritize,
} from "./utils/dashboardUtils";

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "/api";
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
  const [recentIngestResult, setRecentIngestResult] = useState<RecentIngestResultResponse | null>(null);
  const [recentBeforeProbeResult, setRecentBeforeProbeResult] = useState<RecentBeforeProbeResponse | null>(null);
  const [recentBackfillProbeResult, setRecentBackfillProbeResult] = useState<RecentBackfillProbeResponse | null>(null);
  const [authTransitioning, setAuthTransitioning] = useState(false);
  const [loadingProfile, setLoadingProfile] = useState(false);
  const [loadingExtendedProfile, setLoadingExtendedProfile] = useState(false);
  const [loadingRecentSection, setLoadingRecentSection] = useState(false);
  const [loadingHistoryRecompute, setLoadingHistoryRecompute] = useState(false);
  const [profileLoadAttempted, setProfileLoadAttempted] = useState(false);
  const [reloadCooldownUntil, setReloadCooldownUntil] = useState<number | null>(null);
  const [reloadCooldownDurationMs, setReloadCooldownDurationMs] = useState<number>(60_000);
  const [reloadCountdownNow, setReloadCountdownNow] = useState(Date.now());
  const [openSections, setOpenSections] = useState<Record<SectionKey, boolean>>(INITIAL_OPEN_SECTIONS);
  const [sectionPages, setSectionPages] = useState<Record<SectionKey, number>>(INITIAL_SECTION_PAGES);
  const [profileMenuOpen, setProfileMenuOpen] = useState(false);
  const [brandMenuOpen, setBrandMenuOpen] = useState(false);
  const [experimentalMenuOpen, setExperimentalMenuOpen] = useState(false);
  const [profileSettingsOpen, setProfileSettingsOpen] = useState(false);
  const [playerMenuOpen, setPlayerMenuOpen] = useState(false);
  const [rateLimitMenuOpen, setRateLimitMenuOpen] = useState(false);
  const [selectedPreview, setSelectedPreview] = useState<PreviewItem | null>(null);
  const [albumTrackEntries, setAlbumTrackEntries] = useState<AlbumTrackEntry[]>([]);
  const [albumTrackEntriesLoading, setAlbumTrackEntriesLoading] = useState(false);
  const [albumTrackEntriesError, setAlbumTrackEntriesError] = useState<string | null>(null);
  const [representativeTrack, setRepresentativeTrack] = useState<RecentTrack | null>(null);
  const [representativeLoading, setRepresentativeLoading] = useState(false);
  const [representativeReason, setRepresentativeReason] = useState<string | null>(null);
  const [playerReady, setPlayerReady] = useState(false);
  const [playerError, setPlayerError] = useState<string | null>(null);
  const [playerRecentTracks, setPlayerRecentTracks] = useState<RecentTrack[]>([]);
  const [playerRecentTracksLoading, setPlayerRecentTracksLoading] = useState(false);
  const [playerRecentTracksError, setPlayerRecentTracksError] = useState<string | null>(null);
  const [playerQueueTracks, setPlayerQueueTracks] = useState<PlayerQueueTrack[]>([]);
  const [playerQueueSource, setPlayerQueueSource] = useState<"listenlab" | "spotify" | null>(null);
  const [queuePlaylistUri, setQueuePlaylistUri] = useState<string | null>(null);
  const [playerQueueLoading, setPlayerQueueLoading] = useState(false);
  const [playerQueueError, setPlayerQueueError] = useState<string | null>(null);
  const [activePlayerListenEventId, setActivePlayerListenEventId] = useState<number | null>(null);
  const [currentTrack, setCurrentTrack] = useState<PlayerTrackSummary | null>(null);
  const [playbackPaused, setPlaybackPaused] = useState(true);
  const [playbackPositionMs, setPlaybackPositionMs] = useState(0);
  const [playbackDurationMs, setPlaybackDurationMs] = useState(0);
  const [overlayTrackPlaybackExpanded, setOverlayTrackPlaybackExpanded] = useState(false);
  const [overlaySeekMs, setOverlaySeekMs] = useState<number | null>(null);
  const [pausedTimeFlashOn, setPausedTimeFlashOn] = useState(true);
  const [previewingTrackUri, setPreviewingTrackUri] = useState<string | null>(null);
  const [previewPlayedTrackKeys, setPreviewPlayedTrackKeys] = useState<Set<string>>(new Set());
  const [livePlaybackSnapshot, setLivePlaybackSnapshot] = useState<CurrentPlaybackSnapshot | null>(null);
  const [liveDerivedProgressMs, setLiveDerivedProgressMs] = useState(0);
  const [liveAwaitingNextTrack, setLiveAwaitingNextTrack] = useState(false);
  const [livePlaybackProbeComplete, setLivePlaybackProbeComplete] = useState(false);
  const [pendingSeekMs, setPendingSeekMs] = useState<number | null>(null);
  const [liveControlOverrideUntilMs, setLiveControlOverrideUntilMs] = useState<number | null>(null);
  const [recentRange, setRecentRange] = useState<RecentRange>("short_term");
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
  const [mergedTrackSourceFilter, setMergedTrackSourceFilter] = useState<MergedTrackSourceFilter>("all");
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
  const rateLimitMenuRef = useRef<HTMLDivElement | null>(null);
  const spotifyPlayerRef = useRef<SpotifyPlayerInstance | null>(null);
  const spotifyDeviceIdRef = useRef<string | null>(null);
  const previewStopTimerRef = useRef<number | null>(null);
  const previewVolumeRampTimerRef = useRef<number | null>(null);
  const previewingTrackUriRef = useRef<string | null>(null);
  const currentPlayerVolumeRef = useRef(DEFAULT_PLAYER_VOLUME);
  const loadedAlbumTracksAlbumIdRef = useRef<string | null>(null);
  const trackMappingLineageRequestIdRef = useRef(0);
  const playbackPositionMsRef = useRef(0);
  const liveProgressAnchorRef = useRef<{ baseProgressMs: number; receivedAtMs: number; durationMs: number } | null>(null);
  const liveEndRefreshRequestedRef = useRef(false);
  const profileLoadInFlightRef = useRef(false);
  const extendedLoadInFlightRef = useRef(false);
  const quickRecentAutoAttemptRef = useRef<string | null>(null);
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
  const shouldUseLiveSnapshotDisplay = liveReadOnlyMode || (usingLivePlaybackSnapshot && !currentTrack);
  const playerDisplayTrack: PlayerTrackSummary | null = shouldUseLiveSnapshotDisplay
    ? livePlaybackTrackSummary
    : currentTrack;
  const playerDisplayPaused = shouldUseLiveSnapshotDisplay
    ? !Boolean(livePlaybackSnapshot?.is_playing)
    : playbackPaused;
  const playerDisplayPositionMs = shouldUseLiveSnapshotDisplay
    ? Math.max(0, liveDerivedProgressMs)
    : playbackPositionMs;
  const playerDisplayDurationMs = shouldUseLiveSnapshotDisplay
    ? Math.max(0, Number(livePlaybackSnapshot?.duration_ms ?? 0))
    : playbackDurationMs;
  const selectedPreviewPrimaryArtistName = selectedPreview?.kind === "track"
    ? (
      firstArtistFromRecentTrack(selectedPreview.sourceTrack)?.name
      ?? selectedPreview.artistName
      ?? primaryArtistName(selectedPreview.meta)
      ?? null
    )
    : null;
  const selectedPreviewArtistImageUrl = selectedPreview && selectedPreview.kind === "track"
    ? findArtistImageUrl(selectedPreviewPrimaryArtistName ?? selectedPreview.artistName ?? selectedPreview.meta)
    : null;
  const selectedPreviewCanOpenArtist = Boolean(selectedPreview?.kind === "track" && selectedPreviewPrimaryArtistName);
  const selectedPreviewCanOpenAlbum = Boolean(
    selectedPreview?.kind === "track"
    && (selectedPreview.albumId || selectedPreview.sourceTrack?.album_id || selectedPreview.sourceTrack?.album_name || selectedPreview.detail),
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
  const selectedPreviewTrackIsCurrent = Boolean(
    selectedPreview?.kind === "track"
    && selectedPreviewEffectiveTrackUri
    && currentTrack?.uri === selectedPreviewEffectiveTrackUri,
  );
  const selectedPreviewTrackBaseDurationMs = selectedPreview?.kind === "track"
    ? (
      selectedPreviewMatchedAlbumTrack?.durationMs
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
  const selectedPreviewTrackOptimisticSummary: PlayerTrackSummary | null = selectedPreview?.kind === "track"
    ? {
      name: selectedPreview.label,
      artists: selectedPreviewPrimaryArtistName ?? selectedPreview.meta ?? "Unknown artist",
      album: selectedPreview.sourceTrack?.album_name ?? selectedPreview.detail ?? "Unknown album",
      image: selectedPreview.image ?? null,
      uri: selectedPreviewEffectiveTrackUri,
      durationMs: selectedPreviewTrackBaseDurationMs ?? 0,
    }
    : null;
  const knownPlayerTracks = profile
    ? [
      ...(profile.recent_tracks ?? []),
      ...(profile.top_tracks ?? []),
      ...(profile.recent_top_tracks ?? []),
      ...(profile.recent_likes_tracks ?? []),
    ]
    : [];
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
  const playerDisplayArtist = firstArtistFromRecentTrack(playerDisplayKnownTrack);
  const playerDisplayArtistName = playerDisplayArtist?.name ?? playerDisplayPrimaryArtistFromDisplay ?? null;
  const playerDisplayArtistId = playerDisplayArtist?.artist_id ?? playerDisplayArtist?.id ?? null;
  const playerDisplayAlbumName = playerDisplayKnownTrack?.album_name ?? playerDisplayTrack?.album ?? null;
  const playerDisplayAlbumId = playerDisplayKnownTrack?.album_id ?? null;
  const playerDisplayAlbumYear = playerDisplayKnownTrack?.album_release_year ?? null;
  const playerDisplayAlbumLabel = playerDisplayAlbumName
    ? (playerDisplayAlbumYear ? `${playerDisplayAlbumYear} - ${playerDisplayAlbumName}` : playerDisplayAlbumName)
    : "Choose something to play";
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
  const canControlPlayback = !liveReadOnlyMode || livePlaybackOnListenLabDevice;
  const canSeekSelectedPreview = Boolean(
    canControlPlayback
    && selectedPreviewTrackIsCurrent
    && selectedPreviewTrackBaseDurationMs != null
    && selectedPreviewTrackBaseDurationMs > 0,
  );
  const livePlaybackControlTooltip = liveReadOnlyMode
    ? `Playing on ${livePlaybackSnapshot?.device_name ?? "another device"}. Click to control on ListenLab.`
    : undefined;

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
  const showRateLimitReload = experienceMode === "full" && (spotifyCooldownActive || Boolean(statusMessage && statusMessage.includes("rate-limiting")));
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
    void loadListeningLogBatch(true);
  }, [appPage, listeningLogLoaded, listeningLogLoading, profile, recentDebugSourceFilter]);

  useEffect(() => {
    if (appPage !== "formulaLab" || !profile) {
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
        window.setTimeout(() => {
          if (session?.authenticated && experienceMode === "full") {
            void triggerRecentIngestPollForTrackEnd();
          }
        }, LIVE_TRACK_END_RECENT_POLL_DELAY_MS);
      }
    }, LIVE_PLAYBACK_PROGRESS_TICK_MS);

    return () => {
      window.clearInterval(timer);
    };
  }, [experienceMode, livePlaybackSnapshot?.is_playing, session?.authenticated, usingLivePlaybackSnapshot]);

  useEffect(() => {
    if (usingLivePlaybackSnapshot && pendingSeekMs != null) {
      setPendingSeekMs(null);
    }
  }, [pendingSeekMs, usingLivePlaybackSnapshot]);

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
    if (
      experienceMode === "local"
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
    quickRecentAutoAttemptRef.current = null;
  }, [session?.spotify_user_id]);

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
    if (!showRateLimitReload) {
      setRateLimitMenuOpen(false);
    }
  }, [showRateLimitReload]);

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
      if (!rateLimitMenuRef.current?.contains(event.target as Node)) {
        setRateLimitMenuOpen(false);
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
        setRateLimitMenuOpen(false);
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
    setRepresentativeTrack(null);
    setRepresentativeLoading(false);
    setRepresentativeReason(null);

    if (experienceMode === "local" || !hasPremiumPlayback || !selectedPreview?.entityId || spotifyCooldownActive) {
      return () => {
        cancelled = true;
      };
    }

    if (selectedPreview.kind !== "artist" && selectedPreview.kind !== "album") {
      return () => {
        cancelled = true;
      };
    }

    const activePreview = selectedPreview;

    async function loadRepresentativeTrack() {
      setRepresentativeLoading(true);
      try {
        const response = await fetch(
          `${apiBaseUrl}/preview/representative?kind=${encodeURIComponent(activePreview.kind)}&spotify_id=${encodeURIComponent(activePreview.entityId ?? "")}`,
          { credentials: "include" },
        );
        if (!response.ok) {
          throw new Error("Failed to load representative track.");
        }
        const data = (await response.json()) as RepresentativePreviewResponse;
        if (!cancelled) {
          setRepresentativeTrack(data.track);
          setRepresentativeReason(data.reason ?? null);
        }
      } catch {
        if (!cancelled) {
          setRepresentativeTrack(null);
          setRepresentativeReason("request_failed");
        }
      } finally {
        if (!cancelled) {
          setRepresentativeLoading(false);
        }
      }
    }

    void loadRepresentativeTrack();
    return () => {
      cancelled = true;
    };
  }, [experienceMode, hasPremiumPlayback, selectedPreview, spotifyCooldownActive]);

  useEffect(() => {
    let cancelled = false;

    if (
      experienceMode === "local"
      || !selectedPreview
      || (selectedPreview.kind !== "track" && selectedPreview.kind !== "album")
      || spotifyCooldownActive
    ) {
      loadedAlbumTracksAlbumIdRef.current = null;
      setAlbumTrackEntries([]);
      setAlbumTrackEntriesLoading(false);
      setAlbumTrackEntriesError(null);
      return () => {
        cancelled = true;
      };
    }

    const selectedTrackId = selectedPreview.kind === "track"
      ? (selectedPreview.trackId ?? selectedPreview.entityId)
      : null;
    const albumId = selectedPreview.albumId ?? selectedPreview.sourceTrack?.album_id ?? selectedPreview.entityId ?? null;
    if (!albumId) {
      loadedAlbumTracksAlbumIdRef.current = null;
      setAlbumTrackEntries([]);
      setAlbumTrackEntriesLoading(false);
      setAlbumTrackEntriesError("Album track list is unavailable for this item.");
      return () => {
        cancelled = true;
      };
    }
    const albumIdSafe = albumId;
    const albumAlreadyLoaded = loadedAlbumTracksAlbumIdRef.current === albumIdSafe;
    if (albumAlreadyLoaded) {
      setAlbumTrackEntries((current) => current.map((row) => ({
        ...row,
        isSelected: Boolean(selectedTrackId && row.id && selectedTrackId === row.id),
      })));
      setAlbumTrackEntriesLoading(false);
      setAlbumTrackEntriesError(null);
      return () => {
        cancelled = true;
      };
    }

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

    async function loadAlbumTrackEntries() {
      setAlbumTrackEntriesLoading(true);
      try {
        const token = await fetchPlaybackToken();
        const response = await fetch(
          `https://api.spotify.com/v1/albums/${encodeURIComponent(albumIdSafe)}/tracks?limit=50`,
          {
            headers: {
              Authorization: `Bearer ${token}`,
            },
          },
        );

        if (!response.ok) {
          throw new Error(`Failed to load album tracks (${response.status}).`);
        }

        const payload = (await response.json()) as {
          items?: Array<{
            id?: string | null;
            name?: string | null;
            uri?: string | null;
            duration_ms?: number | null;
            artists?: Array<{ name?: string | null }>;
          }>;
        };
        const rows = (payload.items ?? []).map((item) => {
          const id = item.id ?? null;
          const artistNames = (item.artists ?? []).map((artist) => artist.name ?? "").filter(Boolean).join(", ");
          const normalizedKey = normalizedTrackArtistKey(item.name ?? null, artistNames || null);
          const isTopTrack = Boolean((id && topTrackIds.has(id)) || normalizedTopTrackKeys.has(normalizedKey));
          const sourceTrack = id ? (knownTracksById.get(id) ?? null) : null;
          const lastPlayedAt = id ? (latestPlayedAtByTrackId.get(id) ?? null) : null;
          return {
            id,
            name: item.name ?? "Unknown track",
            uri: item.uri ?? null,
            durationMs: typeof item.duration_ms === "number" && Number.isFinite(item.duration_ms) ? Math.max(0, item.duration_ms) : null,
            artistName: artistNames || null,
            sourceTrack,
            lastPlayedAt,
            isSelected: Boolean(selectedTrackId && id && selectedTrackId === id),
            isTopTrack,
          } satisfies AlbumTrackEntry;
        });

        if (!cancelled) {
          setAlbumTrackEntries(rows);
          loadedAlbumTracksAlbumIdRef.current = albumIdSafe;
          if (rows.length === 0) {
            setAlbumTrackEntriesError("No tracks were returned for this album.");
          }
        }
      } catch (error) {
        if (!cancelled) {
          setAlbumTrackEntriesError(error instanceof Error ? error.message : "Album track list could not be loaded.");
        }
      } finally {
        if (!cancelled) {
          setAlbumTrackEntriesLoading(false);
        }
      }
    }

    void loadAlbumTrackEntries();
    return () => {
      cancelled = true;
    };
  }, [experienceMode, profile?.recent_likes_tracks, profile?.recent_top_tracks, profile?.recent_tracks, profile?.top_tracks, selectedPreview, spotifyCooldownActive]);

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
      await fetch(`${apiBaseUrl}/auth/player-listen-event`, {
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
    } catch {
      // Player progress persistence is best-effort and should not interrupt playback.
    }
  }

  function spotifyEntityUrl(kind: "track" | "artist" | "album", id: string | null | undefined) {
    return id ? `https://open.spotify.com/${kind}/${id}` : "";
  }

  function openPlayerTrackDetails() {
    if (!playerDisplayTrack || !usingLivePlaybackSnapshot) {
      return;
    }
    const trackId = spotifyTrackIdFromUri(playerDisplayTrack.uri) ?? livePlaybackSnapshot?.item_id ?? null;
    const trackUrl = spotifyTrackUrl(playerDisplayTrack.uri) ?? (trackId ? `https://open.spotify.com/track/${trackId}` : "");
    if (!trackUrl) {
      return;
    }
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
      albumId: playerDisplayKnownTrack?.album_id ?? null,
      artistName: playerDisplayTrack.artists || null,
      sourceTrack: playerDisplayKnownTrack ?? null,
    });
  }

  function openPlayerArtistDetails() {
    if (!playerDisplayArtistName) {
      return;
    }
    const artistUrl = playerDisplayArtist?.url ?? spotifyEntityUrl("artist", playerDisplayArtistId);
    setSelectedPreview({
      image: findArtistImageUrl(playerDisplayArtistName) ?? null,
      fallbackLabel: "A",
      label: playerDisplayArtistName,
      meta: null,
      detail: null,
      kind: "artist",
      entityId: playerDisplayArtistId,
      trackUri: null,
      url: artistUrl,
      trackId: null,
      albumId: null,
      artistName: playerDisplayArtistName,
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
      sourceTrack: playerDisplayKnownTrack ?? null,
    });
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
      albumId: track.album_id ?? null,
      artistName: track.artist_name ?? null,
      sourceTrack: track,
    });
  }

  function openQueuePlayerTrackDetails(track: PlayerQueueTrack) {
    setSelectedPreview({
      image: track.image,
      fallbackLabel: "T",
      label: track.name,
      meta: track.artists,
      detail: track.album,
      kind: "track",
      entityId: track.trackId,
      trackUri: track.uri,
      url: spotifyTrackUrl(track.uri) ?? "",
      trackId: track.trackId,
      albumId: track.albumId,
      artistName: track.artists,
      sourceTrack: null,
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
    if (!currentTrack || playbackPaused) {
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
  }, [currentTrack, playbackDurationMs, playbackPaused]);

  useEffect(() => {
    playbackPositionMsRef.current = playbackPositionMs;
  }, [playbackPositionMs]);

  useEffect(() => {
    if (!currentTrack || playbackPaused || activePlayerListenEventId == null) {
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
  }, [activePlayerListenEventId, currentTrack, playbackDurationMs, playbackPaused]);

  useEffect(() => {
    setPlayerRecentTracks(dedupeRecentTracksForPlayer(profile?.recent_tracks ?? []));
    setPlayerRecentTracksError(null);
  }, [profile?.recent_tracks]);

  useEffect(() => {
    if (!playerMenuOpen || !profile || playerRecentTracksLoading) {
      return;
    }

    void loadPlayerRecentTracks();
  }, [playerMenuOpen, profile, recentRange, experienceMode]);

  useEffect(() => {
    if (!playerMenuOpen || !profile) {
      return;
    }

    if (usingRecentLikedStartupFallback) {
      setPlayerQueueTracks(recentTracksToPlayerQueueTracks(profile.recent_likes_tracks.slice(1)));
      setPlayerQueueSource("listenlab");
      setPlayerQueueError(null);
      return;
    }

    if (!liveSpotifyPlaybackShouldOwnQueue && playerQueueSource === "listenlab") {
      return;
    }

    if (playerQueueLoading) {
      return;
    }

    void loadPlayerQueueTracks();
  }, [playerMenuOpen, profile, experienceMode, usingRecentLikedStartupFallback, liveSpotifyPlaybackShouldOwnQueue, playerQueueSource]);

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
    if (!liveSpotifyPlaybackShouldOwnQueue || !livePlaybackSnapshot || !livePlaybackTrackSummary) {
      return;
    }
    setPlayerQueueSource("spotify");
    setCurrentTrack(livePlaybackTrackSummary);
    setPlaybackPaused(!Boolean(livePlaybackSnapshot.is_playing));
    setPlaybackPositionMs(Math.max(0, Number(livePlaybackSnapshot.progress_ms ?? 0)));
    setPlaybackDurationMs(Math.max(0, Number(livePlaybackSnapshot.duration_ms ?? 0)));
  }, [livePlaybackSnapshot, livePlaybackTrackSummary, liveSpotifyPlaybackShouldOwnQueue]);

  async function playTrackUri(trackUri: string | null, positionMs = 0, options?: { syncQueuePlaylist?: boolean }) {
    if (!trackUri) {
      setPlayerError("This item does not have a playable Spotify track.");
      return false;
    }
    let syncedPlaylistUri: string | null = null;
    if (options?.syncQueuePlaylist && playerQueueSource === "listenlab") {
      const queuePlaylistUris = queuePlaylistTrackUris(trackUri, playerQueueTracks);
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
    if (currentTrack?.uri && (playbackDurationMs <= 0 || currentTrack.durationMs <= 0)) {
      return playTrackUri(currentTrack.uri, Math.max(0, playbackPositionMs), {
        syncQueuePlaylist: playerQueueSource === "listenlab",
      });
    }
    const player = spotifyPlayerRef.current;
    if (player) {
      try {
        await player.resume();
        setPlaybackPaused(false);
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
      setPlayerError(null);
      return true;
    } catch (error) {
      setPlayerError(error instanceof Error ? error.message : "Spotify playback could not be resumed.");
      return false;
    }
  }

  async function togglePlayerPlayback() {
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

  function handlePlayerPrimaryButtonClick() {
    if (!liveReadOnlyMode) {
      void togglePlayerPlayback();
      return;
    }
    if (!playerDisplayPaused) {
      void takeOverAndPausePlayback();
      return;
    }
    void takeOverPlaybackFromLiveSnapshot();
  }

  async function handlePopupTrackPlayback(trackUri: string | null, options?: PopupTrackPlaybackOptions) {
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

      const playbackStarted = await playTrackUri(trackUri, 0, { syncQueuePlaylist: true });
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

  function handleSelectedPreviewTrackPlay(trackUri: string | null) {
    clearPreviewPlaybackState();
    setOverlayTrackPlaybackExpanded(true);
    void handlePopupTrackPlayback(trackUri, {
      optimisticTrack: selectedPreviewTrackOptimisticSummary,
      sourceTrack: selectedPreview?.sourceTrack ?? null,
    });
  }

  async function seekPlayer(positionMs: number) {
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

  function isTrackPlaying(trackUri: string | null) {
    return Boolean(trackUri && currentTrack?.uri === trackUri && !playbackPaused);
  }

  function openAlbumTrackPreview(track: AlbumTrackEntry) {
    if (!selectedPreview || (selectedPreview.kind !== "track" && selectedPreview.kind !== "album")) {
      return;
    }
    const previewTrackUri = trackUriWithFallback(track.uri, track.id);
    const previewTrackUrl = spotifyTrackUrl(previewTrackUri) ?? (track.id ? `https://open.spotify.com/track/${track.id}` : selectedPreview.url);
    setSelectedPreview({
      image: track.sourceTrack?.image_url ?? selectedPreview.image ?? null,
      fallbackLabel: selectedPreview.fallbackLabel,
      label: track.name,
      meta: track.artistName ?? track.sourceTrack?.artist_name ?? selectedPreview.meta ?? null,
      detail: track.sourceTrack?.album_name ?? selectedPreview.detail,
      kind: "track",
      entityId: track.id ?? null,
      trackUri: previewTrackUri,
      url: previewTrackUrl,
      trackId: track.id ?? null,
      albumId: selectedPreview.albumId ?? selectedPreview.sourceTrack?.album_id ?? selectedPreview.entityId ?? null,
      artistName: track.artistName ?? track.sourceTrack?.artist_name ?? selectedPreview.artistName ?? null,
      sourceTrack: track.sourceTrack ?? selectedPreview.sourceTrack ?? null,
    });
  }

  function openSelectedTrackArtistPreview() {
    if (!selectedPreview || selectedPreview.kind !== "track" || !selectedPreviewPrimaryArtistName) {
      return;
    }
    const sourceTrack = selectedPreview.sourceTrack ?? null;
    const artist = firstArtistFromRecentTrack(sourceTrack);
    const artistId = artist?.artist_id ?? artist?.id ?? null;
    setSelectedPreview({
      image: findArtistImageUrl(selectedPreviewPrimaryArtistName) ?? selectedPreviewArtistImageUrl ?? selectedPreview.image ?? null,
      fallbackLabel: "A",
      label: selectedPreviewPrimaryArtistName,
      meta: null,
      detail: null,
      kind: "artist",
      entityId: artistId,
      trackUri: null,
      url: artist?.url ?? spotifyEntityUrl("artist", artistId),
      trackId: null,
      albumId: null,
      artistName: selectedPreviewPrimaryArtistName,
      sourceTrack,
    });
  }

  function openSelectedTrackAlbumPreview() {
    if (!selectedPreview || selectedPreview.kind !== "track") {
      return;
    }
    const sourceTrack = selectedPreview.sourceTrack ?? null;
    const albumId = selectedPreview.albumId ?? sourceTrack?.album_id ?? null;
    const albumName = sourceTrack?.album_name ?? selectedPreview.detail ?? "Unknown album";
    const albumYear = sourceTrack?.album_release_year ?? null;
    setSelectedPreview({
      image: selectedPreview.image ?? sourceTrack?.image_url ?? null,
      fallbackLabel: "L",
      label: albumName,
      meta: sourceTrack?.artist_name ?? selectedPreview.artistName ?? selectedPreview.meta ?? null,
      detail: albumYear,
      kind: "album",
      entityId: albumId,
      trackUri: null,
      url: sourceTrack?.album_url ?? spotifyEntityUrl("album", albumId),
      trackId: null,
      albumId,
      artistName: sourceTrack?.artist_name ?? selectedPreview.artistName ?? null,
      sourceTrack,
    });
  }

  function playerSummaryFromAlbumTrack(track: AlbumTrackEntry): PlayerTrackSummary {
    const previewTrackUri = trackUriWithFallback(track.uri, track.id);
    return {
      name: track.name,
      artists: track.artistName ?? track.sourceTrack?.artist_name ?? selectedPreview?.artistName ?? "Unknown artist",
      album: track.sourceTrack?.album_name ?? selectedPreview?.sourceTrack?.album_name ?? selectedPreview?.detail ?? "Unknown album",
      image: track.sourceTrack?.image_url ?? selectedPreview?.image ?? null,
      uri: previewTrackUri,
      durationMs: Math.max(0, track.durationMs ?? track.sourceTrack?.duration_ms ?? 0),
    };
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

  function clearPreviewPlaybackState() {
    if (previewStopTimerRef.current != null) {
      window.clearTimeout(previewStopTimerRef.current);
      previewStopTimerRef.current = null;
    }
    setPreviewingTrackUriState(null);
    restoreDefaultPlayerVolume();
  }

  async function stopTrackPreviewPlayback() {
    const hasActivePreview = Boolean(previewingTrackUriRef.current);
    clearPreviewPlaybackState();
    if (!hasActivePreview) {
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
    setCurrentTrack({
      ...playerSummaryFromAlbumTrack(track),
      uri: rowTrackUri,
      durationMs,
    });
    setPlaybackPaused(false);
    setPlaybackPositionMs(randomStartMs);
    setPlaybackDurationMs(durationMs);
    setPreviewingTrackUriState(rowTrackUri);
    startPreviewVolumeRamp();
    previewStopTimerRef.current = window.setTimeout(() => {
      void stopTrackPreviewPlayback();
    }, 20_000);
  }

  async function handleAlbumTrackPlay(track: AlbumTrackEntry, trackUri: string | null) {
    if (!trackUri) {
      return;
    }
    clearPreviewPlaybackState();
    setOverlayTrackPlaybackExpanded(true);
    const playbackStarted = await handlePopupTrackPlayback(trackUri, {
      optimisticTrack: playerSummaryFromAlbumTrack(track),
      sourceTrack: track.sourceTrack,
    });
    if (playbackStarted) {
      openAlbumTrackPreview(track);
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
    setRateLimitMenuOpen(false);
    setPlayerQueueTracks([]);
    setPlayerQueueSource(null);
    setQueuePlaylistUri(null);
    setReloadCooldownUntil(null);
    setReloadCooldownDurationMs(60_000);
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
        setPlayerQueueSource(null);
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

  async function triggerRecentIngestPollForTrackEnd() {
    try {
      await fetch(`${apiBaseUrl}/auth/recent-ingest/poll-now`, {
        method: "POST",
        credentials: "include",
      });
    } catch {
      // Keep track-end UX resilient even if this background ingest trigger fails.
    }
  }

  function startLogin() {
    window.location.href = `${apiBaseUrl}/auth/login`;
  }

  function startRecentIngestLogin() {
    window.location.href = `${apiBaseUrl}/auth/login?mode=recent_ingest`;
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
        setRecentIngestResult(null);
        setStatusMessage("Spotify auth succeeded, but no ingest result was returned.");
        return;
      }
      setRecentIngestResult(data);
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

  async function runRecentBeforeProbe() {
    try {
      const response = await fetch(`${apiBaseUrl}/auth/recent-ingest/probe-before?days=90&limit=50`, {
        credentials: "include",
      });
      const data = (await response.json()) as RecentBeforeProbeResponse;
      if (!response.ok) {
        throw new Error(data.detail || `Probe failed (${response.status})`);
      }
      setRecentBeforeProbeResult(data);
      setStatusMessage(
        `Before-90d probe: ${data.returned_items ?? 0} rows (${data.earliest_played_at ?? "n/a"} to ${data.latest_played_at ?? "n/a"}).`,
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : "Before-90d probe failed.";
      setRecentBeforeProbeResult({ ok: false, detail: message });
      setStatusMessage(`Before-90d probe failed: ${message}`);
    }
  }

  async function runRecentBackfillProbe() {
    try {
      const response = await fetch(`${apiBaseUrl}/auth/recent-ingest/probe-backfill?limit=50&max_pages=10`, {
        credentials: "include",
      });
      const data = (await response.json()) as RecentBackfillProbeResponse;
      if (!response.ok) {
        throw new Error(data.detail || `Backfill probe failed (${response.status})`);
      }
      setRecentBackfillProbeResult(data);
      setStatusMessage(
        `Backfill probe: ${data.total_items ?? 0} items across ${data.pages_fetched ?? 0} pages (${data.earliest_played_at ?? "n/a"} to ${data.latest_played_at ?? "n/a"}).`,
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : "Backfill probe failed.";
      setRecentBackfillProbeResult({ ok: false, detail: message });
      setStatusMessage(`Backfill probe failed: ${message}`);
    }
  }

  async function reconnectSpotify() {
    const response = await fetch(`${apiBaseUrl}/cache/rebuild`, {
      method: "POST",
      credentials: "include",
    });
    if (!response.ok) {
      let detail = "Failed to refresh cache before reconnecting Spotify.";
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
    startLogin();
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
      void reconnectSpotify();
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

    if (
      section === "playlists"
      && !isCurrentlyOpen
      && experienceMode === "full"
      && profile
      && !profile.extended_loaded
      && !loadingExtendedProfile
    ) {
      void loadExtendedProfile(recentRange, analysisMode);
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
    const selectTrackRankingMode = (nextMode: TrackRankingMode) => {
      if (nextMode === trackRankingMode) {
        return;
      }
      setTrackRankingRefreshPending(true);
      setSectionPages((current) => ({
        ...current,
        tracksAllTime: 0,
      }));
      setTrackRankingMode(nextMode);
    };

    return (
      <div className="track-ranking-toggle" role="group" aria-label="Top track ranking mode">
        {showTrackRankingSpinner ? (
          <span className="recent-range-vinyl-spinner" aria-hidden="true">
            <span className="recent-range-vinyl-center" />
          </span>
        ) : null}
        <button
          className={`track-ranking-chip${trackRankingMode === "plays" ? " track-ranking-chip-active" : ""}`}
          onClick={() => selectTrackRankingMode("plays")}
          type="button"
        >
          Plays
        </button>
        <button
          className={`track-ranking-chip${trackRankingMode === "mix" ? " track-ranking-chip-active" : ""}`}
          onClick={() => selectTrackRankingMode("mix")}
          type="button"
        >
          Mix
        </button>
        <button
          className={`track-ranking-chip${trackRankingMode === "longevity" ? " track-ranking-chip-active" : ""}`}
          onClick={() => selectTrackRankingMode("longevity")}
          type="button"
        >
          Longevity
        </button>
      </div>
    );
  }

  function renderMergedTrackSourceFilterToggle() {
    return (
      <div className="track-ranking-toggle" role="group" aria-label="Merged track source filter">
        {MERGED_TRACK_SOURCE_FILTER_OPTIONS.map((option) => (
          <button
            className={`track-ranking-chip${mergedTrackSourceFilter === option.value ? " track-ranking-chip-active" : ""}`}
            key={option.value}
            onClick={() => setMergedTrackSourceFilter(option.value)}
            type="button"
          >
            {option.label}
          </button>
        ))}
      </div>
    );
  }

  function renderRankMovementFilterToggle() {
    return (
      <div className="track-ranking-toggle" role="group" aria-label="Formula rank movement filter">
        {RANK_MOVEMENT_FILTER_OPTIONS.map((option) => (
          <button
            className={`track-ranking-chip${rankMovementFilter === option.value ? " track-ranking-chip-active" : ""}`}
            key={option.value}
            onClick={() => setRankMovementFilter(option.value)}
            type="button"
          >
            {option.label}
          </button>
        ))}
      </div>
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

  function renderPreviewCard(item: PreviewItem, key: string) {
    return (
      <button
        className="preview-card"
        key={key}
        onClick={() => setSelectedPreview(item)}
        type="button"
      >
        {item.image ? (
          <img alt={item.label} className="preview-thumb" src={item.image} />
        ) : (
          <div className="preview-thumb preview-thumb-fallback" aria-hidden="true">
            {item.fallbackLabel ?? item.label.slice(0, 1).toUpperCase()}
          </div>
        )}
        <span className="preview-overlay">
          <span className="preview-label">{item.label}</span>
          {item.meta ? <span className="preview-meta">{item.meta}</span> : null}
          {item.detail ? <span className="preview-detail">{item.detail}</span> : null}
        </span>
      </button>
    );
  }

  function recentUnavailableCopy(defaultCopy: string) {
    if (experienceMode === "local") {
      return "Recent sections in restricted local mode come from local history data only.";
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
    if (experienceMode === "full" && spotifyCooldownActive) {
      setStatusMessage(formatCooldownCopy(reloadSecondsRemaining));
      return;
    }
    if (profileLoadInFlightRef.current) {
      return;
    }
    profileLoadInFlightRef.current = true;
    setLoadingProfile(true);
    setProfileLoadAttempted(true);
    setStatusMessage(experienceMode === "local" ? "Loading local history..." : "Loading your Spotify data...");
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
      const endpoint = experienceMode === "local" ? "/me/local" : "/me";
      const response = await fetch(
        `${apiBaseUrl}${endpoint}?recent_range=${encodeURIComponent(recentRange)}&analysis_mode=${encodeURIComponent(analysisMode)}`,
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
          detail = formatCooldownCopy(cooldownSeconds);
        }
        throw new Error(detail);
      }

      const data = (await response.json()) as ProfileResponse;
      let hydratedProfile = data;
      if (experienceMode === "local") {
        hydratedProfile = {
          ...hydratedProfile,
          username: session?.spotify_user_id ?? hydratedProfile.username,
          display_name: session?.display_name ?? hydratedProfile.display_name,
          email: session?.email ?? hydratedProfile.email,
        };
      }
      if ((data.analysis_mode ?? analysisMode) === "quick" && experienceMode !== "local") {
        setStatusMessage("Loading recent activity...");
        try {
          const recentData = await fetchRecentSections(data.recent_range ?? recentRange);
          hydratedProfile = {
            ...hydratedProfile,
            recent_range: recentData.recent_range,
            recent_window_days: recentData.recent_window_days,
            recent_top_artists: recentData.recent_top_artists,
            recent_top_artists_available: recentData.recent_top_artists_available,
            recent_top_tracks: recentData.recent_top_tracks,
            recent_top_tracks_available: recentData.recent_top_tracks_available,
            recent_top_albums: recentData.recent_top_albums,
            recent_top_albums_available: recentData.recent_top_albums_available,
            recent_tracks: recentData.recent_tracks,
            recent_tracks_available: recentData.recent_tracks_available,
            recent_likes_tracks: recentData.recent_likes_tracks,
            recent_likes_available: recentData.recent_likes_available,
          };
        } catch (recentError) {
          const message = recentError instanceof Error ? recentError.message : "Recent activity could not be preloaded.";
          setStatusHistory((current) => [...current, `Recent preload warning: ${message}`]);
        }
      }

      setProfile(hydratedProfile);
      setAnalysisMode(hydratedProfile.analysis_mode ?? analysisMode);
      setAuthTransitioning(false);
      setSectionPages(INITIAL_SECTION_PAGES);
      setStatusMessage("");
      setStatusHistory((current) =>
        current.length > 0 ? [...current, "Initial load complete."] : ["Initial load started.", "Initial load complete."],
      );
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
    setPlayerQueueSource(null);
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
    setMergedTrackSourceFilter("all");
    setIdentityAudit(null);
    setIdentityAuditLoading(false);
    setIdentityAuditLoaded(false);
    setIdentityAuditError("");
    setIdentityAuditLastLoadedAt(null);
  }

  function renderPaging(section: SectionKey, itemCount: number) {
    return renderPagingWithPageSize(section, itemCount, PAGE_SIZE);
  }

  function renderPagingWithPageSize(section: SectionKey, itemCount: number, pageSize: number) {
    return (
      <DashboardPaging
        section={section}
        itemCount={itemCount}
        pageSize={pageSize}
        sectionPage={sectionPages[section]}
        moveSectionPage={moveSectionPage}
      />
    );
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

  function renderArtistColumn(
    section: SectionKey,
    items: FollowedArtist[],
    available: boolean,
    emptyCopy: string,
    unavailableCopy: string,
    unavailableAction?: ReactNode,
  ) {
    if (!available) {
      return (
        <div className="section-unavailable">
          <p className="empty-copy">{unavailableCopy}</p>
          {unavailableAction ? <div className="section-unavailable-action">{unavailableAction}</div> : null}
        </div>
      );
    }
    if (items.length === 0) {
      return <p className="empty-copy">{emptyCopy}</p>;
    }

    const pageItems = visibleItems(section, items);
    return (
      <>
        <div className="item-list">
          {pageItems.map((artist, index) => (
            renderDashboardListCard(
              {
                href: artist.url,
                entityId: artist.artist_id,
                imageUrl: artist.image_url,
                imageAlt: `${artist.name ?? "Artist"} portrait`,
                fallbackLabel: "A",
                primaryText: artist.name ?? "Unknown artist",
                secondaryText:
                  artist.genres.length > 0
                    ? artist.genres.join(", ")
                    : artist.popularity != null
                      ? `Popularity ${artist.popularity}/100`
                      : "Spotify artist",
                tertiaryText: formatHistoryDebugLine(artist),
              },
              artist.artist_id ?? `${artist.name}-${index}`,
            )
          ))}
          {Array.from({ length: emptySlots(pageItems) }).map((_, index) => (
            <div className="list-row list-row-placeholder" key={`${section}-empty-${index}`} aria-hidden="true" />
          ))}
        </div>
        {renderPaging(section, items.length)}
      </>
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
    if (!available) {
      return (
        <div className="section-unavailable">
          <p className="empty-copy">{unavailableCopy}</p>
          {unavailableAction ? <div className="section-unavailable-action">{unavailableAction}</div> : null}
        </div>
      );
    }
    if (items.length === 0) {
      return <p className="empty-copy">{emptyCopy}</p>;
    }

    const rankedItems = presorted ? items : sortedTracksForView(section, items, trackRankingMode);
    const isAllTimeTrackSection =
      section === "tracksAllTime" ||
      section === "tracksAllTimeCurrent" ||
      section === "tracksAllTimeNew";
    const showSourceBadge = section !== "tracksAllTimeCurrent" && section !== "tracksAllTimeNew";
    const formulaRankDeltaText = (track: RecentTrack) =>
      section === "tracksAllTimeCurrent" || section === "tracksAllTimeNew"
        ? formatFormulaRankDelta(track)
        : null;

    const cappedRows = isAllTimeTrackSection || section === "tracksRecent"
      ? capTracksPerAlbum(rankedItems, 1)
      : rankedItems.map((track) => ({ track, hiddenCount: 0 }));
    const pageRows = paged ? visibleItems(section, cappedRows) : cappedRows;
    return (
      <>
        <div className="item-list">
          {pageRows.map((row, index) =>
            renderDashboardListCard(
              {
                href: row.track.url,
                entityId: row.track.track_id,
                imageUrl: row.track.image_url,
                imageAlt: `${row.track.album_name ?? row.track.track_name ?? "Album"} cover`,
                fallbackLabel: "T",
                primaryText: row.track.track_name ?? "Unknown track",
                primaryBadgeText: formulaRankDeltaText(row.track) ?? (showSourceBadge ? formatTrackSourceBadge(row.track) : null),
                secondaryBadgeText: row.hiddenCount > 0 ? `+${row.hiddenCount} more` : null,
                secondaryText: row.track.artist_name ?? "Unknown artist",
                tertiaryText: row.track.album_name ?? "Unknown album",
                metricText: isAllTimeTrackSection
                  ? (
                      section === "tracksAllTimeNew"
                        ? `${row.track.play_count ?? 0} | ${formatTrackLongevity(row.track) ?? "0d"}`
                        : formatTrackRankingMetric(row.track, trackRankingMode)
                    )
                  : null,
                trackUri: row.track.uri ?? null,
                previewTrack: row.track,
              },
              row.track.track_id ?? `${row.track.track_name}-${index}-${section}`,
            ),
          )}
          {Array.from({ length: emptySlots(pageRows) }).map((_, index) => (
            <div className="list-row list-row-placeholder" key={`${section}-empty-${index}`} aria-hidden="true" />
          ))}
        </div>
        {paged ? renderPaging(section, cappedRows.length) : null}
      </>
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

  function renderAlbumMemberRows(
    rows: Array<{
      release_album_id: number;
      release_album_name: string;
      artist_name: string;
      spotify_album_id?: string | null;
      spotify_album_name?: string | null;
    }>,
  ) {
    return (
      <div style={{ display: "grid", gap: "8px" }}>
        {rows.map((row) => (
          <div
            key={`album-member-${row.release_album_id}`}
            style={{
              alignItems: "center",
              background: "rgba(255, 255, 255, 0.03)",
              borderRadius: "12px",
              display: "grid",
              gap: "4px",
              gridTemplateColumns: "minmax(0, 1.3fr) minmax(0, 1fr)",
              padding: "10px 12px",
            }}
          >
            <div style={{ minWidth: 0 }}>
              <div style={{ fontWeight: 700, overflowWrap: "anywhere" }}>{row.release_album_name}</div>
              <div className="empty-copy" style={{ margin: 0 }}>{row.artist_name}</div>
            </div>
            <div style={{ justifySelf: "end", textAlign: "right" }}>
              <div style={{ fontFamily: "monospace", fontSize: "12px" }}>release_album {row.release_album_id}</div>
              {row.spotify_album_id ? (
                <a
                  className="empty-copy"
                  href={spotifyAlbumUrl(row.spotify_album_id)}
                  rel="noreferrer"
                  style={{ display: "block", margin: 0, overflowWrap: "anywhere" }}
                  target="_blank"
                >
                  {row.spotify_album_id}
                  {row.spotify_album_name ? ` (${row.spotify_album_name})` : ""}
                </a>
              ) : null}
            </div>
          </div>
        ))}
      </div>
    );
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
      <article
        key={`album-dup-card-${target.key}`}
        style={{
          background: "rgba(255, 255, 255, 0.03)",
          border: "1px solid rgba(255, 255, 255, 0.08)",
          borderRadius: "18px",
          display: "grid",
          gap: "14px",
          padding: "18px",
        }}
      >
        <div style={{ alignItems: "start", display: "flex", gap: "16px", justifyContent: "space-between" }}>
          <div style={{ minWidth: 0 }}>
            {target.spotifyAlbumId ? (
              <h3 style={{ margin: 0 }}>
                <a href={spotifyAlbumUrl(target.spotifyAlbumId)} rel="noreferrer" target="_blank">{target.title}</a>
              </h3>
            ) : (
              <h3 style={{ margin: 0 }}>{target.title}</h3>
            )}
            <p className="empty-copy" style={{ margin: "6px 0 0 0" }}>{target.subtitle}</p>
          </div>
          <div style={{ display: "grid", gap: "8px", justifyItems: "end" }}>
            <span className="identity-audit-stat">
              <span>Duplicate count</span>
              <strong>{target.duplicateCount}</strong>
            </span>
            {preview ? renderAlbumMergeReadinessBadge(preview.merge_readiness) : null}
          </div>
        </div>
        {extraMeta}
        <div style={{ display: "flex", flexWrap: "wrap", gap: "8px" }}>
          <span className="identity-audit-stat">
            <span>Release album IDs</span>
            <strong>{target.releaseAlbumIds.join(", ")}</strong>
          </span>
          <span className="identity-audit-stat">
            <span>Warnings</span>
            <strong>{preview?.warnings.length ?? 0}</strong>
          </span>
          <span className="identity-audit-stat">
            <span>Review source</span>
            <strong>{target.sourceLabel}</strong>
          </span>
          <span className="identity-audit-stat">
            <span>Reason</span>
            <strong>{albumMergeReasonLabel(albumMergeReasonKey(preview))}</strong>
          </span>
        </div>
        {preview ? <p className="identity-audit-tab-copy" style={{ margin: 0 }}>{plainEnglishAlbumMergeExplanation(preview)}</p> : null}
        {warningSummary ? <p className="empty-copy" style={{ margin: 0 }}>Warning summary: {warningSummary}</p> : null}
        {previewError ? <p className="empty-copy">{previewError}</p> : null}
        {dryRunError ? <p className="empty-copy">{dryRunError}</p> : null}
        {renderAlbumMemberRows(rows)}
        <div style={{ display: "flex", flexWrap: "wrap", gap: "10px" }}>
          <button
            className="track-ranking-chip"
            disabled={releaseAlbumMergePreviewLoadingKey !== null}
            onClick={() => {
              setSelectedAlbumMergeReviewKey(target.key);
              setAlbumIdentityAuditTab("merge_review");
              void loadReleaseAlbumMergePreview(target.key, target.releaseAlbumIds);
            }}
            type="button"
          >
            {releaseAlbumMergePreviewLoadingKey === target.key ? "Loading..." : "Preview merge"}
          </button>
          {preview?.survivor_release_album_id != null ? (() => {
            const survivorReleaseAlbumId = preview.survivor_release_album_id;
            return (
              <button
                className="secondary-button"
                disabled={releaseAlbumMergeDryRunLoadingKey !== null}
                onClick={() => {
                  setSelectedAlbumMergeReviewKey(target.key);
                  setAlbumIdentityAuditTab("merge_review");
                  void loadReleaseAlbumMergeDryRun(target.key, target.releaseAlbumIds, survivorReleaseAlbumId);
                }}
                type="button"
              >
                {releaseAlbumMergeDryRunLoadingKey === target.key ? "Loading..." : "Dry run"}
              </button>
            );
          })() : null}
        </div>
        {preview || dryRun ? (
          <details>
            <summary>Details</summary>
            {preview ? renderReleaseAlbumMergePreview(target.key) : null}
          </details>
        ) : null}
      </article>
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
      <div className="identity-audit-overview-grid">
        <article className="identity-audit-overview-card">
          <h3>Suspicious Splits</h3>
          <p>Same normalized title/artist with multiple Spotify IDs.</p>
          <strong>{canonicalCount}</strong>
        </article>
        <article className="identity-audit-overview-card">
          <h3>Ambiguous Mappings</h3>
          <p>Multiple source tracks folded under a single release track.</p>
          <strong>{releaseCount}</strong>
        </article>
        <article className="identity-audit-overview-card">
          <h3>Grouping Concerns</h3>
          <p>Release tracks grouped together for analysis.</p>
          <strong>{compositionCount}</strong>
        </article>
        <article className="identity-audit-overview-card">
          <h3>Suggested Matches</h3>
          <p>Conservative title/artist matches awaiting review.</p>
          <strong>{suggestedCount}</strong>
        </article>
        <article className="identity-audit-overview-card">
          <h3>Needs Review</h3>
          <p>Items requiring human judgment across variant-rule families.</p>
          <strong>{ambiguousCount}</strong>
        </article>
      </div>
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
      <div className="identity-audit-overview-grid">
        <article className="identity-audit-overview-card">
          <h3>Duplicate Albums</h3>
          <p>Strongest album duplicate signal using one resolved Spotify album.</p>
          <strong>{albumDuplicateLookupResult?.total ?? 0}</strong>
        </article>
        <article className="identity-audit-overview-card">
          <h3>Duplicate Name + Artist</h3>
          <p>Weaker text-based album duplicate signal when Spotify ID is missing or mixed.</p>
          <strong>{albumNameDuplicateLookupResult?.total ?? 0}</strong>
        </article>
        <article className="identity-audit-overview-card">
          <h3>Merge Previews</h3>
          <p>Album groups already previewed in this session.</p>
          <strong>{previewedCount}</strong>
        </article>
        <article className="identity-audit-overview-card">
          <h3>Dry Runs</h3>
          <p>Groups with a row-level dry-run plan available.</p>
          <strong>{dryRunCount}</strong>
        </article>
      </div>
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
    const selectedTarget = targets.find((target) => target.key === selectedAlbumMergeReviewKey) ?? null;
    const reviewedTargets = targets.filter((target) => releaseAlbumMergePreviewByKey[target.key] || releaseAlbumMergeDryRunByKey[target.key]);
    return (
      <div className="identity-audit-grid">
        <p className="identity-audit-tab-copy">
          Merge Review keeps the selected album group front and center and preserves full preview and dry-run details.
        </p>
        {!selectedTarget && reviewedTargets.length === 0 ? (
          <p className="empty-copy">Choose Preview merge from an album duplicate group to start a review.</p>
        ) : null}
        {selectedTarget ? (
          <div className="identity-audit-group">
            <div className="tracks-formula-heading">
              <h3>Selected Group</h3>
              <span>{selectedTarget.sourceLabel}</span>
            </div>
            {renderReleaseAlbumMergePreview(selectedTarget.key)}
          </div>
        ) : null}
        {reviewedTargets.length > 0 ? (
          <div className="identity-audit-group">
            <div className="tracks-formula-heading">
              <h3>Reviewed Groups</h3>
              <span>{reviewedTargets.length}</span>
            </div>
            <div style={{ display: "grid", gap: "12px" }}>
              {reviewedTargets.map((target) => {
                const preview = releaseAlbumMergePreviewByKey[target.key];
                const dryRun = releaseAlbumMergeDryRunByKey[target.key];
                return (
                  <button
                    key={`album-reviewed-target-${target.key}`}
                    className="secondary-button"
                    onClick={() => setSelectedAlbumMergeReviewKey(target.key)}
                    style={{ alignItems: "center", display: "flex", justifyContent: "space-between", textAlign: "left" }}
                    type="button"
                  >
                    <span>
                      <strong>{target.title}</strong>
                      <span className="empty-copy" style={{ display: "block", marginTop: "4px" }}>{target.releaseAlbumIds.join(", ")}</span>
                    </span>
                    <span style={{ alignItems: "center", display: "flex", gap: "8px" }}>
                      {preview ? renderAlbumMergeReadinessBadge(preview.merge_readiness) : null}
                      {dryRun ? <span className="empty-copy">Dry run ready</span> : null}
                    </span>
                  </button>
                );
              })}
            </div>
          </div>
        ) : null}
      </div>
    );
  }

  function renderArtistIdentityAuditPlaceholder() {
    return (
      <div className="identity-audit-grid">
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Artists</h3>
            <span>Placeholder</span>
          </div>
          <p className="identity-audit-tab-copy">
            Artist audit review is not wired yet. This space is reserved so artist joins can land without mixing them into track or album review.
          </p>
          <p className="empty-copy">No artist-specific audit diagnostics are available in this build.</p>
        </div>
      </div>
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
    const familyOptions = identityAuditAmbiguous?.family_counts ?? [];
    const suggestedItems = identityAuditSuggestedGroups?.items ?? [];
    const filteredItems = computeAmbiguousTrackItems();
    const unifiedItems = computeUnifiedReviewItems();
    const visibleItems = filteredItems.slice(0, identityAuditAmbiguousVisibleCount);
    const focusedItem = identityAuditFocusedReviewKey == null
      ? null
      : (unifiedItems.find((item) => item.decision_key === identityAuditFocusedReviewKey) ?? null);
    const focusedDecision = focusedItem ? identityAuditLocalDecisions[focusedItem.decision_key] : undefined;
    const reviewedAmbiguousCount = filteredItems.reduce((count, item) => (
      isReviewedDecision(identityAuditLocalDecisions[trackDecisionKey(item)]) ? count + 1 : count
    ), 0);
    const reviewedSuggestedCount = suggestedItems.reduce((count, group) => {
      const decision = identityAuditLocalDecisions[groupDecisionKey(group)];
      return isReviewedDecision(decision) ? count + 1 : count;
    }, 0);
    const reviewedCount = reviewedAmbiguousCount + reviewedSuggestedCount;
    const totalReviewableCount = filteredItems.length + suggestedItems.length;
    const summaryByFamily = new Map<string, { total: number; approved: number; rejected: number; skipped: number; unreviewed: number }>();
    for (const item of unifiedItems) {
      const current = summaryByFamily.get(item.family_label) ?? { total: 0, approved: 0, rejected: 0, skipped: 0, unreviewed: 0 };
      current.total += 1;
      const verdict = identityAuditLocalDecisions[item.decision_key]?.verdict ?? "unsure";
      if (verdict === "good_to_group") {
        current.approved += 1;
      } else if (verdict === "not_good") {
        current.rejected += 1;
      } else if (verdict === "skipped") {
        current.skipped += 1;
      } else {
        current.unreviewed += 1;
      }
      summaryByFamily.set(item.family_label, current);
    }
    const summaryEntries = Array.from(summaryByFamily.entries())
      .sort((left, right) => right[1].total - left[1].total || left[0].localeCompare(right[0]));
    const visibleSummaryEntries = summaryEntries.slice(0, 8);
    const remainingSummaryCount = Math.max(0, summaryEntries.length - visibleSummaryEntries.length);

    const groupApproved: Array<Record<string, unknown>> = [];
    const groupRejected: Array<Record<string, unknown>> = [];
    const groupSkipped: Array<Record<string, unknown>> = [];
    const trackApproved: Array<Record<string, unknown>> = [];
    const trackRejected: Array<Record<string, unknown>> = [];
    const trackSkipped: Array<Record<string, unknown>> = [];

    for (const item of unifiedItems) {
      const decision = identityAuditLocalDecisions[item.decision_key];
      if (!decision || decision.verdict === "unsure") {
        continue;
      }
      if (item.item_type === "group") {
        const group = item.group;
        const label = group?.analysis_track_name || (group?.analysis_track_id != null ? `track_family ${group.analysis_track_id}` : item.decision_key);
        const entry = {
          decision_key: item.decision_key,
          id: group?.analysis_track_id ?? item.decision_key,
          decision: decision.verdict,
          label,
          family: group?.song_family_key ?? item.family_label,
          bucket: item.bucket_label,
          would: decision.verdict === "good_to_group"
            ? `Would group as composition family: ${label}`
            : decision.verdict === "not_good"
              ? `Would keep suggested group separate: ${label}`
              : `Would defer suggested group: ${label}`,
          source: group
            ? {
                analysis_track_id: group.analysis_track_id,
                analysis_track_name: group.analysis_track_name,
                song_family_key: group.song_family_key,
                release_track_count: group.release_track_count,
                confidence: group.confidence,
                match_method: group.match_method,
              }
            : null,
        };
        if (decision.verdict === "good_to_group") {
          groupApproved.push(entry);
        } else if (decision.verdict === "not_good") {
          groupRejected.push(entry);
        } else {
          groupSkipped.push(entry);
        }
      } else {
        const track = item.track;
        const label = track?.release_track_name || (track?.release_track_id != null ? `release_track ${track.release_track_id}` : item.decision_key);
        const entry = {
          decision_key: item.decision_key,
          id: track?.release_track_id ?? item.decision_key,
          decision: decision.verdict,
          label,
          family: track?.dominant_family ?? item.family_label,
          bucket: track?.bucket ?? item.bucket_label,
          would: decision.verdict === "good_to_group"
            ? `Would accept track identity mapping: ${label}`
            : decision.verdict === "not_good"
              ? `Would reject track identity mapping: ${label}`
              : `Would defer track decision: ${label}`,
          source: track
            ? {
                release_track_id: track.release_track_id,
                release_track_name: track.release_track_name,
                artist_name: track.artist_name,
                analysis_name: track.analysis_name,
                bucket: track.bucket,
                dominant_family: track.dominant_family,
                review_families: track.review_families,
                confidence: track.confidence,
              }
            : null,
        };
        if (decision.verdict === "good_to_group") {
          trackApproved.push(entry);
        } else if (decision.verdict === "not_good") {
          trackRejected.push(entry);
        } else {
          trackSkipped.push(entry);
        }
      }
    }

    const totalLocalDecisions = (
      groupApproved.length
      + groupRejected.length
      + groupSkipped.length
      + trackApproved.length
      + trackRejected.length
      + trackSkipped.length
    );
    const previewPayload = {
      generated_at: new Date().toISOString(),
      summary: {
        total_local_decisions: totalLocalDecisions,
        groups: {
          approved: groupApproved.length,
          rejected: groupRejected.length,
          skipped: groupSkipped.length,
        },
        tracks: {
          approved: trackApproved.length,
          rejected: trackRejected.length,
          skipped: trackSkipped.length,
        },
      },
      decisions: {
        groups: {
          approved: groupApproved,
          rejected: groupRejected,
          skipped: groupSkipped,
        },
        tracks: {
          approved: trackApproved,
          rejected: trackRejected,
          skipped: trackSkipped,
        },
      },
    };
    const previewJson = JSON.stringify(previewPayload, null, 2);
    const canSaveSubmission = Boolean(
      totalLocalDecisions > 0
      && identityAuditPreviewValidationResult
      && !identityAuditPreviewValidationLoading,
    );

    const copyPreviewJson = async () => {
      if (!("clipboard" in navigator) || typeof navigator.clipboard?.writeText !== "function") {
        setIdentityAuditPreviewCopyStatus("Clipboard unavailable");
        return;
      }
      try {
        await navigator.clipboard.writeText(previewJson);
        setIdentityAuditPreviewCopyStatus("Copied JSON");
      } catch {
        setIdentityAuditPreviewCopyStatus("Copy failed");
      }
    };

    const downloadPreviewJson = () => {
      try {
        const blob = new Blob([previewJson], { type: "application/json;charset=utf-8" });
        const objectUrl = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = objectUrl;
        link.download = "identity-audit-submission-preview.json";
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(objectUrl);
      } catch {
        // Keep silent; this is a convenience path only.
      }
    };

    const validatePreviewJson = async () => {
      if (identityAuditPreviewValidationLoading) {
        return;
      }
      setIdentityAuditPreviewValidationLoading(true);
      setIdentityAuditPreviewValidationError("");
      try {
        const response = await fetch(
          `${apiBaseUrl}/debug/tracks/identity-audit/submission-preview/validate`,
          {
            method: "POST",
            credentials: "include",
            headers: { "Content-Type": "application/json" },
            body: previewJson,
          },
        );
        if (!response.ok) {
          let detail = "Failed to validate submission preview.";
          try {
            const payload = (await response.json()) as { detail?: string };
            if (payload.detail) {
              detail = payload.detail;
            }
          } catch {
            // keep fallback
          }
          throw new Error(detail);
        }
        const payload = (await response.json()) as SubmissionPreviewValidationResponse;
        setIdentityAuditPreviewValidationResult(payload);
        setIdentityAuditPreviewValidatedAt(Date.now());
      } catch (error) {
        setIdentityAuditPreviewValidationError(formatUiErrorMessage(error, "Failed to validate preview."));
        setIdentityAuditPreviewValidationResult(null);
        setIdentityAuditPreviewValidatedAt(null);
      } finally {
        setIdentityAuditPreviewValidationLoading(false);
      }
    };

    const saveSubmissionPreview = async () => {
      if (identityAuditSubmissionSaveLoading || !canSaveSubmission) {
        return;
      }
      setIdentityAuditSubmissionSaveLoading(true);
      setIdentityAuditSubmissionSaveError("");
      setIdentityAuditSubmissionSaveResult(null);
      try {
        const response = await fetch(
          `${apiBaseUrl}/debug/tracks/identity-audit/submissions`,
          {
            method: "POST",
            credentials: "include",
            headers: { "Content-Type": "application/json" },
            body: previewJson,
          },
        );
        if (!response.ok) {
          let detail = "Failed to save submission.";
          try {
            const payload = (await response.json()) as { detail?: string };
            if (payload.detail) {
              detail = payload.detail;
            }
          } catch {
            // keep fallback
          }
          throw new Error(detail);
        }
        const payload = (await response.json()) as IdentityAuditSubmissionSaveResponse;
        setIdentityAuditSubmissionSaveResult(payload);
        void loadIdentityAuditSavedSubmissions(true);
      } catch (error) {
        setIdentityAuditSubmissionSaveError(formatUiErrorMessage(error, "Failed to save submission."));
      } finally {
        setIdentityAuditSubmissionSaveLoading(false);
      }
    };

    const renderPreviewBucket = (title: string, entries: Array<Record<string, unknown>>) => (
      <div className="identity-audit-group" key={`preview-${title}`}>
        <div className="tracks-formula-heading">
          <h3>{title}</h3>
          <span>{entries.length}</span>
        </div>
        {entries.length === 0 ? (
          <p className="empty-copy">None</p>
        ) : (
          <div className="identity-audit-variant-list">
            {entries.map((entry, index) => (
              <div className="identity-audit-variant" key={`preview-entry-${title}-${String(entry.decision_key)}-${index}`}>
                <div className="identity-audit-variant-main">
                  <strong>{String(entry.label ?? entry.id ?? "Unknown item")}</strong>
                  <span>{String(entry.would ?? "")}</span>
                  <code>{String(entry.decision_key ?? "")}</code>
                </div>
                <div className="identity-audit-variant-stats">
                  {entry.family ? <span>{String(entry.family)}</span> : null}
                  {entry.bucket ? <span>{String(entry.bucket)}</span> : null}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    );

    const applyFocusedAction = (verdict: LocalReviewVerdict) => {
      if (!focusedItem) {
        return;
      }
      const nextDecisions = {
        ...identityAuditLocalDecisions,
        [focusedItem.decision_key]: {
          verdict,
          grouping_target: verdict === "good_to_group"
            ? (identityAuditLocalDecisions[focusedItem.decision_key]?.grouping_target ?? "same_composition")
            : null,
          note: identityAuditLocalDecisions[focusedItem.decision_key]?.note ?? "",
          updated_at_ms: Date.now(),
        },
      };
      updateLocalReviewDecision(focusedItem.decision_key, {
        verdict,
        grouping_target: verdict === "good_to_group"
          ? (identityAuditLocalDecisions[focusedItem.decision_key]?.grouping_target ?? "same_composition")
          : null,
      });
      setIdentityAuditFocusedReviewKey(findNextUnreviewedDecisionKey(unifiedItems, focusedItem.decision_key, nextDecisions));
    };

    return (
      <div className="identity-audit-grid">
        <div className="identity-audit-ambiguous-toolbar">
          <p className="identity-audit-tab-copy">
            Work one queue from candidate to decision, then validate and save. Saved submissions remain dry-run only unless a future apply path is added.
          </p>
          <div className="identity-audit-ambiguous-summary">
            <span className="identity-audit-pill">Local only (not saved)</span>
            <span className="identity-audit-pill">Reviewed {reviewedCount} / {totalReviewableCount}</span>
            <span className="identity-audit-pill">Shortcuts: A approve, R reject, S skip, N next</span>
            <button
              className="secondary-button"
              onClick={() => {
                setIdentityAuditLocalDecisions({});
                setIdentityAuditPreviewCopyStatus("");
                setIdentityAuditPreviewValidationLoading(false);
                setIdentityAuditPreviewValidationError("");
                setIdentityAuditPreviewValidationResult(null);
                setIdentityAuditPreviewValidatedAt(null);
                setIdentityAuditSubmissionSaveLoading(false);
                setIdentityAuditSubmissionSaveError("");
                setIdentityAuditSubmissionSaveResult(null);
              }}
              type="button"
            >
              Reset local decisions
            </button>
          </div>
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Workflow</h3>
            <span>{reviewedCount} / {totalReviewableCount} reviewed</span>
          </div>
          <div className="identity-audit-stats">
            <span className="identity-audit-stat"><span>1 Candidate</span><strong>{unifiedItems.length} queued</strong></span>
            <span className="identity-audit-stat"><span>2 Review</span><strong>{focusedItem ? "active" : "complete"}</strong></span>
            <span className="identity-audit-stat"><span>3 Decision</span><strong>{totalLocalDecisions} local</strong></span>
            <span className="identity-audit-stat"><span>4 Validate</span><strong>{identityAuditPreviewValidationResult ? "validated" : "not validated"}</strong></span>
            <span className="identity-audit-stat"><span>5 Save</span><strong>{identityAuditSubmissionSaveResult ? `#${identityAuditSubmissionSaveResult.submission_id}` : "not saved"}</strong></span>
          </div>
        </div>
        <div className="identity-audit-ambiguous-filters">
          <label>
            Family
            <select
              onChange={(event) => setIdentityAuditAmbiguousFamilyFilter(event.target.value)}
              value={identityAuditAmbiguousFamilyFilter}
            >
              <option value="all">All families</option>
              {familyOptions.map((family) => (
                <option key={`family-${family.family}`} value={family.family}>{family.family} ({family.count})</option>
              ))}
            </select>
          </label>
          <label>
            Bucket
            <select
              onChange={(event) => setIdentityAuditAmbiguousBucketFilter(event.target.value as "all" | "grouped" | "ungrouped")}
              value={identityAuditAmbiguousBucketFilter}
            >
              <option value="all">All</option>
              <option value="grouped">Grouped</option>
              <option value="ungrouped">Ungrouped</option>
            </select>
          </label>
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Candidate Summary</h3>
            <span>{summaryEntries.length} buckets</span>
          </div>
          {visibleSummaryEntries.length > 0 ? (
            <div className="identity-audit-stats">
              {visibleSummaryEntries.map(([label, counts]) => (
                <span className="identity-audit-stat" key={`summary-${label}`}>
                  <span>{label}</span>
                  <strong>
                    {counts.total} total | {counts.approved} approved | {counts.rejected} rejected | {counts.skipped} skipped | {counts.unreviewed} unreviewed
                  </strong>
                </span>
              ))}
              {remainingSummaryCount > 0 ? (
                <span className="identity-audit-stat"><span>More buckets</span><strong>+{remainingSummaryCount} more</strong></span>
              ) : null}
            </div>
          ) : (
            <p className="empty-copy">No review buckets available yet.</p>
          )}
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Review Active Candidate</h3>
            <span>{findNextUnreviewedDecisionKey(unifiedItems) ? "Ready" : "Complete"}</span>
          </div>
          {focusedItem ? (
            <article className="identity-audit-example">
              <div className="identity-audit-example-header">
                <div>
                  <h4>{focusedItem.title}</h4>
                  <p>{focusedItem.subtitle}</p>
                </div>
                <span className="identity-audit-type-badge">{focusedItem.item_type === "group" ? "Suggested match" : "Needs review"}</span>
              </div>
              <div className="identity-audit-stats">
                <span className="identity-audit-stat"><span>Issue group</span><strong>{focusedItem.bucket_label}</strong></span>
                <span className="identity-audit-stat"><span>Reason</span><strong>{focusedItem.family_label}</strong></span>
                <span className="identity-audit-stat"><span>Decision</span><strong>{focusedDecision?.verdict ?? "unreviewed"}</strong></span>
              </div>
              <div className="identity-audit-ambiguous-summary">
                <button className="secondary-button" onClick={() => applyFocusedAction("good_to_group")} type="button">Approve</button>
                <button className="secondary-button" onClick={() => applyFocusedAction("not_good")} type="button">Reject</button>
                <button className="secondary-button" onClick={() => applyFocusedAction("skipped")} type="button">Skip</button>
                <button
                  className="secondary-button"
                  onClick={() => setIdentityAuditFocusedReviewKey(findNextUnreviewedDecisionKey(unifiedItems, focusedItem.decision_key))}
                  type="button"
                >
                  Next unreviewed
                </button>
              </div>
            </article>
          ) : (
            <p className="empty-copy">All items reviewed locally.</p>
          )}
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Validate and Save Decisions</h3>
            <span>{totalLocalDecisions} decisions</span>
          </div>
          <div className="identity-audit-ambiguous-summary">
            <span className="identity-audit-pill">Groups: {groupApproved.length} approved, {groupRejected.length} rejected, {groupSkipped.length} skipped</span>
            <span className="identity-audit-pill">Tracks: {trackApproved.length} approved, {trackRejected.length} rejected, {trackSkipped.length} skipped</span>
            <button className="secondary-button" onClick={() => void copyPreviewJson()} type="button">Copy JSON</button>
            <button className="secondary-button" onClick={downloadPreviewJson} type="button">Download JSON</button>
            <button
              className="secondary-button"
              disabled={identityAuditPreviewValidationLoading}
              onClick={() => void validatePreviewJson()}
              type="button"
            >
              {identityAuditPreviewValidationLoading
                ? "Validating..."
                : identityAuditPreviewValidationResult
                  ? "Revalidate Preview"
                  : "Validate Preview"}
            </button>
            <button
              className="secondary-button"
              disabled={!canSaveSubmission || identityAuditSubmissionSaveLoading}
              onClick={() => void saveSubmissionPreview()}
              type="button"
            >
              {identityAuditSubmissionSaveLoading ? "Saving..." : "Save Submission"}
            </button>
            {identityAuditPreviewCopyStatus ? <span className="identity-audit-pill">{identityAuditPreviewCopyStatus}</span> : null}
          </div>
          <p className="empty-copy">Saved only. No changes applied.</p>
          {identityAuditPreviewValidationResult
            && (identityAuditPreviewValidationResult.summary.warnings > 0
              || identityAuditPreviewValidationResult.summary.unknown_groups > 0
              || identityAuditPreviewValidationResult.summary.unknown_tracks > 0) ? (
            <p className="empty-copy">Validation has warnings; saved record will include them.</p>
            ) : null}
          {identityAuditPreviewValidationError ? <p className="empty-copy">{identityAuditPreviewValidationError}</p> : null}
          {identityAuditSubmissionSaveError ? <p className="empty-copy">{identityAuditSubmissionSaveError}</p> : null}
          {identityAuditSubmissionSaveResult ? (
            <div className="identity-audit-group">
              <div className="tracks-formula-heading">
                <h3>Saved Submission</h3>
                <span>#{identityAuditSubmissionSaveResult.submission_id}</span>
              </div>
              <p className="empty-copy">
                Saved submission #{identityAuditSubmissionSaveResult.submission_id}
                {" "}
                ({identityAuditSubmissionSaveResult.status}) at {new Date(identityAuditSubmissionSaveResult.created_at).toLocaleString()}.
              </p>
              <div className="identity-audit-stats">
                <span className="identity-audit-stat"><span>Warnings</span><strong>{identityAuditSubmissionSaveResult.warnings.length}</strong></span>
                <span className="identity-audit-stat"><span>Unknown groups</span><strong>{identityAuditSubmissionSaveResult.unknown_items.groups.length}</strong></span>
                <span className="identity-audit-stat"><span>Unknown tracks</span><strong>{identityAuditSubmissionSaveResult.unknown_items.tracks.length}</strong></span>
              </div>
            </div>
          ) : null}
          {identityAuditPreviewValidationResult ? (
            <div className="identity-audit-group">
              <div className="tracks-formula-heading">
                <h3>Validation Result</h3>
                <span>{identityAuditPreviewValidationResult.ok ? "ok" : "failed"}</span>
              </div>
              {identityAuditPreviewValidatedAt ? (
                <p className="empty-copy">Validated at {new Date(identityAuditPreviewValidatedAt).toLocaleTimeString()}</p>
              ) : null}
              <div className="identity-audit-stats">
                <span className="identity-audit-stat"><span>Total</span><strong>{identityAuditPreviewValidationResult.summary.total_decisions}</strong></span>
                <span className="identity-audit-stat"><span>Groups</span><strong>{identityAuditPreviewValidationResult.summary.group_decisions}</strong></span>
                <span className="identity-audit-stat"><span>Tracks</span><strong>{identityAuditPreviewValidationResult.summary.track_decisions}</strong></span>
                <span className="identity-audit-stat"><span>Warnings</span><strong>{identityAuditPreviewValidationResult.summary.warnings}</strong></span>
                <span className="identity-audit-stat"><span>Unknown groups</span><strong>{identityAuditPreviewValidationResult.summary.unknown_groups}</strong></span>
                <span className="identity-audit-stat"><span>Unknown tracks</span><strong>{identityAuditPreviewValidationResult.summary.unknown_tracks}</strong></span>
              </div>
              {identityAuditPreviewValidationResult.summary.total_decisions === 0 ? (
                <p className="empty-copy">No decisions to validate.</p>
              ) : null}
              {identityAuditPreviewValidationResult.warnings.length > 0 ? (
                <div className="identity-audit-variant-list">
                  {identityAuditPreviewValidationResult.warnings.map((warning, index) => (
                    <div className="identity-audit-variant" key={`validation-warning-${index}`}>
                      <div className="identity-audit-variant-main">
                        <strong>Warning</strong>
                        <span>{warning}</span>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="empty-copy">No validation warnings.</p>
              )}
              <div className="identity-audit-group">
                <div className="tracks-formula-heading">
                  <h3>Unknown Groups</h3>
                  <span>{identityAuditPreviewValidationResult.unknown_items.groups.length}</span>
                </div>
                {identityAuditPreviewValidationResult.unknown_items.groups.length > 0 ? (
                  <div className="identity-audit-variant-list">
                    {identityAuditPreviewValidationResult.unknown_items.groups.map((item, index) => (
                      <div className="identity-audit-variant" key={`unknown-group-${index}`}>
                        <div className="identity-audit-variant-main">
                          <strong>{String(item.label ?? item.id ?? "Unknown group")}</strong>
                          <code>{String(item.decision_key ?? "")}</code>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="empty-copy">None.</p>
                )}
              </div>
              <div className="identity-audit-group">
                <div className="tracks-formula-heading">
                  <h3>Unknown Tracks</h3>
                  <span>{identityAuditPreviewValidationResult.unknown_items.tracks.length}</span>
                </div>
                {identityAuditPreviewValidationResult.unknown_items.tracks.length > 0 ? (
                  <div className="identity-audit-variant-list">
                    {identityAuditPreviewValidationResult.unknown_items.tracks.map((item, index) => (
                      <div className="identity-audit-variant" key={`unknown-track-${index}`}>
                        <div className="identity-audit-variant-main">
                          <strong>{String(item.label ?? item.id ?? "Unknown track")}</strong>
                          <code>{String(item.decision_key ?? "")}</code>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="empty-copy">None.</p>
                )}
              </div>
            </div>
          ) : null}
          {totalLocalDecisions === 0 ? (
            <p className="empty-copy">No local decisions yet.</p>
          ) : (
            <div className="identity-audit-grid">
              <div className="identity-audit-group">
                <div className="tracks-formula-heading">
                  <h3>Group Decisions</h3>
                  <span>{groupApproved.length + groupRejected.length + groupSkipped.length}</span>
                </div>
                {renderPreviewBucket("Approved", groupApproved)}
                {renderPreviewBucket("Rejected", groupRejected)}
                {renderPreviewBucket("Skipped", groupSkipped)}
              </div>
              <div className="identity-audit-group">
                <div className="tracks-formula-heading">
                  <h3>Track Decisions</h3>
                  <span>{trackApproved.length + trackRejected.length + trackSkipped.length}</span>
                </div>
                {renderPreviewBucket("Approved", trackApproved)}
                {renderPreviewBucket("Rejected", trackRejected)}
                {renderPreviewBucket("Skipped", trackSkipped)}
              </div>
            </div>
          )}
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Saved Decision Sets</h3>
            <span>{identityAuditSavedSubmissions?.total ?? 0}</span>
          </div>
          <div className="identity-audit-ambiguous-summary">
            <button
              className="secondary-button"
              disabled={identityAuditSavedSubmissionsLoading}
              onClick={() => void loadIdentityAuditSavedSubmissions(true)}
              type="button"
            >
              {identityAuditSavedSubmissionsLoading ? "Refreshing..." : "Refresh saved submissions"}
            </button>
          </div>
          {identityAuditSavedSubmissionsError ? <p className="empty-copy">{identityAuditSavedSubmissionsError}</p> : null}
          {!identityAuditSavedSubmissions && !identityAuditSavedSubmissionsError ? (
            <p className="empty-copy">{identityAuditSavedSubmissionsLoading ? "Loading saved submissions..." : "Saved submissions are not loaded yet."}</p>
          ) : null}
          {identityAuditSavedSubmissions && identityAuditSavedSubmissions.items.length === 0 ? (
            <p className="empty-copy">No saved submissions yet.</p>
          ) : null}
          {identityAuditSavedSubmissions && identityAuditSavedSubmissions.items.length > 0 ? (
            <div className="identity-audit-variant-list">
              {identityAuditSavedSubmissions.items.map((item) => (
                <div className="identity-audit-variant" key={`saved-submission-${item.id}`}>
                  <div className="identity-audit-variant-main">
                    <strong>#{item.id} • {item.status}</strong>
                    <span>{new Date(item.created_at).toLocaleString()}</span>
                    <span>
                      {Number(item.summary.total_decisions ?? 0)} decisions • {item.warnings_count} warnings • {item.unknown_groups} unknown groups • {item.unknown_tracks} unknown tracks
                    </span>
                  </div>
                  <div className="identity-audit-variant-stats">
                    <button className="secondary-button" onClick={() => void viewIdentityAuditSavedSubmission(item.id)} type="button">View</button>
                  </div>
                </div>
              ))}
            </div>
          ) : null}
          {identityAuditSavedSubmissionDetailError ? <p className="empty-copy">{identityAuditSavedSubmissionDetailError}</p> : null}
          {identityAuditSavedSubmissionDetailLoading ? <p className="empty-copy">Loading saved submission...</p> : null}
          {identityAuditSavedSubmissionDetail ? (
            <div>
              <div className="tracks-formula-heading">
                <h3>Saved Submission Detail</h3>
                <span>#{identityAuditSavedSubmissionDetail.item.id}</span>
              </div>
              <div className="identity-audit-ambiguous-summary">
                <button
                  className="secondary-button"
                  disabled={identityAuditSavedSubmissionDryRunLoading}
                  onClick={() => void dryRunIdentityAuditSavedSubmission(identityAuditSavedSubmissionDetail.item.id)}
                  type="button"
                >
                  {identityAuditSavedSubmissionDryRunLoading
                    ? "Running dry run..."
                    : identityAuditSavedSubmissionDryRun
                      ? "Re-run Dry Run"
                      : "Dry Run"}
                </button>
              </div>
              <p className="empty-copy">Dry run only. No changes applied.</p>
              <div className="identity-audit-stats">
                <span className="identity-audit-stat"><span>Status</span><strong>{identityAuditSavedSubmissionDetail.item.status}</strong></span>
                <span className="identity-audit-stat"><span>Created</span><strong>{new Date(identityAuditSavedSubmissionDetail.item.created_at).toLocaleString()}</strong></span>
                <span className="identity-audit-stat"><span>Total</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.total_decisions}</strong></span>
                <span className="identity-audit-stat"><span>Groups</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.group_decisions}</strong></span>
                <span className="identity-audit-stat"><span>Tracks</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.track_decisions}</strong></span>
              </div>
              <div className="identity-audit-stats">
                <span className="identity-audit-stat"><span>Approved</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.approved}</strong></span>
                <span className="identity-audit-stat"><span>Rejected</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.rejected}</strong></span>
                <span className="identity-audit-stat"><span>Skipped</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.skipped}</strong></span>
                <span className="identity-audit-stat"><span>Warnings</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.warnings}</strong></span>
              </div>
              {identityAuditSavedSubmissionDryRunError ? <p className="empty-copy">{identityAuditSavedSubmissionDryRunError}</p> : null}
              {identityAuditSavedSubmissionDryRun ? (
                <div className="identity-audit-group">
                  <div className="tracks-formula-heading">
                    <h3>Dry Run Result</h3>
                    <span>#{identityAuditSavedSubmissionDryRun.submission_id} • {identityAuditSavedSubmissionDryRun.status}</span>
                  </div>
                  {identityAuditSavedSubmissionDryRunAt ? (
                    <p className="empty-copy">Dry run at {new Date(identityAuditSavedSubmissionDryRunAt).toLocaleTimeString()}</p>
                  ) : null}
                  <div className="identity-audit-stats">
                    <span className="identity-audit-stat"><span>Would apply</span><strong>{identityAuditSavedSubmissionDryRun.summary.would_apply}</strong></span>
                    <span className="identity-audit-stat"><span>Approved groups</span><strong>{identityAuditSavedSubmissionDryRun.summary.approved_groups}</strong></span>
                    <span className="identity-audit-stat"><span>Approved tracks</span><strong>{identityAuditSavedSubmissionDryRun.summary.approved_tracks}</strong></span>
                    <span className="identity-audit-stat"><span>Rejected no-ops</span><strong>{identityAuditSavedSubmissionDryRun.summary.rejected}</strong></span>
                    <span className="identity-audit-stat"><span>Skipped no-ops</span><strong>{identityAuditSavedSubmissionDryRun.summary.skipped}</strong></span>
                  </div>
                  <div className="identity-audit-stats">
                    <span className="identity-audit-stat"><span>Warnings</span><strong>{identityAuditSavedSubmissionDryRun.summary.warnings}</strong></span>
                    <span className="identity-audit-stat"><span>Unknown groups</span><strong>{identityAuditSavedSubmissionDryRun.summary.unknown_groups}</strong></span>
                    <span className="identity-audit-stat"><span>Unknown tracks</span><strong>{identityAuditSavedSubmissionDryRun.summary.unknown_tracks}</strong></span>
                  </div>
                  {identityAuditSavedSubmissionDryRun.warnings.length > 0 ? (
                    <div className="identity-audit-variant-list">
                      {identityAuditSavedSubmissionDryRun.warnings.map((warning, index) => (
                        <div className="identity-audit-variant" key={`dry-run-warning-${index}`}>
                          <div className="identity-audit-variant-main">
                            <strong>Warning</strong>
                            <span>{warning}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : null}
                  <div className="identity-audit-group">
                    <div className="tracks-formula-heading">
                      <h3>Plan</h3>
                      <span>{identityAuditSavedSubmissionDryRun.plan.groups.length + identityAuditSavedSubmissionDryRun.plan.tracks.length} items</span>
                    </div>
                    {identityAuditSavedSubmissionDryRun.plan.groups.length === 0 && identityAuditSavedSubmissionDryRun.plan.tracks.length === 0 ? (
                      <p className="empty-copy">No plan items.</p>
                    ) : (
                      <div className="identity-audit-variant-list">
                        {identityAuditSavedSubmissionDryRun.plan.groups.map((item, index) => (
                          <div className="identity-audit-variant" key={`dry-run-group-${index}`}>
                            <div className="identity-audit-variant-main">
                              <strong>{String(item.label ?? item.id ?? "Group item")}</strong>
                              <span>{String(item.action ?? "would_accept_group")}</span>
                              <code>{String(item.decision_key ?? "")}</code>
                            </div>
                          </div>
                        ))}
                        {identityAuditSavedSubmissionDryRun.plan.tracks.map((item, index) => (
                          <div className="identity-audit-variant" key={`dry-run-track-${index}`}>
                            <div className="identity-audit-variant-main">
                              <strong>{String(item.label ?? item.id ?? "Track item")}</strong>
                              <span>{String(item.action ?? "would_accept_track_mapping")}</span>
                              <code>{String(item.decision_key ?? "")}</code>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Candidate Source: Suggested Matches</h3>
            <span>{suggestedItems.length} groups</span>
          </div>
          {identityAuditSuggestedError ? <p className="empty-copy">{identityAuditSuggestedError}</p> : null}
          {!identityAuditSuggestedGroups && !identityAuditSuggestedError ? (
            <p className="empty-copy">{identityAuditSuggestedLoading ? "Loading suggested groups..." : "Suggested groups are not loaded yet."}</p>
          ) : null}
          {suggestedItems.length > 0 ? (
            <div className="identity-audit-examples">
              {suggestedItems.map((group) => {
                const decisionKey = groupDecisionKey(group);
                const decision = identityAuditLocalDecisions[decisionKey] ?? {
                  verdict: "unsure" as LocalReviewVerdict,
                  grouping_target: null,
                  note: "",
                  updated_at_ms: 0,
                };
                return (
                  <article className="identity-audit-example" key={`suggested-${group.analysis_track_id}`}>
                    <div className="identity-audit-example-header">
                      <div>
                        <h4>{group.analysis_track_name || `Track Family ${group.analysis_track_id}`}</h4>
                        <p>{group.match_method || "suggested"} | {Math.round(group.confidence * 100)}% confidence</p>
                      </div>
                      <span className="identity-audit-type-badge">Suggested match</span>
                    </div>
                    <div className="identity-audit-stats">
                      <span className="identity-audit-stat"><span>Release tracks</span><strong>{group.release_track_count}</strong></span>
                      {group.song_family_key ? <span className="identity-audit-stat"><span>Family key</span><strong>{group.song_family_key}</strong></span> : null}
                    </div>
                    <div className="identity-audit-review-controls">
                      <label>
                        Decision
                        <select
                          onChange={(event) => {
                            const nextVerdict = event.target.value as LocalReviewVerdict;
                            updateLocalReviewDecision(decisionKey, {
                              verdict: nextVerdict,
                              grouping_target: nextVerdict === "good_to_group" ? (decision.grouping_target ?? "same_composition") : null,
                            });
                          }}
                          value={decision.verdict}
                        >
                          <option value="unsure">Unreviewed</option>
                          <option value="good_to_group">Good to group</option>
                          <option value="not_good">Not good</option>
                          <option value="skipped">Skipped</option>
                        </select>
                      </label>
                      <label>
                        Grouping target
                        <select
                          disabled={decision.verdict !== "good_to_group"}
                          onChange={(event) =>
                            updateLocalReviewDecision(decisionKey, {
                              grouping_target: event.target.value as Exclude<LocalGroupingTarget, null>,
                            })}
                          value={decision.grouping_target ?? "same_composition"}
                        >
                          <option value="same_composition">Group as same work</option>
                          <option value="same_release_track_only">Keep as release-only match</option>
                        </select>
                      </label>
                    </div>
                    <label className="identity-audit-review-note">
                      Note
                      <textarea
                        onChange={(event) => updateLocalReviewDecision(decisionKey, { note: event.target.value })}
                        placeholder="Optional review context"
                        rows={2}
                        value={decision.note}
                      />
                    </label>
                    <div className="identity-audit-variant-list">
                      {group.release_tracks.map((releaseTrack) => (
                        <div className="identity-audit-variant" key={`group-${group.analysis_track_id}-${releaseTrack.release_track_id}`}>
                          <div className="identity-audit-variant-main">
                            <strong>{releaseTrack.release_track_name}</strong>
                            <span>{releaseTrack.primary_artists || "Unknown artists"}</span>
                            <code>release {releaseTrack.release_track_id}</code>
                          </div>
                          <div className="identity-audit-variant-stats">
                            {releaseTrack.album_names ? <span>{releaseTrack.album_names}</span> : null}
                          </div>
                        </div>
                      ))}
                    </div>
                  </article>
                );
              })}
            </div>
          ) : identityAuditSuggestedGroups ? (
            <p className="empty-copy">No suggested groups returned.</p>
          ) : null}
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Candidate Source: Needs Review</h3>
            <span>{filteredItems.length} rows</span>
          </div>
          {identityAuditAmbiguousError ? <p className="empty-copy">{identityAuditAmbiguousError}</p> : null}
          {!identityAuditAmbiguous && !identityAuditAmbiguousError ? (
            <p className="empty-copy">{identityAuditAmbiguousLoading ? "Loading ambiguous queue..." : "Ambiguous queue is not loaded yet."}</p>
          ) : null}
          {identityAuditAmbiguous?.parse_warning ? (
            <p className="empty-copy">Parser warning: {identityAuditAmbiguous.parse_warning}</p>
          ) : null}
          {visibleItems.length > 0 ? (
            <div className="identity-audit-examples">
              {visibleItems.map((item) => {
                const decision = identityAuditLocalDecisions[trackDecisionKey(item)] ?? {
                  verdict: "unsure" as LocalReviewVerdict,
                  grouping_target: null,
                  note: "",
                  updated_at_ms: 0,
                };
                return (
                  <article className="identity-audit-example" key={`ambiguous-${item.entry_id}`}>
                  <div className="identity-audit-example-header">
                    <div>
                      <h4>{item.release_track_name}</h4>
                      <p>{item.artist_name} | {item.bucket} | {item.analysis_name ?? "no analysis mapping"}</p>
                    </div>
                    <span className="identity-audit-type-badge">{item.dominant_family ?? "ambiguous"}</span>
                  </div>
                  <div className="identity-audit-stats">
                    <span className="identity-audit-stat"><span>release</span><strong>{item.release_track_id}</strong></span>
                    {item.confidence != null ? <span className="identity-audit-stat"><span>confidence</span><strong>{Math.round(item.confidence * 100)}%</strong></span> : null}
                    {item.song_family_key ? <span className="identity-audit-stat"><span>family key</span><strong>{item.song_family_key}</strong></span> : null}
                    {item.review_families.map((family) => (
                      <span className="identity-audit-stat" key={`${item.entry_id}-${family}`}><span>rule</span><strong>{family}</strong></span>
                    ))}
                  </div>
                  <div className="identity-audit-review-controls">
                    <label>
                      Decision
                      <select
                        onChange={(event) => {
                          const nextVerdict = event.target.value as LocalReviewVerdict;
                          updateLocalReviewDecision(trackDecisionKey(item), {
                            verdict: nextVerdict,
                            grouping_target: nextVerdict === "good_to_group" ? (decision.grouping_target ?? "same_composition") : null,
                          });
                        }}
                        value={decision.verdict}
                      >
                        <option value="unsure">Unreviewed</option>
                        <option value="good_to_group">Good to group</option>
                        <option value="not_good">Not good</option>
                        <option value="skipped">Skipped</option>
                      </select>
                    </label>
                    <label>
                      Grouping target
                      <select
                        disabled={decision.verdict !== "good_to_group"}
                        onChange={(event) =>
                          updateLocalReviewDecision(trackDecisionKey(item), {
                            grouping_target: event.target.value as Exclude<LocalGroupingTarget, null>,
                          })}
                        value={decision.grouping_target ?? "same_composition"}
                      >
                        <option value="same_composition">Group as same work</option>
                        <option value="same_release_track_only">Keep as release-only match</option>
                      </select>
                    </label>
                  </div>
                  <label className="identity-audit-review-note">
                    Note
                    <textarea
                      onChange={(event) => updateLocalReviewDecision(trackDecisionKey(item), { note: event.target.value })}
                      placeholder="Optional review context"
                      rows={2}
                      value={decision.note}
                    />
                  </label>
                </article>
                );
              })}
            </div>
          ) : identityAuditAmbiguous ? (
            <p className="empty-copy">No ambiguous rows match the current filters.</p>
          ) : null}
          {filteredItems.length > visibleItems.length ? (
            <div className="identity-audit-load-more-row">
              <button
                className="secondary-button"
                onClick={() => setIdentityAuditAmbiguousVisibleCount((current) => current + IDENTITY_AUDIT_AMBIGUOUS_VISIBLE_STEP)}
                type="button"
              >
                Show more ({filteredItems.length - visibleItems.length} remaining)
              </button>
            </div>
          ) : null}
        </div>
      </div>
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
    const coveragePercent = typeof catalogBackfillCoverage?.track_duration_coverage_percent === "number"
      ? `${catalogBackfillCoverage.track_duration_coverage_percent.toFixed(2)}%`
      : "0.00%";
    return (
      <div className="identity-audit-grid">
        <p className="identity-audit-tab-copy">
          Album catalog is operational state: Spotify metadata, tracklist completeness, queue/enrichment status, and catalog lookup.
        </p>
        <div className="identity-audit-overview-grid">
          <article className="identity-audit-overview-card">
            <h3>Known Albums</h3>
            <p>Release albums known locally.</p>
            <strong>{catalogBackfillCoverage?.known_release_albums ?? 0}</strong>
          </article>
          <article className="identity-audit-overview-card">
            <h3>Catalog Rows</h3>
            <p>Spotify album metadata rows.</p>
            <strong>{catalogBackfillCoverage?.album_catalog_rows ?? 0}</strong>
          </article>
          <article className="identity-audit-overview-card">
            <h3>Tracklists</h3>
            <p>Stored album-track rows.</p>
            <strong>{catalogBackfillCoverage?.album_track_rows ?? 0}</strong>
          </article>
          <article className="identity-audit-overview-card">
            <h3>Track Coverage</h3>
            <p>Release tracks with duration metadata.</p>
            <strong>{coveragePercent}</strong>
          </article>
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Catalog Operations</h3>
            <span>lookup and queue</span>
          </div>
          <p className="identity-audit-tab-copy">
            Search Lookup remains the shared operational lookup tool. Opening it from here defaults the tool to albums.
          </p>
          <div style={{ display: "flex", flexWrap: "wrap", gap: "10px" }}>
            <button
              className="primary-button"
              onClick={() => {
                setSearchLookupEntityType("albums");
                setAppPage("searchLookup");
              }}
              type="button"
            >
              Open Album Lookup
            </button>
            <button
              className="secondary-button"
              onClick={() => setAppPage("catalogBackfill")}
              type="button"
            >
              Open Catalog Backfill
            </button>
            <button
              className="secondary-button"
              disabled={catalogBackfillCoverageLoading}
              onClick={() => void loadCatalogBackfillCoverage(true)}
              type="button"
            >
              {catalogBackfillCoverageLoading ? "Refreshing..." : "Refresh catalog summary"}
            </button>
          </div>
          {catalogBackfillCoverageError ? <p className="empty-copy">{catalogBackfillCoverageError}</p> : null}
          {catalogBackfillCoverageLastLoadedAt ? (
            <p className="empty-copy">Catalog summary loaded {new Date(catalogBackfillCoverageLastLoadedAt).toLocaleTimeString()}</p>
          ) : null}
        </div>
      </div>
    );
  }

  function renderIdentityAuditPage() {
    if (!profile) {
      return null;
    }
    const trackTabs: Array<{ value: TrackIdentityAuditTab; label: string }> = [
      { value: "problems", label: "Problems" },
      { value: "mapping", label: "Mapping" },
      { value: "review_queue", label: "Review Queue" },
    ];
    const albumTabs: Array<{ value: AlbumIdentityAuditTab; label: string }> = [
      { value: "problems", label: "Problems" },
      { value: "merge_review", label: "Merge Review" },
      { value: "catalog", label: "Catalog" },
    ];
    const identityEntityTabs: Array<{ value: IdentityAuditEntityTab; label: string }> = [
      { value: "tracks", label: "Tracks" },
      { value: "albums", label: "Albums" },
      { value: "artists", label: "Artists" },
    ];

    return (
      <section className="info-card info-card-wide tracks-only-card" id="identity-audit-page">
        <div className="tracks-only-header">
          <div>
            <h2>Identity Audit</h2>
            <p className="tracks-only-subtitle">
              Find identity problems, inspect mappings and evidence, then review decisions before any grouping behavior is promoted.
            </p>
          </div>
          <div className="section-column-header-actions">
            <button
              className="secondary-button tracks-page-link-button"
              disabled={identityAuditLoading || identityAuditSuggestedLoading || identityAuditAmbiguousLoading}
              onClick={() => {
                void loadIdentityAudit(true);
                void loadIdentityAuditSuggestedGroups(true);
                void loadIdentityAuditAmbiguousReview(true);
              }}
              type="button"
            >
              {(identityAuditLoading || identityAuditSuggestedLoading || identityAuditAmbiguousLoading) ? "Reloading..." : "Reload all"}
            </button>
            <button
              className="secondary-button tracks-only-back-button"
              onClick={() => setAppPage("dashboard")}
              type="button"
            >
              Back to dashboard
            </button>
          </div>
        </div>
        <div className="tracks-only-summary">
          <span>Identity samples: {identityAudit ? `${identityAudit.limit} per group` : "not loaded"}</span>
          <span>Suggested groups: {identityAuditSuggestedGroups?.summary.total_groups ?? 0}</span>
          <span>Ambiguous queue: {identityAuditAmbiguous?.summary.total_review_entries ?? 0}</span>
          <span>Album duplicate Spotify ID groups: {albumDuplicateLookupLoaded ? (albumDuplicateLookupResult?.total ?? 0) : "not loaded"}</span>
          <span>Album duplicate name groups: {albumNameDuplicateLookupLoaded ? (albumNameDuplicateLookupResult?.total ?? 0) : "not loaded"}</span>
          {identityAuditLastLoadedAt ? <span>Identity loaded {new Date(identityAuditLastLoadedAt).toLocaleTimeString()}</span> : null}
          {identityAuditSuggestedLastLoadedAt ? <span>Suggested loaded {new Date(identityAuditSuggestedLastLoadedAt).toLocaleTimeString()}</span> : null}
          {identityAuditAmbiguousLastLoadedAt ? <span>Ambiguous loaded {new Date(identityAuditAmbiguousLastLoadedAt).toLocaleTimeString()}</span> : null}
          {albumDuplicateLookupLastLoadedAt ? <span>Album duplicates loaded {new Date(albumDuplicateLookupLastLoadedAt).toLocaleTimeString()}</span> : null}
          {albumNameDuplicateLookupLastLoadedAt ? <span>Album names loaded {new Date(albumNameDuplicateLookupLastLoadedAt).toLocaleTimeString()}</span> : null}
        </div>
        <div className="track-ranking-toggle identity-audit-tabs" role="group" aria-label="Identity audit entity type">
          {identityEntityTabs.map((tab) => (
            <button
              className={`track-ranking-chip${identityAuditEntityTab === tab.value ? " track-ranking-chip-active" : ""}`}
              key={`identity-entity-tab-${tab.value}`}
              onClick={() => setIdentityAuditEntityTab(tab.value)}
              type="button"
            >
              {tab.label}
            </button>
          ))}
        </div>
        {identityAuditEntityTab !== "artists" ? (
          <div className="track-ranking-toggle identity-audit-tabs" role="group" aria-label="Identity audit sections">
            {identityAuditEntityTab === "tracks"
              ? trackTabs.map((tab) => (
                <button
                  className={`track-ranking-chip${trackIdentityAuditTab === tab.value ? " track-ranking-chip-active" : ""}`}
                  key={`track-identity-tab-${tab.value}`}
                  onClick={() => setTrackIdentityAuditTab(tab.value)}
                  type="button"
                >
                  {tab.label}
                </button>
              ))
              : null}
            {identityAuditEntityTab === "albums"
              ? albumTabs.map((tab) => (
                <button
                  className={`track-ranking-chip${albumIdentityAuditTab === tab.value ? " track-ranking-chip-active" : ""}`}
                  key={`album-identity-tab-${tab.value}`}
                  onClick={() => setAlbumIdentityAuditTab(tab.value)}
                  type="button"
                >
                  {tab.label}
                </button>
              ))
              : null}
          </div>
        ) : null}
        {identityAuditEntityTab === "tracks" && trackIdentityAuditTab === "problems" ? renderTrackIdentityAuditProblemsTab() : null}
        {identityAuditEntityTab === "tracks" && trackIdentityAuditTab === "mapping" ? renderTrackIdentityAuditMappingTab() : null}
        {identityAuditEntityTab === "tracks" && trackIdentityAuditTab === "review_queue" ? renderTrackIdentityAuditAmbiguousTab() : null}
        {identityAuditEntityTab === "albums" && albumIdentityAuditTab === "problems" ? renderAlbumIdentityAuditProblemsTab() : null}
        {identityAuditEntityTab === "albums" && albumIdentityAuditTab === "merge_review" ? renderAlbumIdentityAuditMergeReviewTab() : null}
        {identityAuditEntityTab === "albums" && albumIdentityAuditTab === "catalog" ? renderAlbumIdentityAuditCatalogTab() : null}
        {identityAuditEntityTab === "artists" ? renderArtistIdentityAuditPlaceholder() : null}
      </section>
    );
  }

  function renderAlbumColumn(
    section: SectionKey,
    items: TopAlbum[],
    available: boolean,
    emptyCopy: string,
    unavailableCopy: string,
    unavailableAction?: ReactNode,
  ) {
    if (!available) {
      return (
        <div className="section-unavailable">
          <p className="empty-copy">{unavailableCopy}</p>
          {unavailableAction ? <div className="section-unavailable-action">{unavailableAction}</div> : null}
        </div>
      );
    }
    if (items.length === 0) {
      return <p className="empty-copy">{emptyCopy}</p>;
    }

    const pageItems = visibleItems(section, items);
    return (
      <>
        <div className="item-list">
          {pageItems.map((album, index) =>
            renderDashboardListCard(
              {
                href: album.url,
                entityId: album.album_id,
                imageUrl: album.image_url,
                imageAlt: `${album.name ?? "Album"} cover`,
                fallbackLabel: "A",
                primaryText: album.name ?? "Unknown album",
                secondaryText: album.artist_name ?? "Unknown artist",
                tertiaryText:
                  formatHistoryDebugLine(album) ??
                  formatAlbumSummary(album),
                metricText: formatAlbumBreadth(album),
              },
              album.album_id ?? `${album.name}-${index}-${section}`,
            ),
          )}
          {Array.from({ length: emptySlots(pageItems) }).map((_, index) => (
            <div className="list-row list-row-placeholder" key={`${section}-empty-${index}`} aria-hidden="true" />
          ))}
        </div>
        {renderPaging(section, items.length)}
      </>
    );
  }

  function renderPlaylistColumn(
    section: SectionKey,
    items: OwnedPlaylist[],
    available: boolean,
    emptyCopy: string,
    unavailableCopy: string,
    paged: boolean = true,
  ) {
    if (!available) {
      return <p className="empty-copy">{unavailableCopy}</p>;
    }
    if (items.length === 0) {
      return <p className="empty-copy">{emptyCopy}</p>;
    }

    const pageItems = paged ? visibleItems(section, items) : items;
    return (
      <>
        <div className="item-list">
          {pageItems.map((playlist, index) =>
            renderDashboardListCard(
              {
                href: playlist.url,
                entityId: playlist.playlist_id,
                imageUrl: playlist.image_url,
                imageAlt: `${playlist.name ?? "Playlist"} cover`,
                fallbackLabel: "P",
                primaryText: playlist.name ?? "Untitled playlist",
                primaryClamp: "two-line-clamp",
                secondaryText: playlist.description?.trim() || null,
                tertiaryText:
                  playlist.track_count != null ? `${playlist.track_count} tracks` : "Playlist",
              },
              playlist.playlist_id ?? `${playlist.name}-${index}-${section}`,
            ),
          )}
          {Array.from({ length: emptySlots(pageItems) }).map((_, index) => (
            <div className="list-row list-row-placeholder" key={`${section}-empty-${index}`} aria-hidden="true" />
          ))}
        </div>
        {paged ? renderPaging(section, items.length) : null}
      </>
    );
  }

  function renderDualSectionCard(props: {
    title: ReactNode;
    section: SectionKey;
    anchorId: string;
    leftTitle: ReactNode;
    rightTitle: ReactNode;
    leftContent: ReactNode;
    rightContent: ReactNode;
    previewItemsLeft: PreviewItem[];
    previewItemsRight: PreviewItem[];
    collapsedPreviewItems?: PreviewItem[];
  }) {
    const {
      title,
      section,
      anchorId,
      leftTitle,
      rightTitle,
      leftContent,
      rightContent,
      previewItemsLeft,
      previewItemsRight,
      collapsedPreviewItems,
    } = props;

    return (
      <section className="info-card info-card-wide" id={anchorId}>
        <button className="section-toggle section-toggle-header" onClick={() => toggleSection(section, anchorId)} type="button">
          <h2>{title}</h2>
        </button>
        {openSections[section] ? (
          <div className="artists-grid">
            <div className="artists-column">
              {typeof leftTitle === "string" ? <h3>{leftTitle}</h3> : leftTitle}
              {leftContent}
            </div>
            <div className="artists-column">
              {typeof rightTitle === "string" ? <h3>{rightTitle}</h3> : rightTitle}
              {rightContent}
            </div>
          </div>
        ) : (
          <div className="preview-strip">
            {(collapsedPreviewItems ?? previewItemsLeft.concat(previewItemsRight)).slice(0, 5).map((item, index) =>
              renderPreviewCard(item, `${String(typeof title === "string" ? title : "section")}-${item.image}-${index}`),
            )}
          </div>
        )}
        <button className="section-toggle section-toggle-footer" onClick={() => toggleSection(section, anchorId)} type="button">
          <span>{openSections[section] ? "^" : "v"}</span>
        </button>
      </section>
    );
  }

  function renderPlaylistsSection() {
    if (!profile) {
      return null;
    }

    const visiblePlaylists = visibleItemsWithPageSize(
      "playlists",
      profile.owned_playlists,
      PLAYLISTS_PAGE_SIZE,
    );
    const playlistColumns = splitItems(visiblePlaylists);

    return (
      <section className="info-card info-card-wide" id="playlists">
        <button className="section-toggle section-toggle-header" onClick={() => toggleSection("playlists", "playlists")} type="button">
          <h2>{renderSectionTitle("Playlists", "playlists")}</h2>
        </button>
        {openSections.playlists ? (
          profile.owned_playlists_available ? (
            profile.owned_playlists.length > 0 ? (
              <div className="artists-grid">
                <div className="artists-column">
                  {renderPlaylistColumn(
                    "playlists",
                    playlistColumns.left,
                    true,
                    "No playlists were returned by Spotify for this account.",
                    "",
                    false,
                  )}
                </div>
                <div className="artists-column">
                  {playlistColumns.right.length > 0
                    ? renderPlaylistColumn(
                        "playlists",
                        playlistColumns.right,
                        true,
                        "No playlists were returned by Spotify for this account.",
                        "",
                        false,
                      )
                    : <p className="empty-copy">No more playlists in this column yet.</p>}
                </div>
              </div>
            ) : (
              <p className="empty-copy">No playlists were returned by Spotify for this account.</p>
            )
          ) : (
            <p className="empty-copy">
              {quickUnavailableCopy("Playlist access is not available for this session yet. Log out and log back in to grant access.")}
            </p>
          )
        ) : (
          <div className="preview-strip">
            {previewItems(profile.owned_playlists).map((item, index) =>
              renderPreviewCard(item, `playlists-${item.image}-${index}`),
            )}
          </div>
        )}
        {openSections.playlists && profile.owned_playlists.length > PLAYLISTS_PAGE_SIZE
          ? renderPagingWithPageSize("playlists", profile.owned_playlists.length, PLAYLISTS_PAGE_SIZE)
          : null}
        <button className="section-toggle section-toggle-footer" onClick={() => toggleSection("playlists", "playlists")} type="button">
          <span>{openSections.playlists ? "^" : "v"}</span>
        </button>
      </section>
    );
  }

  function handleCooldownRetry() {
    setReloadCooldownUntil(null);
    setReloadCooldownDurationMs(60_000);
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

  async function fetchRecentSections(targetRange: RecentRange): Promise<RecentSectionResponse> {
    const endpoint = experienceMode === "local" ? "/me/local/recent" : "/me/recent";
    const response = await fetch(
      `${apiBaseUrl}${endpoint}?recent_range=${encodeURIComponent(targetRange)}&limit=${encodeURIComponent(String(RECENT_SECTION_FETCH_LIMIT))}`,
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
        setReloadCooldownDurationMs(cooldownSeconds * 1000);
        setReloadCooldownUntil(Date.now() + cooldownSeconds * 1000);
        detail = formatCooldownCopy(cooldownSeconds);
      }
      throw new Error(detail);
    }
    return (await response.json()) as RecentSectionResponse;
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
      setPlayerRecentTracksLoading(false);
    }
  }

  async function loadPlayerQueueTracks() {
    if (experienceMode === "local") {
      setPlayerQueueTracks([]);
      setPlayerQueueSource(null);
      setQueuePlaylistUri(null);
      setPlayerQueueError(null);
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
      const payload = (await response.json()) as {
        queue?: Array<{
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
        }>;
      };
      const liveQueueTracks = (payload.queue ?? [])
        .filter((item) => item.type === "track" || item.uri?.startsWith("spotify:track:"))
        .map((item) => ({
          name: item.name ?? "Unknown track",
          artists: (item.artists ?? []).map((artist) => artist.name ?? "").filter(Boolean).join(", ") || "Unknown artist",
          album: item.album?.name ?? "Unknown album",
          image: item.album?.images?.find((image) => image.url)?.url ?? null,
          uri: item.uri ?? (item.id ? `spotify:track:${item.id}` : null),
          durationMs: Math.max(0, Number(item.duration_ms ?? 0)),
          trackId: item.id ?? spotifyTrackIdFromUri(item.uri ?? null),
          albumId: item.album?.id ?? null,
        }))
        .slice(0, PLAYER_RECENT_FETCH_LIMIT);
      setPlayerQueueTracks(queueRepeatsTrack(liveQueueTracks, currentTrack?.uri) ? [] : liveQueueTracks);
      setPlayerQueueSource("spotify");
    } catch (error) {
      setPlayerQueueTracks([]);
      setPlayerQueueSource(null);
      setPlayerQueueError(formatUiErrorMessage(error, "Failed to load Spotify queue."));
    } finally {
      setPlayerQueueLoading(false);
    }
  }

  async function fetchListeningLog(
    limit: number,
    offset: number,
    sourceFilter: RecentDebugSourceFilter,
  ): Promise<ListeningLogResponse> {
    const endpoint = "/debug/listening-log";
    const response = await fetch(
      `${apiBaseUrl}${endpoint}?limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}&source_filter=${encodeURIComponent(sourceFilter)}`,
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

  async function loadListeningLogBatch(reset: boolean = false) {
    if (listeningLogLoading) {
      return;
    }
    setListeningLogLoading(true);
    setListeningLogError("");
    try {
      const targetOffset = reset ? 0 : listeningLogOffset;
      const payload = await fetchListeningLog(50, targetOffset, recentDebugSourceFilter);
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

  async function refreshRecentSection(targetRange: RecentRange = recentRange) {
    if (experienceMode === "full" && spotifyCooldownActive) {
      setStatusMessage(formatCooldownCopy(reloadSecondsRemaining));
      setStatusHistory((current) => [...current, "Spotify cooldown active. Recent refresh paused."]);
      return;
    }
    if (loadingRecentSection) {
      return;
    }
    setLoadingRecentSection(true);
    setStatusMessage("Refreshing recent sections...");
    try {
      const data = await fetchRecentSections(targetRange);
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
              recent_likes_tracks: data.recent_likes_tracks,
              recent_likes_available: data.recent_likes_available,
            }
          : current,
      );
      if (targetRange !== recentRange) {
        setRecentRange(targetRange);
      }
      setStatusMessage("");
      setStatusHistory((current) => [...current, "Recent sections refreshed."]);
    } catch (error) {
      const message = formatUiErrorMessage(error, "Failed to refresh recent sections.");
      setStatusMessage(message);
      setStatusHistory((current) => [...current, `Recent refresh error: ${message}`]);
    } finally {
      setLoadingRecentSection(false);
    }
  }

  const showLoadingScreen = (authTransitioning || session?.authenticated || experienceMode === "local") && !profile;
  const heroTitle = "ListenLab";
  const heroCopy =
    "Connect your account and browse the listening, library, and profile details Spotify already makes available to ListenLab.";

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
          <div className="top-bar">
            <div className="top-copy">
              <p className="eyebrow">ListenLab</p>
              <h1>{heroTitle}</h1>
              <p className="lede three-line-clamp">{heroCopy}</p>
            </div>

            <div className="top-side">
              {renderExperienceModeToggle()}
              <button className="primary-button top-login-button" onClick={handleAuthAction} type="button">
                {experienceMode === "local" ? "Open restricted local mode" : "Log in with Spotify"}
              </button>
              {experienceMode === "full" ? (
                <button className="secondary-button top-login-button" onClick={startRecentIngestLogin} type="button">
                  Connect Spotify and ingest recent plays
                </button>
              ) : null}
              {experienceMode === "full" ? (
                <button className="secondary-button top-login-button" onClick={() => void runRecentBeforeProbe()} type="button">
                  Probe recent API before 90 days
                </button>
              ) : null}
              {experienceMode === "full" ? (
                <button className="secondary-button top-login-button" onClick={() => void runRecentBackfillProbe()} type="button">
                  Probe recent API paging (50 x up to 10)
                </button>
              ) : null}
              {recentIngestResult ? (
                <p className="empty-copy">
                  {recentIngestResult.auth_succeeded && recentIngestResult.ingest_succeeded
                    ? `Recent ingest succeeded: ${recentIngestResult.row_count ?? 0} rows (${recentIngestResult.earliest_api_played_at ?? "n/a"} to ${recentIngestResult.latest_api_played_at ?? "n/a"}).`
                    : `Recent ingest failed: ${recentIngestResult.error ?? "unknown error"}`}
                </p>
              ) : null}
              {recentBeforeProbeResult ? (
                <p className="empty-copy">
                  {recentBeforeProbeResult.ok
                    ? `Before-90d probe: ${recentBeforeProbeResult.returned_items ?? 0} rows (${recentBeforeProbeResult.earliest_played_at ?? "n/a"} to ${recentBeforeProbeResult.latest_played_at ?? "n/a"}).`
                    : `Before-90d probe failed: ${recentBeforeProbeResult.detail ?? "unknown error"}`}
                </p>
              ) : null}
              {recentBackfillProbeResult ? (
                <p className="empty-copy">
                  {recentBackfillProbeResult.ok
                    ? `Backfill probe: ${recentBackfillProbeResult.total_items ?? 0} items across ${recentBackfillProbeResult.pages_fetched ?? 0} pages (${recentBackfillProbeResult.earliest_played_at ?? "n/a"} to ${recentBackfillProbeResult.latest_played_at ?? "n/a"}).`
                    : `Backfill probe failed: ${recentBackfillProbeResult.detail ?? "unknown error"}`}
                </p>
              ) : null}
            </div>
          </div>
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
                      setRateLimitMenuOpen(false);
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
                {showRateLimitReload ? (
                  <div className="profile-menu-shell profile-menu-shell-inline" ref={rateLimitMenuRef}>
                    <button
                      aria-expanded={rateLimitMenuOpen}
                      className={`bar-trigger bar-trigger-cooldown${reloadReady ? " bar-trigger-cooldown-ready" : ""}`}
                      onClick={() => {
                        setRateLimitMenuOpen((current) => !current);
                        setBrandMenuOpen(false);
                        setExperimentalMenuOpen(false);
                        setPlayerMenuOpen(false);
                        setProfileMenuOpen(false);
                      }}
                      type="button"
                    >
                      <span className="cooldown-chip-record" aria-hidden="true">
                        <span className="cooldown-chip-record-center" />
                      </span>
                      <span className="cooldown-chip-label">{reloadReady ? "Ready" : "Cooldown"}</span>
                    </button>

                    {rateLimitMenuOpen ? (
                      <section className="profile-card top-profile-card profile-menu-card rate-limit-menu-card">
                        <div className="profile-panel-top">
                          <div>
                            <h2>Spotify cooldown</h2>
                            <p className="empty-copy">
                              {reloadReady
                                ? "Cooldown completed. Spotify sync actions are available again."
                                : `Spotify requests are paused for ${formatCooldownTimerLabel(reloadSecondsRemaining)}.`}
                            </p>
                            <p className="empty-copy">Local sections stay available while cooldown is active.</p>
                          </div>
                        </div>
                        <div className="actions actions-right actions-in-card">
                          <button
                            className="secondary-button"
                            disabled={!reloadReady || loadingProfile || loadingRecentSection || loadingExtendedProfile}
                            onClick={() => {
                              setReloadCooldownUntil(null);
                              setReloadCooldownDurationMs(60_000);
                              setRateLimitMenuOpen(false);
                              void refreshRecentSection(recentRange);
                            }}
                            type="button"
                          >
                            Retry Spotify sync
                          </button>
                        </div>
                      </section>
                    ) : null}
                  </div>
                ) : null}
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
                      setRateLimitMenuOpen(false);
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
                {experienceMode === "full" ? (
                  <button className="jump-link" onClick={() => openAndScrollToSection("recent", "activity")} type="button">
                    Activity
                  </button>
                ) : null}
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
                {hasPremiumPlayback ? (
                <div className="profile-menu-shell profile-menu-shell-inline" ref={playerMenuRef}>
                  <button
                    aria-expanded={playerMenuOpen}
                    className="bar-trigger bar-trigger-player"
                    onClick={() => {
                      setPlayerMenuOpen((current) => !current);
                      setProfileMenuOpen(false);
                      setBrandMenuOpen(false);
                      setExperimentalMenuOpen(false);
                      setRateLimitMenuOpen(false);
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
                    <section className="profile-card top-profile-card profile-menu-card player-menu-card">
                      <div className="player-menu-layout">
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
                                      {track.track_name ?? "Unknown track"}
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
                                      className="player-menu-title-button single-line-ellipsis"
                                      onClick={() => openPlayerTrackDetails()}
                                      type="button"
                                    >
                                      {playerDisplayTrack.name ?? "ListenLab Player"}
                                    </button>
                                  ) : (
                                    <span className="single-line-ellipsis">{playerDisplayTrack?.name ?? "ListenLab Player"}</span>
                                  )}
                                </h2>
                                {playerDisplayTrack?.uri ? (
                                  <a
                                    aria-label="Open in Spotify"
                                    className="player-menu-external"
                                    href={spotifyTrackUrl(playerDisplayTrack.uri) ?? undefined}
                                    rel="noreferrer"
                                    target="_blank"
                                  >
                                    {"\u2197"}
                                  </a>
                                ) : null}
                              </div>
                              {playerDisplayArtistName ? (
                                <button
                                  className="player-menu-meta-button player-menu-line single-line-ellipsis"
                                  onClick={() => openPlayerArtistDetails()}
                                  type="button"
                                >
                                  {playerDisplayArtistName}
                                </button>
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
                                onMouseUp={() => {
                                  if (canControlPlayback && pendingSeekMs != null) {
                                    void seekPlayer(pendingSeekMs);
                                  }
                                }}
                                onTouchEnd={() => {
                                  if (canControlPlayback && pendingSeekMs != null) {
                                    void seekPlayer(pendingSeekMs);
                                  }
                                }}
                                step={1000}
                                title={usingLivePlaybackSnapshot ? livePlaybackControlTooltip : undefined}
                                type="range"
                                value={pendingSeekMs ?? playerDisplayPositionMs}
                              />
                              <div className="player-progress-times">
                                <span>{formatPlaybackClock(pendingSeekMs ?? playerDisplayPositionMs)}</span>
                                <span>{formatPlaybackClock(playerDisplayDurationMs || playerDisplayTrack.durationMs || 0)}</span>
                              </div>
                            </div>
                          ) : null}

                          <div className="actions actions-centered actions-in-card">
                            <span title={livePlaybackControlTooltip}>
                              <button
                                className={`primary-button${liveReadOnlyMode ? " primary-button-readonly" : ""}`}
                                disabled={!playerDisplayTrack || (!playerReady && !usingLivePlaybackSnapshot)}
                                onClick={() => handlePlayerPrimaryButtonClick()}
                                type="button"
                              >
                                {playerDisplayPaused ? "Play" : "Pause"}
                              </button>
                            </span>
                          </div>

                          {usingLivePlaybackSnapshot && liveAwaitingNextTrack ? (
                            <p className="empty-copy">Track ended. Checking for the next song...</p>
                          ) : null}
                          {playerError ? <p className="empty-copy">{playerError}</p> : null}
                          {!usingLivePlaybackSnapshot && !playerReady && !playerError ? <p className="empty-copy">Connecting to Spotify player...</p> : null}
                        </div>

                        <aside className="player-recent-column player-queue-column" aria-label="Spotify queue">
                          <div className="player-recent-header">
                            <h3>Queue</h3>
                            {playerQueueLoading ? <span>Loading</span> : null}
                          </div>
                          <div className="player-recent-list">
                            {playerQueueTracks.map((track, index) => (
                              <button
                                className="player-recent-row"
                                key={`${track.uri ?? track.trackId ?? track.name}-${index}`}
                                onClick={() => openQueuePlayerTrackDetails(track)}
                                type="button"
                              >
                                {track.image ? (
                                  <img alt="" className="player-recent-cover" src={track.image} />
                                ) : (
                                  <span className="player-recent-cover player-recent-cover-fallback" aria-hidden="true">
                                    {track.name.slice(0, 1).toUpperCase()}
                                  </span>
                                )}
                                <span className="player-recent-copy">
                                  <span className="player-recent-track single-line-ellipsis">
                                    {track.name}
                                  </span>
                                  <span className="player-recent-artist single-line-ellipsis">
                                    {track.artists}
                                  </span>
                                </span>
                              </button>
                            ))}
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
                      setRateLimitMenuOpen(false);
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
            {appPage === "formulaLab" ? (
              <div className="dashboard-grid">
                <FormulaLabPage
                  hasProfile={Boolean(profile)}
                  mergedTracks={mergedTracks}
                  mergedTracksLoaded={mergedTracksLoaded}
                  mergedTracksLoading={mergedTracksLoading}
                  mergedTracksError={mergedTracksError}
                  mergedTracksLastLoadedAt={mergedTracksLastLoadedAt}
                  mergedTrackSourceFilter={mergedTrackSourceFilter}
                  rankMovementFilter={rankMovementFilter}
                  trackRankingMode={trackRankingMode}
                  renderMergedTrackSourceFilterToggle={renderMergedTrackSourceFilterToggle}
                  renderTrackRankingToggle={renderTrackRankingToggle}
                  renderRankMovementFilterToggle={renderRankMovementFilterToggle}
                  renderTrackColumn={renderTrackColumn}
                  reloadTrackRankings={reloadTrackRankings}
                  onBack={() => setAppPage("dashboard")}
                />
              </div>
            ) : appPage === "identityAudit" ? (
              <div className="dashboard-grid">
                {renderIdentityAuditPage()}
              </div>
            ) : appPage === "recentDebug" ? (
              <div className="dashboard-grid">
                <RecentDebugPage
                  hasProfile={Boolean(profile)}
                  listeningLogTracks={listeningLogTracks}
                  listeningLogLoading={listeningLogLoading}
                  listeningLogError={listeningLogError}
                  listeningLogOffset={listeningLogOffset}
                  listeningLogHasMore={listeningLogHasMore}
                  listeningLogLastLoadedAt={listeningLogLastLoadedAt}
                  recentDebugSourceFilter={recentDebugSourceFilter}
                  setRecentDebugSourceFilter={setRecentDebugSourceFilter}
                  setListeningLogTracks={setListeningLogTracks}
                  setListeningLogHasMore={setListeningLogHasMore}
                  setListeningLogOffset={setListeningLogOffset}
                  setListeningLogLoaded={setListeningLogLoaded}
                  setListeningLogLastLoadedAt={setListeningLogLastLoadedAt}
                  setListeningLogError={setListeningLogError}
                  showDebugLinkFields={showDebugLinkFields}
                  setShowDebugLinkFields={setShowDebugLinkFields}
                  openDebugSessions={openDebugSessions}
                  setOpenDebugSessions={setOpenDebugSessions}
                  openDebugTracks={openDebugTracks}
                  setOpenDebugTracks={setOpenDebugTracks}
                  loadListeningLogBatch={loadListeningLogBatch}
                  onBack={() => setAppPage("dashboard")}
                  onSelectPreview={setSelectedPreview}
                />
              </div>
            ) : appPage === "catalogBackfill" ? (
              <div className="dashboard-grid">
                <CatalogBackfillPage
                  hasProfile={Boolean(profile)}
                  catalogBackfillTab={catalogBackfillTab}
                  setCatalogBackfillTab={setCatalogBackfillTab}
                  catalogBackfillCoverage={catalogBackfillCoverage}
                  catalogBackfillCoverageLoading={catalogBackfillCoverageLoading}
                  catalogBackfillCoverageError={catalogBackfillCoverageError}
                  catalogBackfillCoverageLastLoadedAt={catalogBackfillCoverageLastLoadedAt}
                  catalogBackfillRuns={catalogBackfillRuns}
                  catalogBackfillRunsLoading={catalogBackfillRunsLoading}
                  catalogBackfillRunsError={catalogBackfillRunsError}
                  catalogBackfillRunsLastLoadedAt={catalogBackfillRunsLastLoadedAt}
                  catalogBackfillQueue={catalogBackfillQueue}
                  catalogBackfillQueueLoading={catalogBackfillQueueLoading}
                  catalogBackfillQueueError={catalogBackfillQueueError}
                  catalogBackfillQueueLastLoadedAt={catalogBackfillQueueLastLoadedAt}
                  catalogBackfillQueueStatusFilter={catalogBackfillQueueStatusFilter}
                  catalogBackfillQueueReasonFilter={catalogBackfillQueueReasonFilter}
                  catalogBackfillQueueRepairLoading={catalogBackfillQueueRepairLoading}
                  catalogBackfillQueueRepairMessage={catalogBackfillQueueRepairMessage}
                  catalogBackfillLatestResult={catalogBackfillLatestResult}
                  catalogBackfillRunLoading={catalogBackfillRunLoading}
                  catalogBackfillRunError={catalogBackfillRunError}
                  catalogBackfillLimit={catalogBackfillLimit}
                  setCatalogBackfillLimit={setCatalogBackfillLimit}
                  catalogBackfillOffset={catalogBackfillOffset}
                  setCatalogBackfillOffset={setCatalogBackfillOffset}
                  catalogBackfillMarket={catalogBackfillMarket}
                  setCatalogBackfillMarket={setCatalogBackfillMarket}
                  catalogBackfillForceRefresh={catalogBackfillForceRefresh}
                  setCatalogBackfillForceRefresh={setCatalogBackfillForceRefresh}
                  catalogBackfillMaxRequests={catalogBackfillMaxRequests}
                  setCatalogBackfillMaxRequests={setCatalogBackfillMaxRequests}
                  catalogBackfillMaxRuntimeSeconds={catalogBackfillMaxRuntimeSeconds}
                  setCatalogBackfillMaxRuntimeSeconds={setCatalogBackfillMaxRuntimeSeconds}
                  catalogBackfillFullRunMode={catalogBackfillFullRunMode}
                  setCatalogBackfillFullRunMode={setCatalogBackfillFullRunMode}
                  catalogBackfillAlbumTracklistPolicy={catalogBackfillAlbumTracklistPolicy}
                  setCatalogBackfillAlbumTracklistPolicy={setCatalogBackfillAlbumTracklistPolicy}
                  catalogBackfillMaxAlbumTracksPagesPerAlbum={catalogBackfillMaxAlbumTracksPagesPerAlbum}
                  setCatalogBackfillMaxAlbumTracksPagesPerAlbum={setCatalogBackfillMaxAlbumTracksPagesPerAlbum}
                  catalogBackfillIncludeAlbums={catalogBackfillIncludeAlbums}
                  setCatalogBackfillIncludeAlbums={setCatalogBackfillIncludeAlbums}
                  loadCatalogBackfillCoverage={loadCatalogBackfillCoverage}
                  loadCatalogBackfillRuns={loadCatalogBackfillRuns}
                  loadCatalogBackfillQueue={loadCatalogBackfillQueue}
                  repairCatalogBackfillQueueStatuses={repairCatalogBackfillQueueStatuses}
                  runCatalogBackfill={runCatalogBackfill}
                  onBack={() => setAppPage("dashboard")}
                />
              </div>
            ) : appPage === "searchLookup" ? (
              <div className="dashboard-grid">
                <SearchLookupPage
                  hasProfile={Boolean(profile)}
                  searchLookupEntityType={searchLookupEntityType}
                  setSearchLookupEntityType={setSearchLookupEntityType}
                  albumCatalogLookupQ={albumCatalogLookupQ}
                  setAlbumCatalogLookupQ={setAlbumCatalogLookupQ}
                  albumCatalogLookupStatus={albumCatalogLookupStatus}
                  setAlbumCatalogLookupStatus={setAlbumCatalogLookupStatus}
                  trackCatalogLookupStatus={trackCatalogLookupStatus}
                  setTrackCatalogLookupStatus={setTrackCatalogLookupStatus}
                  searchLookupQueueStatus={searchLookupQueueStatus}
                  setSearchLookupQueueStatus={setSearchLookupQueueStatus}
                  searchLookupSort={searchLookupSort}
                  setSearchLookupSort={setSearchLookupSort}
                  albumCatalogLookupResult={albumCatalogLookupResult}
                  albumCatalogLookupLoading={albumCatalogLookupLoading}
                  albumCatalogLookupError={albumCatalogLookupError}
                  albumCatalogLookupLastLoadedAt={albumCatalogLookupLastLoadedAt}
                  trackCatalogLookupResult={trackCatalogLookupResult}
                  trackCatalogLookupLoading={trackCatalogLookupLoading}
                  trackCatalogLookupError={trackCatalogLookupError}
                  trackCatalogLookupLastLoadedAt={trackCatalogLookupLastLoadedAt}
                  albumCatalogLookupEnqueueLoading={albumCatalogLookupEnqueueLoading}
                  albumCatalogLookupEnqueueError={albumCatalogLookupEnqueueError}
                  setAlbumCatalogLookupEnqueueError={setAlbumCatalogLookupEnqueueError}
                  albumCatalogLookupEnqueueResult={albumCatalogLookupEnqueueResult}
                  setAlbumCatalogLookupEnqueueResult={setAlbumCatalogLookupEnqueueResult}
                  loadActiveSearchLookup={loadActiveSearchLookup}
                  enqueueVisibleIncompleteLookupAlbums={enqueueVisibleIncompleteLookupAlbums}
                  enqueueVisibleIncompleteLookupTracks={enqueueVisibleIncompleteLookupTracks}
                  openAlbumLookupPreview={openAlbumLookupPreview}
                  openTrackLookupPreview={openTrackLookupPreview}
                  onBack={() => setAppPage("dashboard")}
                />
              </div>
            ) : (
            <div className="dashboard-grid">
              {renderDualSectionCard({
                title: renderSectionTitle("Activity", "recent_likes"),
                section: "recent",
                anchorId: "activity",
                leftTitle: (
                  <div className="section-column-header">
                    <h3>Recently played</h3>
                  </div>
                ),
                rightTitle: renderSectionTitle("Recently liked", "recent_likes"),
                leftContent: renderTrackColumn(
                  "recent",
                  profile.recent_tracks,
                  profile.recent_tracks_available,
                  "Spotify returned no recent listening history.",
                  recentUnavailableCopy(
                    "Recent listening is not available for this session yet. Log out and log back in to grant the updated Spotify permissions.",
                  ),
                  analysisMode === "quick" && experienceMode === "full" ? (
                    <button
                      className="secondary-button inline-reload-button"
                      disabled={loadingRecentSection}
                      onClick={() => void refreshRecentSection()}
                      type="button"
                    >
                      {loadingRecentSection ? "Refreshing..." : "Reload this section"}
                    </button>
                  ) : null,
                ),
                rightContent: renderTrackColumn(
                  "likes",
                  profile.recent_likes_tracks,
                  profile.recent_likes_available,
                  "Spotify returned no recently liked tracks.",
                  recentUnavailableCopy(
                    "Liked tracks are not available for this session yet. Log out and log back in to grant library access.",
                  ),
                  analysisMode === "quick" && experienceMode === "full" ? (
                    <button
                      className="secondary-button inline-reload-button"
                      disabled={loadingRecentSection}
                      onClick={() => void refreshRecentSection()}
                      type="button"
                    >
                      {loadingRecentSection ? "Refreshing..." : "Reload this section"}
                    </button>
                  ) : null,
                ),
                previewItemsLeft: previewItems(profile.recent_tracks),
                previewItemsRight: previewItems(profile.recent_likes_tracks),
                collapsedPreviewItems: previewItems(collapseRecentPreviewTracks(profile.recent_tracks)),
              })}

              {renderDualSectionCard({
                title: renderSectionTitle("Tracks"),
                section: "tracks",
                anchorId: "tracks",
                leftTitle: (
                  <div className="section-column-header">
                    <h3>All time</h3>
                    <div className="section-column-header-actions">
                      {renderTrackRankingToggle()}
                    </div>
                  </div>
                ),
                rightTitle: renderRecentRangeHeader(),
                leftContent: renderTrackColumn(
                  "tracksAllTime",
                  profile.top_tracks,
                  profile.top_tracks_available,
                  "Spotify returned no top tracks for this account.",
                  quickUnavailableCopy("Top tracks are not available for this session yet. Log out and log back in to grant access."),
                ),
                rightContent: renderTrackColumn(
                  "tracksRecent",
                  profile.recent_top_tracks,
                  profile.recent_top_tracks_available,
                  "Spotify returned no recent top tracks for this account.",
                  recentUnavailableCopy(
                    experienceMode === "local"
                      ? "Recent top tracks are unavailable in restricted local mode."
                      : "Recent top tracks are not available for this session yet. Log out and log back in to grant access.",
                  ),
                  analysisMode === "quick" && experienceMode === "full" ? (
                    <button
                      className="secondary-button inline-reload-button"
                      disabled={loadingRecentSection}
                      onClick={() => void refreshRecentSection()}
                      type="button"
                    >
                      {loadingRecentSection ? "Refreshing..." : "Reload this section"}
                    </button>
                  ) : null,
                ),
                previewItemsLeft: previewItems(profile.top_tracks),
                previewItemsRight: previewItems(profile.recent_top_tracks),
                collapsedPreviewItems: previewItems(
                  collapseTrackPreviewAlbums([
                    ...profile.top_tracks,
                    ...profile.recent_top_tracks,
                  ]),
                ),
              })}

              {renderDualSectionCard({
                title: renderSectionTitle("Artists"),
                section: "artists",
                anchorId: "artists",
                leftTitle: "All time",
                rightTitle: renderRecentRangeHeader(),
                leftContent: renderArtistColumn(
                  "artistsAllTime",
                  profile.followed_artists,
                  profile.followed_artists_list_available,
                  "Spotify returned no top artists for this account.",
                  quickUnavailableCopy("Top artists are not available for this session yet. Log out and log back in to grant access."),
                ),
                rightContent: renderArtistColumn(
                  "artistsRecent",
                  profile.recent_top_artists,
                  profile.recent_top_artists_available,
                  "Spotify returned no recent top artists for this account.",
                  recentUnavailableCopy(
                    experienceMode === "local"
                      ? "Recent top artists are unavailable in restricted local mode."
                      : "Recent top artists are not available for this session yet. Log out and log back in to grant access.",
                  ),
                  analysisMode === "quick" && experienceMode === "full" ? (
                    <button
                      className="secondary-button inline-reload-button"
                      disabled={loadingRecentSection}
                      onClick={() => void refreshRecentSection()}
                      type="button"
                    >
                      {loadingRecentSection ? "Refreshing..." : "Reload this section"}
                    </button>
                  ) : null,
                ),
                previewItemsLeft: previewItems(profile.followed_artists),
                previewItemsRight: previewItems(profile.recent_top_artists),
              })}

              {renderDualSectionCard({
                title: renderSectionTitle("Albums"),
                section: "albums",
                anchorId: "albums",
                leftTitle: "All time",
                rightTitle: renderRecentRangeHeader(),
                leftContent: renderAlbumColumn(
                  "albumsAllTime",
                  profile.top_albums,
                  profile.top_albums_available,
                  "Spotify returned no top albums for this account.",
                  quickUnavailableCopy("Top albums are not available for this session yet. Log out and log back in to grant access."),
                ),
                rightContent: renderAlbumColumn(
                  "albumsRecent",
                  profile.recent_top_albums,
                  profile.recent_top_albums_available,
                  "Spotify returned no recent top albums for this account.",
                  recentUnavailableCopy(
                    experienceMode === "local"
                      ? "Recent top albums are unavailable in restricted local mode."
                      : "Recent top albums are not available for this session yet. Log out and log back in to grant access.",
                  ),
                  analysisMode === "quick" && experienceMode === "full" ? (
                    <button
                      className="secondary-button inline-reload-button"
                      disabled={loadingRecentSection}
                      onClick={() => void refreshRecentSection()}
                      type="button"
                    >
                      {loadingRecentSection ? "Refreshing..." : "Reload this section"}
                    </button>
                  ) : null,
                ),
                previewItemsLeft: previewItems(profile.top_albums),
                previewItemsRight: previewItems(profile.recent_top_albums),
              })}

              {renderPlaylistsSection()}
            </div>
            )}
          </>
        )}
        </section>
      </main>
      {selectedPreview ? (
        <div
          aria-modal="true"
          className="detail-modal-backdrop"
          onClick={() => setSelectedPreview(null)}
          role="dialog"
        >
          <section className="detail-modal" onClick={(event) => event.stopPropagation()}>
            {selectedPreview.url ? (
              <a
                aria-label="Open in Spotify"
                className="detail-modal-spotify-corner"
                href={selectedPreview.url}
                rel="noreferrer"
                target="_blank"
              >
                <img alt="" src={spotifyLogoDataUrl} />
              </a>
            ) : null}
            <div className="detail-modal-left">
              {selectedPreview.image ? (
                <img alt={selectedPreview.label} className="detail-modal-image" src={selectedPreview.image} />
              ) : (
                <div className="detail-modal-image detail-modal-image-fallback" aria-hidden="true">
                  {selectedPreview.fallbackLabel ?? selectedPreview.label.slice(0, 1).toUpperCase()}
                </div>
              )}
            </div>
            <div className="detail-modal-copy">
              <h2>{selectedPreview.label}</h2>
              {selectedPreview.meta ? (
                <div className="detail-modal-meta detail-modal-meta-with-image">
                  {selectedPreview.kind === "track" && selectedPreviewArtistImageUrl ? (
                    <img
                      alt=""
                      className="detail-modal-artist-image"
                      src={selectedPreviewArtistImageUrl}
                    />
                  ) : null}
                  {selectedPreviewCanOpenArtist ? (
                    <button
                      className="detail-modal-inline-link detail-modal-meta-text"
                      onClick={openSelectedTrackArtistPreview}
                      type="button"
                    >
                      {selectedPreviewPrimaryArtistName}
                    </button>
                  ) : (
                    <span className="detail-modal-meta-text">{selectedPreview.meta}</span>
                  )}
                </div>
              ) : null}
              {selectedPreview.detail && selectedPreview.kind !== "track" ? <p className="detail-modal-detail">{selectedPreview.detail}</p> : null}
              {selectedPreview.kind === "track" && !selectedPreviewEffectiveTrackUri ? (
                <p className="detail-modal-preview-missing">This track does not have a playable Spotify URI.</p>
              ) : null}
              {hasPremiumPlayback && (selectedPreview.kind === "artist" || selectedPreview.kind === "album") && representativeLoading ? (
                <p className="detail-modal-preview-missing">Finding a song to play...</p>
              ) : null}
              {hasPremiumPlayback && (selectedPreview.kind === "artist" || selectedPreview.kind === "album") && representativeTrack ? (
                <div className="detail-modal-audio">
                  <p className="detail-modal-audio-label">
                    {selectedPreview.kind === "artist" ? "Top song" : "Representative song"}
                  </p>
                  <p className="detail-modal-related-track">
                    {representativeTrack.track_name ?? "Unknown track"}
                    {representativeTrack.artist_name ? ` - ${representativeTrack.artist_name}` : ""}
                  </p>
                  {!representativeTrack.uri ? (
                    <p className="detail-modal-preview-missing">
                      {representativeReasonMessage(representativeReason, selectedPreview.kind)}
                    </p>
                  ) : null}
                </div>
              ) : null}
              {hasPremiumPlayback && (selectedPreview.kind === "artist" || selectedPreview.kind === "album") && !representativeLoading && !representativeTrack ? (
                <p className="detail-modal-preview-missing">
                  {representativeReasonMessage(representativeReason, selectedPreview.kind)}
                </p>
              ) : null}
              <div className="actions actions-in-card detail-modal-actions">
                {hasPremiumPlayback && selectedPreview.kind === "track" && selectedPreviewEffectiveTrackUri ? (
                  <div className={`detail-top-play-control${overlayTrackPlaybackExpanded ? " detail-top-play-control-expanded" : ""}`}>
                    <button
                      aria-label={isTrackPlaying(selectedPreviewEffectiveTrackUri) ? "Pause in ListenLab" : "Play in ListenLab"}
                      className={`secondary-button detail-icon-button detail-top-play-toggle${isTrackPlaying(selectedPreviewEffectiveTrackUri) ? " detail-icon-button-playing" : ""}`}
                      onClick={() => handleSelectedPreviewTrackPlay(selectedPreviewEffectiveTrackUri)}
                      type="button"
                    >
                      <span className={`detail-top-play-glyph${isTrackPlaying(selectedPreviewEffectiveTrackUri) ? " detail-top-play-glyph-active" : ""}`} aria-hidden="true">
                        {isTrackPlaying(selectedPreviewEffectiveTrackUri) ? (
                          <span className="detail-pause-bars"><span /><span /></span>
                        ) : (
                          <span className="detail-play-icon">{"\u25B6"}</span>
                        )}
                      </span>
                    </button>
                    {overlayTrackPlaybackExpanded ? (
                      <>
                        <span className="detail-top-play-time detail-top-play-time-elapsed">
                          {formatPlaybackClock(overlaySeekMs ?? selectedPreviewTrackElapsedDisplayMs)}
                        </span>
                        <span className="detail-top-play-inline-progress">
                          <span className="detail-top-play-inline-progress-fill" style={{ width: `${selectedPreviewTrackProgressPercent}%` }} />
                          <span className={`detail-top-play-inline-wave${isTrackPlaying(selectedPreviewEffectiveTrackUri) ? " detail-top-play-inline-wave-active" : ""}`} />
                          <input
                            aria-label="Seek selected track"
                            className="detail-top-play-inline-slider"
                            disabled={!canSeekSelectedPreview}
                            max={Math.max(selectedPreviewTrackTotalDisplayMs, 1)}
                            min={0}
                            onChange={(event) => setOverlaySeekMs(Number(event.currentTarget.value))}
                            onMouseUp={() => {
                              if (canSeekSelectedPreview && overlaySeekMs != null) {
                                void seekPlayer(overlaySeekMs);
                              }
                            }}
                            onTouchEnd={() => {
                              if (canSeekSelectedPreview && overlaySeekMs != null) {
                                void seekPlayer(overlaySeekMs);
                              }
                            }}
                            step={1000}
                            type="range"
                            value={overlaySeekMs ?? selectedPreviewTrackElapsedDisplayMs}
                          />
                        </span>
                      </>
                    ) : null}
                    <span className="detail-top-play-time detail-top-play-time-total">
                      {formatPlaybackClock(selectedPreviewTrackTotalDisplayMs)}
                    </span>
                  </div>
                ) : null}
                {hasPremiumPlayback && (selectedPreview.kind === "artist" || selectedPreview.kind === "album") && representativeTrack?.uri ? (
                  <button
                    aria-label={isTrackPlaying(representativeTrack.uri ?? null) ? "Currently playing in ListenLab" : "Play in ListenLab"}
                    className={`secondary-button detail-icon-button${isTrackPlaying(representativeTrack.uri ?? null) ? " detail-icon-button-playing" : ""}`}
                    onClick={() => void handlePopupTrackPlayback(representativeTrack.uri ?? null)}
                    type="button"
                  >
                    {isTrackPlaying(representativeTrack.uri ?? null) ? (
                      <span className="detail-pause-bars" aria-hidden="true"><span /><span /></span>
                    ) : (
                      <span className="detail-play-icon" aria-hidden="true">{"\u25B6"}</span>
                    )}
                  </button>
                ) : null}
              </div>
            </div>
            {selectedPreview.kind === "track" || selectedPreview.kind === "album" ? (
              <div className="detail-modal-album-tracks detail-modal-album-tracks-full">
                {selectedPreview.kind === "track" && selectedPreviewCanOpenAlbum ? (
                  <button
                    className="detail-modal-inline-link detail-modal-album-title"
                    onClick={openSelectedTrackAlbumPreview}
                    type="button"
                  >
                    {previewAlbumHeading(selectedPreview)}
                  </button>
                ) : (
                  <p className="detail-modal-album-title">
                    {selectedPreview.kind === "album" ? selectedPreview.label : previewAlbumHeading(selectedPreview)}
                  </p>
                )}
                <div className="detail-modal-album-header" aria-hidden="true">
                  <span className="detail-modal-album-preview-header">Preview</span>
                  <span className="detail-modal-album-last-played-header">Played</span>
                </div>
                {albumTrackEntriesLoading ? (
                  <p className="detail-modal-preview-missing">Loading album songs...</p>
                ) : null}
                {!albumTrackEntriesLoading && albumTrackEntriesError ? (
                  <p className="detail-modal-preview-missing">{albumTrackEntriesError}</p>
                ) : null}
                {!albumTrackEntriesLoading && !albumTrackEntriesError && albumTrackEntries.length > 0 ? (
                  <ul className="detail-album-track-list">
                    {albumTrackEntries.map((track) => {
                      const rowTrackUri = track.uri ?? (track.id ? `spotify:track:${track.id}` : null);
                      const rowIsCurrentTrack = Boolean(rowTrackUri && currentTrack?.uri === rowTrackUri);
                      const rowPlaying = isTrackPlaying(rowTrackUri);
                      const rowPreviewPlaying = Boolean(rowTrackUri && previewingTrackUri === rowTrackUri);
                      const rowPreviewActive = Boolean(rowPreviewPlaying && rowPlaying);
                      const rowPreviewKey = albumTrackPreviewKey(track, rowTrackUri);
                      const rowPreviewPlayed = previewPlayedTrackKeys.has(rowPreviewKey);
                      const rowPausedCurrent = Boolean(rowIsCurrentTrack && playbackPaused);
                      const rowLastPlayed = formatMonthDay(track.lastPlayedAt);
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
                            <button
                              aria-label={rowPlaying ? "Currently playing in ListenLab" : rowTrackUri ? `Play ${track.name} in ListenLab` : `${track.name} is not playable`}
                              className={`secondary-button detail-album-track-play-button${rowPlaying ? " detail-icon-button-playing" : ""}`}
                              disabled={!rowTrackUri}
                              onClick={() => {
                                void handleAlbumTrackPlay(track, rowTrackUri);
                              }}
                              type="button"
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
                            </button>
                          ) : null}
                          <button
                            className="detail-album-track-name-button single-line-ellipsis"
                            onClick={() => openAlbumTrackPreview(track)}
                            type="button"
                          >
                            {track.name}
                          </button>
                          <div className="detail-album-track-actions">
                            {hasPremiumPlayback ? (
                              <button
                                aria-label={rowPreviewPlaying ? `Stop preview for ${track.name}` : `Preview ${track.name}`}
                                className={`detail-album-track-preview-button${rowPreviewActive ? " detail-album-track-preview-button-active" : ""}${rowPreviewPlayed ? " detail-album-track-preview-button-played" : ""}`}
                                disabled={!rowTrackUri}
                                onClick={() => {
                                  void toggleAlbumTrackPreview(track, rowTrackUri);
                                }}
                                type="button"
                              />
                            ) : (
                              <span className="detail-album-track-preview-placeholder" aria-hidden="true" />
                            )}
                            {rowLastPlayed ? <span className="detail-album-track-last-played">{rowLastPlayed}</span> : <span className="detail-album-track-last-played">-</span>}
                          </div>
                        </li>
                      );
                    })}
                  </ul>
                ) : null}
              </div>
            ) : null}
          </section>
        </div>
      ) : null}
    </>
  );
}
