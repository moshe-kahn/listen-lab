import { lazy, Suspense } from "react";

import type {
  AnalysisMode,
  AppPage,
  ExperienceMode,
  ProfileResponse,
  RankMovementFilter,
  RecentCompletionFilter,
  RecentRange,
  TrackRankingMode,
} from "../../types/appTypes";
import { LIKED_TRACKS_RECENT_DISPLAY_LIMIT } from "../../constants/appConstants";
import { DashboardAlbumColumn, DashboardArtistColumn } from "./DashboardColumns";
import { DashboardPlaylistsSection } from "./DashboardPlaylistsSection";
import { DualSectionCard } from "./DualSectionCard";

const CatalogBackfillPage = lazy(() => import("../catalogBackfill/CatalogBackfillPage").then((module) => ({
  default: module.CatalogBackfillPage,
})));
const FormulaLabPage = lazy(() => import("../formulaLab/FormulaLabPage").then((module) => ({
  default: module.FormulaLabPage,
})));
const RecentDebugPage = lazy(() => import("../recentDebug/RecentDebugPage").then((module) => ({
  default: module.RecentDebugPage,
})));
const SearchLookupPage = lazy(() => import("../searchLookup/SearchLookupPage").then((module) => ({
  default: module.SearchLookupPage,
})));

function RouteChunkFallback() {
  return (
    <section className="info-card info-card-wide tracks-only-card">
      <p className="empty-copy">Loading page...</p>
    </section>
  );
}

type DashboardSectionsProps = {
  activityRecentTracks: any;
  activityPreviewTracks: any;
  albumCatalogLookupEnqueueError: any;
  albumCatalogLookupEnqueueLoading: any;
  albumCatalogLookupEnqueueResult: any;
  albumCatalogLookupError: any;
  albumCatalogLookupLastLoadedAt: any;
  albumCatalogLookupLoading: any;
  albumCatalogLookupQ: any;
  albumCatalogLookupResult: any;
  albumCatalogLookupStatus: any;
  allTimeLikedMatchCount: any;
  allTimeTopTracks: any;
  allTimeTopTracksAvailableForDisplay: boolean;
  allTimeTrackIdCount: any;
  analysisMode: AnalysisMode;
  appPage: AppPage;
  activePlaylistPlayback: any;
  cachedLikedTracks: any;
  catalogBackfillAlbumTracklistPolicy: any;
  catalogBackfillCoverage: any;
  catalogBackfillCoverageError: any;
  catalogBackfillCoverageLastLoadedAt: any;
  catalogBackfillCoverageLoading: any;
  catalogBackfillForceRefresh: any;
  catalogBackfillFullRunMode: any;
  catalogBackfillIncludeAlbums: any;
  catalogBackfillLatestResult: any;
  catalogBackfillLimit: any;
  catalogBackfillMarket: any;
  catalogBackfillMaxAlbumTracksPagesPerAlbum: any;
  catalogBackfillMaxRequests: any;
  catalogBackfillMaxRuntimeSeconds: any;
  catalogBackfillOffset: any;
  catalogBackfillQueue: any;
  catalogBackfillQueueError: any;
  catalogBackfillQueueLastLoadedAt: any;
  catalogBackfillQueueLoading: any;
  catalogBackfillQueueReasonFilter: any;
  catalogBackfillQueueRepairLoading: any;
  catalogBackfillQueueRepairMessage: any;
  catalogBackfillQueueStatusFilter: any;
  catalogBackfillRunError: any;
  catalogBackfillRunLoading: any;
  catalogBackfillRuns: any;
  catalogBackfillRunsError: any;
  catalogBackfillRunsLastLoadedAt: any;
  catalogBackfillRunsLoading: any;
  catalogBackfillTab: any;
  collapseRecentPreviewTracks: any;
  collapseTrackPreviewAlbums: any;
  enqueueVisibleIncompleteLookupAlbums: any;
  enqueueVisibleIncompleteLookupTracks: any;
  experienceMode: ExperienceMode;
  apiBaseUrl: string;
  likedTracksAvailableForActivity: any;
  likedTracksCacheStatus: any;
  likedTracksCountMode: any;
  likedTracksError: any;
  likedTracksForActivity: any;
  likedTracksForActivitySource: any;
  likedTracksLoading: any;
  likedTracksShuffleEnabled: any;
  likedTracksSortMode: any;
  likedTracksSyncing: any;
  likedTracksTotalLabel: any;
  listeningLogError: any;
  listeningLogHasMore: any;
  listeningLogLastLoadedAt: any;
  listeningLogLoading: any;
  listeningLogOffset: any;
  listeningLogTracks: any;
  loadActiveSearchLookup: any;
  loadCatalogBackfillCoverage: any;
  loadCatalogBackfillQueue: any;
  loadCatalogBackfillRuns: any;
  loadListeningLogBatch: any;
  loadingRecentSection: any;
  mergedTrackSourceFilter: any;
  mergedTracks: any;
  mergedTracksError: any;
  mergedTracksLastLoadedAt: any;
  mergedTracksLoaded: any;
  mergedTracksLoading: any;
  moveSectionPage: any;
  openAlbumLookupPreview: any;
  openDebugSessions: any;
  openDebugTracks: any;
  openListeningLogPage: any;
  openSections: any;
  openTrackLookupPreview: any;
  previewItems: any;
  profile: ProfileResponse;
  quickUnavailableCopy: any;
  rankMovementFilter: RankMovementFilter;
  recentDebugSourceFilter: any;
  recentCompletionFilter: RecentCompletionFilter;
  recentLikedOnly: boolean;
  recentTaggedOnly: boolean;
  recentRange: RecentRange;
  recentTopTracksAvailableForDisplay: any;
  recentTopTracksForDisplay: any;
  recentUnavailableCopy: any;
  refreshRecentSection: any;
  reloadTrackRankings: any;
  renderHomePlayerPanel: any;
  renderSavedPanel: any;
  renderLibraryPanel: any;
  playlistOverlayOpen: boolean;
  playlistOverlayOptions: any;
  renderIdentityAuditPage: any;
  renderMergedTrackSourceFilterToggle: any;
  renderRankMovementFilterToggle: any;
  renderRecentRangeHeader: any;
  renderSectionTitle: any;
  renderTrackColumn: any;
  renderTrackRankingToggle: any;
  repairCatalogBackfillQueueStatuses: any;
  runCatalogBackfill: any;
  searchLookupEntityType: any;
  searchLookupQueueStatus: any;
  searchLookupSort: any;
  sectionPages: any;
  setAlbumCatalogLookupEnqueueError: any;
  setAlbumCatalogLookupEnqueueResult: any;
  setAlbumCatalogLookupQ: any;
  setAlbumCatalogLookupStatus: any;
  setAppPage: any;
  setCatalogBackfillAlbumTracklistPolicy: any;
  setCatalogBackfillForceRefresh: any;
  setCatalogBackfillFullRunMode: any;
  setCatalogBackfillIncludeAlbums: any;
  setCatalogBackfillLimit: any;
  setCatalogBackfillMarket: any;
  setCatalogBackfillMaxAlbumTracksPagesPerAlbum: any;
  setCatalogBackfillMaxRequests: any;
  setCatalogBackfillMaxRuntimeSeconds: any;
  setCatalogBackfillOffset: any;
  setCatalogBackfillTab: any;
  setLikedTracksCountMode: any;
  setLikedTracksShuffleEnabled: any;
  setLikedTracksShuffleNonce: any;
  setLikedTracksSortMode: any;
  setPlaylistOverlayOpen: any;
  setPlaylistOverlayOptions: any;
  setListeningLogError: any;
  setListeningLogHasMore: any;
  setListeningLogLastLoadedAt: any;
  setListeningLogLoaded: any;
  setListeningLogOffset: any;
  setListeningLogTracks: any;
  setOpenDebugSessions: any;
  setOpenDebugTracks: any;
  setRecentDebugSourceFilter: any;
  setRecentCompletionFilter: any;
  setRecentLikedOnly: any;
  setRecentTaggedOnly: any;
  setSearchLookupEntityType: any;
  setSearchLookupQueueStatus: any;
  setSearchLookupSort: any;
  setSelectedPreview: any;
  hidePlaylistFromListenLab: any;
  unhidePlaylistInListenLab: any;
  deletePlaylistFromSpotify: any;
  setShowDebugLinkFields: any;
  setTrackCatalogLookupStatus: any;
  showDebugLinkFields: any;
  spotifyCooldownActive: any;
  syncLikedTracks: any;
  toggleSection: any;
  trackCatalogLookupError: any;
  trackCatalogLookupLastLoadedAt: any;
  trackCatalogLookupLoading: any;
  trackCatalogLookupResult: any;
  trackCatalogLookupStatus: any;
  trackRankingMode: TrackRankingMode;
  usingLikedTracksFallback: any;
  visibleItemsWithPageSize: any;
};

export function DashboardSections(props: DashboardSectionsProps) {
  const {
    albumCatalogLookupEnqueueError,
    albumCatalogLookupEnqueueLoading,
    albumCatalogLookupEnqueueResult,
    albumCatalogLookupError,
    albumCatalogLookupLastLoadedAt,
    albumCatalogLookupLoading,
    albumCatalogLookupQ,
    albumCatalogLookupResult,
    albumCatalogLookupStatus,
    allTimeLikedMatchCount,
    allTimeTopTracks,
    allTimeTopTracksAvailableForDisplay,
    allTimeTrackIdCount,
    analysisMode,
    appPage,
    activePlaylistPlayback,
    activityRecentTracks,
    activityPreviewTracks,
    cachedLikedTracks,
    catalogBackfillAlbumTracklistPolicy,
    catalogBackfillCoverage,
    catalogBackfillCoverageError,
    catalogBackfillCoverageLastLoadedAt,
    catalogBackfillCoverageLoading,
    catalogBackfillForceRefresh,
    catalogBackfillFullRunMode,
    catalogBackfillIncludeAlbums,
    catalogBackfillLatestResult,
    catalogBackfillLimit,
    catalogBackfillMarket,
    catalogBackfillMaxAlbumTracksPagesPerAlbum,
    catalogBackfillMaxRequests,
    catalogBackfillMaxRuntimeSeconds,
    catalogBackfillOffset,
    catalogBackfillQueue,
    catalogBackfillQueueError,
    catalogBackfillQueueLastLoadedAt,
    catalogBackfillQueueLoading,
    catalogBackfillQueueReasonFilter,
    catalogBackfillQueueRepairLoading,
    catalogBackfillQueueRepairMessage,
    catalogBackfillQueueStatusFilter,
    catalogBackfillRunError,
    catalogBackfillRunLoading,
    catalogBackfillRuns,
    catalogBackfillRunsError,
    catalogBackfillRunsLastLoadedAt,
    catalogBackfillRunsLoading,
    catalogBackfillTab,
    collapseRecentPreviewTracks,
    collapseTrackPreviewAlbums,
    enqueueVisibleIncompleteLookupAlbums,
    enqueueVisibleIncompleteLookupTracks,
    experienceMode,
    apiBaseUrl,
    likedTracksAvailableForActivity,
    likedTracksCacheStatus,
    likedTracksCountMode,
    likedTracksError,
    likedTracksForActivity,
    likedTracksForActivitySource,
    likedTracksLoading,
    likedTracksShuffleEnabled,
    likedTracksSortMode,
    likedTracksSyncing,
    likedTracksTotalLabel,
    listeningLogError,
    listeningLogHasMore,
    listeningLogLastLoadedAt,
    listeningLogLoading,
    listeningLogOffset,
    listeningLogTracks,
    loadActiveSearchLookup,
    loadCatalogBackfillCoverage,
    loadCatalogBackfillQueue,
    loadCatalogBackfillRuns,
    loadListeningLogBatch,
    loadingRecentSection,
    mergedTrackSourceFilter,
    mergedTracks,
    mergedTracksError,
    mergedTracksLastLoadedAt,
    mergedTracksLoaded,
    mergedTracksLoading,
    moveSectionPage,
    openAlbumLookupPreview,
    openDebugSessions,
    openDebugTracks,
    openListeningLogPage,
    openSections,
    openTrackLookupPreview,
    previewItems,
    profile,
    quickUnavailableCopy,
    rankMovementFilter,
    recentDebugSourceFilter,
    recentCompletionFilter,
    recentLikedOnly,
    recentTaggedOnly,
    recentRange,
    recentTopTracksAvailableForDisplay,
    recentTopTracksForDisplay,
    recentUnavailableCopy,
    refreshRecentSection,
    reloadTrackRankings,
    renderHomePlayerPanel,
    renderSavedPanel,
    renderLibraryPanel,
    playlistOverlayOpen,
    playlistOverlayOptions,
    renderIdentityAuditPage,
    renderMergedTrackSourceFilterToggle,
    renderRankMovementFilterToggle,
    renderRecentRangeHeader,
    renderSectionTitle,
    renderTrackColumn,
    renderTrackRankingToggle,
    repairCatalogBackfillQueueStatuses,
    runCatalogBackfill,
    searchLookupEntityType,
    searchLookupQueueStatus,
    searchLookupSort,
    sectionPages,
    setAlbumCatalogLookupEnqueueError,
    setAlbumCatalogLookupEnqueueResult,
    setAlbumCatalogLookupQ,
    setAlbumCatalogLookupStatus,
    setAppPage,
    setCatalogBackfillAlbumTracklistPolicy,
    setCatalogBackfillForceRefresh,
    setCatalogBackfillFullRunMode,
    setCatalogBackfillIncludeAlbums,
    setCatalogBackfillLimit,
    setCatalogBackfillMarket,
    setCatalogBackfillMaxAlbumTracksPagesPerAlbum,
    setCatalogBackfillMaxRequests,
    setCatalogBackfillMaxRuntimeSeconds,
    setCatalogBackfillOffset,
    setCatalogBackfillTab,
    setLikedTracksCountMode,
    setLikedTracksShuffleEnabled,
    setLikedTracksShuffleNonce,
    setLikedTracksSortMode,
    setPlaylistOverlayOpen,
    setPlaylistOverlayOptions,
    setListeningLogError,
    setListeningLogHasMore,
    setListeningLogLastLoadedAt,
    setListeningLogLoaded,
    setListeningLogOffset,
    setListeningLogTracks,
    setOpenDebugSessions,
    setOpenDebugTracks,
    setRecentDebugSourceFilter,
    setRecentCompletionFilter,
    setRecentLikedOnly,
    setRecentTaggedOnly,
    setSearchLookupEntityType,
    setSearchLookupQueueStatus,
    setSearchLookupSort,
    setSelectedPreview,
    hidePlaylistFromListenLab,
    unhidePlaylistInListenLab,
    deletePlaylistFromSpotify,
    setShowDebugLinkFields,
    setTrackCatalogLookupStatus,
    showDebugLinkFields,
    spotifyCooldownActive,
    syncLikedTracks,
    toggleSection,
    trackCatalogLookupError,
    trackCatalogLookupLastLoadedAt,
    trackCatalogLookupLoading,
    trackCatalogLookupResult,
    trackCatalogLookupStatus,
    trackRankingMode,
    usingLikedTracksFallback,
    visibleItemsWithPageSize,
  } = props;
  return appPage === "formulaLab" ? (
            <div className="dashboard-grid">
              <Suspense fallback={<RouteChunkFallback />}>
                <FormulaLabPage
                  hasProfile={Boolean(profile)}
                  knownTracks={[
                    ...allTimeTopTracks,
                    ...(profile.recent_top_tracks ?? []),
                    ...(profile.recent_tracks ?? []),
                    ...(profile.recent_likes_tracks ?? []),
                  ]}
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
              </Suspense>
              </div>
            ) : appPage === "identityAudit" ? (
              <div className="dashboard-grid">
                {renderIdentityAuditPage()}
              </div>
            ) : appPage === "recentDebug" ? (
              <div className="dashboard-grid">
                <Suspense fallback={<RouteChunkFallback />}>
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
                </Suspense>
              </div>
            ) : appPage === "catalogBackfill" ? (
              <div className="dashboard-grid">
                <Suspense fallback={<RouteChunkFallback />}>
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
                </Suspense>
              </div>
            ) : appPage === "searchLookup" ? (
              <div className="dashboard-grid">
                <Suspense fallback={<RouteChunkFallback />}>
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
                </Suspense>
              </div>
            ) : appPage === "dashboard" ? (
            <div className="dashboard-grid">
              {renderHomePlayerPanel()}
              {renderSavedPanel()}
              {renderLibraryPanel()}
              <DualSectionCard
                title={renderSectionTitle("Activity", "recent_likes")}
                section="recent"
                anchorId="activity"
                leftTitle={(
                  <div className="section-column-header activity-liked-header">
                    <span className="activity-column-heading">Listened</span>
                    <div className="section-column-header-actions">
                      <div className="track-ranking-toggle recent-play-filter-toggle" role="group" aria-label="Recently played filter">
                        {[
                          ["listened", "Completed"],
                          ["all", "All"],
                        ].map(([value, label]) => (
                          <button
                            className={`track-ranking-chip${recentCompletionFilter === value ? " track-ranking-chip-active" : ""}`}
                            key={value}
                            onClick={() => setRecentCompletionFilter(value as RecentCompletionFilter)}
                            type="button"
                          >
                            {label}
                          </button>
                        ))}
                      </div>
                      <button
                        className={`track-ranking-chip activity-filter-toggle-chip${recentLikedOnly ? " track-ranking-chip-active" : ""}`}
                        onClick={() => setRecentLikedOnly((current: boolean) => !current)}
                        type="button"
                      >
                        Liked
                      </button>
                      <button
                        className={`track-ranking-chip activity-filter-toggle-chip${recentTaggedOnly ? " track-ranking-chip-active" : ""}`}
                        onClick={() => setRecentTaggedOnly((current: boolean) => !current)}
                        type="button"
                      >
                        Tagged
                      </button>
                    </div>
                    <button className="secondary-button inline-reload-button listen-log-button" onClick={openListeningLogPage} type="button">
                      Listen Log
                    </button>
                  </div>
                )}
                rightTitle={(
                  <div className="section-column-header activity-liked-header">
                    <span className="activity-column-heading">Liked</span>
                    <div className="track-ranking-toggle liked-tracks-display-toggle" aria-label="Liked tracks display">
                      {[
                        ["100", String(LIKED_TRACKS_RECENT_DISPLAY_LIMIT)],
                        ["all", likedTracksTotalLabel],
                      ].map(([value, label]) => (
                        <button
                          className={`track-ranking-chip${likedTracksCountMode === value ? " track-ranking-chip-active" : ""}`}
                          key={value}
                          onClick={() => setLikedTracksCountMode(value as "100" | "all")}
                          type="button"
                        >
                          {label}
                        </button>
                      ))}
                    </div>
                    <div className="track-ranking-toggle liked-tracks-sort-toggle" aria-label="Liked tracks sort">
                      {[
                        ["recent", "Recent"],
                        ["older", "Older"],
                      ].map(([value, label]) => (
                        <button
                          className={`track-ranking-chip${likedTracksSortMode === value ? " track-ranking-chip-active" : ""}`}
                          key={value}
                          onClick={() => setLikedTracksSortMode(value as "recent" | "older")}
                          type="button"
                        >
                          {label}
                        </button>
                        ))}
                    </div>
                    <div className="track-ranking-toggle liked-tracks-shuffle-toggle" aria-label="Liked tracks order">
                      <button
                        aria-label="Show liked tracks in order"
                        aria-pressed={!likedTracksShuffleEnabled}
                        className={`track-ranking-chip liked-tracks-icon-chip${!likedTracksShuffleEnabled ? " track-ranking-chip-active" : ""}`}
                        disabled={likedTracksForActivitySource.length === 0}
                        onClick={() => setLikedTracksShuffleEnabled(false)}
                        title="In order"
                        type="button"
                      >
                        <svg aria-hidden="true" viewBox="0 0 24 24">
                          <path d="M4 7h12.6l-2.4-2.4L15.6 3 21 8.4l-5.4 5.4-1.4-1.6 2.4-2.2H4V7Zm0 8h12.6l-2.4-2.4 1.4-1.6 5.4 5.4-5.4 5.4-1.4-1.6 2.4-2.2H4v-3Z" fill="currentColor" />
                        </svg>
                      </button>
                      <button
                        aria-label="Shuffle liked tracks"
                        aria-pressed={likedTracksShuffleEnabled}
                        className={`track-ranking-chip liked-tracks-icon-chip${likedTracksShuffleEnabled ? " track-ranking-chip-active" : ""}`}
                        disabled={likedTracksForActivitySource.length === 0}
                        onClick={() => {
                          setLikedTracksShuffleEnabled(true);
                          setLikedTracksShuffleNonce((current: number) => current + 1);
                        }}
                        title="Shuffle"
                        type="button"
                      >
                        <svg aria-hidden="true" viewBox="0 0 24 24">
                          <path d="M16.5 3.8 21 8.3l-4.5 4.5-1.4-1.4 2.1-2.1h-1.5c-2.3 0-3.6.9-5.1 3.4-1.8 3.1-3.8 4.5-7.1 4.5H2v-2h1.5c2.4 0 3.7-.9 5.3-3.5 1.8-3 3.7-4.4 6.9-4.4h1.5l-2.1-2.1 1.4-1.4ZM2 6.3h1.5c2.3 0 4 .7 5.4 2.3l-1.3 1.6C6.5 8.9 5.3 8.3 3.5 8.3H2v-2Zm12.8 7.4 1.4-1.4 4.8 4.8-4.8 4.8-1.4-1.4 2.1-2.1h-1.2c-2 0-3.6-.6-4.9-1.9l1.2-1.7c1.1 1.1 2.2 1.6 3.7 1.6h1.2l-2.1-2.1Z" fill="currentColor" />
                        </svg>
                      </button>
                    </div>
                    {experienceMode === "full" ? (
                      <button
                        className="secondary-button inline-reload-button"
                        disabled={likedTracksSyncing || spotifyCooldownActive}
                        onClick={() => void syncLikedTracks()}
                        type="button"
                      >
                        {likedTracksSyncing ? "Refreshing..." : "Refresh"}
                      </button>
                    ) : null}
                  </div>
                )}
                leftContent={renderTrackColumn(
                  "recent",
                  activityRecentTracks,
                  activityRecentTracks.length > 0 || profile.recent_tracks_available,
                  "Spotify returned no recent listening history.",
                  recentUnavailableCopy(
                    "Recent listening is not available for this session yet. Log out and log back in to grant the updated Spotify permissions.",
                  ),
                  analysisMode === "quick" && experienceMode === "full" ? (
                    <button
                      className="secondary-button inline-reload-button"
                      disabled={loadingRecentSection}
                      onClick={() => void refreshRecentSection(recentRange)}
                      type="button"
                    >
                      {loadingRecentSection ? "Refreshing..." : "Reload this section"}
                    </button>
                  ) : null,
                )}
                rightContent={(
                  <>
                    {usingLikedTracksFallback ? (
                      <p className="empty-copy liked-tracks-cache-note">
                        Latest from Spotify. Refresh to populate the local cache.
                      </p>
                    ) : null}
                    {!usingLikedTracksFallback && cachedLikedTracks.length === 0 && !likedTracksLoading ? (
                      <p className="empty-copy liked-tracks-cache-note">
                        Liked tracks cache is empty. Refresh to load saved songs.
                      </p>
                    ) : null}
                    {likedTracksLoading ? <p className="empty-copy liked-tracks-cache-note">Loading liked tracks cache...</p> : null}
                    {likedTracksCacheStatus ? <p className="liked-tracks-cache-status">{likedTracksCacheStatus}</p> : null}
                    {likedTracksError ? <p className="empty-copy liked-tracks-cache-note">{likedTracksError}</p> : null}
                    {renderTrackColumn(
                      "likes",
                      likedTracksForActivity,
                      likedTracksAvailableForActivity,
                      "No liked tracks are available in the cache yet.",
                      recentUnavailableCopy(
                        "Liked tracks are not available for this session yet. Log out and log back in to grant library access.",
                      ),
                      analysisMode === "quick" && experienceMode === "full" ? (
                        <button
                          className="secondary-button inline-reload-button"
                          disabled={loadingRecentSection}
                          onClick={() => void refreshRecentSection(recentRange)}
                          type="button"
                        >
                          {loadingRecentSection ? "Refreshing..." : "Reload this section"}
                        </button>
                      ) : null,
                      false,
                      true,
                    )}
                  </>
                )}
                previewItemsLeft={previewItems(activityRecentTracks)}
                previewItemsRight={previewItems(likedTracksForActivity)}
                collapsedPreviewItems={previewItems(collapseRecentPreviewTracks(activityPreviewTracks))}
                isOpen={openSections.recent}
                toggleSection={toggleSection}
                onSelectPreview={setSelectedPreview}
              />

              {profile && playlistOverlayOpen ? (
                <DashboardPlaylistsSection
                  ownedPlaylists={profile.owned_playlists}
                  ownedPlaylistsAvailable={profile.owned_playlists_available}
                  apiBaseUrl={apiBaseUrl}
                  playlistOverlayOpen={playlistOverlayOpen}
                  playlistOverlayOptions={playlistOverlayOptions}
                  playlistsOpen={false}
                  setPlaylistOverlayOpen={setPlaylistOverlayOpen}
                  onConsumePlaylistOverlayOptions={() => setPlaylistOverlayOptions(null)}
                  toggleSection={toggleSection}
                  onSelectPreview={setSelectedPreview}
                  activePlaylistPlayback={activePlaylistPlayback}
                  onHidePlaylist={hidePlaylistFromListenLab}
                  onUnhidePlaylist={unhidePlaylistInListenLab}
                  onDeletePlaylist={deletePlaylistFromSpotify}
                  renderSectionTitle={renderSectionTitle}
                />
              ) : null}

              <section className="top-meta-section" id="top">
                <div className="top-meta-header">
                  <h2>Charts</h2>
                </div>

              <DualSectionCard
                title={renderSectionTitle("Tracks")}
                section="tracks"
                anchorId="tracks"
                leftTitle={(
                  <div className="section-column-header">
                    <h3>All time</h3>
                    <span className="liked-match-diagnostic">
                      {allTimeLikedMatchCount} / {allTimeTrackIdCount} liked matches
                    </span>
                    <div className="section-column-header-actions">
                      {renderTrackRankingToggle()}
                    </div>
                  </div>
                )}
                rightTitle={renderRecentRangeHeader()}
                leftContent={renderTrackColumn(
                  "tracksAllTime",
                  allTimeTopTracks,
                  allTimeTopTracksAvailableForDisplay,
                  "Spotify returned no top tracks for this account.",
                  quickUnavailableCopy("Top tracks are not available for this session yet. Log out and log back in to grant access."),
                )}
                rightContent={renderTrackColumn(
                  "tracksRecent",
                  recentTopTracksForDisplay,
                  recentTopTracksAvailableForDisplay,
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
                      onClick={() => void refreshRecentSection(recentRange)}
                      type="button"
                    >
                      {loadingRecentSection ? "Refreshing..." : "Reload this section"}
                    </button>
                  ) : null,
                )}
                previewItemsLeft={previewItems(allTimeTopTracks)}
                previewItemsRight={previewItems(recentTopTracksForDisplay)}
                collapsedPreviewItems={previewItems(collapseTrackPreviewAlbums(allTimeTopTracks))}
                isOpen={openSections.tracks}
                toggleSection={toggleSection}
                onSelectPreview={setSelectedPreview}
              />

              <DualSectionCard
                title={renderSectionTitle("Artists")}
                section="artists"
                anchorId="artists"
                leftTitle="All time"
                rightTitle={renderRecentRangeHeader()}
                leftContent={(
                  <DashboardArtistColumn
                    section="artistsAllTime"
                    items={profile.followed_artists}
                    available={profile.followed_artists_list_available}
                    emptyCopy="Spotify returned no top artists for this account."
                    unavailableCopy={quickUnavailableCopy("Top artists are not available for this session yet. Log out and log back in to grant access.")}
                    sectionPage={sectionPages.artistsAllTime}
                    moveSectionPage={moveSectionPage}
                    onSelectPreview={setSelectedPreview}
                  />
                )}
                rightContent={(
                  <DashboardArtistColumn
                    section="artistsRecent"
                    items={profile.recent_top_artists}
                    available={profile.recent_top_artists_available}
                    emptyCopy="Spotify returned no recent top artists for this account."
                    unavailableCopy={recentUnavailableCopy(
                      experienceMode === "local"
                        ? "Recent top artists are unavailable in restricted local mode."
                        : "Recent top artists are not available for this session yet. Log out and log back in to grant access.",
                    )}
                    unavailableAction={analysisMode === "quick" && experienceMode === "full" ? (
                      <button
                        className="secondary-button inline-reload-button"
                        disabled={loadingRecentSection}
                        onClick={() => void refreshRecentSection(recentRange)}
                        type="button"
                      >
                        {loadingRecentSection ? "Refreshing..." : "Reload this section"}
                      </button>
                    ) : null}
                    sectionPage={sectionPages.artistsRecent}
                    moveSectionPage={moveSectionPage}
                    onSelectPreview={setSelectedPreview}
                  />
                )}
                previewItemsLeft={previewItems(profile.followed_artists)}
                previewItemsRight={previewItems(profile.recent_top_artists)}
                isOpen={openSections.artists}
                toggleSection={toggleSection}
                onSelectPreview={setSelectedPreview}
              />

              <DualSectionCard
                title={renderSectionTitle("Albums")}
                section="albums"
                anchorId="albums"
                leftTitle="All time"
                rightTitle={renderRecentRangeHeader()}
                leftContent={(
                  <DashboardAlbumColumn
                    section="albumsAllTime"
                    items={profile.top_albums}
                    available={profile.top_albums_available}
                    emptyCopy="Spotify returned no top albums for this account."
                    unavailableCopy={quickUnavailableCopy("Top albums are not available for this session yet. Log out and log back in to grant access.")}
                    sectionPage={sectionPages.albumsAllTime}
                    moveSectionPage={moveSectionPage}
                    onSelectPreview={setSelectedPreview}
                  />
                )}
                rightContent={(
                  <DashboardAlbumColumn
                    section="albumsRecent"
                    items={profile.recent_top_albums}
                    available={profile.recent_top_albums_available}
                    emptyCopy="Spotify returned no recent top albums for this account."
                    unavailableCopy={recentUnavailableCopy(
                      experienceMode === "local"
                        ? "Recent top albums are unavailable in restricted local mode."
                        : "Recent top albums are not available for this session yet. Log out and log back in to grant access.",
                    )}
                    unavailableAction={analysisMode === "quick" && experienceMode === "full" ? (
                      <button
                        className="secondary-button inline-reload-button"
                        disabled={loadingRecentSection}
                        onClick={() => void refreshRecentSection(recentRange)}
                        type="button"
                      >
                        {loadingRecentSection ? "Refreshing..." : "Reload this section"}
                      </button>
                    ) : null}
                    sectionPage={sectionPages.albumsRecent}
                    moveSectionPage={moveSectionPage}
                    onSelectPreview={setSelectedPreview}
                  />
                )}
                previewItemsLeft={previewItems(profile.top_albums)}
                previewItemsRight={previewItems(profile.recent_top_albums)}
                isOpen={openSections.albums}
                toggleSection={toggleSection}
                onSelectPreview={setSelectedPreview}
              />

              </section>
            </div>
  ) : null;
}
