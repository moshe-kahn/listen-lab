import type { Dispatch, SetStateAction } from "react";
import type {
  CatalogBackfillCoverageResponse,
  CatalogBackfillQueueReasonFilter,
  CatalogBackfillQueueResponse,
  CatalogBackfillRunMode,
  CatalogBackfillRunResponse,
  CatalogBackfillRunsResponse,
  CatalogBackfillTab,
} from "../../types/appTypes";
import { formatDebugTimestamp } from "../../utils/dashboardUtils";

type CatalogBackfillPageProps = {
  hasProfile: boolean;
  catalogBackfillTab: CatalogBackfillTab;
  setCatalogBackfillTab: Dispatch<SetStateAction<CatalogBackfillTab>>;
  catalogBackfillCoverage: CatalogBackfillCoverageResponse | null;
  catalogBackfillCoverageLoading: boolean;
  catalogBackfillCoverageError: string;
  catalogBackfillCoverageLastLoadedAt: number | null;
  catalogBackfillRuns: CatalogBackfillRunsResponse | null;
  catalogBackfillRunsLoading: boolean;
  catalogBackfillRunsError: string;
  catalogBackfillRunsLastLoadedAt: number | null;
  catalogBackfillQueue: CatalogBackfillQueueResponse | null;
  catalogBackfillQueueLoading: boolean;
  catalogBackfillQueueError: string;
  catalogBackfillQueueLastLoadedAt: number | null;
  catalogBackfillQueueStatusFilter: "all" | "pending" | "done" | "error";
  catalogBackfillQueueReasonFilter: CatalogBackfillQueueReasonFilter;
  catalogBackfillQueueRepairLoading: boolean;
  catalogBackfillQueueRepairMessage: string;
  catalogBackfillLatestResult: CatalogBackfillRunResponse | null;
  catalogBackfillRunLoading: boolean;
  catalogBackfillRunError: string;
  catalogBackfillLimit: number;
  setCatalogBackfillLimit: Dispatch<SetStateAction<number>>;
  catalogBackfillOffset: number;
  setCatalogBackfillOffset: Dispatch<SetStateAction<number>>;
  catalogBackfillMarket: string;
  setCatalogBackfillMarket: Dispatch<SetStateAction<string>>;
  catalogBackfillForceRefresh: boolean;
  setCatalogBackfillForceRefresh: Dispatch<SetStateAction<boolean>>;
  catalogBackfillMaxRequests: number;
  setCatalogBackfillMaxRequests: Dispatch<SetStateAction<number>>;
  catalogBackfillMaxRuntimeSeconds: number;
  setCatalogBackfillMaxRuntimeSeconds: Dispatch<SetStateAction<number>>;
  catalogBackfillFullRunMode: "tracklists_relevant" | "full_catalog";
  setCatalogBackfillFullRunMode: Dispatch<SetStateAction<"tracklists_relevant" | "full_catalog">>;
  catalogBackfillAlbumTracklistPolicy: "all" | "priority_only" | "relevant_albums" | "none";
  setCatalogBackfillAlbumTracklistPolicy: Dispatch<SetStateAction<"all" | "priority_only" | "relevant_albums" | "none">>;
  catalogBackfillMaxAlbumTracksPagesPerAlbum: number;
  setCatalogBackfillMaxAlbumTracksPagesPerAlbum: Dispatch<SetStateAction<number>>;
  catalogBackfillIncludeAlbums: boolean;
  setCatalogBackfillIncludeAlbums: Dispatch<SetStateAction<boolean>>;
  loadCatalogBackfillCoverage: (reset?: boolean) => void | Promise<void>;
  loadCatalogBackfillRuns: (reset?: boolean) => void | Promise<void>;
  loadCatalogBackfillQueue: (
    reset?: boolean,
    explicitFilter?: "all" | "pending" | "done" | "error",
    explicitReasonFilter?: CatalogBackfillQueueReasonFilter,
  ) => void | Promise<void>;
  repairCatalogBackfillQueueStatuses: () => void | Promise<void>;
  runCatalogBackfill: (runMode: CatalogBackfillRunMode) => void | Promise<void>;
  onBack: () => void;
};

export function CatalogBackfillPage({
  hasProfile,
  catalogBackfillTab,
  setCatalogBackfillTab,
  catalogBackfillCoverage,
  catalogBackfillCoverageLoading,
  catalogBackfillCoverageError,
  catalogBackfillCoverageLastLoadedAt,
  catalogBackfillRuns,
  catalogBackfillRunsLoading,
  catalogBackfillRunsError,
  catalogBackfillRunsLastLoadedAt,
  catalogBackfillQueue,
  catalogBackfillQueueLoading,
  catalogBackfillQueueError,
  catalogBackfillQueueLastLoadedAt,
  catalogBackfillQueueStatusFilter,
  catalogBackfillQueueReasonFilter,
  catalogBackfillQueueRepairLoading,
  catalogBackfillQueueRepairMessage,
  catalogBackfillLatestResult,
  catalogBackfillRunLoading,
  catalogBackfillRunError,
  catalogBackfillLimit,
  setCatalogBackfillLimit,
  catalogBackfillOffset,
  setCatalogBackfillOffset,
  catalogBackfillMarket,
  setCatalogBackfillMarket,
  catalogBackfillForceRefresh,
  setCatalogBackfillForceRefresh,
  catalogBackfillMaxRequests,
  setCatalogBackfillMaxRequests,
  catalogBackfillMaxRuntimeSeconds,
  setCatalogBackfillMaxRuntimeSeconds,
  catalogBackfillFullRunMode,
  setCatalogBackfillFullRunMode,
  catalogBackfillAlbumTracklistPolicy,
  setCatalogBackfillAlbumTracklistPolicy,
  catalogBackfillMaxAlbumTracksPagesPerAlbum,
  setCatalogBackfillMaxAlbumTracksPagesPerAlbum,
  catalogBackfillIncludeAlbums,
  setCatalogBackfillIncludeAlbums,
  loadCatalogBackfillCoverage,
  loadCatalogBackfillRuns,
  loadCatalogBackfillQueue,
  repairCatalogBackfillQueueStatuses,
  runCatalogBackfill,
  onBack,
}: CatalogBackfillPageProps) {
  if (!hasProfile) {
    return null;
  }

  const latestDisplayRun = catalogBackfillLatestResult ?? catalogBackfillCoverage?.latest_run ?? null;
  const identityCritical = catalogBackfillCoverage?.identity_critical;
  const catalogExpansion = catalogBackfillCoverage?.catalog_expansion;
  const coveragePercent = typeof catalogBackfillCoverage?.track_duration_coverage_percent === "number"
    ? `${catalogBackfillCoverage.track_duration_coverage_percent.toFixed(2)}%`
    : "0.00%";
  const missingPriorityTrackMetadata = identityCritical?.missing_priority_track_metadata ?? 0;
  const missingIdentityCritical = missingPriorityTrackMetadata + (identityCritical?.missing_source_album_metadata ?? 0);
  const latestWarnings = Array.isArray((latestDisplayRun as { warnings?: unknown[] } | null)?.warnings)
    ? ((latestDisplayRun as { warnings?: string[] }).warnings ?? [])
    : [];
  const latestWarningsCount = latestWarnings.length > 0
    ? latestWarnings.length
    : (latestDisplayRun?.warnings_count ?? 0);
  const showLatestLastError = Boolean(latestDisplayRun?.last_error) && (latestDisplayRun?.status ?? "unknown") !== "ok";

  return (
    <section className="info-card info-card-wide tracks-only-card" id="catalog-backfill-page">
      <div className="tracks-only-header">
        <div>
          <h2>Catalog Backfill</h2>
          <p className="tracks-only-subtitle">Run and monitor Spotify catalog enrichment for static track and album metadata.</p>
          <p className="empty-copy">Catalog enrichment only. No identity mappings are changed.</p>
        </div>
        <div className="section-column-header-actions">
          <button
            className="secondary-button"
            disabled={catalogBackfillCoverageLoading || catalogBackfillRunsLoading || catalogBackfillQueueLoading}
            onClick={() => {
              void loadCatalogBackfillCoverage(true);
              void loadCatalogBackfillRuns(true);
              void loadCatalogBackfillQueue(true);
            }}
            type="button"
          >
            {(catalogBackfillCoverageLoading || catalogBackfillRunsLoading || catalogBackfillQueueLoading) ? "Refreshing..." : "Refresh all"}
          </button>
          <button
            className="secondary-button tracks-only-back-button"
            onClick={onBack}
            type="button"
          >
            Back to dashboard
          </button>
        </div>
      </div>

      <div className="track-ranking-toggle" role="group" aria-label="Catalog backfill sections">
        {[
          ["overview", "Overview"],
          ["priorityMetadata", "Priority Metadata"],
          ["fullBackfill", "Full Backfill"],
          ["queue", "Queue"],
          ["recentRuns", "Recent Runs"],
        ].map(([value, label]) => (
          <button
            className={`track-ranking-chip${catalogBackfillTab === value ? " track-ranking-chip-active" : ""}`}
            key={`catalog-backfill-tab-${value}`}
            onClick={() => setCatalogBackfillTab(value as CatalogBackfillTab)}
            type="button"
          >
            {label}
          </button>
        ))}
      </div>

      {catalogBackfillTab === "overview" ? (
        <div className="info-card-body">
          <h3>Overview</h3>
          {catalogBackfillCoverageError ? <p className="empty-copy">{catalogBackfillCoverageError}</p> : null}
          {!catalogBackfillCoverage && catalogBackfillCoverageLoading ? <p className="empty-copy">Loading coverage...</p> : null}
          <div className="tracks-only-summary">
            <span>Known release tracks: {catalogBackfillCoverage?.known_release_tracks ?? 0}</span>
            <span>Track catalog rows: {catalogBackfillCoverage?.track_catalog_rows ?? 0}</span>
            <span>Duration coverage: {catalogBackfillCoverage?.track_duration_coverage_count ?? 0} ({coveragePercent})</span>
            <span>Known release albums: {catalogBackfillCoverage?.known_release_albums ?? 0}</span>
            <span>Album catalog rows: {catalogBackfillCoverage?.album_catalog_rows ?? 0}</span>
            <span>Album track rows: {catalogBackfillCoverage?.album_track_rows ?? 0}</span>
            <span>Priority track metadata: {missingPriorityTrackMetadata}</span>
            <span>Deferred track metadata: {catalogExpansion?.missing_deferred_track_metadata ?? 0}</span>
            <span>Missing tracklists: {catalogExpansion?.missing_album_tracklists ?? 0}</span>
            <span>Recent run errors: {catalogBackfillCoverage?.recent_errors_count ?? 0}</span>
            {catalogBackfillCoverageLastLoadedAt ? <span>Coverage loaded {new Date(catalogBackfillCoverageLastLoadedAt).toLocaleTimeString()}</span> : null}
          </div>
          {catalogBackfillCoverage?.latest_run ? (
            <p className="empty-copy">
              Latest run {catalogBackfillCoverage.latest_run.id}: {catalogBackfillCoverage.latest_run.status ?? "unknown"} | mode {catalogBackfillCoverage.latest_run.run_mode ?? "unknown"} | started{" "}
              {formatDebugTimestamp(catalogBackfillCoverage.latest_run.started_at)}
            </p>
          ) : (
            <p className="empty-copy">No catalog backfill runs recorded yet.</p>
          )}
        </div>
      ) : null}

      {catalogBackfillTab === "priorityMetadata" ? (
        <div className="info-card-body">
          <h3>Priority Metadata</h3>
          <p className="empty-copy">Fetches source-layer Spotify track and album metadata for identity decisions. It does not expand unlistened album tracklists.</p>
          <div className="tracks-only-summary">
            <span>Total missing source track metadata: {identityCritical?.missing_source_track_metadata ?? 0}</span>
            <span>Priority track metadata: {missingPriorityTrackMetadata}</span>
            <span>Identity-ambiguous track metadata: {identityCritical?.missing_identity_ambiguous_track_metadata ?? 0}</span>
            <span>Top-listened track metadata: {identityCritical?.missing_top_track_metadata ?? 0}</span>
            <span>Deferred track metadata: {catalogExpansion?.missing_deferred_track_metadata ?? 0}</span>
            <span>Missing source album metadata: {identityCritical?.missing_source_album_metadata ?? 0}</span>
            <span>Missing identity-critical metadata: {missingIdentityCritical}</span>
            <span>Missing ISRC: {identityCritical?.missing_track_isrc ?? 0}</span>
            <span>Missing duration: {identityCritical?.missing_track_duration_ms ?? 0}</span>
            <span>Missing album release date: {identityCritical?.missing_album_release_date ?? 0}</span>
            <span>Missing album external IDs: {identityCritical?.missing_album_external_ids ?? 0}</span>
          </div>
          <div className="identity-audit-ambiguous-toolbar">
            <label>
              Limit
              <input min={1} onChange={(event) => setCatalogBackfillLimit(Math.max(1, Number(event.target.value) || 1))} type="number" value={catalogBackfillLimit} />
            </label>
            <label>
              Offset
              <input min={0} onChange={(event) => setCatalogBackfillOffset(Math.max(0, Number(event.target.value) || 0))} type="number" value={catalogBackfillOffset} />
            </label>
            <label>
              Market
              <input onChange={(event) => setCatalogBackfillMarket(event.target.value.toUpperCase())} type="text" value={catalogBackfillMarket} />
            </label>
            <label>
              Max requests
              <input max={1000} min={1} onChange={(event) => setCatalogBackfillMaxRequests(Math.min(1000, Math.max(1, Number(event.target.value) || 1)))} type="number" value={catalogBackfillMaxRequests} />
            </label>
            <label>
              Max runtime (s)
              <input max={300} min={5} onChange={(event) => setCatalogBackfillMaxRuntimeSeconds(Math.min(300, Math.max(5, Number(event.target.value) || 5)))} type="number" value={catalogBackfillMaxRuntimeSeconds} />
            </label>
            <label className="recent-debug-filter">
              <input checked={catalogBackfillForceRefresh} onChange={(event) => setCatalogBackfillForceRefresh(event.currentTarget.checked)} type="checkbox" />
              Force refresh
            </label>
            <button className="primary-button" disabled={catalogBackfillRunLoading} onClick={() => void runCatalogBackfill("metadata_only")} type="button">
              {catalogBackfillRunLoading ? "Running..." : "Run priority metadata"}
            </button>
          </div>
          <p className="empty-copy">Config: run_mode=metadata_only, include_albums=true, album_tracklist_policy=none, reason=identity_metadata.</p>
          {catalogBackfillRunError ? <p className="empty-copy">{catalogBackfillRunError}</p> : null}
        </div>
      ) : null}

      {catalogBackfillTab === "fullBackfill" ? (
        <div className="info-card-body">
          <h3>Full Backfill</h3>
          <p className="empty-copy">Slower tracklist-capable enrichment for broader catalog completeness. This is secondary to priority identity metadata.</p>
          <div className="tracks-only-summary">
            <span>Missing tracklists: {catalogExpansion?.missing_album_tracklists ?? 0}</span>
            <span>Relevant album tracklist backlog: {catalogExpansion?.relevant_album_tracklist_backlog ?? 0}</span>
            <span>Unlistened tracklist rows stored: {catalogExpansion?.unlistened_tracklist_rows ?? 0}</span>
          </div>
          <div className="identity-audit-ambiguous-toolbar">
            <label>
              Run mode
              <select onChange={(event) => setCatalogBackfillFullRunMode(event.target.value as "tracklists_relevant" | "full_catalog")} value={catalogBackfillFullRunMode}>
                <option value="tracklists_relevant">tracklists_relevant</option>
                <option value="full_catalog">full_catalog</option>
              </select>
            </label>
            <label>
              Album tracklist policy
              <select onChange={(event) => setCatalogBackfillAlbumTracklistPolicy(event.target.value as "all" | "priority_only" | "relevant_albums" | "none")} value={catalogBackfillAlbumTracklistPolicy}>
                <option value="relevant_albums">Relevant albums</option>
                <option value="priority_only">Prioritized only</option>
                <option value="all">All</option>
              </select>
            </label>
            <label>
              Limit
              <input min={1} onChange={(event) => setCatalogBackfillLimit(Math.max(1, Number(event.target.value) || 1))} type="number" value={catalogBackfillLimit} />
            </label>
            <label>
              Max album pages
              <input max={50} min={1} onChange={(event) => setCatalogBackfillMaxAlbumTracksPagesPerAlbum(Math.min(50, Math.max(1, Number(event.target.value) || 1)))} type="number" value={catalogBackfillMaxAlbumTracksPagesPerAlbum} />
            </label>
            <label className="recent-debug-filter">
              <input checked={catalogBackfillIncludeAlbums} onChange={(event) => setCatalogBackfillIncludeAlbums(event.currentTarget.checked)} type="checkbox" />
              Include albums
            </label>
            <button className="secondary-button" disabled={catalogBackfillRunLoading} onClick={() => void runCatalogBackfill(catalogBackfillFullRunMode)} type="button">
              {catalogBackfillRunLoading ? "Running..." : "Run full tracklist backfill"}
            </button>
          </div>
          <p className="empty-copy">Config: run_mode={catalogBackfillFullRunMode}, album_tracklist_policy={catalogBackfillAlbumTracklistPolicy}, reason={catalogBackfillFullRunMode === "tracklists_relevant" ? "tracklist_completion" : "full_backfill"}.</p>
          {catalogBackfillRunError ? <p className="empty-copy">{catalogBackfillRunError}</p> : null}
        </div>
      ) : null}

      {latestDisplayRun ? (
        <div className="info-card-body">
          <h3>Latest Run Result</h3>
          <div className="tracks-only-summary">
            <span>Status: {latestDisplayRun.status ?? "unknown"}</span>
            <span>Mode: {latestDisplayRun.run_mode ?? "unknown"}</span>
            <span>Reason: {latestDisplayRun.run_reason ?? "none"}</span>
            <span>Partial: {latestDisplayRun.partial ? "yes" : "no"}</span>
            <span>Stop reason: {latestDisplayRun.stop_reason ?? "none"}</span>
            <span>Tracks seen/fetched/upserted: {latestDisplayRun.tracks_seen} / {latestDisplayRun.tracks_fetched} / {latestDisplayRun.tracks_upserted}</span>
            <span>Albums seen/fetched: {latestDisplayRun.albums_seen} / {latestDisplayRun.albums_fetched}</span>
            <span>Album tracklists seen/skipped/fetched: {latestDisplayRun.album_tracklists_seen ?? 0} / {latestDisplayRun.album_tracklists_skipped_by_policy ?? 0} / {latestDisplayRun.album_tracklists_fetched ?? 0}</span>
            <span>Album tracklist policy: {latestDisplayRun.album_tracklist_policy ?? "all"}</span>
            <span>Errors: {latestDisplayRun.errors}</span>
            <span>Requests total: {latestDisplayRun.requests_total}</span>
            <span>Requests 429: {latestDisplayRun.requests_429}</span>
            <span>Warnings: {latestWarningsCount}</span>
            {latestWarnings.length > 0 ? <span>Warning details: {latestWarnings.join(" | ")}</span> : null}
            {showLatestLastError ? <span>Last error: {latestDisplayRun.last_error}</span> : null}
          </div>
        </div>
      ) : null}

      {catalogBackfillTab === "recentRuns" ? (
        <div className="info-card-body">
          <div className="section-column-header">
            <h3>Recent Runs</h3>
            <button className="secondary-button" disabled={catalogBackfillRunsLoading} onClick={() => void loadCatalogBackfillRuns(true)} type="button">
              {catalogBackfillRunsLoading ? "Refreshing..." : "Refresh"}
            </button>
          </div>
          {catalogBackfillRunsError ? <p className="empty-copy">{catalogBackfillRunsError}</p> : null}
          {!catalogBackfillRuns && catalogBackfillRunsLoading ? <p className="empty-copy">Loading recent runs...</p> : null}
          {catalogBackfillRunsLastLoadedAt ? <p className="empty-copy">Runs loaded {new Date(catalogBackfillRunsLastLoadedAt).toLocaleTimeString()}</p> : null}
          {!catalogBackfillRuns || catalogBackfillRuns.items.length === 0 ? (
            <p className="empty-copy">No runs available.</p>
          ) : (
            <div className="recent-debug-grid">
              {catalogBackfillRuns.items.map((run) => {
                const runWarnings = Array.isArray(run.warnings) ? run.warnings : [];
                const runWarningsCount = runWarnings.length > 0 ? runWarnings.length : (run.warnings_count ?? 0);
                const runLastError = (run.status ?? "unknown") === "ok" ? "none" : (run.last_error ?? "none");
                return (
                  <div className="recent-debug-row" key={`catalog-run-${run.id}`}>
                    <span className="recent-debug-key">Run {run.id} | {formatDebugTimestamp(run.started_at)} {"->"} {formatDebugTimestamp(run.completed_at)}</span>
                    <span className="recent-debug-value">
                      mode={run.run_mode ?? "unknown"} | reason={run.run_reason ?? "none"} | policy={run.album_tracklist_policy ?? "all"} | status={run.status ?? "unknown"}{run.status === "partial" ? " [PARTIAL/STOPPED]" : ""} | tracks={run.tracks_seen}/{run.tracks_fetched}/{run.tracks_upserted} | albums={run.albums_seen}/{run.albums_fetched} | album_tracks={run.album_tracks_upserted} | errors={run.errors} | requests_429={run.requests_429} | warnings={runWarningsCount} | last_error={runLastError}
                    </span>
                    {runWarnings.length > 0 ? <span className="recent-debug-value">warning_details={runWarnings.join(" | ")}</span> : null}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      ) : null}

      {catalogBackfillTab === "queue" ? (
        <div className="info-card-body">
          <div className="section-column-header">
            <h3>Queue</h3>
            <div className="section-column-header-actions">
              <label>
                Status
                <select
                  onChange={(event) => {
                    const nextFilter = event.target.value as "all" | "pending" | "done" | "error";
                    void loadCatalogBackfillQueue(true, nextFilter, catalogBackfillQueueReasonFilter);
                  }}
                  value={catalogBackfillQueueStatusFilter}
                >
                  <option value="all">All</option>
                  <option value="pending">Pending</option>
                  <option value="done">Done</option>
                  <option value="error">Error</option>
                </select>
              </label>
              <label>
                Reason
                <select
                  onChange={(event) => {
                    const nextFilter = event.target.value as CatalogBackfillQueueReasonFilter;
                    void loadCatalogBackfillQueue(true, catalogBackfillQueueStatusFilter, nextFilter);
                  }}
                  value={catalogBackfillQueueReasonFilter}
                >
                  <option value="all">All reasons</option>
                  <option value="identity_metadata">identity_metadata</option>
                  <option value="manual_priority">manual_priority</option>
                  <option value="tracklist_completion">tracklist_completion</option>
                  <option value="full_backfill">full_backfill</option>
                </select>
              </label>
              <button className="secondary-button" disabled={catalogBackfillQueueRepairLoading} onClick={() => void repairCatalogBackfillQueueStatuses()} type="button">
                {catalogBackfillQueueRepairLoading ? "Repairing..." : "Repair queue statuses"}
              </button>
              <button className="secondary-button" disabled={catalogBackfillQueueLoading} onClick={() => void loadCatalogBackfillQueue(true)} type="button">
                {catalogBackfillQueueLoading ? "Refreshing..." : "Refresh"}
              </button>
            </div>
          </div>
          <div className="tracks-only-summary">
            <span>Pending: {catalogBackfillQueue?.counts.pending ?? 0}</span>
            <span>Done: {catalogBackfillQueue?.counts.done ?? 0}</span>
            <span>Error: {catalogBackfillQueue?.counts.error ?? 0}</span>
            <span>identity_metadata: {catalogBackfillQueue?.reason_counts?.identity_metadata ?? 0}</span>
            <span>manual_priority: {catalogBackfillQueue?.reason_counts?.manual_priority ?? 0}</span>
            <span>tracklist_completion: {catalogBackfillQueue?.reason_counts?.tracklist_completion ?? 0}</span>
            <span>full_backfill: {catalogBackfillQueue?.reason_counts?.full_backfill ?? 0}</span>
            <span>Total: {catalogBackfillQueue?.total ?? 0}</span>
          </div>
          {catalogBackfillQueueRepairMessage ? <p className="empty-copy">{catalogBackfillQueueRepairMessage}</p> : null}
          {catalogBackfillQueueError ? <p className="empty-copy">{catalogBackfillQueueError}</p> : null}
          {!catalogBackfillQueue && catalogBackfillQueueLoading ? <p className="empty-copy">Loading queue...</p> : null}
          {catalogBackfillQueueLastLoadedAt ? <p className="empty-copy">Queue loaded {new Date(catalogBackfillQueueLastLoadedAt).toLocaleTimeString()}</p> : null}
          {!catalogBackfillQueue || catalogBackfillQueue.items.length === 0 ? (
            <p className="empty-copy">No queue items.</p>
          ) : (
            <div className="recent-debug-grid">
              {catalogBackfillQueue.items.map((item) => (
                <div className="recent-debug-row" key={`catalog-queue-${item.id}`}>
                  <span className="recent-debug-key">{item.entity_type}:{item.spotify_id} | status={item.status} | priority={item.priority} | attempts={item.attempts}</span>
                  <span className="recent-debug-value">requested={formatDebugTimestamp(item.requested_at)} | last_attempted={formatDebugTimestamp(item.last_attempted_at)} | reason={item.reason ?? "none"} | last_error={item.last_error ?? "none"}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      ) : null}
    </section>
  );
}
