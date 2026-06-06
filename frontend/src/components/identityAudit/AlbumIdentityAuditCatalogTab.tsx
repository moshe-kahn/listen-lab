import type { CatalogBackfillCoverageResponse } from "../../types/appTypes";

type AlbumIdentityAuditCatalogTabProps = {
  catalogBackfillCoverage: CatalogBackfillCoverageResponse | null;
  catalogBackfillCoverageLoading: boolean;
  catalogBackfillCoverageError: string;
  catalogBackfillCoverageLastLoadedAt: number | null;
  onOpenAlbumLookup: () => void;
  onOpenCatalogBackfill: () => void;
  onRefreshCatalogSummary: () => void;
};

export function AlbumIdentityAuditCatalogTab({
  catalogBackfillCoverage,
  catalogBackfillCoverageLoading,
  catalogBackfillCoverageError,
  catalogBackfillCoverageLastLoadedAt,
  onOpenAlbumLookup,
  onOpenCatalogBackfill,
  onRefreshCatalogSummary,
}: AlbumIdentityAuditCatalogTabProps) {
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
            onClick={onOpenAlbumLookup}
            type="button"
          >
            Open Album Lookup
          </button>
          <button
            className="secondary-button"
            onClick={onOpenCatalogBackfill}
            type="button"
          >
            Open Catalog Backfill
          </button>
          <button
            className="secondary-button"
            disabled={catalogBackfillCoverageLoading}
            onClick={onRefreshCatalogSummary}
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
