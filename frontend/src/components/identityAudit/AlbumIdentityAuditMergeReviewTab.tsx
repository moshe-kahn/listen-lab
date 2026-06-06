import type { ReactNode } from "react";

import type {
  AlbumMergeReviewTarget,
  ReleaseAlbumMergeDryRunResponse,
  ReleaseAlbumMergePreviewResponse,
} from "../../types/appTypes";

type AlbumIdentityAuditMergeReviewTabProps = {
  targets: AlbumMergeReviewTarget[];
  selectedAlbumMergeReviewKey: string | null;
  releaseAlbumMergePreviewByKey: Record<string, ReleaseAlbumMergePreviewResponse>;
  releaseAlbumMergeDryRunByKey: Record<string, ReleaseAlbumMergeDryRunResponse>;
  renderAlbumMergeReadinessBadge: (value: string | null | undefined) => ReactNode;
  renderReleaseAlbumMergePreview: (key: string) => ReactNode;
  onSelectTarget: (key: string) => void;
};

export function AlbumIdentityAuditMergeReviewTab({
  targets,
  selectedAlbumMergeReviewKey,
  releaseAlbumMergePreviewByKey,
  releaseAlbumMergeDryRunByKey,
  renderAlbumMergeReadinessBadge,
  renderReleaseAlbumMergePreview,
  onSelectTarget,
}: AlbumIdentityAuditMergeReviewTabProps) {
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
                  onClick={() => onSelectTarget(target.key)}
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
