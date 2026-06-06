import type { ReactNode } from "react";

import type {
  AlbumMergeReviewTarget,
  ReleaseAlbumMergeDryRunResponse,
  ReleaseAlbumMergePreviewResponse,
} from "../../types/appTypes";

type AlbumDuplicateMergeRow = {
  release_album_id: number;
  release_album_name: string;
  artist_name: string;
  spotify_album_id?: string | null;
  spotify_album_name?: string | null;
};

type AlbumDuplicateMergeCardProps = {
  target: AlbumMergeReviewTarget;
  rows: AlbumDuplicateMergeRow[];
  extraMeta: ReactNode;
  preview?: ReleaseAlbumMergePreviewResponse;
  dryRun?: ReleaseAlbumMergeDryRunResponse;
  previewError?: string;
  dryRunError?: string;
  warningSummary: string | null;
  previewLoadingKey: string | null;
  dryRunLoadingKey: string | null;
  spotifyAlbumUrl: (spotifyAlbumId: string | null | undefined) => string;
  albumMergeReasonLabel: (reasonKey: string) => string;
  albumMergeReasonKey: (preview: ReleaseAlbumMergePreviewResponse | undefined) => string;
  plainEnglishAlbumMergeExplanation: (preview: ReleaseAlbumMergePreviewResponse | undefined) => string | null;
  renderAlbumMergeReadinessBadge: (value: string | null | undefined) => ReactNode;
  renderReleaseAlbumMergePreview: (key: string) => ReactNode;
  onPreviewMerge: (target: AlbumMergeReviewTarget) => void;
  onDryRunMerge: (target: AlbumMergeReviewTarget, survivorReleaseAlbumId: number) => void;
};

function AlbumDuplicateMergeRows({
  rows,
  spotifyAlbumUrl,
}: {
  rows: AlbumDuplicateMergeRow[];
  spotifyAlbumUrl: (spotifyAlbumId: string | null | undefined) => string;
}) {
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

export function AlbumDuplicateMergeCard({
  target,
  rows,
  extraMeta,
  preview,
  dryRun,
  previewError,
  dryRunError,
  warningSummary,
  previewLoadingKey,
  dryRunLoadingKey,
  spotifyAlbumUrl,
  albumMergeReasonLabel,
  albumMergeReasonKey,
  plainEnglishAlbumMergeExplanation,
  renderAlbumMergeReadinessBadge,
  renderReleaseAlbumMergePreview,
  onPreviewMerge,
  onDryRunMerge,
}: AlbumDuplicateMergeCardProps) {
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
      <AlbumDuplicateMergeRows rows={rows} spotifyAlbumUrl={spotifyAlbumUrl} />
      <div style={{ display: "flex", flexWrap: "wrap", gap: "10px" }}>
        <button
          className="track-ranking-chip"
          disabled={previewLoadingKey !== null}
          onClick={() => onPreviewMerge(target)}
          type="button"
        >
          {previewLoadingKey === target.key ? "Loading..." : "Preview merge"}
        </button>
        {preview?.survivor_release_album_id != null ? (() => {
          const survivorReleaseAlbumId = preview.survivor_release_album_id;
          return (
            <button
              className="secondary-button"
              disabled={dryRunLoadingKey !== null}
              onClick={() => onDryRunMerge(target, survivorReleaseAlbumId)}
              type="button"
            >
              {dryRunLoadingKey === target.key ? "Loading..." : "Dry run"}
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
