import type { DashboardListCardProps, PreviewItem } from "../../types/appTypes";
import { LikedBadge } from "../common/LikedBadge";
import { ReleaseSiblingBadge } from "../common/ReleaseSiblingBadge";

type DashboardListCardComponentProps = DashboardListCardProps & {
  previewKind: PreviewItem["kind"];
  previewTrackUri: string | null;
  onSelectPreview: (preview: PreviewItem) => void;
};

export function DashboardListCard({
  href,
  entityId,
  imageUrl,
  imageAlt,
  fallbackLabel,
  primaryText,
  secondaryText,
  tertiaryText,
  metricText,
  primaryBadgeText,
  primaryInlineBadgeText,
  liked,
  playlistOwnerFollowedByYou,
  releaseSibling,
  releaseSiblingSourceCount,
  releaseSiblingDuplicateSourceCount,
  releaseTrackClusterCandidateType,
  releaseTrackClusterRelationshipKind,
  secondaryBadgeText,
  completionRatio,
  completionMarkers,
  trackUri,
  previewTrack,
  primaryClamp = "single-line-ellipsis",
  previewKind,
  previewTrackUri,
  onSelectPreview,
  imageOverlay,
  rowAction,
  muted = false,
  cardClassName,
  previewOverrides,
}: DashboardListCardComponentProps) {
  const secondaryValue = secondaryText && secondaryText.trim().length > 0 ? secondaryText : "\u00A0";
  const tertiaryValue = tertiaryText && tertiaryText.trim().length > 0 ? tertiaryText : "\u00A0";
  const secondaryPlaceholder = !(secondaryText && secondaryText.trim().length > 0);
  const tertiaryPlaceholder = !(tertiaryText && tertiaryText.trim().length > 0);
  const hasTopRightContent = Boolean(liked || releaseSibling || primaryBadgeText || secondaryBadgeText || metricText);

  return (
    <button
      className={[
        "list-row list-link dashboard-card-row",
        cardClassName,
        muted ? "dashboard-card-row-muted" : null,
      ].filter(Boolean).join(" ")}
      onClick={() =>
        onSelectPreview({
          image: imageUrl ?? null,
          fallbackLabel,
          label: primaryText,
          meta: secondaryText ?? null,
          detail: tertiaryText ?? null,
          kind: previewKind,
          entityId: entityId ?? null,
          trackUri: previewKind === "track"
            ? previewTrackUri
            : trackUri ?? null,
          url: href ?? "",
          trackId: previewTrack?.track_id ?? null,
          releaseTrackId: previewTrack?.release_track_id ?? null,
          releaseTrackName: previewTrack?.release_track_name ?? null,
          releaseTrackSourceCount: previewTrack?.release_track_source_count ?? null,
          releaseTrackDuplicateSourceCount: previewTrack?.release_track_duplicate_source_count ?? null,
          hasReleaseTrackSiblings: previewTrack?.has_release_track_siblings ?? null,
          releaseTrackClusterCandidateType: previewTrack?.release_track_cluster_candidate_type ?? null,
          releaseTrackClusterRelationshipKind: previewTrack?.release_track_cluster_relationship_kind ?? null,
          albumId: previewTrack?.album_id ?? null,
          artistName: previewTrack?.artist_name ?? null,
          sourceTrack: previewTrack ?? null,
          playlistOwnerFollowedByYou: playlistOwnerFollowedByYou ?? null,
          ...previewOverrides,
        })}
      type="button"
    >
      <div className="dashboard-card-layout">
        <div className="list-primary">
          <div className="list-art-frame">
            {imageUrl ? (
              <img alt={imageAlt} className="list-art" src={imageUrl} />
            ) : (
              <div className="list-art list-art-fallback" aria-hidden="true">
                {fallbackLabel}
              </div>
            )}
            {imageOverlay ? <div className="card-image-overlay">{imageOverlay}</div> : null}
            {rowAction ? <div className="card-row-action">{rowAction}</div> : null}
          </div>
          <div className="card-copy">
            <div className="card-primary-line">
              <strong className={`card-primary ${primaryClamp}`}>{primaryText}</strong>
              {primaryInlineBadgeText ? <span className="card-primary-inline-badge">{primaryInlineBadgeText}</span> : null}
            </div>
            <p
              aria-hidden={secondaryPlaceholder}
              className={`card-secondary single-line-ellipsis${secondaryPlaceholder ? " card-line-placeholder" : ""}`}
            >
              {secondaryValue}
            </p>
            <p
              aria-hidden={tertiaryPlaceholder}
              className={`card-tertiary single-line-ellipsis${tertiaryPlaceholder ? " card-line-placeholder" : ""}`}
            >
              {tertiaryValue}
            </p>
          </div>
        </div>
        {hasTopRightContent ? (
          <div className="card-right-stack">
            {liked || releaseSibling || primaryBadgeText || secondaryBadgeText ? (
              <div className="card-badge-stack">
                {liked ? <LikedBadge className="card-liked-badge" /> : null}
                {releaseSibling ? (
                  <ReleaseSiblingBadge
                    className="card-release-sibling-badge"
                    sourceCount={releaseSiblingSourceCount}
                    duplicateSourceCount={releaseSiblingDuplicateSourceCount}
                    clusterCandidateType={releaseTrackClusterCandidateType}
                    clusterRelationshipKind={releaseTrackClusterRelationshipKind}
                  />
                ) : null}
                {primaryBadgeText ? <span className="card-inline-badge">{primaryBadgeText}</span> : null}
                {secondaryBadgeText ? <span className="card-inline-badge">{secondaryBadgeText}</span> : null}
              </div>
            ) : null}
            {metricText ? <div className="card-metric">{metricText}</div> : null}
          </div>
        ) : null}
        {typeof completionRatio === "number" ? (
          <span className="card-completion" aria-hidden="true">
            <span
              className="card-completion-fill"
              style={{ width: `${Math.round(Math.max(0, Math.min(1, completionRatio)) * 100)}%` }}
            />
            {completionMarkers?.map((marker) => {
              const markerPercent = Math.round(Math.max(0, Math.min(1, marker.ratio)) * 100);
              return (
                <span
                  className={`card-completion-marker${marker.count > 1 ? " card-completion-marker-counted" : ""}`}
                  key={`${markerPercent}-${marker.count}`}
                  style={{ left: `${markerPercent}%` }}
                >
                  {marker.count > 1 ? <span className="card-completion-marker-count">x{marker.count}</span> : null}
                </span>
              );
            })}
          </span>
        ) : null}
      </div>
    </button>
  );
}
