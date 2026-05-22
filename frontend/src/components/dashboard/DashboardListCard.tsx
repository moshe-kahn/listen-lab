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
  liked,
  releaseSibling,
  releaseSiblingSourceCount,
  secondaryBadgeText,
  completionRatio,
  trackUri,
  previewTrack,
  primaryClamp = "single-line-ellipsis",
  previewKind,
  previewTrackUri,
  onSelectPreview,
}: DashboardListCardComponentProps) {
  const secondaryValue = secondaryText && secondaryText.trim().length > 0 ? secondaryText : "\u00A0";
  const tertiaryValue = tertiaryText && tertiaryText.trim().length > 0 ? tertiaryText : "\u00A0";
  const secondaryPlaceholder = !(secondaryText && secondaryText.trim().length > 0);
  const tertiaryPlaceholder = !(tertiaryText && tertiaryText.trim().length > 0);
  const hasTopRightContent = Boolean(liked || releaseSibling || primaryBadgeText || secondaryBadgeText || metricText);

  return (
    <button
      className="list-row list-link dashboard-card-row"
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
          albumId: previewTrack?.album_id ?? null,
          artistName: previewTrack?.artist_name ?? null,
          sourceTrack: previewTrack ?? null,
        })}
      type="button"
    >
      <div className="dashboard-card-layout">
        <div className="list-primary">
          {imageUrl ? (
            <img alt={imageAlt} className="list-art" src={imageUrl} />
          ) : (
            <div className="list-art list-art-fallback" aria-hidden="true">
              {fallbackLabel}
            </div>
          )}
          <div className="card-copy">
            <div className="card-primary-line">
              <strong className={`card-primary ${primaryClamp}`}>{primaryText}</strong>
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
            {typeof completionRatio === "number" ? (
              <span className="card-completion" aria-hidden="true">
                <span
                  className="card-completion-fill"
                  style={{ width: `${Math.round(Math.max(0, Math.min(1, completionRatio)) * 100)}%` }}
                />
              </span>
            ) : null}
          </div>
        </div>
        {hasTopRightContent ? (
          <div className="card-right-stack">
            {liked || releaseSibling || primaryBadgeText || secondaryBadgeText ? (
              <div className="card-badge-stack">
                {liked ? <LikedBadge className="card-liked-badge" /> : null}
                {releaseSibling ? <ReleaseSiblingBadge className="card-release-sibling-badge" sourceCount={releaseSiblingSourceCount} /> : null}
                {primaryBadgeText ? <span className="card-inline-badge">{primaryBadgeText}</span> : null}
                {secondaryBadgeText ? <span className="card-inline-badge">{secondaryBadgeText}</span> : null}
              </div>
            ) : null}
            {metricText ? <div className="card-metric">{metricText}</div> : null}
          </div>
        ) : null}
      </div>
    </button>
  );
}
