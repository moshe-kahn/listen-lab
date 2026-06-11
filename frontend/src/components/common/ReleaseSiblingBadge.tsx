import { trackRelationTags } from "../../utils/trackRelationTags";

type ReleaseSiblingBadgeProps = {
  className?: string;
  sourceCount?: number | null;
  duplicateSourceCount?: number | null;
  clusterCandidateType?: string | null;
  clusterRelationshipKind?: string | null;
};

export function ReleaseSiblingBadge({
  className = "",
  sourceCount = null,
  duplicateSourceCount = null,
  clusterCandidateType = null,
  clusterRelationshipKind = null,
}: ReleaseSiblingBadgeProps) {
  const tags = trackRelationTags({
    releaseTrackDuplicateSourceCount: duplicateSourceCount,
    releaseTrackSourceCount: sourceCount,
    hasReleaseTrackSiblings: sourceCount != null ? sourceCount > 1 : null,
    releaseTrackClusterCandidateType: clusterCandidateType,
    releaseTrackClusterRelationshipKind: clusterRelationshipKind,
  });
  const title = tags.title || (
    sourceCount && sourceCount > 1
      ? `Recording group: ${sourceCount} tracks`
      : "Recording group"
  );
  return (
    <span aria-label={title} className={`release-sibling-badge relation-tags-badge ${className}`.trim()} title={title}>
      {tags.text || "R"}
    </span>
  );
}
