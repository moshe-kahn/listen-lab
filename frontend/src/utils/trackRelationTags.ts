export type TrackRelationTagEntry = {
  code: "D" | "R" | "V" | "C";
  label: string;
};

const coverRemixRelationshipKinds = new Set(["remix", "rework", "cover"]);

export type TrackRelationTagInput = {
  releaseTrackDuplicateSourceCount?: number | null;
  releaseTrackSourceCount?: number | null;
  hasReleaseTrackSiblings?: boolean | null;
  releaseTrackClusterCandidateType?: string | null;
  releaseTrackClusterRelationshipKind?: string | null;
};

export function trackRelationTagEntries(input: TrackRelationTagInput): TrackRelationTagEntry[] {
  const duplicateCount = Number(input.releaseTrackDuplicateSourceCount ?? 0);
  const hasDuplicateSources = duplicateCount > 1;
  const isRecordingGroup = input.releaseTrackClusterCandidateType === "recording_track_candidate";
  const isTrackFamily = input.releaseTrackClusterCandidateType === "track_family_candidate";
  const isCoverRemixFamily = isTrackFamily && coverRemixRelationshipKinds.has(input.releaseTrackClusterRelationshipKind ?? "");
  const isVariationFamily = isTrackFamily && !isCoverRemixFamily;
  const hasLegacySiblingOnly = !isRecordingGroup
    && !isTrackFamily
    && !hasDuplicateSources
    && Boolean(input.hasReleaseTrackSiblings || Number(input.releaseTrackSourceCount ?? 0) > 1);

  return [
    hasDuplicateSources ? { code: "D" as const, label: "duplicate source grouping" } : null,
    isRecordingGroup || hasLegacySiblingOnly ? { code: "R" as const, label: "recording group" } : null,
    isVariationFamily ? { code: "V" as const, label: "variation" } : null,
    isCoverRemixFamily ? { code: "C" as const, label: "cover/remix/rework" } : null,
  ].filter((entry): entry is TrackRelationTagEntry => Boolean(entry));
}

export function trackRelationTags(input: TrackRelationTagInput) {
  const entries = trackRelationTagEntries(input);
  return {
    entries,
    text: entries.map((entry) => entry.code).join(""),
    title: entries.length > 0
      ? `Track relation: ${entries.map((entry) => entry.label).join(", ")}`
      : "",
  };
}
