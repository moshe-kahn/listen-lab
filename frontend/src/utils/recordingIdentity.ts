export type RecordingIdentityLike = {
  release_track_id?: number | null;
  recording_release_track_ids?: number[] | null;
};

export function recordingIdentityTokens(item: RecordingIdentityLike): string[] {
  const recordingReleaseTrackIds = (item.recording_release_track_ids ?? [])
    .filter((value): value is number => Number.isFinite(value))
    .sort((left, right) => left - right);
  if (recordingReleaseTrackIds.length > 0) {
    return recordingReleaseTrackIds.map((releaseTrackId) => `recording:${releaseTrackId}`);
  }
  return item.release_track_id != null ? [`release:${item.release_track_id}`] : [];
}

export function recordingIdentityReleaseTrackIds(item: RecordingIdentityLike): number[] {
  const ids = new Set<number>();
  if (typeof item.release_track_id === "number" && Number.isFinite(item.release_track_id)) {
    ids.add(item.release_track_id);
  }
  for (const releaseTrackId of item.recording_release_track_ids ?? []) {
    if (typeof releaseTrackId === "number" && Number.isFinite(releaseTrackId)) {
      ids.add(releaseTrackId);
    }
  }
  return Array.from(ids);
}

export function recordingIdentityMatchesAnyReleaseTrackId(
  item: RecordingIdentityLike,
  releaseTrackIds?: Set<number> | null,
) {
  if (!releaseTrackIds || releaseTrackIds.size === 0) {
    return false;
  }
  return recordingIdentityReleaseTrackIds(item).some((releaseTrackId) => releaseTrackIds.has(releaseTrackId));
}

export function recordingIdentitiesOverlap(left: RecordingIdentityLike, right: RecordingIdentityLike) {
  const leftTokens = new Set(recordingIdentityTokens(left));
  return recordingIdentityTokens(right).some((token) => leftTokens.has(token));
}

export function mergeRowsBySharedRecordingIdentity<T>(
  rows: T[],
  tokensForRow: (row: T) => string[],
): T[][] {
  const groups: Array<{ tokens: Set<string>; rows: T[] }> = [];
  rows.forEach((row) => {
    const rowTokens = new Set(tokensForRow(row));
    const matchingGroups = groups.filter((group) => [...rowTokens].some((token) => group.tokens.has(token)));
    if (matchingGroups.length === 0) {
      groups.push({ tokens: rowTokens, rows: [row] });
      return;
    }
    const targetGroup = matchingGroups[0];
    targetGroup.rows.push(row);
    rowTokens.forEach((token) => targetGroup.tokens.add(token));
    matchingGroups.slice(1).forEach((group) => {
      group.rows.forEach((groupRow) => targetGroup.rows.push(groupRow));
      group.tokens.forEach((token) => targetGroup.tokens.add(token));
      groups.splice(groups.indexOf(group), 1);
    });
  });
  return groups.map((group) => group.rows);
}
