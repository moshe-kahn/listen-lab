import {
  DEBUG_SESSION_BREAK_MS,
  PAGE_SIZE,
  RECENT_RANGE_OPTIONS,
} from "../constants/appConstants";
import type {
  AlbumCatalogLookupItem,
  PreviewItem,
  ProfileResponse,
  RecentRange,
  RecentTrack,
  SectionKey,
  TopAlbum,
  TopPlaylist,
  TrackCatalogLookupItem,
  TrackRankingMode,
} from "../types/appTypes";

export type DebugSession = {
  id: string;
  tracks: RecentTrack[];
  startedAt: number | null;
  endedAt: number | null;
};

export function clampProgress(progressMs: number, durationMs: number) {
  const safeDuration = Math.max(0, Number(durationMs || 0));
  const safeProgress = Math.max(0, Number(progressMs || 0));
  return safeDuration > 0 ? Math.min(safeProgress, safeDuration) : safeProgress;
}

export function formatListeningSince(firstPlayedAt: string | null | undefined) {
  if (!firstPlayedAt) {
    return null;
  }
  const firstDate = new Date(firstPlayedAt);
  if (Number.isNaN(firstDate.getTime())) {
    return null;
  }
  return `Listening since ${firstDate.getUTCFullYear()}`;
}

export function parseTimestampMs(value: string | null | undefined): number | null {
  if (!value) {
    return null;
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed.getTime();
}

export function formatMonthDay(value: string | null | undefined): string | null {
  const parsedMs = parseTimestampMs(value);
  if (parsedMs == null) {
    return null;
  }
  const parsed = new Date(parsedMs);
  return `${parsed.getMonth() + 1}/${parsed.getDate()}`;
}

export function primaryArtistName(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const primary = value.split(",")[0]?.trim() ?? "";
  return primary || null;
}

export function firstArtistFromRecentTrack(track: RecentTrack | null | undefined) {
  return track?.artists?.find((artist) => Boolean(artist?.name || artist?.id || artist?.artist_id)) ?? null;
}

export function previewAlbumHeading(preview: PreviewItem): string {
  const albumName = preview.sourceTrack?.album_name ?? preview.detail ?? "Album";
  const albumYear = preview.sourceTrack?.album_release_year ?? preview.sourceAlbumYear ?? null;
  return albumYear ? `${albumYear} - ${albumName}` : albumName;
}

export function recentRangeLabel(range: RecentRange) {
  return RECENT_RANGE_OPTIONS.find((option) => option.value === range)?.label ?? "Recent";
}

export function albumLookupRowIsNotBackfilled(item: AlbumCatalogLookupItem): boolean {
  return item.catalog_fetched_at === null && item.catalog_last_status === null && item.catalog_last_error === null;
}

export function albumLookupRowHasCatalogError(item: AlbumCatalogLookupItem): boolean {
  return item.catalog_last_status === "error" || Boolean(item.catalog_last_error);
}

export function albumLookupStatusLabel(item: AlbumCatalogLookupItem): "Complete" | "Missing metadata" | "Tracklist incomplete" | "Error" {
  if (albumLookupRowHasCatalogError(item)) {
    return "Error";
  }
  if (albumLookupRowIsNotBackfilled(item)) {
    return "Missing metadata";
  }
  if (!item.tracklist_complete) {
    return "Tracklist incomplete";
  }
  return "Complete";
}

export function albumLookupRowIsIncompleteForEnqueue(item: AlbumCatalogLookupItem): boolean {
  if (!item.spotify_album_id) {
    return false;
  }
  return albumLookupRowIsNotBackfilled(item) || !item.tracklist_complete || albumLookupRowHasCatalogError(item);
}

export function rowIsPendingQueue(queueStatus: string | null | undefined): boolean {
  return String(queueStatus ?? "").trim().toLowerCase() === "pending";
}

export function queueStatusLabel(queueStatus: string | null | undefined): "Not queued" | "Pending" | "Done" | "Error" {
  const normalized = String(queueStatus ?? "").trim().toLowerCase();
  if (normalized === "pending") {
    return "Pending";
  }
  if (normalized === "done") {
    return "Done";
  }
  if (normalized === "error") {
    return "Error";
  }
  return "Not queued";
}

export function albumLookupRowCanBulkPrioritize(item: AlbumCatalogLookupItem): boolean {
  return albumLookupRowIsIncompleteForEnqueue(item) && !rowIsPendingQueue(item.queue_status);
}

export function trackLookupRowHasCatalogError(item: TrackCatalogLookupItem): boolean {
  return item.catalog_last_status === "error" || Boolean(item.catalog_last_error);
}

export function trackLookupRowIsNotBackfilled(item: TrackCatalogLookupItem): boolean {
  return item.catalog_fetched_at === null && item.catalog_last_status === null && item.catalog_last_error === null;
}

export function trackLookupStatusLabel(item: TrackCatalogLookupItem): "Complete" | "Missing duration" | "Missing metadata" | "Error" {
  if (trackLookupRowHasCatalogError(item)) {
    return "Error";
  }
  if (trackLookupRowIsNotBackfilled(item)) {
    return "Missing metadata";
  }
  if (item.duration_ms === null) {
    return "Missing duration";
  }
  return "Complete";
}

export function trackLookupRowIsIncompleteForEnqueue(item: TrackCatalogLookupItem): boolean {
  if (!item.spotify_track_id) {
    return false;
  }
  const statusLabel = trackLookupStatusLabel(item);
  return statusLabel === "Missing metadata" || statusLabel === "Missing duration" || statusLabel === "Error";
}

export function trackLookupRowCanBulkPrioritize(item: TrackCatalogLookupItem): boolean {
  return trackLookupRowIsIncompleteForEnqueue(item) && !rowIsPendingQueue(item.queue_status);
}

export function normalizedTrackArtistKey(trackName: string | null | undefined, artistName: string | null | undefined) {
  return `${(trackName ?? "").trim().toLowerCase()}::${(artistName ?? "").trim().toLowerCase()}`;
}

export function formulaTrackKey(track: RecentTrack) {
  return (
    track.track_id
    ?? track.uri
    ?? normalizedTrackArtistKey(track.track_name, track.artist_name)
  );
}

export function formatFormulaRankDelta(track: RecentTrack): string | null {
  const delta = Number(track.formula_rank_delta ?? 0);
  if (!Number.isFinite(delta) || delta === 0) {
    return null;
  }
  return delta > 0 ? `+${delta}` : String(delta);
}

export function formatTrackSourceBadge(track: RecentTrack): string | null {
  if (track.source_label === "both") {
    return "Both";
  }
  if (track.source_label === "api") {
    return "API";
  }
  if (track.source_label === "recent") {
    return "Recent";
  }
  if (track.source_label === "history") {
    return "History";
  }
  if (track.has_recent_source && track.has_history_source) {
    return "Both";
  }
  if (track.has_recent_source) {
    return "Recent";
  }
  if (track.has_history_source) {
    return "History";
  }
  return null;
}

export function previewImages(items: Array<{ image_url?: string | null }>) {
  return items
    .map((item) => item.image_url)
    .filter((image): image is string => Boolean(image))
    .slice(0, 5);
}

export function previewItems(
  items: Array<{
    image_url?: string | null;
    name?: string | null;
    track_name?: string | null;
    track_id?: string | null;
    release_track_id?: number | null;
    release_track_name?: string | null;
    release_track_source_count?: number | null;
    has_release_track_siblings?: boolean | null;
    artist_id?: string | null;
    artist_name?: string | null;
    album_name?: string | null;
    album_id?: string | null;
    release_year?: string | null;
    uri?: string | null;
    playlist_name?: string | null;
    playlist_id?: string | null;
    description?: string | null;
    track_count?: number | null;
    url?: string | null;
    album_url?: string | null;
    playlist_url?: string | null;
  }>,
) {
  return items
    .map((item) => {
      const label = item.name ?? item.track_name ?? item.playlist_name ?? "";
      const isTrack = Boolean(item.track_name);
      const isPlaylist = Boolean(item.playlist_name);
      const kind: PreviewItem["kind"] = isTrack
        ? "track"
        : isPlaylist
          ? "playlist"
          : item.release_year
            ? "album"
            : item.artist_name
              ? "album"
              : "artist";
      const meta = isTrack
        ? item.artist_name ?? null
        : isPlaylist
          ? item.track_count != null
            ? `${item.track_count} tracks`
            : "Playlist"
          : item.artist_name ?? null;
      const detail = isTrack
        ? item.album_name ?? null
        : item.release_year
          ? item.release_year
          : isPlaylist
            ? item.description?.trim() || null
            : null;

      return {
        image: item.image_url ?? null,
        label,
        meta,
        detail,
        kind,
        entityId: isTrack
          ? item.track_id ?? null
          : isPlaylist
            ? item.playlist_id ?? null
            : item.album_id ?? item.artist_id ?? null,
        trackUri: item.uri ?? null,
        url: item.url ?? item.album_url ?? item.playlist_url ?? "",
        trackId: isTrack ? item.track_id ?? null : null,
        releaseTrackId: isTrack ? item.release_track_id ?? null : null,
        releaseTrackName: isTrack ? item.release_track_name ?? null : null,
        releaseTrackSourceCount: isTrack ? item.release_track_source_count ?? null : null,
        hasReleaseTrackSiblings: isTrack ? item.has_release_track_siblings ?? null : null,
        albumId: isTrack || kind === "album" ? item.album_id ?? null : null,
        artistName: isTrack || kind === "album" ? item.artist_name ?? null : null,
        artists: kind === "album" && item.artist_name
          ? item.artist_name.split(",").map((name) => ({ name: name.trim() })).filter((artist) => Boolean(artist.name))
          : null,
        sourceAlbumId: kind === "album" ? item.album_id ?? null : null,
        sourceAlbumName: kind === "album" ? label : null,
        sourceAlbumImage: kind === "album" ? item.image_url ?? null : null,
        sourceAlbumUrl: kind === "album" ? item.url ?? item.album_url ?? "" : null,
        sourceAlbumYear: kind === "album" ? item.release_year ?? null : null,
        sourceTrack: isTrack ? item as RecentTrack : null,
      } satisfies PreviewItem;
    })
    .filter((item) => Boolean(item.label && item.url))
    .slice(0, 5);
}

export function emptySlots<T>(items: T[]) {
  return Math.max(0, PAGE_SIZE - items.length);
}

export function splitItems<T>(items: T[]) {
  const midpoint = Math.ceil(items.length / 2);
  return {
    left: items.slice(0, midpoint),
    right: items.slice(midpoint),
  };
}

export function formatAlbumSummary(album: TopAlbum) {
  const names = album.represented_track_names.filter(Boolean);
  if (names.length === 0) {
    return `${album.track_representation_count} tracks represented`;
  }
  if (names.length <= 2) {
    return names.join(" | ");
  }
  return `${names.slice(0, 2).join(", ")} +${names.length - 2} more`;
}

export function formatAlbumBreadth(album: TopAlbum) {
  const count = Math.max(0, album.track_representation_count ?? 0);
  return count === 1 ? "1 track" : `${count} tracks`;
}

export function formatHistoryDebugLine(item: {
  debug?: {
    source?: string;
    score?: number;
    total_ms?: number;
    play_count?: number;
    distinct_tracks?: number;
  };
}) {
  if (item.debug?.source !== "history") {
    return null;
  }

  const hours = item.debug.total_ms != null ? `${(item.debug.total_ms / 3_600_000).toFixed(1)}h` : null;
  const plays = item.debug.play_count != null ? `${item.debug.play_count} plays` : null;
  const tracks = item.debug.distinct_tracks != null ? `${item.debug.distinct_tracks} tracks` : null;
  return [hours, plays, tracks].filter(Boolean).join(" | ");
}

export function formatTrackLongevity(track: RecentTrack) {
  const spanDays = Math.max(0, Math.floor(track.listening_span_days ?? 0));
  if (spanDays <= 0) {
    return null;
  }
  if (spanDays >= 365) {
    const years = spanDays / 365.25;
    return years >= 10 ? `${Math.round(years)}y` : `${years.toFixed(1)}y`;
  }
  if (spanDays >= 30) {
    const months = Math.floor(spanDays / 30);
    return `${months}mo`;
  }
  return `${spanDays}d`;
}

export function formatTrackLongevityMetric(track: RecentTrack) {
  const longevity = formatTrackLongevity(track);
  const consistency = Math.max(0, Math.min(1, Number(track.consistency_ratio ?? 0)));
  if (!longevity) {
    return null;
  }
  return `${longevity} | ${Math.round(consistency * 100)}%`;
}

export function getTrackLongevityScore(track: RecentTrack) {
  return Number(track.longevity_score ?? 0);
}

export function formatTrackLongevitySortMetric(track: RecentTrack) {
  const score = getTrackLongevityScore(track);
  if (score <= 0) {
    return null;
  }
  const longevity = formatTrackLongevity(track) ?? "0d";
  return `${score.toFixed(2)} | ${longevity}`;
}

export function formatTrackRankingMetric(track: RecentTrack, trackRankingMode: TrackRankingMode) {
  if (trackRankingMode === "plays") {
    const plays = getTrackPlayCount(track);
    return plays > 0 ? `${plays} plays` : null;
  }
  if (trackRankingMode === "longevity") {
    return formatTrackLongevitySortMetric(track);
  }
  const plays = getTrackPlayCount(track);
  const longevityMetric = formatTrackLongevitySortMetric(track);
  if (plays > 0 && longevityMetric) {
    return `${plays} plays | ${longevityMetric}`;
  }
  if (plays > 0) {
    return `${plays} plays`;
  }
  return longevityMetric;
}

export function baselineFormulaLabel(trackRankingMode: TrackRankingMode) {
  if (trackRankingMode === "plays") {
    return "raw plays";
  }
  if (trackRankingMode === "longevity") {
    return "linear longevity";
  }
  return "linear blend";
}

export function candidateFormulaLabel(trackRankingMode: TrackRankingMode) {
  if (trackRankingMode === "plays") {
    return "recent-boosted plays";
  }
  if (trackRankingMode === "longevity") {
    return "recent-boosted longevity";
  }
  return "recent-boosted blend";
}

export function formatTrackLongevityWithConsistency(track: RecentTrack) {
  const longevity = formatTrackLongevity(track);
  const consistency = Math.max(0, Math.min(1, Number(track.consistency_ratio ?? 0)));
  if (!longevity) {
    return null;
  }
  const consistencyPercent = Math.round(consistency * 100);
  return `${longevity} · ${consistencyPercent}%`;
}

export function getTrackPlayCount(track: RecentTrack): number {
  return Number(track.play_count ?? 0);
}

export function getNormalizedPlays(track: RecentTrack, maxPlays: number): number {
  return getTrackPlayCount(track) / Math.max(1, maxPlays);
}

export function getNormalizedLongevity(track: RecentTrack, maxLongevity: number): number {
  return getTrackLongevityScore(track) / Math.max(1, maxLongevity);
}

export function getOldPlaysScore(track: RecentTrack, maxPlays: number): number {
  return getTrackPlayCount(track) / Math.max(1, maxPlays);
}

export function getNewPlaysScore(track: RecentTrack, maxPlays: number): number {
  const normalized = getTrackPlayCount(track) / Math.max(1, maxPlays);
  const recentBoost = Number(track.recent_play_count ?? 0) > 0 ? 0.35 : 0;
  return Math.sqrt(normalized) * 0.65 + recentBoost;
}

export function getOldLongevityScore(track: RecentTrack, maxLongevity: number): number {
  return getTrackLongevityScore(track) / Math.max(1, maxLongevity);
}

export function getNewLongevityScore(track: RecentTrack, maxLongevity: number): number {
  const normalized = getTrackLongevityScore(track) / Math.max(1, maxLongevity);
  const recentBoost = Number(track.recent_play_count ?? 0) > 0 ? 0.25 : 0;
  return Math.sqrt(normalized) * 0.75 + recentBoost;
}

export function getOldMixScore(
  track: RecentTrack,
  maxPlays: number,
  maxLongevity: number,
): number {
  return getOldPlaysScore(track, maxPlays) * 0.58
    + getOldLongevityScore(track, maxLongevity) * 0.42;
}

export function getNewMixScore(
  track: RecentTrack,
  maxPlays: number,
  maxLongevity: number,
): number {
  return getNewPlaysScore(track, maxPlays) * 0.70
    + getNewLongevityScore(track, maxLongevity) * 0.30;
}

export function sortedTracksForView(section: SectionKey, tracks: RecentTrack[], trackRankingMode: TrackRankingMode) {
  const isCurrentSection = section === "tracksAllTime" || section === "tracksAllTimeCurrent";
  const isNewSection = section === "tracksAllTimeNew";

  if (!isCurrentSection && !isNewSection) {
    return tracks;
  }

  const withMetrics = tracks.some(
    (track) =>
      getTrackPlayCount(track) > 0 ||
      Number(track.listening_span_days ?? 0) > 0 ||
      getTrackLongevityScore(track) > 0,
  );

  if (!withMetrics) {
    return tracks;
  }

  const ranked = [...tracks];
  const maxPlays = Math.max(1, ...ranked.map((track) => getTrackPlayCount(track)));
  const maxLongevity = Math.max(1, ...ranked.map((track) => getTrackLongevityScore(track)));

  ranked.sort((a, b) => {
    let aScore = 0;
    let bScore = 0;

    if (isNewSection) {
      if (trackRankingMode === "plays") {
        aScore = getNewPlaysScore(a, maxPlays);
        bScore = getNewPlaysScore(b, maxPlays);
      } else if (trackRankingMode === "longevity") {
        aScore = getNewLongevityScore(a, maxLongevity);
        bScore = getNewLongevityScore(b, maxLongevity);
      } else {
        aScore = getNewMixScore(a, maxPlays, maxLongevity);
        bScore = getNewMixScore(b, maxPlays, maxLongevity);
      }
    } else {
      if (trackRankingMode === "plays") {
        aScore = getOldPlaysScore(a, maxPlays);
        bScore = getOldPlaysScore(b, maxPlays);
      } else if (trackRankingMode === "longevity") {
        aScore = getOldLongevityScore(a, maxLongevity);
        bScore = getOldLongevityScore(b, maxLongevity);
      } else {
        aScore = getOldMixScore(a, maxPlays, maxLongevity);
        bScore = getOldMixScore(b, maxPlays, maxLongevity);
      }
    }

    const scoreDelta = bScore - aScore;
    if (Math.abs(scoreDelta) > 1e-6) {
      return scoreDelta;
    }

    const playsDelta = getTrackPlayCount(b) - getTrackPlayCount(a);
    if (playsDelta !== 0) {
      return playsDelta;
    }

    const longevityDelta = getTrackLongevityScore(b) - getTrackLongevityScore(a);
    if (Math.abs(longevityDelta) > 1e-6) {
      return longevityDelta;
    }

    const spanDelta = Number(b.listening_span_days ?? 0) - Number(a.listening_span_days ?? 0);
    if (spanDelta !== 0) {
      return spanDelta;
    }

    return 0;
  });

  return ranked;
}

export function albumGroupingKey(track: RecentTrack) {
  if (track.album_id) {
    return track.album_id;
  }
  const albumName = (track.album_name ?? "").trim().toLowerCase();
  const artistName = (track.artist_name ?? "").trim().toLowerCase();
  if (!albumName) {
    return null;
  }
  return `${albumName}::${artistName}`;
}

export function capTracksPerAlbum(items: RecentTrack[], maxPerAlbum: number) {
  const albumTotals = new Map<string, number>();
  items.forEach((track) => {
    const key = albumGroupingKey(track);
    if (!key) {
      return;
    }
    albumTotals.set(key, (albumTotals.get(key) ?? 0) + 1);
  });

  const albumSeen = new Map<string, number>();
  const rows: Array<{ track: RecentTrack; hiddenCount: number }> = [];

  items.forEach((track) => {
    const key = albumGroupingKey(track);
    if (!key) {
      rows.push({ track, hiddenCount: 0 });
      return;
    }

    const seen = albumSeen.get(key) ?? 0;
    if (seen >= maxPerAlbum) {
      return;
    }

    const totalInAlbum = albumTotals.get(key) ?? 0;
    const hiddenCount = seen + 1 === maxPerAlbum ? Math.max(0, totalInAlbum - maxPerAlbum) : 0;
    rows.push({ track, hiddenCount });
    albumSeen.set(key, seen + 1);
  });

  return rows;
}

export function collapseRecentPreviewTracks(items: RecentTrack[]) {
  const collapsed: RecentTrack[] = [];
  const seenAlbumKeys = new Set<string>();
  for (const track of items) {
    const albumKey = albumGroupingKey(track);
    if (albumKey) {
      if (seenAlbumKeys.has(albumKey)) {
        continue;
      }
      seenAlbumKeys.add(albumKey);
    }
    collapsed.push(track);
  }
  return collapsed;
}

export function collapseTrackPreviewAlbums(items: RecentTrack[]) {
  const collapsed: RecentTrack[] = [];
  const seenAlbumKeys = new Set<string>();
  for (const track of items) {
    const albumKey = albumGroupingKey(track);
    if (albumKey) {
      if (seenAlbumKeys.has(albumKey)) {
        continue;
      }
      seenAlbumKeys.add(albumKey);
    }
    collapsed.push(track);
  }
  return collapsed;
}

export function formatLoadingStatusDetailed(phase: string | null, elapsedSeconds: number) {
  const elapsed = `${elapsedSeconds.toFixed(1)}s`;
  return phase ? `Working on ${phase}... (${elapsed})` : `Loading your Spotify data... (${elapsed})`;
}

export function formatLoadingStatusUi(phase: string | null) {
  if (!phase) {
    return "Loading your Spotify data...";
  }
  const normalized = phase.toLowerCase();
  if (normalized.includes("followed artist count")) {
    return "Checking artist count...";
  }
  if (normalized.includes("top artists all time")) {
    return "Loading top artists...";
  }
  if (normalized.includes("top artists recent")) {
    return "Loading recent artists...";
  }
  if (normalized.includes("top tracks all time")) {
    return "Loading top tracks...";
  }
  if (normalized.includes("top tracks recent")) {
    return "Loading recent tracks...";
  }
  if (normalized.includes("recent listening")) {
    return "Loading recent activity...";
  }
  if (normalized.includes("liked tracks")) {
    return "Loading liked tracks...";
  }
  if (normalized.includes("profile")) {
    return "Loading profile...";
  }
  if (normalized.includes("playlist")) {
    return "Loading playlists...";
  }
  if (normalized.includes("albums")) {
    return "Loading albums...";
  }
  if (normalized.includes("analyzing")) {
    return "Analyzing listening history...";
  }
  if (normalized.includes("formula") || normalized.includes("metrics")) {
    return "Analyzing listening patterns...";
  }
  if (normalized.includes("precomputed local insights")) {
    return "Loading local analysis cache...";
  }
  if (normalized.includes("local analysis cache write")) {
    return "Loading local cache updates...";
  }
  if (normalized.includes("loading per-user cached recent sections")) {
    return "Loading cached recent sections...";
  }
  if (normalized.includes("history")) {
    return "Analyzing listening history...";
  }
  if (normalized.includes("finishing") || normalized.includes("complete")) {
    return "Finalizing dashboard...";
  }
  return "Loading your Spotify data...";
}

export function formatPlaylistSummary(playlist: TopPlaylist, mode: "recent" | "allTime") {
  const matches =
    mode === "recent"
      ? [
          playlist.match_counts.short_term_top > 0
            ? `${playlist.match_counts.short_term_top} top tracks`
            : null,
          playlist.match_counts.recently_played > 0
            ? `${playlist.match_counts.recently_played} recently played`
            : null,
          playlist.match_counts.liked > 0
            ? `${playlist.match_counts.liked} liked tracks`
            : null,
        ]
      : [
          playlist.match_counts.long_term_top > 0
            ? `${playlist.match_counts.long_term_top} top tracks`
            : null,
          playlist.match_counts.liked > 0
            ? `${playlist.match_counts.liked} liked tracks`
            : null,
          playlist.track_count != null ? `${playlist.track_count} total tracks` : null,
        ];

  return matches.filter(Boolean).join(" | ") || `${playlist.track_count ?? 0} tracks`;
}

export function mergeExtendedProfile(previous: ProfileResponse | null, next: ProfileResponse) {
  if (!previous) {
    return next;
  }

  return {
    ...next,
    recent_tracks: previous.recent_tracks.length > 0 ? previous.recent_tracks : next.recent_tracks,
    recent_tracks_available: previous.recent_tracks_available || next.recent_tracks_available,
    recent_likes_tracks: previous.recent_likes_tracks.length > 0 ? previous.recent_likes_tracks : next.recent_likes_tracks,
    recent_likes_available: previous.recent_likes_available || next.recent_likes_available,
  };
}

export function formatRelativeSyncTime(timestampSeconds?: number | null) {
  if (!timestampSeconds || !Number.isFinite(timestampSeconds)) {
    return null;
  }
  const deltaSeconds = Math.max(0, Math.round(Date.now() / 1000 - timestampSeconds));
  if (deltaSeconds < 60) {
    return "just now";
  }
  if (deltaSeconds < 3600) {
    const minutes = Math.floor(deltaSeconds / 60);
    return `${minutes} minute${minutes === 1 ? "" : "s"} ago`;
  }
  if (deltaSeconds < 86_400) {
    const hours = Math.floor(deltaSeconds / 3600);
    return `${hours} hour${hours === 1 ? "" : "s"} ago`;
  }
  const days = Math.floor(deltaSeconds / 86_400);
  return `${days} day${days === 1 ? "" : "s"} ago`;
}

export function parseCooldownSeconds(detail: string) {
  const secondMatch = detail.match(/about (\d+) seconds/i);
  if (secondMatch) {
    const parsedSeconds = Number(secondMatch[1]);
    if (Number.isFinite(parsedSeconds)) {
      return Math.min(600, Math.max(1, Math.round(parsedSeconds)));
    }
  }
  const minuteMatch = detail.match(/about (\d+) minutes/i);
  if (minuteMatch) {
    const parsedMinutes = Number(minuteMatch[1]);
    if (Number.isFinite(parsedMinutes)) {
      return Math.min(600, Math.max(60, Math.round(parsedMinutes * 60)));
    }
  }
  return null;
}

export function formatCooldownCopy(totalSeconds: number) {
  if (totalSeconds >= 120) {
    const minutes = Math.ceil(totalSeconds / 60);
    return `Spotify is rate-limiting requests right now. Try again in about ${minutes} minutes.`;
  }
  return "Spotify is rate-limiting requests right now. Try again in about a minute.";
}

export function formatCooldownTimerLabel(totalSeconds: number) {
  const safeSeconds = Math.max(0, totalSeconds);
  const minutes = Math.floor(safeSeconds / 60);
  const seconds = safeSeconds % 60;
  return `${minutes}:${seconds.toString().padStart(2, "0")}`;
}

export function formatUiErrorMessage(error: unknown, fallback: string) {
  const raw = error instanceof Error ? error.message : "";
  const lower = raw.toLowerCase();
  if (
    error instanceof TypeError
    || lower.includes("failed to fetch")
    || lower.includes("networkerror")
    || lower.includes("network request failed")
  ) {
    return "Can’t reach ListenLab API. Start backend on 127.0.0.1:8000 and refresh.";
  }
  if (lower.includes("cors")) {
    return "Backend blocked by CORS. Use localhost/127.0.0.1 defaults.";
  }
  return raw || fallback;
}

export function formatDurationMs(value: unknown): string {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    return "Unknown";
  }
  const totalSeconds = Math.round(value / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}:${String(seconds).padStart(2, "0")}`;
}

export function formatDebugTimestamp(value: unknown): string {
  if (typeof value !== "string" || !value) {
    return "Unknown";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString();
}

export function formatTimeOnly(value: unknown): string {
  if (typeof value !== "string" || !value) {
    return "Unknown";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" });
}

export function trackEstimatedMs(track: RecentTrack): number | null {
  if (typeof track.estimated_played_ms === "number" && Number.isFinite(track.estimated_played_ms)) {
    return Math.max(0, track.estimated_played_ms);
  }
  if (typeof track.duration_ms === "number" && Number.isFinite(track.duration_ms)) {
    return Math.max(0, track.duration_ms);
  }
  return null;
}

export function formatDebugLabel(key: string): string {
  if (key === "spotify_played_at") {
    return "Played at";
  }
  if (key === "played_at_gap_ms") {
    return "Gap to previous play";
  }
  return key
    .replace(/_/g, " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

export function isLinkOrUriField(key: string, value: unknown): boolean {
  if (key.toLowerCase().includes("url") || key.toLowerCase().includes("uri") || key.toLowerCase().includes("href")) {
    return true;
  }
  if (typeof value === "string" && /^https?:\/\//i.test(value)) {
    return true;
  }
  return false;
}

export function isComputedField(key: string): boolean {
  if (key.startsWith("estimated_")) {
    return true;
  }
  return [
    "played_at_gap_ms",
    "duration_seconds",
    "spotify_played_at_unix_ms",
    "play_count",
    "all_time_play_count",
    "recent_play_count",
    "first_played_at",
    "last_played_at",
    "listening_span_days",
    "listening_span_years",
    "active_months_count",
    "span_months_count",
    "consistency_ratio",
    "longevity_score",
    "estimated_completion_ratio",
  ].includes(key);
}

export function formatDebugValue(key: string, value: unknown): string {
  if (value === null) {
    return "null";
  }
  if (value === undefined) {
    return "undefined";
  }
  if (key === "artists" && Array.isArray(value)) {
    const names = value
      .map((artist) => (artist && typeof artist === "object" ? (artist as { name?: unknown }).name : null))
      .filter((name): name is string => typeof name === "string" && name.length > 0);
    return names.length > 0 ? names.join(", ") : "[]";
  }
  if (key.endsWith("_at")) {
    return formatDebugTimestamp(value);
  }
  if (key === "duration_ms" || key === "estimated_played_ms") {
    return formatDurationMs(value);
  }
  if (key === "played_at_gap_ms") {
    return formatDurationMs(value);
  }
  if (key === "duration_seconds" || key === "estimated_played_seconds") {
    return typeof value === "number" ? formatDurationMs(value * 1000) : String(value);
  }
  if (key === "estimated_completion_ratio" || key === "consistency_ratio") {
    return typeof value === "number" ? `${(value * 100).toFixed(1)}%` : String(value);
  }
  if (typeof value === "string") {
    return value.length > 0 ? value : '""';
  }
  if (typeof value === "object") {
    try {
      return JSON.stringify(value);
    } catch {
      return "[unserializable value]";
    }
  }
  return String(value);
}

export function trackPlayedAtMs(track: RecentTrack): number | null {
  if (!track.spotify_played_at) {
    return null;
  }
  const parsed = new Date(track.spotify_played_at);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed.getTime();
}

export function buildDebugSessions(tracks: RecentTrack[]): DebugSession[] {
  const sortedTracks = [...tracks].sort((a, b) => {
    const aMs = trackPlayedAtMs(a) ?? -1;
    const bMs = trackPlayedAtMs(b) ?? -1;
    return bMs - aMs;
  });
  const sessions: DebugSession[] = [];
  let currentTracks: RecentTrack[] = [];
  let previousTrackMs: number | null = null;

  for (const track of sortedTracks) {
    const currentTrackMs = trackPlayedAtMs(track);
    const startsNewSession = (
      currentTracks.length > 0
      && (
        currentTrackMs == null
        || previousTrackMs == null
        || previousTrackMs - currentTrackMs > DEBUG_SESSION_BREAK_MS
        || previousTrackMs < currentTrackMs
      )
    );

    if (startsNewSession) {
      const allTimes = currentTracks
        .map((sessionTrack) => trackPlayedAtMs(sessionTrack))
        .filter((value): value is number => value != null);
      const startedAt = allTimes.length > 0 ? Math.max(...allTimes) : null;
      const endedAt = allTimes.length > 0 ? Math.min(...allTimes) : null;
      sessions.push({
        id: `session-${sessions.length + 1}-${startedAt ?? "na"}-${endedAt ?? "na"}`,
        tracks: currentTracks,
        startedAt,
        endedAt,
      });
      currentTracks = [];
    }

    currentTracks.push(track);
    previousTrackMs = currentTrackMs;
  }

  if (currentTracks.length > 0) {
    const allTimes = currentTracks
      .map((sessionTrack) => trackPlayedAtMs(sessionTrack))
      .filter((value): value is number => value != null);
    const startedAt = allTimes.length > 0 ? Math.max(...allTimes) : null;
    const endedAt = allTimes.length > 0 ? Math.min(...allTimes) : null;
    sessions.push({
      id: `session-${sessions.length + 1}-${startedAt ?? "na"}-${endedAt ?? "na"}`,
      tracks: currentTracks,
      startedAt,
      endedAt,
    });
  }

  return sessions;
}

export function formatSessionRange(session: DebugSession): string {
  if (session.startedAt == null || session.endedAt == null) {
    return "Time range unavailable";
  }
  const earlierMs = Math.min(session.startedAt, session.endedAt);
  const laterMs = Math.max(session.startedAt, session.endedAt);
  const earlier = new Date(earlierMs);
  const later = new Date(laterMs);
  const sameLocalDate = (
    earlier.getFullYear() === later.getFullYear()
    && earlier.getMonth() === later.getMonth()
    && earlier.getDate() === later.getDate()
  );
  if (sameLocalDate) {
    const dateText = earlier.toLocaleDateString();
    const startTime = earlier.toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" });
    const endTime = later.toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" });
    return `${dateText} ${startTime} - ${endTime}`;
  }
  const startText = earlier.toLocaleString();
  const endText = later.toLocaleString();
  return `${startText} - ${endText}`;
}

export function debugTrackKey(sessionId: string, track: RecentTrack, index: number): string {
  return `${sessionId}-${track.spotify_played_at ?? "na"}-${track.track_id ?? "no-id"}-${index}`;
}
