import { PLAYER_RECENT_FETCH_LIMIT } from "../constants/appConstants";
import type { PlayerQueueTrack, PlayerTrackSummary, RecentPlayFilter, RecentTrack, SpotifyPlayerState } from "../types/appTypes";

export const QUEUE_PLAYLIST_URI_LIMIT = 100;

export function currentTrackFromState(state: SpotifyPlayerState): PlayerTrackSummary {
  const current = state.track_window.current_track;
  return {
    name: current.name,
    artists: current.artists.map((artist) => artist.name).join(", "),
    album: current.album.name,
    image: current.album.images[0]?.url ?? null,
    uri: current.uri,
    durationMs: state.duration || current.duration_ms || 0,
  };
}

export function spotifyTrackUrl(trackUri: string | null) {
  if (!trackUri?.startsWith("spotify:track:")) {
    return null;
  }
  const trackId = trackUri.split(":")[2];
  return trackId ? `https://open.spotify.com/track/${trackId}` : null;
}

export function spotifyTrackIdFromUri(trackUri: string | null) {
  if (!trackUri?.startsWith("spotify:track:")) {
    return null;
  }
  const trackId = trackUri.split(":")[2];
  return trackId || null;
}

function playerRecentTrackKey(track: RecentTrack) {
  const id = track.track_id?.trim();
  if (id) {
    return `id:${id}`;
  }
  const uri = track.uri?.trim();
  if (uri) {
    return `uri:${uri}`;
  }
  return `text:${(track.track_name ?? "").trim().toLocaleLowerCase()}::${(track.artist_name ?? "").trim().toLocaleLowerCase()}`;
}

export function activityRecentTrackKey(track: RecentTrack) {
  const releaseTrackId = track.release_track_id;
  if (typeof releaseTrackId === "number" && Number.isFinite(releaseTrackId) && releaseTrackId > 0) {
    return `release_track:${releaseTrackId}`;
  }
  const id = track.track_id?.trim();
  if (id) {
    return `spotify_track:${id}`;
  }
  return `text:${(track.track_name ?? "").trim().toLocaleLowerCase()}::${(track.artist_name ?? "").trim().toLocaleLowerCase()}`;
}

function recentTrackIsComplete(track: RecentTrack) {
  if (typeof track.estimated_completion_ratio === "number" && track.estimated_completion_ratio >= 0.98) {
    return true;
  }
  const estimatedPlayedMs = Number(track.estimated_played_ms ?? 0);
  const durationMs = Number(track.duration_ms ?? 0);
  return durationMs > 0 && estimatedPlayedMs >= durationMs * 0.98;
}

export function recentTrackCompletionRatio(track: RecentTrack) {
  if (typeof track.estimated_completion_ratio === "number" && Number.isFinite(track.estimated_completion_ratio)) {
    return Math.max(0, Math.min(1, track.estimated_completion_ratio));
  }
  const estimatedPlayedMs = Number(track.estimated_played_ms ?? 0);
  const durationMs = Number(track.duration_ms ?? 0);
  if (durationMs > 0 && Number.isFinite(estimatedPlayedMs)) {
    return Math.max(0, Math.min(1, estimatedPlayedMs / durationMs));
  }
  return null;
}

function recentTrackMatchesFilter(track: RecentTrack, filter: RecentPlayFilter) {
  if (filter === "all") {
    return true;
  }
  const ratio = recentTrackCompletionRatio(track);
  if (ratio === null) {
    return false;
  }
  return filter === "listened"
    ? ratio >= 0.65
    : ratio < 0.65;
}

export function filterAndDedupeRecentTracksForActivity(
  tracks: RecentTrack[],
  filter: RecentPlayFilter,
  limit = tracks.length,
  likedTrackIds?: Set<string>,
  likedReleaseTrackIds?: Set<number>,
) {
  const groups = new Map<string, RecentTrack[]>();
  const orderedKeys: string[] = [];
  for (const track of tracks) {
    if (filter === "liked") {
      const isKnownLiked = Boolean(
        track.is_liked === true
        || track.source_label === "liked_cache"
        || (typeof track.release_track_id === "number" && likedReleaseTrackIds?.has(track.release_track_id))
        || (track.track_id && likedTrackIds?.has(track.track_id)),
      );
      if (!isKnownLiked) {
        continue;
      }
    } else if (!recentTrackMatchesFilter(track, filter)) {
      continue;
    }
    const key = activityRecentTrackKey(track);
    const group = groups.get(key);
    if (group) {
      group.push(track);
    } else {
      groups.set(key, [track]);
      orderedKeys.push(key);
    }
  }

  const unique: RecentTrack[] = [];
  for (const key of orderedKeys) {
    const group = groups.get(key) ?? [];
    if (group.length === 0) {
      continue;
    }
    const representative = group.reduce((best, candidate) => {
      const bestRatio = recentTrackCompletionRatio(best);
      const candidateRatio = recentTrackCompletionRatio(candidate);
      if (candidateRatio === null) {
        return best;
      }
      if (bestRatio === null || candidateRatio > bestRatio) {
        return candidate;
      }
      return best;
    }, group[0]);
    unique.push({
      ...representative,
      filtered_play_count: group.length,
    });
    if (unique.length >= limit) {
      break;
    }
  }
  return unique;
}

export function dedupeRecentTracksForPlayer(tracks: RecentTrack[], limit = PLAYER_RECENT_FETCH_LIMIT) {
  const groups = new Map<string, RecentTrack[]>();
  const orderedKeys: string[] = [];
  for (const track of tracks) {
    const key = playerRecentTrackKey(track);
    const group = groups.get(key);
    if (group) {
      group.push(track);
    } else {
      groups.set(key, [track]);
      orderedKeys.push(key);
    }
  }

  const unique: RecentTrack[] = [];
  for (const key of orderedKeys) {
    const group = groups.get(key) ?? [];
    if (group.length === 0) {
      continue;
    }
    const completedTracks = group.filter(recentTrackIsComplete);
    const representative = completedTracks[0] ?? group[0];
    unique.push({
      ...representative,
      estimated_completion_ratio: completedTracks.length > 0
        ? Math.max(1, Number(representative.estimated_completion_ratio ?? 0))
        : representative.estimated_completion_ratio,
      completed_play_count: completedTracks.length,
    });
    if (unique.length >= limit) {
      break;
    }
  }
  return unique;
}

export function recentTracksToPlayerQueueTracks(tracks: RecentTrack[]): PlayerQueueTrack[] {
  return tracks.map((track) => {
    const uri = trackUriWithFallback(track.uri, track.track_id);
    return {
      name: track.track_name ?? "Unknown track",
      artists: track.artist_name ?? "Unknown artist",
      album: track.album_name ?? "Unknown album",
      image: track.image_url ?? null,
      uri,
      durationMs: Math.max(0, Number(track.duration_ms ?? 0)),
      trackId: track.track_id ?? spotifyTrackIdFromUri(uri),
      releaseTrackId: track.release_track_id ?? null,
      releaseTrackName: track.release_track_name ?? null,
      releaseTrackSourceCount: track.release_track_source_count ?? null,
      hasReleaseTrackSiblings: track.has_release_track_siblings ?? null,
      albumId: track.album_id ?? null,
      artistItems: track.artists ?? null,
      isLiked: track.is_liked ?? (track.source_label === "liked_cache" ? true : null),
      likedAt: track.liked_at ?? null,
    };
  });
}

export function queueRepeatsTrack(queueTracks: PlayerQueueTrack[], trackUri: string | null | undefined) {
  const trackId = spotifyTrackIdFromUri(trackUri ?? null);
  if (!trackId && !trackUri) {
    return false;
  }
  return queueTracks.length > 0 && queueTracks.every((track) => {
    const queueTrackId = track.trackId ?? spotifyTrackIdFromUri(track.uri);
    return Boolean(
      (trackId && queueTrackId && queueTrackId === trackId)
      || (trackUri && track.uri && track.uri === trackUri),
    );
  });
}

function playerQueueTrackIdentity(track: PlayerQueueTrack | null | undefined) {
  return track?.trackId ?? spotifyTrackIdFromUri(track?.uri ?? null) ?? track?.uri ?? null;
}

export function collapseRepeatedQueueCycle(queueTracks: PlayerQueueTrack[]) {
  if (queueTracks.length < 4) {
    return queueTracks;
  }
  const identities = queueTracks.map(playerQueueTrackIdentity);
  if (identities.some((identity) => !identity)) {
    return queueTracks;
  }
  const maxCycleLength = Math.floor(queueTracks.length / 2);
  for (let cycleLength = 1; cycleLength <= maxCycleLength; cycleLength += 1) {
    if (queueTracks.length < cycleLength * 2) {
      continue;
    }
    const repeatsCycle = identities.every((identity, index) => identity === identities[index % cycleLength]);
    if (repeatsCycle) {
      return queueTracks.slice(0, cycleLength);
    }
  }
  return queueTracks;
}

export function queuePlaylistTrackUris(currentTrackUri: string | null, queueTracks: PlayerQueueTrack[]) {
  const uris: string[] = [];
  for (const uri of [currentTrackUri, ...queueTracks.map((track) => track.uri)]) {
    if (uri?.startsWith("spotify:track:")) {
      uris.push(uri);
    }
    if (uris.length >= QUEUE_PLAYLIST_URI_LIMIT) {
      break;
    }
  }
  return uris;
}

export function trackUriWithFallback(trackUri: string | null | undefined, trackId: string | null | undefined) {
  if (trackUri && trackUri.startsWith("spotify:track:")) {
    return trackUri;
  }
  if (trackId) {
    return `spotify:track:${trackId}`;
  }
  return null;
}

export function formatPlaybackClock(totalMs: number) {
  const safeSeconds = Math.max(0, Math.floor(totalMs / 1000));
  const minutes = Math.floor(safeSeconds / 60);
  const seconds = safeSeconds % 60;
  return `${minutes}:${seconds.toString().padStart(2, "0")}`;
}
