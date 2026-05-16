import { PLAYER_RECENT_FETCH_LIMIT } from "../constants/appConstants";
import type { PlayerQueueTrack, PlayerTrackSummary, RecentTrack, SpotifyPlayerState } from "../types/appTypes";

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

export function dedupeRecentTracksForPlayer(tracks: RecentTrack[], limit = PLAYER_RECENT_FETCH_LIMIT) {
  const seen = new Set<string>();
  const unique: RecentTrack[] = [];
  for (const track of tracks) {
    const key = playerRecentTrackKey(track);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    unique.push(track);
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
      albumId: track.album_id ?? null,
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
