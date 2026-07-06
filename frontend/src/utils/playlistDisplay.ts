import type { OwnedPlaylist } from "../types/appTypes";

export function looksLikeSpotifyUserId(value: string | null | undefined) {
  const text = String(value ?? "").trim();
  const compact = text.replace(/[^A-Za-z0-9]/g, "");
  return (
    compact.length >= 18
    && compact.length >= text.length - 2
    && /\d/.test(compact)
    && /[A-Z]/.test(compact)
    && !/[a-z]/.test(compact)
  );
}

export function spotifyUserLabel(value: string | null | undefined) {
  const text = String(value ?? "").trim();
  if (!text || looksLikeSpotifyUserId(text)) {
    return "Unknown";
  }
  return text;
}

export function spotifyUserDisplayName(
  displayName: string | null | undefined,
  userId?: string | null,
) {
  const name = String(displayName ?? "").trim();
  const id = String(userId ?? "").trim();
  const value = name || id;
  return spotifyUserLabel(value);
}

export function playlistOwnerDisplayName(playlist: OwnedPlaylist) {
  return spotifyUserDisplayName(playlist.owner_name, playlist.owner_id);
}

export function playlistContributorNames(playlist: OwnedPlaylist) {
  const names = Array.isArray(playlist.contributor_summary?.names)
    ? playlist.contributor_summary.names
    : [];
  const cleaned = names
    .map((name) => String(name ?? "").trim())
    .filter(Boolean)
    .map(spotifyUserLabel);
  const ownerLabel = playlist.is_owned
    ? "You"
    : String(playlist.contributor_summary?.owner_display_name ?? playlistOwnerDisplayName(playlist)).trim();
  const ownerName = spotifyUserLabel(ownerLabel);
  const uniqueNames = Array.from(new Set(cleaned));
  if (ownerName && uniqueNames.includes(ownerName)) {
    return [ownerName, ...uniqueNames.filter((name) => name !== ownerName)];
  }
  return uniqueNames;
}

export function compactPlaylistContributorLabel(playlist: OwnedPlaylist, limit = 2) {
  const names = playlistContributorNames(playlist);
  if (names.length === 0) {
    return null;
  }
  if (names.length <= limit) {
    return names.join(", ");
  }
  return `${names.slice(0, limit).join(", ")} +${names.length - limit}`;
}

export function playlistEditorDisplayLabel(playlist: OwnedPlaylist) {
  if (playlist.is_collaborative) {
    const contributors = compactPlaylistContributorLabel(playlist);
    return contributors ? `Collab: ${contributors}` : "Collabs";
  }
  if (playlist.is_owned) {
    return "Yours";
  }
  return playlistOwnerDisplayName(playlist);
}
