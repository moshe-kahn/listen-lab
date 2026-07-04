const REMASTER_SUFFIX = /\s[-–—]\s(?:\d{4}\s+)?remaster(?:ed)?(?:\s+\d{4})?$/i;
const REMASTER_PAREN_SUFFIX = /\s*[\[(][^\])]*(?:\d{4}\s+)?remaster(?:ed)?(?:\s+\d{4})?[^\])]*[\])]\s*$/i;
const FEATURED_ARTIST_SUFFIX = /\s*(?:[-–—]\s*)?[\[(]?\s*(?:feat\.?|featuring)\s+([^\])]+?)[\])]?\s*$/i;
const ALBUM_REMASTER_SUFFIX = /\s*\([^)]*\bremaster(?:ed)?(?:\s+edition)?\)\s*$/i;

export function displayTrackName(name: string) {
  const cleaned = name
    .replace(FEATURED_ARTIST_SUFFIX, "")
    .replace(REMASTER_SUFFIX, "")
    .replace(REMASTER_PAREN_SUFFIX, "")
    .trim();
  return cleaned || name;
}

export function featuredArtistsFromTrackName(name: string | null | undefined) {
  const match = String(name ?? "").match(FEATURED_ARTIST_SUFFIX);
  return match?.[1]?.trim().replace(/\s+/g, " ") || null;
}

export function displayTrackArtistName(trackName: string | null | undefined, artistName: string | null | undefined) {
  const baseArtist = String(artistName ?? "").trim();
  const featuredArtists = featuredArtistsFromTrackName(trackName);
  if (!featuredArtists) {
    return baseArtist;
  }
  const baseKey = baseArtist.toLocaleLowerCase();
  const missingFeaturedArtists = featuredArtists
    .split(/\s*(?:,|&|\band\b)\s*/i)
    .map((artist) => artist.trim())
    .filter(Boolean)
    .filter((artist) => !baseKey.includes(artist.toLocaleLowerCase()));
  return [baseArtist, ...missingFeaturedArtists].filter(Boolean).join(", ");
}

export function remasterYearFromTrackName(name: string | null | undefined) {
  const match = String(name ?? "").match(/\b(\d{4})\s+remaster(?:ed)?\b/i);
  return match ? Number(match[1]) : null;
}

export function displayAlbumName(name: string) {
  return name.replace(ALBUM_REMASTER_SUFFIX, "").trim() || name;
}
