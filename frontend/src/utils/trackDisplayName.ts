const REMASTER_SUFFIX = /\s[-–—]\s(?:\d{4}\s+)?remaster(?:ed)?$/i;
const ALBUM_REMASTER_SUFFIX = /\s*\([^)]*\bremaster(?:ed)?(?:\s+edition)?\)\s*$/i;

export function displayTrackName(name: string) {
  return name.replace(REMASTER_SUFFIX, "").trim() || name;
}

export function remasterYearFromTrackName(name: string | null | undefined) {
  const match = String(name ?? "").match(/\b(\d{4})\s+remaster(?:ed)?\b/i);
  return match ? Number(match[1]) : null;
}

export function displayAlbumName(name: string) {
  return name.replace(ALBUM_REMASTER_SUFFIX, "").trim() || name;
}
