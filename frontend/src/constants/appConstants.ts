import type { SectionKey } from "../types/appTypes";

export const spotifyLogoDataUrl =
  "data:image/svg+xml;utf8," +
  encodeURIComponent(
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 168 168">
      <circle cx="84" cy="84" r="84" fill="#1ed760"/>
      <path d="M121.2 113.3a6 6 0 0 1-8.3 2C90.2 101.5 61.6 98.6 27.8 106.6a6 6 0 1 1-2.8-11.7c36.8-8.8 68.3-5.5 93.8 9.9a6 6 0 0 1 2.4 8.5z" fill="#0b120f"/>
      <path d="M130.5 89.8a7.4 7.4 0 0 1-10.2 2.4c-26-16-65.6-20.7-96.3-11.4a7.4 7.4 0 0 1-4.3-14.1c35.2-10.7 79.2-5.3 108.3 12.6a7.4 7.4 0 0 1 2.5 10.5z" fill="#0b120f"/>
      <path d="M131.6 65.3C100.9 47 50.2 45.4 20.9 54.2A8.9 8.9 0 0 1 15.8 37c33.7-10.2 89.7-8.3 124.9 12.7a8.9 8.9 0 1 1-9.1 15.6z" fill="#0b120f"/>
    </svg>`,
  );
export const spotifyAppsUrl = "https://www.spotify.com/us/account/apps/";
export const githubRepoUrl = "https://github.com/moshe-kahn/listen-labs";
export const EXPERIENCE_MODE_STORAGE_KEY = "listenlab-experience-mode";
export const SPOTIFY_COOLDOWN_UNTIL_STORAGE_KEY = "listenlab-spotify-cooldown-until";
export const SPOTIFY_COOLDOWN_DURATION_STORAGE_KEY = "listenlab-spotify-cooldown-duration";
export const LIVE_PLAYBACK_POLL_INTERVAL_MS = 30 * 60 * 1000;
export const LIVE_PLAYBACK_PROGRESS_TICK_MS = 500;
export const DEFAULT_PLAYER_VOLUME = 0.8;
export const PREVIEW_RAMP_START_VOLUME = 0.24;
export const PREVIEW_RAMP_DURATION_MS = 4_200;
export const PREVIEW_RAMP_STEP_MS = 90;
export const PAGE_SIZE = 5;
export const RECENT_SECTION_FETCH_LIMIT = 10;
export const LIKED_TRACKS_FETCH_LIMIT = 200;
export const LIKED_TRACKS_RECENT_DISPLAY_LIMIT = 100;
export const LIKED_TRACKS_SHUFFLE_POOL_LIMIT = 500;
export const PLAYER_RECENT_FETCH_LIMIT = 50;
export const PLAYLISTS_PAGE_SIZE = 10;
export const IDENTITY_AUDIT_AMBIGUOUS_VISIBLE_STEP = 100;
export const DEBUG_SESSION_BREAK_MS = 45 * 60 * 1000;
export const DEBUG_GAP_MARKER_MIN_MS = 5_000;
export const DEBUG_GAP_MARKER_MAX_MS = 10 * 60 * 1000;
export const RECENT_RANGE_OPTIONS = [
  { value: "short_term", label: "4 weeks" },
  { value: "medium_term", label: "6 months" },
] as const;
export const MERGED_TRACK_SOURCE_FILTER_OPTIONS = [
  { value: "all", label: "All plays" },
  { value: "recent", label: "API only" },
  { value: "history", label: "History only" },
  { value: "both", label: "Matched" },
] as const;
export const RECENT_DEBUG_SOURCE_FILTER_OPTIONS = [
  { value: "all", label: "All" },
  { value: "api", label: "API" },
  { value: "history", label: "History" },
  { value: "both", label: "Both" },
] as const;
export const RANK_MOVEMENT_FILTER_OPTIONS = [
  { value: "all", label: "All" },
  { value: "risers", label: "Risers" },
  { value: "fallers", label: "Fallers" },
] as const;

export const INITIAL_OPEN_SECTIONS: Record<SectionKey, boolean> = {
  artists: false,
  artistsAllTime: false,
  artistsRecent: false,
  tracks: false,
  tracksAllTime: false,
  tracksRecent: false,
  tracksAllTimeNew: false,
  tracksAllTimeCurrent: false,
  albums: false,
  albumsAllTime: false,
  albumsRecent: false,
  playlists: false,
  playlistsAllTime: false,
  playlistsRecent: false,
  recent: false,
  likes: false,
};

export const INITIAL_SECTION_PAGES: Record<SectionKey, number> = {
  artists: 0,
  artistsAllTime: 0,
  artistsRecent: 0,
  tracks: 0,
  tracksAllTime: 0,
  tracksAllTimeNew: 0,
  tracksAllTimeCurrent: 0,
  tracksRecent: 0,
  albums: 0,
  albumsAllTime: 0,
  albumsRecent: 0,
  playlists: 0,
  playlistsAllTime: 0,
  playlistsRecent: 0,
  recent: 0,
  likes: 0,
};
