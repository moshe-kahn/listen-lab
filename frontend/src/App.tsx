import { type ReactNode, useEffect, useRef, useState } from "react";

type SessionResponse = {
  authenticated: boolean;
  display_name: string | null;
  spotify_user_id: string | null;
  email?: string | null;
};

type ProfileProgressResponse = {
  active: boolean;
  phase: string | null;
  elapsed_seconds: number;
  events?: Array<{
    phase: string;
    at_seconds: number;
  }>;
};

type RecentTrack = {
  track_id: string | null;
  track_name: string | null;
  artist_name: string | null;
  album_name: string | null;
  url?: string | null;
  image_url?: string | null;
  album_id?: string | null;
};

type MatchCounts = {
  short_term_top: number;
  long_term_top: number;
  recently_played: number;
  liked: number;
  playlist_size: number;
};

type TopPlaylist = {
  playlist_id: string | null;
  playlist_name: string | null;
  playlist_url: string | null;
  image_url?: string | null;
  track_count: number | null;
  score: number;
  match_counts: MatchCounts;
};

type OwnedPlaylist = {
  playlist_id: string | null;
  name: string | null;
  track_count: number | null;
  description?: string | null;
  is_public?: boolean | null;
  url: string | null;
  image_url?: string | null;
};

type FollowedArtist = {
  artist_id: string | null;
  name: string | null;
  followers_total: number | null;
  genres: string[];
  popularity?: number | null;
  url: string | null;
  image_url?: string | null;
  debug?: {
    source?: string;
    score?: number;
    total_ms?: number;
    play_count?: number;
    distinct_tracks?: number;
  };
};

type TopAlbum = {
  album_id: string | null;
  name: string | null;
  artist_name: string | null;
  url: string | null;
  image_url?: string | null;
  track_representation_count: number;
  rank_score: number;
  album_score: number;
  represented_track_names: string[];
  debug?: {
    source?: string;
    score?: number;
    total_ms?: number;
    play_count?: number;
    distinct_tracks?: number;
  };
};

type ProfileResponse = {
  id: string;
  display_name: string | null;
  email: string | null;
  product: string | null;
  country: string | null;
  username: string | null;
  followers_total: number | null;
  followed_artists_total: number | null;
  followed_artists_available: boolean;
  followed_artists: FollowedArtist[];
  followed_artists_list_available: boolean;
  recent_top_artists: FollowedArtist[];
  recent_top_artists_available: boolean;
  top_tracks: RecentTrack[];
  top_tracks_available: boolean;
  recent_top_tracks: RecentTrack[];
  recent_top_tracks_available: boolean;
  top_albums: TopAlbum[];
  top_albums_available: boolean;
  recent_top_albums: TopAlbum[];
  recent_top_albums_available: boolean;
  top_playlists_recent: TopPlaylist[];
  top_playlists_all_time: TopPlaylist[];
  top_playlists_available: boolean;
  history_insights_available?: boolean;
  profile_url: string | null;
  image_url: string | null;
  recent_tracks: RecentTrack[];
  recent_tracks_available: boolean;
  owned_playlists: OwnedPlaylist[];
  owned_playlists_available: boolean;
  recent_likes_tracks: RecentTrack[];
  recent_likes_available: boolean;
  extended_loaded?: boolean;
};

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8000";
const githubRepoUrl = "https://github.com/moshe-kahn/listen-labs";
const PAGE_SIZE = 5;
const PLAYLISTS_PAGE_SIZE = 10;
const spotifyLogoDataUrl =
  "data:image/svg+xml;utf8," +
  encodeURIComponent(
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 168 168">
      <circle cx="84" cy="84" r="84" fill="#1ed760"/>
      <path d="M121.2 113.3a6 6 0 0 1-8.3 2C90.2 101.5 61.6 98.6 27.8 106.6a6 6 0 1 1-2.8-11.7c36.8-8.8 68.3-5.5 93.8 9.9a6 6 0 0 1 2.4 8.5z" fill="#0b120f"/>
      <path d="M130.5 89.8a7.4 7.4 0 0 1-10.2 2.4c-26-16-65.6-20.7-96.3-11.4a7.4 7.4 0 0 1-4.3-14.1c35.2-10.7 79.2-5.3 108.3 12.6a7.4 7.4 0 0 1 2.5 10.5z" fill="#0b120f"/>
      <path d="M131.6 65.3C100.9 47 50.2 45.4 20.9 54.2A8.9 8.9 0 0 1 15.8 37c33.7-10.2 89.7-8.3 124.9 12.7a8.9 8.9 0 1 1-9.1 15.6z" fill="#0b120f"/>
    </svg>`,
  );

type SectionKey =
  | "artists"
  | "artistsAllTime"
  | "artistsRecent"
  | "tracks"
  | "tracksAllTime"
  | "tracksRecent"
  | "albums"
  | "albumsAllTime"
  | "albumsRecent"
  | "playlists"
  | "playlistsAllTime"
  | "playlistsRecent"
  | "recent"
  | "likes";

const INITIAL_OPEN_SECTIONS: Record<SectionKey, boolean> = {
  artists: false,
  artistsAllTime: false,
  artistsRecent: false,
  tracks: false,
  tracksAllTime: false,
  tracksRecent: false,
  albums: false,
  albumsAllTime: false,
  albumsRecent: false,
  playlists: false,
  playlistsAllTime: false,
  playlistsRecent: false,
  recent: false,
  likes: false,
};

const INITIAL_SECTION_PAGES: Record<SectionKey, number> = {
  artists: 0,
  artistsAllTime: 0,
  artistsRecent: 0,
  tracks: 0,
  tracksAllTime: 0,
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

type DashboardListCardProps = {
  href?: string | null;
  imageUrl?: string | null;
  imageAlt: string;
  fallbackLabel: string;
  primaryText: string;
  secondaryText?: string | null;
  tertiaryText?: string | null;
  primaryClamp?: "single-line-ellipsis" | "two-line-clamp";
};

export function App() {
  const [session, setSession] = useState<SessionResponse | null>(null);
  const [profile, setProfile] = useState<ProfileResponse | null>(null);
  const [statusMessage, setStatusMessage] = useState("Checking authentication state...");
  const [statusHistory, setStatusHistory] = useState<string[]>([]);
  const [authTransitioning, setAuthTransitioning] = useState(false);
  const [loadingProfile, setLoadingProfile] = useState(false);
  const [loadingExtendedProfile, setLoadingExtendedProfile] = useState(false);
  const [profileLoadAttempted, setProfileLoadAttempted] = useState(false);
  const [openSections, setOpenSections] = useState<Record<SectionKey, boolean>>(INITIAL_OPEN_SECTIONS);
  const [sectionPages, setSectionPages] = useState<Record<SectionKey, number>>(INITIAL_SECTION_PAGES);
  const [profileMenuOpen, setProfileMenuOpen] = useState(false);
  const [brandMenuOpen, setBrandMenuOpen] = useState(false);
  const profileMenuRef = useRef<HTMLDivElement | null>(null);
  const brandMenuRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const url = new URL(window.location.href);
    if (url.pathname === "/auth/callback") {
      const status = url.searchParams.get("status");
      setAuthTransitioning(status === "success");
      setStatusMessage(
        status === "success"
          ? "Spotify login succeeded. Session restored."
          : "Spotify login did not complete successfully.",
      );
      window.history.replaceState({}, "", "/");
    }
  }, []);

  useEffect(() => {
    void loadSession();
  }, []);

  useEffect(() => {
    if (session?.authenticated && !profile && !loadingProfile && !profileLoadAttempted) {
      void loadProfile();
    }
  }, [loadingProfile, profile, profileLoadAttempted, session]);

  useEffect(() => {
    function handlePointerDown(event: MouseEvent) {
      if (!profileMenuRef.current?.contains(event.target as Node)) {
        setProfileMenuOpen(false);
      }
      if (!brandMenuRef.current?.contains(event.target as Node)) {
        setBrandMenuOpen(false);
      }
    }

    document.addEventListener("mousedown", handlePointerDown);
    return () => {
      document.removeEventListener("mousedown", handlePointerDown);
    };
  }, []);

  async function loadSession() {
    try {
      const response = await fetch(`${apiBaseUrl}/auth/session`, {
        credentials: "include",
      });

      if (!response.ok) {
        throw new Error("Failed to load auth session.");
      }

      const data = (await response.json()) as SessionResponse;
      setSession(data);
      setProfileLoadAttempted(false);
      setOpenSections(INITIAL_OPEN_SECTIONS);
      setSectionPages(INITIAL_SECTION_PAGES);

      if (data.authenticated) {
        setStatusMessage("");
        setStatusHistory([]);
        setAuthTransitioning(true);
      } else {
        setProfile(null);
        setProfileMenuOpen(false);
        setBrandMenuOpen(false);
        setStatusMessage("Not connected yet. Use Spotify login to start the auth flow.");
        setStatusHistory([]);
        setAuthTransitioning(false);
      }
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : "Failed to load session.");
    }
  }

  function startLogin() {
    window.location.href = `${apiBaseUrl}/auth/login`;
  }

  async function reconnectSpotify() {
    await fetch(`${apiBaseUrl}/cache/rebuild`, {
      method: "POST",
      credentials: "include",
    });
    startLogin();
  }

  function handleAuthAction() {
    if (session?.authenticated) {
      void reconnectSpotify();
      return;
    }
    startLogin();
  }

  function toggleSection(section: SectionKey) {
    setOpenSections((current) => ({
      ...current,
      [section]: !current[section],
    }));
  }

  function openAndScrollToSection(section: SectionKey, anchorId: string) {
    setOpenSections((current) => ({
      ...current,
      artists: false,
      tracks: false,
      albums: false,
      playlists: false,
      recent: false,
      [section]: true,
    }));

    window.setTimeout(() => {
      const element = document.getElementById(anchorId);
      if (element) {
        element.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    }, 0);
  }

  function moveSectionPage(section: SectionKey, direction: -1 | 1, itemCount: number, pageSize: number = PAGE_SIZE) {
    const maxPage = Math.max(0, Math.ceil(itemCount / pageSize) - 1);
    setSectionPages((current) => ({
      ...current,
      [section]: Math.min(maxPage, Math.max(0, current[section] + direction)),
    }));
  }

  function visibleItems<T>(section: SectionKey, items: T[]) {
    const start = sectionPages[section] * PAGE_SIZE;
    return items.slice(start, start + PAGE_SIZE);
  }

  function visibleItemsWithPageSize<T>(section: SectionKey, items: T[], pageSize: number) {
    const start = sectionPages[section] * pageSize;
    return items.slice(start, start + pageSize);
  }

  function previewImages(items: Array<{ image_url?: string | null }>) {
    return items
      .map((item) => item.image_url)
      .filter((image): image is string => Boolean(image))
      .slice(0, 5);
  }

  function previewItems(
    items: Array<{
      image_url?: string | null;
      name?: string | null;
      track_name?: string | null;
      playlist_name?: string | null;
      url?: string | null;
      album_url?: string | null;
      playlist_url?: string | null;
    }>,
  ) {
    return items
      .map((item) => ({
        image: item.image_url,
        label: item.name ?? item.track_name ?? item.playlist_name ?? "",
        url: item.url ?? item.album_url ?? item.playlist_url ?? "",
      }))
      .filter(
        (item): item is { image: string; label: string; url: string } =>
          Boolean(item.image && item.label && item.url),
      )
      .slice(0, 5);
  }

  function emptySlots<T>(items: T[]) {
    return Math.max(0, PAGE_SIZE - items.length);
  }

  function formatAlbumSummary(album: TopAlbum) {
    const names = album.represented_track_names.filter(Boolean);
    if (names.length === 0) {
      return `${album.track_representation_count} top tracks`;
    }
    if (names.length <= 2) {
      return names.join(" | ");
    }
    return `${names.slice(0, 2).join(", ")} +${names.length - 2} more`;
  }

  function formatHistoryDebugLine(item: {
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

  function formatLoadingStatus(phase: string | null, elapsedSeconds: number) {
    const elapsed = `${elapsedSeconds.toFixed(1)}s`;
    return phase ? `Loading ${phase}... (${elapsed})` : `Loading your Spotify data... (${elapsed})`;
  }

  function formatPlaylistSummary(playlist: TopPlaylist, mode: "recent" | "allTime") {
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

  async function loadProfile() {
    setLoadingProfile(true);
    setProfileLoadAttempted(true);
    setStatusMessage("Loading your Spotify data...");
    setStatusHistory(["Initial load started."]);
    let pollingActive = true;
    let progressTimer: number | null = null;
    const startedAt = performance.now();

    const updateProgress = async () => {
      const fallbackElapsed = (performance.now() - startedAt) / 1000;
      try {
        const response = await fetch(`${apiBaseUrl}/me/progress`, {
          credentials: "include",
        });
        if (!response.ok) {
          if (pollingActive) {
            setStatusMessage(formatLoadingStatus(null, fallbackElapsed));
          }
          return;
        }
        const data = (await response.json()) as ProfileProgressResponse;
        if (!pollingActive) {
          return;
        }
        setStatusMessage(
          formatLoadingStatus(
            data.active ? data.phase : null,
            data.active ? data.elapsed_seconds : fallbackElapsed,
          ),
        );
        if (data.events?.length) {
          setStatusHistory(
            [
              "Initial load started.",
              ...data.events.map((event) => `initial ${event.at_seconds.toFixed(1)}s: ${event.phase}`),
            ],
          );
        } else {
          setStatusHistory(["Initial load started.", `initial ${formatLoadingStatus(null, fallbackElapsed)}`]);
        }
      } catch {
        if (pollingActive) {
          setStatusMessage(formatLoadingStatus(null, fallbackElapsed));
          setStatusHistory(["Initial load started.", `initial ${formatLoadingStatus(null, fallbackElapsed)}`]);
        }
      }
    };

    await updateProgress();
    progressTimer = window.setInterval(() => {
      void updateProgress();
    }, 500);
    try {
      const response = await fetch(`${apiBaseUrl}/me`, {
        method: "GET",
        credentials: "include",
      });

      if (!response.ok) {
        let detail = "Failed to load Spotify profile.";
        try {
          const payload = (await response.json()) as { detail?: string };
          if (payload.detail) {
            detail = payload.detail;
          }
        } catch {
          // ignore invalid error payloads
        }
        if (response.status === 403) {
          detail = "Spotify permission missing. Log out and log back in to grant the latest scopes.";
        }
        throw new Error(detail);
      }

      const data = (await response.json()) as ProfileResponse;
      setProfile(data);
      setAuthTransitioning(false);
      setSectionPages(INITIAL_SECTION_PAGES);
      setStatusMessage("");
      setStatusHistory((current) =>
        current.length > 0 ? [...current, "Initial load complete."] : ["Initial load started.", "Initial load complete."],
      );
      void loadExtendedProfile();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load Spotify profile.";
      setStatusMessage(message);
      setAuthTransitioning(false);
      setStatusHistory((current) => (current.length > 0 ? [...current, `Error: ${message}`] : [message]));
    } finally {
      pollingActive = false;
      if (progressTimer != null) {
        window.clearInterval(progressTimer);
      }
      setLoadingProfile(false);
    }
  }

  async function loadExtendedProfile() {
    setLoadingExtendedProfile(true);
    setStatusHistory((current) => [...current, "Background expansion started."]);
    let pollingActive = true;
    let progressTimer: number | null = null;
    const startedAt = performance.now();

    const updateProgress = async () => {
      const fallbackElapsed = (performance.now() - startedAt) / 1000;
      try {
        const response = await fetch(`${apiBaseUrl}/me/progress`, {
          credentials: "include",
        });
        if (!response.ok) {
          return;
        }
        const data = (await response.json()) as ProfileProgressResponse;
        if (!pollingActive) {
          return;
        }
        if (data.events?.length) {
          setStatusHistory((current) => {
            const prefix = current.filter((entry) => !entry.startsWith("background "));
            const extensionEvents = (data.events ?? []).map(
              (event) => `background ${event.at_seconds.toFixed(1)}s: ${event.phase}`,
            );
            return [...prefix, ...extensionEvents];
          });
        } else {
          setStatusHistory((current) => {
            const prefix = current.filter((entry) => !entry.startsWith("background "));
            return [...prefix, `background ${formatLoadingStatus(null, fallbackElapsed)}`];
          });
        }
      } catch {
        // ignore background progress failures
      }
    };

    progressTimer = window.setInterval(() => {
      void updateProgress();
    }, 500);
    try {
      const response = await fetch(`${apiBaseUrl}/me?mode=extended`, {
        credentials: "include",
      });

      if (!response.ok) {
        let detail = "Failed to load Spotify profile.";
        try {
          const payload = (await response.json()) as { detail?: string };
          if (payload.detail) {
            detail = payload.detail;
          }
        } catch {
          // ignore invalid error payloads
        }
        if (response.status === 403) {
          detail = "Spotify permission missing. Log out and log back in to grant the latest scopes.";
        }
        throw new Error(detail);
      }

      const data = (await response.json()) as ProfileResponse;
      setProfile(data);
      setStatusMessage("");
      setStatusHistory((current) => {
        const filtered = current.filter((entry) => !entry.startsWith("background "));
        return [...filtered, "Background expansion complete."];
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load extended Spotify profile.";
      setStatusHistory((current) => {
        const filtered = current.filter((entry) => !entry.startsWith("background "));
        return [...filtered, `Background expansion error: ${message}`];
      });
    } finally {
      pollingActive = false;
      if (progressTimer != null) {
        window.clearInterval(progressTimer);
      }
      setLoadingExtendedProfile(false);
    }
  }

  async function logout() {
    await fetch(`${apiBaseUrl}/auth/logout`, {
      method: "POST",
      credentials: "include",
    });
    setSession({
      authenticated: false,
      display_name: null,
      spotify_user_id: null,
      email: null,
    });
    setProfile(null);
    setProfileLoadAttempted(false);
    setOpenSections(INITIAL_OPEN_SECTIONS);
    setSectionPages(INITIAL_SECTION_PAGES);
    setStatusMessage("Signed out.");
    setStatusHistory([]);
    setAuthTransitioning(false);
    setProfileMenuOpen(false);
    setBrandMenuOpen(false);
  }

  function renderPaging(section: SectionKey, itemCount: number) {
    return renderPagingWithPageSize(section, itemCount, PAGE_SIZE);
  }

  function renderPagingWithPageSize(section: SectionKey, itemCount: number, pageSize: number) {
    if (itemCount <= pageSize) {
      return null;
    }

    return (
      <div className="section-nav">
        <button
          className="secondary-button"
          disabled={sectionPages[section] === 0}
          onClick={() => moveSectionPage(section, -1, itemCount, pageSize)}
          type="button"
        >
          {"<"}
        </button>
        <span>
          {sectionPages[section] + 1} / {Math.ceil(itemCount / pageSize)}
        </span>
        <button
          className="secondary-button"
          disabled={(sectionPages[section] + 1) * pageSize >= itemCount}
          onClick={() => moveSectionPage(section, 1, itemCount, pageSize)}
          type="button"
        >
          {">"}
        </button>
      </div>
    );
  }

  function renderDashboardListCard(props: DashboardListCardProps, key: string) {
    const {
      href,
      imageUrl,
      imageAlt,
      fallbackLabel,
      primaryText,
      secondaryText,
      tertiaryText,
      primaryClamp = "single-line-ellipsis",
    } = props;

    return (
      <a
        className="list-row list-link dashboard-card-row"
        href={href ?? undefined}
        key={key}
        rel="noreferrer"
        target="_blank"
      >
        <div className="list-primary dashboard-card-layout">
          {imageUrl ? (
            <img alt={imageAlt} className="list-art" src={imageUrl} />
          ) : (
            <div className="list-art list-art-fallback" aria-hidden="true">
              {fallbackLabel}
            </div>
          )}
          <div className="card-copy">
            <strong className={`card-primary ${primaryClamp}`}>{primaryText}</strong>
            {secondaryText ? <p className="card-secondary single-line-ellipsis">{secondaryText}</p> : null}
            {tertiaryText ? <p className="card-tertiary single-line-ellipsis">{tertiaryText}</p> : null}
          </div>
        </div>
      </a>
    );
  }

  function renderArtistColumn(
    section: SectionKey,
    items: FollowedArtist[],
    available: boolean,
    emptyCopy: string,
    unavailableCopy: string,
  ) {
    if (!available) {
      return <p className="empty-copy">{unavailableCopy}</p>;
    }
    if (items.length === 0) {
      return <p className="empty-copy">{emptyCopy}</p>;
    }

    const pageItems = visibleItems(section, items);
    return (
      <>
        <div className="item-list">
          {pageItems.map((artist, index) => (
            renderDashboardListCard(
              {
                href: artist.url,
                imageUrl: artist.image_url,
                imageAlt: `${artist.name ?? "Artist"} portrait`,
                fallbackLabel: "A",
                primaryText: artist.name ?? "Unknown artist",
                secondaryText:
                  artist.genres.length > 0
                    ? artist.genres.join(", ")
                    : artist.popularity != null
                      ? `Popularity ${artist.popularity}/100`
                      : "Spotify artist",
                tertiaryText: formatHistoryDebugLine(artist),
              },
              artist.artist_id ?? `${artist.name}-${index}`,
            )
          ))}
          {Array.from({ length: emptySlots(pageItems) }).map((_, index) => (
            <div className="list-row list-row-placeholder" key={`${section}-empty-${index}`} aria-hidden="true" />
          ))}
        </div>
        {renderPaging(section, items.length)}
      </>
    );
  }

  function renderTrackColumn(
    section: SectionKey,
    items: RecentTrack[],
    available: boolean,
    emptyCopy: string,
    unavailableCopy: string,
  ) {
    if (!available) {
      return <p className="empty-copy">{unavailableCopy}</p>;
    }
    if (items.length === 0) {
      return <p className="empty-copy">{emptyCopy}</p>;
    }

    const pageItems = visibleItems(section, items);
    return (
      <>
        <div className="item-list">
          {pageItems.map((track, index) =>
            renderDashboardListCard(
              {
                href: track.url,
                imageUrl: track.image_url,
                imageAlt: `${track.album_name ?? track.track_name ?? "Album"} cover`,
                fallbackLabel: "T",
                primaryText: track.track_name ?? "Unknown track",
                secondaryText: track.artist_name ?? "Unknown artist",
                tertiaryText: track.album_name ?? "Unknown album",
              },
              track.track_id ?? `${track.track_name}-${index}-${section}`,
            ),
          )}
          {Array.from({ length: emptySlots(pageItems) }).map((_, index) => (
            <div className="list-row list-row-placeholder" key={`${section}-empty-${index}`} aria-hidden="true" />
          ))}
        </div>
        {renderPaging(section, items.length)}
      </>
    );
  }

  function renderAlbumColumn(
    section: SectionKey,
    items: TopAlbum[],
    available: boolean,
    emptyCopy: string,
    unavailableCopy: string,
  ) {
    if (!available) {
      return <p className="empty-copy">{unavailableCopy}</p>;
    }
    if (items.length === 0) {
      return <p className="empty-copy">{emptyCopy}</p>;
    }

    const pageItems = visibleItems(section, items);
    return (
      <>
        <div className="item-list">
          {pageItems.map((album, index) =>
            renderDashboardListCard(
              {
                href: album.url,
                imageUrl: album.image_url,
                imageAlt: `${album.name ?? "Album"} cover`,
                fallbackLabel: "A",
                primaryText: album.name ?? "Unknown album",
                secondaryText: album.artist_name ?? "Unknown artist",
                tertiaryText:
                  formatHistoryDebugLine(album) ??
                  formatAlbumSummary(album),
              },
              album.album_id ?? `${album.name}-${index}-${section}`,
            ),
          )}
          {Array.from({ length: emptySlots(pageItems) }).map((_, index) => (
            <div className="list-row list-row-placeholder" key={`${section}-empty-${index}`} aria-hidden="true" />
          ))}
        </div>
        {renderPaging(section, items.length)}
      </>
    );
  }

  function renderPlaylistColumn(
    section: SectionKey,
    items: OwnedPlaylist[],
    available: boolean,
    emptyCopy: string,
    unavailableCopy: string,
    paged: boolean = true,
  ) {
    if (!available) {
      return <p className="empty-copy">{unavailableCopy}</p>;
    }
    if (items.length === 0) {
      return <p className="empty-copy">{emptyCopy}</p>;
    }

    const pageItems = paged ? visibleItems(section, items) : items;
    return (
      <>
        <div className="item-list">
          {pageItems.map((playlist, index) =>
            renderDashboardListCard(
              {
                href: playlist.url,
                imageUrl: playlist.image_url,
                imageAlt: `${playlist.name ?? "Playlist"} cover`,
                fallbackLabel: "P",
                primaryText: playlist.name ?? "Untitled playlist",
                primaryClamp: "two-line-clamp",
                secondaryText: playlist.description?.trim() || null,
                tertiaryText:
                  playlist.track_count != null ? `${playlist.track_count} tracks` : "Playlist",
              },
              playlist.playlist_id ?? `${playlist.name}-${index}-${section}`,
            ),
          )}
          {Array.from({ length: emptySlots(pageItems) }).map((_, index) => (
            <div className="list-row list-row-placeholder" key={`${section}-empty-${index}`} aria-hidden="true" />
          ))}
        </div>
        {paged ? renderPaging(section, items.length) : null}
      </>
    );
  }

  function splitItems<T>(items: T[]) {
    const midpoint = Math.ceil(items.length / 2);
    return {
      left: items.slice(0, midpoint),
      right: items.slice(midpoint),
    };
  }

  function renderDualSectionCard(props: {
    title: string;
    section: SectionKey;
    anchorId: string;
    leftTitle: string;
    rightTitle: string;
    leftContent: ReactNode;
    rightContent: ReactNode;
    previewItemsLeft: Array<{ image: string; label: string; url: string }>;
    previewItemsRight: Array<{ image: string; label: string; url: string }>;
  }) {
    const {
      title,
      section,
      anchorId,
      leftTitle,
      rightTitle,
      leftContent,
      rightContent,
      previewItemsLeft,
      previewItemsRight,
    } = props;

    return (
      <section className="info-card info-card-wide" id={anchorId}>
        <button className="section-toggle section-toggle-header" onClick={() => toggleSection(section)} type="button">
          <h2>{title}</h2>
        </button>
        {openSections[section] ? (
          <div className="artists-grid">
            <div className="artists-column">
              <h3>{leftTitle}</h3>
              {leftContent}
            </div>
            <div className="artists-column">
              <h3>{rightTitle}</h3>
              {rightContent}
            </div>
          </div>
        ) : (
          <div className="preview-strip">
            {previewItemsLeft.concat(previewItemsRight).slice(0, 5).map((item, index) => (
              <a
                className="preview-card"
                href={item.url}
                key={`${title}-${item.image}-${index}`}
                rel="noreferrer"
                target="_blank"
              >
                <img
                  alt={item.label}
                  className="preview-thumb"
                  src={item.image}
                  title={item.label}
                />
                <span className="preview-label">{item.label}</span>
              </a>
            ))}
          </div>
        )}
        <button className="section-toggle section-toggle-footer" onClick={() => toggleSection(section)} type="button">
          <span>{openSections[section] ? "^" : "v"}</span>
        </button>
      </section>
    );
  }

  function renderPlaylistsSection() {
    if (!profile) {
      return null;
    }

    const visiblePlaylists = visibleItemsWithPageSize(
      "playlists",
      profile.owned_playlists,
      PLAYLISTS_PAGE_SIZE,
    );
    const playlistColumns = splitItems(visiblePlaylists);

    return (
      <section className="info-card info-card-wide" id="playlists">
        <button className="section-toggle section-toggle-header" onClick={() => toggleSection("playlists")} type="button">
          <h2>Playlists</h2>
        </button>
        {openSections.playlists ? (
          profile.owned_playlists_available ? (
            profile.owned_playlists.length > 0 ? (
              <div className="artists-grid">
                <div className="artists-column">
                  {renderPlaylistColumn(
                    "playlists",
                    playlistColumns.left,
                    true,
                    "Spotify returned no playlists for this account.",
                    "",
                    false,
                  )}
                </div>
                <div className="artists-column">
                  {playlistColumns.right.length > 0
                    ? renderPlaylistColumn(
                        "playlists",
                        playlistColumns.right,
                        true,
                        "Spotify returned no playlists for this account.",
                        "",
                        false,
                      )
                    : <p className="empty-copy">No more playlists in this column yet.</p>}
                </div>
              </div>
            ) : (
              <p className="empty-copy">Spotify returned no playlists for this account.</p>
            )
          ) : (
            <p className="empty-copy">
              Playlist access is not available for this session yet. Log out and log back in to grant access.
            </p>
          )
        ) : (
          <div className="preview-strip">
            {previewItems(profile.owned_playlists).map((item, index) => (
              <a
                className="preview-card"
                href={item.url}
                key={`playlists-${item.image}-${index}`}
                rel="noreferrer"
                target="_blank"
              >
                <img alt={item.label} className="preview-thumb" src={item.image} title={item.label} />
                <span className="preview-label">{item.label}</span>
              </a>
            ))}
          </div>
        )}
        {openSections.playlists && profile.owned_playlists.length > PLAYLISTS_PAGE_SIZE
          ? renderPagingWithPageSize("playlists", profile.owned_playlists.length, PLAYLISTS_PAGE_SIZE)
          : null}
        <button className="section-toggle section-toggle-footer" onClick={() => toggleSection("playlists")} type="button">
          <span>{openSections.playlists ? "^" : "v"}</span>
        </button>
      </section>
    );
  }

  function renderLoadingScreen() {
    const latestHistory = statusHistory.length > 0 ? statusHistory[statusHistory.length - 1] : null;
    const loadingLabel =
      statusMessage && !statusMessage.startsWith("Spotify login succeeded")
        ? statusMessage
        : latestHistory ?? "Analyzing your music...";

    return (
      <main className="app-shell">
        <section className="loading-screen">
          <div className="loading-graphic" aria-hidden="true">
            <div className="loading-headphones">
              <div className="loading-headphones-band" />
              <div className="loading-headphones-cup loading-headphones-cup-left" />
              <div className="loading-headphones-cup loading-headphones-cup-right" />
            </div>
          </div>
          <p className="eyebrow">ListenLab</p>
          <h1>Your music is being analyzed</h1>
          <p className="loading-copy two-line-clamp">
            We&apos;re pulling together your recent activity, favorites, and history-backed listening patterns.
          </p>
          <p className="loading-phase single-line-ellipsis">{loadingLabel}</p>
        </section>
      </main>
    );
  }

  const showLoadingScreen = (authTransitioning || session?.authenticated) && !profile;
  const heroTitle = "ListenLab";
  const heroCopy =
    "Connect your account and browse the listening, library, and profile details Spotify already makes available to ListenLab.";

  if (showLoadingScreen) {
    return renderLoadingScreen();
  }

  return (
    <main className="app-shell">
      <section className="hero-card">
        {!profile ? (
          <div className="top-bar">
            <div className="top-copy">
              <p className="eyebrow">ListenLab</p>
              <h1>{heroTitle}</h1>
              <p className="lede three-line-clamp">{heroCopy}</p>
            </div>

            <div className="top-side">
              <div className="profile-menu-shell" ref={profileMenuRef}>
                <button
                  aria-expanded={profileMenuOpen}
                  className="profile-trigger"
                  onClick={() => setProfileMenuOpen((current) => !current)}
                  type="button"
                >
                  <img alt="Spotify logo" className="profile-image profile-image-compact" src={spotifyLogoDataUrl} />
                  <span className="profile-trigger-copy">
                    <span className="profile-username single-line-ellipsis">@spotify</span>
                  </span>
                </button>

                {profileMenuOpen ? (
                  <section className="profile-card top-profile-card profile-menu-card">
                    <div className="profile-header">
                      <div className="profile-identity">
                        <img alt="Spotify logo" className="profile-image" src={spotifyLogoDataUrl} />
                        <div>
                          <h2 className="two-line-clamp">Spotify</h2>
                          <p className="profile-username single-line-ellipsis">@spotify</p>
                        </div>
                      </div>
                    </div>

                    <div className="actions actions-right actions-in-card">
                      <button
                        className="primary-button"
                        onClick={handleAuthAction}
                        type="button"
                      >
                        Log in with Spotify
                      </button>
                    </div>
                  </section>
                ) : null}
              </div>
            </div>
          </div>
        ) : null}

        {!profile ? null : (
          <>
            <nav className="jump-links jump-links-sticky" aria-label="Dashboard sections">
              <div className="sticky-bar-left">
                <div className="profile-menu-shell profile-menu-shell-inline" ref={brandMenuRef}>
                  <button
                    aria-expanded={brandMenuOpen}
                    className="bar-trigger bar-trigger-brand"
                    onClick={() => {
                      setBrandMenuOpen((current) => !current);
                      setProfileMenuOpen(false);
                    }}
                    type="button"
                  >
                    ListenLab
                  </button>

                  {brandMenuOpen ? (
                    <section className="profile-card top-profile-card profile-menu-card">
                      <div className="profile-header">
                        <div>
                          <h2>ListenLab</h2>
                          <p className="empty-copy">
                            A local Spotify listening dashboard for exploring recent activity, favorites, albums, artists,
                            and playlists.
                          </p>
                        </div>
                      </div>

                      <div className="actions actions-right actions-in-card">
                        <a className="secondary-button bar-link-button" href={githubRepoUrl} rel="noreferrer" target="_blank">
                          View on GitHub
                        </a>
                      </div>
                    </section>
                  ) : null}
                </div>
              </div>

              <div className="sticky-bar-center">
                <button className="jump-link" onClick={() => openAndScrollToSection("artists", "artists")} type="button">
                  Artists
                </button>
                <button className="jump-link" onClick={() => openAndScrollToSection("tracks", "tracks")} type="button">
                  Tracks
                </button>
                <button className="jump-link" onClick={() => openAndScrollToSection("albums", "albums")} type="button">
                  Albums
                </button>
                <button
                  className="jump-link"
                  onClick={() => openAndScrollToSection("playlists", "playlists")}
                  type="button"
                >
                  Playlists
                </button>
                <button className="jump-link" onClick={() => openAndScrollToSection("recent", "activity")} type="button">
                  Activity
                </button>
              </div>

              <div className="sticky-bar-right">
                <div className="profile-menu-shell profile-menu-shell-inline" ref={profileMenuRef}>
                  <button
                    aria-expanded={profileMenuOpen}
                    className="bar-trigger bar-trigger-user"
                    onClick={() => {
                      setProfileMenuOpen((current) => !current);
                      setBrandMenuOpen(false);
                    }}
                    type="button"
                  >
                    <span className="profile-username profile-username-nav single-line-ellipsis">
                      @{profile.username ?? "spotify-user"}
                    </span>
                  </button>

                  {profileMenuOpen ? (
                    <section className="profile-card top-profile-card profile-menu-card">
                      <div className="profile-header">
                        <a
                          className="profile-identity"
                          href={profile.profile_url ?? undefined}
                          rel="noreferrer"
                          target="_blank"
                        >
                          {profile.image_url ? (
                            <img
                              alt={`${profile.display_name ?? "Spotify user"} profile`}
                              className="profile-image"
                              src={profile.image_url}
                            />
                          ) : (
                            <div className="profile-image profile-image-fallback" aria-hidden="true">
                              {(profile.display_name ?? "S").slice(0, 1).toUpperCase()}
                            </div>
                          )}

                          <div>
                            <h2 className="two-line-clamp">{profile.display_name ?? "Spotify user"}</h2>
                            <p className="profile-username single-line-ellipsis">@{profile.username ?? "spotify-user"}</p>
                          </div>
                        </a>
                      </div>

                      <div className="actions actions-right actions-in-card">
                        <button
                          className="primary-button"
                          onClick={handleAuthAction}
                          type="button"
                        >
                          Reconnect Spotify
                        </button>
                        <button className="secondary-button" onClick={() => void logout()} type="button">
                          Log out
                        </button>
                      </div>
                    </section>
                  ) : null}
                </div>
              </div>
            </nav>

          <div className="dashboard-grid">
            {renderDualSectionCard({
              title: "Top Artists",
              section: "artists",
              anchorId: "artists",
              leftTitle: "All time",
              rightTitle: "Recent",
              leftContent: renderArtistColumn(
                "artistsAllTime",
                profile.followed_artists,
                profile.followed_artists_list_available,
                "Spotify returned no top artists for this account.",
                "Top artists are not available for this session yet. Log out and log back in to grant access.",
              ),
              rightContent: renderArtistColumn(
                "artistsRecent",
                profile.recent_top_artists,
                profile.recent_top_artists_available,
                "Spotify returned no recent top artists for this account.",
                "Recent top artists are not available for this session yet. Log out and log back in to grant access.",
              ),
              previewItemsLeft: previewItems(profile.followed_artists),
              previewItemsRight: previewItems(profile.recent_top_artists),
            })}

            {renderDualSectionCard({
              title: "Top Tracks",
              section: "tracks",
              anchorId: "tracks",
              leftTitle: "All time",
              rightTitle: "Recent",
              leftContent: renderTrackColumn(
                "tracksAllTime",
                profile.top_tracks,
                profile.top_tracks_available,
                "Spotify returned no top tracks for this account.",
                "Top tracks are not available for this session yet. Log out and log back in to grant access.",
              ),
              rightContent: renderTrackColumn(
                "tracksRecent",
                profile.recent_top_tracks,
                profile.recent_top_tracks_available,
                "Spotify returned no recent top tracks for this account.",
                "Recent top tracks are not available for this session yet. Log out and log back in to grant access.",
              ),
              previewItemsLeft: previewItems(profile.top_tracks),
              previewItemsRight: previewItems(profile.recent_top_tracks),
            })}

            {renderDualSectionCard({
              title: "Top Albums",
              section: "albums",
              anchorId: "albums",
              leftTitle: "All time",
              rightTitle: "Recent",
              leftContent: renderAlbumColumn(
                "albumsAllTime",
                profile.top_albums,
                profile.top_albums_available,
                "Spotify returned no top albums for this account.",
                "Top albums are not available for this session yet. Log out and log back in to grant access.",
              ),
              rightContent: renderAlbumColumn(
                "albumsRecent",
                profile.recent_top_albums,
                profile.recent_top_albums_available,
                "Spotify returned no recent top albums for this account.",
                "Recent top albums are not available for this session yet. Log out and log back in to grant access.",
              ),
              previewItemsLeft: previewItems(profile.top_albums),
              previewItemsRight: previewItems(profile.recent_top_albums),
            })}

            {renderPlaylistsSection()}

            {renderDualSectionCard({
              title: "Current Activity",
              section: "recent",
              anchorId: "activity",
              leftTitle: "Recently played",
              rightTitle: "Recently liked",
              leftContent: renderTrackColumn(
                "recent",
                profile.recent_tracks,
                profile.recent_tracks_available,
                "Spotify returned no recent listening history.",
                "Recent listening is not available for this session yet. Log out and log back in to grant the updated Spotify permissions.",
              ),
              rightContent: renderTrackColumn(
                "likes",
                profile.recent_likes_tracks,
                profile.recent_likes_available,
                "Spotify returned no recently liked tracks.",
                "Liked tracks are not available for this session yet. Log out and log back in to grant library access.",
              ),
              previewItemsLeft: previewItems(profile.recent_tracks),
              previewItemsRight: previewItems(profile.recent_likes_tracks),
            })}
          </div>
          </>
        )}
      </section>
    </main>
  );
}
