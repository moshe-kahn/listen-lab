import { type ReactNode, useEffect, useState } from "react";

type SessionResponse = {
  authenticated: boolean;
  display_name: string | null;
  spotify_user_id: string | null;
  email?: string | null;
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
};

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8000";
const PAGE_SIZE = 5;
const PLAYLISTS_PAGE_SIZE = 10;

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
  const [loadingProfile, setLoadingProfile] = useState(false);
  const [profileLoadAttempted, setProfileLoadAttempted] = useState(false);
  const [openSections, setOpenSections] = useState<Record<SectionKey, boolean>>(INITIAL_OPEN_SECTIONS);
  const [sectionPages, setSectionPages] = useState<Record<SectionKey, number>>(INITIAL_SECTION_PAGES);

  useEffect(() => {
    const url = new URL(window.location.href);
    if (url.pathname === "/auth/callback") {
      const status = url.searchParams.get("status");
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
      } else {
        setProfile(null);
        setStatusMessage("Not connected yet. Use Spotify login to start the auth flow.");
      }
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : "Failed to load session.");
    }
  }

  function startLogin() {
    window.location.href = `${apiBaseUrl}/auth/login`;
  }

  function toggleSection(section: SectionKey) {
    setOpenSections((current) => ({
      ...current,
      [section]: !current[section],
    }));
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
    try {
      const response = await fetch(`${apiBaseUrl}/me`, {
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
      setSectionPages(INITIAL_SECTION_PAGES);
      setStatusMessage("");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load Spotify profile.";
      setStatusMessage(message);
    } finally {
      setLoadingProfile(false);
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
      leftTitle,
      rightTitle,
      leftContent,
      rightContent,
      previewItemsLeft,
      previewItemsRight,
    } = props;

    return (
      <section className="info-card info-card-wide">
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
      <section className="info-card info-card-wide">
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

  return (
    <main className="app-shell">
      <section className="hero-card">
        <div className="top-bar">
          <div className="top-copy">
            <p className="eyebrow">ListenLab</p>
            <h1>Your Spotify snapshot</h1>
            <p className="lede three-line-clamp">
              Connect your account and browse the listening, library, and profile details Spotify
              already makes available to ListenLab.
            </p>
          </div>

          <div className="top-side">
            {profile ? (
              <section className="profile-card top-profile-card">
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

                <div className="profile-links">
                  {statusMessage ? <p className="inline-status">{statusMessage}</p> : null}
                </div>
              </section>
            ) : null}

            <div className="actions actions-right">
              <button className="primary-button" onClick={startLogin} type="button">
                {session?.authenticated ? "Reconnect Spotify" : "Log in with Spotify"}
              </button>
              {session?.authenticated ? (
                <button className="secondary-button" onClick={() => void logout()} type="button">
                  Log out
                </button>
              ) : null}
            </div>
          </div>
        </div>

        {!profile ? (
          <>
            <div className="status-panel">
              <h2>Status</h2>
              <p>
                {statusMessage ||
                  (loadingProfile
                    ? "Loading your Spotify data..."
                    : "Connected to Spotify. Waiting for profile data to finish loading.")}
              </p>
            </div>

            <div className="info-card">
              <h2>Waiting for Spotify data</h2>
              <p className="empty-copy">
                Sign in to load your Spotify profile summary here. Once connected, this page will
                show your account snapshot, recent listening, and playlists you own.
              </p>
            </div>
          </>
        ) : (
          <div className="dashboard-grid">
            {renderDualSectionCard({
              title: "Top Artists",
              section: "artists",
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
        )}
      </section>
    </main>
  );
}
