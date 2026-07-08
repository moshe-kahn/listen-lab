import { type CSSProperties, useState } from "react";

type SandboxTrack = {
  id: string;
  title: string;
  artist: string;
  album: string;
  duration: string;
  color: string;
  status?: string;
};

type SandboxAlbum = {
  id: string;
  title: string;
  tag: string;
  color: string;
};

type SandboxPlaylist = {
  id: string;
  title: string;
  meta: string;
  color: string;
};

type SandboxQueueGroup = {
  id: string;
  label: string;
  meta: string;
  color: string;
  tracks: SandboxTrack[];
};

const currentTrack: SandboxTrack = {
  id: "current",
  title: "Colors",
  artist: "Black Pumas",
  album: "Black Pumas",
  duration: "4:06",
  color: "#d9332e",
  status: "Current",
};

const historyTracks: SandboxTrack[] = [
  { id: "history-1", title: "Sweet Conversations", artist: "Black Pumas", album: "Chronicles", duration: "4:02", color: "#d34a31" },
  { id: "history-2", title: "Daydreaming", artist: "Radiohead", album: "A Moon Shaped Pool", duration: "6:24", color: "#b8bdc2" },
  { id: "history-3", title: "Texas Sun", artist: "Khruangbin, Leon Bridges", album: "Texas Sun", duration: "4:12", color: "#3e8f8f" },
];

const albums: SandboxAlbum[] = [
  { id: "album-1", title: "Black Pumas", tag: "2019 Album", color: "#d9332e" },
  { id: "album-2", title: "Black Pumas Deluxe", tag: "2020 Deluxe", color: "#c99536" },
  { id: "album-3", title: "Colors", tag: "2019 Single", color: "#2a6f83" },
  { id: "album-4", title: "Live At Arlyn", tag: "2021 Live", color: "#6b5ba8" },
];

const playlists: SandboxPlaylist[] = [
  { id: "playlist-1", title: "Recent Likes", meta: "11 tracks", color: "#d9332e" },
  { id: "playlist-2", title: "Morning Drive", meta: "42 tracks", color: "#2a6f83" },
  { id: "playlist-3", title: "Late Night Albums", meta: "8 tracks", color: "#273f74" },
  { id: "playlist-4", title: "Radio Pulls", meta: "19 tracks", color: "#6b5ba8" },
];

const queueGroups: SandboxQueueGroup[] = [
  {
    id: "queue-1",
    label: "Recent Likes",
    meta: "11 tracks - track 8",
    color: "#d9332e",
    tracks: [
      { id: "queue-1-0", title: "Black Moon Rising", artist: "Black Pumas", album: "Black Pumas", duration: "3:42", color: "#8a352e", status: "Previous" },
      currentTrack,
      { id: "queue-1-2", title: "Fire", artist: "Black Pumas", album: "Black Pumas", duration: "4:06", color: "#bf5539", status: "Up next" },
    ],
  },
  {
    id: "queue-2",
    label: "Late Night Albums",
    meta: "8 tracks - next",
    color: "#273f74",
    tracks: [
      { id: "queue-2-1", title: "The Adults Are Talking", artist: "The Strokes", album: "The New Abnormal", duration: "5:09", color: "#db6c54" },
      { id: "queue-2-2", title: "Giorgio by Moroder", artist: "Daft Punk", album: "Random Access Memories", duration: "9:04", color: "#80634f" },
    ],
  },
];

function noop() {
  return undefined;
}

function Swatch({ color, label, className = "" }: { color: string; label: string; className?: string }) {
  return (
    <span className={`player-ui-sandbox-swatch ${className}`} style={{ "--sandbox-cover": color } as CSSProperties} aria-hidden="true">
      {label.slice(0, 1).toUpperCase()}
    </span>
  );
}

function TrackRow({ track, index }: { track: SandboxTrack; index?: number }) {
  const current = track.status === "Current";
  const upNext = track.status === "Up next";
  return (
    <div className={`player-recent-row player-queue-row${current ? " player-queue-row-current" : ""}${upNext ? " player-queue-row-up-next" : ""}`}>
      <button aria-label={`Play ${track.title}`} className="player-queue-cover-button" onClick={noop} type="button">
        <Swatch color={track.color} label={track.album} className="player-recent-cover" />
        <span className="player-queue-cover-play" aria-hidden="true">
          <svg viewBox="0 0 24 24">
            <path d="M8 5.5v13l10-6.5-10-6.5Z" />
          </svg>
        </span>
      </button>
      <button className="player-recent-copy player-queue-copy-button" onClick={noop} type="button">
        <span className="player-recent-track single-line-ellipsis">{track.title}</span>
        <span className="player-recent-artist single-line-ellipsis">{track.artist}</span>
      </button>
      <span className="player-queue-row-actions">
        <button className="player-queue-bookmark-button" onClick={noop} type="button" aria-label={`Bookmark ${track.title}`}>
          <svg aria-hidden="true" viewBox="0 0 20 20">
            <path d="M5 3.5h10v13l-5-3.2-5 3.2v-13Z" />
          </svg>
        </button>
        {track.status ? <span className={`player-queue-status${upNext ? " player-queue-status-next" : ""}`}>{track.status}</span> : null}
        {!track.status && index != null ? <span className="player-queue-status player-queue-status-played">{index + 1}</span> : null}
      </span>
    </div>
  );
}

export function PlayerUiSandbox() {
  const [openGroupId, setOpenGroupId] = useState(queueGroups[0].id);
  const [openTrackPopup, setOpenTrackPopup] = useState<"previous" | "next" | null>(null);
  const openGroup = queueGroups.find((group) => group.id === openGroupId) ?? queueGroups[0];
  const previousTrack = openGroup.tracks.find((track) => track.status === "Previous") ?? historyTracks[0];
  const nextTrack = openGroup.tracks.find((track) => track.status === "Up next") ?? openGroup.tracks.find((track) => track.id !== currentTrack.id);

  return (
    <main className="player-ui-sandbox">
      <header className="player-ui-sandbox-header">
        <div>
          <p>Player UI sandbox</p>
          <h1>Static data, no API calls</h1>
        </div>
        <a href="/" aria-label="Exit sandbox">Exit</a>
      </header>

      <section className="info-card info-card-wide player-home-panel player-ui-sandbox-shell player-ui-sandbox-variant-a" id="player-sandbox-a" aria-label="Player UI sandbox variant A">
        <div className="player-ui-sandbox-variant-label">
          <span>Variant A</span>
          <strong>Current replica</strong>
        </div>
        <div className="player-home-layout">
          <aside className="player-recent-column player-home-history-column" aria-label="Dummy recently played songs">
            <div className="player-recent-header">
              <h3>History</h3>
            </div>
            <div className="player-recent-list">
              {historyTracks.map((track, index) => (
                <button className="player-recent-row player-home-history-row" key={track.id} onClick={noop} type="button">
                  <Swatch color={track.color} label={track.album} className="player-recent-cover" />
                  <span className="player-recent-copy">
                    <span className="player-recent-track single-line-ellipsis">{track.title}</span>
                    <span className="player-recent-artist single-line-ellipsis">{track.artist}</span>
                    <span className="player-recent-completion" aria-hidden="true">
                      <span className="player-recent-completion-fill" style={{ width: `${78 - index * 18}%` }} />
                    </span>
                  </span>
                </button>
              ))}
            </div>
            <button className="secondary-button player-menu-footer-button" onClick={noop} type="button">
              complete listen log
            </button>
          </aside>

          <div className="player-current-column">
            <div className="player-home-control-box">
              <div className="player-menu-summary">
                <div className="player-menu-copy">
                  <div className="player-menu-copy-top">
                    <h2>
                      <button className="player-menu-title-button player-menu-title-scroll" onClick={noop} type="button">
                        <span>{currentTrack.title}</span>
                      </button>
                    </h2>
                  </div>
                  <div className="player-menu-artist-row">
                    <button className="player-menu-meta-button player-menu-line player-menu-artist-button single-line-ellipsis" onClick={noop} type="button">
                      <Swatch color="#2f6f54" label={currentTrack.artist} className="player-menu-artist-image" />
                      <span className="single-line-ellipsis">{currentTrack.artist}</span>
                    </button>
                    <button aria-label="Open in Spotify" className="player-menu-external player-menu-external-inline" onClick={noop} type="button">
                      <svg aria-hidden="true" viewBox="0 0 24 24">
                        <path d="M12 2.2a9.8 9.8 0 1 0 0 19.6 9.8 9.8 0 0 0 0-19.6Zm4.49 14.13a.72.72 0 0 1-.99.24c-2.7-1.65-6.1-2.02-10.1-1.11a.72.72 0 1 1-.32-1.4c4.38-1 8.14-.57 11.17 1.28.34.2.44.65.24.99Z" />
                      </svg>
                    </button>
                  </div>
                </div>
              </div>

              <div className="player-progress" aria-label="Playback progress">
                <input className="player-progress-slider" max={246000} min={0} onChange={noop} step={1000} type="range" value={103000} />
                <div className="player-progress-times">
                  <span>1:43</span>
                  <span>{currentTrack.duration}</span>
                </div>
              </div>

              <div className="actions actions-centered actions-in-card player-transport-controls">
                <button aria-label="Previous track" className="secondary-button player-icon-button" onClick={noop} type="button">
                  <svg aria-hidden="true" viewBox="0 0 24 24">
                    <path d="M7 5h2v14H7V5Zm3.7 7L19 5.8v12.4L10.7 12Z" />
                  </svg>
                </button>
                <button className="primary-button" onClick={noop} type="button">Pause</button>
                <button aria-label="Next track" className="secondary-button player-icon-button" onClick={noop} type="button">
                  <svg aria-hidden="true" viewBox="0 0 24 24">
                    <path d="M15 5h2v14h-2V5ZM5 5.8 13.3 12 5 18.2V5.8Z" />
                  </svg>
                </button>
              </div>

              <div className="player-track-secondary-actions" aria-label="Track edit actions">
                <button aria-label="Remove current song from queue" className="secondary-button player-icon-button player-track-minus-button" onClick={noop} type="button">
                  <span aria-hidden="true">-</span>
                </button>
                <button aria-label="Track actions" className="secondary-button player-icon-button player-track-plus-button" onClick={noop} type="button">
                  <span aria-hidden="true">+</span>
                </button>
              </div>
            </div>
          </div>

          <aside className="player-recent-column player-queue-column player-home-queue-column" aria-label="Dummy ListenLab queue">
            <div className="player-recent-header">
              <div className="player-queue-heading-menu">
                <button aria-expanded="false" className="player-queue-heading-button" onClick={noop} type="button">
                  <span className="player-queue-heading-main">Queue</span>
                  <span className="player-queue-heading-context single-line-ellipsis">{openGroup.label}</span>
                </button>
              </div>
              <div className="player-queue-header-actions">
                <button className="player-queue-header-button" onClick={noop} type="button" aria-label="Loop queue">
                  <svg aria-hidden="true" viewBox="0 0 24 24">
                    <path d="M7 7h9.2l-1.8-1.8L15.8 3.8 20 8l-4.2 4.2-1.4-1.4L16.2 9H7a3 3 0 0 0 0 6h1v2H7A5 5 0 0 1 7 7Z" />
                  </svg>
                </button>
              </div>
            </div>
            <div className="player-ui-sandbox-context-list">
              {queueGroups.map((group) => (
                <button
                  className={`player-queue-context-item${group.id === openGroup.id ? " player-queue-context-item-active" : ""}`}
                  key={group.id}
                  onClick={() => setOpenGroupId(group.id)}
                  type="button"
                >
                  <Swatch color={group.color} label={group.label} className="player-queue-context-image player-queue-context-image-fallback" />
                  <span className="player-queue-context-copy">
                    <span className="single-line-ellipsis">{group.label}</span>
                    <span className="single-line-ellipsis">{group.meta}</span>
                  </span>
                  {group.id === openGroup.id ? <span className="player-queue-current-dot" aria-label="Current queue context" /> : null}
                </button>
              ))}
            </div>
            <div className="player-recent-list">
              {openGroup.tracks.map((track, index) => (
                <div className="player-home-queue-preview-row" key={track.id}>
                  <TrackRow track={track} index={index} />
                </div>
              ))}
            </div>
          </aside>

          <div className="player-home-related">
            <div className="player-home-appearances">
              <section className="player-home-appearance-section" aria-label="Dummy album appearances">
                <span className="player-home-appearance-label">Appears on</span>
                <div className="player-home-album-strip">
                  {albums.map((album) => (
                    <button className="player-home-album-card" key={album.id} onClick={noop} type="button">
                      <span className="player-home-album-card-art">
                        <Swatch color={album.color} label={album.title} className="player-home-album-card-fallback" />
                        <span className="player-home-album-card-tags">
                          <span>{album.tag}</span>
                        </span>
                      </span>
                      <span className="player-home-album-card-title single-line-ellipsis">{album.title}</span>
                    </button>
                  ))}
                </div>
              </section>
              <section className="player-home-appearance-section" aria-label="Dummy playlist memberships">
                <span className="player-home-appearance-label">In playlists</span>
                <div className="player-home-playlist-strip">
                  {playlists.map((playlist) => (
                    <button className="player-home-playlist-card" key={playlist.id} onClick={noop} type="button">
                      <Swatch color={playlist.color} label={playlist.title} className="player-home-playlist-card-image player-home-playlist-card-fallback" />
                      <span className="single-line-ellipsis">
                        {playlist.title}
                        <small className="single-line-ellipsis">{playlist.meta}</small>
                      </span>
                    </button>
                  ))}
                </div>
              </section>
            </div>
          </div>
        </div>
      </section>

      <section className="info-card info-card-wide player-home-panel player-ui-sandbox-shell player-ui-sandbox-variant-b" id="player-sandbox-b" aria-label="Player UI sandbox variant B">
        <div className="player-ui-sandbox-variant-label">
          <span>Variant B</span>
          <strong>Compact center, boxed navigation</strong>
        </div>
        <div className="player-ui-sandbox-b-layout">
          <section className="player-current-column player-ui-sandbox-b-current" aria-label="Dummy current track">
            <div className="player-home-control-box">
              {openGroup ? (
                <button className="player-ui-sandbox-context-box" onClick={noop} type="button" aria-label={`Current context ${openGroup.label}`}>
                  <Swatch color={openGroup.color} label={openGroup.label} className="player-ui-sandbox-context-box-image" />
                  <span className="player-ui-sandbox-context-box-copy">
                    <span className="player-ui-sandbox-control-kicker">Context</span>
                    <strong className="single-line-ellipsis">{openGroup.label}</strong>
                    <small className="single-line-ellipsis">{openGroup.meta}</small>
                  </span>
                </button>
              ) : null}
              <div className="player-menu-summary">
                <div className="player-menu-copy">
                  <div className="player-menu-copy-top">
                    <h2>
                      <button className="player-menu-title-button player-menu-title-scroll" onClick={noop} type="button">
                        <span>{currentTrack.title}</span>
                      </button>
                    </h2>
                  </div>
                  <div className="player-menu-artist-row">
                    <button className="player-menu-meta-button player-menu-line player-menu-artist-button single-line-ellipsis" onClick={noop} type="button">
                      <Swatch color="#2f6f54" label={currentTrack.artist} className="player-menu-artist-image" />
                      <span className="single-line-ellipsis">{currentTrack.artist}</span>
                    </button>
                  </div>
                </div>
              </div>
              <div className="player-ui-sandbox-nav-box" aria-label="Playback navigation">
                <div className="player-progress" aria-label="Playback progress">
                  <input className="player-progress-slider" max={246000} min={0} onChange={noop} step={1000} type="range" value={103000} />
                  <div className="player-progress-times">
                    <span>1:43</span>
                    <span>{currentTrack.duration}</span>
                  </div>
                </div>
                <div className="actions actions-centered actions-in-card player-transport-controls player-ui-sandbox-b-transport">
                  <div className="player-ui-sandbox-nav-track player-ui-sandbox-nav-track-prev">
                    <button
                      aria-expanded={openTrackPopup === "previous"}
                      className="player-ui-sandbox-nav-track-button"
                      onClick={() => setOpenTrackPopup(openTrackPopup === "previous" ? null : "previous")}
                      type="button"
                    >
                      <strong className="single-line-ellipsis">{previousTrack.title}</strong>
                      <small className="single-line-ellipsis">{previousTrack.artist}</small>
                    </button>
                    {openTrackPopup === "previous" ? (
                      <div className="player-ui-sandbox-track-popover player-ui-sandbox-track-popover-prev" role="dialog" aria-label="Previous track details">
                        <strong>{previousTrack.title}</strong>
                        <span>{previousTrack.artist}</span>
                        <small>{previousTrack.album} - {previousTrack.duration}</small>
                      </div>
                    ) : null}
                    <button className="player-ui-sandbox-nav-link" onClick={noop} type="button">Recents</button>
                  </div>
                  <div className="player-ui-sandbox-button-cluster">
                    <button aria-label={`Previous track ${previousTrack.title}`} className="secondary-button player-ui-sandbox-nav-button player-ui-sandbox-nav-button-back" onClick={noop} type="button">
                      <svg viewBox="0 0 24 24">
                        <path d="M7 5h2v14H7V5Zm3.7 7L19 5.8v12.4L10.7 12Z" />
                      </svg>
                    </button>
                    <button className="primary-button player-ui-sandbox-pause-button" onClick={noop} type="button">Pause</button>
                    {nextTrack ? (
                      <button aria-label={`Next track ${nextTrack.title}`} className="secondary-button player-ui-sandbox-nav-button player-ui-sandbox-nav-button-next" onClick={noop} type="button">
                        <svg viewBox="0 0 24 24">
                          <path d="M15 5h2v14h-2V5ZM5 5.8 13.3 12 5 18.2V5.8Z" />
                        </svg>
                      </button>
                    ) : null}
                  </div>
                  {nextTrack ? (
                    <div className="player-ui-sandbox-nav-track player-ui-sandbox-nav-track-next">
                      <button
                        aria-expanded={openTrackPopup === "next"}
                        className="player-ui-sandbox-nav-track-button"
                        onClick={() => setOpenTrackPopup(openTrackPopup === "next" ? null : "next")}
                        type="button"
                      >
                        <strong className="single-line-ellipsis">{nextTrack.title}</strong>
                        <small className="single-line-ellipsis">{nextTrack.artist}</small>
                      </button>
                      {openTrackPopup === "next" ? (
                        <div className="player-ui-sandbox-track-popover player-ui-sandbox-track-popover-next" role="dialog" aria-label="Next track details">
                          <strong>{nextTrack.title}</strong>
                          <span>{nextTrack.artist}</span>
                          <small>{nextTrack.album} - {nextTrack.duration}</small>
                        </div>
                      ) : null}
                      <button className="player-ui-sandbox-nav-link" onClick={noop} type="button">Queue</button>
                    </div>
                  ) : null}
                </div>
              </div>
              <div className="player-ui-sandbox-action-box" aria-label="Track edit actions">
                <span className="player-ui-sandbox-control-kicker">Track actions</span>
                <div className="player-track-secondary-actions">
                  <button aria-label="Remove current song from queue" className="secondary-button player-icon-button player-track-minus-button" onClick={noop} type="button">
                    <span aria-hidden="true">-</span>
                  </button>
                  <button aria-label="Track actions" className="secondary-button player-icon-button player-track-plus-button" onClick={noop} type="button">
                    <span aria-hidden="true">+</span>
                  </button>
                </div>
              </div>
            </div>
            <div className="player-home-related">
              <div className="player-home-appearances">
                <section className="player-home-appearance-section" aria-label="Dummy album appearances">
                  <span className="player-home-appearance-label">Appears on</span>
                  <div className="player-home-album-strip">
                    {albums.map((album) => (
                      <button className="player-home-album-card" key={album.id} onClick={noop} type="button">
                        <span className="player-home-album-card-art">
                          <Swatch color={album.color} label={album.title} className="player-home-album-card-fallback" />
                          <span className="player-home-album-card-tags">
                            <span>{album.tag}</span>
                          </span>
                        </span>
                        <span className="player-home-album-card-title single-line-ellipsis">{album.title}</span>
                      </button>
                    ))}
                  </div>
                </section>
                <section className="player-home-appearance-section" aria-label="Dummy playlist memberships">
                  <span className="player-home-appearance-label">In playlists</span>
                  <div className="player-home-playlist-strip">
                    {playlists.map((playlist) => (
                      <button className="player-home-playlist-card" key={playlist.id} onClick={noop} type="button">
                        <Swatch color={playlist.color} label={playlist.title} className="player-home-playlist-card-image player-home-playlist-card-fallback" />
                        <span className="single-line-ellipsis">
                          {playlist.title}
                          <small className="single-line-ellipsis">{playlist.meta}</small>
                        </span>
                      </button>
                    ))}
                  </div>
                </section>
              </div>
            </div>
          </section>
        </div>
      </section>
    </main>
  );
}
