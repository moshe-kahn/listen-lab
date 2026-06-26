import type { RecentTrack } from "../../types/appTypes";
import { PlaybackActionMenu, type PlaybackAction } from "../playback/PlaybackActionMenu";

type PlaylistTrackListProps = {
  currentTrackUri: string | null;
  entries: RecentTrack[];
  error: string | null;
  formatPlaybackClock: (positionMs: number) => string;
  hasMore: boolean;
  hasPremiumPlayback: boolean;
  isTrackPlaying: (trackUri: string | null) => boolean;
  loading: boolean;
  onPlayAll: (action: PlaybackAction) => Promise<void>;
  onPlayTrack: (track: RecentTrack, action: PlaybackAction) => Promise<void>;
  onSelectTrack: (track: RecentTrack) => void;
  playbackDurationMs: number;
  playbackPaused: boolean;
  playbackPositionMs: number;
  trackUriWithFallback: (uri: string | null | undefined, trackId: string | null | undefined) => string | null;
};

function playlistTrackArtists(track: RecentTrack) {
  return track.artists?.map((artist) => artist.name).filter(Boolean).join(", ") || track.artist_name || "Unknown artist";
}

export function PlaylistTrackList({
  currentTrackUri,
  entries,
  error,
  formatPlaybackClock,
  hasMore,
  hasPremiumPlayback,
  isTrackPlaying,
  loading,
  onPlayAll,
  onPlayTrack,
  onSelectTrack,
  playbackDurationMs,
  playbackPaused,
  playbackPositionMs,
  trackUriWithFallback,
}: PlaylistTrackListProps) {
  const playableEntries = entries.filter((track) => trackUriWithFallback(track.uri, track.track_id));

  return (
    <div className="detail-modal-album-tracks detail-modal-album-tracks-full detail-modal-album-tracks-no-with">
      <div className="detail-modal-album-header">
        {hasPremiumPlayback ? (
          <PlaybackActionMenu
            ariaLabel="Playlist playback options"
            buttonClassName="detail-album-play-all-button"
            disabled={playableEntries.length === 0}
            placement="adjacent"
            onAction={onPlayAll}
          >
            Play all
          </PlaybackActionMenu>
        ) : (
          <span aria-hidden="true" />
        )}
        <span className="detail-modal-album-title-header">
          {entries.length > 0 ? `${entries.length}${hasMore ? "+" : ""} tracks` : "Tracks"}
        </span>
        <span className="detail-modal-album-liked-header">Album</span>
        <span className="detail-modal-album-actions-header">
          <span className="detail-modal-album-preview-header">Artist</span>
          <span className="detail-modal-album-last-played-header">Time</span>
        </span>
      </div>
      {loading && entries.length === 0 ? <p className="detail-modal-preview-missing">Loading playlist...</p> : null}
      {!loading && error ? <p className="detail-modal-preview-missing">{error}</p> : null}
      {!error && entries.length > 0 ? (
        <>
          <ul className={`detail-album-track-list${loading ? " detail-album-track-list-updating" : ""}`}>
            {entries.map((track, index) => {
              const rowTrackUri = trackUriWithFallback(track.uri, track.track_id);
              const rowPlaying = isTrackPlaying(rowTrackUri);
              const rowIsCurrentTrack = Boolean(rowTrackUri && currentTrackUri === rowTrackUri);
              const rowBaseDurationMs = track.duration_ms ?? (rowIsCurrentTrack ? playbackDurationMs : null);
              const rowButtonTimeMs = rowIsCurrentTrack
                ? rowPlaying
                  ? Math.min(Math.max(0, playbackPositionMs), rowBaseDurationMs ?? playbackPositionMs)
                  : playbackPaused
                    ? Math.max(0, playbackPositionMs)
                    : rowBaseDurationMs
                : rowBaseDurationMs;
              return (
                <li className={`detail-album-track-row${rowIsCurrentTrack ? " detail-album-track-row-selected" : ""}`} key={track.track_id ?? `${track.track_name}-${index}`}>
                  {hasPremiumPlayback ? (
                    <PlaybackActionMenu
                      ariaLabel={rowPlaying ? "Currently playing in ListenLab" : rowTrackUri ? `Play ${track.track_name ?? "track"} in ListenLab` : `${track.track_name ?? "Track"} is not playable`}
                      buttonClassName={`secondary-button detail-album-track-play-button${rowPlaying ? " detail-icon-button-playing" : ""}`}
                      disabled={!rowTrackUri}
                      isPlaying={rowPlaying}
                      placement="overlay-trigger"
                      onAction={(action) => onPlayTrack(track, action)}
                    >
                      {rowPlaying ? (
                        <span className="detail-wave-icon" aria-hidden="true">
                          <span />
                          <span />
                          <span />
                        </span>
                      ) : (
                        <span className="detail-play-icon" aria-hidden="true">{"\u25B6"}</span>
                      )}
                      <span className={`detail-album-track-play-time${rowIsCurrentTrack && playbackPaused ? " detail-album-track-play-time-flash" : ""}`}>
                        {rowButtonTimeMs != null ? formatPlaybackClock(rowButtonTimeMs) : "?:??"}
                      </span>
                    </PlaybackActionMenu>
                  ) : <span aria-hidden="true" />}
                  <button
                    className="detail-album-track-name-button single-line-ellipsis"
                    onClick={() => onSelectTrack(track)}
                    type="button"
                  >
                    {track.track_name ?? "Unknown track"}
                  </button>
                  <span className="detail-modal-album-track-tags single-line-ellipsis">{track.album_name ?? "Unknown album"}</span>
                  <span className="detail-modal-album-track-actions">
                    <span className="detail-modal-album-track-preview single-line-ellipsis">{playlistTrackArtists(track)}</span>
                    <span className="detail-modal-album-track-last-played">
                      {rowBaseDurationMs != null ? formatPlaybackClock(rowBaseDurationMs) : "--"}
                    </span>
                  </span>
                </li>
              );
            })}
          </ul>
          {hasMore ? <p className="detail-modal-preview-missing">Showing first {entries.length} tracks.</p> : null}
        </>
      ) : null}
      {!loading && !error && entries.length === 0 ? <p className="detail-modal-preview-missing">No tracks were returned for this playlist.</p> : null}
    </div>
  );
}
