import type { CSSProperties, Dispatch, ReactNode, Ref, SetStateAction } from "react";

import type {
  AlbumTrackEntry,
  ArtistAlbumEntry,
  PlaybackActionRequest,
  PlayerQueueTrack,
  PlayerTrackSummary,
  PreviewItem,
  RecordingRelationRows,
  RecordingTrackCandidateMember,
  ReleaseTrackDetailResponse,
  ReleaseTrackDetailSourceVersion,
  TrackArtistEntry,
} from "../../types/appTypes";
import { LikedBadge } from "../common/LikedBadge";
import { NewTrackBadge } from "../common/NewTrackBadge";
import { ReleaseSiblingBadge } from "../common/ReleaseSiblingBadge";
import { PlaybackActionMenu, type PlaybackAction } from "../playback/PlaybackActionMenu";

type AlbumPlaybackQueue = {
  playlistUris: string[];
  queueTracks: PlayerQueueTrack[];
  queueCursor: number;
  queueContext: {
    label: string;
    url?: string | null;
  };
};

type DetailPreviewModalProps = {
  albumTrackEntries: AlbumTrackEntry[];
  albumTrackEntriesError: string | null;
  albumTrackEntriesLoading: boolean;
  albumTrackIsKnownLiked: (track: AlbumTrackEntry) => boolean;
  albumTrackLastSortMode: "recent" | "oldest" | null;
  albumTrackListRef: Ref<HTMLUListElement>;
  albumTrackPreviewKey: (track: AlbumTrackEntry, rowTrackUri: string | null) => string;
  albumTracklistSummaryLabel: (entries: AlbumTrackEntry[]) => string;
  artistEntriesForAlbumTrack: (track: AlbumTrackEntry) => TrackArtistEntry[];
  artistNameMatches: (candidate: string | null | undefined, target: string | null | undefined) => boolean;
  backendSelectedPreviewArtistAlbums: ArtistAlbumEntry[] | null;
  buildAlbumPlaybackQueue: (selectedTrackUri: string | null, entries?: AlbumTrackEntry[], contextPreview?: PreviewItem | null) => AlbumPlaybackQueue | null;
  clearAlbumWithArtistHighlight: () => void;
  currentTrack: PlayerTrackSummary | null;
  detailOptionsOpen: boolean;
  displayAlbumTrackEntries: AlbumTrackEntry[];
  familyCoverRemixRelationshipKinds: Set<string>;
  formatCompactRelativeAge: (value: string | null | undefined) => string | null;
  formatPlaybackClock: (positionMs: number) => string;
  handleAlbumPlayAll: (action?: PlaybackAction) => Promise<void>;
  handlePlaybackAction: (action: PlaybackAction, request: PlaybackActionRequest) => Promise<void>;
  hasPremiumPlayback: boolean;
  hoveredAlbumWithArtistName: string | null;
  isTrackPlaying: (trackUri: string | null) => boolean;
  localStarredTrackById: Record<string, boolean>;
  nextLastPlayedSortMode: (current: "recent" | "oldest" | null) => "recent" | "oldest" | null;
  openAlbumTrackPreview: (track: AlbumTrackEntry) => void;
  openAlbumWithArtistPreview: (artist: TrackArtistEntry) => void;
  openRecordingCandidateReleaseTrack: (member: RecordingTrackCandidateMember, detailView?: "recording" | "release") => void;
  openReleaseSourceVersion: (version: ReleaseTrackDetailSourceVersion, detailView?: "recording" | "release") => void;
  openSelectedAlbumArtistPreview: (artist?: TrackArtistEntry) => void;
  openSelectedArtistMemberPreview: (artist: TrackArtistEntry) => void;
  openSelectedTrackAlbumPreview: () => void;
  openSelectedTrackArtistPreview: (artist?: TrackArtistEntry) => void;
  pausedTimeFlashOn: boolean;
  playbackDurationMs: number;
  playbackPaused: boolean;
  playbackPositionMs: number;
  playerSummaryFromAlbumTrack: (track: AlbumTrackEntry) => PlayerTrackSummary;
  previewAlbumHeading: (preview: PreviewItem) => string | null;
  previewPlayedTrackKeys: Set<string>;
  previewingTrackUri: string | null;
  recordingMemberAlbumImageUrl: (member: RecordingTrackCandidateMember) => string | null;
  recordingMemberReleaseYear: (member: RecordingTrackCandidateMember) => string | null;
  recordingVariationStripRef: Ref<HTMLDivElement>;
  releaseSourceVersionAlbumImageUrl: (version: ReleaseTrackDetailSourceVersion) => string | null;
  releaseSourceVersionPlayCountLabel: (version: ReleaseTrackDetailSourceVersion) => string | null;
  renderSelectedPreviewArtistAlbumSection: (title: string, albums: ArtistAlbumEntry[]) => ReactNode;
  scheduleAlbumWithArtistHighlight: (artistName: string) => void;
  scrollRecordingVariationStrip: (direction: -1 | 1) => void;
  selectedAlbumTrackMarkerTop: (entries: AlbumTrackEntry[], minScrollableTrackCount?: number) => string | null;
  selectedPreview: PreviewItem | null;
  selectedPreviewAlbumGuestArtists: TrackArtistEntry[];
  selectedPreviewAlbumHasGuestArtists: boolean;
  selectedPreviewAlbumMainArtists: TrackArtistEntry[];
  selectedPreviewAlbumSummary: string | null;
  selectedPreviewAppearsOnAlbums: ArtistAlbumEntry[];
  selectedPreviewArtistAlbumsForDisplay: ArtistAlbumEntry[];
  selectedPreviewArtistImageUrl: string | null;
  selectedPreviewArtists: TrackArtistEntry[];
  selectedPreviewCanOpenAlbum: boolean;
  selectedPreviewCanOpenArtist: boolean;
  selectedPreviewCanonicalTrackTitle: string | null;
  selectedPreviewCurrentSpotifyTrackId: string | null;
  selectedPreviewDetailView: "recording" | "release";
  selectedPreviewDisplayRelationRows: RecordingRelationRows;
  selectedPreviewHasReleaseSibling: boolean;
  selectedPreviewIsBookmarked: boolean;
  selectedPreviewIsKnownLiked: boolean;
  selectedPreviewIsSharedArtistPage: boolean;
  selectedPreviewLastListenedLabel: string | null;
  selectedPreviewListenCountLabel: string | null;
  selectedPreviewOtherRecordingMembers: RecordingTrackCandidateMember[];
  selectedPreviewPlaybackTrackUri: string | null;
  selectedPreviewPrimaryArtistAlbums: ArtistAlbumEntry[];
  selectedPreviewRecordingCandidateError: string | null;
  selectedPreviewReleaseAlbumVariationCount: number;
  selectedPreviewReleaseSiblingSourceCount: number | null;
  selectedPreviewReleaseSourceVersionNeedsArrows: boolean;
  selectedPreviewReleaseSourceVersions: ReleaseTrackDetailSourceVersion[];
  selectedPreviewReleaseTrackDetailError: string | null;
  selectedPreviewReleaseTrackDetailReady: ReleaseTrackDetailResponse | null;
  selectedPreviewStarTrackId: string | null;
  selectedPreviewTrackDurationLabel: string | null;
  selectedPreviewTrackGuestArtists: TrackArtistEntry[];
  selectedPreviewTrackMainArtists: TrackArtistEntry[];
  selectedPreviewTrackOptimisticSummary: PlayerTrackSummary | null;
  setAlbumTrackLastSortMode: Dispatch<SetStateAction<"recent" | "oldest" | null>>;
  setDetailOptionsOpen: Dispatch<SetStateAction<boolean>>;
  setLocalBookmarkedTrackById: Dispatch<SetStateAction<Record<string, boolean>>>;
  setLocalStarredTrackById: Dispatch<SetStateAction<Record<string, boolean>>>;
  setSelectedPreview: Dispatch<SetStateAction<PreviewItem | null>>;
  setSelectedPreviewDetailView: Dispatch<SetStateAction<"recording" | "release">>;
  spotifyTrackIdFromUri: (uri: string | null) => string | null;
  toggleAlbumTrackPreview: (track: AlbumTrackEntry, rowTrackUri: string | null) => Promise<void>;
  variationSubtitleFromTitle: (title: string | null | undefined) => string | null;
};

export function DetailPreviewModal(props: DetailPreviewModalProps) {
  const {
    albumTrackEntries,
    albumTrackEntriesError,
    albumTrackEntriesLoading,
    albumTrackIsKnownLiked,
    albumTrackLastSortMode,
    albumTrackListRef,
    albumTrackPreviewKey,
    albumTracklistSummaryLabel,
    artistEntriesForAlbumTrack,
    artistNameMatches,
    backendSelectedPreviewArtistAlbums,
    buildAlbumPlaybackQueue,
    clearAlbumWithArtistHighlight,
    currentTrack,
    detailOptionsOpen,
    displayAlbumTrackEntries,
    familyCoverRemixRelationshipKinds,
    formatCompactRelativeAge,
    formatPlaybackClock,
    handleAlbumPlayAll,
    handlePlaybackAction,
    hasPremiumPlayback,
    hoveredAlbumWithArtistName,
    isTrackPlaying,
    localStarredTrackById,
    nextLastPlayedSortMode,
    openAlbumTrackPreview,
    openAlbumWithArtistPreview,
    openRecordingCandidateReleaseTrack,
    openReleaseSourceVersion,
    openSelectedAlbumArtistPreview,
    openSelectedArtistMemberPreview,
    openSelectedTrackAlbumPreview,
    openSelectedTrackArtistPreview,
    pausedTimeFlashOn,
    playbackDurationMs,
    playbackPaused,
    playbackPositionMs,
    playerSummaryFromAlbumTrack,
    previewAlbumHeading,
    previewPlayedTrackKeys,
    previewingTrackUri,
    recordingMemberAlbumImageUrl,
    recordingMemberReleaseYear,
    recordingVariationStripRef,
    releaseSourceVersionAlbumImageUrl,
    releaseSourceVersionPlayCountLabel,
    renderSelectedPreviewArtistAlbumSection,
    scheduleAlbumWithArtistHighlight,
    scrollRecordingVariationStrip,
    selectedAlbumTrackMarkerTop,
    selectedPreview,
    selectedPreviewAlbumGuestArtists,
    selectedPreviewAlbumHasGuestArtists,
    selectedPreviewAlbumMainArtists,
    selectedPreviewAlbumSummary,
    selectedPreviewAppearsOnAlbums,
    selectedPreviewArtistAlbumsForDisplay,
    selectedPreviewArtistImageUrl,
    selectedPreviewArtists,
    selectedPreviewCanOpenAlbum,
    selectedPreviewCanOpenArtist,
    selectedPreviewCanonicalTrackTitle,
    selectedPreviewCurrentSpotifyTrackId,
    selectedPreviewDetailView,
    selectedPreviewDisplayRelationRows,
    selectedPreviewHasReleaseSibling,
    selectedPreviewIsBookmarked,
    selectedPreviewIsKnownLiked,
    selectedPreviewIsSharedArtistPage,
    selectedPreviewLastListenedLabel,
    selectedPreviewListenCountLabel,
    selectedPreviewOtherRecordingMembers,
    selectedPreviewPlaybackTrackUri,
    selectedPreviewPrimaryArtistAlbums,
    selectedPreviewRecordingCandidateError,
    selectedPreviewReleaseAlbumVariationCount,
    selectedPreviewReleaseSiblingSourceCount,
    selectedPreviewReleaseSourceVersionNeedsArrows,
    selectedPreviewReleaseSourceVersions,
    selectedPreviewReleaseTrackDetailError,
    selectedPreviewReleaseTrackDetailReady,
    selectedPreviewStarTrackId,
    selectedPreviewTrackDurationLabel,
    selectedPreviewTrackGuestArtists,
    selectedPreviewTrackMainArtists,
    selectedPreviewTrackOptimisticSummary,
    setAlbumTrackLastSortMode,
    setDetailOptionsOpen,
    setLocalBookmarkedTrackById,
    setLocalStarredTrackById,
    setSelectedPreview,
    setSelectedPreviewDetailView,
    spotifyTrackIdFromUri,
    toggleAlbumTrackPreview,
    variationSubtitleFromTitle,
  } = props;
  return selectedPreview ? (
        <div
          aria-modal="true"
          className="detail-modal-backdrop"
          onClick={() => setSelectedPreview(null)}
          role="dialog"
        >
          <section className="detail-modal" onClick={(event) => event.stopPropagation()}>
            <div className="detail-modal-options">
              <button
                aria-expanded={detailOptionsOpen}
                aria-label="Track options"
                className="detail-modal-options-button"
                onClick={() => setDetailOptionsOpen((current) => !current)}
                type="button"
              >
                <span aria-hidden="true">⚙</span>
              </button>
              {detailOptionsOpen ? (
                <div className="detail-modal-options-menu">
                  {selectedPreview.url ? (
                    <a
                      className="detail-modal-options-item"
                      href={selectedPreview.url}
                      rel="noreferrer"
                      target="_blank"
                    >
                      Open in Spotify
                    </a>
                  ) : null}
                  {selectedPreview.kind === "track" ? (
                    <button
                      className="detail-modal-options-item"
                      onClick={() => {
                        setSelectedPreviewDetailView((current) => current === "release" ? "recording" : "release");
                        setDetailOptionsOpen(false);
                      }}
                      type="button"
                    >
                      {selectedPreviewDetailView === "release" ? "View recording track" : "View release track"}
                    </button>
                  ) : null}
                </div>
              ) : null}
            </div>
            <div className="detail-modal-left">
              {selectedPreview.kind === "track" && selectedPreviewDetailView === "recording" ? (
                <button
                  aria-label="Open album view"
                  className="detail-modal-image-button"
                  onClick={openSelectedTrackAlbumPreview}
                  type="button"
                >
                  {selectedPreview.image ? (
                    <img alt={selectedPreview.label} className="detail-modal-image" src={selectedPreview.image} />
                  ) : (
                    <span className="detail-modal-image detail-modal-image-fallback" aria-hidden="true">
                      {selectedPreview.fallbackLabel ?? selectedPreview.label.slice(0, 1).toUpperCase()}
                    </span>
                  )}
                </button>
              ) : selectedPreview.image ? (
                <img alt={selectedPreview.label} className="detail-modal-image" src={selectedPreview.image} />
              ) : (
                <div className="detail-modal-image detail-modal-image-fallback" aria-hidden="true">
                  {selectedPreview.fallbackLabel ?? selectedPreview.label.slice(0, 1).toUpperCase()}
                </div>
              )}
              {selectedPreview.kind === "track" && selectedPreviewCanOpenAlbum ? (
                <button
                  className="detail-modal-inline-link detail-modal-cover-album-title"
                  onClick={openSelectedTrackAlbumPreview}
                  type="button"
                >
                  {previewAlbumHeading(selectedPreview)}
                </button>
              ) : selectedPreview.kind === "track" || selectedPreview.kind === "album" ? (
                <p className="detail-modal-cover-album-title">{previewAlbumHeading(selectedPreview)}</p>
              ) : null}
            </div>
            <div className="detail-modal-copy">
              <h2 className={selectedPreview.kind === "track" ? "detail-modal-track-title" : undefined}>
                {selectedPreview.kind !== "track" && selectedPreviewIsKnownLiked ? <LikedBadge className="detail-liked-badge" /> : null}
                {selectedPreview.kind !== "track" && selectedPreviewHasReleaseSibling ? (
                  <ReleaseSiblingBadge className="detail-release-sibling-badge" sourceCount={selectedPreviewReleaseSiblingSourceCount} />
                ) : null}
                {selectedPreviewIsSharedArtistPage ? (
                  <span className="detail-modal-artist-links">
                    {selectedPreviewArtists.map((artist, index) => {
                      const artistName = artist.name?.trim();
                      if (!artistName) {
                        return null;
                      }
                      return (
                        <span className="detail-modal-artist-link-wrap" key={`${artist.artist_id ?? artist.id ?? artistName}-${index}`}>
                          {index > 0 ? <span className="detail-modal-artist-separator">, </span> : null}
                          <button
                            className="detail-modal-inline-link"
                            onClick={() => openSelectedArtistMemberPreview(artist)}
                            type="button"
                          >
                            {artistName}
                          </button>
                        </span>
                      );
                    })}
                  </span>
                ) : selectedPreview.kind === "track" && selectedPreviewCanonicalTrackTitle
                  ? selectedPreviewCanonicalTrackTitle
                  : selectedPreview.kind === "album" && selectedPreview.detail ? `${selectedPreview.label} (${selectedPreview.detail})` : selectedPreview.label}
              </h2>
              {hasPremiumPlayback && selectedPreview.kind === "track" && selectedPreviewPlaybackTrackUri ? (
                <div className="detail-track-action-row" aria-label="Track playback actions">
                  {[
                    ["play_now", isTrackPlaying(selectedPreviewPlaybackTrackUri) ? "Resume" : "Play now"],
                    ["play_next", "Play next"],
                    ["add_to_queue", "Add to queue"],
                  ].map(([action, label]) => (
                    <button
                      className={`secondary-button detail-track-action-button detail-track-action-button-${action}${action === "play_now" && isTrackPlaying(selectedPreviewPlaybackTrackUri) ? " detail-track-action-button-playing" : ""}`}
                      key={action}
                      onClick={() => {
                        const albumQueue = buildAlbumPlaybackQueue(selectedPreviewPlaybackTrackUri);
                        void handlePlaybackAction(action as PlaybackAction, {
                          trackUri: selectedPreviewPlaybackTrackUri,
                          optimisticTrack: selectedPreviewTrackOptimisticSummary,
                          queueCursor: albumQueue?.queueCursor,
                          queueContext: albumQueue?.queueContext,
                          queuePlaylistUris: albumQueue?.playlistUris,
                          queueTracks: albumQueue?.queueTracks,
                          sourceTrack: selectedPreview?.sourceTrack ?? null,
                        });
                      }}
                      type="button"
                    >
                      {action === "play_now" ? (
                        <span className={`detail-top-play-glyph${isTrackPlaying(selectedPreviewPlaybackTrackUri) ? " detail-top-play-glyph-active" : ""}`} aria-hidden="true">
                          {isTrackPlaying(selectedPreviewPlaybackTrackUri) ? (
                            <span className="detail-pause-bars"><span /><span /></span>
                          ) : (
                            <span className="detail-play-icon">{"\u25B6"}</span>
                          )}
                        </span>
                      ) : null}
                      <span>{label}</span>
                    </button>
                  ))}
                  <button
                    aria-label={selectedPreviewIsBookmarked ? "Remove bookmark" : "Bookmark"}
                    aria-pressed={selectedPreviewIsBookmarked}
                    className={`secondary-button detail-track-action-button detail-track-bookmark-button${selectedPreviewIsBookmarked ? " detail-track-action-button-active" : ""}`}
                    onClick={() => {
                      if (!selectedPreviewStarTrackId) {
                        return;
                      }
                      setLocalBookmarkedTrackById((current) => ({
                        ...current,
                        [selectedPreviewStarTrackId]: !selectedPreviewIsBookmarked,
                      }));
                    }}
                    title={selectedPreviewIsBookmarked ? "Saved for later locally. Click to remove bookmark." : "Save for later locally."}
                    type="button"
                  >
                    <svg aria-hidden="true" viewBox="0 0 20 20">
                      <path d="M5 3.5h10v13l-5-3.2-5 3.2v-13Z" />
                    </svg>
                  </button>
                  <button
                    aria-label={selectedPreviewIsKnownLiked ? "Liked song" : "Not liked"}
                    aria-pressed={selectedPreviewIsKnownLiked}
                    className={`secondary-button detail-track-action-button detail-track-star-button${selectedPreviewIsKnownLiked ? " detail-track-action-button-active" : ""}`}
                    onClick={() => {
                      if (!selectedPreviewStarTrackId) {
                        return;
                      }
                      setLocalStarredTrackById((current) => ({
                        ...current,
                        [selectedPreviewStarTrackId]: !selectedPreviewIsKnownLiked,
                      }));
                    }}
                    title={selectedPreviewIsKnownLiked ? "Liked locally. Click to unstar." : "Not liked locally. Click to star."}
                    type="button"
                  >
                    <span aria-hidden="true">{selectedPreviewIsKnownLiked ? "★" : "☆"}</span>
                  </button>
                </div>
              ) : null}
              {selectedPreview.kind === "track"
                && (selectedPreviewListenCountLabel || selectedPreviewTrackDurationLabel || selectedPreviewLastListenedLabel) ? (
                <div className="detail-track-action-meta" aria-label="Track summary">
                  {selectedPreviewTrackDurationLabel ? <span>{selectedPreviewTrackDurationLabel}</span> : null}
                  {selectedPreviewLastListenedLabel ? <span>Last {selectedPreviewLastListenedLabel}</span> : null}
                  {selectedPreviewListenCountLabel ? <span className="detail-track-action-meta-listens">{selectedPreviewListenCountLabel}</span> : null}
                </div>
              ) : null}
              {selectedPreview.kind === "track" && selectedPreviewCanOpenArtist ? (
                <div className="detail-modal-track-artist-heading detail-modal-meta-with-image">
                  {(selectedPreviewTrackMainArtists[0]?.image_url ?? selectedPreviewArtistImageUrl) ? (
                    <img
                      alt=""
                      className="detail-modal-artist-image detail-modal-track-artist-image"
                      src={selectedPreviewTrackMainArtists[0]?.image_url ?? selectedPreviewArtistImageUrl ?? undefined}
                    />
                  ) : null}
                  <span className="detail-modal-artist-links">
                    {selectedPreviewTrackMainArtists.map((artist, index) => {
                      const artistName = artist.name?.trim();
                      if (!artistName) {
                        return null;
                      }
                      return (
                        <span className="detail-modal-artist-link-wrap" key={`${artist.artist_id ?? artist.id ?? artistName}-${index}`}>
                          {index > 0 ? <span className="detail-modal-artist-separator">, </span> : null}
                          <button
                            className="detail-modal-inline-link"
                            onClick={() => openSelectedTrackArtistPreview(artist)}
                            type="button"
                          >
                            {artistName}
                          </button>
                        </span>
                      );
                    })}
                  </span>
                </div>
              ) : null}
              {selectedPreview.kind === "album" ? (
                <div className="detail-modal-album-meta-block">
                  {selectedPreviewAlbumSummary ? (
                    <span className="detail-modal-meta-text detail-modal-album-summary">{selectedPreviewAlbumSummary}</span>
                  ) : null}
                  {selectedPreviewAlbumMainArtists.length > 0 ? (
                    <div className="detail-modal-meta detail-modal-meta-with-image">
                      {selectedPreviewArtistImageUrl ? (
                        <img
                          alt=""
                          className="detail-modal-artist-image"
                          src={selectedPreviewArtistImageUrl}
                        />
                      ) : null}
                      <span className="detail-modal-artist-links detail-modal-meta-text">
                        {selectedPreviewAlbumMainArtists.map((artist, index) => {
                          const artistName = artist.name?.trim();
                          if (!artistName) {
                            return null;
                          }
                          return (
                            <span className="detail-modal-artist-link-wrap" key={`${artist.artist_id ?? artist.id ?? artistName}-${index}`}>
                              {index > 0 ? <span className="detail-modal-artist-separator">, </span> : null}
                              <button
                                className="detail-modal-inline-link"
                                onClick={() => openSelectedAlbumArtistPreview(artist)}
                                type="button"
                              >
                                {artistName}
                              </button>
                            </span>
                          );
                        })}
                      </span>
                    </div>
                  ) : null}
                  {selectedPreviewAlbumGuestArtists.length > 0 ? (
                    <p className="detail-modal-with-artists">
                      <span>with </span>
                      <span className="detail-modal-artist-links">
                        {selectedPreviewAlbumGuestArtists.map((artist, index) => {
                          const artistName = artist.name?.trim();
                          if (!artistName) {
                            return null;
                          }
                          return (
                            <span className="detail-modal-artist-link-wrap" key={`${artist.artist_id ?? artist.id ?? artistName}-${index}`}>
                              {index > 0 ? <span className="detail-modal-artist-separator">, </span> : null}
                              <button
                                className="detail-modal-inline-link"
                                onClick={() => openSelectedAlbumArtistPreview(artist)}
                                onMouseEnter={() => scheduleAlbumWithArtistHighlight(artistName)}
                                onMouseLeave={clearAlbumWithArtistHighlight}
                                onFocus={() => scheduleAlbumWithArtistHighlight(artistName)}
                                onBlur={clearAlbumWithArtistHighlight}
                                type="button"
                              >
                                {artistName}
                              </button>
                            </span>
                          );
                        })}
                      </span>
                    </p>
                  ) : null}
                </div>
              ) : selectedPreview.kind === "track" && selectedPreviewCanOpenArtist && selectedPreviewTrackGuestArtists.length > 0 ? (
                <div className="detail-modal-album-meta-block">
                  <p className="detail-modal-with-artists">
                    <span>with </span>
                    <span className="detail-modal-artist-links">
                      {selectedPreviewTrackGuestArtists.map((artist, index) => {
                        const artistName = artist.name?.trim();
                        if (!artistName) {
                          return null;
                        }
                        return (
                          <span className="detail-modal-artist-link-wrap" key={`${artist.artist_id ?? artist.id ?? artistName}-${index}`}>
                            {index > 0 ? <span className="detail-modal-artist-separator">, </span> : null}
                            <button
                              className="detail-modal-inline-link"
                              onClick={() => openSelectedTrackArtistPreview(artist)}
                              type="button"
                            >
                              {artistName}
                            </button>
                          </span>
                        );
                      })}
                    </span>
                  </p>
                </div>
              ) : selectedPreview.meta && !(selectedPreview.kind === "track" && selectedPreviewCanOpenArtist) ? (
                <div className="detail-modal-meta detail-modal-meta-with-image">
                  {selectedPreviewCanOpenArtist ? (
                    <span className="detail-modal-artist-links detail-modal-meta-text">
                      {selectedPreviewArtists.map((artist, index) => {
                        const artistName = artist.name?.trim();
                        if (!artistName) {
                          return null;
                        }
                        return (
                          <span className="detail-modal-artist-link-wrap" key={`${artist.artist_id ?? artist.id ?? artistName}-${index}`}>
                            {index > 0 ? <span className="detail-modal-artist-separator">, </span> : null}
                            <button
                              className="detail-modal-inline-link"
                              onClick={() => {
                                if (selectedPreview.kind === "album") {
                                  openSelectedAlbumArtistPreview(artist);
                                  return;
                                }
                                openSelectedTrackArtistPreview(artist);
                              }}
                              type="button"
                            >
                              {artistName}
                            </button>
                          </span>
                        );
                      })}
                    </span>
                  ) : (
                    <span className="detail-modal-meta-text">{selectedPreview.meta}</span>
                  )}
                </div>
              ) : null}
              {selectedPreview.kind === "track" && !selectedPreviewReleaseTrackDetailReady && selectedPreviewReleaseTrackDetailError ? (
                <p className="detail-modal-release-note">{selectedPreviewReleaseTrackDetailError}</p>
              ) : null}
              {selectedPreview.kind === "track" && selectedPreviewRecordingCandidateError ? (
                <p className="detail-modal-release-note">{selectedPreviewRecordingCandidateError}</p>
              ) : null}
              {selectedPreview.detail && selectedPreview.kind !== "track" && selectedPreview.kind !== "album" ? <p className="detail-modal-detail">{selectedPreview.detail}</p> : null}
              {selectedPreview.kind === "track" && !selectedPreviewPlaybackTrackUri ? (
                <p className="detail-modal-preview-missing">This track does not have a playable Spotify URI.</p>
              ) : null}
              {selectedPreview.kind === "artist" ? (
                <>
                  {selectedPreviewIsSharedArtistPage || !backendSelectedPreviewArtistAlbums ? (
                    renderSelectedPreviewArtistAlbumSection(
                      "Albums",
                      selectedPreviewArtistAlbumsForDisplay,
                    )
                  ) : (
                    <>
                      {renderSelectedPreviewArtistAlbumSection(
                        "Albums",
                        selectedPreviewPrimaryArtistAlbums,
                      )}
                      {renderSelectedPreviewArtistAlbumSection(
                        "Appears on",
                        selectedPreviewAppearsOnAlbums,
                      )}
                    </>
                  )}
                </>
              ) : null}
            </div>
            {selectedPreview.kind === "track" && selectedPreviewDetailView === "recording" && selectedPreviewDisplayRelationRows.recording.length > 0 ? (
              <div className="detail-modal-recording-variations">
                <div className="detail-modal-recording-variations-header">
                  <span>Recording variations</span>
                </div>
                <div className="detail-modal-recording-variation-strip">
                  {selectedPreviewDisplayRelationRows.recording.map((member) => {
                    const albumImageUrl = recordingMemberAlbumImageUrl(member);
                    const title = [recordingMemberReleaseYear(member), member.album || "Unknown album"].filter(Boolean).join(" · ");
                    const subtitle = variationSubtitleFromTitle(member.title);
                    return (
                      <button
                        className="detail-modal-recording-variation-cover"
                        key={`recording-cover-${member.release_track_id}`}
                        onClick={() => openRecordingCandidateReleaseTrack(member, "recording")}
                        title={title}
                        type="button"
                      >
                        <span className="detail-modal-recording-variation-art">
                          <span className="detail-modal-recording-variation-kind">R</span>
                          {albumImageUrl ? (
                            <img alt="" src={albumImageUrl} />
                          ) : (
                            <span className="detail-modal-recording-variation-fallback" aria-hidden="true">{(member.album || member.title || "?").slice(0, 1).toUpperCase()}</span>
                          )}
                        </span>
                        <span className="detail-modal-recording-variation-copy">
                          {subtitle ? <span className="detail-modal-recording-variation-subtitle">{subtitle}</span> : null}
                          <span className="detail-modal-recording-variation-album">{member.album || "Unknown album"}</span>
                          <span className="detail-modal-recording-variation-year">{recordingMemberReleaseYear(member)}</span>
                        </span>
                      </button>
                    );
                  })}
                </div>
              </div>
            ) : null}
            {selectedPreview.kind === "track" && selectedPreviewDetailView === "recording" && selectedPreviewDisplayRelationRows.contextStyle.length > 0 ? (
              <div className="detail-modal-recording-variations">
                <div className="detail-modal-recording-variations-header">
                  <span>Variations</span>
                </div>
                <div className="detail-modal-recording-variation-strip">
                  {selectedPreviewDisplayRelationRows.contextStyle.map((member) => {
                    const albumImageUrl = recordingMemberAlbumImageUrl(member);
                    const title = [recordingMemberReleaseYear(member), member.album || "Unknown album"].filter(Boolean).join(" · ");
                    const subtitle = variationSubtitleFromTitle(member.title);
                    return (
                      <button
                        className="detail-modal-recording-variation-cover"
                        key={`family-cover-${member.release_track_id}`}
                        onClick={() => openRecordingCandidateReleaseTrack(member, "recording")}
                        title={title}
                        type="button"
                      >
                        <span className="detail-modal-recording-variation-art">
                          <span className="detail-modal-recording-variation-kind">V</span>
                          {albumImageUrl ? (
                            <img alt="" src={albumImageUrl} />
                          ) : (
                            <span className="detail-modal-recording-variation-fallback" aria-hidden="true">{(member.album || member.title || "?").slice(0, 1).toUpperCase()}</span>
                          )}
                        </span>
                        <span className="detail-modal-recording-variation-copy">
                          {subtitle ? <span className="detail-modal-recording-variation-subtitle">{subtitle}</span> : null}
                          <span className="detail-modal-recording-variation-album">{member.album || "Unknown album"}</span>
                          <span className="detail-modal-recording-variation-year">{recordingMemberReleaseYear(member)}</span>
                        </span>
                      </button>
                    );
                  })}
                </div>
              </div>
            ) : null}
            {selectedPreview.kind === "track" && selectedPreviewDetailView === "recording" && selectedPreviewDisplayRelationRows.coverRemix.length > 0 ? (
              <div className="detail-modal-recording-variations">
                <div className="detail-modal-recording-variations-header">
                  <span>Covers / remixes</span>
                </div>
                <div className="detail-modal-recording-variation-strip">
                  {selectedPreviewDisplayRelationRows.coverRemix.map((member) => {
                    const albumImageUrl = recordingMemberAlbumImageUrl(member);
                    const title = [recordingMemberReleaseYear(member), member.album || "Unknown album"].filter(Boolean).join(" · ");
                    const subtitle = variationSubtitleFromTitle(member.title);
                    return (
                      <button
                        className="detail-modal-recording-variation-cover"
                        key={`cover-remix-cover-${member.release_track_id}`}
                        onClick={() => openRecordingCandidateReleaseTrack(member, "recording")}
                        title={title}
                        type="button"
                      >
                        <span className="detail-modal-recording-variation-art">
                          <span className="detail-modal-recording-variation-kind">C</span>
                          {albumImageUrl ? (
                            <img alt="" src={albumImageUrl} />
                          ) : (
                            <span className="detail-modal-recording-variation-fallback" aria-hidden="true">{(member.album || member.title || "?").slice(0, 1).toUpperCase()}</span>
                          )}
                        </span>
                        <span className="detail-modal-recording-variation-copy">
                          {subtitle ? <span className="detail-modal-recording-variation-subtitle">{subtitle}</span> : null}
                          <span className="detail-modal-recording-variation-album">{member.album || "Unknown album"}</span>
                          <span className="detail-modal-recording-variation-year">{recordingMemberReleaseYear(member)}</span>
                        </span>
                      </button>
                    );
                  })}
                </div>
              </div>
            ) : null}
            {selectedPreview.kind === "track" || selectedPreview.kind === "album" ? (
              <div className={`detail-modal-album-tracks detail-modal-album-tracks-full${selectedPreview.kind === "track" ? " detail-modal-album-tracks-track detail-modal-album-tracks-no-with" : ""}${selectedPreview.kind !== "track" && selectedPreviewAlbumHasGuestArtists ? "" : " detail-modal-album-tracks-no-with"}`}>
                <div className="detail-modal-album-header">
                  {hasPremiumPlayback ? (
                    <PlaybackActionMenu
                      ariaLabel="Album playback options"
                      buttonClassName="detail-album-play-all-button"
                      placement={selectedPreview.kind === "track" ? "overlay-trigger" : "adjacent"}
                      onAction={(action) => handleAlbumPlayAll(action)}
                    >
                      Play all
                    </PlaybackActionMenu>
                  ) : (
                    <span aria-hidden="true" />
                  )}
                  <span className="detail-modal-album-title-header">{albumTracklistSummaryLabel(albumTrackEntries)}</span>
                  {selectedPreview.kind !== "track" && selectedPreviewAlbumHasGuestArtists ? <span className="detail-modal-album-with-header">With</span> : null}
                  <span className="detail-modal-album-liked-header">Tags</span>
                  {selectedPreview.kind !== "track" ? <span className="detail-modal-album-preview-header">Preview</span> : null}
                  <button
                    className={`detail-modal-album-last-played-header detail-modal-album-sort-header${albumTrackLastSortMode ? " detail-modal-album-sort-header-active" : ""}`}
                    onClick={() => setAlbumTrackLastSortMode((current) => nextLastPlayedSortMode(current))}
                    type="button"
                  >
                    Last
                    {albumTrackLastSortMode ? (
                      <span aria-hidden="true">{albumTrackLastSortMode === "recent" ? "↓" : "↑"}</span>
                    ) : null}
                  </button>
                </div>
                {albumTrackEntriesLoading && albumTrackEntries.length === 0 ? (
                  <p className="detail-modal-preview-missing">Loading album songs...</p>
                ) : null}
                {!albumTrackEntriesLoading && albumTrackEntriesError ? (
                  <p className="detail-modal-preview-missing">{albumTrackEntriesError}</p>
                ) : null}
                {!albumTrackEntriesError && albumTrackEntries.length > 0 ? (
                  <div className="detail-album-track-list-wrap">
                    {selectedAlbumTrackMarkerTop(displayAlbumTrackEntries) ? (
                      <span
                        className="detail-album-track-scroll-marker"
                        style={{ "--detail-album-track-marker-top": selectedAlbumTrackMarkerTop(displayAlbumTrackEntries) } as CSSProperties}
                        aria-hidden="true"
                      />
                    ) : null}
                    <ul className={`detail-album-track-list${albumTrackEntriesLoading ? " detail-album-track-list-updating" : ""}`} ref={albumTrackListRef}>
                      {displayAlbumTrackEntries.map((track) => {
                      const rowTrackUri = track.uri ?? (track.id ? `spotify:track:${track.id}` : null);
                      const rowIsCurrentTrack = Boolean(rowTrackUri && currentTrack?.uri === rowTrackUri);
                      const rowPlaying = isTrackPlaying(rowTrackUri);
                      const rowPreviewPlaying = Boolean(rowTrackUri && previewingTrackUri === rowTrackUri);
                      const rowPreviewActive = Boolean(rowPreviewPlaying && rowPlaying);
                      const rowPreviewKey = albumTrackPreviewKey(track, rowTrackUri);
                      const rowPreviewPlayed = previewPlayedTrackKeys.has(rowPreviewKey);
                      const rowPausedCurrent = Boolean(rowIsCurrentTrack && playbackPaused);
                      const rowLastPlayed = formatCompactRelativeAge(track.lastPlayedAt);
                      const rowIsUnlistened = !track.lastPlayedAt && track.playCount <= 0;
                      const rowHasDuplicateSources = track.releaseTrackDuplicateSourceCount > 1;
                      const rowIsRecordingGroup = track.releaseTrackClusterCandidateType === "recording_track_candidate";
                      const rowIsCoverRemixFamily = track.releaseTrackClusterCandidateType === "track_family_candidate"
                        && familyCoverRemixRelationshipKinds.has(track.releaseTrackClusterRelationshipKind ?? "");
                      const rowIsVariationFamily = track.releaseTrackClusterCandidateType === "track_family_candidate"
                        && !rowIsCoverRemixFamily;
                      const rowRelationTagEntries = [
                        rowHasDuplicateSources ? { code: "D", label: "duplicate source grouping" } : null,
                        rowIsRecordingGroup ? { code: "R", label: "recording group" } : null,
                        rowIsVariationFamily ? { code: "V", label: "variation" } : null,
                        rowIsCoverRemixFamily ? { code: "C", label: "cover/remix/rework" } : null,
                      ].filter((entry): entry is { code: string; label: string } => Boolean(entry));
                      const rowRelationTags = rowRelationTagEntries.map((entry) => entry.code).join("");
                      const rowRelationTagsTitle = rowRelationTagEntries.length > 0
                        ? `Track relation: ${rowRelationTagEntries.map((entry) => entry.label).join(", ")}`
                        : "";
                      const rowStarTrackId = track.id ?? spotifyTrackIdFromUri(rowTrackUri);
                      const rowMatchesSelectedReleaseTrack = Boolean(
                        selectedPreview.kind === "track"
                        && (
                          (track.releaseTrackId != null && track.releaseTrackId === selectedPreview.releaseTrackId)
                          || (rowStarTrackId && selectedPreviewReleaseTrackDetailReady?.source_versions.some((version) => version.spotify_track_id === rowStarTrackId))
                        ),
                      );
                      const rowIsLiked = rowStarTrackId && rowStarTrackId in localStarredTrackById
                        ? localStarredTrackById[rowStarTrackId]
                        : (rowMatchesSelectedReleaseTrack && selectedPreviewIsKnownLiked) || albumTrackIsKnownLiked(track);
                      const mainArtistNames = new Set(
                        selectedPreview.kind === "album" || selectedPreview.kind === "track"
                          ? selectedPreviewAlbumMainArtists.map((artist) => artist.name?.trim().toLocaleLowerCase()).filter(Boolean)
                          : [],
                      );
                      const rowWithArtists = selectedPreview.kind === "album" || selectedPreview.kind === "track"
                        ? artistEntriesForAlbumTrack(track).filter((artist) => {
                          const artistName = artist.name?.trim().toLocaleLowerCase();
                          return Boolean(artistName && !mainArtistNames.has(artistName));
                        })
                        : [];
                      const rowMatchesHighlightedArtist = Boolean(
                        selectedPreview.kind === "album"
                        && selectedPreview.albumHighlightArtistNames?.some((artistName) => artistNameMatches(track.artistName, artistName)),
                      );
                      const rowMatchesHoveredWithArtist = Boolean(
                        hoveredAlbumWithArtistName && artistNameMatches(track.artistName, hoveredAlbumWithArtistName),
                      );
                      const rowBaseDurationMs = (
                        track.durationMs
                        ?? (rowIsCurrentTrack
                          ? (playbackDurationMs > 0 ? playbackDurationMs : currentTrack?.durationMs ?? null)
                          : null)
                      );
                      const rowElapsedMs = rowIsCurrentTrack
                        ? (
                          rowBaseDurationMs != null
                            ? Math.min(Math.max(0, playbackPositionMs), rowBaseDurationMs)
                            : Math.max(0, playbackPositionMs)
                        )
                        : null;
                      const rowButtonTimeMs = rowIsCurrentTrack
                        ? (
                          rowPlaying
                            ? rowElapsedMs
                            : (rowPausedCurrent ? (pausedTimeFlashOn ? rowElapsedMs : rowBaseDurationMs) : rowBaseDurationMs)
                        )
                        : rowBaseDurationMs;
                      return (
	                        <li className={`detail-album-track-row${track.isSelected ? " detail-album-track-row-selected" : ""}${rowMatchesHighlightedArtist || rowMatchesHoveredWithArtist ? " detail-album-track-row-artist-highlighted" : ""}`} key={track.id ?? track.name}>
	                          {hasPremiumPlayback ? (
	                            <PlaybackActionMenu
                              ariaLabel={rowPlaying ? "Currently playing in ListenLab" : rowTrackUri ? `Play ${track.name} in ListenLab` : `${track.name} is not playable`}
                              buttonClassName={`secondary-button detail-album-track-play-button${rowPlaying ? " detail-icon-button-playing" : ""}`}
                              disabled={!rowTrackUri}
                              isPlaying={rowPlaying}
                              placement="overlay-trigger"
                              onAction={(action) => {
                                const albumQueue = buildAlbumPlaybackQueue(rowTrackUri);
                                return handlePlaybackAction(action, {
                                  trackUri: rowTrackUri,
                                  optimisticTrack: playerSummaryFromAlbumTrack(track),
                                  queueCursor: albumQueue?.queueCursor,
                                  queueContext: albumQueue?.queueContext,
                                  queuePlaylistUris: albumQueue?.playlistUris,
                                  queueTracks: albumQueue?.queueTracks,
                                  sourceTrack: track.sourceTrack,
                                }).then(() => {
                                  if (action === "play_now") {
                                    openAlbumTrackPreview(track);
                                  }
                                });
                              }}
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
                              <span className={`detail-album-track-play-time${rowPausedCurrent ? " detail-album-track-play-time-flash" : ""}`}>
                                {rowButtonTimeMs != null ? formatPlaybackClock(rowButtonTimeMs) : "?:??"}
	                              </span>
	                            </PlaybackActionMenu>
	                          ) : <span aria-hidden="true" />}
                          <button
                            className="detail-album-track-name-button single-line-ellipsis"
                            onClick={() => openAlbumTrackPreview(track)}
                            type="button"
                          >
                            <span className="single-line-ellipsis">{track.name}</span>
                          </button>
                          {selectedPreview.kind !== "track" && selectedPreviewAlbumHasGuestArtists ? (
                            <span className="detail-album-track-with single-line-ellipsis">
                              {rowWithArtists.map((artist, index) => {
                                const artistName = artist.name?.trim();
                                if (!artistName) {
                                  return null;
                                }
                                return (
                                  <span className="detail-modal-artist-link-wrap" key={`${artist.artist_id ?? artist.id ?? artistName}-${index}`}>
                                    {index > 0 ? <span className="detail-modal-artist-separator">, </span> : null}
                                    <button
                                      className="detail-modal-inline-link"
                                      onClick={() => openAlbumWithArtistPreview(artist)}
                                      type="button"
                                    >
                                      {artistName}
                                    </button>
                                  </span>
                                );
                              })}
                            </span>
                          ) : null}
                          <span className="detail-album-track-liked-cell">
                            <span className="detail-album-track-badges">
                              {rowIsLiked ? <LikedBadge className="detail-album-track-liked-badge" /> : null}
                              {rowRelationTags ? (
                                <span
                                  className="relation-tags-badge detail-album-track-relation-badge"
                                  title={rowRelationTagsTitle}
                                  aria-label={rowRelationTagsTitle}
                                >
                                  {rowRelationTags}
                                </span>
                              ) : null}
                            </span>
                          </span>
                          <div className="detail-album-track-actions">
                            {selectedPreview.kind !== "track" && hasPremiumPlayback ? (
                              <button
                                aria-label={rowPreviewPlaying ? `Stop preview for ${track.name}` : `Preview ${track.name}`}
                                className={`detail-album-track-preview-button${rowPreviewActive ? " detail-album-track-preview-button-active" : ""}${rowPreviewPlayed ? " detail-album-track-preview-button-played" : ""}`}
                                disabled={!rowTrackUri}
                                onClick={() => {
                                  void toggleAlbumTrackPreview(track, rowTrackUri);
                                }}
                                type="button"
                              />
                            ) : selectedPreview.kind !== "track" ? (
                              <span className="detail-album-track-preview-placeholder" aria-hidden="true" />
                            ) : null}
                            {rowLastPlayed ? (
                              <span className="detail-album-track-last-played">{rowLastPlayed}</span>
                            ) : rowIsUnlistened ? (
                              <span className="detail-album-track-last-played">
                                <NewTrackBadge className="detail-album-track-played-new-badge" />
                              </span>
                            ) : (
                              <span className="detail-album-track-last-played">-</span>
                            )}
                          </div>
                        </li>
                      );
                      })}
                    </ul>
                  </div>
                ) : null}
              </div>
            ) : null}
            {selectedPreview.kind === "track" && selectedPreviewDetailView === "release" && selectedPreviewReleaseAlbumVariationCount > 1 ? (
              <div className="detail-modal-recording-variations detail-modal-release-source-albums">
                <div className="detail-modal-recording-variations-header">
                  <span>Release albums</span>
                  {selectedPreviewReleaseSourceVersionNeedsArrows ? (
                    <span className="detail-modal-recording-variation-controls">
                      <button aria-label="Previous source album covers" onClick={() => scrollRecordingVariationStrip(-1)} type="button">{"<"}</button>
                      <button aria-label="Next source album covers" onClick={() => scrollRecordingVariationStrip(1)} type="button">{">"}</button>
                    </span>
                  ) : null}
                </div>
                <div className="detail-modal-recording-variation-strip" ref={recordingVariationStripRef}>
                  {selectedPreviewReleaseSourceVersions.map((version) => {
                    const isSelectedSourceVersion = version.spotify_track_id === selectedPreviewCurrentSpotifyTrackId;
                    const albumImageUrl = releaseSourceVersionAlbumImageUrl(version);
                    const subtitle = variationSubtitleFromTitle(version.name);
                    const title = [
                      version.album_release_year,
                      version.album_name || "Unknown album",
                      releaseSourceVersionPlayCountLabel(version),
                    ].filter(Boolean).join(" · ");
                    return (
                      <button
                        className={`detail-modal-recording-variation-cover${isSelectedSourceVersion ? " detail-modal-recording-variation-cover-selected" : ""}`}
                        key={`release-source-cover-${version.source_track_id}`}
                        onClick={() => openReleaseSourceVersion(version, "release")}
                        title={title}
                        type="button"
                      >
                        <span className="detail-modal-recording-variation-art">
                          <span className="detail-modal-recording-variation-kind">Source</span>
                          {albumImageUrl ? (
                            <img alt="" src={albumImageUrl} />
                          ) : (
                            <span className="detail-modal-recording-variation-fallback" aria-hidden="true">{(version.album_name || version.name || "?").slice(0, 1).toUpperCase()}</span>
                          )}
                          {version.is_playback_choice ? (
                            <span className="detail-modal-recording-variation-badge">Rep</span>
                          ) : null}
                        </span>
                        <span className="detail-modal-recording-variation-copy">
                          {subtitle ? <span className="detail-modal-recording-variation-subtitle">{subtitle}</span> : null}
                          <span className="detail-modal-recording-variation-album">{version.album_name || "Unknown album"}</span>
                          <span className="detail-modal-recording-variation-year">{version.album_release_year || releaseSourceVersionPlayCountLabel(version)}</span>
                        </span>
                      </button>
                    );
                  })}
                  {selectedPreviewOtherRecordingMembers.map((member) => {
                    const albumImageUrl = recordingMemberAlbumImageUrl(member);
                    const title = [recordingMemberReleaseYear(member), member.album || "Unknown album"].filter(Boolean).join(" · ");
                    const subtitle = variationSubtitleFromTitle(member.title);
                    return (
                      <button
                        className="detail-modal-recording-variation-cover"
                        key={`release-member-cover-${member.release_track_id}`}
                        onClick={() => openRecordingCandidateReleaseTrack(member, "release")}
                        title={title}
                        type="button"
                      >
                        <span className="detail-modal-recording-variation-art">
                          <span className="detail-modal-recording-variation-kind">Recording</span>
                          {albumImageUrl ? (
                            <img alt="" src={albumImageUrl} />
                          ) : (
                            <span className="detail-modal-recording-variation-fallback" aria-hidden="true">{(member.album || member.title || "?").slice(0, 1).toUpperCase()}</span>
                          )}
                        </span>
                        <span className="detail-modal-recording-variation-copy">
                          {subtitle ? <span className="detail-modal-recording-variation-subtitle">{subtitle}</span> : null}
                          <span className="detail-modal-recording-variation-album">{member.album || "Unknown album"}</span>
                          <span className="detail-modal-recording-variation-year">{recordingMemberReleaseYear(member)}</span>
                        </span>
                      </button>
                    );
                  })}
                </div>
              </div>
            ) : null}
          </section>
        </div>
      ) : null;
}
