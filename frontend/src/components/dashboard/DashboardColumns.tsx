import { useEffect, useState, type ReactNode } from "react";
import type { FollowedArtist, OwnedPlaylist, PreviewItem, SectionKey, TopAlbum } from "../../types/appTypes";
import { PAGE_SIZE } from "../../constants/appConstants";
import {
  emptySlots,
  formatAlbumBreadth,
  formatAlbumSummary,
  formatHistoryDebugLine,
  spotifyPlaylistIdFromUrl,
} from "../../utils/dashboardUtils";
import { DashboardListCard } from "./DashboardListCard";
import { DashboardPaging } from "./DashboardPaging";

type DashboardColumnBaseProps = {
  section: SectionKey;
  available: boolean;
  emptyCopy: string;
  unavailableCopy: string;
  sectionPage: number;
  moveSectionPage: (section: SectionKey, direction: -1 | 1, itemCount: number, pageSize?: number) => void;
  onSelectPreview: (preview: PreviewItem) => void;
};

type ArtistColumnProps = DashboardColumnBaseProps & {
  items: FollowedArtist[];
  unavailableAction?: ReactNode;
};

type AlbumColumnProps = DashboardColumnBaseProps & {
  items: TopAlbum[];
  unavailableAction?: ReactNode;
};

type PlaylistColumnProps = DashboardColumnBaseProps & {
  items: OwnedPlaylist[];
  paged?: boolean;
  activePlaylistPlayback?: {
    playlistId: string | null;
    playlistName?: string | null;
    trackId?: string | null;
    trackUri?: string | null;
    position?: number | null;
    isPlaying: boolean;
  } | null;
  onHidePlaylist?: (playlist: OwnedPlaylist) => void;
  onUnhidePlaylist?: (playlist: OwnedPlaylist) => void;
  onDeletePlaylist?: (playlist: OwnedPlaylist) => void;
  playlistEditMode?: boolean;
  playlistEditCloseAction?: "save" | "cancel" | null;
  playlistLists?: Array<{ id: string; name: string; playlistIds: string[] }>;
  onTogglePlaylistList?: (playlist: OwnedPlaylist, listId: string) => void;
  pinnedPlaylistIds?: string[];
  onTogglePinnedPlaylist?: (playlist: OwnedPlaylist) => void;
};

type PlaylistActionMode = "add" | "remove";
type PlaylistActionDraft = {
  listIds: string[];
  pinned: boolean;
};

function playlistTrackCountLabel(trackCount: number | null | undefined) {
  if (typeof trackCount !== "number" || !Number.isFinite(trackCount)) {
    return "Tracks unknown";
  }
  return `${trackCount.toLocaleString()} ${trackCount === 1 ? "track" : "tracks"}`;
}

function playlistSaveCountLabel(saveCount: number | null | undefined) {
  if (typeof saveCount !== "number" || !Number.isFinite(saveCount) || saveCount <= 0) {
    return null;
  }
  return `${saveCount.toLocaleString()} ${saveCount === 1 ? "save" : "saves"}`;
}

export function DashboardArtistColumn({
  section,
  items,
  available,
  emptyCopy,
  unavailableCopy,
  unavailableAction,
  sectionPage,
  moveSectionPage,
  onSelectPreview,
}: ArtistColumnProps) {
  if (!available) {
    return (
      <div className="section-unavailable">
        <p className="empty-copy">{unavailableCopy}</p>
        {unavailableAction ? <div className="section-unavailable-action">{unavailableAction}</div> : null}
      </div>
    );
  }
  if (items.length === 0) {
    return <p className="empty-copy">{emptyCopy}</p>;
  }

  const pageItems = items.slice(sectionPage * PAGE_SIZE, sectionPage * PAGE_SIZE + PAGE_SIZE);
  return (
    <>
      <div className="item-list">
        {pageItems.map((artist, index) => (
          <DashboardListCard
            key={artist.artist_id ?? `${artist.name}-${index}`}
            href={artist.url}
            entityId={artist.artist_id}
            imageUrl={artist.image_url}
            imageAlt={`${artist.name ?? "Artist"} portrait`}
            fallbackLabel="A"
            primaryText={artist.name ?? "Unknown artist"}
            secondaryText={
              artist.genres.length > 0
                ? artist.genres.join(", ")
                : artist.popularity != null
                  ? `Popularity ${artist.popularity}/100`
                  : "Spotify artist"
            }
            tertiaryText={formatHistoryDebugLine(artist)}
            previewKind="artist"
            previewTrackUri={null}
            onSelectPreview={onSelectPreview}
          />
        ))}
        {Array.from({ length: emptySlots(pageItems) }).map((_, index) => (
          <div className="list-row list-row-placeholder" key={`${section}-empty-${index}`} aria-hidden="true" />
        ))}
      </div>
      <DashboardPaging
        section={section}
        itemCount={items.length}
        pageSize={PAGE_SIZE}
        sectionPage={sectionPage}
        moveSectionPage={moveSectionPage}
      />
    </>
  );
}

export function DashboardAlbumColumn({
  section,
  items,
  available,
  emptyCopy,
  unavailableCopy,
  unavailableAction,
  sectionPage,
  moveSectionPage,
  onSelectPreview,
}: AlbumColumnProps) {
  if (!available) {
    return (
      <div className="section-unavailable">
        <p className="empty-copy">{unavailableCopy}</p>
        {unavailableAction ? <div className="section-unavailable-action">{unavailableAction}</div> : null}
      </div>
    );
  }
  if (items.length === 0) {
    return <p className="empty-copy">{emptyCopy}</p>;
  }

  const pageItems = items.slice(sectionPage * PAGE_SIZE, sectionPage * PAGE_SIZE + PAGE_SIZE);
  return (
    <>
      <div className="item-list">
        {pageItems.map((album, index) => (
          <DashboardListCard
            key={album.album_id ?? `${album.name}-${index}-${section}`}
            href={album.url}
            entityId={album.album_id}
            imageUrl={album.image_url}
            imageAlt={`${album.name ?? "Album"} cover`}
            fallbackLabel="A"
            primaryText={album.name ?? "Unknown album"}
            secondaryText={album.artist_name ?? "Unknown artist"}
            tertiaryText={
              formatHistoryDebugLine(album) ??
              formatAlbumSummary(album)
            }
            metricText={formatAlbumBreadth(album)}
            previewKind="artist"
            previewTrackUri={null}
            onSelectPreview={onSelectPreview}
          />
        ))}
        {Array.from({ length: emptySlots(pageItems) }).map((_, index) => (
          <div className="list-row list-row-placeholder" key={`${section}-empty-${index}`} aria-hidden="true" />
        ))}
      </div>
      <DashboardPaging
        section={section}
        itemCount={items.length}
        pageSize={PAGE_SIZE}
        sectionPage={sectionPage}
        moveSectionPage={moveSectionPage}
      />
    </>
  );
}

export function DashboardPlaylistColumn({
  section,
  items,
  available,
  emptyCopy,
  unavailableCopy,
  sectionPage,
  moveSectionPage,
  onSelectPreview,
  activePlaylistPlayback = null,
  paged = true,
  onHidePlaylist,
  onUnhidePlaylist,
  onDeletePlaylist,
  playlistEditMode = false,
  playlistEditCloseAction = null,
  playlistLists = [],
  onTogglePlaylistList,
  pinnedPlaylistIds = [],
  onTogglePinnedPlaylist,
}: PlaylistColumnProps) {
  const [openPlaylistAction, setOpenPlaylistAction] = useState<{ key: string; mode: PlaylistActionMode } | null>(null);
  const [playlistActionDrafts, setPlaylistActionDrafts] = useState<Record<string, PlaylistActionDraft>>({});

  const playlistDraftKey = (playlist: OwnedPlaylist, playlistId: string | null | undefined, index: number) => (
    playlistId ?? playlist.url ?? `${playlist.name ?? "playlist"}-${index}`
  );
  const playlistActualListIds = (playlistId: string | null | undefined) => (
    playlistLists.filter((list) => list.playlistIds.includes(playlistId ?? "")).map((list) => list.id)
  );
  const draftFromCurrent = (playlistId: string | null | undefined): PlaylistActionDraft => ({
    listIds: playlistActualListIds(playlistId),
    pinned: pinnedPlaylistIds.includes(playlistId ?? ""),
  });
  const applyPlaylistDraft = (playlist: OwnedPlaylist, playlistId: string | null | undefined, draft: PlaylistActionDraft) => {
    const actualListIds = playlistActualListIds(playlistId);
    const actualPinned = pinnedPlaylistIds.includes(playlistId ?? "");
    playlistLists.forEach((list) => {
      const shouldInclude = draft.listIds.includes(list.id);
      const doesInclude = actualListIds.includes(list.id);
      if (shouldInclude !== doesInclude) {
        onTogglePlaylistList?.(playlist, list.id);
      }
    });
    if (draft.pinned !== actualPinned) {
      onTogglePinnedPlaylist?.(playlist);
    }
  };
  const openAction = (playlist: OwnedPlaylist, playlistId: string | null | undefined, index: number, mode: PlaylistActionMode) => {
    const key = playlistDraftKey(playlist, playlistId, index);
    setOpenPlaylistAction({ key, mode });
    if (mode === "add") {
      setPlaylistActionDrafts((current) => current[key] ? current : { ...current, [key]: draftFromCurrent(playlistId) });
    }
  };
  const discardAction = (playlist: OwnedPlaylist, playlistId: string | null | undefined, index: number) => {
    const key = playlistDraftKey(playlist, playlistId, index);
    setOpenPlaylistAction((current) => current?.key === key ? null : current);
    setPlaylistActionDrafts((current) => {
      const next = { ...current };
      delete next[key];
      return next;
    });
  };
  const confirmAction = (playlist: OwnedPlaylist, playlistId: string | null | undefined, index: number) => {
    const key = playlistDraftKey(playlist, playlistId, index);
    const draft = playlistActionDrafts[key];
    if (draft) {
      applyPlaylistDraft(playlist, playlistId, draft);
    }
    discardAction(playlist, playlistId, index);
  };

  useEffect(() => {
    if (playlistEditMode) {
      return;
    }
    if (playlistEditCloseAction === "cancel") {
      setPlaylistActionDrafts({});
      setOpenPlaylistAction(null);
      return;
    }
    if (playlistEditCloseAction !== "save") {
      return;
    }
    const playlistByKey = new Map<string, { playlist: OwnedPlaylist; playlistId: string | null | undefined }>();
    items.forEach((playlist, index) => {
      const playlistId = playlist.playlist_id ?? spotifyPlaylistIdFromUrl(playlist.url);
      playlistByKey.set(playlistDraftKey(playlist, playlistId, index), { playlist, playlistId });
    });
    Object.entries(playlistActionDrafts).forEach(([key, draft]) => {
      const match = playlistByKey.get(key);
      if (match) {
        applyPlaylistDraft(match.playlist, match.playlistId, draft);
      }
    });
    setPlaylistActionDrafts({});
    setOpenPlaylistAction(null);
  }, [playlistEditMode, playlistEditCloseAction]);

  useEffect(() => {
    if (!playlistEditMode) {
      return;
    }
    function closeOpenPlaylistActionMenus(event: MouseEvent) {
      const target = event.target;
      if (target instanceof Element && target.closest(".card-playlist-edit-actions")) {
        return;
      }
      setOpenPlaylistAction(null);
    }
    document.addEventListener("mousedown", closeOpenPlaylistActionMenus);
    return () => {
      document.removeEventListener("mousedown", closeOpenPlaylistActionMenus);
    };
  }, [playlistEditMode]);

  if (!available) {
    return <p className="empty-copy">{unavailableCopy}</p>;
  }
  if (items.length === 0) {
    return <p className="empty-copy">{emptyCopy}</p>;
  }

  const pageItems = paged ? items.slice(sectionPage * PAGE_SIZE, sectionPage * PAGE_SIZE + PAGE_SIZE) : items;
  return (
    <>
      <div className="item-list">
        {pageItems.map((playlist, index) => {
          const playlistId = playlist.playlist_id ?? spotifyPlaylistIdFromUrl(playlist.url);
          const isActivePlaylist = Boolean(playlistId && activePlaylistPlayback?.playlistId === playlistId);
          const draftKey = playlistDraftKey(playlist, playlistId, index);
          const activeAction = openPlaylistAction?.key === draftKey ? openPlaylistAction.mode : null;
          const playlistDraft = playlistActionDrafts[draftKey] ?? draftFromCurrent(playlistId);
          const playlistCategoryLabels = playlistLists
            .filter((list) => list.playlistIds.includes(playlistId ?? ""))
            .map((list) => list.name);
          const playlistSaveLabel = playlistSaveCountLabel(playlist.followers_total);
          const maxPlaylistOverlayRows = 4;
          const privateRows = playlist.is_public === false ? ["Private"] : [];
          const categoryRowLimit = Math.max(0, maxPlaylistOverlayRows - privateRows.length);
          const categoryRows = playlistCategoryLabels.slice(0, categoryRowLimit).map((label, labelIndex) => (
            labelIndex === categoryRowLimit - 1 && playlistCategoryLabels.length > categoryRowLimit
              ? `${label} +${playlistCategoryLabels.length - categoryRowLimit}`
              : label
          ));
          const fillerRows = [
            playlistTrackCountLabel(playlist.track_count),
            playlistSaveLabel,
          ].filter((label): label is string => Boolean(label)).slice(0, Math.max(0, maxPlaylistOverlayRows - privateRows.length - categoryRows.length));
          const playlistOverlayRows = [...privateRows, ...fillerRows, ...categoryRows];
          return (
            <DashboardListCard
              key={playlistId ?? `${playlist.name}-${index}-${section}`}
              href={playlist.url}
              entityId={playlistId}
              imageUrl={playlist.image_url}
              imageAlt={`${playlist.name ?? "Playlist"} cover`}
              fallbackLabel="P"
              primaryText={playlist.name ?? "Untitled playlist"}
              primaryClamp="two-line-clamp"
              secondaryText={null}
              tertiaryText={null}
              imageOverlay={(
                <span className="card-playlist-image-hover">
                  {isActivePlaylist ? (
                    <span className={`card-playlist-now-playing${activePlaylistPlayback?.isPlaying ? " card-playlist-now-playing-active" : ""}`} aria-label={activePlaylistPlayback?.isPlaying ? "Playlist playing" : "Playlist paused"}>
                      <span />
                      <span />
                      <span />
                    </span>
                  ) : null}
                  {playlistOverlayRows.map((row, rowIndex) => (
                    <span
                      className={rowIndex >= fillerRows.length ? "card-playlist-hover-category" : undefined}
                      key={`${row}-${rowIndex}`}
                    >
                      {row}
                    </span>
                  ))}
                </span>
              )}
              playlistOwnerFollowedByYou={playlist.owner_followed_by_you ?? null}
              muted={Boolean(playlist.hidden_by_user)}
              cardClassName={[
                "dashboard-card-row-playlist",
                isActivePlaylist ? "dashboard-card-row-playlist-now-playing" : null,
                playlistEditMode ? "dashboard-card-row-playlist-editing" : null,
                activeAction ? "dashboard-card-row-playlist-action-open" : null,
              ].filter(Boolean).join(" ")}
              previewKind="playlist"
              previewTrackUri={null}
              previewOverrides={isActivePlaylist ? {
                focusPlaylistPosition: activePlaylistPlayback?.position ?? null,
                focusSpotifyTrackId: activePlaylistPlayback?.trackId ?? null,
                trackUri: activePlaylistPlayback?.trackUri ?? null,
              } : undefined}
              onSelectPreview={onSelectPreview}
              rowAction={playlistEditMode ? (
                <span
                  className="card-playlist-edit-actions"
                  onClick={(event) => {
                    event.preventDefault();
                    event.stopPropagation();
                  }}
                  onMouseDown={(event) => {
                    event.stopPropagation();
                  }}
                >
                  {playlist.hidden_by_user ? (
                    <button
                      className="card-playlist-unhide-button"
                      onClick={(event) => {
                        event.preventDefault();
                        event.stopPropagation();
                        onUnhidePlaylist?.(playlist);
                      }}
                      type="button"
                    >
                      <span aria-hidden="true">+</span>
                      <span>Unhide</span>
                    </button>
                  ) : (
                    <>
                      <span className="card-playlist-action-buttons">
                        {activeAction ? (
                          <>
                            {activeAction === "remove" ? (
                              <button
                                aria-label="Discard playlist edit options"
                                className="card-playlist-symbol-button"
                                onClick={(event) => {
                                  event.preventDefault();
                                  event.stopPropagation();
                                  discardAction(playlist, playlistId, index);
                                }}
                                type="button"
                              >
                                ×
                              </button>
                            ) : (
                              <button
                                aria-label="Save playlist edit options"
                                className="card-playlist-symbol-button card-playlist-symbol-button-confirm"
                                onClick={(event) => {
                                  event.preventDefault();
                                  event.stopPropagation();
                                  confirmAction(playlist, playlistId, index);
                                }}
                                type="button"
                              >
                                ✓
                              </button>
                            )}
                            {activeAction === "add" ? (
                              <button
                                aria-label="Discard playlist edit options"
                                className="card-playlist-symbol-button"
                                onClick={(event) => {
                                  event.preventDefault();
                                  event.stopPropagation();
                                  discardAction(playlist, playlistId, index);
                                }}
                                type="button"
                              >
                                ×
                              </button>
                            ) : (
                              <button
                                aria-label="Save playlist edit options"
                                className="card-playlist-symbol-button card-playlist-symbol-button-confirm"
                                onClick={(event) => {
                                  event.preventDefault();
                                  event.stopPropagation();
                                  confirmAction(playlist, playlistId, index);
                                }}
                                type="button"
                              >
                                ✓
                              </button>
                            )}
                          </>
                        ) : (
                          <>
                            <button
                              aria-label="Remove playlist options"
                              className="card-playlist-symbol-button"
                              onClick={(event) => {
                                event.preventDefault();
                                event.stopPropagation();
                                openAction(playlist, playlistId, index, "remove");
                              }}
                              type="button"
                            >
                              -
                            </button>
                            <button
                              aria-label="Add playlist options"
                              className="card-playlist-symbol-button"
                              onClick={(event) => {
                                event.preventDefault();
                                event.stopPropagation();
                                openAction(playlist, playlistId, index, "add");
                              }}
                              type="button"
                            >
                              +
                            </button>
                          </>
                        )}
                      </span>
                      {activeAction === "remove" ? (
                        <span className="card-playlist-action-popover card-playlist-action-popover-remove">
                          <span
                            className="card-playlist-action-item"
                            onClick={(event) => {
                              event.preventDefault();
                              event.stopPropagation();
                              onHidePlaylist?.(playlist);
                            }}
                            role="button"
                            tabIndex={0}
                          >
                            Hide
                          </span>
                          <span
                            className="card-playlist-action-item"
                            onClick={(event) => {
                              event.preventDefault();
                              event.stopPropagation();
                              onDeletePlaylist?.(playlist);
                            }}
                            role="button"
                            tabIndex={0}
                          >
                            Delete
                          </span>
                        </span>
                      ) : null}
                      {activeAction === "add" ? (
                        <span className="card-playlist-action-popover card-playlist-action-popover-wide">
                          {playlistLists.length > 0 ? playlistLists.map((list) => {
                            const checked = playlistDraft.listIds.includes(list.id);
                            return (
                              <span
                                aria-checked={checked}
                                className="card-playlist-action-item"
                                key={list.id}
                                onClick={(event) => {
                                  event.preventDefault();
                                  event.stopPropagation();
                                  setPlaylistActionDrafts((current) => {
                                    const currentDraft = current[draftKey] ?? draftFromCurrent(playlistId);
                                    const listIds = currentDraft.listIds.includes(list.id)
                                      ? currentDraft.listIds.filter((listId) => listId !== list.id)
                                      : [...currentDraft.listIds, list.id];
                                    return {
                                      ...current,
                                      [draftKey]: { ...currentDraft, listIds },
                                    };
                                  });
                                }}
                                role="checkbox"
                                tabIndex={0}
                              >
                                <span className="card-playlist-action-checkbox" aria-hidden="true">{checked ? "✓" : ""}</span>
                                <span className="card-playlist-action-label">{list.name}</span>
                              </span>
                            );
                          }) : <span className="card-playlist-action-item card-playlist-action-item-muted">Create a category first</span>}
                          <span
                            aria-checked={playlistDraft.pinned}
                            className="card-playlist-action-item"
                            onClick={(event) => {
                              event.preventDefault();
                              event.stopPropagation();
                              setPlaylistActionDrafts((current) => {
                                const currentDraft = current[draftKey] ?? draftFromCurrent(playlistId);
                                return {
                                  ...current,
                                  [draftKey]: { ...currentDraft, pinned: !currentDraft.pinned },
                                };
                              });
                            }}
                            role="checkbox"
                            tabIndex={0}
                          >
                            <span className="card-playlist-action-checkbox" aria-hidden="true">
                              {playlistDraft.pinned ? "✓" : ""}
                            </span>
                            <span className="card-playlist-action-label">Pin</span>
                          </span>
                        </span>
                      ) : null}
                    </>
                  )}
                  {playlist.is_collaborative ? (
                    <span
                      className="card-playlist-collab-marker"
                      title="collaborative playlist"
                    >
                      👥
                    </span>
                  ) : null}
                </span>
              ) : null}
            />
          );
        })}
        {paged ? Array.from({ length: emptySlots(pageItems) }).map((_, index) => (
          <div className="list-row list-row-placeholder" key={`${section}-empty-${index}`} aria-hidden="true" />
        )) : null}
      </div>
      {paged ? (
        <DashboardPaging
          section={section}
          itemCount={items.length}
          pageSize={PAGE_SIZE}
          sectionPage={sectionPage}
          moveSectionPage={moveSectionPage}
        />
      ) : null}
    </>
  );
}
