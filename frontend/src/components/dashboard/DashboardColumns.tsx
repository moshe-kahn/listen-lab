import type { ReactNode } from "react";
import type { FollowedArtist, OwnedPlaylist, PreviewItem, SectionKey, TopAlbum } from "../../types/appTypes";
import { PAGE_SIZE } from "../../constants/appConstants";
import {
  emptySlots,
  formatAlbumBreadth,
  formatAlbumSummary,
  formatHistoryDebugLine,
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
};

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
  paged = true,
}: PlaylistColumnProps) {
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
        {pageItems.map((playlist, index) => (
          <DashboardListCard
            key={playlist.playlist_id ?? `${playlist.name}-${index}-${section}`}
            href={playlist.url}
            entityId={playlist.playlist_id}
            imageUrl={playlist.image_url}
            imageAlt={`${playlist.name ?? "Playlist"} cover`}
            fallbackLabel="P"
            primaryText={playlist.name ?? "Untitled playlist"}
            primaryClamp="two-line-clamp"
            secondaryText={playlist.description?.trim() || null}
            tertiaryText={
              playlist.track_count != null ? `${playlist.track_count} tracks` : "Playlist"
            }
            previewKind="playlist"
            previewTrackUri={null}
            onSelectPreview={onSelectPreview}
          />
        ))}
        {Array.from({ length: emptySlots(pageItems) }).map((_, index) => (
          <div className="list-row list-row-placeholder" key={`${section}-empty-${index}`} aria-hidden="true" />
        ))}
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
