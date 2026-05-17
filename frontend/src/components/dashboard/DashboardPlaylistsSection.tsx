import type { ReactNode } from "react";
import type { OwnedPlaylist, PreviewItem, SectionKey } from "../../types/appTypes";
import { PLAYLISTS_PAGE_SIZE } from "../../constants/appConstants";
import { previewItems, splitItems } from "../../utils/dashboardUtils";

type DashboardPlaylistsSectionProps = {
  ownedPlaylists: OwnedPlaylist[];
  ownedPlaylistsAvailable: boolean;
  playlistsOpen: boolean;
  toggleSection: (section: SectionKey, anchorId?: string) => void;
  visibleItemsWithPageSize: <T>(section: SectionKey, items: T[], pageSize: number) => T[];
  renderPlaylistColumn: (
    section: SectionKey,
    items: OwnedPlaylist[],
    available: boolean,
    emptyCopy: string,
    unavailableCopy: string,
    paged?: boolean,
  ) => ReactNode;
  renderSectionTitle: (title: string, staleSection?: string) => ReactNode;
  quickUnavailableCopy: (defaultCopy: string) => string;
  renderPreviewCard: (item: PreviewItem, key: string) => ReactNode;
  renderPagingWithPageSize: (section: SectionKey, itemCount: number, pageSize: number) => ReactNode;
};

export function DashboardPlaylistsSection({
  ownedPlaylists,
  ownedPlaylistsAvailable,
  playlistsOpen,
  toggleSection,
  visibleItemsWithPageSize,
  renderPlaylistColumn,
  renderSectionTitle,
  quickUnavailableCopy,
  renderPreviewCard,
  renderPagingWithPageSize,
}: DashboardPlaylistsSectionProps) {
  const visiblePlaylists = visibleItemsWithPageSize(
    "playlists",
    ownedPlaylists,
    PLAYLISTS_PAGE_SIZE,
  );
  const playlistColumns = splitItems(visiblePlaylists);

  return (
    <section className="info-card info-card-wide" id="playlists">
      <button className="section-toggle section-toggle-header" onClick={() => toggleSection("playlists", "playlists")} type="button">
        <h2>{renderSectionTitle("Playlists", "playlists")}</h2>
      </button>
      {playlistsOpen ? (
        ownedPlaylistsAvailable ? (
          ownedPlaylists.length > 0 ? (
            <div className="artists-grid">
              <div className="artists-column">
                {renderPlaylistColumn(
                  "playlists",
                  playlistColumns.left,
                  true,
                  "No playlists were returned by Spotify for this account.",
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
                      "No playlists were returned by Spotify for this account.",
                      "",
                      false,
                    )
                  : <p className="empty-copy">No more playlists in this column yet.</p>}
              </div>
            </div>
          ) : (
            <p className="empty-copy">No playlists were returned by Spotify for this account.</p>
          )
        ) : (
          <p className="empty-copy">
            {quickUnavailableCopy("Playlist access is not available for this session yet. Log out and log back in to grant access.")}
          </p>
        )
      ) : (
        <div className="preview-strip">
          {previewItems(ownedPlaylists).map((item, index) =>
            renderPreviewCard(item, `playlists-${item.image}-${index}`),
          )}
        </div>
      )}
      {playlistsOpen && ownedPlaylists.length > PLAYLISTS_PAGE_SIZE
        ? renderPagingWithPageSize("playlists", ownedPlaylists.length, PLAYLISTS_PAGE_SIZE)
        : null}
      <button className="section-toggle section-toggle-footer" onClick={() => toggleSection("playlists", "playlists")} type="button">
        <span>{playlistsOpen ? "^" : "v"}</span>
      </button>
    </section>
  );
}
