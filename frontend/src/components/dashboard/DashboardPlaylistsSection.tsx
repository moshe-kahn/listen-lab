import type { ReactNode } from "react";
import type { OwnedPlaylist, PreviewItem, SectionKey } from "../../types/appTypes";
import { PLAYLISTS_PAGE_SIZE } from "../../constants/appConstants";
import { previewItems, splitItems } from "../../utils/dashboardUtils";
import { DashboardPlaylistColumn } from "./DashboardColumns";
import { DashboardPaging } from "./DashboardPaging";
import { PreviewCard } from "./PreviewCard";

type DashboardPlaylistsSectionProps = {
  ownedPlaylists: OwnedPlaylist[];
  ownedPlaylistsAvailable: boolean;
  playlistsOpen: boolean;
  toggleSection: (section: SectionKey, anchorId?: string) => void;
  sectionPage: number;
  moveSectionPage: (section: SectionKey, direction: -1 | 1, itemCount: number, pageSize?: number) => void;
  onSelectPreview: (preview: PreviewItem) => void;
  visibleItemsWithPageSize: <T>(section: SectionKey, items: T[], pageSize: number) => T[];
  renderSectionTitle: (title: string, staleSection?: string) => ReactNode;
  quickUnavailableCopy: (defaultCopy: string) => string;
};

export function DashboardPlaylistsSection({
  ownedPlaylists,
  ownedPlaylistsAvailable,
  playlistsOpen,
  toggleSection,
  sectionPage,
  moveSectionPage,
  onSelectPreview,
  visibleItemsWithPageSize,
  renderSectionTitle,
  quickUnavailableCopy,
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
                <DashboardPlaylistColumn
                  section="playlists"
                  items={playlistColumns.left}
                  available={true}
                  emptyCopy="No playlists were returned by Spotify for this account."
                  unavailableCopy=""
                  sectionPage={sectionPage}
                  moveSectionPage={moveSectionPage}
                  onSelectPreview={onSelectPreview}
                  paged={false}
                />
              </div>
              <div className="artists-column">
                {playlistColumns.right.length > 0
                  ? (
                    <DashboardPlaylistColumn
                      section="playlists"
                      items={playlistColumns.right}
                      available={true}
                      emptyCopy="No playlists were returned by Spotify for this account."
                      unavailableCopy=""
                      sectionPage={sectionPage}
                      moveSectionPage={moveSectionPage}
                      onSelectPreview={onSelectPreview}
                      paged={false}
                    />
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
            <PreviewCard
              item={item}
              key={`playlists-${item.image}-${index}`}
              onSelectPreview={onSelectPreview}
            />,
          )}
        </div>
      )}
      {playlistsOpen && ownedPlaylists.length > PLAYLISTS_PAGE_SIZE
        ? (
          <DashboardPaging
            section="playlists"
            itemCount={ownedPlaylists.length}
            pageSize={PLAYLISTS_PAGE_SIZE}
            sectionPage={sectionPage}
            moveSectionPage={moveSectionPage}
          />
        )
        : null}
      <button className="section-toggle section-toggle-footer" onClick={() => toggleSection("playlists", "playlists")} type="button">
        <span>{playlistsOpen ? "^" : "v"}</span>
      </button>
    </section>
  );
}
