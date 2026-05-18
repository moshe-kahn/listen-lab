import type { ReactNode } from "react";
import type { DashboardListCardProps, RecentTrack, SectionKey, TrackRankingMode } from "../../types/appTypes";
import { PAGE_SIZE } from "../../constants/appConstants";
import {
  capTracksPerAlbum,
  emptySlots,
  formatFormulaRankDelta,
  formatTrackLongevity,
  formatTrackRankingMetric,
  formatTrackSourceBadge,
  sortedTracksForView,
} from "../../utils/dashboardUtils";
import { DashboardPaging } from "./DashboardPaging";

type DashboardTrackColumnProps = {
  section: SectionKey;
  items: RecentTrack[];
  available: boolean;
  emptyCopy: string;
  unavailableCopy: string;
  unavailableAction?: ReactNode;
  paged: boolean;
  presorted: boolean;
  trackRankingMode: TrackRankingMode;
  sectionPage: number;
  moveSectionPage: (section: SectionKey, direction: -1 | 1, itemCount: number, pageSize?: number) => void;
  visibleItems: <T>(section: SectionKey, items: T[]) => T[];
  renderDashboardListCard: (props: DashboardListCardProps, key: string) => ReactNode;
};

export function DashboardTrackColumn({
  section,
  items,
  available,
  emptyCopy,
  unavailableCopy,
  unavailableAction,
  paged,
  presorted,
  trackRankingMode,
  sectionPage,
  moveSectionPage,
  visibleItems,
  renderDashboardListCard,
}: DashboardTrackColumnProps) {
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

  const rankedItems = presorted ? items : sortedTracksForView(section, items, trackRankingMode);
  const isAllTimeTrackSection =
    section === "tracksAllTime" ||
    section === "tracksAllTimeCurrent" ||
    section === "tracksAllTimeNew";
  const showSourceBadge = section !== "tracksAllTimeCurrent" && section !== "tracksAllTimeNew";
  const formulaRankDeltaText = (track: RecentTrack) =>
    section === "tracksAllTimeCurrent" || section === "tracksAllTimeNew"
      ? formatFormulaRankDelta(track)
      : null;

  const cappedRows = isAllTimeTrackSection || section === "tracksRecent"
    ? capTracksPerAlbum(rankedItems, 1)
    : rankedItems.map((track) => ({ track, hiddenCount: 0 }));
  const pageRows = paged ? visibleItems(section, cappedRows) : cappedRows;
  return (
    <>
      <div className="item-list">
        {pageRows.map((row, index) =>
          renderDashboardListCard(
            {
              href: row.track.url,
              entityId: row.track.track_id,
              imageUrl: row.track.image_url,
              imageAlt: `${row.track.album_name ?? row.track.track_name ?? "Album"} cover`,
              fallbackLabel: "T",
              primaryText: row.track.track_name ?? "Unknown track",
              primaryBadgeText: formulaRankDeltaText(row.track) ?? (showSourceBadge ? formatTrackSourceBadge(row.track) : null),
              secondaryBadgeText: row.hiddenCount > 0 ? `+${row.hiddenCount} more` : null,
              secondaryText: row.track.artist_name ?? "Unknown artist",
              tertiaryText: row.track.album_name ?? "Unknown album",
              metricText: isAllTimeTrackSection
                ? (
                    section === "tracksAllTimeNew"
                      ? `${row.track.play_count ?? 0} | ${formatTrackLongevity(row.track) ?? "0d"}`
                      : formatTrackRankingMetric(row.track, trackRankingMode)
                  )
                : null,
              trackUri: row.track.uri ?? null,
              previewTrack: row.track,
            },
            row.track.track_id ?? `${row.track.track_name}-${index}-${section}`,
          ),
        )}
        {Array.from({ length: emptySlots(pageRows) }).map((_, index) => (
          <div className="list-row list-row-placeholder" key={`${section}-empty-${index}`} aria-hidden="true" />
        ))}
      </div>
      {paged ? (
        <DashboardPaging
          section={section}
          itemCount={cappedRows.length}
          pageSize={PAGE_SIZE}
          sectionPage={sectionPage}
          moveSectionPage={moveSectionPage}
        />
      ) : null}
    </>
  );
}
