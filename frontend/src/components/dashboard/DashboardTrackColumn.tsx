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
import { recentTrackCompletionRatio } from "../../utils/playbackUtils";
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
  likedTrackIds?: Set<string>;
  releaseTrackSiblingById?: Map<string, number>;
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
  likedTrackIds,
  releaseTrackSiblingById,
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
  const completedRepeatText = (track: RecentTrack) =>
    section === "recent" && Number(track.filtered_play_count ?? track.completed_play_count ?? 0) > 1
      ? `x${track.filtered_play_count ?? track.completed_play_count}`
      : null;
  const completionRatio = (track: RecentTrack) =>
    section === "recent"
      ? recentTrackCompletionRatio(track)
      : null;
  const trackIsKnownLiked = (track: RecentTrack) =>
    Boolean(
      track.is_liked
      ?? (track.source_label === "liked_cache" ? true : null)
      ?? (track.track_id && likedTrackIds?.has(track.track_id) ? true : null),
    );
  const releaseSiblingSourceCount = (track: RecentTrack) =>
    track.track_id ? (releaseTrackSiblingById?.get(track.track_id) ?? 0) : 0;

  const cappedRows = isAllTimeTrackSection || section === "tracksRecent"
    ? capTracksPerAlbum(rankedItems, 1)
    : rankedItems.map((track) => ({ track, hiddenCount: 0 }));
  const pageRows = paged ? visibleItems(section, cappedRows) : cappedRows;
  return (
    <>
      <div className={`item-list${section === "recent" || section === "likes" ? " item-list-scroll" : ""}`}>
        {pageRows.map((row, index) =>
          renderDashboardListCard(
            {
              href: row.track.url,
              entityId: row.track.track_id,
              imageUrl: row.track.image_url,
              imageAlt: `${row.track.album_name ?? row.track.track_name ?? "Album"} cover`,
              fallbackLabel: "T",
              primaryText: row.track.track_name ?? "Unknown track",
              liked: trackIsKnownLiked(row.track),
              releaseSibling: releaseSiblingSourceCount(row.track) > 1,
              releaseSiblingSourceCount: releaseSiblingSourceCount(row.track),
              primaryBadgeText: formulaRankDeltaText(row.track) ?? completedRepeatText(row.track) ?? (showSourceBadge ? formatTrackSourceBadge(row.track) : null),
              secondaryBadgeText: row.hiddenCount > 0 ? `+${row.hiddenCount} more` : null,
              secondaryText: row.track.artist_name ?? "Unknown artist",
              tertiaryText: row.track.album_name ?? "Unknown album",
              completionRatio: completionRatio(row.track),
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
        {section !== "recent" ? Array.from({ length: emptySlots(pageRows) }).map((_, index) => (
          <div className="list-row list-row-placeholder" key={`${section}-empty-${index}`} aria-hidden="true" />
        )) : null}
      </div>
      {paged && section !== "recent" ? (
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
