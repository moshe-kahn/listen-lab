import type { ReactNode } from "react";
import type {
  MergedTrackSourceFilter,
  RankMovementFilter,
  RecentTrack,
  SectionKey,
  TrackRankingMode,
} from "../../types/appTypes";
import {
  MERGED_TRACK_SOURCE_FILTER_OPTIONS,
  RANK_MOVEMENT_FILTER_OPTIONS,
} from "../../constants/appConstants";
import {
  baselineFormulaLabel,
  candidateFormulaLabel,
  formulaTrackKey,
  sortedTracksForView,
} from "../../utils/dashboardUtils";

type FormulaLabPageProps = {
  hasProfile: boolean;
  mergedTracks: RecentTrack[];
  mergedTracksLoaded: boolean;
  mergedTracksLoading: boolean;
  mergedTracksError: string;
  mergedTracksLastLoadedAt: number | null;
  mergedTrackSourceFilter: MergedTrackSourceFilter;
  rankMovementFilter: RankMovementFilter;
  trackRankingMode: TrackRankingMode;
  renderMergedTrackSourceFilterToggle: () => ReactNode;
  renderTrackRankingToggle: () => ReactNode;
  renderRankMovementFilterToggle: () => ReactNode;
  renderTrackColumn: (
    section: SectionKey,
    items: RecentTrack[],
    available: boolean,
    emptyCopy: string,
    unavailableCopy: string,
    unavailableAction?: ReactNode,
    paged?: boolean,
    presorted?: boolean,
  ) => ReactNode;
  reloadTrackRankings: () => void;
  onBack: () => void;
};

export function FormulaLabPage({
  hasProfile,
  mergedTracks,
  mergedTracksLoaded,
  mergedTracksLoading,
  mergedTracksError,
  mergedTracksLastLoadedAt,
  mergedTrackSourceFilter,
  rankMovementFilter,
  trackRankingMode,
  renderMergedTrackSourceFilterToggle,
  renderTrackRankingToggle,
  renderRankMovementFilterToggle,
  renderTrackColumn,
  reloadTrackRankings,
  onBack,
}: FormulaLabPageProps) {
  if (!hasProfile) {
    return null;
  }

  const filteredMergedTracks = mergedTracks.filter((track) => {
    if (mergedTrackSourceFilter === "recent") {
      return track.source_label === "recent" || (track.has_recent_source && !track.has_history_source);
    }
    if (mergedTrackSourceFilter === "history") {
      return track.source_label === "history" || (track.has_history_source && !track.has_recent_source);
    }
    if (mergedTrackSourceFilter === "both") {
      return track.source_label === "both" || (track.has_recent_source && track.has_history_source);
    }
    return true;
  });
  const baselineRankedTracks = sortedTracksForView("tracksAllTimeCurrent", filteredMergedTracks, trackRankingMode);
  const candidateRankedTracks = sortedTracksForView("tracksAllTimeNew", filteredMergedTracks, trackRankingMode);
  const baselineRankByTrackKey = new Map(
    baselineRankedTracks.map((track, index) => [formulaTrackKey(track), index + 1]),
  );
  const candidateRankByTrackKey = new Map(
    candidateRankedTracks.map((track, index) => [formulaTrackKey(track), index + 1]),
  );
  const annotateRankMovement = (track: RecentTrack): RecentTrack => {
    const key = formulaTrackKey(track);
    const baselineRank = baselineRankByTrackKey.get(key);
    const candidateRank = candidateRankByTrackKey.get(key);
    const delta = baselineRank != null && candidateRank != null ? baselineRank - candidateRank : 0;
    return { ...track, formula_rank_delta: delta };
  };
  const rankMovementMatches = (track: RecentTrack) => {
    const delta = Number(track.formula_rank_delta ?? 0);
    if (rankMovementFilter === "risers") {
      return delta > 0;
    }
    if (rankMovementFilter === "fallers") {
      return delta < 0;
    }
    return true;
  };
  const baselineDisplayTracks = baselineRankedTracks
    .map(annotateRankMovement)
    .filter(rankMovementMatches);
  const candidateDisplayTracks = candidateRankedTracks
    .map(annotateRankMovement)
    .filter(rankMovementMatches);
  const movementFilteredTracks = filteredMergedTracks
    .map(annotateRankMovement)
    .filter((track) => {
      return rankMovementMatches(track);
    });
  const filteredTrackCount = movementFilteredTracks.length;
  const sourceFilterLabel = MERGED_TRACK_SOURCE_FILTER_OPTIONS.find((option) => option.value === mergedTrackSourceFilter)?.label ?? "All plays";
  const rankMovementLabel = RANK_MOVEMENT_FILTER_OPTIONS.find((option) => option.value === rankMovementFilter)?.label ?? "All";
  const mergedTrackEmptyCopy = mergedTracksLoaded
    ? `No ${sourceFilterLabel.toLowerCase()} tracks are available for this comparison.`
    : "Loading track rankings...";
  const mergedTrackUnavailableCopy = mergedTracksLoading
    ? "Loading track rankings..."
    : (mergedTracksError || "Track rankings are not available yet.");
  const formulaModeLabel =
    trackRankingMode === "plays"
      ? "plays"
      : trackRankingMode === "longevity"
        ? "longevity"
        : "mix";

  return (
    <section className="info-card info-card-wide tracks-only-card" id="tracks-page">
      <div className="tracks-only-header">
        <div className="section-column-header tracks-only-header-copy">
          <div>
            <h2>Top Tracks Formula Lab</h2>
            <p className="tracks-only-subtitle">
              Compare canonical track rankings built from merged play history.
            </p>
          </div>
          <div className="section-column-header-actions tracks-only-controls">
            {renderMergedTrackSourceFilterToggle()}
            {renderTrackRankingToggle()}
            {renderRankMovementFilterToggle()}
            <button
              className="secondary-button tracks-page-link-button"
              disabled={mergedTracksLoading}
              onClick={reloadTrackRankings}
              type="button"
            >
              {mergedTracksLoading ? "Reloading..." : "Reload rankings"}
            </button>
          </div>
        </div>
        <button
          className="secondary-button tracks-only-back-button"
          onClick={onBack}
          type="button"
        >
          Back to dashboard
        </button>
      </div>
      <div className="tracks-only-summary">
        <span>{filteredTrackCount} tracks in comparison</span>
        <span>{formulaModeLabel} mode</span>
        <span>{rankMovementLabel}</span>
        <span>{sourceFilterLabel}</span>
        {mergedTracksLastLoadedAt ? (
          <span>Loaded {new Date(mergedTracksLastLoadedAt).toLocaleTimeString()}</span>
        ) : null}
      </div>
      {mergedTracksError ? (
        <p className="empty-copy">
          {mergedTracksError}
          {" "}
          Refresh this page after confirming the frontend is pointed at the same backend where `/auth/session` is authenticated.
        </p>
      ) : null}
      <div className="artists-grid">
        <div className="artists-column">
          <div className="tracks-formula-heading">
            <h3>Baseline formula</h3>
            <span>{baselineFormulaLabel(trackRankingMode)}</span>
          </div>
          {renderTrackColumn(
            "tracksAllTimeCurrent",
            baselineDisplayTracks,
            mergedTracksLoaded && !mergedTracksLoading && !mergedTracksError,
            mergedTrackEmptyCopy,
            mergedTrackUnavailableCopy,
            undefined,
            false,
            true,
          )}
        </div>
        <div className="artists-column">
          <div className="tracks-formula-heading">
            <h3>Candidate formula</h3>
            <span>{candidateFormulaLabel(trackRankingMode)}</span>
          </div>
          {renderTrackColumn(
            "tracksAllTimeNew",
            candidateDisplayTracks,
            mergedTracksLoaded && !mergedTracksLoading && !mergedTracksError,
            mergedTrackEmptyCopy,
            mergedTrackUnavailableCopy,
            undefined,
            false,
            true,
          )}
        </div>
      </div>
    </section>
  );
}
