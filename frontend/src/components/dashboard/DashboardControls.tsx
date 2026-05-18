import type { MergedTrackSourceFilter, RankMovementFilter, TrackRankingMode } from "../../types/appTypes";
import {
  MERGED_TRACK_SOURCE_FILTER_OPTIONS,
  RANK_MOVEMENT_FILTER_OPTIONS,
} from "../../constants/appConstants";

type TrackRankingToggleProps = {
  trackRankingMode: TrackRankingMode;
  showTrackRankingSpinner: boolean;
  onSelectTrackRankingMode: (nextMode: TrackRankingMode) => void;
};

type MergedTrackSourceFilterToggleProps = {
  mergedTrackSourceFilter: MergedTrackSourceFilter;
  setMergedTrackSourceFilter: (value: MergedTrackSourceFilter) => void;
};

type RankMovementFilterToggleProps = {
  rankMovementFilter: RankMovementFilter;
  setRankMovementFilter: (value: RankMovementFilter) => void;
};

export function TrackRankingToggle({
  trackRankingMode,
  showTrackRankingSpinner,
  onSelectTrackRankingMode,
}: TrackRankingToggleProps) {
  return (
    <div className="track-ranking-toggle" role="group" aria-label="Top track ranking mode">
      {showTrackRankingSpinner ? (
        <span className="recent-range-vinyl-spinner" aria-hidden="true">
          <span className="recent-range-vinyl-center" />
        </span>
      ) : null}
      <button
        className={`track-ranking-chip${trackRankingMode === "plays" ? " track-ranking-chip-active" : ""}`}
        onClick={() => onSelectTrackRankingMode("plays")}
        type="button"
      >
        Plays
      </button>
      <button
        className={`track-ranking-chip${trackRankingMode === "mix" ? " track-ranking-chip-active" : ""}`}
        onClick={() => onSelectTrackRankingMode("mix")}
        type="button"
      >
        Mix
      </button>
      <button
        className={`track-ranking-chip${trackRankingMode === "longevity" ? " track-ranking-chip-active" : ""}`}
        onClick={() => onSelectTrackRankingMode("longevity")}
        type="button"
      >
        Longevity
      </button>
    </div>
  );
}

export function MergedTrackSourceFilterToggle({
  mergedTrackSourceFilter,
  setMergedTrackSourceFilter,
}: MergedTrackSourceFilterToggleProps) {
  return (
    <div className="track-ranking-toggle" role="group" aria-label="Merged track source filter">
      {MERGED_TRACK_SOURCE_FILTER_OPTIONS.map((option) => (
        <button
          className={`track-ranking-chip${mergedTrackSourceFilter === option.value ? " track-ranking-chip-active" : ""}`}
          key={option.value}
          onClick={() => setMergedTrackSourceFilter(option.value)}
          type="button"
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}

export function RankMovementFilterToggle({
  rankMovementFilter,
  setRankMovementFilter,
}: RankMovementFilterToggleProps) {
  return (
    <div className="track-ranking-toggle" role="group" aria-label="Formula rank movement filter">
      {RANK_MOVEMENT_FILTER_OPTIONS.map((option) => (
        <button
          className={`track-ranking-chip${rankMovementFilter === option.value ? " track-ranking-chip-active" : ""}`}
          key={option.value}
          onClick={() => setRankMovementFilter(option.value)}
          type="button"
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}
