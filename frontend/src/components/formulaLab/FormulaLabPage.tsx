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
  knownTracks: RecentTrack[];
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
  knownTracks,
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

  const knownTrackByKey = new Map<string, RecentTrack>();
  for (const track of knownTracks) {
    knownTrackByKey.set(formulaTrackKey(track), track);
  }
  const hydrateTrack = (track: RecentTrack): RecentTrack => {
    const knownTrack = knownTrackByKey.get(formulaTrackKey(track));
    if (!knownTrack) {
      return track;
    }
    return {
      ...track,
      release_track_id: track.release_track_id ?? knownTrack.release_track_id,
      release_track_name: track.release_track_name ?? knownTrack.release_track_name,
      release_track_source_count: track.release_track_source_count ?? knownTrack.release_track_source_count,
      release_track_duplicate_source_count: track.release_track_duplicate_source_count ?? knownTrack.release_track_duplicate_source_count,
      has_release_track_siblings: track.has_release_track_siblings ?? knownTrack.has_release_track_siblings,
      release_track_cluster_candidate_type: track.release_track_cluster_candidate_type ?? knownTrack.release_track_cluster_candidate_type,
      release_track_cluster_relationship_kind: track.release_track_cluster_relationship_kind ?? knownTrack.release_track_cluster_relationship_kind,
      recording_release_track_ids: track.recording_release_track_ids ?? knownTrack.recording_release_track_ids,
      album_name: track.album_name ?? knownTrack.album_name,
      album_id: track.album_id ?? knownTrack.album_id,
      album_url: track.album_url ?? knownTrack.album_url,
      image_url: track.image_url ?? knownTrack.image_url,
      url: track.url ?? knownTrack.url,
      uri: track.uri ?? knownTrack.uri,
      artists: track.artists ?? knownTrack.artists,
      duration_ms: track.duration_ms ?? knownTrack.duration_ms,
      recording_play_count: track.recording_play_count ?? knownTrack.recording_play_count,
      recording_first_played_at: track.recording_first_played_at ?? knownTrack.recording_first_played_at,
      recording_last_played_at: track.recording_last_played_at ?? knownTrack.recording_last_played_at,
    };
  };

  const hydratedMergedTracks = mergedTracks.map(hydrateTrack);
  const filteredMergedTracks = hydratedMergedTracks.filter((track) => {
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
  const annotateRankMovement = (track: RecentTrack, displayRank: number): RecentTrack => {
    const key = formulaTrackKey(track);
    const baselineRank = baselineRankByTrackKey.get(key);
    const candidateRank = candidateRankByTrackKey.get(key);
    const delta = baselineRank != null && candidateRank != null ? baselineRank - candidateRank : 0;
    return { ...track, formula_rank_delta: delta, formula_rank: displayRank };
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
    .map((track, index) => annotateRankMovement(track, index + 1))
    .filter(rankMovementMatches);
  const candidateDisplayTracks = candidateRankedTracks
    .map((track, index) => annotateRankMovement(track, index + 1))
    .filter(rankMovementMatches);
  const movementFilteredTracks = filteredMergedTracks
    .map((track, index) => annotateRankMovement(track, index + 1))
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
