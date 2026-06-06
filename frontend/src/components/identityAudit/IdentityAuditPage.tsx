import type { ReactNode } from "react";

import type {
  AlbumIdentityAuditTab,
  IdentityAuditEntityTab,
  TrackIdentityAuditTab,
} from "../../utils/identityAuditPrefs";
import type { RecordingTrackCandidateMember } from "../../types/appTypes";
import { ArtistDuplicateAuditTab } from "./ArtistDuplicateAuditTab";
import { RecordingTrackCandidatesTab } from "./RecordingTrackCandidatesTab";
import { ReleaseTrackDurationConflictsTab } from "./ReleaseTrackDurationConflictsTab";

type IdentityAuditPageProps = {
  identityAuditLoading: boolean;
  identityAuditSuggestedLoading: boolean;
  identityAuditAmbiguousLoading: boolean;
  identityAuditLimit: number | null;
  suggestedGroupTotal: number;
  ambiguousReviewTotal: number;
  albumDuplicateLookupLoaded: boolean;
  albumDuplicateTotal: number;
  albumNameDuplicateLookupLoaded: boolean;
  albumNameDuplicateTotal: number;
  identityAuditLastLoadedAt: number | null;
  identityAuditSuggestedLastLoadedAt: number | null;
  identityAuditAmbiguousLastLoadedAt: number | null;
  albumDuplicateLookupLastLoadedAt: number | null;
  albumNameDuplicateLookupLastLoadedAt: number | null;
  identityAuditEntityTab: IdentityAuditEntityTab;
  trackIdentityAuditTab: TrackIdentityAuditTab;
  albumIdentityAuditTab: AlbumIdentityAuditTab;
  onReloadAll: () => void;
  onBackToDashboard: () => void;
  setIdentityAuditEntityTab: (value: IdentityAuditEntityTab) => void;
  setTrackIdentityAuditTab: (value: TrackIdentityAuditTab) => void;
  setAlbumIdentityAuditTab: (value: AlbumIdentityAuditTab) => void;
  onOpenRecordingCandidateReleaseTrack: (member: RecordingTrackCandidateMember) => void;
  renderTrackProblemsTab: () => ReactNode;
  renderTrackMappingTab: () => ReactNode;
  renderTrackReviewQueueTab: () => ReactNode;
  renderAlbumProblemsTab: () => ReactNode;
  renderAlbumMergeReviewTab: () => ReactNode;
  renderAlbumCatalogTab: () => ReactNode;
};

const trackTabs: Array<{ value: TrackIdentityAuditTab; label: string }> = [
  { value: "problems", label: "Problems" },
  { value: "mapping", label: "Mapping" },
  { value: "review_queue", label: "Review Queue" },
  { value: "recording_tracks", label: "Recording Tracks" },
  { value: "duration_conflicts", label: "Duration Conflicts" },
];

const albumTabs: Array<{ value: AlbumIdentityAuditTab; label: string }> = [
  { value: "problems", label: "Problems" },
  { value: "merge_review", label: "Merge Review" },
  { value: "catalog", label: "Catalog" },
];

const identityEntityTabs: Array<{ value: IdentityAuditEntityTab; label: string }> = [
  { value: "tracks", label: "Tracks" },
  { value: "albums", label: "Albums" },
  { value: "artists", label: "Artists" },
];

export function IdentityAuditPage({
  identityAuditLoading,
  identityAuditSuggestedLoading,
  identityAuditAmbiguousLoading,
  identityAuditLimit,
  suggestedGroupTotal,
  ambiguousReviewTotal,
  albumDuplicateLookupLoaded,
  albumDuplicateTotal,
  albumNameDuplicateLookupLoaded,
  albumNameDuplicateTotal,
  identityAuditLastLoadedAt,
  identityAuditSuggestedLastLoadedAt,
  identityAuditAmbiguousLastLoadedAt,
  albumDuplicateLookupLastLoadedAt,
  albumNameDuplicateLookupLastLoadedAt,
  identityAuditEntityTab,
  trackIdentityAuditTab,
  albumIdentityAuditTab,
  onReloadAll,
  onBackToDashboard,
  setIdentityAuditEntityTab,
  setTrackIdentityAuditTab,
  setAlbumIdentityAuditTab,
  onOpenRecordingCandidateReleaseTrack,
  renderTrackProblemsTab,
  renderTrackMappingTab,
  renderTrackReviewQueueTab,
  renderAlbumProblemsTab,
  renderAlbumMergeReviewTab,
  renderAlbumCatalogTab,
}: IdentityAuditPageProps) {
  const reloading = identityAuditLoading || identityAuditSuggestedLoading || identityAuditAmbiguousLoading;

  return (
    <section className="info-card info-card-wide tracks-only-card" id="identity-audit-page">
      <div className="tracks-only-header">
        <div>
          <h2>Identity Audit</h2>
          <p className="tracks-only-subtitle">
            Find identity problems, inspect mappings and evidence, then review decisions before any grouping behavior is promoted.
          </p>
        </div>
        <div className="section-column-header-actions">
          <button
            className="secondary-button tracks-page-link-button"
            disabled={reloading}
            onClick={onReloadAll}
            type="button"
          >
            {reloading ? "Reloading..." : "Reload all"}
          </button>
          <button
            className="secondary-button tracks-only-back-button"
            onClick={onBackToDashboard}
            type="button"
          >
            Back to dashboard
          </button>
        </div>
      </div>
      <div className="tracks-only-summary">
        <span>Identity samples: {identityAuditLimit != null ? `${identityAuditLimit} per group` : "not loaded"}</span>
        <span>Suggested groups: {suggestedGroupTotal}</span>
        <span>Ambiguous queue: {ambiguousReviewTotal}</span>
        <span>Album duplicate Spotify ID groups: {albumDuplicateLookupLoaded ? albumDuplicateTotal : "not loaded"}</span>
        <span>Album duplicate name groups: {albumNameDuplicateLookupLoaded ? albumNameDuplicateTotal : "not loaded"}</span>
        {identityAuditLastLoadedAt ? <span>Identity loaded {new Date(identityAuditLastLoadedAt).toLocaleTimeString()}</span> : null}
        {identityAuditSuggestedLastLoadedAt ? <span>Suggested loaded {new Date(identityAuditSuggestedLastLoadedAt).toLocaleTimeString()}</span> : null}
        {identityAuditAmbiguousLastLoadedAt ? <span>Ambiguous loaded {new Date(identityAuditAmbiguousLastLoadedAt).toLocaleTimeString()}</span> : null}
        {albumDuplicateLookupLastLoadedAt ? <span>Album duplicates loaded {new Date(albumDuplicateLookupLastLoadedAt).toLocaleTimeString()}</span> : null}
        {albumNameDuplicateLookupLastLoadedAt ? <span>Album names loaded {new Date(albumNameDuplicateLookupLastLoadedAt).toLocaleTimeString()}</span> : null}
      </div>
      <div className="track-ranking-toggle identity-audit-tabs" role="group" aria-label="Identity audit entity type">
        {identityEntityTabs.map((tab) => (
          <button
            className={`track-ranking-chip${identityAuditEntityTab === tab.value ? " track-ranking-chip-active" : ""}`}
            key={`identity-entity-tab-${tab.value}`}
            onClick={() => setIdentityAuditEntityTab(tab.value)}
            type="button"
          >
            {tab.label}
          </button>
        ))}
      </div>
      {identityAuditEntityTab !== "artists" ? (
        <div className="track-ranking-toggle identity-audit-tabs" role="group" aria-label="Identity audit sections">
          {identityAuditEntityTab === "tracks"
            ? trackTabs.map((tab) => (
              <button
                className={`track-ranking-chip${trackIdentityAuditTab === tab.value ? " track-ranking-chip-active" : ""}`}
                key={`track-identity-tab-${tab.value}`}
                onClick={() => setTrackIdentityAuditTab(tab.value)}
                type="button"
              >
                {tab.label}
              </button>
            ))
            : null}
          {identityAuditEntityTab === "albums"
            ? albumTabs.map((tab) => (
              <button
                className={`track-ranking-chip${albumIdentityAuditTab === tab.value ? " track-ranking-chip-active" : ""}`}
                key={`album-identity-tab-${tab.value}`}
                onClick={() => setAlbumIdentityAuditTab(tab.value)}
                type="button"
              >
                {tab.label}
              </button>
            ))
            : null}
        </div>
      ) : null}
      {identityAuditEntityTab === "tracks" && trackIdentityAuditTab === "problems" ? renderTrackProblemsTab() : null}
      {identityAuditEntityTab === "tracks" && trackIdentityAuditTab === "mapping" ? renderTrackMappingTab() : null}
      {identityAuditEntityTab === "tracks" && trackIdentityAuditTab === "review_queue" ? renderTrackReviewQueueTab() : null}
      {identityAuditEntityTab === "tracks" && trackIdentityAuditTab === "recording_tracks" ? (
        <RecordingTrackCandidatesTab onOpenReleaseTrack={onOpenRecordingCandidateReleaseTrack} />
      ) : null}
      {identityAuditEntityTab === "tracks" && trackIdentityAuditTab === "duration_conflicts" ? <ReleaseTrackDurationConflictsTab /> : null}
      {identityAuditEntityTab === "albums" && albumIdentityAuditTab === "problems" ? renderAlbumProblemsTab() : null}
      {identityAuditEntityTab === "albums" && albumIdentityAuditTab === "merge_review" ? renderAlbumMergeReviewTab() : null}
      {identityAuditEntityTab === "albums" && albumIdentityAuditTab === "catalog" ? renderAlbumCatalogTab() : null}
      {identityAuditEntityTab === "artists" ? <ArtistDuplicateAuditTab /> : null}
    </section>
  );
}
