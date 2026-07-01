import type { ReactNode } from "react";

import type {
  AlbumIdentityAuditTab,
  IdentityAuditEntityTab,
  TrackIdentityAuditTab,
} from "../../utils/identityAuditPrefs";
import type { RecordingTrackCandidateMember } from "../../types/appTypes";
import { AlbumHistorySpotifyRepairTab } from "./AlbumHistorySpotifyRepairTab";
import { ArtistIdentityAuditTab } from "./ArtistIdentityAuditTab";
import { RecordingTrackCandidatesTab } from "./RecordingTrackCandidatesTab";
import { ReleaseTrackDurationConflictsTab } from "./ReleaseTrackDurationConflictsTab";

type TrackAuditWorkflow = "ready_review" | "needs_evidence" | "repair_tools" | "catalog_checks";
type AlbumAuditWorkflow = "ready_repair" | "needs_review" | "catalog_health";

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

const trackWorkflowGroups: Array<{
  value: TrackAuditWorkflow;
  label: string;
  copy: string;
  tabs: Array<{ value: TrackIdentityAuditTab; label: string }>;
}> = [
  {
    value: "ready_review",
    label: "Ready to Review",
    copy: "Start here: issue cards and saved review decisions.",
    tabs: [
      { value: "problems", label: "Issues" },
      { value: "review_queue", label: "Saved Reviews" },
    ],
  },
  {
    value: "needs_evidence",
    label: "Needs Evidence",
    copy: "Inspect source-track mapping before making a judgment.",
    tabs: [{ value: "mapping", label: "Mapping Review" }],
  },
  {
    value: "repair_tools",
    label: "Repair Tools",
    copy: "Inspect and open generated recording groups.",
    tabs: [{ value: "recording_tracks", label: "Recording Groups" }],
  },
  {
    value: "catalog_checks",
    label: "Catalog Checks",
    copy: "Check metadata problems that can block identity decisions.",
    tabs: [{ value: "duration_conflicts", label: "Duration Checks" }],
  },
];

const albumWorkflowGroups: Array<{
  value: AlbumAuditWorkflow;
  label: string;
  copy: string;
  tabs: Array<{ value: AlbumIdentityAuditTab; label: string }>;
}> = [
  {
    value: "ready_repair",
    label: "Ready to Repair",
    copy: "Run safe repairs or inspect a specific merge preview.",
    tabs: [
      { value: "history_spotify_repair", label: "Safe Repair" },
      { value: "merge_review", label: "Merge Review" },
    ],
  },
  {
    value: "needs_review",
    label: "Needs Review",
    copy: "Review likely album duplicates and weaker name conflicts.",
    tabs: [{ value: "problems", label: "Issues" }],
  },
  {
    value: "catalog_health",
    label: "Catalog Health",
    copy: "Check album metadata and tracklist coverage.",
    tabs: [{ value: "catalog", label: "Catalog Health" }],
  },
];

const identityEntityTabs: Array<{ value: IdentityAuditEntityTab; label: string }> = [
  { value: "tracks", label: "Tracks" },
  { value: "albums", label: "Albums" },
  { value: "artists", label: "Artists" },
];

function activeTrackWorkflow(tab: TrackIdentityAuditTab) {
  return trackWorkflowGroups.find((group) => group.tabs.some((item) => item.value === tab)) ?? trackWorkflowGroups[0];
}

function activeAlbumWorkflow(tab: AlbumIdentityAuditTab) {
  return albumWorkflowGroups.find((group) => group.tabs.some((item) => item.value === tab)) ?? albumWorkflowGroups[0];
}

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
  const currentTrackWorkflow = activeTrackWorkflow(trackIdentityAuditTab);
  const currentAlbumWorkflow = activeAlbumWorkflow(albumIdentityAuditTab);
  const activeWorkflowCopy = identityAuditEntityTab === "tracks"
    ? currentTrackWorkflow.copy
    : identityAuditEntityTab === "albums"
      ? currentAlbumWorkflow.copy
      : "Review artist identity repair and review cases.";

  return (
    <section className="info-card info-card-wide tracks-only-card" id="identity-audit-page">
      <div className="tracks-only-header">
        <div>
          <h2>Identity Audit</h2>
          <p className="tracks-only-subtitle">
            Review track, album, and artist identity issues.
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
      {identityAuditEntityTab === "tracks" ? (
        <div className="tracks-only-summary">
          <span>Samples: {identityAuditLimit != null ? `${identityAuditLimit} per group` : "not loaded"}</span>
          <span>Suggested: {suggestedGroupTotal}</span>
          <span>Needs review: {ambiguousReviewTotal}</span>
          {identityAuditLastLoadedAt ? <span>Loaded {new Date(identityAuditLastLoadedAt).toLocaleTimeString()}</span> : null}
          {identityAuditSuggestedLastLoadedAt ? <span>Suggestions loaded {new Date(identityAuditSuggestedLastLoadedAt).toLocaleTimeString()}</span> : null}
          {identityAuditAmbiguousLastLoadedAt ? <span>Review queue loaded {new Date(identityAuditAmbiguousLastLoadedAt).toLocaleTimeString()}</span> : null}
        </div>
      ) : null}
      {identityAuditEntityTab === "albums" ? (
        <div className="tracks-only-summary">
          <span>Duplicate Spotify IDs: {albumDuplicateLookupLoaded ? albumDuplicateTotal : "not loaded"}</span>
          <span>Duplicate names: {albumNameDuplicateLookupLoaded ? albumNameDuplicateTotal : "not loaded"}</span>
          {albumDuplicateLookupLastLoadedAt ? <span>Spotify IDs loaded {new Date(albumDuplicateLookupLastLoadedAt).toLocaleTimeString()}</span> : null}
          {albumNameDuplicateLookupLastLoadedAt ? <span>Names loaded {new Date(albumNameDuplicateLookupLastLoadedAt).toLocaleTimeString()}</span> : null}
        </div>
      ) : null}
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
        <div className="identity-audit-workflow-nav">
          <div className="track-ranking-toggle identity-audit-tabs" role="group" aria-label="Identity audit workflow">
            {identityAuditEntityTab === "tracks"
              ? trackWorkflowGroups.map((group) => (
                <button
                  className={`track-ranking-chip${currentTrackWorkflow.value === group.value ? " track-ranking-chip-active" : ""}`}
                  key={`track-identity-workflow-${group.value}`}
                  onClick={() => setTrackIdentityAuditTab(group.tabs[0].value)}
                  type="button"
                >
                  {group.label}
                </button>
              ))
              : null}
            {identityAuditEntityTab === "albums"
              ? albumWorkflowGroups.map((group) => (
                <button
                  className={`track-ranking-chip${currentAlbumWorkflow.value === group.value ? " track-ranking-chip-active" : ""}`}
                  key={`album-identity-workflow-${group.value}`}
                  onClick={() => setAlbumIdentityAuditTab(group.tabs[0].value)}
                  type="button"
                >
                  {group.label}
                </button>
              ))
              : null}
          </div>
          <div className="identity-audit-next-action">
            <span>Next action</span>
            <strong>{activeWorkflowCopy}</strong>
          </div>
          {identityAuditEntityTab === "tracks" && currentTrackWorkflow.tabs.length > 1 ? (
            <div className="track-ranking-toggle identity-audit-tool-tabs" role="group" aria-label="Track audit tools">
              {currentTrackWorkflow.tabs.map((tab) => (
                <button
                  className={`track-ranking-chip${trackIdentityAuditTab === tab.value ? " track-ranking-chip-active" : ""}`}
                  key={`track-identity-tool-${tab.value}`}
                  onClick={() => setTrackIdentityAuditTab(tab.value)}
                  type="button"
                >
                  {tab.label}
                </button>
              ))}
            </div>
          ) : null}
          {identityAuditEntityTab === "albums" && currentAlbumWorkflow.tabs.length > 1 ? (
            <div className="track-ranking-toggle identity-audit-tool-tabs" role="group" aria-label="Album audit tools">
              {currentAlbumWorkflow.tabs.map((tab) => (
              <button
                className={`track-ranking-chip${albumIdentityAuditTab === tab.value ? " track-ranking-chip-active" : ""}`}
                key={`album-identity-tool-${tab.value}`}
                onClick={() => setAlbumIdentityAuditTab(tab.value)}
                type="button"
              >
                {tab.label}
              </button>
              ))}
            </div>
          ) : null}
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
      {identityAuditEntityTab === "albums" && albumIdentityAuditTab === "history_spotify_repair" ? <AlbumHistorySpotifyRepairTab /> : null}
      {identityAuditEntityTab === "albums" && albumIdentityAuditTab === "catalog" ? renderAlbumCatalogTab() : null}
      {identityAuditEntityTab === "artists" ? <ArtistIdentityAuditTab /> : null}
    </section>
  );
}
