import type { Dispatch, SetStateAction } from "react";
import type {
  AmbiguousReviewItem,
  AmbiguousReviewResponse,
  IdentityAuditSavedSubmissionListResponse,
  IdentityAuditSavedSubmissionReadResponse,
  IdentityAuditSubmissionDryRunResponse,
  IdentityAuditSubmissionSaveResponse,
  LocalGroupingTarget,
  LocalReviewDecision,
  LocalReviewVerdict,
  SubmissionPreviewValidationResponse,
  SuggestedAnalysisGroup,
  SuggestedGroupsResponse,
  UnifiedReviewItem,
} from "../../types/appTypes";
import { IDENTITY_AUDIT_AMBIGUOUS_VISIBLE_STEP } from "../../constants/appConstants";
import { formatUiErrorMessage } from "../../utils/dashboardUtils";

type TrackIdentityAuditAmbiguousTabProps = {
  computeAmbiguousTrackItems: () => AmbiguousReviewItem[];
  computeUnifiedReviewItems: () => UnifiedReviewItem[];
  dryRunIdentityAuditSavedSubmission: (submissionId: number) => void;
  findNextUnreviewedDecisionKey: (
    items: UnifiedReviewItem[],
    afterKey?: string | null,
    decisions?: Record<string, LocalReviewDecision>,
  ) => string | null;
  groupDecisionKey: (group: SuggestedAnalysisGroup) => string;
  identityAuditAmbiguous: AmbiguousReviewResponse | null;
  identityAuditAmbiguousBucketFilter: "all" | "grouped" | "ungrouped";
  identityAuditAmbiguousError: string;
  identityAuditAmbiguousFamilyFilter: string;
  identityAuditAmbiguousLoading: boolean;
  identityAuditAmbiguousVisibleCount: number;
  identityAuditFocusedReviewKey: string | null;
  identityAuditLocalDecisions: Record<string, LocalReviewDecision>;
  identityAuditPreviewCopyStatus: string;
  identityAuditPreviewValidatedAt: number | null;
  identityAuditPreviewValidationError: string;
  identityAuditPreviewValidationLoading: boolean;
  identityAuditPreviewValidationResult: SubmissionPreviewValidationResponse | null;
  identityAuditSavedSubmissionDetail: IdentityAuditSavedSubmissionReadResponse | null;
  identityAuditSavedSubmissionDetailError: string;
  identityAuditSavedSubmissionDetailLoading: boolean;
  identityAuditSavedSubmissionDryRun: IdentityAuditSubmissionDryRunResponse | null;
  identityAuditSavedSubmissionDryRunAt: number | null;
  identityAuditSavedSubmissionDryRunError: string;
  identityAuditSavedSubmissionDryRunLoading: boolean;
  identityAuditSavedSubmissions: IdentityAuditSavedSubmissionListResponse | null;
  identityAuditSavedSubmissionsError: string;
  identityAuditSavedSubmissionsLoading: boolean;
  identityAuditSubmissionSaveError: string;
  identityAuditSubmissionSaveLoading: boolean;
  identityAuditSubmissionSaveResult: IdentityAuditSubmissionSaveResponse | null;
  identityAuditSuggestedError: string;
  identityAuditSuggestedGroups: SuggestedGroupsResponse | null;
  identityAuditSuggestedLoading: boolean;
  isReviewedDecision: (decision: LocalReviewDecision | undefined) => boolean;
  loadIdentityAuditSavedSubmissions: (reset?: boolean) => void;
  setIdentityAuditAmbiguousBucketFilter: Dispatch<SetStateAction<"all" | "grouped" | "ungrouped">>;
  setIdentityAuditAmbiguousFamilyFilter: Dispatch<SetStateAction<string>>;
  setIdentityAuditAmbiguousVisibleCount: Dispatch<SetStateAction<number>>;
  setIdentityAuditFocusedReviewKey: Dispatch<SetStateAction<string | null>>;
  setIdentityAuditLocalDecisions: Dispatch<SetStateAction<Record<string, LocalReviewDecision>>>;
  setIdentityAuditPreviewCopyStatus: Dispatch<SetStateAction<string>>;
  setIdentityAuditPreviewValidatedAt: Dispatch<SetStateAction<number | null>>;
  setIdentityAuditPreviewValidationError: Dispatch<SetStateAction<string>>;
  setIdentityAuditPreviewValidationLoading: Dispatch<SetStateAction<boolean>>;
  setIdentityAuditPreviewValidationResult: Dispatch<SetStateAction<SubmissionPreviewValidationResponse | null>>;
  setIdentityAuditSubmissionSaveError: Dispatch<SetStateAction<string>>;
  setIdentityAuditSubmissionSaveLoading: Dispatch<SetStateAction<boolean>>;
  setIdentityAuditSubmissionSaveResult: Dispatch<SetStateAction<IdentityAuditSubmissionSaveResponse | null>>;
  trackDecisionKey: (track: AmbiguousReviewItem) => string;
  updateLocalReviewDecision: (entryId: string, patch: Partial<LocalReviewDecision>) => void;
  viewIdentityAuditSavedSubmission: (submissionId: number) => void;
};

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "/api";

export function TrackIdentityAuditAmbiguousTab({
  computeAmbiguousTrackItems,
  computeUnifiedReviewItems,
  dryRunIdentityAuditSavedSubmission,
  findNextUnreviewedDecisionKey,
  groupDecisionKey,
  identityAuditAmbiguous,
  identityAuditAmbiguousBucketFilter,
  identityAuditAmbiguousError,
  identityAuditAmbiguousFamilyFilter,
  identityAuditAmbiguousLoading,
  identityAuditAmbiguousVisibleCount,
  identityAuditFocusedReviewKey,
  identityAuditLocalDecisions,
  identityAuditPreviewCopyStatus,
  identityAuditPreviewValidatedAt,
  identityAuditPreviewValidationError,
  identityAuditPreviewValidationLoading,
  identityAuditPreviewValidationResult,
  identityAuditSavedSubmissionDetail,
  identityAuditSavedSubmissionDetailError,
  identityAuditSavedSubmissionDetailLoading,
  identityAuditSavedSubmissionDryRun,
  identityAuditSavedSubmissionDryRunAt,
  identityAuditSavedSubmissionDryRunError,
  identityAuditSavedSubmissionDryRunLoading,
  identityAuditSavedSubmissions,
  identityAuditSavedSubmissionsError,
  identityAuditSavedSubmissionsLoading,
  identityAuditSubmissionSaveError,
  identityAuditSubmissionSaveLoading,
  identityAuditSubmissionSaveResult,
  identityAuditSuggestedError,
  identityAuditSuggestedGroups,
  identityAuditSuggestedLoading,
  isReviewedDecision,
  loadIdentityAuditSavedSubmissions,
  setIdentityAuditAmbiguousBucketFilter,
  setIdentityAuditAmbiguousFamilyFilter,
  setIdentityAuditAmbiguousVisibleCount,
  setIdentityAuditFocusedReviewKey,
  setIdentityAuditLocalDecisions,
  setIdentityAuditPreviewCopyStatus,
  setIdentityAuditPreviewValidatedAt,
  setIdentityAuditPreviewValidationError,
  setIdentityAuditPreviewValidationLoading,
  setIdentityAuditPreviewValidationResult,
  setIdentityAuditSubmissionSaveError,
  setIdentityAuditSubmissionSaveLoading,
  setIdentityAuditSubmissionSaveResult,
  trackDecisionKey,
  updateLocalReviewDecision,
  viewIdentityAuditSavedSubmission,
}: TrackIdentityAuditAmbiguousTabProps) {
    const familyOptions = identityAuditAmbiguous?.family_counts ?? [];
    const suggestedItems = identityAuditSuggestedGroups?.items ?? [];
    const filteredItems = computeAmbiguousTrackItems();
    const unifiedItems = computeUnifiedReviewItems();
    const visibleItems = filteredItems.slice(0, identityAuditAmbiguousVisibleCount);
    const focusedItem = identityAuditFocusedReviewKey == null
      ? null
      : (unifiedItems.find((item) => item.decision_key === identityAuditFocusedReviewKey) ?? null);
    const focusedDecision = focusedItem ? identityAuditLocalDecisions[focusedItem.decision_key] : undefined;
    const reviewedAmbiguousCount = filteredItems.reduce((count, item) => (
      isReviewedDecision(identityAuditLocalDecisions[trackDecisionKey(item)]) ? count + 1 : count
    ), 0);
    const reviewedSuggestedCount = suggestedItems.reduce((count, group) => {
      const decision = identityAuditLocalDecisions[groupDecisionKey(group)];
      return isReviewedDecision(decision) ? count + 1 : count;
    }, 0);
    const reviewedCount = reviewedAmbiguousCount + reviewedSuggestedCount;
    const totalReviewableCount = filteredItems.length + suggestedItems.length;
    const summaryByFamily = new Map<string, { total: number; approved: number; rejected: number; skipped: number; unreviewed: number }>();
    for (const item of unifiedItems) {
      const current = summaryByFamily.get(item.family_label) ?? { total: 0, approved: 0, rejected: 0, skipped: 0, unreviewed: 0 };
      current.total += 1;
      const verdict = identityAuditLocalDecisions[item.decision_key]?.verdict ?? "unsure";
      if (verdict === "good_to_group") {
        current.approved += 1;
      } else if (verdict === "not_good") {
        current.rejected += 1;
      } else if (verdict === "skipped") {
        current.skipped += 1;
      } else {
        current.unreviewed += 1;
      }
      summaryByFamily.set(item.family_label, current);
    }
    const summaryEntries = Array.from(summaryByFamily.entries())
      .sort((left, right) => right[1].total - left[1].total || left[0].localeCompare(right[0]));
    const visibleSummaryEntries = summaryEntries.slice(0, 8);
    const remainingSummaryCount = Math.max(0, summaryEntries.length - visibleSummaryEntries.length);

    const groupApproved: Array<Record<string, unknown>> = [];
    const groupRejected: Array<Record<string, unknown>> = [];
    const groupSkipped: Array<Record<string, unknown>> = [];
    const trackApproved: Array<Record<string, unknown>> = [];
    const trackRejected: Array<Record<string, unknown>> = [];
    const trackSkipped: Array<Record<string, unknown>> = [];

    for (const item of unifiedItems) {
      const decision = identityAuditLocalDecisions[item.decision_key];
      if (!decision || decision.verdict === "unsure") {
        continue;
      }
      if (item.item_type === "group") {
        const group = item.group;
        const label = group?.analysis_track_name || (group?.analysis_track_id != null ? `track_family ${group.analysis_track_id}` : item.decision_key);
        const entry = {
          decision_key: item.decision_key,
          id: group?.analysis_track_id ?? item.decision_key,
          decision: decision.verdict,
          label,
          family: group?.song_family_key ?? item.family_label,
          bucket: item.bucket_label,
          would: decision.verdict === "good_to_group"
            ? `Would group as composition family: ${label}`
            : decision.verdict === "not_good"
              ? `Would keep suggested group separate: ${label}`
              : `Would defer suggested group: ${label}`,
          source: group
            ? {
                analysis_track_id: group.analysis_track_id,
                analysis_track_name: group.analysis_track_name,
                song_family_key: group.song_family_key,
                release_track_count: group.release_track_count,
                confidence: group.confidence,
                match_method: group.match_method,
              }
            : null,
        };
        if (decision.verdict === "good_to_group") {
          groupApproved.push(entry);
        } else if (decision.verdict === "not_good") {
          groupRejected.push(entry);
        } else {
          groupSkipped.push(entry);
        }
      } else {
        const track = item.track;
        const label = track?.release_track_name || (track?.release_track_id != null ? `release_track ${track.release_track_id}` : item.decision_key);
        const entry = {
          decision_key: item.decision_key,
          id: track?.release_track_id ?? item.decision_key,
          decision: decision.verdict,
          label,
          family: track?.dominant_family ?? item.family_label,
          bucket: track?.bucket ?? item.bucket_label,
          would: decision.verdict === "good_to_group"
            ? `Would accept track identity mapping: ${label}`
            : decision.verdict === "not_good"
              ? `Would reject track identity mapping: ${label}`
              : `Would defer track decision: ${label}`,
          source: track
            ? {
                release_track_id: track.release_track_id,
                release_track_name: track.release_track_name,
                artist_name: track.artist_name,
                analysis_name: track.analysis_name,
                bucket: track.bucket,
                dominant_family: track.dominant_family,
                review_families: track.review_families,
                confidence: track.confidence,
              }
            : null,
        };
        if (decision.verdict === "good_to_group") {
          trackApproved.push(entry);
        } else if (decision.verdict === "not_good") {
          trackRejected.push(entry);
        } else {
          trackSkipped.push(entry);
        }
      }
    }

    const totalLocalDecisions = (
      groupApproved.length
      + groupRejected.length
      + groupSkipped.length
      + trackApproved.length
      + trackRejected.length
      + trackSkipped.length
    );
    const previewPayload = {
      generated_at: new Date().toISOString(),
      summary: {
        total_local_decisions: totalLocalDecisions,
        groups: {
          approved: groupApproved.length,
          rejected: groupRejected.length,
          skipped: groupSkipped.length,
        },
        tracks: {
          approved: trackApproved.length,
          rejected: trackRejected.length,
          skipped: trackSkipped.length,
        },
      },
      decisions: {
        groups: {
          approved: groupApproved,
          rejected: groupRejected,
          skipped: groupSkipped,
        },
        tracks: {
          approved: trackApproved,
          rejected: trackRejected,
          skipped: trackSkipped,
        },
      },
    };
    const previewJson = JSON.stringify(previewPayload, null, 2);
    const canSaveSubmission = Boolean(
      totalLocalDecisions > 0
      && identityAuditPreviewValidationResult
      && !identityAuditPreviewValidationLoading,
    );

    const copyPreviewJson = async () => {
      if (!("clipboard" in navigator) || typeof navigator.clipboard?.writeText !== "function") {
        setIdentityAuditPreviewCopyStatus("Clipboard unavailable");
        return;
      }
      try {
        await navigator.clipboard.writeText(previewJson);
        setIdentityAuditPreviewCopyStatus("Copied JSON");
      } catch {
        setIdentityAuditPreviewCopyStatus("Copy failed");
      }
    };

    const downloadPreviewJson = () => {
      try {
        const blob = new Blob([previewJson], { type: "application/json;charset=utf-8" });
        const objectUrl = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = objectUrl;
        link.download = "identity-audit-submission-preview.json";
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(objectUrl);
      } catch {
        // Keep silent; this is a convenience path only.
      }
    };

    const validatePreviewJson = async () => {
      if (identityAuditPreviewValidationLoading) {
        return;
      }
      setIdentityAuditPreviewValidationLoading(true);
      setIdentityAuditPreviewValidationError("");
      try {
        const response = await fetch(
          `${apiBaseUrl}/debug/tracks/identity-audit/submission-preview/validate`,
          {
            method: "POST",
            credentials: "include",
            headers: { "Content-Type": "application/json" },
            body: previewJson,
          },
        );
        if (!response.ok) {
          let detail = "Failed to validate submission preview.";
          try {
            const payload = (await response.json()) as { detail?: string };
            if (payload.detail) {
              detail = payload.detail;
            }
          } catch {
            // keep fallback
          }
          throw new Error(detail);
        }
        const payload = (await response.json()) as SubmissionPreviewValidationResponse;
        setIdentityAuditPreviewValidationResult(payload);
        setIdentityAuditPreviewValidatedAt(Date.now());
      } catch (error) {
        setIdentityAuditPreviewValidationError(formatUiErrorMessage(error, "Failed to validate preview."));
        setIdentityAuditPreviewValidationResult(null);
        setIdentityAuditPreviewValidatedAt(null);
      } finally {
        setIdentityAuditPreviewValidationLoading(false);
      }
    };

    const saveSubmissionPreview = async () => {
      if (identityAuditSubmissionSaveLoading || !canSaveSubmission) {
        return;
      }
      setIdentityAuditSubmissionSaveLoading(true);
      setIdentityAuditSubmissionSaveError("");
      setIdentityAuditSubmissionSaveResult(null);
      try {
        const response = await fetch(
          `${apiBaseUrl}/debug/tracks/identity-audit/submissions`,
          {
            method: "POST",
            credentials: "include",
            headers: { "Content-Type": "application/json" },
            body: previewJson,
          },
        );
        if (!response.ok) {
          let detail = "Failed to save submission.";
          try {
            const payload = (await response.json()) as { detail?: string };
            if (payload.detail) {
              detail = payload.detail;
            }
          } catch {
            // keep fallback
          }
          throw new Error(detail);
        }
        const payload = (await response.json()) as IdentityAuditSubmissionSaveResponse;
        setIdentityAuditSubmissionSaveResult(payload);
        void loadIdentityAuditSavedSubmissions(true);
      } catch (error) {
        setIdentityAuditSubmissionSaveError(formatUiErrorMessage(error, "Failed to save submission."));
      } finally {
        setIdentityAuditSubmissionSaveLoading(false);
      }
    };

    const renderPreviewBucket = (title: string, entries: Array<Record<string, unknown>>) => (
      <div className="identity-audit-group" key={`preview-${title}`}>
        <div className="tracks-formula-heading">
          <h3>{title}</h3>
          <span>{entries.length}</span>
        </div>
        {entries.length === 0 ? (
          <p className="empty-copy">None</p>
        ) : (
          <div className="identity-audit-variant-list">
            {entries.map((entry, index) => (
              <div className="identity-audit-variant" key={`preview-entry-${title}-${String(entry.decision_key)}-${index}`}>
                <div className="identity-audit-variant-main">
                  <strong>{String(entry.label ?? entry.id ?? "Unknown item")}</strong>
                  <span>{String(entry.would ?? "")}</span>
                  <code>{String(entry.decision_key ?? "")}</code>
                </div>
                <div className="identity-audit-variant-stats">
                  {entry.family ? <span>{String(entry.family)}</span> : null}
                  {entry.bucket ? <span>{String(entry.bucket)}</span> : null}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    );

    const applyFocusedAction = (verdict: LocalReviewVerdict) => {
      if (!focusedItem) {
        return;
      }
      const nextDecisions = {
        ...identityAuditLocalDecisions,
        [focusedItem.decision_key]: {
          verdict,
          grouping_target: verdict === "good_to_group"
            ? (identityAuditLocalDecisions[focusedItem.decision_key]?.grouping_target ?? "same_composition")
            : null,
          note: identityAuditLocalDecisions[focusedItem.decision_key]?.note ?? "",
          updated_at_ms: Date.now(),
        },
      };
      updateLocalReviewDecision(focusedItem.decision_key, {
        verdict,
        grouping_target: verdict === "good_to_group"
          ? (identityAuditLocalDecisions[focusedItem.decision_key]?.grouping_target ?? "same_composition")
          : null,
      });
      setIdentityAuditFocusedReviewKey(findNextUnreviewedDecisionKey(unifiedItems, focusedItem.decision_key, nextDecisions));
    };

    return (
      <div className="identity-audit-grid">
        <div className="identity-audit-ambiguous-toolbar">
          <p className="identity-audit-tab-copy">
            Work one queue from candidate to decision, then validate and save. Saved submissions remain dry-run only unless a future apply path is added.
          </p>
          <div className="identity-audit-ambiguous-summary">
            <span className="identity-audit-pill">Local only (not saved)</span>
            <span className="identity-audit-pill">Reviewed {reviewedCount} / {totalReviewableCount}</span>
            <span className="identity-audit-pill">Shortcuts: A approve, R reject, S skip, N next</span>
            <button
              className="secondary-button"
              onClick={() => {
                setIdentityAuditLocalDecisions({});
                setIdentityAuditPreviewCopyStatus("");
                setIdentityAuditPreviewValidationLoading(false);
                setIdentityAuditPreviewValidationError("");
                setIdentityAuditPreviewValidationResult(null);
                setIdentityAuditPreviewValidatedAt(null);
                setIdentityAuditSubmissionSaveLoading(false);
                setIdentityAuditSubmissionSaveError("");
                setIdentityAuditSubmissionSaveResult(null);
              }}
              type="button"
            >
              Reset local decisions
            </button>
          </div>
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Workflow</h3>
            <span>{reviewedCount} / {totalReviewableCount} reviewed</span>
          </div>
          <div className="identity-audit-stats">
            <span className="identity-audit-stat"><span>1 Candidate</span><strong>{unifiedItems.length} queued</strong></span>
            <span className="identity-audit-stat"><span>2 Review</span><strong>{focusedItem ? "active" : "complete"}</strong></span>
            <span className="identity-audit-stat"><span>3 Decision</span><strong>{totalLocalDecisions} local</strong></span>
            <span className="identity-audit-stat"><span>4 Validate</span><strong>{identityAuditPreviewValidationResult ? "validated" : "not validated"}</strong></span>
            <span className="identity-audit-stat"><span>5 Save</span><strong>{identityAuditSubmissionSaveResult ? `#${identityAuditSubmissionSaveResult.submission_id}` : "not saved"}</strong></span>
          </div>
        </div>
        <div className="identity-audit-ambiguous-filters">
          <label>
            Family
            <select
              onChange={(event) => setIdentityAuditAmbiguousFamilyFilter(event.target.value)}
              value={identityAuditAmbiguousFamilyFilter}
            >
              <option value="all">All families</option>
              {familyOptions.map((family) => (
                <option key={`family-${family.family}`} value={family.family}>{family.family} ({family.count})</option>
              ))}
            </select>
          </label>
          <label>
            Bucket
            <select
              onChange={(event) => setIdentityAuditAmbiguousBucketFilter(event.target.value as "all" | "grouped" | "ungrouped")}
              value={identityAuditAmbiguousBucketFilter}
            >
              <option value="all">All</option>
              <option value="grouped">Grouped</option>
              <option value="ungrouped">Ungrouped</option>
            </select>
          </label>
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Candidate Summary</h3>
            <span>{summaryEntries.length} buckets</span>
          </div>
          {visibleSummaryEntries.length > 0 ? (
            <div className="identity-audit-stats">
              {visibleSummaryEntries.map(([label, counts]) => (
                <span className="identity-audit-stat" key={`summary-${label}`}>
                  <span>{label}</span>
                  <strong>
                    {counts.total} total | {counts.approved} approved | {counts.rejected} rejected | {counts.skipped} skipped | {counts.unreviewed} unreviewed
                  </strong>
                </span>
              ))}
              {remainingSummaryCount > 0 ? (
                <span className="identity-audit-stat"><span>More buckets</span><strong>+{remainingSummaryCount} more</strong></span>
              ) : null}
            </div>
          ) : (
            <p className="empty-copy">No review buckets available yet.</p>
          )}
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Review Active Candidate</h3>
            <span>{findNextUnreviewedDecisionKey(unifiedItems) ? "Ready" : "Complete"}</span>
          </div>
          {focusedItem ? (
            <article className="identity-audit-example">
              <div className="identity-audit-example-header">
                <div>
                  <h4>{focusedItem.title}</h4>
                  <p>{focusedItem.subtitle}</p>
                </div>
                <span className="identity-audit-type-badge">{focusedItem.item_type === "group" ? "Suggested match" : "Needs review"}</span>
              </div>
              <div className="identity-audit-stats">
                <span className="identity-audit-stat"><span>Issue group</span><strong>{focusedItem.bucket_label}</strong></span>
                <span className="identity-audit-stat"><span>Reason</span><strong>{focusedItem.family_label}</strong></span>
                <span className="identity-audit-stat"><span>Decision</span><strong>{focusedDecision?.verdict ?? "unreviewed"}</strong></span>
              </div>
              <div className="identity-audit-ambiguous-summary">
                <button className="secondary-button" onClick={() => applyFocusedAction("good_to_group")} type="button">Approve</button>
                <button className="secondary-button" onClick={() => applyFocusedAction("not_good")} type="button">Reject</button>
                <button className="secondary-button" onClick={() => applyFocusedAction("skipped")} type="button">Skip</button>
                <button
                  className="secondary-button"
                  onClick={() => setIdentityAuditFocusedReviewKey(findNextUnreviewedDecisionKey(unifiedItems, focusedItem.decision_key))}
                  type="button"
                >
                  Next unreviewed
                </button>
              </div>
            </article>
          ) : (
            <p className="empty-copy">All items reviewed locally.</p>
          )}
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Validate and Save Decisions</h3>
            <span>{totalLocalDecisions} decisions</span>
          </div>
          <div className="identity-audit-ambiguous-summary">
            <span className="identity-audit-pill">Groups: {groupApproved.length} approved, {groupRejected.length} rejected, {groupSkipped.length} skipped</span>
            <span className="identity-audit-pill">Tracks: {trackApproved.length} approved, {trackRejected.length} rejected, {trackSkipped.length} skipped</span>
            <button className="secondary-button" onClick={() => void copyPreviewJson()} type="button">Copy JSON</button>
            <button className="secondary-button" onClick={downloadPreviewJson} type="button">Download JSON</button>
            <button
              className="secondary-button"
              disabled={identityAuditPreviewValidationLoading}
              onClick={() => void validatePreviewJson()}
              type="button"
            >
              {identityAuditPreviewValidationLoading
                ? "Validating..."
                : identityAuditPreviewValidationResult
                  ? "Revalidate Preview"
                  : "Validate Preview"}
            </button>
            <button
              className="secondary-button"
              disabled={!canSaveSubmission || identityAuditSubmissionSaveLoading}
              onClick={() => void saveSubmissionPreview()}
              type="button"
            >
              {identityAuditSubmissionSaveLoading ? "Saving..." : "Save Submission"}
            </button>
            {identityAuditPreviewCopyStatus ? <span className="identity-audit-pill">{identityAuditPreviewCopyStatus}</span> : null}
          </div>
          <p className="empty-copy">Saved only. No changes applied.</p>
          {identityAuditPreviewValidationResult
            && (identityAuditPreviewValidationResult.summary.warnings > 0
              || identityAuditPreviewValidationResult.summary.unknown_groups > 0
              || identityAuditPreviewValidationResult.summary.unknown_tracks > 0) ? (
            <p className="empty-copy">Validation has warnings; saved record will include them.</p>
            ) : null}
          {identityAuditPreviewValidationError ? <p className="empty-copy">{identityAuditPreviewValidationError}</p> : null}
          {identityAuditSubmissionSaveError ? <p className="empty-copy">{identityAuditSubmissionSaveError}</p> : null}
          {identityAuditSubmissionSaveResult ? (
            <div className="identity-audit-group">
              <div className="tracks-formula-heading">
                <h3>Saved Submission</h3>
                <span>#{identityAuditSubmissionSaveResult.submission_id}</span>
              </div>
              <p className="empty-copy">
                Saved submission #{identityAuditSubmissionSaveResult.submission_id}
                {" "}
                ({identityAuditSubmissionSaveResult.status}) at {new Date(identityAuditSubmissionSaveResult.created_at).toLocaleString()}.
              </p>
              <div className="identity-audit-stats">
                <span className="identity-audit-stat"><span>Warnings</span><strong>{identityAuditSubmissionSaveResult.warnings.length}</strong></span>
                <span className="identity-audit-stat"><span>Unknown groups</span><strong>{identityAuditSubmissionSaveResult.unknown_items.groups.length}</strong></span>
                <span className="identity-audit-stat"><span>Unknown tracks</span><strong>{identityAuditSubmissionSaveResult.unknown_items.tracks.length}</strong></span>
              </div>
            </div>
          ) : null}
          {identityAuditPreviewValidationResult ? (
            <div className="identity-audit-group">
              <div className="tracks-formula-heading">
                <h3>Validation Result</h3>
                <span>{identityAuditPreviewValidationResult.ok ? "ok" : "failed"}</span>
              </div>
              {identityAuditPreviewValidatedAt ? (
                <p className="empty-copy">Validated at {new Date(identityAuditPreviewValidatedAt).toLocaleTimeString()}</p>
              ) : null}
              <div className="identity-audit-stats">
                <span className="identity-audit-stat"><span>Total</span><strong>{identityAuditPreviewValidationResult.summary.total_decisions}</strong></span>
                <span className="identity-audit-stat"><span>Groups</span><strong>{identityAuditPreviewValidationResult.summary.group_decisions}</strong></span>
                <span className="identity-audit-stat"><span>Tracks</span><strong>{identityAuditPreviewValidationResult.summary.track_decisions}</strong></span>
                <span className="identity-audit-stat"><span>Warnings</span><strong>{identityAuditPreviewValidationResult.summary.warnings}</strong></span>
                <span className="identity-audit-stat"><span>Unknown groups</span><strong>{identityAuditPreviewValidationResult.summary.unknown_groups}</strong></span>
                <span className="identity-audit-stat"><span>Unknown tracks</span><strong>{identityAuditPreviewValidationResult.summary.unknown_tracks}</strong></span>
              </div>
              {identityAuditPreviewValidationResult.summary.total_decisions === 0 ? (
                <p className="empty-copy">No decisions to validate.</p>
              ) : null}
              {identityAuditPreviewValidationResult.warnings.length > 0 ? (
                <div className="identity-audit-variant-list">
                  {identityAuditPreviewValidationResult.warnings.map((warning, index) => (
                    <div className="identity-audit-variant" key={`validation-warning-${index}`}>
                      <div className="identity-audit-variant-main">
                        <strong>Warning</strong>
                        <span>{warning}</span>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="empty-copy">No validation warnings.</p>
              )}
              <div className="identity-audit-group">
                <div className="tracks-formula-heading">
                  <h3>Unknown Groups</h3>
                  <span>{identityAuditPreviewValidationResult.unknown_items.groups.length}</span>
                </div>
                {identityAuditPreviewValidationResult.unknown_items.groups.length > 0 ? (
                  <div className="identity-audit-variant-list">
                    {identityAuditPreviewValidationResult.unknown_items.groups.map((item, index) => (
                      <div className="identity-audit-variant" key={`unknown-group-${index}`}>
                        <div className="identity-audit-variant-main">
                          <strong>{String(item.label ?? item.id ?? "Unknown group")}</strong>
                          <code>{String(item.decision_key ?? "")}</code>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="empty-copy">None.</p>
                )}
              </div>
              <div className="identity-audit-group">
                <div className="tracks-formula-heading">
                  <h3>Unknown Tracks</h3>
                  <span>{identityAuditPreviewValidationResult.unknown_items.tracks.length}</span>
                </div>
                {identityAuditPreviewValidationResult.unknown_items.tracks.length > 0 ? (
                  <div className="identity-audit-variant-list">
                    {identityAuditPreviewValidationResult.unknown_items.tracks.map((item, index) => (
                      <div className="identity-audit-variant" key={`unknown-track-${index}`}>
                        <div className="identity-audit-variant-main">
                          <strong>{String(item.label ?? item.id ?? "Unknown track")}</strong>
                          <code>{String(item.decision_key ?? "")}</code>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="empty-copy">None.</p>
                )}
              </div>
            </div>
          ) : null}
          {totalLocalDecisions === 0 ? (
            <p className="empty-copy">No local decisions yet.</p>
          ) : (
            <div className="identity-audit-grid">
              <div className="identity-audit-group">
                <div className="tracks-formula-heading">
                  <h3>Group Decisions</h3>
                  <span>{groupApproved.length + groupRejected.length + groupSkipped.length}</span>
                </div>
                {renderPreviewBucket("Approved", groupApproved)}
                {renderPreviewBucket("Rejected", groupRejected)}
                {renderPreviewBucket("Skipped", groupSkipped)}
              </div>
              <div className="identity-audit-group">
                <div className="tracks-formula-heading">
                  <h3>Track Decisions</h3>
                  <span>{trackApproved.length + trackRejected.length + trackSkipped.length}</span>
                </div>
                {renderPreviewBucket("Approved", trackApproved)}
                {renderPreviewBucket("Rejected", trackRejected)}
                {renderPreviewBucket("Skipped", trackSkipped)}
              </div>
            </div>
          )}
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Saved Decision Sets</h3>
            <span>{identityAuditSavedSubmissions?.total ?? 0}</span>
          </div>
          <div className="identity-audit-ambiguous-summary">
            <button
              className="secondary-button"
              disabled={identityAuditSavedSubmissionsLoading}
              onClick={() => void loadIdentityAuditSavedSubmissions(true)}
              type="button"
            >
              {identityAuditSavedSubmissionsLoading ? "Refreshing..." : "Refresh saved submissions"}
            </button>
          </div>
          {identityAuditSavedSubmissionsError ? <p className="empty-copy">{identityAuditSavedSubmissionsError}</p> : null}
          {!identityAuditSavedSubmissions && !identityAuditSavedSubmissionsError ? (
            <p className="empty-copy">{identityAuditSavedSubmissionsLoading ? "Loading saved submissions..." : "Saved submissions are not loaded yet."}</p>
          ) : null}
          {identityAuditSavedSubmissions && identityAuditSavedSubmissions.items.length === 0 ? (
            <p className="empty-copy">No saved submissions yet.</p>
          ) : null}
          {identityAuditSavedSubmissions && identityAuditSavedSubmissions.items.length > 0 ? (
            <div className="identity-audit-variant-list">
              {identityAuditSavedSubmissions.items.map((item) => (
                <div className="identity-audit-variant" key={`saved-submission-${item.id}`}>
                  <div className="identity-audit-variant-main">
                    <strong>#{item.id} • {item.status}</strong>
                    <span>{new Date(item.created_at).toLocaleString()}</span>
                    <span>
                      {Number(item.summary.total_decisions ?? 0)} decisions • {item.warnings_count} warnings • {item.unknown_groups} unknown groups • {item.unknown_tracks} unknown tracks
                    </span>
                  </div>
                  <div className="identity-audit-variant-stats">
                    <button className="secondary-button" onClick={() => void viewIdentityAuditSavedSubmission(item.id)} type="button">View</button>
                  </div>
                </div>
              ))}
            </div>
          ) : null}
          {identityAuditSavedSubmissionDetailError ? <p className="empty-copy">{identityAuditSavedSubmissionDetailError}</p> : null}
          {identityAuditSavedSubmissionDetailLoading ? <p className="empty-copy">Loading saved submission...</p> : null}
          {identityAuditSavedSubmissionDetail ? (
            <div>
              <div className="tracks-formula-heading">
                <h3>Saved Submission Detail</h3>
                <span>#{identityAuditSavedSubmissionDetail.item.id}</span>
              </div>
              <div className="identity-audit-ambiguous-summary">
                <button
                  className="secondary-button"
                  disabled={identityAuditSavedSubmissionDryRunLoading}
                  onClick={() => void dryRunIdentityAuditSavedSubmission(identityAuditSavedSubmissionDetail.item.id)}
                  type="button"
                >
                  {identityAuditSavedSubmissionDryRunLoading
                    ? "Running dry run..."
                    : identityAuditSavedSubmissionDryRun
                      ? "Re-run Dry Run"
                      : "Dry Run"}
                </button>
              </div>
              <p className="empty-copy">Dry run only. No changes applied.</p>
              <div className="identity-audit-stats">
                <span className="identity-audit-stat"><span>Status</span><strong>{identityAuditSavedSubmissionDetail.item.status}</strong></span>
                <span className="identity-audit-stat"><span>Created</span><strong>{new Date(identityAuditSavedSubmissionDetail.item.created_at).toLocaleString()}</strong></span>
                <span className="identity-audit-stat"><span>Total</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.total_decisions}</strong></span>
                <span className="identity-audit-stat"><span>Groups</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.group_decisions}</strong></span>
                <span className="identity-audit-stat"><span>Tracks</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.track_decisions}</strong></span>
              </div>
              <div className="identity-audit-stats">
                <span className="identity-audit-stat"><span>Approved</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.approved}</strong></span>
                <span className="identity-audit-stat"><span>Rejected</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.rejected}</strong></span>
                <span className="identity-audit-stat"><span>Skipped</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.skipped}</strong></span>
                <span className="identity-audit-stat"><span>Warnings</span><strong>{identityAuditSavedSubmissionDetail.item.validation.summary.warnings}</strong></span>
              </div>
              {identityAuditSavedSubmissionDryRunError ? <p className="empty-copy">{identityAuditSavedSubmissionDryRunError}</p> : null}
              {identityAuditSavedSubmissionDryRun ? (
                <div className="identity-audit-group">
                  <div className="tracks-formula-heading">
                    <h3>Dry Run Result</h3>
                    <span>#{identityAuditSavedSubmissionDryRun.submission_id} • {identityAuditSavedSubmissionDryRun.status}</span>
                  </div>
                  {identityAuditSavedSubmissionDryRunAt ? (
                    <p className="empty-copy">Dry run at {new Date(identityAuditSavedSubmissionDryRunAt).toLocaleTimeString()}</p>
                  ) : null}
                  <div className="identity-audit-stats">
                    <span className="identity-audit-stat"><span>Would apply</span><strong>{identityAuditSavedSubmissionDryRun.summary.would_apply}</strong></span>
                    <span className="identity-audit-stat"><span>Approved groups</span><strong>{identityAuditSavedSubmissionDryRun.summary.approved_groups}</strong></span>
                    <span className="identity-audit-stat"><span>Approved tracks</span><strong>{identityAuditSavedSubmissionDryRun.summary.approved_tracks}</strong></span>
                    <span className="identity-audit-stat"><span>Rejected no-ops</span><strong>{identityAuditSavedSubmissionDryRun.summary.rejected}</strong></span>
                    <span className="identity-audit-stat"><span>Skipped no-ops</span><strong>{identityAuditSavedSubmissionDryRun.summary.skipped}</strong></span>
                  </div>
                  <div className="identity-audit-stats">
                    <span className="identity-audit-stat"><span>Warnings</span><strong>{identityAuditSavedSubmissionDryRun.summary.warnings}</strong></span>
                    <span className="identity-audit-stat"><span>Unknown groups</span><strong>{identityAuditSavedSubmissionDryRun.summary.unknown_groups}</strong></span>
                    <span className="identity-audit-stat"><span>Unknown tracks</span><strong>{identityAuditSavedSubmissionDryRun.summary.unknown_tracks}</strong></span>
                  </div>
                  {identityAuditSavedSubmissionDryRun.warnings.length > 0 ? (
                    <div className="identity-audit-variant-list">
                      {identityAuditSavedSubmissionDryRun.warnings.map((warning, index) => (
                        <div className="identity-audit-variant" key={`dry-run-warning-${index}`}>
                          <div className="identity-audit-variant-main">
                            <strong>Warning</strong>
                            <span>{warning}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : null}
                  <div className="identity-audit-group">
                    <div className="tracks-formula-heading">
                      <h3>Plan</h3>
                      <span>{identityAuditSavedSubmissionDryRun.plan.groups.length + identityAuditSavedSubmissionDryRun.plan.tracks.length} items</span>
                    </div>
                    {identityAuditSavedSubmissionDryRun.plan.groups.length === 0 && identityAuditSavedSubmissionDryRun.plan.tracks.length === 0 ? (
                      <p className="empty-copy">No plan items.</p>
                    ) : (
                      <div className="identity-audit-variant-list">
                        {identityAuditSavedSubmissionDryRun.plan.groups.map((item, index) => (
                          <div className="identity-audit-variant" key={`dry-run-group-${index}`}>
                            <div className="identity-audit-variant-main">
                              <strong>{String(item.label ?? item.id ?? "Group item")}</strong>
                              <span>{String(item.action ?? "would_accept_group")}</span>
                              <code>{String(item.decision_key ?? "")}</code>
                            </div>
                          </div>
                        ))}
                        {identityAuditSavedSubmissionDryRun.plan.tracks.map((item, index) => (
                          <div className="identity-audit-variant" key={`dry-run-track-${index}`}>
                            <div className="identity-audit-variant-main">
                              <strong>{String(item.label ?? item.id ?? "Track item")}</strong>
                              <span>{String(item.action ?? "would_accept_track_mapping")}</span>
                              <code>{String(item.decision_key ?? "")}</code>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Candidate Source: Suggested Matches</h3>
            <span>{suggestedItems.length} groups</span>
          </div>
          {identityAuditSuggestedError ? <p className="empty-copy">{identityAuditSuggestedError}</p> : null}
          {!identityAuditSuggestedGroups && !identityAuditSuggestedError ? (
            <p className="empty-copy">{identityAuditSuggestedLoading ? "Loading suggested groups..." : "Suggested groups are not loaded yet."}</p>
          ) : null}
          {suggestedItems.length > 0 ? (
            <div className="identity-audit-examples">
              {suggestedItems.map((group) => {
                const decisionKey = groupDecisionKey(group);
                const decision = identityAuditLocalDecisions[decisionKey] ?? {
                  verdict: "unsure" as LocalReviewVerdict,
                  grouping_target: null,
                  note: "",
                  updated_at_ms: 0,
                };
                return (
                  <article className="identity-audit-example" key={`suggested-${group.analysis_track_id}`}>
                    <div className="identity-audit-example-header">
                      <div>
                        <h4>{group.analysis_track_name || `Track Family ${group.analysis_track_id}`}</h4>
                        <p>{group.match_method || "suggested"} | {Math.round(group.confidence * 100)}% confidence</p>
                      </div>
                      <span className="identity-audit-type-badge">Suggested match</span>
                    </div>
                    <div className="identity-audit-stats">
                      <span className="identity-audit-stat"><span>Release tracks</span><strong>{group.release_track_count}</strong></span>
                      {group.song_family_key ? <span className="identity-audit-stat"><span>Family key</span><strong>{group.song_family_key}</strong></span> : null}
                    </div>
                    <div className="identity-audit-review-controls">
                      <label>
                        Decision
                        <select
                          onChange={(event) => {
                            const nextVerdict = event.target.value as LocalReviewVerdict;
                            updateLocalReviewDecision(decisionKey, {
                              verdict: nextVerdict,
                              grouping_target: nextVerdict === "good_to_group" ? (decision.grouping_target ?? "same_composition") : null,
                            });
                          }}
                          value={decision.verdict}
                        >
                          <option value="unsure">Unreviewed</option>
                          <option value="good_to_group">Good to group</option>
                          <option value="not_good">Not good</option>
                          <option value="skipped">Skipped</option>
                        </select>
                      </label>
                      <label>
                        Grouping target
                        <select
                          disabled={decision.verdict !== "good_to_group"}
                          onChange={(event) =>
                            updateLocalReviewDecision(decisionKey, {
                              grouping_target: event.target.value as Exclude<LocalGroupingTarget, null>,
                            })}
                          value={decision.grouping_target ?? "same_composition"}
                        >
                          <option value="same_composition">Group as same work</option>
                          <option value="same_release_track_only">Keep as release-only match</option>
                        </select>
                      </label>
                    </div>
                    <label className="identity-audit-review-note">
                      Note
                      <textarea
                        onChange={(event) => updateLocalReviewDecision(decisionKey, { note: event.target.value })}
                        placeholder="Optional review context"
                        rows={2}
                        value={decision.note}
                      />
                    </label>
                    <div className="identity-audit-variant-list">
                      {group.release_tracks.map((releaseTrack) => (
                        <div className="identity-audit-variant" key={`group-${group.analysis_track_id}-${releaseTrack.release_track_id}`}>
                          <div className="identity-audit-variant-main">
                            <strong>{releaseTrack.release_track_name}</strong>
                            <span>{releaseTrack.primary_artists || "Unknown artists"}</span>
                            <code>release {releaseTrack.release_track_id}</code>
                          </div>
                          <div className="identity-audit-variant-stats">
                            {releaseTrack.album_names ? <span>{releaseTrack.album_names}</span> : null}
                          </div>
                        </div>
                      ))}
                    </div>
                  </article>
                );
              })}
            </div>
          ) : identityAuditSuggestedGroups ? (
            <p className="empty-copy">No suggested groups returned.</p>
          ) : null}
        </div>
        <div className="identity-audit-group">
          <div className="tracks-formula-heading">
            <h3>Candidate Source: Needs Review</h3>
            <span>{filteredItems.length} rows</span>
          </div>
          {identityAuditAmbiguousError ? <p className="empty-copy">{identityAuditAmbiguousError}</p> : null}
          {!identityAuditAmbiguous && !identityAuditAmbiguousError ? (
            <p className="empty-copy">{identityAuditAmbiguousLoading ? "Loading ambiguous queue..." : "Ambiguous queue is not loaded yet."}</p>
          ) : null}
          {identityAuditAmbiguous?.parse_warning ? (
            <p className="empty-copy">Parser warning: {identityAuditAmbiguous.parse_warning}</p>
          ) : null}
          {visibleItems.length > 0 ? (
            <div className="identity-audit-examples">
              {visibleItems.map((item) => {
                const decision = identityAuditLocalDecisions[trackDecisionKey(item)] ?? {
                  verdict: "unsure" as LocalReviewVerdict,
                  grouping_target: null,
                  note: "",
                  updated_at_ms: 0,
                };
                return (
                  <article className="identity-audit-example" key={`ambiguous-${item.entry_id}`}>
                  <div className="identity-audit-example-header">
                    <div>
                      <h4>{item.release_track_name}</h4>
                      <p>{item.artist_name} | {item.bucket} | {item.analysis_name ?? "no analysis mapping"}</p>
                    </div>
                    <span className="identity-audit-type-badge">{item.dominant_family ?? "ambiguous"}</span>
                  </div>
                  <div className="identity-audit-stats">
                    <span className="identity-audit-stat"><span>release</span><strong>{item.release_track_id}</strong></span>
                    {item.confidence != null ? <span className="identity-audit-stat"><span>confidence</span><strong>{Math.round(item.confidence * 100)}%</strong></span> : null}
                    {item.song_family_key ? <span className="identity-audit-stat"><span>family key</span><strong>{item.song_family_key}</strong></span> : null}
                    {item.review_families.map((family) => (
                      <span className="identity-audit-stat" key={`${item.entry_id}-${family}`}><span>rule</span><strong>{family}</strong></span>
                    ))}
                  </div>
                  <div className="identity-audit-review-controls">
                    <label>
                      Decision
                      <select
                        onChange={(event) => {
                          const nextVerdict = event.target.value as LocalReviewVerdict;
                          updateLocalReviewDecision(trackDecisionKey(item), {
                            verdict: nextVerdict,
                            grouping_target: nextVerdict === "good_to_group" ? (decision.grouping_target ?? "same_composition") : null,
                          });
                        }}
                        value={decision.verdict}
                      >
                        <option value="unsure">Unreviewed</option>
                        <option value="good_to_group">Good to group</option>
                        <option value="not_good">Not good</option>
                        <option value="skipped">Skipped</option>
                      </select>
                    </label>
                    <label>
                      Grouping target
                      <select
                        disabled={decision.verdict !== "good_to_group"}
                        onChange={(event) =>
                          updateLocalReviewDecision(trackDecisionKey(item), {
                            grouping_target: event.target.value as Exclude<LocalGroupingTarget, null>,
                          })}
                        value={decision.grouping_target ?? "same_composition"}
                      >
                        <option value="same_composition">Group as same work</option>
                        <option value="same_release_track_only">Keep as release-only match</option>
                      </select>
                    </label>
                  </div>
                  <label className="identity-audit-review-note">
                    Note
                    <textarea
                      onChange={(event) => updateLocalReviewDecision(trackDecisionKey(item), { note: event.target.value })}
                      placeholder="Optional review context"
                      rows={2}
                      value={decision.note}
                    />
                  </label>
                </article>
                );
              })}
            </div>
          ) : identityAuditAmbiguous ? (
            <p className="empty-copy">No ambiguous rows match the current filters.</p>
          ) : null}
          {filteredItems.length > visibleItems.length ? (
            <div className="identity-audit-load-more-row">
              <button
                className="secondary-button"
                onClick={() => setIdentityAuditAmbiguousVisibleCount((current) => current + IDENTITY_AUDIT_AMBIGUOUS_VISIBLE_STEP)}
                type="button"
              >
                Show more ({filteredItems.length - visibleItems.length} remaining)
              </button>
            </div>
          ) : null}
        </div>
      </div>
    );
}
