import { useEffect, useMemo, useState } from "react";

import {
  fetchRecordingTrackCandidates,
  fetchRecordingTrackCandidateReviews,
  fetchRecordingTrackCandidatesSummary,
  saveRecordingTrackCandidateReview,
} from "../../api/appApi";
import type {
  RecordingTrackCandidateFilters,
  RecordingTrackCandidateItem,
  RecordingTrackCandidateMember,
  RecordingTrackCandidateType,
  RecordingTrackCandidateReviewItem,
  RecordingTrackReviewDecision,
  RecordingTrackCandidatesResponse,
  RecordingTrackCandidatesSummary,
  RecordingTrackSafetyStatus,
} from "../../types/appTypes";
import { formatDurationMs, formatUiErrorMessage } from "../../utils/dashboardUtils";

type SafetyFilter = "all" | RecordingTrackSafetyStatus;
type CandidateTypeFilter = "all" | RecordingTrackCandidateType;
type EvidenceBucketFilter = "all"
  | "same_isrc"
  | "conflicting_isrc_but_compatible_metadata"
  | "missing_isrc_but_compatible_metadata"
  | "partial_isrc_match"
  | "variant_flag_excluded"
  | "metadata_review_required";
type IsrcFilter = "all" | "same" | "partial" | "missing" | "conflicting";
type RecordingTrackSort = "confidence_desc" | "confidence_asc" | "member_count_desc" | "member_count_asc";
type ReviewFilter = "all" | "unreviewed" | "reviewed" | RecordingTrackReviewDecision;
type ReviewDraft = {
  decision: RecordingTrackReviewDecision;
  reviewer_note: string;
  preferred_representative_release_track_id: string;
};
type RecordingTrackCandidatesTabProps = {
  onOpenReleaseTrack?: (member: RecordingTrackCandidateMember) => void;
};

const RELATIONSHIP_KIND_OPTIONS = [
  "same_isrc",
  "remaster",
  "rerelease",
  "single_release",
  "compilation_appearance",
  "soundtrack_appearance",
  "near_match",
  "radio_edit",
  "live",
  "demo",
  "acoustic",
  "remix",
  "rerecording",
  "alternate_take",
];

const EVIDENCE_BUCKET_OPTIONS: EvidenceBucketFilter[] = [
  "same_isrc",
  "conflicting_isrc_but_compatible_metadata",
  "missing_isrc_but_compatible_metadata",
  "partial_isrc_match",
  "variant_flag_excluded",
  "metadata_review_required",
];

const REVIEW_DECISION_OPTIONS: Array<{ value: RecordingTrackReviewDecision; label: string }> = [
  { value: "accepted", label: "Accept" },
  { value: "rejected", label: "Reject" },
  { value: "unsure", label: "Unsure" },
  { value: "needs_more_metadata", label: "Needs metadata" },
  { value: "wrong_representative", label: "Wrong representative" },
  { value: "maybe_split", label: "Maybe split" },
  { value: "maybe_merge_more", label: "Maybe merge more" },
];

function displayToken(value: string | number | null | undefined, fallback: string = "n/a") {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value).replace(/_/g, " ");
}

function statValue(summary: RecordingTrackCandidatesSummary | null, path: "recording" | "family" | "safe" | "review" | "sameIsrc" | "conflictingIsrc" | "missingCompatible" | "partialIsrc" | "variantExcluded") {
  if (!summary) {
    return "not loaded";
  }
  if (path === "recording") {
    return summary.count_by_candidate_type.recording_track_candidate ?? 0;
  }
  if (path === "family") {
    return summary.count_by_candidate_type.track_family_candidate ?? 0;
  }
  if (path === "safe") {
    return summary.count_by_safety_status.safe_candidate ?? 0;
  }
  if (path === "review") {
    return summary.count_by_safety_status.needs_review ?? 0;
  }
  if (path === "sameIsrc") {
    return summary.count_by_evidence_bucket?.same_isrc ?? summary.count_with_same_isrc_evidence ?? 0;
  }
  if (path === "conflictingIsrc") {
    return summary.count_by_evidence_bucket?.conflicting_isrc_but_compatible_metadata ?? 0;
  }
  if (path === "missingCompatible") {
    return summary.count_by_evidence_bucket?.missing_isrc_but_compatible_metadata ?? 0;
  }
  if (path === "partialIsrc") {
    return summary.count_by_evidence_bucket?.partial_isrc_match ?? 0;
  }
  return summary.count_by_evidence_bucket?.variant_flag_excluded ?? 0;
}

function isSameIsrcCandidate(item: RecordingTrackCandidateItem) {
  const values = new Set(
    item.members.flatMap((member) => member.isrc_values ?? (member.isrc ? [member.isrc] : [])),
  );
  return item.members.length > 0
    && item.members.every((member) => (member.isrc_values?.length ?? (member.isrc ? 1 : 0)) > 0)
    && values.size === 1;
}

function isPartialIsrcCandidate(item: RecordingTrackCandidateItem) {
  const membersWithIsrc = item.members.filter((member) => (member.isrc_values?.length ?? (member.isrc ? 1 : 0)) > 0).length;
  return membersWithIsrc > 0 && membersWithIsrc < item.members.length;
}

function isMissingIsrcCandidate(item: RecordingTrackCandidateItem) {
  return item.members.every((member) => (member.isrc_values?.length ?? (member.isrc ? 1 : 0)) === 0);
}

function isConflictingIsrcCandidate(item: RecordingTrackCandidateItem) {
  const values = new Set(
    item.members.flatMap((member) => member.isrc_values ?? (member.isrc ? [member.isrc] : [])),
  );
  return values.size > 1;
}

function itemMatchesIsrcFilter(item: RecordingTrackCandidateItem, filter: IsrcFilter) {
  if (filter === "all") {
    return true;
  }
  if (filter === "same") {
    return isSameIsrcCandidate(item);
  }
  if (filter === "partial") {
    return isPartialIsrcCandidate(item);
  }
  if (filter === "missing") {
    return isMissingIsrcCandidate(item);
  }
  return isConflictingIsrcCandidate(item);
}

function riskFlags(item: RecordingTrackCandidateItem) {
  const flags: string[] = [];
  const reviewText = item.why_review.join(" ").toLowerCase();
  if (item.candidate_type === "track_family_candidate" || item.evidence_bucket === "variant_flag_excluded") {
    flags.push("Variant flag excluded");
  }
  if (reviewText.includes("multiple isrc") || item.evidence_bucket === "conflicting_isrc_but_compatible_metadata" || isConflictingIsrcCandidate(item)) {
    flags.push("Conflicting ISRC");
  }
  if (reviewText.includes("partial isrc") || item.evidence_bucket === "partial_isrc_match") {
    flags.push("Partial ISRC");
  }
  if (reviewText.includes("missing isrc") || item.evidence_bucket === "missing_isrc_but_compatible_metadata") {
    flags.push("Missing ISRC");
  }
  if (reviewText.includes("duration delta") || item.members.some((member) => Number(member.evidence.duration_delta_ms ?? 0) > 2000)) {
    flags.push("Duration differs");
  }
  if (reviewText.includes("title")) {
    flags.push("Title mismatch");
  }
  if (reviewText.includes("artist")) {
    flags.push("Artist mismatch");
  }
  if (reviewText.includes("many albums")) {
    flags.push("Many albums");
  }
  return Array.from(new Set(flags));
}

function memberIsrcLabel(member: RecordingTrackCandidateMember) {
  const values = member.isrc_values?.length ? member.isrc_values : member.isrc ? [member.isrc] : [];
  return values.length > 0 ? values.join(", ") : "Missing ISRC";
}

function memberDurationLabel(member: RecordingTrackCandidateMember) {
  const durations = member.duration_values_ms?.length ? member.duration_values_ms : member.duration_ms ? [member.duration_ms] : [];
  if (durations.length === 0) {
    return "No duration";
  }
  return durations.map((duration) => formatDurationMs(duration)).join(", ");
}

function memberReleaseYear(member: RecordingTrackCandidateMember) {
  const rawDate = member.album_release_dates?.find((value) => /^\d{4}/.test(String(value ?? "")));
  return rawDate ? String(rawDate).slice(0, 4) : null;
}

function renderEvidencePills(item: RecordingTrackCandidateItem) {
  const hasDurationDelta = item.members.some((member) => {
    const delta = member.evidence.duration_delta_ms;
    return typeof delta === "number" && delta > 0;
  });
  const versionTokens = new Set(item.members.flatMap((member) => member.evidence.version_tokens ?? []));
  return (
    <div className="identity-audit-stats">
      {isSameIsrcCandidate(item) ? <span className="identity-audit-stat recording-track-good"><span>Same ISRC</span><strong>yes</strong></span> : null}
      {isPartialIsrcCandidate(item) ? <span className="identity-audit-stat recording-track-warn"><span>Partial ISRC</span><strong>check</strong></span> : null}
      {isConflictingIsrcCandidate(item) ? <span className="identity-audit-stat recording-track-warn"><span>Conflicting ISRC</span><strong>check</strong></span> : null}
      {isMissingIsrcCandidate(item) ? <span className="identity-audit-stat recording-track-muted"><span>Missing ISRC</span><strong>yes</strong></span> : null}
      {hasDurationDelta ? <span className="identity-audit-stat recording-track-warn"><span>Duration differs</span><strong>yes</strong></span> : null}
      {versionTokens.size > 0 ? <span className="identity-audit-stat"><span>Version token</span><strong>{Array.from(versionTokens).slice(0, 3).join(", ")}</strong></span> : null}
      {item.safety_status === "needs_review" ? <span className="identity-audit-stat recording-track-warn"><span>Review required</span><strong>yes</strong></span> : null}
    </div>
  );
}

function renderMember(member: RecordingTrackCandidateMember, onOpenReleaseTrack?: (member: RecordingTrackCandidateMember) => void) {
  const buttonDisabled = !onOpenReleaseTrack;
  return (
    <button
      className="identity-audit-variant recording-track-member recording-track-member-compact recording-track-variation-button"
      disabled={buttonDisabled}
      key={`recording-member-${member.release_track_id}`}
      onClick={() => onOpenReleaseTrack?.(member)}
      type="button"
    >
      <span className="identity-audit-variant-main">
        <strong>{member.title || `Release track ${member.release_track_id}`}</strong>
        <span>{member.artist || "Unknown artist"}</span>
        <span>{[member.album || "Unknown album", memberReleaseYear(member)].filter(Boolean).join(" · ")}</span>
      </span>
      <span className="identity-audit-variant-stats">
        <span>release {member.release_track_id}</span>
        <span>{memberDurationLabel(member)}</span>
        <span>{displayToken(member.evidence.album_context, "context unknown")}</span>
        {member.evidence.version_tokens?.map((token) => <span key={`${member.release_track_id}-${token}`}>{token}</span>)}
        <span>{memberIsrcLabel(member)}</span>
      </span>
    </button>
  );
}

export function RecordingTrackCandidatesTab({ onOpenReleaseTrack }: RecordingTrackCandidatesTabProps) {
  const [summary, setSummary] = useState<RecordingTrackCandidatesSummary | null>(null);
  const [items, setItems] = useState<RecordingTrackCandidatesResponse | null>(null);
  const [reviews, setReviews] = useState<Record<string, RecordingTrackCandidateReviewItem>>({});
  const [loading, setLoading] = useState(false);
  const [summaryLoading, setSummaryLoading] = useState(false);
  const [reviewsLoading, setReviewsLoading] = useState(false);
  const [error, setError] = useState("");
  const [summaryError, setSummaryError] = useState("");
  const [reviewsError, setReviewsError] = useState("");
  const [reviewSaveError, setReviewSaveError] = useState<Record<string, string>>({});
  const [reviewSaving, setReviewSaving] = useState<Record<string, boolean>>({});
  const [reviewDrafts, setReviewDrafts] = useState<Record<string, ReviewDraft>>({});
  const [safetyStatus, setSafetyStatus] = useState<SafetyFilter>("all");
  const [candidateType, setCandidateType] = useState<CandidateTypeFilter>("recording_track_candidate");
  const [relationshipKind, setRelationshipKind] = useState("all");
  const [evidenceBucket, setEvidenceBucket] = useState<EvidenceBucketFilter>("all");
  const [isrcFilter, setIsrcFilter] = useState<IsrcFilter>("all");
  const [sort, setSort] = useState<RecordingTrackSort>("confidence_desc");
  const [reviewFilter, setReviewFilter] = useState<ReviewFilter>("all");
  const [query, setQuery] = useState("");
  const [artist, setArtist] = useState("");
  const [includeFamilyCandidates, setIncludeFamilyCandidates] = useState(false);
  const [limit, setLimit] = useState(50);
  const [offset, setOffset] = useState(0);

  const filters = useMemo<RecordingTrackCandidateFilters>(() => ({
    limit,
    offset,
    safety_status: safetyStatus === "all" ? undefined : safetyStatus,
    candidate_type: candidateType === "all" ? undefined : candidateType,
    relationship_kind: relationshipKind === "all" ? undefined : relationshipKind,
    include_track_family_candidates: includeFamilyCandidates || candidateType === "track_family_candidate",
    q: query,
    artist,
  }), [artist, candidateType, includeFamilyCandidates, limit, offset, query, relationshipKind, safetyStatus]);

  useEffect(() => {
    let cancelled = false;
    async function loadSummary() {
      setSummaryLoading(true);
      setSummaryError("");
      try {
        const payload = await fetchRecordingTrackCandidatesSummary();
        if (!cancelled) {
          setSummary(payload);
        }
      } catch (loadError) {
        if (!cancelled) {
          setSummaryError(formatUiErrorMessage(loadError, "Failed to load recording track summary."));
        }
      } finally {
        if (!cancelled) {
          setSummaryLoading(false);
        }
      }
    }
    void loadSummary();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    async function loadReviews() {
      setReviewsLoading(true);
      setReviewsError("");
      try {
        const payload = await fetchRecordingTrackCandidateReviews();
        if (!cancelled) {
          const nextReviews: Record<string, RecordingTrackCandidateReviewItem> = {};
          for (const item of payload.items) {
            nextReviews[item.candidate_key] = item;
          }
          setReviews(nextReviews);
        }
      } catch (loadError) {
        if (!cancelled) {
          setReviewsError(formatUiErrorMessage(loadError, "Failed to load saved recording track reviews."));
        }
      } finally {
        if (!cancelled) {
          setReviewsLoading(false);
        }
      }
    }
    void loadReviews();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    async function loadCandidates() {
      setLoading(true);
      setError("");
      try {
        const payload = await fetchRecordingTrackCandidates(filters);
        if (!cancelled) {
          setItems(payload);
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(formatUiErrorMessage(loadError, "Failed to load recording track candidates."));
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }
    void loadCandidates();
    return () => {
      cancelled = true;
    };
  }, [filters]);

  const topReviewReasons = Object.entries(summary?.top_needs_review_reasons ?? {}).slice(0, 5);
  const reviewCounts = useMemo(() => {
    const counts: Record<string, number> = { reviewed: 0 };
    for (const review of Object.values(reviews)) {
      counts.reviewed += 1;
      counts[review.decision] = (counts[review.decision] ?? 0) + 1;
    }
    return counts;
  }, [reviews]);
  const visibleItems = useMemo(() => {
    const filtered = (items?.items ?? []).filter((item) => {
      if (evidenceBucket !== "all" && (item.evidence_bucket ?? "metadata_review_required") !== evidenceBucket) {
        return false;
      }
      if (!itemMatchesIsrcFilter(item, isrcFilter)) {
        return false;
      }
      const review = reviews[item.candidate_key];
      if (reviewFilter === "unreviewed") {
        return !review;
      }
      if (reviewFilter === "reviewed") {
        return Boolean(review);
      }
      if (reviewFilter !== "all") {
        return review?.decision === reviewFilter;
      }
      return true;
    });
    return filtered.sort((a, b) => {
      if (sort === "confidence_asc") {
        return a.confidence - b.confidence || a.candidate_key.localeCompare(b.candidate_key);
      }
      if (sort === "member_count_desc") {
        return b.members.length - a.members.length || b.confidence - a.confidence || a.candidate_key.localeCompare(b.candidate_key);
      }
      if (sort === "member_count_asc") {
        return a.members.length - b.members.length || b.confidence - a.confidence || a.candidate_key.localeCompare(b.candidate_key);
      }
      return b.confidence - a.confidence || b.members.length - a.members.length || a.candidate_key.localeCompare(b.candidate_key);
    });
  }, [evidenceBucket, isrcFilter, items?.items, reviewFilter, reviews, sort]);

  function draftForItem(item: RecordingTrackCandidateItem): ReviewDraft {
    const saved = reviews[item.candidate_key];
    return reviewDrafts[item.candidate_key] ?? {
      decision: saved?.decision ?? "unsure",
      reviewer_note: saved?.reviewer_note ?? "",
      preferred_representative_release_track_id: saved?.preferred_representative_release_track_id != null
        ? String(saved.preferred_representative_release_track_id)
        : "",
    };
  }

  function updateReviewDraft(candidateKey: string, patch: Partial<ReviewDraft>) {
    setReviewDrafts((current) => ({
      ...current,
      [candidateKey]: {
        ...(current[candidateKey] ?? {
          decision: reviews[candidateKey]?.decision ?? "unsure",
          reviewer_note: reviews[candidateKey]?.reviewer_note ?? "",
          preferred_representative_release_track_id: reviews[candidateKey]?.preferred_representative_release_track_id != null
            ? String(reviews[candidateKey].preferred_representative_release_track_id)
            : "",
        }),
        ...patch,
      },
    }));
  }

  async function saveReview(item: RecordingTrackCandidateItem) {
    const draft = draftForItem(item);
    const preferredReleaseTrackId = draft.preferred_representative_release_track_id
      ? Number(draft.preferred_representative_release_track_id)
      : null;
    const preferredMember = preferredReleaseTrackId == null
      ? null
      : item.members.find((member) => member.release_track_id === preferredReleaseTrackId) ?? null;
    setReviewSaving((current) => ({ ...current, [item.candidate_key]: true }));
    setReviewSaveError((current) => ({ ...current, [item.candidate_key]: "" }));
    try {
      const payload = await saveRecordingTrackCandidateReview({
        candidate_key: item.candidate_key,
        decision: draft.decision,
        reviewer_note: draft.reviewer_note.trim() || null,
        preferred_representative_release_track_id: preferredReleaseTrackId,
        preferred_playback_source_track_id: preferredMember?.source_track_db_ids?.[0] ?? null,
        candidate_snapshot: item,
      });
      setReviews((current) => ({
        ...current,
        [payload.item.candidate_key]: payload.item,
      }));
      setReviewDrafts((current) => ({
        ...current,
        [payload.item.candidate_key]: {
          decision: payload.item.decision,
          reviewer_note: payload.item.reviewer_note ?? "",
          preferred_representative_release_track_id: payload.item.preferred_representative_release_track_id != null
            ? String(payload.item.preferred_representative_release_track_id)
            : "",
        },
      }));
    } catch (saveError) {
      setReviewSaveError((current) => ({
        ...current,
        [item.candidate_key]: formatUiErrorMessage(saveError, "Failed to save review decision."),
      }));
    } finally {
      setReviewSaving((current) => ({ ...current, [item.candidate_key]: false }));
    }
  }

  return (
    <div className="identity-audit-grid">
      <p className="identity-audit-tab-copy">
        Read-only recording-track candidates show app-level same-song evidence between release tracks. This view does not save, apply, promote, or change playback/aggregation.
      </p>
      <div className="identity-audit-overview-grid">
        <article className="identity-audit-overview-card">
          <h3>Total Groups</h3>
          <strong>{summary?.total_candidate_groups ?? (summaryLoading ? "Loading..." : "not loaded")}</strong>
          <p>all candidate groups</p>
        </article>
        <article className="identity-audit-overview-card">
          <h3>Recording Tracks</h3>
          <strong>{statValue(summary, "recording")}</strong>
          <p>normal same-song candidates</p>
        </article>
        <article className="identity-audit-overview-card">
          <h3>Track Family</h3>
          <strong>{statValue(summary, "family")}</strong>
          <p>broader variants</p>
        </article>
        <article className="identity-audit-overview-card">
          <h3>Safe</h3>
          <strong>{statValue(summary, "safe")}</strong>
          <p>safe candidate</p>
        </article>
        <article className="identity-audit-overview-card">
          <h3>Needs Review</h3>
          <strong>{statValue(summary, "review")}</strong>
          <p>requires inspection</p>
        </article>
        <article className="identity-audit-overview-card">
          <h3>Same ISRC</h3>
          <strong>{statValue(summary, "sameIsrc")}</strong>
          <p>shared ISRC groups</p>
        </article>
        <article className="identity-audit-overview-card">
          <h3>ISRC Conflict</h3>
          <strong>{statValue(summary, "conflictingIsrc")}</strong>
          <p>compatible metadata</p>
        </article>
        <article className="identity-audit-overview-card">
          <h3>Missing ISRC</h3>
          <strong>{statValue(summary, "missingCompatible")}</strong>
          <p>compatible metadata</p>
        </article>
        <article className="identity-audit-overview-card">
          <h3>Partial ISRC</h3>
          <strong>{statValue(summary, "partialIsrc")}</strong>
          <p>some source evidence</p>
        </article>
        <article className="identity-audit-overview-card">
          <h3>Variants</h3>
          <strong>{statValue(summary, "variantExcluded")}</strong>
          <p>family-level flags</p>
        </article>
      </div>
      {summaryError ? <p className="empty-copy">{summaryError}</p> : null}
      {reviewsError ? <p className="empty-copy">{reviewsError}</p> : null}
      <div className="identity-audit-stats">
        <span className="identity-audit-stat recording-track-muted">
          <span>Review decision only</span>
          <strong>does not apply identity changes</strong>
        </span>
        <span className="identity-audit-stat"><span>Saved reviews</span><strong>{reviewsLoading ? "loading" : reviewCounts.reviewed}</strong></span>
        <span className="identity-audit-stat"><span>Accepted</span><strong>{reviewCounts.accepted ?? 0}</strong></span>
        <span className="identity-audit-stat"><span>Rejected</span><strong>{reviewCounts.rejected ?? 0}</strong></span>
        <span className="identity-audit-stat"><span>Unsure</span><strong>{reviewCounts.unsure ?? 0}</strong></span>
      </div>
      {topReviewReasons.length > 0 ? (
        <div className="identity-audit-group">
          <div className="identity-audit-stats">
            {topReviewReasons.map(([reason, count]) => (
              <span className="identity-audit-stat recording-track-warn" key={`review-reason-${reason}`}>
                <span>{reason}</span>
                <strong>{count}</strong>
              </span>
            ))}
          </div>
        </div>
      ) : null}
      <div className="identity-audit-group">
        <div className="identity-audit-ambiguous-toolbar recording-track-filter-grid">
          <label>
            <span>Safety</span>
            <select value={safetyStatus} onChange={(event) => { setSafetyStatus(event.target.value as SafetyFilter); setOffset(0); }}>
              <option value="all">all</option>
              <option value="safe_candidate">safe_candidate</option>
              <option value="needs_review">needs_review</option>
              <option value="unsafe">unsafe</option>
            </select>
          </label>
          <label>
            <span>Candidate type</span>
            <select value={candidateType} onChange={(event) => { setCandidateType(event.target.value as CandidateTypeFilter); setOffset(0); }}>
              <option value="all">all</option>
              <option value="recording_track_candidate">recording_track_candidate</option>
              <option value="track_family_candidate">track_family_candidate</option>
            </select>
          </label>
          <label>
            <span>Relationship</span>
            <select value={relationshipKind} onChange={(event) => { setRelationshipKind(event.target.value); setOffset(0); }}>
              <option value="all">all</option>
              {RELATIONSHIP_KIND_OPTIONS.map((kind) => <option value={kind} key={kind}>{kind}</option>)}
            </select>
          </label>
          <label>
            <span>Evidence bucket</span>
            <select value={evidenceBucket} onChange={(event) => setEvidenceBucket(event.target.value as EvidenceBucketFilter)}>
              <option value="all">all</option>
              {EVIDENCE_BUCKET_OPTIONS.map((bucket) => <option value={bucket} key={bucket}>{bucket}</option>)}
            </select>
          </label>
          <label>
            <span>ISRC state</span>
            <select value={isrcFilter} onChange={(event) => setIsrcFilter(event.target.value as IsrcFilter)}>
              <option value="all">all</option>
              <option value="same">same</option>
              <option value="partial">partial</option>
              <option value="missing">missing</option>
              <option value="conflicting">conflicting</option>
            </select>
          </label>
          <label>
            <span>Sort</span>
            <select value={sort} onChange={(event) => setSort(event.target.value as RecordingTrackSort)}>
              <option value="confidence_desc">confidence high to low</option>
              <option value="confidence_asc">confidence low to high</option>
              <option value="member_count_desc">member count high to low</option>
              <option value="member_count_asc">member count low to high</option>
            </select>
          </label>
          <label>
            <span>Review</span>
            <select value={reviewFilter} onChange={(event) => setReviewFilter(event.target.value as ReviewFilter)}>
              <option value="all">all</option>
              <option value="unreviewed">unreviewed</option>
              <option value="reviewed">reviewed</option>
              {REVIEW_DECISION_OPTIONS.map((option) => <option value={option.value} key={option.value}>{option.value}</option>)}
            </select>
          </label>
          <label>
            <span>Title search</span>
            <input value={query} onChange={(event) => { setQuery(event.target.value); setOffset(0); }} placeholder="song title" />
          </label>
          <label>
            <span>Artist search</span>
            <input value={artist} onChange={(event) => { setArtist(event.target.value); setOffset(0); }} placeholder="artist" />
          </label>
          <label>
            <span>Limit</span>
            <select value={limit} onChange={(event) => { setLimit(Number(event.target.value)); setOffset(0); }}>
              <option value={25}>25</option>
              <option value={50}>50</option>
              <option value={100}>100</option>
            </select>
          </label>
          <label className="recording-track-checkbox">
            <input
              checked={includeFamilyCandidates}
              onChange={(event) => { setIncludeFamilyCandidates(event.target.checked); setOffset(0); }}
              type="checkbox"
            />
            <span>Include track-family candidates</span>
          </label>
        </div>
      </div>
      <div className="identity-audit-group">
        <div className="source-release-track-header">
          <div>
            <h4>Candidate Groups</h4>
            <p className="identity-audit-tab-copy">
              {items ? `${visibleItems.length} visible of ${items.returned} loaded (${items.total} matching server filters)` : loading ? "Loading candidates..." : "Candidates are not loaded yet."}
            </p>
          </div>
          <div className="section-column-header-actions">
            <button
              className="secondary-button"
              disabled={offset === 0 || loading}
              onClick={() => setOffset(Math.max(0, offset - limit))}
              type="button"
            >
              Previous
            </button>
            <button
              className="secondary-button"
              disabled={!items?.has_more || loading}
              onClick={() => setOffset(offset + limit)}
              type="button"
            >
              Next
            </button>
          </div>
        </div>
        {error ? <p className="empty-copy">{error}</p> : null}
        {!error && loading && !items ? <p className="empty-copy">Loading recording track candidates...</p> : null}
        {!loading && items?.items.length === 0 ? <p className="empty-copy">No candidate groups match these filters.</p> : null}
        {!loading && items && items.items.length > 0 && visibleItems.length === 0 ? <p className="empty-copy">No loaded candidate groups match the local inspection filters.</p> : null}
        <div className="identity-audit-examples">
          {visibleItems.map((item) => {
            const flags = riskFlags(item);
            const savedReview = reviews[item.candidate_key];
            const draft = draftForItem(item);
            return (
              <article className="identity-audit-example recording-track-candidate-card" key={item.candidate_key}>
                <div className="identity-audit-example-header">
                  <div>
                    <h4>{item.display_name || "Unnamed song candidate"}</h4>
                    <p>{item.members.length} release tracks · representative release {item.representative.release_track_id ?? "n/a"}</p>
                  </div>
                  <span className={`identity-audit-type-badge recording-track-status-${item.safety_status}`}>
                    {displayToken(item.safety_status)}
                  </span>
                </div>
                <div className="identity-audit-stats">
                  <span className="identity-audit-stat"><span>Type</span><strong>{displayToken(item.candidate_type)}</strong></span>
                  <span className="identity-audit-stat"><span>Confidence</span><strong>{Math.round(item.confidence * 100)}%</strong></span>
                  <span className="identity-audit-stat"><span>Members</span><strong>{item.members.length}</strong></span>
                  <span className="identity-audit-stat"><span>Relationship</span><strong>{displayToken(item.relationship_kind)}</strong></span>
                  <span className="identity-audit-stat"><span>Strength</span><strong>{displayToken(item.relationship_strength)}</strong></span>
                  {item.evidence_bucket ? <span className="identity-audit-stat"><span>Evidence</span><strong>{displayToken(item.evidence_bucket)}</strong></span> : null}
                  {savedReview ? (
                    <span className="identity-audit-stat recording-track-good"><span>Reviewed</span><strong>{displayToken(savedReview.decision)}</strong></span>
                  ) : (
                    <span className="identity-audit-stat recording-track-muted"><span>Reviewed</span><strong>no</strong></span>
                  )}
                </div>
                {renderEvidencePills(item)}
                {flags.length > 0 ? (
                  <div className="identity-audit-stats">
                    {flags.map((flag) => (
                      <span className="identity-audit-stat recording-track-risk" key={`${item.candidate_key}-${flag}`}>
                        <span>Risk</span><strong>{flag}</strong>
                      </span>
                    ))}
                  </div>
                ) : null}
                <div className="recording-track-representative-box">
                  <span>Representative diagnostics</span>
                  <strong>release {item.representative.release_track_id ?? "n/a"} · source {item.representative.source_track_id ?? "n/a"}</strong>
                  <p>{item.representative.reason || "No recommendation reason supplied."}</p>
                  {savedReview ? (
                    <p>
                      Saved {displayToken(savedReview.decision)} at {new Date(savedReview.updated_at).toLocaleString()}
                      {savedReview.preferred_representative_release_track_id != null ? ` · preferred release ${savedReview.preferred_representative_release_track_id}` : ""}
                    </p>
                  ) : null}
                  {savedReview?.reviewer_note ? <p>Note: {savedReview.reviewer_note}</p> : null}
                </div>
                <div className="recording-track-review-box">
                  <span>Review decision only — does not apply identity changes.</span>
                  <div className="recording-track-decision-grid">
                    {REVIEW_DECISION_OPTIONS.map((option) => (
                      <label className="recording-track-decision-option" key={`${item.candidate_key}-${option.value}`}>
                        <input
                          checked={draft.decision === option.value}
                          onChange={() => updateReviewDraft(item.candidate_key, { decision: option.value })}
                          type="radio"
                        />
                        <span>{option.label}</span>
                      </label>
                    ))}
                  </div>
                  <label className="recording-track-review-field">
                    <span>Preferred representative</span>
                    <select
                      value={draft.preferred_representative_release_track_id}
                      onChange={(event) => updateReviewDraft(item.candidate_key, { preferred_representative_release_track_id: event.target.value })}
                    >
                      <option value="">No preference</option>
                      {item.members.map((member) => (
                        <option value={member.release_track_id} key={`${item.candidate_key}-preferred-${member.release_track_id}`}>
                          {member.release_track_id} · {member.title || "Untitled"} · {member.album || "Unknown album"}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="recording-track-review-field">
                    <span>Review note</span>
                    <textarea
                      onChange={(event) => updateReviewDraft(item.candidate_key, { reviewer_note: event.target.value })}
                      placeholder="Manual notes for future inspection"
                      rows={2}
                      value={draft.reviewer_note}
                    />
                  </label>
                  {reviewSaveError[item.candidate_key] ? <p className="empty-copy">{reviewSaveError[item.candidate_key]}</p> : null}
                  <button
                    className="secondary-button"
                    disabled={Boolean(reviewSaving[item.candidate_key])}
                    onClick={() => void saveReview(item)}
                    type="button"
                  >
                    {reviewSaving[item.candidate_key] ? "Saving review..." : "Save review"}
                  </button>
                </div>
                <div className="recording-track-reasons-grid">
                  <div>
                    <span>Why grouped</span>
                    {item.why_grouped.length > 0 ? (
                      <ul>
                        {item.why_grouped.map((reason) => <li key={`${item.candidate_key}-grouped-${reason}`}>{reason}</li>)}
                      </ul>
                    ) : <p>No grouping reasons supplied.</p>}
                  </div>
                  <div>
                    <span>Why review</span>
                    {item.why_review.length > 0 ? (
                      <ul>
                        {item.why_review.map((reason) => <li key={`${item.candidate_key}-review-${reason}`}>{reason}</li>)}
                      </ul>
                    ) : <p>No review reasons supplied.</p>}
                  </div>
                </div>
                <div className="recording-track-member-details">
                  <span className="recording-track-variations-title">Variations</span>
                  <div className="identity-audit-variant-list">
                    {item.members.map((member) => renderMember(member, onOpenReleaseTrack))}
                  </div>
                </div>
              </article>
            );
          })}
        </div>
      </div>
    </div>
  );
}
