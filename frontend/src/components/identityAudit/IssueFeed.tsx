import type { ReactNode } from "react";

export type TrackIdentityAuditTab = "problems" | "mapping" | "review_queue";
export type AlbumIdentityAuditTab = "problems" | "merge_review" | "catalog";
export type IdentityAuditEntityTab = "tracks" | "albums" | "artists";
export type IdentityAuditIssueSort = "severity" | "unresolved" | "confidence" | "affected" | "metadata";
export type IdentityAuditIssueReviewState = "reviewed" | "dismissed";
export type IssueSeverity = "high" | "medium" | "low";

export type NormalizedAuditIssue = {
  key: string;
  typeLabel: string;
  entityLabel: string;
  whyFlagged: string;
  evidenceSummary: string;
  confidenceLabel: string;
  confidenceScore: number | null;
  severityLabel: IssueSeverity;
  affectedCount: number | string;
  affectedScore: number;
  reviewStatus: string;
  isResolved: boolean;
  isBlocked: boolean;
  suggestedAction: string;
  onOpenMapping?: () => void;
  onReview?: () => void;
  details: ReactNode;
};

export function issueSeverityForCount(count: number | null, fallback: IssueSeverity = "medium"): IssueSeverity {
  if (count == null) {
    return fallback;
  }
  if (count >= 5) {
    return "high";
  }
  if (count >= 2) {
    return "medium";
  }
  return "low";
}

function issueSeverityTone(severity: IssueSeverity) {
  if (severity === "high") {
    return { background: "#fdecec", border: "#f2c3c3", color: "#9a1f1f" };
  }
  if (severity === "medium") {
    return { background: "#fff7e6", border: "#f1ddb0", color: "#8a5b00" };
  }
  return { background: "#e9f7ee", border: "#c4e9d2", color: "#1f6f40" };
}

function issueSeverityScore(severity: IssueSeverity) {
  if (severity === "high") {
    return 3;
  }
  if (severity === "medium") {
    return 2;
  }
  return 1;
}

function reviewStateTone(state: string) {
  if (state.includes("submitted") || state.includes("validated") || state.includes("reviewed") || state.includes("previewed")) {
    return { background: "#e9f7ee", border: "#c4e9d2", color: "#1f6f40" };
  }
  if (state.includes("blocked") || state.includes("metadata")) {
    return { background: "#fdecec", border: "#f2c3c3", color: "#9a1f1f" };
  }
  if (state.includes("review") || state.includes("unreviewed")) {
    return { background: "#fff7e6", border: "#f1ddb0", color: "#8a5b00" };
  }
  return { background: "#f2f3f5", border: "#d8dbe1", color: "#4f5663" };
}

function IssueChip({ label, tone }: { label: string; tone?: { background: string; border: string; color: string } }) {
  const colors = tone ?? { background: "#f2f3f5", border: "#d8dbe1", color: "#4f5663" };
  return (
    <span
      className="track-ranking-chip"
      style={{ background: colors.background, borderColor: colors.border, color: colors.color }}
    >
      {label}
    </span>
  );
}

function issueEffectiveReviewStatus(issue: NormalizedAuditIssue, reviewState: Record<string, IdentityAuditIssueReviewState>) {
  const storedState = reviewState[issue.key];
  if (storedState === "reviewed") {
    return "reviewed";
  }
  if (storedState === "dismissed") {
    return "dismissed";
  }
  return issue.reviewStatus;
}

function issueEffectiveResolved(issue: NormalizedAuditIssue, reviewState: Record<string, IdentityAuditIssueReviewState>) {
  return issue.isResolved || Boolean(reviewState[issue.key]);
}

function sortAuditIssues(issues: NormalizedAuditIssue[], sort: IdentityAuditIssueSort, reviewState: Record<string, IdentityAuditIssueReviewState>) {
  return [...issues].sort((a, b) => {
    if (sort === "unresolved") {
      return Number(issueEffectiveResolved(a, reviewState)) - Number(issueEffectiveResolved(b, reviewState))
        || issueSeverityScore(b.severityLabel) - issueSeverityScore(a.severityLabel)
        || b.affectedScore - a.affectedScore;
    }
    if (sort === "confidence") {
      return (b.confidenceScore ?? -1) - (a.confidenceScore ?? -1)
        || issueSeverityScore(b.severityLabel) - issueSeverityScore(a.severityLabel);
    }
    if (sort === "affected") {
      return b.affectedScore - a.affectedScore
        || issueSeverityScore(b.severityLabel) - issueSeverityScore(a.severityLabel);
    }
    if (sort === "metadata") {
      return Number(b.isBlocked) - Number(a.isBlocked)
        || issueSeverityScore(b.severityLabel) - issueSeverityScore(a.severityLabel);
    }
    return issueSeverityScore(b.severityLabel) - issueSeverityScore(a.severityLabel)
      || Number(issueEffectiveResolved(a, reviewState)) - Number(issueEffectiveResolved(b, reviewState))
      || b.affectedScore - a.affectedScore;
  });
}

function IssueSortControls({ sort, setSort }: { sort: IdentityAuditIssueSort; setSort: (value: IdentityAuditIssueSort) => void }) {
  const options: Array<{ value: IdentityAuditIssueSort; label: string }> = [
    { value: "severity", label: "Highest severity" },
    { value: "unresolved", label: "Unresolved first" },
    { value: "confidence", label: "Highest confidence" },
    { value: "affected", label: "Largest scope" },
    { value: "metadata", label: "Metadata blockers" },
  ];
  return (
    <label>
      Sort
      <select onChange={(event) => setSort(event.target.value as IdentityAuditIssueSort)} value={sort}>
        {options.map((option) => (
          <option key={`identity-issue-sort-${option.value}`} value={option.value}>{option.label}</option>
        ))}
      </select>
    </label>
  );
}

type IssueCardProps = {
  issue: NormalizedAuditIssue;
  reviewState: Record<string, IdentityAuditIssueReviewState>;
  expandedIssueKeys: Record<string, boolean>;
  setIssueReviewState: (issueKey: string, state: IdentityAuditIssueReviewState | null) => void;
  setIssueExpanded: (issueKey: string, expanded: boolean) => void;
};

function IssueCard({ issue, reviewState, expandedIssueKeys, setIssueReviewState, setIssueExpanded }: IssueCardProps) {
  const reviewStatus = issueEffectiveReviewStatus(issue, reviewState);
  const isExpanded = Boolean(expandedIssueKeys[issue.key]);
  return (
    <details
      className="identity-audit-example"
      key={issue.key}
      open={isExpanded}
      onToggle={(event) => setIssueExpanded(issue.key, event.currentTarget.open)}
    >
      <summary>
        <div className="identity-audit-example-header">
          <div>
            <h4>{issue.entityLabel}</h4>
            <p>{issue.whyFlagged}</p>
          </div>
          <span className="identity-audit-type-badge">{issue.typeLabel}</span>
        </div>
        <div className="identity-audit-stats">
          <span className="identity-audit-stat"><span>Evidence</span><strong>{issue.evidenceSummary}</strong></span>
          <span className="identity-audit-stat"><span>Affected</span><strong>{issue.affectedCount}</strong></span>
          <span className="identity-audit-stat"><span>Confidence</span><strong>{issue.confidenceLabel}</strong></span>
          <span className="identity-audit-stat"><span>Review</span><strong>{reviewStatus}</strong></span>
        </div>
        <div className="identity-audit-ambiguous-summary">
          <IssueChip label={issue.severityLabel} tone={issueSeverityTone(issue.severityLabel)} />
          <IssueChip label={reviewStatus} tone={reviewStateTone(reviewStatus)} />
          {issue.isBlocked ? <IssueChip label="metadata incomplete" tone={reviewStateTone("metadata incomplete")} /> : null}
          <IssueChip label={issue.suggestedAction} />
          {issue.onOpenMapping ? (
            <button
              className="secondary-button"
              onClick={(event) => {
                event.preventDefault();
                event.stopPropagation();
                issue.onOpenMapping?.();
              }}
              type="button"
            >
              Open in Mapping
            </button>
          ) : null}
          {issue.onReview ? (
            <button
              className="secondary-button"
              onClick={(event) => {
                event.preventDefault();
                event.stopPropagation();
                issue.onReview?.();
              }}
              type="button"
            >
              Review Candidate
            </button>
          ) : null}
          <button
            className="secondary-button"
            onClick={(event) => {
              event.preventDefault();
              event.stopPropagation();
              setIssueReviewState(issue.key, "reviewed");
            }}
            type="button"
          >
            Mark reviewed
          </button>
          <button
            className="secondary-button"
            onClick={(event) => {
              event.preventDefault();
              event.stopPropagation();
              setIssueReviewState(issue.key, "dismissed");
            }}
            type="button"
          >
            Dismiss
          </button>
          {reviewState[issue.key] ? (
            <button
              className="secondary-button"
              onClick={(event) => {
                event.preventDefault();
                event.stopPropagation();
                setIssueReviewState(issue.key, null);
              }}
              type="button"
            >
              Clear state
            </button>
          ) : null}
        </div>
      </summary>
      {isExpanded ? <div style={{ marginTop: "12px" }}>{issue.details}</div> : null}
    </details>
  );
}

type IssueFeedProps = {
  title: string;
  issues: NormalizedAuditIssue[];
  emptyCopy: string;
  sort: IdentityAuditIssueSort;
  setSort: (value: IdentityAuditIssueSort) => void;
  reviewState: Record<string, IdentityAuditIssueReviewState>;
  expandedIssueKeys: Record<string, boolean>;
  setIssueReviewState: (issueKey: string, state: IdentityAuditIssueReviewState | null) => void;
  setIssueExpanded: (issueKey: string, expanded: boolean) => void;
  resetIssueState: () => void;
};

export function IssueFeed({
  title,
  issues,
  emptyCopy,
  sort,
  setSort,
  reviewState,
  expandedIssueKeys,
  setIssueReviewState,
  setIssueExpanded,
  resetIssueState,
}: IssueFeedProps) {
  const sortedIssues = sortAuditIssues(issues, sort, reviewState);
  const highCount = issues.filter((issue) => issue.severityLabel === "high").length;
  const blockedCount = issues.filter((issue) => issue.isBlocked).length;
  const unresolvedCount = issues.filter((issue) => !issueEffectiveResolved(issue, reviewState)).length;
  const nextUnresolvedIssue = sortedIssues.find((issue) => !issueEffectiveResolved(issue, reviewState)) ?? null;
  return (
    <div className="identity-audit-group">
      <div className="tracks-formula-heading">
        <h3>{title}</h3>
        <span>{issues.length} issues</span>
      </div>
      <div className="tracks-only-summary">
        <span>High severity: {highCount}</span>
        <span>Unresolved: {unresolvedCount}</span>
        <span>Metadata blockers: {blockedCount}</span>
      </div>
      <div className="identity-audit-ambiguous-summary">
        <IssueSortControls sort={sort} setSort={setSort} />
        <button
          className="secondary-button"
          disabled={!nextUnresolvedIssue}
          onClick={() => {
            if (nextUnresolvedIssue) {
              setIssueExpanded(nextUnresolvedIssue.key, true);
            }
          }}
          type="button"
        >
          Next unresolved
        </button>
        <button
          className="secondary-button"
          disabled={Object.keys(reviewState).length === 0 && Object.keys(expandedIssueKeys).length === 0}
          onClick={resetIssueState}
          type="button"
        >
          Reset local issue state
        </button>
      </div>
      {issues.length === 0 ? (
        <p className="empty-copy">{emptyCopy}</p>
      ) : (
        <div className="identity-audit-examples">
          {sortedIssues.map((issue) => (
            <IssueCard
              expandedIssueKeys={expandedIssueKeys}
              issue={issue}
              key={issue.key}
              reviewState={reviewState}
              setIssueExpanded={setIssueExpanded}
              setIssueReviewState={setIssueReviewState}
            />
          ))}
        </div>
      )}
    </div>
  );
}
