import type {
  AlbumIdentityAuditTab,
  IdentityAuditEntityTab,
  IdentityAuditIssueReviewState,
  IdentityAuditIssueSort,
  TrackIdentityAuditTab,
} from "../components/identityAudit/IssueFeed";

export type {
  AlbumIdentityAuditTab,
  IdentityAuditEntityTab,
  IdentityAuditIssueReviewState,
  IdentityAuditIssueSort,
  TrackIdentityAuditTab,
};

export type IdentityAuditPersistedPrefs = {
  entityTab?: IdentityAuditEntityTab;
  trackTab?: TrackIdentityAuditTab;
  albumTab?: AlbumIdentityAuditTab;
  trackIssueSort?: IdentityAuditIssueSort;
  albumIssueSort?: IdentityAuditIssueSort;
  issueReviewState?: Record<string, IdentityAuditIssueReviewState>;
  expandedIssueKeys?: Record<string, boolean>;
};

const IDENTITY_AUDIT_PREFS_STORAGE_KEY = "listenlab-identity-audit-prefs";

export function loadIdentityAuditPersistedPrefs(): IdentityAuditPersistedPrefs {
  try {
    const raw = window.localStorage.getItem(IDENTITY_AUDIT_PREFS_STORAGE_KEY);
    if (!raw) {
      return {};
    }
    const parsed = JSON.parse(raw) as IdentityAuditPersistedPrefs;
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

export function saveIdentityAuditPersistedPrefs(prefs: IdentityAuditPersistedPrefs) {
  try {
    window.localStorage.setItem(IDENTITY_AUDIT_PREFS_STORAGE_KEY, JSON.stringify(prefs));
  } catch {
    // Persistence is best-effort; audit workflow must keep working without localStorage.
  }
}
