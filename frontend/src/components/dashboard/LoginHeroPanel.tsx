import type { ReactNode } from "react";
import type {
  RecentBackfillProbeResponse,
  RecentBeforeProbeResponse,
  RecentIngestResultResponse,
  ExperienceMode,
} from "../../types/appTypes";

type LoginHeroPanelProps = {
  heroTitle: string;
  heroCopy: string;
  experienceMode: ExperienceMode;
  recentIngestResult: RecentIngestResultResponse | null;
  recentBeforeProbeResult: RecentBeforeProbeResponse | null;
  recentBackfillProbeResult: RecentBackfillProbeResponse | null;
  renderExperienceModeToggle: () => ReactNode;
  handleAuthAction: () => void;
  startRecentIngestLogin: () => void;
  runRecentBeforeProbe: () => void;
  runRecentBackfillProbe: () => void;
};

export function LoginHeroPanel({
  heroTitle,
  heroCopy,
  experienceMode,
  recentIngestResult,
  recentBeforeProbeResult,
  recentBackfillProbeResult,
  renderExperienceModeToggle,
  handleAuthAction,
  startRecentIngestLogin,
  runRecentBeforeProbe,
  runRecentBackfillProbe,
}: LoginHeroPanelProps) {
  return (
    <div className="top-bar">
      <div className="top-copy">
        <p className="eyebrow">ListenLab</p>
        <h1>{heroTitle}</h1>
        <p className="lede three-line-clamp">{heroCopy}</p>
      </div>

      <div className="top-side">
        {renderExperienceModeToggle()}
        <button className="primary-button top-login-button" onClick={handleAuthAction} type="button">
          {experienceMode === "local" ? "Open restricted local mode" : "Log in with Spotify"}
        </button>
        {experienceMode === "full" ? (
          <button className="secondary-button top-login-button" onClick={startRecentIngestLogin} type="button">
            Connect Spotify and ingest recent plays
          </button>
        ) : null}
        {experienceMode === "full" ? (
          <button className="secondary-button top-login-button" onClick={() => runRecentBeforeProbe()} type="button">
            Probe recent API before 90 days
          </button>
        ) : null}
        {experienceMode === "full" ? (
          <button className="secondary-button top-login-button" onClick={() => runRecentBackfillProbe()} type="button">
            Probe recent API paging (50 x up to 10)
          </button>
        ) : null}
        {recentIngestResult ? (
          <p className="empty-copy">
            {recentIngestResult.auth_succeeded && recentIngestResult.ingest_succeeded
              ? `Recent ingest succeeded: ${recentIngestResult.row_count ?? 0} rows (${recentIngestResult.earliest_api_played_at ?? "n/a"} to ${recentIngestResult.latest_api_played_at ?? "n/a"}).`
              : `Recent ingest failed: ${recentIngestResult.error ?? "unknown error"}`}
          </p>
        ) : null}
        {recentBeforeProbeResult ? (
          <p className="empty-copy">
            {recentBeforeProbeResult.ok
              ? `Before-90d probe: ${recentBeforeProbeResult.returned_items ?? 0} rows (${recentBeforeProbeResult.earliest_played_at ?? "n/a"} to ${recentBeforeProbeResult.latest_played_at ?? "n/a"}).`
              : `Before-90d probe failed: ${recentBeforeProbeResult.detail ?? "unknown error"}`}
          </p>
        ) : null}
        {recentBackfillProbeResult ? (
          <p className="empty-copy">
            {recentBackfillProbeResult.ok
              ? `Backfill probe: ${recentBackfillProbeResult.total_items ?? 0} items across ${recentBackfillProbeResult.pages_fetched ?? 0} pages (${recentBackfillProbeResult.earliest_played_at ?? "n/a"} to ${recentBackfillProbeResult.latest_played_at ?? "n/a"}).`
              : `Backfill probe failed: ${recentBackfillProbeResult.detail ?? "unknown error"}`}
          </p>
        ) : null}
      </div>
    </div>
  );
}
