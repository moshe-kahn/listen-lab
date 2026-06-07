import type { CSSProperties } from "react";
import type { AnalysisMode } from "../../types/appTypes";

type CooldownRetryControlProps = {
  showRateLimitReload: boolean;
  reloadReady: boolean;
  reloadProgress: number;
  loadingProfile: boolean;
  loadingRecentSection: boolean;
  loadingExtendedProfile: boolean;
  onCooldownRetry: () => void;
};

type LoadingDisplayProps = CooldownRetryControlProps & {
  statusHistory: string[];
  statusMessage: string;
  analysisMode: AnalysisMode;
};

function loadingDisplayCopy(_statusHistory: string[], statusMessage: string, analysisMode: AnalysisMode) {
  const loadingLabel =
    statusMessage && !statusMessage.startsWith("Spotify login succeeded")
      ? statusMessage
      : "Loading your Spotify data...";
  const analyzingStage = loadingLabel.toLowerCase().startsWith("analyzing");
  const quickLoadMode = analysisMode === "quick" && !analyzingStage;

  return { loadingLabel, quickLoadMode };
}

export function CooldownRetryControl({
  showRateLimitReload,
  reloadReady,
  reloadProgress,
  loadingProfile,
  loadingRecentSection,
  loadingExtendedProfile,
  onCooldownRetry,
}: CooldownRetryControlProps) {
  if (!showRateLimitReload) {
    return null;
  }

  return (
    <div className="loading-retry-row">
      <button
        className={`secondary-button loading-retry-button${reloadReady ? " loading-retry-button-ready" : ""}`}
        aria-label={reloadReady ? "Reload" : "Waiting for Spotify cooldown"}
        disabled={!reloadReady || loadingProfile || loadingRecentSection || loadingExtendedProfile}
        onClick={onCooldownRetry}
        style={{ opacity: 0.35 + reloadProgress * 0.65 }}
        type="button"
      >
        <span
          className="loading-retry-clock"
          aria-hidden="true"
          style={{ "--reload-progress": `${reloadProgress * 360}deg` } as CSSProperties}
        >
          <span className="loading-retry-clock-face">
            <span className="loading-retry-clock-groove loading-retry-clock-groove-outer" />
            <span className="loading-retry-clock-groove loading-retry-clock-groove-inner" />
            <span className="loading-retry-clock-pie" />
            <span className="loading-retry-clock-center-ring" />
            <span className="loading-retry-clock-center">
              {reloadReady ? "\u21bb" : ""}
            </span>
          </span>
        </span>
      </button>
    </div>
  );
}

export function LoadingScreen(props: LoadingDisplayProps) {
  const { loadingLabel, quickLoadMode } = loadingDisplayCopy(props.statusHistory, props.statusMessage, props.analysisMode);

  return (
    <main className="app-shell">
      <section className={`loading-screen${props.showRateLimitReload && !props.reloadReady ? " loading-screen-error" : ""}`}>
        <div className="loading-graphic" aria-hidden="true">
          <div className={`loading-headphones${props.showRateLimitReload && !props.reloadReady ? " loading-headphones-error" : ""}`}>
            <div className="loading-headphones-band" />
            <div className="loading-headphones-cup loading-headphones-cup-left" />
            <div className="loading-headphones-cup loading-headphones-cup-right" />
          </div>
        </div>
        <p className="eyebrow">ListenLab</p>
        <h1>{quickLoadMode ? "Loading your Spotify profile" : "Your music is being analyzed"}</h1>
        <p className="loading-copy two-line-clamp">
          {quickLoadMode
            ? "We're starting with a lighter profile view so you can get in quickly."
            : "We're pulling together your recent activity, favorites, and history-backed listening patterns."}
        </p>
        <p className="loading-phase single-line-ellipsis">{loadingLabel}</p>
        <CooldownRetryControl {...props} />
      </section>
    </main>
  );
}

export function FullAnalysisOverlay(props: LoadingDisplayProps) {
  const { loadingLabel, quickLoadMode } = loadingDisplayCopy(props.statusHistory, props.statusMessage, props.analysisMode);

  return (
    <div className="loading-overlay-backdrop" role="status" aria-live="polite">
      <section className={`loading-screen${props.showRateLimitReload ? " loading-screen-error" : ""}`}>
        <div className="loading-graphic" aria-hidden="true">
          <div className={`loading-headphones${props.showRateLimitReload ? " loading-headphones-error" : ""}`}>
            <div className="loading-headphones-band" />
            <div className="loading-headphones-cup loading-headphones-cup-left" />
            <div className="loading-headphones-cup loading-headphones-cup-right" />
          </div>
        </div>
        <p className="eyebrow">ListenLab</p>
        <h1>{quickLoadMode ? "Loading your Spotify profile" : "Your music is being analyzed"}</h1>
        <p className="loading-copy two-line-clamp">
          {quickLoadMode
            ? "We're starting with a lighter profile view so you can get in quickly."
            : "We're pulling together your recent activity, favorites, and history-backed listening patterns."}
        </p>
        <p className="loading-phase single-line-ellipsis">{loadingLabel}</p>
        <CooldownRetryControl {...props} />
      </section>
    </div>
  );
}
