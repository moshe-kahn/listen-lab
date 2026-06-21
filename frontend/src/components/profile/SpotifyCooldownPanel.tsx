import { formatCooldownTimerLabel } from "../../utils/dashboardUtils";

type SpotifyCooldownPanelProps = {
  loading: boolean;
  ready: boolean;
  secondsRemaining: number;
  onRetry: () => void;
};

export function SpotifyCooldownPanel({
  loading,
  ready,
  secondsRemaining,
  onRetry,
}: SpotifyCooldownPanelProps) {
  return (
    <section className={`profile-cooldown-panel${ready ? " profile-cooldown-panel-ready" : ""}`}>
      <div className="profile-cooldown-copy">
        <span className="cooldown-chip-record" aria-hidden="true">
          <span className="cooldown-chip-record-center" />
        </span>
        <div>
          <strong>{ready ? "Spotify ready" : "Spotify cooldown"}</strong>
          <p>
            {ready
              ? "Spotify sync is available again."
              : `Requests paused for ${formatCooldownTimerLabel(secondsRemaining)}.`}
          </p>
        </div>
      </div>
      <button className="secondary-button" disabled={!ready || loading} onClick={onRetry} type="button">
        Retry
      </button>
    </section>
  );
}
