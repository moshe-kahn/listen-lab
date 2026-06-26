import type { ReactNode } from "react";
import type { ExperienceMode } from "../../types/appTypes";

type LoginHeroPanelProps = {
  heroTitle: string;
  heroCopy: string;
  experienceMode: ExperienceMode;
  renderExperienceModeToggle: () => ReactNode;
  handleAuthAction: () => void;
};

export function LoginHeroPanel({
  heroTitle,
  heroCopy,
  experienceMode,
  renderExperienceModeToggle,
  handleAuthAction,
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
      </div>
    </div>
  );
}
