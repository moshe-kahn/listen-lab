import { useState } from "react";

import { ArtistDuplicateAuditTab } from "./ArtistDuplicateAuditTab";
import { ArtistPromotionSkipsTab } from "./ArtistPromotionSkipsTab";

type ArtistAuditSection = "promotion_skips" | "duplicate_repair";

export function ArtistIdentityAuditTab() {
  const [section, setSection] = useState<ArtistAuditSection>("promotion_skips");

  return (
    <>
      <div className="track-ranking-toggle identity-audit-tabs" role="group" aria-label="Artist identity audit section">
        <button
          className={`track-ranking-chip${section === "promotion_skips" ? " track-ranking-chip-active" : ""}`}
          onClick={() => setSection("promotion_skips")}
          type="button"
        >
          Promotion Skips
        </button>
        <button
          className={`track-ranking-chip${section === "duplicate_repair" ? " track-ranking-chip-active" : ""}`}
          onClick={() => setSection("duplicate_repair")}
          type="button"
        >
          Duplicate Repair
        </button>
      </div>
      {section === "promotion_skips" ? <ArtistPromotionSkipsTab /> : <ArtistDuplicateAuditTab />}
    </>
  );
}
