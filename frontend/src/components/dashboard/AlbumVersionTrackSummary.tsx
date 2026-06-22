import { useEffect, useRef } from "react";

import type { AlbumFamilyContext, AlbumTrackEntry } from "../../types/appTypes";

type AlbumVersionTrackSummaryProps = {
  context: AlbumFamilyContext;
  entries: AlbumTrackEntry[];
};

function durationLabel(durationMs: number) {
  const totalSeconds = Math.max(0, Math.round(durationMs / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}:${String(seconds).padStart(2, "0")}`;
}

export function AlbumVersionTrackSummary({ context, entries }: AlbumVersionTrackSummaryProps) {
  const detailsRef = useRef<HTMLDetailsElement>(null);
  const selectedVersion = context.versions.find((version) => version.spotify_album_id === context.selected_spotify_album_id)
    ?? context.versions.find((version) => version.is_selected)
    ?? context.versions[0];
  const selectedEntries = entries.filter((entry) => !entry.familyExclusive);
  const selectedTrackCount = selectedVersion?.total_tracks ?? selectedEntries.length;
  const selectedDurationMs = selectedVersion?.total_duration_ms
    ?? selectedEntries.reduce((total, entry) => total + Math.max(0, entry.durationMs ?? 0), 0);
  const comparisonVersions = selectedVersion ? context.versions.filter((version) => (
    version.spotify_album_id !== selectedVersion.spotify_album_id
    && version.total_tracks != null
  )) : [];

  useEffect(() => {
    const closeOnOutsidePointer = (event: PointerEvent) => {
      const details = detailsRef.current;
      if (details?.open && event.target instanceof Node && !details.contains(event.target)) {
        details.removeAttribute("open");
      }
    };
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        detailsRef.current?.removeAttribute("open");
      }
    };
    document.addEventListener("pointerdown", closeOnOutsidePointer);
    document.addEventListener("keydown", closeOnEscape);
    return () => {
      document.removeEventListener("pointerdown", closeOnOutsidePointer);
      document.removeEventListener("keydown", closeOnEscape);
    };
  }, []);

  const summary = `${selectedTrackCount} ${selectedTrackCount === 1 ? "Track" : "Tracks"}${selectedDurationMs > 0 ? ` (${durationLabel(selectedDurationMs)})` : ""}`;
  if (comparisonVersions.length === 0) {
    return <span className="detail-modal-album-title-header">{summary}</span>;
  }

  return (
    <details className="detail-modal-album-expansion-summary" ref={detailsRef}>
      <summary>{summary}</summary>
      <div className="detail-modal-album-expansion-menu">
        {comparisonVersions.map((version) => {
          const trackDifference = Number(version.total_tracks ?? 0) - selectedTrackCount;
          const signedDifference = trackDifference > 0 ? `+${trackDifference}` : String(trackDifference);
          return (
            <span key={version.spotify_album_id}>
              <strong>{version.label}:</strong> {signedDifference} {Math.abs(trackDifference) === 1 ? "track" : "tracks"}
            </span>
          );
        })}
      </div>
    </details>
  );
}
