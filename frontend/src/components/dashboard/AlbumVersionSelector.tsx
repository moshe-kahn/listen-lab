import { useEffect, useRef } from "react";

import type { AlbumFamilyVersion } from "../../types/appTypes";

type AlbumVersionSelectorProps = {
  onSelect: (spotifyAlbumId: string) => void;
  selectedSpotifyAlbumId: string;
  versions: AlbumFamilyVersion[];
};

function albumLengthLabel(durationMs: number | null) {
  if (durationMs == null) {
    return null;
  }
  return `${Math.round(durationMs / 60_000)} min`;
}

function versionSummary(version: AlbumFamilyVersion) {
  const parts = [
    version.release_year,
    version.total_tracks == null ? null : `${version.total_tracks} ${version.total_tracks === 1 ? "track" : "tracks"}`,
    albumLengthLabel(version.total_duration_ms),
  ].filter(Boolean);
  return parts.join(" · ");
}

function compactVersionLabel(label: string) {
  return label.replace(/\s+Edition$/i, "");
}

export function AlbumVersionSelector({ onSelect, selectedSpotifyAlbumId, versions }: AlbumVersionSelectorProps) {
  const detailsRef = useRef<HTMLDetailsElement>(null);
  const selectedVersion = versions.find((version) => version.spotify_album_id === selectedSpotifyAlbumId) ?? versions[0];

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

  if (!selectedVersion) {
    return null;
  }

  return (
    <details className="detail-modal-album-version-control" ref={detailsRef}>
      <summary aria-label={`Current album version: ${selectedVersion.label}`}>{compactVersionLabel(selectedVersion.label)}</summary>
      <div className="detail-modal-album-version-menu" role="menu">
        {versions.map((version) => (
          <button
            aria-current={version.spotify_album_id === selectedSpotifyAlbumId ? "true" : undefined}
            className="detail-modal-album-version-option"
            key={version.spotify_album_id}
            onClick={() => {
              detailsRef.current?.removeAttribute("open");
              onSelect(version.spotify_album_id);
            }}
            role="menuitem"
            type="button"
          >
            {version.image_url ? (
              <img alt="" src={version.image_url} />
            ) : (
              <span className="detail-modal-album-version-art-fallback" aria-hidden="true">{version.label.slice(0, 1)}</span>
            )}
            <span className="detail-modal-album-version-copy">
              <strong>{version.menu_label || version.label}</strong>
              <span>{versionSummary(version)}</span>
            </span>
          </button>
        ))}
      </div>
    </details>
  );
}
