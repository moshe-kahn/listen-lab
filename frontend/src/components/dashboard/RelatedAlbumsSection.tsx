import type { AlbumFamilyContext, ArtistAlbumEntry } from "../../types/appTypes";

type RelatedAlbumsSectionProps = {
  context: AlbumFamilyContext | null;
  relatedAlbums?: ArtistAlbumEntry[];
  onSelect: (spotifyAlbumId: string) => void;
  onSelectAlbum?: (album: ArtistAlbumEntry) => void;
};

function albumMetaLabel(version: AlbumFamilyContext["versions"][number]) {
  const parts = [
    version.release_year,
    version.total_tracks == null ? null : `${version.total_tracks} ${version.total_tracks === 1 ? "track" : "tracks"}`,
  ].filter(Boolean);
  return parts.join(" · ");
}

function relatedAlbumKind(album: ArtistAlbumEntry) {
  const type = String(album.albumType ?? "").trim().toLocaleLowerCase();
  const name = album.name.trim().toLocaleLowerCase();
  if (/\b(remix|rmx)\b/.test(name)) {
    return "Remix";
  }
  if (/\blive\b/.test(name)) {
    return "Live";
  }
  if (type === "single" || /\bsingle\b/.test(name)) {
    return "Single";
  }
  if ((album.trackCount ?? 0) > 0 && (album.trackCount ?? 0) <= 6) {
    return "EP";
  }
  if (/\bdeluxe\b/.test(name)) {
    return "Deluxe";
  }
  if (/\banniversary\b/.test(name)) {
    return "Anniversary";
  }
  if (/\bremaster(?:ed)?\b/.test(name)) {
    return "Remaster";
  }
  return album.relationship === "appears_on" ? "Appears on" : "Album";
}

function relatedAlbumMeta(album: ArtistAlbumEntry) {
  const parts = [
    relatedAlbumKind(album),
    album.releaseYear,
    album.trackCount == null ? null : `${album.trackCount} ${album.trackCount === 1 ? "track" : "tracks"}`,
  ].filter(Boolean);
  return parts.join(" · ");
}

function isAlbumEdition(album: ArtistAlbumEntry) {
  const type = String(album.albumType ?? "").trim().toLocaleLowerCase();
  const name = album.name.trim().toLocaleLowerCase();
  if (album.relationship && album.relationship !== "album") {
    return false;
  }
  if (type === "single" || /\b(single|ep)\b/.test(name)) {
    return false;
  }
  return /\b(deluxe|expanded|anniversary|remaster(?:ed)?|reissue|edition|mono|stereo)\b/.test(name);
}

export function RelatedAlbumsSection({ context, relatedAlbums = [], onSelect, onSelectAlbum }: RelatedAlbumsSectionProps) {
  const relatedVersions = (context?.versions ?? []).filter((version) => (
    version.spotify_album_id !== context?.selected_spotify_album_id
  ));
  const relatedAlbumCards = relatedAlbums.filter((album) => Boolean(album.albumId || album.name));
  const relatedVersionAlbumCards = relatedAlbumCards.filter(isAlbumEdition);
  const otherRelatedAlbumCards = relatedAlbumCards.filter((album) => !isAlbumEdition(album));

  if (relatedVersions.length === 0 && relatedAlbumCards.length === 0) {
    return null;
  }

  return (
    <div className="detail-related-album-sections">
      {relatedVersions.length > 0 || relatedVersionAlbumCards.length > 0 ? (
        <section className="detail-related-albums" aria-label="Other album versions">
          <div className="detail-related-albums-header">
            <h3>Other Versions</h3>
          </div>
          <div className="detail-related-albums-list">
            {relatedVersions.map((version) => (
              <button
                className="detail-related-album-card"
                key={version.spotify_album_id}
                onClick={() => onSelect(version.spotify_album_id)}
                type="button"
              >
                {version.image_url ? (
                  <img alt="" src={version.image_url} />
                ) : (
                  <span className="detail-related-album-art-fallback" aria-hidden="true">
                    {(version.name || version.label).slice(0, 1).toUpperCase()}
                  </span>
                )}
                <span className="detail-related-album-copy">
                  <strong>{version.name}</strong>
                  <span>{version.label}</span>
                  {albumMetaLabel(version) ? <small>{albumMetaLabel(version)}</small> : null}
                </span>
              </button>
            ))}
            {relatedVersionAlbumCards.map((album) => (
              <button
                className="detail-related-album-card"
                key={album.albumId ?? album.name}
                onClick={() => onSelectAlbum?.(album)}
                type="button"
              >
                {album.imageUrl ? (
                  <img alt="" src={album.imageUrl} />
                ) : (
                  <span className="detail-related-album-art-fallback" aria-hidden="true">
                    {album.name.slice(0, 1).toUpperCase()}
                  </span>
                )}
                <span className="detail-related-album-copy">
                  <strong>{album.name}</strong>
                  <span>{album.artistName ?? relatedAlbumKind(album)}</span>
                  <small>{relatedAlbumMeta(album)}</small>
                </span>
              </button>
            ))}
          </div>
        </section>
      ) : null}
      {otherRelatedAlbumCards.length > 0 ? (
        <section className="detail-related-albums" aria-label="Related albums">
          <div className="detail-related-albums-header">
            <h3>Related Albums</h3>
          </div>
          <div className="detail-related-albums-list">
            {otherRelatedAlbumCards.map((album) => (
              <button
                className="detail-related-album-card"
                key={album.albumId ?? album.name}
                onClick={() => onSelectAlbum?.(album)}
                type="button"
              >
                {album.imageUrl ? (
                  <img alt="" src={album.imageUrl} />
                ) : (
                  <span className="detail-related-album-art-fallback" aria-hidden="true">
                    {album.name.slice(0, 1).toUpperCase()}
                  </span>
                )}
                <span className="detail-related-album-copy">
                  <strong>{album.name}</strong>
                  <span>{album.artistName ?? relatedAlbumKind(album)}</span>
                  <small>{relatedAlbumMeta(album)}</small>
                </span>
              </button>
            ))}
          </div>
        </section>
      ) : null}
    </div>
  );
}
