import type { PlaylistMembership, RecentTrack, RecordingTrackCandidateMember } from "../../types/appTypes";

type HomeAlbumAppearance = {
  key: string;
  name: string;
  imageUrl: string | null;
  year: string | null;
  releaseType: string | null;
  onClick: () => void;
};

type HomeAlbumAppearanceStripProps = {
  currentAlbum: {
    name: string | null;
    imageUrl: string | null;
    year: string | null;
    albumType: string | null;
    onClick: () => void;
  };
  recordingMembers: RecordingTrackCandidateMember[];
  recordingMemberAlbumImageUrl: (member: RecordingTrackCandidateMember) => string | null;
  recordingMemberAlbumName: (member: RecordingTrackCandidateMember) => string | null;
  recordingMemberReleaseYear: (member: RecordingTrackCandidateMember) => string | null;
  onMemberClick: (member: RecordingTrackCandidateMember) => void;
  onPlaylistClick: (membership: PlaylistMembership) => void;
  playlistMemberships: PlaylistMembership[];
  sourceTrack: RecentTrack | null;
};

function releaseTypeLabel(rawType: string | null | undefined, albumName: string | null | undefined) {
  const type = String(rawType ?? "").trim().toLocaleLowerCase();
  const name = String(albumName ?? "").trim().toLocaleLowerCase();
  if (type === "single") {
    return "Single";
  }
  if (type === "compilation") {
    return "Compilation";
  }
  if (/\b(soundtrack|ost|original score|motion picture|bande originale|bo du film)\b/.test(name)) {
    return "Soundtrack";
  }
  return type === "album" ? null : type ? type.replace(/^\w/, (char) => char.toLocaleUpperCase()) : null;
}

function preferredAlbumType(member: RecordingTrackCandidateMember) {
  const directVersion = member.album_versions?.find((version) => Boolean(version.spotify_album_id && version.is_direct_source_album))
    ?? member.album_versions?.find((version) => Boolean(version.spotify_album_id))
    ?? member.album_versions?.[0]
    ?? null;
  return directVersion?.album_type ?? member.album_types?.[0] ?? null;
}

export function HomeAlbumAppearanceStrip({
  currentAlbum,
  recordingMembers,
  recordingMemberAlbumImageUrl,
  recordingMemberAlbumName,
  recordingMemberReleaseYear,
  onMemberClick,
  onPlaylistClick,
  playlistMemberships,
  sourceTrack,
}: HomeAlbumAppearanceStripProps) {
  const appearances: HomeAlbumAppearance[] = [];
  if (currentAlbum.name) {
    appearances.push({
      key: `current-${sourceTrack?.album_id ?? currentAlbum.name}`,
      name: currentAlbum.name,
      imageUrl: currentAlbum.imageUrl,
      year: currentAlbum.year,
      releaseType: releaseTypeLabel(currentAlbum.albumType, currentAlbum.name),
      onClick: currentAlbum.onClick,
    });
  }

  for (const member of recordingMembers) {
    const albumName = recordingMemberAlbumName(member);
    const dedupeKey = `${member.release_track_id}-${albumName ?? ""}`;
    if (!albumName || appearances.some((appearance) => appearance.name.trim().toLocaleLowerCase() === albumName.trim().toLocaleLowerCase())) {
      continue;
    }
    appearances.push({
      key: dedupeKey,
      name: albumName,
      imageUrl: recordingMemberAlbumImageUrl(member),
      year: recordingMemberReleaseYear(member),
      releaseType: releaseTypeLabel(preferredAlbumType(member), albumName),
      onClick: () => onMemberClick(member),
    });
  }

  const uniquePlaylistMemberships = Array.from(
    playlistMemberships.reduce((items, membership) => {
      if (!items.has(membership.playlist_id)) {
        items.set(membership.playlist_id, membership);
      }
      return items;
    }, new Map<string, PlaylistMembership>()).values(),
  ).slice(0, 8);

  if (appearances.length === 0 && uniquePlaylistMemberships.length === 0) {
    return null;
  }

  return (
    <div className="player-home-appearances" aria-label="Albums and playlists for this track">
      {appearances.length > 0 ? (
        <div className="player-home-appearance-section">
          <span className="player-home-appearance-label">Albums</span>
          <div className="player-home-album-strip">
            {appearances.map((appearance) => {
              const tags = [appearance.year, appearance.releaseType].filter(Boolean);
              return (
                <button className="player-home-album-card" key={appearance.key} onClick={appearance.onClick} type="button">
                  <span className="player-home-album-card-art">
                    {appearance.imageUrl ? (
                      <img alt="" src={appearance.imageUrl} />
                    ) : (
                      <span className="player-home-album-card-fallback" aria-hidden="true">
                        {appearance.name.slice(0, 1).toUpperCase()}
                      </span>
                    )}
                    {tags.length > 0 ? (
                      <span className="player-home-album-card-tags">
                        {tags.map((tag) => <span key={tag}>{tag}</span>)}
                      </span>
                    ) : null}
                  </span>
                  <span className="player-home-album-card-title single-line-ellipsis">{appearance.name}</span>
                </button>
              );
            })}
          </div>
        </div>
      ) : null}
      {uniquePlaylistMemberships.length > 0 ? (
        <div className="player-home-appearance-section">
          <span className="player-home-appearance-label">Playlists</span>
          <div className="player-home-playlist-strip">
            {uniquePlaylistMemberships.map((membership) => (
              <button className="player-home-playlist-card" key={membership.playlist_id} onClick={() => onPlaylistClick(membership)} type="button">
                {membership.playlist_image_url ? (
                  <img alt="" className="player-home-playlist-card-image" src={membership.playlist_image_url} />
                ) : (
                  <span className="player-home-playlist-card-image player-home-playlist-card-fallback" aria-hidden="true">
                    {(membership.playlist_name ?? "P").slice(0, 1).toUpperCase()}
                  </span>
                )}
                <span className="single-line-ellipsis">{membership.playlist_name ?? "Untitled playlist"}</span>
              </button>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}
