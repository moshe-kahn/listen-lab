import type { RecentTrack, RecordingTrackCandidateMember } from "../../types/appTypes";

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

  if (appearances.length === 0) {
    return null;
  }

  return (
    <div className="player-home-album-strip" aria-label="Albums this track appears on">
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
  );
}
