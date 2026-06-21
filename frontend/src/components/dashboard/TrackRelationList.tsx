import type { RecordingRelationRows, RecordingTrackCandidateMember } from "../../types/appTypes";

type TrackRelationListProps = {
  relatedTracks: RecordingRelationRows["songFamily"];
  albumImageForMember: (member: RecordingTrackCandidateMember) => string | null;
  albumNameForMember: (member: RecordingTrackCandidateMember) => string | null;
  releaseYearForMember: (member: RecordingTrackCandidateMember) => string | null;
  onOpenRelatedTrack: (member: RecordingTrackCandidateMember) => void;
};

function uniqueArtistNames(names: Array<string | null | undefined>) {
  const seen = new Set<string>();
  return names
    .map((name) => name?.trim() ?? "")
    .filter((name) => {
      const key = name.toLocaleLowerCase();
      if (!key || seen.has(key)) {
        return false;
      }
      seen.add(key);
      return true;
    });
}

export function TrackRelationList({
  relatedTracks,
  albumImageForMember,
  albumNameForMember,
  releaseYearForMember,
  onOpenRelatedTrack,
}: TrackRelationListProps) {
  return (
    <div className="detail-relation-list">
      <div className="detail-relation-rows">
        {relatedTracks.map(({ member, badge, qualifier, originalArtists }) => {
          const albumImageUrl = albumImageForMember(member);
          const albumName = albumNameForMember(member) || "Unknown album";
          const originalArtistNames = new Set(
            originalArtists.map((artist) => artist.name?.trim().toLocaleLowerCase()).filter(Boolean),
          );
          const addedArtists = uniqueArtistNames((member.artists ?? []).map((artist) => artist.name))
            .filter((name) => !originalArtistNames.has(name.toLocaleLowerCase()));
          const fallbackArtistNames = uniqueArtistNames(String(member.artist ?? "").split("|"))
            .filter((name) => !originalArtistNames.has(name.toLocaleLowerCase()));
          const relatedArtistText = (addedArtists.length > 0 ? addedArtists : fallbackArtistNames).join(", ");
          const fullArtistNames = uniqueArtistNames((member.artists ?? []).map((artist) => artist.name));
          const displayArtistText = badge === "Original"
            ? fullArtistNames.join(", ") || String(member.artist ?? "").split("|").map((name) => name.trim()).filter(Boolean).join(", ")
            : relatedArtistText || fullArtistNames.join(", ");
          const yearAlbum = [releaseYearForMember(member), albumName].filter(Boolean).join(" - ");
          return (
            <button
              className="detail-relation-row"
              key={`cover-remix-row-${member.release_track_id}`}
              onClick={() => onOpenRelatedTrack(member)}
              type="button"
            >
              <span className="detail-relation-art">
                {albumImageUrl ? (
                  <img alt="" src={albumImageUrl} />
                ) : (
                  <span aria-hidden="true">{(albumName || member.title || "?").slice(0, 1).toUpperCase()}</span>
                )}
              </span>
              <span className="detail-relation-copy">
                <strong className="single-line-ellipsis">
                  <span className="detail-relation-badge">{badge}</span>
                  {qualifier ? <span className="detail-relation-qualifier">{qualifier}</span> : null}
                </strong>
                {displayArtistText ? <span className="detail-relation-artist-line single-line-ellipsis">{displayArtistText}</span> : null}
                <span className="single-line-ellipsis">{yearAlbum}</span>
              </span>
            </button>
          );
        })}
      </div>
    </div>
  );
}
