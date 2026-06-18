import type { RecordingRelationRows, RecordingTrackCandidateMember } from "../../types/appTypes";

type TrackRelationListProps = {
  relatedTracks: RecordingRelationRows["songFamily"];
  albumImageForMember: (member: RecordingTrackCandidateMember) => string | null;
  releaseYearForMember: (member: RecordingTrackCandidateMember) => string | null;
  onOpenRelatedTrack: (member: RecordingTrackCandidateMember) => void;
};

export function TrackRelationList({
  relatedTracks,
  albumImageForMember,
  releaseYearForMember,
  onOpenRelatedTrack,
}: TrackRelationListProps) {
  return (
    <div className="detail-relation-list">
      <div className="detail-relation-rows">
        {relatedTracks.map(({ member, badge, originalArtists }) => {
          const albumImageUrl = albumImageForMember(member);
          const originalArtistNames = new Set(
            originalArtists.map((artist) => artist.name?.trim().toLocaleLowerCase()).filter(Boolean),
          );
          const addedArtists = (member.artists ?? [])
            .map((artist) => artist.name?.trim())
            .filter((name): name is string => Boolean(name && !originalArtistNames.has(name.toLocaleLowerCase())));
          const fallbackArtistNames = String(member.artist ?? "")
            .split("|")
            .map((name) => name.trim())
            .filter((name) => name && !originalArtistNames.has(name.toLocaleLowerCase()));
          const relatedArtistText = (addedArtists.length > 0 ? addedArtists : fallbackArtistNames).join(", ");
          const fullArtistNames = (member.artists ?? [])
            .map((artist) => artist.name?.trim())
            .filter((name): name is string => Boolean(name));
          const displayArtistText = badge === "Original"
            ? fullArtistNames.join(", ") || String(member.artist ?? "").split("|").map((name) => name.trim()).filter(Boolean).join(", ")
            : relatedArtistText || fullArtistNames.join(", ");
          const yearAlbum = [releaseYearForMember(member), member.album || "Unknown album"].filter(Boolean).join(" - ");
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
                  <span aria-hidden="true">{(member.album || member.title || "?").slice(0, 1).toUpperCase()}</span>
                )}
              </span>
              <span className="detail-relation-copy">
                <strong className="single-line-ellipsis">
                  <span className="detail-relation-badge">{badge}</span>
                  {displayArtistText ? <span className="detail-relation-artist">{displayArtistText}</span> : null}
                </strong>
                <span className="single-line-ellipsis">{yearAlbum}</span>
              </span>
            </button>
          );
        })}
      </div>
    </div>
  );
}
