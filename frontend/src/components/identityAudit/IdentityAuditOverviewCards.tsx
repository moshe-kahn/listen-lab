type OverviewCard = {
  title: string;
  copy: string;
  value: number | string;
};

function IdentityAuditOverviewGrid({ cards }: { cards: OverviewCard[] }) {
  return (
    <div className="identity-audit-overview-grid">
      {cards.map((card) => (
        <article className="identity-audit-overview-card" key={`identity-overview-${card.title}`}>
          <h3>{card.title}</h3>
          <p>{card.copy}</p>
          <strong>{card.value}</strong>
        </article>
      ))}
    </div>
  );
}

type TrackIdentityAuditOverviewCardsProps = {
  canonicalCount: number;
  releaseCount: number;
  compositionCount: number;
  suggestedCount: number;
  ambiguousCount: number;
};

export function TrackIdentityAuditOverviewCards({
  canonicalCount,
  releaseCount,
  compositionCount,
  suggestedCount,
  ambiguousCount,
}: TrackIdentityAuditOverviewCardsProps) {
  return (
    <IdentityAuditOverviewGrid
      cards={[
        {
          title: "Suspicious Splits",
          copy: "Tracks that may be split across multiple Spotify IDs.",
          value: canonicalCount,
        },
        {
          title: "Source Mapping Issues",
          copy: "Source tracks that may be folded together incorrectly.",
          value: releaseCount,
        },
        {
          title: "Recording Groups",
          copy: "Release tracks grouped as possible recording versions.",
          value: compositionCount,
        },
        {
          title: "Ready To Review",
          copy: "Conservative title/artist matches ready for judgment.",
          value: suggestedCount,
        },
        {
          title: "Needs Review",
          copy: "Items requiring human judgment across variant-rule families.",
          value: ambiguousCount,
        },
      ]}
    />
  );
}

type AlbumIdentityAuditOverviewCardsProps = {
  duplicateAlbumCount: number;
  duplicateNameArtistCount: number;
  previewedCount: number;
  dryRunCount: number;
};

export function AlbumIdentityAuditOverviewCards({
  duplicateAlbumCount,
  duplicateNameArtistCount,
  previewedCount,
  dryRunCount,
}: AlbumIdentityAuditOverviewCardsProps) {
  return (
    <IdentityAuditOverviewGrid
      cards={[
        {
          title: "Likely Duplicate Albums",
          copy: "Album rows with strong Spotify-backed duplicate evidence.",
          value: duplicateAlbumCount,
        },
        {
          title: "Name Conflicts",
          copy: "Same album and artist text, but weaker provider evidence.",
          value: duplicateNameArtistCount,
        },
        {
          title: "Merge Previews",
          copy: "Album groups already previewed in this session.",
          value: previewedCount,
        },
        {
          title: "Dry Runs",
          copy: "Groups with a row-level dry-run plan available.",
          value: dryRunCount,
        },
      ]}
    />
  );
}
