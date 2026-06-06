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
          copy: "Same normalized title/artist with multiple Spotify IDs.",
          value: canonicalCount,
        },
        {
          title: "Ambiguous Mappings",
          copy: "Multiple source tracks folded under a single release track.",
          value: releaseCount,
        },
        {
          title: "Grouping Concerns",
          copy: "Release tracks grouped together for analysis.",
          value: compositionCount,
        },
        {
          title: "Suggested Matches",
          copy: "Conservative title/artist matches awaiting review.",
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
          title: "Duplicate Albums",
          copy: "Strongest album duplicate signal using one resolved Spotify album.",
          value: duplicateAlbumCount,
        },
        {
          title: "Duplicate Name + Artist",
          copy: "Weaker text-based album duplicate signal when Spotify ID is missing or mixed.",
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
