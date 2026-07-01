export type TrackIdentityAuditExample = Record<string, unknown>;

export function identityAuditTitle(example: TrackIdentityAuditExample): string {
  const candidates = [
    example.track_name,
    example.release_track_name,
    example.analysis_track_name,
    example.artist_name,
    example.grouping_note,
    example.example_type,
  ];
  const title = candidates.find((value) => typeof value === "string" && value.trim().length > 0);
  return typeof title === "string" ? title : "Identity example";
}

export function identityAuditMeta(example: TrackIdentityAuditExample): string {
  const parts = [
    typeof example.artist_name === "string" ? example.artist_name : null,
    typeof example.listen_count === "number" ? `${example.listen_count} listens` : null,
    typeof example.folded_listen_count === "number" ? `${example.folded_listen_count} folded listens` : null,
    typeof example.spotify_track_id_count === "number" ? `${example.spotify_track_id_count} Spotify IDs` : null,
    typeof example.source_track_count === "number" ? `${example.source_track_count} source tracks` : null,
    typeof example.release_track_count === "number" ? `${example.release_track_count} release tracks` : null,
  ];
  return parts.filter(Boolean).join(" | ");
}

export function auditString(value: unknown, fallback: string = "Unknown") {
  return typeof value === "string" && value.trim().length > 0 ? value : fallback;
}

export function auditNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

export function auditList(value: unknown): TrackIdentityAuditExample[] {
  return Array.isArray(value) ? value.filter((item): item is TrackIdentityAuditExample => Boolean(item) && typeof item === "object" && !Array.isArray(item)) : [];
}

function renderAuditStat(label: string, value: unknown) {
  if (value == null || value === "") {
    return null;
  }
  return (
    <span className="identity-audit-stat">
      <span>{label}</span>
      <strong>{String(value)}</strong>
    </span>
  );
}

function renderAuditVariantList(items: TrackIdentityAuditExample[], kind: "canonical" | "release" | "composition") {
  if (items.length === 0) {
    return <p className="empty-copy">No variants returned.</p>;
  }
  return (
    <div className="identity-audit-variant-list">
      {items.map((item, index) => {
        const title = kind === "composition"
          ? auditString(item.release_track_name, "Release track")
          : auditString(item.track_name ?? item.source_name_raw, "Variant");
        const subtitle = kind === "canonical"
          ? auditString(item.album_name, "Unknown album")
          : kind === "release"
            ? auditString(item.match_method, "Mapping")
            : auditString(item.status, "Suggestion");
        const listens = auditNumber(item.listen_count);
        const confidence = auditNumber(item.confidence);
        const idText = kind === "composition"
          ? `release ${auditString(item.release_track_id, "n/a")}`
          : auditString(item.spotify_track_id ?? item.external_id, "No Spotify ID");
        return (
          <div className="identity-audit-variant" key={`${idText}-${index}`}>
            <div className="identity-audit-variant-main">
              <strong>{title}</strong>
              <span>{subtitle}</span>
              <code>{idText}</code>
            </div>
            <div className="identity-audit-variant-stats">
              {listens != null ? <span>{listens} listens</span> : null}
              {confidence != null ? <span>{Math.round(confidence * 100)}% confidence</span> : null}
              {typeof item.source_track_count === "number" ? <span>{item.source_track_count} sources</span> : null}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function identityAuditActionLabel(kind: "canonical" | "release" | "composition") {
  if (kind === "canonical") {
    return "Review whether the Spotify IDs are separate versions or the same recording.";
  }
  if (kind === "release") {
    return "Review source-track mappings before folding source history together.";
  }
  return "Review whether these release tracks belong in the same recording group.";
}

function countLabel(value: unknown, fallback: string) {
  const count = auditNumber(value);
  return count == null ? fallback : String(count);
}

function identityAuditWhyLabel(example: TrackIdentityAuditExample, kind: "canonical" | "release" | "composition") {
  if (kind === "canonical") {
    return `${countLabel(example.spotify_track_id_count, "Multiple")} Spotify IDs share the same normalized title/artist.`;
  }
  if (kind === "release") {
    return `${countLabel(example.source_track_count, "Multiple")} source tracks are folded into one release track.`;
  }
  return `${countLabel(example.release_track_count, "Multiple")} release tracks are grouped for analysis.`;
}

export function renderIdentityAuditExample(example: TrackIdentityAuditExample, index: number) {
  const exampleType = auditString(example.example_type, "identity");
  const isCanonical = exampleType === "same_name_canonical_split";
  const isRelease = exampleType === "release_track_source_split";
  const isComposition = exampleType === "analysis_track_group";
  const title = identityAuditTitle(example);
  const meta = identityAuditMeta(example);
  const variantItems = isCanonical
    ? auditList(example.variants)
    : isRelease
      ? auditList(example.source_tracks)
      : auditList(example.release_tracks);
  const variantKind = isCanonical ? "canonical" : isRelease ? "release" : "composition";

  return (
    <article className="identity-audit-example" key={`${exampleType}-${title}-${index}`}>
      <div className="identity-audit-example-header">
        <div>
          <h4>{title}</h4>
          {meta ? <p>{meta}</p> : null}
        </div>
        <span className="identity-audit-type-badge">
          {isCanonical ? "Canonical" : isRelease ? "Release" : "Composition"}
        </span>
      </div>
      <div className="identity-audit-card-summary">
        <div>
          <span>What happened</span>
          <strong>{title}</strong>
        </div>
        <div>
          <span>Why flagged</span>
          <strong>{identityAuditWhyLabel(example, variantKind)}</strong>
        </div>
        <div>
          <span>Action</span>
          <strong>{identityAuditActionLabel(variantKind)}</strong>
        </div>
      </div>
      <div className="identity-audit-stats">
        {renderAuditStat("Spotify IDs", example.spotify_track_id_count)}
        {renderAuditStat("Sources", example.source_track_count)}
        {renderAuditStat("Release tracks", example.release_track_count)}
        {renderAuditStat("Folded listens", example.folded_listen_count)}
        {renderAuditStat("First listened", example.first_listened_at)}
        {renderAuditStat("Last listened", example.last_listened_at)}
      </div>
      {typeof example.grouping_note === "string" ? (
        <p className="identity-audit-note">{example.grouping_note}</p>
      ) : null}
      <details className="identity-audit-details">
        <summary>Details</summary>
        {renderAuditVariantList(variantItems, variantKind)}
      </details>
    </article>
  );
}

export function renderIdentityAuditGroup(title: string, examples: TrackIdentityAuditExample[]) {
  return (
    <div className="identity-audit-group">
      <div className="tracks-formula-heading">
        <h3>{title}</h3>
        <span>{examples.length} examples</span>
      </div>
      {examples.length === 0 ? (
        <p className="empty-copy">No examples returned for this group.</p>
      ) : (
        <div className="identity-audit-examples">
          {examples.map((example, index) => renderIdentityAuditExample(example, index))}
        </div>
      )}
    </div>
  );
}
