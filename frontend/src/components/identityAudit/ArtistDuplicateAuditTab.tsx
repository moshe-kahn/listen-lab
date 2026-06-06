import { useEffect, useMemo, useState } from "react";

import { cleanupArtistCompositeCredits, fetchArtistDuplicateAudit, repairArtistDuplicates } from "../../api/appApi";
import type {
  ArtistCompositeCreditCleanupResponse,
  ArtistDuplicateAuditArtist,
  ArtistDuplicateAuditResponse,
  ArtistDuplicateRepairResponse,
  ArtistExactDuplicateGroup,
  ArtistSimilarSameAlbumGroup,
  ArtistStylizationDuplicateGroup,
} from "../../types/appTypes";

type ArtistAuditCategory =
  | "exact_name"
  | "exact_name_identity_evidence_safe_repair"
  | "exact_name_album_title_provider_context_safe_repair"
  | "exact_name_only_review"
  | "exact_name_no_provider_review_only"
  | "ambiguous_provider_review_only"
  | "exact_name_orphan_placeholder_review"
  | "stylization"
  | "similar_same_album"
  | "composite_credit";

const categoryLabels: Record<ArtistAuditCategory, string> = {
  exact_name: "All Exact",
  exact_name_identity_evidence_safe_repair: "Safe Evidence",
  exact_name_album_title_provider_context_safe_repair: "Safe Album Title",
  exact_name_only_review: "Exact Review",
  exact_name_no_provider_review_only: "Text Only",
  ambiguous_provider_review_only: "Multi-Provider",
  exact_name_orphan_placeholder_review: "Orphans",
  stylization: "Stylization",
  similar_same_album: "Same Album",
  composite_credit: "Composite Credit",
};

const categoryOrder: ArtistAuditCategory[] = [
  "exact_name",
  "exact_name_identity_evidence_safe_repair",
  "exact_name_album_title_provider_context_safe_repair",
  "exact_name_only_review",
  "exact_name_no_provider_review_only",
  "ambiguous_provider_review_only",
  "exact_name_orphan_placeholder_review",
  "stylization",
  "similar_same_album",
  "composite_credit",
];

const exactNameCategories = new Set<ArtistAuditCategory>([
  "exact_name",
  "exact_name_identity_evidence_safe_repair",
  "exact_name_album_title_provider_context_safe_repair",
  "exact_name_only_review",
  "exact_name_no_provider_review_only",
  "ambiguous_provider_review_only",
  "exact_name_orphan_placeholder_review",
]);

const groupCategoryLabels: Record<string, string> = {
  exact_name_identity_evidence_safe_repair: "safe identity evidence",
  exact_name_album_title_provider_context_safe_repair: "safe album-title evidence",
  exact_name_album_title_evidence_safe_repair: "safe album-title evidence",
  exact_name_only_review: "exact-name review",
  exact_name_no_provider_review_only: "text-only review",
  exact_name_orphan_placeholder_review: "orphan review",
  ambiguous_provider_review_only: "multi-provider review",
  same_normalized_name_provider_with_identity_evidence: "safe identity evidence",
  same_normalized_name_provider_with_album_title_context: "safe album-title evidence",
};

const evidenceLabels: Record<string, string> = {
  shared_release_album_id: "shared album id",
  shared_release_track_id: "shared track id",
  reconciled_source_album: "reconciled album",
  reconciled_source_track: "reconciled track",
  shared_normalized_album_title_with_provider_context: "same album title",
  shared_normalized_album_title: "same album title",
};

function readableGroupCategory(value?: string | null) {
  if (!value) {
    return "review";
  }
  return groupCategoryLabels[value] ?? value.split("_").join(" ");
}

function spotifyArtistUrl(artist: ArtistDuplicateAuditArtist) {
  const spotifySource = artist.source_artist_maps.find((source) => source.source_name === "spotify");
  if (spotifySource?.external_uri?.startsWith("spotify:artist:")) {
    return `https://open.spotify.com/artist/${encodeURIComponent(spotifySource.external_uri.split(":").pop() ?? "")}`;
  }
  if (spotifySource?.external_id) {
    return `https://open.spotify.com/artist/${encodeURIComponent(spotifySource.external_id)}`;
  }
  return null;
}

function artistSourceLabel(artist: ArtistDuplicateAuditArtist) {
  if (artist.provider_source_ids.length > 0) {
    return artist.provider_source_ids.map((source) => `${source.source_name}:${source.external_id}`).join(", ");
  }
  if (artist.text_only) {
    return "text-only";
  }
  return "unmapped";
}

function repeatedIdsByArtist(values: Array<{ artistId: number; id: number }>) {
  const artistIdsByValue = new Map<number, Set<number>>();
  for (const value of values) {
    const artistIds = artistIdsByValue.get(value.id) ?? new Set<number>();
    artistIds.add(value.artistId);
    artistIdsByValue.set(value.id, artistIds);
  }
  return new Set([...artistIdsByValue.entries()].filter(([, artistIds]) => artistIds.size > 1).map(([value]) => value));
}

function repeatedNamesByArtist(values: Array<{ artistId: number; name: string | null }>) {
  const artistIdsByName = new Map<string, Set<number>>();
  for (const value of values) {
    if (!value.name) {
      continue;
    }
    const artistIds = artistIdsByName.get(value.name) ?? new Set<number>();
    artistIds.add(value.artistId);
    artistIdsByName.set(value.name, artistIds);
  }
  return new Set([...artistIdsByName.entries()].filter(([, artistIds]) => artistIds.size > 1).map(([value]) => value));
}

function inventoryLabel(kind: "album" | "track", count: number) {
  return `${count} ${kind}${count === 1 ? "" : "s"}`;
}

function renderArtistRows(artists: ArtistDuplicateAuditArtist[], canonicalArtistId?: number | null) {
  const commonAlbumIds = repeatedIdsByArtist(
    artists.flatMap((artist) => artist.albums.map((album) => ({ artistId: artist.artist_id, id: album.release_album_id }))),
  );
  const commonAlbumNames = repeatedNamesByArtist(
    artists.flatMap((artist) => artist.albums.map((album) => ({ artistId: artist.artist_id, name: album.normalized_name }))),
  );
  const commonTrackIds = repeatedIdsByArtist(
    artists.flatMap((artist) => artist.tracks.map((track) => ({ artistId: artist.artist_id, id: track.release_track_id }))),
  );

  return (
    <div className="identity-audit-variant-list">
      {artists.map((artist) => {
        const visibleAlbums = artist.albums.slice(0, 3);
        const visibleTracks = artist.tracks.slice(0, 3);
        return (
        <div className="identity-audit-variant" key={`artist-duplicate-row-${artist.artist_id}`}>
          <div className="identity-audit-variant-main">
            <div className="artist-audit-name-row">
              {spotifyArtistUrl(artist) ? (
                <a href={spotifyArtistUrl(artist) ?? undefined} rel="noreferrer" target="_blank">
                  {artist.display_name || "Unknown artist"}
                </a>
              ) : (
                <strong>{artist.display_name || "Unknown artist"}</strong>
              )}
              <span>
                artist {artist.artist_id}
                {canonicalArtistId === artist.artist_id ? " · canonical" : ""}
              </span>
            </div>
            <code>{artistSourceLabel(artist)}</code>
            <div className="artist-audit-inline-inventory">
              <div className="artist-audit-inline-inventory-row">
                <span className="artist-audit-inventory-label">Albums</span>
                {visibleAlbums.length === 0 ? <span className="artist-audit-inventory-empty">none</span> : null}
                {visibleAlbums.map((album) => {
                  const isCommon = commonAlbumIds.has(album.release_album_id) || commonAlbumNames.has(album.normalized_name ?? "");
                  return (
                    <span
                      className={`artist-audit-inventory-chip${isCommon ? " artist-audit-common-link" : ""}`}
                      key={`artist-row-album-${artist.artist_id}-${album.release_album_id}`}
                    >
                      {album.album_name} · {album.release_album_id}
                    </span>
                  );
                })}
                {artist.albums.length > visibleAlbums.length ? <span className="artist-audit-inventory-empty">+{artist.albums.length - visibleAlbums.length} more</span> : null}
              </div>
              <div className="artist-audit-inline-inventory-row">
                <span className="artist-audit-inventory-label">Tracks</span>
                {visibleTracks.length === 0 ? <span className="artist-audit-inventory-empty">none</span> : null}
                {visibleTracks.map((track) => {
                  const isCommon = commonTrackIds.has(track.release_track_id);
                  return (
                    <span
                      className={`artist-audit-inventory-chip${isCommon ? " artist-audit-common-link" : ""}`}
                      key={`artist-row-track-${artist.artist_id}-${track.release_track_id}`}
                    >
                      {track.track_name} · {track.release_track_id}
                    </span>
                  );
                })}
                {artist.tracks.length > visibleTracks.length ? <span className="artist-audit-inventory-empty">+{artist.tracks.length - visibleTracks.length} more</span> : null}
              </div>
            </div>
          </div>
          <div className="identity-audit-variant-stats">
            <span className={artist.provider_backed ? "recording-track-good" : "recording-track-muted"}>
              {artist.provider_backed ? "provider-backed" : "text-only"}
            </span>
            <span>{inventoryLabel("album", artist.album_artist_link_count)}</span>
            <span>{inventoryLabel("track", artist.track_artist_link_count)}</span>
          </div>
        </div>
      );})}
    </div>
  );
}

function repairCount(plan: ArtistDuplicateRepairResponse | null) {
  if (!plan) {
    return 0;
  }
  return (
    plan.source_mappings_to_move.length
    + plan.source_mappings_to_delete.length
    + plan.album_links_to_move.length
    + plan.album_links_to_delete.length
    + plan.track_links_to_move.length
    + plan.track_links_to_delete.length
    + plan.artist_rows_to_delete.length
  );
}

function evidenceSummary(plan: ArtistDuplicateRepairResponse | null) {
  const counts = plan?.evidence_type_counts ?? {};
  const orderedTypes = [
    "shared_release_album_id",
    "shared_release_track_id",
    "reconciled_source_album",
    "reconciled_source_track",
    "shared_normalized_album_title_with_provider_context",
    "shared_normalized_album_title",
  ];
  return orderedTypes
    .filter((type) => counts[type])
    .map((type) => `${type}: ${counts[type]}`)
    .join("\n");
}

export function ArtistDuplicateAuditTab() {
  const [audit, setAudit] = useState<ArtistDuplicateAuditResponse | null>(null);
  const [repairPlan, setRepairPlan] = useState<ArtistDuplicateRepairResponse | null>(null);
  const [compositeCleanupPlan, setCompositeCleanupPlan] = useState<ArtistCompositeCreditCleanupResponse | null>(null);
  const [category, setCategory] = useState<ArtistAuditCategory>("exact_name");
  const [loading, setLoading] = useState(false);
  const [repairLoading, setRepairLoading] = useState(false);
  const [compositeCleanupLoading, setCompositeCleanupLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [repairError, setRepairError] = useState<string | null>(null);
  const [compositeCleanupError, setCompositeCleanupError] = useState<string | null>(null);
  const [lastLoadedAt, setLastLoadedAt] = useState<string | null>(null);
  const [lastRepairAt, setLastRepairAt] = useState<string | null>(null);

  async function loadAudit() {
    setLoading(true);
    setError(null);
    try {
      const payload = await fetchArtistDuplicateAudit();
      setAudit(payload);
      setLastLoadedAt(new Date().toISOString());
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : "Failed to load artist duplicate audit.");
    } finally {
      setLoading(false);
    }
  }

  async function runRepair(dryRun: boolean) {
    if (!dryRun) {
      const summary = evidenceSummary(repairPlan) || "No evidence-backed changes are currently planned.";
      const ok = window.confirm(
        `Apply safe artist duplicate repair?\n\nOnly exact-name groups with evidence are modified. Same-name-only, stylization, and similar-name groups are not modified.\n\nEvidence counts:\n${summary}`,
      );
      if (!ok) {
        return;
      }
    }
    setRepairLoading(true);
    setRepairError(null);
    try {
      const payload = await repairArtistDuplicates(dryRun);
      setRepairPlan(payload);
      setLastRepairAt(new Date().toISOString());
      if (!dryRun) {
        await loadAudit();
      }
    } catch (loadError) {
      setRepairError(loadError instanceof Error ? loadError.message : "Failed to run artist duplicate repair.");
    } finally {
      setRepairLoading(false);
    }
  }

  async function runCompositeCleanup(dryRun: boolean) {
    if (!dryRun) {
      const albumDeleteCount = compositeCleanupPlan?.album_links_to_delete.length ?? 0;
      const trackDeleteCount = compositeCleanupPlan?.track_links_to_delete.length ?? 0;
      const ok = window.confirm(
        `Apply composite credit cleanup?\n\nOnly ready composite credit groups are modified. Review-only groups and provider-backed comma artist names are not modified.\n\nPlanned deletes:\n${albumDeleteCount} album links\n${trackDeleteCount} track links`,
      );
      if (!ok) {
        return;
      }
    }
    setCompositeCleanupLoading(true);
    setCompositeCleanupError(null);
    try {
      const payload = await cleanupArtistCompositeCredits(dryRun);
      setCompositeCleanupPlan(payload);
      if (!dryRun) {
        await loadAudit();
      }
    } catch (loadError) {
      setCompositeCleanupError(loadError instanceof Error ? loadError.message : "Failed to run composite credit cleanup.");
    } finally {
      setCompositeCleanupLoading(false);
    }
  }

  useEffect(() => {
    void loadAudit();
  }, []);

  const activeGroups = useMemo(() => {
    if (!audit) {
      return [];
    }
    return audit.candidate_categories[category].groups;
  }, [audit, category]);

  const canApplyRepair = Boolean(repairPlan?.dry_run && repairPlan.safe_groups.length > 0 && repairCount(repairPlan) > 0);
  const canApplyCompositeCleanup = Boolean(
    compositeCleanupPlan?.dry_run
    && compositeCleanupPlan.safe_groups.length > 0
    && (compositeCleanupPlan.album_links_to_delete.length + compositeCleanupPlan.track_links_to_delete.length) > 0,
  );

  const visibleCategories = useMemo(() => {
    if (!audit) {
      return categoryOrder.filter((item) => item === "exact_name");
    }
    return categoryOrder.filter((item) => item === "exact_name" || audit.candidate_categories[item].groups.length > 0);
  }, [audit]);

  useEffect(() => {
    if (!visibleCategories.includes(category)) {
      setCategory("exact_name");
    }
  }, [category, visibleCategories]);

  function renderGroupTitle(title: string, subtitle: string) {
    return (
      <div className="identity-audit-card-title-button">
        <span>
          <strong>{title}</strong>
          <small>{subtitle}</small>
        </span>
      </div>
    );
  }

  return (
    <div className="identity-audit-grid">
      <div className="identity-audit-group">
        <div className="section-column-header">
          <div>
            <h3>Artist Duplicates</h3>
            <p className="identity-audit-tab-copy">
              Same artist name alone is review-only. Repair requires exact normalized artist name, exactly one provider-backed artist, text-only duplicates, and shared album/track/source evidence.
            </p>
          </div>
          <div className="section-column-header-actions">
            <button className="secondary-button" disabled={loading} onClick={() => void loadAudit()} type="button">
              {loading ? "Loading..." : "Reload"}
            </button>
            <button className="secondary-button" disabled={repairLoading} onClick={() => void runRepair(true)} type="button">
              {repairLoading ? "Running..." : "Dry Run Repair"}
            </button>
            <button className="secondary-button" disabled={repairLoading || !canApplyRepair} onClick={() => void runRepair(false)} type="button">
              Apply Repair
            </button>
            <button className="secondary-button" disabled={compositeCleanupLoading} onClick={() => void runCompositeCleanup(true)} type="button">
              {compositeCleanupLoading ? "Running..." : "Dry Run Composite"}
            </button>
            <button className="secondary-button" disabled={compositeCleanupLoading || !canApplyCompositeCleanup} onClick={() => void runCompositeCleanup(false)} type="button">
              Apply Composite
            </button>
          </div>
        </div>
        <div className="identity-audit-ambiguous-summary">
          <span className="identity-audit-pill">Exact: {audit?.summary.exact_name_groups ?? 0}</span>
          <span className="identity-audit-pill">Safe: {(audit?.summary.exact_name_identity_evidence_safe_repair_groups ?? 0) + (audit?.summary.exact_name_album_title_provider_context_safe_repair_groups ?? 0)}</span>
          <span className="identity-audit-pill">Text only: {audit?.summary.exact_name_no_provider_review_only_groups ?? 0}</span>
          <span className="identity-audit-pill">Multi-provider: {audit?.summary.ambiguous_provider_review_only_groups ?? 0}</span>
          <span className="identity-audit-pill">Stylization: {audit?.summary.stylization_groups ?? 0}</span>
          <span className="identity-audit-pill">Same album: {audit?.summary.similar_same_album_groups ?? 0}</span>
          <span className="identity-audit-pill">Composite: {audit?.summary.composite_credit_groups ?? 0}</span>
          {lastLoadedAt ? <span className="identity-audit-pill">Loaded {new Date(lastLoadedAt).toLocaleTimeString()}</span> : null}
          {lastRepairAt ? <span className="identity-audit-pill">Repair checked {new Date(lastRepairAt).toLocaleTimeString()}</span> : null}
        </div>
        <p className="identity-audit-tab-copy">
          Album-title evidence is repairable only under strict exact-name/provider-backed gates and provider album context. Stylization and similar-name buckets are never repaired.
          Composite credits show comma-separated history artist values; treat them as raw-credit cleanup candidates, not artist merges.
        </p>
        <div className="track-ranking-toggle identity-audit-tabs" role="group" aria-label="Artist duplicate category">
          {visibleCategories.map((item) => (
            <button
              className={`track-ranking-chip${category === item ? " track-ranking-chip-active" : ""}`}
              key={`artist-audit-category-${item}`}
              onClick={() => setCategory(item)}
              type="button"
            >
              {categoryLabels[item]}
            </button>
          ))}
        </div>
        {error ? <p className="empty-copy">{error}</p> : null}
        {repairError ? <p className="empty-copy">{repairError}</p> : null}
        {compositeCleanupError ? <p className="empty-copy">{compositeCleanupError}</p> : null}
        {repairPlan ? (
          <div className="identity-audit-stats">
            <span className="identity-audit-stat"><span>Safe groups</span><strong>{repairPlan.safe_groups.length}</strong></span>
            <span className="identity-audit-stat"><span>Skipped groups</span><strong>{repairPlan.skipped_groups.length}</strong></span>
            <span className="identity-audit-stat"><span>Source maps</span><strong>{repairPlan.source_mappings_to_move.length} move · {repairPlan.source_mappings_to_delete.length} delete</strong></span>
            <span className="identity-audit-stat"><span>Album links</span><strong>{repairPlan.album_links_to_move.length} move · {repairPlan.album_links_to_delete.length} delete</strong></span>
            <span className="identity-audit-stat"><span>Track links</span><strong>{repairPlan.track_links_to_move.length} move · {repairPlan.track_links_to_delete.length} delete</strong></span>
            <span className="identity-audit-stat"><span>Artists</span><strong>{repairPlan.artist_rows_to_delete.length} delete</strong></span>
            {Object.entries(repairPlan.evidence_type_counts ?? {}).map(([type, count]) => (
              <span className="identity-audit-stat" key={`artist-repair-evidence-${type}`}><span>{type}</span><strong>{count}</strong></span>
            ))}
          </div>
        ) : null}
        {compositeCleanupPlan ? (
          <div className="identity-audit-stats">
            <span className="identity-audit-stat"><span>Composite safe groups</span><strong>{compositeCleanupPlan.safe_groups.length}</strong></span>
            <span className="identity-audit-stat"><span>Composite skipped</span><strong>{compositeCleanupPlan.skipped_groups.length}</strong></span>
            <span className="identity-audit-stat"><span>Composite album links</span><strong>{compositeCleanupPlan.album_links_to_delete.length} delete</strong></span>
            <span className="identity-audit-stat"><span>Composite track links</span><strong>{compositeCleanupPlan.track_links_to_delete.length} delete</strong></span>
            <span className="identity-audit-stat"><span>Composite artists</span><strong>{compositeCleanupPlan.artist_rows_to_delete.length} check orphan</strong></span>
          </div>
        ) : null}
        {!loading && !error && activeGroups.length === 0 ? <p className="empty-copy">No artist duplicate candidates in this category.</p> : null}
        <div className="identity-audit-examples">
          {exactNameCategories.has(category) ? (activeGroups as ArtistExactDuplicateGroup[]).map((group) => {
            const groupKey = `artist-exact-${category}-${group.normalized_name}`;
            return (
            <article className="recording-track-candidate-card" key={groupKey}>
              <div className="identity-audit-example-header">
                {renderGroupTitle(
                  group.normalized_name,
                  `${group.artists.length} rows · ${readableGroupCategory(group.category ?? group.recommendation_reason)}`,
                )}
                <span className={`identity-audit-type-badge${group.repairable ? " recording-track-good" : " recording-track-warn"}`}>
                  {group.repairable ? "safe repair" : "review only"}
                </span>
              </div>
              {group.evidence_types?.length ? (
                <div className="identity-audit-stats">
                  {group.evidence_types.map((type) => (
                    <span className="identity-audit-stat" key={`artist-exact-evidence-${group.normalized_name}-${type}`}>
                      <strong>{evidenceLabels[type] ?? type.split("_").join(" ")}</strong>
                    </span>
                  ))}
                </div>
              ) : null}
              {renderArtistRows(group.artists, group.recommended_canonical_artist_id)}
            </article>
          );}) : null}
          {category === "stylization" ? (activeGroups as ArtistStylizationDuplicateGroup[]).map((group) => {
            const groupKey = `artist-style-${group.stylization_key}`;
            return (
            <article className="recording-track-candidate-card" key={groupKey}>
              <div className="identity-audit-example-header">
                {renderGroupTitle(
                  group.normalized_names.join(" / "),
                  `${group.artists.length} rows · uniform ${group.uniform_matching_text ?? group.matching_key ?? group.stylization_key}`,
                )}
                <span className="identity-audit-type-badge recording-track-warn">review only</span>
              </div>
              <div className="identity-audit-stats">
                <span className="identity-audit-stat">
                  <span>matching key</span>
                  <strong>{group.matching_key ?? group.stylization_key}</strong>
                </span>
              </div>
              {renderArtistRows(group.artists)}
            </article>
          );}) : null}
          {(category === "similar_same_album" || category === "composite_credit") ? (activeGroups as ArtistSimilarSameAlbumGroup[]).map((group, index) => {
            const groupKey = `artist-${category}-${index}`;
            return (
            <article className="recording-track-candidate-card" key={groupKey}>
              <div className="identity-audit-example-header">
                {renderGroupTitle(
                  group.artists.map((artist) => artist.display_name).join(" / "),
                  `${group.shared_album_count} shared albums${group.name_similarity != null ? ` · similarity ${Math.round(group.name_similarity * 100)}%` : ""} · ${group.reason}`,
                )}
                <span className="identity-audit-type-badge recording-track-warn">review only</span>
              </div>
              <div className="identity-audit-stats">
                {group.shared_albums.slice(0, 5).map((album) => (
                  <span className="identity-audit-stat" key={`artist-same-album-${index}-${album.release_album_id}`}>
                    <span>album {album.release_album_id}</span>
                    <strong>{album.album_name}</strong>
                  </span>
                ))}
                {group.cleanup_plan ? (
                  <>
                    <span className="identity-audit-stat">
                      <span>credit parts</span>
                      <strong>{group.cleanup_plan.credit_parts.map((part) => part.display_name).join(" + ")}</strong>
                    </span>
                    <span className="identity-audit-stat">
                      <span>cleanup</span>
                      <strong>{group.cleanup_plan.ready_for_cleanup ? "ready for dry-run cleanup" : "needs review"}</strong>
                    </span>
                    <span className="identity-audit-stat">
                      <span>planned deletes</span>
                      <strong>{group.cleanup_plan.album_links_to_delete.length} album · {group.cleanup_plan.track_links_to_delete.length} track</strong>
                    </span>
                  </>
                ) : null}
              </div>
              {renderArtistRows(group.artists)}
            </article>
          );}) : null}
        </div>
      </div>
    </div>
  );
}
