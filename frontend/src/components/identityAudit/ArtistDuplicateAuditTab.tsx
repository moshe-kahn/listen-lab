import { useEffect, useMemo, useState, type ReactNode } from "react";

import { cleanupArtistCompositeCredits, fetchArtistDuplicateAudit, fetchArtistPromotionSkips, repairArtistDuplicates } from "../../api/appApi";
import type {
  ArtistCompositeCreditCleanupResponse,
  ArtistDuplicateAuditArtist,
  ArtistDuplicateAuditResponse,
  ArtistDuplicateRepairResponse,
  ArtistExactDuplicateGroup,
  ArtistPromotionSkipLogItem,
  ArtistPromotionSkipLogResponse,
  ArtistSimilarSameAlbumGroup,
  ArtistStylizationDuplicateGroup,
} from "../../types/appTypes";

type ArtistAuditWorkflow = "ready" | "review";
type ArtistAuditGroup = ArtistExactDuplicateGroup | ArtistStylizationDuplicateGroup | ArtistSimilarSameAlbumGroup;

const workflowLabels: Record<ArtistAuditWorkflow, string> = {
  ready: "Ready to Repair",
  review: "Needs Review",
};

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

const reasonLabels: Record<string, string> = {
  ambiguous_text_only_artist: "ambiguous text-only",
  missing_album_track_evidence: "missing evidence",
  provider_backed_name_collision: "provider collision",
};

type PromotionSkipSummary = {
  items: ArtistPromotionSkipLogItem[];
  totalOccurrences: number;
  reasonCounts: Record<string, number>;
};

function readableGroupCategory(value?: string | null) {
  if (!value) {
    return "review";
  }
  return groupCategoryLabels[value] ?? value.split("_").join(" ");
}

function readableReason(value: string) {
  return reasonLabels[value] ?? value.split("_").join(" ");
}

function shortDateTime(value: string | null | undefined) {
  if (!value) {
    return "unknown";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function normalizedArtistKey(value: string | null | undefined) {
  return (value ?? "").trim().toLocaleLowerCase();
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

function rowCountLabel(count: number) {
  return `${count} artist row${count === 1 ? "" : "s"}`;
}

function artistNames(artists: ArtistDuplicateAuditArtist[]) {
  return artists.map((artist) => artist.display_name || `artist ${artist.artist_id}`).join(" / ");
}

function canonicalArtistLabel(artists: ArtistDuplicateAuditArtist[], canonicalArtistId?: number | null) {
  const canonical = artists.find((artist) => artist.artist_id === canonicalArtistId);
  if (canonical) {
    return `${canonical.display_name || "Unknown artist"} (${canonical.artist_id})`;
  }
  return canonicalArtistId ? `artist ${canonicalArtistId}` : "the provider-backed artist";
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

function addPromotionSkipSummary(map: Map<string, PromotionSkipSummary>, item: ArtistPromotionSkipLogItem) {
  const key = normalizedArtistKey(item.normalized_name);
  if (!key) {
    return;
  }
  const summary = map.get(key) ?? { items: [], totalOccurrences: 0, reasonCounts: {} };
  summary.items.push(item);
  summary.totalOccurrences += item.occurrence_count;
  summary.reasonCounts[item.reason] = (summary.reasonCounts[item.reason] ?? 0) + item.occurrence_count;
  map.set(key, summary);
}

function skipReasonText(summary: PromotionSkipSummary) {
  return Object.entries(summary.reasonCounts)
    .sort(([, left], [, right]) => right - left)
    .map(([reason, count]) => `${readableReason(reason)} ${count}x`)
    .join(" · ");
}

function PromotionSkipBadges({ summary }: { summary: PromotionSkipSummary | null | undefined }) {
  if (!summary) {
    return null;
  }
  return (
    <div className="identity-audit-stats identity-audit-skip-summary">
      <span className="identity-audit-stat identity-audit-skip-stat">
        <span>skipped during import</span>
        <strong>{summary.totalOccurrences}x</strong>
      </span>
      {Object.entries(summary.reasonCounts).map(([reason, count]) => (
        <span className="identity-audit-stat" key={`artist-promotion-skip-reason-${reason}`}>
          <span>{readableReason(reason)}</span>
          <strong>{count}</strong>
        </span>
      ))}
    </div>
  );
}

function artistAuditGroups(audit: ArtistDuplicateAuditResponse): ArtistAuditGroup[] {
  return [
    ...audit.candidate_categories.exact_name.groups,
    ...audit.candidate_categories.exact_name_identity_evidence_safe_repair.groups,
    ...audit.candidate_categories.exact_name_album_title_provider_context_safe_repair.groups,
    ...audit.candidate_categories.exact_name_only_review.groups,
    ...audit.candidate_categories.exact_name_no_provider_review_only.groups,
    ...audit.candidate_categories.ambiguous_provider_review_only.groups,
    ...audit.candidate_categories.exact_name_orphan_placeholder_review.groups,
    ...audit.candidate_categories.stylization.groups,
    ...audit.candidate_categories.similar_same_album.groups,
    ...audit.candidate_categories.composite_credit.groups,
  ];
}

function evidenceText(group: ArtistExactDuplicateGroup) {
  const labels = (group.evidence_types ?? []).map((type) => evidenceLabels[type] ?? type.split("_").join(" "));
  if (labels.length > 0) {
    return labels.join(", ");
  }
  return readableGroupCategory(group.category ?? group.recommendation_reason);
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
  const [promotionSkips, setPromotionSkips] = useState<ArtistPromotionSkipLogResponse | null>(null);
  const [repairPlan, setRepairPlan] = useState<ArtistDuplicateRepairResponse | null>(null);
  const [compositeCleanupPlan, setCompositeCleanupPlan] = useState<ArtistCompositeCreditCleanupResponse | null>(null);
  const [workflow, setWorkflow] = useState<ArtistAuditWorkflow>("ready");
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
      const [payload, skipPayload] = await Promise.all([fetchArtistDuplicateAudit(), fetchArtistPromotionSkips()]);
      setAudit(payload);
      setPromotionSkips(skipPayload);
      setLastLoadedAt(new Date().toISOString());
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : "Failed to load artist audit.");
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

  const promotionSkipsByName = useMemo(() => {
    const map = new Map<string, PromotionSkipSummary>();
    for (const item of promotionSkips?.items ?? []) {
      addPromotionSkipSummary(map, item);
    }
    return map;
  }, [promotionSkips]);

  const auditPromotionSkipKeys = useMemo(() => {
    const keys = new Set<string>();
    if (!audit) {
      return keys;
    }
    for (const group of artistAuditGroups(audit)) {
      if ("normalized_name" in group) {
        keys.add(normalizedArtistKey(group.normalized_name));
      }
      for (const artist of group.artists) {
        keys.add(normalizedArtistKey(artist.display_name));
      }
    }
    return keys;
  }, [audit]);

  const unmatchedPromotionSkips = useMemo(() => {
    return (promotionSkips?.items ?? []).filter((item) => !auditPromotionSkipKeys.has(normalizedArtistKey(item.normalized_name)));
  }, [auditPromotionSkipKeys, promotionSkips]);

  const canApplyRepair = Boolean(repairPlan?.dry_run && repairPlan.safe_groups.length > 0 && repairCount(repairPlan) > 0);
  const canApplyCompositeCleanup = Boolean(
    compositeCleanupPlan?.dry_run
    && compositeCleanupPlan.safe_groups.length > 0
    && (compositeCleanupPlan.album_links_to_delete.length + compositeCleanupPlan.track_links_to_delete.length) > 0,
  );

  const readyCompositeGroups = useMemo(() => {
    return (audit?.candidate_categories.composite_credit.groups ?? []).filter((group) => group.cleanup_plan?.ready_for_cleanup);
  }, [audit]);

  const reviewCompositeGroups = useMemo(() => {
    return (audit?.candidate_categories.composite_credit.groups ?? []).filter((group) => !group.cleanup_plan?.ready_for_cleanup);
  }, [audit]);

  const reviewExactGroups = useMemo(() => {
    if (!audit) {
      return [];
    }
    return [
      ...audit.candidate_categories.exact_name_only_review.groups,
      ...audit.candidate_categories.exact_name_no_provider_review_only.groups,
      ...audit.candidate_categories.ambiguous_provider_review_only.groups,
      ...audit.candidate_categories.exact_name_orphan_placeholder_review.groups,
    ];
  }, [audit]);

  const reviewNameGroups = useMemo(() => {
    return [
      ...reviewExactGroups,
      ...(audit?.candidate_categories.stylization.groups ?? []),
      ...(audit?.candidate_categories.similar_same_album.groups ?? []),
    ];
  }, [audit, reviewExactGroups]);

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

  function groupPromotionSummary(group: ArtistAuditGroup) {
    const summaries = new Map<string, PromotionSkipSummary>();
    if ("normalized_name" in group) {
      const summary = promotionSkipsByName.get(normalizedArtistKey(group.normalized_name));
      if (summary) {
        summaries.set(normalizedArtistKey(group.normalized_name), summary);
      }
    }
    for (const artist of group.artists) {
      const key = normalizedArtistKey(artist.display_name);
      const summary = promotionSkipsByName.get(key);
      if (summary) {
        summaries.set(key, summary);
      }
    }
    if (summaries.size === 0) {
      return null;
    }
    const combined: PromotionSkipSummary = { items: [], totalOccurrences: 0, reasonCounts: {} };
    for (const summary of summaries.values()) {
      combined.items.push(...summary.items);
      combined.totalOccurrences += summary.totalOccurrences;
      for (const [reason, count] of Object.entries(summary.reasonCounts)) {
        combined.reasonCounts[reason] = (combined.reasonCounts[reason] ?? 0) + count;
      }
    }
    return combined;
  }

  function renderSection(title: string, detail: string, count: number, content: ReactNode) {
    if (count === 0) {
      return null;
    }
    return (
      <section className="identity-audit-workflow-section">
        <div className="identity-audit-workflow-section-header">
          <div>
            <h4>{title}</h4>
            <p>{detail}</p>
          </div>
          <span className="identity-audit-pill">{count}</span>
        </div>
        <div className="identity-audit-examples">{content}</div>
      </section>
    );
  }

  function renderExactGroupCards(groups: ArtistExactDuplicateGroup[]) {
    return groups.map((group) => {
      const groupKey = `artist-exact-${group.category ?? "exact"}-${group.normalized_name}`;
      const skipSummary = groupPromotionSummary(group);
      const actionText = group.repairable
        ? `Will merge duplicate rows into ${canonicalArtistLabel(group.artists, group.recommended_canonical_artist_id)}.`
        : "No automatic repair. Needs stronger shared album, track, or provider evidence.";
      return (
        <article className="recording-track-candidate-card" key={groupKey}>
          <div className="identity-audit-example-header">
            {renderGroupTitle(
              group.normalized_name,
              `${rowCountLabel(group.artists.length)} · ${readableGroupCategory(group.category ?? group.recommendation_reason)}`,
            )}
            <span className={`identity-audit-type-badge${group.repairable ? " recording-track-good" : " recording-track-warn"}`}>
              {group.repairable ? "safe repair" : "review only"}
            </span>
          </div>
          <div className="identity-audit-card-summary">
            <div>
              <span>What happened</span>
              <strong>{rowCountLabel(group.artists.length)} named {group.normalized_name}</strong>
            </div>
            <div>
              <span>{group.repairable ? "Why safe" : "Why blocked"}</span>
              <strong>{evidenceText(group)}</strong>
            </div>
            <div>
              <span>Action</span>
              <strong>{actionText}</strong>
            </div>
          </div>
          <PromotionSkipBadges summary={skipSummary} />
          <details className="identity-audit-details">
            <summary>Details</summary>
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
          </details>
        </article>
      );
    });
  }

  function renderNameConflictCards(groups: ArtistAuditGroup[]) {
    return groups.map((group, index) => {
      const skipSummary = groupPromotionSummary(group);
      if ("normalized_name" in group) {
        return renderExactGroupCards([group])[0];
      }
      if ("stylization_key" in group) {
        return (
          <article className="recording-track-candidate-card" key={`artist-style-${group.stylization_key}`}>
            <div className="identity-audit-example-header">
              {renderGroupTitle(
                group.normalized_names.join(" / "),
                `${group.artists.length} rows · style-only name conflict`,
              )}
              <span className="identity-audit-type-badge recording-track-warn">review only</span>
            </div>
            <div className="identity-audit-card-summary">
              <div>
                <span>What happened</span>
                <strong>{rowCountLabel(group.artists.length)} with similar spelling or punctuation</strong>
              </div>
              <div>
                <span>Why blocked</span>
                <strong>Style match only; no shared identity evidence.</strong>
              </div>
              <div>
                <span>Action</span>
                <strong>Review manually before merging.</strong>
              </div>
            </div>
            <PromotionSkipBadges summary={skipSummary} />
            <details className="identity-audit-details">
              <summary>Details</summary>
              <div className="identity-audit-stats">
                <span className="identity-audit-stat">
                  <span>matching key</span>
                  <strong>{group.matching_key ?? group.stylization_key}</strong>
                </span>
              </div>
              {renderArtistRows(group.artists)}
            </details>
          </article>
        );
      }
      return (
        <article className="recording-track-candidate-card" key={`artist-name-conflict-${index}`}>
          <div className="identity-audit-example-header">
            {renderGroupTitle(
              group.artists.map((artist) => artist.display_name).join(" / "),
              `${group.shared_album_count} shared albums${group.name_similarity != null ? ` · similarity ${Math.round(group.name_similarity * 100)}%` : ""} · ${group.reason}`,
            )}
            <span className="identity-audit-type-badge recording-track-warn">review only</span>
          </div>
          <div className="identity-audit-card-summary">
            <div>
              <span>What happened</span>
              <strong>{artistNames(group.artists)}</strong>
            </div>
            <div>
              <span>Why blocked</span>
              <strong>Similar names appear on shared albums, but that is not enough to merge artists.</strong>
            </div>
            <div>
              <span>Action</span>
              <strong>Review manually or wait for stronger source evidence.</strong>
            </div>
          </div>
          <PromotionSkipBadges summary={skipSummary} />
          <details className="identity-audit-details">
            <summary>Details</summary>
            <div className="identity-audit-stats">
              {group.shared_albums.slice(0, 5).map((album) => (
                <span className="identity-audit-stat" key={`artist-same-album-${index}-${album.release_album_id}`}>
                  <span>album {album.release_album_id}</span>
                  <strong>{album.album_name}</strong>
                </span>
              ))}
            </div>
            {renderArtistRows(group.artists)}
          </details>
        </article>
      );
    });
  }

  function renderCompositeCards(groups: ArtistSimilarSameAlbumGroup[]) {
    return groups.map((group, index) => {
      const skipSummary = groupPromotionSummary(group);
      const ready = Boolean(group.cleanup_plan?.ready_for_cleanup);
      return (
        <article className="recording-track-candidate-card" key={`artist-composite-${index}`}>
          <div className="identity-audit-example-header">
            {renderGroupTitle(
              group.artists.map((artist) => artist.display_name).join(" / "),
              `${group.shared_album_count} shared albums · ${group.reason}`,
            )}
            <span className={`identity-audit-type-badge${group.cleanup_plan?.ready_for_cleanup ? " recording-track-good" : " recording-track-warn"}`}>
              {group.cleanup_plan?.ready_for_cleanup ? "ready cleanup" : "review only"}
            </span>
          </div>
          <div className="identity-audit-card-summary">
            <div>
              <span>What happened</span>
              <strong>Combined credit: {artistNames(group.artists)}</strong>
            </div>
            <div>
              <span>{ready ? "Why safe" : "Why blocked"}</span>
              <strong>{ready ? "All credit parts have matching artist rows." : "Not all credit parts can be mapped safely."}</strong>
            </div>
            <div>
              <span>Action</span>
              <strong>{ready ? "Can remove the combined-credit links after dry run." : "Needs review before cleanup."}</strong>
            </div>
          </div>
          <PromotionSkipBadges summary={skipSummary} />
          <details className="identity-audit-details">
            <summary>Details</summary>
            <div className="identity-audit-stats">
              {group.cleanup_plan ? (
                <>
                  <span className="identity-audit-stat">
                    <span>credit parts</span>
                    <strong>{group.cleanup_plan.credit_parts.map((part) => part.display_name).join(" + ")}</strong>
                  </span>
                  <span className="identity-audit-stat">
                    <span>planned deletes</span>
                    <strong>{group.cleanup_plan.album_links_to_delete.length} album · {group.cleanup_plan.track_links_to_delete.length} track</strong>
                  </span>
                </>
              ) : null}
            </div>
            {renderArtistRows(group.artists)}
          </details>
        </article>
      );
    });
  }

  function renderPromotionSkipCards(items: ArtistPromotionSkipLogItem[]) {
    return items.map((item) => (
      <article className="recording-track-candidate-card artist-promotion-skip-card" key={`artist-promotion-skip-${item.id}`}>
        <div className="identity-audit-example-header">
          {renderGroupTitle(
            item.normalized_name || "Unknown artist",
            `${readableReason(item.reason)} · ${item.occurrence_count} skipped import${item.occurrence_count === 1 ? "" : "s"}`,
          )}
          <span className="identity-audit-type-badge recording-track-warn">review only</span>
        </div>
        <div className="identity-audit-card-summary">
          <div>
            <span>What happened</span>
            <strong>Spotify promotion was blocked during import.</strong>
          </div>
          <div>
            <span>Why blocked</span>
            <strong>{readableReason(item.reason)}</strong>
          </div>
          <div>
            <span>Action</span>
            <strong>Needs a matching duplicate case or stronger album/track evidence.</strong>
          </div>
        </div>
        <div className="artist-audit-inline-inventory">
          <div className="artist-audit-inline-inventory-row">
            <span className="artist-audit-inventory-label">Context</span>
            {item.artist_id ? <span className="artist-audit-inventory-chip">artist {item.artist_id}</span> : null}
            {item.release_album_id ? <span className="artist-audit-inventory-chip">album {item.release_album_id}</span> : null}
            {item.release_track_id ? <span className="artist-audit-inventory-chip">track {item.release_track_id}</span> : null}
            {item.provider_artist_ids.length ? <span className="artist-audit-inventory-chip">provider {item.provider_artist_ids.join(", ")}</span> : null}
            {item.text_only_artist_ids.length ? <span className="artist-audit-inventory-chip">text-only {item.text_only_artist_ids.join(", ")}</span> : null}
          </div>
          <div className="artist-audit-inline-inventory-row">
            <span className="artist-audit-inventory-label">Reason</span>
            <span className="artist-audit-inventory-empty">{skipReasonText({ items: [item], totalOccurrences: item.occurrence_count, reasonCounts: { [item.reason]: item.occurrence_count } })}</span>
          </div>
          <div className="artist-audit-inline-inventory-row">
            <span className="artist-audit-inventory-label">Seen</span>
            <span className="artist-audit-inventory-empty">{shortDateTime(item.first_seen_at)} to {shortDateTime(item.last_seen_at)}</span>
          </div>
        </div>
      </article>
    ));
  }

  return (
    <div className="identity-audit-grid">
      <div className="identity-audit-group">
        <div className="section-column-header">
          <div>
            <h3>Artist Duplicates</h3>
            <p className="identity-audit-tab-copy">
              Repairable cases are separated from review-only cases. Import skips are shown as evidence badges on matching artist cases.
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
          <span className="identity-audit-pill">Safe: {(audit?.summary.exact_name_identity_evidence_safe_repair_groups ?? 0) + (audit?.summary.exact_name_album_title_provider_context_safe_repair_groups ?? 0)}</span>
          <span className="identity-audit-pill">Text only: {audit?.summary.exact_name_no_provider_review_only_groups ?? 0}</span>
          <span className="identity-audit-pill">Multi-provider: {audit?.summary.ambiguous_provider_review_only_groups ?? 0}</span>
          <span className="identity-audit-pill">Stylization: {audit?.summary.stylization_groups ?? 0}</span>
          <span className="identity-audit-pill">Same album: {audit?.summary.similar_same_album_groups ?? 0}</span>
          <span className="identity-audit-pill">Composite: {audit?.summary.composite_credit_groups ?? 0}</span>
          <span className="identity-audit-pill">Import skips: {promotionSkips?.summary.total ?? 0}</span>
          {lastLoadedAt ? <span className="identity-audit-pill">Loaded {new Date(lastLoadedAt).toLocaleTimeString()}</span> : null}
          {lastRepairAt ? <span className="identity-audit-pill">Repair checked {new Date(lastRepairAt).toLocaleTimeString()}</span> : null}
        </div>
        <p className="identity-audit-tab-copy">
          Same name alone stays review-only. Safe repair requires exact normalized artist name, one provider-backed artist, text-only duplicates, and shared album/track/source evidence.
        </p>
        <div className="track-ranking-toggle identity-audit-tabs" role="group" aria-label="Artist audit workflow">
          {(Object.keys(workflowLabels) as ArtistAuditWorkflow[]).map((item) => (
            <button
              className={`track-ranking-chip${workflow === item ? " track-ranking-chip-active" : ""}`}
              key={`artist-audit-workflow-${item}`}
              onClick={() => setWorkflow(item)}
              type="button"
            >
              {workflowLabels[item]}
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
        {workflow === "ready" ? (
          <>
            {renderSection(
              "Strong Matches",
              "Exact artist names with shared album, track, or source evidence. These are the safest automatic merges.",
              audit?.candidate_categories.exact_name_identity_evidence_safe_repair.groups.length ?? 0,
              renderExactGroupCards(audit?.candidate_categories.exact_name_identity_evidence_safe_repair.groups ?? []),
            )}
            {renderSection(
              "Album Matches",
              "Exact artist names backed by provider album context. Safe, but slightly weaker than direct track/source evidence.",
              audit?.candidate_categories.exact_name_album_title_provider_context_safe_repair.groups.length ?? 0,
              renderExactGroupCards(audit?.candidate_categories.exact_name_album_title_provider_context_safe_repair.groups ?? []),
            )}
            {renderSection(
              "Composite Credits",
              "History rows where a combined artist credit can be cleaned into separate artist links.",
              readyCompositeGroups.length,
              renderCompositeCards(readyCompositeGroups),
            )}
            {!loading
              && !error
              && (audit?.candidate_categories.exact_name_identity_evidence_safe_repair.groups.length ?? 0) === 0
              && (audit?.candidate_categories.exact_name_album_title_provider_context_safe_repair.groups.length ?? 0) === 0
              && readyCompositeGroups.length === 0 ? (
                <p className="empty-copy">No ready artist repairs.</p>
              ) : null}
          </>
        ) : (
          <>
            {renderSection(
              "Name Conflicts",
              "Same or similar artist names that need stronger evidence before repair.",
              reviewNameGroups.length,
              renderNameConflictCards(reviewNameGroups),
            )}
            {renderSection(
              "Composite Credits",
              "Combined artist credits that are not ready for automatic cleanup.",
              reviewCompositeGroups.length,
              renderCompositeCards(reviewCompositeGroups),
            )}
            {renderSection(
              "Import Skips",
              "Spotify artist promotions blocked during import, with no matching repair case yet.",
              unmatchedPromotionSkips.length,
              renderPromotionSkipCards(unmatchedPromotionSkips),
            )}
            {!loading && !error && reviewNameGroups.length === 0 && reviewCompositeGroups.length === 0 && unmatchedPromotionSkips.length === 0 ? (
              <p className="empty-copy">No artist review cases.</p>
            ) : null}
          </>
        )}
      </div>
    </div>
  );
}
