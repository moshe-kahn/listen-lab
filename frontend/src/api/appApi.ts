import type {
  AlbumCatalogLookupResponse,
  AlbumDuplicateLookupResponse,
  AlbumNameDuplicateLookupResponse,
  ArtistAlbumEvidenceResponse,
  ArtistCompositeCreditCleanupResponse,
  ArtistDuplicateAuditResponse,
  ArtistDuplicateRepairResponse,
  CatalogBackfillCoverageResponse,
  CatalogBackfillEnqueueResponse,
  CatalogBackfillQueueReasonFilter,
  CatalogBackfillQueueRepairResponse,
  CatalogBackfillQueueResponse,
  CatalogBackfillRunsResponse,
  IdentityAuditSavedSubmissionListResponse,
  IdentityAuditSavedSubmissionReadResponse,
  IdentityAuditSubmissionDryRunResponse,
  LikedTracksResponse,
  LikedTracksSyncResponse,
  MergedTrackAggregateResponse,
  ReleaseTrackDetailResponse,
  ReleaseTrackMetadataResponse,
  ReleaseAlbumMergeDryRunResponse,
  ReleaseAlbumMergePreviewResponse,
  SuggestedGroupsResponse,
  TrackCatalogLookupResponse,
  TrackDuplicateLookupResponse,
  TrackIdentityAuditResponse,
  TrackMappingLineageResponse,
  AmbiguousReviewResponse,
  RecordingTrackCandidateFilters,
  RecordingTrackCandidateLookupResponse,
  RecordingTrackCandidatesResponse,
  RecordingTrackCandidatesSummary,
  RecordingTrackCandidateReviewsResponse,
  RecordingTrackCandidateReviewSaveRequest,
  RecordingTrackCandidateReviewSaveResponse,
  ReleaseTrackDurationConflictsResponse,
} from "../types/appTypes";

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "/api";
const TRACK_MAPPING_FETCH_TIMEOUT_MS = 20000;
const TRACKS_FORMULA_FETCH_LIMIT = 100;
const LIKED_TRACKS_SYNC_FAILURE_SIMULATION_KEY = "listenlab.simulateLikedSyncFailure";
const LIKED_TRACKS_SYNC_FAILURE_SIMULATION_QUERY = "simulate_liked_sync_failure";

export async function fetchLikedTracks(limit: number = 50, offset: number = 0): Promise<LikedTracksResponse> {
    const response = await fetch(
      `${apiBaseUrl}/me/liked-tracks?limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load liked tracks cache.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Liked Tracks (${response.status}): ${detail}`);
    }
    return (await response.json()) as LikedTracksResponse;
  }

export async function fetchAllLikedTracks(pageLimit: number = 200): Promise<LikedTracksResponse> {
    const firstPage = await fetchLikedTracks(pageLimit, 0);
    const items = [...firstPage.items];
    let nextOffset = firstPage.offset + firstPage.items.length;
    let hasMore = firstPage.has_more;
    let metadata = firstPage.metadata;
    while (hasMore) {
      const page = await fetchLikedTracks(pageLimit, nextOffset);
      items.push(...page.items);
      metadata = page.metadata ?? metadata;
      hasMore = page.has_more;
      nextOffset = page.offset + page.items.length;
      if (page.items.length === 0) {
        break;
      }
    }
    return {
      ...firstPage,
      items,
      has_more: false,
      limit: pageLimit,
      offset: 0,
      metadata,
    };
  }

export async function fetchLikedTrackContains(spotifyTrackId: string): Promise<{ spotify_track_id: string; is_liked: boolean }> {
    const response = await fetch(
      `${apiBaseUrl}/me/liked-tracks/contains?spotify_track_id=${encodeURIComponent(spotifyTrackId)}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to check liked track.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      throw new Error(`Liked Track Contains (${response.status}): ${detail}`);
    }
    return (await response.json()) as { spotify_track_id: string; is_liked: boolean };
  }

export async function postLikedTracksSync(mode: "quick" | "full" = "quick"): Promise<LikedTracksSyncResponse> {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    const body: { mode: "quick" | "full"; simulate_failure_reason?: string } = { mode };
    if (import.meta.env.DEV) {
      const simulatedReason = (
        new URLSearchParams(window.location.search).get(LIKED_TRACKS_SYNC_FAILURE_SIMULATION_QUERY)
        ?? window.localStorage.getItem(LIKED_TRACKS_SYNC_FAILURE_SIMULATION_KEY)
      )?.trim();
      if (simulatedReason) {
        body.simulate_failure_reason = simulatedReason;
        headers["X-ListenLab-Debug-Sync-Failure"] = "1";
      }
    }
    const response = await fetch(`${apiBaseUrl}/me/liked-tracks/sync`, {
      method: "POST",
      credentials: "include",
      headers,
      body: JSON.stringify(body),
    });
    let payload: LikedTracksSyncResponse | { detail?: string } | null = null;
    try {
      payload = (await response.json()) as LikedTracksSyncResponse | { detail?: string };
    } catch {
      // ignore invalid error payloads
    }
    if (!response.ok) {
      let detail = "Failed to sync liked tracks.";
      if (payload && "detail" in payload && payload.detail) {
        detail = payload.detail;
      }
      throw new Error(`Liked Tracks Sync (${response.status}): ${detail}`);
    }
    if (!payload) {
      throw new Error("Liked Tracks Sync: backend returned an empty response.");
    }
    return payload as LikedTracksSyncResponse;
  }

export async function fetchReleaseTrackMetadata(spotifyTrackIds: string[]): Promise<ReleaseTrackMetadataResponse> {
    const response = await fetch(`${apiBaseUrl}/tracks/release-track-metadata`, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ spotify_track_ids: spotifyTrackIds }),
    });
    if (!response.ok) {
      let detail = "Failed to load release track metadata.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      throw new Error(`Release Track Metadata (${response.status}): ${detail}`);
    }
    return (await response.json()) as ReleaseTrackMetadataResponse;
  }

export async function fetchReleaseTrackDetail(
    releaseTrackId: number,
    contextSpotifyTrackId?: string | null,
  ): Promise<ReleaseTrackDetailResponse> {
    const params = new URLSearchParams();
    const contextId = contextSpotifyTrackId?.trim();
    if (contextId) {
      params.set("context_spotify_track_id", contextId);
    }
    const query = params.toString();
    const response = await fetch(
      `${apiBaseUrl}/tracks/release-track/${encodeURIComponent(String(releaseTrackId))}${query ? `?${query}` : ""}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load release track detail.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      throw new Error(`Release Track Detail (${response.status}): ${detail}`);
    }
    return (await response.json()) as ReleaseTrackDetailResponse;
  }

export async function fetchArtistAlbumEvidence(
    artistNames: string[],
    sourceAlbumId?: string | null,
    sourceAlbumName?: string | null,
  ): Promise<ArtistAlbumEvidenceResponse> {
    const params = new URLSearchParams();
    for (const artistName of artistNames) {
      const cleanName = artistName.trim();
      if (cleanName) {
        params.append("artist_names", cleanName);
      }
    }
    if (sourceAlbumId?.trim()) {
      params.set("source_album_id", sourceAlbumId.trim());
    }
    if (sourceAlbumName?.trim()) {
      params.set("source_album_name", sourceAlbumName.trim());
    }
    const response = await fetch(`${apiBaseUrl}/auth/artist-albums?${params.toString()}`, {
      credentials: "include",
    });
    if (!response.ok) {
      let detail = "Failed to load artist albums.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      throw new Error(`Artist Albums (${response.status}): ${detail}`);
    }
    return (await response.json()) as ArtistAlbumEvidenceResponse;
  }

export async function fetchCatalogBackfillCoverage(): Promise<CatalogBackfillCoverageResponse> {
    const response = await fetch(`${apiBaseUrl}/debug/spotify/catalog-backfill/coverage`, {
      credentials: "include",
    });
    if (!response.ok) {
      let detail = "Failed to load catalog backfill coverage.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Catalog Backfill Coverage (${response.status}): ${detail}`);
    }
    return (await response.json()) as CatalogBackfillCoverageResponse;
  }

export async function fetchCatalogBackfillRuns(limit: number = 20, offset: number = 0): Promise<CatalogBackfillRunsResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/spotify/catalog-backfill/runs?limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}`,
      {
        credentials: "include",
      },
    );
    if (!response.ok) {
      let detail = "Failed to load catalog backfill runs.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Catalog Backfill Runs (${response.status}): ${detail}`);
    }
    return (await response.json()) as CatalogBackfillRunsResponse;
  }

export async function fetchCatalogBackfillQueue(
    statusFilter: "all" | "pending" | "done" | "error" = "all",
    reasonFilter: CatalogBackfillQueueReasonFilter = "all",
    limit: number = 50,
    offset: number = 0
  ): Promise<CatalogBackfillQueueResponse> {
    const statusQuery = statusFilter === "all" ? "" : `&status=${encodeURIComponent(statusFilter)}`;
    const reasonQuery = reasonFilter === "all" ? "" : `&reason=${encodeURIComponent(reasonFilter)}`;
    const response = await fetch(
      `${apiBaseUrl}/debug/spotify/catalog-backfill/queue?limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}${statusQuery}${reasonQuery}`,
      {
        credentials: "include",
      },
    );
    if (!response.ok) {
      let detail = "Failed to load catalog backfill queue.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Catalog Backfill Queue (${response.status}): ${detail}`);
    }
    return (await response.json()) as CatalogBackfillQueueResponse;
  }

export async function postCatalogBackfillQueueRepair(): Promise<CatalogBackfillQueueRepairResponse> {
    const response = await fetch(`${apiBaseUrl}/debug/spotify/catalog-backfill/queue/repair`, {
      method: "POST",
      credentials: "include",
    });
    const payload = (await response.json()) as CatalogBackfillQueueRepairResponse | { detail?: string; error?: { message?: string } };
    if (!response.ok || !("ok" in payload && payload.ok)) {
      let detail = "Failed to repair queue statuses.";
      if ("error" in payload && payload.error?.message) {
        detail = payload.error.message;
      } else if ("detail" in payload && payload.detail) {
        detail = payload.detail;
      }
      throw new Error(`Catalog Backfill Queue Repair (${response.status}): ${detail}`);
    }
    return payload as CatalogBackfillQueueRepairResponse;
  }

export async function fetchAlbumCatalogLookup(
    q: string,
    catalogStatus: "all" | "backfilled" | "not_backfilled" | "tracklist_complete" | "tracklist_incomplete" | "error",
    queueStatus: "all" | "not_queued" | "pending" | "done" | "error",
    sort: "default" | "recently_backfilled" | "name" | "incomplete_first",
    limit: number = 50,
    offset: number = 0
  ): Promise<AlbumCatalogLookupResponse> {
    const qQuery = q.trim() ? `&q=${encodeURIComponent(q.trim())}` : "";
    const response = await fetch(
      `${apiBaseUrl}/debug/search/albums?catalog_status=${encodeURIComponent(catalogStatus)}&queue_status=${encodeURIComponent(queueStatus)}&sort=${encodeURIComponent(sort)}&limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}${qQuery}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to search albums.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Album Catalog Lookup (${response.status}): ${detail}`);
    }
    return (await response.json()) as AlbumCatalogLookupResponse;
  }

export async function fetchTrackCatalogLookup(
    q: string,
    catalogStatus: "all" | "backfilled" | "not_backfilled" | "duration_missing" | "error",
    queueStatus: "all" | "not_queued" | "pending" | "done" | "error",
    sort: "default" | "recently_backfilled" | "name" | "incomplete_first",
    limit: number = 50,
    offset: number = 0
  ): Promise<TrackCatalogLookupResponse> {
    const qQuery = q.trim() ? `&q=${encodeURIComponent(q.trim())}` : "";
    const response = await fetch(
      `${apiBaseUrl}/debug/search/tracks?catalog_status=${encodeURIComponent(catalogStatus)}&queue_status=${encodeURIComponent(queueStatus)}&sort=${encodeURIComponent(sort)}&limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}${qQuery}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to search tracks.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Track Catalog Lookup (${response.status}): ${detail}`);
    }
    return (await response.json()) as TrackCatalogLookupResponse;
  }

export async function fetchAlbumDuplicateLookup(
    limit: number = 200,
    offset: number = 0
  ): Promise<AlbumDuplicateLookupResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/search/albums/duplicates?limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load duplicate albums.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Album Duplicate Lookup (${response.status}): ${detail}`);
    }
    return (await response.json()) as AlbumDuplicateLookupResponse;
  }

export async function fetchTrackDuplicateLookup(
    limit: number = 200,
    offset: number = 0
  ): Promise<TrackDuplicateLookupResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/search/tracks/duplicates?limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load duplicate tracks.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Track Duplicate Lookup (${response.status}): ${detail}`);
    }
    return (await response.json()) as TrackDuplicateLookupResponse;
  }

export async function fetchTrackMappingLineage(
    q: string,
    mappingKind: "all" | "source_release" | "release_family" = "source_release",
    sourceMetadata: "all" | "complete" | "incomplete" = "all",
    confirmationCertainty: "all" | "certain" | "uncertain" = "all",
    limit: number = 50,
    offset: number = 0
  ): Promise<TrackMappingLineageResponse> {
    const qQuery = q.trim() ? `&q=${encodeURIComponent(q.trim())}` : "";
    const mappingKindQuery = `&mapping_kind=${encodeURIComponent(mappingKind)}`;
    const sourceMetadataQuery = `&source_metadata=${encodeURIComponent(sourceMetadata)}`;
    const confirmationCertaintyQuery = `&confirmation_certainty=${encodeURIComponent(confirmationCertainty)}`;
    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => controller.abort(), TRACK_MAPPING_FETCH_TIMEOUT_MS);
    let response: Response;
    try {
      response = await fetch(
        `${apiBaseUrl}/debug/search/tracks/lineage?limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}${mappingKindQuery}${sourceMetadataQuery}${confirmationCertaintyQuery}${qQuery}`,
        { credentials: "include", signal: controller.signal },
      );
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") {
        throw new Error("Track Mapping Lookup timed out after 20 seconds.");
      }
      throw error;
    } finally {
      window.clearTimeout(timeoutId);
    }
    if (!response.ok) {
      let detail = "Failed to load track mapping.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Track Mapping Lookup (${response.status}): ${detail}`);
    }
    return (await response.json()) as TrackMappingLineageResponse;
  }

export async function fetchAlbumNameDuplicateLookup(
    limit: number = 200,
    offset: number = 0
  ): Promise<AlbumNameDuplicateLookupResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/search/albums/duplicates-by-name?limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load duplicate albums by name.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Album Name Duplicate Lookup (${response.status}): ${detail}`);
    }
    return (await response.json()) as AlbumNameDuplicateLookupResponse;
  }

export async function postReleaseAlbumMergePreview(releaseAlbumIds: number[]): Promise<ReleaseAlbumMergePreviewResponse> {
    const response = await fetch(`${apiBaseUrl}/debug/identity/release-albums/merge-preview`, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ release_album_ids: releaseAlbumIds }),
    });
    const payload = (await response.json()) as ReleaseAlbumMergePreviewResponse | { detail?: string };
    if (!response.ok) {
      let detail = "Failed to preview release album merge.";
      if ("detail" in payload && payload.detail) {
        detail = payload.detail;
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Release Album Merge Preview (${response.status}): ${detail}`);
    }
    return payload as ReleaseAlbumMergePreviewResponse;
  }

export async function postReleaseAlbumMergeDryRun(releaseAlbumIds: number[], survivorReleaseAlbumId: number): Promise<ReleaseAlbumMergeDryRunResponse> {
    const response = await fetch(`${apiBaseUrl}/debug/identity/release-albums/merge-dry-run`, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        release_album_ids: releaseAlbumIds,
        survivor_release_album_id: survivorReleaseAlbumId,
      }),
    });
    const payload = (await response.json()) as ReleaseAlbumMergeDryRunResponse | { detail?: string };
    if (!response.ok) {
      let detail = "Failed to dry run release album merge.";
      if ("detail" in payload && payload.detail) {
        detail = payload.detail;
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Release Album Merge Dry Run (${response.status}): ${detail}`);
    }
    return payload as ReleaseAlbumMergeDryRunResponse;
  }


export async function enqueueCatalogBackfillItems(
    items: Array<{ entity_type: "track" | "album"; spotify_id: string; reason?: string; priority?: number }>
  ): Promise<CatalogBackfillEnqueueResponse> {
    const response = await fetch(`${apiBaseUrl}/debug/spotify/catalog-backfill/enqueue`, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ items }),
    });
    const payload = (await response.json()) as CatalogBackfillEnqueueResponse | { detail?: string; error?: { message?: string } };
    if (!response.ok || !("ok" in payload && payload.ok)) {
      let detail = "Catalog enqueue failed.";
      if ("error" in payload && payload.error?.message) {
        detail = payload.error.message;
      } else if ("detail" in payload && payload.detail) {
        detail = payload.detail;
      }
      throw new Error(`Catalog Backfill Enqueue (${response.status}): ${detail}`);
    }
    return payload as CatalogBackfillEnqueueResponse;
  }

export async function fetchMergedTrackAggregate(): Promise<MergedTrackAggregateResponse> {
    const response = await fetch(
      `${apiBaseUrl}/tracks/merged-aggregate?limit=${TRACKS_FORMULA_FETCH_LIMIT}&recent_window_days=28&source_filter=all`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load merged track aggregate.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Merged Tracks (${response.status}): ${detail}`);
    }
    return (await response.json()) as MergedTrackAggregateResponse;
  }

export async function fetchIdentityAudit(): Promise<TrackIdentityAuditResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/tracks/identity-audit?limit=5`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load identity audit.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Identity Audit (${response.status}): ${detail}`);
    }
    return (await response.json()) as TrackIdentityAuditResponse;
  }

export async function fetchArtistDuplicateAudit(): Promise<ArtistDuplicateAuditResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/artists/duplicate-audit`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load artist duplicate audit.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Artist Duplicate Audit (${response.status}): ${detail}`);
    }
    return (await response.json()) as ArtistDuplicateAuditResponse;
  }

export async function repairArtistDuplicates(dryRun: boolean = true): Promise<ArtistDuplicateRepairResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/artists/duplicate-repair?dry_run=${encodeURIComponent(String(dryRun))}`,
      {
        method: "POST",
        credentials: "include",
      },
    );
    if (!response.ok) {
      let detail = "Failed to repair artist duplicates.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Artist Duplicate Repair (${response.status}): ${detail}`);
    }
    return (await response.json()) as ArtistDuplicateRepairResponse;
  }

export async function cleanupArtistCompositeCredits(dryRun: boolean = true): Promise<ArtistCompositeCreditCleanupResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/artists/composite-credit-cleanup?dry_run=${encodeURIComponent(String(dryRun))}`,
      {
        method: "POST",
        credentials: "include",
      },
    );
    if (!response.ok) {
      let detail = "Failed to clean up artist composite credits.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Artist Composite Credit Cleanup (${response.status}): ${detail}`);
    }
    return (await response.json()) as ArtistCompositeCreditCleanupResponse;
  }

export async function fetchIdentityAuditSuggestedGroups(): Promise<SuggestedGroupsResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/tracks/identity-audit/suggested-groups?limit=50&offset=0&status_filter=suggested`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load suggested composition groups.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Identity Audit Suggested Groups (${response.status}): ${detail}`);
    }
    return (await response.json()) as SuggestedGroupsResponse;
  }

export async function fetchIdentityAuditAmbiguousReview(): Promise<AmbiguousReviewResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/tracks/identity-audit/ambiguous-review?limit=500&offset=0`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load ambiguous review queue.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Identity Audit Ambiguous Review (${response.status}): ${detail}`);
    }
    return (await response.json()) as AmbiguousReviewResponse;
  }

export async function fetchRecordingTrackCandidates(
    filters: RecordingTrackCandidateFilters = {},
  ): Promise<RecordingTrackCandidatesResponse> {
    const params = new URLSearchParams();
    params.set("limit", String(filters.limit ?? 50));
    params.set("offset", String(filters.offset ?? 0));
    if (filters.safety_status) {
      params.set("safety_status", filters.safety_status);
    }
    if (filters.candidate_type) {
      params.set("candidate_type", filters.candidate_type);
    }
    if (filters.relationship_kind?.trim()) {
      params.set("relationship_kind", filters.relationship_kind.trim());
    }
    if (typeof filters.min_confidence === "number" && Number.isFinite(filters.min_confidence)) {
      params.set("min_confidence", String(filters.min_confidence));
    }
    if (typeof filters.include_track_family_candidates === "boolean") {
      params.set("include_track_family_candidates", String(filters.include_track_family_candidates));
    }
    if (filters.q?.trim()) {
      params.set("q", filters.q.trim());
    }
    if (filters.artist?.trim()) {
      params.set("artist", filters.artist.trim());
    }
    const response = await fetch(
      `${apiBaseUrl}/debug/tracks/recording-track-candidates?${params.toString()}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load recording track candidates.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Recording Track Candidates (${response.status}): ${detail}`);
    }
    return (await response.json()) as RecordingTrackCandidatesResponse;
  }

export async function fetchRecordingTrackCandidatesSummary(): Promise<RecordingTrackCandidatesSummary> {
    const response = await fetch(
      `${apiBaseUrl}/debug/tracks/recording-track-candidates/summary`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load recording track candidate summary.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Recording Track Candidate Summary (${response.status}): ${detail}`);
    }
    return (await response.json()) as RecordingTrackCandidatesSummary;
  }

export async function fetchRecordingTrackCandidateByReleaseTrack(releaseTrackId: number): Promise<RecordingTrackCandidateLookupResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/tracks/recording-track-candidates/by-release/${encodeURIComponent(String(releaseTrackId))}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load recording track candidate.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Recording Track Candidate (${response.status}): ${detail}`);
    }
    return (await response.json()) as RecordingTrackCandidateLookupResponse;
  }

export async function fetchReleaseTrackDurationConflicts(
    limit: number = 100,
    offset: number = 0,
    minDurationDeltaMs: number = 2000,
  ): Promise<ReleaseTrackDurationConflictsResponse> {
    const params = new URLSearchParams();
    params.set("limit", String(limit));
    params.set("offset", String(offset));
    params.set("min_duration_delta_ms", String(minDurationDeltaMs));
    const response = await fetch(
      `${apiBaseUrl}/debug/tracks/release-track-duration-conflicts?${params.toString()}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load release-track duration conflicts.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Release Track Duration Conflicts (${response.status}): ${detail}`);
    }
    return (await response.json()) as ReleaseTrackDurationConflictsResponse;
  }

export async function fetchRecordingTrackCandidateReviews(limit: number = 2000, offset: number = 0): Promise<RecordingTrackCandidateReviewsResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/tracks/recording-track-candidate-reviews?limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load recording track candidate reviews.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Recording Track Candidate Reviews (${response.status}): ${detail}`);
    }
    return (await response.json()) as RecordingTrackCandidateReviewsResponse;
  }

export async function saveRecordingTrackCandidateReview(
    payload: RecordingTrackCandidateReviewSaveRequest,
  ): Promise<RecordingTrackCandidateReviewSaveResponse> {
    const response = await fetch(`${apiBaseUrl}/debug/tracks/recording-track-candidate-reviews`, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      let detail = "Failed to save recording track candidate review.";
      try {
        const errorPayload = (await response.json()) as { detail?: string };
        if (errorPayload.detail) {
          detail = errorPayload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Recording Track Candidate Review Save (${response.status}): ${detail}`);
    }
    return (await response.json()) as RecordingTrackCandidateReviewSaveResponse;
  }

export async function fetchIdentityAuditSavedSubmissions(limit: number = 20, offset: number = 0): Promise<IdentityAuditSavedSubmissionListResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/tracks/identity-audit/submissions?limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load saved submissions.";
      try {
        const payload = (await response.json()) as { detail?: string };
        if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Identity Audit Saved Submissions (${response.status}): ${detail}`);
    }
    return (await response.json()) as IdentityAuditSavedSubmissionListResponse;
  }

export async function fetchIdentityAuditSavedSubmissionById(submissionId: number): Promise<IdentityAuditSavedSubmissionReadResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/tracks/identity-audit/submissions/${encodeURIComponent(String(submissionId))}`,
      { credentials: "include" },
    );
    if (!response.ok) {
      let detail = "Failed to load saved submission.";
      try {
        const payload = (await response.json()) as { detail?: string; error?: { message?: string } };
        if (payload.error?.message) {
          detail = payload.error.message;
        } else if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Identity Audit Saved Submission (${response.status}): ${detail}`);
    }
    return (await response.json()) as IdentityAuditSavedSubmissionReadResponse;
  }

export async function fetchIdentityAuditSavedSubmissionDryRun(submissionId: number): Promise<IdentityAuditSubmissionDryRunResponse> {
    const response = await fetch(
      `${apiBaseUrl}/debug/tracks/identity-audit/submissions/${encodeURIComponent(String(submissionId))}/dry-run`,
      {
        method: "POST",
        credentials: "include",
      },
    );
    if (!response.ok) {
      let detail = "Failed to run dry run.";
      try {
        const payload = (await response.json()) as { detail?: string; error?: { message?: string } };
        if (payload.error?.message) {
          detail = payload.error.message;
        } else if (payload.detail) {
          detail = payload.detail;
        }
      } catch {
        // ignore invalid error payloads
      }
      if (response.status === 401) {
        detail = "Not authenticated with Spotify for this browser session.";
      }
      throw new Error(`Identity Audit Submission Dry Run (${response.status}): ${detail}`);
    }
    return (await response.json()) as IdentityAuditSubmissionDryRunResponse;
  }
