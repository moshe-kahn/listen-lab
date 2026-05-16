import type {
  AlbumCatalogLookupResponse,
  AlbumDuplicateLookupResponse,
  AlbumNameDuplicateLookupResponse,
  CatalogBackfillCoverageResponse,
  CatalogBackfillEnqueueResponse,
  CatalogBackfillQueueReasonFilter,
  CatalogBackfillQueueRepairResponse,
  CatalogBackfillQueueResponse,
  CatalogBackfillRunsResponse,
  IdentityAuditSavedSubmissionListResponse,
  IdentityAuditSavedSubmissionReadResponse,
  IdentityAuditSubmissionDryRunResponse,
  MergedTrackAggregateResponse,
  ReleaseAlbumMergeDryRunResponse,
  ReleaseAlbumMergePreviewResponse,
  SuggestedGroupsResponse,
  TrackCatalogLookupResponse,
  TrackDuplicateLookupResponse,
  TrackIdentityAuditResponse,
  TrackMappingLineageResponse,
  AmbiguousReviewResponse,
} from "../types/appTypes";

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "/api";
const TRACK_MAPPING_FETCH_TIMEOUT_MS = 20000;
const TRACKS_FORMULA_FETCH_LIMIT = 100;

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

