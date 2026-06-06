import type { TrackIdentityAuditExample } from "../components/identityAudit/IdentityAuditDiagnostics";

export type SessionResponse = {
  authenticated: boolean;
  display_name: string | null;
  spotify_user_id: string | null;
  email?: string | null;
};

export type ProfileProgressResponse = {
  active: boolean;
  phase: string | null;
  elapsed_seconds: number;
  events?: Array<{
    phase: string;
    at_seconds: number;
  }>;
};

export type RecentTrack = {
  event_id?: number | null;
  track_id: string | null;
  release_track_id?: number | null;
  release_track_name?: string | null;
  release_track_source_count?: number | null;
  release_track_duplicate_source_count?: number | null;
  has_release_track_siblings?: boolean | null;
  release_track_cluster_candidate_type?: string | null;
  release_track_cluster_relationship_kind?: string | null;
  track_name: string | null;
  artist_name: string | null;
  album_name: string | null;
  album_release_year?: string | null;
  artists?: Array<{
    artist_id?: string | null;
    id?: string | null;
    name?: string | null;
    uri?: string | null;
    url?: string | null;
    image_url?: string | null;
  }> | null;
  duration_ms?: number | null;
  duration_seconds?: number | null;
  uri?: string | null;
  preview_url?: string | null;
  url?: string | null;
  image_url?: string | null;
  album_id?: string | null;
  album_url?: string | null;
  spotify_played_at?: string | null;
  spotify_played_at_unix_ms?: number | null;
  spotify_context_type?: string | null;
  spotify_context_uri?: string | null;
  spotify_context_url?: string | null;
  spotify_context_href?: string | null;
  spotify_is_local?: boolean | null;
  spotify_track_type?: string | null;
  spotify_track_number?: number | null;
  spotify_disc_number?: number | null;
  spotify_explicit?: boolean | null;
  spotify_popularity?: number | null;
  spotify_album_type?: string | null;
  spotify_album_total_tracks?: number | null;
  spotify_available_markets_count?: number | null;
  played_at_gap_ms?: number | null;
  estimated_played_ms?: number | null;
  estimated_played_seconds?: number | null;
  estimated_completion_ratio?: number | null;
  completed_play_count?: number | null;
  filtered_play_count?: number | null;
  play_count?: number | null;
  all_time_play_count?: number | null;
  recent_play_count?: number | null;
  first_played_at?: string | null;
  last_played_at?: string | null;
  listening_span_days?: number | null;
  listening_span_years?: number | null;
  active_months_count?: number | null;
  span_months_count?: number | null;
  consistency_ratio?: number | null;
  longevity_score?: number | null;
  has_recent_source?: boolean | null;
  has_history_source?: boolean | null;
  source_label?: "recent" | "history" | "both" | "api" | "liked_cache" | null;
  recent_source_event_count?: number | null;
  history_source_event_count?: number | null;
  matched_source_event_count?: number | null;
  timing_source?: string | null;
  matched_state?: string | null;
  raw_spotify_recent_id?: number | null;
  raw_spotify_history_id?: number | null;
  liked_at?: string | null;
  is_liked?: boolean | null;
  first_seen_at?: string | null;
  unliked_at?: string | null;
  spotify_skipped?: boolean | null;
  spotify_shuffle?: boolean | null;
  spotify_offline?: boolean | null;
  formula_rank_delta?: number | null;
};

export type LikedTracksSyncMetadata = {
  sync_key: string;
  last_quick_sync_at: string | null;
  last_completed_full_sync_at: string | null;
  last_attempted_sync_at: string | null;
  last_sync_mode: string | null;
  last_stopped_reason: string | null;
  last_full_completed: boolean;
  last_active_count: number | null;
  last_tracks_seen: number | null;
  last_pages_seen: number | null;
  updated_at: string;
};

export type LikedTracksResponse = {
  items: RecentTrack[];
  has_more: boolean;
  limit: number;
  offset: number;
  metadata: LikedTracksSyncMetadata | null;
};

export type LikedTracksSyncResponse = {
  sync_mode: "quick" | "full";
  full_completed: boolean;
  stopped_reason: string;
  pages_seen: number;
  tracks_seen: number;
  tracks_upserted: number;
  active_likes: number;
  marked_unliked: number;
  warnings: string[];
  errors: string[];
  metadata: LikedTracksSyncMetadata | null;
};

export type ReleaseTrackMetadataItem = {
  release_track_id: number;
  release_track_name: string;
  release_track_source_count: number;
  release_track_duplicate_source_count?: number | null;
  has_release_track_siblings: boolean;
  release_track_cluster_candidate_type?: string | null;
  release_track_cluster_relationship_kind?: string | null;
};

export type ReleaseTrackMetadataResponse = {
  items: Record<string, ReleaseTrackMetadataItem>;
};

export type ReleaseTrackDetailArtist = {
  name: string;
  role?: string | null;
  billing_index?: number | null;
};

export type ReleaseTrackDetailSourceVersion = {
  source_track_id: number;
  spotify_track_id: string | null;
  uri: string | null;
  name: string | null;
  artists: ReleaseTrackDetailArtist[];
  album_id: string | null;
  album_name: string | null;
  album_image_url: string | null;
  album_release_year?: string | null;
  duration_ms: number | null;
  explicit: boolean | null;
  playable: boolean | null;
  play_count?: number | null;
  last_played_at?: string | null;
  spotify_url: string | null;
  is_context: boolean;
  is_playback_choice: boolean;
};

export type ReleaseTrackDetailResponse = {
  release_track: {
    id: number;
    name: string;
    artists: ReleaseTrackDetailArtist[];
    duration_ms: number | null;
    source_count: number;
  };
  display: {
    title: string;
    artist_name: string | null;
    image_url: string | null;
    album_name: string | null;
    spotify_url: string | null;
    source_spotify_track_id: string | null;
  };
  playback: {
    spotify_track_id: string | null;
    uri: string | null;
    reason: "context_source" | "preferred_playable_source" | "unavailable";
  };
  source_versions: ReleaseTrackDetailSourceVersion[];
};

export type RecordingTrackCandidateLookupResponse = {
  item: RecordingTrackCandidateItem | null;
  items?: RecordingTrackCandidateItem[];
  source: {
    kind: string;
    uses_spotify_api: boolean;
    mutates_identity: boolean;
  };
};

export type ArtistAlbumRelationship = "album" | "appears_on" | "unknown";

export type ArtistAlbumEvidenceItem = {
  album_id: string | null;
  album_name: string;
  album_artist_names: string[];
  image_url: string | null;
  url: string | null;
  release_year: string | null;
  total_tracks: number | null;
  cached_track_count: number;
  matching_artist_names: string[];
  matching_track_count_by_artist: Record<string, number>;
  all_targets_present: boolean;
  tracklist_complete: boolean;
  relationship: ArtistAlbumRelationship;
  evidence: string;
};

export type ArtistAlbumEvidenceResponse = {
  items: ArtistAlbumEvidenceItem[];
};

export type MatchCounts = {
  short_term_top: number;
  long_term_top: number;
  recently_played: number;
  liked: number;
  playlist_size: number;
};

export type TopPlaylist = {
  playlist_id: string | null;
  playlist_name: string | null;
  playlist_url: string | null;
  image_url?: string | null;
  track_count: number | null;
  score: number;
  match_counts: MatchCounts;
};

export type OwnedPlaylist = {
  playlist_id: string | null;
  name: string | null;
  track_count: number | null;
  description?: string | null;
  is_public?: boolean | null;
  url: string | null;
  image_url?: string | null;
};

export type FollowedArtist = {
  artist_id: string | null;
  name: string | null;
  followers_total: number | null;
  genres: string[];
  popularity?: number | null;
  url: string | null;
  image_url?: string | null;
  debug?: {
    source?: string;
    score?: number;
    total_ms?: number;
    play_count?: number;
    distinct_tracks?: number;
  };
};

export type TopAlbum = {
  album_id: string | null;
  name: string | null;
  artist_name: string | null;
  release_year?: string | null;
  url: string | null;
  image_url?: string | null;
  track_representation_count: number;
  rank_score: number;
  album_score: number;
  represented_track_names: string[];
  debug?: {
    source?: string;
    score?: number;
    total_ms?: number;
    play_count?: number;
    distinct_tracks?: number;
  };
};

export type ProfileResponse = {
  id: string;
  display_name: string | null;
  email: string | null;
  product: string | null;
  country: string | null;
  username: string | null;
  followers_total: number | null;
  followed_artists_total: number | null;
  followed_artists_available: boolean;
  followed_artists: FollowedArtist[];
  followed_artists_list_available: boolean;
  recent_top_artists: FollowedArtist[];
  recent_top_artists_available: boolean;
  top_tracks: RecentTrack[];
  top_tracks_available: boolean;
  recent_top_tracks: RecentTrack[];
  recent_top_tracks_available: boolean;
  top_albums: TopAlbum[];
  top_albums_available: boolean;
  recent_top_albums: TopAlbum[];
  recent_top_albums_available: boolean;
  analysis_mode?: "quick" | "full";
  experience_mode?: "full" | "local";
  recent_range?: "short_term" | "medium_term";
  recent_window_days?: number;
  top_playlists_recent: TopPlaylist[];
  top_playlists_all_time: TopPlaylist[];
  top_playlists_available: boolean;
  history_insights_available?: boolean;
  history_first_played_at?: string | null;
  history_last_played_at?: string | null;
  history_total_listen_ms?: number | null;
  history_total_play_count?: number | null;
  profile_url: string | null;
  image_url: string | null;
  recent_tracks: RecentTrack[];
  recent_tracks_available: boolean;
  owned_playlists: OwnedPlaylist[];
  owned_playlists_available: boolean;
  recent_likes_tracks: RecentTrack[];
  recent_likes_available: boolean;
  extended_loaded?: boolean;
  stale_sections?: string[];
  local_last_synced_at?: number | null;
};

export type RecentSectionResponse = {
  recent_range: "short_term" | "medium_term";
  recent_window_days: number;
  recent_top_artists: FollowedArtist[];
  recent_top_artists_available: boolean;
  recent_top_tracks: RecentTrack[];
  recent_top_tracks_available: boolean;
  recent_top_albums: TopAlbum[];
  recent_top_albums_available: boolean;
  recent_tracks: RecentTrack[];
  recent_tracks_available: boolean;
  recent_likes_tracks: RecentTrack[];
  recent_likes_available: boolean;
};

export type RecentArchiveResponse = {
  items: RecentTrack[];
  has_more: boolean;
  limit: number;
  offset: number;
};

export type ListeningLogResponse = {
  items: RecentTrack[];
  has_more: boolean;
  limit: number;
  offset: number;
  source_filter: "all" | "api" | "history" | "both";
};

export type MergedTrackSourceFilter = "all" | "recent" | "history" | "both";
export type RecentDebugSourceFilter = "all" | "api" | "history" | "both";

export type MergedTrackAggregateResponse = {
  limit: number;
  recent_window_days: number;
  source_filter: MergedTrackSourceFilter;
  returned_items: number;
  excluded_unknown_identity_count: number;
  items: RecentTrack[];
};

export type TrackIdentityAuditResponse = {
  limit: number;
  same_name_canonical_splits: TrackIdentityAuditExample[];
  release_track_source_splits: TrackIdentityAuditExample[];
  analysis_track_groups: TrackIdentityAuditExample[];
};

export type AmbiguousReviewComponent = {
  label: string;
  family: string;
  semantic_category: string;
  groupable_by_default: boolean;
};

export type AmbiguousReviewItem = {
  entry_id: string;
  bucket: "grouped" | "ungrouped";
  release_track_id: number;
  release_track_name: string;
  artist_name: string;
  analysis_name: string | null;
  song_family_key: string | null;
  confidence: number | null;
  review_families: string[];
  dominant_family: string | null;
  base_title_anchor: string | null;
  components: AmbiguousReviewComponent[];
  raw_component_summary: string;
};

export type AmbiguousReviewResponse = {
  source: {
    kind: string;
    path: string;
    generated_at: string | null;
  };
  summary: {
    grouped_review_entries: number;
    ungrouped_review_entries: number;
    total_review_entries: number;
  };
  family_counts: Array<{
    family: string;
    count: number;
  }>;
  pagination: {
    limit: number;
    offset: number;
    returned: number;
    has_more: boolean;
  };
  filters: {
    family: string | null;
    bucket: string | null;
  };
  items: AmbiguousReviewItem[];
  parse_warning: string;
};

export type RecordingTrackCandidateType = "recording_track_candidate" | "track_family_candidate";
export type RecordingTrackSafetyStatus = "safe_candidate" | "needs_review" | "unsafe";
export type RecordingTrackReviewDecision =
  | "accepted"
  | "rejected"
  | "unsure"
  | "needs_more_metadata"
  | "wrong_representative"
  | "maybe_split"
  | "maybe_merge_more";

export type RecordingTrackCandidateFilters = {
  limit?: number;
  offset?: number;
  safety_status?: RecordingTrackSafetyStatus;
  candidate_type?: RecordingTrackCandidateType;
  relationship_kind?: string;
  min_confidence?: number;
  include_track_family_candidates?: boolean;
  q?: string;
  artist?: string;
};

export type RecordingTrackCandidateRepresentative = {
  release_track_id: number | null;
  source_track_id: number | string | null;
  reason: string;
};

export type RecordingTrackCandidateEvidence = {
  normalized_title?: string;
  version_tokens?: string[];
  album_context?: string;
  duration_delta_ms?: number | null;
};

export type RecordingTrackCandidateMember = {
  release_track_id: number;
  title: string;
  artist: string;
  album: string;
  release_album_ids?: number[];
  spotify_album_ids?: string[];
  album_image_urls?: string[];
  album_release_dates?: string[];
  album_types?: string[];
  source_track_ids: string[];
  source_track_db_ids?: number[];
  source_track_uris?: string[];
  isrc: string | null;
  isrc_values?: string[];
  duration_ms: number | null;
  duration_values_ms?: number[];
  evidence: RecordingTrackCandidateEvidence;
};

export type RecordingTrackCandidateItem = {
  candidate_key: string;
  display_name: string;
  candidate_type: RecordingTrackCandidateType;
  safety_status: RecordingTrackSafetyStatus;
  confidence: number;
  relationship_kind: string;
  relationship_strength: string;
  evidence_bucket?: string;
  representative: RecordingTrackCandidateRepresentative;
  members: RecordingTrackCandidateMember[];
  why_grouped: string[];
  why_review: string[];
};

export type RecordingTrackCandidatesResponse = {
  items: RecordingTrackCandidateItem[];
  limit: number;
  offset: number;
  total: number;
  returned: number;
  has_more: boolean;
  filters: {
    safety_status: string | null;
    candidate_type: string | null;
    relationship_kind: string | null;
    min_confidence: number | null;
    include_track_family_candidates: boolean;
    same_isrc_only?: boolean;
    q: string | null;
    artist: string | null;
  };
  source: {
    kind: string;
    uses_spotify_api: boolean;
    mutates_identity: boolean;
  };
};

export type ReleaseTrackDurationConflictSourceTrack = {
  source_track_db_id: number;
  spotify_track_id: string;
  spotify_uri: string;
  spotify_url: string | null;
  spotify_track_name: string;
  duration_ms: number;
  explicit: boolean | null;
  spotify_album_id: string;
  album_name: string;
  album_release_date: string;
  album_type: string;
  isrc: string;
  match_method: string;
  confidence: number;
  explanation: string;
};

export type ReleaseTrackDurationConflictItem = {
  release_track_id: number;
  release_track_name: string;
  release_track_normalized_name: string;
  release_track_duration_ms: number | null;
  duration_source: string;
  duration_confidence: string;
  min_duration_ms: number;
  max_duration_ms: number;
  duration_delta_ms: number;
  distinct_duration_count: number;
  source_track_count: number;
  source_tracks: ReleaseTrackDurationConflictSourceTrack[];
};

export type ReleaseTrackDurationConflictsResponse = {
  ok: boolean;
  items: ReleaseTrackDurationConflictItem[];
  total: number;
  limit: number;
  offset: number;
  has_more: boolean;
  min_duration_delta_ms: number;
  source: {
    kind: string;
    uses_spotify_api: boolean;
    mutates_identity: boolean;
  };
};

export type ArtistSourceMap = {
  id: number;
  artist_id: number;
  source_artist_id: number;
  source_name: string;
  external_id: string;
  external_uri: string | null;
  source_name_raw: string | null;
  match_method: string;
  confidence: number;
  status: string;
  is_user_confirmed: number;
  explanation: string | null;
};

export type ArtistDuplicateAuditArtist = {
  artist_id: number;
  display_name: string;
  sort_name: string | null;
  normalized_name?: string | null;
  provider_backed: boolean;
  provider_source_ids: Array<{ source_name: string; external_id: string }>;
  text_only: boolean;
  source_artist_maps: ArtistSourceMap[];
  album_artist_link_count: number;
  track_artist_link_count: number;
  albums: Array<{
    release_album_id: number;
    album_name: string;
    normalized_name: string | null;
    release_year: number | null;
    role: string;
    billing_index: number | null;
    credited_as: string | null;
    match_method: string;
    confidence: number;
    source_basis: string | null;
  }>;
  tracks: Array<{
    release_track_id: number;
    track_name: string;
    normalized_name: string | null;
    duration_ms: number | null;
    role: string;
    billing_index: number | null;
    credited_as: string | null;
    match_method: string;
    confidence: number;
    source_basis: string | null;
    album_names: string[];
  }>;
};

export type ArtistExactDuplicateGroup = {
  normalized_name: string;
  canonical_name: string;
  artists: ArtistDuplicateAuditArtist[];
  recommended_canonical_artist_id: number | null;
  duplicate_candidate_artist_ids: number[];
  recommendation_reason: string;
  category?: string;
  repairable?: boolean;
  evidence_by_duplicate_artist_id?: Record<string, Array<Record<string, unknown>>>;
  evidence_types?: string[];
  review_reason?: string;
};

export type ArtistStylizationDuplicateGroup = {
  category: "stylization";
  stylization_key: string;
  matching_key?: string;
  uniform_matching_text?: string;
  normalized_names: string[];
  repairable: false;
  reason: string;
  artists: ArtistDuplicateAuditArtist[];
};

export type ArtistCompositeCleanupPlan = {
  composite_artist_id: number;
  composite_display_name: string;
  credit_parts: Array<{ display_name: string; normalized_name: string }>;
  matched_parts: Array<{ display_name: string; normalized_name: string; matched_artist_ids: number[] }>;
  missing_parts: Array<{ display_name: string; normalized_name: string; matched_artist_ids: number[] }>;
  ambiguous_parts: Array<{ display_name: string; normalized_name: string; matched_artist_ids: number[] }>;
  matched_artist_ids: number[];
  all_parts_matched: boolean;
  album_links_to_delete: Array<{ link_id: number; release_album_id: number; artist_id: number; role: string }>;
  album_links_to_insert: Array<{ release_album_id: number; artist_id: number; role: string }>;
  track_links_to_delete: Array<{ link_id: number; release_track_id: number; artist_id: number; role: string; matched_artist_ids: number[] }>;
  track_links_review_only: Array<{ link_id: number; release_track_id: number; artist_id: number; role: string; matched_artist_ids: number[]; missing_artist_ids: number[] }>;
  ready_for_cleanup: boolean;
};

export type ArtistSimilarSameAlbumGroup = {
  category: "similar_same_album" | "composite_credit";
  repairable: false;
  reason: string;
  name_similarity?: number;
  shared_album_count: number;
  shared_albums: Array<{ release_album_id: number; album_name: string; role: string }>;
  composite_artist_id?: number;
  cleanup_plan?: ArtistCompositeCleanupPlan;
  artists: ArtistDuplicateAuditArtist[];
};

export type ArtistDuplicateAuditResponse = {
  groups_found: number;
  groups: ArtistExactDuplicateGroup[];
  candidate_categories: {
    exact_name: {
      label: string;
      repairable: true;
      groups: ArtistExactDuplicateGroup[];
    };
    exact_name_identity_evidence_safe_repair: {
      label: string;
      repairable: true;
      groups: ArtistExactDuplicateGroup[];
    };
    exact_name_album_title_provider_context_safe_repair: {
      label: string;
      repairable: true;
      groups: ArtistExactDuplicateGroup[];
    };
    exact_name_album_title_evidence_safe_repair: {
      label: string;
      repairable: true;
      groups: ArtistExactDuplicateGroup[];
    };
    exact_name_only_review: {
      label: string;
      repairable: false;
      groups: ArtistExactDuplicateGroup[];
    };
    exact_name_orphan_placeholder_review: {
      label: string;
      repairable: false;
      groups: ArtistExactDuplicateGroup[];
    };
    ambiguous_provider_review_only: {
      label: string;
      repairable: false;
      groups: ArtistExactDuplicateGroup[];
    };
    exact_name_no_provider_review_only: {
      label: string;
      repairable: false;
      groups: ArtistExactDuplicateGroup[];
    };
    stylization: {
      label: string;
      repairable: false;
      groups: ArtistStylizationDuplicateGroup[];
    };
    similar_same_album: {
      label: string;
      repairable: false;
      groups: ArtistSimilarSameAlbumGroup[];
    };
    composite_credit: {
      label: string;
      repairable: false;
      groups: ArtistSimilarSameAlbumGroup[];
    };
  };
  summary: {
    exact_name_groups: number;
    exact_name_identity_evidence_safe_repair_groups?: number;
    exact_name_album_title_provider_context_safe_repair_groups?: number;
    exact_name_album_title_evidence_safe_repair_groups?: number;
    exact_name_only_review_groups?: number;
    exact_name_orphan_placeholder_review_groups?: number;
    ambiguous_provider_review_only_groups?: number;
    exact_name_no_provider_review_only_groups?: number;
    stylization_groups: number;
    similar_same_album_groups: number;
    composite_credit_groups?: number;
  };
};

export type ArtistDuplicateRepairResponse = {
  dry_run: boolean;
  groups_found: number;
  safe_groups: Array<{ normalized_name: string; canonical_artist_id: number; duplicate_artist_ids: number[] }>;
  skipped_groups: Array<{ normalized_name: string; reason: string; artist_ids: number[]; unsafe_artist_ids?: number[] }>;
  source_mappings_to_move: Array<Record<string, unknown>>;
  source_mappings_to_delete: Array<Record<string, unknown>>;
  album_links_to_move: Array<Record<string, unknown>>;
  album_links_to_delete: Array<Record<string, unknown>>;
  track_links_to_move: Array<Record<string, unknown>>;
  track_links_to_delete: Array<Record<string, unknown>>;
  artist_rows_to_delete: number[];
  evidence_type_counts: Record<string, number>;
  artist_rows_deleted?: number[];
  artist_row_deletes_skipped?: Array<Record<string, unknown>>;
};

export type ArtistCompositeCreditCleanupResponse = {
  dry_run: boolean;
  groups_found: number;
  safe_groups: Array<{
    composite_artist_id: number;
    composite_display_name: string;
    matched_artist_ids: number[];
    credit_parts: Array<{ display_name: string; normalized_name: string }>;
    album_links_to_delete: Array<Record<string, unknown>>;
    track_links_to_delete: Array<Record<string, unknown>>;
  }>;
  skipped_groups: Array<Record<string, unknown>>;
  album_links_to_delete: Array<Record<string, unknown>>;
  track_links_to_delete: Array<Record<string, unknown>>;
  artist_rows_to_delete: number[];
  artist_rows_deleted?: number[];
  artist_row_deletes_skipped?: Array<Record<string, unknown>>;
};

export type RecordingTrackCandidatesSummary = {
  total_candidate_groups: number;
  source_tracks_with_isrc_available?: number;
  release_tracks_with_isrc_available?: number;
  release_tracks_with_multiple_isrcs?: number;
  count_by_candidate_type: Record<string, number>;
  count_by_safety_status: Record<string, number>;
  count_by_relationship_kind: Record<string, number>;
  count_by_relationship_strength: Record<string, number>;
  count_by_evidence_bucket?: Record<string, number>;
  count_with_same_isrc_evidence: number;
  count_with_missing_isrc: number;
  count_by_has_isrc_evidence?: Record<string, number>;
  count_by_same_isrc?: Record<string, number>;
  count_with_duration_delta_over_threshold?: number;
  duration_delta_threshold_ms?: number;
  top_needs_review_reasons?: Record<string, number>;
  sample_safe_candidate_groups?: RecordingTrackCandidateItem[];
  sample_needs_review_groups?: RecordingTrackCandidateItem[];
  sample_track_family_candidate_groups?: RecordingTrackCandidateItem[];
  sample_same_isrc_groups?: RecordingTrackCandidateItem[];
  sample_metadata_missing_groups?: RecordingTrackCandidateItem[];
  sample_conflicting_isrc_compatible_metadata_groups?: RecordingTrackCandidateItem[];
  sample_missing_isrc_compatible_metadata_groups?: RecordingTrackCandidateItem[];
  sample_partial_isrc_match_groups?: RecordingTrackCandidateItem[];
  sample_variant_flag_excluded_groups?: RecordingTrackCandidateItem[];
  source: {
    kind: string;
    uses_spotify_api: boolean;
    mutates_identity: boolean;
  };
};

export type RecordingTrackCandidateReviewItem = {
  id: number;
  candidate_key: string;
  decision: RecordingTrackReviewDecision;
  reviewer_note: string | null;
  preferred_representative_release_track_id: number | null;
  preferred_playback_source_track_id: number | null;
  candidate_snapshot: RecordingTrackCandidateItem | Record<string, unknown>;
  created_at: string;
  updated_at: string;
};

export type RecordingTrackCandidateReviewsResponse = {
  ok: boolean;
  items: RecordingTrackCandidateReviewItem[];
  total: number;
  limit: number;
  offset: number;
  source: {
    kind: string;
    uses_spotify_api: boolean;
    mutates_identity: boolean;
  };
};

export type RecordingTrackCandidateReviewSaveRequest = {
  candidate_key: string;
  decision: RecordingTrackReviewDecision;
  reviewer_note?: string | null;
  preferred_representative_release_track_id?: number | null;
  preferred_playback_source_track_id?: number | null;
  candidate_snapshot: RecordingTrackCandidateItem;
};

export type RecordingTrackCandidateReviewSaveResponse = {
  ok: boolean;
  item: RecordingTrackCandidateReviewItem;
  source: {
    kind: string;
    uses_spotify_api: boolean;
    mutates_identity: boolean;
  };
};

export type SuggestedGroupReleaseTrack = {
  release_track_id: number;
  release_track_name: string;
  normalized_name: string;
  primary_artists: string;
  album_names: string;
  source_refs: string;
  source_map_methods: string;
};

export type SuggestedAnalysisGroup = {
  analysis_track_id: number;
  analysis_track_name: string;
  grouping_note: string;
  grouping_hash: string | null;
  song_family_key: string | null;
  match_method: string;
  confidence: number;
  status: string;
  release_track_count: number;
  release_tracks: SuggestedGroupReleaseTrack[];
};

export type SuggestedGroupsResponse = {
  summary: {
    total_groups: number;
    status: string;
  };
  pagination: {
    limit: number;
    offset: number;
    returned: number;
    has_more: boolean;
  };
  items: SuggestedAnalysisGroup[];
};

export type LocalReviewVerdict = "good_to_group" | "not_good" | "skipped" | "unsure";
export type LocalGroupingTarget = "same_composition" | "same_release_track_only" | null;
export type LocalReviewDecision = {
  verdict: LocalReviewVerdict;
  grouping_target: LocalGroupingTarget;
  note: string;
  updated_at_ms: number;
};

export type SubmissionPreviewValidationResponse = {
  ok: boolean;
  summary: {
    total_decisions: number;
    group_decisions: number;
    track_decisions: number;
    approved: number;
    rejected: number;
    skipped: number;
    unknown_groups: number;
    unknown_tracks: number;
    warnings: number;
  };
  warnings: string[];
  unknown_items: {
    groups: Array<Record<string, unknown>>;
    tracks: Array<Record<string, unknown>>;
  };
  validated: {
    groups: {
      approved: Array<Record<string, unknown>>;
      rejected: Array<Record<string, unknown>>;
      skipped: Array<Record<string, unknown>>;
    };
    tracks: {
      approved: Array<Record<string, unknown>>;
      rejected: Array<Record<string, unknown>>;
      skipped: Array<Record<string, unknown>>;
    };
  };
};

export type IdentityAuditSubmissionSaveResponse = {
  ok: boolean;
  submission_id: number;
  status: string;
  created_at: string;
  summary: {
    total_decisions: number;
    group_decisions: number;
    track_decisions: number;
    approved: number;
    rejected: number;
    skipped: number;
    unknown_groups: number;
    unknown_tracks: number;
    warnings: number;
  };
  warnings: string[];
  unknown_items: {
    groups: Array<Record<string, unknown>>;
    tracks: Array<Record<string, unknown>>;
  };
};

export type IdentityAuditSavedSubmissionListItem = {
  id: number;
  created_at: string;
  status: string;
  summary: {
    total_decisions?: number;
    group_decisions?: number;
    track_decisions?: number;
    approved?: number;
    rejected?: number;
    skipped?: number;
    unknown_groups?: number;
    unknown_tracks?: number;
    warnings?: number;
  };
  warnings_count: number;
  unknown_groups: number;
  unknown_tracks: number;
  notes: string | null;
};

export type IdentityAuditSavedSubmissionListResponse = {
  ok: boolean;
  items: IdentityAuditSavedSubmissionListItem[];
  total: number;
};

export type IdentityAuditSavedSubmissionReadResponse = {
  ok: boolean;
  item: {
    id: number;
    created_at: string;
    status: string;
    payload: Record<string, unknown>;
    validation: SubmissionPreviewValidationResponse;
    notes: string | null;
    promoted_at: string | null;
  };
};

export type IdentityAuditSubmissionDryRunResponse = {
  ok: boolean;
  submission_id: number;
  status: "dry_run";
  validation: SubmissionPreviewValidationResponse;
  summary: {
    approved_groups: number;
    approved_tracks: number;
    rejected: number;
    skipped: number;
    would_apply: number;
    warnings: number;
    unknown_groups: number;
    unknown_tracks: number;
  };
  plan: {
    groups: Array<Record<string, unknown>>;
    tracks: Array<Record<string, unknown>>;
  };
  noops: {
    rejected: Array<Record<string, unknown>>;
    skipped: Array<Record<string, unknown>>;
  };
  warnings: string[];
};

export type CatalogBackfillRunItem = {
  id: number;
  started_at: string | null;
  completed_at: string | null;
  market: string | null;
  status: string | null;
  tracks_seen: number;
  tracks_fetched: number;
  tracks_upserted: number;
  albums_seen: number;
  albums_fetched: number;
  album_tracks_upserted: number;
  album_tracklists_seen?: number;
  album_tracklists_skipped_by_policy?: number;
  album_tracklists_fetched?: number;
  skipped: number;
  errors: number;
  requests_total: number;
  requests_success: number;
  requests_429: number;
  requests_failed: number;
  initial_request_delay_seconds: number;
  final_request_delay_seconds: number;
  effective_requests_per_minute: number;
  peak_requests_last_30_seconds: number;
  max_retry_after_seconds: number;
  last_retry_after_seconds?: number;
  has_more: boolean;
  last_error: string | null;
  warnings?: string[];
  warnings_count?: number;
  partial?: boolean | null;
  stop_reason?: string | null;
  run_mode?: "metadata_only" | "tracklists_relevant" | "full_catalog" | string;
  run_reason?: string | null;
  album_tracklist_policy?: "all" | "priority_only" | "relevant_albums" | "none" | string;
};

export type CatalogBackfillRunsResponse = {
  ok: boolean;
  items: CatalogBackfillRunItem[];
  total: number;
};

export type CatalogBackfillQueueItem = {
  id: number;
  entity_type: "track" | "album" | string;
  spotify_id: string;
  reason: string | null;
  priority: number;
  status: "pending" | "done" | "error" | string;
  requested_at: string | null;
  last_attempted_at: string | null;
  attempts: number;
  last_error: string | null;
};

export type CatalogBackfillQueueResponse = {
  ok: boolean;
  items: CatalogBackfillQueueItem[];
  total: number;
  counts: {
    pending: number;
    done: number;
    error: number;
  };
  reason_counts?: {
    identity_metadata: number;
    manual_priority: number;
    tracklist_completion: number;
    full_backfill: number;
    other: number;
  };
};

export type CatalogBackfillQueueRepairResponse = {
  ok: boolean;
  repaired: number;
};

export type AlbumCatalogLookupItem = {
  release_album_id: number;
  release_album_name: string;
  artist_name: string;
  spotify_album_id: string | null;
  spotify_album_name: string | null;
  album_type: string | null;
  release_date: string | null;
  total_tracks: number | null;
  album_track_rows: number;
  tracklist_complete: boolean;
  catalog_fetched_at: string | null;
  catalog_last_status: string | null;
  catalog_last_error: string | null;
  queue_status: "not_queued" | "pending" | "done" | "error" | string;
  queue_priority: number | null;
  queue_requested_at: string | null;
  queue_attempts: number | null;
  queue_last_error: string | null;
};

export type AlbumCatalogLookupResponse = {
  ok: boolean;
  items: AlbumCatalogLookupItem[];
  total: number;
};

export type AlbumDuplicateReleaseItem = {
  release_album_id: number;
  release_album_name: string;
  artist_name: string;
  album_track_rows: number;
  total_tracks: number | null;
  catalog_status: string | null;
  queue_status: "not_queued" | "pending" | "done" | "error" | string;
};

export type AlbumDuplicateGroupItem = {
  spotify_album_id: string;
  spotify_album_name: string | null;
  duplicate_count: number;
  release_albums: AlbumDuplicateReleaseItem[];
};

export type AlbumDuplicateLookupResponse = {
  ok: boolean;
  items: AlbumDuplicateGroupItem[];
  total: number;
};

export type AlbumNameDuplicateGroupItem = {
  normalized_album_name: string;
  normalized_primary_artist: string;
  duplicate_count: number;
  spotify_album_ids: string[];
  release_albums: Array<{
    release_album_id: number;
    release_album_name: string;
    artist_name: string;
    spotify_album_id: string | null;
    spotify_album_name: string | null;
    album_track_rows: number;
    total_tracks: number | null;
    catalog_status: string | null;
    queue_status: "not_queued" | "pending" | "done" | "error" | string;
  }>;
};

export type AlbumNameDuplicateLookupResponse = {
  ok: boolean;
  items: AlbumNameDuplicateGroupItem[];
  total: number;
};

export type AlbumMergeReviewTarget = {
  key: string;
  title: string;
  subtitle: string;
  releaseAlbumIds: number[];
  duplicateCount: number;
  sourceLabel: string;
  spotifyAlbumId?: string | null;
  spotifyAlbumName?: string | null;
  warningSummary?: string | null;
};

export type ReleaseAlbumMergePreviewResponse = {
  ok: boolean;
  survivor_release_album_id: number | null;
  merge_release_album_ids: number[];
  merge_readiness: "safe_candidate" | "needs_review" | "unsafe" | string;
  readiness_reasons: string[];
  warnings: string[];
  affected: {
    source_album_map_rows: number;
    album_artist_rows: number;
    release_track_rows: number;
    album_track_rows: number;
    album_track_conflicts: number;
    raw_play_event_rows: number;
  };
  proposed_operations: string[];
};

export type ReleaseAlbumMergeDryRunResponse = {
  ok: boolean;
  blocked: boolean;
  blocked_reasons: string[];
  merge_readiness: "safe_candidate" | "needs_review" | "unsafe" | string;
  readiness_reasons: string[];
  survivor_release_album_id: number | null;
  merge_release_album_ids: number[];
  rows_affected: Record<string, number>;
  plan: {
    source_album_map_repoints: Array<Record<string, unknown>>;
    album_artist_inserts: Array<Record<string, unknown>>;
    album_artist_deletes: Array<Record<string, unknown>>;
    album_track_repoints: Array<Record<string, unknown>>;
    album_track_conflicts: Array<Record<string, unknown>>;
    release_album_retirements: Array<Record<string, unknown>>;
  };
  statements: string[];
};

export type TrackDuplicateReleaseItem = {
  release_track_id: number;
  release_track_name: string;
  artist_name: string;
  release_album_name: string;
  spotify_album_id: string | null;
  catalog_status: string | null;
  queue_status: "not_queued" | "pending" | "done" | "error" | string;
};

export type TrackDuplicateGroupItem = {
  spotify_track_id: string;
  spotify_track_name: string | null;
  duration_ms: number | null;
  duration_display: string | null;
  duplicate_count: number;
  release_tracks: TrackDuplicateReleaseItem[];
};

export type TrackDuplicateLookupResponse = {
  ok: boolean;
  items: TrackDuplicateGroupItem[];
  total: number;
};

export type TrackCatalogLookupItem = {
  release_track_id: number;
  release_track_name: string;
  artist_name: string;
  release_album_name: string;
  spotify_track_id: string | null;
  spotify_track_name: string | null;
  duration_ms: number | null;
  duration_display: string | null;
  album_id: string | null;
  catalog_fetched_at: string | null;
  catalog_last_status: string | null;
  catalog_last_error: string | null;
  queue_status: "not_queued" | "pending" | "done" | "error" | string;
  queue_priority: number | null;
  queue_requested_at: string | null;
  queue_attempts: number | null;
  queue_last_error: string | null;
};

export type TrackCatalogLookupResponse = {
  ok: boolean;
  items: TrackCatalogLookupItem[];
  total: number;
};

export type TrackMappingSourceItem = {
  source_track_id: number;
  source_name: string;
  external_id: string | null;
  source_name_raw: string | null;
  spotify_track_name: string | null;
  duration_ms: number | null;
  duration_display: string | null;
  album_id: string | null;
  album_name: string | null;
  embedded_album_name: string | null;
  source_album_name: string | null;
  album_name_display: string | null;
  album_name_display_source: string | null;
  album_release_date: string | null;
  album_total_tracks: number | null;
  album_copyright: string | null;
  disc_number: number | null;
  track_number: number | null;
  catalog_fetched_at: string | null;
  metadata_complete: boolean;
  metadata_gaps: string[];
  play_count: number;
  match_method: string;
  confidence: number | null;
  status: string;
  is_user_confirmed: boolean;
  isrc?: string | null;
};

export type TrackMappingConfirmationPreview = {
  readiness: "safe_candidate" | "needs_review" | "unsafe" | string;
  action: "read_only_preview" | string;
  reasons: string[];
  evidence: {
    source_count: number;
    normalized_album_names: string[];
    positions: string[];
    normalized_track_names: string[];
    duration_delta_ms: number;
    isrc_values: string[];
    version_flag_sources: Array<{ spotify_track_id: string | null; track_name: string | null }>;
    incomplete_sources?: Array<string | number | null>;
  };
};

export type TrackMappingSourceReleaseGroup = {
  release_track_id: number;
  release_track_name: string;
  artist_name: string;
  release_album_name: string;
  source_count: number;
  source_metadata_complete_count: number;
  source_metadata_incomplete_count: number;
  all_source_metadata_complete: boolean;
  confirmation_preview: TrackMappingConfirmationPreview;
  sources: TrackMappingSourceItem[];
};

export type TrackMappingReleaseItem = {
  release_track_id: number;
  release_track_name: string;
  artist_name: string;
  release_album_name: string;
  source_count: number;
  play_count: number;
  match_method: string;
  confidence: number | null;
  status: string;
  is_user_confirmed: boolean;
};

export type TrackMappingReleaseFamilyGroup = {
  analysis_track_id: number;
  track_family_name: string;
  grouping_note: string | null;
  release_count: number;
  release_tracks: TrackMappingReleaseItem[];
};

export type TrackMappingLineageResponse = {
  ok: boolean;
  source_release: {
    total: number;
    groups: TrackMappingSourceReleaseGroup[];
    included_statuses: string[];
    source_metadata_filter: "all" | "complete" | "incomplete";
    confirmation_certainty_filter?: "all" | "certain" | "uncertain" | string;
    has_more?: boolean;
    total_is_exact?: boolean;
    map_counts: Array<{ status: string; is_user_confirmed: boolean; count: number }>;
  };
  release_family: {
    total: number;
    groups: TrackMappingReleaseFamilyGroup[];
    included_statuses: string[];
    map_counts: Array<{ status: string; is_user_confirmed: boolean; count: number }>;
  };
  limit: number;
  offset: number;
};

export type CatalogBackfillCoverageResponse = {
  ok: boolean;
  known_release_tracks: number;
  track_catalog_rows: number;
  track_duration_coverage_count: number;
  track_duration_coverage_percent: number;
  known_release_albums: number;
  album_catalog_rows: number;
  album_track_rows: number;
  latest_run: CatalogBackfillRunItem | null;
  recent_errors_count: number;
  identity_critical?: {
    missing_source_track_metadata: number;
    missing_priority_track_metadata?: number;
    missing_identity_ambiguous_track_metadata?: number;
    missing_top_track_metadata?: number;
    missing_source_album_metadata: number;
    missing_track_isrc: number;
    missing_track_duration_ms: number;
    missing_album_release_date: number;
    missing_album_external_ids: number;
  };
  catalog_expansion?: {
    missing_deferred_track_metadata?: number;
    missing_album_tracklists: number;
    relevant_album_tracklist_backlog: number;
    unlistened_tracklist_rows: number;
  };
  track_metadata_priority?: {
    priority_scope: string;
    counts: {
      total_missing_accepted_source_track_metadata: number;
      missing_priority_track_metadata: number;
      missing_identity_ambiguous_track_metadata: number;
      missing_top_track_metadata: number;
      identity_top_overlap: number;
      missing_deferred_track_metadata: number;
    };
  };
};

export type CatalogBackfillRunResponse = {
  ok: boolean;
  run_id: number;
  status: string;
  tracks_seen: number;
  tracks_fetched: number;
  tracks_upserted: number;
  albums_seen: number;
  albums_fetched: number;
  album_tracks_upserted: number;
  album_tracklists_seen?: number;
  album_tracklists_skipped_by_policy?: number;
  album_tracklists_fetched?: number;
  skipped: number;
  errors: number;
  requests_total: number;
  requests_success: number;
  requests_429: number;
  requests_failed: number;
  initial_request_delay_seconds: number;
  final_request_delay_seconds: number;
  effective_requests_per_minute: number;
  peak_requests_last_30_seconds: number;
  max_retry_after_seconds: number;
  last_retry_after_seconds: number;
  has_more: boolean;
  warnings: string[];
  warnings_count?: number;
  partial: boolean;
  stop_reason: string | null;
  market: string;
  run_mode?: "metadata_only" | "tracklists_relevant" | "full_catalog" | string;
  run_reason?: string | null;
  limit: number;
  offset: number;
  include_albums: boolean;
  force_refresh: boolean;
  album_tracklist_policy?: "all" | "priority_only" | "relevant_albums" | "none" | string;
  max_runtime_seconds: number;
  max_requests: number;
  max_errors: number;
  max_album_tracks_pages_per_album: number;
  max_429: number;
  last_error: string | null;
};

export type CatalogBackfillEnqueueResponse = {
  ok: boolean;
  received: number;
  enqueued: number;
  already_complete: number;
  updated: number;
  invalid: number;
};

export type UnifiedReviewItem = {
  decision_key: string;
  item_type: "group" | "track";
  title: string;
  subtitle: string;
  bucket_label: string;
  family_label: string;
  group: SuggestedAnalysisGroup | null;
  track: AmbiguousReviewItem | null;
};


export type RecentRange = "short_term" | "medium_term";
export type AnalysisMode = "quick" | "full";
export type ExperienceMode = "full" | "local";
export type ExperienceVisualMode = ExperienceMode | "test";
export type TrackRankingMode = "plays" | "mix" | "longevity";
export type RankMovementFilter = "all" | "risers" | "fallers";
export type AppPage = "dashboard" | "formulaLab" | "identityAudit" | "recentDebug" | "catalogBackfill" | "searchLookup";
export type CatalogBackfillTab = "overview" | "priorityMetadata" | "fullBackfill" | "queue" | "recentRuns";
export type CatalogBackfillRunMode = "metadata_only" | "tracklists_relevant" | "full_catalog";
export type CatalogBackfillQueueReasonFilter = "all" | "identity_metadata" | "manual_priority" | "tracklist_completion" | "full_backfill";
export type SectionKey =
  | "artists"
  | "artistsAllTime"
  | "artistsRecent"
  | "tracks"
  | "tracksAllTime"
  | "tracksAllTimeNew"
  | "tracksAllTimeCurrent"
  | "tracksRecent"
  | "albums"
  | "albumsAllTime"
  | "albumsRecent"
  | "playlists"
  | "playlistsAllTime"
  | "playlistsRecent"
  | "recent"
  | "likes";

export type RecentPlayFilter = "listened" | "liked" | "all" | "skipped";

export type DashboardListCardProps = {
  href?: string | null;
  entityId?: string | null;
  imageUrl?: string | null;
  imageAlt: string;
  fallbackLabel: string;
  primaryText: string;
  secondaryText?: string | null;
  tertiaryText?: string | null;
  metricText?: string | null;
  primaryBadgeText?: string | null;
  liked?: boolean;
  releaseSibling?: boolean;
  releaseSiblingSourceCount?: number | null;
  secondaryBadgeText?: string | null;
  completionRatio?: number | null;
  trackUri?: string | null;
  previewTrack?: RecentTrack | null;
  primaryClamp?: "single-line-ellipsis" | "two-line-clamp";
};

export type PreviewItem = {
  image: string | null;
  fallbackLabel?: string;
  label: string;
  meta: string | null;
  detail: string | null;
  kind: "artist" | "track" | "album" | "playlist";
  entityId: string | null;
  trackUri: string | null;
  url: string;
  trackId?: string | null;
  releaseTrackId?: number | null;
  releaseTrackName?: string | null;
  releaseTrackSourceCount?: number | null;
  hasReleaseTrackSiblings?: boolean | null;
  albumId?: string | null;
  artistName?: string | null;
  artists?: Array<{
    artist_id?: string | null;
    id?: string | null;
    name?: string | null;
    uri?: string | null;
    url?: string | null;
    image_url?: string | null;
  }> | null;
  sourceAlbumId?: string | null;
  sourceAlbumName?: string | null;
  sourceAlbumImage?: string | null;
  sourceAlbumUrl?: string | null;
  sourceAlbumYear?: string | null;
  albumHighlightArtistNames?: string[] | null;
  targetArtists?: Array<{
    artist_id?: string | null;
    id?: string | null;
    name?: string | null;
    uri?: string | null;
    url?: string | null;
    image_url?: string | null;
  }> | null;
  sourceTrack?: RecentTrack | null;
  preferredDetailView?: "recording" | "release" | null;
};

export type AuthTokenResponse = {
  access_token: string;
  token_type: string;
  expires_in?: number | null;
};

export type RecentIngestResultResponse = {
  has_result: boolean;
  flow?: string;
  auth_succeeded?: boolean;
  ingest_succeeded?: boolean;
  error?: string | null;
  row_count?: number;
  earliest_api_played_at?: string | null;
  latest_api_played_at?: string | null;
};

export type RecentBeforeProbeResponse = {
  ok: boolean;
  token_source?: string;
  days?: number;
  limit?: number;
  before_iso?: string;
  returned_items?: number;
  earliest_played_at?: string | null;
  latest_played_at?: string | null;
  detail?: string;
};

export type RecentBackfillProbeResponse = {
  ok: boolean;
  token_source?: string;
  limit?: number;
  max_pages?: number;
  pages_fetched?: number;
  total_items?: number;
  earliest_played_at?: string | null;
  latest_played_at?: string | null;
  detail?: string;
};

export type FullAvailabilityResponse = {
  available: boolean;
  blocked: boolean;
  reason: string;
  detail?: string | null;
  retry_after_seconds?: number | null;
};

export type CurrentPlaybackSnapshot = {
  item_type: string | null;
  item_id: string | null;
  release_track_id?: number | null;
  release_track_name?: string | null;
  release_track_source_count?: number | null;
  release_track_duplicate_source_count?: number | null;
  has_release_track_siblings?: boolean | null;
  release_track_cluster_candidate_type?: string | null;
  release_track_cluster_relationship_kind?: string | null;
  name: string | null;
  uri: string | null;
  image_url: string | null;
  artist_names: string[];
  album_name: string | null;
  device_id: string | null;
  progress_ms: number | null;
  duration_ms: number | null;
  is_playing: boolean;
  device_name: string | null;
  device_type: string | null;
  timestamp: number | null;
};

export type CurrentPlaybackResponse = {
  status: "ok" | "failed" | "skipped";
  has_playback?: boolean;
  snapshot?: CurrentPlaybackSnapshot | null;
};

export type PlayerTrackSummary = {
  name: string;
  artists: string;
  album: string;
  image: string | null;
  uri: string | null;
  durationMs: number;
};

export type PlayerQueueTrack = PlayerTrackSummary & {
  trackId: string | null;
  releaseTrackId?: number | null;
  releaseTrackName?: string | null;
  releaseTrackSourceCount?: number | null;
  hasReleaseTrackSiblings?: boolean | null;
  albumId: string | null;
  isLiked?: boolean | null;
  likedAt?: string | null;
  artistItems?: RecentTrack["artists"];
};

export type SpotifyPlayerState = {
  paused: boolean;
  position: number;
  duration: number;
  track_window: {
    current_track: {
      name: string;
      uri: string;
      duration_ms: number;
      album: { name: string; images: Array<{ url: string }> };
      artists: Array<{ name: string }>;
    };
  };
};

export type AlbumTrackEntry = {
  id: string | null;
  name: string;
  uri: string | null;
  durationMs: number | null;
  artistName: string | null;
  sourceTrack: RecentTrack | null;
  lastPlayedAt: string | null;
  playCount: number;
  isSelected: boolean;
  isTopTrack: boolean;
  releaseTrackId: number | null;
  releaseTrackName: string | null;
  releaseTrackSourceCount: number;
  releaseTrackDuplicateSourceCount: number;
  hasReleaseTrackSiblings: boolean;
  releaseTrackClusterCandidateType: string | null;
  releaseTrackClusterRelationshipKind: string | null;
};

export type SpotifyPlayerInstance = {
  activateElement?: () => Promise<void>;
  addListener: (event: string, callback: (payload: any) => void) => void;
  connect: () => Promise<boolean>;
  disconnect: () => void;
  nextTrack?: () => Promise<void>;
  pause: () => Promise<void>;
  previousTrack?: () => Promise<void>;
  resume: () => Promise<void>;
  seek: (positionMs: number) => Promise<void>;
  setVolume?: (volume: number) => Promise<void>;
  togglePlay: () => Promise<void>;
};

export type PopupTrackPlaybackOptions = {
  optimisticTrack?: PlayerTrackSummary | null;
  queueCursor?: number | null;
  queueContext?: {
    label: string;
    url?: string | null;
  } | null;
  queuePlaylistUris?: string[] | null;
  queueTracks?: PlayerQueueTrack[] | null;
  sourceTrack?: RecentTrack | null;
};
