import { useEffect, useMemo, useRef, useState, type SetStateAction } from "react";
import type { OwnedPlaylist, PlayerQueueTrack, PlayerTrackSummary } from "../../types/appTypes";
import { compactPlaylistContributorLabel, playlistEditorDisplayLabel, playlistOwnerDisplayName, spotifyUserLabel } from "../../utils/playlistDisplay";

export type PlayerSavedPanelTab = "queues" | "bookmarks" | "playlists" | "likes";
export type SavedPlaylistFilter = "yours" | "collabs" | "others" | "private";
type SavedPlaylistDropdown = "filter" | "organize" | "actions" | null;
export type SavedPlaylistGrouping = "none" | "editor" | "category" | "track_count";
export type SavedPlaylistSort = "name_asc" | "name_desc" | "tracks_desc" | "tracks_asc";
type SavedPlaylistVisibility = "public" | "private";
export type SavedPlaylistOverlayOptions = {
  categoryIds: string[];
  filters: SavedPlaylistFilter[];
  yoursVisibility: SavedPlaylistVisibility[];
  collabVisibility: SavedPlaylistVisibility[];
  sort: SavedPlaylistSort;
  grouping: SavedPlaylistGrouping;
  editMode: boolean;
};
type PlaylistList = {
  id: string;
  name: string;
  playlistIds: string[];
};

export type SavedPlayerQueueGroup = {
  id: string;
  label: string;
  url?: string | null;
  imageUrl?: string | null;
  cursor?: number | null;
  tracks: PlayerQueueTrack[];
};

export type SavedPlayerQueueSnapshot = {
  id: string;
  savedAt: string;
  context?: {
    label?: string | null;
    url?: string | null;
  } | null;
  source?: "listenlab" | "spotify" | null;
  activeCursor?: number | null;
  playedKeys?: string[];
  groups: SavedPlayerQueueGroup[];
  currentTrack?: PlayerTrackSummary | null;
};

export type SavedTrackBookmark = {
  id: string;
  bookmarkedAt: string;
  track: PlayerQueueTrack;
  context?: {
    type: "playlist" | "album" | "artist" | "track" | "queue" | "player";
    label: string;
    url?: string | null;
    imageUrl?: string | null;
    entityId?: string | null;
    position?: number | null;
  } | null;
};

export type SavedEntityBookmark = {
  id: string;
  bookmarkedAt: string;
  type: "playlist" | "album" | "artist";
  label: string;
  url?: string | null;
  imageUrl?: string | null;
  entityId?: string | null;
  meta?: string | null;
  detail?: string | null;
};

type PlayerSavedPanelProps = {
  activeTab: PlayerSavedPanelTab;
  savedQueues: SavedPlayerQueueSnapshot[];
  trackBookmarks: SavedTrackBookmark[];
  entityBookmarks: SavedEntityBookmark[];
  ownedPlaylists: OwnedPlaylist[];
  ownedPlaylistsAvailable: boolean;
  onTabChange: (tab: PlayerSavedPanelTab) => void;
  onRestoreSavedQueue: (snapshot: SavedPlayerQueueSnapshot) => void;
  onDeleteSavedQueue: (snapshotId: string) => void;
  onPlayBookmark: (action: "play_now" | "play_next", bookmark: SavedTrackBookmark) => void;
  onOpenBookmark: (bookmark: SavedTrackBookmark) => void;
  onDeleteBookmark: (bookmarkId: string) => void;
  onOpenEntityBookmark: (bookmark: SavedEntityBookmark) => void;
  onDeleteEntityBookmark: (bookmarkId: string) => void;
  onOpenOwnedPlaylist: (playlist: OwnedPlaylist) => void;
  onOpenPlaylistsOverlay: (options: SavedPlaylistOverlayOptions) => void;
};

const tabs: Array<{ value: PlayerSavedPanelTab; label: string; empty: string }> = [
  { value: "queues", label: "Queues", empty: "Saved queue history will appear here." },
  { value: "bookmarks", label: "Bookmarks", empty: "Bookmarked tracks, albums, artists, and playlists will appear here." },
  { value: "playlists", label: "Playlists", empty: "Playlist shortcuts will appear here." },
  { value: "likes", label: "Likes", empty: "Liked tracks and albums will appear here." },
];
const PLAYLIST_LISTS_STORAGE_KEY = "listenlab.playlistLists.v1";
const UNCATEGORIZED_PLAYLIST_CATEGORY_ID = "__uncategorized__";
const savedPlaylistFilterOptions: Array<{ value: SavedPlaylistFilter; label: string }> = [
  { value: "yours", label: "Yours" },
  { value: "collabs", label: "Collabs" },
  { value: "others", label: "Others" },
];
const savedPlaylistGroupingOptions: Array<{ value: SavedPlaylistGrouping; label: string }> = [
  { value: "none", label: "No grouping" },
  { value: "editor", label: "Editor" },
  { value: "category", label: "Category" },
  { value: "track_count", label: "Track count" },
];
const savedPlaylistSortOptions: Array<{ value: SavedPlaylistSort; label: string }> = [
  { value: "name_asc", label: "Name A-Z" },
  { value: "name_desc", label: "Name Z-A" },
  { value: "tracks_desc", label: "Tracks high-low" },
  { value: "tracks_asc", label: "Tracks low-high" },
];
function formatSavedAt(value: string) {
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) {
    return "Saved recently";
  }
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date(timestamp));
}

function savedQueueTitle(snapshot: SavedPlayerQueueSnapshot) {
  return snapshot.context?.label || snapshot.groups[0]?.label || "Saved queue";
}

function savedQueueImage(snapshot: SavedPlayerQueueSnapshot) {
  return snapshot.groups.find((group) => group.imageUrl)?.imageUrl
    ?? snapshot.groups.flatMap((group) => group.tracks).find((track) => track.image)?.image
    ?? null;
}

function savedQueueTrackCount(snapshot: SavedPlayerQueueSnapshot) {
  return snapshot.groups.reduce((total, group) => total + group.tracks.length, 0);
}

function bookmarkContextLabel(bookmark: SavedTrackBookmark) {
  if (!bookmark.context?.label) {
    return null;
  }
  const typeLabel = bookmark.context.type === "player"
    ? "Player"
    : bookmark.context.type.slice(0, 1).toUpperCase() + bookmark.context.type.slice(1);
  return `${typeLabel}: ${bookmark.context.label}`;
}

function tabCount(tab: PlayerSavedPanelTab, savedQueues: SavedPlayerQueueSnapshot[], trackBookmarks: SavedTrackBookmark[], entityBookmarks: SavedEntityBookmark[]) {
  if (tab === "queues") {
    return savedQueues.length;
  }
  if (tab === "bookmarks") {
    return trackBookmarks.length + entityBookmarks.length;
  }
  return 0;
}

function readStoredPlaylistLists(): PlaylistList[] {
  if (typeof window === "undefined") {
    return [];
  }
  try {
    const parsed = JSON.parse(window.localStorage.getItem(PLAYLIST_LISTS_STORAGE_KEY) ?? "[]");
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed
      .map((item) => ({
        id: String(item?.id ?? "").trim(),
        name: String(item?.name ?? "").trim(),
        playlistIds: Array.isArray(item?.playlistIds)
          ? item.playlistIds.map((value: unknown) => String(value ?? "").trim()).filter(Boolean)
          : [],
      }))
      .filter((item) => item.id && item.name);
  } catch {
    return [];
  }
}

function playlistTitle(playlist: OwnedPlaylist) {
  return playlist.name || "Untitled playlist";
}

function playlistMeta(playlist: OwnedPlaylist) {
  const parts: string[] = [];
  if (typeof playlist.track_count === "number") {
    parts.push(`${playlist.track_count.toLocaleString()} track${playlist.track_count === 1 ? "" : "s"}`);
  }
  if (playlist.is_public === false) {
    parts.push("Private");
  }
  if (playlist.is_collaborative) {
    const contributors = compactPlaylistContributorLabel(playlist);
    parts.push(contributors ? `Collab: ${contributors}` : "Collab");
  }
  if (!playlist.is_owned && playlist.owner_name) {
    parts.push(playlistOwnerDisplayName(playlist));
  }
  return parts.join(" · ") || "Playlist";
}

function playlistTrackCount(playlist: OwnedPlaylist) {
  return typeof playlist.track_count === "number" ? playlist.track_count : 0;
}

function playlistStableId(playlist: OwnedPlaylist) {
  return playlist.playlist_id ?? playlist.url ?? "";
}

function playlistEditorGroupLabel(playlist: OwnedPlaylist) {
  return playlistEditorDisplayLabel(playlist);
}

function playlistGroupDisplayLabel(label: string) {
  return spotifyUserLabel(label);
}

function playlistCategoryGroupLabel(playlist: OwnedPlaylist, playlistLists: PlaylistList[]) {
  const playlistId = playlistStableId(playlist);
  if (!playlistId) {
    return "Uncategorized";
  }
  return playlistLists.find((list) => list.playlistIds.includes(playlistId))?.name ?? "Uncategorized";
}

function playlistTrackCountGroupLabel(playlist: OwnedPlaylist) {
  const count = playlistTrackCount(playlist);
  if (count === 0) {
    return "0 tracks";
  }
  if (count <= 15) {
    return "1-15 tracks";
  }
  if (count <= 50) {
    return "16-50 tracks";
  }
  if (count <= 200) {
    return "51-200 tracks";
  }
  if (count <= 1000) {
    return "201-1,000 tracks";
  }
  return "1,001+ tracks";
}

function playlistTrackCountGroupRank(label: string) {
  return [
    "0 tracks",
    "1-15 tracks",
    "16-50 tracks",
    "51-200 tracks",
    "201-1,000 tracks",
    "1,001+ tracks",
  ].indexOf(label);
}

function compareSavedPlaylists(left: OwnedPlaylist, right: OwnedPlaylist, sort: SavedPlaylistSort) {
  if (sort === "tracks_desc" || sort === "tracks_asc") {
    const direction = sort === "tracks_desc" ? -1 : 1;
    const countDiff = playlistTrackCount(left) - playlistTrackCount(right);
    if (countDiff !== 0) {
      return countDiff * direction;
    }
  }
  const nameDiff = playlistTitle(left).localeCompare(playlistTitle(right));
  return sort === "name_desc" ? -nameDiff : nameDiff;
}

function playlistVisibilityMatches(playlist: OwnedPlaylist, visibility: SavedPlaylistVisibility[]) {
  if (visibility.length === 0) {
    return false;
  }
  return (
    (visibility.includes("public") && playlist.is_public !== false)
    || (visibility.includes("private") && playlist.is_public === false)
  );
}

function playlistMatchesFilter(
  playlist: OwnedPlaylist,
  filters: SavedPlaylistFilter[],
  yoursVisibility: SavedPlaylistVisibility[],
  collabVisibility: SavedPlaylistVisibility[],
) {
  if (filters.length === 0) {
    return true;
  }
  if (
    filters.includes("yours")
    && playlist.is_owned
    && !playlist.is_collaborative
    && playlistVisibilityMatches(playlist, yoursVisibility)
  ) {
    return true;
  }
  if (
    filters.includes("collabs")
    && playlist.is_collaborative
    && playlistVisibilityMatches(playlist, collabVisibility)
  ) {
    return true;
  }
  if (filters.includes("others") && !playlist.is_owned) {
    return true;
  }
  if (filters.includes("private") && playlist.is_public === false) {
    return true;
  }
  return false;
}

function playlistFilterCount(playlist: OwnedPlaylist, filter: SavedPlaylistFilter) {
  if (filter === "yours") {
    return Boolean(playlist.is_owned && !playlist.is_collaborative);
  }
  if (filter === "collabs") {
    return Boolean(playlist.is_collaborative);
  }
  if (filter === "others") {
    return !playlist.is_owned;
  }
  if (filter === "private") {
    return playlist.is_public === false;
  }
  return false;
}

function playlistMatchesCategory(playlist: OwnedPlaylist, playlistLists: PlaylistList[], selectedCategoryIds: string[]) {
  if (selectedCategoryIds.length === 0) {
    return true;
  }
  const playlistId = playlistStableId(playlist);
  if (!playlistId) {
    return false;
  }
  const categoryIds = playlistLists
    .filter((list) => list.playlistIds.includes(playlistId))
    .map((list) => list.id);
  if (selectedCategoryIds.includes(UNCATEGORIZED_PLAYLIST_CATEGORY_ID) && categoryIds.length === 0) {
    return true;
  }
  return categoryIds.some((listId) => selectedCategoryIds.includes(listId));
}

export function PlayerSavedPanel({
  activeTab,
  savedQueues,
  trackBookmarks,
  entityBookmarks,
  ownedPlaylists,
  ownedPlaylistsAvailable,
  onTabChange,
  onRestoreSavedQueue,
  onDeleteSavedQueue,
  onPlayBookmark,
  onOpenBookmark,
  onDeleteBookmark,
  onOpenEntityBookmark,
  onDeleteEntityBookmark,
  onOpenOwnedPlaylist,
  onOpenPlaylistsOverlay,
}: PlayerSavedPanelProps) {
  const active = tabs.find((tab) => tab.value === activeTab) ?? tabs[0];
  const savedPlaylistControlsRef = useRef<HTMLDivElement | null>(null);
  const savedPlaylistFilterDropdownRef = useRef<HTMLDivElement | null>(null);
  const savedPlaylistOrganizeDropdownRef = useRef<HTMLDivElement | null>(null);
  const savedPlaylistActionsDropdownRef = useRef<HTMLDivElement | null>(null);
  const previousSavedPlaylistCategorySignatureRef = useRef("");
  const [savedPlaylistFilters, setSavedPlaylistFilters] = useState<SavedPlaylistFilter[]>([]);
  const [savedPlaylistYoursVisibility, setSavedPlaylistYoursVisibility] = useState<SavedPlaylistVisibility[]>(["public", "private"]);
  const [savedPlaylistCollabVisibility, setSavedPlaylistCollabVisibility] = useState<SavedPlaylistVisibility[]>(["public", "private"]);
  const [savedPlaylistCategoryIds, setSavedPlaylistCategoryIds] = useState<string[]>([]);
  const [savedPlaylistCategoriesCollapsed, setSavedPlaylistCategoriesCollapsed] = useState(false);
  const [savedPlaylistGrouping, setSavedPlaylistGrouping] = useState<SavedPlaylistGrouping>("none");
  const [savedPlaylistSort, setSavedPlaylistSort] = useState<SavedPlaylistSort>("name_asc");
  const [savedPlaylistDropdown, setSavedPlaylistDropdown] = useState<SavedPlaylistDropdown>(null);
  const [playlistLists, setPlaylistLists] = useState<PlaylistList[]>(() => readStoredPlaylistLists());
  const savedPlaylistCategorySignature = savedPlaylistCategoryIds.join("\u0000");
  const visibleOwnedPlaylists = ownedPlaylists.filter((playlist) => !playlist.hidden_by_user);
  const categoryFilteredOwnedPlaylists = useMemo(() => (
    visibleOwnedPlaylists.filter((playlist) => playlistMatchesCategory(playlist, playlistLists, savedPlaylistCategoryIds))
  ), [playlistLists, savedPlaylistCategoryIds, visibleOwnedPlaylists]);
  const filterCounts = useMemo(() => (
    savedPlaylistFilterOptions.reduce<Record<SavedPlaylistFilter, number>>((counts, option) => {
      counts[option.value] = categoryFilteredOwnedPlaylists.filter((playlist) => playlistFilterCount(playlist, option.value)).length;
      return counts;
    }, {
      yours: 0,
      collabs: 0,
      others: 0,
      private: 0,
    })
  ), [categoryFilteredOwnedPlaylists]);
  const visibleOwnedPlaylistIds = useMemo(() => (
    new Set(visibleOwnedPlaylists.map(playlistStableId).filter(Boolean))
  ), [visibleOwnedPlaylists]);
  const categoryCounts = useMemo(() => (
    playlistLists.reduce<Record<string, number>>((counts, list) => {
      counts[list.id] = list.playlistIds.filter((playlistId) => visibleOwnedPlaylistIds.has(playlistId)).length;
      return counts;
    }, {})
  ), [playlistLists, visibleOwnedPlaylistIds]);
  const savedPlaylistItems = useMemo(() => (
    categoryFilteredOwnedPlaylists
      .filter((playlist) => playlistMatchesFilter(
        playlist,
        savedPlaylistFilters,
        savedPlaylistYoursVisibility,
        savedPlaylistCollabVisibility,
      ))
      .slice()
      .sort((left, right) => compareSavedPlaylists(left, right, savedPlaylistSort))
  ), [
    savedPlaylistCollabVisibility,
    savedPlaylistFilters,
    savedPlaylistSort,
    savedPlaylistYoursVisibility,
    categoryFilteredOwnedPlaylists,
  ]);
  const savedPlaylistVisibleItems = savedPlaylistItems.slice(0, 25);
  const savedPlaylistOnlyYoursSelected = savedPlaylistFilters.length === 1 && savedPlaylistFilters[0] === "yours";
  const savedPlaylistSingleCategorySelected = savedPlaylistCategoryIds.length === 1;
  const savedPlaylistGroups = useMemo(() => {
    const grouped = new Map<string, OwnedPlaylist[]>();
    savedPlaylistItems.forEach((playlist) => {
      const label = savedPlaylistGrouping === "editor"
        ? playlistEditorGroupLabel(playlist)
        : savedPlaylistGrouping === "category"
          ? playlistCategoryGroupLabel(playlist, playlistLists)
          : savedPlaylistGrouping === "track_count"
            ? playlistTrackCountGroupLabel(playlist)
            : "";
      grouped.set(label, [...(grouped.get(label) ?? []), playlist]);
    });
    const groups = Array.from(grouped.entries()).map(([label, playlists]) => ({ label, playlists }));
    if (savedPlaylistGrouping !== "track_count") {
      return groups;
    }
    return groups
      .sort((left, right) => playlistTrackCountGroupRank(left.label) - playlistTrackCountGroupRank(right.label))
      .map((group) => ({
        ...group,
        playlists: group.playlists.slice().sort((left, right) => playlistTrackCount(left) - playlistTrackCount(right) || playlistTitle(left).localeCompare(playlistTitle(right))),
      }));
  }, [playlistLists, savedPlaylistGrouping, savedPlaylistItems]);
  const savedPlaylistFilterLabel = savedPlaylistFilters.length === 0
    ? "All editors"
    : savedPlaylistFilters.length === 1
      ? savedPlaylistFilterOptions.find((option) => option.value === savedPlaylistFilters[0])?.label ?? "Filter"
      : `${savedPlaylistFilters.length} filters`;

  useEffect(() => {
    if (activeTab === "playlists") {
      setPlaylistLists(readStoredPlaylistLists());
    }
  }, [activeTab]);

  useEffect(() => {
    const categoryChanged = previousSavedPlaylistCategorySignatureRef.current !== savedPlaylistCategorySignature;
    previousSavedPlaylistCategorySignatureRef.current = savedPlaylistCategorySignature;
    if (!categoryChanged || savedPlaylistFilters.length === 0 || categoryFilteredOwnedPlaylists.length === 0 || savedPlaylistItems.length > 0) {
      return;
    }
    setSavedPlaylistFilters([]);
    setSavedPlaylistYoursVisibility(["public", "private"]);
    setSavedPlaylistCollabVisibility(["public", "private"]);
  }, [categoryFilteredOwnedPlaylists.length, savedPlaylistCategorySignature, savedPlaylistFilters.length, savedPlaylistItems.length]);

  useEffect(() => {
    if (!savedPlaylistDropdown) {
      return;
    }
    function handlePointerDown(event: PointerEvent) {
      const target = event.target;
      const activeDropdown = savedPlaylistDropdown === "filter"
        ? savedPlaylistFilterDropdownRef.current
        : savedPlaylistDropdown === "organize"
          ? savedPlaylistOrganizeDropdownRef.current
          : savedPlaylistDropdown === "actions"
            ? savedPlaylistActionsDropdownRef.current
          : null;
      if (target instanceof Element && activeDropdown?.contains(target)) {
        return;
      }
      setSavedPlaylistDropdown(null);
    }
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setSavedPlaylistDropdown(null);
      }
    }
    document.addEventListener("pointerdown", handlePointerDown, true);
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("pointerdown", handlePointerDown, true);
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [savedPlaylistDropdown]);

  const toggleSavedPlaylistFilter = (filter: SavedPlaylistFilter) => {
    setSavedPlaylistFilters((current) => {
      const allFilters = savedPlaylistFilterOptions.map((option) => option.value);
      const next = current.length === 0
        ? allFilters.filter((item) => item !== filter)
        : current.includes(filter)
          ? current.filter((item) => item !== filter)
          : [...current, filter];
      return next.length === 0 || next.length === allFilters.length ? [] : next;
    });
  };
  const toggleSavedPlaylistCategory = (categoryId: string) => {
    setSavedPlaylistCategoryIds((current) => (
      current.includes(categoryId)
        ? current.filter((item) => item !== categoryId)
        : [...current, categoryId]
    ));
  };
  const renderSavedPlaylistVisibilityToggle = (
    label: string,
    visibility: SavedPlaylistVisibility[],
    setter: (value: SetStateAction<SavedPlaylistVisibility[]>) => void,
  ) => {
    const bothSelected = visibility.includes("public") && visibility.includes("private");
    const options: Array<{ key: string; label: string; title: string; value: SavedPlaylistVisibility[] }> = [
      { key: "public", label: "🌐", title: "Public only", value: ["public"] },
      { key: "both", label: "&", title: "Public and private", value: ["public", "private"] },
      { key: "private", label: "🔒", title: "Private only", value: ["private"] },
    ];
    return (
      <span className={`playlist-owner-visibility-toggle${bothSelected ? " playlist-owner-visibility-toggle-both-active" : ""}`} aria-label={label}>
        {options.map((option) => {
          const active = option.value.length === visibility.length && option.value.every((value) => visibility.includes(value));
          return (
            <span
              aria-label={option.title}
              aria-pressed={active}
              className={`playlist-owner-visibility-button playlist-owner-visibility-button-${option.key}${active ? " playlist-owner-visibility-button-active" : ""}`}
              key={option.key}
              onClick={(event) => {
                event.preventDefault();
                event.stopPropagation();
                setter(option.value);
              }}
              role="button"
              tabIndex={-1}
              title={option.title}
            >
              {option.label}
            </span>
          );
        })}
      </span>
    );
  };
  const showAllSavedPlaylistCategories = () => {
    setSavedPlaylistCategoryIds([]);
  };
  const currentSavedPlaylistOverlayOptions = (editMode: boolean): SavedPlaylistOverlayOptions => ({
    categoryIds: [...savedPlaylistCategoryIds],
    filters: [...savedPlaylistFilters],
    yoursVisibility: [...savedPlaylistYoursVisibility],
    collabVisibility: [...savedPlaylistCollabVisibility],
    sort: savedPlaylistSort,
    grouping: savedPlaylistGrouping,
    editMode,
  });
  const renderSavedPlaylistCard = (playlist: OwnedPlaylist, index: number) => (
    <button
      className="player-saved-playlist-card"
      key={playlist.playlist_id ?? playlist.url ?? `${playlistTitle(playlist)}-${index}`}
      onClick={() => onOpenOwnedPlaylist(playlist)}
      title={playlistMeta(playlist)}
      type="button"
    >
      <span className="player-saved-playlist-art" aria-hidden="true">
        {playlist.image_url ? <img alt="" src={playlist.image_url} /> : <span>{playlistTitle(playlist).slice(0, 1).toUpperCase()}</span>}
      </span>
      <span className="player-saved-playlist-copy">
        <strong className="single-line-ellipsis">{playlistTitle(playlist)}</strong>
      </span>
    </button>
  );

  return (
    <section className="player-saved-panel" id="saved" aria-labelledby="player-saved-heading">
      <div className="player-saved-panel-header">
        <h3 id="player-saved-heading">Saved</h3>
      </div>
      <div className="player-saved-panel-layout">
        <aside className="player-saved-panel-sidebar" aria-label="Saved navigation and filters">
          <div className="player-saved-panel-tabs" role="tablist" aria-label="Saved">
            {tabs.map((tab) => (
              <div className="player-saved-panel-tab-group" key={tab.value}>
                <div
                  aria-selected={activeTab === tab.value}
                  className={activeTab === tab.value ? "player-saved-panel-tab player-saved-panel-tab-active" : "player-saved-panel-tab"}
                  role="tab"
                >
                  <button
                    className="player-saved-panel-tab-main"
                    onClick={() => {
                      if (tab.value === "playlists" && activeTab === "playlists") {
                        setSavedPlaylistCategoriesCollapsed((current) => !current);
                        return;
                      }
                      onTabChange(tab.value);
                      if (tab.value === "playlists") {
                        setSavedPlaylistCategoriesCollapsed(false);
                      }
                    }}
                    type="button"
                  >
                    <span>{tab.label}</span>
                  </button>
                  {tab.value === "playlists" && activeTab === "playlists" ? (
                    <div className="player-saved-tab-action-menu" ref={savedPlaylistActionsDropdownRef}>
                      <button
                        aria-expanded={savedPlaylistDropdown === "actions"}
                        aria-label="Playlist actions"
                        className="player-saved-panel-tab-open"
                        onClick={() => setSavedPlaylistDropdown((current) => current === "actions" ? null : "actions")}
                        title="Playlist actions"
                        type="button"
                      >
                        <svg aria-hidden="true" viewBox="0 0 24 24">
                          <path d="M19.4 13.5c.1-.5.1-1 .1-1.5s0-1-.1-1.5l2-1.5-2-3.5-2.4 1a8 8 0 0 0-2.6-1.5L14 2h-4l-.4 3a8 8 0 0 0-2.6 1.5l-2.4-1-2 3.5 2 1.5A9 9 0 0 0 4.5 12c0 .5 0 1 .1 1.5l-2 1.5 2 3.5 2.4-1a8 8 0 0 0 2.6 1.5l.4 3h4l.4-3a8 8 0 0 0 2.6-1.5l2.4 1 2-3.5-2-1.5ZM12 15.5A3.5 3.5 0 1 1 12 8a3.5 3.5 0 0 1 0 7.5Z" />
                        </svg>
                      </button>
                      {savedPlaylistDropdown === "actions" ? (
                        <div className="playlist-type-dropdown-menu player-saved-tab-action-dropdown" role="menu">
                          <button
                            className="playlist-type-dropdown-item"
                            onClick={() => onOpenPlaylistsOverlay(currentSavedPlaylistOverlayOptions(false))}
                            role="menuitem"
                            type="button"
                          >
                            <span className="playlist-type-dropdown-check" aria-hidden="true">↗</span>
                            <span>Open full</span>
                          </button>
                          <button
                            className="playlist-type-dropdown-item"
                            onClick={() => onOpenPlaylistsOverlay(currentSavedPlaylistOverlayOptions(true))}
                            role="menuitem"
                            type="button"
                          >
                            <span className="playlist-type-dropdown-check" aria-hidden="true">✓</span>
                            <span>Edit categories</span>
                          </button>
                          <button
                            className="playlist-type-dropdown-item"
                            onClick={() => window.open("https://open.spotify.com/collection/playlists", "_blank", "noopener,noreferrer")}
                            role="menuitem"
                            type="button"
                          >
                            <span className="playlist-type-dropdown-check" aria-hidden="true">♪</span>
                            <span>Open Spotify</span>
                          </button>
                        </div>
                      ) : null}
                    </div>
                  ) : (
                    <span className="player-saved-panel-tab-count">
                      {tab.value === "playlists" ? visibleOwnedPlaylists.length : tabCount(tab.value, savedQueues, trackBookmarks, entityBookmarks)}
                    </span>
                  )}
                </div>
                {tab.value === "playlists" && activeTab === "playlists" && !savedPlaylistCategoriesCollapsed ? (
                  <div className="player-saved-category-nav" aria-label="Playlist categories">
	                    <button
	                      aria-current={savedPlaylistCategoryIds.length === 0 ? "true" : undefined}
	                      className={`player-saved-category-nav-item player-saved-category-nav-item-meta${savedPlaylistCategoryIds.length === 0 ? " player-saved-category-nav-item-active" : ""}`}
	                      onClick={() => setSavedPlaylistCategoryIds([])}
	                      type="button"
	                    >
	                      <span className="player-saved-category-meta-spacer" aria-hidden="true" />
	                      <span>All categories</span>
	                      <span className="player-saved-panel-tab-count">{visibleOwnedPlaylists.length}</span>
	                    </button>
	                    {playlistLists.length > 0 ? playlistLists.map((list) => {
	                      const checked = savedPlaylistCategoryIds.length === 0 || savedPlaylistCategoryIds.includes(list.id);
	                      const active = savedPlaylistCategoryIds.includes(list.id);
	                      return (
                        <button
                          aria-checked={checked}
                          className={`player-saved-category-nav-item${checked ? " player-saved-category-nav-item-included" : ""}${active ? " player-saved-category-nav-item-active" : ""}`}
                          key={list.id}
                          onClick={() => setSavedPlaylistCategoryIds([list.id])}
                          role="menuitemcheckbox"
                          type="button"
                        >
                          <span
                            className="player-saved-category-check"
                            onClick={(event) => {
                              event.preventDefault();
                              event.stopPropagation();
                              toggleSavedPlaylistCategory(list.id);
                            }}
                            role="button"
                            tabIndex={-1}
                          >
	                            {checked ? "✓" : ""}
                          </span>
                          <span className="player-saved-category-label">{list.name}</span>
                          <span className="player-saved-panel-tab-count">{categoryCounts[list.id] ?? 0}</span>
                        </button>
                      );
	                    }) : (
	                      <span className="player-saved-category-empty">No categories yet</span>
	                    )}
	                  </div>
                ) : null}
              </div>
            ))}
          </div>
        </aside>
        <div className="player-saved-panel-main">
          <div className="player-saved-panel-controls" ref={savedPlaylistControlsRef}>
            <div className="player-saved-controls-primary">
            <div className="player-saved-control-field player-saved-editor-field">
              <div className="playlist-type-dropdown player-saved-control-dropdown" ref={savedPlaylistFilterDropdownRef}>
                <button
                  aria-expanded={savedPlaylistDropdown === "filter"}
                  className="playlist-type-dropdown-button"
                  disabled={activeTab !== "playlists"}
                  onClick={() => setSavedPlaylistDropdown((current) => current === "filter" ? null : "filter")}
                  type="button"
                >
                  {activeTab === "playlists" && savedPlaylistFilters.length > 0 ? <span className="playlist-dropdown-active-dot" aria-hidden="true" /> : null}
                  <span>{activeTab === "playlists" ? savedPlaylistFilterLabel : "All"}</span>
                  <span className="playlist-type-dropdown-chevron" aria-hidden="true">v</span>
                </button>
                {savedPlaylistDropdown === "filter" ? (
                  <div className="playlist-type-dropdown-menu player-saved-control-menu" role="menu">
                    <button
                      aria-checked={savedPlaylistFilters.length === 0}
                      className={`playlist-type-dropdown-item${savedPlaylistFilters.length === 0 ? " playlist-type-dropdown-item-active" : ""}`}
                      onClick={() => setSavedPlaylistFilters([])}
                      role="menuitemcheckbox"
                      type="button"
                    >
                      <span className="playlist-type-dropdown-check" aria-hidden="true">{savedPlaylistFilters.length === 0 ? "✓" : ""}</span>
                      <span>All</span>
                      <span className="playlist-tab-count">{categoryFilteredOwnedPlaylists.length}</span>
                    </button>
                    <div className="playlist-type-dropdown-divider" />
                    {savedPlaylistFilterOptions.map((option) => {
                      const checked = savedPlaylistFilters.length === 0 || savedPlaylistFilters.includes(option.value);
                      const optionCount = filterCounts[option.value];
                      const optionDisabled = !checked && optionCount === 0;
                      const showVisibilityToggle = checked && (option.value === "yours" || option.value === "collabs");
                      const visibility = option.value === "yours" ? savedPlaylistYoursVisibility : savedPlaylistCollabVisibility;
                      const setVisibility = option.value === "yours" ? setSavedPlaylistYoursVisibility : setSavedPlaylistCollabVisibility;
                      return (
                        <div className="player-saved-editor-option" key={option.value}>
                        <button
                          aria-checked={checked}
                          className={`playlist-type-dropdown-item${checked ? " playlist-type-dropdown-item-active" : ""}`}
                          disabled={optionDisabled}
                          onClick={() => setSavedPlaylistFilters([option.value])}
                          role="menuitemcheckbox"
                          type="button"
                        >
                          <span
                            className="playlist-type-dropdown-check"
                            onClick={(event) => {
                              event.preventDefault();
                              event.stopPropagation();
                              if (optionDisabled) {
                                return;
                              }
                              toggleSavedPlaylistFilter(option.value);
                            }}
                            role="button"
                            tabIndex={-1}
                          >
                            {checked ? "✓" : ""}
                          </span>
                          <span>{option.label}</span>
                          <span className="playlist-tab-count">{optionCount}</span>
                        </button>
                        {showVisibilityToggle ? (
                          <div className="player-saved-editor-visibility-row">
                            {renderSavedPlaylistVisibilityToggle(`${option.label} visibility`, visibility, setVisibility)}
                          </div>
                        ) : null}
                        </div>
                      );
                    })}
                  </div>
                ) : null}
              </div>
            </div>
            <div className="player-saved-control-field player-saved-organize-field">
              <div className="playlist-type-dropdown player-saved-control-dropdown" ref={savedPlaylistOrganizeDropdownRef}>
                <button
                  aria-expanded={savedPlaylistDropdown === "organize"}
                  className="playlist-type-dropdown-button"
                  disabled={activeTab !== "playlists"}
                  onClick={() => setSavedPlaylistDropdown((current) => current === "organize" ? null : "organize")}
                  type="button"
                >
                  {activeTab === "playlists" && (savedPlaylistSort !== "name_asc" || savedPlaylistGrouping !== "none") ? <span className="playlist-dropdown-active-dot" aria-hidden="true" /> : null}
                  <span>Organize</span>
                  <span className="playlist-type-dropdown-chevron" aria-hidden="true">v</span>
                </button>
                {savedPlaylistDropdown === "organize" ? (
                  <div className="playlist-type-dropdown-menu player-saved-control-menu player-saved-organize-menu" role="menu">
                    <div className="player-saved-organize-section-label">Sort</div>
                    {savedPlaylistSortOptions.map((option) => {
                      const checked = savedPlaylistSort === option.value;
                      return (
                        <button
                          aria-checked={checked}
                          className={`playlist-type-dropdown-item${checked ? " playlist-type-dropdown-item-active" : ""}`}
                          key={option.value}
                          onClick={() => setSavedPlaylistSort(option.value)}
                          role="menuitemradio"
                          type="button"
                        >
                          <span className="playlist-type-dropdown-check" aria-hidden="true">{checked ? "✓" : ""}</span>
                          <span>{option.label}</span>
                        </button>
                      );
                    })}
                    <div className="playlist-type-dropdown-divider" />
                    <div className="player-saved-organize-section-label">Group</div>
                    {savedPlaylistGroupingOptions.map((option) => {
                      const checked = savedPlaylistGrouping === option.value;
                      const disabled = !checked && (
                        (option.value === "editor" && savedPlaylistOnlyYoursSelected)
                        || (option.value === "category" && savedPlaylistSingleCategorySelected)
                      );
                      return (
                        <button
                          aria-checked={checked}
                          className={`playlist-type-dropdown-item${checked ? " playlist-type-dropdown-item-active" : ""}`}
                          disabled={disabled}
                          key={option.value}
                          onClick={() => setSavedPlaylistGrouping((current) => current === option.value ? "none" : option.value)}
                          role="menuitemradio"
                          type="button"
                        >
                          <span className="playlist-type-dropdown-check" aria-hidden="true">{checked ? "✓" : ""}</span>
                          <span>{option.label}</span>
                        </button>
                      );
                    })}
                  </div>
                ) : null}
              </div>
            </div>
            <div className="player-saved-controls-summary">
              <div className="playlist-filter-summary-actions player-saved-playlist-summary">
                <span className="playlist-filter-result-count">
                  {activeTab === "playlists" ? savedPlaylistItems.length.toLocaleString() : 0} shown
                </span>
                {activeTab === "playlists" && savedPlaylistCategoryIds.length > 0 ? (
                  <button
                    className="playlist-filter-summary-button"
                    onClick={showAllSavedPlaylistCategories}
                    type="button"
                  >
                    Show all categories
                  </button>
                ) : null}
              </div>
            </div>
            </div>
          </div>
          <div className="player-saved-panel-body" role="tabpanel">
          {activeTab === "queues" ? (
            savedQueues.length > 0 ? (
              <div className="player-saved-queue-list">
                {savedQueues.map((snapshot) => {
                  const imageUrl = savedQueueImage(snapshot);
                  const trackCount = savedQueueTrackCount(snapshot);
                  const activeCursor = snapshot.activeCursor != null && snapshot.activeCursor >= 0 ? snapshot.activeCursor : null;
                  return (
                    <div className="player-saved-queue-row" key={snapshot.id}>
                      <div className="player-saved-queue-image" aria-hidden="true">
                        {imageUrl ? <img alt="" src={imageUrl} /> : <span>{savedQueueTitle(snapshot).slice(0, 1).toUpperCase()}</span>}
                      </div>
                      <div className="player-saved-queue-copy">
                        <span className="player-saved-queue-title single-line-ellipsis">{savedQueueTitle(snapshot)}</span>
                        <span className="player-saved-queue-meta single-line-ellipsis">
                          {formatSavedAt(snapshot.savedAt)} · {snapshot.groups.length} context{snapshot.groups.length === 1 ? "" : "s"} · {trackCount} track{trackCount === 1 ? "" : "s"}
                          {activeCursor != null && trackCount > 0 ? ` · position ${Math.min(activeCursor + 1, trackCount)}/${trackCount}` : ""}
                        </span>
                      </div>
                      <div className="player-saved-queue-actions">
                        <button onClick={() => onRestoreSavedQueue(snapshot)} type="button">
                          Restore
                        </button>
                        <button aria-label={`Delete saved queue ${savedQueueTitle(snapshot)}`} onClick={() => onDeleteSavedQueue(snapshot.id)} type="button">
                          Delete
                        </button>
                      </div>
                    </div>
                  );
                })}
              </div>
            ) : (
              <p>{active.empty}</p>
            )
          ) : activeTab === "bookmarks" ? (
            trackBookmarks.length > 0 || entityBookmarks.length > 0 ? (
              <div className="player-saved-queue-list">
                {entityBookmarks.map((bookmark) => (
                  <div className="player-saved-queue-row player-track-bookmark-row" key={bookmark.id}>
                    <div className="player-saved-queue-image" aria-hidden="true">
                      {bookmark.imageUrl ? <img alt="" src={bookmark.imageUrl} /> : <span>{bookmark.label.slice(0, 1).toUpperCase()}</span>}
                    </div>
                    <div className="player-saved-queue-copy">
                      <span className="player-saved-queue-title single-line-ellipsis">{bookmark.label}</span>
                      <span className="player-saved-queue-meta single-line-ellipsis">
                        {bookmark.type.slice(0, 1).toUpperCase() + bookmark.type.slice(1)}
                        {bookmark.meta ? ` · ${bookmark.meta}` : ""}
                        {bookmark.detail ? ` · ${bookmark.detail}` : ""}
                        {" · "}
                        {formatSavedAt(bookmark.bookmarkedAt)}
                      </span>
                    </div>
                    <div className="player-saved-queue-actions">
                      <button onClick={() => onOpenEntityBookmark(bookmark)} type="button">
                        Open
                      </button>
                      <button aria-label={`Remove bookmark ${bookmark.label}`} onClick={() => onDeleteEntityBookmark(bookmark.id)} type="button">
                        Remove
                      </button>
                    </div>
                  </div>
                ))}
                {trackBookmarks.map((bookmark) => (
                  <div className="player-saved-queue-row player-track-bookmark-row" key={bookmark.id}>
                    <div className="player-saved-queue-image" aria-hidden="true">
                      {bookmark.track.image ? <img alt="" src={bookmark.track.image} /> : <span>{bookmark.track.name.slice(0, 1).toUpperCase()}</span>}
                    </div>
                    <div className="player-saved-queue-copy">
                      <span className="player-saved-queue-title single-line-ellipsis">{bookmark.track.name}</span>
                      <span className="player-saved-queue-meta single-line-ellipsis">
                        {bookmark.track.artists || "Unknown artist"} · {bookmark.track.album || "Unknown album"} · {formatSavedAt(bookmark.bookmarkedAt)}
                      </span>
                      {bookmarkContextLabel(bookmark) ? (
                        <span className="player-saved-queue-meta player-track-bookmark-context single-line-ellipsis">
                          {bookmarkContextLabel(bookmark)}
                          {bookmark.context?.position != null ? ` · #${bookmark.context.position + 1}` : ""}
                        </span>
                      ) : null}
                    </div>
                    <div className="player-saved-queue-actions">
                      <button disabled={!bookmark.track.uri} onClick={() => onPlayBookmark("play_now", bookmark)} type="button">
                        Play
                      </button>
                      <button disabled={!bookmark.track.uri} onClick={() => onPlayBookmark("play_next", bookmark)} type="button">
                        Next
                      </button>
                      <button onClick={() => onOpenBookmark(bookmark)} type="button">
                        Open
                      </button>
                      <button aria-label={`Remove bookmark ${bookmark.track.name}`} onClick={() => onDeleteBookmark(bookmark.id)} type="button">
                        Remove
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <p>{active.empty}</p>
            )
	          ) : activeTab === "playlists" ? (
	            ownedPlaylistsAvailable || ownedPlaylists.length > 0 ? (
		              visibleOwnedPlaylists.length > 0 ? (
		                <div className="player-saved-playlist-panel">
		                  {savedPlaylistItems.length > 0 ? (
                    savedPlaylistGrouping === "none" ? (
                      <div className="player-saved-playlist-grid">
                        {savedPlaylistVisibleItems.map(renderSavedPlaylistCard)}
                      </div>
                    ) : (
                      <div className="player-saved-playlist-groups">
                        {savedPlaylistGroups.map((group) => (
                          <section className="player-saved-playlist-group" key={group.label || "all"}>
                            <div className="player-saved-playlist-group-header">
                              <span className="single-line-ellipsis">{playlistGroupDisplayLabel(group.label || "All playlists")}</span>
                              <span>{group.playlists.length}</span>
                            </div>
                            <div className="player-saved-playlist-grid">
                              {group.playlists.map(renderSavedPlaylistCard)}
                            </div>
                          </section>
                        ))}
                      </div>
                    )
	                  ) : (
	                    <p>No playlists match this filter.</p>
	                  )}
                </div>
              ) : (
                <p>No visible playlists.</p>
              )
            ) : (
              <p>Playlist metadata is still loading.</p>
            )
          ) : (
            <p>{active.empty}</p>
          )}
          </div>
        </div>
      </div>
    </section>
  );
}
