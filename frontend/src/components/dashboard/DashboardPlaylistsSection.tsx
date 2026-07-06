import { useEffect, useRef, useState, type ReactNode } from "react";
import type { OwnedPlaylist, PreviewItem, SectionKey } from "../../types/appTypes";
import type { SavedPlaylistGrouping, SavedPlaylistOverlayOptions, SavedPlaylistSort } from "../playback/PlayerBottomDrawer";
import { previewItems } from "../../utils/dashboardUtils";
import { playlistEditorDisplayLabel, spotifyUserLabel } from "../../utils/playlistDisplay";
import { DashboardPlaylistColumn } from "./DashboardColumns";
import { PreviewCard } from "./PreviewCard";

type PlaylistTabKey = "all" | "created" | "collaborations" | "others";

type MinePlaylistVisibility = "public" | "private";
type CollaborationPlaylistOwner = "yours" | "others";
type PlaylistCategoryFilterMode = "all" | "none" | "selected";
type PlaylistTrackCountBucket = "zero" | "small" | "medium" | "large" | "xlarge" | "huge";
type PlaylistList = {
  id: string;
  name: string;
  playlistIds: string[];
};
type PlaylistEditSnapshot = {
  playlistLists: PlaylistList[];
  pinnedPlaylistIds: string[];
  hiddenPlaylistIds: string[];
};

const PLAYLIST_LISTS_STORAGE_KEY = "listenlab.playlistLists.v1";
const PINNED_PLAYLISTS_STORAGE_KEY = "listenlab.pinnedPlaylists.v1";
const PLAYLIST_FILTERS_STORAGE_KEY = "listenlab.playlistFilters.v1";
const PLAYLIST_TAB_ORDER_STORAGE_KEY = "listenlab.playlistTabOrder.v1";
const UNCATEGORIZED_PLAYLIST_CATEGORY_ID = "__uncategorized__";

const PLAYLIST_TABS: Array<{ key: PlaylistTabKey; label: string }> = [
  { key: "all", label: "All" },
  { key: "created", label: "Yours" },
  { key: "collaborations", label: "Collabs" },
  { key: "others", label: "Others" },
];
const PLAYLIST_SORT_OPTIONS: Array<{ value: SavedPlaylistSort; label: string }> = [
  { value: "name_asc", label: "Name A-Z" },
  { value: "name_desc", label: "Name Z-A" },
  { value: "tracks_desc", label: "Tracks high-low" },
  { value: "tracks_asc", label: "Tracks low-high" },
];
const PLAYLIST_GROUPING_OPTIONS: Array<{ value: SavedPlaylistGrouping; label: string }> = [
  { value: "none", label: "No grouping" },
  { value: "editor", label: "Editor" },
  { value: "category", label: "Category" },
  { value: "track_count", label: "Track count" },
];
const PLAYLIST_TRACK_COUNT_BUCKETS: Array<{ value: PlaylistTrackCountBucket; label: string }> = [
  { value: "zero", label: "0 tracks" },
  { value: "small", label: "1-15 tracks" },
  { value: "medium", label: "16-50 tracks" },
  { value: "large", label: "51-200 tracks" },
  { value: "xlarge", label: "201-1,000 tracks" },
  { value: "huge", label: "1,001+ tracks" },
];

function isMinePublicPlaylist(playlist: OwnedPlaylist) {
  return Boolean(playlist.is_owned && playlist.is_public !== false);
}

function isMinePrivatePlaylist(playlist: OwnedPlaylist) {
  return Boolean(playlist.is_owned && playlist.is_public === false);
}

function isExclusiveMinePlaylist(playlist: OwnedPlaylist) {
  return Boolean(playlist.is_owned && !playlist.is_collaborative);
}

function playlistsForTab(
  playlists: OwnedPlaylist[],
  tab: PlaylistTabKey,
  mineVisibility: MinePlaylistVisibility[],
  collaborationOwners: CollaborationPlaylistOwner[] = ["yours", "others"],
  collaborationVisibility: MinePlaylistVisibility[] = ["public", "private"],
) {
  switch (tab) {
    case "created":
      return playlists.filter((playlist) => (
        isExclusiveMinePlaylist(playlist)
        && (
          (mineVisibility.includes("public") && playlist.is_public !== false)
          || (mineVisibility.includes("private") && playlist.is_public === false)
        )
      ));
    case "others":
      return playlists.filter((playlist) => !playlist.is_owned);
    case "collaborations":
      return playlists.filter((playlist) => (
        playlist.is_collaborative
        && (
          (collaborationOwners.includes("yours") && playlist.is_owned)
          || (collaborationOwners.includes("others") && !playlist.is_owned)
        )
        && (
          (collaborationVisibility.includes("public") && playlist.is_public !== false)
          || (collaborationVisibility.includes("private") && playlist.is_public === false)
        )
      ));
    case "all":
    default:
      return playlists;
  }
}

function uniquePlaylists(playlists: OwnedPlaylist[]) {
  const seen = new Set<string>();
  return playlists.filter((playlist, index) => {
    const key = playlist.playlist_id ?? playlist.url ?? `${playlist.name ?? "playlist"}-${index}`;
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

function playlistStableId(playlist: OwnedPlaylist, index = 0) {
  return playlist.playlist_id ?? playlist.url ?? `${playlist.name ?? "playlist"}-${index}`;
}

function playlistTitle(playlist: OwnedPlaylist) {
  return playlist.name || "Untitled playlist";
}

function playlistTrackCount(playlist: OwnedPlaylist) {
  return typeof playlist.track_count === "number" ? playlist.track_count : 0;
}

function playlistEditorGroupUrl(playlists: OwnedPlaylist[]) {
  const externalOwnerIds = Array.from(new Set(playlists
    .filter((playlist) => !playlist.is_owned && !playlist.is_collaborative)
    .map((playlist) => String(playlist.owner_id ?? "").trim())
    .filter(Boolean)));
  return externalOwnerIds.length === 1 ? `https://open.spotify.com/user/${encodeURIComponent(externalOwnerIds[0])}` : null;
}

function playlistTrackCountGroupLabel(playlist: OwnedPlaylist) {
  return PLAYLIST_TRACK_COUNT_BUCKETS.find((bucket) => bucket.value === playlistTrackCountBucket(playlist))?.label ?? "1,001+ tracks";
}

function playlistTrackCountBucket(playlist: OwnedPlaylist): PlaylistTrackCountBucket {
  const count = playlistTrackCount(playlist);
  if (count === 0) {
    return "zero";
  }
  if (count <= 15) {
    return "small";
  }
  if (count <= 50) {
    return "medium";
  }
  if (count <= 200) {
    return "large";
  }
  if (count <= 1000) {
    return "xlarge";
  }
  return "huge";
}

function playlistTrackCountGroupRank(label: string) {
  return PLAYLIST_TRACK_COUNT_BUCKETS.findIndex((bucket) => bucket.label === label);
}

function playlistGroupDisplayLabel(label: string) {
  return spotifyUserLabel(label);
}

function comparePlaylists(left: OwnedPlaylist, right: OwnedPlaylist, sort: SavedPlaylistSort) {
  if (sort === "tracks_desc" || sort === "tracks_asc") {
    const direction = sort === "tracks_desc" ? -1 : 1;
    const countDiff = playlistTrackCount(left) - playlistTrackCount(right);
    if (countDiff !== 0) {
      return countDiff * direction;
    }
  }
  const nameDiff = playlistTitle(left).localeCompare(playlistTitle(right), undefined, { sensitivity: "base" });
  return sort === "name_desc" ? -nameDiff : nameDiff;
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

function readStoredPinnedPlaylistIds(): string[] {
  if (typeof window === "undefined") {
    return [];
  }
  try {
    const parsed = JSON.parse(window.localStorage.getItem(PINNED_PLAYLISTS_STORAGE_KEY) ?? "[]");
    return Array.isArray(parsed) ? parsed.map((value) => String(value ?? "").trim()).filter(Boolean) : [];
  } catch {
    return [];
  }
}

function readStoredPlaylistTabOrder(): PlaylistTabKey[] {
  const fallback = PLAYLIST_TABS.map((tab) => tab.key);
  if (typeof window === "undefined") {
    return fallback;
  }
  try {
    const parsed = JSON.parse(window.localStorage.getItem(PLAYLIST_TAB_ORDER_STORAGE_KEY) ?? "[]");
    if (!Array.isArray(parsed)) {
      return fallback;
    }
    const stored = parsed.filter((value: unknown): value is PlaylistTabKey => (
      value === "all"
      || value === "created"
      || value === "collaborations"
      || value === "others"
    ));
    return [...stored, ...fallback.filter((key) => !stored.includes(key))];
  } catch {
    return fallback;
  }
}

function clonePlaylistLists(lists: PlaylistList[]): PlaylistList[] {
  return lists.map((list) => ({ ...list, playlistIds: [...list.playlistIds] }));
}

function readStoredPlaylistFilters(): {
  selectedTabs: PlaylistTabKey[];
  mineVisibility: MinePlaylistVisibility[];
  collaborationVisibility: MinePlaylistVisibility[];
  collaborationOwners: CollaborationPlaylistOwner[];
  categoryFilterMode: PlaylistCategoryFilterMode;
  selectedListIds: string[];
} {
  const fallback = {
    selectedTabs: ["all"] as PlaylistTabKey[],
    mineVisibility: ["public", "private"] as MinePlaylistVisibility[],
    collaborationVisibility: ["public", "private"] as MinePlaylistVisibility[],
    collaborationOwners: ["yours", "others"] as CollaborationPlaylistOwner[],
    categoryFilterMode: "all" as PlaylistCategoryFilterMode,
    selectedListIds: [] as string[],
  };
  if (typeof window === "undefined") {
    return fallback;
  }
  try {
    const parsed = JSON.parse(window.localStorage.getItem(PLAYLIST_FILTERS_STORAGE_KEY) ?? "{}");
    const selectedTabs = Array.isArray(parsed?.selectedTabs)
      ? parsed.selectedTabs.filter((value: unknown): value is PlaylistTabKey => (
        value === "all"
        || value === "created"
        || value === "collaborations"
        || value === "others"
      ))
      : fallback.selectedTabs;
    const mineVisibility = Array.isArray(parsed?.mineVisibility)
      ? parsed.mineVisibility.filter((value: unknown): value is MinePlaylistVisibility => value === "public" || value === "private")
      : fallback.mineVisibility;
    const collaborationOwners = Array.isArray(parsed?.collaborationOwners)
      ? parsed.collaborationOwners.filter((value: unknown): value is CollaborationPlaylistOwner => value === "yours" || value === "others")
      : fallback.collaborationOwners;
    const collaborationVisibility = Array.isArray(parsed?.collaborationVisibility)
      ? parsed.collaborationVisibility.filter((value: unknown): value is MinePlaylistVisibility => value === "public" || value === "private")
      : fallback.collaborationVisibility;
    const selectedListIds = Array.isArray(parsed?.selectedListIds)
      ? parsed.selectedListIds.map((value: unknown) => String(value ?? "").trim()).filter(Boolean)
      : fallback.selectedListIds;
    const categoryFilterMode = parsed?.categoryFilterMode === "none" || parsed?.categoryFilterMode === "selected" || parsed?.categoryFilterMode === "all"
      ? parsed.categoryFilterMode as PlaylistCategoryFilterMode
      : selectedListIds.length > 0 ? "selected" : fallback.categoryFilterMode;
    return {
      selectedTabs: selectedTabs.length > 0 ? selectedTabs : fallback.selectedTabs,
      mineVisibility: mineVisibility.length > 0 ? mineVisibility : fallback.mineVisibility,
      collaborationVisibility: collaborationVisibility.length > 0 ? collaborationVisibility : fallback.collaborationVisibility,
      collaborationOwners: collaborationOwners.length > 0 ? collaborationOwners : fallback.collaborationOwners,
      categoryFilterMode,
      selectedListIds,
    };
  } catch {
    return fallback;
  }
}

function playlistsForSelection(
  playlists: OwnedPlaylist[],
  selectedTabs: PlaylistTabKey[],
  mineVisibility: MinePlaylistVisibility[],
  collaborationOwners: CollaborationPlaylistOwner[],
  collaborationVisibility: MinePlaylistVisibility[],
) {
  if (selectedTabs.includes("all")) {
    return playlists;
  }
  return uniquePlaylists(selectedTabs.flatMap((tab) => playlistsForTab(playlists, tab, mineVisibility, collaborationOwners, collaborationVisibility)));
}

type DashboardPlaylistsSectionProps = {
  ownedPlaylists: OwnedPlaylist[];
  ownedPlaylistsAvailable: boolean;
  apiBaseUrl: string;
  activePlaylistPlayback?: {
    playlistId: string | null;
    playlistName?: string | null;
    trackId?: string | null;
    trackUri?: string | null;
    position?: number | null;
    isPlaying: boolean;
  } | null;
  playlistOverlayOpen: boolean;
  playlistOverlayOptions: SavedPlaylistOverlayOptions | null;
  playlistsOpen: boolean;
  setPlaylistOverlayOpen: (open: boolean) => void;
  onConsumePlaylistOverlayOptions: () => void;
  toggleSection: (section: SectionKey, anchorId?: string) => void;
  onSelectPreview: (preview: PreviewItem) => void;
  onHidePlaylist: (playlist: OwnedPlaylist) => void;
  onUnhidePlaylist: (playlist: OwnedPlaylist) => void;
  onDeletePlaylist: (playlist: OwnedPlaylist) => void;
  renderSectionTitle: (title: string, staleSection?: string) => ReactNode;
};

export function DashboardPlaylistsSection({
  ownedPlaylists,
  ownedPlaylistsAvailable,
  apiBaseUrl,
  activePlaylistPlayback,
  playlistOverlayOpen,
  playlistOverlayOptions,
  playlistsOpen,
  setPlaylistOverlayOpen,
  onConsumePlaylistOverlayOptions,
  toggleSection,
  onSelectPreview,
  onHidePlaylist,
  onUnhidePlaylist,
  onDeletePlaylist,
  renderSectionTitle,
}: DashboardPlaylistsSectionProps) {
  const playlistFiltersRef = useRef<HTMLDivElement | null>(null);
  const storedPlaylistFilters = useRef(readStoredPlaylistFilters());
  const previousPlaylistCategorySignatureRef = useRef("");
  const [selectedPlaylistTabs, setSelectedPlaylistTabs] = useState<PlaylistTabKey[]>(() => storedPlaylistFilters.current.selectedTabs);
  const [mineVisibility, setMineVisibility] = useState<MinePlaylistVisibility[]>(() => storedPlaylistFilters.current.mineVisibility);
  const [collaborationVisibility, setCollaborationVisibility] = useState<MinePlaylistVisibility[]>(() => storedPlaylistFilters.current.collaborationVisibility);
  const [collaborationOwners, setCollaborationOwners] = useState<CollaborationPlaylistOwner[]>(() => storedPlaylistFilters.current.collaborationOwners);
  const [playlistTypeMenuOpen, setPlaylistTypeMenuOpen] = useState(false);
  const [collaborationOwnerMenuOpen, setCollaborationOwnerMenuOpen] = useState(false);
  const [playlistTrackCountMenuOpen, setPlaylistTrackCountMenuOpen] = useState(false);
  const [playlistSortMenuOpen, setPlaylistSortMenuOpen] = useState(false);
  const [playlistGroupMenuOpen, setPlaylistGroupMenuOpen] = useState(false);
  const [playlistEditMode, setPlaylistEditMode] = useState(false);
  const [showHiddenPlaylists, setShowHiddenPlaylists] = useState(false);
  const [playlistListMenuOpen, setPlaylistListMenuOpen] = useState(false);
  const [playlistLists, setPlaylistLists] = useState<PlaylistList[]>(() => readStoredPlaylistLists());
  const [pinnedPlaylistIds, setPinnedPlaylistIds] = useState<string[]>(() => readStoredPinnedPlaylistIds());
  const [playlistCategoryFilterMode, setPlaylistCategoryFilterMode] = useState<PlaylistCategoryFilterMode>(() => storedPlaylistFilters.current.categoryFilterMode);
  const [selectedPlaylistListIds, setSelectedPlaylistListIds] = useState<string[]>(() => storedPlaylistFilters.current.selectedListIds);
  const [playlistCategoryCreatorOpen, setPlaylistCategoryCreatorOpen] = useState(false);
  const [newPlaylistListName, setNewPlaylistListName] = useState("");
  const [playlistCategoriesLoaded, setPlaylistCategoriesLoaded] = useState(false);
  const [playlistEditSnapshot, setPlaylistEditSnapshot] = useState<PlaylistEditSnapshot | null>(null);
  const [draftHiddenPlaylistIds, setDraftHiddenPlaylistIds] = useState<string[]>([]);
  const [playlistTabOrder, setPlaylistTabOrder] = useState<PlaylistTabKey[]>(() => readStoredPlaylistTabOrder());
  const [playlistEditCloseAction, setPlaylistEditCloseAction] = useState<"save" | "cancel" | null>(null);
  const [playlistSort, setPlaylistSort] = useState<SavedPlaylistSort>("name_asc");
  const [playlistGrouping, setPlaylistGrouping] = useState<SavedPlaylistGrouping>("none");
  const [selectedTrackCountBuckets, setSelectedTrackCountBuckets] = useState<PlaylistTrackCountBucket[]>([]);
  const playlistsFullOpen = playlistsOpen || playlistOverlayOpen;
  const playlistCategorySignature = `${playlistCategoryFilterMode}:${showHiddenPlaylists ? "hidden" : "visible"}:${selectedPlaylistListIds.join("\u0000")}`;

  useEffect(() => {
    if (!playlistOverlayOpen) {
      return;
    }
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setPlaylistOverlayOpen(false);
      }
    }
    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [playlistOverlayOpen]);

  useEffect(() => {
    if (!playlistOverlayOpen || typeof document === "undefined") {
      return;
    }
    const previousBodyOverflow = document.body.style.overflow;
    const previousHtmlOverflow = document.documentElement.style.overflow;
    const previousBodyOverscroll = document.body.style.overscrollBehavior;
    const previousHtmlOverscroll = document.documentElement.style.overscrollBehavior;
    document.body.style.overflow = "hidden";
    document.documentElement.style.overflow = "hidden";
    document.body.style.overscrollBehavior = "none";
    document.documentElement.style.overscrollBehavior = "none";
    return () => {
      document.body.style.overflow = previousBodyOverflow;
      document.documentElement.style.overflow = previousHtmlOverflow;
      document.body.style.overscrollBehavior = previousBodyOverscroll;
      document.documentElement.style.overscrollBehavior = previousHtmlOverscroll;
    };
  }, [playlistOverlayOpen]);

  useEffect(() => {
    if (playlistsFullOpen) {
      return;
    }
    setPlaylistTypeMenuOpen(false);
    setCollaborationOwnerMenuOpen(false);
    setPlaylistTrackCountMenuOpen(false);
    setPlaylistSortMenuOpen(false);
    setPlaylistGroupMenuOpen(false);
    setPlaylistListMenuOpen(false);
    setPlaylistCategoryCreatorOpen(false);
    setPlaylistEditMode(false);
    setPlaylistEditCloseAction(null);
    setShowHiddenPlaylists(false);
    setPlaylistEditSnapshot(null);
    setDraftHiddenPlaylistIds([]);
    setSelectedPlaylistTabs((current) => current.length > 0 ? current : ["all"]);
    setMineVisibility((current) => current.length > 0 ? current : ["public", "private"]);
    setCollaborationVisibility((current) => current.length > 0 ? current : ["public", "private"]);
    setCollaborationOwners((current) => current.length > 0 ? current : ["yours", "others"]);
  }, [playlistsFullOpen]);

  useEffect(() => {
    if (!playlistTypeMenuOpen && !collaborationOwnerMenuOpen && !playlistListMenuOpen && !playlistTrackCountMenuOpen && !playlistSortMenuOpen && !playlistGroupMenuOpen) {
      return;
    }
    function closeOpenPlaylistMenus() {
      closePlaylistTypeMenu();
      closeCollaborationOwnerMenu();
      closePlaylistListMenu();
      setPlaylistTrackCountMenuOpen(false);
      setPlaylistSortMenuOpen(false);
      setPlaylistGroupMenuOpen(false);
    }
    function handlePointerDown(event: PointerEvent) {
      const target = event.target;
      if (!(target instanceof Element)) {
        return;
      }
      if (target.closest(".playlist-type-dropdown")) {
        return;
      }
      closeOpenPlaylistMenus();
    }
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        closeOpenPlaylistMenus();
      }
    }
    document.addEventListener("pointerdown", handlePointerDown, true);
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("pointerdown", handlePointerDown, true);
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [playlistTypeMenuOpen, collaborationOwnerMenuOpen, playlistListMenuOpen, playlistTrackCountMenuOpen, playlistSortMenuOpen, playlistGroupMenuOpen]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    window.localStorage.setItem(PLAYLIST_LISTS_STORAGE_KEY, JSON.stringify(playlistLists));
  }, [playlistLists]);

  useEffect(() => {
    if (!playlistsFullOpen || playlistCategoriesLoaded) {
      return;
    }
    let cancelled = false;
    async function loadPlaylistCategories() {
      try {
        const response = await fetch(`${apiBaseUrl}/playlists/categories`, { credentials: "include" });
        if (!response.ok) {
          throw new Error(`Playlist categories failed to load (${response.status}).`);
        }
        const payload = await response.json() as { items?: PlaylistList[] };
        const sqlCategories = Array.isArray(payload.items) ? payload.items : [];
        if (cancelled) {
          return;
        }
        if (sqlCategories.length > 0) {
          setPlaylistLists(sqlCategories);
          setPlaylistCategoriesLoaded(true);
          return;
        }
        const localCategories = readStoredPlaylistLists();
        if (localCategories.length === 0) {
          setPlaylistLists([]);
          setPlaylistCategoriesLoaded(true);
          return;
        }
        const migratedCategories: PlaylistList[] = [];
        for (const localCategory of localCategories) {
          const createResponse = await fetch(`${apiBaseUrl}/playlists/categories`, {
            method: "POST",
            credentials: "include",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name: localCategory.name }),
          });
          if (!createResponse.ok) {
            continue;
          }
          const createPayload = await createResponse.json() as { category?: PlaylistList };
          const category = createPayload.category;
          if (!category?.id) {
            continue;
          }
          const playlistIds = Array.from(new Set(localCategory.playlistIds));
          for (const playlistId of playlistIds) {
            await fetch(`${apiBaseUrl}/playlists/categories/${encodeURIComponent(category.id)}/playlists/${encodeURIComponent(playlistId)}`, {
              method: "PUT",
              credentials: "include",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ included: true }),
            });
          }
          migratedCategories.push({ ...category, playlistIds });
        }
        if (!cancelled) {
          setPlaylistLists(migratedCategories);
          setPlaylistCategoriesLoaded(true);
        }
      } catch (error) {
        console.error(error);
        if (!cancelled) {
          setPlaylistCategoriesLoaded(true);
        }
      }
    }
    void loadPlaylistCategories();
    return () => {
      cancelled = true;
    };
  }, [apiBaseUrl, playlistCategoriesLoaded, playlistsFullOpen]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    window.localStorage.setItem(PINNED_PLAYLISTS_STORAGE_KEY, JSON.stringify(pinnedPlaylistIds));
  }, [pinnedPlaylistIds]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    window.localStorage.setItem(PLAYLIST_TAB_ORDER_STORAGE_KEY, JSON.stringify(playlistTabOrder));
  }, [playlistTabOrder]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    window.localStorage.setItem(PLAYLIST_FILTERS_STORAGE_KEY, JSON.stringify({
      selectedTabs: selectedPlaylistTabs,
      mineVisibility,
      collaborationVisibility,
      collaborationOwners,
      categoryFilterMode: playlistCategoryFilterMode,
      selectedListIds: selectedPlaylistListIds,
    }));
  }, [selectedPlaylistTabs, mineVisibility, collaborationVisibility, collaborationOwners, playlistCategoryFilterMode, selectedPlaylistListIds]);

  useEffect(() => {
    setSelectedPlaylistListIds((current) => current.filter((listId) => playlistLists.some((list) => list.id === listId)));
  }, [playlistLists]);

  useEffect(() => {
    if (!playlistOverlayOpen || !playlistOverlayOptions) {
      return;
    }
    const hasPrivateFilter = playlistOverlayOptions.filters.includes("private");
    const nextTabs: PlaylistTabKey[] = playlistOverlayOptions.filters.length === 0
      ? ["all"]
      : [
        playlistOverlayOptions.filters.includes("yours") || hasPrivateFilter ? "created" : null,
        playlistOverlayOptions.filters.includes("collabs") || hasPrivateFilter ? "collaborations" : null,
        playlistOverlayOptions.filters.includes("others") ? "others" : null,
      ].filter((value): value is PlaylistTabKey => Boolean(value));
    setSelectedPlaylistTabs(nextTabs.length > 0 ? nextTabs : ["all"]);
    setMineVisibility(hasPrivateFilter ? ["private"] : playlistOverlayOptions.yoursVisibility);
    setCollaborationVisibility(hasPrivateFilter ? ["private"] : playlistOverlayOptions.collabVisibility);
    setCollaborationOwners(["yours", "others"]);
    if (playlistOverlayOptions.categoryIds.length === 0) {
      setPlaylistCategoryFilterMode("all");
      setSelectedPlaylistListIds([]);
    } else if (playlistOverlayOptions.categoryIds.includes(UNCATEGORIZED_PLAYLIST_CATEGORY_ID)) {
      setPlaylistCategoryFilterMode("none");
      setSelectedPlaylistListIds([]);
    } else {
      setPlaylistCategoryFilterMode("selected");
      setSelectedPlaylistListIds(playlistOverlayOptions.categoryIds);
    }
    setPlaylistSort(playlistOverlayOptions.sort);
    setPlaylistGrouping(playlistOverlayOptions.grouping);
    setSelectedTrackCountBuckets([]);
    if (playlistOverlayOptions.editMode) {
      beginPlaylistEditMode();
    } else {
      setPlaylistEditMode(false);
    }
    onConsumePlaylistOverlayOptions();
  }, [onConsumePlaylistOverlayOptions, playlistOverlayOpen, playlistOverlayOptions]);

  useEffect(() => {
    if (selectedPlaylistTabs.includes("created")) {
      return;
    }
    setMineVisibility((current) => current.length > 0 ? current : ["public", "private"]);
  }, [selectedPlaylistTabs]);

  useEffect(() => {
    if (selectedPlaylistTabs.includes("collaborations")) {
      return;
    }
    setCollaborationOwnerMenuOpen(false);
    setCollaborationVisibility((current) => current.length > 0 ? current : ["public", "private"]);
    setCollaborationOwners((current) => current.length > 0 ? current : ["yours", "others"]);
  }, [selectedPlaylistTabs]);

  const hiddenPlaylistIds = new Set(
    playlistEditMode
      ? draftHiddenPlaylistIds
      : ownedPlaylists.map((playlist, index) => playlist.hidden_by_user ? playlistStableId(playlist, index) : "").filter(Boolean),
  );
  const ownedPlaylistsForDisplay = playlistEditMode
    ? ownedPlaylists.map((playlist, index) => ({
      ...playlist,
      hidden_by_user: hiddenPlaylistIds.has(playlistStableId(playlist, index)),
    }))
    : ownedPlaylists;
  const visibleOwnedPlaylists = ownedPlaylistsForDisplay.filter((playlist) => !playlist.hidden_by_user);
  const hiddenOwnedPlaylistsForDisplay = ownedPlaylistsForDisplay.filter((playlist) => playlist.hidden_by_user);
  const canRenderPlaylistRows = ownedPlaylistsAvailable || ownedPlaylists.length > 0;
  const playlistSelectionSource = showHiddenPlaylists
    ? ownedPlaylistsForDisplay
    : visibleOwnedPlaylists;
  const playlistMatchesCategoryFilter = (playlist: OwnedPlaylist, index: number) => {
    if (playlist.hidden_by_user) {
      return showHiddenPlaylists;
    }
    if (playlistCategoryFilterMode === "all") {
      return true;
    }
    const playlistId = playlistStableId(playlist, index);
    const categoryIds = playlistLists
      .filter((list) => list.playlistIds.includes(playlistId))
      .map((list) => list.id);
    if (playlistCategoryFilterMode === "none") {
      return categoryIds.length === 0;
    }
    if (selectedPlaylistListIds.length === 0) {
      return false;
    }
    return categoryIds.some((listId) => selectedPlaylistListIds.includes(listId));
  };
  const listFilteredOwnedPlaylists = playlistSelectionSource.filter(playlistMatchesCategoryFilter);
  const hiddenPlaylistsMatchingFilters = playlistsForSelection(
    ownedPlaylistsForDisplay.filter((playlist, index) => playlist.hidden_by_user && playlistMatchesCategoryFilter(playlist, index)),
    selectedPlaylistTabs,
    mineVisibility,
    collaborationOwners,
    collaborationVisibility,
  );
  const hiddenPlaylistsMatchingFilterCount = hiddenPlaylistsMatchingFilters.length;
  const editorFilteredPlaylists = playlistsForSelection(listFilteredOwnedPlaylists, selectedPlaylistTabs, mineVisibility, collaborationOwners, collaborationVisibility);
  const trackCountFilteredPlaylists = selectedTrackCountBuckets.length === 0
    ? editorFilteredPlaylists
    : editorFilteredPlaylists.filter((playlist) => selectedTrackCountBuckets.includes(playlistTrackCountBucket(playlist)));
  const selectedPlaylists = trackCountFilteredPlaylists
    .slice()
    .sort((a, b) => (
      Number(pinnedPlaylistIds.includes(playlistStableId(b))) - Number(pinnedPlaylistIds.includes(playlistStableId(a)))
      || comparePlaylists(a, b, playlistSort)
    ));
  useEffect(() => {
    const categoryChanged = previousPlaylistCategorySignatureRef.current !== playlistCategorySignature;
    previousPlaylistCategorySignatureRef.current = playlistCategorySignature;
    if (!categoryChanged || selectedPlaylistTabs.includes("all") || listFilteredOwnedPlaylists.length === 0 || editorFilteredPlaylists.length > 0) {
      return;
    }
    setSelectedPlaylistTabs(["all"]);
  }, [editorFilteredPlaylists.length, listFilteredOwnedPlaylists.length, playlistCategorySignature, selectedPlaylistTabs]);
  const selectedSpecificTabs = selectedPlaylistTabs.filter((tab) => tab !== "all");
  const onlyYoursSelected = !selectedPlaylistTabs.includes("all") && selectedSpecificTabs.length === 1 && selectedSpecificTabs[0] === "created";
  const singleCategorySelected = playlistCategoryFilterMode === "none"
    || (playlistCategoryFilterMode === "selected" && selectedPlaylistListIds.length === 1);
  const selectedPlaylistGroups = Array.from(selectedPlaylists.reduce<Map<string, OwnedPlaylist[]>>((groups, playlist, index) => {
    const playlistId = playlistStableId(playlist, index);
    const label = playlistGrouping === "editor"
      ? playlistEditorDisplayLabel(playlist)
      : playlistGrouping === "category"
        ? playlistLists.find((list) => list.playlistIds.includes(playlistId))?.name ?? "Uncategorized"
        : playlistGrouping === "track_count"
          ? playlistTrackCountGroupLabel(playlist)
          : "";
    groups.set(label, [...(groups.get(label) ?? []), playlist]);
    return groups;
  }, new Map()).entries()).map(([label, playlists]) => ({
    label,
    playlists: playlistGrouping === "track_count"
      ? playlists.slice().sort((left, right) => playlistTrackCount(left) - playlistTrackCount(right) || playlistTitle(left).localeCompare(playlistTitle(right), undefined, { sensitivity: "base" }))
      : playlists,
    spotifyUrl: playlistGrouping === "editor" ? playlistEditorGroupUrl(playlists) : null,
  })).sort((left, right) => (
    playlistGrouping === "track_count"
      ? playlistTrackCountGroupRank(left.label) - playlistTrackCountGroupRank(right.label)
      : 0
  ));
  const allVisiblePlaylistsAreShown = selectedPlaylists.length === visibleOwnedPlaylists.length && !showHiddenPlaylists;
  const collapsedPreviewPlaylists = selectedPlaylists.length > 0 ? selectedPlaylists : visibleOwnedPlaylists;
  const exclusiveMinePlaylists = listFilteredOwnedPlaylists.filter(isExclusiveMinePlaylist);
  const minePublicCount = exclusiveMinePlaylists.filter(isMinePublicPlaylist).length;
  const minePrivateCount = exclusiveMinePlaylists.filter(isMinePrivatePlaylist).length;
  const collaborationYoursCount = listFilteredOwnedPlaylists.filter((playlist) => playlist.is_collaborative && playlist.is_owned && (
    (collaborationVisibility.includes("public") && playlist.is_public !== false)
    || (collaborationVisibility.includes("private") && playlist.is_public === false)
  )).length;
  const collaborationOthersCount = listFilteredOwnedPlaylists.filter((playlist) => playlist.is_collaborative && !playlist.is_owned && (
    (collaborationVisibility.includes("public") && playlist.is_public !== false)
    || (collaborationVisibility.includes("private") && playlist.is_public === false)
  )).length;
  const visiblePlaylistTabs = playlistTabOrder
    .map((key) => PLAYLIST_TABS.find((tab) => tab.key === key))
    .filter((tab): tab is { key: PlaylistTabKey; label: string } => Boolean(tab));

  const playlistTabCounts = visiblePlaylistTabs.reduce<Record<PlaylistTabKey, number>>((counts, tab) => {
    counts[tab.key] = playlistsForTab(listFilteredOwnedPlaylists, tab.key, ["public", "private"], ["yours", "others"], ["public", "private"]).length;
    return counts;
  }, {
    all: 0,
    created: 0,
    collaborations: 0,
    others: 0,
  });
  const trackCountBucketCounts = PLAYLIST_TRACK_COUNT_BUCKETS.reduce<Record<PlaylistTrackCountBucket, number>>((counts, bucket) => {
    counts[bucket.value] = editorFilteredPlaylists.filter((playlist) => playlistTrackCountBucket(playlist) === bucket.value).length;
    return counts;
  }, {
    zero: 0,
    small: 0,
    medium: 0,
    large: 0,
    xlarge: 0,
    huge: 0,
  });
  const mineVisibilityOptions: Array<{ key: MinePlaylistVisibility; label: string; count: number }> = [
    { key: "public", label: "Public", count: minePublicCount },
    { key: "private", label: "Private", count: minePrivateCount },
  ];
  const collaborationOwnerOptions: Array<{ key: CollaborationPlaylistOwner; label: string; count: number }> = [
    { key: "yours", label: "By you", count: collaborationYoursCount },
    { key: "others", label: "By others", count: collaborationOthersCount },
  ];
  const trackCountBucketLabel = selectedTrackCountBuckets.length === 0
    ? "All sizes"
    : selectedTrackCountBuckets.length === 1
      ? PLAYLIST_TRACK_COUNT_BUCKETS.find((bucket) => bucket.value === selectedTrackCountBuckets[0])?.label ?? "Track count"
      : `${selectedTrackCountBuckets.length} sizes`;
  const setMineVisibilityFromOwnerMenu = (nextVisibility: MinePlaylistVisibility[]) => {
    setSelectedPlaylistTabs((current) => {
      if (current.includes("all") || current.includes("created")) {
        return current;
      }
      return [...current, "created"];
    });
    setMineVisibility(nextVisibility);
  };
  const setCollaborationVisibilityFromOwnerMenu = (nextVisibility: MinePlaylistVisibility[]) => {
    setSelectedPlaylistTabs((current) => {
      if (current.includes("all") || current.includes("collaborations")) {
        return current;
      }
      return [...current, "collaborations"];
    });
    setCollaborationVisibility(nextVisibility);
  };
  const closeCollaborationOwnerMenu = () => {
    setCollaborationOwnerMenuOpen(false);
    setCollaborationOwners((current) => current.length > 0 ? current : ["yours", "others"]);
  };
  const closePlaylistListMenu = () => {
    setPlaylistListMenuOpen(false);
    setPlaylistCategoryCreatorOpen(false);
  };
  const toggleTrackCountBucket = (bucket: PlaylistTrackCountBucket) => {
    setSelectedTrackCountBuckets((current) => {
      const allBuckets = PLAYLIST_TRACK_COUNT_BUCKETS.map((option) => option.value);
      const next = current.length === 0
        ? allBuckets.filter((item) => item !== bucket)
        : current.includes(bucket)
          ? current.filter((item) => item !== bucket)
          : [...current, bucket];
      return next.length === 0 || next.length === allBuckets.length ? [] : next;
    });
  };
  const selectOnlyTrackCountBucket = (bucket: PlaylistTrackCountBucket) => {
    setSelectedTrackCountBuckets([bucket]);
  };
  const selectPlaylistCategoryMode = (mode: PlaylistCategoryFilterMode) => {
    setShowHiddenPlaylists(false);
    setPlaylistCategoryFilterMode(mode);
    if (mode !== "selected") {
      setSelectedPlaylistListIds([]);
    }
  };
  const selectOnlyPlaylistListFilter = (listId: string) => {
    setShowHiddenPlaylists(false);
    setPlaylistCategoryFilterMode("selected");
    setSelectedPlaylistListIds([listId]);
  };
  const togglePlaylistListFilter = (listId: string) => {
    setShowHiddenPlaylists(false);
    const base = playlistCategoryFilterMode === "selected" ? selectedPlaylistListIds : [];
    const next = base.includes(listId)
      ? base.filter((id) => id !== listId)
      : [...base, listId];
    setPlaylistCategoryFilterMode(next.length > 0 ? "selected" : "all");
    setSelectedPlaylistListIds(next);
  };
  const reorderPlaylistCategory = (draggedListId: string, targetListId: string) => {
    if (draggedListId === targetListId) {
      return;
    }
    setPlaylistLists((current) => {
      const draggedIndex = current.findIndex((list) => list.id === draggedListId);
      const targetIndex = current.findIndex((list) => list.id === targetListId);
      if (draggedIndex < 0 || targetIndex < 0) {
        return current;
      }
      const next = [...current];
      const [dragged] = next.splice(draggedIndex, 1);
      next.splice(targetIndex, 0, dragged);
      return next;
    });
  };
  const reorderPlaylistTab = (draggedKey: PlaylistTabKey, targetKey: PlaylistTabKey) => {
    if (draggedKey === targetKey) {
      return;
    }
    setPlaylistTabOrder((current) => {
      const draggedIndex = current.indexOf(draggedKey);
      const targetIndex = current.indexOf(targetKey);
      if (draggedIndex < 0 || targetIndex < 0) {
        return current;
      }
      const next = [...current];
      next.splice(draggedIndex, 1);
      next.splice(targetIndex, 0, draggedKey);
      return next;
    });
  };
  const createPlaylistList = async () => {
    const name = newPlaylistListName.trim();
    if (!name) {
      return;
    }
    try {
      const response = await fetch(`${apiBaseUrl}/playlists/categories`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name }),
      });
      if (!response.ok) {
        throw new Error(`Category could not be created (${response.status}).`);
      }
      const payload = await response.json() as { category?: PlaylistList };
      const category = payload.category;
      if (!category?.id) {
        throw new Error("Category response was missing an id.");
      }
      setPlaylistLists((current) => (
        current.some((item) => item.id === category.id)
          ? current.map((item) => item.id === category.id ? { ...item, name: category.name } : item)
          : [...current, category]
      ));
      setPlaylistCategoryFilterMode("selected");
      setSelectedPlaylistListIds((current) => current.includes(category.id) ? current : [...current, category.id]);
      setNewPlaylistListName("");
      setPlaylistCategoryCreatorOpen(false);
    } catch (error) {
      console.error(error);
    }
  };
  const togglePlaylistInList = (playlist: OwnedPlaylist, listId: string) => {
    const playlistId = playlistStableId(playlist);
    let nextIncluded = false;
    setPlaylistLists((current) => current.map((list) => {
      if (list.id !== listId) {
        return list;
      }
      const hasPlaylist = list.playlistIds.includes(playlistId);
      nextIncluded = !hasPlaylist;
      return {
        ...list,
        playlistIds: hasPlaylist
          ? list.playlistIds.filter((id) => id !== playlistId)
          : [...list.playlistIds, playlistId],
      };
    }));
    if (playlistEditMode) {
      return;
    }
    void fetch(`${apiBaseUrl}/playlists/categories/${encodeURIComponent(listId)}/playlists/${encodeURIComponent(playlistId)}`, {
      method: "PUT",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ included: nextIncluded }),
    }).then((response) => {
      if (!response.ok) {
        throw new Error(`Category membership could not be saved (${response.status}).`);
      }
    }).catch((error) => {
      console.error(error);
      setPlaylistLists((current) => current.map((list) => {
        if (list.id !== listId) {
          return list;
        }
        const hasPlaylist = list.playlistIds.includes(playlistId);
        return {
          ...list,
          playlistIds: nextIncluded
            ? list.playlistIds.filter((id) => id !== playlistId)
            : hasPlaylist ? list.playlistIds : [...list.playlistIds, playlistId],
        };
      }));
    });
  };
  const togglePinnedPlaylist = (playlist: OwnedPlaylist) => {
    const playlistId = playlistStableId(playlist);
    setPinnedPlaylistIds((current) => current.includes(playlistId)
      ? current.filter((id) => id !== playlistId)
      : [...current, playlistId]);
  };
  const hidePlaylistInEditMode = (playlist: OwnedPlaylist) => {
    const playlistId = playlistStableId(playlist);
    setDraftHiddenPlaylistIds((current) => current.includes(playlistId) ? current : [...current, playlistId]);
  };
  const unhidePlaylistInEditMode = (playlist: OwnedPlaylist) => {
    const playlistId = playlistStableId(playlist);
    setDraftHiddenPlaylistIds((current) => current.filter((id) => id !== playlistId));
  };
  const toggleCollaborationOwner = (key: CollaborationPlaylistOwner) => {
    setCollaborationOwners((current) => {
      if (!current.includes(key)) {
        return [...current, key];
      }
      if (current.length === 1) {
        return collaborationOwnerOptions
          .map((option) => option.key)
          .filter((optionKey) => optionKey !== key);
      }
      return current.filter((item) => item !== key);
    });
  };
  const playlistTypeLabel = selectedPlaylistTabs.includes("all")
    ? "All Editors"
    : selectedSpecificTabs.includes("created")
      ? selectedSpecificTabs.length === 1 ? "Yours" : "Yours + Others"
      : selectedSpecificTabs.length === 1
        ? PLAYLIST_TABS.find((tab) => tab.key === selectedSpecificTabs[0])?.label ?? "Others"
        : selectedSpecificTabs.length > 1
          ? "Others"
          : "None";
  const closePlaylistTypeMenu = () => {
    setPlaylistTypeMenuOpen(false);
    setSelectedPlaylistTabs((current) => current.length > 0 ? current : ["all"]);
  };
  const mineVisibilityCount = mineVisibilityOptions
    .filter((option) => mineVisibility.includes(option.key))
    .reduce((total, option) => total + option.count, 0);
  const collaborationVisibilityCount = collaborationOwnerOptions
    .filter((option) => collaborationOwners.includes(option.key))
    .reduce((total, option) => total + option.count, 0);
  const collaborationOwnerLabel = collaborationOwners.length === 2
    ? "By you + By others"
    : collaborationOwners.length === 1
      ? collaborationOwnerOptions.find((option) => option.key === collaborationOwners[0])?.label ?? "Collabs"
      : "None";
  const collaborationOwnerCount = collaborationOwnerOptions
    .filter((option) => collaborationOwners.includes(option.key))
    .reduce((total, option) => total + option.count, 0);
  const noneCategoryCount = playlistSelectionSource.filter((playlist, index) => {
    const playlistId = playlistStableId(playlist, index);
    return !playlistLists.some((list) => list.playlistIds.includes(playlistId));
  }).length;
  const hiddenPlaylistCount = hiddenOwnedPlaylistsForDisplay.length;
  const playlistSelectionSourceIds = new Set(playlistSelectionSource.map((playlist, index) => playlistStableId(playlist, index)));
  const playlistListCounts = playlistLists.reduce<Record<string, number>>((counts, list) => {
    counts[list.id] = list.playlistIds.filter((playlistId) => playlistSelectionSourceIds.has(playlistId)).length;
    return counts;
  }, {});
  const activePlaylistListIds = playlistCategoryFilterMode === "selected" ? selectedPlaylistListIds : [];
  const playlistListLabel = playlistCategoryFilterMode === "all"
    ? "All categories"
    : playlistCategoryFilterMode === "none"
      ? "None"
      : selectedPlaylistListIds.length === 1
        ? playlistLists.find((list) => list.id === selectedPlaylistListIds[0])?.name ?? "Categories"
        : `${selectedPlaylistListIds.length} categories`;
  const togglePlaylistTab = (key: PlaylistTabKey) => {
    setSelectedPlaylistTabs((current) => {
      if (key === "all") {
        return current.includes("all") ? ["created"] : ["all"];
      }
      const visibleSpecificTabKeys = visiblePlaylistTabs
        .map((tab) => tab.key)
        .filter((tabKey) => tabKey !== "all");
      if (current.includes("all")) {
        return visibleSpecificTabKeys.filter((tabKey) => tabKey !== key);
      }
      const currentSpecificTabs = current.filter((tab) => tab !== "all");
      if (currentSpecificTabs.includes(key)) {
        return currentSpecificTabs.length === 1
          ? visibleSpecificTabKeys.filter((tabKey) => tabKey !== key)
          : currentSpecificTabs.filter((tab) => tab !== key);
      }
      const next = [...currentSpecificTabs, key];
      return next.length === visibleSpecificTabKeys.length ? ["all"] : next;
    });
  };
  const selectOnlyPlaylistTab = (key: PlaylistTabKey) => {
    setSelectedPlaylistTabs(key === "all" ? ["all"] : [key]);
  };
  const showAllPlaylists = () => {
    setPlaylistCategoryFilterMode("all");
    setSelectedPlaylistListIds([]);
    setSelectedPlaylistTabs(["all"]);
    setMineVisibility(["public", "private"]);
    setCollaborationVisibility(["public", "private"]);
    setCollaborationOwners(["yours", "others"]);
    setSelectedTrackCountBuckets([]);
    setShowHiddenPlaylists(false);
  };
  const beginPlaylistEditMode = () => {
    setPlaylistEditCloseAction(null);
    setPlaylistEditSnapshot({
      playlistLists: clonePlaylistLists(playlistLists),
      pinnedPlaylistIds: [...pinnedPlaylistIds],
      hiddenPlaylistIds: ownedPlaylists
        .map((playlist, index) => playlist.hidden_by_user ? playlistStableId(playlist, index) : "")
        .filter(Boolean),
    });
    setDraftHiddenPlaylistIds(ownedPlaylists
      .map((playlist, index) => playlist.hidden_by_user ? playlistStableId(playlist, index) : "")
      .filter(Boolean));
    setPlaylistEditMode(true);
  };
  const cancelPlaylistEditMode = () => {
    if (playlistEditSnapshot) {
      setPlaylistLists(clonePlaylistLists(playlistEditSnapshot.playlistLists));
      setPinnedPlaylistIds([...playlistEditSnapshot.pinnedPlaylistIds]);
      setDraftHiddenPlaylistIds([...playlistEditSnapshot.hiddenPlaylistIds]);
    }
    setPlaylistEditMode(false);
    setPlaylistEditCloseAction("cancel");
    setShowHiddenPlaylists(false);
    setPlaylistEditSnapshot(null);
    setPlaylistTypeMenuOpen(false);
    setCollaborationOwnerMenuOpen(false);
    setPlaylistTrackCountMenuOpen(false);
    setPlaylistSortMenuOpen(false);
    setPlaylistGroupMenuOpen(false);
    setPlaylistListMenuOpen(false);
    setPlaylistCategoryCreatorOpen(false);
  };
  const persistPlaylistListMembership = async (listId: string, playlistId: string, included: boolean) => {
    const response = await fetch(`${apiBaseUrl}/playlists/categories/${encodeURIComponent(listId)}/playlists/${encodeURIComponent(playlistId)}`, {
      method: "PUT",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ included }),
    });
    if (!response.ok) {
      throw new Error(`Category membership could not be saved (${response.status}).`);
    }
  };
  const savePlaylistEditMode = () => {
    const snapshot = playlistEditSnapshot;
    if (snapshot) {
      const beforeListIds = new Set(snapshot.playlistLists.map((list) => list.id));
      const afterListIds = new Set(playlistLists.map((list) => list.id));
      const allListIds = Array.from(new Set([...beforeListIds, ...afterListIds]));
      allListIds.forEach((listId) => {
        const beforeList = snapshot.playlistLists.find((list) => list.id === listId);
        const afterList = playlistLists.find((list) => list.id === listId);
        const beforePlaylistIds = new Set(beforeList?.playlistIds ?? []);
        const afterPlaylistIds = new Set(afterList?.playlistIds ?? []);
        const allPlaylistIds = Array.from(new Set([...beforePlaylistIds, ...afterPlaylistIds]));
        allPlaylistIds.forEach((playlistId) => {
          const beforeIncluded = beforePlaylistIds.has(playlistId);
          const afterIncluded = afterPlaylistIds.has(playlistId);
          if (beforeIncluded !== afterIncluded) {
            void persistPlaylistListMembership(listId, playlistId, afterIncluded).catch(console.error);
          }
        });
      });
      const beforeHiddenIds = new Set(snapshot.hiddenPlaylistIds);
      const afterHiddenIds = new Set(draftHiddenPlaylistIds);
      const playlistsById = new Map(ownedPlaylists.map((playlist, index) => [playlistStableId(playlist, index), playlist]));
      Array.from(new Set([...beforeHiddenIds, ...afterHiddenIds])).forEach((playlistId) => {
        const playlist = playlistsById.get(playlistId);
        if (!playlist) {
          return;
        }
        const wasHidden = beforeHiddenIds.has(playlistId);
        const isHidden = afterHiddenIds.has(playlistId);
        if (!wasHidden && isHidden) {
          onHidePlaylist(playlist);
        } else if (wasHidden && !isHidden) {
          onUnhidePlaylist(playlist);
        }
      });
    }
    setPlaylistEditCloseAction("save");
    setPlaylistEditMode(false);
    setShowHiddenPlaylists(false);
    setPlaylistEditSnapshot(null);
    setPlaylistTypeMenuOpen(false);
    setCollaborationOwnerMenuOpen(false);
    setPlaylistTrackCountMenuOpen(false);
    setPlaylistSortMenuOpen(false);
    setPlaylistGroupMenuOpen(false);
    setPlaylistListMenuOpen(false);
    setPlaylistCategoryCreatorOpen(false);
  };
  const renderVisibilityToggle = (
    label: string,
    visibility: MinePlaylistVisibility[],
    onChange: (nextVisibility: MinePlaylistVisibility[]) => void,
  ) => {
    const bothSelected = visibility.includes("public") && visibility.includes("private");
    const options: Array<{ key: string; label: string; title: string; value: MinePlaylistVisibility[] }> = [
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
                onChange(option.value);
              }}
              onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  event.stopPropagation();
                  onChange(option.value);
                }
              }}
              role="button"
              tabIndex={0}
              title={option.title}
            >
              {option.label}
            </span>
          );
        })}
      </span>
    );
  };
  const renderPlaylistCategoryNavigation = () => (
    <aside className="playlist-overlay-sidebar" aria-label="Playlist categories">
      <div className="playlist-overlay-sidebar-section">
        <span className="playlist-overlay-sidebar-label">Categories</span>
        <button
          aria-checked={playlistCategoryFilterMode === "all" && !showHiddenPlaylists}
          className={`playlist-overlay-category-item${playlistCategoryFilterMode === "all" && !showHiddenPlaylists ? " playlist-overlay-category-item-active" : ""}`}
          onClick={() => selectPlaylistCategoryMode("all")}
          role="menuitemradio"
          type="button"
        >
          <span className="playlist-overlay-category-meta-spacer" aria-hidden="true" />
          <span>All categories</span>
          <span className="playlist-tab-count">{playlistSelectionSource.length}</span>
        </button>
        {playlistLists.length > 0 ? playlistLists.map((list) => {
          const checked = (playlistCategoryFilterMode === "all" && !showHiddenPlaylists) || (playlistCategoryFilterMode === "selected" && activePlaylistListIds.includes(list.id));
          const active = playlistCategoryFilterMode === "selected" && activePlaylistListIds.includes(list.id);
          return (
            <button
              aria-checked={checked}
              className={`playlist-overlay-category-item${checked ? " playlist-overlay-category-item-included" : ""}${active ? " playlist-overlay-category-item-active" : ""}${playlistEditMode ? " playlist-overlay-category-item-draggable" : ""}`}
              draggable={playlistEditMode}
              key={list.id}
              onClick={() => selectOnlyPlaylistListFilter(list.id)}
              onDragOver={(event) => {
                if (playlistEditMode) {
                  event.preventDefault();
                }
              }}
              onDragStart={(event) => {
                if (!playlistEditMode) {
                  return;
                }
                event.dataTransfer.setData("text/plain", `playlist-category:${list.id}`);
                event.dataTransfer.effectAllowed = "move";
              }}
              onDrop={(event) => {
                if (!playlistEditMode) {
                  return;
                }
                const dragged = event.dataTransfer.getData("text/plain");
                if (dragged.startsWith("playlist-category:")) {
                  event.preventDefault();
                  reorderPlaylistCategory(dragged.replace("playlist-category:", ""), list.id);
                }
              }}
              role="menuitemcheckbox"
              type="button"
            >
              <span
                className="playlist-overlay-category-check"
                onClick={(event) => {
                  event.preventDefault();
                  event.stopPropagation();
                  togglePlaylistListFilter(list.id);
                }}
                role="button"
                tabIndex={-1}
              >
                {checked ? "✓" : ""}
              </span>
              <span className="playlist-overlay-category-label">{list.name}</span>
              <span className="playlist-tab-count">{playlistListCounts[list.id] ?? 0}</span>
            </button>
          );
        }) : (
          <span className="playlist-list-empty">No categories yet</span>
        )}
        <div className="playlist-overlay-category-system-divider" />
        <button
          aria-checked={(playlistCategoryFilterMode === "all" && !showHiddenPlaylists) || playlistCategoryFilterMode === "none"}
          className={`playlist-overlay-category-item playlist-overlay-category-item-system${playlistCategoryFilterMode === "all" && !showHiddenPlaylists ? " playlist-overlay-category-item-included" : ""}${playlistCategoryFilterMode === "none" ? " playlist-overlay-category-item-active" : ""}`}
          onClick={() => selectPlaylistCategoryMode("none")}
          role="menuitemradio"
          type="button"
        >
          <span className="playlist-overlay-category-check" aria-hidden="true">
            {(playlistCategoryFilterMode === "all" && !showHiddenPlaylists) || playlistCategoryFilterMode === "none" ? "✓" : ""}
          </span>
          <span className="playlist-overlay-category-label">Uncategorized</span>
          <span className="playlist-tab-count">{noneCategoryCount}</span>
        </button>
        <div className="playlist-overlay-category-system-divider" />
        <button
          aria-checked={showHiddenPlaylists}
          className={`playlist-overlay-category-item playlist-overlay-category-item-system${showHiddenPlaylists ? " playlist-overlay-category-item-included" : ""}${showHiddenPlaylists && playlistCategoryFilterMode === "selected" && selectedPlaylistListIds.length === 0 ? " playlist-overlay-category-item-active" : ""}`}
          onClick={() => {
            setPlaylistCategoryFilterMode("selected");
            setSelectedPlaylistListIds([]);
            setShowHiddenPlaylists(true);
          }}
          role="menuitemcheckbox"
          type="button"
        >
          <span
            className="playlist-overlay-category-check"
            onClick={(event) => {
              event.preventDefault();
              event.stopPropagation();
              setShowHiddenPlaylists((current) => {
                const next = !current;
                if (next && playlistCategoryFilterMode === "all") {
                  setPlaylistCategoryFilterMode("selected");
                  setSelectedPlaylistListIds([]);
                }
                if (!next && playlistCategoryFilterMode === "selected" && selectedPlaylistListIds.length === 0) {
                  setPlaylistCategoryFilterMode("all");
                }
                return next;
              });
            }}
            role="button"
            tabIndex={-1}
          >
            {showHiddenPlaylists ? "✓" : ""}
          </span>
          <span className="playlist-overlay-category-label">Hidden</span>
          <span className="playlist-tab-count">{hiddenPlaylistCount}</span>
        </button>
      </div>
      <div className="playlist-overlay-sidebar-section playlist-overlay-sidebar-editor">
        {playlistEditMode ? (
          <>
          {playlistCategoryCreatorOpen ? (
            <div className="playlist-list-editor playlist-list-editor-sidebar">
              <input
                autoFocus
                onChange={(event) => setNewPlaylistListName(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter") {
                    event.preventDefault();
                    void createPlaylistList();
                  }
                }}
                placeholder="New category name"
                type="text"
                value={newPlaylistListName}
              />
              <button onClick={() => void createPlaylistList()} type="button">Create</button>
            </div>
          ) : (
            <button
              className="playlist-overlay-category-item playlist-overlay-category-create"
              onClick={() => setPlaylistCategoryCreatorOpen(true)}
              type="button"
            >
              <span className="playlist-overlay-category-check" aria-hidden="true">+</span>
              <span>New category</span>
              <span />
            </button>
          )}
          </>
        ) : null}
        <div className="playlist-overlay-edit-actions">
          {playlistEditMode ? (
            <>
              <button className="playlist-edit-toggle playlist-edit-toggle-active" onClick={savePlaylistEditMode} type="button">
                Save
              </button>
              <button className="playlist-edit-toggle" onClick={cancelPlaylistEditMode} type="button">
                Close
              </button>
            </>
          ) : (
            <button className="playlist-edit-toggle" onClick={beginPlaylistEditMode} type="button">
              Edit
            </button>
          )}
        </div>
      </div>
    </aside>
  );

  return (
    <>
    {playlistOverlayOpen ? (
      <div
        className="playlist-overlay-backdrop"
        onMouseDown={() => setPlaylistOverlayOpen(false)}
        role="presentation"
      />
    ) : null}
    <section className={`info-card info-card-wide${playlistOverlayOpen ? " playlist-overlay-panel" : ""}`} id="playlists">
      <div className="playlist-section-header">
        <button
          className="section-toggle section-toggle-header"
          onClick={() => {
            if (!playlistOverlayOpen) {
              toggleSection("playlists", "playlists");
            }
          }}
          type="button"
        >
          <h2>{renderSectionTitle("Playlists", "playlists")}</h2>
        </button>
        {!playlistOverlayOpen ? (
          <button
            aria-label="Open playlists overlay"
            className="playlist-overlay-toggle"
            onClick={() => setPlaylistOverlayOpen(true)}
            title="Open playlists"
            type="button"
          >
            <svg aria-hidden="true" viewBox="0 0 24 24">
              <path d="M14 4h6v6h-2V7.4l-5.3 5.3-1.4-1.4L16.6 6H14V4ZM4 4h7v2H6v12h12v-5h2v7H4V4Z" />
            </svg>
          </button>
        ) : null}
      </div>
      {playlistsFullOpen ? (
        canRenderPlaylistRows ? (
          ownedPlaylists.length > 0 ? (
            <>
              <div className={playlistOverlayOpen ? "playlist-overlay-content" : "playlist-inline-content"}>
                {playlistOverlayOpen ? renderPlaylistCategoryNavigation() : null}
                <div className={playlistOverlayOpen ? "playlist-overlay-main" : "playlist-inline-main"}>
              <div className="playlist-filter-controls" ref={playlistFiltersRef}>
                {!playlistOverlayOpen ? (
                <div className="playlist-type-dropdown">
                  <button
                    aria-expanded={playlistListMenuOpen}
                    className="playlist-type-dropdown-button"
                    onClick={() => {
                      if (playlistListMenuOpen) {
                        closePlaylistListMenu();
                      } else {
                        closePlaylistTypeMenu();
                        closeCollaborationOwnerMenu();
                        setPlaylistTrackCountMenuOpen(false);
                        setPlaylistSortMenuOpen(false);
                        setPlaylistGroupMenuOpen(false);
                        setPlaylistListMenuOpen(true);
                      }
                    }}
                    type="button"
                  >
                    {playlistCategoryFilterMode !== "all" ? <span className="playlist-dropdown-active-dot" aria-hidden="true" /> : null}
                    <span>{playlistListLabel}</span>
                    <span className="playlist-type-dropdown-chevron" aria-hidden="true">v</span>
                  </button>
                  {playlistListMenuOpen ? (
                    <div className="playlist-type-dropdown-menu playlist-list-dropdown-menu" role="menu">
                      <button
                        aria-checked={playlistCategoryFilterMode === "all"}
                        className={`playlist-type-dropdown-item${playlistCategoryFilterMode === "all" ? " playlist-type-dropdown-item-active" : ""}`}
                        onClick={() => selectPlaylistCategoryMode("all")}
                        role="menuitemcheckbox"
                        type="button"
                      >
                        <span className="playlist-type-dropdown-check" aria-hidden="true">
                          {playlistCategoryFilterMode === "all" ? "✓" : ""}
                        </span>
                        <span>All</span>
                        <span className="playlist-tab-count">{playlistSelectionSource.length}</span>
                      </button>
                      <div className="playlist-type-dropdown-divider" />
                      {playlistLists.length > 0 ? playlistLists.map((list) => (
                        <button
                          aria-checked={activePlaylistListIds.includes(list.id)}
                          className={`playlist-type-dropdown-item${activePlaylistListIds.includes(list.id) ? " playlist-type-dropdown-item-active" : ""}`}
                          draggable={playlistEditMode}
                          onDragOver={(event) => {
                            if (playlistEditMode) {
                              event.preventDefault();
                            }
                          }}
                          onDragStart={(event) => {
                            if (!playlistEditMode) {
                              return;
                            }
                            event.dataTransfer.setData("text/plain", `playlist-category:${list.id}`);
                            event.dataTransfer.effectAllowed = "move";
                          }}
                          onDrop={(event) => {
                            if (!playlistEditMode) {
                              return;
                            }
                            const dragged = event.dataTransfer.getData("text/plain");
                            if (dragged.startsWith("playlist-category:")) {
                              event.preventDefault();
                              reorderPlaylistCategory(dragged.replace("playlist-category:", ""), list.id);
                            }
                          }}
                          key={list.id}
                          onClick={() => togglePlaylistListFilter(list.id)}
                          role="menuitemcheckbox"
                          type="button"
                        >
                          <span className="playlist-type-dropdown-check" aria-hidden="true">
                            {activePlaylistListIds.includes(list.id) ? "✓" : ""}
                          </span>
                          <span>{list.name}</span>
                          <span className="playlist-tab-count">{list.playlistIds.length}</span>
                        </button>
                      )) : (
                        <span className="playlist-list-empty">No categories yet</span>
                      )}
                      <div className="playlist-type-dropdown-divider" />
                      <button
                        aria-checked={playlistCategoryFilterMode === "all" || playlistCategoryFilterMode === "none"}
                        className={`playlist-type-dropdown-item${playlistCategoryFilterMode === "all" || playlistCategoryFilterMode === "none" ? " playlist-type-dropdown-item-active" : ""}`}
                        onClick={() => selectPlaylistCategoryMode("none")}
                        role="menuitemcheckbox"
                        type="button"
                      >
                        <span className="playlist-type-dropdown-check" aria-hidden="true">
                          {playlistCategoryFilterMode === "all" || playlistCategoryFilterMode === "none" ? "✓" : ""}
                        </span>
                        <span>None</span>
                        <span className="playlist-tab-count">{noneCategoryCount}</span>
                      </button>
                      {playlistEditMode ? (
                        playlistCategoryCreatorOpen ? (
                          <div className="playlist-list-editor playlist-list-editor-dropdown playlist-category-create-divider">
                            <input
                              autoFocus
                              onChange={(event) => setNewPlaylistListName(event.target.value)}
                              onKeyDown={(event) => {
                                if (event.key === "Enter") {
                                  event.preventDefault();
                                  void createPlaylistList();
                                }
                              }}
                              placeholder="New category name"
                              type="text"
                              value={newPlaylistListName}
                            />
                            <button onClick={() => void createPlaylistList()} type="button">Create</button>
                          </div>
                        ) : (
                          <button
                            className="playlist-type-dropdown-item playlist-category-create-divider"
                            onClick={() => setPlaylistCategoryCreatorOpen(true)}
                            role="menuitem"
                            type="button"
                          >
                            <span className="playlist-type-dropdown-check" aria-hidden="true">+</span>
                            <span>New category</span>
                            <span />
                          </button>
                        )
                      ) : null}
                    </div>
                  ) : null}
                </div>
                ) : null}
                <div className="playlist-type-dropdown">
                  <button
                    aria-expanded={playlistTypeMenuOpen}
                    className="playlist-type-dropdown-button"
                    onClick={() => {
                      if (playlistTypeMenuOpen) {
                        closePlaylistTypeMenu();
                      } else {
                        setPlaylistTypeMenuOpen(true);
                        closePlaylistListMenu();
                        closeCollaborationOwnerMenu();
                        setPlaylistTrackCountMenuOpen(false);
                        setPlaylistSortMenuOpen(false);
                        setPlaylistGroupMenuOpen(false);
                      }
                    }}
                    type="button"
                  >
                    {!selectedPlaylistTabs.includes("all") ? <span className="playlist-dropdown-active-dot" aria-hidden="true" /> : null}
                    <span>{playlistTypeLabel}</span>
                    <span className="playlist-type-dropdown-chevron" aria-hidden="true">v</span>
                  </button>
                  {playlistTypeMenuOpen ? (
                    <div className="playlist-type-dropdown-menu" role="menu">
                      {visiblePlaylistTabs.map((tab) => {
                        const tabChecked = selectedPlaylistTabs.includes("all") || selectedPlaylistTabs.includes(tab.key);
                        const tabCount = tab.key === "created"
                          ? mineVisibilityCount
                          : tab.key === "collaborations"
                            ? collaborationVisibilityCount
                            : playlistTabCounts[tab.key];
                        const tabDisabled = tab.key !== "all" && !tabChecked && tabCount === 0;
                        const tabHasVisibilityToggle = tabChecked && (tab.key === "created" || tab.key === "collaborations");
                        return (
                        <div
                          className={`playlist-type-dropdown-group${tab.key === "all" ? " playlist-type-dropdown-group-divider" : ""}${playlistEditMode ? " playlist-type-dropdown-group-draggable" : ""}`}
                          draggable={playlistEditMode}
                          key={tab.key}
                          onDragOver={(event) => {
                            if (playlistEditMode) {
                              event.preventDefault();
                            }
                          }}
                          onDragStart={(event) => {
                            if (!playlistEditMode) {
                              return;
                            }
                            event.dataTransfer.setData("text/plain", `playlist-tab:${tab.key}`);
                            event.dataTransfer.effectAllowed = "move";
                          }}
                          onDrop={(event) => {
                            if (!playlistEditMode) {
                              return;
                            }
                            const dragged = event.dataTransfer.getData("text/plain");
                            if (dragged.startsWith("playlist-tab:")) {
                              event.preventDefault();
                              reorderPlaylistTab(dragged.replace("playlist-tab:", "") as PlaylistTabKey, tab.key);
                            }
                          }}
                        >
                          <button
                            className={`playlist-type-dropdown-item${tabChecked ? " playlist-type-dropdown-item-active" : ""}${tabHasVisibilityToggle ? " playlist-type-dropdown-item-with-toggle" : ""}`}
                            disabled={tabDisabled}
                            onClick={() => selectOnlyPlaylistTab(tab.key)}
                            role="menuitemcheckbox"
                            aria-checked={tabChecked}
                            type="button"
                          >
                            <span
                              className="playlist-type-dropdown-check"
                              onClick={(event) => {
                                event.preventDefault();
                                event.stopPropagation();
                                if (tabDisabled) {
                                  return;
                                }
                                togglePlaylistTab(tab.key);
                              }}
                              role="button"
                              tabIndex={-1}
                            >
                              {tabChecked ? "✓" : ""}
                            </span>
                            <span>{tab.label}</span>
                            {tab.key === "created" && tabChecked ? (
                              renderVisibilityToggle("Yours playlist visibility", mineVisibility, setMineVisibilityFromOwnerMenu)
                            ) : null}
                            {tab.key === "collaborations" && tabChecked ? (
                              renderVisibilityToggle("Collabs playlist visibility", collaborationVisibility, setCollaborationVisibilityFromOwnerMenu)
                            ) : null}
                            <span className="playlist-tab-count">
                              {tabCount}
                            </span>
                          </button>
                        </div>
                      );
                      })}
                    </div>
                  ) : null}
                </div>
                {selectedPlaylistTabs.includes("collaborations") && collaborationOthersCount > 0 ? (
                  <div className="playlist-type-dropdown playlist-subfilter-dropdown">
                    <button
                      aria-expanded={collaborationOwnerMenuOpen}
                      className="playlist-type-dropdown-button playlist-subfilter-dropdown-button"
                      onClick={() => {
                        if (collaborationOwnerMenuOpen) {
                          closeCollaborationOwnerMenu();
                      } else {
                        closePlaylistTypeMenu();
                        closePlaylistListMenu();
                        setPlaylistTrackCountMenuOpen(false);
                        setPlaylistSortMenuOpen(false);
                        setPlaylistGroupMenuOpen(false);
                        setCollaborationOwnerMenuOpen(true);
                      }
                      }}
                      type="button"
                    >
                      {collaborationOwners.length !== 2 ? <span className="playlist-dropdown-active-dot" aria-hidden="true" /> : null}
                      <span>{collaborationOwnerLabel}</span>
                      <span className="playlist-type-dropdown-chevron" aria-hidden="true">v</span>
                    </button>
                    {collaborationOwnerMenuOpen ? (
                      <div className="playlist-type-dropdown-menu playlist-subfilter-dropdown-menu" role="menu">
                        {collaborationOwnerOptions.map((option) => (
                          <button
                            aria-checked={collaborationOwners.includes(option.key)}
                            className={`playlist-type-dropdown-item${collaborationOwners.includes(option.key) ? " playlist-type-dropdown-item-active" : ""}`}
                            disabled={!collaborationOwners.includes(option.key) && option.count === 0}
                            key={option.key}
                            onClick={() => toggleCollaborationOwner(option.key)}
                            role="menuitemcheckbox"
                            type="button"
                          >
                            <span className="playlist-type-dropdown-check" aria-hidden="true">
                              {collaborationOwners.includes(option.key) ? "✓" : ""}
                            </span>
                            <span>{option.label}</span>
                            <span className="playlist-tab-count">{option.count}</span>
                          </button>
                        ))}
                      </div>
                    ) : null}
                  </div>
                ) : null}
                {playlistOverlayOpen ? (
                  <div className="playlist-type-dropdown playlist-track-count-dropdown">
                    <button
                      aria-expanded={playlistTrackCountMenuOpen}
                      className="playlist-type-dropdown-button"
                      onClick={() => {
                        if (playlistTrackCountMenuOpen) {
                          setPlaylistTrackCountMenuOpen(false);
                        } else {
                          closePlaylistTypeMenu();
                          closePlaylistListMenu();
                          closeCollaborationOwnerMenu();
                          setPlaylistSortMenuOpen(false);
                          setPlaylistGroupMenuOpen(false);
                          setPlaylistTrackCountMenuOpen(true);
                        }
                      }}
                      type="button"
                    >
                      {selectedTrackCountBuckets.length > 0 ? <span className="playlist-dropdown-active-dot" aria-hidden="true" /> : null}
                      <span>{trackCountBucketLabel}</span>
                      <span className="playlist-type-dropdown-chevron" aria-hidden="true">v</span>
                    </button>
                    {playlistTrackCountMenuOpen ? (
                      <div className="playlist-type-dropdown-menu playlist-track-count-dropdown-menu" role="menu">
                        <button
                          aria-checked={selectedTrackCountBuckets.length === 0}
                          className={`playlist-type-dropdown-item${selectedTrackCountBuckets.length === 0 ? " playlist-type-dropdown-item-active" : ""}`}
                          onClick={() => setSelectedTrackCountBuckets([])}
                          role="menuitemcheckbox"
                          type="button"
                        >
                          <span className="playlist-type-dropdown-check" aria-hidden="true">{selectedTrackCountBuckets.length === 0 ? "✓" : ""}</span>
                          <span>All</span>
                          <span className="playlist-tab-count">{editorFilteredPlaylists.length}</span>
                        </button>
                        <div className="playlist-type-dropdown-divider" />
                        {PLAYLIST_TRACK_COUNT_BUCKETS.map((bucket) => {
                          const checked = selectedTrackCountBuckets.length === 0 || selectedTrackCountBuckets.includes(bucket.value);
                          const count = trackCountBucketCounts[bucket.value];
                          const disabled = !checked && count === 0;
                          return (
                            <button
                              aria-checked={checked}
                              className={`playlist-type-dropdown-item${checked ? " playlist-type-dropdown-item-active" : ""}`}
                              disabled={disabled}
                              key={bucket.value}
                              onClick={() => selectOnlyTrackCountBucket(bucket.value)}
                              role="menuitemcheckbox"
                              type="button"
                            >
                              <span
                                className="playlist-type-dropdown-check"
                                onClick={(event) => {
                                  event.preventDefault();
                                  event.stopPropagation();
                                  if (disabled) {
                                    return;
                                  }
                                  toggleTrackCountBucket(bucket.value);
                                }}
                                role="button"
                                tabIndex={-1}
                              >
                                {checked ? "✓" : ""}
                              </span>
                              <span>{bucket.label}</span>
                              <span className="playlist-tab-count">{count}</span>
                            </button>
                          );
                        })}
                      </div>
                    ) : null}
                  </div>
                ) : null}
                <div className="playlist-type-dropdown playlist-sort-dropdown">
                  <button
                    aria-expanded={playlistSortMenuOpen}
                    className="playlist-type-dropdown-button"
                    onClick={() => {
                      if (playlistSortMenuOpen) {
                        setPlaylistSortMenuOpen(false);
                      } else {
                        closePlaylistTypeMenu();
                        closePlaylistListMenu();
                        closeCollaborationOwnerMenu();
                        setPlaylistTrackCountMenuOpen(false);
                        setPlaylistGroupMenuOpen(false);
                        setPlaylistSortMenuOpen(true);
                      }
                    }}
                    type="button"
                  >
                    {playlistSort !== "name_asc" ? <span className="playlist-dropdown-active-dot" aria-hidden="true" /> : null}
                    <span>Sort</span>
                    <span className="playlist-type-dropdown-chevron" aria-hidden="true">v</span>
                  </button>
                  {playlistSortMenuOpen ? (
                    <div className="playlist-type-dropdown-menu playlist-sort-dropdown-menu" role="menu">
                      {PLAYLIST_SORT_OPTIONS.map((option) => {
                        const checked = playlistSort === option.value;
                        return (
                          <button
                            aria-checked={checked}
                            className={`playlist-type-dropdown-item${checked ? " playlist-type-dropdown-item-active" : ""}`}
                            key={option.value}
                            onClick={() => setPlaylistSort(option.value)}
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
                <div className="playlist-type-dropdown playlist-group-dropdown">
                  <button
                    aria-expanded={playlistGroupMenuOpen}
                    className="playlist-type-dropdown-button"
                    onClick={() => {
                      if (playlistGroupMenuOpen) {
                        setPlaylistGroupMenuOpen(false);
                      } else {
                        closePlaylistTypeMenu();
                        closePlaylistListMenu();
                        closeCollaborationOwnerMenu();
                        setPlaylistTrackCountMenuOpen(false);
                        setPlaylistSortMenuOpen(false);
                        setPlaylistGroupMenuOpen(true);
                      }
                    }}
                    type="button"
                  >
                    {playlistGrouping !== "none" ? <span className="playlist-dropdown-active-dot" aria-hidden="true" /> : null}
                    <span>Group</span>
                    <span className="playlist-type-dropdown-chevron" aria-hidden="true">v</span>
                  </button>
                  {playlistGroupMenuOpen ? (
                    <div className="playlist-type-dropdown-menu playlist-group-dropdown-menu" role="menu">
                      {PLAYLIST_GROUPING_OPTIONS.map((option) => {
                        const checked = playlistGrouping === option.value;
                        const disabled = !checked && (
                          (option.value === "editor" && onlyYoursSelected)
                          || (option.value === "category" && singleCategorySelected)
                        );
                        return (
                          <button
                            aria-checked={checked}
                            className={`playlist-type-dropdown-item${checked ? " playlist-type-dropdown-item-active" : ""}`}
                            disabled={disabled}
                            key={option.value}
                            onClick={() => setPlaylistGrouping((current) => current === option.value ? "none" : option.value)}
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
                <div className="playlist-filter-summary-actions">
                  <span className="playlist-filter-result-count">
                    {selectedPlaylists.length.toLocaleString()} selected
                  </span>
                  {!allVisiblePlaylistsAreShown ? (
                    <button
                      className="playlist-filter-summary-button"
                      onClick={showAllPlaylists}
                      type="button"
                    >
                      Show all
                      <span className="playlist-tab-count">{visibleOwnedPlaylists.length.toLocaleString()}</span>
                    </button>
                  ) : null}
                  {playlistEditMode && hiddenPlaylistsMatchingFilterCount > 0 ? (
                    <button
                      className={`playlist-filter-summary-button${showHiddenPlaylists ? " playlist-filter-summary-button-active" : ""}`}
                      onClick={() => setShowHiddenPlaylists((current) => !current)}
                      type="button"
                    >
                      {showHiddenPlaylists ? "Hide hidden" : allVisiblePlaylistsAreShown ? "Show hidden" : "Hidden"}
                      <span className="playlist-tab-count">{hiddenPlaylistsMatchingFilterCount}</span>
                    </button>
                  ) : null}
                </div>
                {!playlistOverlayOpen ? (
                  <div className="playlist-edit-actions">
                    {playlistEditMode ? (
                      <>
                        <button
                          className="playlist-edit-toggle playlist-edit-toggle-active"
                          onClick={savePlaylistEditMode}
                          type="button"
                        >
                          Save changes
                        </button>
                        <button
                          className="playlist-edit-toggle"
                          onClick={cancelPlaylistEditMode}
                          type="button"
                        >
                          Cancel
                        </button>
                      </>
                    ) : (
                      <button
                        className="playlist-edit-toggle"
                        onClick={beginPlaylistEditMode}
                        type="button"
                      >
                        Edit
                      </button>
                    )}
                  </div>
                ) : null}
              </div>
              {selectedPlaylists.length > 0 ? (
                <div className="playlist-grid-scroll">
                  {playlistGrouping === "none" ? (
                    <DashboardPlaylistColumn
                      section="playlists"
                      items={selectedPlaylists}
                      available={true}
                      emptyCopy="No playlists were returned for this playlist type."
                      unavailableCopy=""
                      sectionPage={0}
                      moveSectionPage={() => undefined}
                      onSelectPreview={onSelectPreview}
                      activePlaylistPlayback={activePlaylistPlayback}
                      onHidePlaylist={playlistEditMode ? hidePlaylistInEditMode : undefined}
                      onUnhidePlaylist={playlistEditMode ? unhidePlaylistInEditMode : undefined}
                      onDeletePlaylist={playlistEditMode ? onDeletePlaylist : undefined}
                      playlistEditMode={playlistEditMode}
                      playlistEditCloseAction={playlistEditCloseAction}
                      playlistLists={playlistLists}
                      onTogglePlaylistList={togglePlaylistInList}
                      pinnedPlaylistIds={pinnedPlaylistIds}
                      onTogglePinnedPlaylist={togglePinnedPlaylist}
                      paged={false}
                    />
                  ) : (
                    <div className="playlist-overlay-groups">
                      {selectedPlaylistGroups.map((group) => {
                        const groupLabel = playlistGroupDisplayLabel(group.label || "All playlists");
                        return (
                          <section className="playlist-overlay-group" key={group.label || "all"}>
                            <div className="playlist-overlay-group-header">
                              {group.spotifyUrl ? (
                                <a
                                  className="playlist-overlay-group-title playlist-overlay-group-title-link"
                                  href={group.spotifyUrl}
                                  rel="noreferrer"
                                  target="_blank"
                                >
                                  {groupLabel}
                                </a>
                              ) : (
                                <span className="playlist-overlay-group-title">{groupLabel}</span>
                              )}
                              <span>{group.playlists.length}</span>
                            </div>
                            <DashboardPlaylistColumn
                              section="playlists"
                              items={group.playlists}
                              available={true}
                              emptyCopy="No playlists were returned for this playlist type."
                              unavailableCopy=""
                              sectionPage={0}
                              moveSectionPage={() => undefined}
                              onSelectPreview={onSelectPreview}
                              activePlaylistPlayback={activePlaylistPlayback}
                              onHidePlaylist={playlistEditMode ? hidePlaylistInEditMode : undefined}
                              onUnhidePlaylist={playlistEditMode ? unhidePlaylistInEditMode : undefined}
                              onDeletePlaylist={playlistEditMode ? onDeletePlaylist : undefined}
                              playlistEditMode={playlistEditMode}
                              playlistEditCloseAction={playlistEditCloseAction}
                              playlistLists={playlistLists}
                              onTogglePlaylistList={togglePlaylistInList}
                              pinnedPlaylistIds={pinnedPlaylistIds}
                              onTogglePinnedPlaylist={togglePinnedPlaylist}
                              paged={false}
                            />
                          </section>
                        );
                      })}
                    </div>
                  )}
                </div>
              ) : (
                <p className="empty-copy">No playlists were returned for this playlist type.</p>
              )}
                </div>
              </div>
            </>
          ) : (
            <p className="empty-copy">No playlists were returned by Spotify for this account.</p>
          )
        ) : (
          <p className="empty-copy">
            Playlist metadata is still loading. It should appear after the first profile refresh finishes.
          </p>
        )
      ) : (
        <div className="preview-strip">
          {previewItems(collapsedPreviewPlaylists).map((item, index) => {
            const isActivePlaylist = Boolean(item.entityId && activePlaylistPlayback?.playlistId === item.entityId);
            const previewItem = isActivePlaylist
              ? {
                ...item,
                focusPlaylistPosition: activePlaylistPlayback?.position ?? null,
                focusSpotifyTrackId: activePlaylistPlayback?.trackId ?? null,
                trackUri: activePlaylistPlayback?.trackUri ?? item.trackUri,
              }
              : item;
            return (
              <PreviewCard
                activePlayback={isActivePlaylist ? { isPlaying: Boolean(activePlaylistPlayback?.isPlaying) } : null}
                item={previewItem}
                key={`playlists-${item.image}-${index}`}
                onSelectPreview={onSelectPreview}
              />
            );
          })}
        </div>
      )}
      {!playlistOverlayOpen ? (
        <button
          className="section-toggle section-toggle-footer"
          onClick={() => toggleSection("playlists", "playlists")}
          type="button"
        >
          <span>{playlistsFullOpen ? "^" : "v"}</span>
        </button>
      ) : null}
    </section>
    </>
  );
}
