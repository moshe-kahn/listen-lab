import { useEffect, useRef, useState, type ReactNode } from "react";
import type { OwnedPlaylist, PreviewItem, SectionKey } from "../../types/appTypes";
import { previewItems } from "../../utils/dashboardUtils";
import { DashboardPlaylistColumn } from "./DashboardColumns";
import { PreviewCard } from "./PreviewCard";

type PlaylistTabKey = "all" | "created" | "collaborations" | "spotify" | "others";

type MinePlaylistVisibility = "public" | "private";
type CollaborationPlaylistOwner = "yours" | "others";
type PlaylistCategoryFilterMode = "all" | "none" | "selected";
type PlaylistList = {
  id: string;
  name: string;
  playlistIds: string[];
};

const PLAYLIST_LISTS_STORAGE_KEY = "listenlab.playlistLists.v1";
const PINNED_PLAYLISTS_STORAGE_KEY = "listenlab.pinnedPlaylists.v1";
const PLAYLIST_FILTERS_STORAGE_KEY = "listenlab.playlistFilters.v1";

const PLAYLIST_TABS: Array<{ key: PlaylistTabKey; label: string }> = [
  { key: "all", label: "All" },
  { key: "created", label: "Yours" },
  { key: "collaborations", label: "Collabs" },
  { key: "spotify", label: "Spotify" },
  { key: "others", label: "Others" },
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

function isSpotifyPlaylist(playlist: OwnedPlaylist) {
  return !playlist.is_owned && (
    playlist.owner_id?.trim().toLocaleLowerCase() === "spotify"
    || playlist.owner_name?.trim().toLocaleLowerCase() === "spotify"
  );
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
      return playlists.filter((playlist) => !playlist.is_owned && !isSpotifyPlaylist(playlist));
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
    case "spotify":
      return playlists.filter(isSpotifyPlaylist);
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
        || value === "spotify"
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
  playlistsOpen: boolean;
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
  playlistsOpen,
  toggleSection,
  onSelectPreview,
  onHidePlaylist,
  onUnhidePlaylist,
  onDeletePlaylist,
  renderSectionTitle,
}: DashboardPlaylistsSectionProps) {
  const playlistFiltersRef = useRef<HTMLDivElement | null>(null);
  const storedPlaylistFilters = useRef(readStoredPlaylistFilters());
  const [selectedPlaylistTabs, setSelectedPlaylistTabs] = useState<PlaylistTabKey[]>(() => storedPlaylistFilters.current.selectedTabs);
  const [mineVisibility, setMineVisibility] = useState<MinePlaylistVisibility[]>(() => storedPlaylistFilters.current.mineVisibility);
  const [collaborationVisibility, setCollaborationVisibility] = useState<MinePlaylistVisibility[]>(() => storedPlaylistFilters.current.collaborationVisibility);
  const [collaborationOwners, setCollaborationOwners] = useState<CollaborationPlaylistOwner[]>(() => storedPlaylistFilters.current.collaborationOwners);
  const [playlistTypeMenuOpen, setPlaylistTypeMenuOpen] = useState(false);
  const [collaborationOwnerMenuOpen, setCollaborationOwnerMenuOpen] = useState(false);
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

  useEffect(() => {
    if (playlistsOpen) {
      return;
    }
    setPlaylistTypeMenuOpen(false);
    setCollaborationOwnerMenuOpen(false);
    setPlaylistListMenuOpen(false);
    setPlaylistCategoryCreatorOpen(false);
    setPlaylistEditMode(false);
    setShowHiddenPlaylists(false);
    setSelectedPlaylistTabs((current) => current.length > 0 ? current : ["all"]);
    setMineVisibility((current) => current.length > 0 ? current : ["public", "private"]);
    setCollaborationVisibility((current) => current.length > 0 ? current : ["public", "private"]);
    setCollaborationOwners((current) => current.length > 0 ? current : ["yours", "others"]);
  }, [playlistsOpen]);

  useEffect(() => {
    if (!playlistTypeMenuOpen && !collaborationOwnerMenuOpen && !playlistListMenuOpen) {
      return;
    }
    function closeOpenPlaylistMenus() {
      closePlaylistTypeMenu();
      closeCollaborationOwnerMenu();
      closePlaylistListMenu();
    }
    function handlePointerDown(event: PointerEvent) {
      const target = event.target;
      if (!(target instanceof Element)) {
        return;
      }
      if (playlistFiltersRef.current?.contains(target)) {
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
  }, [playlistTypeMenuOpen, collaborationOwnerMenuOpen, playlistListMenuOpen]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    window.localStorage.setItem(PLAYLIST_LISTS_STORAGE_KEY, JSON.stringify(playlistLists));
  }, [playlistLists]);

  useEffect(() => {
    if (!playlistsOpen || playlistCategoriesLoaded) {
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
  }, [apiBaseUrl, playlistCategoriesLoaded, playlistsOpen]);

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

  const visibleOwnedPlaylists = ownedPlaylists.filter((playlist) => !playlist.hidden_by_user);
  const canRenderPlaylistRows = ownedPlaylistsAvailable || ownedPlaylists.length > 0;
  const hiddenPlaylistCount = ownedPlaylists.length - visibleOwnedPlaylists.length;
  const playlistSelectionSource = playlistEditMode && showHiddenPlaylists
    ? ownedPlaylists
    : visibleOwnedPlaylists;
  const listFilteredOwnedPlaylists = playlistSelectionSource.filter((playlist, index) => {
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
  });
  const selectedPlaylists = playlistsForSelection(listFilteredOwnedPlaylists, selectedPlaylistTabs, mineVisibility, collaborationOwners, collaborationVisibility)
    .slice()
    .sort((a, b) => Number(pinnedPlaylistIds.includes(playlistStableId(b))) - Number(pinnedPlaylistIds.includes(playlistStableId(a))));
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
  const collaborationTotalCount = collaborationYoursCount + collaborationOthersCount;
  const visiblePlaylistTabs = PLAYLIST_TABS.filter((tab) => tab.key !== "collaborations" || collaborationTotalCount > 0);

  useEffect(() => {
    if (collaborationTotalCount > 0) {
      return;
    }
    setSelectedPlaylistTabs((current) => {
      const next = current.filter((tab) => tab !== "collaborations");
      return next.length > 0 ? next : ["all"];
    });
  }, [collaborationTotalCount]);

  const playlistTabCounts = visiblePlaylistTabs.reduce<Record<PlaylistTabKey, number>>((counts, tab) => {
    counts[tab.key] = playlistsForTab(listFilteredOwnedPlaylists, tab.key, ["public", "private"], ["yours", "others"], ["public", "private"]).length;
    return counts;
  }, {
    all: 0,
    created: 0,
    collaborations: 0,
    spotify: 0,
    others: 0,
  });
  const mineVisibilityOptions: Array<{ key: MinePlaylistVisibility; label: string; count: number }> = [
    { key: "public", label: "Public", count: minePublicCount },
    { key: "private", label: "Private", count: minePrivateCount },
  ];
  const collaborationOwnerOptions: Array<{ key: CollaborationPlaylistOwner; label: string; count: number }> = [
    { key: "yours", label: "By you", count: collaborationYoursCount },
    { key: "others", label: "By others", count: collaborationOthersCount },
  ];
  const toggleMineVisibility = (key: MinePlaylistVisibility) => {
    setMineVisibility((current) => {
      if (current.length === 1 && current.includes(key)) {
        return ["public", "private"];
      }
      return [key];
    });
  };
  const toggleMineVisibilityFromOwnerMenu = (key: MinePlaylistVisibility) => {
    setSelectedPlaylistTabs((current) => (
      current.includes("created")
        ? current
        : [...current.filter((tab) => tab !== "all"), "created"]
    ));
    toggleMineVisibility(key);
  };
  const toggleCollaborationVisibilityFromOwnerMenu = (key: MinePlaylistVisibility) => {
    setSelectedPlaylistTabs((current) => (
      current.includes("collaborations")
        ? current
        : [...current.filter((tab) => tab !== "all"), "collaborations"]
    ));
    setCollaborationVisibility((current) => {
      if (current.length === 1 && current.includes(key)) {
        return ["public", "private"];
      }
      return [key];
    });
  };
  const closeCollaborationOwnerMenu = () => {
    setCollaborationOwnerMenuOpen(false);
    setCollaborationOwners((current) => current.length > 0 ? current : ["yours", "others"]);
  };
  const closePlaylistListMenu = () => {
    setPlaylistListMenuOpen(false);
    setPlaylistCategoryCreatorOpen(false);
  };
  const selectPlaylistCategoryMode = (mode: PlaylistCategoryFilterMode) => {
    setPlaylistCategoryFilterMode(mode);
    if (mode !== "selected") {
      setSelectedPlaylistListIds([]);
    }
  };
  const togglePlaylistListFilter = (listId: string) => {
    setPlaylistCategoryFilterMode("selected");
    setSelectedPlaylistListIds((current) => {
      const allListIds = playlistLists.map((list) => list.id);
      if (playlistCategoryFilterMode === "all") {
        return allListIds.filter((id) => id !== listId);
      }
      if (playlistCategoryFilterMode === "none") {
        return [listId];
      }
      return current.includes(listId)
        ? current.filter((id) => id !== listId)
        : [...current, listId];
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
  const selectedSpecificTabs = selectedPlaylistTabs.filter((tab) => tab !== "all");
  const playlistTypeLabel = selectedPlaylistTabs.includes("all")
    ? "All Owners"
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
  const activePlaylistListIds = playlistCategoryFilterMode === "all"
    ? playlistLists.map((list) => list.id)
    : playlistCategoryFilterMode === "none"
      ? []
      : selectedPlaylistListIds;
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
      const currentSpecificTabs = current.filter((tab) => tab !== "all");
      if (currentSpecificTabs.includes(key)) {
        return currentSpecificTabs.length === 1
          ? currentSpecificTabs
          : currentSpecificTabs.filter((tab) => tab !== key);
      }
      return [...currentSpecificTabs, key];
    });
  };

  return (
    <section className="info-card info-card-wide" id="playlists">
      <button className="section-toggle section-toggle-header" onClick={() => toggleSection("playlists", "playlists")} type="button">
        <h2>{renderSectionTitle("Playlists", "playlists")}</h2>
      </button>
      {playlistsOpen ? (
        canRenderPlaylistRows ? (
          ownedPlaylists.length > 0 ? (
            <>
              <div className="playlist-filter-controls" ref={playlistFiltersRef}>
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
                        setPlaylistListMenuOpen(true);
                      }
                    }}
                    type="button"
                  >
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
                      <button
                        aria-checked={playlistCategoryFilterMode === "none"}
                        className={`playlist-type-dropdown-item${playlistCategoryFilterMode === "none" ? " playlist-type-dropdown-item-active" : ""}`}
                        onClick={() => selectPlaylistCategoryMode("none")}
                        role="menuitemcheckbox"
                        type="button"
                      >
                        <span className="playlist-type-dropdown-check" aria-hidden="true">
                          {playlistCategoryFilterMode === "none" ? "✓" : ""}
                        </span>
                        <span>None</span>
                        <span className="playlist-tab-count">{noneCategoryCount}</span>
                      </button>
                      <div className="playlist-type-dropdown-divider" />
                      {playlistLists.length > 0 ? playlistLists.map((list) => (
                        <button
                          aria-checked={activePlaylistListIds.includes(list.id)}
                          className={`playlist-type-dropdown-item${activePlaylistListIds.includes(list.id) ? " playlist-type-dropdown-item-active" : ""}`}
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
                      }
                    }}
                    type="button"
                  >
                    <span>{playlistTypeLabel}</span>
                    <span className="playlist-type-dropdown-chevron" aria-hidden="true">v</span>
                  </button>
                  {playlistTypeMenuOpen ? (
                    <div className="playlist-type-dropdown-menu" role="menu">
                      {visiblePlaylistTabs.map((tab) => {
                        const tabChecked = selectedPlaylistTabs.includes("all") || selectedPlaylistTabs.includes(tab.key);
                        const tabHasVisibilityToggle = tabChecked && (tab.key === "created" || tab.key === "collaborations");
                        return (
                        <div className={`playlist-type-dropdown-group${tab.key === "all" ? " playlist-type-dropdown-group-divider" : ""}`} key={tab.key}>
                          <button
                            className={`playlist-type-dropdown-item${tabChecked ? " playlist-type-dropdown-item-active" : ""}${tabHasVisibilityToggle ? " playlist-type-dropdown-item-with-toggle" : ""}`}
                            onClick={() => togglePlaylistTab(tab.key)}
                            role="menuitemcheckbox"
                            aria-checked={tabChecked}
                            type="button"
                          >
                            <span className="playlist-type-dropdown-check" aria-hidden="true">
                              {tabChecked ? "✓" : ""}
                            </span>
                            <span>{tab.label}</span>
                            {tab.key === "created" && tabChecked ? (
                              <span className="playlist-owner-visibility-toggle" aria-label="Yours playlist visibility">
                                <span
                                  aria-label="Public playlists"
                                  aria-pressed={mineVisibility.includes("public")}
                                  className={`playlist-owner-visibility-button${mineVisibility.includes("public") ? " playlist-owner-visibility-button-active" : ""}`}
                                  onClick={(event) => {
                                    event.preventDefault();
                                    event.stopPropagation();
                                    toggleMineVisibilityFromOwnerMenu("public");
                                  }}
                                  onKeyDown={(event) => {
                                    if (event.key === "Enter" || event.key === " ") {
                                      event.preventDefault();
                                      event.stopPropagation();
                                      toggleMineVisibilityFromOwnerMenu("public");
                                    }
                                  }}
                                  role="button"
                                  tabIndex={0}
                                  title="Public"
                                >
                                  🌐
                                </span>
                                <span
                                  aria-label="Private playlists"
                                  aria-pressed={mineVisibility.includes("private")}
                                  className={`playlist-owner-visibility-button${mineVisibility.includes("private") ? " playlist-owner-visibility-button-active" : ""}`}
                                  onClick={(event) => {
                                    event.preventDefault();
                                    event.stopPropagation();
                                    toggleMineVisibilityFromOwnerMenu("private");
                                  }}
                                  onKeyDown={(event) => {
                                    if (event.key === "Enter" || event.key === " ") {
                                      event.preventDefault();
                                      event.stopPropagation();
                                      toggleMineVisibilityFromOwnerMenu("private");
                                    }
                                  }}
                                  role="button"
                                  tabIndex={0}
                                  title="Private"
                                >
                                  🔒
                                </span>
                              </span>
                            ) : null}
                            {tab.key === "collaborations" && tabChecked ? (
                              <span className="playlist-owner-visibility-toggle" aria-label="Collabs playlist visibility">
                                <span
                                  aria-label="Public collaborative playlists"
                                  aria-pressed={collaborationVisibility.includes("public")}
                                  className={`playlist-owner-visibility-button${collaborationVisibility.includes("public") ? " playlist-owner-visibility-button-active" : ""}`}
                                  onClick={(event) => {
                                    event.preventDefault();
                                    event.stopPropagation();
                                    toggleCollaborationVisibilityFromOwnerMenu("public");
                                  }}
                                  onKeyDown={(event) => {
                                    if (event.key === "Enter" || event.key === " ") {
                                      event.preventDefault();
                                      event.stopPropagation();
                                      toggleCollaborationVisibilityFromOwnerMenu("public");
                                    }
                                  }}
                                  role="button"
                                  tabIndex={0}
                                  title="Public"
                                >
                                  🌐
                                </span>
                                <span
                                  aria-label="Private collaborative playlists"
                                  aria-pressed={collaborationVisibility.includes("private")}
                                  className={`playlist-owner-visibility-button${collaborationVisibility.includes("private") ? " playlist-owner-visibility-button-active" : ""}`}
                                  onClick={(event) => {
                                    event.preventDefault();
                                    event.stopPropagation();
                                    toggleCollaborationVisibilityFromOwnerMenu("private");
                                  }}
                                  onKeyDown={(event) => {
                                    if (event.key === "Enter" || event.key === " ") {
                                      event.preventDefault();
                                      event.stopPropagation();
                                      toggleCollaborationVisibilityFromOwnerMenu("private");
                                    }
                                  }}
                                  role="button"
                                  tabIndex={0}
                                  title="Private"
                                >
                                  🔒
                                </span>
                              </span>
                            ) : null}
                            <span className="playlist-tab-count">
                              {tab.key === "created"
                                ? mineVisibilityCount
                                : tab.key === "collaborations"
                                  ? collaborationVisibilityCount
                                  : playlistTabCounts[tab.key]}
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
                          setCollaborationOwnerMenuOpen(true);
                        }
                      }}
                      type="button"
                    >
                      <span>{collaborationOwnerLabel}</span>
                      <span className="playlist-type-dropdown-chevron" aria-hidden="true">v</span>
                    </button>
                    {collaborationOwnerMenuOpen ? (
                      <div className="playlist-type-dropdown-menu playlist-subfilter-dropdown-menu" role="menu">
                        {collaborationOwnerOptions.map((option) => (
                          <button
                            aria-checked={collaborationOwners.includes(option.key)}
                            className={`playlist-type-dropdown-item${collaborationOwners.includes(option.key) ? " playlist-type-dropdown-item-active" : ""}`}
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
                <span className="playlist-filter-result-count">{selectedPlaylists.length}</span>
                <div className="playlist-edit-actions">
                  {playlistEditMode && hiddenPlaylistCount > 0 ? (
                    <button
                      className={`playlist-edit-toggle${showHiddenPlaylists ? " playlist-edit-toggle-active" : ""}`}
                      onClick={() => setShowHiddenPlaylists((current) => !current)}
                      type="button"
                    >
                      {showHiddenPlaylists ? "Hide hidden" : "Show hidden"}
                      <span className="playlist-tab-count">{hiddenPlaylistCount}</span>
                    </button>
                  ) : null}
                  <button
                    className={`playlist-edit-toggle${playlistEditMode ? " playlist-edit-toggle-active" : ""}`}
                    onClick={() => {
                      setPlaylistEditMode((current) => {
                        const next = !current;
                        if (!next) {
                          setShowHiddenPlaylists(false);
                        }
                        return next;
                      });
                    }}
                    type="button"
                  >
                    {playlistEditMode ? "Save changes" : "Edit"}
                  </button>
                </div>
              </div>
              {selectedPlaylists.length > 0 ? (
                <div className="playlist-grid-scroll">
                  <DashboardPlaylistColumn
                    section="playlists"
                    items={selectedPlaylists}
                    available={true}
                    emptyCopy="No playlists were returned for this playlist type."
                    unavailableCopy=""
                    sectionPage={0}
                    moveSectionPage={() => undefined}
                    onSelectPreview={onSelectPreview}
                    onHidePlaylist={playlistEditMode ? onHidePlaylist : undefined}
                    onUnhidePlaylist={playlistEditMode ? onUnhidePlaylist : undefined}
                    onDeletePlaylist={playlistEditMode ? onDeletePlaylist : undefined}
                    playlistEditMode={playlistEditMode}
                    playlistLists={playlistLists}
                    onTogglePlaylistList={togglePlaylistInList}
                    pinnedPlaylistIds={pinnedPlaylistIds}
                    onTogglePinnedPlaylist={togglePinnedPlaylist}
                    paged={false}
                  />
                </div>
              ) : (
                <p className="empty-copy">No playlists were returned for this playlist type.</p>
              )}
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
          {previewItems(collapsedPreviewPlaylists).map((item, index) =>
            <PreviewCard
              item={item}
              key={`playlists-${item.image}-${index}`}
              onSelectPreview={onSelectPreview}
            />,
          )}
        </div>
      )}
      <button className="section-toggle section-toggle-footer" onClick={() => toggleSection("playlists", "playlists")} type="button">
        <span>{playlistsOpen ? "^" : "v"}</span>
      </button>
    </section>
  );
}
