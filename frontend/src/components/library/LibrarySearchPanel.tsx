import { useEffect, useState } from "react";

import { fetchLibraryItems, fetchLibraryStatus, postLibraryRebuild } from "../../api/appApi";
import type { LibraryItem, LibraryKind, LibraryStatusResponse, LibraryStrength, LibraryTrackItem, LibraryTrackVersion } from "../../types/appTypes";
import { formatUiErrorMessage } from "../../utils/dashboardUtils";

type LibrarySort = "recent" | "name" | "listen_count" | "playlist_count";
type LibraryStrengthFilter = LibraryStrength | "all";

type LibrarySearchPanelProps = {
  overlayOpen: boolean;
  onOpenItem: (item: LibraryItem) => void;
  onPlayTrack: (track: LibraryTrackItem) => void;
  onOverlayOpenChange: (open: boolean) => void;
};

type LibraryResultGroup = {
  key: string;
  title: string;
  items: LibraryItem[];
};

const kindOptions: Array<{ value: LibraryKind; label: string }> = [
  { value: "all", label: "All" },
  { value: "track", label: "Tracks" },
  { value: "artist", label: "Artists" },
  { value: "album", label: "Albums" },
  { value: "playlist", label: "Playlists" },
];

const strengthOptions: Array<{ value: LibraryStrengthFilter; label: string }> = [
  { value: "all", label: "All" },
  { value: "primary", label: "Primary" },
  { value: "contextual", label: "Context" },
  { value: "potential", label: "Potential" },
  { value: "ephemeral", label: "Ephemeral" },
];

const sortOptions: Array<{ value: LibrarySort; label: string }> = [
  { value: "recent", label: "Recent" },
  { value: "name", label: "Name" },
  { value: "listen_count", label: "Listens" },
  { value: "playlist_count", label: "Playlists" },
];

function reasonLabel(item: Pick<LibraryItem, "reasons" | "strength">) {
  const labels = (item.reasons ?? []).map((reason) => reason.label).filter(Boolean);
  return labels.slice(0, 3).join(" · ") || item.strength;
}

function statusCopy(status: LibraryStatusResponse | null, rebuilding: boolean) {
  if (rebuilding || status?.status === "running") {
    return "Updating index";
  }
  if (!status || status.status === "missing") {
    return "Create index";
  }
  if (status.status === "error") {
    return status.latest_error || "Index rebuild failed.";
  }
  if (status.stale) {
    return "Rules changed. Rebuild index.";
  }
  return "Update index";
}

function strengthCount(status: LibraryStatusResponse | null, strength: LibraryStrengthFilter) {
  if (!status) {
    return 0;
  }
  if (strength === "all") {
    return status.row_count;
  }
  return status.counts[strength] ?? 0;
}

function itemTitle(item: LibraryItem) {
  return item.kind === "track" ? item.track_name || "Unknown track" : item.label || item.name;
}

function itemSubtitle(item: LibraryItem, compact = false) {
  if (item.kind === "track") {
    if (compact) {
      return item.artist_name || "Unknown artist";
    }
    return `${item.artist_name || "Unknown artist"}${item.album_name ? ` · ${item.album_name}` : ""}`;
  }
  if (item.kind === "album") {
    return item.artist_name || "Unknown artist";
  }
  if (item.kind === "playlist") {
    return `${item.track_count.toLocaleString()} track${item.track_count === 1 ? "" : "s"} in Library`;
  }
  return compact ? "" : `${item.track_count.toLocaleString()} track${item.track_count === 1 ? "" : "s"} in Library`;
}

function itemImage(item: LibraryItem) {
  return item.kind === "track" ? item.image_url : item.image_url ?? null;
}

function normalizeLibraryGroupTitle(value: string) {
  return value
    .trim()
    .toLowerCase()
    .replace(/\s*[\[(]\s*(feat\.?|featuring|ft\.?)\s+[^)\]]+[\])]\s*/g, " ")
    .replace(/[()[\]{}"']/g, "")
    .replace(/\s+/g, " ");
}

function libraryGroupItemRank(item: LibraryItem) {
  if (item.kind === "artist") {
    return 0;
  }
  if (item.kind === "album") {
    return 1;
  }
  if (item.kind === "track") {
    return 2;
  }
  return 3;
}

function groupedLibraryResults(items: LibraryItem[], kind: LibraryKind): Array<LibraryItem | LibraryResultGroup> {
  if (kind !== "all") {
    return items;
  }
  const groups = new Map<string, LibraryResultGroup>();
  const groupableKinds = new Set<LibraryKind>(["artist", "album", "track"]);
  items.forEach((item) => {
    if (!groupableKinds.has(item.kind)) {
      return;
    }
    const title = itemTitle(item);
    const key = normalizeLibraryGroupTitle(title);
    if (!key || title.toLowerCase() === "unknown track") {
      return;
    }
    const group = groups.get(key) ?? { key, title, items: [] };
    group.items.push(item);
    group.items.sort((left, right) => libraryGroupItemRank(left) - libraryGroupItemRank(right));
    groups.set(key, group);
  });
  const groupedKeys = new Set(Array.from(groups.values())
    .filter((group) => new Set(group.items.map((item) => item.kind)).size > 1)
    .map((group) => group.key));
  if (groupedKeys.size === 0) {
    return items;
  }
  const emittedGroups = new Set<string>();
  const output: Array<LibraryItem | LibraryResultGroup> = [];
  items.forEach((item) => {
    const key = normalizeLibraryGroupTitle(itemTitle(item));
    if (!groupedKeys.has(key)) {
      output.push(item);
      return;
    }
    if (emittedGroups.has(key)) {
      return;
    }
    emittedGroups.add(key);
    const group = groups.get(key);
    if (group) {
      output.push(group);
    }
  });
  return output;
}

function GearIcon() {
  return (
    <svg aria-hidden="true" viewBox="0 0 24 24">
      <path d="M19.4 13.5c.1-.5.1-1 .1-1.5s0-1-.1-1.5l2-1.5-2-3.5-2.4 1a8 8 0 0 0-2.6-1.5L14 2.5h-4l-.4 2.5A8 8 0 0 0 7 6.5l-2.4-1-2 3.5 2 1.5a9.9 9.9 0 0 0 0 3l-2 1.5 2 3.5 2.4-1a8 8 0 0 0 2.6 1.5l.4 2.5h4l.4-2.5a8 8 0 0 0 2.6-1.5l2.4 1 2-3.5-2-1.5ZM12 15.5A3.5 3.5 0 1 1 12 8a3.5 3.5 0 0 1 0 7.5Z" />
    </svg>
  );
}

function versionToTrack(track: LibraryTrackItem, version: LibraryTrackVersion): LibraryTrackItem {
  return {
    ...track,
    kind: "track",
    spotify_track_id: version.spotify_track_id,
    track_id: version.track_id,
    track_name: version.track_name,
    artist_name: version.artist_name,
    album_name: version.album_name,
    album_id: version.album_id,
    image_url: version.image_url,
    uri: version.uri,
    url: version.url,
    release_track_id: version.release_track_id,
    strength: version.strength,
    reasons: version.reasons,
    play_count: version.play_count,
    last_played_at: version.last_played_at,
  };
}

function LibraryResultRow({
  compact,
  item,
  onOpenItem,
  onPlayTrack,
  onShowVersions,
}: {
  compact: boolean;
  item: LibraryItem;
  onOpenItem: (item: LibraryItem) => void;
  onPlayTrack: (track: LibraryTrackItem) => void;
  onShowVersions: (track: LibraryTrackItem) => void;
}) {
  const image = itemImage(item);
  const title = itemTitle(item);
  const subtitle = itemSubtitle(item, compact);
  const showImage = !(compact && item.kind === "track");
  const versionCount = item.kind === "track" ? item.version_count ?? 0 : 0;
  return (
    <div className={`library-track-row${compact ? " library-track-row-compact" : ""}`}>
      <button className={`library-track-main${showImage ? "" : " library-track-main-no-art"}`} onClick={() => onOpenItem(item)} type="button">
        {showImage ? (
          <span className="library-track-art" aria-hidden="true">
            {image ? <img alt="" src={image} /> : <span>{(title || "?").slice(0, 1).toUpperCase()}</span>}
          </span>
        ) : null}
        <span className="library-track-copy">
          <strong className="single-line-ellipsis">{title}</strong>
          {subtitle ? <span className="single-line-ellipsis">{subtitle}</span> : null}
          {compact ? null : <span className="library-track-reasons single-line-ellipsis">{reasonLabel(item)}</span>}
        </span>
      </button>
      <span className={`library-strength-pill library-strength-${item.strength}`}>{item.strength}</span>
      {item.kind === "track" ? (
        <button
          aria-label={`Play ${title}`}
          className="library-track-play-button"
          disabled={!item.uri}
          onClick={() => onPlayTrack(item)}
          type="button"
        >
          <span className="library-track-play-icon" aria-hidden="true">▶</span>
          <span>{formatLibraryDuration(item.duration_ms)}</span>
        </button>
      ) : null}
      {item.kind === "track" && versionCount > 0 ? (
        <button className="library-versions-button" onClick={() => onShowVersions(item)} type="button">
          {versionCount} versions
        </button>
      ) : null}
    </div>
  );
}

function formatLibraryDuration(durationMs: number | null | undefined) {
  const totalSeconds = Math.max(0, Math.round(Number(durationMs ?? 0) / 1000));
  if (!totalSeconds) {
    return "?:??";
  }
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}:${String(seconds).padStart(2, "0")}`;
}

export function LibrarySearchPanel({ overlayOpen, onOpenItem, onPlayTrack, onOverlayOpenChange }: LibrarySearchPanelProps) {
  const [status, setStatus] = useState<LibraryStatusResponse | null>(null);
  const [items, setItems] = useState<LibraryItem[]>([]);
  const [kind, setKind] = useState<LibraryKind>("all");
  const [query, setQuery] = useState("");
  const [strength, setStrength] = useState<LibraryStrengthFilter>("all");
  const [sort, setSort] = useState<LibrarySort>("recent");
  const [deepSearch, setDeepSearch] = useState(false);
  const [versionTrack, setVersionTrack] = useState<LibraryTrackItem | null>(null);
  const [loading, setLoading] = useState(false);
  const [rebuilding, setRebuilding] = useState(false);
  const [error, setError] = useState("");
  const [kindCounts, setKindCounts] = useState<Partial<Record<LibraryKind, number>>>({});
  const limit = overlayOpen ? 50 : 6;
  const effectiveDeepSearch = overlayOpen && deepSearch;

  useEffect(() => {
    let cancelled = false;
    fetchLibraryStatus()
      .then((payload) => {
        if (!cancelled) {
          setStatus(payload);
        }
      })
      .catch((loadError) => {
        if (!cancelled) {
          setError(formatUiErrorMessage(loadError, "Failed to load Library status."));
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    const handle = window.setTimeout(() => {
      setLoading(true);
      setError("");
      fetchLibraryItems({ kind, q: query, strength, sort, limit, offset: 0, deep: effectiveDeepSearch })
        .then((payload) => {
          if (cancelled) {
            return;
          }
          setItems(payload.items ?? []);
          setStatus(payload.status);
          setKindCounts((current) => ({ ...current, [kind]: payload.total ?? 0 }));
        })
        .catch((loadError) => {
          if (!cancelled) {
            setError(formatUiErrorMessage(loadError, "Failed to load Library."));
            setItems([]);
          }
        })
        .finally(() => {
          if (!cancelled) {
            setLoading(false);
          }
        });
    }, 180);
    return () => {
      cancelled = true;
      window.clearTimeout(handle);
    };
  }, [effectiveDeepSearch, kind, limit, query, sort, strength]);

  useEffect(() => {
    let cancelled = false;
    const handle = window.setTimeout(() => {
      void Promise.all(
        kindOptions.map((option) => (
          fetchLibraryItems({ kind: option.value, q: query, strength, sort: "recent", limit: 1, offset: 0, deep: effectiveDeepSearch })
            .then((payload) => [option.value, payload.total ?? 0] as const)
        )),
      )
        .then((entries) => {
          if (cancelled) {
            return;
          }
          setKindCounts(Object.fromEntries(entries) as Partial<Record<LibraryKind, number>>);
        })
        .catch(() => undefined);
    }, 220);
    return () => {
      cancelled = true;
      window.clearTimeout(handle);
    };
  }, [effectiveDeepSearch, query, strength]);

  useEffect(() => {
    if (!rebuilding) {
      return;
    }
    const handle = window.setInterval(() => {
      fetchLibraryStatus()
        .then((payload) => {
          setStatus(payload);
          if (payload.status !== "running") {
            setRebuilding(false);
            void fetchLibraryItems({ kind, q: query, strength, sort, limit, offset: 0, deep: effectiveDeepSearch }).then((tracksPayload) => {
              setItems(tracksPayload.items ?? []);
              setStatus(tracksPayload.status);
              setKindCounts((current) => ({ ...current, [kind]: tracksPayload.total ?? 0 }));
            });
          }
        })
        .catch(() => undefined);
    }, 1500);
    return () => window.clearInterval(handle);
  }, [effectiveDeepSearch, kind, limit, query, rebuilding, sort, strength]);

  async function rebuildLibrary() {
    setRebuilding(true);
    setError("");
    try {
      const payload = await postLibraryRebuild();
      setStatus({
        ...payload.status,
        status: payload.scheduled ? "running" : payload.status.status,
      });
      try {
        const refreshed = await fetchLibraryStatus();
        setStatus(refreshed);
      } catch {
        // Polling effect will keep checking after the rebuild is scheduled.
      }
    } catch (rebuildError) {
      setRebuilding(false);
      setError(formatUiErrorMessage(rebuildError, "Failed to start Library rebuild."));
    }
  }

  function renderBody({ showFooter }: { showFooter: boolean }) {
    const compact = !showFooter;
    const resultItems = groupedLibraryResults(items, kind);
    return (
      <>
        <div className={`library-search-controls${showFooter ? " library-search-controls-full" : ""}`}>
          <label className="library-search-field">
            <input
              aria-label="Search"
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Search"
              type="search"
              value={query}
            />
          </label>
          <label className="library-sort-field">
            <select aria-label="Sort" onChange={(event) => setSort(event.target.value as LibrarySort)} value={sort}>
              {sortOptions.map((option) => (
                <option key={option.value} value={option.value}>{option.label}</option>
              ))}
            </select>
          </label>
          {showFooter ? (
            <button
              aria-pressed={deepSearch}
              className={`library-deep-toggle${deepSearch ? " library-deep-toggle-active" : ""}`}
              onClick={() => setDeepSearch((current) => !current)}
              title="Include matches from related tracks, albums, and playlists"
              type="button"
            >
              Deep
            </button>
          ) : null}
        </div>
        <div className="library-kind-tabs" role="tablist" aria-label="Library result type">
          {kindOptions.map((option) => (
            <button
              aria-selected={kind === option.value}
              className={[
                "library-kind-tab",
                option.value === "all" ? "library-kind-tab-all" : "",
                kind === option.value ? "library-kind-tab-active" : "",
              ].filter(Boolean).join(" ")}
              key={option.value}
              onClick={() => setKind(option.value)}
              role="tab"
              type="button"
            >
              <span>{option.label}</span>
              <small>{(kindCounts[option.value] ?? 0).toLocaleString()}</small>
            </button>
          ))}
        </div>
        <div className="library-track-list">
          {resultItems.map((item) => (
            "items" in item ? (
              <div className="library-result-group" key={`group-${item.key}`}>
                {item.items.map((groupItem) => (
                  <LibraryResultRow
                    compact={compact}
                    item={groupItem}
                    key={`${groupItem.kind}-${groupItem.kind === "track" ? groupItem.library_group_key ?? groupItem.spotify_track_id : groupItem.entity_id ?? groupItem.label}`}
                    onOpenItem={onOpenItem}
                    onPlayTrack={onPlayTrack}
                    onShowVersions={setVersionTrack}
                  />
                ))}
              </div>
            ) : (
              <LibraryResultRow
                compact={compact}
                item={item}
                key={`${item.kind}-${item.kind === "track" ? item.library_group_key ?? item.spotify_track_id : item.entity_id ?? item.label}`}
                onOpenItem={onOpenItem}
                onPlayTrack={onPlayTrack}
                onShowVersions={setVersionTrack}
              />
            )
          ))}
          {!loading && items.length === 0 ? <p className="empty-copy">No matching results.</p> : null}
          {loading ? <p className="empty-copy">Loading...</p> : null}
        </div>
        {error ? <p className="empty-copy library-error">{error}</p> : null}
        {showFooter ? (
          <div className="library-filter-footer">
            <button
              aria-label={statusCopy(status, rebuilding)}
              className="library-index-button"
              disabled={rebuilding || status?.status === "running"}
              onClick={rebuildLibrary}
              title={statusCopy(status, rebuilding)}
              type="button"
            >
              <GearIcon />
            </button>
            <div className="library-strength-tabs" role="tablist" aria-label="Library strength">
              {strengthOptions.map((option) => (
                <button
                  aria-selected={strength === option.value}
                  className={strength === option.value ? "library-strength-tab library-strength-tab-active" : "library-strength-tab"}
                  key={option.value}
                  onClick={() => setStrength(option.value)}
                  role="tab"
                  type="button"
                >
                  <span>{option.label}</span>
                  <small>{strengthCount(status, option.value).toLocaleString()}</small>
                </button>
              ))}
            </div>
          </div>
        ) : null}
      </>
    );
  }

  return (
    <section className="library-search-panel" aria-label="Library search">
      {renderBody({ showFooter: false })}
      {overlayOpen ? (
        <div
          className="library-overlay"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget) {
              onOverlayOpenChange(false);
            }
          }}
          aria-modal="true"
          aria-label="Library"
          role="dialog"
        >
          <div className="library-overlay-panel">
            <div className="library-overlay-header">
              <h3>Library</h3>
              <button aria-label="Close Library" onClick={() => onOverlayOpenChange(false)} type="button">Close</button>
            </div>
            {renderBody({ showFooter: true })}
          </div>
        </div>
      ) : null}
      {versionTrack ? (
        <div
          className="library-overlay library-version-overlay"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget) {
              setVersionTrack(null);
            }
          }}
          role="dialog"
          aria-modal="true"
          aria-labelledby="library-version-heading"
        >
          <div className="library-overlay-panel library-version-panel">
            <div className="library-overlay-header">
              <div>
                <h3 id="library-version-heading">Versions</h3>
                <p>{versionTrack.track_name || "Unknown track"}</p>
              </div>
              <button aria-label="Close versions" onClick={() => setVersionTrack(null)} type="button">Close</button>
            </div>
            <div className="library-version-list">
              {(versionTrack.versions ?? []).map((version) => (
                <button
                  className="library-track-row library-version-row"
                  key={`${version.release_track_id ?? "release"}-${version.spotify_track_id}`}
                  onClick={() => {
                    onOpenItem(versionToTrack(versionTrack, version));
                    setVersionTrack(null);
                  }}
                  type="button"
                >
                  <span className="library-track-art" aria-hidden="true">
                    {version.image_url ? <img alt="" src={version.image_url} /> : <span>{(version.track_name || "?").slice(0, 1).toUpperCase()}</span>}
                  </span>
                  <span className="library-track-copy">
                    <strong className="single-line-ellipsis">{version.track_name || "Unknown track"}</strong>
                    <span className="single-line-ellipsis">{version.artist_name || "Unknown artist"}{version.album_name ? ` · ${version.album_name}` : ""}</span>
                    <span className="library-track-reasons single-line-ellipsis">{reasonLabel(versionToTrack(versionTrack, version))}</span>
                  </span>
                  <span className={`library-strength-pill library-strength-${version.strength}`}>{version.strength}</span>
                </button>
              ))}
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}
