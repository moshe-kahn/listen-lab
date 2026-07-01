export type PlayerBottomDrawerTab = "previousQueues" | "bookmarks" | "playlists" | "explore";

type PlayerBottomDrawerProps = {
  activeTab: PlayerBottomDrawerTab;
  expanded: boolean;
  onTabChange: (tab: PlayerBottomDrawerTab) => void;
  onToggle: () => void;
};

const tabs: Array<{ value: PlayerBottomDrawerTab; label: string; empty: string }> = [
  { value: "previousQueues", label: "Previous Queues", empty: "Saved queue history will appear here." },
  { value: "bookmarks", label: "Bookmarks", empty: "Bookmarked tracks will appear here." },
  { value: "playlists", label: "Playlists", empty: "Playlist shortcuts will appear here." },
  { value: "explore", label: "Explore", empty: "Discovery tools will appear here." },
];

export function PlayerBottomDrawer({
  activeTab,
  expanded,
  onTabChange,
  onToggle,
}: PlayerBottomDrawerProps) {
  const active = tabs.find((tab) => tab.value === activeTab) ?? tabs[0];

  return (
    <div className={`player-bottom-drawer${expanded ? " player-bottom-drawer-expanded" : ""}`}>
      <div className="player-bottom-drawer-bar">
        <button
          aria-expanded={expanded}
          aria-label={expanded ? "Collapse player drawer" : "Expand player drawer"}
          className="player-bottom-drawer-toggle"
          onClick={onToggle}
          type="button"
        >
          <span aria-hidden="true">{expanded ? "⌄" : "⌃"}</span>
        </button>
        <div className="player-bottom-drawer-tabs" role="tablist" aria-label="Player drawer">
          {tabs.map((tab) => (
            <button
              aria-selected={activeTab === tab.value}
              className={activeTab === tab.value ? "player-bottom-drawer-tab player-bottom-drawer-tab-active" : "player-bottom-drawer-tab"}
              key={tab.value}
              onClick={() => {
                onTabChange(tab.value);
                if (!expanded) {
                  onToggle();
                }
              }}
              role="tab"
              type="button"
            >
              {tab.label}
            </button>
          ))}
        </div>
      </div>
      {expanded ? (
        <div className="player-bottom-drawer-panel" role="tabpanel">
          <p>{active.empty}</p>
        </div>
      ) : null}
    </div>
  );
}
