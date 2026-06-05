import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import type { ReactNode } from "react";

export type PlaybackAction = "play_now" | "play_next" | "add_to_queue";

export type PlaybackMenuExtraAction = {
  label: string;
  ariaLabel?: string;
  disabled?: boolean;
  title?: string;
  icon: "bookmark";
  onSelect?: () => void | Promise<void>;
};

type PlaybackActionMenuProps = {
  ariaLabel: string;
  buttonClassName: string;
  disabled?: boolean;
  extraActions?: PlaybackMenuExtraAction[];
  isPlaying?: boolean;
  menuAlign?: "left" | "right";
  placement?: "adjacent" | "overlay-trigger";
  onAction: (action: PlaybackAction) => void | Promise<void>;
  children: ReactNode;
};

export function PlaybackActionMenu({
  ariaLabel,
  buttonClassName,
  disabled = false,
  extraActions = [],
  isPlaying = false,
  menuAlign = "left",
  placement = "adjacent",
  onAction,
  children,
}: PlaybackActionMenuProps) {
  const [open, setOpen] = useState(false);
  const [position, setPosition] = useState<{ left: number; top: number } | null>(null);
  const rootRef = useRef<HTMLSpanElement | null>(null);
  const buttonRef = useRef<HTMLButtonElement | null>(null);
  const popoverRef = useRef<HTMLSpanElement | null>(null);

  const updatePosition = useCallback(() => {
    const rect = buttonRef.current?.getBoundingClientRect();
    if (!rect) {
      return;
    }
    const width = 166;
    const height = popoverRef.current?.offsetHeight ?? 122;
    const centeredLeft = placement === "overlay-trigger"
      ? rect.left
      : rect.left + (rect.width / 2) - (width / 2);
    const left = menuAlign === "right"
      ? Math.max(8, Math.min(window.innerWidth - width - 8, rect.right - width))
      : Math.max(8, Math.min(window.innerWidth - width - 8, centeredLeft));
    if (placement === "overlay-trigger") {
      setPosition({
        left,
        top: Math.max(8, Math.min(window.innerHeight - height - 8, rect.top)),
      });
      return;
    }
    const aboveTop = rect.top - height - 8;
    const belowTop = rect.bottom + 8;
    setPosition({
      left,
      top: aboveTop >= 8 ? aboveTop : Math.min(window.innerHeight - height - 8, belowTop),
    });
  }, [menuAlign, placement]);

  useLayoutEffect(() => {
    if (open) {
      updatePosition();
    }
  }, [open, updatePosition]);

  useEffect(() => {
    if (!open) {
      return undefined;
    }
    const handlePointerDown = (event: PointerEvent) => {
      const target = event.target as Node;
      if (!rootRef.current?.contains(target) && !popoverRef.current?.contains(target)) {
        setOpen(false);
      }
    };
    const handleReposition = () => updatePosition();
    window.addEventListener("pointerdown", handlePointerDown);
    window.addEventListener("resize", handleReposition);
    window.addEventListener("scroll", handleReposition, true);
    return () => {
      window.removeEventListener("pointerdown", handlePointerDown);
      window.removeEventListener("resize", handleReposition);
      window.removeEventListener("scroll", handleReposition, true);
    };
  }, [open, updatePosition]);

  async function selectAction(action: PlaybackAction) {
    setOpen(false);
    await onAction(action);
  }

  async function selectExtraAction(action: PlaybackMenuExtraAction) {
    if (action.disabled || !action.onSelect) {
      return;
    }
    setOpen(false);
    await action.onSelect();
  }

  return (
    <span className={`playback-action-menu playback-action-menu-${menuAlign}`} ref={rootRef}>
      <button
        aria-expanded={open}
        aria-haspopup="menu"
        aria-label={ariaLabel}
        className={buttonClassName}
        disabled={disabled}
        onClick={() => setOpen((current) => !current)}
        ref={buttonRef}
        type="button"
      >
        {children}
      </button>
      {open ? createPortal(
        <span
          className={`playback-action-menu-popover playback-action-menu-popover-${placement}`}
          ref={popoverRef}
          role="menu"
          style={position ? { left: position.left, top: position.top } : undefined}
        >
          <button onClick={() => void selectAction("play_now")} role="menuitem" type="button">
            <PlaybackActionIcon kind="play" />
            <span>{isPlaying ? "Resume / pause" : "Play now"}</span>
          </button>
          <button onClick={() => void selectAction("play_next")} role="menuitem" type="button">
            <PlaybackActionIcon kind="next" />
            <span>Play next</span>
          </button>
          <button onClick={() => void selectAction("add_to_queue")} role="menuitem" type="button">
            <PlaybackActionIcon kind="queue" />
            <span>Add to queue</span>
          </button>
          {extraActions.map((action) => (
            <button
              aria-label={action.ariaLabel ?? action.label}
              disabled={action.disabled}
              key={`${action.icon}-${action.label}`}
              onClick={() => void selectExtraAction(action)}
              role="menuitem"
              title={action.title}
              type="button"
            >
              <PlaybackActionIcon kind={action.icon} />
              <span>{action.label}</span>
            </button>
          ))}
        </span>,
        document.body,
      ) : null}
    </span>
  );
}

function PlaybackActionIcon({ kind }: { kind: "play" | "next" | "queue" | "bookmark" }) {
  if (kind === "bookmark") {
    return (
      <svg aria-hidden="true" className="playback-action-menu-icon playback-action-menu-icon-filled" viewBox="0 0 20 20">
        <path d="M5 3.5h10v13l-5-3.2-5 3.2v-13Z" />
      </svg>
    );
  }
  if (kind === "queue") {
    return (
      <svg aria-hidden="true" className="playback-action-menu-icon playback-action-menu-icon-filled" viewBox="0 0 20 20">
        <path d="M4 5h7" />
        <path d="M4 10h7" />
        <path d="M4 15h5" />
        <path d="M14 11v6" />
        <path d="M11 14h6" />
      </svg>
    );
  }
  if (kind === "next") {
    return (
      <svg aria-hidden="true" className="playback-action-menu-icon" viewBox="0 0 20 20">
        <path d="M5 5.5v9l7-4.5-7-4.5z" />
        <path d="M14 5v10" />
      </svg>
    );
  }
  return (
    <svg aria-hidden="true" className="playback-action-menu-icon playback-action-menu-icon-filled" viewBox="0 0 20 20">
      <path d="M6 5.5v9l8-4.5-8-4.5z" />
    </svg>
  );
}
