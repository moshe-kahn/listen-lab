type NewTrackBadgeProps = {
  className?: string;
  title?: string;
};

export function NewTrackBadge({ className = "", title = "Not listened yet" }: NewTrackBadgeProps) {
  return (
    <span className={`new-track-badge${className ? ` ${className}` : ""}`} title={title} aria-label={title}>
      ✦
    </span>
  );
}
