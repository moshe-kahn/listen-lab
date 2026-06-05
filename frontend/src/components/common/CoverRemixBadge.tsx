type CoverRemixBadgeProps = {
  className?: string;
  title?: string;
};

export function CoverRemixBadge({ className = "", title = "Cover, remix, or rework" }: CoverRemixBadgeProps) {
  return (
    <span className={`cover-remix-badge${className ? ` ${className}` : ""}`} title={title} aria-label={title}>
      C
    </span>
  );
}
