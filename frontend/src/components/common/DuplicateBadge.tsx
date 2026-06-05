type DuplicateBadgeProps = {
  className?: string;
  title?: string;
};

export function DuplicateBadge({ className = "", title = "Duplicate source grouping" }: DuplicateBadgeProps) {
  return (
    <span className={`duplicate-badge${className ? ` ${className}` : ""}`} title={title} aria-label={title}>
      D
    </span>
  );
}
