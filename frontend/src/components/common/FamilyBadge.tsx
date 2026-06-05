type FamilyBadgeProps = {
  className?: string;
  title?: string;
};

export function FamilyBadge({ className = "", title = "Track family" }: FamilyBadgeProps) {
  return (
    <span className={`family-badge${className ? ` ${className}` : ""}`} title={title} aria-label={title}>
      V
    </span>
  );
}
