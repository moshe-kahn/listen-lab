type LikedBadgeProps = {
  className?: string;
  title?: string;
};

export function LikedBadge({ className = "", title = "Liked" }: LikedBadgeProps) {
  return (
    <span className={`liked-badge${className ? ` ${className}` : ""}`} title={title} aria-label={title}>
      ★
    </span>
  );
}
