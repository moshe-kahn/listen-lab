type ReleaseSiblingBadgeProps = {
  className?: string;
  sourceCount?: number | null;
};

export function ReleaseSiblingBadge({ className = "", sourceCount = null }: ReleaseSiblingBadgeProps) {
  const title = sourceCount && sourceCount > 1
    ? `Recording group: ${sourceCount} tracks`
    : "Recording group";
  return (
    <span aria-label={title} className={`release-sibling-badge ${className}`.trim()} title={title}>
      R
    </span>
  );
}
