type ReleaseSiblingBadgeProps = {
  className?: string;
  sourceCount?: number | null;
};

export function ReleaseSiblingBadge({ className = "", sourceCount = null }: ReleaseSiblingBadgeProps) {
  const title = sourceCount && sourceCount > 1
    ? `Recording/family cluster: ${sourceCount} tracks or source versions`
    : "Recording/family cluster";
  return (
    <span aria-label={title} className={`release-sibling-badge ${className}`.trim()} title={title}>
      RT
    </span>
  );
}
