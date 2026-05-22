type ReleaseSiblingBadgeProps = {
  className?: string;
  sourceCount?: number | null;
};

export function ReleaseSiblingBadge({ className = "", sourceCount = null }: ReleaseSiblingBadgeProps) {
  const title = sourceCount && sourceCount > 1
    ? `Grouped release track: ${sourceCount} source tracks`
    : "Grouped release track";
  return (
    <span aria-label={title} className={`release-sibling-badge ${className}`.trim()} title={title}>
      RT
    </span>
  );
}
