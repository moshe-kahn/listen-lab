import type { PreviewItem } from "../../types/appTypes";

type PreviewCardProps = {
  item: PreviewItem;
  onSelectPreview: (item: PreviewItem) => void;
};

export function PreviewCard({ item, onSelectPreview }: PreviewCardProps) {
  return (
    <button
      className="preview-card"
      onClick={() => onSelectPreview(item)}
      type="button"
    >
      {item.image ? (
        <img alt={item.label} className="preview-thumb" src={item.image} />
      ) : (
        <div className="preview-thumb preview-thumb-fallback" aria-hidden="true">
          {item.fallbackLabel ?? item.label.slice(0, 1).toUpperCase()}
        </div>
      )}
      <span className="preview-overlay">
        <span className="preview-label">{item.label}</span>
        {item.meta ? <span className="preview-meta">{item.meta}</span> : null}
        {item.detail ? <span className="preview-detail">{item.detail}</span> : null}
      </span>
    </button>
  );
}
