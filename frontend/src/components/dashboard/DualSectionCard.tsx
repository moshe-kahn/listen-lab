import type { ReactNode } from "react";
import type { PreviewItem, SectionKey } from "../../types/appTypes";

type DualSectionCardProps = {
  title: ReactNode;
  section: SectionKey;
  anchorId: string;
  leftTitle: ReactNode;
  rightTitle: ReactNode;
  leftContent: ReactNode;
  rightContent: ReactNode;
  previewItemsLeft: PreviewItem[];
  previewItemsRight: PreviewItem[];
  collapsedPreviewItems?: PreviewItem[];
  isOpen: boolean;
  toggleSection: (section: SectionKey, anchorId?: string) => void;
  renderPreviewCard: (item: PreviewItem, key: string) => ReactNode;
};

export function DualSectionCard({
  title,
  section,
  anchorId,
  leftTitle,
  rightTitle,
  leftContent,
  rightContent,
  previewItemsLeft,
  previewItemsRight,
  collapsedPreviewItems,
  isOpen,
  toggleSection,
  renderPreviewCard,
}: DualSectionCardProps) {
  return (
    <section className="info-card info-card-wide" id={anchorId}>
      <button className="section-toggle section-toggle-header" onClick={() => toggleSection(section, anchorId)} type="button">
        <h2>{title}</h2>
      </button>
      {isOpen ? (
        <div className="artists-grid">
          <div className="artists-column">
            {typeof leftTitle === "string" ? <h3>{leftTitle}</h3> : leftTitle}
            {leftContent}
          </div>
          <div className="artists-column">
            {typeof rightTitle === "string" ? <h3>{rightTitle}</h3> : rightTitle}
            {rightContent}
          </div>
        </div>
      ) : (
        <div className="preview-strip">
          {(collapsedPreviewItems ?? previewItemsLeft.concat(previewItemsRight)).slice(0, 5).map((item, index) =>
            renderPreviewCard(item, `${String(typeof title === "string" ? title : "section")}-${item.image}-${index}`),
          )}
        </div>
      )}
      <button className="section-toggle section-toggle-footer" onClick={() => toggleSection(section, anchorId)} type="button">
        <span>{isOpen ? "^" : "v"}</span>
      </button>
    </section>
  );
}
