import type { SectionKey } from "../../types/appTypes";

type DashboardPagingProps = {
  section: SectionKey;
  itemCount: number;
  pageSize: number;
  sectionPage: number;
  moveSectionPage: (section: SectionKey, direction: -1 | 1, itemCount: number, pageSize?: number) => void;
};

export function DashboardPaging({
  section,
  itemCount,
  pageSize,
  sectionPage,
  moveSectionPage,
}: DashboardPagingProps) {
  if (itemCount <= pageSize) {
    return null;
  }

  return (
    <div className="section-nav">
      <button
        className="secondary-button"
        disabled={sectionPage === 0}
        onClick={() => moveSectionPage(section, -1, itemCount, pageSize)}
        type="button"
      >
        {"<"}
      </button>
      <span>
        {sectionPage + 1} / {Math.ceil(itemCount / pageSize)}
      </span>
      <button
        className="secondary-button"
        disabled={(sectionPage + 1) * pageSize >= itemCount}
        onClick={() => moveSectionPage(section, 1, itemCount, pageSize)}
        type="button"
      >
        {">"}
      </button>
    </div>
  );
}
