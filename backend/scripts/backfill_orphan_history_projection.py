from __future__ import annotations

import json

from backend.app.play_event_projector import (
    audit_eligible_unlinked_history_rows,
    reconcile_fact_play_events_for_ingest_run,
)


def main() -> None:
    before = audit_eligible_unlinked_history_rows()
    projection = reconcile_fact_play_events_for_ingest_run(
        source_type="export",
        run_id="manual-orphan-history-backfill",
    )
    after = audit_eligible_unlinked_history_rows()
    print(json.dumps({"before": before, "projection": projection, "after": after}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
