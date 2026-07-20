BRONZE:
Bronze (gharchive.bronze_dev.events): one row per GitHub event, all 5 event types mixed together, payload untouched as raw string
Gold: grain per mart — TBD

Partitioning strategy — event_date. This scopes replaceWhere to a single day's folder, so reruns only touch and rewrite one partition instead of scanning or rewriting the full table. Matches the per-date loop structure used to backfill all 7 days.

Idempotency mechanism — replaceWhere partition-overwrite. On rerun, the target partition's existing data is deleted and replaced entirely by the new write — not merged or appended. This is what makes reruns safe: running the same write multiple times produces the same row count each time, instead of duplicating rows. MERGE and control tables were rejected as unnecessary complexity for append-only source data.

MERGE solves row-level updates on mutable data — I don't have that here, GitHub events are immutable historical facts. replaceWhere gives me idempotent reruns with much simpler logic, matched to data that's append-only by nature. Using MERGE would have been solving a problem I didn't have.

Known data gaps — 2020-06-10 (14/24 hours, confirmed source 404), 2020-06-13/06-14 show lower row counts than weekdays (~1.8M vs ~2.4–3.4M). File counts confirmed 24/24 for both — not a download gap. Consistent with reduced GitHub activity on weekends. No action needed.

Bronze→Silver boundary — payload flattening happens in Silver, not Bronze, because the 5 event types have structurally different payload shapes (e.g. PushEvent has no action field, WatchEvent/IssuesEvent do). Flattening in Bronze would force one schema onto all event types, silently nulling fields that don't apply. Bronze keeps payload as an untouched raw string so no type-specific schema decision is made prematurely; Silver splits by event type and flattens each shape correctly.

SILVER:
Silver: one row per event, per event type (5 separate outputs — PushEvent, PullRequestEvent, IssuesEvent, WatchEvent, ForkEvent), payload flattened

Read pattern: loop per date, write Silver partitioned by event_date, replaceWhere per date — mirroring Bronze exactly.