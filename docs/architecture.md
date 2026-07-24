# ARCHITECTURE.md — databricks_gharchive

## BRONZE

Bronze (gharchive.bronze_dev.events): one row per GitHub event, all 5 event types mixed together, payload untouched as raw string.

**Partitioning strategy** — event_date. This scopes replaceWhere to a single day's folder, so reruns only touch and rewrite one partition instead of scanning or rewriting the full table. Matches the per-date loop structure used to backfill all 7 days.

**Idempotency mechanism** — replaceWhere partition-overwrite. On rerun, the target partition's existing data is deleted and replaced entirely by the new write — not merged or appended. This is what makes reruns safe: running the same write multiple times produces the same row count each time, instead of duplicating rows. MERGE and control tables were rejected as unnecessary complexity for append-only source data.

**Why not MERGE** — MERGE solves row-level updates on mutable data. GitHub events are immutable historical facts, so that problem doesn't exist here. replaceWhere gives idempotent reruns with much simpler logic, matched to data that's append-only by nature. Using MERGE would have been solving a problem I didn't have.

**Known data gaps** — 2020-06-10 (14/24 hours, confirmed source 404), 2020-06-13/06-14 show lower row counts than weekdays (~1.8M vs ~2.4–3.4M). File counts confirmed 24/24 for both dates — not a download gap. Consistent with reduced GitHub activity on weekends. No action needed.

**Bronze→Silver boundary** — payload flattening happens in Silver, not Bronze, because the 5 event types have structurally different payload shapes (e.g. PushEvent has no action field, WatchEvent/IssuesEvent do). Flattening in Bronze would force one schema onto all event types, silently nulling fields that don't apply. Bronze keeps payload as an untouched raw string so no type-specific schema decision is made prematurely; Silver splits by event type and flattens each shape correctly.

**Read pattern** — `spark.read.text` + `get_json_object`, not `spark.read.json`. The latter infers one StructType across all event types and silently nulls fields that don't match — dangerous for mixed-schema JSON like this.

---

## SILVER

Silver: one row per event, per event type (5 separate outputs — PushEvent, PullRequestEvent, IssuesEvent, WatchEvent, IssueCommentEvent), payload flattened.

**Event type selection** — the locked event-type list was revised during the build: ForkEvent was replaced with IssueCommentEvent. ForkEvent was low-signal for the analysis this project targets — it doesn't reflect developer activity or engagement the way pushes, PRs, issues, and comments do. IssueCommentEvent adds direct value to PR/issue activity analysis. The other 4 types (PushEvent, PullRequestEvent, IssuesEvent, WatchEvent) were unchanged from the original plan.

**Read pattern** — loop per date, write Silver partitioned by event_date, replaceWhere per date — mirroring Bronze exactly.

**repo_id / actor_id gap and fix** — The original Silver select dropped `repo_id` and `actor_id` entirely from all 5 tables — an oversight, not a decision, caught mid-Gold-build. Fixed by adding `repo_id` and `actor_id` back into all 5 Silver tables. `org_id` was deliberately left out — no mart does org-level analysis, so keeping it wasn't justified. All 5 tables were rebuilt and row counts re-verified against Bronze per-type counts to confirm the fix didn't change row totals: push_events 7,590,014 / pull_request_events 1,585,494 / issues_events 432,014 / issue_comment_events 980,921 / watch_events 914,891.

**Silent-fail bug pattern** — this project surfaced the same category of bug three separate times, each one a small mismatch that failed silently instead of erroring loudly:
1.a JSON path typo (`$.acion` instead of `$.action`) in one of the Silver `get_json_object` extractions, which returned null instead of throwing an error.
2. an event-type filter mismatch (`"IssueEvent"` vs the correct `"IssuesEvent"`), which silently matched zero rows instead of failing.
3. **The `silver_gold_tally` singular test** — the test file referenced a source name (`silver_dev.push_events` in one place, `silver.push_events` in another) that didn't match what was declared in `sources.yml`. dbt didn't error — it silently excluded the test from the run. `dbt test` reported "Completed successfully" with a full PASS count that simply never included this test.

The common thread: none of these failed the way you'd want a bug to fail. They didn't throw errors — they quietly returned nulls, zero rows, or an incomplete test count, and everything downstream looked healthy. The fix in each case wasn't just correcting the string — it was verifying output against an independent number (Bronze per-type row counts, manual row inspection, the "found N tests" line in dbt's own output) rather than trusting a clean run status at face value.

---

## GOLD

Gold: 4 marts in the `gold/` dbt project. Grain differs per mart, deliberately — chosen based on what each mart needs to answer, not a fixed convention.

| Mart | Source | Grain | Why |
|---|---|---|---|
| `top_repos_by_stars` | `source('silver','watch_events')` | `repo_id` (no date) | All-time ranking — a date dimension would add nothing since the question is "which repos have the most stars," not "which repos got stars on which day." |
| `daily_events_per_repo` | `source('bronze','events')` | `(repo_id, event_date)` | Sources Bronze, not Silver, because it needs total event volume including ForkEvent — which Silver dropped. This mart's totals are correctly higher than the Silver-sourced marts for that reason, not a bug. |
| `pr_issue_activity` | union of `pull_request_events`, `issues_events`, `issue_comment_events` | `(repo_id, event_date)` | Built as a single grouped union tagged by `source_type`, not a 3-way join — a join risks dropping rows where one event type has no match for a given repo/date, a union does not. |
| `push_events` | `source('silver','push_events')` | `(repo_id, event_date)` | `push_events` (row count) and `commits_pushed` (sum of `push_size`) are kept as separate columns because they answer different questions — how often a repo was pushed to, versus how much code moved. Collapsing them into one number would lose that distinction. |

**star_events naming correction** — `top_repos_by_stars.star_events` was originally going to be named `star_events_7d`, but the underlying query has no date filter — it's a lifetime count. The `_7d` suffix was dropped because it implied a fixed window that doesn't exist in the query. If a rolling 7-day version is wanted later, it needs an actual date filter added, not just a rename.

**Materialization** — all 4 marts use `materialized='table'` (full rebuild on every run). This is correct because the underlying dataset is static and closed (7 fixed days, no new data arriving) — not because the output happens to be small. An incremental model would add complexity with no benefit here, since there's nothing incremental about the source.

---

## TESTING

Schema tests (21 total): `not_null` on all grain and metric columns across all 4 marts; `unique` on `repo_id` for `top_repos_by_stars` (single-column grain); `dbt_utils.unique_combination_of_columns` on `(repo_id, event_date)` for the other 3 marts (compound grain — a single-column `unique` test would not catch a real grain violation here, since `repo_id` is legitimately non-unique across different dates).

Singular tests (2): `prs_merged ≤ pr_events` in `pr_issue_activity` — the only test that catches a logical impossibility, not just a data-quality nuisance. And a Silver-to-Gold reconciliation test on `push_events` — `sum(push_events)` in the mart must equal `count(*)` in `silver.push_events`. This test exists specifically because of the repo_id gap and the two silent-fail bugs above: it's the one check that would have caught a silent aggregation-boundary error before it reached Gold.

Final result: PASS=21, WARN=0, ERROR=0.