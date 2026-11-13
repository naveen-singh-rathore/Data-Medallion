# Gold metric contracts

Built by `python -m pipeline.cli build-gold` (and automatically after **Finish review** in the UI) from
`silver.records_clean_valid` of the run's current silver build. SQL: `gold/sql/*.sql`; business settings:
`gold/model.yaml`. The same text is on every column in the database (`COMMENT ON COLUMN`).

## Row population and the flagged-row policy

- **Tickets** = silver rows with `_status` `kept` or `flagged`. Rejected and duplicate rows are not tickets.
- **Flagged rows are included** in every count (tickets, resolved, opened, cost). They are left out of
  **time metrics only**, and only when their dates are unusable: a ticket is `usable_for_time_metrics`
  when it is resolved, both dates are known, and `resolved_at >= created_at`.
- **Resolved** = `status` in `resolved_statuses` (`resolved`, `closed`). A missing status is not resolved.
- Missing groups (priority, category, building) are reported as `(unknown)`, never dropped.
- **Category** = the rule value when it is a standard value (`category_source = rule`), else the AI label
  when its confidence is >= 0.6 (`ai`), else unknown (`none`).

## gold.fact_tickets (one row per ticket)

| Column | Definition |
| --- | --- |
| `resolution_hours` | `resolved_at - created_at` in hours, whenever both exist (may be negative; never used then) |
| `sla_met` | `resolution_hours <= sla_hours`; NULL unless usable for time metrics and `sla_hours` known |
| `usable_for_time_metrics` | resolved AND both dates known AND `resolved_at >= created_at` |
| `exclusion_reason` | `not_resolved` / `missing_created_at` / `missing_resolved_at` / `resolved_before_created`; NULL when usable |
| `category_source` | `rule` / `ai` / `none` |

Dimensions `dim_category`, `dim_building`, `dim_priority`: distinct values with ticket counts.

## gold.resolution_sla (created month x priority x category)

| Metric | Numerator | Denominator | Exclusions |
| --- | --- | --- | --- |
| `tickets` | all tickets in the group | - | none |
| `resolved` | resolved tickets | - | - |
| `usable` | resolved tickets usable for time metrics | - | see above |
| `excluded_count` | resolved tickets NOT usable | - | - |
| `coverage_pct` | `usable` | `resolved` | - |
| `median_` / `avg_` / `p90_resolution_hours` | `resolution_hours` of usable rows | usable rows | not usable |
| `sla_met_rate_pct` | usable rows with `sla_met` | `sla_measured` = usable rows with `sla_hours` | not usable, or no SLA |
| `escalation_rate_pct` | tickets whose **current** status is in `escalated_statuses` | `tickets` | none. Built only when such a status exists in the data; a snapshot, since the source has no status history |

## gold.volume_backlog (month x category x building)

| Metric | Definition |
| --- | --- |
| `opened` | tickets created in the month; tickets with unknown `created_at` are in one `month = NULL` row. **Sum = fact rows** |
| `closed` | resolved tickets whose **valid** `resolved_at` (>= `created_at`) is in the month |
| `closed_date_unknown` | resolved tickets created in the month whose closing date is missing or before creation |
| `open_at_month_end` | created on or before month end and not validly closed by then; tickets closed at an unknown time are not in the backlog |

## gold.cost_summary (category x building)

| Metric | Numerator | Denominator |
| --- | --- | --- |
| `tickets_with_cost` | tickets with a known cost (placeholders and unreadable amounts are NULL in silver) | - |
| `cost_coverage_pct` | `tickets_with_cost` | `tickets` |
| `total_cost` | sum of known costs (0 when none). **Sum = sum of fact cost** | - |
| `avg_cost` / `median_cost` | known costs | `tickets_with_cost` |

## gold.data_quality_findings (the business case)

One row per finding that limits the metrics: title, severity, rows affected / denominator, which metric it
affects, impact and recommendation. Produced from the fact table on every build:

- `resolution_dates_unreliable` (or `_out_of_order`): share of tickets resolved before creation, compared
  with random pairing of the same dates and the correlation of the two dates. "Unreliable" means the
  observed share equals random pairing (within 3 points) and |correlation| < 0.1, so even correctly ordered
  durations carry no information.
- `open_tickets_with_resolution_date`, `cost_missing`, `category_recovered_by_ai`, `time_metric_exclusions`.

## Reconciliation (the build fails unless all hold)

- `fact_tickets` rows = silver valid rows
- sum(`volume_backlog.opened`) = sum(`resolution_sla.tickets`) = sum(`cost_summary.tickets`) = fact rows
- sum(`cost_summary.total_cost`) = sum(`fact_tickets.cost`)

Everything is built in `gold_staging` and swapped into `gold` in one transaction; a failed build leaves the
previous gold untouched and is recorded in `meta.gold_builds`.
