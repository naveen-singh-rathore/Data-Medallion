-- One row per usable silver ticket (silver <target>_valid: _status kept or flagged).
CREATE TABLE {schema}.fact_tickets AS
WITH base AS (
    SELECT s.bronze_id, s.{id}::text AS ticket_id, {category_final} AS category, {category_source} AS category_source,
           {priority_value} AS priority, {location_value} AS building, s.{status} AS status,
           s.{opened} AS created_at, s.{closed} AS resolved_at, s.{sla}::numeric AS sla_hours, s.{money}::numeric AS cost,
           coalesce(s.{status} = ANY({resolved_statuses}), false) AS is_resolved
      FROM {silver} s
), timed AS (
    SELECT *, date_trunc('month', created_at)::date AS created_month,
           CASE WHEN created_at IS NOT NULL AND resolved_at IS NOT NULL
                THEN round((extract(epoch FROM resolved_at - created_at) / 3600)::numeric, 2) END AS resolution_hours,
           CASE WHEN NOT is_resolved THEN 'not_resolved'
                WHEN created_at IS NULL THEN 'missing_created_at'
                WHEN resolved_at IS NULL THEN 'missing_resolved_at'
                WHEN resolved_at < created_at THEN 'resolved_before_created' END AS exclusion_reason
      FROM base
)
SELECT ticket_id, category, category_source, priority, building, status, created_at, resolved_at, created_month,
       is_resolved, resolution_hours, sla_hours,
       CASE WHEN exclusion_reason IS NULL AND sla_hours IS NOT NULL THEN resolution_hours <= sla_hours END AS sla_met,
       cost, exclusion_reason IS NULL AS usable_for_time_metrics, exclusion_reason,
       {silver_build_id}::uuid AS silver_build_id, bronze_id
  FROM timed;

ALTER TABLE {schema}.fact_tickets ADD PRIMARY KEY (bronze_id);
COMMENT ON TABLE {schema}.fact_tickets IS 'One row per usable silver ticket (kept + flagged; rejected and duplicate rows are not tickets). Flagged rows are INCLUDED here; time metrics exclude them only through usable_for_time_metrics.';
COMMENT ON COLUMN {schema}.fact_tickets.category IS 'Final category: the rule value when it is a standard value (category_source = rule), else the AI label when confidence >= ai_min_confidence (ai), else NULL (none).';
COMMENT ON COLUMN {schema}.fact_tickets.category_source IS 'rule | ai | none - where the final category came from.';
COMMENT ON COLUMN {schema}.fact_tickets.priority IS 'A standard value of the approved priority rule; values outside its list are NULL (reported as (unknown)), never shown as their own priority.';
COMMENT ON COLUMN {schema}.fact_tickets.created_month IS 'First day of the month of created_at; NULL when created_at is unknown.';
COMMENT ON COLUMN {schema}.fact_tickets.is_resolved IS 'status is one of resolved_statuses (gold/model.yaml). Missing status = not resolved.';
COMMENT ON COLUMN {schema}.fact_tickets.resolution_hours IS 'resolved_at - created_at in hours, whenever both exist (can be negative: see usable_for_time_metrics).';
COMMENT ON COLUMN {schema}.fact_tickets.sla_met IS 'resolution_hours <= sla_hours; NULL unless usable_for_time_metrics and sla_hours is known.';
COMMENT ON COLUMN {schema}.fact_tickets.usable_for_time_metrics IS 'is_resolved AND both dates known AND resolved_at >= created_at. The only rows any time metric uses.';
COMMENT ON COLUMN {schema}.fact_tickets.exclusion_reason IS 'Why the row is not usable for time metrics: not_resolved | missing_created_at | missing_resolved_at | resolved_before_created; NULL when usable.';
COMMENT ON COLUMN {schema}.fact_tickets.silver_build_id IS 'meta.silver_builds row this gold build read.';

CREATE TABLE {schema}.dim_category AS
SELECT category, category_source, count(*) AS tickets FROM {schema}.fact_tickets GROUP BY 1, 2;
COMMENT ON TABLE {schema}.dim_category IS 'Distinct final categories (NULL = unknown) by source, with ticket counts.';
CREATE TABLE {schema}.dim_building AS
SELECT building, count(*) AS tickets FROM {schema}.fact_tickets GROUP BY 1;
COMMENT ON TABLE {schema}.dim_building IS 'Distinct buildings (NULL = unknown) with ticket counts.';
CREATE TABLE {schema}.dim_priority AS
SELECT priority, count(*) AS tickets FROM {schema}.fact_tickets GROUP BY 1;
COMMENT ON TABLE {schema}.dim_priority IS 'Distinct priorities (NULL = unknown) with ticket counts.';
