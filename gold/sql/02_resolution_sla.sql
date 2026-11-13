-- Month (created) x priority x category. Unknown values are grouped as '(unknown)'.
CREATE TABLE {schema}.resolution_sla AS
SELECT created_month AS month, coalesce(priority, '(unknown)') AS priority, coalesce(category, '(unknown)') AS category,
       count(*) AS tickets,
       count(*) FILTER (WHERE is_resolved) AS resolved,
       count(*) FILTER (WHERE usable_for_time_metrics) AS usable,
       count(*) FILTER (WHERE is_resolved AND NOT usable_for_time_metrics) AS excluded_count,
       round(100.0 * count(*) FILTER (WHERE usable_for_time_metrics) / nullif(count(*) FILTER (WHERE is_resolved), 0), 1)
           AS coverage_pct,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY resolution_hours) FILTER (WHERE usable_for_time_metrics)::numeric, 2)
           AS median_resolution_hours,
       round(avg(resolution_hours) FILTER (WHERE usable_for_time_metrics), 2) AS avg_resolution_hours,
       round(percentile_cont(0.9) WITHIN GROUP (ORDER BY resolution_hours) FILTER (WHERE usable_for_time_metrics)::numeric, 2)
           AS p90_resolution_hours,
       count(*) FILTER (WHERE sla_met IS NOT NULL) AS sla_measured,
       round(100.0 * count(*) FILTER (WHERE sla_met) / nullif(count(*) FILTER (WHERE sla_met IS NOT NULL), 0), 1)
           AS sla_met_rate_pct
       {escalation_select}
  FROM {schema}.fact_tickets
 GROUP BY 1, 2, 3;

COMMENT ON TABLE {schema}.resolution_sla IS 'Resolution time and SLA by created month x priority x category. Time metrics use ONLY usable_for_time_metrics rows; flagged rows are included in tickets/resolved and excluded from time metrics when their dates are unusable.';
COMMENT ON COLUMN {schema}.resolution_sla.month IS 'Month of created_at (NULL: created_at unknown).';
COMMENT ON COLUMN {schema}.resolution_sla.tickets IS 'All fact tickets in the group (kept + flagged).';
COMMENT ON COLUMN {schema}.resolution_sla.resolved IS 'Tickets with is_resolved.';
COMMENT ON COLUMN {schema}.resolution_sla.usable IS 'Resolved tickets usable for time metrics (both dates known, resolved_at >= created_at).';
COMMENT ON COLUMN {schema}.resolution_sla.excluded_count IS 'Resolved tickets left out of every time metric (see fact_tickets.exclusion_reason).';
COMMENT ON COLUMN {schema}.resolution_sla.coverage_pct IS '100 * usable / resolved. How much of the resolved work the time metrics describe.';
COMMENT ON COLUMN {schema}.resolution_sla.median_resolution_hours IS 'Median resolution_hours over usable rows. Numerator/denominator: usable rows only.';
COMMENT ON COLUMN {schema}.resolution_sla.avg_resolution_hours IS 'Mean resolution_hours over usable rows.';
COMMENT ON COLUMN {schema}.resolution_sla.p90_resolution_hours IS '90th percentile (continuous) of resolution_hours over usable rows.';
COMMENT ON COLUMN {schema}.resolution_sla.sla_measured IS 'Usable rows with a known sla_hours: the denominator of sla_met_rate_pct.';
COMMENT ON COLUMN {schema}.resolution_sla.sla_met_rate_pct IS '100 * (usable rows with resolution_hours <= sla_hours) / sla_measured.';
