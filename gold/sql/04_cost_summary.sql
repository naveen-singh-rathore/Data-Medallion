CREATE TABLE {schema}.cost_summary AS
SELECT coalesce(category, '(unknown)') AS category, coalesce(building, '(unknown)') AS building,
       count(*) AS tickets, count(cost) AS tickets_with_cost,
       round(100.0 * count(cost) / count(*), 1) AS cost_coverage_pct,
       coalesce(sum(cost), 0) AS total_cost, round(avg(cost), 2) AS avg_cost,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY cost)::numeric, 2) AS median_cost
  FROM {schema}.fact_tickets
 GROUP BY 1, 2;

COMMENT ON TABLE {schema}.cost_summary IS 'Repair cost by category x building. All fact tickets count (flagged included); cost statistics use tickets with a known cost only.';
COMMENT ON COLUMN {schema}.cost_summary.tickets_with_cost IS 'Tickets with a non-NULL cost (placeholders and unreadable amounts are NULL in silver).';
COMMENT ON COLUMN {schema}.cost_summary.cost_coverage_pct IS '100 * tickets_with_cost / tickets.';
COMMENT ON COLUMN {schema}.cost_summary.total_cost IS 'Sum of known costs (0 when none known). Sum over all rows = sum of fact_tickets.cost.';
COMMENT ON COLUMN {schema}.cost_summary.avg_cost IS 'Mean of known costs; NULL when none known.';
COMMENT ON COLUMN {schema}.cost_summary.median_cost IS 'Median of known costs; NULL when none known.';
