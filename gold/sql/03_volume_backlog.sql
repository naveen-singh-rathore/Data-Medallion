-- Month x category x building. A closure counts only when its date is valid (resolved_at >= created_at);
-- resolved tickets with an unusable closing date are counted in closed_date_unknown (in their created
-- month) and are not part of the backlog after that.
CREATE TABLE {schema}.volume_backlog AS
WITH f AS (
    SELECT coalesce(category, '(unknown)') AS category, coalesce(building, '(unknown)') AS building, created_month,
           date_trunc('month', resolved_at)::date AS resolved_month, is_resolved,
           (is_resolved AND usable_for_time_metrics) AS closed_known
      FROM {schema}.fact_tickets
), months AS (
    SELECT generate_series(min(m), max(m), interval '1 month')::date AS month
      FROM (SELECT created_month AS m FROM f UNION ALL SELECT resolved_month FROM f WHERE closed_known) x
), combos AS (SELECT DISTINCT category, building FROM f),
grid AS (SELECT month, category, building FROM months CROSS JOIN combos),
opened AS (SELECT created_month AS month, category, building, count(*) AS n FROM f GROUP BY 1, 2, 3),
closed AS (SELECT resolved_month AS month, category, building, count(*) AS n FROM f WHERE closed_known GROUP BY 1, 2, 3),
unknown AS (SELECT created_month AS month, category, building, count(*) AS n FROM f
             WHERE is_resolved AND NOT closed_known GROUP BY 1, 2, 3),
backlog_in AS (SELECT created_month AS month, category, building, count(*) AS n FROM f
                WHERE created_month IS NOT NULL AND NOT (is_resolved AND NOT closed_known) GROUP BY 1, 2, 3),
monthly AS (
    SELECT g.month, g.category, g.building, coalesce(o.n, 0) AS opened, coalesce(c.n, 0) AS closed,
           coalesce(u.n, 0) AS closed_date_unknown,
           sum(coalesce(b.n, 0) - coalesce(c.n, 0)) OVER (PARTITION BY g.category, g.building ORDER BY g.month)
               AS open_at_month_end
      FROM grid g
      LEFT JOIN opened o ON o.month = g.month AND o.category = g.category AND o.building = g.building
      LEFT JOIN closed c ON c.month = g.month AND c.category = g.category AND c.building = g.building
      LEFT JOIN unknown u ON u.month = g.month AND u.category = g.category AND u.building = g.building
      LEFT JOIN backlog_in b ON b.month = g.month AND b.category = g.category AND b.building = g.building
)
SELECT month, category, building, opened, closed, closed_date_unknown, open_at_month_end::integer AS open_at_month_end
  FROM monthly WHERE opened + closed + closed_date_unknown + open_at_month_end > 0
UNION ALL  -- tickets whose created_at is unknown: counted as opened in a NULL month, never in the backlog
SELECT NULL, category, building, count(*), 0, count(*) FILTER (WHERE is_resolved AND NOT closed_known), NULL
  FROM f WHERE created_month IS NULL GROUP BY 2, 3;

COMMENT ON TABLE {schema}.volume_backlog IS 'Tickets opened and closed per month x category x building, and the open backlog at month end. All fact tickets count as opened (flagged rows included); only valid closing dates move the backlog.';
COMMENT ON COLUMN {schema}.volume_backlog.month IS 'Calendar month; NULL row = tickets whose created_at is unknown (opened only).';
COMMENT ON COLUMN {schema}.volume_backlog.opened IS 'Tickets created in the month. Sum over all rows = fact_tickets rows.';
COMMENT ON COLUMN {schema}.volume_backlog.closed IS 'Resolved tickets whose valid resolved_at falls in the month.';
COMMENT ON COLUMN {schema}.volume_backlog.closed_date_unknown IS 'Resolved tickets created in the month whose closing date is missing or before creation: closed at an unknown time.';
COMMENT ON COLUMN {schema}.volume_backlog.open_at_month_end IS 'Tickets created on or before month end that are not validly closed by then, excluding tickets closed at an unknown time.';
