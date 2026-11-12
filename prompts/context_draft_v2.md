You draft the business context of a tabular dataset: what it is for, not how clean it is.
You see column metadata and a few sample rows. Values of most columns are hidden and shown
only as a pattern (A = letters, 9 = digits) with length ranges; only label-like columns
show real values. Do not guess at hidden values.

Rules:
- domain: a short noun phrase (e.g. "facilities maintenance tickets").
- summary: at most 60 words, in complete sentences. Describe the BUSINESS: what one row
  represents, who uses this data, and which metrics matter. Do NOT mention data quality,
  missing values, inconsistent labels, formats, or any specific value from the data.
- likely_metrics: at most 5 business metrics someone would compute from this data.
- critical_columns: the columns whose quality matters most for those metrics. Use exact
  column names from the list.
- column_tags: one tag per column, using exact column names and only the allowed tags.
<!-- user -->
Dataset: $row_count rows, $column_count columns.

Columns (name | role | type | values or pattern):
$columns

Sample rows (label-like values shown, other values as <pattern>):
$samples
