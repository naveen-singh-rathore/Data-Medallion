-- Safe casts and helpers used by compiled silver SQL. They return NULL instead of raising,
-- so one bad value never aborts a build. Python twins: pipeline/profile_values.py
-- (normalization_key), pipeline/profile_columns.py (mask), pipeline/profile_dates.py (formats).

CREATE OR REPLACE FUNCTION silver.try_numeric(v text) RETURNS numeric
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE WHEN v ~ '^\s*[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d{1,3})?\s*$' THEN btrim(v)::numeric END
$$;
COMMENT ON FUNCTION silver.try_numeric(text) IS 'text -> numeric, NULL when not a plain number.';

CREATE OR REPLACE FUNCTION silver.try_int(v text) RETURNS bigint
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE WHEN v ~ '^\s*[+-]?\d{1,18}\s*$' THEN btrim(v)::bigint END
$$;
COMMENT ON FUNCTION silver.try_int(text) IS 'text -> bigint, NULL when not a plain integer.';

CREATE OR REPLACE FUNCTION silver.try_bool(v text, true_vals text[], false_vals text[]) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN lower(btrim(v)) = ANY (SELECT lower(t) FROM unnest(true_vals) t) THEN true
        WHEN lower(btrim(v)) = ANY (SELECT lower(f) FROM unnest(false_vals) f) THEN false
    END
$$;
COMMENT ON FUNCTION silver.try_bool(text, text[], text[]) IS 'text -> boolean using explicit true/false token lists.';

CREATE OR REPLACE FUNCTION silver.digits_key(v text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT lower(regexp_replace(btrim(v), '(^|[^0-9])0+([0-9])', '\1\2', 'g'))
$$;
COMMENT ON FUNCTION silver.digits_key(text) IS 'Lowercase with leading zeros of each number removed (date round-trip comparison).';

-- fmt is a to_timestamp pattern, or EPOCH / EPOCH_MS / EXCEL. The numeric ranges mirror
-- PROFILE_CONFIG.epoch_range and excel_serial_range (a test keeps them in sync).
CREATE OR REPLACE FUNCTION silver.try_ts(v text, regex text, fmt text) RETURNS timestamp
LANGUAGE plpgsql STABLE PARALLEL UNSAFE SET timezone = 'UTC' AS $$  -- exception blocks need subtransactions: no parallel workers
DECLARE
    n numeric;
    r timestamp;
BEGIN
    IF v IS NULL OR v !~ ('^(?:' || regex || ')$') THEN
        RETURN NULL;
    END IF;
    IF fmt IN ('EPOCH', 'EPOCH_MS', 'EXCEL') THEN
        n := v::numeric;
        IF fmt = 'EPOCH_MS' THEN n := n / 1000; END IF;
        IF fmt = 'EXCEL' THEN
            RETURN CASE WHEN n BETWEEN 20000 AND 60000 THEN timestamp '1899-12-30' + make_interval(days => n::int) END;
        END IF;
        RETURN CASE WHEN n BETWEEN 631152000 AND 4102444800 THEN to_timestamp(n)::timestamp END;
    END IF;
    r := to_timestamp(v, fmt)::timestamp;
    IF r < timestamp '0001-01-01' THEN
        RETURN NULL;
    END IF;
    -- to_timestamp is lenient (month 00 -> January, day 00 -> 1st); Python's strptime is not.
    -- Round-trip: formatting the result back must give the input (ignoring leading zeros and
    -- case). Offsets and fractions can't round-trip, so those formats skip this check.
    IF fmt !~ 'TZH|US' AND silver.digits_key(to_char(r, fmt)) <> silver.digits_key(v) THEN
        RETURN NULL;
    END IF;
    RETURN r;
EXCEPTION WHEN others THEN
    RETURN NULL;  -- impossible dates (Feb 30, month 13) and anything else unparseable
END
$$;
COMMENT ON FUNCTION silver.try_ts(text, text, text) IS 'text -> timestamp (UTC) when it matches regex and parses with fmt; else NULL.';

CREATE OR REPLACE FUNCTION silver.norm_key(v text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE WHEN k ~ '[[:alpha:]]' THEN regexp_replace(k, '[[:space:]\-_./\\]+', '', 'g') ELSE btrim(k) END
      FROM (SELECT lower(regexp_replace(normalize(v, NFKD), '[̀-ͯ]', '', 'g')) AS k) s
$$;
COMMENT ON FUNCTION silver.norm_key(text) IS 'Normalization key: accents stripped, lowercased, separators removed when the value has letters.';

CREATE OR REPLACE FUNCTION silver.pattern_mask(v text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT left(regexp_replace(regexp_replace(v, '[[:alpha:]]+', 'A', 'g'), '[[:digit:]]', '9', 'g'), 40)
$$;
COMMENT ON FUNCTION silver.pattern_mask(text) IS 'Format signature: letter runs -> A, each digit -> 9 (first 40 chars).';

-- Step helpers: each takes the value once, so compiled expressions never repeat their input.
CREATE OR REPLACE FUNCTION silver.blank_to_null(v text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE WHEN v ~ '^[[:space:] ]*$' THEN NULL ELSE v END
$$;

CREATE OR REPLACE FUNCTION silver.strip_thousands(v text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE WHEN v ~ '^[^0-9]*[+-]?\d{1,3}(,\d{3})+(\.\d+)?[^0-9]*$' THEN replace(v, ',', '') ELSE v END
$$;
COMMENT ON FUNCTION silver.strip_thousands(text) IS '1,234,567.89 -> 1234567.89; anything else unchanged.';

CREATE OR REPLACE FUNCTION silver.european_decimal(v text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE WHEN v ~ '^[^0-9]*[+-]?(\d{1,3}(\.\d{3})+,\d+|\d+,\d{1,2})[^0-9]*$'
                THEN replace(replace(v, '.', ''), ',', '.') ELSE v END
$$;
COMMENT ON FUNCTION silver.european_decimal(text) IS '1.234,56 -> 1234.56 and 3,50 -> 3.50; anything else unchanged.';

CREATE OR REPLACE FUNCTION silver.flags(VARIADIC f text[]) RETURNS jsonb
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE WHEN count(x) = 0 THEN NULL ELSE jsonb_agg(x) END FROM unnest(f) AS x WHERE x IS NOT NULL
$$;
COMMENT ON FUNCTION silver.flags(text[]) IS 'Non-null flags as a jsonb array, NULL when there are none.';

-- EXECUTE for pipeline_ro is granted in 008 (the role is created there).
