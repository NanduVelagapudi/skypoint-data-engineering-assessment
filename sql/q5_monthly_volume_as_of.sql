-- Query pattern 5: the same monthly totals as query 1, as they were known at the end of a batch.
-- Parameter: $as_of_batch, for example 'batch_002'. The current version as of that batch is the
-- latest version first seen in it or earlier; batch ids compare as text (batch_NNN).
-- As in query 1, an encounter whose as-of current version has a Task 6 ERROR is left out, with
-- no fallback to an older version.
WITH known AS (
    SELECT *
    FROM mart.fact_encounter_version
    WHERE first_seen_batch_id <= $as_of_batch
    QUALIFY row_number() OVER (PARTITION BY encounter_key ORDER BY last_updated_ts_utc DESC) = 1
)
SELECT d.year,
       d.month,
       k.facility_id,
       fac.facility_name,
       k.encounter_type,
       count(*)                AS encounters,
       sum(k.billed_amount_usd) AS billed_amount_usd
FROM known AS k
JOIN mart.dim_date AS d ON d.date_key = k.admit_date
LEFT JOIN mart.dim_facility AS fac ON fac.facility_id = k.facility_id
WHERE k.claim_status IS DISTINCT FROM 'VOID'
  AND NOT EXISTS (
      SELECT 1 FROM clean.version_dq_issues AS i
      WHERE i.version_key = k.version_key AND i.severity = 'ERROR'
  )
GROUP BY ALL
ORDER BY ALL;
