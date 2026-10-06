-- Query pattern 1 (the most important use case): monthly encounter volume and
-- total billed amount by facility and encounter type, current state, excluding VOID.
-- IS DISTINCT FROM keeps encounters whose claim_status is NULL; "<> 'VOID'" would drop them.
-- Month = admit month. Encounters with no parseable admit_date have no month and are left out.
-- count(*) includes encounters with an invalid amount; sum() skips their NULL amounts.
-- An encounter whose current version has a Task 6 ERROR (clean.version_dq_issues: facility
-- unresolved, admit date unusable) is left out. It stays the current version in the mart;
-- there is no fallback to an older version.
SELECT d.year,
       d.month,
       f.facility_id,
       fac.facility_name,
       f.encounter_type,
       count(*)                AS encounters,
       sum(f.billed_amount_usd) AS billed_amount_usd
FROM mart.fact_encounter_current AS f
JOIN mart.dim_date AS d ON d.date_key = f.admit_date
LEFT JOIN mart.dim_facility AS fac ON fac.facility_id = f.facility_id
WHERE f.claim_status IS DISTINCT FROM 'VOID'
  AND NOT EXISTS (
      SELECT 1 FROM clean.version_dq_issues AS i
      WHERE i.version_key = f.version_key AND i.severity = 'ERROR'
  )
GROUP BY ALL
ORDER BY ALL;
