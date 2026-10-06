-- Query pattern 6: everything quarantined or rejected, with reason codes.
--   FILE  a file of a rejected batch (nothing from it was loaded)
--   ROW   a row of an accepted batch that could not be placed in version order
-- Task 6 adds version-level data-quality issues to this list.
SELECT 'FILE'              AS level,
       batch_id,
       file_name,
       NULL::INTEGER       AS source_row_number,
       reason              AS reason_codes
FROM ops.batch_audit
WHERE status = 'REJECTED'
UNION ALL
SELECT 'ROW',
       batch_id,
       file_name,
       source_row_number,
       outcome_reason
FROM clean.encounter_row_outcomes
WHERE outcome = 'QUARANTINED'
ORDER BY batch_id, file_name, source_row_number NULLS FIRST;
