-- Query pattern 6: everything quarantined or rejected, with reason codes (ops.quarantine).
--   FILE     a file of a rejected batch: by Task 1 validation or by the publish gate
--            (nothing from it was loaded; row_count = rows received)
--   ROW      a row of an accepted batch that could not be placed in version order (Task 4),
--            or a failing row of a batch the publish gate rejected
--   VERSION  a version with a Task 6 ERROR, at its first arrival; it stays in the history
--            (and is_current_version says whether it is still current) but analytics exclude it
SELECT quarantine_level  AS level,
       quarantine_source AS source,
       batch_id,
       file_name,
       source_row_number,
       source_record_id,
       version_key,
       is_current_version,
       reason_codes,
       reason_detail
FROM ops.quarantine
ORDER BY batch_id, file_name, source_row_number NULLS FIRST, level;
