-- Query pattern 3: for a row of the export, the source batch, file and row number that
-- supplied its current version, with the batch audit of that file.
-- Parameter: $encounter_key. A user with access to the restricted raw layer can continue to
-- raw.encounters on (source_batch_id, source_file_name, source_row_number).
SELECT f.encounter_key,
       f.source_system,
       f.source_record_id,
       f.source_batch_id,
       f.source_file_name,
       f.source_row_number,
       a.status         AS batch_status,
       a.received_count AS file_received_count
FROM mart.fact_encounter_current AS f
JOIN ops.batch_audit AS a ON a.batch_id = f.source_batch_id AND a.file_name = f.source_file_name
WHERE f.encounter_key = $encounter_key;
