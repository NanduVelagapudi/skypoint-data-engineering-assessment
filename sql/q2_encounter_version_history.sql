-- Query pattern 2: the full version history of one encounter, with the batch each version arrived in.
-- Parameters: $source_system, $source_record_id.
SELECT v.version_key,
       v.last_updated_ts_utc,
       v.first_seen_batch_id         AS arrived_in_batch,
       v.arrival_outcome,
       v.claim_status,
       v.billed_amount_usd,
       v.version_key = c.version_key AS is_current
FROM mart.fact_encounter_version AS v
JOIN mart.fact_encounter_current AS c ON c.encounter_key = v.encounter_key
WHERE v.source_system = $source_system
  AND v.source_record_id = $source_record_id
ORDER BY v.last_updated_ts_utc;
