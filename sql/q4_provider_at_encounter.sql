-- Query pattern 4: the attending provider's specialty and employment status at the time of
-- each encounter. provider_sk was resolved point-in-time on admit_date
-- (dim_provider.valid_from <= admit_date < valid_to); when there is no provider,
-- provider_sk_reason says why.
SELECT f.encounter_key,
       f.admit_date,
       f.attending_npi,
       p.specialty,
       p.employment_status,
       p.valid_from,
       p.valid_to,
       f.provider_sk_reason
FROM mart.fact_encounter_current AS f
LEFT JOIN mart.dim_provider AS p ON p.provider_sk = f.provider_sk
ORDER BY f.encounter_key;
