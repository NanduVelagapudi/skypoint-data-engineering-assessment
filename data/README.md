# Skypoint Data Engineering assessment - data pack

All data is synthetic. No real patients, providers or facilities.

landing/            weekly batches as delivered by the source systems (process in order)
  batch_00N/manifest.json      files, row counts and SHA-256 per file
  batch_00N/encounters_*.csv   one file per source system
reference/
  source_systems_and_facilities.json   source-system conventions and the facility master
  provider_roster/roster_YYYY-MM-DD.csv provider roster snapshots
  icd10_reference.csv                  ICD-10 codes, category and chronic flag

See the assessment brief for the tasks and deliverables.
