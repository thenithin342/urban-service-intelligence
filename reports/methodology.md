# Methodology (Phase 0/1)

ACQUISITION: SODA paged GET with $select/$where/$order/$limit/$offset,
retries with exponential backoff, dedupe by unique_key, CSV + manifest
(URLs, filters, UTC time, counts, SHA256, source rowsUpdatedAt).

PROFILING: pandas; null rates, duplicate rate, date coverage, borough
distribution, invalid coords (NYC bbox flag), closed<created flags.

EDA OUTPUTS: top_categories.csv, by_borough.csv, volume_daily.csv,
profile.json. Deterministic: same input -> byte-identical outputs.

LIMITS: offset paging can drift if dataset changes; mitigated with fixed
historical window + dedupe + manifest reconciliation. Status is a snapshot,
not historical state. Volume != risk. Closure analysis deferred to Phase 6.
