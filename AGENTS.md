# AGENTS.md - rules for coding agents on this repo

- Work incrementally by phase. Do not claim a phase complete without
  working code + executed verification + saved evidence.
- Use real NYC 311 data; small synthetic fixtures only for tests.
- Never invent metrics, counts, or outcomes. Report measured numbers only.
- Chronological splits only for forecasting; no future leakage.
- Keep snapshot time and event time distinct.
- Notebooks are for EDA; production logic lives in src/ + scripts/.
- Add unit + integration tests per phase; run `python -m pytest -q`.
- Do not commit data/raw, data/processed, artifacts, credentials, or models.
- ASCII text for docs/comments when practical.
- On Windows, respect PYSPARK_PYTHON notes in README and docs/decisions.md.
