# Contract fixtures (synthetic)

Small JSON files (`{"meta": {...}, "rows": [...]}`) matching the contracts in
`docs/data_contracts.md`. Every value is invented; `meta.is_fixture` is
`true`. They let the dashboard and its tests run before Member 1 forecasts or
Member 2 mining/closure outputs exist. Never report them as results.
