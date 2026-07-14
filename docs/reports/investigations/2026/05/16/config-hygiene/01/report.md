# 0.156 Config Hygiene Audit

Date: 2026-05-16

Scope:

- `icydb.toml` parser/build boundary
- generated `__icydb_*` endpoint naming
- CLI config preflight for SQL, fixtures, snapshot, and metrics
- demo/test wasm dependency graph for TOML parser leakage
- 0.156 user-facing docs and changelog examples

## Result

PASS with one documentation drift fix and one focused diagnostic coverage
addition.

## Findings

| Area | Result | Notes |
| --- | --- | --- |
| Canister runtime boundary | PASS | Demo and SQL test wasm normal dependency graphs do not include `toml` or `icydb-config-build`. |
| Config authority | PASS | Build scripts read `icydb.toml`, emit typed booleans, and pass them through `icydb-build` actor options. Accepted schema remains runtime authority. |
| CLI target selection | PASS | SQL, observability, and lifecycle commands still require explicit `--canister`; config does not own canister IDs or principal mapping. |
| CLI preflight | PASS | SQL readonly/DDL, snapshot, metrics, and metrics reset calls now check the matching config switch before replica calls. Refresh checks `fixtures` before deciding whether to call `__icydb_fixtures_load`. |
| Endpoint naming | PASS | Active generated endpoints use fixed `__icydb_*` function names. CLI method names and config switches are paired in one `ConfiguredEndpoint` metadata set, so new CLI-generated endpoint calls have one place to add preflight coverage. No active admin SQL, dynamic SQL dispatch, or endpoint-name override shims were found outside historical changelog text. |
| Documentation | FIXED | The 0.156 design still described CLI parsing as inspection-only. It now allows command-runtime parsing for local endpoint preflight diagnostics while keeping generated actor runtime TOML-free. |

## Follow-Up

- Keep any future config keys build-time and endpoint-surface scoped unless a
  separate runtime-config design is approved.
- If a future CLI command calls another generated endpoint family, add the
  method/config-surface pair to the central configured endpoint metadata before
  adding the replica call.
