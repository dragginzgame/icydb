> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Shared snapshot payload — isolated ownership preflight

Date: 2026-09-17. Baseline: current 0.257.21 candidate (shared accepted
selection plus borrowed runtime fingerprinting), not published 0.257.20.
The original preflight is recorded below; production landing confirmation
appears at the end of this receipt.

## Verdict

Recommend landing the shared payload at its existing snapshot owner. In this
fixture it removes retained duplication, reduces allocated Wasm memory and
shrinks raw Wasm. Five of six complete queries use fewer instructions; grouped
count is slightly higher. All paired warm updates use fewer charged cycles.
This is a small ownership change, not a new cache or execution flow.

The [prototype patch](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/17/catalog-selection/06/prototype.patch) wraps the existing persisted snapshot
fields in one private `Rc` payload. Existing builders detach with
`Rc::make_mut` before mutation; accepted wrappers and decoded bundles share
unchanged payloads. Validation, encoding, runtime roots and normalized hash
ownership remain unchanged. There is no interior mutability or per-field
reference graph. Two cache-identity constructors lose `const` because snapshot
access now dereferences the shared payload.

Demonstrated need: the bundle-to-selection deep copy documented in
[receipt 05](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/17/catalog-selection/05/report.md). Simplest alternative: keep that copy. Canonical
owner: `PersistedSchemaSnapshot`. State-space delta: one private storage type,
no new mode, cache, configuration, authority, public API or persisted format.
The prototype changes three files: approximately +77 net lines including a
38-line test; production adds 39 lines across two files. Read-side ownership
is simpler; schema edits gain explicit standard-library copy-on-write handling.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/17/catalog-selection/06/report.md).
