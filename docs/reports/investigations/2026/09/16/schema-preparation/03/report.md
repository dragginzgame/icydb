# Cumulative per-entity projection admission

2026-09-16 · 0.257.20 · one existing allowance, no new runtime budget.

## Outcome

All direct and nested field projections in one entity now share the existing
512-KiB logical allowance (128 bytes per visited kind plus copied relation text).
The allowance resets between entities, never between fields. Semantic fallback
ends only that field's projection; later fields still require admission.
Candidate validation returns the existing Unsupported class and persisted
reconstruction maps rejection to Corruption. No format/API/cache mode is added.

This tightens schema validity: oversized entities must be simplified and their
databases recreated. It does not make accepted cold construction faster, bound
database-wide setup, or establish exact heap/instruction limits. Resolver scratch,
other semantic validation and all-entity runtime-root construction remain open.

## IC qualification

These measurements call the actual production projection-admission helpers with
the old per-field or new per-entity traversal. They exclude fixture construction,
bundle validation/encoding, metadata construction and query execution.
All 114 calls (19 fixtures × 3 repeats × 2 versions) completed with exact
within-version repeat parity. All six scalar fixture shapes and both one-field
direct expansion shapes remain admitted; the other eleven shapes now reject.
Every returned error is checked to be Unsupported.

Fields below exclude the scalar primary key.

| Fixture | Old instructions | New instructions | New outcome |
| --- | ---: | ---: | --- |
| 16 scalar fields | 3,284 | 4,128 | Admit |
| 64 scalar fields | 11,828 | 14,928 | Admit |
| 254 scalar fields | 45,648 | 57,678 | Admit |
| 1 direct field, depth 8 | 367,171 | 367,310 | Admit |
| 1 direct field, depth 10 | 1,469,961 | 1,470,100 | Admit |
| 64 direct fields, depth 10 | 94,048,083 | 1,471,131 | Reject |
| 254 direct fields, depth 10 | 373,252,004 | 1,471,192 | Reject |
| 16 records, depth 8, width 16 | 93,888,451 | 1,471,024 | Reject |
| 8 records, depth 10, width 16 | 202,446,411 | 1,583,077 | Reject |

The largest scalar fixture adds 12,030 instructions (about 26%) to this isolated
check. Rejected shapes stop early; that is not an equivalent-query speedup.
These samples are not universal instruction ceilings. Existing warm accepted
authority reuse does not acquire a new projection check.

| Isolated artifact | Raw Wasm bytes | Defined functions |
| --- | ---: | ---: |
| Per-field baseline | 205,855 | 534 |
| Per-entity candidate | 205,872 | 534 |
| Delta | +17 | 0 |

Production-actor Wasm and whole-query cycle/instruction deltas remain unmeasured.
Two isolated PocketIC instances were used; shared local networks were untouched.

## Reproduction

Reuse the fixtures and raw query export from [probe 01](../01/probe.rs.txt).
Remove bundle/source-binding construction from fixture(), returning only the
AcceptedSchemaSnapshot and AcceptedValueCatalogHandle; remove their unused imports.
Do not validate a bundle during fixture assembly: the oversized cases are the
input being rejected by the measured operation. Replace run() with the relevant
version in [probe-run.rs.txt](probe-run.rs.txt). No surrogate expansion walker
is used. [samples.csv](samples.csv) retains every call.

Reuse [runner 01](../01/runner.rs.txt) with two returned u64 values
(instructions, admitted) and the matching header. Both builds use Rust 1.98.1,
SQL disabled, wasm-release (z, fat LTO, one codegen unit, panic abort), no wasm-opt,
and PocketIC server 16.0.0. Build core as a wasm32-unknown-unknown cdylib in an
isolated source copy, as described in probe 01.

Baseline uses the per-kind admission worktree on HEAD
38d4067a04a2dc11421fc91fa06fa077d9732840. Candidate changes schema
mod.rs, types.rs, types/query_projection.rs and enum_catalog/publication.rs.
The prior shared-schema handoff is not reachable from this probe. Both versions
have identical fixture and runner code; only the measured handoff and production
admission implementation differ. No instrumentation is installed in production.

Wasm SHA-256:
- Baseline: 86f8db567fb779b9e656ae0ab563715bcdbfec0288d67d108787883f453b0f5e
- Candidate: b8bb626eab643e84654a5543a5fa7a16b5cf3e14ef461c7a495cf4fec34b12a3

## Validation

All 75 focused schema tests pass with all features and again without default
features (150 runs). Coverage includes exact node/text limits, direct/nested/mixed
cumulative rejection, candidate and persisted round trips, opaque-field fallback,
255 ordinary scalar fields, and two entities each using the full allowance.
Repository Clippy, focused all-feature library/test Clippy, formatting, diff
checks and layer/schema authority invariants pass. The first focused Clippy run
found a redundant closure; it was fixed before the repository and focused reruns.
Full release tests remain user-owned.
