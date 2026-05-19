# MODULAR AUDIT - Complexity / Surface Hygiene

`icydb-core` first; facade, build, config, CLI, and schema crates only where they
own reachable runtime surface or generated-code wiring.

## Audit Name

Use **Complexity / Surface Hygiene**, not **Code Hygiene**.

Reason: `code hygiene` already means formatting, imports, docs, ordering, and
small readability standards in `docs/governance/code-hygiene.md`. This audit is
narrower and more consequential: remove dead or stale exposed surface, collapse
unnecessary complexity lanes, and verify that remaining surface has a current
IcyDB authority reason.

Recommended slug: `complexity-surface-hygiene`.

## Purpose

Find code that can be deleted, narrowed, or retired because it no longer has a
current role in IcyDB's runtime architecture.

This audit targets:

* dead public or hidden surface
* stale compatibility paths
* generated-model runtime fallbacks
* legacy shims after catalog-native schema acceptance
* orphaned helpers, DTOs, cfg branches, and diagnostics hooks
* complexity that exists only to preserve an obsolete route, format, or caller

This is NOT:

* a style audit
* a general DRY audit
* a correctness audit
* a performance audit
* a module-boundary audit unless exposure creates dead surface
* a redesign proposal exercise

## IcyDB Authority Rules

Apply these before classifying any finding.

* Accepted schema snapshots are runtime authority.
* Generated `EntityModel` / `IndexModel` are allowed only for proposal,
  reconciliation, model-only convenience, and tests.
* Do not preserve runtime fallback reconstruction from generated models.
* Schema mutation work stays catalog-native; SQL DDL is a frontend, not the
  source of mutation semantics.
* Generated canister endpoints use verbatim `__icydb_*` Rust/export names with
  no endpoint `name = ...` override.
* Before `1.0.0`, internal protocols and formats should hard-cut to the latest
  version instead of keeping multi-version compatibility fallbacks.
* For wasm-related deletion decisions, raw non-gzipped `.wasm` bytes are the
  primary size signal; gzip is secondary context.

## Scope

Default in-scope roots:

* `crates/icydb-core/src`
* `crates/icydb/src`
* `crates/icydb-build/src`
* `crates/icydb-config-build/src`
* `crates/icydb-cli/src`
* `crates/icydb-schema/src`
* `crates/icydb-schema-derive/src`

Default exclusions:

* historical docs and changelogs
* generated build output
* target artifacts
* tests, except when they are the only consumer of production surface
* examples, except when they explain why facade surface must remain stable

Test-only code is not dead just because production code does not call it. Mark
it separately as `test-only retained`, `test-only stale`, or `test-only masking
production dead surface`.

## Method Contract

Include this manifest in each report:

* `method_version = CSH-1.0`
* `surface_taxonomy = ST-1`
* `authority_taxonomy = AT-1`
* `deletion_confidence_model = DC-1`
* `compatibility_policy = pre-1.0-hard-cut`
* `wasm_signal_rule = raw-wasm-primary`

Mark the run `non-comparable` if any manifest item changes, if in-scope roots
change, or if test/generated-code inclusion rules change.

## Evidence Classes

Column classes:

* `[M]` Mechanical: direct code, compiler, or generated artifact signal.
* `[C]` Classified: analyst judgment over inspected code.
* `[D]` Derived: formula over mechanical fields.

Evidence modes:

* `mechanical`: generated from commands or compiler output.
* `semi-mechanical`: mechanical seed plus inspected context.
* `classified`: judgment from code reading.

Mention counts are weak signals. Do not classify a surface as dead from `rg`
counts alone.

## Surface Taxonomy

Classify every candidate as exactly one:

* `live-authority`: current runtime authority or stable facade contract.
* `live-generated-boundary`: required by macro/generated code wiring.
* `live-diagnostics`: feature-gated or test-retained diagnostics surface.
* `live-test-support`: production-adjacent helper intentionally owned for tests.
* `stale-compatibility`: retained for obsolete internal format/protocol support.
* `stale-generated-fallback`: generated-model runtime reconstruction or fallback
  that should be retired under current architecture.
* `orphaned-helper`: helper, type, enum variant, or module with no current owner.
* `overexposed-internal`: reachable surface wider than its actual consumers need.
* `duplicate-surface`: parallel API paths where one canonical owner is enough.
* `unclear`: insufficient evidence; requires owner decision.

## Deletion Confidence

Use this scale:

* `high`: no non-test consumers, no facade/generated boundary role, compile and
  focused tests can prove removal.
* `medium`: consumers exist but can be rewired to a canonical owner without
  changing behavior.
* `low`: public/facade/generated/runtime authority surface, or consumer evidence
  is incomplete.
* `blocked`: cannot decide without external owner, release policy, or generated
  artifact evidence.

Do not recommend deletion for `low` or `blocked`; recommend an owner decision or
follow-up audit instead.

## STEP 0 - Run Metadata

Evidence mode: `semi-mechanical`

Capture:

| Field [M/C] | Value |
| ---- | ---- |
| `method_version` | `CSH-1.0` |
| `baseline_report` | path or `N/A` |
| `comparability_status` | `comparable` / `non-comparable` |
| `code_snapshot` | git short SHA or `N/A` |
| `in_scope_roots` | roots inspected |
| `excluded_roots` | roots excluded |
| `generated_code_inclusion` | included / excluded / sampled |
| `test_surface_inclusion` | included / excluded / sampled |

## STEP 1 - Reachable Surface Inventory

Evidence mode: `mechanical`

Inventory surface that can create dead complexity:

* crate-root `pub mod` and `pub use`
* facade `pub mod`, `pub use`, and stable prelude exports
* `#[doc(hidden)]` exports
* `__macro` exports
* `pub(crate)`, `pub(in ...)`, and `pub(super)` items in hub modules
* cfg-gated diagnostics/test exports
* public error variants and DTOs with internal representation payloads

Produce:

| Item [M] | Kind [M] | Path [M] | Visibility [M] | Feature/Cfg [M] | Consumer Evidence [M/C] | Surface Class [C] | Owner [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |

Consumer evidence should prefer compiler reachability, direct imports, generated
output references, or focused code inspection over text counts.

## STEP 2 - Dead / Stale Surface Signals

Evidence mode: `semi-mechanical`

Scan for:

* `#[allow(dead_code)]`, `#[expect(dead_code)]`, `#[expect(unused_imports)]`
* `legacy`, `compat`, `compatibility`, `fallback`, `shim`, `deprecated`
* generated-model bridges such as direct `EntityModel` / `IndexModel` runtime
  reconstruction after schema acceptance
* duplicate route, cursor, commit, storage, SQL, or diagnostics entrypoints
* enum variants whose only remaining purpose is old transition handling
* public DTO fields that expose implementation representation without a stable
  facade reason

Produce:

| Candidate [M] | File [M] | Lines [M] | Signal [M] | Current Consumers [M/C] | Surface Class [C] | Authority Reason [C] | Deletion Confidence [C] | Risk If Removed [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |

## STEP 3 - Runtime Authority Drift Check

Evidence mode: `classified`

Check that runtime behavior does not keep obsolete authorities alive.

Required IcyDB checks:

* accepted `SchemaInfo` / schema snapshot paths are authoritative for runtime
  planning, execution, decoding, and mutation.
* generated `EntityModel` / `IndexModel` paths are limited to proposal,
  reconciliation, model-only convenience, generated-boundary compatibility, or
  tests.
* SQL DDL paths lower into catalog-native schema mutation rather than owning
  mutation semantics.
* generated endpoint metadata does not support endpoint name override shims.
* persisted decoding remains bounded and fallible; removing dead surface must
  not replace a typed contract with ad hoc reconstruction.

Produce:

| Area [C] | Runtime Authority [C] | Alternate Authority Found? [C] | Evidence [M/C] | Allowed Role? [C] | Finding [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |

## STEP 4 - Complexity That Exists Only For Dead Surface

Evidence mode: `semi-mechanical`

Focus on complexity that can shrink by deletion or visibility narrowing, not
general design complexity.

Measure:

* branch sites protecting obsolete compatibility modes
* enum variants used only to route to old behavior
* match arms for no-longer-supported formats or route shapes
* modules whose public API count is high while consumer count is low
* feature-gated branches whose feature no longer has a reachable caller
* facade re-export chains that widen internal implementation surface

Produce:

| Module [M] | Complexity Signal [M] | Dead-Surface Link [C] | Public/Hidden Items [M] | Current Consumers [M/C] | Shrink Action [C] | Expected Blast Radius [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ---- | ---- | ---- | ---- |

## STEP 5 - Facade / Generated Boundary Review

Evidence mode: `classified`

Review `icydb` facade and `__macro` surfaces separately from normal public API.

Do not classify generated-boundary surface as dead until generated output,
macro expansion, or derive tests prove it is unused.

Check:

* facade modules expose stable user-facing concepts, not core internals by
  convenience.
* `#[doc(hidden)]` exports are either generated-boundary requirements or
  explicitly temporary.
* `__macro` exports are consumed by generated code or local core test harnesses.
* macro support does not keep broad runtime internals public when a narrower
  helper would work.

Produce:

| Surface [M] | Boundary Type [C] | Generated Consumer Evidence [M/C] | Could Narrow? [C] | Required Replacement [C] | Deletion Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |

## STEP 6 - Feature / Diagnostics / Test Surface Review

Evidence mode: `semi-mechanical`

Check feature-gated and test-only surfaces:

* `diagnostics`
* `sql`
* generated endpoint switches
* metrics hooks
* test-only re-exports and fixtures

Classify test and diagnostics surface by current owner. Test-only surface may be
valid, but it should not force production visibility wider than necessary.

Produce:

| Surface [M] | Feature/Cfg [M] | Production Consumer? [M/C] | Test/Diagnostics Consumer? [M/C] | Visibility Could Narrow? [C] | Action [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |

## STEP 7 - Removal Safety Plan

Evidence mode: `classified`

For every `high` or `medium` confidence candidate, define the smallest safe
change.

Allowed actions:

* delete
* narrow visibility
* collapse duplicate export to canonical owner
* move to test-only module
* replace stale compatibility branch with current-format hard cut
* add owner decision before touching

Produce:

| Candidate [M] | Action [C] | Owner Boundary [C] | Required Proof [C] | Focused Validation [C] | Wasm Raw Bytes Relevant? [C] | Follow-Up Required? [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |

## STEP 8 - Risk Scoring

Evidence mode: `classified`

Score only removable/narrowable surface, not the whole architecture.

Risk index:

* `0-2`: low dead-surface pressure
* `3-5`: moderate cleanup queue; track follow-ups
* `6-8`: high complexity retained by stale surface
* `9-10`: critical; obsolete authority or fallback path can distort runtime
  behavior or block pre-`1.0.0` hard cuts

Produce:

| Bucket [C] | Count [D] | Highest Risk [C] | Notes [C] |
| ---- | ----: | ---- | ---- |
| stale compatibility |  |  |  |
| stale generated fallback |  |  |  |
| orphaned helper |  |  |  |
| overexposed internal |  |  |  |
| duplicate surface |  |  |  |
| unclear |  |  |  |

## Required Report Sections

Every report must include:

1. run metadata
2. step status table
3. reachable surface inventory summary
4. dead/stale candidate table
5. runtime authority drift findings
6. facade/generated-boundary findings
7. removal safety plan
8. risk score
9. verification readout
10. follow-up actions or explicit "none"

Step status table:

| Step [M] | Status [C] | Evidence Artifact [M/C] | Comparability Impact [C] |
| ---- | ---- | ---- | ---- |
| STEP 0 |  |  |  |
| STEP 1 |  |  |  |
| STEP 2 |  |  |  |
| STEP 3 |  |  |  |
| STEP 4 |  |  |  |
| STEP 5 |  |  |  |
| STEP 6 |  |  |  |
| STEP 7 |  |  |  |
| STEP 8 |  |  |  |

Allowed statuses:

* `PASS`: evidence and table are present.
* `N/A`: method explicitly allows no candidates or no relevant surface.
* `BLOCKED`: evidence could not be produced; include a concrete reason.

## Suggested Evidence Commands

These are prompts for the auditor, not mandatory exact commands:

* enumerate public exports and hidden exports from crate roots
* search for `dead_code`, `unused_imports`, `legacy`, `compat`, `fallback`,
  `shim`, `deprecated`, `EntityModel`, and `IndexModel`
* inspect facade and `__macro` re-export chains
* run focused compile/tests after proposed deletions or visibility narrowing
* compare raw non-gzipped wasm bytes only when the candidate affects canister
  runtime payload

Do not start or stop the local ICP network for this audit.
