# MODULAR AUDIT - Complexity / Surface Hygiene

`icydb-core` first; facade, build, config, CLI, and schema crates only where they
own reachable runtime surface, generated-code wiring, or code whose retention
keeps complexity alive.

## Audit Name

Use **Complexity / Surface Hygiene**, not **Code Hygiene**.

Reason: `code hygiene` already means formatting, imports, docs, ordering, and
small readability standards in `docs/governance/code-hygiene.md`. This audit is
narrower and more consequential: remove dead or stale exposed surface, collapse
unnecessary complexity lanes, and verify that every retained in-scope code unit
has a current IcyDB authority reason.

Recommended slug: `complexity-surface-hygiene`.

## Purpose

Force retained code to justify its current role in IcyDB's runtime architecture,
then find code that can be deleted, narrowed, or retired because that
justification is missing, obsolete, duplicated, or too broad.

This audit targets:

* dead public or hidden surface
* stale compatibility paths
* generated-model runtime fallbacks
* legacy shims after catalog-native schema acceptance
* orphaned helpers, DTOs, cfg branches, and diagnostics hooks
* complexity that exists only to preserve an obsolete route, format, or caller
* code that is "live" only because current callers preserve an old shape
* abstractions whose vocabulary or indirection costs more than their invariant

This is NOT:

* a style audit
* a general DRY audit
* a correctness audit
* a performance audit
* a LoC-reduction contest
* a module-boundary audit unless ownership or exposure is the reason code cannot
  justify itself
* a redesign proposal exercise
* a request for line-by-line prose when adjacent implementation lines share one
  clear authority reason

The goal is simplification with authority intact: delete, narrow, inline, or
move code when that makes the current architecture smaller and clearer. Do not
reduce LoC by removing useful invariants, diagnostics, or generated-boundary
proofs.

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

* `method_version = CSH-1.2`
* `surface_taxonomy = ST-1`
* `authority_taxonomy = AT-1`
* `deletion_confidence_model = DC-1`
* `compatibility_policy = pre-1.0-hard-cut`
* `wasm_signal_rule = raw-wasm-primary`

Mark the run `non-comparable` if any manifest item changes, if in-scope roots
change, or if test/generated-code inclusion rules change.

`CSH-1.2` strengthens `CSH-1.1` by adding deletion pressure and required
disposition decisions. A reference is no longer enough to classify a surface as
live; the audit must also decide whether the consumer should continue to exist,
whether the surface can be narrowed or inlined, and who owns the retained shape.
Reports using `CSH-1.2` are non-comparable with `CSH-1.1` unless they explicitly
explain how the stricter retention/disposition standard was backfilled.

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

## Retention Standard

Every retained in-scope code unit must have a current IcyDB authority reason.
The audit question is:

> If this code unit disappeared or became narrower, what current IcyDB authority
> would fail?

Then ask the stricter CSH-1.2 follow-up:

> Is that failure desirable because it removes an obsolete consumer, broad
> surface, or old vocabulary?

A code unit can be a module, function, type, trait impl, enum variant, match arm,
DTO field, re-export, cfg branch, diagnostics hook, generated-boundary helper,
or tightly related implementation block. Adjacent implementation lines may be
justified together when they serve one clear authority reason.

Acceptable authority reasons:

* current runtime authority
* generated-boundary requirement
* stable facade contract
* test or diagnostics ownership that does not widen production visibility
* narrow implementation support for one of the above

Non-reasons:

* historical compatibility before `1.0.0`
* "it is currently used" without explaining why the consumer should still exist
* convenience re-exporting
* avoiding churn
* possible future use without an owner decision
* test-only use that keeps production surface wider than necessary

If a code unit has no current authority reason, do not classify it as live.
Classify it as `stale-compatibility`, `stale-generated-fallback`,
`orphaned-helper`, `overexposed-internal`, `duplicate-surface`, or `unclear`.
Use `unclear` only when the missing authority reason needs an owner decision,
not as a way to retain code by default.

## Deletion Pressure Standard

Every retained item must answer all of these:

* What breaks if this is deleted?
* Is that break desirable because the caller is stale, test-only, generated-only,
  overexposed, or preserving old vocabulary?
* Is the consumer production code, generated code, diagnostics, tests, or
  historical compatibility?
* Can the consumer be changed more simply than retaining the surface?
* Is the item public because users need it, or because it was convenient?
* Does the item protect a real invariant, or does it only add vocabulary?

Reference reachability is evidence only. It is not a retention reason.

Special pressure rules:

* If only tests use production surface, move the surface to test support or
  delete it unless it guards a production invariant.
* If only generated code uses the surface, it belongs behind `__macro` or another
  generated boundary, not normal public API.
* If an abstraction has one caller and does not protect a meaningful invariant,
  inline it or mark `INLINE NOW`.
* If a crate exists only for a tiny helper, identify the real owner and mark
  `MOVE OWNER`, unless the crate is deliberately preserving a workspace boundary.
* If a huge module is live, the audit must name the owner and either a concrete
  shrink trigger or an explicit reason the module should stay whole.

## Disposition Taxonomy

Every retained or candidate item gets exactly one disposition:

* `DELETE NOW`: remove the item in the current slice.
* `NARROW NOW`: reduce visibility or move behind the correct hidden/generated
  boundary in the current slice.
* `INLINE NOW`: inline a one-caller or vocabulary-only abstraction in the
  current slice.
* `MOVE OWNER`: move the item to the crate/module that owns the invariant.
* `MOVE TO TEST`: move test-only production surface into test support.
* `RETAIN WITH OWNER`: keep the item; report the owner and invariant it protects.
* `DEFER WITH TRIGGER`: keep temporarily; report the exact future event that
  should force deletion, narrowing, inlining, or movement.
* `BLOCKED`: owner decision, generated artifact, or release policy evidence is
  required before changing it.

Avoid bare "defer". A deferral without a trigger is just retention by default.

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

Do not recommend deletion for `low` or `blocked`; recommend `DEFER WITH TRIGGER`
or `BLOCKED` instead. `medium` confidence should normally produce a concrete
`NARROW NOW`, `INLINE NOW`, `MOVE OWNER`, or `MOVE TO TEST` action unless the
blast radius is too broad for the slice.

## STEP 0 - Run Metadata

Evidence mode: `semi-mechanical`

Capture:

| Field [M/C] | Value |
| ---- | ---- |
| `method_version` | `CSH-1.2` |
| `baseline_report` | path or `N/A` |
| `comparability_status` | `comparable` / `non-comparable` |
| `code_snapshot` | git short SHA or `N/A` |
| `in_scope_roots` | roots inspected |
| `excluded_roots` | roots excluded |
| `generated_code_inclusion` | included / excluded / sampled |
| `test_surface_inclusion` | included / excluded / sampled |

## STEP 1 - Reachable Surface And Retention Inventory

Evidence mode: `mechanical`

Inventory surface and retained code units that can create dead complexity:

* crate-root `pub mod` and `pub use`
* facade `pub mod`, `pub use`, and stable prelude exports
* `#[doc(hidden)]` exports
* `__macro` exports
* `pub(crate)`, `pub(in ...)`, and `pub(super)` items in hub modules
* cfg-gated diagnostics/test exports
* public error variants and DTOs with internal representation payloads
* private helpers, branch families, DTO fields, and module-local impl blocks in
  hub or hotspot modules when they materially retain complexity

Produce:

| Item [M] | Kind [M] | Path [M] | Visibility [M] | Feature/Cfg [M] | Consumer Evidence [M/C] | Consumer Should Exist? [C] | Authority Reason [C] | Surface Class [C] | Owner [C] | Disposition [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |

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
* private helpers or match arms retained only because a broad module still uses
  them after its authority moved elsewhere

Produce:

| Candidate [M] | File [M] | Lines [M] | Signal [M] | Current Consumers [M/C] | Consumer Should Exist? [C] | Authority Reason [C] | Surface Class [C] | Deletion Confidence [C] | Disposition [C] | Risk If Removed [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |

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

Focus on complexity that can shrink by deletion, visibility narrowing, inlining,
or ownership movement. General design complexity is out of scope only when each
inspected code unit has a current authority reason.

Measure:

* branch sites protecting obsolete compatibility modes
* enum variants used only to route to old behavior
* match arms for no-longer-supported formats or route shapes
* modules whose public API count is high while consumer count is low
* feature-gated branches whose feature no longer has a reachable caller
* facade re-export chains that widen internal implementation surface
* helper blocks whose only justification is that a nearby live module still
  happens to call them

Produce:

| Module [M] | Complexity Signal [M] | Retention Justification [C] | Dead-Surface Link [C] | Public/Hidden Items [M] | Current Consumers [M/C] | Shrink Action [C] | Disposition [C] | Expected Blast Radius [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ----: | ---- | ---- | ---- | ---- | ---- |

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

| Surface [M] | Boundary Type [C] | Generated Consumer Evidence [M/C] | Could Narrow? [C] | Required Replacement [C] | Deletion Confidence [C] | Disposition [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |

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

| Surface [M] | Feature/Cfg [M] | Production Consumer? [M/C] | Test/Diagnostics Consumer? [M/C] | Visibility Could Narrow? [C] | Action [C] | Disposition [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |

## STEP 7 - Removal Safety Plan

Evidence mode: `classified`

For every `high` or `medium` confidence candidate, and every inspected code unit
whose retention justification is missing or weak, define the smallest safe
change.

Allowed actions:

* delete
* narrow visibility
* inline one-caller or vocabulary-only helpers
* collapse duplicate export to canonical owner
* move owner
* move to test-only module
* replace stale compatibility branch with current-format hard cut
* defer with a specific trigger
* block on owner decision before touching

Produce:

| Candidate [M] | Action [C] | Disposition [C] | Owner Boundary [C] | Required Proof [C] | Focused Validation [C] | Wasm Raw Bytes Relevant? [C] | Follow-Up Trigger [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |

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
3. reachable surface and retention inventory summary
4. dead/stale candidate table
5. runtime authority drift findings
6. facade/generated-boundary findings
7. removal safety plan
8. risk score
9. verification readout
10. disposition summary
11. follow-up actions or explicit "none"

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

Suggested deletion-pressure prompts:

* list items with exactly one production caller and inspect whether the helper
  protects an invariant or just adds vocabulary
* list public or hidden exports consumed only by generated code and check whether
  they belong under `__macro`
* list production items consumed only by tests and check whether they can move to
  test support
* list largest modules and require either a retained-owner explanation or a
  concrete split/deletion trigger
* inspect crate boundaries where a crate remains for one or two helpers and ask
  whether ownership should move back to the real caller

Do not start or stop the local ICP network for this audit.
