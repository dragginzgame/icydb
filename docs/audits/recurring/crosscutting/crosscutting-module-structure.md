# WEEKLY AUDIT - Structure / Module / Visibility Discipline

`icydb-core` (+ facade where relevant)

## Purpose

Verify that architectural boundaries remain:

* layered
* directional
* encapsulated
* narrowly exposed
* intentionally public

This audit measures structural containment and visibility discipline.

It does NOT evaluate:

* correctness
* performance
* features
* style
* refactoring ideas (unless boundary violation is severe)

---

# Audit Rules (Mandatory)

## Evidence Standard

Every non-trivial claim MUST identify:

* module or file
* dependency or exposed item
* visibility scope (`pub`, `pub(crate)`, `pub(super)`, private)
* directional impact or exposure impact

For medium/high/critical findings, evidence must come from inspected file/module
context, not symbol mention counts alone.

## Severity Rules

* `Low`: acceptable but worth monitoring.
* `Medium`: mild boundary pressure or avoidable exposure.
* `High`: clear architectural violation or unstable exposure pattern.
* `Critical`: cross-layer breach, public leak of internals, or dependency cycle
  that materially harms containment.

## Counting + Comparability Rules

* `Publicly reachable from root` means reachable from crate root by public path,
  including via `pub mod`, `pub use`, or nested public items under public modules.
* `Subsystem dependency` means dependency across top-level subsystem roots, not
  intra-subsystem references.
* `Cross-layer dependency` means dependency on a subsystem outside expected
  responsibility layer of that subsystem.
* `Cycle` means real subsystem-level mutual dependency/back-reference, not two
  subsystems both depending on shared lower utilities.
* Ignore test-only code except in explicit test-leakage checks.
* Ignore generated artifacts unless they are committed runtime API surface.
* Treat macros separately from ordinary runtime API.

## Pressure vs Violation Rule

* `Pressure` = coordination/breadth/containment strain.
* `Violation` = directional breach, public internal leak, or real cycle.

Do not collapse pressure and violation language in findings.

---

# STEP 1 - Public Surface Mapping

## 1A. Crate Root Enumeration

Enumerate:

* all `pub mod` at crate root
* all `pub use` re-exports
* all publicly reachable `pub struct`, `pub enum`, `pub trait`
* all publicly reachable `pub fn`
* all publicly reachable `pub type`
* all publicly reachable macros (if any), tracked separately from runtime API

Produce:

| Item | Kind | Path | Publicly Reachable From Root? | Classification | Visibility Scope | Exposure Impact | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |

---

## 1B. Exposure Classification

For each public item, classify:

* intended external API
* facade-support item
* macro-support item
* internal plumbing exposed for convenience
* accidentally exposed
* unclear / requires judgment

Exposure scan must explicitly check:

* executor internals
* planner internals
* recovery/commit machinery
* raw storage types
* `__internal` namespaces (or equivalent)
* internal diagnostics/test helpers
* unstable implementation wiring types

Pressure vs violation guidance:

* convenience overexposure is usually pressure unless it leaks unstable internals
* accidental exposure of internals is a violation

---

## 1C. Public Field Exposure

Scan for:

* `pub struct` with `pub` fields
* public enums exposing representation-heavy/internal variants
* public types exposing `Raw*`, storage-entry, commit, recovery, or executor-owned representation types
* public constructors requiring internal representation types

Produce:

| Type | Public Fields? | Representation Leakage? | Stable DTO/Facade Contract? | Exposure Impact | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |

Decision rule:

* do NOT mark a public field/type as risky when it is clearly a stable DTO/facade contract

---

# STEP 2 - Subsystem Boundary Mapping

Evaluate the following subsystems:

* identity
* types
* serialize
* data
* index
* query/intent
* query/plan
* executable plan
* executor
* commit
* recovery
* cursor
* error
* facade (icydb)

Assignment rule:

* ambiguous modules must be assigned to the nearest owning subsystem and the
  ambiguity must be noted in findings.

For each subsystem:

## 2A. Dependency Direction

Identify:

* what it imports from
* what imports it

Expected high-level layering (reference model):

1. identity / types
2. serialize
3. data
4. index
5. query intent
6. planner
7. executable plan
8. executor
9. commit / recovery
10. facade

Direction rules:

* lower-layer dependency = acceptable
* same-layer dependency = pressure, not automatic violation
* higher-layer dependency = violation unless clearly facade-only wrapping with no behavioral coupling

Produce:

| Subsystem | Depends On | Depended On By | Lower-Layer Dependencies | Same-Layer Dependencies | Upward Dependency Found? | Direction Assessment (Pressure/Violation) | Risk |
| ---- | ---- | ---- | ----: | ----: | ---- | ---- | ---- |

---

## 2B. Circular Dependency Check

Cycle definition for this audit:

* report only real subsystem-level mutual dependency/back-reference patterns
* do NOT report false cycles from shared lower utilities
* do NOT report incidental trait/type references without reverse ownership

Produce:

| Subsystem A | Subsystem B | Real Cycle? | Evidence | Risk |
| ---- | ---- | ---- | ---- | ---- |

---

## 2C. Implementation Leakage

Explicitly check:

* planner referencing executor internals
* executor referencing intent-internal AST details instead of planned/executable forms
* recovery referencing planner logic
* index referencing query-layer constructs
* error layer depending on execution internals
* facade exposing implementation-owned core internals

Each finding MUST include location, dependency, description, and directional
impact.

Produce:

| Violation | Location | Dependency | Description | Directional Impact | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |

---

# STEP 3 - Visibility Hygiene Audit

Evaluate usage of:

* `pub`
* `pub(crate)`
* `pub(super)`
* private (default)

For each subsystem:

## 3A. Overexposure

Identify:

* `pub` items that appear crate-internal only
* `pub(crate)` helpers used only in one module or narrow parent chain
* helper constructors/accessors wider than their call graph appears to require
* modules made public only for tests or convenience imports

Produce:

| Item | Path | Current Visibility | Narrowest Plausible Visibility | Why Narrower Seems Valid | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |

Rule:

* do not recommend narrowing unless usage evidence suggests intended API shape
  remains intact

---

## 3B. Under-Containment Signals

Explicitly detect:

* deep internal helpers used across multiple subsystems
* "utility" modules acting as unofficial cross-layer bridges
* large modules with unusually broad `pub(crate)` surface

Produce:

| Area | Signal | Evidence | Pressure or Violation | Risk |
| ---- | ---- | ---- | ---- | ---- |

---

## 3C. Test Leakage

Check:

* test-only modules/helpers exposed outside `#[cfg(test)]`
* runtime modules importing test utilities
* test helper re-exports leaking into non-test builds

Produce:

| Item | Location | Leakage Type | Build Impact | Risk |
| ---- | ---- | ---- | ---- | ---- |

---

# STEP 4 - Layering Integrity Validation

Using the expected layering model, validate:

### 4A. No Upward References

Explicitly test:

* data does not depend on planner
* index does not depend on executor
* recovery does not depend on planner logic
* lower layers do not encode query policy
* cursor does not own planner semantics unless explicitly intended

Produce:

| Layer/Rule | Upward Dependency Found? | Description | Risk |
| ---- | ---- | ---- | ---- |

---

### 4B. Plan / Execution Separation

Explicitly validate:

* intent does not depend on executor
* planner does not depend on commit/recovery behavior
* executor consumes plan types without mutating planning semantics
* executable plan is a boundary artifact, not a mutable semantic owner

Produce:

| Separation Rule | Breach Found? | Evidence | Risk |
| ---- | ---- | ---- | ---- |

---

### 4C. Facade Containment

Explicitly validate:

* facade does not re-export core internals accidentally
* facade does not expose `Raw*` / storage-owned types
* facade does not flatten unstable namespaces

Produce:

| Facade Item | Leak Type | Exposure Impact | Risk |
| ---- | ---- | ---- | ---- |

---

# STEP 5 - Structural Pressure Indicators

This step records architectural pressure areas. Pressure is not automatic
violation unless directional breach/leak/cycle evidence is present.

Explicitly identify:

* subsystems importing 5+ sibling subsystems
* high-coordination hub modules
* modules spanning identity + index + execution concerns
* enums/traits spanning multiple conceptual layers
* low-layer error types used as de facto universal coordination types

Produce:

| Area | Pressure Type | Why This Is Pressure (Not Yet Violation) | Drift Sensitivity | Risk |
| ---- | ---- | ---- | ---- | ---- |

## 5A. Hub Import Pressure (Required Metric)

For each high-coordination hub module (if present), include:

* `db/mod.rs`
* `executor/load/mod.rs`
* `executor/route/mod.rs`
* `query/plan/mod.rs`

Required for each hub:

1. top imported sibling subsystems by imported symbol count
2. unique sibling subsystem count
3. cross-layer dependency count
4. delta vs previous report
5. HIP calculation

Produce:

| Hub Module | Top Imported Sibling Subsystems (by Symbol Count) | Unique Sibling Subsystems Imported | Cross-Layer Dependency Count | Delta vs Previous Report | HIP | Pressure Band | Risk |
| ---- | ---- | ----: | ----: | ---- | ----: | ---- | ---- |

Formula:

`HIP = cross_layer_dependency_count / max(1, total_unique_imported_subsystems)`

Interpretation bands:

* `< 0.30`: low pressure
* `0.30 - 0.60`: moderate pressure
* `> 0.60`: high pressure

Rule:

* if counts increased, include a one-sentence explanation grounded in observed code movement

---

# STEP 6 - Encapsulation Risk Index

Score each category and include a short basis explanation.

Produce:

| Category | Risk Index (1-10, lower is better) | Basis |
| ---- | ----: | ---- |
| Public Surface Discipline |  |  |
| Layer Directionality |  |  |
| Circularity Safety |  |  |
| Visibility Hygiene |  |  |
| Facade Containment |  |  |

Then provide:

### Overall Structural Risk Index (1-10, lower is better)

Rule:

* overall score must reflect worst real boundary pressures/violations, not a polite average

Interpretation:

* 1-3 = low risk / structurally healthy
* 4-6 = moderate risk / manageable pressure
* 7-8 = high risk / requires monitoring
* 9-10 = critical risk / structural instability

---

# STEP 7 - Drift Sensitivity Analysis

Include only growth vectors supported by observed dependency structure.

Explicitly test vectors such as:

* new `AccessPath`
* DESC / alternate order semantics
* new commit marker
* new public error type
* new execution terminal
* new cursor continuation mode

Produce:

| Growth Vector | Affected Subsystems | Why Multiple Layers Would Change | Drift Risk |
| ---- | ---- | ---- | ---- |

---

# Known Intentional Exceptions

Purpose: prevent relitigating deliberate structural choices every week.

Typical examples:

* facade re-exports of stable DTOs
* executable plan depending on planner-owned immutable artifacts
* cursor depending on execution-neutral continuation contracts

Produce:

| Exception | Why Intentional | Scope Guardrail | Still Valid This Run? |
| ---- | ---- | ---- | ---- |

---

# Delta Since Baseline

Highlight only:

* newly public items
* newly widened visibilities
* new subsystem dependencies
* new hub-pressure increases

Produce:

| Delta Type | Item/Subsystem | Previous | Current | Impact |
| ---- | ---- | ---- | ---- | ---- |

---

# Required Output Sections

0. Run Metadata + Comparability Note
1. Public Surface Map
2. Subsystem Dependency Graph
3. Circularity Findings
4. Visibility Hygiene Findings
5. Layering Violations
6. Structural Pressure Areas
7. Drift Sensitivity Summary
8. Structural Risk Index
9. Verification Readout (`PASS` / `FAIL` / `BLOCKED`)

Run metadata must include:

* target scope
* compared baseline report path
* method tag/version
* comparability status (`comparable` or `non-comparable` with reason)
* exclusions applied
* notable methodology changes vs baseline
* daily baseline rule:
  * first run of day compares to latest prior comparable report or `N/A`
  * same-day reruns compare to that day's `module-structure.md` baseline

Verification rules:

* `PASS` = no high/critical structural violations; only low/moderate pressure
* `FAIL` = any confirmed high/critical layering, exposure, or cycle violation
* `BLOCKED` = insufficient evidence/repo visibility for comparable judgment

---

# Anti-Shallow Requirement

Do NOT:

* give praise
* comment on naming
* comment on formatting
* propose redesign unless severe violation requires it
* produce medium/high findings from grep-only conclusions

Rules:

* do not infer architectural violations from symbol mentions alone
* inspect file/module context for every medium/high finding
* every claim must identify module/file, dependency/exposed item, visibility
  scope, and directional/exposure impact
