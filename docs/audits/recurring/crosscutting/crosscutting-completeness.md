# Completeness Audit Method

## Purpose

This audit is a reusable method for producing a full inventory of feature and
system completeness across the IcyDB codebase.

It is not tied to a single slice, release, or branch. Its purpose is to answer,
for any chosen code state:

- what feature families exist
- what is fully implemented
- what is partial, bounded, or fail-closed
- what is missing
- what is intentionally out of scope
- what architectural seams remain
- what the highest-value next steps are

This audit is intended to be rerun over time so completeness can be assessed
consistently across different versions of the system.

That means the method must optimize for:

- comparability across time
- explicit boundary control
- stable terminology
- repeatable evidence standards

---

## Audit Identity

- Definition path:
  `docs/audits/recurring/crosscutting/crosscutting-completeness.md`
- Report scope: `completeness`
- Current method tag: `Completeness Method V2`
- Report naming:
  `docs/audits/reports/YYYY-MM/YYYY-MM-DD/completeness.md`

If the same audit is rerun more than once on the same day, keep the canonical
report path for the final readout and record superseded same-day attempts in the
report preamble.

`Completeness Method V2` keeps the original feature-breadth and pipeline-depth
model, but refreshes the required proof surface for the current public contract:

- the current SQL contract in `docs/contracts/SQL_SUBSET.md`
- public SQL read, write, DDL, introspection, and `EXPLAIN` entrypoints
- SQL mutation and `RETURNING` semantics where they are part of the public
  session/library surface
- DDL-published schema and index transitions, including expression indexes and
  supported `ALTER TABLE` forms
- blob value handling where the SQL contract admits it
- prepared and cached execution proof where the feature depends on reuse or
  identity stability
- read-only validation discipline and normalized verification statuses

Reports using V2 should explicitly say whether they are comparable with older
V1 reports. A material public-contract expansion should be marked as a method
refresh rather than a direct feature-state regression.

---

## Audit Output

A completeness audit using this method should produce:

1. a system-boundary statement
2. a feature inventory
3. a pipeline-stage completeness read
4. a list of bounded / partial areas
5. a list of absent or out-of-scope areas
6. a list of major architectural seams
7. a prioritized next-step recommendation set

The output may be qualitative, scored, or both.

---

## Core Principle

Completeness is not just feature presence.

A feature is only complete when the relevant parts of the pipeline are also
complete for that feature. Depending on the feature, this may include:

- admitted syntax or public surface
- lowering into semantic form
- canonical or identity behavior
- planning support
- execution support
- explain / diagnostics fidelity
- proof through real-surface tests

This audit therefore measures both:

- **feature breadth**
- **pipeline depth**

---

## Step 1. Define The Audit Boundary

Before inventory begins, define the intended system boundary for the code state
being audited.

Examples:

- single-entity SQL only
- SQL up to grouped SELECT without JOIN
- SQL plus prepared execution
- full query surface including mutation
- public canister surface only

The boundary must explicitly name:

### Included

- features and subsystems intended to count toward completeness

### Excluded

- features intentionally out of scope for the audited line

Out-of-scope items must not be mixed with missing in-scope items.

### Authoritative Proof Surfaces

Each audit must also explicitly name which proof surfaces count as
authoritative for that run.

Examples:

- SQL only
- SQL + fluent
- SQL + fluent + prepared SQL
- public session surfaces
- canister/integration surfaces

If the audit later claims a feature is complete, that claim only applies to the
named proof surfaces for that run.

For the current recurring run, `docs/contracts/SQL_SUBSET.md` is the boundary
authority for public SQL support. The audit may inspect code and tests for
evidence, but the report should not widen or narrow the public contract based on
implementation details alone.

---

## Step 2. Inventory Major System Areas

Inspect the codebase and inventory the major system families.

At minimum, review the following areas.

### A. Surface / Admitted Feature Families

Examples:

- scalar SELECT
- grouped SELECT
- WHERE
- HAVING
- projection expressions
- aggregates
- ORDER BY
- LIMIT / OFFSET
- DISTINCT
- mutation
- SQL DDL
- public SQL writes and `RETURNING`
- blob values and blob predicates
- SQL introspection
- prepared SQL
- EXPLAIN

### B. Expression Families

Examples:

- arithmetic expressions
- boolean expressions
- searched CASE
- simple CASE
- scalar functions
- aggregate functions
- null-sensitive forms

### C. Semantic Pipeline

Examples:

- parse
- lowering
- semantic normalization / canonicalization
- predicate extraction
- plan construction
- route selection
- execution
- explain / diagnostics
- identity / cache behavior

### D. Supporting System Areas

Examples:

- prepared execution
- cache layers
- test harness / proof surfaces
- diagnostics
- architecture invariants
- fail-closed boundaries

The exact feature list may be expanded for the audited code state, but the audit
should always make the chosen taxonomy explicit.

When the audit uses numeric scoring or a headline maturity summary, taxonomy
must be split into two tiers:

### Primary feature rows

These are the rows that count toward the headline completeness read.

Examples:

- scalar `SELECT`
- grouped `SELECT`
- predicates
- projection
- aggregates
- mutation
- `EXPLAIN`

### Supporting rows

These rows provide context, seams, and enabling-system readouts, but do not
count as independent headline feature rows unless the audit explicitly says so.

Examples:

- prepared execution
- cache layers
- diagnostics internals
- proof surfaces
- architecture invariants

This avoids double-counting the same capability once as a product feature and
 again as an enabling subsystem.

---

## Step 3. Classify Each Feature Area

Each feature or subsystem should be classified into one of these states.

### Complete

Use when the feature is fully implemented within the audited boundary and its
relevant pipeline stages are coherent and proven.

Typical properties:

- admitted and usable on intended public surfaces
- lowered into the intended semantic model
- planned and executed correctly
- identity / explain align where relevant
- covered by real-surface proof

### Partial

Use when the feature exists but is incomplete in a meaningful way.

Examples:

- implemented only for some families
- execution works but explain lags
- SQL surface exists but fluent or prepared parity is missing
- broad feature row hides multiple materially different sub-states

### Bounded

Use when support is intentionally restricted and the restriction is explicit and
fail-closed.

Examples:

- a feature is admitted only for one semantic family
- unsupported shapes are rejected uniformly
- a canonicalization family is intentionally narrow

This is stronger than “partial” when the boundary is deliberate and coherent.

### Missing

Use when the feature is expected inside the audited boundary but is absent.

### Out Of Scope

Use when the feature is intentionally outside the audited boundary.

This must be kept distinct from Missing.

Feature-state labels are separate from validation statuses. Use feature states
for product and subsystem completeness, and use validation statuses for command
or inspection outcomes:

- `PASS`: the check completed and supports the claimed evidence
- `FAIL`: the check completed and found a contradiction or regression
- `BLOCKED`: the check could not be run in the current audit constraints

Use `PARTIAL` only as a feature or stage classification, not as a verification
status.

### Required Derivation Rule

The final feature-state label must be derived from the stage reads, not chosen
 independently.

Use these default derivation rules unless the audit explicitly overrides them:

- **Complete**
  - all applicable stages are `Strong`
  - proof exists on the authoritative proof surfaces for that audit
- **Bounded**
  - the feature is intentionally restricted
  - unsupported shapes are fail-closed
  - applicable stages are mostly `Strong` or `Partial`
  - no stage is `Missing` inside the admitted bounded family
- **Partial**
  - the feature exists, but one or more applicable stages are `Partial` or
    `Weak`, or parity across relevant surfaces is inconsistent
- **Missing**
  - one or more required stages for the in-scope feature are effectively absent
- **Out Of Scope**
  - the feature is excluded by the audit boundary

This rule is what makes repeated audits comparable over time.

---

## Step 4. Evaluate Pipeline Completeness

For each major feature area, inspect the relevant pipeline stages.

Suggested stage model:

- Parse / admitted surface
- Lowering
- Canonicalization / semantic identity
- Planning
- Execution
- Explain / diagnostics
- Proof

Not every stage applies equally to every feature. When a stage is structurally
not applicable, mark it as `N/A` rather than forcing a positive or negative
score.

### Stage Read Definitions

#### Strong
The stage is coherent, aligned with the architecture, and supported by evidence.

#### Partial
The stage exists but has gaps, bounded subfamilies, or uneven parity.

#### Weak
The stage exists but is one of the main limiting factors for the feature.

#### Missing
The stage does not exist for the feature.

#### N/A
The stage is structurally not part of the feature being audited.

### Required Applicability Rule

If a stage is structurally not part of the feature, it must be marked `N/A`.

It must not be marked `Strong` only because nearby subsystems are healthy.

Examples:

- a product-surface `EXPLAIN` row may treat runtime query execution as `N/A`
  when the owned surface is explain rendering rather than result production
- mutation rows must not claim `Canonical` unless the audit defines a real
  canonical-identity boundary for that mutation feature

This rule prevents inflated time-series scores.

---

## Step 5. Check Cross-Layer Consistency

A feature is not complete if the layers disagree about what it is.

For each important feature family, inspect whether the following align:

- public admitted surface
- semantic lowering
- canonical / identity form
- planner assumptions
- execution behavior
- explain rendering
- cache / reuse behavior
- proof surfaces

This step is especially important for:

- CASE families
- grouped semantics
- prepared execution
- plan reuse / structural cache behavior
- explain fidelity

The audit should explicitly call out any contradiction where one layer says the
feature exists but another layer does not carry the same contract.

When contradiction exists, the feature must not be labeled `Complete` even if
most stages are otherwise strong.

---

## Step 6. Identify Architectural Seams

Completeness is not only about missing public features. It is also limited by
cross-cutting seams that increase drift risk.

The audit should explicitly inspect for:

- duplicate semantic reasoning paths
- multiple authorities for one decision
- planner / execution divergence
- syntax-owned behavior where semantic ownership is intended
- explain reconstruction instead of artifact ownership
- prepared-path duplication
- incomplete identity follow-through
- coarse feature families hiding materially different states

Seams should be reported even when no user-visible bug exists yet.

---

## Step 7. Separate Breadth From Depth

The audit must distinguish:

### Breadth
How much of the intended product surface exists?

### Depth
How complete is each admitted feature across the pipeline?

This prevents two common mistakes:

- over-crediting features that merely parse
- under-crediting systems that have high semantic depth but intentionally narrow surface

The audit should explicitly say whether the audited system is currently:

- broad and shallow
- narrow and deep
- broad and deep
- fragmented
- bounded but coherent

---

## Step 8. Produce The Inventory Readout

The final audit output should include these sections.

### 1. System Boundary
What counts and what does not.

### 2. Implemented Feature Inventory
List what exists.

### 3. Partial / Bounded Areas
List what exists but is incomplete or intentionally restricted.

### 4. Missing In-Scope Areas
List what should exist inside the audited boundary but does not.

### 5. Out-Of-Scope Areas
List what is intentionally excluded.

### 6. Architectural Seams
List the major cross-cutting risks or consolidation targets.

### 7. Overall Maturity Read
Describe the system in a few clear sentences.

### 8. Recommended Next Steps
Prioritize the next slices implied by the inventory.

---

## Step 9. Optional Scoring Layer

This method may be used with or without numeric scoring.

When numeric scoring is used, keep these rules explicit:

- scores describe the audited boundary only
- `Out Of Scope` must not be treated as `0`
- `N/A` stages must not be averaged as positive or negative values
- broad overlapping feature rows should be identified as overlapping
- scores are only as trustworthy as their evidence

### Default Numeric Mapping

If numeric scoring is used, use one stable default mapping unless the audit
explicitly says otherwise:

- `Strong = 1.0`
- `Partial = 0.5`
- `Weak = 0.25`
- `Missing = 0.0`
- `N/A = excluded from averages`

Feature-state labels such as `Complete`, `Bounded`, and `Partial` remain
qualitative outputs derived from the stage pattern. They are not separate
numeric values.

### Headline Score Rule

If one headline completeness score is reported:

- compute it from primary feature rows only
- exclude supporting rows from the headline average
- report supporting rows separately as context or seam indicators

This keeps repeated audits comparable instead of letting subsystem-count drift
change the score without any real product movement.

Numeric scoring is optional. A qualitative audit is still valid if it clearly
distinguishes complete, partial, bounded, missing, and out-of-scope areas.

---

## Read-Only Run Mode

When the user asks to run this audit read-only:

- do not modify product code, generated artifacts, lockfiles, package manifests,
  or release metadata
- only update the audit definition when the audit method itself is stale
- only create or update the matching audit report artifact
- do not start or stop external services
- treat live canister or network-dependent checks as `BLOCKED` unless the
  required service is already running and the check is explicitly read-only
- record broad checks that would mutate state or require unavailable services as
  `BLOCKED`, not as failures

Existing dirty worktree changes may be inspected for context, but the report
must distinguish them from stable evidence unless the relevant checks pass on
the current snapshot.

---

## Recommended Read-Only Baseline

Use focused checks that exercise breadth across the admitted public surface
without mutating repository state beyond normal build/test artifacts:

- `make check-invariants`
- `cargo test -p icydb-core --features sql query_lowering -- --nocapture`
- `cargo test -p icydb-core --features sql predicate_convergence -- --nocapture`
- `cargo test -p icydb-core --features sql execution_convergence -- --nocapture`
- `cargo test -p icydb-core --features sql explain_cache_convergence -- --nocapture`
- `cargo test -p icydb-core --features sql sql_blob -- --nocapture`
- `cargo test -p icydb-core --features sql execute_sql_query_admits_supported_single_entity_read_shapes -- --nocapture`
- `cargo test -p icydb-core --features sql compile_sql_query_and_execute_compiled_preserve_supported_read_families -- --nocapture`
- `cargo test -p icydb-core --features sql execute_sql_update_admits_supported_single_entity_mutation_shapes -- --nocapture`
- `cargo test -p icydb-core --features sql execute_sql_ddl_publishes_supported_expression_index -- --nocapture`
- `cargo test -p icydb-core --features sql execute_sql_ddl_rename_column_updates_expression_index_metadata -- --nocapture`
- `git diff --check`

The report may substitute nearby focused filters when names drift, but it should
record the substitution and why it preserves the same proof intent.

---

## Reporting Guidance

When writing a completeness audit from this method:

- prefer concrete system families over vague labels
- separate architecture seams from missing public features
- distinguish deliberate boundaries from accidental gaps
- do not collapse “partial” and “bounded” together when the difference matters
- call out contradictions explicitly
- keep out-of-scope items separate from missing ones
- do not claim completeness based only on parsing or isolated runtime support

---

## Recommended Reusable Headings

A concrete completeness audit should usually use headings like:

1. Report Preamble
2. Executive Summary
3. System Boundary
4. Feature Inventory
5. Pipeline Completeness
6. Partial / Bounded Areas
7. Missing In-Scope Areas
8. Out-Of-Scope Areas
9. Architectural Seams
10. Overall Maturity Read
11. Recommended Next Steps

---

## What This Audit Is For

Use this audit when you need a full codebase read such as:

- “What do we actually support right now?”
- “How complete is SQL up to the current boundary?”
- “What is missing before we widen surface area?”
- “What systems are complete versus merely present?”
- “What should we do next if we want the cleanest path forward?”

---

## What This Audit Is Not

This audit is not:

- a narrow feature review
- a single-slice changelog
- a benchmark report
- a code-quality-only audit
- a replacement for proof or CI

It is a reusable method for producing a full inventory of feature completeness.

---

## Summary

A completeness audit should answer, for any chosen code state:

- what exists
- what is complete
- what is partial or bounded
- what is missing
- what is out of scope
- where the major seams still are
- what the next high-value slices should be

That answer should be based on the whole system, not only on public syntax or
isolated implementation fragments.
