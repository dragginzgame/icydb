# IcyDB — Comprehensive Architecture, Complexity, and Redundancy Audit

## Mission

Perform an exhaustive, evidence-based architectural audit of the entire IcyDB workspace.

IcyDB has undergone extensive development across query planning, execution, indexing, schema management, storage, persistence, migrations, SQL, dynamic queries, and runtime integration.

Successive releases have introduced new capabilities, redesigned existing systems, and replaced earlier architectural assumptions.

We now need to determine how much architectural drift, duplication, redundancy, unnecessary abstraction, and historical sediment has accumulated.

**The objective is to make IcyDB substantially smaller, simpler, more coherent, and easier to maintain without sacrificing its intended functionality, correctness, performance, or reliability.**

This is not a cosmetic cleanup.

Question whether entire abstractions, modules, intermediate representations, execution flows, and subsystem boundaries should continue to exist.

No compatibility preservation is required for our own APIs.

We explicitly prefer hard cuts over aliases, adapters, compatibility layers, deprecated entry points, or transitional abstractions.

However, do not confuse removing compatibility with permission to break database correctness, durable data, query semantics, or crash recovery.

### Operating rules

1. **Audit first. Do not implement changes.**
2. Inspect the actual repository rather than assuming documentation describes the current implementation.
3. Read all applicable workspace instructions and project conventions.
4. Examine production code, tests, benchmarks, examples, generated code, build tooling, and relevant documentation.
5. Include every relevant workspace crate.
6. Identify overlapping implementations, even where the code looks superficially different.
7. Prefer deletion and consolidation over additional abstractions.
8. Do not recommend new frameworks, configuration systems, traits, or generic infrastructure unless they demonstrably eliminate more complexity than they introduce.
9. No backwards-compatibility shims, aliases, transitional wrappers, or parallel implementations.
10. Preserve legitimate separation of responsibilities and important performance specializations.
11. Treat query semantics, schema invariants, persisted data, recovery guarantees, and deterministic behavior as non-negotiable unless an explicitly identified product decision changes them.
12. Separate verified findings from architectural hypotheses.
13. Provide exact paths, symbols, supporting evidence, and proposed changes.
14. Do not fabricate performance, size, or instruction savings.
15. Avoid treating lines of code as the sole measure of complexity.

**Do not stop after finding a handful of easy improvements.**

The deliverable is a comprehensive architectural reduction programme.

---

# Phase 1 — Establish the Actual Architecture

Build an inventory of the complete workspace.

For each crate, module, and major subsystem, determine:

- Its current responsibility.
- Its actual production entry points.
- Its callers and dependencies.
- Its exported API surface.
- Its internal abstractions.
- Its runtime and compile-time responsibilities.
- Its relationship with overlapping subsystems.
- Whether its responsibility remains necessary.
- Whether it owns genuine policy or merely delegates.
- Whether it could be deleted or absorbed elsewhere.

Construct actual call-path maps for:

1. SQL query admission, planning, execution, and result delivery.
2. Dynamic or typed query admission, planning, execution, and result delivery.
3. Primary-key lookups.
4. Secondary-index lookups.
5. Range queries and composite predicates.
6. Full scans.
7. INSERT, UPDATE, and DELETE.
8. Schema registration, validation, reconciliation, and installation.
9. Index creation and rebuild.
10. Stable-storage access and persistence.
11. Recovery, replay, and publication.
12. Aggregation, DISTINCT, grouping, and pagination.
13. DDL and administrative operations.
14. Generated entity codecs and runtime integration.

Identify where multiple routes eventually perform the same operations through separate mechanisms.

Specifically distinguish:

- Necessary domain separation.
- Performance-motivated specialization.
- Accidental duplication.
- Historical architecture that survived replacement.
- Thin wrappers that add no meaningful invariants.
- Indirection that no longer serves a purpose.

Produce an architecture map showing what exists today.

Then propose what the architecture should look like after consolidation.

---

# Phase 2 — Detect Architectural Drift

Investigate successive generations of design that coexist unnecessarily.

Look for:

- Old approaches retained alongside newer implementations.
- Replacement systems that never completely replaced their predecessors.
- Duplicate ownership of invariants.
- Redundant admission or validation stages.
- Intermediate representations that simply repackage equivalent information.
- Multiple encodings of the same logical state.
- Repeated conversions between equivalent representations.
- Data structures containing derivable or redundant fields.
- Competing abstractions representing the same concept.
- Historical compatibility behavior.
- Repeated canonicalization or normalization.
- Pass-through services that perform no useful work.
- Abstractions designed for hypothetical future capabilities.
- Complex machinery supporting only one remaining implementation.

For every abstraction, ask:

**If we were designing IcyDB from scratch today, knowing everything we now know, would this abstraction exist?**

If not, propose its removal or absorption.

Consider whether earlier architectural decisions were appropriate at the time but are now unnecessary because the surrounding system has changed.

Do not assume the newest design is automatically superior.

Where a newer abstraction adds more complexity than the system it replaced, consider reverting the underlying architectural choice.

---

# Phase 3 — Query Architecture and Execution Consolidation

Perform an especially detailed audit of the query subsystem.

## 3.1 Query admission

Examine the boundary between SQL queries, dynamic queries, typed predicates, and internal representations.

Investigate:

- Duplicate validation.
- Duplicate type coercion.
- Repeated predicate normalization.
- Multiple query-lowering mechanisms.
- Different admission rules expressing equivalent semantics.
- Semantic information being discarded and later reconstructed.
- Repeated allocation or cloning during admission.
- Errors translated through unnecessary intermediate types.

Determine whether SQL and dynamic queries converge onto one sufficiently canonical execution representation.

Identify opportunities for one shared semantic core without introducing an excessively generic query framework.

## 3.2 Query planning

Inspect:

- Access-path selection.
- Cost comparisons.
- Exact-cardinality decisions.
- Tie-breaking.
- Index eligibility.
- Predicate pushdown.
- Conjunction and disjunction planning.
- Index intersections.
- Candidate generation.
- Plan normalization.
- Residual filters.
- Fallback behavior.

Look for duplicate planning decisions at different layers.

Determine whether planners repeatedly calculate information already known during admission.

Investigate whether planner metadata and cost calculations are unnecessarily complicated.

Identify branches that no longer represent meaningful execution choices.

## 3.3 Execution

Examine the execution implementations of:

- ByKey
- ByKeys
- KeyRange
- IndexPrefix
- IndexMultiLookup
- IndexRange
- IndexBranchSet
- FullScan

Use the repository's actual current variants as authoritative.

Identify common traversal, filtering, materialization, ordering, and continuation logic.

Determine whether some access paths can be merged without harming performance.

Conversely, identify generic abstractions that introduce runtime overhead for paths that should remain specialized.

Look particularly for repeated:

- Iterator construction.
- Cursor advancement.
- Filtering.
- Row decoding.
- Record materialization.
- Boundary checking.
- Ordering validation.
- Continuation construction.
- Error conversion.

Propose the smallest coherent execution architecture.

## 3.4 DISTINCT, aggregation, and grouping

Inspect the complete implementations of DISTINCT, GROUP BY, scalar aggregates, global aggregates, and related streaming paths.

Investigate whether they use overlapping state machines or materialization pipelines.

Look for cases where a query can operate over index entries rather than decoded entity rows.

Identify unnecessary collections, buffering, cloning, sorting, and repeated scans.

Keep algorithmic improvements separate from cosmetic consolidation.

## 3.5 Continuations and pagination

Audit continuation construction, encoding, decoding, validation, and resumption.

Check for redundant state, duplicated cursor logic, unnecessary serialized fields, and repeated validation.

Do not weaken continuation correctness or deterministic resumption semantics.

---

# Phase 4 — Schema, Indexes, and Metadata

Audit the schema subsystem from end to end.

Include:

- Schema identity.
- Accepted-schema authority.
- Schema normalization.
- Canonical ordering.
- Schema validation.
- Schema installation.
- Schema reconciliation.
- Expression indexes.
- Composite primary keys.
- Composite index keys.
- Relation targets.
- Stable allocation metadata.
- Index reconstruction.
- Rebuild and migration state.

Look for multiple representations of identical schema information.

Determine whether metadata is stored redundantly when it could be derived safely and cheaply.

Find repeated sorting, hashing, normalization, comparison, allocation, and validation.

Inspect whether schema application follows one canonical flow or several overlapping flows.

Identify generic metadata infrastructure supporting only one actual use case.

Investigate whether persisted and runtime representations are separated appropriately.

Do not recommend deleting persisted information without proving its recoverability and lifecycle invariants.

---

# Phase 5 — Storage, Memory, and Recovery

Audit the storage architecture and its boundaries.

Include:

- Per-store isolation.
- Backend-neutral traversal.
- Heap storage.
- Stable storage.
- Storage adapters.
- Stable-memory allocation.
- Cached and warmed state.
- Entity serialization.
- Index storage.
- Write staging.
- Publication.
- Redo-only recovery.
- Migration and interrupted recovery.

Determine whether the system has too many layers between an execution operation and the underlying storage.

Inspect each adapter and trait.

Ask:

- Does it represent a real capability boundary?
- Does it support multiple actual implementations?
- Does it centralize an important invariant?
- Does it eliminate meaningful duplication?
- Does it introduce avoidable generic complexity?
- Is it merely forwarding calls?
- Could it be absorbed into an adjacent module?

Look for unnecessary cloning, copying, allocation, decoding, re-encoding, and temporary objects.

Identify redundant metadata caches and state transitions.

Pay special attention to the distinction between simplifying storage machinery and preserving crash consistency.

Any recommendation involving recovery or publication must explicitly identify its failure-mode implications.

---

# Phase 6 — Write Pipeline and Transaction Semantics

Trace INSERT, UPDATE, DELETE, batch staging, conflict detection, and publication.

Look for:

- Repeated validation across write operations.
- Duplicate index-update machinery.
- Unnecessary intermediate write representations.
- Multiple staging paths.
- Repeated entity reconstruction.
- Duplicate unique-key and conflict checks.
- Excessive cloning.
- Repeated schema lookups.
- Redundant serialization.
- Overly fragmented transaction state.

Determine whether the write pipeline can share a smaller set of primitives without weakening atomicity or conflict detection.

Inspect whether INSERT and UPDATE contain parallel implementations that should converge.

Identify intermediate state objects that could disappear entirely.

Do not unify operations merely because they look similar if their lifecycle or failure semantics genuinely differ.

---

# Phase 7 — Rust Abstraction and Type-System Audit

This phase should be aggressive.

Examine every significant:

- Trait.
- Generic parameter.
- Associated type.
- Wrapper struct.
- Newtype.
- Enum.
- Builder.
- Factory.
- Adapter.
- Conversion layer.
- Extension trait.
- Shared helper.
- Macro.
- Lifetime-heavy abstraction.

Determine whether each provides enough value to justify its maintenance cost.

Look for traits with one implementation, wrappers used only once, builder patterns for trivial objects, and generic types whose flexibility is never exercised.

These are candidates, not automatic defects.

Also investigate:

- Needless `Arc`, `Rc`, `Box`, and `Cow`.
- Unnecessary `Clone`.
- Redundant `From` and `Into` conversions.
- Excessive allocation.
- Repeated error mapping.
- Generic code that causes Wasm monomorphization growth.
- Large enums with overlapping variants.
- Redundant state and configuration.
- Unnecessary dynamic dispatch.
- Excessively long chains of delegated method calls.

Evaluate whether replacing several abstractions with straightforward concrete Rust would improve clarity and reduce compiled size.

Prefer plain structs, enums, functions, and well-defined modules wherever sufficient.

Avoid over-engineering the proposed replacement.

---

# Phase 8 — Dead Code, Unused Dependencies, and Historical Sediment

Search the entire repository for:

- Unreachable production code.
- Unused functions and modules.
- Dead feature flags.
- Unused Cargo dependencies.
- Unnecessary dependency features.
- Duplicate dependencies providing similar functionality.
- Obsolete test helpers.
- Redundant fixtures.
- Deprecated APIs.
- Unused macros.
- Unnecessary build scripts.
- Duplicate test infrastructure.
- Abandoned implementation experiments.
- Stale configuration.
- Documentation describing removed behavior.
- Compatibility aliases.
- Historical fallback paths.
- TODOs describing work that is already complete.
- Unused generated code.

Do not equate a lack of local call sites with proof of dead code.

Consider public APIs, macros, proc-macro expansion, feature-gated paths, generated entities, external consumers, and integration fixtures.

Inspect relevant sibling repositories where available, particularly IcyDB's schema and runtime consumers.

Classify every proposed deletion as:

1. Proven unreachable or unused.
2. Redundant with another implementation.
3. Only required by an obsolete API.
4. Potentially externally consumed.
5. Uncertain pending additional evidence.

Use static-analysis tools where helpful, but independently verify important findings.

Do not delete code solely because an automated tool reports it unused.

---

# Phase 9 — Codec and Wasm Footprint

Investigate the full code-size cost of IcyDB.

Include:

- Runtime adapters.
- Generated entity codecs.
- Schema-specific generated code.
- Generic monomorphization.
- Serialization libraries.
- Error and formatting machinery.
- Trait implementations.
- Repeated codec operations.
- Reachable dependency graphs.
- Default and optional Cargo features.

Distinguish:

- Source reduction.
- Raw compiled Wasm reduction.
- Optimized Wasm reduction.
- Compressed artifact reduction.
- Per-entity generated code reduction.
- Runtime instruction reduction.

Review earlier codec and runtime-footprint optimization decisions against the current code.

Identify optimizations that could reduce generated adapter surface or repeated monomorphized implementations.

Do not assume removing 1,000 source lines will materially reduce Wasm size.

Where possible, measure actual costs using controlled builds and existing maintained fixtures.

Report exact toolchain, build flags, baseline sizes, and comparison methods.

---

# Phase 10 — Tests and Qualification Infrastructure

Audit the testing strategy itself.

Look for:

- Duplicate integration tests.
- Overlapping fixtures.
- Repeated environment setup.
- Boilerplate test harnesses.
- Tests for discarded architectures.
- Extremely expensive tests with limited distinct coverage.
- Testing layers that verify the same invariant repeatedly.
- Missing negative cases.
- Missing recovery or boundary tests.
- Brittle snapshot expectations.
- Unnecessary ignored tests.
- Qualification that depends on undocumented manual steps.

Identify which tests can be merged, removed, parameterized, or replaced.

However, do not sacrifice meaningful coverage to reduce test line count.

Distinguish duplicated test code from duplicated test coverage: independent coverage across different boundaries may be intentional.

Recommend one coherent qualification structure covering:

- Unit semantics.
- Integration behavior.
- Storage and recovery.
- Schema transitions.
- Generated entity behavior.
- Performance regressions.
- Wasm size regressions.
- PocketIC execution and cycle measurements.

Highlight missing tests needed to make proposed simplifications safe.

---

# Phase 11 — Cross-Cutting Duplicate Flow Detection

This phase is particularly important.

Do not just search for duplicated text.

Find **semantic duplication**.

Examples include:

- Two modules implementing equivalent admission rules.
- Multiple functions constructing equivalent key ranges.
- Several execution routes implementing identical residual filtering.
- Different subsystems canonicalizing the same metadata.
- Multiple serializers encoding equivalent logical objects.
- Repeated row materialization.
- Independent caches holding the same information.
- Separate loops performing identical traversal with slightly different types.
- Several error types representing the same failure.
- Different state machines implementing identical lifecycle transitions.
- Multiple implementations of equivalent ordering or comparison logic.

For each cluster:

1. Identify every implementation.
2. Explain why they are semantically equivalent or where they differ.
3. Determine whether their differences are intentional.
4. Identify the smallest shared ownership model.
5. Estimate code that could be deleted.
6. Estimate additional abstractions required.
7. Determine whether consolidation creates performance regressions.
8. Recommend retain, merge, specialize, or delete.

**Do not recommend extracting a common helper if doing so increases overall complexity.**

Sometimes the correct solution is to eliminate one entire flow.

---

# Phase 12 — Complexity Hotspots

Identify the twenty most complex areas of the repository.

Use multiple indicators:

- Function size.
- Branching complexity.
- State-machine complexity.
- Number of responsibilities.
- Dependency fan-in and fan-out.
- Generic type complexity.
- Repeated transformations.
- Number of representations.
- Error-path complexity.
- Test setup difficulty.
- Frequency of changes.
- Architectural coupling.

For each hotspot, explain why it is complicated.

Distinguish:

- Essential complexity dictated by database semantics.
- Accidental complexity caused by architecture.
- Complexity introduced by over-abstraction.
- Complexity caused by historical compatibility.
- Complexity that belongs at a different layer.

Propose the simplest viable design.

Do not recommend splitting a large file into smaller files unless doing so genuinely improves ownership and coupling.

Moving code around is not itself a simplification.

---

# Phase 13 — Adversarial Architecture Review

Now critique the architecture as though it were a new database proposal.

Challenge the assumptions behind the current design.

Ask:

1. Do we have too many layers?
2. Are we representing the same information too many times?
3. Is our query planner more complicated than its actual requirements?
4. Are our storage abstractions justified by real backends?
5. Are some optimizations no longer valuable?
6. Are some execution paths over-specialized?
7. Are generic frameworks obscuring straightforward algorithms?
8. Are errors and diagnostics excessively fragmented?
9. Are schema and runtime responsibilities distributed correctly?
10. Are important invariants enforced in one place?
11. Can two or more substantial subsystems be merged?
12. Can an entire stage of the query pipeline disappear?
13. Are we optimizing for hypothetical future features?
14. Have we accidentally built several databases inside one repository?
15. What would we refuse to rebuild if starting over today?

Do not defend the existing architecture simply because it is already implemented.

Also challenge the proposed simplified architecture.

Explain which apparent redundancy is actually necessary.

The objective is not minimal code at any price.

It is the smallest architecture that correctly and efficiently delivers IcyDB's intended capabilities.

---

# Phase 14 — Quantified Simplification Opportunities

Produce a prioritized catalogue of concrete changes.

Every finding must have:

- Unique identifier.
- Severity or priority.
- Affected crates and modules.
- Exact symbols and paths.
- Current implementation.
- Evidence of the problem.
- Proposed replacement or deletion.
- Dependencies on other changes.
- Estimated implementation effort.
- Estimated source-line reduction.
- Estimated change in architectural complexity.
- Expected Wasm impact.
- Expected runtime impact.
- Correctness and recovery risks.
- Required tests.
- Confidence level.

Use these impact categories:

**A — Entire subsystem deletion**

A substantial module, execution flow, or architectural layer can disappear.

**B — Major consolidation**

Several implementations become one coherent mechanism.

**C — Abstraction elimination**

Unnecessary traits, wrappers, generics, adapters, or intermediate representations disappear.

**D — Algorithmic simplification**

A complex algorithm or state machine is replaced by a smaller equivalent implementation.

**E — Allocation and execution efficiency**

Unnecessary cloning, decoding, buffering, materialization, or repeated work is removed.

**F — Dead code and dependency removal**

Unused or obsolete machinery is deleted.

**G — Test and tooling consolidation**

Duplicated qualification infrastructure is simplified.

**H — Architectural correction**

A larger redesign would materially improve the structure but requires greater validation.

### Quantification rules

For each proposal, estimate:

| Metric | Requirement |
|---|---|
| Source LOC | Net deleted minus added |
| Modules | Removed, merged, or added |
| Types/traits | Net reduction |
| Dependencies | Removed or simplified |
| Wasm bytes | Measured where possible, otherwise explicitly unknown |
| Instructions/cycles | Measured where possible, otherwise explicitly unknown |
| Runtime allocations | Measured or identified, not invented |
| Effort | Engineering days or bounded complexity |
| Risk | Low / Medium / High / Critical |
| Confidence | High / Medium / Low |

Account for the fact that findings may overlap.

Never sum overlapping estimated reductions as though they were independent.

Prefer estimates based on inspecting actual affected code rather than arbitrary percentages.

Where benchmarks cannot be run, describe the exact measurement required.

---

# Phase 15 — Build the Target Architecture

Based on the findings, design a coherent simplified IcyDB architecture.

Include:

- Proposed crate boundaries.
- Major module ownership.
- Query admission and normalization.
- Planner responsibilities.
- Execution responsibilities.
- Storage responsibilities.
- Schema ownership.
- Write pipeline.
- Persistence and recovery.
- Codec boundaries.
- Runtime integration.

Provide two diagrams:

**Current architecture:** What exists now, including duplication and unnecessary transitions.

**Target architecture:** The proposed minimum coherent design.

Identify which types, traits, modules, and flows disappear.

Explain why the target architecture is simpler.

Estimate which areas become easier to test, optimize, extend, and maintain.

Do not design an ambitious replacement framework that merely moves complexity into new abstractions.

---

# Phase 16 — Reduction Programme

Construct an implementation roadmap.

Group findings into coherent, bounded release slices.

Prefer:

1. Proven dead-code and dependency removal.
2. Redundant validation and conversion elimination.
3. Safe abstraction consolidation.
4. Query-path consolidation.
5. Schema and metadata simplification.
6. Write and storage simplification.
7. Larger architectural hard cuts.
8. Additional performance-focused reductions.

This is only a suggested ordering. Use actual dependencies and risk to determine the final sequence.

Each slice must:

- Have a clearly named objective.
- Identify exact deletion/consolidation targets.
- Have a measurable code delta.
- Have a bounded scope.
- Avoid unrelated refactors.
- State prerequisites.
- Preserve defined invariants.
- Have focused qualification.
- Specify rollback or recovery implications where relevant.
- Have measurable acceptance criteria.

Where possible, create **negative-code releases**: releases that predominantly delete code while preserving behavior.

Do not combine unrelated changes merely to produce impressive deletion numbers.

For changes to persisted schema, keys, recovery records, or stable-memory structures, explicitly identify whether a durable migration is required.

A hard cut to Rust APIs is acceptable.

An unplanned hard cut to durable storage is not.

---

# Required Deliverables

Produce the following reports.

## 1. Executive Summary

Include:

- Overall architectural health.
- Degree of duplication and drift.
- Most serious complexity sources.
- Five largest consolidation opportunities.
- Five most valuable low-risk deletions.
- Estimated reduction potential.
- Principal risks.
- Recommended direction.

## 2. Complete Findings Register

Rank findings by:

- Expected simplification.
- Practical benefit.
- Risk.
- Implementation cost.
- Confidence.

Provide exact code references.

## 3. Duplicate Architecture Map

Show which modules implement overlapping responsibilities and which should become authoritative.

## 4. Dead Code and Dependency Report

Identify deletion candidates and the evidence supporting them.

## 5. Complexity Hotspot Report

Rank the twenty most problematic areas with concrete proposals.

## 6. Current vs Target Architecture

Include diagrams and a substantive explanation of the desired boundaries.

## 7. Prioritized Hard-Cut Roadmap

Provide a dependency-aware series of implementation slices, with per-slice cost and expected benefit.

## 8. Measurement and Qualification Plan

Explain how to establish reliable baselines for:

- Source complexity.
- Raw Wasm size.
- Optimized Wasm size.
- Per-entity generated footprint.
- Heap allocations.
- Stable-memory operations.
- Instruction consumption.
- Query latency.
- Recovery behavior.

Use matched workloads, equivalent fixtures, and identical build configurations wherever comparisons are made.

## 9. Delete-First Recommendations

Identify the changes that remove the most complexity with the least replacement code.

For each one, explain:

- What disappears.
- Why it is safe.
- What remains authoritative.
- What tests prove the change.
- What measurable benefit to expect.

## 10. Architectural Verdict

Conclude with clear answers:

**A. What percentage of IcyDB's current implementation is plausibly removable or consolidatable?**

Provide a bounded estimate and describe its uncertainty. Do not manufacture precision.

**B. Which five architectural decisions create the most unnecessary complexity today?**

**C. Which three major subsystems would you redesign if starting fresh?**

**D. What should we explicitly retain, even though it appears complicated?**

**E. What is the smallest credible end-state architecture?**

**F. What are the first five release slices you would implement, and why?**

---

# Execution Strategy

Work systematically through the repository.

Start by establishing the actual architecture and repository-wide inventory.

Then inspect each subsystem in depth.

Use code search, dependency analysis, call-site tracing, compilation evidence, and targeted tests as appropriate.

Verify high-impact findings against actual implementations.

Do not rely solely on filenames, grep results, unused-code warnings, or documentation.

Do not mistake a complicated but essential invariant for unnecessary engineering.

Do not recommend removing legitimate performance specializations without comparing their benefits.

Where a finding depends on an unverified assumption, say so clearly.

If the context window or available time prevents complete coverage, record exactly which modules have and have not been reviewed, preserve the findings, and identify remaining work. Do not claim exhaustive coverage when it has not happened.

**Do not modify production code during this audit.**

Produce the audit report and implementation roadmap. A dedicated report under `docs/audits/` is appropriate if consistent with the repository's documentation conventions.

Finish by presenting the ten highest-value findings and recommending the first implementation slice.

Then stop.

Do not begin implementation without explicit authorization.

## Final Directive

IcyDB has been through many iterations, and architectural drift is expected.

We are deliberately looking for opportunities to remove substantial amounts of code.

Be skeptical of every abstraction.

Challenge duplicated ownership.

Challenge historic design decisions.

Challenge unnecessary generality.

Prefer one correct implementation over multiple similar implementations.

Prefer simple, explicit Rust over abstraction for abstraction's sake.

Prefer deleting an obsolete system over cleaning it up.

**The ideal outcome is a smaller, faster, easier-to-understand IcyDB that retains the full capabilities and reliability of the current database, with fewer moving parts and fewer ways for its subsystems to disagree.**