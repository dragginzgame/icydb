# RECURRING AUDIT - Cross-Cutting Layer Violations

## Scope

`icydb-core (crates/icydb-core/src/db/)`

## Purpose

Detect semantic layer violations and authority drift that:

- Do **not** appear as upward imports.
- Do **not** break dependency direction rules.
- Duplicate or re-derive logic outside its owning layer.
- Increase drift sensitivity.
- Multiply invariant enforcement sites.
- Create cross-layer semantic coupling.

## This Audit Targets

- Authority duplication
- Policy re-derivation
- Comparator leakage
- Invariant spread
- Capability fan-out
- Cross-layer semantic bleed

## This Is NOT

- A strict dependency audit
- A style audit
- A redesign proposal
- A layer-merging recommendation

## Layer Direction Model (Reference)

`intent -> query/plan -> access -> executor -> index/storage -> codec`

This audit assumes strict import direction is already enforced.

We are checking semantic authority, not imports.

## Authority Ownership Matrix

The following concerns must have a single owning layer:

| Concern | Sole Owner Layer |
| --- | --- |
| Query shape semantics | intent/query |
| Plan legality + validation | query/plan |
| Access feasibility + canonicalization | access |
| Route capability + execution strategy | executor/route |
| Runtime execution semantics | executor |
| Ordering domain + key comparison | index |
| Envelope containment | index |
| Continuation bound rewriting | index |
| Cursor signature + shape validation | cursor |
| Persistence + durability | commit/storage |
| Wire encoding/decoding | codec |

If logic for a concern appears outside its owner, it is a cross-cutting violation candidate.

## STEP 1 - Policy Re-Derivation Scan

Search for policy predicates implemented in more than one layer.

Examples:

- grouped + distinct legality
- order-required constraints
- limit-required constraints
- continuation eligibility
- access-path eligibility
- pushdown feasibility
- HAVING gating

For each policy predicate found in more than one module, produce:

| Policy | Files | Owner Layer | Non-Owner Layers | Drift Risk | Risk Level |
| --- | --- | --- | --- | --- | --- |

Classify drift risk:

- Low (delegates to owner)
- Medium (logic duplicated but equivalent)
- High (logic differs subtly)

Do **not** recommend merging layers.

If consolidation is safe, recommend extracting a shared helper inside the owner layer only.

## STEP 2 - Ordering Authority Leakage

Ordering semantics must be owned exclusively by `index/*`.

Search for:

- `.cmp(` on index key components
- tuple comparisons for index keys
- lexicographic ordering logic
- manual key comparisons
- envelope containment logic
- raw bound construction

outside:

- `db/index/*`

Produce:

| Comparator Logic | File | Owner Layer | Violation Type | Risk |
| --- | --- | --- | --- | --- |

Violation types:

- Direct duplication
- Partial reimplementation
- Thin wrapper
- Legitimate delegation

Flag any comparator logic outside `index` as High Risk unless it delegates directly.

## STEP 3 - Continuation Authority Leakage

Continuation rewrite + advancement must be owned by `index/envelope.rs` and `index/scan.rs`.

Search for:

- Excluded-anchor rewrite logic
- Bound conversion logic
- Strict-advance comparisons
- Envelope containment re-checks
- Anchor comparisons

outside:

- `index/*`
- `cursor/*`

Produce:

| Logic | File | Owner | Duplicate? | Risk |
| --- | --- | --- | --- | --- |

If rewrite or advancement logic appears in more than two locations, flag as divergence-prone.

## STEP 4 - Access Capability Fan-Out

Access feasibility must be owned by `access/*` and `executor/route/*`.

Search for:

- `AccessPath::` match trees
- `is_*eligible` helpers
- pushdown gating
- capability flags
- route strategy branching

Count:

- Number of match sites on `AccessPath`
- Number of route capability predicates
- Number of execution-mode branches

Produce:

| Enum / Capability | Match Sites | Layers Involved | Fan-Out Risk |
| --- | --- | --- | --- |

Fan-out risk:

- Low (`<=2` layers)
- Medium (`3` layers)
- High (`4+` layers)

## STEP 5 - Invariant Enforcement Spread

For each invariant:

- Envelope containment
- Strict advancement
- Unique enforcement
- Reverse symmetry
- Commit marker lifecycle
- Cursor signature compatibility

Identify enforcement locations.

Produce:

| Invariant | Locations | Owner | Defensive? | Drift Risk |
| --- | --- | --- | --- | --- |

Classification:

- Safety-enhancing redundancy
- Safety-neutral duplication
- Divergence-prone duplication

Flag invariants enforced in more than three sites.

## STEP 6 - Error Classification Cross-Layer Drift

Search for:

- Multiple conversions of the same error type
- Same logical error mapped to different `ErrorClass`
- Same invariant failure surfaced differently in different layers

Produce:

| Error Concept | Mapping Sites | Class Differences? | Risk |
| --- | --- | --- | --- |

## STEP 7 - Semantic Fan-Out Metric

Count:

- Enums matched in `>=3` modules
- Policy predicates implemented in `>=3` modules
- Invariants enforced in `>=3` modules
- Continuation/anchor logic references outside cursor/index

Produce:

| Surface | Count | Risk Level |
| --- | --- | --- |

## STEP 8 - Legitimate Cross-Cutting (Do NOT Merge)

Explicitly identify duplication that is protective.

Examples:

- Planner validation + executor revalidation
- Cursor validation + scan guard
- Forward mutation + replay mutation
- Marker authority + recovery authority

Produce:

| Area | Why Redundant | Risk If Merged |
| --- | --- | --- |

## STEP 9 - Output Summary

Produce:

- High-Risk Cross-Cutting Violations (semantic drift likely)
- Medium-Risk Drift Surfaces (control-plane multiplication)
- Low-Risk / Intentional Redundancy (boundary protection)
- Quantitative Snapshot

Quantitative snapshot fields:

- Policy duplications found: `N`
- Comparator leaks: `N`
- Capability fan-out >2 layers: `N`
- Invariants enforced in >3 sites: `N`
- Protective redundancies: `N`
- Cross-Cutting Risk Index (1-10)

Interpretation:

- `1-3` = Authority clean and centralized
- `4-6` = Moderate semantic spread, manageable
- `7-8` = High drift risk, requires consolidation
- `9-10` = Authority fragmentation

## Anti-Shallow Requirement

Do **not**:

- Recommend merging architectural layers
- Recommend collapsing validation into execution
- Suggest global util modules
- Suggest public API changes
- Comment on naming, formatting, or macros
- Propose speculative redesign

Every violation must include:

- Location
- Concern
- Owner layer
- Drift classification
- Risk level
