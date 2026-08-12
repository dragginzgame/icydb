# Velocity Preservation Governance

This document defines release-engineering rules that protect feature velocity.

The recurring `complexity-and-technical-debt` audit measures future extension
friction from the current codebase. It asks which owner boundaries, state axes,
and decision surfaces make maintained work harder than necessary.

The slice-shape rules in this document are separate delivery guardrails. They
measure PR/review width and landing discipline; they do not define the
recurring audit findings.

---

# 1. Purpose

Future velocity degrades when routine feature work must cross unclear owner
boundaries, mixed-purpose hubs, or decision surfaces spread across unrelated
layers.

Delivery discipline also matters: routine work becomes hard to review when it
lands as one wide cross-layer bundle.

The goal of this document is to keep routine work:

- locally scoped
- layer-bounded
- predictable to review
- cheap to extend in follow-up patches

The recurring audit owns forward-looking structure and debt assessment. These
rules describe code-review and landing slices. One planned minor-line patch is
one reviewable landing patch and worktree handoff. A landing patch does not
acquire a version number until the user names a release target, but agents must
not use that distinction to accumulate multiple planned patches in one worktree
batch.

These rules are intended to guide automated agents and code review.

---

# 2. Minor-Line Patch Contract

Before implementation begins, the design/status tracker for a minor-version
line must group the whole intended line into 1-12 ordered landing patches.
There is no preferred count within that range: use the number justified by the
actual dependency and review boundaries. Multiple design documents in the same
minor line share the same 12-patch maximum.

The range prevents both dozens of tiny pushes and an unreviewable multi-hour
mega-slice. It is not a reason to split one coherent outcome or combine
independent outcomes.

Each landing patch must name:

- its canonical owner and bounded outcome;
- the delivery domains it expects to touch;
- its focused validation boundary; and
- any public, persisted-format, performance, or wasm-size impact it must report.

Each patch must be a substantive, end-to-end review unit. Include the direct
tests, diagnostics, documentation, fixtures, exhaustive-match propagation, and
warning cleanup caused by its bounded outcome. Those are not separate patches.
Conversely, do not combine independent owners or independently reviewable
outcomes merely to reduce the patch count.

One planned landing patch is the default maximum for one agent implementation
turn.
After completing its code, focused validation, status update, and root
`Unreleased` note, the agent stops and hands the landing patch back for review.
It does not start the next planned patch in the same turn.

Continuation language is deliberately bounded:

- `continue`, `keep going`, and `next` mean exactly the next planned landing
  patch within the current minor-version line;
- a statement that the previous patch is live plus `continue` also means one
  next planned landing patch in the same minor line; and
- combining multiple landing patches requires the user to name them and ask for
  them together.

Generic continuation never crosses a minor-version boundary. When no planned
implementation patch remains, it means closeout/readiness work for the current
minor: begin with a read-only audit and report its findings before making
closeout corrections. Approved corrections remain in that line. The closeout
audit itself does not consume a landing patch unless it produces a code
correction.

A different minor may begin only after:

1. the current minor has a reported ready/complete closeout verdict; and
2. after that verdict, the user explicitly names the target minor and directs
   the agent to start it, for example `start 0.212`.

Do not infer that authorization from a roadmap, an existing next design, an
empty tracker, a clean worktree, a successful push, or status questions such
as `what is next?`, `are we done?`, or `push?`.

If the line needs more than 12 honest patches, re-scope it and explain the new
boundary in the tracker. Do not make each patch wider to preserve an oversized
plan, and do not manufacture micro-patches solely to hit a number.

A completed landing patch is normally handed back as a candidate push for the
next patch release in the minor line. Agents must not invent release numbers;
the user decides the exact target and whether a particular handoff is pushed.

---

# 3. Delivery-Domain Reporting

Primary delivery domains:

- `Parser`
- `Lowering / Session`
- `Executor / Planner`
- `Build / Canister`
- `Integration Tests`

Unclassified core runtime files count as `Other Core` until they are assigned
to a more specific domain by rule. Record the domains touched at handoff so
cross-layer work is visible during review.

Crossing several domains is justified when one planned end-to-end outcome
requires direct propagation through them. It is not a reason to stop for a
mechanical approval. It is evidence that the agent should check whether the
work has accidentally combined independent outcomes such as:

- frontend grammar changes
- semantic lowering/runtime changes
- generated-canister glue
- deployment wiring
- integration-harness expansion

into one landing slice. If it has, split or update the tracker before
implementing the additional outcome.

---

# 4. Slice Shape Signals

There is no file-count or delivery-domain execution limit. A coherent hard cut
may legitimately touch many declarations, generated fixtures, tests, and
documentation files while still owning one reviewable architectural outcome.

At handoff, report:

- files touched and approximate line delta;
- primary delivery domains crossed;
- whether the width is semantic or mechanical propagation; and
- whether implementation structure became simpler, stayed neutral, or became
  more complex.

File count alone must not manufacture micro-patches or interrupt direct
propagation. Conversely, a low file count does not justify combining two
independent planned outcomes.

---

# 5. Wide Slice Review

A wide patch remains one landing patch only when every changed file is required
by the same planned owner and outcome. Direct compile fallout, exhaustive
matches, focused tests, documentation, fixtures, and mechanical propagation
belong to that patch without another approval round.

Stop and split or update the tracker when the work reveals a new production
behavior, canonical owner, or independently reviewable outcome. Width,
atomicity, convenience, or the cost of another compile cannot justify folding
that additional outcome into the active patch.

Rules:

- call out the domains that changed;
- explain why the cross-layer change is unavoidable or cheaper to review as one
  unit;
- keep follow-up cleanup work separate unless it is needed for correctness.

CI does not use file count as a proxy for architectural coherence.

---

# 6. Canonical SQL Landing Pattern

Use the following three phases as planning and review lenses for new SQL
feature work. They identify distinct owners and validation needs; they are not
mandatory patch boundaries.

## Phase A — Parser Slice

Primary scope:

- `crates/icydb-core/src/db/sql/parser/**`

Allowed:

- parser AST/types
- parser tests

Forbidden by default:

- lowering changes
- session runtime wiring
- canister/build changes

## Phase B — Lowering / Session Slice

Primary scope:

- `crates/icydb-core/src/db/sql/lowering/**`
- `crates/icydb-core/src/db/session/sql/**`

Allowed:

- semantic lowering
- runtime dispatch
- explain/runtime parity work

Forbidden by default:

- generated canister glue
- canister/bootstrap wiring
- deployment-surface integration changes

## Phase C — Integration / Build / Canister Slice

Primary scope:

- `crates/icydb-model/src/build/actor/db/`
- `canisters/**`
- `testing/**`

Allowed:

- generated actor/build wiring
- bootstrap changes
- canister harness changes
- integration harness expansion

One landing patch may cross all three phases when they are direct propagation
of the same planned end-to-end outcome. If the phases expose independently
reviewable outcomes, split them into separate landing patches instead of using
cross-phase width to hide a mega-slice.

---

# 7. Route Planner Controlled Hub Rule

`crates/icydb-core/src/db/executor/planning/route/planner/mod.rs` is a controlled hub.

Rules:

- do not add direct `sql::*` imports there
- do not add direct `session::*` imports there
- do not increase the number of top-level `db::*` import families casually
- new route features should enter through:
  - `planning/route/planner/entrypoints.rs`
  - `planning/route/planner/feasibility/*`
  - `planning/route/planner/execution/*`
- avoid pushing new semantic branching back into
  `planning/route/planner/mod.rs`

The root planner module is allowed to coordinate existing route-owned contracts.
It is not the place to absorb unrelated frontend or session concerns.

---

# 8. Root Module Re-Centralization Guard

The repository should not silently re-aggregate logic into high-level module
roots after a split.

Guarded roots:

- `crates/icydb-core/src/db/sql/parser/mod.rs`
- `crates/icydb-core/src/db/session/sql/mod.rs`

Rule:

- adding more than approximately `200` lines to one guarded root in one change
  is a mandatory review signal: localize the logic where practical, explain
  unavoidable root growth at handoff, and split it only when the growth
  represents another independently reviewable outcome

This rule is about new accretion, not the historical size of the file.

---

# 9. Enum Shock Radius Guidance

Before adding a new variant to a widely used decision enum, evaluate whether:

- the change will require edits in more than three modules
- a strategy table or owner-local dispatch helper would localize the change

This is especially important for:

- `AggregateKind`
- `AccessPath`
- route-shape and continuation enums that cross subsystem boundaries

The goal is to keep new feature growth owner-local instead of multiplying
switch-site edits across the tree.

---

# 10. CI Enforcement

CI should enforce the route-planner import boundary guard through the
layer-authority invariant gate.

The route-planner guard should:

- fail on direct `sql::*` or `session::*` imports in the planner root
- fail if planner-root import families exceed the configured ceiling
