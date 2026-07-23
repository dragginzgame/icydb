# Velocity Preservation Governance

This document defines release-engineering rules that protect feature velocity.

The recurring `velocity-preservation` audit measures future extension friction
from the current codebase. It asks which owner boundaries, hubs, and decision
surfaces will make future feature work harder unless they are organized better.

The slice-shape rules in this document are separate delivery guardrails. They
measure PR/review width and landing discipline; they do not define the
recurring audit score.

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

The recurring audit owns the forward-looking code-structure score. These rules
describe code-review and landing slices. One planned minor-line patch is one
reviewable landing patch and worktree handoff. A landing patch does not acquire
a version number until the user names a release target, but agents must not use
that distinction to accumulate multiple planned patches in one worktree batch.

These rules are intended to guide automated agents and code review.

---

# 2. Minor-Line Patch Contract

Before implementation begins, the design/status tracker for a minor-version
line must group the whole intended line into roughly 6-8 ordered landing
patches. Multiple design documents in the same minor line share this total;
they do not each receive a separate 6-8-patch allowance.

The normal planning range is 5-9 patches. Fewer than 5 or more than 9 requires
an explicit user agreement and a brief explanation in the tracker. The target
is intentionally approximate: it exists to prevent both dozens of tiny pushes
and one or two multi-hour mega-slices.

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

If honest patches cannot keep the minor line near the target range and the
limits below, re-scope the minor line or agree a different patch count with the
user. Do not make each patch wider to preserve an oversized plan, and do not
manufacture micro-patches solely to hit a number.

A completed landing patch is normally handed back as a candidate push for the
next patch release in the minor line. Agents must not invent release numbers;
the user decides the exact target and whether a particular handoff is pushed.

---

# 3. Release Engineering Rule #1

A routine feature change may span at most two primary delivery domains unless
the user explicitly approves a slice override before implementation crosses
that boundary.

Primary delivery domains:

- `Parser`
- `Lowering / Session`
- `Executor / Planner`
- `Build / Canister`
- `Integration Tests`

Unclassified core runtime files count as `Other Core` until they are assigned
to a more specific domain by rule.

This rule exists to prevent routine work from combining:

- frontend grammar changes
- semantic lowering/runtime changes
- generated-canister glue
- deployment wiring
- integration-harness expansion

into one large landing slice by default.

---

# 4. Slice Shape Limits

Each landing patch must satisfy:

- soft file-count limit: `<= 15`
- hard file-count limit: `<= 25`
- max primary domains touched: `<= 2`

Interpretation:

- `<= 15` changed files is the target for healthy routine work.
- `16..25` files is allowed but should be treated as a warning band.
- `> 25` files requires an explicit user-approved override obtained before the
  slice is widened.
- touching more than two primary domains requires the same explicit approval
  even if file count stays below the hard limit.

Docs-only or governance-only edits are not the primary target of this rule.
They may still trip the hard limit mechanically, but should be rare and require
the same documented override path.

---

# 5. Wide Slice Review

If a projected landing patch exceeds the hard file limit or the domain limit,
the agent must split it or stop and obtain explicit user approval before making
the wider edit. The agent cannot grant its own override based on coherence,
atomicity, convenience, or the cost of another compile.

When the user approves an override, its explanation belongs in the patch/PR
summary, not in a special trailer format.

Rules:

- call out the domains that changed;
- explain why the cross-layer change is unavoidable or cheaper to review as one
  unit;
- keep follow-up cleanup work separate unless it is needed for correctness.

CI may not enforce every limit, but the limits remain agent execution rules.

---

# 6. Canonical SQL Landing Pattern

New SQL feature work should land in three phases whenever practical.

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

- `crates/icydb-build/src/db/`
- `canisters/**`
- `testing/**`

Allowed:

- generated actor/build wiring
- bootstrap changes
- canister harness changes
- integration harness expansion

This phased landing pattern is the default for routine SQL growth.

If one landing patch needs to cross all three phases, stop and use the explicit
user-approved slice override contract before making the cross-phase edits.

These phases should land as separate reviewable slices. They do not acquire
separate version numbers unless the user chooses to publish them separately.

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
  requires an explicit user-approved slice override or a split before handoff

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
