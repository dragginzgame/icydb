# Velocity Preservation Governance

This document defines release-engineering rules that protect feature velocity.

The recurring `velocity-preservation` audit measures change cost after the
fact.

These rules turn the highest-signal audit findings into enforceable process and
CI guardrails.

---

# 1. Purpose

Velocity degrades when routine feature work lands as one wide cross-layer
bundle.

The goal of this document is to keep routine work:

- locally scoped
- layer-bounded
- predictable to review
- cheap to extend in follow-up patches

These rules are intended to be followed by automated agents and by CI checks.

---

# 2. Release Engineering Rule #1

A routine feature change may span at most two primary delivery domains unless
the author provides an explicit slice override.

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

# 3. Slice Shape Limits

Routine feature pull requests should satisfy:

- soft file-count limit: `<= 15`
- hard file-count limit: `<= 25`
- max primary domains touched: `<= 2`

Interpretation:

- `<= 15` changed files is the target for healthy routine work.
- `16..25` files is allowed but should be treated as a warning band.
- `> 25` files requires an explicit override.
- touching more than two primary domains requires an explicit override even if
  file count stays below the hard limit.

Docs-only or governance-only edits are not the primary target of this rule.
They may still trip the hard limit mechanically, but should be rare and can
use the documented override path.

---

# 4. Slice Override Contract

If a pull request exceeds the hard file limit or the domain limit, it must
include both of these trailer lines in the PR body:

`Slice-Override: yes`

`Slice-Justification: <why the cross-layer change is unavoidable>`

Rules:

- `Slice-Override: yes` is required exactly when limits are exceeded.
- `Slice-Justification:` must be non-empty.
- CI may fail wide PRs that do not include both lines.
- CI should print override usage so later audits can track override frequency.

This is an escape hatch, not a default workflow.

---

# 5. Canonical SQL Landing Pattern

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

- `crates/icydb-build/src/db.rs`
- `canisters/**`
- `testing/**`

Allowed:

- generated actor/build wiring
- bootstrap changes
- canister harness changes
- integration harness expansion

This phased landing pattern is the default for routine SQL growth.

If one pull request needs to cross all three phases, use the slice override
contract and explain why the split was not practical.

---

# 6. Route Planner Controlled Hub Rule

`crates/icydb-core/src/db/executor/route/planner/mod.rs` is a controlled hub.

Rules:

- do not add direct `sql::*` imports there
- do not add direct `session::*` imports there
- do not increase the number of top-level `db::*` import families casually
- new route features should enter through:
  - `route/planner/entrypoints/*`
  - `route/planner/feasibility/*`
  - `route/planner/execution/*`
- avoid pushing new semantic branching back into `route/planner/mod.rs`

The root planner module is allowed to coordinate existing route-owned contracts.
It is not the place to absorb unrelated frontend or session concerns.

---

# 7. Root Module Re-Centralization Guard

The repository should not silently re-aggregate logic into high-level module
roots after a split.

Guarded roots:

- `crates/icydb-core/src/db/sql/parser/mod.rs`
- `crates/icydb-core/src/db/session/sql/mod.rs`

Rule:

- adding more than approximately `200` lines to one guarded root in one change
  requires an explicit slice override or a follow-up split in the same change

This rule is about new accretion, not the historical size of the file.

---

# 8. Enum Shock Radius Guidance

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

# 9. CI Enforcement

CI should enforce at least these checks:

- slice-shape gate for PRs
- route-planner import boundary guard

The slice-shape gate should:

- count changed files
- classify touched primary domains
- fail wide PRs without the required override trailers
- print metrics and override usage for audit tracking

The route-planner guard should:

- fail on direct `sql::*` or `session::*` imports in the planner root
- fail if planner-root import families exceed the configured ceiling

