# Simplicity And Maintainability Governance

## Purpose

This document governs scope creep, incidental complexity, duplicated flows,
and technical debt. It applies to design, implementation, review, testing, and
audit work.

The objective is not minimum line count or maximum reuse. It is the smallest
maintained state space that satisfies demonstrated product and safety needs
with clear ownership.

Delivery boundaries remain governed by `velocity-preservation.md`. Architecture
authority remains governed by project contracts such as accepted-schema
authority and planner artifact discipline. This document supplies the decision
gate used before those mechanisms are extended.

## Core Rule

Avoid scope creep and incidental complexity; prioritise simplicity and
maintainability.

Prefer, in order:

1. delete an obsolete path;
2. reuse an existing authority or flow;
3. narrow an existing surface;
4. change a safe default or derive behavior automatically;
5. extend an existing owned contract; and
6. add a new concept only when the preceding options are insufficient.

Do not treat a plausible capability, audit observation, design backlog item, or
testable combination as a requirement. The no-build outcome is always valid.

## No-Build Gate

Before adding a new behavior axis, answer:

1. What demonstrated user, correctness, safety, or measured performance problem
   exists?
2. What is the smallest outcome that resolves it?
3. Can an existing authority, contract, route, or default resolve it?
4. What simpler alternative was considered, and why is it insufficient?
5. Which layer is the canonical owner?
6. What independent states or combinations does the proposal add?
7. Which existing path becomes simpler or disappears?
8. Is the ongoing maintenance cost proportionate to the demonstrated benefit?

If these answers are absent, remain in investigation or design. Do not begin
implementation merely because the change is feasible.

The gate is a reasoning requirement, not a form. A short paragraph is enough
for a narrow change. A new persisted state machine or public execution mode
requires explicit design evidence.

## State-Space Delta

An independent axis is a choice that can combine with other maintained choices
and therefore multiply behavior that implementation, tests, recovery, and
users must understand.

Common axes include:

- public modes, modifiers, endpoints, and result variants;
- configuration options and default-policy choices;
- planner routes, execution strategies, and fallback families;
- persisted phases, statuses, versions, checkpoints, and recovery outcomes;
- cursor, protocol, replay, and artifact formats;
- authorization and visibility states;
- widely consumed enum variants; and
- separate SQL, Fluent, generated, facade, or runtime semantic paths.

For every added axis, report:

- the axis and its admitted values;
- which existing axes it can combine with;
- where invalid combinations are rejected or made unrepresentable;
- the canonical decision owner; and
- whether another axis or path was removed.

Do not multiply counts into a headline number when combinations are constrained
or inapplicable. The purpose is to expose interactions and ownership, not to
manufacture a score.

Tests, diagnostics, documentation, fixtures, exhaustive matches, and mechanical
API propagation do not create independent product states by themselves. They
still contribute maintenance cost and must remain proportionate, but they must
not be used to split one semantic outcome into artificial patches.

## Canonical Authority And Converged Flow

Equivalent behavior should have one semantic owner and converge on one internal
execution contract as early as practical.

The maintained pattern is:

```text
owner derives -> contract carries -> consumers project
```

Frontends may remain distinct when they serve different users, but they should
lower into shared semantic and runtime contracts. Planner, runtime, diagnostics,
and `EXPLAIN` must not independently rediscover the same policy.

When a downstream consumer needs more information, extend the owner-carried
artifact. Do not create a parallel classifier, infer policy from partial shape,
or reparse an earlier representation.

Similar code is not automatically debt. Retain duplication when it:

- enforces a boundary independently;
- fails closed at a separate trust boundary;
- preserves recovery or corruption containment; or
- is a measured hot-path specialization whose convergence would regress cost.

Any retained duplication must name that reason. Convenience, historical
compatibility, and possible future reuse are not sufficient reasons before
1.0.

## Configuration And Automatic Behavior

Prefer a singular safe automatic behavior over user configuration.

Add configuration only when deployments have genuinely different valid policy
requirements that cannot be derived from accepted runtime authority. A new
option must define its owner, default, validation, interaction with existing
options, upgrade behavior, and removal conditions.

Do not expose configuration merely to avoid making an architectural decision.
Do not create a second bootstrap, schema, memory, planner, or recovery authority
behind an option.

## SQL Performance Breadth

Prefer improving a reusable physical capability across the widest safely
provable SQL surface over accumulating query-shape or field-type fast paths.
Before adding an optimized route, inventory the adjacent query families,
numeric or scalar kinds, and planner shapes that share the same correctness
invariant. Where one owner-carried execution contract can cover them without
adding independent states or runtime classification, design and measure that
coherent surface together.

Do not add a sequence of near-identical executor variants for individual SQL
spellings or field kinds when one planner-owned physical primitive can serve
them. Report the types and query families improved per added planner branch,
execution branch, production line, and raw Wasm byte. An intentionally narrow
optimization must explain why adjacent cases cannot safely share its contract
and state the evidence that would trigger consolidation or broader replacement.

Breadth does not authorize speculative semantics, unsafe generalization, or a
multi-outcome landing patch. Unsupported cases must continue through the one
canonical fallback, and a broad optimization remains one coherent outcome only
when its variants share the same authority, correctness proof, lifecycle, and
execution mechanism. If that common contract cannot be demonstrated, prefer
further measurement or no-build over another isolated fast path.

## Technical Debt

Technical debt is current, evidenced friction in maintaining or extending the
supported system. It is not synonymous with large files, TODO comments,
unsupported wishlist features, old report findings, or code that could be
written differently.

The primary debt families are:

- duplicated-flow debt: equivalent semantics or execution implemented more
  than once;
- state-space debt: modes, phases, formats, or combinations that cost more to
  maintain than their current value; and
- ownership debt: decisions or surfaces spread across unclear or unrelated
  owners.

At most one active debt ledger may be authoritative. Report-local issue
inventories and immutable audit reports are evidence, not competing backlogs.

Each active debt item records:

- stable identity;
- concrete present friction;
- canonical owner;
- evidence source;
- intended disposition or reason for acceptance; and
- a trigger for reconsideration when it is not being fixed now.

Debt lifecycle has exactly three states:

- `ACTIVE`: action or an owner decision is currently warranted;
- `ACCEPTED`: the cost is understood and retained until its stated trigger; and
- `RESOLVED`: the maintained system no longer carries the reported friction.

Do not rediscover accepted debt in every audit. Reopen it only when its trigger,
evidence, or risk materially changes. Do not introduce cleanup quotas or perform
unrelated refactors merely to reduce a debt count.

## Tests And Evidence

Tests justify maintained semantics and trust boundaries. They do not justify an
unnecessary product axis or preserve an incidental implementation shape.

For a behavior change, prefer:

1. the smallest owner-local semantic or regression proof;
2. a boundary/rejection proof when the failure can cross that boundary; and
3. one end-to-end proof when generated, wire, persistence, or canister behavior
   is materially involved.

Do not repeat equivalent assertions through every surface. Do not construct a
Cartesian matrix merely because combinations can be generated. Add an
interaction case when the interaction can change behavior, route selection,
state transition, recovery, or cost.

Independent reference models, mutation models, generators, replay formats, and
performance harnesses are maintained subsystems. Their concepts and formats
must remain bounded, current-only before 1.0, and simpler than the behavior they
are intended to verify.

## Audit And Design Discipline

An audit finding does not authorize implementation. It identifies evidence for
review and prioritization.

Audits should share evidence collection when they inspect the same owners and
must avoid parallel taxonomies for the same concept. Retain separate audits only
when they answer materially different safety or product questions.

Design backlogs are lists to shrink. Before promoting a candidate into an
implementation plan:

- confirm the current gap;
- check for existing maintained capability;
- measure or demonstrate the need;
- consider rejection or deferral; and
- apply the no-build and state-space gates above.

Metrics are signals, not targets. File count, line count, branch count, test
count, audit score, and planned patch count must not be optimized without an
architectural or user outcome.

## Landing-Patch Readout

Every implementation handoff reports a short complexity delta:

1. independent behavior axes added or removed;
2. duplicated semantic or execution flows added, removed, or retained;
3. technical debt added, resolved, accepted, or unchanged;
4. canonical owner and convergence point; and
5. whether implementation structure became simpler, stayed neutral, or became
   more complex.

Use `none` or `unchanged` when appropriate. Do not create ceremonial prose for
a mechanical change.

Performance and raw Wasm evidence remain required when relevant. A performance
improvement does not automatically justify additional state or abstraction;
report the implementation-shape cost beside the measured benefit.

## Review Stop Conditions

Stop and rescope before adding work when:

- the proposal solves more than the demonstrated problem;
- an independent new owner or user-visible behavior emerges;
- a second semantic or execution path is being introduced for convenience;
- configuration is replacing a missing architectural decision;
- tests are expanding faster than distinct behavioral risk;
- a persisted job or protocol adds states without explicit recovery ownership;
- an audit method is becoming larger than the actionable evidence it produces;
  or
- the implementation cannot explain why the no-build option is insufficient.

Stopping is a successful governance outcome. Record the smaller next decision
or leave the candidate unimplemented.
