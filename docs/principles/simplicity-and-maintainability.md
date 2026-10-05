# Simplicity And Maintainability

## Purpose

The goal is the smallest maintained state space that satisfies demonstrated
product and safety needs with clear ownership. Minimum line count and maximum
reuse are not goals by themselves.

This principle was generalized from IcyDB's no-build, state-space, and
canonical-authority review gates.

## Preferred order

When resolving a need, prefer to:

1. delete an obsolete path;
2. reuse an existing authority or flow;
3. narrow an existing surface;
4. change a safe default or derive behavior automatically;
5. extend an existing owned contract; and
6. add a new concept only when the preceding options are insufficient.

A plausible feature, audit observation, backlog item, or testable combination
is not a requirement. No change is a valid outcome when the need is not
demonstrated.

## No-build gate

Before adding a behavior axis, answer:

1. What demonstrated user, correctness, safety, or measured-performance
   problem exists?
2. What is the smallest outcome that resolves it?
3. Can an existing authority, contract, route, or default resolve it?
4. What simpler alternative was considered, and why is it insufficient?
5. Which component is the canonical owner?
6. What independently variable states or combinations are added?
7. Which existing path becomes simpler or disappears?
8. Is the ongoing maintenance cost proportionate to the benefit?

The answer may be a short paragraph for a narrow change. A public mode,
persisted state machine, protocol, or recovery path needs explicit design
evidence.

## State-space delta

An independent axis is a choice that combines with other maintained choices
and multiplies behavior that implementation, tests, recovery, and users must
understand. Common axes include:

- public modes, endpoints, modifiers, and result variants;
- configuration options and default-policy choices;
- execution strategies and fallback families;
- persisted phases, statuses, versions, and recovery outcomes;
- protocol, cursor, replay, and artifact formats;
- authorization and visibility states; and
- widely consumed enum variants.

For an added axis, report its admitted values, interactions, invalid-state
boundary, canonical owner, and any path it replaces. Do not manufacture a
headline combination count when combinations are constrained.

Tests, diagnostics, documentation, fixtures, and mechanical propagation add
maintenance cost but do not create independent product states. They should not
be split into artificial changes when they directly prove one outcome.

## Canonical authority and converged flow

Equivalent behavior should have one semantic owner and converge on one
internal contract as early as practical:

```text
owner derives -> contract carries -> consumers project
```

When a downstream consumer needs more information, extend the owner-carried
artifact. Do not add a parallel classifier, reparse an earlier representation,
or infer policy from partial shape.

Similar code is not automatically debt. Duplication may be correct when it
independently enforces a trust boundary, contains corruption or recovery risk,
or is a measured hot-path specialization. Name that reason explicitly.

## Configuration

Prefer one safe automatic behavior over configuration. Add configuration only
for genuinely different valid policies that cannot be derived from existing
authority. A new option needs an owner, default, validation, interaction rules,
lifecycle behavior, and removal conditions.

Configuration must not conceal an unresolved architecture decision or create a
second authority for the same behavior.

## Evidence and review

Prefer the smallest evidence set that proves the maintained risk:

1. an owner-local semantic or regression proof;
2. a boundary or rejection proof when invalid state can cross that boundary;
3. one end-to-end proof when a generated, wire, persistence, or external-effect
   boundary is materially involved.

Stop and rescope if the proposal solves more than the demonstrated problem,
introduces another independent owner or behavior, adds configuration instead
of making a decision, or cannot explain why no-build is insufficient.

Consumers own product-specific performance metrics, transition and retirement
procedures, exact release gates, and additional handoff fields within the shared
baseline. They do not independently override its hard-cut, continuation,
validation-authority or effect-authorization rules.
