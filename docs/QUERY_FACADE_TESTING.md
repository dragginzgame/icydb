# Query Facade Testing Guide

This document captures how to test query facade invariants in a way that
enforces architectural boundaries and avoids validation drift.

## Scope

- Query facade and executor boundary tests.
- Contract-level invariants (type, responsibility, determinism).
- Trybuild compile-fail tests as the primary mechanism.

Non-goals:
- Planner optimality or cost-model assertions.
- Performance tests.
- Index selection heuristics.

## Invariant Categories (Name Them Explicitly)

Use these labels in test prompts and comments:

- Type boundary invariants (what must not compile)
- Responsibility boundary invariants (what layer must not validate or decide)
- Semantic stability invariants (planner choice must not affect correctness)
- Corruption vs Unsupported vs Internal classification
- Determinism invariants (same intent -> same plan fingerprint)

## Compile-Fail Tests (Trybuild First)

Rules:

- Assume tests run in a separate crate context. Do not rely on `pub(crate)`.
- Prefer negative test names: `cannot_*` or `must_not_*`.
- Each test must include a one-sentence comment stating which contract invariant
  it enforces.

Examples of boundary proofs:

- Prove `LogicalPlan` cannot be named by user code.
- Prove an executor cannot be called with an unplanned query.
- Prove an `ExecutablePlan<E>` cannot be created without a planner.

## Runtime Tests (When Behavior Matters)

Only use runtime tests when behavior cannot be proven by type boundaries.

Rules:

- Name the failure mode precisely and assert the exact error variant.
- Explain why any other error variant would violate the contract.
- Do not add executor-side validation to make tests pass.

## Prompt Template (Recommended)

Use this template when requesting tests:

```
You are writing tests for a Rust database query facade with strict architectural
boundaries.

Goal: enforce intent-level and executor-safety invariants mechanically.

Constraints:
- Assume tests run in a separate crate context.
- Prefer compile-fail tests (trybuild) for type and visibility invariants.
- Prefer runtime tests only where behavior depends on execution.
- Do not add new validation logic to make tests pass.

Task:
Write tests that enforce the following invariant: [state invariant].

For each test, include a one-sentence justification tying it to the contract.
If an invariant cannot be tested mechanically, explain why.
```

## When Not to Ask for Tests

Avoid asking for tests of:

- performance
- planner optimality
- cost models
- index selection heuristics

These lock in accidental behavior and will change as the planner evolves.
