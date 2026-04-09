# Planner Artifact Discipline

## Purpose

This document freezes one architectural rule that now spans both aggregate
execution and single-entity planning:

- derive once
- carry structurally
- project, do not recompute

This is a governance rule for planner-owned and preparation-owned decision
artifacts.

It exists to prevent drift between:

- planner and runtime
- preparation and execution
- execution and `EXPLAIN`

---

# 1. Core Rule

When the system decides behavior through a structural artifact, that artifact
must be:

1. derived once by its owning layer
2. carried structurally into downstream layers
3. projected by runtime and explain surfaces
4. extended at the source if new detail is needed

Downstream layers must not rebuild the same policy with local branch trees.

---

# 2. What Counts As An Artifact

Examples include:

- prepared aggregate strategies
- planner route profiles
- access-choice explain snapshots
- route contracts and route reasons
- grouped execution strategy payloads

The exact type name is not important.

What matters is that the type is the behavioral source for a decision that
other layers must trust.

---

# 3. Required Pattern

The required pattern is:

`owner derives -> contract is carried -> consumers project`

Examples:

- aggregate preparation decides scalar or numeric behavior, then runtime and
  `EXPLAIN` project from the prepared strategy
- planner decides index choice and route fallback, then route/runtime and
  `EXPLAIN` project from planner-owned artifacts

If a renderer or runtime path needs more detail, the fix is:

- extend the carried artifact

The fix is not:

- infer the missing detail locally from adjacent fields

---

# 4. Forbidden Pattern

The following pattern is prohibited:

`owner derives -> downstream code re-classifies -> explain re-classifies again`

This usually shows up as:

- planner classifier
- runtime classifier
- `EXPLAIN` classifier

all describing the same behavior with slightly different branch logic.

That is the failure mode this rule exists to stop.

---

# 5. Extension Rule

If a downstream surface needs one new behavior detail:

1. extend the source artifact
2. thread that field through the existing structural boundary
3. project it where needed

Do not:

- derive a parallel enum in `EXPLAIN`
- infer fallback reasons from node shape alone
- infer runtime behavior from partial metadata when the owner can carry the
  answer explicitly

---

# 6. Applicability

This rule applies whenever behavior is supposed to remain coherent across:

- planner
- route capability
- execution runtime
- `EXPLAIN`

and also across:

- preparation
- aggregate execution
- aggregate `EXPLAIN`

This is especially important for new grouped-planning work, where there is a
high risk of recreating the same grouped classification independently in
multiple layers.

---

# 7. Practical Litmus Test

Before adding new logic, ask:

- Is this behavior already decided by an existing artifact?
- If yes, why am I branching on it again here?
- If I need more detail, can I extend the artifact instead?

If the honest answer is “I am rebuilding policy downstream,” stop and move the
change back to the artifact owner.
