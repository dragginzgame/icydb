# Decision Artifact Discipline

## Purpose

Behavior decided by one component should not be independently reconstructed by
each downstream consumer. The shared rule is:

```text
derive once -> carry structurally -> project, do not recompute
```

This principle was generalized from IcyDB's planner-artifact discipline, but
it applies to any prepared decision, route, policy result, capability record,
or execution contract.

## Required pattern

When a structural artifact is the authority for a decision:

1. the owning component derives it once;
2. the contract carries it across downstream boundaries;
3. runtime, diagnostics, renderers, and explain surfaces project from it; and
4. missing detail is added at the owning artifact rather than inferred later.

The artifact may be a named type, immutable record, enum, or bounded payload.
Its name is less important than its ownership and authority.

## Failure pattern

Avoid this shape:

```text
owner decides -> consumer reclassifies -> diagnostics reclassify again
```

Repeated branch trees drift in eligibility, fallback reasons, error handling,
and observability even when they begin equivalent.

Do not derive a parallel enum for a renderer, infer a reason from incidental
node shape, or rebuild policy from partial metadata when the owner can carry
the answer explicitly.

## Boundary test

Before adding downstream logic, ask:

- Has an upstream owner already decided this behavior?
- Is the proposed branch projecting that decision or rebuilding it?
- If detail is missing, can the owner-carried artifact be extended?
- Does independent validation protect a separate trust boundary, or merely
  duplicate classification?

Independent validation remains appropriate at trust, corruption, and external
input boundaries. It should validate the carried decision, not invent another
semantic authority.

Consumers own the concrete artifact types, component boundaries, and any
performance evidence required to carry more detail.

