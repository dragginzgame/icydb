# Complexity and technical debt

Apply the [common audit contract](README.md) and
[simplicity guidance](../docs/principles/simplicity-and-maintainability.md).
Review affected owners after adding modes, state machines, configuration axes,
execution routes or widely consumed variants, or when ordinary changes repeatedly
cross unrelated owners. A broad baseline must be explicitly requested.

The question is whether maintained behavior creates unnecessary state space,
ownership spread or demonstrated maintenance friction. Size, branch counts and
TODOs are signals for inspection, not debt scores or automatic refactor triggers.

## Method

1. Map current independent behavior axes: admitted values, canonical owner,
   interacting axes and invalid combinations. Exclude removed formats and
   historical records from current product states; test/diagnostic propagation
   is maintenance cost, not another product mode.
2. Map important decisions to their owner, semantic consumers, plumbing consumers
   and cross-owner switch sites. Look for variants requiring repeated semantic
   edits, orchestration that absorbs domain policy, unnecessarily broad visibility,
   and persisted transitions without a clear owner.
3. Rehearse at most three plausible near-term changes by source inspection. Name
   the expected owner, semantic modules affected, boundaries crossed and present
   blocker. A rehearsal is evidence of current friction, not a new roadmap or
   implementation request.
4. Classify evidenced debt as duplicated flow, state-space or ownership debt.
   Link [flow-convergence](flow-convergence-and-duplication.md) evidence for a
   duplicated decision rather than repeating that investigation.
5. Account for generated/test code, mechanical moves, recovery containment,
   public facade coordination and measured specialization before recommending
   change. Name a concretely simpler owner shape and apply the no-build gate.

## Findings

For each item record evidence, current friction, owner and trigger. Use one
disposition: fix now within separate repair authority, fix when touched, accept
until a named trigger, or not debt. Retaining a justified design is a valid result.

Include the state-space map, decision spread, rehearsals and comparable change
in the report. Separate measured counts from classification. Never infer speed,
correctness or delivery productivity from an aggregate complexity score.
