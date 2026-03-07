What you just found is a class of architectural bug, not a typical logic bug. They happen when two layers encode the same policy differently. The best way to find more is to systematically hunt for policy duplication and capability drift.

Below are concrete techniques that work very well for engines like yours.

1. Look for “Executor Doing Planner Work”

Your invariant is:

planner → route/access → executor → storage

So search for executor code that decides eligibility, not just execution.

Red flags in executor code:

if access_path == ...
if index_kind == ...
if plan.is_distinct()
if order.is_empty()
if limit.is_some()

Those are policy questions, not execution mechanics.

Search pattern:

rg "is_distinct|limit|offset|order|shape|access_path" crates/icydb-core/src/db/executor

Ask:

Is the executor deciding eligibility instead of route/access?

That’s exactly the bug you just found.

2. Diff Route Capability vs Executor Branching

For each optimization, compare:

route capability checks
executor fast-path conditions

They should match.

Example workflow:

Find capability definitions

rg "supports_" crates/icydb-core/src/db/access

Find executor fast-paths

rg "fast_path|eligible" crates/icydb-core/src/db/executor

Compare the conditions.

If executor checks something that route does not expose, that’s a seam.

That’s how you discovered the COUNT split.

3. Search for “Fallback Paths”

Fast paths almost always look like:

if fast_path_eligible {
    run_fast_path()
} else {
    fallback()
}

Now ask:

Who decided fast_path_eligible?

If the answer is executor, you probably found another policy leak.

Search:

rg "fast_path|fastpath" crates/icydb-core/src/db
4. Compare AccessPath Usage Across Layers

You already saw in your audit:

AccessPath references: 116
files: 13

That’s a large policy surface.

Run:

rg "AccessPath::" crates/icydb-core/src/db

Look for logic like:

match access_path {
    AccessPath::IndexRange => ...

If that appears in:

executor
planner
route

there’s a risk the policy is duplicated.

You want:

route/access → interpret AccessPath
executor → just execute it
5. Use “Optimization Inventory Drift”

You already created an optimization inventory in your design doc.

Turn that into a bug detector.

For each row:

Optimization	Route	Executor
COUNT PK fast path	capability	fast path
BYTES fast path	???	fast path

Ask:

Does route expose a capability for this optimization?

If not, executor probably inferred it.

That’s another seam.

6. Look for Missing Trace Variants

You just added:

PrimaryKeyTopNSeek
SecondaryOrderTopNSeek

Trace taxonomy is a powerful bug detector.

Rule:

Every optimization should have a trace variant.

Now search:

rg "ExecutionOptimization" crates/icydb-core

Then ask:

Is there an optimization happening that has no trace variant?

If yes, it’s probably an implicit optimization.

Implicit optimizations are almost always policy leaks.

7. Run “Layer Ownership Diff”

You already run:

scripts/ci/check-layer-authority-invariants.sh

Extend that idea manually.

Pick a concept:

COUNT
LIMIT
DISTINCT
ORDER
projection

Search where each appears:

rg "distinct" crates/icydb-core/src/db

Now check:

query/plan
route/access
executor

If two layers implement the same policy independently, you likely found a bug.

8. Audit “Missing Capability Enums”

Your fix introduced something like:

CountPushdownKind

These enums are the correct architectural boundary.

Look for places where a decision exists but no enum models it.

Examples that often hide bugs:

projection satisfaction
limit pushdown
count folding
covering index eligibility
grouped aggregation order

If logic exists but no enum expresses it, the policy is probably duplicated somewhere.

9. Use EXPLAIN as a Bug Detector

Now that EXPLAIN exists, you can run queries and ask:

Is the optimization explainable?

If you see behavior but no EXPLAIN marker, it’s likely happening implicitly.

Implicit optimizations often hide layer violations.

10. Look for “Same Condition Repeated in 2+ Places”

Example pattern:

if !plan.is_distinct()

Search:

rg "!plan.is_distinct"

If that appears in:

executor
route
planner

then the policy is duplicated.

It should live in exactly one layer.

A Good Heuristic

When reviewing code, ask this question constantly:

Is this code deciding WHAT is allowed,
or HOW to execute it?

If it’s deciding what is allowed, it belongs in:

planner
or
route/access

If it’s deciding how to execute, it belongs in:

executor

The bug you found violated that rule.

The Best Systematic Approach

Once per release, run this audit:

List all optimizations

For each optimization verify:

planner semantic gate
route capability
executor implementation
trace variant
explain visibility

If any step is missing or duplicated, you likely found a bug.

If you'd like, I can also show you three very likely architectural seams that probably still exist in your engine, based on the modules and audits you shared. They are the most common places engines like this leak policy.