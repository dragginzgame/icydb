### LLMs PLEASE IGNORE THIS FILE
### It's just here so I can manage multiple prompts without scrolling up and down constantly


3. Another major offender: decoding in loops

You still have:

decode::<E>(row)
decode_batch::<E>(...)
RawRow::try_decode::<E>

inside runtime paths.

Even if “just helpers”, if they sit inside execution:

👉 they force generic instantiation of entire call chains.

4. Cursor + ordering is still generic
compare_entities::<E>
resolve_order_spec::<E>

Sorting is:

hot path

reused everywhere

👉 This alone can generate a lot of duplicated code.

5. Aggregate layer is still heavily generic

You still have:

resolve_numeric_aggregate_target_slot::<E>

execute_count_*::<E>

grouped_fold::<E>

👉 Aggregates are one of the largest wasm contributors in DB engines.

6. What this means structurally

You have:

✔ Structural planning
✔ Partial runtime erasure

But:

❌ Execution still depends on <E>

So effectively:

You have a generic execution engine wrapped in an erased shell

That gives you:

complexity of dynamic

size of static

Worst of both worlds.

7. The rule you must enforce (hard constraint)

You need to adopt this invariant:

NO <E> inside any function reachable from execution kernel

Concretely:

Forbidden inside runtime:

LoadExecutor::<E>

decode::<E> (inside loops)

compare_entities::<E>

aggregate execution <E>

stream traversal <E>

8. What <E> is allowed to do

You already said this correctly earlier:

keep E at API / schema / validation boundaries

Translate that to code:

Allowed:

session::<E>

describe::<E>

insert::<E>

decode::<E> → ONLY at final boundary

9. What you must replace these with
Replace <E> with:
1. EntityTag

You already have it:

EntityTag(u64)
2. Slot-based access

Instead of:

resolve_*::<E>(field)

Use:

FieldSlot

(precomputed at planning time)

3. Structural row

Instead of:

decode::<E>(row)

Use:

RowView

Only decode to E at the very end.

4. Runtime vtable WITHOUT generics

Right now:

execution_runtime_core_vtable::<E>()

Must become:

execution_runtime_core_vtable(entity_tag)

Single implementation.

10. The most important fix (highest ROI)
Kill this pattern:
ctx.as_typed::<E>()

This guarantees:

monomorphization

duplicated code paths

Replace with:

ctx.as_structural()
11. Minimal target architecture

You want:

ONE execution path:
fn execute(plan: ExecutablePlan, ctx: ExecutionContext) -> Page

Inside:

no generics

no <E>

no typed decode

12. Expected impact

If you remove <E> from:

executor

aggregate

stream

projection

👉 You should see:

18 MB → ~3–6 MB

13. What NOT to waste time on

Do NOT:

micro-optimize SQLite flags

tweak compile options

chase small removals

👉 Your problem is structural duplication, not feature flags

14. Concrete next step (do this now)

Run:

twiggy top -n 50 your.wasm | rg "<E>"

You will likely see:

repeated executor symbols

repeated aggregate symbols

That confirms everything above.

15. Bottom line

You are very close, but:

Your runtime is still secretly generic

Until you eliminate <E> from execution:

👉 You will not get under IC limits