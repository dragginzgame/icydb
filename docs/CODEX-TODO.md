### LLMs PLEASE IGNORE THIS FILE
### It's just here so I can manage multiple prompts without scrolling up and down constantly

🔧 Consolidation Audit Prompt (Strict Merge-Only Pass)

Prompt:

You are performing a structural consolidation audit on a Rust database engine.

Scope:
crates/icydb-core/src/db/

Objective:
Reduce module count, eliminate misplaced files, consolidate fragmented subsystems, and enforce strict layer direction.

You are NOT allowed to:

Propose creating new modules unless replacing multiple existing ones.

Propose further splitting files.

Suggest cosmetic renames.

Redesign architecture.

Suggest abstract refactors unrelated to placement or consolidation.

You ARE required to:

Identify files or directories that can be merged.

Identify test harness code living in production namespaces.

Identify duplicate namespace roots (e.g., intent in two places).

Identify thin wrapper modules or indirection-only files.

Identify wrong-layer placements.

Enforce declared layer model strictly.

Prefer merging over moving.

Prefer flattening over deepening.

Authoritative Layer Model

session
→ query
→ executor
→ access
→ index / data / relation
→ commit
→ codec

Rules:

Lower layers must not import higher layers.

Contracts must be thin and neutral.

Each invariant has one canonical owner.

Each concept has one namespace root.

Test harness must not inflate production namespace.

Output Format (Mandatory)

Produce the following sections:

1️⃣ High-Confidence Merge Candidates

For each:

Files involved

Why they belong together

Target merged location

Which files will be deleted

No speculative merges.

2️⃣ Directory Flattening Candidates

List directories that:

Exist mainly due to tests

Contain <4 production files

Add unnecessary nesting

Propose flattening plan.

3️⃣ Wrong-Layer Placements

For each:

File

Why it violates layer direction

Correct location

4️⃣ Test Harness Relocations

List test-only files currently inside production namespaces.

For each:

Current path

Recommended test-root location

Why

5️⃣ Thin Wrappers / Shims

List:

Re-export modules

Indirection-only files

Compatibility remnants

Recommend deletion or merge target.

6️⃣ Dead Scaffolding

High-confidence only:

Unused enum variants

Unconstructed branches

Test-only helpers in production modules

7️⃣ Final Compressed Module Map

Provide a compressed ideal tree for db/
Do NOT increase directory count.
Do NOT introduce new roots.
Reduce nesting.

Constraints

Merge-first bias.

No module inflation.

No theoretical redesign.

Concrete, actionable consolidation only.

Be conservative and precise.