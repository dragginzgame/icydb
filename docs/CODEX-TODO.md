### LLMs PLEASE IGNORE THIS FILE
### It's just here so I can manage multiple prompts without scrolling up and down constantly

🔥 Structural Compression Audit (Hardline Pass)
Prompt

You are performing a hardline structural compression audit on a Rust database engine.

Scope:

crates/icydb-core/src/db/

Objective:

Reduce db/ to its minimal structurally correct form without redesigning the architecture.

This is not a cleanup pass.
This is a compression pass.

The goal is to eliminate:

Namespace duplication

Split conceptual ownership

Fragmented subsystems

Micro-modules

Indirection-only files

Test harness inflation

Cross-layer leakage

You must aggressively compress structure while preserving declared layer direction.

Authoritative Layer Model (Non-Negotiable)
session
→ query
→ executor
→ access
→ index / data / relation
→ commit
→ codec

Rules:

Lower layers must not import higher layers.

Each concept has one canonical owner.

Each invariant has one namespace root.

Contracts must be neutral and minimal.

Production namespace must not contain harness infrastructure.

No duplicated conceptual roots.

No split ownership of the same abstraction.

Forbidden

You may NOT:

Propose creating new modules (unless replacing multiple).

Suggest further file splitting.

Suggest renames for aesthetics.

Suggest architectural redesign.

Suggest new abstractions.

Suggest trait refactors.

Speculate about future features.

Suggest moving logic unless it removes duplication or layering violation.

No theory.
No architecture brainstorming.
Only structural compression.

Mandatory Aggression Rules

You MUST:

Prefer merging over moving.

Prefer flattening over nesting.

Prefer deleting shims over preserving compatibility.

Prefer collapsing thin modules.

Collapse any module that only re-exports.

Collapse any directory with <4 production files unless strongly justified.

Eliminate dual namespace roots.

Eliminate any subsystem split across two trees.

If two modules share a conceptual noun, they must be unified or one deleted.

Required Analysis Dimensions

In addition to merge candidates, you must explicitly analyze:

Duplicate Concept Roots
(e.g. predicate in two places, aggregate in two places, plan in two places)

Split Ownership
(e.g. execution logic split between model + kernel)

Namespace Inflation
(deep trees where files could live at parent)

Hidden Shims
(compatibility re-exports or alias modules)

Test Contamination
(deep harness directories under production)

Contract Surface Area
(are contract types owned in multiple places?)

Structural Symmetry
(do similar subsystems follow different shapes?)

Output Format (Strict)

Produce exactly these sections:

1️⃣ High-Confidence Merge Eliminations

For each:

Files involved

Why conceptual duplication exists

Why they must be unified

Target merged location

Files/directories to delete

Net file count reduction

No speculative merges.
Only high-confidence compression.

2️⃣ Duplicate Concept Roots

List every concept that appears in multiple namespace roots.

For each:

Locations

Canonical owner

What must be deleted

Why this reduces entropy

3️⃣ Subsystem Fragmentation

Identify subsystems split across multiple directories.

For each:

Current layout

Why it is fragmented

Compression target

Files to delete

4️⃣ Directory Flattening Targets

List directories that:

Exist for routing only

Contain <4 production files

Exist only to host tests

Deepen tree without conceptual separation

Provide flattening plan.

5️⃣ Wrong-Layer Placements

For each:

File

Layer violation

Correct layer

What can be deleted after move

No hypotheticals.

6️⃣ Test Namespace Extraction Plan

List all production namespaces that contain:

tests/

tests.rs

Harness-only helpers

Test-only utilities in non-#[cfg(test)] code

For each:

Current path

New path under crates/icydb-core/tests/

Estimated file reduction inside db/

7️⃣ Thin Wrapper Elimination

List modules that:

Only re-export

Only route to submodule

Exist as legacy compatibility

Add zero new behavior

For each:

Target collapse location

Files deleted

Why this is safe

8️⃣ Compressed Canonical db/ Tree

Produce a compressed db/ tree that:

Does not add roots

Does not increase directory count

Minimizes nesting

Removes duplicate roots

Reflects canonical ownership

Removes all flagged shims

The output tree must be smaller than current.

Compression Success Criteria

The audit is successful only if:

File count decreases

Directory depth decreases

No concept appears in two roots

No thin modules remain

No harness directory inflates production tree

Layer direction remains correct

Tone Requirement

Be ruthless but precise.
No fluff.
No speculation.
No redesign.

This is a structural compression report.