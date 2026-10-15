# Change reporting

This rule is part of the [engineering baseline](../DRAGGINZGAME.md). It governs
completed maintainer reports: delivery handoffs, audit/inspection findings and
issue-only outcomes. Progress messages need not repeat the metric block.
Keep the report proportional; these fields require neither a saved report nor
a new template, workflow, installer or persistent change ledger.

## Symbol changes

Use the neutral label **symbol changes**, with separately counted groups:

- **Deleted:** genuinely removed functions, methods and types, public or private,
  including declarations in deleted files. List every exact qualified name,
  former file/module, reason and replacement when applicable. Group shared reasons
  without dropping names. Inlining a removed helper is a deletion with its
  replacement identified.
- **Moved:** retained declarations relocated to another file/module. Give exact
  names and old/new owners. Do not count them as deleted or newly added symbols.
- **Renamed:** retained declarations whose declared identifiers change. Give old/new names and
  owners. Do not count them as deletions/additions. If a declaration both moves
  and is renamed, identify it once, mark both operations and disclose that the
  move/rename counts overlap; do not sum them as distinct symbols.
- Explain edited declarations and replacement implementations separately. A
  changed signature, body or field does not delete the retained declaration.
  Similar spelling alone does not establish continuity: trace callers, ownership
  and behavior before classifying a replacement as a rename.

Show zero counts for empty deletion/move/rename groups in a cleanup handoff.
The full inventory belongs **only in the final response to the maintainer in the
Codex app**, never in repository files, audit reports, changelogs, commit messages,
PRs or issues. Those surfaces still document meaningful public API removals and
required consumer actions. The fictional example below demonstrates the rule;
it is not a repository's removal inventory.

## Required metric block

Every completed maintainer report includes these measures, even when all are zero:

| Measure | Required values |
| --- | --- |
| Rust physical lines | added / removed / net |
| Type declarations | added / deleted; moved / renamed separately |
| Type-declaration physical lines | added / removed / net |
| Documentation physical lines | added / removed / net |

Use signed added/removed/net values such as `+12 / -9 / +3`; retain both churn
counts even when the net is zero. Semantic declaration counts count identities,
not matching lines. A move or rename can therefore produce line additions and
removals while producing zero semantic additions/deletions. Never describe raw
Git removed lines as functionality deleted. Unavailable counts must say
`unavailable` with the missing evidence; do not omit them, guess or substitute zero.

### Scope and counting units

- Name the before/after source identities and task-attributed paths or hunks.
  For a clean checkout, the starting commit and final working tree are sufficient.
  With incoming dirty work, retain its starting content for comparison and exclude
  unrelated edits. Include task-owned untracked files as additions. If attribution
  cannot be recovered, report that limitation instead of claiming a task total.
  Distinguish this task's delta from an accumulated release delta; label both if
  both are useful. Comparisons do not authorize staging, committing or cleanup.
- **Rust:** all maintained, authored `.rs` source in scope, including production,
  tests, examples, build scripts and macro definitions. Count physical changed
  lines, including blanks and comments, with rename detection disabled. This is
  Git line churn, not the runtime/test code LOC from `make cloc`.
- **Types:** explicit authored `struct`, `enum`, `trait` and type-alias items in
  those Rust files,
  including items inside modules or macro definitions. Count each declaration
  once at its source location, independent of feature configurations or repeated
  macro expansion. Variants, generic parameters, associated-type requirements,
  `impl` blocks, functions and unions are outside this metric's four item kinds;
  the broader symbol inventory still includes deleted types such as unions.
  Identify retained/moved/renamed items by source review, not a regex or diff total.
- **Type lines:** added/removed physical lines within those items' before/after
  source spans, including attached attributes/Rustdoc, fields, trait members and
  bodies, comments, blank lines and delimiters. Exclude unrelated adjacent comments
  and enclosing modules. Count a changed line once per side even when item spans
  overlap. Review Rust structure to establish spans; text matching alone does not
  distinguish declarations from strings or comments.
- **Documentation:** maintained standalone documentation (`README`, `AGENTS`,
  baseline/rules, guides, changelogs and authored reports, using the repository's
  documented formats/paths), plus Rustdoc `///`, `//!`, `/** ... */`, `/*! ... */`
  and `#[doc = ...]` source spans. Count physical lines, including blank lines and
  code fences in standalone documents. Ordinary Rust comments are not Rustdoc.
  Do not treat every `.txt` data file as documentation merely by extension.
- Exclude generated output, vendored/snapshotted copies, lockfiles, binaries,
  build artifacts and external/generated macro expansions. Name any relevant
  excluded paths. Count authored source at its canonical owner; do not count the
  same canonical change again through distributed copies. Other languages/data
  may have additional labelled measures; these categories are not a whole-repo
  size total.
- State overlaps: type lines are a subset of Rust lines; Rustdoc belongs to both
  Rust and documentation, and to type lines when attached to a counted type.
  Never sum overlapping categories into a total.

Use existing Git diffs and source inspection for a bounded batch. For tracked
changes, `git diff --no-renames --numstat BASE -- PATHS...` supplies physical line
counts; `--unified=0` supplies changed old/new ranges for type and Rustdoc spans.
Include staged and unstaged changes relative to the chosen baseline. Inspect
task-owned untracked files separately, or compare temporary before/after trees
with `git diff --no-index --no-renames`; its status 1 means differences were found.
Name the selection and commands so another reviewer can reproduce the totals.
Do not stage files just to count them or use stock LOC differences as churn counts.

## Worked example

These are complete fictional files. Every displayed content line ends with LF;
there are no other files. Compare the `before` and `after` trees with
`git diff --no-index --no-renames --numstat before/ after/`. An absent file is empty
for the comparison. No source is generated or vendored; blanks are included.

Before `src/model.rs`:

```rust
pub struct Removed;
pub struct Moved;
pub struct OldName;
pub struct Edited {
    pub value: u8,
}
```

After `src/model.rs`:

```rust
pub struct NewName;
pub struct Edited {
    pub value: u16,
}
```

After `src/moved.rs` (absent before):

```rust
pub struct Moved;
```

Before `src/lib.rs`:

```rust
mod model;
```

After `src/lib.rs`:

```rust
mod model;
mod moved;
```

Before `README.md`:

```markdown
# Example
```

After `README.md`:

```markdown
# Example

Changed the model layout.
```

Example final handoff:

> Scope: the complete fictional before/after trees above, not a release total.
> Symbol changes: deleted **1** (`model::Removed`, formerly `src/model.rs`,
> obsolete placeholder, no replacement); moved **1** (`model::Moved` in
> `src/model.rs` to `moved::Moved` in `src/moved.rs`); renamed **1**
> (`model::OldName` to `model::NewName`, both in `src/model.rs`). Separately,
> `model::Edited` retains its identity and changes its field from `u8` to `u16`.
> Rust lines **+4 / -4 / 0**; types **+0 / -1**, moved **1**, renamed **1**;
> type lines **+3 / -4 / -1**; docs **+2 / -0 / +2**.
> Physical lines include blanks/comments; type lines overlap Rust. No Rustdoc
> changes, generated files or vendored files occur in this example.

The per-file Git counts are `src/model.rs +2/-4`, `src/moved.rs +1/-0`,
`src/lib.rs +1/-0`, and `README.md +2/-0`. The module declaration addition is
Rust churn but not a type declaration or type line. No new semantic type was
introduced by the move or rename.

Example issue-only final report:

> Reviewed issues against commit ABC; source was unchanged during this task.
> Scope: task-attributed working-tree changes relative to its starting state.
> Rust lines **+0 / -0 / 0**; types **+0 / -0**, moved **0**, renamed **0**;
> type lines **+0 / -0 / 0**; docs **+0 / -0 / 0**.
> GitHub issue text is outside repository source/documentation counts.
