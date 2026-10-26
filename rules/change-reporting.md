# Change reporting

This rule is part of the [engineering baseline](../DRAGGINZGAME.md). It governs
completed maintainer reports: delivery handoffs, audit/inspection findings and
issue-only outcomes. Default to the practical outcome, relevant verification and
actionable blockers, with file or evidence links where useful. Report material
compatibility and consumer actions. Keep routine reports brief.

Line metrics and symbol inventories are **opt-in** when the maintainer requests
them or an explicitly selected audit requires that measurement. A cleanup task,
inspection or completed batch alone does not activate them. Do not calculate
metrics, enumerate private symbols or print zero blocks just to finish a task.
No saved report, template or persistent change ledger is required.

The sections below define requested accounting, not mandatory handoff fields.

## Symbol changes

Use the neutral label **symbol changes**, limited to two counted groups:

- **Deleted:** genuinely removed functions, methods and types, public or private,
  including declarations in deleted files. List every exact qualified name,
  former file/module, and a brief shared reason or replacement where useful.
  Group related names compactly. Inlining a removed helper is a deletion with its
  replacement identified.
- **Created:** genuinely new functions, methods and types, public or private.
  Give their qualified names and file/module, grouping related names compactly.
- Omit inventories and counts of moved, renamed or edited declarations. A retained
  declaration is neither created nor deleted because its path, name, signature,
  body or fields changed. Trace identity before classifying a replacement.
  Summarize structural cleanup by outcome in at most a short paragraph; do not
  expand a module reorganization into hundreds of per-symbol entries.

When neither group has entries, use one short line: "Symbol changes: created 0;
deleted 0." The created/deleted inventory belongs **only in the final response to the maintainer in the
Codex app**, never in repository files, audit reports, changelogs, commit messages,
PRs or issues. Those surfaces still document meaningful public API removals and
required consumer actions.

## Requested metric block

When detailed change accounting is requested, use the requested measures below.
Include explicit zeroes within that selected scope, not unrelated categories:

| Measure | Values |
| --- | --- |
| Rust physical lines | added / removed / net |
| Type declarations | created / deleted only |
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
  the broader symbol inventory includes created/deleted types such as unions.
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
