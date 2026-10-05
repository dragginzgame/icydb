# Changelog rules

These rules are part of the mandatory [engineering baseline](../DRAGGINZGAME.md)
for repositories with changelogs. Read them before editing release notes.
Consumer overlays own audience, presentation and package-specific views within
this contract; the [release contract](../docs/releases.md) owns release execution.

## Content and writing

- Root `CHANGELOG.md` is the concise release ledger. Lead with the outcome and
  practical effect for the repository's users, operators or developers. Use
  plain language and consolidate related changes into short bullets or summaries.
- Update the current draft by default when a meaningful behavior or maintained
  tooling batch is complete. Cover the coherent outcome, including relevant
  public APIs, commands, output fields, diagnostics and operational changes.
  Choose and maintain its next-version heading automatically under the rules
  below; do not wait for a separate changelog or version-number request.
  Another compatible implementation slice extends that draft.
- Omit formatting-only churn, incidental internal renames, routine test runs
  and governance-only edits unless they change a maintained surface or the
  maintainer explicitly requests a release note. Meaningful coverage or test
  tooling changes may be noted; a `Testing` section describes those changes,
  rather than listing validation commands.
- Use only relevant sections, such as `Added`, `Changed`, `Fixed`, `Removed`
  and `Breaking`. Small entries may use a brief summary without subsections.
  Preserve the established heading style; emojis and descriptive release titles
  are consumer presentation choices.
- Keep implementation inventories, internal paths, test names, footprint tables
  and long examples out of root summaries unless needed to explain a public
  contract or required action. Move useful detail to the linked minor-line notes.
  Include a small representative example when it makes a newly supported
  command, query or configuration clearer; do not invent examples for cleanup.
- Describe supported behavior and measured effects accurately. Do not present
  planned work, a passing substitute test or an expected speed-up as shipped or
  qualified behavior. Routine validation results and readiness belong in the
  handoff or their existing evidence owner, rather than repeated release prose.

## Issue links and compatibility

- When a change fixes a GitHub issue, its changelog entry must include a clickable
  link to that issue, for example `[#123](https://github.com/OWNER/REPO/issues/123)`.
  Link every resolved issue relevant to the entry. When both root and detailed
  notes describe the fix, include the issue links in both views. Bare issue
  numbers or local tracking labels do not replace links; never invent issue URLs.
- Mark breaking public API or semantic changes explicitly in the root summary.
  State the affected contract and required consumer action, with fuller detail
  in the minor-line notes when needed. Include relevant API, CLI, response/error,
  wire/storage and execution-semantic changes; do not bury them under cleanup.
- State migration, regeneration, reinstall or reset implications accurately.
  A pre-1.0 hard cut does not waive retained-data or external-consumer obligations.
  Breaking public API or semantic changes require a minor release before 1.0.
  Report a conflicting selected patch target rather than silently renumbering it.

## Automatic next-version selection

- Keep one numbered, undated pending release at the top of `CHANGELOG.md`, using
  `## [X.Y.Z]`. Automatically create or update it as part of completing meaningful
  work. Do not use `Unreleased`, an unnumbered `Draft` or a separate notes queue.
  Keep this pending entry undated until release preparation finalizes it.
- Derive the candidate from the latest finalized release and the complete pending
  batch since that release, not just the latest edit. Check the maintained
  release ledger, known tags/publication and canonical version source; an already
  prepared manifest or pending heading is not another released base to increment.
  Report a concrete identity conflict or missing initial version instead of
  inventing release history. Do not ask for routine version confirmation when
  the base and compatibility impact are clear.
- Before 1.0, choose the next patch for compatible fixes, additions and internal
  cleanup. Choose the next minor, resetting patch to zero, if any pending change
  breaks public API, observable semantics, CLI/output or wire/storage contracts.
  This includes a breaking hard cut, removal, or required consumer regeneration,
  reinstall or reset. An internal hard cut with no consumer contract change does
  not by itself require a minor release. Moving to 1.0 remains an explicit
  maintainer decision, rather than an automatic response to a pre-1.0 hard cut.
- From 1.0 onward, apply SemVer: compatible fixes use patch, compatible public
  additions use minor, and incompatible public changes use major. Honour an
  explicit maintainer-selected version when it satisfies the compatibility
  requirement; report an incompatible selected target instead of silently
  overriding it or describing a breaking release as compatible.
- Reuse the same pending entry until it is released. If later work makes an
  automatically selected patch breaking before 1.0, relabel that entire entry
  to the next minor from the same released base and carry all pending notes with
  it. Do not increment again for each slice, create multiple pending versions,
  or rewrite a tagged/published entry. Keep every maintained changelog view and
  detail link aligned, including when a candidate changes minor line.
- In the handoff, state the selected next version and the compatibility reason,
  including the affected contract and required action for a breaking change.
  Selecting a changelog heading is automatic documentation maintenance; it does
  not change manifests, lockfiles or release defaults, or authorize commits,
  tags, pushes, publication or deployment. Commits remain maintainer-owned.

For a latest finalized release of `0.14.7`:

| Complete pending batch | Top pending heading |
| --- | --- |
| Compatible fix or addition | `## [0.14.8]` |
| More compatible work before releasing that patch | Keep `## [0.14.8]` |
| Breaking hard cut, including one added to that pending patch | `## [0.15.0]`, carrying the complete pending batch |
| Compatible work added to that pending minor | Keep `## [0.15.0]` |

## Finalization and release history

- Keep the latest finalized release at the top until subsequent meaningful work
  needs its automatically numbered pending entry. Once a release tag exists or
  publication is reported, preserve that entry and collect later work under the
  next candidate derived from the new released base.
- During explicitly authorized release preparation, reconcile the numbered
  candidate with the selected release command, include work missing from the
  notes, and finalize it as `## [X.Y.Z] - YYYY-MM-DD` with the UTC release date.
  Avoid duplicate entries for the same target; report a conflicting command
  instead of silently relabelling the prepared version.
- Changelog presentation, section position and pending labels must not block
  deployment. Release identity consistency belongs in release preparation.
  Report and repair missing notes when practical; explanatory prose is not an
  executable release or deployment guard.
- Preserve historical entries, dates, order and content. Do not silently rewrite
  published summaries to reflect today's implementation, delete history or
  reclassify imported undated releases as drafts. Historical notes are evidence,
  rather than authority for current supported contracts.
- Archive or correct historical notes only when explicitly authorized. Archiving
  retains all historical detail and leaves the version, date, concise summary and
  clickable detail link in the root ledger. Do not merge different minor lines
  into one detail file or discard content to shorten the root.
- Pending versions and date fields record note preparation; tags and registries
  establish their own publication state. Changelog text does not prove validation,
  publication, consumer adoption or deployment.

## Detailed notes when needed

- A small repository may keep its notes entirely in root `CHANGELOG.md`. Add
  detailed notes when release volume or explanation makes the root hard to scan,
  or retain them when the repository already uses that structure.
- Use one `docs/changelog/<major>.<minor>.md` file for all patches in a minor
  line, rather than one file per patch. Use the automatically selected candidate's
  minor line for pending detail notes. If that candidate changes line, move only
  its unpublished notes to the matching file and update the root link; preserve
  finalized history in its original files.
- A root minor-line index uses one concise summary per patch, normally one short
  bullet. Product-facing ledgers may retain linked release titles and brief prose.
  Keep minor lines and their patch summaries newest first; detailed patch sections
  follow the same order. Preserve existing historical ordering when adopting
  these rules rather than reorganizing published notes without authorization.
- Every patch in a minor-line index has a matching detail section. Keep root and
  detailed notes aligned on scope, patch identity, issue links and compatibility.
  Where both views date a patch, their dates must agree. Small detail sections
  may remain short. Extend both views for the same draft.
- Root entries must link to their detail files with clickable Markdown links.
  Resolve relative targets from the containing file: a root ledger uses, for
  example, `[Detailed notes](docs/changelog/0.33.md)`.
- Detailed notes explain meaningful implementation changes, operator-facing
  command/output surfaces, changed invariants and coverage, and required consumer
  actions. Link to evidence and operational procedures at their existing owners
  instead of copying command logs, release receipts or issue status.

Before handoff, check that the top pending version reflects the complete batch's
compatibility impact, the current notes cover the completed work, issue and
detail links resolve, both views agree when present, and historical content is
preserved. This review does not require running a broad validation or release gate.
