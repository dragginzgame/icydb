# IcyDB Changelog Governance

This document defines the authoritative rules for maintaining
group ledgers and minor-line changelog archives.

These rules are intended to be followed by automated agents.

---

# 1. Purpose

Each `releases/<group>/CHANGELOG.md` is the canonical ledger for its release group.
Root `CHANGELOG.md` only indexes the `database` and `cli` ledgers and the unchanged
coordinated history in `releases/history.md`. For IcyDB, "main changelog" means
the owning group's `releases/<group>/CHANGELOG.md`: keep every completed fix's
descriptive issue link beside its outcome there, and preserve those links after
release. The maintainer selected this presentation on 2026-11-03; the current
Shared group-changelog policy now agrees. Group ledgers remain the release/version
authority.

It records high-level architectural and behavioral changes per release.

It must remain concise and structured.

Detailed change breakdowns belong in:

`releases/<group>/<major>.<minor>.md`

For example: [docs/changelog/0.33.md](../changelog/0.33.md)

---

# 2. File Structure

## 2.1 Canonical Ledger

- Ledger: `releases/<group>/CHANGELOG.md`
- Must contain:
  - Version headers
  - Date
  - High-level summary sections
  - Links to detailed notes
- Keep each patch under its existing version heading. Follow the shared
  [readable release layout](https://github.com/dragginzgame/toko-tooling/blob/5806e6e4943f6be600cd53385498ce08a5f804c4/rules/changelogs.md#readable-release-layout):
  separate independent outcomes and retain descriptive issue links beside fixes.

## 2.2 Detailed Minor Notes

- Location: `releases/<group>/<major>.<minor>.md`
- Contains:
  - Deep architectural explanation
  - Internal module movements
  - Test matrix expansions
  - Execution-shape changes
  - Validation and invariant notes
  - Migration commentary
- This is the preferred place for code examples, LoC snapshots, and fenced blocks (` ``` `) that improve scanability.
- Detailed minor notes may be substantially more verbose than group main-changelog entries.

All patch releases in the same minor line share one detailed notes file.
Example: `0.33.0`, `0.33.1`, and `0.33.2` all map to [docs/changelog/0.33.md](../changelog/0.33.md).

The group main changelog must link to the detailed file when present.

## 2.3 Active Version Work

The latest release or numbered pending entry stays at the top. The repository has no
`Unreleased` section or separate notes queue. Record meaningful completed
behavior and tooling in the current group entry and matching minor-line notes;
governance-only edits need no note unless requested.

Apply [the shared automatic next-version rules](https://github.com/dragginzgame/toko-tooling/blob/5806e6e4943f6be600cd53385498ce08a5f804c4/rules/changelogs.md).
Use one undated `## [X.Y.Z]` at the top of the group ledger and matching minor-line notes.
Derive the version from the latest finalized release and the complete pending
batch: compatible pre-1.0 work selects the next patch; breaking public API,
semantics or output selects the next minor. Honour compatible maintainer overrides
and report conflicting selections. Do not increment again for each landing slice.
The next minor always increments the minor by one and resets patch to zero;
reuse that pending line until released. Do not ask the user to name or confirm
the number when they direct work to start on the next minor.

When the pending candidate changes minor line, move only its unpublished notes
and update the group ledger's detail link. Keep finalized notes in their original files.
Selecting that heading does not change Cargo versions or authorize new
implementation scope, commits, tags, pushes or publication.

Release preparation reconciles the computed candidate with the pending heading
and adds its UTC date. Note presentation must not block deployment; concrete
release identity conflicts must be resolved before preparation.

---

# 3. Version Entry Rules (Group Main Changelog)

Released entries use the following form; pending entries use
`## [X.Y.Z]` without a date:

## [<version>] – <YYYY-MM-DD> – <Short Title>

### Added
- High-level new capabilities

### Changed
- Architectural or behavioral changes

### Removed
- Removed APIs, contracts, or behaviors

Rules:

1. Keep the existing changelog structure and header format.
2. Smaller entries may omit the title segment and use:
   `## [<version>] - <YYYY-MM-DD>`.
3. Use the shared emoji subsection format; include only relevant categories.
4. If an entry reaches 4 lines or more of changelog content, split it into subsection headers.
5. A small release may use one `📝 Summary` subsection with concise bullets.
6. For internal cleanup/audit passes, use subsection headers and include an explicit `Audit` subsection with footprint stats.
7. If a section like `Changed` becomes large, split into topic-based subheaders (for example `Changed - Aggregate Execution`, `Changed - Structure`).
8. Do not include file paths.
9. Do not include test names.
10. Do not include internal refactor noise.
11. Aim for roughly 15 bullets in a group entry without removing meaningful issue coverage.
12. If a section exceeds ~4 lines of explanation, move detail to `releases/<group>/<major>.<minor>.md`.
13. Keep separate outcome bullets within each patch; do not compress a broad
    pending batch into one omnibus summary.
14. Each bullet describes one coherent outcome. Link its owning issue with its
    number and a descriptive title, following the shared issue-link rules.
15. Move supporting implementation detail to `releases/<group>/<major>.<minor>.md`;
    keep meaningful fixes and their issue links visible in the group ledger.
16. Extend the existing pending version as fixes complete. Add or update outcome
    bullets without replacing earlier issue-linked fixes or creating a version
    per slice. After publication, use the shared next-version selection rules.

## 3.1 Section Header Emoji Mapping

When section headers are used in group ledgers or their minor-line notes,
emoji-prefixed headers are the default and must use this fixed mapping:

- `Added=➕`
- `Changed=🔧`
- `Fixed=🩹`
- `Removed=🗑️`
- `Breaking=⚠️`
- `Migration Notes=🧭`
- `Summary=📝`
- `Cleanup=🧹`
- `Audit=📊`
- `Testing=🧪`
- `Governance=🥾`
- `Documentation=📚`

Keep emoji usage consistent across releases.

## 3.2 Link Formatting

For group-ledger references to detailed notes, links must be clickable Markdown links.

Use:

`[docs/changelog/0.33.md](../changelog/0.33.md)`

Do not use plain backticked path text for detailed-breakdown links.

---

# 4. Automation Rules for Agents

During ordinary development:

1. Prefer focused code slices and focused validation.
2. Keep meaningful completed behavior/tooling in the current draft and shared
   minor-line notes before handoff. Governance-only edits remain exempt unless
   the user requests a note.
3. Automatically maintain one numbered, undated pending entry from the latest
   finalized release and the complete batch. Date it only during release
   preparation; numbering alone authorizes no package or publication effects.
4. Never add an `Unreleased` section or rewrite a published version.

When preparing a release:

1. Identify all changes since last version tag.
2. Group changes into:
   - Added
   - Changed
   - Removed
3. Extract only architectural or behavioral changes.
4. Ignore:
   - Formatting-only changes
   - Test-only changes (unless behaviorally significant)
   - Internal renames without surface impact
5. Generate a concise summary entry in the selected group ledger.
6. Generate or update docs/changelog/<major>.<minor>.md with full detail.
7. Insert a clickable Markdown link from the group ledger to its detailed file.
8. Use the version specified by the release request or the existing latest changelog entry.
9. Do not create a new version header if the newest entry already exists for the target version.
10. If a change set is changelog-policy/governance-only, do not add or update release notes in `releases/<group>/CHANGELOG.md` or `releases/<group>/<major>.<minor>.md` unless requested.
11. Reconcile the complete candidate into the already active group and detailed
    entries. Reconstruct missing notes when practical, but do not make their
    absence alone a mechanical release blocker.

In agent sessions, version bump commands, release commits, tags, and pushes are
user-owned. Agents may prepare release notes and report readiness, but they do
not execute those publication actions.

Agents must never:

- Delete historical version entries.
- Rewrite previous release summaries.
- Reorder version history.
- Collapse multiple minor lines into one detailed file.
- Add release notes for changelog-policy/governance-only edits (for example updates to `docs/governance/changelog.md`, `AGENTS.md`, or changelog formatting policy), unless explicitly requested as a documented release artifact.
- Treat a small development slice as requiring its own patch release.

---

# 5. Breaking Changes

Breaking public API or semantic changes require a minor release before 1.0.
Select the pending heading automatically, but do not accept a conflicting patch target or reuse a
frozen storage discriminator; coordinate retained consumers and reset/retirement
obligations before the hard cut.

If a change alters:

- Public API
- Response types
- Cursor format
- Execution semantics
- Error taxonomy
- Persistence format

The group main-changelog entry must:

- Include a clear note under "Changed" or "Removed".
- Mention migration implications.
- Be explicitly marked as potentially breaking.

---

# 6. Archival Policy

Older detailed entries may be moved from a group main changelog
into its `releases/<group>/<major>.<minor>.md` if the group ledger grows large.

When archiving:

- Leave the version header in the group ledger.
- Replace detailed content with a summary.
- Insert link to detailed file.

Historical content must never be discarded.

---

# 7. SemVer Enforcement

- MAJOR: incompatible surface or behavioral changes.
- MINOR: additive capability.
- PATCH: internal fixes without surface change.

Agents must not bump version without checking semantic impact.
When updating changelog entries, target the upcoming release version even if `Cargo.toml` still has the previous published version.

---

# 8. Writing Style, Verbosity, and Jargon

Use plain, industry-friendly language.

Required writing style:

- Lead with outcome and user impact.
- Keep wording concise and junior-friendly.
- Avoid jargon unless the technical term materially improves clarity.
- Keep entries intentionally brief and non-technical by default.
- Include deep internal names only when required for migration or debugging.
- Prefer a small number of consolidated bullets over long fragmented lists.
- Explain why a change matters, not only what changed.

Bullet and detail rules:

- Prefer short bullets (1-2 sentences), with inline code formatting for API/type names when relevant.
- Bullets do not need to be single-line if additional sentence context is needed.
- In group ledgers, use short outcome bullets with descriptive issue links;
  preserve separate fixes as the pending batch grows. Supporting detail belongs
  in the minor-line notes, rather than an omnibus paragraph or detached issue list.
- Avoid deep implementation detail (module paths, helper names, routing internals) unless required for migration/debugging.
- In selected group ledger, avoid code examples/LoC dumps unless strictly necessary.
- Prefer placing code examples, LoC snapshots, and fenced blocks in `releases/<group>/<major>.<minor>.md`.
- Inline fenced examples are optional, not mandatory.
- In selected group ledger, include at most one inline fenced example per minor version (`0.x.x` line), and only when it materially improves clarity.
- In `releases/<group>/<major>.<minor>.md`, include at most one inline fenced example per patch entry (`## 0.x.y`), and only when it materially improves clarity.
- Use inline fenced examples only for meaningful code, config, or flow snapshots that explain behavior better than prose; if no good example exists, skip it.
- If a minor-version patch makes a new user-visible SQL query family executable, include one representative SQL example for that patch in `releases/<group>/<major>.<minor>.md`.
- That example should show a real newly-admitted query shape, not just a nearby query that was already possible before the patch.
- Prefer the smallest query that demonstrates the newly-shipped surface clearly.
- The group main changelog may omit the example when the one-line summary is clear enough, but the detailed minor-line notes should carry it for query-surface widenings.

Testing section rules:

- Do not add a `Testing` section for routine validation runs (`make check`, `make test`, `cargo test`).
- Add `Testing` only when the release adds or changes tests, coverage, or test tooling.

---

# 9. Release Flow

For each release:

1. Include every relevant change in the numbered, undated group and minor-line
   pending entries. Select the release kind matching the whole batch's compatibility.
2. The maintainer reviews and commits the source and notes; start from a clean
   tree on the configured release branch.
3. Invoke `make release-patch RELEASE_GROUP=database`, `make release-minor RELEASE_GROUP=database` or `make release-major RELEASE_GROUP=database`.
   The shared runner checks release identities, validates the committed source,
   saves preparation intent, prepares and formats exact metadata, then stages,
   commits, tags and atomically pushes the branch and selected tag. Consumer
   receipts bind the prepared transition to the tested source and dependency graph.
4. After a failure, inspect the retained plan and lock owner. Resume a saved
   candidate with `make release-resume RELEASE_GROUP=database VERSION=X.Y.Z`; a preflight or validation
   failure before preparation permits a fresh attempt. Never reset conflicting
   source, index or remote identities to force a retry.
5. Invoke package publication or deployment separately when authorized. Retain
   build outputs, validation evidence and recovery plans after successful push;
   cleanup is separately scoped and explicitly invoked.

The [shared release contract](https://github.com/dragginzgame/toko-tooling/blob/5806e6e4943f6be600cd53385498ce08a5f804c4/docs/releases/releases.md) owns execution and recovery.
Pending notes are batch boundaries, not a new version per landing slice. A failed
validation leaves package versions, release commits and tags untouched. Agent
sessions maintain notes and focused evidence; the maintainer owns release execution.

---

# 10. Ownership

Changelog governance is architectural, not cosmetic.

It documents system evolution and must reflect real semantic shifts.

It is part of IcyDB's correctness discipline.

## Independent group ownership

Use `RELEASE_GROUP=cli` for developer CLI releases and `RELEASE_GROUP=database`
for the coordinated engine, schema, model, macros and approved Core dev-support packages.
Each group derives its pending version from its own last release. Moving an
unpublished draft to a new minor also updates that group’s `metadata_files` entry
in `ci/cargo-release-groups.json`. Preserve all finalized details in place.
A database dependency update may change the root catalog without releasing the
CLI package; publish the CLI separately when its adopted contract needs delivery.
The isolated Canic workspace retains its own qualification lock and versions.
