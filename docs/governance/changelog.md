# IcyDB Changelog Governance

This document defines the authoritative rules for maintaining
`CHANGELOG.md` and minor-line changelog archives.

These rules are intended to be followed by automated agents.

---

# 1. Purpose

The root `CHANGELOG.md` file is the canonical release ledger for IcyDB.

It records high-level architectural and behavioral changes per release.

It must remain concise and structured.

Detailed change breakdowns belong in:

`docs/changelog/<major>.<minor>.md`

For example: [docs/changelog/0.33.md](../changelog/0.33.md)

---

# 2. File Structure

## 2.1 Canonical Ledger

- Root: `CHANGELOG.md`
- Must contain:
  - Version headers
  - Date
  - High-level summary sections
  - Links to detailed notes
- Root minor-line summary entries must use exactly one concise bullet per patch version.

## 2.2 Detailed Minor Notes

- Location: `docs/changelog/<major>.<minor>.md`
- Contains:
  - Deep architectural explanation
  - Internal module movements
  - Test matrix expansions
  - Execution-shape changes
  - Validation and invariant notes
  - Migration commentary
- This is the preferred place for code examples, LoC snapshots, and fenced blocks (` ``` `) that improve scanability.
- Detailed minor notes may be substantially more verbose than root changelog entries.

All patch releases in the same minor line share one detailed notes file.
Example: `0.33.0`, `0.33.1`, and `0.33.2` all map to [docs/changelog/0.33.md](../changelog/0.33.md).

The root changelog must link to the detailed file when present.

## 2.3 Active Version Work

Canonical rule: the repository has no `Unreleased` section. Every code slice
is recorded directly under the latest active root version and its shared
`docs/changelog/<major>.<minor>.md` entry.

The newest root version is active while it has no matching `v<version>` release
tag and the user has not reported it pushed, published, or live. Revise its one
concise root patch bullet and detailed notes as coherent work accumulates; do
not create one patch entry per landing slice. Governance-only edits do not
require a note unless explicitly requested.

Once the latest version has a matching release tag or is reported published,
never rewrite it. If work continues in the same explicitly authorized minor
line, automatically open the next patch number. When the user explicitly
starts a different minor line, automatically open its `.0` entry. This
changelog-only version selection does not authorize crossing a minor boundary,
mutating Cargo versions, committing, tagging, or publishing.

This is authoring and handoff discipline, not a mechanical push or release
gate. If a note is missing, report and reconstruct it when practical, but do
not stop an otherwise ready push or release solely because the changelog was
not updated.

---

# 3. Version Entry Rules (Root CHANGELOG.md)

Each version entry must follow:

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
3. Changelog subsections are optional; include only sections relevant to that release.
4. If an entry reaches 4 lines or more of changelog content, split it into subsection headers.
5. For small cleanup releases, prefer no subsection headers; use a short plain-language summary with concise bullets.
6. For internal cleanup/audit passes, use subsection headers and include an explicit `Audit` subsection with footprint stats.
7. If a section like `Changed` becomes large, split into topic-based subheaders (for example `Changed - Aggregate Execution`, `Changed - Structure`).
8. Do not include file paths.
9. Do not include test names.
10. Do not include internal refactor noise.
11. Do not exceed ~15 bullets total in the root entry.
12. If a section exceeds ~4 lines of explanation, move detail to `docs/changelog/<major>.<minor>.md`.
13. For a root minor-line entry (`<major>.<minor>.x`), use exactly one bullet per patch version listed in that minor line.
14. Each root minor-line patch bullet must be a high-level summary sentence, not an exhaustive implementation list.
15. If a patch bullet starts becoming a multi-clause internal inventory, shorten it and move detail to `docs/changelog/<major>.<minor>.md`.
16. Do not add a new root patch bullet for every code slice. Update the one
    active patch bullet until that version is reported published; then open the
    next patch only as defined by the active-version rule above.

## 3.1 Section Header Emoji Mapping

When section headers are used in `CHANGELOG.md` or `docs/changelog/*.md`,
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

For root changelog references to detailed notes, links must be clickable Markdown links.

Use:

`[docs/changelog/0.33.md](../changelog/0.33.md)`

Do not use plain backticked path text for detailed-breakdown links.

---

# 4. Automation Rules for Agents

During ordinary development:

1. Prefer focused code slices and focused validation.
2. For every code slice, update the latest active root patch bullet and its
   shared minor-line notes before handoff. Governance-only edits remain exempt
   unless the user requests a note.
3. If the latest version has a matching release tag or was reported published,
   select the next patch within the same explicitly authorized minor
   automatically. An explicitly started new minor begins at `.0`.
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
5. Generate a concise summary entry in root CHANGELOG.md.
6. Generate or update docs/changelog/<major>.<minor>.md with full detail.
7. Insert clickable Markdown link from root file to detailed file.
8. Use the version specified by the release request or the existing latest changelog entry.
9. Do not create a new version header if the newest entry already exists for the target version.
10. If a change set is changelog-policy/governance-only, do not add or update release notes in `CHANGELOG.md` or `docs/changelog/<major>.<minor>.md`.
11. Reconcile the complete candidate into the already active root and detailed
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

If a change alters:

- Public API
- Response types
- Cursor format
- Execution semantics
- Error taxonomy
- Persistence format

The root entry must:

- Include a clear note under "Changed" or "Removed".
- Mention migration implications.
- Be explicitly marked as potentially breaking.

---

# 6. Archival Policy

Older detailed entries may be moved from root CHANGELOG.md
into docs/changelog/<major>.<minor>.md if the root file grows large.

When archiving:

- Leave version header in root.
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
- In root minor-line summaries, prefer one short sentence per patch bullet; avoid long multi-clause bullets that enumerate every internal change.
- Avoid deep implementation detail (module paths, helper names, routing internals) unless required for migration/debugging.
- In root `CHANGELOG.md`, avoid code examples/LoC dumps unless strictly necessary.
- Prefer placing code examples, LoC snapshots, and fenced blocks in `docs/changelog/<major>.<minor>.md`.
- Inline fenced examples are optional, not mandatory.
- In root `CHANGELOG.md`, include at most one inline fenced example per minor version (`0.x.x` line), and only when it materially improves clarity.
- In `docs/changelog/<major>.<minor>.md`, include at most one inline fenced example per patch entry (`## 0.x.y`), and only when it materially improves clarity.
- Use inline fenced examples only for meaningful code, config, or flow snapshots that explain behavior better than prose; if no good example exists, skip it.
- If a minor-version patch makes a new user-visible SQL query family executable, include one representative SQL example for that patch in `docs/changelog/<major>.<minor>.md`.
- That example should show a real newly-admitted query shape, not just a nearby query that was already possible before the patch.
- Prefer the smallest query that demonstrates the newly-shipped surface clearly.
- Root `CHANGELOG.md` may still omit the example when the one-line summary is clear enough, but the detailed minor-line notes should carry it for query-surface widenings.

Testing section rules:

- Do not add a `Testing` section for routine validation runs (`make check`, `make test`, `cargo test`).
- Add `Testing` only when the release adds or changes tests, coverage, or test tooling.

---

# 9. Release Flow

For each release:

1. Confirm the active target patch entry includes every relevant candidate change.
2. Update CHANGELOG.md with one concise bullet for the target patch.
3. Create or update docs/changelog/<major>.<minor>.md.
4. Commit the code and changelog changes.
5. Run `make patch`, `make minor`, or `make major`. The target runs
   `make validate` once against the source candidate before any version mutation. Root or
   detailed changelog edits may remain staged or unstaged and are included in
   the release transition; every other tracked change stops the release before
   the expensive gate starts and is checked again afterward. The version bump
   resolves offline to preserve the tested dependency graph, then records the
   exact version-and-release-note diff. A failure leaves any generated mutation
   visible for review; release tooling never restores files automatically.
6. Review the release diff.
7. Run `make release-stage` to stage known release files.
8. Run `make release-commit`. It must verify the staged diff against the tested
   candidate receipt, commit only that transition, verify the committed diff
   again, then tag it and record the exact release-commit receipt. It must not
   rerun validation after creating the commit.
9. Run `make release-push` to publish the release tag. Push performs no hidden
   validation; the explicit pre-bump `make validate` workflow owns that work.
   Successful push cleanup removes transient release state but preserves the
   validated Cargo build cache. Use `make release-clean` when an explicit full
   build-cache and transient-state cleanup is required.

Order must be preserved.
Patch releases are batch boundaries, not required endpoints for each code
slice. After code and changelog changes are committed, the reviewable release flow is
`make patch`, `git diff`, `make release-stage`, `make release-commit`,
`make release-push`, then `cargo publish`. A failed validation workflow leaves the
version, release commit, and tag untouched.

---

# 10. Ownership

Changelog governance is architectural, not cosmetic.

It documents system evolution and must reflect real semantic shifts.

It is part of IcyDB's correctness discipline.
