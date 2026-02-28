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

For example: [docs/changelog/0.33.md](docs/changelog/0.33.md)

---

# 2. File Structure

## 2.1 Canonical Ledger

- Root: `CHANGELOG.md`
- Must contain:
  - Version headers
  - Date
  - High-level summary sections
  - Links to detailed notes
- Root minor-line summary entries should use 2-3 concise bullets per release line.

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
Example: `0.33.0`, `0.33.1`, and `0.33.2` all map to [docs/changelog/0.33.md](docs/changelog/0.33.md).

The root changelog must link to the detailed file when present.

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
6. For structural cleanup/audit passes, use subsection headers and include an explicit `Audit` subsection with footprint stats.
7. If a section like `Changed` becomes large, split into topic-based subheaders (for example `Changed - Aggregate Execution`, `Changed - Structure`).
8. Do not include file paths.
9. Do not include test names.
10. Do not include internal refactor noise.
11. Do not exceed ~15 bullets total in the root entry.
12. If a section exceeds ~4 lines of explanation, move detail to `docs/changelog/<major>.<minor>.md`.
13. For a root minor-line entry (`<major>.<minor>.x`), target 2-3 summary bullets.

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

`[docs/changelog/0.33.md](docs/changelog/0.33.md)`

Do not use plain backticked path text for detailed-breakdown links.

---

# 4. Automation Rules for Agents

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

Agents must never:

- Delete historical version entries.
- Rewrite previous release summaries.
- Reorder version history.
- Collapse multiple minor lines into one detailed file.

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
- Avoid deep implementation detail (module paths, helper names, routing internals) unless required for migration/debugging.
- In root `CHANGELOG.md`, avoid code examples/LoC dumps unless strictly necessary.
- Prefer placing code examples, LoC snapshots, and fenced blocks in `docs/changelog/<major>.<minor>.md`.
- Use fenced code blocks in detailed minor docs whenever they materially improve readability and break up dense text.

Testing section rules:

- Do not add a `Testing` section for routine validation runs (`make check`, `make test`, `cargo test`).
- Add `Testing` only when the release adds or changes tests, coverage, or test tooling.

---

# 9. Release Flow

For each release:

1. Update version in Cargo.toml.
2. Update CHANGELOG.md.
3. Create or update docs/changelog/<major>.<minor>.md.
4. Commit.
5. Tag release.

Order must be preserved.
Typical release flow is `make patch` followed by `cargo publish`.

---

# 10. Ownership

Changelog governance is architectural, not cosmetic.

It documents system evolution and must reflect real semantic shifts.

It is part of IcyDB's correctness discipline.
