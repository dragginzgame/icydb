# IcyDB Changelog Governance

This document defines the authoritative rules for maintaining
`CHANGELOG.md` and version-specific changelog archives.

These rules are intended to be followed by automated agents.

---

# 1. Purpose

The root `CHANGELOG.md` file is the canonical release ledger for IcyDB.

It records high-level architectural and behavioral changes per release.

It must remain concise and structured.

Detailed change breakdowns belong in:

docs/changelog/<version>.md

---

# 2. File Structure

## 2.1 Canonical Ledger

- Root: `CHANGELOG.md`
- Must contain:
  - Version headers
  - Date
  - High-level summary sections
  - Links to detailed notes

## 2.2 Detailed Version Notes

- Location: `docs/changelog/<version>.md`
- Contains:
  - Deep architectural explanation
  - Internal module movements
  - Test matrix expansions
  - Execution-shape changes
  - Validation and invariant notes
  - Migration commentary

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

1. Do not include file paths.
2. Do not include test names.
3. Do not include internal refactor noise.
4. Do not exceed ~15 bullet points total.
5. If a section exceeds ~4 lines of explanation,
   move detail to docs/changelog/<version>.md.

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
6. Generate or update docs/changelog/<version>.md with full detail.
7. Insert link from root file to detailed file.

Agents must never:

- Delete historical version entries.
- Rewrite previous release summaries.
- Reorder version history.
- Collapse multiple versions into one.

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
into docs/changelog/<version>.md if the root file grows large.

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

---

# 8. Release Flow

For each release:

1. Update version in Cargo.toml.
2. Update CHANGELOG.md.
3. Create or update docs/changelog/<version>.md.
4. Commit.
5. Tag release.

Order must be preserved.

---

# 9. Ownership

Changelog governance is architectural, not cosmetic.

It documents system evolution and must reflect real semantic shifts.

It is part of IcyDB's correctness discipline.