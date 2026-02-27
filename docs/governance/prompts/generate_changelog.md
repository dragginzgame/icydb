# IcyDB Automated Changelog Generation Prompt

You are generating a release changelog for IcyDB.

You MUST follow the rules defined in:

docs/governance/changelog.md

Before writing anything, read and comply with that document.

---

## Inputs

- Target version: <VERSION>
- Release date: <YYYY-MM-DD>
- Git diff: changes since last tag
- Last released version: <PREVIOUS_VERSION>

---

## Required Output

You must generate TWO outputs:

1. Root CHANGELOG.md entry (concise)
2. docs/changelog/<VERSION>.md (detailed)

---

## Step 1 — Classify Changes

From the diff:

1. Identify architectural changes.
2. Identify behavioral changes.
3. Identify public API surface changes.
4. Identify execution semantic changes.
5. Identify error taxonomy changes.
6. Identify persistence or cursor changes.

Ignore:

- Formatting-only changes
- Comment-only changes
- Test-only changes (unless behaviorally meaningful)
- Internal renames without surface impact

---

## Step 2 — Determine Version Impact

Apply SemVer rules:

- MAJOR if incompatible surface or behavioral change
- MINOR if additive feature
- PATCH if internal fix only

If proposed version violates SemVer, flag inconsistency.

---

## Step 3 — Generate Root CHANGELOG Entry

Format EXACTLY:

## [<VERSION>] – <DATE> – <Short Title>

### Added
- ...

### Changed
- ...

### Removed
- ...

Rules:

- Do NOT include file paths.
- Do NOT include test names.
- Do NOT exceed 15 bullets total.
- Keep bullets concise and architectural.
- If detailed explanation is needed, defer to docs/changelog/<VERSION>.md.
- If breaking, clearly state migration impact.

End with:

See detailed breakdown:
`docs/changelog/<VERSION>.md`

---

## Step 4 — Generate Detailed Version File

File path:

docs/changelog/<VERSION>.md

Structure:

# IcyDB <VERSION>

## Overview

High-level description of release theme.

## Architectural Changes

Detailed explanation of internal shifts.

## Execution Changes

Executor, planner, or runtime modifications.

## Validation & Invariants

New or changed invariants.

## Error Model Changes

Typed error additions or modifications.

## Compatibility Notes

Migration notes (if any).

## Test & Verification Matrix

Summarize meaningful verification additions.
Do not list every test.

---

## Step 5 — Preservation Rules

You MUST:

- Append to root CHANGELOG.md.
- Never modify previous version entries.
- Never reorder history.
- Never delete content.
- Never collapse prior versions.

---

## Step 6 — Quality Checklist

Before finalizing:

- Does root entry remain concise?
- Are deep details moved to detailed file?
- Are breaking changes explicitly stated?
- Does version match semantic impact?
- Are irrelevant changes excluded?

If any rule is violated, correct before finalizing.

---

## Output Format

Return:

1. Root CHANGELOG.md snippet.
2. Full content for docs/changelog/<VERSION>.md.

Do NOT include commentary.
Do NOT include analysis.
Return only file-ready content.