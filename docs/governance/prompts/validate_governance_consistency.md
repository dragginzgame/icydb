You are auditing governance consistency across IcyDB documentation.

You must validate and normalize consistency between:

- CHANGELOG.md
- docs/governance/changelog.md
- AGENTS.md

Do NOT delete historical entries.
Do NOT rewrite release history.
Do NOT summarize past versions.
Do NOT invent semantic changes.

This is a governance consistency audit only.

---

# Phase 1 — Load Documents

Read:

1. CHANGELOG.md
2. docs/governance/changelog.md
3. AGENTS.md (root)
4. docs/governance/release-process.md (if present)
5. docs/changelog/ directory (if present)

---

# Phase 2 — Validate Structural Consistency

Ensure:

1. CHANGELOG.md follows Keep a Changelog structure.
2. Version headers follow format:

   ## [<version>] – <YYYY-MM-DD> – <Title>

3. Root changelog contains only high-level summaries.
4. Detailed entries (if large) are moved to docs/changelog/<version>.md.
5. Links from root changelog to detailed files are valid.
6. No broken relative links exist.

If violations are found:
- Propose precise structural correction.
- Do NOT remove historical content.
- Move detail into docs/changelog/<version>.md if necessary.

---

# Phase 3 — Validate Governance Rules

Ensure docs/governance/changelog.md defines:

- Canonical ledger policy
- Archival policy
- SemVer enforcement
- Automation rules
- Breaking-change policy
- Release flow

If missing sections:
- Add them.
- Preserve original intent.
- Do not remove custom rules.

---

# Phase 4 — Validate AGENTS.md Alignment

Ensure AGENTS.md:

- Does not contradict changelog governance.
- Does not instruct agents to modify historical entries.
- Does not instruct version rewriting.
- Clearly references governance documents where appropriate.

If AGENTS.md contains changelog instructions:
- Ensure they defer to docs/governance/changelog.md.
- Remove duplicated rules.
- Replace duplication with reference.

AGENTS.md must not override governance.

---

# Phase 5 — Validate SemVer Discipline

Cross-check:

- Version bumps match described change scope.
- No minor version includes breaking language without note.
- No patch version contains feature-level language.
- Breaking changes are clearly marked.

If inconsistencies are found:
- Flag them clearly.
- Suggest correction.
- Do NOT silently modify version numbers.

---

# Phase 6 — Link and Reference Integrity

Ensure:

- All docs/changelog/<version>.md files referenced exist.
- All referenced governance files exist.
- No dead internal links remain.
- No references to old directory structures remain.

Update relative paths if audits/ was moved under docs/.

---

# Phase 7 — Output

Produce:

1. A governance consistency report:
   - List of issues found
   - Severity (Critical / Structural / Cosmetic)
   - Recommended corrections

2. Updated versions of:
   - docs/governance/changelog.md (if needed)
   - AGENTS.md (if needed)
   - Any corrected link references

3. A final validation summary confirming:
   - No historical changelog content was removed
   - SemVer rules remain intact
   - Governance is internally consistent

Do NOT produce commentary outside these outputs.
Do NOT summarize history.
Do NOT rewrite past entries.