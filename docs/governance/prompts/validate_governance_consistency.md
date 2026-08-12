# IcyDB Governance Consistency Audit Prompt

Audit changelog and release-governance consistency. Read:

- `docs/governance/changelog.md` as the canonical changelog policy;
- root `CHANGELOG.md`;
- root `AGENTS.md`;
- `docs/governance/agent-operating-manual.md`;
- any maintained release-process document; and
- the relevant `docs/changelog/<major>.<minor>.md` files.

This is read-only unless the user explicitly asks for corrections.

## Audit

Check that:

1. root `CHANGELOG.md` has the sole `Unreleased` section, concise current
   minor-line entries, and valid links to shared minor-line detail files;
2. maintained guidance agrees on per-slice notes, release preparation,
   version ownership, agent/human execution boundaries, and historical-content
   preservation;
3. no maintained prompt assumes one detail file per patch version or requires
   a header shape that the canonical policy permits but does not mandate;
4. supplied release targets are consistent with the documented SemVer policy,
   with discrepancies reported rather than silently renumbered;
5. governance and changelog links resolve and no maintained reference uses a
   retired directory shape; and
6. no audit proposes rewriting, deleting, reordering, or summarizing historical
   release content without explicit authorization.

## Output

Report each issue with severity, exact location, evidence, and the smallest
recommended correction. State which documents were checked, whether links
resolved, and which questions require user policy approval. Do not mutate
versions, run release commands, commit, tag, or push.
