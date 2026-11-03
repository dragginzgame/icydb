# IcyDB Changelog Preparation Prompt

Prepare changelog material for IcyDB. Before acting, read and follow
the shared `rules/changelogs.md` and local `docs/governance/changelog.md`.
`AGENTS.md` establishes policy precedence; this helper does not override either.

## Inputs

- Selected release group, latest active version, matching-tag state, and whether the user has
  reported it published
- Target version, when supplied explicitly by the user
- Release date, only during explicitly authorized release preparation
- Changes since the last released tag
- Existing active-version group ledger and detailed notes

Maintain one numbered, undated pending entry per release group. Derive its
version from the latest finalized release and the complete pending batch under
the shared numbering rules; another compatible fix extends the same entry.
Keep published history unchanged. Never create `Draft` or `Unreleased` sections,
change package versions, or cross an implementation minor boundary implicitly.
Breaking public API or semantic changes require a minor release before 1.0;
report a conflicting target.

## Required Work

1. Classify behavioral, architectural, public-surface, execution, diagnostic,
   persistence, and migration effects. Ignore formatting-only work, incidental
   internal renames, and tests that do not establish meaningful behavior.
2. Report any mismatch between the supplied version and the repository's
   current SemVer policy; never silently change the target.
3. Extend the selected pending group entry in `releases/<group>/CHANGELOG.md`
   and its `releases/<group>/<major>.<minor>.md` details. Every fixed issue needs
   a direct descriptive link beside its outcome in that group's main changelog.
   Retain those links after release. Under IcyDB's maintainer-approved override,
   the top-level `CHANGELOG.md` remains group/history navigation only.
4. Preserve earlier issue-linked fixes when adding outcomes. Use relevant emoji
   sections and separate concise bullets; never replace a broad pending batch
   with one omnibus paragraph. Keep each issue's number and descriptive title
   in its clickable link beside the owning change, in both views when both
   describe that specific fix. Distinguish partial corrections from full fixes.
5. Preserve patch identities, chronological order and all published content.
   Link each group entry to its matching minor-line detail file. Keep prose
   user-impact first; supporting implementation and migration details belong
   in that file, not an inventory of private symbols or routine test commands.

Do not create another notes queue, add a `Draft` or `Unreleased` section,
rewrite a reported-published version, pre-bump manifests or `Cargo.lock`, run
release commands, commit, tag, or push. A missing slice note should be reported
and reconstructed when practical, but its absence alone is not a mechanical
release blocker.

Return the proposed edits plus any policy or evidence issue that still needs
review.
