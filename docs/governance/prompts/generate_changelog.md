# IcyDB Changelog Preparation Prompt

Prepare changelog material for IcyDB. Before acting, read and follow
`docs/governance/changelog.md`; it is the sole policy authority and overrides
this helper when the two differ.

## Inputs

- Latest active root version, matching-tag state, and whether the user has
  reported it published
- Target version, when supplied explicitly by the user
- Release date
- Changes since the last released tag
- Existing active-version root and detailed notes

If the user has not supplied a target version, update the latest active version
directly. If it has a matching release tag or was reported published, open the
next patch in the same explicitly authorized minor; an explicitly started new
minor opens at `.0`. Never create an `Unreleased` section or cross a minor
boundary implicitly.

## Required Work

1. Classify behavioral, architectural, public-surface, execution, diagnostic,
   persistence, and migration effects. Ignore formatting-only work, incidental
   internal renames, and tests that do not establish meaningful behavior.
2. Report any mismatch between the supplied version and the repository's
   current SemVer policy; never silently change the target.
3. Keep one concise root bullet for the active target patch and update the
   shared minor-line detail file at `docs/changelog/<major>.<minor>.md`.
4. Preserve the existing root header style, section structure, chronological
   order, and all historical content. Link the root entry to the minor-line
   detail file when it exists.
5. Keep root prose user-impact first. Put implementation explanation,
   validation evidence, and migration detail in the minor-line file.

Do not create `docs/changelog/<version>.md`, add an `Unreleased` section,
rewrite a reported-published version, pre-bump manifests or `Cargo.lock`, run
release commands, commit, tag, or push. A missing slice note should be reported
and reconstructed when practical, but its absence alone is not a mechanical
release blocker.

Return the proposed edits plus any policy or evidence issue that still needs
review.
