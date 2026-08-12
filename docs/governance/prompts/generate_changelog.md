# IcyDB Changelog Preparation Prompt

Prepare changelog material for IcyDB. Before acting, read and follow
`docs/governance/changelog.md`; it is the sole policy authority and overrides
this helper when the two differ.

## Inputs

- Target version, supplied explicitly by the user
- Release date
- Changes since the last released tag
- Existing root `Unreleased` notes

If the user has not supplied a target version, update only the root
`CHANGELOG.md` `Unreleased` section. Do not infer a patch number.

## Required Work

1. Classify behavioral, architectural, public-surface, execution, diagnostic,
   persistence, and migration effects. Ignore formatting-only work, incidental
   internal renames, and tests that do not establish meaningful behavior.
2. Report any mismatch between the supplied version and the repository's
   current SemVer policy; never silently change the target.
3. For release preparation, collapse relevant `Unreleased` material into one
   concise root bullet for the target patch and update the shared minor-line
   detail file at `docs/changelog/<major>.<minor>.md`.
4. Preserve the existing root header style, section structure, chronological
   order, and all historical content. Link the root entry to the minor-line
   detail file when it exists.
5. Keep root prose user-impact first. Put implementation explanation,
   validation evidence, and migration detail in the minor-line file.

Do not create `docs/changelog/<version>.md`, add another `Unreleased` section,
pre-bump manifests or `Cargo.lock`, run release commands, commit, tag, or push.
A missing slice note should be reported and reconstructed when practical, but
its absence alone is not a mechanical release blocker.

Return the proposed edits plus any policy or evidence issue that still needs
review.
