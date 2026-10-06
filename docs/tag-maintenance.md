# Explicit tag maintenance

`scripts/dev/delete-github-tags-up-to.pl` shares the tag-selection and deletion
mechanics maintained independently in Canic and IcyDB. It requires Git
and Perl core modules on Linux/macOS. It is a separately selected maintainer
operation; releases, publication and ordinary cleanup never invoke it.

## Preview and selection

From a checkout root, list local stable-version tags through an explicit cutoff:

```sh
perl scripts/dev/delete-github-tags-up-to.pl --cutoff 0.1.3
```

Use `--repo /path/to/checkout` to select another checkout explicitly. Add
`--remote origin` to inventory that remote's single configured **push URL** too.
Preview creates no maintenance state and changes no refs; remote inventory does
perform a read-only network request. There is no implicit remote or cutoff.

`0.1.3` or `v0.1.3` includes stable tags up to that exact version. `0.1` or `v0.1`
includes earlier versions and every patch in that minor line. A single number,
leading zeros, prereleases and build suffixes are rejected as cutoffs. Only
canonical stable `X.Y.Z`/`vX.Y.Z` tag names are selected; other tags remain untouched.
Sorting compares decimal components without fixed-width arithmetic or `sort -V`.

The preview prints each selected tag and its object ID. Annotated tags use their
tag-object identity, so replacing an annotation is an identity change even when
the peeled commit stays the same. Symbolic tags are rejected.

## Maintainer deletion

After reviewing the preview, a maintainer may explicitly select deletion:

```sh
perl scripts/dev/delete-github-tags-up-to.pl --cutoff 0.1.3 --remote origin \
  --delete-local --delete-remote --yes
```

Either deletion scope may be used alone. Remote deletion requires `--remote`;
every deletion requires `--yes`. The helper freezes a fresh selection at the
start of the deletion attempt; preview output is not a persisted approval file.
Review source, configured destination and current tags before selecting the effect.

Remote deletion runs first, in batches of at most 50 exact tag refspecs. Each
push uses `--atomic`, `--no-follow-tags` and an explicit expected object ID for
each tag through Git's [force-with-lease contract](https://git-scm.com/docs/git-push).
Unsupported atomic push, changed identities and failed observations stop the
operation. There is no fallback push or retry within a failing invocation.
Atomicity applies to each batch, not to all batches together.

When both scopes are selected, local tags remain until every selected remote tag
is observed absent. Local deletion then uses one
[Git ref transaction](https://git-scm.com/docs/git-update-ref) with expected IDs
and no symbolic dereferencing. Other refs and tags created after the saved
selection are excluded. The helper does not fetch objects, create backup tags,
delete GitHub Release objects or clean build artifacts.

## Interruption and evidence

The repository's common Git directory owns `tag-maintenance/lock`, so linked
worktrees share concurrency protection. Its owner file records the process and
checkout. Never clear an occupied lock until its process is known to have stopped.
Other Git writers are not governed by this lock; expected-value updates protect
the selected refs against identity changes.

Before any deletion, the helper saves `tag-maintenance/pending.json`, containing
the exact selection, scopes and destination. Private `attempt.*` directories
retain command output and status. An interrupted or failed operation leaves this
intent intact. Rerun the same command to reconcile it: absent tags are already
complete, unchanged tags remain eligible, and changed tags stop the retry. A lost
push reply is never treated as success or replayed without fresh observation.
New tags within the cutoff are excluded while that saved intent is pending.

Changing the cutoff, deletion scopes or destination while an intent is pending
is refused. Inspect the saved intent, retained logs and actual refs to resolve
an identity conflict; do not discard the intent merely to make a retry proceed.
On success, the intent moves into its attempt directory and remains as evidence.
Only the helper-owned lock is removed. These records preserve identities and
diagnostics, not Git object backups; existing local tags provide recovery copies
only until their separately selected deletion succeeds.

## Adoption and qualification

Adopt the helper and this guide through a reviewed committed snapshot. Replace
the old Canic/IcyDB `.sh` implementation, update invocations to `perl` and require
an explicit locally reviewed cutoff. IcyDB's old single-number shorthand must be
spelled as a full minor cutoff, such as `0.210`. Keep product cutoff decisions
local. Delete the old generic fixtures only after the replacement is qualified;
retain consumer-specific checks and documentation. Track adoption in
[#11](https://github.com/dragginzgame/shared-tooling/issues/11) and its linked
consumer issues separately from upstream implementation.

`scripts/ci/test-tag-maintenance.pl` substitutes every Git command to exercise
selection, failed inventories, exact identities, confirmation, concurrency,
batch failures, saved retry and uncertain replies. It creates no real commits or
tags and performs no real pushes. The portable suite runs it in the existing
native Linux/macOS matrix; local Linux success does not establish macOS evidence.
