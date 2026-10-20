# One Cargo publication attempt

`scripts/ci/run-cargo-publication.sh` executes one explicitly authorized,
consumer-admitted Cargo command. It owns retained intent, exact arguments,
command output and exit statuses. It never installs tools, discovers missing
packages, retries an upload or decides whether a previous upload succeeded.
The [release contract](releases.md) keeps publication separate from release.

## Calling contract

Requires Bash 3.2, GNU Make, Git, Cargo, tee and standard Unix utilities. Select and qualify
a Cargo version supporting every requested option and repeated package selection;
this does not change the library's MSRV. Run from the consumer's admitted source
checkout. Prepare an ignored evidence parent and any explicit target directory
before the consumer's final source check. For example, after consumer admission:

```bash
bash scripts/ci/run-cargo-publication.sh check \
  target/publication/check-unique "$admitted_commit" crates-io \
  --package example-library@1.2.3 --package example-cli@1.2.3 \
  --all-features
```

`check` always adds `--dry-run`; `publish` may upload and needs explicit publication
authorization. Both add `--locked` and the selected named registry. There is no
implicit workspace selection: every package must be an exact `NAME@X.Y.Z`, with
no duplicate names. Different selected package versions are allowed. Cargo
resolves those selectors; the consumer must first validate the complete eligible
package/version set and any intentionally omitted, already-published packages.

Supported additional options are `--manifest-path`, `--target-dir`, `--features`,
`--all-features`, `--no-default-features`, `--no-verify` and, for check only,
`--allow-dirty`. Path-option directories must already exist. Other options are
refused before allocation or Cargo execution. Explicit manifests must be regular,
non-symlink files within the selected Git checkout. Registry configuration and credentials
remain with the caller's Cargo environment; do not put secrets in feature/path
arguments. The recorded registry name is not independent proof of its configured
endpoint. Explicit offline settings remain authoritative.

Before dispatch the helper checks the supplied source commit against HEAD and
rechecks both HEAD and Git status after recording intent. Upload requires clean
source. A dirty check requires explicit `--allow-dirty` for Cargo to admit it.
These are race checks, not a seal over arbitrary concurrent filesystem changes.
The consumer retains its wider publication lock, source/tag/provenance admission,
registry observations, verification policy and final eligibility check.

## Evidence and interruption

The attempt directory must not exist, including as a dangling symlink. Its parent
must exist. Raw and resolved directory paths must contain no CR/LF. Spaces work.
An atomic `.cargo-publication.lock` in that parent excludes concurrent helper
attempts sharing the parent; consumers must use one stable parent. This does not
replace a consumer lock covering admission, provenance preparation and recovery.

Before Cargo starts, the fresh directory receives `intent` (mode, source, registry,
then exact package selectors), `cwd`, `working-status`, and NUL-separated
`arguments` including the executable name. A `dispatch` marker records that Cargo
may have started. `cargo.log` retains combined output. On completion, `cargo-status`,
`log-status` and `status` record the two pipeline statuses and returned status.
Cargo's nonzero status wins over a logger failure; logger failure cannot turn into
success. Evidence is retained on success and failure. No attempt is overwritten.

A completed attempt releases the helper lock. An interrupted or incomplete attempt
retains it for inspection, including failures while writing evidence. A dispatch
marker without completed statuses is uncertain, not proof of registry absence.
No resume or automatic retry exists. The consumer must reconcile exact registry
and provenance state, inspect the lock owner, and explicitly select any fresh
attempt. A failed invocation is never permission to repeat an upload blindly.
Status zero reports command completion, not independent registry readback.

## Prepared package provenance

`scripts/ci/check-cargo-package-provenance.sh` checks archives already prepared by
Cargo, before any upload. The caller supplies a fresh evidence directory, existing
archive directory, declared source repository, exact commit and each admitted
package/version with its repository-relative directory:

```bash
bash scripts/ci/check-cargo-package-provenance.sh \
  target/publication/provenance-unique target/package "$repository" "$commit" \
  --package example-library@1.2.3 crates/library \
  --package example-cli@2.0.0 apps/cli
```

Use `.` for a package at the repository root. Package versions may differ;
duplicate names and abnormal relative paths refuse before allocating evidence.
Directories containing spaces work; CR/LF paths and symlink archives refuse.
The caller owns archive creation, complete package eligibility, trusted namespace
custody and exclusion of concurrent archive/evidence replacement. This helper
does not prepare packages or authorize publication, network access or retries.
Fetching from an external repository requires the caller's network authority.

Each regular archive must contain exactly one JSON provenance value with the
selected commit, clean Git state and exact package directory. The helper retains
the extracted JSON and tar diagnostics, then fetches that commit from the declared
repository into a fresh bare object store. Its object format matches the admitted
SHA-1 or SHA-256 identity, independently of ambient Git defaults.
Inherited Git object/directory overrides
are cleared so cached objects cannot substitute for retrieval. Failed commands
refuse even when they print plausible output; the retrieved commit must match.
Source repository configuration, credentials and transport policy stay caller-owned.

The fresh evidence directory retains NUL-separated `packages` and `paths`, the
repository/commit `source-request`, per-package `*-vcs.json` and `*-vcs.log`, and
Git initialization/fetch logs and `source.git`. Only complete success writes
`source-verified`. Occupied evidence is never overwritten or retried. The caller
retains its wider admission lock, final source recheck and interruption recovery.
Select the helper as a snapshot entrypoint; its guide is a declared companion.
The focused fixture uses real local Git history and archives without external
network requests; native macOS qualification remains separate.

## Consumer adoption boundary

Keep existing adapters until their own equivalence checks pass against a reviewed
snapshot. Export the runner and its declared companion; do not patch vendored
copies. Consumers retain these distinct obligations:

| Consumer | Checks and recovery that stay local |
| --- | --- |
| Host Tooling | Package allowlist, synchronized versions, licenses, declared registry/repository, package preparation, explicit provenance-helper inputs, all-feature selection and final source admission. |
| Canic | Resume-package admission, tri-state registry observations, selected missing set and final verification of the complete matching set, including predecessors omitted by resume. |
| IcyDB | Complete publishable-set admission, validation-only mode, resume selection and exact release-receipt qualification before choosing `--no-verify`. |
| Query | Exact absence versus lookup failure, source admission, library visibility before CLI upload, bounded retry settings and partial-upload reconciliation. |

Cargo owns dependency ordering, packaging, verification and upload for its admitted
selection. Do not assume this command proves equivalence with each consumer's
polling or recovery contract. Shared fixtures substitute Cargo/Git/logger effects
to test dispatch and evidence. They do not prove real registry propagation,
consumer policy or native host qualification. Consumer adoption must separately
exercise partial publication, unavailable observations, omitted packages, source
changes and actual supported Cargo behavior without unauthorized uploads.
