# Consuming Shared Tooling Snapshots

CI and release behavior must not depend on a sibling checkout, a moving Git
branch, or network availability. Consumers vendor a reviewed file set and
record its exact Shared Tooling source revision.

## Initial snapshot

Run the refresh helper from a clean Shared Tooling checkout:

```bash
/path/to/shared-tooling/scripts/distribution/refresh-consumer.sh \
  --consumer /path/to/consumer \
  --file scripts/ci/verify-file-checksum.sh \
  --file scripts/ci/verify-shared-tooling-snapshot.sh \
  --file scripts/ci/run-validation-targets.sh
```

The helper validates every source path, exports regular files and their
executable modes from the selected Git revision at the same relative paths,
and writes `.shared-tooling.snapshot` in the consumer. Ignored or otherwise
uncommitted files cannot enter the snapshot, and concurrent working-tree edits
cannot change the exported bytes. The manifest
records format version `1`, source remote, source commit, and the SHA-256 digest
and executable state of every vendored file.

Use `--manifest <relative-path>` to choose a different manifest location. Its
parent directories are created before consumer files are replaced. Pass the
same option to subsequent refreshes and drift verification.

File paths are intentionally identical in source and consumer. A repository
that needs a different path or behavior owns an adapter rather than a patched
shared copy.

Refresh checks every declared destination before replacing any file. It refuses
to overwrite staged or unstaged changes, deletions, or existing untracked/ignored
files. Preserve or reconcile those changes before retrying; unrelated dirty paths
remain allowed. A destination already matching the selected source bytes and
executable state is safe to retry, including after an interrupted refresh. Only
declare paths owned by the shared snapshot; the consumer's `AGENTS.md` remains local.
The manifest is reviewed configuration: an intentional file-set edit is read as
input and replaced with the resulting manifest, rather than rejected as dirty work.

## Refresh

After the initial snapshot, omit `--file`. The existing manifest owns the file
set:

```bash
/path/to/shared-tooling/scripts/distribution/refresh-consumer.sh \
  --consumer /path/to/consumer
```

Review the resulting consumer diff normally. Refresh never deletes a file and
does not commit, stage, or push changes.

The source remote must continue to match the manifest exactly. Switching
between SSH, HTTPS, or a fork is an explicit provenance change and requires a
reviewed manifest recreation.

Refresh stages files in a temporary directory under the consumer so the final
manifest replacement stays on the same filesystem. Consumers should ignore
`.shared-tooling-refresh.*` in case an ungraceful process termination prevents
normal cleanup.

Refresh replaces files individually and then publishes the manifest; it is not
an atomic replacement of the whole file set. If interrupted, stop consumer
validation, inspect the partial diff, and refresh again from the same reviewed
source revision. Verify the completed snapshot before resuming validation.

To change the declared file set, edit or recreate the manifest as an explicit
reviewed consumer change; ordinary refresh does not silently widen it.

## Drift verification

Consumers that vendor the verifier and checksum helper can check their snapshot
without the Shared Tooling checkout or network access:

```bash
bash scripts/ci/verify-shared-tooling-snapshot.sh
```

The verifier fails when a declared file is absent, symlinked, has different
content, or changes executable state. It validates local snapshot integrity;
the source commit and normal review establish provenance.

## Shared baseline and local instructions

[`DRAGGINZGAME.md`](../DRAGGINZGAME.md) is the reusable engineering baseline.
Vendor it with its linked guides so the complete rule set remains readable
offline. Keep the consumer's `AGENTS.md` as its entry point and local overlay;
do not copy Shared Tooling's `AGENTS.md` over it.

For a new governance snapshot, use this complete documentation file set with
the required verifiers, adding any selected tools to the same command:

```bash
/path/to/shared-tooling/scripts/distribution/refresh-consumer.sh \
  --consumer /path/to/consumer \
  --file DRAGGINZGAME.md \
  --file rules/changelogs.md \
  --file rules/cargo-dependencies.md \
  --file rules/dependency-pinning.md \
  --file rules/git-hooks.md \
  --file rules/agent-maintenance.md \
  --file docs/releases.md \
  --file scripts/ci/run-release.sh \
  --file scripts/ci/next-release-version.sh \
  --file scripts/ci/finalize-release-changelog.awk \
  --file scripts/ci/check-dependency-pins.sh \
  --file scripts/ci/dependency-pins.jq \
  --file docs/principles/README.md \
  --file docs/principles/decision-artifact-discipline.md \
  --file docs/principles/reviewable-changes.md \
  --file docs/principles/rust-code-hygiene.md \
  --file docs/principles/simplicity-and-maintainability.md \
  --file docs/consuming-snapshots.md \
  --file docs/supported-hosts.md \
  --file scripts/ci/verify-file-checksum.sh \
  --file scripts/ci/verify-shared-tooling-snapshot.sh
```

For an existing snapshot, update its declared file set through the reviewed
manifest procedure above, including all shared rule files. Include
`docs/releases.md`, the release runner and its version/changelog helpers when
adopting the release command contract. The local `AGENTS.md` must direct contributors to
`DRAGGINZGAME.md`, identify `.shared-tooling.snapshot` as its source record, and
state local product contracts, commands and approved exceptions. Resolve local
conflicts before claiming adoption. Do not edit a vendored shared document in
place or attribute dirty upstream bytes to a committed revision.

A revision-bound baseline reference remains an allowed alternative under the
baseline. It must identify the exact source revision and document; a branch URL
or moving sibling path does not establish which rules were reviewed. Snapshot
integrity checks detect changes to declared files; they do not prove that a
consumer has adopted the newest policy or resolved its local instruction conflicts.
Rust consumers also add `.githooks/pre-commit` and
`scripts/dev/install-git-hooks.sh` to the declared file set, align their formatting
targets with the [hook contract](../rules/git-hooks.md), and enable the hook through
`make install-hooks`. Refresh preserves executable modes but does not activate
hooks or replace Git configuration. Review existing local hooks before declaring
their paths for replacement; preserve and reconcile their obligations.
Release adoption also requires aligning the consumer's entry points, adapters,
instructions and checks with the [release contract](releases.md), including
artifact retention and the exact atomic branch/tag push. A passing snapshot
check alone does not verify those behaviors.
Pinning adoption also requires the checker and its jq module, prepared Git/jq/yq
tools (and Cargo for Rust workspaces), a CI/release invocation, and consumer-owned
qualification for locked builds and external inputs. Consumers may also vendor
`scripts/ci/install-yq.sh` with the checksum helper; choose and record their own
reviewed version and platform digests. Resolve existing exceptions under the
[pinning policy](../rules/dependency-pinning.md) before claiming adoption.
