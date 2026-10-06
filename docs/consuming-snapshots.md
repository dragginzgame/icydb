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

When refreshing `install-actionlint.sh`, `install-shellcheck.sh` or
`install-gitleaks.sh`, also declare `scripts/ci/install-ci-tool.sh` and the existing
`scripts/ci/verify-file-checksum.sh`. The three entry points now share that
implementation; refreshing only an entry point leaves an incomplete installation.
Pins and command arguments remain consumer-owned and unchanged.

Cargo inheritance adoption refreshes `check-dependency-pins.sh` and
`dependency-pins.jq` together, then adds `--cargo-inheritance` to the consumer's
CI/release invocation. The workspace-version reader is independently available as
`scripts/ci/read-cargo-workspace-version.sh`. Include
`docs/verification-helpers.md` for their dependencies and boundary contracts.

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
  --file audits/README.md \
  --file audits/code-hygiene.md \
  --file audits/flow-convergence-and-duplication.md \
  --file audits/complexity-and-technical-debt.md \
  --file audits/module-surface-hardening.md \
  --file audits/module-cleanup.md \
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
  --file docs/provenance.md \
  --file docs/verification-helpers.md \
  --file docs/ic-tools.md \
  --file docs/local-setup.md \
  --file ci/ic-tools.tsv \
  --file ci/tool-versions.env \
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

## Audit method adoption

Include the complete `audits/` file set above when adopting this baseline, so
its method links work offline. The snapshot's reviewed source revision identifies
the common contract; do not read methods from a moving sibling checkout or copy
uncommitted documents and attribute them to an older revision.

For each selected existing local audit, map its questions to the shared method
and retain product-specific obligations in a local overlay under the consumer's
existing audit directory. Link to the vendored `audits/` definition and record
local source roots, authority boundaries, focused checks, allowed measurement
methods and report destination. Resolve conflicts with the common contract,
including automatic fixes, unconditional broad gates and composite scores.

Replace duplicated generic prose only after the snapshot is installed. Update
local catalogs, entrypoints and any method fingerprints in the same batch.
Where a frozen method is needed to reproduce earlier reports, preserve its old
identity as historical and ineligible for new runs; do not label historical
scores comparable to the new method. Keep domain-specific audits and executed
reports in their existing repository. This does not migrate report trees,
introduce shared runtime measurement tooling, or retire distinct safety proofs.

Verify snapshot integrity, local links, obligation coverage and method selection
by walking a representative existing report against the new method and overlay.
Record this as an adoption review, not a newly executed product audit. Script or
product behavior changes need their own relevant validation and host evidence;
documentation adoption alone does not call for full CI or native builds.

## Local IC tool adoption

After the new files are committed and reviewed, add `ci/ic-tools.tsv`,
`scripts/dev/install-ic-tools.sh`, `scripts/ci/verify-evidence-checksums.sh` and
`scripts/ci/verify-file-checksum.sh` to the snapshot, with `docs/ic-tools.md`.
Also include `scripts/dev/install-host-tools.sh`, `ci/tool-versions.env` and
`docs/local-setup.md` for the pinned jq/yq setup. Add `/.tools/` to the consumer's
ignore rules. Expose these targets:

```makefile
IC_TOOL_PINS ?= ci/ic-tools.tsv
HOST_TOOL_VERSIONS ?= ci/tool-versions.env
export PATH := $(CURDIR)/.tools/host/bin:$(CURDIR)/.tools/ic/bin:$(PATH)

.PHONY: install-tools tools-check install-host-tools host-tools-check install-ic-tools ic-tools-check
install-tools:
	+$(MAKE) --no-print-directory install-host-tools
	+$(MAKE) --no-print-directory install-ic-tools

tools-check:
	+$(MAKE) --no-print-directory host-tools-check
	+$(MAKE) --no-print-directory ic-tools-check

install-host-tools:
	bash scripts/dev/install-host-tools.sh --versions "$(HOST_TOOL_VERSIONS)"

host-tools-check:
	bash scripts/dev/install-host-tools.sh --versions "$(HOST_TOOL_VERSIONS)" --check

install-ic-tools:
	bash scripts/dev/install-ic-tools.sh --pins "$(IC_TOOL_PINS)"

ic-tools-check:
	bash scripts/dev/install-ic-tools.sh --pins "$(IC_TOOL_PINS)" --check
```

Review pins against existing qualified versions before activation. A local
exception uses its own explicitly selected matrix outside the snapshot; remove
superseded pin ownership rather than maintaining two independent selections.
Update local setup/CI callers to use `.tools/host/bin` and `.tools/ic/bin`; CI
must put these paths on its own PATH after explicit setup. Remove separate
package-manager jq/yq selections and preserve one owner for each parser pin.
The existing single-yq installer remains available to consumers that need only
that standalone parser; it accepts caller-owned pins rather than defining them.
Document the [system bootstrap prerequisites](local-setup.md#bootstrap-prerequisites)
and retain product-specific
alignment checks. Retire duplicated installer bodies once callers have moved;
do not patch vendored files. Keep package-manager setup for unrelated tools.

Qualify explicit installation and offline checking on the consumer's declared
native hosts, plus relevant product checks for changed tool selections. Do not
claim deployment or PocketIC client/server compatibility from `--version` alone.
Snapshot integrity and consumer adoption remain separate from upstream fixtures.

The evidence-manifest helper can also be adopted independently with the existing
checksum verifier. The nonempty Cargo test helper and exact release-tag checker
have no shared helper dependencies. Consumers keep test arguments, manifest
selection and publication/release authority in their adapters.

The documentation-link, release-command and crates.io observation helpers also
have no shared helper dependencies. Follow their explicit input and failure
contracts in [verification helpers](verification-helpers.md), include that
document and each selected helper in the snapshot, and move callers before
deleting duplicated code. Keep product-specific checks and publication policy
local. Adoption must wait for a reviewed committed source revision; local
upstream tests do not establish that consumers have refreshed their snapshots.

File-digest generation uses the checksum verifier's additive `--print` interface,
so its existing snapshot file set is sufficient. Refresh that verifier before
changing local hash callers. Advisory database preparation is independently
available as `scripts/ci/prepare-rustsec-db.sh` with no shared-script dependencies;
include it and `docs/verification-helpers.md` when adopting it. Replace only
database acquisition/isolation, retain consumer audit policy and failure evidence,
and pass `--no-fetch` when auditing the prepared database so its identity stays
bound to the recorded commit.
