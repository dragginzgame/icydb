# Shared Tooling Adoption

IcyDB adopts the [shared engineering baseline](https://github.com/dragginzgame/shared-tooling/blob/e16c9c99bd800567189c8024eaf4242a5d1c9e29/DRAGGINZGAME.md)
at revision `e16c9c99bd800567189c8024eaf4242a5d1c9e29`. Root
[AGENTS.md](../../AGENTS.md) is the local overlay. There are no baseline exceptions.
Product architecture, resource limits, exact qualification gates and release
targets remain local. This reference is immutable; a sibling checkout does not
silently change the adopted rules.

## Tool ownership

The canonical shared scripts are recorded in
[the snapshot manifest](../../.shared-tooling.snapshot), including their source
revision, SHA-256 digests and executable modes. Refresh from a clean checkout
with the upstream distribution helper, then review and validate the consumer
diff. Never edit a declared snapshot file locally.

Two demonstrated consumer requirements need adapters outside that snapshot:

- IcyDB's validation handoff requires one complete log for all failed targets.
  The shared runner owns execution and diagnostics; the local adapter combines
  its retained raw logs and maintains `target/validation-failures/latest.log`.
  It adds no validation target, retry or alternate execution flow.
- IcyDB owns actionlint's version and admitted platform digests. One local pin
  file supplies installation and workflow checks. The shared installer owns
  downloading, checksum verification and installation; the adapter selects the
  consumer's pin and destination without introducing a version override.

The snapshot is an existing upstream format-1 provenance boundary, not new
runtime state. The adapters keep one execution/installation owner; neither
changes database protocols or release selection. Bash 3.2 remains the portable
syntax target. Current upstream macOS snapshot-verifier execution fails with an
empty-array/nounset error; Ubuntu regressions and lint pass in the
[reviewed CI run](https://github.com/dragginzgame/shared-tooling/actions/runs/37275788887).
This adoption does not claim macOS runtime qualification or patch vendored code
to work around that upstream failure. IcyDB's configured CI host is Ubuntu.

The shared script set is limited to the runner, installer, checksum/snapshot
verifiers and the already-identical LOC tool. No sccache lifecycle change is
introduced without a demonstrated consumer failure. Official actionlint 1.7.12
[release checksums](https://github.com/rhysd/actionlint/releases/download/v1.7.12/actionlint_1.7.12_checksums.txt)
are recorded in [the consumer pin file](../../scripts/ci/actionlint-checksums.tsv);
changing that file is the explicit tool-version/platform admission workflow.
Installer platform branches are installation capabilities, not host CI claims.

## Host qualification

macOS support is required, including dependency setup, native tools, builds,
tests and deployment tooling. The declared host targets and current evidence are:

| Host target | Qualification |
| --- | --- |
| Ubuntu 24.04, x86-64 | Configured IcyDB CI; focused tooling checks pass locally on Linux. |
| macOS 15, ARM64 and x86-64 | Required targets; IcyDB native execution is pending and no macOS CI lane is configured. |

The shared snapshot verifier currently fails on upstream macOS 15 / Bash 3.2
before consumer qualification. Its vendored bytes are unchanged in this refresh.
IcyDB's workstation bootstrap also requires `apt-get`; its Linux dependency list
is not a macOS installation recipe. Host-specific setup and native build, test
and deployment-tool qualification remain unresolved. These gaps do not weaken
the support requirement; passing Ubuntu tooling checks do not establish
macOS release readiness. See [installation prerequisites](../../INSTALLING.md#system-prerequisites).
No macOS exception or native qualification is claimed.

## Focused qualification

Verify snapshot integrity, shell syntax/lint, the local adapters' focused
regressions, workflow invariants and documentation links. Full workspace,
release and deployment gates require an explicit request or configured CI.
Downloaded tool execution is qualified separately from offline installer
fixtures; do not label a stubbed download as a live installation.

The consumer checks pass on Linux: snapshot verification, offline adapter
regressions, ShellCheck, workflow lint/invariants, release-preservation fixtures,
links and whitespace. A real actionlint 1.7.12 Linux amd64 archive is separately
downloaded, verified and installed only in a disposable directory. Full suites
and macOS execution remain unrun; no database compilation or runtime cost
measurement is required for this tooling-only adoption.

The latest review refreshes the source record and common baseline to `e16c9c9`;
all five vendored tool files retain their previously verified bytes. The user's
independent lockfile updates to ic-memory 0.25.4 and ic-testkit 0.15.4 are preserved.
Earlier Rust results used other dependency revisions and do not qualify these
new pins; Rust and full release validation remain separate from this tooling evidence.

## Cleanup disposition

- `platform()` in `scripts/ci/install-actionlint.sh` is replaced by upstream
  `resolve_platform()`; consumer version/digest selection belongs to the adapter.
- `persist_combined_failure_log()` in `scripts/ci/run-validation-targets.sh`
  is replaced by the consumer adapter's aggregation of retained raw logs.
  Upstream remains the sole target-execution owner.
- `cleanup_validation_runner_test()` in the workflow-invariant script is
  removed with its temporary, prose-coupled probes. The dedicated focused
  adapter regressions own and clean their fixtures.

The existing `main()`, `persist_failure_log()`, `print_failure_detail()` and
`write_github_summary()` names remain in the corresponding canonical installer
or runner; their implementations are upstream-owned replacements, not deleted
behavior. No Rust function, method or type is removed by this adoption.

The adoption changes 24 files with approximately 680 net added lines, primarily
immutable shared verifiers, adapter regressions and governance evidence. The
full local runner/installer implementations are replaced by shared authorities
and two thin adapters; database execution shape is unchanged. Raw Wasm-size,
IC-cycle and instruction deltas are unmeasured for this tooling-only change.
