# Shared Tooling Adoption

IcyDB adopts the [vendored shared engineering baseline](../../DRAGGINZGAME.md)
at reviewed revision `c0206f1943238e21bd00fbe01658e6a0864c24fa`. Root
[AGENTS.md](../../AGENTS.md) is the local overlay; there are no baseline
exceptions. Product architecture, resource limits, exact qualification gates
remain local; standard release commands follow the shared contract. A sibling checkout cannot silently change
these rules.

## Ownership and provenance

[The snapshot manifest](../../.shared-tooling.snapshot) records twenty-three exact
upstream files, including the baseline and all linked rules, shared principles,
consumer/host guidance, formatting hook and installer, and selected tools and
release fixtures. Every entry records SHA-256 and
executable mode. Refresh through the upstream distribution helper from a clean
checkout, then review and validate the consumer diff. Never patch declared
snapshot files locally. The recorded HTTPS source identifies the same upstream
repository as the original adoption.

Common governance delegates to the [shared principles](../principles/README.md).
IcyDB overlays retain product-specific architecture and validation requirements;
CODEOWNERS covers shared guidance, the manifest and consumer tooling. The public
GitHub description, “ic database”, was reviewed against the README and remains
accurate; no remote metadata change was needed.

Two demonstrated consumer requirements use adapters outside the snapshot:

- The shared runner owns execution and diagnostics. IcyDB combines retained raw
  logs into `target/validation-failures/latest.log` for a complete handoff. This
  adds no validation target, retry or alternate execution flow.
- The shared actionlint installer owns download, verification and installation.
  IcyDB's adapter supplies the consumer version, platform digests and destination
  from [one pin file](../../scripts/ci/actionlint-checksums.tsv).

The format-1 manifest is an existing upstream provenance boundary, not database
state. The refreshed LOC tool counts disjoint member-owned files, excludes nested
workspace members and classifies test paths relative to each crate. No sccache
lifecycle change is introduced without a demonstrated consumer failure.

## Host qualification

macOS support is required for setup, native tools, builds, tests and deployment
tooling. Bash 3.2 is the portable syntax target. The current evidence is:

| Host target | Qualification |
| --- | --- |
| Ubuntu 24.04, x86-64 | Existing CI lanes; the focused checks below pass locally on Linux. |
| macOS 15, ARM64 | Required CI lane configured with `macos-15`; native execution pending. |
| macOS 15, x86-64 | Required CI lane configured with `macos-15-intel`; native execution pending. |

[CI](../../.github/workflows/ci.yml) now gates its central check on both native
macOS lanes as well as Ubuntu. The macOS lanes run real workstation installation,
system Bash snapshot/adapter/setup/report fixtures, focused library and CLI
checks, integrity and optimizer tests, a production canister build, formatting
and native ICP tool version checks. GitHub documents the runner architectures in
[its hosted-runner reference](https://docs.github.com/en/actions/reference/runners/github-hosted-runners).
This closes the missing CI configuration; it does not claim an unexecuted lane
has passed. Deployment workflows outside these selections remain unqualified
on macOS until native evidence exists.

Setup selects Homebrew prerequisites on macOS and apt prerequisites on Linux.
Binaryen 132 has admitted archive and executable digests for both macOS
architectures and Linux x86-64. Shell installation, verification and Rust artifact
checks consume [one table](../../scripts/ci/wasm-optimizer-checksums.tsv). The macOS
archives were downloaded and hashed on Linux; installer fixtures qualify
selection and rejection, not native execution. Report scripts use Bash 3.2
array operations and either host SHA-256 implementation.

At the original `e16c9c99` baseline adoption, upstream macOS snapshot verification
failed on an empty array under nounset; Ubuntu passed in the
[reviewed upstream CI run](https://github.com/dragginzgame/shared-tooling/actions/runs/37275788887).
The current snapshot includes the upstream correction. Linux checks do not
establish macOS release readiness or weaken its support requirement. See
[installation prerequisites](../../INSTALLING.md#system-prerequisites).

## Focused evidence and boundaries

Snapshot integrity, shell syntax/ShellCheck, offline consumer adapter and
workstation fixtures, workflow lint/invariants, documentation links and
whitespace pass on Linux. Setup fixtures exercise install/update selection,
repository-root execution, lockfile preservation, required tools and optimizer
integrity before execution/replacement. Rustup is an explicit prerequisite;
workstation updates do not audit or resolve repository dependencies.

The original workstation review stopped integration Clippy at the removed
ic-memory `AllocationDeclaration::validate()` API and the Wasm invariant wrapper
at missing jq. These failures were captured before repair. The current locked
ic-memory 0.25.10 exposes an opaque committed capability that guarantees valid,
unique declarations; IcyDB now checks availability and retains exact generation
identity without rebuilding the upstream validation sets. Bootstrap error
classification also uses only current upstream variants. User lockfile updates
and repository package versions are preserved.

Focused strict Clippy passes for core/facade production and test targets, the
integration optimizer/report targets and the CLI. The installed Linux optimizer passes its
version and binary digest checks. All Wasm audit capture cases, including the
default subject set, and post-link invariants pass with an official jq 1.8.1
binary verified against the release SHA-256 and installed only in a disposable
directory. Stubbed downloads are not reported as live installations. Earlier
standalone optimizer checks remain narrower evidence than current package lint.

All sixteen selected native tests pass: five Quick integrity tests, six Deep
integrity/session tests, four public bootstrap diagnostic tests and the real
pinned optimizer contract. These qualify maintained behavior at the changed
boundaries; they do not run a PocketIC network or a full workspace suite.

The existing terminal CI check owns qualification. Adding two declared host
lanes is the simplest way to exercise the support requirement, with no new
runtime mode, persisted state or database protocol. The optimizer table expands
one prerequisite authority to three assets; Homebrew's unqualified Binaryen
cannot replace the admitted release. Full workspace/release gates remain
user-owned validation unless explicitly requested or run by configured CI.
Native macOS execution remains pending; raw Wasm-size, IC-cycle and instruction
deltas are unmeasured.

## Cleanup disposition

The current workstation/adoption cleanup removes:

- `run_update_checks()` from `scripts/dev/workstation-setup.sh`: dependency
  audits and resolution do not belong to tool updates; explicit dependency
  maintenance remains the owner.
- `validate_committed_allocation_declarations()` from
  `crates/icydb-core/src/db/integrity/proof.rs`: the opaque upstream committed
  capability owns declaration validity and uniqueness; local availability and
  generation checks remain.
- `tests::declaration()` and
  `tests::quick_allocation_registry_closure_requires_unique_keys_and_slots()`
  from that module: fabricated declarations duplicated the upstream contract;
  maintained integrity and proof behavior remain covered by current tests.
- The Linux-only `WASM_OPT_SHA256` constant from
  `testing/integration/src/wasm_optimizer.rs`: `wasm_opt_sha256()` reads the
  admitted native platform digest from the common table.

The earlier baseline adoption replaced installer `platform()` with upstream
`resolve_platform()`, moved runner `persist_combined_failure_log()` to the
consumer adapter and removed `cleanup_validation_runner_test()` with its
prose-coupled probes. Dedicated adapter fixtures own those regressions. Existing
canonical runner/installer function names remain upstream-owned replacements.

This workstation/adoption batch changes 39 files with approximately 1,100 net
added lines, excluding the user's lockfile updates. Most additions are immutable
shared guidance, host CI and focused fixtures. Runtime declaration handling is
simpler; installation/report authorities converge without adding database state.
Raw Wasm-size, IC-cycle and instruction deltas remain unmeasured.

## Standard releases and extraction

Standard SemVer entry points use the [common release contract](../releases.md)
with explicit `RELEASE_REMOTE=origin` and `RELEASE_BRANCH=main`. They retain the
complete IcyDB gate and source/tag-bound receipts. The runner owns Git effects;
consumer adapters own exact metadata, UTC notes and retained dependency selection.
`make release-resume VERSION=X.Y.Z` resumes the saved candidate after inspection
of `.git/release-state/` and its lock owner. Publishing and cleanup remain separate.
The superseded manual bump/stage/commit/push path and its confirmation helper
have been deleted. The shared runner owns the sole release execution flow;
consumer callbacks retain metadata, dependency-selection and receipt obligations.
Preflight uses the existing locked fetch target to prepare the selected Cargo
cache; the validation gate and metadata preparation remain offline. A fetch
failure stops before those phases without changing dependency selection.
Release fixtures are explicit entries in the invariant gate, rather than nested
under cleanup checks. No release mode or persisted state is added by this removal.

Shared `ic-metrics` arithmetic is wired through an explicit local path dependency.
Its exact pin and lock entry now select the maintainer-tagged 0.1.1 package. Locked
offline Linux metadata and the selected metrics-enabled Core library build pass.
Unfiltered metadata required an uncached Windows-only package; no online retry or
other dependency selection changed. Nine focused state tests and strict selected
core Clippy passed during extraction. No IC cost delta, native macOS release
qualification or registry publication is claimed. [Dependency adoption](https://github.com/dragginzgame/icydb/issues/298)
tracks replacement of the temporary path.

## Shared Tooling 0.1.2 refresh

The snapshot now adopts automatic numbered pending notes, the direct Cargo
dependency catalog policy, the isolated-index formatting hook and safe local
installer. All fifty Cargo manifests inherit their direct dependencies.
Developer setup and CI read formatter versions from `ci/tool-versions.env`;
native CI and release validation run the complete `fmt-check` target. The shared
release runner allows fresh preflight/validation retries before preparation and
retains exact recovery plans afterward. Numbered note finalization preserves
published minor-line history.

Snapshot integrity and isolated fixture checks qualify their own boundaries;
local hook activation and actual staged-source formatting are separate evidence.
The temporary `../ic-metrics` dependency cannot resolve in the isolated index
export. The maintainer elected to retain local development wiring; actual staged
formatting remains unqualified while that dependency is absent from the export.
No sibling sources are copied or formatted to hide that prerequisite. Native
macOS execution of this refresh remains pending.
