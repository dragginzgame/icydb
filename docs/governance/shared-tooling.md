# Shared Tooling Adoption

IcyDB adopts the [vendored shared engineering baseline](../../DRAGGINZGAME.md)
at reviewed revision `d957d1f8801885c5b69e4a9ef900155f5f2a8a9d`. Root
[AGENTS.md](../../AGENTS.md) is the local overlay; there are no baseline
exceptions. Product architecture, resource limits, exact qualification gates
remain local; standard release commands follow the shared contract. A sibling checkout cannot silently change
these rules.

## Ownership and provenance

[The snapshot manifest](../../.shared-tooling.snapshot) records fifty-one exact
upstream files, including the baseline and all linked rules, shared principles,
consumer/host guidance, formatting hook, shared audit methods, pinned host/IC
setup and selected verification helpers and release fixtures. Every entry records SHA-256 and
executable mode. Refresh through the upstream distribution helper from a clean
checkout, then review and validate the consumer diff. Never patch declared
snapshot files locally. The recorded HTTPS source identifies the same upstream
repository as the original adoption.

Common governance delegates to the [shared principles](../principles/README.md).
IcyDB overlays retain product-specific architecture and validation requirements;
CODEOWNERS covers shared guidance, the manifest and consumer tooling. The public
GitHub description, “ic database”, was reviewed against the README and remains
accurate; no remote metadata change was needed.

Host artifact hashing and Wasm inspection consume registry `ic-host-tools` through
`icydb-testing-integration` and `icydb-cli`; it is absent from runtime and canister dependencies.
The library owns streaming SHA-256 computation, digest formatting, explicit
executable resolution, hex decoding and core Wasm structure decoding for size
reports. CLI normalization retains its current labels and whitespace handling
before shared decoding; no command grammar or Candid policy changes. Inspection count ceilings derive from input length;
the consumer keeps defined-function counts and code-section payload bytes in
report format v1. Structure inspection does not establish instruction/type
validity or deployability. IcyDB retains
optimizer pins and admission, paths, report format v1, subprocess execution and
optimization policy. Report hashing preserves its whole-stream byte allowance.
The native host CI lanes compile both consumers and exercise optimizer admission,
report identities and Wasm structural counts; Linux qualification does not establish native
macOS execution.

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

The shared lockfile rewriter owns exact local package identities and preserves
external selections. IcyDB derives its owned roster from locked metadata,
validates the resulting graph offline and retains failed preparation inputs;
the existing release adapter still owns rollback. The shared formatting checker
qualifies the actual consumer Make targets and isolated hook, while IcyDB retains
derive sorting and unusual-filename coverage. Hook activation is a separate
configuration check.

[Tag maintenance](../tag-maintenance.md) now uses the reviewed shared Perl owner
and requires an explicit cutoff. Product release and publication flows do not
invoke it. Read-only preview creates no state; explicitly confirmed deletion owns
only its Git-directory lock, saved selection and attempt evidence. This adoption
does not authorize tag deletion or remote writes
([#305](https://github.com/dragginzgame/icydb/issues/305)).

## Consumer-owned instruction measurement

Core uses published registry ic-metrics 0.2.0 for dependency-free arithmetic.
Core and all direct audit/test callers read counter 1 through the existing CDK
API; reader-only fixture dependencies and direct feature selection are removed.
IcyDB uses the shared primitive `record_sample` API. IC Timers 0.14.0 uses the
same metrics 0.2 identity, including through IcyDB's hidden generated-glue
re-export. No mixed `MeasurementSummary` identities remain. Inclusive spans,
native zero, report shapes, sample arithmetic, replication and reset/journal-debt
boundaries are unchanged.
Published IC Timers 0.14.0 removes the prior transitive 0.1 dependency and feature
request, reading through its existing `ic0::performance_counter(1)` adapter.
The root requirement is 0.14; the lockfile contains one ic-metrics 0.2.0 package
without features or dependencies. The official Cargo index confirms both
published releases and the coordinated timer dependency.
IcyDB retains registry resolution without a local override or compatibility reader.
[ic-metrics #10](https://github.com/dragginzgame/ic-metrics/issues/10) coordinates
publication and owning downstream adoption.

The pre-publication adapter qualification used a concurrent locked graph
(ic-memory 0.28.0, ic-testkit 0.19.0, ic-host-tools 0.1.12). On that graph, strict
Core metrics library/tests Clippy and ten selected state tests passed. All thirteen
selected Wasm packages and twelve native fixture/canister packages passed strict
library Clippy. The initial Wasm attempt
caught an incomplete child-module reader rename; its log remains alongside the
corrected checks. Four Core/source/manifest/lock identities stayed unchanged;
fixture identities are recorded with the corrected qualification. Offline cache
preparation copied the already verified selected local Host SDK archive/index,
without network or package upgrades. Retained inputs/logs are in ic-metrics'
`target/evidence/arithmetic-cut-020/icydb/`. These are focused Linux/compiler
results, not IC execution, whole-database cost evidence or complete native CI.

The published 0.2 adoption check preserves the already selected root requirement
and lockfile (ic-memory 0.28.2, ic-testkit 0.19.1, ic-host-tools 0.1.14).
Registry metadata confirms no ic-metrics 0.2 features or dependencies, and its
summary source/tests are byte-identical to published 0.1.9. Ten existing metrics
state tests pass, including zero/saturation, failed attempts, reset identity and
Candid round trips. Strict Core metrics host library/tests and Wasm library
Clippy pass, as do the dynamic-query and metrics-enabled SQL canister Wasm
library checks. Dependency declarations and documentation references pass.
Inputs, registry observations, source identities and focused logs are retained
under `target/ic-metrics-020-adoption/`. Full workspace/release gates, native
macOS qualification and actual IC execution were not run; raw Wasm bytes, IC
cycles and instructions remain unmeasured for this adoption.

The coordinated timer closeout preserves the maintainer-selected 0.14 requirement
and 0.14.0 lock selection. Offline metadata verifies one metrics 0.2.0 identity
across Core and timer callers. Strict startup-timer/lifecycle-participant library
Clippy passes on native Linux and Wasm, and both focused startup facade Candid
tests pass. Pin and documentation checks pass. Inputs and logs are retained under
`target/ic-metrics-020-timer-closeout/`; actual IC execution, full gates and native
macOS qualification remain unperformed.

## Dependency selection

The [shared declaration checker](../../scripts/ci/check-dependency-pins.sh) and
[jq module](../../scripts/ci/dependency-pins.jq) run through
`make check-dependency-pins` in static CI, native host qualification and the
release gate. `make install-dev` / `make update-dev` prepare pinned jq, checksum-verified
Mike Farah yq and PCRE2-enabled ripgrep in `.tools/host/bin`; Git and the selected Rust toolchain
are prerequisites. The checker is offline and never changes dependency selections.

The normal gate opts into `--cargo-inheritance`, sharing parsed ordinary,
development, build and target dependency checks with root catalog ownership.
The local graph guard retains coupled pins, dependency bans, sensitive version
uniqueness and the time selection. Shared workspace-version observation replaces
local text readers and the cargo-get prerequisite. Release receipts retain local
source selection: committed reads export that exact tree and its package targets,
then use the same offline reader. Failed exports/observations cannot replace a
receipt and retain their inputs. The shared CI installer implementation is
included with the actionlint entry point; IcyDB's pin adapter remains local
([#306](https://github.com/dragginzgame/icydb/issues/306)).

[Exact constraints](../../ci/dependency-pinning-exceptions.json) have two owners:

- Published IcyDB crates share generated-code and schema contracts, so all six
  registry-facing workspace edges must match the release exactly. The existing
  release bump updates only their exception values through the same structured
  projection used by candidate admission. Rollback, staging and receipt identity
  include this metadata. No manual exception update is required after a release.
- The bundled `rusqlite =0.40.2` backend is the pinned SQLite correctness oracle
  described by `icydb-testing-sqlite-reference`. Its constraint remains fixed
  across IcyDB releases; changes need explicit dependency review and applicable
  SQL evidence. It is not a canister dependency.

There is one maintained Cargo workspace and tracked lockfile, and no external
Cargo paths or Git dependencies. CI, release validation and artifact-producing
Cargo commands use `--locked`. Authorized future dependency changes must prepare
and cheaply verify every affected independent graph if one is introduced.

[The common pin matrix](../../ci/ic-tools.tsv) selects ICP CLI 1.6.0,
ic-wasm 0.11.1, ic-admin, didc, Binaryen 132 and PocketIC 16.0.0 for all three
supported hosts. [Shared host selections](../../ci/tool-versions.env) select jq,
yq, PCRE2-enabled ripgrep and the common formatter; [IcyDB utility selections](../../ci/icydb-tools.env)
retain candid-extractor, Twiggy and Cargo helpers. `make install-tools` explicitly
installs verified host/IC toolsets; `make tools-check` checks them offline.
Make and CI select checkout-local executables. Workstation setup delegates to
these commands, with no second npm/Cargo ICP or ic-wasm installation.

IcyDB retains raw optimizer executable admission and the Binaryen 132 pipeline
identity. Shared archive pins are provisioning metadata, not optimizer admission.
[Client/server alignment](../../scripts/ci/check-pocketic-alignment.sh) requires
the three PocketIC server selections to match the locked client before testing.
Ordinary validation never downloads missing executables.

## Audit and verification ownership

Shared Tooling 0.1.8 is adopted from committed revision
`d957d1f8801885c5b69e4a9ef900155f5f2a8a9d`. The
[shared methods](../../audits/README.md) own generic audit procedure;
[IcyDB's catalog](../audits/README.md) selects scopes, report paths, test evidence
and product overlays:

| Method | Local retained authority |
| --- | --- |
| [Code hygiene](../../audits/code-hygiene.md) | Product architecture and [code style](code-hygiene/README.md). |
| [Flow convergence](../audits/recurring/crosscutting/crosscutting-flow-convergence-and-duplication.md) | Catalog/SQL execution, publication and interruption recovery. |
| [Complexity and debt](../audits/recurring/crosscutting/crosscutting-complexity-and-technical-debt.md) | State-space decisions and permitted IC/Wasm measurements. |
| [Module hardening](../audits/targeted/modules/module-surface-hardening.md) | Facade/generated-code boundaries and exact focused qualification. |
| [Module cleanup](../audits/targeted/modules/module-cleanup-runner.md) | Accepted cleanup scope, consumer contracts and source-bound handoff. |

The four replaced local methods remain byte-for-byte in the
[historical method archive](../audits/archive/shared-adoption/README.md).
Historical reports are unchanged and must be interpreted with their original
method identity. New runs identify both the reviewed shared revision and local
overlay; method changes do not retrospectively qualify or rescore old reports.
Audit adoption does not add an automatic product audit or broad gate
([#301](https://github.com/dragginzgame/icydb/issues/301)).

[Shared verification helpers](../verification-helpers.md) own Markdown navigation,
standard Make release-command smoke checks, exact crates.io presence observation
and portable file digests. Consumer adapters retain document selection,
persisted-source inventory and README version facts, release-cache and receipt
checks, publication ordering/wait policy, and report subject/schema identities.
Registry observation distinguishes absent from unavailable; neither triggers a
publication without existing consumer admission. The release-command fixture
uses a substitute runner and cannot create commits or tags.
The setup adoption addresses
[#302](https://github.com/dragginzgame/icydb/issues/302); the source snapshot
boundary prevents a moving sibling checkout from changing validation behavior.

The representative historical
[2026-10-01 flow report](../reports/recurring/2026/10/01/flow-convergence-and-duplication/01/report.md)
was walked through its owner map, convergence traces, findings, retained
separations and verification limits. Shared flow guidance plus the local overlay
retain the accounting-before-state-change, accepted-catalog, grammar and
distinct-budget obligations shown there. The old report still identifies its
original method and source; this adoption performs no new runtime audit and
makes no numerical comparison with it.

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
architectures and Linux x86-64. Shared installation consumes [one archive pin matrix](../../ci/ic-tools.tsv);
shell admission and Rust artifact checks consume the separate qualified raw
[executable digest table](../../scripts/ci/wasm-optimizer-checksums.tsv). The macOS
archives were downloaded and hashed on Linux; installer fixtures qualify
selection and rejection, not native execution. Report scripts use Bash 3.2
array operations and either host SHA-256 implementation.

At the original `e16c9c99` baseline adoption, upstream macOS snapshot verification
failed on an empty array under nounset; Ubuntu passed in the
[reviewed upstream CI run](https://github.com/dragginzgame/shared-tooling/actions/runs/37275788887).
The current snapshot includes the upstream correction. Linux checks do not
establish macOS release readiness or weaken its support requirement. See
[installation prerequisites](../../INSTALLING.md#system-prerequisites).

### 2026-10-06 Cargo and installer adoption

The committed 0.1.8 snapshot is source-bound to `d957d1f`. Focused Linux
qualification exercises the actual inheritance Make gate and retained graph
policies, publication/version refusal, exact committed-source reads and failed
exports, release preparation and installer candidate preservation. Portable
checks on Linux with Bash 3.2 are distinct from native macOS qualification.
Source identities and logs, including the corrected TOML fixture setup failure,
are retained under `target/shared-tooling-018/`.

The upstream [0.1.8 CI run](https://github.com/dragginzgame/shared-tooling/actions/runs/37484175750)
passes Linux and lint/security but fails both native macOS portable lanes before
the new Cargo fixture is reached. Upstream is preparing exact archive restoration
and retained diagnostics; those moving, uncommitted changes are excluded from
this snapshot. IcyDB's native lanes include the consumer metadata and committed
source checks. Native qualification remains pending; no complete macOS adoption
is claimed. Full workspace/release gates were not requested; raw Wasm bytes,
IC cycles and instructions remain unmeasured.

### Earlier 2026-10-06 refresh qualification

The refresh to committed revision `9f8c7c7` passes exact 49-file snapshot
verification, focused shell checks, caller lockfile success/refusal cases and
workstation fixtures on Linux. Official pinned ripgrep 15.2.0 is installed and
verified with PCRE2 support. The actual consumer formatting checker passes
manifest/Rust sorting, partial staging, formatter failure and preservation cases;
IcyDB's derive sorting and unusual selected paths also pass. The installed hook
configuration is `.githooks`.

All 75 shared tag-maintenance substitute checks pass. The consumer's read-only
preview preserves the exact tag inventory and index and creates no maintenance
state. Native CI includes this preview alongside actual formatting qualification.
No real commits, tags, pushes, deletion, release or network lifecycle actions
were performed. Maintainer dependency changes are preserved. Focused logs and
source identities are retained under `target/shared-tooling-9f8c7c7/`.
Full workspace/release gates and native macOS execution remain user-owned or
CI-owned validation; raw Wasm bytes, IC cycles and instructions are unmeasured.

### Initial 2026-10-06 adoption qualification

Focused Linux qualification of the dirty IcyDB adoption on `db8a0cc44` passes:
real checksum-verified local installation and offline toolset checks, current
PocketIC alignment and raw optimizer admission, consumer setup/refusal fixtures,
documentation fixtures/navigation, dependency declarations, workflow lint and
invariants, shared adapters, release-command/cache/receipt and note-finalization
fixtures, and 32 Wasm audit capture cases. Release and publication fixtures use
command substitutes; no real release, registry upload or Git commit occurs.
Failed ShellCheck/actionlint attempts were retained and corrected before handoff.

Strict focused CLI and integration library/report Clippy pass with locked offline
inputs. Eleven selected native Rust tests pass: two CLI response tests, one real
pinned optimizer test and eight report tests. The selected registry host library
is `ic-host-tools 0.1.12`. The maintainer's dependency updates and parallel
metrics-reader corrections are preserved; owned package versions, published
notes and all historical reports remain unchanged. Test selectors and input
identities are retained under `target/shared-tools-followup`.

The adopted source's
[0.1.7 CI run](https://github.com/dragginzgame/shared-tooling/actions/runs/37458968809)
was queued at inspection. Its native qualification and the changed IcyDB macOS
lanes are pending, distinct from these Linux checks. Full workspace and release
gates were not requested and remain user-owned. No ICP or PocketIC network was
started. Raw Wasm-size, IC-cycle and instruction deltas are unmeasured.

The tooling adoption touches approximately 72 files with about 1,300 net added
lines, primarily immutable shared material and frozen historical methods.
Consumer code is smaller and its implementation shape is simpler: generic
installation, decoding, resolution and verification converge on shared owners.
There is no new database state, runtime mode, report format or compatibility path.

## Earlier qualification evidence and boundaries

The following sections retain prior adoption evidence. Their source/package
identities and measured results describe those earlier batches, not the current
dependency selection or 0.1.7 qualification.

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

## Earlier cleanup disposition

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
Ordinary release commands reconcile unfinished committed releases automatically,
then validate the requested increment when HEAD contains newer fixes or the
requested increment differs. Late receipt callbacks inspect `RELEASE_COMMIT`,
which may precede HEAD. `make release-resume VERSION=X.Y.Z` selects only that
saved version explicitly. Publishing and cleanup remain separate.
The superseded manual bump/stage/commit/push path and its confirmation helper
have been deleted. The shared runner owns the sole release execution flow;
consumer callbacks retain metadata, dependency-selection and receipt obligations.
Preflight uses the existing locked fetch target to prepare the selected Cargo
cache; the validation gate and metadata preparation remain offline. A fetch
failure stops before those phases without changing dependency selection.
Release fixtures are explicit entries in the invariant gate, rather than nested
under cleanup checks. No release mode or persisted state is added by this removal.

The 0.265.0 release adopted shared `ic-metrics` arithmetic from registry 0.1.3
with the compatible root `0.1` requirement and no sibling path. An isolated worktree of committed
adoption source `1a8511c3a` resolves exactly that registry package and passes
metrics-enabled, warning-denied Core library Clippy with locked offline Linux
inputs. Its selected cache was checked before compilation; the existing
designated IcyDB Cargo home supplied dependencies and the worktree's separate
`target/icydb` retained artifacts. No dependency selection or consumer package
metadata changed. All nine focused metrics-state tests pass, covering saturation,
reset identity, failed owner attempts, lifecycle/journal separation, report
bounds/order and the current Candid shape. Primary source and lock bytes were
rechecked against the isolated inputs. After the primary release command stopped,
active-process/free-lock and exact-revision checks admitted the prepared note
corrections and obsolete-comment removal. Primary locked Linux metadata and
manifest sorting pass; typed Cargo metadata and lock bytes are unchanged.
An initial one-off history assertion assumed the pending-only detailed ledger
had a second release heading and failed; exact intended-bullet comparison
verified that all other changelog bytes remain unchanged.
These are native consumer-contract checks, not IC instruction measurements or
native macOS qualification. [Dependency adoption](https://github.com/dragginzgame/icydb/issues/298)
tracks the remaining owning release/CI qualification.

## Historical Shared Tooling 0.1.2 refresh

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

The current compatible 0.265.1 batch selects registry ic-metrics 0.1.5 with
feature `ic` and delegates the Wasm call-context reader to its safe binding.
Native zero, inclusive spans, saturation, reset identity and reports remain
unchanged. Before propagation, an isolated worktree at `43a66e10a` (package
0.264.10) passed strict metrics-enabled native/Wasm Core Clippy and all nine
state tests. Its designated cache initially lacked the new package index entry;
an explicit locked fixture fetch supplied only the selected reader dependency.
Copied owned artifacts and separate build directories were retained.

The maintainer released 0.265.0 while that isolated qualification ran. Primary
source identity, clean-tree, process and lock checks then admitted the migration
without modifying any package version or finalized changelog. At the actual
0.265.0 package identity, native/Wasm strict Core Clippy and all nine selected
state tests pass using the primary designated target and cache. Every other
lock record is preserved. The first failed cache lookup and earlier package
results remain distinct from this primary qualification. No full local gate,
consumer IC measurement or performance improvement is claimed. The runtime
change replaces one direct read with the shared owner, adding no state, mode or
attribution API. [Issue #298](https://github.com/dragginzgame/icydb/issues/298)
retains owning release/CI coordination.

The earlier temporary `../ic-metrics` dependency prevented isolated-index
resolution. Registry adoption removes that sibling prerequisite. Actual staged
formatting and native macOS execution of this refresh still require their own
qualification; dependency resolution alone does not supply hook evidence.

### Audit instruction-reader integration

The compatible 0.265.1 batch also delegates all 205 audit/fixture counter-1 reads
across 19 source files to registry `ic-metrics 0.1.5` on Wasm. Ten canisters and
the nested-relation fixture inherit the workspace dependency; the lockfile adds
only eleven dependency edges, preserving every package version, source and
checksum. Counter-0 message measurements remain unchanged. Each consumer's
native import retains the CDK's unsupported host binding for Candid compilation;
Core's existing native zero policy stays separate. There is no new public API,
runtime configuration, attribution policy, persisted state or report shape.

Focused Linux qualification passes strict native default-feature Clippy, native
measurement-feature/Candid Clippy, and Wasm measurement-feature Clippy for the
eleven affected packages. Five real PocketIC tests cover durable update metrics,
query and trapped-query isolation, reset/debt conservation, upgrade recovery,
timer recurrence/coalescing, traps and instruction exhaustion. Two Clippy
default-construction warnings in the affected schema-measurement fixture were
corrected before further qualification.

Matched `wasm-release` builds use Rust 1.99.0 and the unchanged selected package
versions. Canonical Binaryen 132 post-link flags and the admitted optimizer digest
produce these raw non-gzipped deployable sizes:

| Subject | Before bytes | After bytes | Delta bytes |
| --- | ---: | ---: | ---: |
| One-entity dynamic-query audit | 3,030,668 | 3,030,668 | 0 |
| Startup timer probe | 208,081 | 208,077 | -4 |

PocketIC 16.0.0 instruction comparisons on the optimized dynamic-query artifacts
are unchanged for 200-execution point, distinct-point, scan and grouped workloads:
42,337,663; 85,372,864; 48,779,217; and 43,485,367 instructions respectively.
The preceding compiler-emitted artifact comparison added six instructions per
workload; that result is distinct from the optimized deployable measurement.
Source/lock/tool/server identities, both artifact pairs, measurements and logs
are retained under `target/ic-metrics-integration`. Every server started for this
qualification was stopped by its owning wrapper. IC-cycle deltas, other
canister-size deltas and native macOS execution remain unmeasured. Full workspace
and release gates were not requested and remain user-owned.

The code/dependency propagation changes 31 files by approximately +114 lines;
three existing documentation files record its scope and evidence. Implementation
shape stays neutral: compile-time imports replace local IC reads, with no new
helpers or behavior axes. Existing upstream
[reader qualification #3](https://github.com/dragginzgame/ic-metrics/issues/3) and
[consumer adoption #298](https://github.com/dragginzgame/icydb/issues/298) own the
remaining hosted/release coordination; no additional library API gap was found.
The upstream [documentation follow-up](https://github.com/dragginzgame/ic-metrics/issues/3#issuecomment-6011590931)
requests a platform-gated Rust import example for native Candid builds; detailed
unpublished qualification artifacts remain local.
