# Shared Tooling Adoption

IcyDB adopts the [vendored shared engineering baseline](../../DRAGGINZGAME.md)
at reviewed revision `bfb50bd0884b5e6c5ee9592056531c6108f96d73`. Root
[AGENTS.md](../../AGENTS.md) is the local overlay. The shared approved layout
retains the 42 existing canister, schema and testing packages in their
restored directories, in the single root workspace and lockfile; no new package
move is part of this adoption ([#310](https://github.com/dragginzgame/icydb/issues/310)).
Product architecture, resource limits, exact qualification gates
remain local; standard release commands follow the shared contract. A sibling checkout cannot silently change
these rules.

## Ownership and provenance

[The snapshot manifest](../../.shared-tooling.snapshot) records sixty-two exact
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

The four direct registry host packages at 0.4 are consumed only by host tooling.
`icydb-testing-integration` selects `ic-host-artifacts` with its `wasm` feature
for streaming hashes, digest formatting and report/method structure inspection,
and `ic-host-process` for executable resolution, pinned admission and bounded
execution. `icydb-cli` uses the artifact stream reader for diagnostic error JSON,
`ic-host-fs` for opened schema-artifact admission and bounded reads, and retains
IC response decoding in `ic-host-tools`.
These direct dependencies are absent from runtime and canister packages.
CLI normalization retains its current labels and whitespace handling
before shared decoding; no command grammar or Candid policy changes. Inspection count ceilings derive from input length;
the consumer keeps defined-function counts and code-section payload bytes in
report format v1. Structure inspection does not establish instruction/type
validity or deployability. IcyDB retains method/CDK policy, JSON/provenance checks,
optimizer pins, paths, report format v1, publication/cache and optimization policy.
Diagnostic reads retain 64 KiB/2 MiB limits and selected-file symlink behavior;
schema artifacts require regular files. Optimizer execution explicitly supplies
the inherited environment, with 1 MiB per captured stream and a 600-second
operational deadline. Shared admission checks the pin and version; its handle
rechecks the executable digest before each batch transform. Captured failure and
cleanup evidence is retained. Report hashing preserves its whole-stream allowance.
The native host CI lanes compile both consumers and exercise optimizer admission,
report identities and Wasm structural counts; Linux qualification does not establish native
macOS execution.

The current published host source is `6b171744def811882ba6c71d50135efa898302a9` in
[IC Host Tooling](https://github.com/dragginzgame/ic-host-tooling). Its four
0.4.0 archives match the selected registry checksums and all 48 packaged Rust
source files match that revision. Testkit 0.20.0's locked dependencies on the four
0.3.3 host packages are retained until its owning
[adoption issue](https://github.com/dragginzgame/ic-testkit/issues/13) is resolved;
IcyDB adds no compatibility path. Candid extraction and streamed ICP artifact
publication now use these shared owners, with qualification below
([#307](https://github.com/dragginzgame/icydb/issues/307)).

Two demonstrated consumer requirements use adapters outside the snapshot:

- The shared runner owns execution and diagnostics. IcyDB combines retained raw
  logs into `target/validation-failures/latest.log` for a complete handoff. This
  adds no validation target, retry or alternate execution flow.
- The shared actionlint installer owns download, verification and installation.
  IcyDB's adapter supplies the consumer version, platform digests and destination
  from [one pin file](../../scripts/ci/actionlint-checksums.tsv).

The format-1 manifest is an existing upstream provenance boundary, not database
state. The refreshed LOC tool counts disjoint member-owned files, excludes nested
workspace members and Cargo's selected target directory, and classifies test
paths relative to each crate. No sccache
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
yq, PCRE2-enabled ripgrep, cloc 2.10 and the common formatter; [IcyDB utility selections](../../ci/icydb-tools.env)
retain candid-extractor, Twiggy and Cargo helpers. `make install-tools` explicitly
installs verified host/IC toolsets; `make tools-check` checks them offline.
[The shared Make include](../../make/tools.mk) owns setup, offline verification,
`make cloc` for this workspace and `make cloc-tooling` for a sibling tooling
inventory. Make and CI select checkout-local executables; workstation setup
uses the same targets and does not install a second system cloc. Interactive
shells must add `.tools/host/bin` and `.tools/ic/bin` to PATH explicitly, as
[the setup guide](../local-setup.md) describes. IcyDB adds independent PocketIC
alignment and raw optimizer admission prerequisites to `ic-tools-check`; the
shared include retains bundle verification. No second npm/Cargo ICP or ic-wasm
installation is selected ([#302](https://github.com/dragginzgame/icydb/issues/302)).

IcyDB retains raw optimizer executable admission and the Binaryen 132 pipeline
identity. Shared archive pins are provisioning metadata, not optimizer admission.
[Client/server alignment](../../scripts/ci/check-pocketic-alignment.sh) requires
the three PocketIC server selections to match the locked client before testing.
Ordinary validation never downloads missing executables.

## Audit and verification ownership

Shared Tooling 0.1.11 is adopted from committed revision
`46c02774a8335cb3949d6f04284c4f53375353c1`. The
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

### 2026-10-07 compatible Shared Tooling 0.1.12 pass

The 56-file snapshot selects committed `33c2a6f`. Its complete governance export,
read-only exact-commit CI helper and nonempty Cargo runner come from a clean
detached source checkout. The distribution helper exported that exact selection
to an isolated consumer; every existing local snapshot byte/mode was qualified
before applying it, preserving unrelated dirty work and the real Git index.
No shared file was patched locally. Baseline and maintenance rule move together;
the installer/verifier/release fixes retain their consumer-owned inputs.

Focused Linux checks pass: compromised-checksum-helper and distribution refusals,
destination-drift release fixtures, exact-commit CI-selection fixtures, installer
preservation, IcyDB adapters/metadata, graph, shell lint and workflow invariants.
Release/CI-selection/verifier and nonempty-runner boundaries also pass under
Linux Bash 3.2. Real dependency-free Cargo execution qualifies passing selection,
zero-test exit 3 and retained failed-test exit 101; the initially missing fixture
lockfile refusal was retained before explicit offline fixture preparation.
Product Rust tests and full repository/release gates were not run.

The upstream exact-source [0.1.12 CI](https://github.com/dragginzgame/shared-tooling/actions/runs/37511845192)
passes Linux and both native macOS jobs. IcyDB's native CI now checks shared CI
inspection and uses the nonempty runner for its existing focused tests; those
uncommitted consumer changes have no native result yet
([#309](https://github.com/dragginzgame/icydb/issues/309)). Evidence is retained
under `target/shared-tooling-012-pass/`. Runtime/package versions, dependency
selections and previous Rust edits are preserved; permitted performance deltas
remain unmeasured and no local network lifecycle action occurred.

At that handoff, the newer committed 0.1.13 layout policy was reviewed but not
adopted; its 42 package moves were independently scoped in
[#310](https://github.com/dragginzgame/icydb/issues/310).
The Make-mode bypass remains owned by
[Shared Tooling #30](https://github.com/dragginzgame/shared-tooling/issues/30) and
its consumer [#311](https://github.com/dragginzgame/icydb/issues/311); the LOC
build-output correction remains owned by
[Shared Tooling #31](https://github.com/dragginzgame/shared-tooling/issues/31).
These source observations are separate from the normal-environment passes above.

### 2026-10-07 layout rollback and retained CI repair

The maintainer explicitly requested reversal of the prepared package moves
([#310](https://github.com/dragginzgame/icydb/issues/310)). The 42 packages are
restored to their original `canisters/`, `schema/` and `testing/` locations, with
active callers, fixture/coverage references, shared SQL source paths, current
guides and release inventory restored together. At that rollback, the snapshot returned
to the previously reviewed 56-file 0.1.12 selection. Package identities,
versions, features, dependencies, lockfile and prior source edits are preserved.
The earlier move's evidence remains under `target/shared-tooling-layout/`;
rollback evidence lives under `target/shared-tooling-layout-revert/`.

Shared Tooling 0.1.14 is committed at `25e7ce83149e081e4dcc52c55c33724e44153f2a`.
Its clarified workspace rule allows both reusable packages under `crates/`
and application-owned packages under `apps/<app>/<component-role>/`
([Shared Tooling #34](https://github.com/dragginzgame/shared-tooling/issues/34)).
The rule does not require flattening compliant application packages into
`crates/`. This rollback restores the requested existing paths; it does not
introduce another relocation or claim adoption of the 0.1.14 policy.

The inspected [consumer CI](https://github.com/dragginzgame/icydb/actions/runs/37496328220)
failed static formatter installation because its global sccache wrapper had no
provisioning in that job. The retained correction scopes the wrapper to the
three jobs that install it, and parsed workflow fixtures reject missing
provisioning. CI's MSRV matches the existing manifest's 1.96 declaration. The
workstation correction passes with nested Bash 3.2 on Linux; native macOS
qualification remains pending
([#309](https://github.com/dragginzgame/icydb/issues/309)).

Shared Make-mode, LOC and host-guide fixes are now committed in 0.1.14
([#30](https://github.com/dragginzgame/shared-tooling/issues/30),
[#31](https://github.com/dragginzgame/shared-tooling/issues/31),
[#35](https://github.com/dragginzgame/shared-tooling/issues/35)). Their earlier
prepared-source Linux fixtures passed, but those changes were not included in the
rollback's 0.1.12 snapshot. Their subsequent adoption is recorded below; native
consumer qualification remains separate. No commits, pushes or local network
lifecycle actions occurred; full workspace/release gates were not run. Raw
Wasm-size, IC-cycle and instruction deltas remain unmeasured.

### 2026-10-07 committed 0.1.14 issue repairs

The current 58-file snapshot selects exact committed `25e7ce8`, exported from a
clean detached checkout. Uncommitted sibling LOC/sibling-report changes are
excluded. The new Make execution helper accompanies all three callers and their
consumer fixture copies. It qualifies execution and failure propagation before
Git observation, formatting, or successful validation evidence. The local
Makefile parse barrier prevents an outer ignore-errors invocation from
suppressing a refusal; logger recipe prefixes retain jobserver descriptors.
Existing release and hook behavior uses one shared authority, with no new mode
or release state ([#311](https://github.com/dragginzgame/icydb/issues/311),
[Shared Tooling #30](https://github.com/dragginzgame/shared-tooling/issues/30)).

Focused consumer release, adapter and real staged-formatting fixtures pass on
Linux under Bash 5 and nested Bash 3.2. Unsafe compact/long Make controls are
refused; selected files/index and prior complete logs survive. Real nested
parallel Make retains its selections and jobserver. Release Git effects are
strict substitutes; formatting fixtures reuse existing objects without commits.
Committed LOC fixtures qualify default/custom output paths, including literal
glob characters; all 49 current consumer member totals match the prior report.
Locked member metadata and Cargo.lock are unchanged. Snapshot, scoped ShellCheck,
parsed workflow fixtures, actionlint and documentation checks pass. Evidence is
retained under `target/shared-tooling-014/`. The existing native CI lane includes
the shared release fixture; native consumer execution remains pending, and full
workspace/release gates were not requested. No network lifecycle action occurred.

The maintainer-directed layout exception in AGENTS.md preserves the 42 restored
package paths, one workspace and its lockfile (#310). No product functions,
methods or types are removed. The issue repair changes 30 files by approximately
425 net added lines, principally canonical governance, admission and fixtures;
implementation shape remains neutral, with no additional execution flow or
persisted state. Raw Wasm-size, IC-cycle and instruction deltas are unmeasured.

### 2026-10-07 direct host deduplication

The published 0.3.0 owners now replace six local helpers: `read_name`,
`read_u32_leb`, `take_byte` and `take_bytes` in canister method inspection;
`sha256_hex` and `format_process_failure` in optimizer execution. Shared Wasm
facts own framing; admitted tools own executable hashing/version checks and
bounded capture. A typed local error projection preserves captured diagnostics
and cleanup outcomes. Diagnostic stream/file read mechanics also converge on
shared owners, with one current JSON/provenance authority in IcyDB.

Focused Linux qualification passes: strict CLI/integration Clippy, 53 diagnostic
tests, six method/ABI tests, two real optimizer tests, two cold/warm batch tests
and eight report tests. Manifest ordering, graph, workflow and documentation
checks pass. The CLI adds artifact/filesystem dependency edges; integration
drops the filesystem edge no longer used after shared admission. All package
selections/checksums and the root manifest remain unchanged. The initially
uncached locked IC Metrics 0.2.3 was prepared at that exact version before
successful offline checks; no dependency upgrade was substituted.

Evidence is retained under `target/host-dedup-030/`. Native macOS CI now includes
diagnostic reads, method inspection, spaced optimizer paths, failed-output
preservation and continued batches; native consumer results remain pending.
Full workspace/release gates were not requested. Raw Wasm bytes, IC cycles and
instructions are unmeasured; no ICP/PocketIC lifecycle action occurred.

### 2026-10-06 four-package host adoption

IcyDB's four direct host selections are published 0.3.0 packages from `efd402e`.
Strict selected CLI/integration Clippy passes, including the report binary and
all integration targets with moved digest imports. Eight report tests preserve
whole-stream artifact identities and Wasm structure facts; one real optimizer
admission test and two CLI response-decoding tests pass. The selected lock graph
and manifest ordering pass. Existing package versions are preserved; the graph
adds Wasm parser 0.261.0 for the artifact crate's explicitly enabled feature.

The first offline resolution attempt stopped at the uncached locked IC Memory
0.28.4 selection. Exact cache preparation and locked fetching completed before
successful offline qualification; no unselected dependency upgrade was used.
Evidence is retained under `target/host-packages-030/`. The existing native CI
lanes already compile both consumers and run these host contracts, but the local
changes have no native consumer macOS result yet. Full repository/release gates
were not requested. Raw Wasm bytes, IC cycles and instructions are unmeasured;
no ICP or PocketIC network lifecycle action occurred.

### 2026-10-06 committed 0.1.11 follow-up

At that adoption the 52-file snapshot selected `46c0277`. The baseline and agent-maintenance
rule are refreshed together: authorized local repairs are applied in the working
tree and upstream findings are reported in their owning GitHub issues. IcyDB's
product, broad-validation and release boundaries remain local.

The committed correction for
[Shared Tooling #22](https://github.com/dragginzgame/shared-tooling/issues/22)
keeps successful and ignored Rust `error::` names plain, actual diagnostics and
failed tests highlighted, and surrounding retained context neutral. Shared logger
fixtures and IcyDB's adapter checks pass under Linux Bash 5 and 3.2, including
nested target execution and complete raw combined logs. The adopted release
fixtures also qualify exact changelog ownership for large major/minor/patch
components on the local AWK executable
([Shared Tooling #23](https://github.com/dragginzgame/shared-tooling/issues/23)).
The committed upstream fixture-retention launcher passes its injected failure
and preservation checks under Linux Bash 3.2
([Shared Tooling #21](https://github.com/dragginzgame/shared-tooling/issues/21)).
Logs are retained under `target/shared-tooling-011/`. The
[matching upstream CI run](https://github.com/dragginzgame/shared-tooling/actions/runs/37500153922)
was still running at handoff; neither it nor Linux Bash 3.2 establishes native macOS
qualification. Full IcyDB workspace/release gates and native consumer checks
remain user/CI-owned. Cargo versions, the existing lock update, Git index and
published notes are preserved. Raw Wasm bytes, IC cycles and instructions are
unmeasured; these changes add no database behavior or validation mode.

### 2026-10-06 committed 0.1.10 follow-up

At this adoption the 52-file snapshot selected `21f3ec3`. IcyDB's formatting targets
probe the reviewed cargo-sort pin and prepared rustfmt through the shared offline
owner. The real consumer formatter and staged-hook fixtures pass under Linux
Bash 3.2 with repository-local release scratch directories. Shared formatter
refusal fixtures and all 87 substitute-Git tag-maintenance cases pass; these
checks neither install tools nor mutate real tags. Logs are retained under
`target/shared-tooling-010/`.

[The committed upstream CI run](https://github.com/dragginzgame/shared-tooling/actions/runs/37491682760)
passes Linux and lint/security. Both native macOS portable jobs pass their
formatter and hook checks, then fail in the fixture-retention launcher. The
BSD-sed shebang defect and then-uncommitted correction were tracked in
[Shared Tooling #21](https://github.com/dragginzgame/shared-tooling/issues/21#issuecomment-6020926815).
This is not consumer native qualification. The separate logger correction in
[Shared Tooling #22](https://github.com/dragginzgame/shared-tooling/issues/22)
was also uncommitted and excluded from that snapshot. Both corrections are now
committed in the separately qualified 0.1.11 adoption above.

The concurrent dependency-lock update is preserved. Its selected packages were
fetched with the lock held unchanged into IcyDB's designated Cargo home before
continuing offline focused checks. Full repository/release gates and native
consumer macOS checks remain user/CI-owned. Raw Wasm bytes, IC cycles and
instructions are unmeasured; no timing measurements replace them.

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

## 2026-10-07 host extraction and streamed publication

The selected four 0.3.1 packages come from committed `38a2a51`; all 42 packaged
Rust source files match that revision. Dirty 0.3.2 source and response-only feature
changes are excluded. Integration directly consumes filesystem/tools in addition
to artifacts/process, adding only two dependency edges; every selected package
version and checksum is preserved.

Candid extraction now shares tool admission, bounded capture, executable
rechecks and source before/after identity checks. The local adapter supplies the
existing setup version catalog, explicit inherited environment, 1 MiB per-stream
bounds and a 600-second operational deadline. Its digest captures the selected
installed executable; it does not claim an upstream binary pin. Exact original
stdout remains the artifact/manifest authority, preserving trailing whitespace
and blank lines. Canic normalization does not change IcyDB's retained text.

ICP staging uses shared stream copying and durable per-file replacement,
preserving source Wasm permissions. Publication is not a multi-file transaction.
The selected build feature determines deliberate Candid omission and removal of
stale DID; an unexpected extractor failure preserves the old DID and returns an
error. Report identities use bytes and digest from one admitted file stream.

Focused Linux checks pass: warning-denied integration library/report and the
three modified guard targets; seven artifact tests and eight report tests. The
real installed extractor exercises exact text, paths with spaces, permissions,
source preservation, failed extraction and explicit omission. Existing native CI
already selects the artifact/report tests; native macOS consumer execution and
full workspace/release gates remain pending/user-owned. No ICP or PocketIC
network lifecycle action occurred. The initial missing selected IC Memory 0.30.0
cache and test digest type error remain in retained logs; exact locked fetch and
corrected source precede successful offline checks.

Deleted the three local `extract_candid` closures in schema_guard, sql_guard and
read_authority integration tests; their inspected manifests supply the same
complete text. The production adapter grows by 27 lines to supply admission and
bounds, while duplicate extraction/publication mechanics converge upstream.
No production function, method or type is deleted. New tests and qualification
notes account for the remaining footprint; input hashes and incremental changes
are retained under `target/host-dedup-031/`.

[Consumer #307](https://github.com/dragginzgame/icydb/issues/307) owns this adoption.
Further named external-tool publication is tracked in
[Host #8](https://github.com/dragginzgame/ic-host-tooling/issues/8); borrowed error
evidence access is tracked in [Host #9](https://github.com/dragginzgame/ic-host-tooling/issues/9).
Response-only tools dependencies remain publication-gated in
[Host #3](https://github.com/dragginzgame/ic-host-tooling/issues/3). These are API
proposals, not alternate local implementations or consumed unreleased source.
Raw Wasm-size, IC-cycle and instruction deltas remain unmeasured.


## 2026-10-07 response-only CLI and borrowed process evidence

The four selected host packages at 0.3.3 are published from committed
`3d18ca9a9ed0ac5935a16c5bac99694d8e9a7d0a`; all 46 packaged Rust source files
match that immutable revision. The earlier 0.3.1/0.3.2 qualification above is
historical evidence, not the current dependency selection.

Root `ic-host-tools` disables defaults and declares the 0.3.2 minimum required
for profile selection. The CLI uses response decoding alone and its normal
graph excludes `ic-host-process`; its separately required artifact/filesystem
edges remain. Integration explicitly enables `candid-extraction`, retaining
shared extraction, bounded execution and identity rechecks. The process edge
requires 0.3.3 for borrowed `ToolError::evidence()` and `execution_error()`;
IcyDB's formatter retains captured streams, status and kill/wait diagnostics
without mapping upstream error variants itself. No command/output grammar,
executable admission policy, capture limit or runtime contract changes
([IcyDB #307](https://github.com/dragginzgame/icydb/issues/307),
[Host #3](https://github.com/dragginzgame/ic-host-tooling/issues/3),
[Host #9](https://github.com/dragginzgame/ic-host-tooling/issues/9)).

The existing native CI selections independently compile/test the CLI response
profile and integration extraction. Focused Linux evidence stays under
`target/host-response-032/`; final source records identify 0.3.3 separately from
the first 0.3.2 pass. Native consumer qualification and full workspace/release
gates remain separate. The only lock changes are the four host packages; the
maintainer-selected IC Timers 0.14.7 and IC Testkit 0.20.0 remain unchanged.

Portable external-producer staging remains [Host #8](https://github.com/dragginzgame/ic-host-tooling/issues/8).
Version-only installed-tool admission remains [Host #11](https://github.com/dragginzgame/ic-host-tooling/issues/11);
IcyDB's current Candid digest is still explicitly an installed-tool observation.
Those shared API gaps do not justify local fallback implementations.

## 2026-10-07 Host 0.4 artifact reads and report capture

The maintainer-selected direct dependencies now use published Host 0.4.0 at
`6b171744def811882ba6c71d50135efa898302a9`. Registry archive checksums and all
48 packaged Rust source files match; the exact revision is remotely retrievable.
[Exact-source owner CI](https://github.com/dragginzgame/ic-host-tooling/actions/runs/37602699181)
passed Linux x86_64, macOS 15 ARM/Intel and MSRV. This qualifies the shared owner;
native IcyDB consumer qualification remains separate. Earlier dated sections
record historical selections and checks.

One IcyDB artifact policy selects a descriptor, observes its complete length,
and delegates regular-file admission, bounded streaming and fallible allocation
to `read_opened_file`. All six production Wasm reads use that policy, retaining
the build/cache owner's lifetime and selected-file symlink behavior. This adds
no arbitrary fixed cap or input-size configuration; growth beyond the observed
extent fails instead of increasing allocation. Descriptor custody does not
freeze contents or establish Wasm validity.

Report version/provenance, feature and structural-metric commands use
`capture_command`, retaining caller arguments, cwd and inherited environment.
They now share the existing optimizer envelope of 1 MiB per stream and a
600-second operational deadline, with no retries or process-group ownership.
Nonzero exits, overflow and cleanup failures use IcyDB's existing bounded
evidence formatter. Success parsing and report format remain unchanged.
Inherited-output CLI commands and its response-only dependency profile remain
separate contracts.

Strict selected integration library/report and CLI checks pass, along with
the artifact/report/CLI response selections recorded in
`target/host-adoption-040/`. These focused Linux checks do not establish native
consumer macOS execution or full workspace/release readiness. No ICP/PocketIC
network lifecycle action or permitted performance measurement was performed.

Testkit 0.20.0 retains all four 0.3.3 packages alongside IcyDB's direct 0.4.0
packages; its public re-exports require a minor adoption release
([Testkit #13](https://github.com/dragginzgame/ic-testkit/issues/13)). No Cargo
patch, forced resolution or compatibility path was introduced. `copy_reader`
returns `CopyError`, which has no `io::Error` conversion in 0.4: publication
retains its existing typed copy-error wrapper. The initial audit's proposed
direct conversion was incorrect; the correction and remaining owner-level
projection gap are on [Host #14](https://github.com/dragginzgame/ic-host-tooling/issues/14#issuecomment-6035787706).
Named optimizer staging and observed-identity extractor admission still await
Host #8/#11. No upstream or sibling files were edited.
