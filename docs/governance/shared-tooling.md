# Shared Tooling Adoption

IcyDB adopts the [vendored shared engineering baseline](../../DRAGGINZGAME.md)
at reviewed revision `08b6a6ae4e1ea39140f42eebd692a506c4e08c09`
(Shared 0.13.3 plus the reviewed fixture-probe extension). Root
[AGENTS.md](../../AGENTS.md) is the local overlay. The shared approved layout
retains existing canister, schema and testing packages in their
restored directories, in the single root workspace and lockfile; no new package
move is part of this adoption ([#310](https://github.com/dragginzgame/icydb/issues/310)).
Product architecture, resource limits, exact qualification gates
remain local; standard release commands follow the shared contract. A sibling checkout cannot silently change
these rules.

## Ownership and provenance

### Structural Rust scanner source

The additive [.shared-rust-source.snapshot](../../.shared-rust-source.snapshot)
selects only Shared Tooling's panic scanner, semantic fixtures and compilation
guide at reviewed commit `48f546ad4671351bba45f89fb007a2c42be521e1`
([Shared #225 — Rust panic scanner](https://github.com/dragginzgame/shared-tooling/issues/225),
[PR #226](https://github.com/dragginzgame/shared-tooling/pull/226)). This is the
reviewed contribution commit, not a claim of a released scanner API. The root governance/tool snapshot above remains unchanged. Both selections use
the same unchanged verifier bytes; the panic gate verifies the additive source
before compilation. A later full adoption can consolidate these roots through
the existing exporter ownership handover.

Private `testing/source-checks/` compiles the shared source under the existing
workspace and selected lockfile. It depends only on Syn/proc-macro2, with no
database, Build, Testkit or PocketIC edge. IcyDB retains source-root discovery,
required nonempty inventories and actual production scans. Shared owns syntax,
macro, cfg and parser regressions; its source has no independent installed tool
or dependency selection.

### Selected IC Build 0.5.0 — 2026-10-15

The incoming manifest and lock already select registry Build 0.5.0. Its archive
matches checksum `9f05b745a916221ec7ca511de9fdba79304c582711b0b8c3ec815cba070716ce`
and identifies release/tag source `4feef850010ed725edb9b9e9adeaed140baf5106`.
The refreshed registry index lists 0.5.0 as the latest non-yanked stable version.
This is the published source, not the newer dirty sibling checkout.

IcyDB makes one `with_extra_env` call for encoded compiler flags, so the new
merge-across-calls contract preserves its recipe. Neither the removed
`with_rustc_program` nor `artifacts::test_target_dir` has a maintained caller;
compiler admission and fixture paths remain with their existing owners. Rust
1.91 is below the private build package's floor. No local API fallback, source
override, cache identity or recipe change is needed. The Cargo-flags helper is
still absent from the published API; [Build PR #37](https://github.com/dragginzgame/ic-build/pull/37)
remains open, so the qualified consumer deletion is still publication-gated.

Strict build all-target Clippy passes. All 22 distinct build-library cases and
14 report cases pass, including real-Cargo flag precedence, single/batch cache
identity, changed remap delivery and retained partial-failure outcomes. The first
library run passed 21 cases and failed only because stable lacked its Wasm
standard library. Explicitly installing that target permits the failed case to
pass; the failed fixture/log is retained. The 0.5.0 selected graph stays fixed at
manifest/lock SHA-256
`61169339d807b2b95843ef2f1fc5d4a1e14cc847225cbee8c72dc57a98fff7a2` /
`52d9b75712c97e951b9a493e0839e38288eee07af65f99297a7d75ad967d0988`.
Public normal/build dependency inspection still excludes Build and Host. No
Rust source, Cargo selection, package version, source override or Git delivery
change is made. Full workspace/release and native macOS gates remain unselected;
raw Wasm/IC cycle/instruction effects are unmeasured. No IC network lifecycle
operation runs. Evidence: `.cache/ic-build-050`.

### Selected IC Build 0.4.0 — 2026-10-15

The incoming registry selection is Build 0.4.0, checksum
`17bc5e5c137a0055ce069d2d244017aafcddcfe47fd92bf2141bf7e7154c1010`,
whose archive identifies released source `d223747a003c35954d02e43ed110dd23707affa1`.
Its Rust 1.91 floor is within IcyDB's private contributor floor; the public
library normal/build graph excludes Build and Host. IcyDB uses retained artifact
records and has no calls to the removed Wasm path/readiness helpers. No local
replacement, cache format or recipe change is introduced.

Strict build all-target lint, 22 build-library and 14 report cases pass with
Host 0.18.5 / Timers 0.23.1 on fixed lock
`af6a4986fc1cf451e78ec4f9dadbaba70e9114dc8c489f612c10361a4211ae8d`.
The concurrent Timers update and its initial cache failure are retained separately
from the initial 0.23.0 qualification. Existing native-host receipts identify
their delivered graphs. Full workspace/release and native macOS checks remain
unselected. Evidence: `.cache/build-refresh-02742` and the current 0.274 tracker.

### Delivered 0.275.0 full-gate closeout — 2026-10-16

Released IcyDB `e104fe1c9f5921336f0e5b292acbda99f8311476` retains the reviewed
Shared 0.13.3 snapshot. [PR #353's complete CI](https://github.com/dragginzgame/icydb/actions/runs/38759471077)
passes Linux workspace/core/MSRV/Tier A/Tier B/Wasm report, both native macOS 15
hosts and the aggregate check. Head `e5eb616dd5ff5e882d66cebb9a68eddcbaaf850f`
differs from that release only in two workflow action-pin files; product source,
lock, adopted snapshot and common catalogs are identical. Workspace CI now
installs the Wasm target before its real encoded-flags build-cache case.

This closes [#311](https://github.com/dragginzgame/icydb/issues/311) and
[#357](https://github.com/dragginzgame/icydb/issues/357)'s delivered adoption
criteria. The older records below retain their original evidence and gaps.
Later intermittent fixture failures are corrected separately by
[PR #368](https://github.com/dragginzgame/icydb/pull/368); the incoming dependency
graph and Memory 0.43 diagnostics remain unqualified under
[#365](https://github.com/dragginzgame/icydb/issues/365). No local broad gate or
release operation was run for this closeout.

### Delivered 0.274.1 native-host acceptance — 2026-10-15

Released IcyDB `361347ed90f580f15b26d432945e44f3ce6f3d7a` selects the
Shared 0.13.3 plus fixture-extension snapshot recorded below. Its latest
[CI matrix](https://github.com/dragginzgame/icydb/actions/runs/38635605619)
passes both [Apple Silicon](https://github.com/dragginzgame/icydb/actions/runs/38635605619/job/116097841601)
and [Intel macOS](https://github.com/dragginzgame/icydb/actions/runs/38635605619/job/116097842086).
Actual native logs confirm workstation setup, offline tools, portable automation,
release/receipt/hook fixtures, all 126 completion controls, diagnostic/artifact
tests, Testkit-managed and explicit-server startup, post-link failure isolation,
report tests, production Wasm construction and formatting. These are actual
native results, replacing the earlier pending-host status for that delivered source.

The complete matrix remains failed: Linux workspace build-cache flag propagation
reaches a real Wasm build without its target installed. Core, static, Tier A,
Tier B, public MSRV and Wasm evidence pass; the separate
[Tier C run](https://github.com/dragginzgame/icydb/actions/runs/38668416205) also passes.
The pending compatible correction prepares that target before workspace dispatch
and qualifies its absence/order/failure policy. Full-gate acceptance remains open
under #302/#311/#357 and build/report delivery under #361 until corrected delivered
CI passes. This does not qualify the incoming dirty dependency updates.
Exact job observations/logs and focused checks are retained in
`.cache/issues-02742`; historical failures and their original identities remain
unchanged. No new native run, release or local IC network lifecycle is invoked.

### Shared 0.13.3 with the maintained fixture extension — 2026-10-14

Canonical preview/refresh selects committed source
`08b6a6ae4e1ea39140f42eebd692a506c4e08c09`, combining upstream main
`d963db4b85b4fa65560fd87e8398f228955a2b71` with the existing
[Shared PR #183](https://github.com/dragginzgame/shared-tooling/pull/183)
fixture extension. The source displays 0.13.3; its additive 0.13.4 notes are
pending PR integration, not another release. The retained extension is required
by IcyDB's current root aliases and diagnostic-failure controls.

The same 92 entrypoints derive the same 105 payloads. Only the verification
helper guide and fixture lifecycle implementation change. Shared now admits
fixture completion separately from cleanup; cleanup still preserves original
failure status and failed evidence. No tool catalog, compiler, Cargo selection,
workflow gate or Testkit ownership change is required. Optional upstream helpers
and pending PR capabilities are not inferred from changelog headings.

The candidate passes the full 126-case IcyDB completion/retention roster on Linux
Bash 5 and genuine Bash 3.2, together with affected producer completion/retention,
consumer Make adapters, receipt callbacks and standard-release substitutes.
Snapshot integrity, KEEP-only repeat preview and selected source-following
ShellCheck pass. No real release, canister or network lifecycle effect occurs.
Full workspace/release and native macOS qualification remain unrun. Upstream
0.13.3 CI was queued at review
([Shared run](https://github.com/dragginzgame/shared-tooling/actions/runs/38592784005)).

Evidence is retained in `.cache/shared0133` and
`/tmp/icydb-shared0133.C4agLD/`. The user explicitly authorized updating the
existing PR branch after automatic review requested delivery approval. Revision
`08b6a6a` is published on that branch and canonically adopted in the normal
IcyDB checkout. Upstream integration/release and IcyDB staging/commits remain
separate maintainer actions.


### Shared 0.12.8 with the maintained fixture extension — 2026-10-14

Canonical preview/refresh selects committed source
`776390473585e5dc1ba25dcdc35a3071da1289bf`: upstream 0.12.8 at
`eafa07cac1fdba7d6303d061ed8af458aacf32b7`, merged into the existing
[Shared PR #183](https://github.com/dragginzgame/shared-tooling/pull/183).
The PR remains an unpublished extension, rather than a released Shared patch.
The 92 existing roots now derive 105 payloads. Two required guides cover
explicit Rust toolchain setup and CI change selection; release documentation
also records the publication-guide companion. Existing publication documentation
was already selected here. Refresh does not enable publication commands.

The shared changelog guidance now requires concise outcome-linked release
summaries. IcyDB's current root draft points to its detailed outcome notes
instead of repeating the full issue roster. The shared development/MSRV action
is available upstream but remains unselected: IcyDB's distinct compiler jobs
have different setup requirements and do not jointly prepare both toolchains.
IcyDB retains its compiler versions, common catalogs, Testkit ownership and
validation gates. Executable snapshot bytes and modes are unchanged.

Focused consumer/producer fixture, integrity and stable-preview evidence is
recorded in the current status tracker. Complete workspace/release and native
macOS gates remain separate; Linux Bash 3.2 is not macOS qualification. The
upstream 0.12.8 CI run was queued at inspection
([Shared run](https://github.com/dragginzgame/shared-tooling/actions/runs/38558617555)).
Evidence is retained under `.cache/shared0128`. Imported snapshot copies and
prior pending work are excluded from this adoption's authored change metrics.

### Reviewed fixture-probe extension — 2026-10-14

Canonical preview/refresh selects clean committed source
`53a8d0134a62e715dc310e10487d59fed41b89fd`, published as
[Shared PR #183](https://github.com/dragginzgame/shared-tooling/pull/183).
This is a reviewed branch over Shared 0.12.5, not a newer released version.
The single new root selects the common probe owner: 92 roots, 103 payloads.
Common catalogs remain unchanged. IcyDB keeps its roster and seven selected
cases while delegating injection/status/evidence checks, including its diagnostic
failure case. Inherited root aliases are cleared only for disposable children;
poisoned-root cases leave the unrelated path untouched. All 126 cases pass on
Linux Bash 5 and genuine Bash 3.2. The producer
retention/collector matrix, its new root-name cases and source-following
ShellCheck also pass on both shells. Snapshot integrity and KEEP-only preview
pass. Native and full gates remain separate; no producer merge/release is implied.

### Shared 0.12.5 and IC Build 0.3.0 — 2026-10-14

Canonical preview/refresh selects clean committed Shared source
`4256171b5b7ec4c4cc4078548c1c1617ee150699`: 91 roots, 102 payloads.
The CI evidence action now delegates selection/archive handling to
`collect-tool-evidence.sh`; host/IC admission delegates its shared bundle decision
to the tool-store owner. Common catalogs remain byte-identical. Optional fleet
reports, README rendering and upstream-only Python are outside this selection.
The shared focused-validation policy now agrees with the retained local gate
selection; existing broad and native acceptance obligations are unchanged.

The incoming lock selects registry IC Build 0.3.0, checksum
`415c50c37611014ab87016292bffb7ca366498c52062d226a0c9e886346683f8`,
archive VCS `84cc436ddacf3e9ef21b62167bb9a50a52f871ff`, with Host 0.17.11.
Removed watched-input APIs have no IcyDB callers. Wasm/retained-cache calls remain
on the existing engine; descriptor custody and verified destination copying
stay upstream. This contributor integration does not change the published
IcyDB API, data semantics, persisted format or supported library build floor.
Dependency selection and own package versions are preserved.

Explicit common tool setup and offline checks, snapshot integrity and KEEP-only
repeat preview pass. Strict integration-library Clippy and all five maintained
cache-adapter tests pass, including actual single/batch fixture Wasm builds,
encoded flags/cache identity, retention/progress controls and post-link partial
failure isolation. Host/IC installer, actual evidence collector, tool-command,
Rust-tool, consumer-adapter and isolated note-finalization fixtures pass on Linux
Bash 5 and genuine Bash 3.2.57. Selected ShellCheck with sourced helpers and the
extracted action body, Wasm post-link admission and touched-document links pass.
The first ShellCheck invocation omitted source-following and produced incomplete
analysis; the corrected selected gate passes without source changes. Final
Cargo/adapter fingerprints stay fixed. Evidence is in
`.cache/shared0125-build030`; imported payloads are excluded from authored deltas.

Exact-source CI was queued at inspection
([Shared run](https://github.com/dragginzgame/shared-tooling/actions/runs/38547187749),
[Build run](https://github.com/dragginzgame/ic-build/actions/runs/38546827916)).
This draft has no hosted consumer acceptance. Keep #302/#357 open through their
native gates; Linux Bash 3.2 does not qualify macOS. Full workspace/release gates
were not selected. Raw Wasm-size, IC cycle and instruction deltas are unmeasured.
No simulator/network lifecycle, Git delivery or release operation occurred.

### Shared 0.12.0 — 2026-10-14

Canonical preview/refresh selected `f7fe31bf80a2b445bf37a470527f001210241aea`
from a clean temporary checkout; the sibling's uncommitted release documentation
was excluded.
The existing roots plus two new linked-document roots form 91 entrypoints and
101 payloads, including the common literal tool-catalog reader. The documents
complete the new release/maintenance links without adding publication commands.
Common versions/digests match the prior snapshot. Strict
candidate finalization refuses unnumbered or misplaced headings; both local
views are checked before replacement, and missing detail notes are no longer
synthesized. Three lock/pin/note fixtures reuse shared cleanup and retention.

Common tool setup and offline admission, snapshot integrity, selected ShellCheck,
KEEP-only preview and twelve release/tool/adapter/completion fixture families pass on
Linux Bash 5 and genuine Bash 3.2.57. Completion qualification covers 126 injected
exits on each shell. All release effects are confined to disposable/substitute
fixtures; the real source index, versions, commits and refs are unchanged. The
incoming Testkit graph remains its sole server-selection/setup owner; no network
or simulator lifecycle was started. Evidence is retained in `.cache/shared0120`.
After a concurrent user lock update, final tool admission, eight parser cases
and strict core SQL/metrics/no-default lint pass with Metrics 0.9.3. Initial and
final Cargo fingerprints are retained separately; no dependency was reselected.

The new acceptance procedure uses current delivered source and its latest full
required CI matrix. Local drafts have no hosted acceptance, and Linux fixture
passes do not qualify native macOS. Keep #302/#357 open for their owned native
and adoption gates. Full workspace/release gates remain skipped user-owned
validation; raw Wasm, IC cycles and instructions are unmeasured.
The exact [Shared source run](https://github.com/dragginzgame/shared-tooling/actions/runs/38525497845)
was in progress at inspection; no complete upstream matrix is claimed.

### Delivered completion evidence and macOS lint caller — 2026-10-13

Published 0.273.0 (`ed542f97e17ccca9d2dc35284e06c730230a0c63`) passes the
126-case completion/retention matrix on both
[Intel macOS](https://github.com/dragginzgame/icydb/actions/runs/38378345770/job/115232260383)
and [Apple Silicon](https://github.com/dragginzgame/icydb/actions/runs/38378345770/job/115232260409)
before those jobs fail at their later selected ShellCheck command. Actual receipt
callbacks, standard release, hook and portable fixtures also reach their passing
completion markers. The remaining normal candidate-receipt fixture now passes in
disposable repositories on Linux Bash 5 and genuine Bash 3.2.57, using the same
published subject/test bytes. Git effects stay inside those temporary fixtures.
The original false-success finding is closed under
[#342](https://github.com/dragginzgame/icydb/issues/342).

The later host failure is reproducible with the exact selected lint command:
SC1091 refuses to follow `tool-store.sh`, then SC2154 lacks its `store_slot`
assignment. The consumer CI command now enables source following, preserving
warning checks and immutable Shared 0.11.1 bytes. Selected ShellCheck, workflow
lint and topology checks pass; another native macOS run is still required under
[#302](https://github.com/dragginzgame/icydb/issues/302). Evidence is retained in
`.cache/backlog-0274` and the downloaded published job logs. No runtime or tool
selection changes; full workspace/release gates remain separately selected.

### Shared 0.11.1 and IC Build 0.2.3 — 2026-10-13

Canonical refresh uses the committed Shared 0.11.1 revision above from a clean
temporary source checkout. The sibling's unpublished fixture edits are excluded.
The snapshot now has 89 entrypoints and the same 98-file closure: retire the
actionlint/ShellCheck forwarding scripts and derive the common tag checker and
fixture lifecycle helper. CI and the consumer actionlint adapter call the
canonical installer directly; two local fixtures share cleanup and failure
retention. The completion probe accepts the maintained EXIT trap and stages its
shared helper before testing interruption. Existing tool versions/digests,
compiler selections and Testkit ownership remain unchanged
([Shared #161](https://github.com/dragginzgame/shared-tooling/issues/161)).

The incoming lock already selects registry IC Build 0.2.3, SHA-256
`9f5baf0449153f28de324d37dc9787298d77788940e0bce88f3d0c252b018ca5`,
whose published tag resolves to `23e047b7672aae0d5ba442c4b384c4b782835c87`.
Its standalone source-evidence API stays upstream: IcyDB already delegates Cargo
input/cache discovery to the maintained Wasm engine and needs no second inventory
or new adapter. Unpublished staging/cache edits are excluded; manifests and
lockfile selections are preserved. Strict integration/artifact-contract Clippy,
nine artifact/Candid cases and five cache cases pass, including a real Wasm
fixture with retained batch outputs and encoded flags
([#307](https://github.com/dragginzgame/icydb/issues/307)).

Common tool preparation/admission, snapshot integrity, KEEP-only refresh preview,
selected ShellCheck, workflow lint/invariants, CI inspection, tool commands,
consumer adapters and substitute release runner/receipt fixtures pass on Linux.
Completion/retention covers 126 injected exits on each of Linux Bash 5 and genuine
Bash 3.2.57. Consumer adapters, CI inspection, tool commands and release-runner
fixtures pass on both shells. Evidence is retained in `.cache/shared0111-build023`.
Exact-source upstream CI was queued when reviewed
([Shared run](https://github.com/dragginzgame/shared-tooling/actions/runs/38418557877),
[Build run](https://github.com/dragginzgame/ic-build/actions/runs/38416043907));
native macOS and full workspace/release qualification remain separate. Raw Wasm,
cycles and instruction deltas are unmeasured. No simulator/network lifecycle,
Git delivery or release effects occurred.

### Shared 0.10.1 catalog ownership and Host 0.17.2 — 2026-10-13

Canonical refresh adopts committed Shared 0.10.1 with the existing 91 roots and
98-file closure. Common host/Cargo versions and all IC versions/digests match
the previous snapshot. Make, standalone installers, formatting, freshness
inspection and failure evidence use snapshot-owned defaults; IcyDB needs no
exception catalog. Root-layout workflow consumers already read those same
immutable catalogs. Compiler/MSRV, product tools and Testkit remain locally
owned. Cargo cache keys now retain reuse across unrelated literal catalog edits,
while tool/build changes still invalidate them
([#357](https://github.com/dragginzgame/icydb/issues/357),
[Shared #154](https://github.com/dragginzgame/shared-tooling/issues/154)).

The incoming lock already selects Host 0.17.2 from
`e3afa6ddf022309e7ba97f6cefb312c48738db4e`. Its release changes producer fixtures
and example failure retention, requiring no IcyDB API adapter. Explicit common
setup and offline admission, strict CLI/integration Clippy, nine artifact/Candid
tests and 21 CLI tests pass. CLI normal dependencies remain parser-free.
Evidence is retained in `.cache/host0172-shared0101`. Native macOS and full
workspace/release validation remain separate; raw Wasm, cycle and instruction
deltas are unmeasured. No simulator/network lifecycle or Git delivery occurred.

### Registry IC Build 0.2 adoption — 2026-10-13

The authorized continuation updates the shared build catalog to registry 0.2
and locks IC Build 0.2.0, archive SHA-256
`fb055d1b5649636c09a52f23fdc091417c732cbb87a2125d5a24f93c043c8df8`,
from `9ddcd4ee0b2ac85f0608b8aaf49493cf015f295a`. The three unused Host 0.16
packages retire. All other registry versions/checksums and own package versions
are preserved; runtime work and the prior Candid feature correction remain
intact. No local producer override or compatibility build flow is retained.

Strict selected integration library/artifact-contract Clippy, all nine
artifact/Candid cases and five cache cases pass against the published graph.
The cache cases include a real Wasm build preserving encoded flags and retained
batch artifacts. The ordinary CLI normal
dependency tree remains parser-free. Receipts live under
`/tmp/ic-build-020-registry-adoption/icydb-*`. Existing 0.273.1 notes own this
compatible integration change. Native macOS and full workspace/release checks
remain separate; Wasm/cycle/instruction deltas are unmeasured. No simulator or
network lifecycle, Git delivery or release action occurred
([#307](https://github.com/dragginzgame/icydb/issues/307)).

### Earlier Host 0.17 Candid integration qualification — 2026-10-13

The authorized consumer correction enables `candid-service` alongside
`candid-extraction` in native integration. Host 0.17's interface-only profile
no longer provides service parsing. Root defaults and the ordinary CLI selection
stay unchanged; its normal dependency tree excludes `candid_parser`.

The incoming registry graph now selects IC Build 0.1.4 and Host 0.17. Focused
integration library/artifact-contract Clippy and all nine artifact/Candid tests
pass. Evidence is `/tmp/icydb-host017-{integration-clippy,artifact-contract-clippy,artifact-tests,cli-normal-tree}.log`.
The explicit offline metadata preparation preserves the captured lock bytes;
unrelated catalog/runtime work is retained. No own package version is changed.

The same selected Clippy and nine tests pass against the unpublished IC Build
0.2.0/Host 0.17 candidate in `/tmp/ic-build-020-consumers.GxZq30/icydb/`.
That frozen fixture alone selects 0.2 and a local producer override. The real
catalog remains registry-based; publication precedes adoption. This is Linux
correctness evidence, not native macOS or full workspace/release qualification.
Raw Wasm, cycles and instruction deltas are unmeasured. No simulator/network
lifecycle, release or Git delivery action occurred
([#307](https://github.com/dragginzgame/icydb/issues/307)).

### Released Shared 0.9.13 and Host 0.16 adoption

Canonical refresh from committed Shared 0.9.13 updates eleven payloads without
changing the 91 roots or 98-file closure. Tool locks and Cargo-source acquisition
preserve their original filesystem/fetch failure status; CI inspection refuses
an unavailable implicit branch. Locked-source preparation is authorized during
ordinary development, while explicit offline choices remain authoritative.
Sibling 0.9.14 edits are unpublished and excluded.

The maintainer-selected Host 0.16.0 graph needs no local API workaround: the CLI
uses minimal response/artifact features, while integration explicitly selects
Wasm inspection, Candid extraction and IC interfaces. IC Build 0.1.2 still brings
Host 0.15.2 alongside the direct 0.16 selection; producer convergence remains
upstream-owned. Updated shared build-helper identities are explicitly prepared
through common tool and Testkit setup. No tool pins or Cargo files are edited.

Focused qualification passes 109 Host consumer tests and strict CLI/integration
Clippy. CI inspection, Rust tools, host tools, IC tools and consumer adapters pass
on Linux Bash 5 and genuine Bash 3.2. Snapshot integrity, KEEP-only preview,
selected ShellCheck, offline tool admission and documentation checks pass. The
CLI graph contains no extraction engine or Candid parser. One governed PocketIC
probe remains intentionally ignored; no network lifecycle was started. Native
macOS and full repository/release gates remain unrun. Host tooling changes claim
no deployed Wasm/cycle/instruction delta. Evidence: `.cache/host016-shared0913`.

### Earlier pending Shared 0.9.10 adoption

Canonical refresh adds the shared change-reporting rule and updates its baseline
references: 91 roots and 98 verified files. This reporting-only update changes no
installer or execution policy. Authored change counts exclude distributed copies
and incoming work. Native macOS qualification remains pending.

### Earlier pending Shared 0.9.9 adoption

Canonical preview and refresh from isolated committed Shared 0.9.9 update six
payloads and add the Rust-tool fixture's required cache-key helper, preserving
ninety roots with a 97-file closure. Nothing is retired. Cargo offline policy
retains its TOML boolean type; explicit environment selection keeps precedence.
The changed policy digest is prepared through the common installer and selected
Testkit setup, preserving earlier builds. PR admission checks complete JSON
identities before shell extraction or saved-state writes and retains raw lookup
attempts. Rust-tool qualification uses its prepared compiler directory and
existing Git history, while checking real staged hooks and recording forbidden
Git attempts
([#302](https://github.com/dragginzgame/icydb/issues/302),
[Shared #140](https://github.com/dragginzgame/shared-tooling/issues/140),
[Shared #139](https://github.com/dragginzgame/shared-tooling/issues/139),
[Shared #143](https://github.com/dragginzgame/shared-tooling/issues/143),
[Shared #144](https://github.com/dragginzgame/shared-tooling/issues/144)).

Rust-tool, consumer-adapter and exact-source PR fixtures pass on Linux Bash 5
and genuine Bash 3.2.57. PR qualification uses disposable local Git histories
and a GitHub command substitute; no live release effect is qualified. Snapshot
integrity on both shells, KEEP-only repeat preview, selected ShellCheck,
complete offline tool admission and documentation/diff checks pass. Evidence
is retained in `.cache/upstream-02721` against released IcyDB
`ffa481df71c13aea53c01a8c32a89290f45133a2` plus this local adoption.

All ten selected IC crates already match the latest stable non-yanked registry
versions: Build 0.1.1, CDK 0.20.3, four Host packages at 0.15.1, Memory 0.35.19,
Metrics 0.8.1, Testkit 0.35.0 and Timers 0.21.1. Cargo files, executable pins
and toolchain are unchanged; unpublished sibling work is excluded. This refresh
does not add CI caching or consumer execution routes. No local IC network
lifecycle or release action occurs. Native macOS and full repository/release
gates were not selected; raw Wasm, cycle and instruction deltas are unmeasured.

### Earlier 0.272.0 Shared 0.9.4 adoption

Canonical preview and refresh from isolated committed Shared 0.9.4 update eleven
payloads, preserving ninety roots, the 96-file closure and executable pins.
Host/IC setup uses one bounded transient download retry owner. Permanent errors,
checksum failures and exhausted retries preserve failed candidates and prior
selections. Host, IC and Rust installers reject supplied and resolved CR/LF
consumer paths before configuration or setup. Evidence verification rejects NUL
bytes before hashing and parses the captured manifest. Snapshots and release
recovery require complete 40- or 64-character Git identities
([#302](https://github.com/dragginzgame/icydb/issues/302),
[Shared #133](https://github.com/dragginzgame/shared-tooling/issues/133),
[Shared #138](https://github.com/dragginzgame/shared-tooling/issues/138)).

Eighteen focused fixture invocations pass on Linux Bash 5 and genuine Bash
3.2.57: checksum, path admission, host/IC store, real-curl loopback retry,
distribution, release recovery/PR substitutes and consumer adapters. The adapter
fixture now copies the pending Tier A nonempty-test helper and supplies a test
result from its Cargo substitute; the initial missing-helper failure is retained
separately. ShellCheck passes with sourced companions followed. Snapshot
integrity on both shells, KEEP-only repeat preview, actual offline host/IC
admission, documentation and diff checks pass. Qualification evidence is retained
in `.cache/shared-094-adoption`; executable pins and incoming Cargo/toolchain
inputs are preserved. The sibling's uncommitted 0.9.5 artifact-discovery work is
excluded. Native macOS, full repository/release gates and IC/raw-Wasm
measurements remain unrun; no local IC network lifecycle or release action occurs.

### Earlier pending 0.271.1 Shared 0.9.1 adoption

Canonical refresh from isolated committed Shared 0.9.1 updates four payloads:
local setup documentation, the two host/IC installers and their shared store.
Ninety roots and the 96-file closure remain unchanged. Installation lifecycle is
shared rather than duplicated: checkout and store locking, cache verification,
activation and cleanup retain one owner. Cleanup attempts both lock releases,
preserves the original installation error and makes a cleanup-only failure fail
([#302](https://github.com/dragginzgame/icydb/issues/302)).

Host and IC fixtures pass on Linux with Bash 5 and genuine Bash 3.2.57, including
cleanup-only failures, original-error preservation and shared-lock release.
Snapshot distribution, integrity, KEEP-only repeat preview and selected ShellCheck
pass. Actual offline host/IC admission succeeds without installation or pin changes.
The store-test companion is sourced by those fixture owners; an initial direct
invocation was invalid and remains recorded separately. Evidence is retained in
`.cache/shared-091-adoption`. Shared's subsequent dirty checksum work is excluded.
Native macOS, full repository/release gates and IC/raw-Wasm measurements remain
unrun; no network lifecycle or release action occurs.

### Earlier pending 0.271.1 Shared 0.9.0 adoption

Canonical refresh from an isolated clean checkout of committed Shared 0.9.0
updates the verifier and its consumer documentation, preserving ninety explicit
roots and the 96-file closure. The verifier's `--read-manifest` mode is now the
common syntax reader used by snapshot refresh and upstream fleet reports. It
emits no records on malformed input, rejects NUL bytes and parses the same
captured input it validates. Syntax reading does not establish payload integrity
or ownership; ordinary verification retains those checks
([#302](https://github.com/dragginzgame/icydb/issues/302),
[Shared #133](https://github.com/dragginzgame/shared-tooling/issues/133)).

Snapshot-distribution and IcyDB adapter fixtures pass on Linux Bash 5 and
genuine Bash 3.2.57 (Make 3.81 for the legacy adapter run). Local snapshot
integrity on both shells, KEEP-only repeat preview and selected ShellCheck pass.
Evidence is retained in `.cache/shared-090-adoption`. Executable pins and Cargo
selections are preserved; the sibling's dirty 0.9.1 work is excluded. Native
macOS, full repository/release gates and IC/raw-Wasm measurements remain unrun.

### Earlier pending 0.271.1 Shared 0.8.4 adoption

Canonical preview and refresh from an isolated clean checkout of committed
Shared 0.8.4 update eleven payloads and add the Cargo-tool freshness task guide
required by the maintained task navigation. Ninety explicit roots retain a
96-file closure, with no retirement or selected executable version change.
Script companions are derived; governance documents and optional functionality
remain explicit consumer selections. The freshness command and fleet aggregation
remain with their Shared Tooling owner
([#302](https://github.com/dragginzgame/icydb/issues/302)).

The common Make closure includes its installers and default LOC helper. Snapshot
copying uses a temporary file beside each destination and an atomic rename after
rechecking ownership, edits and index state. IcyDB ignores interrupted payload
scratch at every depth; the manifest still publishes last and retries use the
same exact source. Formatting admission follows the selected Rust catalog.
Cargo-tool installation refuses redirected observation leaves before writing
diagnostics, and the LOC helper offers `--json` without parsing display tables
([Shared #131](https://github.com/dragginzgame/shared-tooling/issues/131),
[Shared #135](https://github.com/dragginzgame/shared-tooling/issues/135),
[Shared #136](https://github.com/dragginzgame/shared-tooling/issues/136)).

Twelve focused fixture invocations pass on Linux Bash 5/Make 4.3 and genuine
Bash 3.2.57/Make 3.81: snapshot distribution/interruption, split-catalog Make
formatting, Cargo-tool evidence refusal, host-tool admission, JSON LOC and real
consumer hooks. Actual offline Rust-tool admission and root formatting pass.
IcyDB's complete JSON LOC report matches its human-readable crate rows and
totals. Snapshot, KEEP-only repeat preview, selected ShellCheck, documentation,
scratch-ignore and diff checks pass. Evidence is retained in
`.cache/shared-084-adoption`; incoming Cargo, toolchain, agent and pin files are
preserved. No tool installation, network lifecycle or release action occurs.
Full repository/release gates, native macOS and matched IC/raw-Wasm measurements
remain unrun. This compatible adoption is recorded in the pending 0.271.1 notes.

### Published 0.271.0 Shared 0.8.1 adoption

Canonical refresh from an isolated clean checkout of committed Shared 0.8.1
updates five payloads and the source manifest. The 89 entrypoints, 95-file
closure and selected executable versions are unchanged; no payload is retired.
Snapshot readers reject extra, empty or trailing fields without trimming literal
filename spaces. Cargo-tool installation retains the exact rejected build
identity and native observation alongside the originals, names changed fields
and refuses activation without retrying or replacing the previous selection
([#302](https://github.com/dragginzgame/icydb/issues/302),
[Shared #133](https://github.com/dragginzgame/shared-tooling/issues/133),
[Shared #134](https://github.com/dragginzgame/shared-tooling/issues/134)).

Six focused fixture invocations pass on Linux Bash 5/Make 4.3 and genuine
Bash 3.2.57/Make 3.81: shared record-reader consistency, Cargo-tool failure
evidence and real consumer hooks. Snapshot integrity, repeat preview with only
KEEP, actual offline Rust-tool admission, formatting, selected ShellCheck and
documentation checks pass. Initial record-fixture attempts omitted the prepared
host-tool PATH; corrected runs use IcyDB's selected tools. Evidence is retained
in `.cache/shared-081-adoption`. Existing pins, Cargo and pending fixture edits
are preserved; full repository/release gates, native macOS and matched IC/raw-Wasm
measurements remain unrun. No installation or network lifecycle action occurs.

### Earlier 0.271 Shared 0.8.0 adoption

The canonical exporter records explicit entrypoints and derives their shared
companion closure from committed Shared 0.8.0. IcyDB retains all 95 existing
payloads: local commands, fixture exports, CI's directory-level evidence-action
references and maintained governance navigation require their current owners.
Future refreshes reuse the recorded roots; select directly used helpers with
`--add-entrypoint`, or replace the complete selection with `--entrypoint`.
Preview reports exact additions, updates and unchanged owned retirements;
do not edit generated payload records or use the removed file-selection and
retirement options
([#302](https://github.com/dragginzgame/icydb/issues/302),
[Shared #131](https://github.com/dragginzgame/shared-tooling/issues/131)).

Shared now selects `cargo-sort-derives` 0.14.1, which preserves literal `$crate`
paths in direct and conditional derives. Explicit shared setup prepares the
selected formatter; current macro semantics require qualification beyond the
version check. Library MSRV, Cargo dependencies, IC tool pins and PocketIC
ownership remain unchanged. The snapshot was exported while the sibling source
was clean; later uncommitted sibling changes are excluded and qualification uses
an isolated checkout of the recorded revision.

Snapshot verification, root formatting and ten focused fixture invocations pass
on Linux Bash 5/Make 4.3 and genuine Bash 3.2.57/Make 3.81. The fixtures qualify
entrypoint adoption, automatic retirement and interruption/refusal boundaries,
current Rust-tool receipts, real consumer hooks and validation adapters. A
separate direct/conditional exported-macro probe preserves `$crate`, compiles
from another crate and remains stable on repeat sorting. Repeat preview reports
only KEEP. Selected ShellCheck, documentation and affected CI/post-link policies
pass. Evidence is retained in `.cache/shared-080-adoption`; the local `target/`
directory was cleared during adoption, so earlier setup/preview logs are no
longer retained. Native macOS and full repository/release gates remain unrun;
matched IC/raw-Wasm deltas are unmeasured. No network lifecycle or release action
occurs, and incoming Cargo, timer fixture, toolchain and agent inputs are preserved.

### Earlier 0.271 Shared 0.6.5 adoption

The canonical exporter advances the reviewed 95-file snapshot from Shared 0.6.3
to committed 0.6.5, including the common Make entrypoint and its interface guide.
The entrypoint owns common setup, offline checks, formatting admission/output,
hook installation and release routing. IcyDB supplies its complete derive-sort,
Cargo-sort and rustfmt commands as paired formatter inputs and retains local
Cargo paths, optimizer admission and the ordered Testkit extension. The portable
gate depends on `shared-tooling-check`; duplicate consumer recipes are removed
([#302](https://github.com/dragginzgame/icydb/issues/302),
[Shared #130](https://github.com/dragginzgame/shared-tooling/issues/130)).

Consumer validation declares Cargo.toml, Cargo.lock and rust-toolchain.toml as
prepared inputs to the shared runner. Checkpoints compare those bytes between
targets, stop later stages on drift or unreadable input and retain original
failure status and evidence. A multi-command target finishes before its next
checkpoint; this is not an editor lock or continuous source monitor
([Shared #129](https://github.com/dragginzgame/shared-tooling/issues/129)).

Shared's staged-hook context admits already prepared receipts against exported
pins/compiler without installing tools or changing unrelated index contents.
Current fixture exports include the common entrypoint and preserve real product
recipes, substituting command effects in disposable checkouts. Existing common
tool identities/pins are unchanged; no application API, persistence, timer or
PocketIC infrastructure owner is added. Evidence is retained in
`target/shared-065-adoption`; native macOS, full gates and IC/raw-Wasm measurements
remain separate qualifications.

Ten focused selections pass on both Linux Bash 5/Make 4.3 and Bash 3.2.57/Make
3.81, including real formatter/hook preservation, input checkpoints, ordered
tool/Testkit admission and substituted release routing/receipts. Root formatting,
scoped lint, documentation and affected policy checks pass. Explicit setup
prepares the incoming lockfile's Testkit 0.34.1 CLI through Shared and admits the
existing server through Testkit; complete offline `tools-check` then passes.
No network lifecycle or release action occurs. Incoming Cargo files, toolchain,
agent overlay and real index are preserved; subsequent dirty sibling changes are
excluded from the recorded revision.

### Current 0.270.4 tooling qualification

The canonical exporter adopts committed Shared 0.6.3
`4fd979d13c32adb3cb2f51f213ebfc4f98339b3d` from an isolated clean checkout,
retaining the 93-file selection. Dirty sibling documentation is excluded.
The upstream Cargo setup propagation correction resolves the earlier local
blocker; real Linux common-tool, locked Testkit and host/IC setup pass through
the canonical owners, followed by complete offline admission. Setup reuses
admitted shared payloads; no consumer installer, fallback or store is added
([#302](https://github.com/dragginzgame/icydb/issues/302),
[Shared #120](https://github.com/dragginzgame/shared-tooling/issues/120)).

Shared Cargo and release-runner fixtures plus consumer common-Make, Testkit,
validation-adapter, standard-release and receipt fixtures pass on Linux
Bash 5/Make 4.3 and genuine Bash 3.2.57/Make 3.81. Release, build and download
effects are substituted in private fixtures. These qualify failure propagation
and fresh-validation recovery, not live delivery or native macOS
([#299](https://github.com/dragginzgame/icydb/issues/299),
[Shared #126](https://github.com/dragginzgame/shared-tooling/issues/126)).

The incoming direct Host 0.14.0 selection uses required command-completion
status. Both success-only consumer boundaries explicitly use Host's
`require_success` before Wasm publication or report parsing. Four optimizer,
ten report and eight artifact tests pass; the nonzero-exit optimizer and report
controls fail before the policy repair. Testkit 0.33.8 still selects Host 0.13.5
transitively; no dependency override removes its owner's selection
([#307](https://github.com/dragginzgame/icydb/issues/307),
[Host #66](https://github.com/dragginzgame/ic-host-tooling/issues/66)).

Evidence: `target/shared-063-host014`. The first restricted-network Cargo
failure is retained; explicit locked network preparation qualifies the same
unchanged lockfile. User Cargo/governance inputs and the real Git index remain
unchanged. No ICP/PocketIC network is started, stopped or restarted. Native
macOS, full repository/release gates and IC/raw-Wasm measurements remain
unrun; this compatible uncommitted draft is not delivered acceptance.

### Published 0.270.3 Shared 0.6.0 qualification

Adopt committed Shared 0.6.0 revision
`75180980a0c4e4252d8a915aaecfaefd92bf9a4e` from an isolated clean checkout.
Canonical preview/export adds `scripts/dev/cargo-tool-build.sh` and verifies
93 selected files. Cargo tools now use the shared store with exact standard
build identities; the common PATH remains `.tools/rust/common/bin`, and the
existing Testkit adapter already consumes the returned selected path. No
consumer installer, slot parser or build environment is added. Failure
collection's canonical selector now owns local Rust and shared references,
retaining safe local evidence with explicit incompleteness on store refusal
([#302](https://github.com/dragginzgame/icydb/issues/302),
[Shared #120](https://github.com/dragginzgame/shared-tooling/issues/120),
[Shared #121](https://github.com/dragginzgame/shared-tooling/issues/121)).

Sixteen focused fixture invocations pass on Linux Bash 5/Make 4.3 and genuine
Bash 3.2.57/Make 3.81: shared Cargo, Make commands, host/IC store and collector
fixtures, plus consumer Testkit, workstation, validation adapters and workflow
admission. These substitute downloads/builds and do not qualify native macOS.
Snapshot, selected ShellCheck, workflow lint, documentation links and formatting
checks pass.

Actual offline common-tool admission refuses the old local selections, as
required by the hard cut. A separate guarded isolated common-setup probe
reproduces Shared #120 on both shells: a held shared lock and refused activation
are concealed by a success return. The test guard blocks the attempted root
activation path; the prior common selection and foreign lock remain unchanged.
At that handoff, explicit common/selected setup was unqualified until the
canonical propagation defect was corrected. Evidence: `target/shared-060-adoption`.
No live store lock is removed, no old installation is copied or used as a
fallback, and no PocketIC network is started. Library APIs, runtime/storage
formats, dependency pins and optimizer selection are unchanged; this remains
compatible tooling work in the pending patch.

### Earlier 0.270.3 Shared 0.5.0 qualification

Adopt committed Shared 0.5.0 revision
`a306180ce534227fcb98b882fba9a526d5d747e0` from an isolated clean checkout.
Canonical preview identifies the new `scripts/dev/tool-store.sh` companion;
the completed refresh records 92 selected files. Shared's host/IC installers
and evidence collector own per-user bundle storage and checkout references.
Each repository retains its pin selection; previous physical installations
are preserved. Common Cargo slots remain local and project their complete set
through `.tools/rust/common/bin`. CI, workstation, formatting and admission
callers use that current path. No IcyDB cache, installer or link manager is added
([#302](https://github.com/dragginzgame/icydb/issues/302),
[Shared #120](https://github.com/dragginzgame/shared-tooling/issues/120)).

The host/IC storage and evidence fixtures, common/selected Cargo fixture, shared
Make-command fixture and consumer workstation, workflow-admission, Testkit and
adapter fixtures pass on Linux Bash 5/Make 4.3 and genuine Bash 3.2.57/Make 3.81.
Downloads and Cargo installation are substituted inside isolated fixture stores.
The real formatting/hook fixture also passes on both shell/Make selections,
preserving inputs and staging only in disposable repositories.

Actual Linux host/IC setup and offline checks pass through the canonical
per-user store, including IcyDB's unchanged Binaryen 133 raw-byte admission.
Canonical installation prepares the three common Cargo slots; offline checks
pass from the checkout and an outside working directory. The incoming lockfile's
Testkit 0.33.4 CLI is prepared through the same selected installer, and Testkit
admits its existing server installation. The complete `tools-check` passes
offline. No local network is started, stopped or restarted. All 27 fingerprinted
prior local bundle/formatter files, Cargo inputs and the real Git index remain
unchanged. Full evidence selection admits exactly the two shared generations
referenced by this checkout, alongside its retained local evidence; compact
selection verifies both active bundles. An initial selector probe refused a
nonexistent diagnostics directory; the corrected explicit directory passes.
Snapshot, selected ShellCheck, actionlint, workflow policy, documentation links,
formatting and whitespace checks pass. Evidence: `target/shared-050-adoption`.

Contributor setup changes are compatible for IcyDB's unchanged library API,
runtime/storage contracts and supported consumer build requirements, so the
pending notes remain 0.270.3. No Cargo version or dependency selection change
is part of this adoption. PocketIC remains exclusively Testkit-owned. Full
repository/release gates, live IC behavior and native macOS qualification remain
separate; the exact Shared source's native hosted acceptance is pending.

### Current 0.270.2 tooling qualification

The draft now adopts committed Shared 0.3.11 revision
`db143ae9ae140eb66826c413360723d23c078f66`. Canonical refresh from an isolated
clean checkout records the corrected producer version; all 91 selected payload
digests and modes are unchanged. The stronger upstream diagnostic-summary
fixture separates neighboring diagnostic families and passes on Linux Bash 5/
Make 4.3 and genuine Bash 3.2.57/Make 3.81. Prior payload qualification remains
applicable. Snapshot verification, source references and selected documentation
checks pass; Cargo inputs remain unchanged. Evidence is retained under
`target/cleanup-2702-shared0311`. This is a source-metadata correction, not new
runtime or native macOS qualification
([#302](https://github.com/dragginzgame/icydb/issues/302),
[Shared #114](https://github.com/dragginzgame/shared-tooling/issues/114)).

The earlier draft advanced to reviewed shared revision
`37af29b2f8fb4532744b600c5f85ce716a8b57cf`. Its commit is labelled `0.3.10`,
but its committed `VERSION` contains `0.3.9`; that canonical export accurately
records the annotation and exact revision. It excluded the then-uncommitted
Shared 0.3.11 fixture work. The existing 91-file selection includes anchored physical
temporary paths in the release-command checker and the derive sorter's scoped
macro-protection guidance. Formatter pins and unaffected IcyDB macros are
unchanged ([#302](https://github.com/dragginzgame/icydb/issues/302),
[Shared #112](https://github.com/dragginzgame/shared-tooling/issues/112),
[Shared #113](https://github.com/dragginzgame/shared-tooling/issues/113)).

The existing consumer release fixture also anchors its own root and qualifies
the actual Make adapters with relative and aliased temporary parents. Focused
shared host/command fixtures, consumer release/formatting fixtures and the
shared macro-protection hook/downstream compilation fixture pass on Linux
Bash 5/Make 4.3 and genuine Bash 3.2.57/Make 3.81. Downloads and release effects
are substituted; formatter tools are explicitly prepared, and the real hook
preserves staged/working inputs. Original relative-path failure evidence is
retained at `target/cleanup-2702-shared0310` with the passing logs. Snapshot,
selected ShellCheck and documentation checks pass; Cargo inputs remain
unchanged. Native macOS and full repository/release gates remain unrun. There
are no real installation, network lifecycle, commit, push or release effects.

The earlier draft adopted reviewed Shared 0.3.9 revision
`1ed2ca523186641e0e0cdf77e3cb10814c22a430` through the canonical exporter from
an immutable clean checkout. The existing 91-file selection is retained. Rust
tool preflight, installation and offline checks share the consumer directory;
Testkit CLI selection continues to delegate to that owner. Live and retained
validation diagnostics use the shared runner's single predicate. No consumer
wrapper, installer, classifier or infrastructure policy is added
([#302](https://github.com/dragginzgame/icydb/issues/302),
[Shared #101](https://github.com/dragginzgame/shared-tooling/issues/101),
[Shared #110](https://github.com/dragginzgame/shared-tooling/issues/110)).

The canonical Rust-tool fixture, consumer workstation/Testkit/adapter fixtures
and upstream validation-runner fixture pass on Linux Bash 5/Make 4.3 and genuine
Bash 3.2.57/Make 3.81. The adapter also proves early fatal diagnostics survive a
long ordinary tail into its retained error log and GitHub summary. Real offline
Rust/Testkit CLI checks pass from outside the repository; complete offline tool
admission, selected ShellCheck, snapshot and documentation checks pass. Cargo
inputs remain unchanged. Evidence is retained at `target/cleanup-2702-shared039`.
No network lifecycle, installation, commit, push or release occurred. This
qualifies the tooling adoption, not new runtime dependencies, native macOS or
full repository/release gates; those broader qualifications remain separate.

After the reported 0.270.1 publication, continued #311 qualification reproduces
closed jobserver descriptors at the actual `version` recipe under parallel Make.
Metadata and release receipt callbacks now retain descriptors and consistently
inherit the existing repository-local Cargo home and target directory. Shared
Make-mode admission remains authoritative; no wrapper or flag parser is added.

The existing adapter fixture follows real version/pin admission, formatting,
note finalization and preparation rollback, substituting Cargo, version mutation
and candidate receipt effects. Early failures stop before later calls;
metadata-stage failure restores the original notes after finalization. The
existing receipt fixture follows real committed-source/tag receipt checks with
Git observations and Cargo substituted, preserves exact selected commit identity
and retains existing receipts when Cargo admission fails. No Git commit/tag/push,
version bump, publication or server lifecycle occurs.

Linux Bash 5/GNU Make 4.3 and genuine Bash 3.2.57/GNU Make 3.81 pass the adapter,
standard-release and receipt fixtures. Evidence is retained at
`target/cleanup-2702-metadata-jobserver`; the pre-repair descriptor failure is
`/tmp/icydb-shared-adapters.Tf5T54/native/version.log`. Failed harness setup
attempts remain separately retained. These substitute checks do not qualify the
incoming Host 0.12.8/Memory 0.35.4 lockfile, native macOS or delivered source;
full product/release gates remain user-owned. Other installation/publication and
compiled invariant handoffs remain separate #311 follow-up families.

The next focused #311 batch repairs nine installation/package/publication and
compiled-invariant recipes. Actual CLI/workstation and package/publication Make
probes substitute tools and Git observations, checking live descriptors and
existing caller/workspace Cargo path ownership. Controlled Cargo failures
qualify the scanner build, dependency-graph admission and runtime scanner through
their actual aggregate Make targets; unrelated gates are substituted and no
commit-producing receipt body executes. The receipt fixture's recipe also
retains descriptors, while its normal commit-producing body remains user-owned.

Published 0.270.1 [CI 38069259302](https://github.com/dragginzgame/icydb/actions/runs/38069259302)
passes product/core/workspace, MSRV, Tier A/B and Wasm-size lanes, but both native
macOS portable jobs fail at the adapter fixture's assumed unset thread cap.
`RUST_TEST_THREADS=2` reproduces the same trace mismatch locally. The fixture
now selects its own baseline and separately verifies inherited cap forwarding
versus explicit core/workspace lane caps; production test policy is unchanged.

The three extended fixtures and 126-case completion matrix pass on Linux
Bash 5/Make 4.3 and genuine Bash 3.2.57/Make 3.81, including CI's thread setting.
All 18 IcyDB-owned shell fixture handlers are covered by the existing completion
matrix; canonical shared fixtures retain upstream ownership. Native macOS and
delivered pending-source acceptance remain open. Initial setup failures remain
separate from passing qualification, including a recipe-shell substitution
failure refused by sandboxed sudo without installation and a missing substitute
for snapshot verification. Evidence: `target/cleanup-2702-final-handoffs`.

The incoming locked Host 0.12.8/Memory 0.35.4/Timers 0.17.5 graph needed an
explicit fetch after offline admission refused uncached Memory. Its integration
library passes 25 tests and selected strict library/test Clippy passes. Manifest
and lockfile hashes remain unchanged. This native qualification does not by
itself qualify IC memory/timer lifecycle behavior or a full repository gate.

The same locked graph also passes all three logical-memory init/upgrade/recovery
tests and the timer trap/upgrade/re-registration regression using the server
admitted by Testkit's offline check. Testkit owns the four local instance
lifecycles; no PocketIC process remains afterwards. Instruction-exhaustion and
timer-coalescing tests are outside this selected check. Raw Wasm/cycle/instruction
deltas remain unmeasured; full workspace/release gates and current native macOS
remain unrun locally.

### Current 0.270.1 tooling qualification

The initial Shared 0.3.7 adoption selected committed
`34e5ad7aac3599306c9572bb547f2239d09df1a3`. Its 90-file export includes strict
Git-observation failure handling, portable assertion repairs and the canonical
lockfile-selected Cargo installer. The local Testkit runner delegates package,
source and version admission through `--lockfile`; it no longer resolves Cargo
metadata or parses a version independently. Testkit retains server ownership.
The maintainer's incoming lockfile already selects Host 0.12.6; its Rust sources
are unchanged from 0.12.5. No dependency or package versions are changed here.

At that adoption, `ci/ic-tools.tsv` temporarily remained the sole consumer-owned
archive matrix outside the snapshot, as permitted by Shared's IC adoption
contract. Binaryen 132 stayed paired with the then-qualified executable digests
and post-link policy. This admitted current canonical setup without inventing a second
installer, a second matrix or a compatibility route. Earlier qualification
records below retain their original source identities.

Focused qualification is retained in `target/cleanup-2701-shared037`: canonical
selected-Cargo fixtures and actual consumer setup, Testkit, formatting/hook,
adapter and workflow-policy fixtures pass on Linux Bash 5 and Bash 3.2/Make 3.81.
Completion/retention controls pass 126 cases per profile. The initially missing
Testkit 0.33 CLI slot refuses with its selected identity and setup handoff;
explicit CLI preparation and separate offline admission then pass on both
profiles. Complete offline tool admission also passes. All 66 published Host
0.12.6 Rust files match their reviewed owner; twelve focused artifact/optimizer
tests and strict integration-library Clippy pass. Snapshot, workflow lint,
selected ShellCheck, documentation and diff checks pass. Cargo input hashes are
unchanged. No real server, network lifecycle or release effect was invoked.
This is native tooling qualification, not Binaryen 133 product or native macOS
qualification; full repository/release gates remain user-owned.

The maintainer subsequently authorized Binaryen 133. The canonical exporter
restores `ci/ic-tools.tsv` to the same reviewed Shared 0.3.7 snapshot, now with
91 selected files. All three official release archives match the shared pins;
their extracted executable digests replace the consumer admission table.
The only current pipeline is Binaryen 133 with the existing ordered flags and
feature policy. Its identity changes with the optimizer, so retained post-link
cache entries cannot satisfy the new recipe. Setup uses the shared installer;
previous bundles and failed artifacts remain retained. No fallback, second
installer or new persisted format is added. Run `make install-ic-tools` before
offline checks. Product/native qualification remains tracked by
[#302](https://github.com/dragginzgame/icydb/issues/302); evidence is retained in
`target/binaryen-133-adoption`. Linux setup, offline admission and download-disabled
reuse pass on Bash 5 and Bash 3.2/Make 3.81; the previous bundle's byte manifest
still verifies. Twelve artifact/optimizer helper tests, the focused canister
post-link test, strict library/test/report Clippy, report/admission fixtures,
ShellCheck, snapshot, documentation and diff checks pass. The production SQL
fixture also builds and passes actual artifact/report admission with the maintained
feature set. Testkit owns the two local canister-test lifecycles; no deployment
or release occurs. The incoming Host 0.12.7 lockfile is preserved.

The same compiler-emitted SQL fixture has SHA-256
`7d312a6a5c62cf3b0a8403209de5b5fb796dd643b0fdb9152175407ca0ae6e2a`.
Its 132/133 local-profile outputs are 4,478,088/4,477,627 raw bytes (delta -461).
Measured ordered-query instructions are 2,932,746/2,932,656 (delta -90);
schema exact-reentry instructions are 45,005,758/44,938,916 (delta -66,842).
Both variants preserve query results through initialization and upgrade; the
133 transform is deterministic. These are matched fixture measurements, not a
fleet-wide performance claim. Cycles remain unmeasured. Native macOS execution,
the full artifact matrix and repository/release gates remain unqualified.

The maintainer requested continued error cleanup after 0.270.0 went live.
Canonical Shared 0.3.3 `d63f0cfaba8ab2961d6012064adbf051c1898bc1` refreshes
91 selected files from an immutable clean clone, including the README task
required by the maintenance catalog. The committed production validation runner
now rejects malformed inherited nesting depth before log creation or dispatch
and requires explicit completion before success. Shared remains the sole depth
admission owner; the IcyDB adapter selects its repository and log root. Sibling
uncommitted changes remain excluded. The pending compatible batch changes no Cargo package
versions, tool pins, server policy or runtime storage contract.

The actual released Testkit fixture reproduces a nounset error returning zero
under Bash 3.2. Seventeen maintained consumer tooling/release fixture owners now
require explicit completion and retain incomplete evidence; the new fault
injection regression also protects itself. That boolean is transient validation
state at the existing fixture owner, not a production mode or persisted format.
Exit status alone is the simpler alternative and is disproved by the retained
reproduction. The maintained failure matrix has 119 cases per shell profile,
including Cargo adoption, Shared adapters, release locks, pin preparation,
publication, release notes, invariant scanners, read admission and Wasm reports.
Remaining independent fixture families stay with
[#342](https://github.com/dragginzgame/icydb/issues/342).

IcyDB-owned formatter, selected Testkit setup/check, native core/workspace/
Tier A CI, local unit/durability/integration tests, build/report, development,
documentation, feature, managed Tier B and native Tier C recipes preserve Cargo's
jobserver descriptors through recursive
recipe marking. Actual parallel recipe
probes exercise live readable/writable descriptors; the disposable formatter
control without that marking fails. The existing canonical execution guard
continues to reject unsafe Make modes. Other direct Cargo recipe families and
native consumer qualification remain with
[#311](https://github.com/dragginzgame/icydb/issues/311).

Consumer setup, Testkit, workflow, formatting/hook, release-adapter and receipt
fixtures pass on Linux Bash 5 and genuine Bash 3.2/Make 3.81. Focused native
integration compilation passes with the incoming Host 0.12.2, Memory 0.35.1,
Metrics 0.5.1, Timers 0.17.1 and Testkit 0.32.0 graph. The maintainer then
selected Memory 0.35.2 during wrapper qualification. The initial uncached-crate
refusals occurred before server launch; explicit locked fetch prepares that graph
without changing its selection. Both input captures and attempts are retained.
The later native adapter check and both real-CLI/substitute-server wrapper
profiles pass with Memory 0.35.2. Each wrapper profile completes five cases;
completion qualification passes 48 cases per profile. Actual parallel Linux
setup and separate offline checks pass, including offline admission under
Bash 3.2/Make 3.81. Canonical host/retention and selected Rust-tool fixtures,
consumer saved-release recovery, selected ShellCheck, actionlint, snapshot and
documentation checks pass. No real network was started; wrapper substitutes
own harmless sleeping processes and qualify their cleanup. Latest captured
manifest/lock hashes remain unchanged by final checks.
This is native adapter/tooling qualification, not IC init/upgrade or native macOS
qualification. Earlier evidence: `target/cleanup-2701`.
Committed Shared 0.3.3's canonical validation-runner fixture and the actual IcyDB
adapter pass on both shell profiles, including malformed-depth refusal before
dispatch and repository/log selection from another working directory. Normal
Cargo-adoption, release-lock and pin-preparation fixtures pass alongside all
72 completion/retention cases per profile. The initial metadata attempts omitted
Cargo from the qualification PATH and refused discovery; both retained fixtures
precede successful checks with the prepared toolchain. The new evidence is in
`target/cleanup-2701-shared033`. Its captured Cargo inputs remain unchanged.

The subsequent maintainer-requested review checks committed Shared 0.3.4
`169d77b8440568c5200eede971625126181f7bb2` and Host 0.12.3
`aec863191b3c96ef879e59af701cbe1451595f25`. Shared's changed common selection is
Binaryen 133; IcyDB still admits the qualified 132 executable bytes and pipeline.
Keep the current 0.3.3 snapshot until actual post-link/bundle/startup qualification
can update the matrix and product policy together. The new shared synthetic
smoke does not establish that product evidence or native macOS acceptance.
No second matrix, compatibility path or local installer is added; the existing
adoption boundary remains with
[#302](https://github.com/dragginzgame/icydb/issues/302).

Published Host 0.12.3 matches all 66 Rust sources in the reviewed sibling; those
sources are unchanged from 0.12.2. Existing IcyDB artifact publication and process
admission already consume the Host owners. The incoming graph has Host 0.12.3,
Memory 0.35.2, Metrics 0.5.2, Timers 0.17.2 and Testkit 0.32.1. Initial offline
compilation refused the uncached Timers selection before compiling; explicit
locked fetch prepares the graph without changing it. Focused native integration
compilation, four optimizer checks and eight artifact checks then pass.

Five more fixture owners now protect completion and retained evidence. A new
diagnostic-reader failure control reproduces report cleanup replacing the original
status 23 with status 7; best-effort replay now preserves the original status
and retained fixture. All 119
actual-handler cases, normal publication/finalization/read-admission fixtures,
59 real scanner cases and 32 Wasm-report cases pass on both shell profiles.
Selected ShellCheck annotations identify literal child-shell programs and
Markdown delimiters. This is native/tooling qualification, including the current
132 optimizer, not a 133 canister or new Testkit lifecycle qualification.
Evidence: `target/cleanup-2701-host0123`. Captured Cargo inputs remain unchanged;
no real network was started, release performed or sibling files edited.
The subsequent native CI handoff repair qualifies all twelve Cargo calls across
the actual core, workspace and Tier A entrypoints using harmless Cargo
substitutes. The pre-fix actual Makefile control fails on advertised but closed
descriptors and remains retained. Recursive marking repairs that transport;
the canonical shared Make admission remains the only execution-mode owner.
Both shell profiles pass descriptor, cache-root, libtest-concurrency, failure/log
retention and four unsafe-mode controls. This executes no product CI gate or
workspace compilation. Completion qualification still passes 119 cases per
profile; workflow policy, selected ShellCheck, documentation, snapshot and diff
checks pass. Evidence: `target/cleanup-2701-native-jobserver`. Cargo.toml stayed
unchanged, but the maintainer's lockfile updates arrived during these checks:
Host 0.12.4, Memory 0.35.3, Metrics 0.5.3 and Testkit 0.32.2. Preserve those
selections and both input captures. Transport fixtures substitute Cargo and do
not qualify that new runtime graph; other direct Cargo families stay with #311.
The next local test-family repair marks eleven Cargo recipes and exercises
thirteen handoffs through unit, no-default smoke, durability, feedback and SQL
matrix entrypoints. Together with native CI, the existing fixture qualifies
25 handoffs per shell profile without running product tests. Six server-using
calls preserve the caller's explicit path and consumer temp root; Testkit retains
runtime admission and lifecycle authority. Failed offline admission and missing
feedback selection refuse before Cargo. Both profiles also qualify nested local
failure retention and unsafe-mode refusal, with the same 119-case completion
matrix and workflow-policy controls. Selected ShellCheck, snapshot, documentation
and diff checks pass. Evidence: `target/cleanup-2701-local-jobserver`. Cargo inputs
remain unchanged in this repair. This is transport qualification, not dependency
compilation, server admission or runtime lifecycle qualification.
The next repair marks 30 build/report, development, documentation, feature,
Tier C and managed Tier B recipes. The existing actual-Make fixture now qualifies
53 successful Cargo handoffs plus two controlled report-builder failures per
profile, without compiling or running product gates. It preserves builder
profiles/canister selections, inherited cache roots and bounded test concurrency.
The real report shell chain reaches Cargo with live descriptors; controlled
builder failure returns through Make and preserves existing artifact bytes.
Tier C clears stale receipts, requires newly emitted receipts and preserves the
exact replay selection and single-thread option. Empty shard/replay selections
refuse before Cargo. Tier B substitutes only the existing managed-server wrapper
boundary; its actual outer Make and nested validation dispatch remain in use.
Unsafe-mode controls cover the newly qualified outer entrypoint families.
The first Tier B fixture attempt lacked the temp directory normally created by
setup; canonical nested Make admission correctly refused it. Preparing that
fixture prerequisite allows both profiles to pass. Earlier incorrect multiline
recipe marking attempts are retained separately from corrected passing checks.
No production temp-directory policy changed.

The remaining local release-candidate receipt handler now requires explicit
completion and retains failed evidence. Its pre-fix Bash 3.2 nounset control
returns zero; the corrected actual-handler matrix passes 126 cases per profile.
That injection stops immediately after trap installation, before Git effects.
The normal receipt fixture creates Git commits and remains unrun under the
maintainer's no-commit rule. Existing completed-success cleanup and genuine
nonzero status preservation are qualified by the actual-handler controls.
The workflow fixture's native-server mutation no longer matched recursive
recipes. Updating it exposes a further Bash 3.2 false-success boundary: a failed
assertion inside the helper's conditional branch can continue and count the case.
The helper now explicitly exits on a policy mismatch; a current-baseline
unexpected-acceptance control proves rejection before the normal mutation matrix.
All 54 workflow cases and that assertion control pass on both profiles.
Focused ShellCheck, workflow policy, snapshot, documentation and diff checks
pass. Evidence: `target/cleanup-2701-build-jobserver`. Cargo input fingerprints
remain unchanged. Remaining indirect Cargo/release metadata recipe families,
delivered-source and native macOS acceptance stay with #311/#342.
Full workspace/release gates and native macOS remain user-owned. Wasm, cycle
and instruction deltas are unmeasured for this tooling-only batch.

### Current 0.270 hard-cut qualification

The maintainer authorized the Memory 0.34 hard cut and carrying the unpublished
0.269.2 batch into 0.270. [The current tracker](../design/0.270-memory-pool/0.270-status.md)
and [#338](https://github.com/dragginzgame/icydb/issues/338) own the bounded scope.
Published Memory 0.34.0's archive and 46 Rust sources match revision
`958080df899ebfa7bb9c2d4c93bb8664bd23575d`. One checked host pool replaces
component ranges; committed key identities, geometry and recovery records remain
unchanged. Incoming Host 0.11 uses per-stream output limits and typed cleanup;
The later incoming Testkit 0.29 selection consolidates its native helpers onto
Host 0.11, removing the earlier split generation. The final graph has Testkit
0.29.0, Timers 0.16.7 and stable structures 0.7.2, each with one identity. The
later incoming Memory 0.34.1 and Metrics 0.4.0 archives retain all 46 and seven
Rust library files respectively from reviewed 0.34.0 and 0.3.7. Timers library
sources are unchanged from 0.16.5.

Canonical Shared 0.3.0 export covers 90 recorded selections at committed
`88a73139a0f083344c41a6f6f4b5c3a8aca7dc1d`. This authorized extension of the
pending 0.270 tooling slice follows
[Shared #98](https://github.com/dragginzgame/shared-tooling/issues/98) and
[IcyDB #302](https://github.com/dragginzgame/icydb/issues/302).
Shared owns complete host → IC → Rust orchestration; IcyDB registers its existing
Testkit targets through the ordered product lists. The redundant aggregate
prerequisites and product target's duplicate host setup are removed. Complete
pins, installers and setup guidance refresh together. Native-only CI clears
only the product extension lists, retaining complete common setup and checking;
live lanes retain Testkit server setup. The shared failure collector retains
aggregate logs, failed candidates, Rust builds and formatting diagnostics.
No new tool pin, PocketIC policy, compatibility path or persisted state is added.

Focused consumer setup, selected-Testkit, release-preflight, saved-release
recovery and 54-case workflow fixtures pass on Linux Bash 5 and genuine Bash
3.2/Make 3.81. Canonical complete-host, parallel aggregate and actual failure
collector fixtures pass on both profiles; snapshot integrity, selected
ShellCheck, workflow lint and documentation links pass. Actual Linux setup
reuses the common bundles and admits the incoming locked Testkit 0.30 selection;
a separate offline check passes. The first setup's uncached locked dependency
refusal is retained, followed by explicit locked fetch and successful setup.
Cargo.toml/Cargo.lock hashes remain unchanged. No network lifecycle operation
or release occurred. Evidence: `target/shared-030-adoption`. This tooling check
does not establish runtime qualification for the incoming Testkit 0.30 graph.
Native macOS and full workspace/release validation remain user-owned.

The snapshot carries reviewed 0.2.13 path/tool admission repairs and 0.2.14's
CI inspection repair: unavailable failed-step logs are reported and retained;
empty logs do not establish a diagnosis for a failed run. The earlier focused
CI inspection fixture profiles pass.
The path comparison reproduces the old trimmed-root acceptance and verifies
LF/CR and resolved-alias refusal with ordinary-path controls, without modifying
fixtures. Snapshot and selected-Rust-tool fixtures pass on Linux Bash 5 and
genuine Bash 3.2/Make 3.81. Consumer release/setup fixtures qualify selected
Testkit preparation, explicit offline behavior and parallel early refusal.
Release fixtures substitute effectful commands; they perform no release.
Saved-release recovery also passes with substitute Git effects on both profiles.
Focused native bootstrap/admission, generated store/startup, allocation reporting,
CLI and carried SQL regressions pass. Strict selected Clippy, renamed-facade
Wasm installation and five Testkit-owned IC init/upgrade/recovery cases pass;
the three logical-memory cases also pass with the incoming Testkit 0.29 selection.
The later Memory 0.34.1/Metrics 0.4.0 graph passes strict Clippy, all 15 native
admission cases and all three logical-memory IC cases. Timers 0.16.6 still
requires Metrics 0.3, retaining a second 0.3.7 identity beside root 0.4.0;
[Timers #37](https://github.com/dragginzgame/ic-timers/issues/37) owns alignment.
Timers 0.16.7 retains all 15 library files from 0.16.6; the current graph passes
strict Clippy and a focused startup trap/upgrade case. Six successful managed
scopes stopped through Testkit. The initial sandbox
loopback refusal and omitted metrics feature failure are retained as corrected
qualification attempts. Evidence: `target/memory-034-audit`.

The maintainer's explicit focused-check-only instruction is the scoped exception
to the refreshed baseline's standing broad-validation authority. Full workspace
and release gates remain user-owned. Linux Bash 3.2 does not qualify native
macOS; declared native hosts remain required. MSRV 1.96 is not installed locally.
External composed-host qualification remains at
[Memory #44](https://github.com/dragginzgame/ic-memory/issues/44).
Matched raw Wasm, IC-cycle and instruction deltas are unmeasured.

### Carried 0.269.2 Host and Timers qualification

The earlier incoming lock selected one published Host 0.10.2 generation, Timers 0.16.5
and Testkit 0.28.1. All six registry archives match the selected checksums and
their committed source (121 Rust source files). Timers' library sources are
unchanged from 0.16.2; no timer adapter hard cut is needed. Host's pathname
directory-suffix correction does not replace IcyDB's admission before splitting
a descriptor-relative diagnostic target. Retain that existing parent and
create-only policy ([#307](https://github.com/dragginzgame/icydb/issues/307)).

Prepare the locked Testkit CLI through the shared installer, then admit it
offline. The existing managed wrapper selects both operation-idle and hard
lifetimes through Testkit's published CLI. This fixes pre-client compilation
losing its server under the idle default; no heartbeat, installer, version
checker or application supervisor is added
([#340](https://github.com/dragginzgame/icydb/issues/340)). Native Tier C scenarios
use independent existing request roots, retaining each mutation sequence's
complete setup/write/readback flow and shared schema/cache authority
([#341](https://github.com/dragginzgame/icydb/issues/341)).

Focused qualification passes 22 CLI/artifact/optimizer tests, five real-CLI
substitute-server wrapper cases on both Linux Bash/Make profiles, native shards
4 and 0 with complete clean receipts, DISTINCT oracle and request-budget
controls, selected strict core/CLI/integration Clippy and two timer/generated
actor Wasm checks. Source/checksum and lock preservation evidence is retained
under `target/host-timers-2692`. No live server/network is provisioned or started.
Full workspace/release gates, live Tier B and native macOS remain user-owned;
raw Wasm bytes, IC cycles and instructions are unmeasured.

### Prior 0.269.1 Host qualification

The earlier qualification selected published Host 0.10.0 with Testkit 0.28.0.
IcyDB's artifact copier uses the consolidated streamed writer with explicit
replace/0o666 staging options and still copies the source permissions before
publication. Its Candid and artifact diagnostics retain the complete publication
error, including cleanup failures. This is host-tooling adoption without an
IcyDB public API or retained-format change
([#307](https://github.com/dragginzgame/icydb/issues/307)). That qualification excluded
Host's then-uncommitted 0.10.1 parent-sync repair. Explicit `make install-testkit` prepares
the newly locked CLI; offline validation only checks the admitted installation.

Focused Linux qualification passes strict integration-library/binary and CLI
Clippy, twenty-four artifact/optimizer/post-link/CLI/report tests, selected shell
and documentation checks, and offline tool admission. Setup and locked-Testkit
caller fixtures pass with Bash 5/Make 4.3 and Bash 3.2/Make 3.81 after carrying
the two previously omitted shared Make includes. One actual startup-adapter
test passes through the managed Bash 3.2 wrapper; Testkit starts and stops its
server. Evidence is retained under `target/host010-adoption`. Native macOS and
full workspace/release gates remain user-owned. Raw Wasm, cycles and instructions
are unmeasured; no function, method or type is removed.

### Carried 0.269.2 formatting follow-up

The earlier snapshot adopted committed Shared 0.2.11
`83efac446348dea024798a331d77933b24b429dc` through the canonical exporter.
Its 87-file selection adds the shared execution include and formatting reporter;
central sibling reports remain outside this consumer. Shared Make now owns
parse-time execution admission, including cleared/replaced Make flags. IcyDB's
supplementary flag parser is removed. Standard release routing remains shared.

IcyDB's custom formatting recipe runs derive sorting, manifest sorting and
rustfmt in that order, after canonical tool admission. Wrap the complete sequence
once with the shared reporter rather than the simple-workspace formatting include.
The two separate derive prerequisite targets are removed. Existing Cargo
directories, offline operation, package roster and check flags are preserved.
Successful formatting prints one line; a failed formatter stops the sequence
and retains complete stdout/stderr in `RUNNER_TEMP/formatting.*` (or TMPDIR or
/tmp locally). Static and native host CI upload those failed logs through the
existing artifact action. Release, hook, setup and Testkit fixtures carry the
new shared companions
([#336](https://github.com/dragginzgame/icydb/issues/336),
[Shared #30](https://github.com/dragginzgame/shared-tooling/issues/30),
[Shared #92](https://github.com/dragginzgame/shared-tooling/issues/92)).

Focused consumer checks pass on Linux Bash 5 and genuine Bash 3.2/GNU Make
3.81: actual hooks and derive sorting, one-line success, exact failure status
and complete retained stdout/stderr, formatter ordering, parallel dispatch,
unsafe modes with cleared/replaced flags, and unrelated/index/lock preservation.
Actual prepared check-only formatting, release/setup/Testkit substitute fixtures,
the canonical formatter fixture, selected ShellCheck/workflow policy, snapshot
integrity and local documentation links pass. Evidence: `target/issue-336`.
Native macOS consumer qualification and full workspace/release gates remain
user-owned; raw Wasm, cycles and instructions are unmeasured. No actual release,
network lifecycle action or Cargo version change ran.

### Published 0.269.1 snapshot evidence

The previous snapshot adopted committed Shared 0.2.6
`ce13a5314916891fd239d9b199b4a91b04775054` through its canonical exporter.
The eighty-three existing selections remain, with only
[the release include](../../make/release.mk) and
[the formatting include](../../make/rust-format.mk) added. Hook-path repairs,
export-companion admission and the Apple Make formatter-fixture correction
come from committed source; newer dirty sibling changes are excluded
([#309](https://github.com/dragginzgame/icydb/issues/309),
[Shared #89](https://github.com/dragginzgame/shared-tooling/issues/89),
[Shared #73](https://github.com/dragginzgame/shared-tooling/issues/73),
[Shared #85](https://github.com/dragginzgame/shared-tooling/issues/85)).

That adoption put patch/minor/major/resume routing, conflicting release-goal
admission, manifest sorting and rustfmt under Shared Make. IcyDB retained its
parse-time execution refusal, release adapters, receipt checks, preflight
preparation, derive sorter and repo-local Cargo directories. Its derive
prerequisites ran after tool admission and before the shared recipe, with
rustfmt last. Missing tools and failed derive sorting stopped later formatting,
including parallel Make. Existing release, hook, setup and Testkit fixtures
carried those includes
([Shared #91](https://github.com/dragginzgame/shared-tooling/issues/91),
[Shared #92](https://github.com/dragginzgame/shared-tooling/issues/92)).

Focused release/format adoption checks pass on Linux Bash 5 and genuine Bash
3.2. Release fixtures substitute Git/runner effects; formatting fixtures exercise
real tool execution, derive ordering, dependency sorting, staged-file/index
preservation, unusual selected paths, parallel dispatch and failed derive
prerequisites. Canonical formatting-owner fixtures, snapshot integrity,
selected shell/workflow checks and documentation references pass. Evidence:
`target/shared-make-2691`. Native macOS and full workspace/release gates remain
separate user-owned checks. No actual release or network lifecycle action ran.

Static CI prepares and checks the locked Testkit CLI without downloading its
official server. Its real CLI/substitute-server wrapper fixtures remain active.
Rust matrix lanes retain shared host, IC and formatting tool admission; only
Tier B prepares Testkit's real server. Native workspace and Tier A Make bodies
no longer resolve a server before their native tests. Native macOS setup and
live startup qualification remain required. The CI prerequisite change adds no
installation mode, server selector or supervisor
([#334](https://github.com/dragginzgame/icydb/issues/334)).

Initial CI prerequisite qualification against Shared 0.2.5 passes workflow
policy and its 51 fixture cases, native
caller/refusal fixtures and the five real CLI/substitute-server wrapper cases
on Bash 5 and genuine Bash 3.2. Actual locked Testkit 0.27.1 installation and
offline CLI admission pass; one Host 0.9.3 generation and Timers 0.16.2 remain
selected in the incoming lockfile. The official server bundle was not
provisioned or launched. Consumer real formatting, canonical Shared hook
fixtures, snapshot verification, workflow lint and selected ShellCheck pass.
Evidence is retained in `target/ci-cleanup-2691`. Native macOS, full workspace
and release gates remain user-owned qualification; raw Wasm, IC-cycle and
instruction deltas are unmeasured. No function, method or type is removed.
Initial CI prerequisite complexity: twenty-two files, approximately 225 net added lines, chiefly shared
documentation and fixture coverage. Server prerequisites are simpler; the
existing installer, CLI and policy owners remain authoritative.

### Published 0.269.0 adoption

The tooling follow-up, carried into the authorized 0.269 batch, adopts Shared 0.2.2
`ee48bb37c98c771e77b92fd891f0757d8c1c8b99` through its canonical exporter,
retaining the same eighty-three-file selection. CI tool installation now
publishes to the exact executable path: a destination that becomes a directory
cannot redirect publication into its child. Failed publication retains the
candidate and existing destination
([Shared #88](https://github.com/dragginzgame/shared-tooling/issues/88),
[#309](https://github.com/dragginzgame/icydb/issues/309)). The shared owner
fixture qualifies this boundary; IcyDB retains its existing caller fixtures
without copying another installer test suite.

The maintainer's selected graph converges on Host 0.9.2, Testkit 0.27.0,
Memory 0.33.0, Metrics 0.3.1 and Timers 0.16.0. Testkit's duplicate Host 0.8
generation is gone. Its startup error record is already consumed through
Display, so no IcyDB error adapter is required
([#307](https://github.com/dragginzgame/icydb/issues/307),
[Testkit #30](https://github.com/dragginzgame/ic-testkit/issues/30)). Host and
Memory retain the runtime APIs used here; Metrics and Timers agree on the
Metrics 0.3 measurement owner. Sibling uncommitted release work is excluded.

Focused Linux qualification passes canonical CI installer fixtures and existing
consumer adapters on Bash 5/3.2, locked Testkit selection/refusal fixtures,
all five wrapper cases, strict integration/CLI Clippy, artifact and optimizer
checks, post-link retention, public Memory bootstrap boundaries and Metrics
report rendering. Actual Testkit 0.27 setup/check passes, including admission
with an empty PATH. IcyDB's actual startup adapter passes directly and through
the managed Bash 3.2 wrapper. Testkit started/stopped both local servers; none
remain running. Logs are under `target/packages-2681`. The maintainer's Cargo
files are unchanged, with lock SHA-256
`56f6c5a5ed4068d04273a4013ce7a7a9ffd88029cb51d848b8a77ccd6d5455c2`.

[Published Testkit 0.27 CI](https://github.com/dragginzgame/ic-testkit/actions/runs/37923315911)
passes Linux checks, MSRV, portable-host and concurrency jobs; its native macOS
jobs remain queued at inspection.
[Shared 0.2.2 CI](https://github.com/dragginzgame/shared-tooling/actions/runs/37918955655)
and [published IcyDB 0.268.0 CI](https://github.com/dragginzgame/icydb/actions/runs/37923831876)
remain queued. These are committed-source observations, not acceptance of this
uncommitted follow-up. Native macOS and full workspace/release gates remain
open or user-owned. Raw Wasm, IC cycles and instruction deltas are unmeasured.
No functions, methods or types were removed; owner reuse adds no local engine.

### Published 0.268.0 adoption

The maintainer selected 0.268 on 2026-10-09 for the PocketIC ownership hard cut,
carrying the unpublished 0.267.5 fixes. The
[line tracker](../design/0.268-testkit-tooling/0.268-status.md) groups the three
independently reviewable outcomes. The canonical exporter refreshes the same
eighty-three-file selection from clean committed Shared 0.2.1
`06b2e22f6bd213f1a590eb2a8797aee34c42dd69`. The 0.2.0 hard cut retires PocketIC
from the common bundle; 0.2.1 also fixes installation/offline admission of the
last pin record without a final newline
([Shared #87](https://github.com/dragginzgame/shared-tooling/issues/87)).
The retired PocketIC helpers were already
unselected here; no second server catalog, installer or compatibility policy
replaces them ([#327](https://github.com/dragginzgame/icydb/issues/327),
[Shared #76](https://github.com/dragginzgame/shared-tooling/issues/76)).

`make fetch install-tools tools-check` prepares the locked Testkit CLI through
the canonical selected Cargo installer, provisions its server explicitly and
checks both owners offline. `install-testkit` replaces `install-pocketic-runner`;
`testkit-check` prints the admitted server path. Make's ordinary test lanes refuse
failed admission before invoking Cargo. The existing managed wrapper uses the
same CLI, retaining signals, full failure logs and its single-server topology.
External server overrides remain under Testkit's runtime admission. Workstation
and CI preparation use these same targets. Tests never download.

The local closeout removes Make's redundant server-path admission before the
managed Tier B wrapper: Testkit now checks that selection once in its managed
run contract. An empty override selects the prepared owner bundle. The wrapper
still owns IcyDB's signal relay and failed-output retention. Consumer fixtures
retain locked selection and caller refusal checks; shared-bundle preservation
stays with the canonical Shared fixtures and actual retained-byte evidence.
The existing five-case wrapper fixture also exercises Make's managed handoff.
These fixtures pass on Bash 5/3.2, alongside workflow policy/all forty-one cases,
selected ShellCheck and a real managed Make smoke check using a harmless child.
Testkit started/stopped a local server for that smoke check; no product suite
ran. Evidence is under `target/testkit-local-cleanup`. Incoming Host 0.9 manifest
and lock selections are unchanged, with lock SHA-256
`c78530c77c4aa9eaeb2aacb15aab1e73e707c776015813ee1b7bddc7571cf63b`.
The uncached Host packages initially refused offline CLI selection; explicit
locked fetch prepared them. This is tooling qualification, not Host 0.9 Rust API
or native product qualification. No functions, methods or types were removed.

Actual Linux five-tool installation/check and Testkit CLI/server setup/check
pass. Testkit checks its prepared server with `PATH=/nonexistent`. The old
six-tool bundle remains retained with unchanged receipt-covered files, pins and
receipt. Canonical IC, consumer selection/refusal, workstation and five-case
wrapper fixtures pass on Bash 5 and genuine Linux Bash 3.2; substitute fixture
passes are separate from actual setup and product startup evidence. Logs remain
under `target/shared-hardcut-020`.
The actual IcyDB startup adapter also passes directly and through the maintained
managed wrapper under Bash 3.2, exercising canister creation and cycle-balance
updates. Local PocketIC servers were started for these focused checks; Testkit
owns their shutdown. All forty-one workflow fixtures, workflow policy, actual
`make tools-check`, selected ShellCheck, snapshot and local documentation checks
pass. The initially uncached maintainer-updated Metrics selection refused offline
metadata before installation; explicit `make fetch` prepared the current graph.
Its lockfile remains byte-identical at SHA-256
`0ad8773283bc1e2c5bc012ff6b757fe2374e9b19c5d404198fbe05aa1722ded7`.

Native owner setup/check and actual launch
were established by published Testkit 0.25.4 in
[owner CI](https://github.com/dragginzgame/ic-testkit/actions/runs/37901828971).
Selected published Testkit 0.25.5's CI remains queued at review, as does
[Shared 0.2.1 CI](https://github.com/dragginzgame/shared-tooling/actions/runs/37918240103).
The preceding [Shared 0.2.0 CI](https://github.com/dragginzgame/shared-tooling/actions/runs/37916384666)
passes Linux and lint/security while both macOS jobs remain queued. Refreshed
0.2.1 canonical IC fixtures pass Bash 5/3.2, including the final-record repair,
and IcyDB's actual aggregate offline tool check passes with Bash 3.2.
Matching IcyDB committed/native evidence remains open under #327/#309; Linux
Bash 3.2 does not qualify macOS. Full workspace/release gates and raw
Wasm/IC-cycle/instruction deltas remain unmeasured or user-owned.

The earlier compatible batch, now carried into 0.268.0, adopted Shared Tooling 0.1.38,
`926a20606591214ab29faa236b0b584e4857439e`, from a clean isolated checkout
through the canonical exporter. Its eighty-three-file selection adds the
canonical dependency fixture to the existing portable CI lane. The sibling
checkout's dirty VERSION and retired fleet/PocketIC helpers are excluded.
The old consumer checker reproduces the malformed-exception/trailing-array
bypass; the refreshed checker refuses it and preserves the exception, manifest,
lockfile and index. Shared remains the only exception checker and installer
authority ([Shared #86](https://github.com/dragginzgame/shared-tooling/issues/86),
[Shared #85](https://github.com/dragginzgame/shared-tooling/issues/85)).

Focused Linux qualification passes dependency fixtures on Bash 5 and genuine
Bash 3.2, actual consumer declaration/inheritance checks, selected ShellCheck,
snapshot verification, and real staged formatting. Shared installer fixtures
pass on both Bash versions with substitute Cargo; they do not qualify registry
installation. Canonical hook fixtures also pass. Logs remain under
`target/shared-tooling-038`. The exact-source
[Shared CI](https://github.com/dragginzgame/shared-tooling/actions/runs/37912382208)
passes Linux and lint/security; both native macOS jobs remain queued at review.
Consumer committed/native evidence remains
[#309](https://github.com/dragginzgame/icydb/issues/309).

That earlier review covered published Host 0.8.10
(`e944f114f7542d27ead5df8996d1b2df04e06c31`). Host 0.8.9/0.8.10 change
release preparation and shared tooling without Rust library source changes
from 0.8.8, so this adoption requires no new Host API calls. Host 0.8.9's
full CI passes; exact-source
[Host 0.8.10 CI](https://github.com/dragginzgame/ic-host-tooling/actions/runs/37915010103)
passes Linux and MSRV while both macOS jobs remain queued. The maintainer's
lock selections, Host artifacts/fs 0.8.10, process/tools 0.8.9 and Testkit
0.25.5, remain byte-identical at SHA-256
`a6a4db77c56f20ed02e7d4de60b9f052d9ccedf0153b31db2b0d5742774c8a80`.
No Host runtime qualification, full workspace/release gate, installation or
network lifecycle action is part of this tooling repair. Native consumer macOS
and raw Wasm/IC-cycle/instruction deltas remain unmeasured. PocketIC's pending
setup handoff remains with Testkit and
[#327](https://github.com/dragginzgame/icydb/issues/327).

Published 0.267.4 adopts committed Shared Tooling 0.1.34,
`3d33cd250fcae7dbe5cabe44b2abd6b2c91a1822`, through the canonical exporter
from a clean isolated checkout. The eighty-two-file selection adds only the
shared ShellCheck entry point and keeps retired fleet/PocketIC helpers omitted.
Static CI uses the existing reviewed ShellCheck 0.11.0 version/digest and shared
installer instead of Ubuntu's 0.9 package. The source-bound
[original static job](https://github.com/dragginzgame/icydb/actions/runs/37820158590/job/113458818709)
reports SC2015 from a shared checker before its later invariants run; that is
linter-selection drift, not a reason to patch the immutable checker or suppress
its diagnostic. Native workstation ShellCheck retains its current consumer
setup. Matching committed CI and complete macOS qualification remain
[#309](https://github.com/dragginzgame/icydb/issues/309).

The earlier read-only review on 2026-10-09 covered Shared Tooling 0.1.37
(`dc4fdf0f78928d75b69bbf43b37c690c53a04d1e`) and published Testkit 0.25.5
(`311c39b9a7f9cc04fec050797c3324842e338328`). Shared's selected Cargo
binary/example installer can replace the separate runner installation flow,
retaining exact package/profile receipts, offline byte verification and failed
attempts. Its latest hook correction also finds prepared formatters without a
user PATH export. Shared 0.1.35's three-host CI passes; 0.1.36/0.1.37 and both
Testkit 0.25.5 workflows remain queued at inspection. This is review, not a
snapshot refresh or consumer/native runtime qualification. The maintainer's
existing lock update selects Testkit 0.25.5 and is preserved; uncommitted
Testkit 0.25.6 work is excluded. Adoption remains with
[#309](https://github.com/dragginzgame/icydb/issues/309) and
[#327](https://github.com/dragginzgame/icydb/issues/327).

The workflow's exact installer block passes a real authenticated download and
version check, then the installed binary passes the maintained repository
ShellCheck gate. Workflow policy and all forty-one fixtures, actionlint, snapshot
and selected documentation checks pass. Evidence, including the original failure,
is retained under `target/tooling-ci-2674`. No Rust source, dependency selection
or runtime behavior changed; native macOS and full workspace/release gates were
not run. Raw Wasm, IC-cycle and instruction deltas are unmeasured.

The separate read-only sibling review covers Host 0.8.7
(`8edce53c43bb872cd4aaa15659e37f0236adc203`), Timers 0.14.22
(`3c288715a64952a418cb9649a9b9bee27be56e85`) and Metrics 0.2.17
(`52be29e4bc2433b3d2912a0c09538be993dfb1b2`). Host adds bounded no-follow
streaming hashing; current trusted report hashes use the existing following
contract. Timers changes tooling/evidence without Rust runtime changes. Metrics
adds checked histogram reporting and exact scaled ratios; current local
measurement policy does not need either for this CI repair. Incoming lock
selections for these releases are preserved at SHA-256
`e7018854664cdb7e0c4890f8cd17eb7a363cf20ef56a23abb3e2564cb7d468fc`;
this source review does not claim new native product qualification.

The current compatible 0.267.4 follow-up adopts committed Shared Tooling 0.1.33,
`ddd3e1c01ba8aab13a56277e05679e43a8a9d88a`, through its canonical exporter
from a clean isolated checkout. Fleet reports are optional upstream; the local
caller review finds no product, setup or CI use of the two selected reporters.
Their files and manifest records are retired together, and Make help now lists
only the local workspace report. This removes 398 vendored code lines across
423 physical lines, rather than deleting independent implementations. Local
`make cloc`, cloc installation, setup/check commands, checksum helpers and
product/adapter coverage remain selected. The shared command fixture remains
because it qualifies the maintained setup and local-report boundary, using
disposable stubs for optional extensions. The shared catalog and maintenance
tasks describe central fleet reports, which should run from Shared Tooling
([#331](https://github.com/dragginzgame/icydb/issues/331),
[Shared #83](https://github.com/dragginzgame/shared-tooling/issues/83)).
The reduced snapshot contains eighty-one files. The exact-source
[owner CI](https://github.com/dragginzgame/shared-tooling/actions/runs/37898351268)
passes its Linux regression and lint/security jobs, while both native macOS
jobs remain queued at review. Native qualification is therefore incomplete.
Focused consumer checks pass the remaining snapshot, shared command fixtures
on Linux Bash 5 and genuine Bash 3.2, actual offline `make tools-check`, actual
local `make cloc`, selected ShellCheck and documentation links. The unselected
fleet target returns the owner's explicit diagnostic without scanning siblings.
Evidence remains under `target/shared-tooling-033`; the locked graph is unchanged
from the 0.1.32 checks below. No installation, compilation, network lifecycle,
full workspace or release gate was run for this selection-only cleanup. Native
consumer macOS and raw Wasm/IC-cycle/instruction deltas remain unmeasured.

The compatible 0.267.4 follow-up refreshes the same eighty-three-file selection
from clean, committed Shared Tooling 0.1.32,
`635a39a9dd5f8d021fa9c9196b591e00521a7e02`. IC tool reuse now compares validated
records rather than comments or record order, while retaining installed receipt
bytes, hashes and executable checks. No consumer pin parser or additional setup
flow is introduced. The updated baseline keeps fleet reports with Shared Tooling;
consumer CI need not duplicate sibling dashboards. Its
[exact-source owner CI](https://github.com/dragginzgame/shared-tooling/actions/runs/37893234402)
passes Linux and both native macOS hosts; consumer macOS qualification remains
separate. IC Host 0.8.5 has no Rust API changes from 0.8.4 and adopts this same
installer correction. The subsequently selected Host 0.8.6 adds caller-owned
communication limits with an optional deadline while retaining output bounds,
cancellation and cleanup. Its [exact-source CI](https://github.com/dragginzgame/ic-host-tooling/actions/runs/37896909262)
passes; all four selected registry archives bind to source
`9f3d9a83def91030056c78e44c9efaa489be7d12`. IcyDB's cached Cargo build remains
with Testkit rather than adding a consumer process engine. Remaining captured
ICP CLI calls need caller-owned bounds and cleanup policy before consolidation
([#307](https://github.com/dragginzgame/icydb/issues/307)).
PocketIC infrastructure ownership and its pending published setup/check handoff
remain [Testkit #38](https://github.com/dragginzgame/ic-testkit/issues/38).

Focused Linux checks pass for the eighty-three-file snapshot, real offline
bundle reuse including reordered/commented pins, installer and workstation
fixtures, workflow policy and documentation links. No tool installation was
needed. Final integration Clippy, coverage-manifest checks and the required live
SQL/SQLite profile pass with lockfile SHA-256
`860504e542effd40cc0a382292cfff56b00b3888cc2e78a1ace8717988af66f7`
(Host 0.8.6, Testkit 0.25.3, Memory 0.31.9, Metrics 0.2.16). Testkit started
and tore down the invocation-owned PocketIC server for the live profile.
Evidence is retained under `target/shared-tooling-032` and `target/issue-329`;
earlier failed or differently selected checks retain their separate logs.
Consumer macOS, full workspace and release gates remain skipped user-owned
validation. Raw Wasm, IC-cycle and instruction deltas are unmeasured.

The published 0.267.3 follow-up adopts Shared Tooling 0.1.30,
`4e274a2219c0b0cc3af68ec65658b373253518fb`, through its exporter from an
isolated clean checkout. Its reviewed PocketIC 16.1.0 pins replace 16.0.0
for all three declared hosts. The retained migration failure log shows all
eight tests rejected at Testkit startup before any migration work because
the selected client expects 16.1.0. Explicit `make install-ic-tools` prepares
the corrected bundle; ordinary validation does not install it. Testkit still
owns client/server version admission
([#327](https://github.com/dragginzgame/icydb/issues/327),
[Testkit #34](https://github.com/dragginzgame/ic-testkit/issues/34)).

The existing sixty-seven-file selection is refreshed intact, with sixteen
explicit governance companions for its linked task catalog and release helpers.
This adopts installer path/link admission corrections without copying another
pin catalog or adding a consumer version policy. The task catalog does not
activate scheduling, and release delivery remains direct; no release command,
PR delivery, schedule or publication was executed. The exact-source
[upstream CI](https://github.com/dragginzgame/shared-tooling/actions/runs/37809818114)
was queued at review, so that run does not yet qualify native macOS adoption.

Linux consumer qualification uses unchanged lockfile SHA-256
`d82813ceac8a5220b87307b131c02fb80be33117ed3b64710ffa8e02765b1989`
(Host 0.8.3, Testkit 0.25.2, PocketIC client 16.1.0). The real migration
binary passes all eight maintained tests, with one intentional ignore. The
explicitly installed runner uses Testkit's published locked dependency graph;
its restart/resume migration probe and all five wrapper fixtures pass.
Snapshot, offline bundle/optimizer, inherited-`CDPATH`, installer, workstation,
adapter, workflow, documentation-link and command-stub release checks pass.
Logs are retained under `target/pocketic-327-*` and
`target/shared-tooling-030-*`. An initial extra managed probe omitted the Cargo
target/cache environment, started an uncached artifact build and lost the
server to its default idle timeout; that failed log and server outputs remain
separate from the corrected prepared-path run. The canonical lifetime boundary
is tracked in [Testkit #37](https://github.com/dragginzgame/ic-testkit/issues/37).
Only invocation-owned servers
were started and torn down; retained tool bundles and build evidence remain.
The migration source/successor raw Wasm bytes and hashes match the original
failure log (8,984,849 / 9,029,641 bytes), so their raw size delta is zero.
Cycle/instruction deltas are unmeasured because the original run failed before
execution. Native macOS consumer execution, full workspace and release gates
remain skipped user-owned validation.

The compatible 0.267.2 follow-up adopts Shared Tooling 0.1.20,
`3ecc48e579f6cf6e6ab01a6645d8a250fc8c6934`, exported from an isolated clean
checkout. Its [exact-source CI](https://github.com/dragginzgame/shared-tooling/actions/runs/37641211708)
passes Linux and both native macOS hosts. The original seventy-file snapshot
included the pin validator, PocketIC equality helpers and contribution rules.
That selection retained sixty-seven files after retiring the three unused
PocketIC equality helpers/fixtures under the maintainer's ownership direction.
IcyDB's explicit no-commit/no-push instructions remain in force. No release command or PR delivery was executed.

The validation adapter now supplies only the repository, evidence root and
target arguments. Shared Tooling owns complete failed-target aggregation in
dispatch order and `target/validation-failures/latest-combined.log`;
`latest.log` retains its last-failed-target meaning. Make supplies
tool selections to the shared installer; Testkit owns PocketIC
client/server version admission at managed startup. There is no second parser,
logger, pin catalog, retry or execution route ([#302](https://github.com/dragginzgame/icydb/issues/302),
[#318](https://github.com/dragginzgame/icydb/issues/318)).

The Wasm reporter and deployable builder share optimizer admission. A caller's
selected path resolves under the same host digest/version pins; report fields
project the admitted identity and feature/metrics commands retain its executable
drift check. Report format, flags, working-directory/environment policy and
capture bounds are preserved. No independent version/hash admission remains
in the reporter ([#307](https://github.com/dragginzgame/icydb/issues/307)).

The 0.267.1 setup follow-up used Shared Tooling 0.1.18,
`a3430b34b32a60f3b245a2b4f7e2f5321556fe56`
([#302](https://github.com/dragginzgame/icydb/issues/302)). Its
[exact-source upstream CI](https://github.com/dragginzgame/shared-tooling/actions/runs/37604299590)
passes Linux and both native macOS architectures. Export used a disposable
checkout at that exact commit; later sibling source was neither edited nor
consumed. The snapshot also includes its published LOC, note-finalization and
validation-status corrections. At that revision the combined-log adapter was
IcyDB-owned; the bounded adoption above replaces it.

The demonstrated requirement is one setup and pin owner for the same three
existing Cargo-installed tools. Reusing the shared installer replaces local
recipes and constants; no extra install mode or version-selection policy is
introduced. Executables and retained installation builds live under
`.tools/rust`; the formatter, Candid admission, Make and CI all select that owner.
The existing Rust toolchain and IcyDB-only utility catalog are preserved.

Focused Linux qualification includes a real offline installation of all three
tools, an offline check and repeat setup without installation. Shared installer,
actual consumer Make/workstation, workflow and validation-adapter fixtures pass;
the adapter fixtures assert Make's status 2 for failed recipes. Real formatter,
staged-hook preservation, release-dispatch/runner and note-finalization fixtures
pass, as do aggregate offline tool checks and actual workspace LOC reporting.
Selected integration library/fixture-builder strict Clippy and all eight
artifact tests pass with the canonical Candid pin. Initial adapter/lint failures
are retained alongside corrected logs under `target/shared-tooling-018-adoption/`.
Missing Host 0.4.2 cache entries were explicitly prepared from the existing local
cache after checking their locked archive hashes; the maintainer's dirty
Cargo.lock is byte-identical to its starting state. No workspace/package
versions, upstream files, network lifecycle or release effects were changed.
Native consumer macOS execution and full workspace/release gates remain
unperformed; Wasm bytes, IC cycles and instruction deltas are unmeasured.

[The snapshot manifest](../../.shared-tooling.snapshot) records eighty-five exact
upstream files, including the baseline and all linked rules, shared principles,
consumer/host guidance, formatting hook, shared audit methods, pinned host/IC/Rust
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

The four direct registry host packages at 0.8 are consumed only by host tooling.
`icydb-testing-integration` selects `ic-host-artifacts` with its `wasm` feature
for streaming hashes, digest formatting and report/method structure inspection,
and `ic-host-process` for executable resolution, pinned admission and bounded
execution. `icydb-cli` uses the artifact stream reader for diagnostic error JSON,
`ic-host-fs` for opened schema-artifact admission and bounded reads, and retains
IC response decoding in `ic-host-tools`.
These direct dependencies are absent from runtime and canister packages.
The selected Testkit 0.25.2 and all four direct Host 0.8.3 packages converge on
one host generation. The upstream release boundary is tracked by
[Testkit #32](https://github.com/dragginzgame/ic-testkit/issues/32); no local
override, re-export shim or second process implementation is added.
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
Host 0.8's optional TERM-grace policy is not selected by these capture callers;
the existing shared defaults remain authoritative. Failure projection includes
all four shared cleanup categories: TERM, group KILL, direct-child KILL and reap.
The native host CI lanes compile both consumers and exercise optimizer admission,
report identities and Wasm structural counts; Linux qualification does not establish native
macOS execution.

The earlier 2026-10-08 Host 0.8.0 Linux consumer qualification used lockfile SHA-256
`5e1e2484776822df034da2129a3b3ac7b5c4a536659b62c9bea962bde9d93d4d`.
All four Host 0.8.0 archives match their locked checksums and identify source
`fc74f679c7503ef9dd2db8fbd893c5c5907c72c4`; Testkit 0.25.0 identifies
`6b6d2cfe7f4c3e7a204a0c18beb6edb8895007b4`. Ninety focused integration,
report and CLI tests and strict consumer Clippy checks passed. The live
PocketIC startup test remained ignored; no runner reinstall or network
lifecycle action was performed. Native macOS consumer qualification remains
tracked in [#309](https://github.com/dragginzgame/icydb/issues/309).
Raw Wasm bytes, IC cycles and instructions were not measured.

Host 0.8.1 was separately qualified on Linux with lockfile SHA-256
`92e6193c8427a899238863a9e41579aaf5e4c0a4267e4a784d1a459e7a976d4d`.
All four registry archives match their locked checksums and identify source
`973f00a029dd873c242a4d06e9f8d2d5172ff0df`; Testkit remains 0.25.0.
The same ninety focused tests and strict consumer Clippy checks passed with
unchanged lockfile bytes. Existing callers use the improved bounded readers,
capture and Candid normalization; no consumer implementation changed.
The new nonblocking path lock has no direct IcyDB caller. Remaining Testkit
lock admission convergence is tracked in
[Testkit #35](https://github.com/dragginzgame/ic-testkit/issues/35), including
its distinct shared retention lock obligation. The live PocketIC, native
macOS and permitted resource-measurement limitations above still apply.
Host's Shared Tooling installer refresh is separate from Cargo adoption: the
then-current IcyDB snapshot failed an inherited-`CDPATH`, relative-consumer
host-tool check while its control without `CDPATH` passed. The snapshot refresh
above repairs that check; the original finding is recorded in
[#302](https://github.com/dragginzgame/icydb/issues/302#issuecomment-6063134657).

Initial consumer qualification used `6501d0e9fa7ba0439ec7a4010ca7bf0205e1d712` in
[IC Host Tooling](https://github.com/dragginzgame/ic-host-tooling). Its four
0.4.2 archives match the selected registry checksums and all 54 packaged Rust
source files match that revision. Testkit 0.21.0's checksum-verified archive has
38 Rust files matching `13df6bcf7ca913fc045e10f2060de3968f1b870b`; its Host 0.4
adoption converges the selected lock to one version of each host package
([Testkit #13](https://github.com/dragginzgame/ic-testkit/issues/13)).
The maintainer's manifest/lock selections are preserved. Candid extraction and
streamed ICP artifact publication use these shared owners, with qualification below
([#307](https://github.com/dragginzgame/icydb/issues/307)).

The current 0.267.1 consumer follow-up removes the observed hash/`ToolSpec`
composition in favor of `admit_version` and `VersionSpec`; installer provenance,
the exact Candid version, resolution, environment, output limits and raw text
policy remain consumer-owned. `CopyError` now uses the owner's `io::Error`
conversion. Named optimizer staging delegates exclusive creation, identity
checks, sync, rename and owned cleanup to `write_named_with`. IcyDB keeps exact
Binaryen admission, flags and bounded execution, and checks the eight-byte Wasm
header before allowing publication. Producer/before-publication cleanup evidence
and visible-output directory-sync failures remain distinguishable; no retry occurs.

All three direct test-instance callers now reuse the existing fixture startup
adapter, which delegates selection to `PocketIcStartupConfig::from_env`. The
batch wrapper exports `IC_TESTKIT_POCKET_IC_URL`; direct startup requires the
prepared `POCKET_IC_BIN`. The unread download setting is removed
([#317](https://github.com/dragginzgame/icydb/issues/317)). The published Testkit
runner owns server startup, readiness, process
groups, cancellation and reaping. Its explicit output-file contract preserves complete streams;
IcyDB selects fresh scratch paths, relays shell signals, presents bounded tails
and retains failed outputs. Successful invocation-owned scratch is removed
([Testkit #29](https://github.com/dragginzgame/ic-testkit/issues/29),
[#307](https://github.com/dragginzgame/icydb/issues/307)).

Run `make fetch` and `bash scripts/ci/testkit-runner.sh` explicitly before the
portable wrapper fixtures; their substitute server needs only the admitted CLI.
For Tier B or other live tests, use `make install-testkit testkit-check` to
prepare the real server as well. Cargo metadata selects the single locked Testkit
version; Shared's selected Cargo installer prepares its published CLI with
package/profile receipts under `.tools/rust`. Testkit prepares the server under
`.tools/ic-testkit-server`. Prior `.tools/testkit` outputs remain retained.
CI performs this
setup before validation. Ordinary validation never installs or downloads the
runner. This handoff removes the second server supervisor; the only retained
local policy is evidence custody and the existing 30-second startup/900-second
server lifetime. No additional runtime mode or retry is introduced.

The critical runtime panic gate requires the prepared Cargo cache as well as
ripgrep. Its shell adapter discovers executor, commit, journal and startup
sources; a development-only example in the existing integration package uses
Syn to parse Rust and exclude whole test-only items. Make supplies the same
repository Cargo home/target as other focused tooling. Validation is locked and
offline, retains macro-body coverage, and rejects unreadable or malformed source.
The former AWK `brace_delta`, `cfg_test_item` and `reset_skip` functions are
removed rather than extended into a Rust lexer
([#320](https://github.com/dragginzgame/icydb/issues/320)).

The Rust CI lanes run the existing `make fetch` before their selected shared
tool checks; Tier B additionally prepares and admits the Testkit server.
Workstation install/update prepares the same locked cache before its offline
checks, so native CI reuses that preparation for runner installation and library
qualification. Cache restoration never replaces explicit preparation, and a
failed fetch stops before tool admission or hook activation
([#309](https://github.com/dragginzgame/icydb/issues/309)).

`make check-portable-automation` owns the existing common fixture selection for
both `check-invariants` and native macOS CI. Twenty duplicate workflow entries
are removed without dropping a selected check. Host-specific checks stay with
their callers; both binary and governed-server startup have explicit native CI
qualification. This creates no second validation engine or new lane
([#309](https://github.com/dragginzgame/icydb/issues/309)).

Initial focused consumer qualification passes strict integration library/test/report
Clippy, real Binaryen staging and producer-output refusal, Candid/artifact and
report fixtures, post-link failure continuation, and both live startup modes.
Both owned PocketIC servers stopped after qualification; the initial sandbox
localhost-bind refusal remains retained separately. The complete selected
portable automation gate passes on Linux under Bash 5 and Bash 3.2.57.
The published IcyDB [release CI](https://github.com/dragginzgame/icydb/actions/runs/37622060406)
passed all Rust lanes but failed static ShellCheck and both native workstation
fixtures. The current fixture normalizes its temporary root and documents
indirect callbacks for older ShellCheck; its aliased-path failure reproduces
before the correction and passes afterward under both Bash versions. Native CI
also supplies a physical temporary-directory spelling to shared fixtures pending
their qualified owner correction. The older failure logs remain evidence;
Linux Bash 3.2 does not establish macOS qualification.

Source/archive identities and focused evidence are retained under
`target/tooling-dedup-267/`. Upstream exact-source
[Host CI](https://github.com/dragginzgame/ic-host-tooling/actions/runs/37624014360)
passes Linux and MSRV but fails its macOS tool-command fixture before library
qualification. [Testkit CI](https://github.com/dragginzgame/ic-testkit/actions/runs/37619750022)
passes its Linux complete check and all MSRV lanes, but lacks concurrency-job
startup configuration and has macOS canonical-path fixture failures. Those
failures remain separate from local consumer evidence.

The prior review selected Host 0.4.3 with Testkit 0.21.1, `ic-memory` 0.31.1,
`ic-metrics` 0.2.8 and `ic-timers` 0.14.12. Its Host archive checksums and 56 Rust
files matched `644d49c096ae05c2e17e1b6aacf14770988c5cf6`. Testkit's archive
checksum and 38 Rust files match `a98d751fb6991c12a5d0cd8faaddabe1e879199d`.
[Testkit's exact-source CI](https://github.com/dragginzgame/ic-testkit/actions/runs/37635582990)
passes. Against that lock, five backlog and 24 integration library tests
pass, as does strict integration library Clippy; the live startup test remains
ignored in this selection. Evidence is retained under `target/issue-review-267/`.
The initial offline missing-package refusal is retained separately; fetching the
selected packages does not change the lock or manifest.

[Host 0.4.3 CI](https://github.com/dragginzgame/ic-host-tooling/actions/runs/37639415895)
passes Linux and MSRV but fails both macOS builds: the new filesystem permission
conversion passes `u32` to Darwin's `u16` raw mode. This is an upstream library
compile failure, separate from the passing Linux consumer tests
([#307](https://github.com/dragginzgame/icydb/issues/307)).

The previous package review selected all four Host 0.4.5 packages, preserving
the other selections above. Their archive checksums match the lock, and all 56
packaged Rust files match `93a905b048bcaa2a0aed4214ac2f13f065dc2905`. Strict
integration library Clippy and all 24 selected library tests pass; the one live
startup fixture remains ignored. Evidence is retained under
`target/new-packages-267/`; neither manifest nor lock changed during review.
[Host 0.4.5 CI](https://github.com/dragginzgame/ic-host-tooling/actions/runs/37645681743)
passes Linux/MSRV and compiles on both macOS architectures, fixing the permission
conversion. Both native macOS jobs then fail the descriptor filename fixture's
unconditional non-UTF-8 publication expectation with a typed pre-publication
`Illegal byte sequence` error. This remaining native qualification gap is tracked
in [Host #19](https://github.com/dragginzgame/ic-host-tooling/issues/19).

[Shared 0.1.20 CI](https://github.com/dragginzgame/shared-tooling/actions/runs/37641211708)
now passes lint/security, Linux and both native macOS portable jobs after a
rerun. The initial native failures had unavailable GitHub logs; their cause
remains unverified. Exact source `3ecc48e579f6cf6e6ab01a6645d8a250fc8c6934` is
qualified upstream; the bounded consumer adoption described above now records
that exact revision. Native consumer execution remains a separate qualification
([#302](https://github.com/dragginzgame/icydb/issues/302),
[#318](https://github.com/dragginzgame/icydb/issues/318)).

An isolated Git checkout of that exact shared source passes the extended Rust
installer fixtures under Linux Bash 5 and Bash 3.2, and the combined logger
fixtures. The new offline Rust check passes against IcyDB's prepared tool route;
PocketIC alignment passes against its actual manifest and current offline lock
at 16.0.0. Evidence is retained under `target/new-packages-267/`. The initial
archive-only logger fixture lacked required Git history; its failure is retained
separately from the passing Git-checkout run. These checks qualify the reviewed
helpers; their later snapshot adoption and native consumer execution are separate.

The preceding maintainer-selected lock contained all four Host 0.4.6 packages, Testkit
0.21.2 and Metrics 0.2.9. The six archives match their locked checksums; all 56
packaged Host Rust files match `0fb05f9e18f032425188d68e1d69317a0f0127d5`.
[Host 0.4.6 CI](https://github.com/dragginzgame/ic-host-tooling/actions/runs/37648086908)
passes Linux, MSRV and both native macOS hosts, repairing the prior filesystem
fixture expectation. Consumer source/cache review and focused evidence are under
`target/tooling-convergence-267/`; no manifest or lock selection was changed.
Shared Tooling 0.1.21 at `45e34e92b43edb9543d5b7212774f87f8334079f` adds optional
PR release delivery, but its inspected Intel macOS job was cancelled
([run](https://github.com/dragginzgame/shared-tooling/actions/runs/37652236506)).
It is reviewed separately and is not the adopted snapshot.

Earlier 2026-10-08 qualification selected all four direct Host 0.5.0 packages
at `db637fac8b7a9ef62301e1d9009ffeb5ffcd0be7`. Their locked archive checksums
and all 71 packaged Rust files match the reviewed release. Missing selected
cache entries were prepared from the already-local registry after archive and
index checksum checks; no dependency was re-resolved. [Host 0.5 release
CI](https://github.com/dragginzgame/ic-host-tooling/actions/runs/37744999108)
passes Linux, MSRV and macOS 15 Intel/ARM. Consumer qualification remains distinct
from this upstream native evidence.

Transform, feature and metrics calls now share `run_wasm_optimizer`, retaining
one existing optimizer execution policy with inherited environment, 1 MiB per
stream, a 600-second operational deadline and contextual failure evidence.
Admission, pins, arguments, working directories and report format remain the
same. Interactive ICP execution still needs a separate policy. New Wasm facts and
optional IC limits are not added to the existing report contract.

That earlier Testkit 0.21.3 selection also retained Host 0.4.6; the shared child
replacement and convergence were delivered upstream under
[Testkit #25](https://github.com/dragginzgame/ic-testkit/issues/25).
Source/cache and focused consumer evidence for that selection are retained under
`target/ic-host-050-adoption/`. Consumer adoption
remains [#307](https://github.com/dragginzgame/icydb/issues/307); matching native
consumer qualification remains [#309](https://github.com/dragginzgame/icydb/issues/309).
Focused Linux checks pass strict selected CLI/integration Clippy, 3 optimizer,
10 report, 8 artifact, 53 diagnostic and 15 ICP command/response tests. Local
documentation reference checks pass. Manifest/lock and every unrelated starting
dirty file are preserved. No full workspace/release gate or native consumer run
was performed; raw Wasm, cycle and instruction deltas remain unmeasured.

Published IcyDB 0.267.1 at `cb8cefca1d68c20025398eae6dfab18a2eb45883`
passes Rust/MSRV, static and Wasm-size jobs in
[its configured CI](https://github.com/dragginzgame/icydb/actions/runs/37653174383).
Both native macOS jobs pass the Cargo-inheritance, PocketIC-supervision,
workflow-policy and critical-runtime scanner fixtures before failing the release
pin fixture. These source-bound results qualify those selected boundaries
([#306](https://github.com/dragginzgame/icydb/issues/306),
[#303](https://github.com/dragginzgame/icydb/issues/303),
[#304](https://github.com/dragginzgame/icydb/issues/304),
[#266](https://github.com/dragginzgame/icydb/issues/266),
[#289](https://github.com/dragginzgame/icydb/issues/289)); they do not qualify
later skipped fixtures, native library/CLI builds or the dirty 0.267.2 batch.
Complete ARM logs and focused Linux closeout evidence are retained under
`target/issue-closeout-267-20261008/`; Intel logs remain under
`target/tooling-convergence-267/`. The release-pin failure is the same on both hosts: template-free
`mktemp -d` selects Darwin's native temp directory, so retained lock/candidate
files are outside the fixture's requested root. The compatible local correction
uses an explicit template. A Darwin-behavior substitute reproduces the failure
and passes with the correction under Linux Bash 5 and Bash 3.2. This is focused
substitute evidence; matching native consumer CI remains outstanding
([#309](https://github.com/dragginzgame/icydb/issues/309)).

Two demonstrated consumer requirements use adapters outside the snapshot:

- The shared runner owns execution, diagnostics and combined raw logs. IcyDB's
  adapter selects repository/evidence roots and forwards target arguments.
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

[The shared pin matrix](../../ci/ic-tools.tsv) selects Quill 0.5.4, ICP CLI 1.6.0,
ic-wasm 0.11.1, didc 0.6.2 and Binaryen 133 for all three
supported hosts. [Shared host selections](../../ci/tool-versions.env) select jq,
yq, PCRE2-enabled ripgrep, cloc 2.10, cargo-sort 2.1.4, cargo-sort-derives 0.13.0
and candid-extractor 0.1.6; [IcyDB utility selections](../../ci/icydb-tools.env)
retain Twiggy, cargo-edit and cargo-watch. `make install-tools` explicitly
installs complete host/IC/Rust toolsets followed by the locked Testkit selection;
`make tools-check` checks the same roster offline. Product targets use the shared
ordered extension lists, and setup/check are separate commands. Prepare the
Rust toolchain and run `make fetch` first. Cargo
owns registry integrity and receipts for the Rust set; its check validates
successful version output for the common set; selected Cargo executables also
require their exact installation receipts and local byte digests.
[The shared Make include](../../make/tools.mk) owns setup, offline verification
and `make cloc` for this workspace. Fleet tooling inventories run centrally
from Shared Tooling; IcyDB omits those optional reporters. Make and CI select
checkout-local executables; workstation setup
uses the same targets and does not install a second system cloc. Interactive
shells must add `.tools/host/bin`, `.tools/ic/bin` and `.tools/rust/common/bin` to PATH explicitly, as
[the setup guide](../local-setup.md) describes. IcyDB adds raw optimizer
admission to `ic-tools-check`; the shared include retains bundle verification. No second npm/Cargo ICP or ic-wasm
installation is selected ([#302](https://github.com/dragginzgame/icydb/issues/302)).

IcyDB retains raw optimizer executable admission and the Binaryen 133 pipeline
identity. Shared archive pins are provisioning metadata, not optimizer admission.
Testkit alone owns PocketIC selection, provisioning, client/server admission and
lifecycle. IcyDB delegates locked Testkit CLI selection to Shared, then uses
Testkit's separate server setup/check commands. It does not maintain a server
pin, package/server version comparison or external-binary version parser.
Shared installation verifies the reviewed common IC downloads, receipts and
executable identities. The retained Shared reference guides describe the
upstream helper catalog, including optional helpers no longer selected here.
Ordinary validation never downloads missing executables.

The maintainer's expanded ownership direction gives Testkit all PocketIC-specific
infrastructure responsibility, including server release/asset selection,
explicit provisioning, offline verification and lifecycle qualification.
IcyDB selects its locked Testkit package and product test topology; Shared may
supply generic mechanisms without maintaining a separate PocketIC policy.
[Testkit #38](https://github.com/dragginzgame/ic-testkit/issues/38) owns the
published setup/check handoff and coordinates retirement of Shared's server
catalog with [Shared #76](https://github.com/dragginzgame/shared-tooling/issues/76).
Testkit 0.25.4 publishes `ic-testkit-server setup|check [--directory DIRECTORY]`
and lets unselected `run` use its prepared owner bundle. Published 0.25.5 adds
`run --idle-ttl` and `PocketIcStartupConfig::with_server_idle_ttl`, separating
operation-idle lifetime from the hard lifetime under
[Testkit #37](https://github.com/dragginzgame/ic-testkit/issues/37).
Published Testkit 0.25.4's three-host setup/check and real launch qualification
now establishes the replacement prerequisite. The explicitly selected 0.268
hard cut adopts Shared's committed five-tool retirement and Testkit's server
path, as recorded above. Prior six-tool bundles/receipts remain retained but
are no longer the active server route. Native IcyDB qualification and owner
publication acceptance remain separate. Do not patch immutable snapshot pins,
create another consumer installer or silently download during tests.

## Audit and verification ownership

The reviewed revision in [the snapshot](../../.shared-tooling.snapshot) supplies
the [shared methods](../../audits/README.md), which own generic audit procedure;
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
| macOS 15, ARM64 | [Delivered 0.270.0 native job passed](https://github.com/dragginzgame/icydb/actions/runs/38051762924/job/114212158255); later source remains separately qualified. |
| macOS 15, x86-64 | [Delivered 0.270.0 native job passed](https://github.com/dragginzgame/icydb/actions/runs/38051762924/job/114212158341); later source remains separately qualified. |

[CI](../../.github/workflows/ci.yml) now gates its central check on both native
macOS lanes as well as Ubuntu. The macOS lanes run real workstation installation,
system Bash snapshot/adapter/setup/report fixtures, focused library and CLI
checks, integrity and optimizer tests, a production canister build, formatting
and native ICP tool version checks. GitHub documents the runner architectures in
[its hosted-runner reference](https://docs.github.com/en/actions/reference/runners/github-hosted-runners).
Delivered source `2a30a7c64ac146023a80696b5fe5458b71db0938` (0.270.0)
passes every configured lane in [run 38051762924](https://github.com/dragginzgame/icydb/actions/runs/38051762924),
including both native macOS jobs, public Rust 1.88 native/Wasm consumers,
workspace regressions and live Tier B. The host logs qualify its Shared 0.3.0
snapshot and Binaryen 132; this does not qualify later Shared 0.3.7/Binaryen 133
adoption or the current dirty 0.270.2 recipes. Existing local evidence and earlier
pending-host observations above retain their original source identities.
Deployment workflows outside these selections remain unqualified
on macOS until native evidence exists.

Setup selects Homebrew prerequisites on macOS and apt prerequisites on Linux.
Binaryen 133 has admitted archive and executable digests for both macOS
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
