# Shared Tooling Adoption

This document describes current ownership and qualification boundaries. Dependency
identities come from [Cargo.toml](../../Cargo.toml), [Cargo.lock](../../Cargo.lock)
and the [reviewed snapshot](../../.shared-tooling.snapshot), not historical notes.
[AGENTS.md](../../AGENTS.md) is IcyDB's local engineering overlay.

## Ownership and provenance

IcyDB selects the **0.31.5 committed snapshot** at
`b2026a118e38de4af1555c527af327cb604eacdf`. The canonical revision-selected
exporter installed it without modifying supplier files; all **159 payloads**
verify against the selected commit and modes. This is a source-snapshot adoption,
not a claim that a supplier release tag exists. Rust **1.99.1** remains both the
minimum in the two workspace manifests and the pinned development compiler.
No IcyDB package versions or release state change in this snapshot refresh;
the published database and retained batches are preserved. Dependency selections
are recorded below.

The current refresh adds the release-batch guide to the export closure and
isolates inherited release settings in the command checker while substituting
both runner layers. Four payloads change from 0.31.3; all 159 payloads match the
reviewed committed source and modes. This does not requalify selected-stage
omissions or native macOS hosts for the new dependency cohort.

The database 0.301.0 deployment exposed the upstream checksum helper's Bash 3.2
parse failure in both macOS hosts. This canonical refresh restores system-Bash
admission, retains NUL/backend refusal and reports missing LOC scratch inputs
without partial totals. The three changed payloads come only from committed
GitHub source; unrelated producer edits are excluded. The database crates remain
0.301.0, with this CI repair carried by CLI 0.296.3. Native-host results must be
observed on that corrected candidate before claiming qualification.

The 0.31.0 hard cut renames the batch runner and configuration settings, removes
old-name aliases and requires schema-2 intent with an explicitly recorded commit
before publication. IcyDB uses `RELEASE_SETUP_TARGET=release-candidate-setup` and
`RELEASE_SETUP_MODE=prepare|check`; Shared owns automatic single-group capture.
The former optional capture default and live-delivery escape are removed. New
snapshot exports require version and entrypoint annotations. Checksum admission
rejects NUL output before shell capture and distinguishes malformed output from
backend failure; npm lockfiles require schema 3.

The completed pre-0.31 `batch-kRCC00xv` remains schema 1 with its original
0.30.9 checkout and tooling under `target/release-frozen/batch-kRCC00xv/`.
It is not an active delivery, and no journal is relabelled or publication commit
reconstructed. Any remaining reconciliation/retirement must use that original
reviewed runner, rather than 0.31's schema-2 interface. That adoption preserved
the deferred Testkit 0.89.3 edit and local delivery notes; subsequent requested
dependency adoption is recorded below. No adoption authorizes discarding
unrelated edits or cleaning retained delivery caches.

The 0.31 consumer qualification passes on Linux with Rust/Cargo 1.99.1.
All 159 annotated payloads match reviewed source and executable modes. Actual
setup, source/manifest-verified native archives, metadata preparation, schema-2
journals, receipts, interruption/uncertain-publication recovery and completed resume pass
in `target/tmp/frozen-icydb.2roRMAMs/`; registry effects are substitutes. The same
fixture rejects schema-1 intent and missing publication identity without
migration or HEAD reconstruction, and retains supplier-order, remote-conflict,
guarded reconciliation and cache-retirement controls. Dispatch, checksum/NUL
and missing-source controls, documentation, shell and schema-3 npm checks pass.
The retained 0.30.9 snapshot independently verifies against its original source.
During that adoption the incoming Testkit 0.89.3 lock bytes were unchanged. No broad product suite,
release preparation, staging, commit, push, publication or cleanup ran for this
adoption. Current-mapping omissions and native macOS remain unqualified; IC
cycles/instructions and total/code-section Wasm deltas are unmeasured.

The 0.30.9 refresh moves captured-checkout setup into Shared's saved batch
context. `release-candidate-setup` prepares the selected host tools and locked
sources through their existing owners; recovery checks the saved-source metadata
editor offline and never installs from a partially prepared lockfile. Shared owns
batch status, resume, fast-forward reconciliation and explicit retirement. The
old local bootstrap prerequisite and batch-resume wrapper are removed.

A new batch still requires clean committed source. Mid-run edits are deferred
only after capture; the update does not authorize adding unrelated incoming edits
to a release or discarding them. Reconciliation also requires a clean normal
checkout. Retirement deliberately refuses IcyDB's ignored `.cache/` directory,
which is outside Shared's narrow `target/` and `.tools/` allowance. Preserve that
cache and delivery evidence unless separately authorized to remove them.

The 0.30.9 consumer qualification passed on Linux with Rust 1.99.1. Evidence is
retained in `target/tmp/frozen-icydb.3nZ3GS56/` and
`target/tmp/shared-0309/frozen-hooks-prepared.log`: real setup, metadata, verified
native archives, receipts and local Git delivery retained the captured graph
across a real normal-fixture dependency update. Interrupted/uncertain recovery,
completed resume, status, remote refusal, dirty reconciliation refusal, clean
fast-forward reconciliation and cache-retirement refusal passed. Registry effects
were substitutes. The first attempt exposed missing sources in the fixture
updater's acquisition cache; preparation now uses the existing fetch owner.
Snapshot/source integrity, dispatch controls, documentation and shell checks also
pass. The incoming real Wire/Derive 0.17.5 lock edit is unchanged. No package
versions, production Git delivery or registry state changed in the adoption
checks. The maintainer subsequently authorized the preserved Wire/Derive 0.17.5
edit for CLI 0.296.2 deployment. macOS and current-mapping release qualification
remain separate.

CLI 0.296.2 subsequently completed real frozen delivery in `batch-kRCC00xv`
at `2db84355523b0af29d4434d718a1e03a4749e528`, after passing the full Linux
release gate. The official archive matches registry checksum, clean source and
original manifest and selects published IcyDB 0.300.0 with Wire 0.17.5. A new
normal-checkout Testkit 0.89.3 lock edit stayed deferred while the batch retained
Testkit 0.89.2 through publication. The normal branch awaits clean reconciliation;
the deferred edit and durable release evidence are preserved. See the
[delivery status](../design/0.300-testkit-frozen/0.300-status.md).

The 0.30.8 refresh adds the canonical frozen release-batch helper and its
underscore-safe recovery identifiers. A qualified batch captures clean committed
source once; later normal-checkout lock edits stay deferred while every selected
group releases and publishes from that captured graph. Durable recovery state
lives under `.git/release-state/frozen/`; the normal checkout is never reset or
silently reconciled. See [frozen release batches](../../docs/release-batches.md).

Grouped release entrypoints capture automatically through Shared. `RELEASE_GROUPS`
defaults to the explicitly selected `RELEASE_GROUP`; select a complete supplier-first
roster with `RELEASE_GROUP=database RELEASE_GROUPS=database,cli`. Publication remains
explicit through `RELEASE_PUBLISH_TARGET=publish`. Clearing the roster refuses;
there is no supported fallback to live-checkout grouped delivery. A batch applies
one increment to every group; mixed increments require resolving the intended
roster/increments before starting delivery. Earlier delivery records describe the
then-supported standard workflow and remain historical evidence.

Actual IcyDB hook qualification passed on Linux using captured source, a local
bare remote, real metadata/receipts and verified native archives. During the
first group's gate, a real dependency update in the normal fixture checkout
remained deferred through the second group's gate. Interrupted validation,
uncertain publication reconciliation, completed resume and remote conflicts
retained their expected boundaries. Registry effects alone were substitutes;
this does not claim real registry delivery or macOS qualification. See the
[qualification evidence](release-validation.md#frozen-hook-qualification).
Captured checkouts prepare tools and locked sources through the existing owners.
Publication metadata uses the validated repository Cargo cache while actual Cargo
publication retains its credential environment. No mutable normal-checkout cache
or credential files are copied into the delivery checkout.

Cargo and browser checks consume Shared's common CBOR ownership catalog. The
existing Rust source-boundary gate also selects `--cbor`, with no new exceptions.
This adds literal source-reference coverage alongside resolved Cargo dependency
checks; it does not claim macro expansion or arbitrary alias resolution.

The retained 0.30.4 cohort adopted the macOS Bash 3.2 LOC-array correction, the shared
lexical Candid source check and bounded experimental-evidence retention.
`check-candid-boundary` scans all first-party Rust roots through the existing
invariant owner, with an empty exact-declaration allowance policy; macro/type
resolution remains a review obligation. Historical report compaction preserves
selected conclusions and original Git references, while new Wasm captures
default to `target/evidence/`. Release/recovery records, saved review inputs,
package versions and dependency selections are preserved. See the
[report retention policy](../reports/README.md).

### Current 0.302 cohort (2026-11-07)

The root catalog selects Build **0.78.0**, Host Artifacts/Fs/Process **0.75.0**,
Host Tools **0.85.0**, Platform **0.25.0**, Storage **0.68.7**, Wire/Derive
**0.17.12**, Metrics **0.29.6** and Testkit **0.92.0**. Runtime **0.20.0**, Types
**0.18.2** and Host Text **0.74.2** remain current. Official registry archive checksums and
supplier source identities were verified for requested updates before adoption;
the incoming compatible Wire patch is preserved. Runtime 0.20 aligns IcyDB's
direct IC0 consumers with Storage, removing the duplicate Runtime 0.19 identity.
Host Tools and native Testkit
now share Platform 0.24, including bounded Candid limits and CBOR client errors.
Canonical Runtime/Types identities are shared. The old Platform 0.23 facade and
Foundation 0.74 identities leave the root lock. Host Tools 0.85 archive checksum is
`7f0fe55f656678940b38567ba06429ad17cec9f7b4ff6a1a0480b09203a75c2c`,
with clean source `07ef09c8723ab79fc9e764fa77d7f1ccfc0482b6`.
Testkit 0.92 archive checksum is
`6a45ed18109c81668ab742d2c3ed9f762331730be294ebaa6682ba28b4fa1113`,
with clean source `ceb99e1c682a92026a80d743bfccada350319c08`.
PocketIC protocol, deadlines and exactly 0.11.0 management DTOs are unchanged.

Platform 0.25's published source is `0a32e8a2f7977a71062d4edec542bea96d63c508`.
Its removed facade entrypoint macros/`cdk` feature are not used by the root
consumer. IcyDB directly selects only BLAKE3; IC0 remains Runtime-owned.
Host Tools/Testkit's published requirements retain Platform 0.24 alongside the
direct 0.25 facade. This supplier split does not duplicate Runtime or Types.

Build progress now configures heartbeat intervals directly; its Cargo-output
switch is retired. Testkit's native execution/cleanup identity changes require
the explicitly authorized 0.302 line. Keep finite request budgets and the sole
Testkit provisioning/admission/lifecycle owner. Existing database storage and
recovery contracts are unchanged. The
[0.302 tracker](../design/0.302-native-cohort/0.302-status.md) owns this adoption.
The maintainer also included independent Canic qualification, owned by
[#373](https://github.com/dragginzgame/icydb/issues/373); preceding exclusions in
historical adoption records do not limit that new scope.

The published Canic adapter remains 0.115.0 with Wire 0.16. Its endpoint
limits exchange fails E0308 against the actual IcyDB Wire 0.17.8 facade in a
disposable consumer. Supplier alignment blocks the independent stack migration;
its manifest/lock and integration clients remain unchanged. See
[#373's compiler evidence](https://github.com/dragginzgame/icydb/issues/373#issuecomment-6706757018).
Root adoption does not establish generated-actor or installed acceptance.

### Requested Build, Platform and Testkit adoption (2026-11-07)

The subsequent Platform follow-up selects **0.23.6**, matching the incoming
lock, at declared floor 0.23.6. Its registry/lock/archive checksum is
`09d519af82b2eadafc4b56ecd6a72b829f0ed13f38b4fcc853a8f79ec7f68a6e`
and supplier source is `51f5fbb7c53000fbb6f7bc3219ee3c2978c08521`.
The published patch shares Candid admission's child-reference buffer, retaining
bytes, traversal, quotas and depth limits. Runtime 0.19.5, Types 0.18.2 and all
other locked selections remain unchanged. Platform's own Shared adoption was
independent; IcyDB's subsequent 0.31.3 refresh is recorded above.

The root selects published **Build 0.77.0** and **Testkit 0.90.0**. Their
official archives match both registry and lock checksums and record supplier
sources `9d87394919b812cb6de55a9abd6dd2fd026c9070` and
`c03161b9f1395c343756fda6b8721e957798c3df`. Declared floors now match the
settled **Platform 0.23.6**, **Runtime 0.19.5** and **Types 0.18.2** selections;
their registry archives and source identities are verified too. Other selected
packages retain their versions; the new Testkit removes unused transitive
dependencies. No supplier instruction/size result is an IcyDB measurement.

Build's shared batch entry now yields four parts. The maintained coordinator
reads the complete failure/error-details tuple and retains collect-all failure
reporting, successful records and post-link artifact custody. Both native tools
are admitted through Shared's existing installer. The Build metadata editor
renders bounded read-only database/CLI candidates without modifying manifests;
Testkit's root-selected server tool admits the existing server bundle.

Testkit's published [0.90 migration contract](https://github.com/dragginzgame/ic-tooling/blob/c03161b9f1395c343756fda6b8721e957798c3df/docs/testkit/client-migration.md#testkit-090-surface-cut)
requires a positive finite request duration at construction. The root startup
fixture supplies **300 seconds**, preserving 0.89's five-minute request
allowance. Its managed startup bound remains **30 seconds**. Public native
`icydb::testing` types now share Testkit 0.90's crate identity; direct consumers
must select that line. The maintainer authorized the new 0.301 line, whose
[tracker](../design/0.301-native-tooling/0.301-status.md) owns focused closeout.
Only unpublished database notes move there; package versions remain unchanged.

[Issue #629](https://github.com/dragginzgame/icydb/issues/629) owns the root
fixture. The inventory also found `testing/integration/src/lib.rs`, but its
manifest explicitly belongs to `testing/canic-adapter`. Its Testkit 0.75.0 and
Platform 0.8.3 selections retain their separate qualification boundary and are
unchanged. Their synchronous helper cannot take the root 0.90 constructor
without a separate composition migration under
[#373](https://github.com/dragginzgame/icydb/issues/373). The adapter manifest,
lock and integration source remain byte-for-byte unchanged; no compatibility
alias, second runtime, server pin, installer or lifecycle owner is added.

Focused qualification passes on Linux with Rust 1.99.1: 42 Rust tests, strict
Build all-target and public startup-consumer lint, root feature-boundary checks,
read-only metadata candidates and facade Wasm compilation with testing/metadata/
SQL/migration enabled. Constructor controls preserve typed zero/overflowing
budget refusals; the managed simulator retains startup rounds and terminal
failures, followed by explicit Testkit-owned instance/server teardown. This is
local qualification, not source delivery or macOS/installed-application evidence.
Broad product/release gates remain unselected and IcyDB cost/size deltas are
unmeasured. See the 0.301 tracker and #629 for the remaining delivery boundary.

### Earlier Testkit 0.89 adoption (2026-11-06)

The root selects published **Testkit 0.89.2**. Its official archive checksum is
`65358cb9f4473db0f059e252d57d0d946b9d33c29c79b01fba34597024ab8e02`
and its clean supplier source is `33c44602ec27c996ab67eb92458bf5022e630760`.
The settled lock preserves incoming Storage 0.68.1, Platform 0.23.4 and
Metrics 0.29.5 selections. The independent Canic manifest and lock are unchanged.

Testkit no longer configures client logging from environment variables or owns
logging workers/subscribers. Applications own their logging sinks; IcyDB used
none of the removed setup. The native `icydb::testing` PocketIC and polling-error
re-exports now have Testkit 0.89's crate identity. Consumers combining those types
with direct Testkit imports must update to the same Testkit line. This is a
pre-1.0 database minor boundary; the maintainer authorized database 0.300.0
and CLI 0.296.2 delivery. No stored format or boot discriminator changes.
See the [upstream migration](https://github.com/dragginzgame/ic-tooling/blob/33c44602ec27c996ab67eb92458bf5022e630760/docs/testkit/client-migration.md#application-owned-logging-in-089).

Focused strict lint and the real managed public startup-polling control pass on
Linux with Rust 1.99.1. The control retains six polling dispositions, round
boundaries and typed failures. The locked Testkit CLI prepared its own server,
started it for the test and reaped it afterward; IcyDB adds no simulator owner.
The full Linux database release gate subsequently passed and database 0.300.0
was tagged, pushed and published at `ead6a522155770bf4c5d289a4d33c90a627ec798`.
All six official archives match registry checksums, clean source and original
manifests; their locks retain Testkit 0.89.2 and Wire 0.17.4. See the
[delivery status](../design/0.300-testkit-frozen/0.300-status.md). macOS and
installed-canister acceptance remain separate.

### Earlier Build, Storage and Testkit adoption (2026-11-06)

The root selects published **IC Build 0.75.0**. Its official archive matches the
registry checksum and records supplier source
`1239e9a824689839a9c1a945a1d011071a8e2377`. Build advances from the locked
0.74.4 selection. Incoming Platform 0.23.2, Wire/Derive 0.17.3 and cc 1.11.1
selections are preserved; the settled lock also retains flate2 1.1.11. The shared
snapshot remains unchanged.

Build removes unused inspection/configuration readers. IcyDB uses its retained
builders and checks the actual single/batch maintenance specification instead
of the removed readers. Six focused cache/input-identity controls and strict
all-target Build-consumer lint pass on Rust 1.99.1. The locked release-metadata
editor is prepared and admitted through the existing Shared installer. Root
metadata, dependency declarations and selected native/Wasm graphs pass; Build
does not enter the public facade's Wasm normal/build graph.

The authorized 0.299 candidate selects Storage **0.68.0** and Testkit **0.88.0**.
Official archives match registry checksums and record supplier sources
`6f87f363a09e20086cbf76fd4dc9caff8bf877ff` and
`0bc2cc4c9b95b6badc9283d5d92abf08036d0dc3`, respectively. Storage replaces all
collection images and changes engine/codec failures to traps. IcyDB admits a
new boot identity before recovery and rejects older images without modifying
them. Native mutation panics poison the Storage runtime; retain no runtime or
leases after such a panic. Explicit collection framing admission remains typed.
Testkit changes the PocketIC type exposed by the public testing helper; IcyDB
uses its retained async client and managed polling/lifecycle owner. See the
[Storage contract](https://github.com/dragginzgame/ic-storage/blob/6f87f363a09e20086cbf76fd4dc9caff8bf877ff/releases/storage/0.68.md)
and [Testkit release](https://github.com/dragginzgame/ic-tooling/blob/0bc2cc4c9b95b6badc9283d5d92abf08036d0dc3/releases/testkit/CHANGELOG.md).
No existing backing is reset or deleted. All-target workspace lint, testing-feature
lint and model/core feature lint pass. Sixteen database-format/recovery controls,
four shared control-map controls, nine public bootstrap/admission controls,
12 malformed-format corpus controls and one Testkit-managed startup-polling
test pass on Linux with Rust 1.99.1. The latter
covers six polling dispositions with the current public PocketIC type; Testkit
started and reaped its owned local server. This is not installed IcyDB lifecycle
acceptance. The full release gate remains required.

The separate Canic metadata check remains blocked by its Storage 0.64 /
Platform 0.17 runtime-guard versus Platform 0.8.3 executor link conflict after
locked source preparation. Its manifest and lock are unchanged; this is the
existing independent [#373](https://github.com/dragginzgame/icydb/issues/373)
qualification gap. The settled full Linux database release gate passed on
Rust 1.99.1, including all 2,723 no-default core cases and 3,883 core workspace
cases. The corrected journal reuse control preserves exact steady capacity after
the first warm refill. Database 0.299.0 is tagged, pushed and published; all six
official crate archives match their checksums and clean release source
`5144ea0f97d2c1365d29a957e9811eac87274a11`. CLI delivery uses its independent
group workflow and ledger. macOS CI and installed-IC acceptance remain separate
qualification obligations.
IC cycles/instructions and total/code-section Wasm-size deltas are unmeasured.

### Preceding published dependency cohort (2026-11-05)

The root catalog adopts Storage **0.67.1**, Testkit **0.87.0**, Wire **0.17.2**,
Platform **0.23.0**, Runtime **0.19.2**, Types **0.18.1** and Host Tools **0.83.0**.
Host Artifacts/FS/Process and Build floors are **0.74.3**, with Text **0.74.2**.
The root lock selects Foundation/Build **0.74.4** and Text **0.74.2**.
The Testkit and Host Tools archives match their official registry SHA-256
checksums and record supplier source identities
`e3b8a88539c54cb1da3ae471954fa234beabdecf` and
`094b7787166fd968f512518c1a0722c73cd86122`, respectively. Both require Rust 1.99.1.
The initial cohort's 13 archive checks and the preceding Platform 0.23.0 archive
check remain historical evidence; the latter records source
`7d2230c585b1a91c11ff12d0fcfce24bda424550`.

Testkit 0.87 removes its blocking client and private runtimes. The pending public
startup helper and polling fixture now use its async client and lending async
observation callbacks. Callers own the runtime and explicitly await instance
closure. Tokio is a native-only development dependency, not a product runtime
dependency. Testkit retains all provisioning, admission and lifecycle ownership;
IcyDB adds no blocking bridge or server selector. Host Tools 0.83 removes its
certificate module; IcyDB's selected project/interface APIs are unchanged.

Both packages now select Platform 0.23, removing Platform 0.22.3 from the root
lock. Runtime/Types and Wire identities remain shared. The product Wasm graph
selects Platform with BLAKE3 only; native Testkit capabilities do not leak into it.
Storage layouts, codec bytes and budgets are unchanged. No storage reset or
compatibility fallback is introduced. The isolated Canic manifest/lock retain
their separately qualified stack.

The Testkit/Host follow-up passes **43 focused Rust tests**: two typed observation
controls, one real PocketIC polling test covering six round/error cases, 17 CLI
transport/projection controls, 14 CLI inventory/funding process controls and nine
build-artifact/interface controls. Strict facade/startup-fixture, CLI and build
library lint pass. Public SQL/migration Wasm compilation also passes with the
`testing` feature enabled; its normal/build graph excludes Testkit, Tokio and
native HTTP/Host tooling. Dependency declarations, resolved graph invariants and
selected documentation checks pass on Rust 1.99.1.

Testkit 0.87 admitted the existing prepared server bundle. The sandbox initially
refused its loopback bind; the focused test then passed with local socket access.
Testkit owned server startup and explicit instance/server teardown. Observations
are scripted typed states, not an installed generated IcyDB canister: fresh-install
and same-release-upgrade application acceptance remain open in
[#315 — Published typed startup driver](https://github.com/dragginzgame/icydb/issues/315).
Broad product/release gates and macOS qualification were not selected. IC cycles,
instructions and total/code-section Wasm-size deltas remain unmeasured.

The preceding Platform 0.23.0 follow-up passed 110 focused predicate/checkpoint/query
fingerprint tests, strict Core SQL/migration library/test lint, facade lint with
native Testkit enabled, and public SQL/migration Wasm compilation on Rust 1.99.1.
Dependency graph/boundary and selected documentation checks passed. That follow-up
required no authored Rust source changes. Its raw working evidence was written
under `target/evidence/platform-adoption-20261105/`; scratch retention is not a
permanent archive or a current qualification claim.
These are focused Linux checks; broad release, macOS and installed-IC gates were
not selected. IC cycles/instructions and total/code-section Wasm-size changes
remain unmeasured.

Shared 0.30.1 adds owning-target scratch defaults, bounded LOC scanner memory,
complete partial-export documentation companions and numbered-draft release
projection repairs. Its 154 payloads are refreshed through the canonical exporter.
The previous `4d09618` LOC presentation adoption remains included.

The initial 0.22.1 cohort's focused Linux qualification passes 232 Rust tests
across storage reopening, row/index stores, accepted predicates and recovery, public Wire envelopes and
schema metadata, startup/Testkit helper contracts, real CLI process controls
and build artifact/bin behavior. Strict affected-package lint covers SQL,
migration, generation, Wire metadata and optional testing; minimal Core/facade
lint also passes. The public SQL/migration facade compiles for Wasm. Dependency
boundaries, grouped draft ordering, all snapshot payloads and documentation pass.
Shared's LOC, formatting-evidence and release-changelog fixtures pass. Old/new LOC
reports agree on counts, classifications, hashes and diagnostic contents; the
queued traversal reorders diagnostic notes. This is focused adoption evidence,
not full product/release, macOS or installed-canister qualification. IC cycles,
instruction counts and total/code-section Wasm-size changes are unmeasured.

The isolated Canic qualification manifest/lock retain their separately recorded
candidate. Published `canic-icydb 0.115.0` is now available and selects Platform
0.22, but its official archive still exposes Wire **0.16.1** requirements for
endpoint limits. It cannot establish composition with this root's Wire 0.17
identity. Neither availability nor root checks qualify that separate stack;
[#373](https://github.com/dragginzgame/icydb/issues/373) retains its acceptance.
The Testkit server runner intentionally follows the isolated adapter lock, as do
its integration clients; root Testkit adoption does not silently replace that
server selection or claim installed qualification.

The preceding `4d09618` LOC presentation update labels unclassified code as `???`, widens
name-to-count spacing, uses lowercase `n/a` and puts diagnostic guidance in
`cloc.sh --help`. JSON category names and totals remain unchanged. That refresh changed only the
reporter and classifier payloads from the preceding `b37a882` selection.
The actual workspace JSON is byte-for-byte identical before and after adoption.
The normal Make report, committed sibling-report regression (alignment, missing
workspaces and partial failures), script lint, snapshot integrity and documentation
checks pass. No product test suite or runtime measurement was selected.

The documentation correction adopts the committed fix for
[Shared #355 — exported documentation links](https://github.com/dragginzgame/shared-tooling/issues/355).
Four exported guides now reference the revision-bound central LOC/audit guide;
consumers do not need a local fleet guide or scheduler. All 397 local references
across 58 exported documents pass, as does the ordinary documentation gate
(1,038 references across 111 documents). The accompanying LOC table correction
keeps one workspace summary row; schema-2 JSON and category totals are unchanged.
The earlier audit below was performed on `08cd1724`; its product evidence is
unchanged by this documentation/presentation refresh.

### 0.30.0 consumer audit (2026-11-05)

This audit applies the revision-bound Shared baseline, engineering details,
Rust hygiene, workspace/dependency, contribution, changelog, host and snapshot
adoption rules, using the [Shared code-hygiene method](../../audits/code-hygiene.md).
It compares the working tree at task entry, not HEAD: the checkout already
contained substantial issue and release-qualification work.

| Rule family | Consumer action and evidence | Qualification boundary |
| --- | --- | --- |
| Snapshot and compiler | Canonical export, 154 checksum/mode matches; both manifests and the toolchain pin select 1.99.1. | Linux compiler/lint evidence; no new macOS qualification. |
| Directory and test ownership | Hard cut to `canisters/tests` and `schema/tests`, with live Cargo/script/documentation references updated. Extract 340 inline test modules plus the documentation example, 275 helper methods/functions and their test state/types. Use normal module discovery. | No package identities, approved workspace boundaries or product features change. |
| LOC roles | Declare 47 fixture, example and build-tooling package roles; run the new reporter for the root and explicit Canic workspace. | Both reports have zero unclassified source. Macro expansion and conditional-attribute diagnostics remain visible; LOC is not generated-code or performance evidence. |
| Lint policy | Audit source, generated attributes, fixtures, Cargo tables and command-line settings. Convert stable suppressions to expectations; remove stale expectations and a blanket fixture cfg allowance. Enable unfulfilled expectations in both workspaces. | Product and root fixture gates qualify the checked feature configurations. Canic-specific conversions await the graph repair below. |
| Source inspection | Generated-endpoint and schema/model scans now propagate search failures. Source guards share test-file classification and refuse metadata failures. Negative fixtures cover missing inputs and unreadable entries; Cargo adoption fixtures mirror renamed targets and retired manifests. | Focused scanner fixtures and affected source guards; no claim of a general Rust parser in text guards. |
| Canonical owners and native APIs | Dependency-boundary checks retain Platform/Storage/Wire, Observability, Testkit and Host ownership. Std-capable code and generated paths use `std`; CLI/build entrypoints use Clap. Build gzip encoding uses the Host artifact owner. | No supplier or server adoption is implied. Handwritten computed/typed diagnostic projections stay intact; the minimal build error does not gain a new dependency just for a derive. |
| Documentation and repository identity | Update the local overlay, hygiene example, live paths, README Storage link and GitHub description; record outcomes in both owning group ledgers. | Published notes, historical source paths and tags are preserved. |
| Delivery, hosts and retained evidence | Preserve independent release groups, full-gate fallback and receipt custody. No tracked Git symlinks were found. | No commits, pushes, release execution, broad product suite, installed-IC acceptance or network lifecycle changes. |

Test-only fields, enum variants and interruption/counter call sites remain at
the runtime layout or control-flow edge they observe. Each affected owner explains
that constraint; fixture implementations and state live in external test files.
Diagnostic macros retain generated test lookup tables beside their single
production declaration roster to avoid a second inventory. Executable tests are
external. This does not authorize wider production visibility or `#[path]` wiring.

Remaining `allow` attributes have configuration-specific reasons: generated
imports/dead code and async bodies vary across consumer expansions; deliberate
compile-fail inputs can stop before the suppressed lint runs. Cargo has no
`expect` level, so reviewed workspace-wide style choices stay in its lint table.
Parser fixtures containing an authored `allow` are input data, not suppressions.

Canic qualification remains blocked by the already tracked
[#373 — canonical Canic adapter adoption](https://github.com/dragginzgame/icydb/issues/373):
its locked Storage/Platform selection mixes incompatible IC executor `links`
owners. Locked metadata and the independent LOC inventory work, but adapter
compilation does not. A locked fetch reproduced that conflict; no unlocked
resolution or supplier override was attempted. Root checks do not qualify this
separate graph. Native macOS, installed-IC checks and full product/release gates
remain user-owned validation. IC cycles, instruction counts and total/code-section
Wasm changes are unmeasured.

Focused validation passed **379 Rust tests**, five source-input scanner cases,
strict lint selections for the seven product packages and root fixtures, the
minimal Core feature selection, the runtime panic scan, dependency boundaries,
shell lint, Cargo inheritance/grouped-ordering adoption fixtures and 118 local
documentation references. Generated token expectations
were updated with the standard-library paths; feature-conditioned imports retain
their original SQL gate. Full repository tests were not selected.

The same 0.30 classifier was run against the task-entry source and the completed
root workspace. Counts are Rust code lines (excluding comments/blanks), not
physical diff lines. Role declarations and test extraction change categories;
the application reduction below does **not** mean deleting that many feature lines.

| Root workspace category | Before | After | Delta |
| --- | ---: | ---: | ---: |
| Library/application | 268,980 | 246,573 | -22,407 |
| Tests | 215,354 | 232,179 | +16,825 |
| Examples | 711 | 865 | +154 |
| Build/tooling | 6 | 4,856 | +4,850 |
| Total | 485,051 | 484,473 | -578 |

The independent Canic inventory reports 34,338 test and 125 example code lines,
with zero application or unclassified lines. A separate source inventory retains
all 5,198 existing test bodies and adds two scanner regressions; it is an inventory
check, not a claim that every body was executed. Diagnostic macro/conditional
classification notices are retained in the LOC output. Non-test code (application,
examples and tooling combined) changes by -17,403 lines; tests change by +16,825,
for a total of -578 code lines.

The previous 0.29.4 snapshot selected
`628ccc75d813bfd9bae777bfbaef55838d25b1b2` and verified 153 payloads. Its retained
release-state, group-changelog and instrumentation-only Metrics contracts remain
in force; earlier cohort evidence below is historical, not new qualification.

The earlier 0.29.4 cohort adopted published **Host Tools 0.81.0** and
**Metrics 0.29.0**.
Their official archive checksums and exact VCS sources are verified: Tools
`8ac12cf92fe04c6f4aec0ce4aad80fcd13df07f0`, Metrics
`0ead785d2f0c7eafa05c9f9a05a2a609f320af0d`. Tools' Rust sources match 0.80.0;
its Platform 0.21 contract matches IcyDB's existing selection. Foundation/Build
0.74.2 and Text 0.74.1 floors now match the already-locked patch releases. The
incoming TOML lock changes are preserved. The isolated Canic qualification stack
retains its separate manifest and lock; no Testkit, Canic or server adoption is
implied.

Metrics stays with Core's application instrumentation. IcyDB's instruction
report now owns its five immutable fields and numeric admission; report getters
return `InstructionMetrics` rather than a recorder. Frontend field names, order,
widths and bytes remain unchanged. CLI floor averages and build-report percentage
arithmetic stay with those reporters. CLI and build/binding tooling no longer
declare Metrics, and the existing dependency-boundary checker admits direct
Metrics declarations only in Core. Recorder restoration is used only in Core
instrumentation tests, never frontend decoding. This is the consumer repair for
[Observability #130 — Instrumentation-only Metrics](https://github.com/dragginzgame/ic-observability/issues/130);
it does not remove or migrate an existing composed endpoint.

Focused Linux qualification passes on the declared/pinned Rust 1.99.1: strict
Schema/Core/CLI/build-generator and Wasm library lint; 42 selected Rust controls
for report admission, reset/overflow quality, display arithmetic, Account codecs,
reflection and actual CLI/Host Tools process boundaries; dependency declaration
and resolved-graph admission; and the unchanged browser contract baseline. The
browser check passes all 91 frames across twelve endpoints and 3,112 truncated
frames plus malformed/value/budget controls. The previous 3,145 count belongs
to the older Account sample: the authorized canonical default-account cut removes
its explicit 32-byte subaccount payload. Metrics report fields and bytes retain
their existing contract.

Native qualification exposed Account reflection fallout from that earlier hard
cut: the custom deserializer's container name differed from serialization, and
an all-zero sample no longer represented the optional nonzero subaccount. Keep
both directions named Account, seed the maintained nonzero option and retain a
focused query-response reflection regression. No retired form or relaxed byte
width was introduced. Logs and archive provenance are under
`target/evidence/tooling-metrics-20261104/`.

Full repository/release gates, new native macOS and installed Canic/PocketIC
qualification were skipped as user-owned validation. No network lifecycle,
staging, commit, push, package version change, retained-data deletion or supplier
working-tree edit occurred. IC cycles/instructions and total/code-section Wasm
changes remain unmeasured. Prior issue fixes and historical evidence below keep
their separate input cohorts; this adoption does not upgrade old release receipts.

The prior cohort selected the Shared Tooling **0.29.1 development snapshot**, public main
commit `55960755351c127b8ef2a03a1f08fdeba068f5e4`, verified against a clean
supplier checkout and the public GitHub revision. Upstream's changelog still
marks 0.29.1 unreleased and no `v0.29.1` tag was selected; this records
committed-source adoption, not a finalized
supplier release. That canonical export owned 152 selected payloads with their
hashes/modes. No supplier working-tree changes are consumed; ordinary integrity
checks remain offline.

The preceding 0.28.2 refresh adopted Shared's baseline/roster observations, strict run-evidence
verifier and atomic receipt byte custody. IcyDB's opt-in qualification path
removes its generic event/timing/log parser, while keeping product selection,
complete source/index inventory, host/compiler/configuration admission and the
reviewed recipe contract local. Qualification receipts use the new
`icydb-release-validation-qualification/2` contract and bind an immutable Shared
proof. Old qualification receipts and historical release commit/candidate
receipts retain their original meanings; no old proof is upgraded or recreated.
The default full release gate and native-host obligations remain unchanged.

That 0.29.1 refresh adopted failed-sort refusal in grouped release metadata,
the native PocketIC dependency exception's transfer to Testkit, and consolidated
direct-edit and fixed-issue-index guidance. The maintainer's 2026-11-03 local
presentation override keeps fixed-issue links beside outcomes in each group's
main changelog, including after release; root `CHANGELOG.md` remains navigation
only. Shared snapshot payloads stay intact. Routine local corrections and tooling
adoption do not require new issues. Product dependency restrictions remain in force; this
refresh does not adopt a different Testkit or composition stack.

For that 0.29.1 cohort, all 152 payloads verified against its selected commit. The actual root dependency
boundary caller and CLI grouped release-metadata/order admission pass on the
unchanged manifests and lock. Shared's focused ordering case qualifies failed
sort refusal and selected-commit handling. Its focused native-client case covers
the four allowed Testkit SDK inputs, aliases, retired owners and tighter policies;
the general matrices stay with Shared.
ShellCheck with sourced companions passes. Evidence is under
`target/shared-tooling-0291/`. These are Linux checks, not native Bash 3.2 or
macOS qualification.

The preceding 0.28.3 cohort retains its UTF-8 caller evidence under
`target/shared-tooling-0283/`; its 31 rejection/expansion cases used substitute
stage bodies and did not qualify real full versus selected product plans.
Earlier adapter and product results retain their original snapshot/graph
cohorts. No old qualification or release receipt is upgraded by this refresh.

Broad gates and new native macOS qualification were skipped as user-owned
validation. Rust 1.99.1 remains the declared and installed compiler/Cargo; root
and isolated-adapter declarations agree. Tool catalogs/selections, dependency
declarations, lockfiles, compiler pins and product versions are unchanged.
No network lifecycle action occurred. Product-source checks are reused while
their relevant source/dependency/tool inputs remain unchanged.

The earlier Host Tools adoption selected published **0.80.0**, aligning its optional Platform
dependency with 0.20 while retaining Types 0.18. Its official archive checksum
and source commit `109244b35678e9d89ecec6963ab535c8d65b2f25` are verified;
the Rust sources match 0.79.0. Foundation 0.74.2 and Text 0.74.1 remain selected.
The CLI enables the existing `icp-project` capability and consumes bounded
`icp project show` output for exact environment/network facts. ICP retains parent
discovery, implicit configuration and recipe resolution; the shared parser owns
YAML interpretation. Unknown facts skip optional fixture funding, and the old
raw network scan is removed ([#616](https://github.com/dragginzgame/icydb/issues/616)).
The separate raw inventory fallback retains Host FS regular-file admission
([#615](https://github.com/dragginzgame/icydb/issues/615)).

Focused strict CLI/build lint, 179 CLI unit and 46 process controls, nine
artifact-helper controls and selected CLI/Core graph policies pass. A real
disposable ICP project queried from a nested directory resolves through the
published parser. New YAML dependencies remain outside the database Wasm graph;
the artifact helper retains `ic-interface`. Incoming Metrics 0.28.2 / Storage
0.66.1 lock selections and the isolated Canic lock are preserved. Evidence is
under `target/host-tools-080/` and `target/evidence/cli-effective-project/`.
This is Linux native qualification; broad gates, new macOS, composed-canister
artifacts and installed-IC checks were not run. IC cycles/instructions and total
uncompressed/code-section Wasm deltas remain unmeasured. No network lifecycle,
commit, push or release action occurred.

Private path-only release helpers remain as delivered in 0.295.1
([#602](https://github.com/dragginzgame/icydb/issues/602)); their prior archive
qualification is separate from the new snapshot's consumer checks.

The pending root graph selects published Platform 0.21.0, with official archive
checksum `e87f01cbae3cc6cbd5b0ffc705c9086618817e4f0045ce67821e3fcbbc338670`
and release source `9d5b6c6d0e9a88010a22d0267aa8395e7a7f6b93` verified. The
selected BLAKE3 source is unchanged; the supplier removes only its unselected
PocketIC facade surface. Runtime 0.19.0, Types 0.18.0 and all unrelated lock
selections are retained. Root locked metadata passes with one Platform version.
The separately retained Canic adapter manifest/lock remain unchanged; its
qualification and locally unavailable `canic-icydb` source stay outside root
release gates under the existing adapter exception and
[#373 — Move generated endpoints into the Canic adapter](https://github.com/dragginzgame/icydb/issues/373).

The 0.296.0 adoption selected published Platform 0.20.0, Runtime
0.19.0, Types 0.18.0, Metrics 0.28.0, Storage 0.66.0 and Wire/Derive 0.16.0.
The official registry archive checksums and source identities are verified:
Metrics `54a92f45c1008a56599ee42786d84553ca950215`, Platform
`cc8214bf6e2a3dcbe1649c1cd4e71d8a44925bdf` and Storage
`0b10639ed1c876d4eda50ef98241a369faffe4b0`.
Platform's Rust source matches 0.19.0 and Storage's matches 0.65.5. IcyDB selects
only Platform BLAKE3 and Runtime raw `ic0`; no PocketIC client, executor, CDK or
guard capability is introduced. Runtime and Types remain at their latest
independently published versions.

Metrics 0.28 removes its optional Wire metadata and dependency. The existing
IcyDB instruction-report DTO retains frontend ownership while Metrics continues
numeric recording/admission and Serde. Storage 0.66 aligns public codec types
with Wire 0.16, removing the older Wire/Derive 0.15 pair from the root lock.
This adoption introduces no new stored-byte format, reset or migration; the
existing fresh-installation-only Storage contract below remains binding.
The maintainer authorized Types 0.18 under
[#611](https://github.com/dragginzgame/icydb/issues/611) in the current 0.296 line.
Public conversions and guard values use the canonical Rust identity; IcyDB-owned
Account/Subaccount protocols remain unchanged. See the
[current tracker](../design/0.296-platform-types/0.296-status.md).


The Metrics 0.28 / Platform 0.20 / Storage 0.66 cohort passes strict Core
library/test, CLI all-target and binding-generator lint; 401 focused Rust tests
(171 Core storage/metrics/format controls, 180 CLI unit and 41 process controls,
six facade admission and three bootstrap controls); public/codec Wasm compilation;
and the unchanged browser baseline (12 methods, 228 native types, 91 exact frames
and 3,145 truncated frames). Actual native CLI and Wasm graphs contain one current
Wire pair, one Runtime and one Types identity, with no SDK, executor, guard or
PocketIC expansion. Dependency policies, notes selection and 27 release-plan
rejection/expansion cases pass. The database metadata selects its pending 0.296
notes and the qualification mapping contract is refreshed for that path only.
Published notes, product versions and independent Canic inputs are preserved.
Full gates, current native macOS, installed Canic and final release artifacts
remain unqualified by these focused checks. IC cycles/instructions and total/code
Wasm-size changes are unmeasured. Evidence is retained under
`target/dependency-adoption-20261103/`.

The preceding Types 0.18 / Wire 0.15.1 cohort passed selected Schema/Core/facade lint, 128 focused
conversion/wire/storage/key/guard tests, actual native/Wasm singleton graphs and
public/codec Wasm compilation. The reviewed browser contract is unchanged across
all 12 methods and 91 exact frames, including its schemas, identities and budgets.
Published 0.295 notes and the independent Canic manifest/lock are preserved.

The first direct Wire 0.16 probe failed with seven native Metrics trait errors.
The existing owner decision in Observability #128 and
[IcyDB #614](https://github.com/dragginzgame/icydb/issues/614) supplies the repair:
IcyDB's instruction-report DTO projects canonical public getters into its own
serialized fields/native schema and delegates decoding to Metrics admission.
Recording state remains canonical and separate from frontend snapshots; existing
report getters still return canonical measurements. Metrics' optional native
metadata is disabled, and the generator registers the service DTO under the
unchanged client name. No arithmetic, numeric validator or foreign Wire trait
implementation is copied. That earlier cohort shows Metrics with no features and
Storage as the only parent of Wire 0.15; IcyDB-owned interfaces use Wire 0.16.
The initial failed probe remains in `/tmp/icydb-wire016-core-check.log`; subsequent
qualification has its own Wire 0.16 cohort rather than relabelling earlier tests.
That cohort passes selected Core/CLI/generator lint, 154 Core report/store/codec
tests, 23 CLI controls, 18 Schema atom/wire controls, 12 facade guard/envelope
controls, public/codec Wasm checking
and the unchanged 12-method/91-frame browser baseline. The optional Platform-types
module is the only published Rust source difference from Wire 0.15.1 and remains
disabled in the product graph. Full gates, macOS, installed Canic and cost
measurements remain outside these focused checks.

The preceding Types 0.17 cohort passed strict Core SQL/migration and
feature-disabled lint, 1,057 selected query/storage/schema/codec/recovery controls,
128 feature-disabled controls and three facade bootstrap tests. Locked metadata,
declaration/graph policies and actual Wasm/native CLI graphs pass, as does public
SQL/migration/metrics and codec-actor Wasm compilation. These are focused local
checks, not a full product gate, hosted CI or installed-Canic qualification.

Storage 0.65 replaces the retained map engine with `ICMAP002` B-tree framing.
This remains a **fresh-installation-only** adoption. Previous map framing rejects
without reinitializing retained stores or schema-control regions; there is no
migration, reset, deletion or deployment. Same-contract restart and recovery
remain required. The independent Canic workspace retains its existing lock and
supplier cohort; root checks do not qualify that composition. Adoption
evidence for the initial Storage 0.65.0 graph is retained in
`.cache/platform-wire-retry/`. Its focused Linux checks passed:
247 Core persistence/schema/metrics tests, nine facade admission/bootstrap tests,
13 CLI observability controls, strict Core test and Core/facade/CLI library lint,
selected Wasm compilation and the unchanged browser
baseline (91 native frames plus truncated/malformed admission controls).
Broad repository, native macOS and installed-IC qualification remain user-owned;
cycle/instruction and uncompressed/code-section Wasm-size deltas are unmeasured.

The complete pre-cleanup record remains in Git at commit
`b1dd1a5ba9a425bff9fbde06d639f714a3ec420d`, at this document's path.
[Earlier published records](https://github.com/dragginzgame/icydb/blob/9ac30667d26728a539e8c640bf2c14b4e1406ba7/docs/governance/shared-tooling.md)
are also available online. Historical versions, failures, temporary evidence
paths and qualification claims apply
to their recorded inputs. Future adoption replaces the current summary here;
release notes and owning GitHub issues retain detailed delivery evidence.

## Compiler-policy adoption

The maintainer approved [#600](https://github.com/dragginzgame/icydb/issues/600):
all owned packages require Rust 1.99.1, which is also the pinned compiler for
development, lint and release qualification. Shared Tooling owns
that same published support policy. The inherited declarations, shared floor
reader and actual Cargo/rustc admission remain the consumer contract; exporting
a new baseline alone is not new package, archive or native-host qualification.
The delivered root outcome is complete; #600 is consolidated into the existing
[#373](https://github.com/dragginzgame/icydb/issues/373) for the remaining actual
independent Canic compiler/host/artifact qualification, which is not waived.

IcyDB's consumer setup action reuses the host installer and shared floor reader,
then admits actual Cargo/rustc identities. Isolated public feature/native/Wasm
and external Canic consumer checks run inside existing workspace lanes, replacing
duplicate compiler-only jobs. The [0.295 tracker](../design/0.295-compiler/0.295-status.md)
records current evidence and qualification gaps. Earlier compiler receipts below
remain bound to their historical source, graph and toolchain.

## Shared installer and retention adoption

The snapshot includes the canonical native-package classifier, tool-retention
helper and owned-tree companion. Explicit setup tracks checkout/tool requirements
and bounds regenerable compilation scratch. Selected executables and receipts
stay under Shared's admission contract. Linux reuse fingerprints relevant native
build packages rather than every installed application. Consumer checks remain
read-only; setup registers requirements. Obsolete executable reclamation remains
a separate previewed, quiescent operation. No maintenance apply, workspace-cache
migration or retained-evidence deletion is part of this adoption.

Optional group validation plans, validation-proof reuse, grouped bootstrap observation and product
compiler-cache maintenance are not new IcyDB release modes. Required companions
supply their documentation/helpers without a duplicate local implementation.
IcyDB retains its complete existing release gates and repository-wide CI; no
group selector or plan-based omission is enabled. The runner's required plan
companion is included in both isolated consumer adapter fixtures.
The existing common formatter tools and exact locked release editor are the real
setup/check probes. Source/byte/native admission must pass before use; newly
admitted tool-build scratch follows the installer's bounded retention contract.

Retained release/CI fixture roots remain outside Cargo output because failure
source and recovery evidence must survive compiler cleanup and remain visible
to the existing collectors. Successful fixture owners remove their own trees;
failed evidence follows explicit consumer retention. These roots and Cargo's
long-lived download cache are not tool-build scratch eligible for Shared's
collector. New regenerable workspace scratch follows the baseline's `target/`
default. No existing evidence root is relocated or deleted by this adoption.

The 0.27.27 focused Linux snapshot and consumer adapter checks pass for all 148
payloads and current positional-runner routing, failure propagation and evidence
retention. Evidence is under `target/evidence/shared-02727/`. This does not qualify
an IcyDB group plan or native macOS. Prior 0.27.26 checks below remain bound to
their original inputs; unchanged tools and dependency selections reuse that evidence.

Prior 0.27.26 focused Linux adoption checks passed: all 145 payloads verified; dependency roles
and external-IC boundaries pass; the real pinned formatter tools and locked
IC Build 0.73.6 release editor pass read-only admission after explicit setup.
The new native identity initially refused the prior selections, as intended;
setup reused already admitted compatible generations without changing versions.
Actual formatting, lock selection with the real editor, substitute release
routing, current-source metadata admission, six compiler controls and 123
workflow controls pass. The metadata fixture now exports the current graph
checker rather than silently executing its archived HEAD implementation.
Evidence is under `.cache/shared-02726/`. No obsolete-generation reclamation,
application build, broad gate, release, native macOS or installed-IC run is
claimed; earlier application/compiler receipts retain their exact inputs.

## Current capability boundary

IcyDB owns accepted-schema authority, query planning/execution, storage/key
formats, recovery semantics, its database facade and application-model generation.
The CLI and test actors consume those contracts. The root workspace contains
23 database, model, CLI and framework-neutral helper packages. The maintainer
approved [separate Canic qualification (#596)](https://github.com/dragginzgame/icydb/issues/596)
on 2026-10-31: 31 packages use
[the adapter workspace](../../testing/canic-adapter/README.md) and its lockfile.
All existing package directories remain under `crates/`, `canisters/`, `schema/`
and `testing/`; no package directory was relocated. This is a specific exception
to [the approved layout](../../rules/rust-workspaces.md#approved-icydb-layout).
[IcyDB #310](https://github.com/dragginzgame/icydb/issues/310) retains future
placement decisions.

| Capability | Canonical owner | IcyDB integration retained |
| --- | --- | --- |
| IC exports, standalone lifecycle and startup timer hosting | Canic's `canic-icydb` adapter | Private database handlers, explicit mounting and installed boundary tests |
| Native payload/envelope encoding, sizing and TypeScript runtime | IC Wire | Endpoint identities, concrete metadata, resource policy and scalar validation |
| Principal/Account identity, text and Serde | IC Platform Types | Database atom conversions retain database-specific identity/encoding |
| IC0 system calls | IC Platform Runtime | Explicit `ic0` feature; no executor or CDK selected |
| Cryptographic and external IC adapters | IC Platform | BLAKE3 for database fingerprints; no workspace-owned direct SDK use |
| Governed allocations, collections and stable codec interfaces | IC Storage | Database keys, host grants, role bindings, codecs, indexes, transactions and retained-control admission |
| Application instrumentation | IC Observability (`ic-metrics`) | Phase/entity attribution and resets; IcyDB owns its report validation and display arithmetic |
| Cargo/Wasm acquisition and retained caches | IC Build | Actor recipes, optimizer/interface policy and report budgets |
| File/process/text primitives | IC Host Tooling | Product artifact admission and CLI presentation |
| PocketIC setup, admission, gateway and lifecycle | IC Testkit | Fixture topology, scenarios and native Wire calls |
| Installers, release plumbing and structural scanner | Shared Tooling | Local Make inputs, release callbacks and source inventories |

Applications explicitly mount `canic_icydb::start!` and `canic_icydb::mount!`.
The codec-only audit's raw export isolates its graph; it is not an application
endpoint owner. Complete managed memory/lifecycle composition remains with
[IcyDB #373 — Move generated endpoints into the Canic adapter](https://github.com/dragginzgame/icydb/issues/373).
Native database operations and precommit reply admission remain IcyDB contracts.
Consumer lifecycle, rollback, browser and recovery tests remain necessary.

## Dependency selection

The root and Canic qualification workspaces resolve independently, with separate
lockfiles. The root graph contains no Canic, CDK, timer runtime, Candid, big integer
or PocketIC packages. Root declaration policy also forbids Canic through aliases,
normal, build, dev and target-specific dependencies. The same checker limits
direct Platform use to Core and the two BLAKE3-based oracle helpers. Schema uses
Types, the facade uses Types/Runtime, and the IC0-only actor uses Runtime.
Types and Runtime have explicit consumer allowlists, including Core's test-only
identity conversions. The resolved-graph guard refuses duplicate Runtime or
Types versions. These owned packages do not relax the external SDK inventory;
direct Candid/CDK/substrate dependencies remain forbidden.
The current root schema family selects Wire 0.16, Metrics 0.29 and Storage 0.66
with Platform 0.21, Runtime 0.19 and Types 0.18. All use the pinned Rust 1.99.1. Storage's map
format changes under the fresh-installation contract above; Wire protocol bytes,
schemas and budgets retain their published contract.
Earlier qualification below belongs to its original supplier selection.
The independent Canic qualification stack keeps its own selections. Current
root qualification passes 33 Core metrics/allocation/retained-control tests and
12 CLI metrics controls, strict native/default-feature lint and default/combined
Wasm compilation. Explicit graph checks preserve optional Metrics and singleton
Memory/Storage. These are Linux/native or cross-compilation results; installed
IC and native macOS remain separate. The registry archive review, lock delta
and commands are retained in `target/evidence/persistence-metrics-20261102/`.
Shared guidance for these independently published owners is tracked in
[Shared Tooling #342](https://github.com/dragginzgame/shared-tooling/issues/342).
The common checker already admits the owned packages; that documentation update
is not an enforcement blocker or a raw-SDK exception.
Native clients use IcyDB's
internal RPC method identities and IC Wire; all 12 methods are checked against
the published adapter for exact frame interoperability. This is a client protocol
projection, not a second exporter, authorization layer or codec implementation.

The qualification graph owns Canic-dependent actors, serving helpers, endpoint
compile fixtures, lifecycle and installed tests. It consumes current root packages
through repository paths and keeps its own aligned supplier selections. A new
supplier combination requires adapter qualification; separating resolution does
not claim that arbitrary Canic/SDK versions interoperate.

Product test, lint, formatting, required CI and release preparation use the root
graph. The explicit adapter gates and path-scoped `Canic qualification` workflow
retain endpoint, lifecycle, installed browser, adapter MSRV and actor size proofs.
An older qualified matching stack can remain at that integration owner. When
using current repository paths, requalify the changed core/adapter pairing before
composed deployment; do not use an old installed pass as evidence for new source.
Root release projection leaves the adapter lock unchanged. Admin-authorized CLI
inspection on 2026-10-31 found `main` unprotected, no repository rulesets and no
effective branch rules, so no global Canic check needed removal. No remote
setting was changed. If configuring protection, follow the product-only check
selection in [branch-protection guidance](../../.github/BRANCH_PROTECTION.md).

The #596 acceptance probe changes only the root Platform requirement to published
0.11.1 in an isolated current-source copy. Its root graph resolves and eight Core
fingerprint tests pass without changing the Canic catalog or lockfile. This
qualifies independent core development; it does not qualify the updated Core
against the older installed adapter stack. Consumer fixtures also reject Canic
aliases and resolve the real root graph with an invalid qualification manifest.

Within the root graph, Internal published
IcyDB edges use exact matching versions because generated code and schema
contracts move together. The bundled SQLite oracle has its own exact constraint;
it is host-only. [Pin exceptions](../../ci/dependency-pinning-exceptions.json)
record both reasons; release tooling updates only release-owned values.

Each workspace has an explicit dependency catalog. Canic and Timers are declared
only in qualification. Ordinary builds and validation use locked inputs. Prepare
an authorized root dependency change with `make fetch`; use `make fetch-adapter`
for qualification before offline validation.
Do not introduce sibling path overrides or silently update inputs during a gate.

`make check-dependency-pins` enables Shared's Cargo-inheritance checks across
normal, build, development and target-specific declarations. `check-invariants`
enables the common [external-IC inventory](../../ci/external-ic-dependencies.txt);
[local policy](../../ci/cargo-dependency-boundaries.json) adds stricter bans.
The local graph guard retains coupled pins, sensitive version uniqueness and
release-specific exclusions. Declaration/graph checks do not prove final Wasm
contents. The pending retained-artifact correction in
[#378](https://github.com/dragginzgame/icydb/issues/378) adds admission to the
single, variant and batch canister builders before ICP staging. Each acquisition,
including cache hits, rechecks the exact target/features through Shared's locked
normal/build graph checker and binds that observation to IC Build's retained
source identity and guards. Logged evidence includes the raw lockfile hash and
compiler/final artifact digests; an older observation never authorizes a new
acquisition. No policy-result cache or second dependency parser is introduced.

The consumer exclusion list rejects build/test drivers, simulator infrastructure
and native oracles. Intentional actor schema fixtures, codec helpers and Canic's
host/schema build dependencies remain permitted. Local standalone fixtures cover
alias/transitive refusal, selected build edges, inactive target/development edges,
cache reuse and changed source/profile/features/artifact bindings. This does not
qualify actual Canic compatibility or every maintained actor recipe. Those and
native macOS/installed qualification remain open in #378; ordinary product
package resolution stays independent of the adapter.

Tool versions come from the [shared host/Rust catalog](../../ci/tool-versions.env),
[IC archive matrix](../../ci/ic-tools.tsv) and [IcyDB utility selections](../../ci/icydb-tools.env).
After preparing Rust and locked dependencies, `make install-tools` explicitly
prepares the selected product tools; `make tools-check` admits them offline.
Canic qualification additionally selects `make install-testkit testkit-check`.
The [setup guide](../../INSTALLING.md) owns prerequisites and workstation usage.
Validation does not install missing tools. Cargo homes and target directories
come from the root Makefile; concurrent work must isolate them.

Testkit alone selects, provisions and admits its server. Shared selects the
Testkit CLI from the qualification lock; IcyDB calls its setup/check interfaces. Retain no independent
PocketIC pin, installer or version parser. Binaryen archive provisioning and
IcyDB's [raw executable admission](../../scripts/ci/wasm-optimizer-checksums.tsv)
are separate obligations; artifact transformation remains explicitly qualified.

## Consumer tooling qualification

The delivered 0.293 adoption requires published Memory/Storage 0.61.0 and uses
checked runtime keys, nonzero bucket configuration and explicit collection
`create`/`load` lifecycle. Valid retained bytes remain compatible; no reset is
performed. Canic qualification keeps its independent dependency selections;
its older pinned supplier stack is not qualified against this public 0.61 cut. See the [adoption status](../design/0.293-memory-storage/0.293-status.md).

The delivered 0.294 adoption selected published Platform 0.17.0, Types 0.16.0 and
IC0 Runtime 0.17.1. Root consumers now depend directly on Types for identity and
Runtime for IC0 rather than the delegating facade. Core retains Platform only
for BLAKE3. The selected registry Platform revision is
`35cffb6f0ab768e00b0f11d6429eff4d7d7e252c`. Public Principal/Account
conversions require the new Types identity; bytes and database formats remain
unchanged. External Principal conversions are explicit test-only features;
IC0 remains executor-free. See the [adoption status](../design/0.294-platform/0.294-status.md).

Earlier in this pending batch, the root selected Wire/Derive 0.14.2, Metrics 0.26.0 and Memory/Storage
0.63.0. It retains Build and Host Artifacts/FS/Process/Text 0.73.3 requirements
(locked to 0.73.6) and Host Tools 0.77.0. The requested persistence/measurement
refresh changes only those three lock identities; no extra supplier alignment
is required. Their published sources are respectively
`d545eccd9dbb52fa32dbc7716b67a0e729619ef1` and
`1b6dab1d07ad92f58843ae68990e530cc572ee1f`. Retained bytes and runtime bodies
are unchanged, so no reset or format migration is required for this adoption.
Direct public-value consumers must align their Rust crate generations. The
independent Canic graph keeps its older selected stack and existing qualification
gap; it does not block root adoption. The earlier Host Tools refresh selected
0.77.0 from 0.76.2 without response/interface changes.
Published source revision `d2b14582ab0b6dcb3ef6e72094b8f7da4b195c1a`
has identical response/interface implementations. CLI response decoding and
Build's explicit `ic-interface` feature select no Platform-backed features;
the supplier's new optional Platform 0.17 identity boundary needs no local adapter.
The independent qualification graph retains Testkit 0.75.0. Foundation and IC
integration release independently; these versions need no numeric lockstep.
The earlier 0.294 schema/facade imports used `ic-platform-types 0.16.0` directly; the
Platform facade re-exports that exact type identity. Memory/Storage 0.63.0 use
Platform 0.17.0 for their unchanged stable-structures facade. The root resolves
one facade, one IC0 Runtime and one canonical Types generation,
with no Canic, CDK, executor, Candid or supplier path override. The published IC0
implementation is byte-for-byte unchanged after extraction; BLAKE3 and Account
implementations are also unchanged. Unselected crypto/certification, executor
and native-client changes are outside this adoption.
Earlier Metrics' `native-wire` feature selected Wire 0.14 on native hosts and no Wire
metadata dependency on Wasm. Optional instrumentation/recording stays optional;
durable adapters remain Persistence-owned and journal debt is independent.
Published Metrics source `90615b5123552ac325bf58873f7e181f4c112109` preserves its
arithmetic and transport implementation; Wire source
`72c2be688afe129cf2a8ebeeb0e648be9d86c13e` changes only the optional Types adapter.
This resolves [Observability #120](https://github.com/dragginzgame/ic-observability/issues/120)
without changing the maintained numeric shapes or browser baseline.
Published Memory/Storage 0.62.0 source
`22eda8ae8b5998e0872630ee3a888ed959f8dc8a` preserves both complete implementation
directories from 0.61.1 and aligns public codec-error sources with Wire 0.14.
Valid retained bytes, bounds, allocation custody and recovery remain unchanged;
no reset is required. Public Memory values and error sources require matching
dependency identities and consumer rebuilds. The incoming Wire/Derive 0.14.1
source also matches 0.14.0 exactly; its supplier changes qualification only.

Memory/Storage 0.61.0 changes Rust API identity while retaining valid allocation
ledger and collection bytes. That original slice preserved incoming tooling
updates without claiming their separate qualification. Host Tools 0.77.0 now
has its own strict consumer lint, 30 focused CLI/artifact tests and Rust 1.96
tooling checks. Native macOS and installed Canic qualification remain separate.
Wire 0.13.3 added an opt-in bulk ByteArray contract; IcyDB retains its existing
ordinary arrays and generated codec contract. The existing
0.56 allocation-ledger and 0.291 IcyDB format cuts still apply; no converter,
ledger-only reset or storage migration is introduced. Wire 0.13 uses the canonical
Platform Types owner and moves transport endpoint hosting into its endpoint
package. IcyDB keeps application codecs on Wire and hosting in Canic; it does not
add another exporter. Existing application IDs, byte/node/depth/allocation limits,
scalar validation and the reviewed browser baseline retain their owners. Exposed
Rust type identities change; align consumers and rebuild as recorded in the
released 0.292 notes. The 0.292.1 patch preserves those Rust identities and the
existing features. Package versions remain release-owned.

Strict root consumer lint, native schema/facade/admission/metrics tests and
Node-generated browser frame checks qualify the current root family. Memory
error projection tests obtain its codec refusals from Memory's own test-support
producer, rather than constructing them with another Wire minor. That test-only
feature does not become a production codec adapter.

Canic qualification retains registry adapter 0.113.0 from
`247ba9d3599658c478373cb0dfc74439a859dd8a`, Platform 0.8.3 and Timers 0.32.4.
Its direct Memory requirement remains 0.60.0; direct fixture Wire now selects
0.14.2 to match current IcyDB limits. Canic's raw-serving callbacks are
codec-generic. The independent lock has been reconciled with current root path
dependencies, including Memory/Storage 0.63.0 and Metrics 0.26.0, while retaining
Canic, Timers and Testkit selections. Locked metadata, strict fixture-serving
and endpoint-fixture lint, both async request endpoint-fixture tests, and Wasm
serving-library compilation pass. Generated metrics endpoints still fail four
Wire-limit checks; the earlier three integration lifecycle encoder failures
remain unqualified. Canic's own helpers expose Wire 0.7 limits. Its old installed
pass does not qualify these changed inputs. No conversion shim, supplier patch,
guard weakening or removal of endpoint/lifecycle coverage is used. Actual
composition and installed acceptance remain pending
[Canic #590](https://github.com/dragginzgame/canic/issues/590) and
[IcyDB #373](https://github.com/dragginzgame/icydb/issues/373); this does not block
root package resolution or its focused checks. Published Timers 0.34.0 also
cannot resolve with this adapter's `^0.32.1` requirement: both claim the same
`ic-timers-global-timer` singleton. Preserve that fence and the isolated old
runtime/codec selection until Canic supplies a compatible adapter. Host and
persistence upgrades do not establish endpoint or installed runtime qualification.

The earlier 137-payload snapshot matched committed source
`143a9abb9ae81feae9400ff3844b03b1ddfdd75a`, verified by the canonical exporter.
The current 148-payload selection is recorded in the provenance above.
At snapshot adoption, release groups were not yet enabled. The subsequently
approved consumer split is described below. Fixture setup derives the pin
checker's new release-group companions through the canonical closure helper.
Platform's type owner may select registry Principal conversion; IcyDB's stricter
SDK declarations and deployment exclusions retain their existing policy.
Consumer Make, grouped-formatting, selected-source metadata and release-pin
fixtures pass. Product release preflight fetches only its root lock; the adapter
owner prepares and qualifies its own graph independently. The actual
snapshot-adoption pin-gate evidence recorded the then-untracked qualification
lockfile from [#596](https://github.com/dragginzgame/icydb/issues/596); both the
previous and adopted checker rejected it. The lock is now tracked; this
reconciliation does not rerun or claim release-pin qualification. Fixture Git
state qualified the snapshot policy without staging real pending files or
weakening tracked-lock admission.

Explicit `make clean` now lets Cargo clean the complete workspace `target/`,
including isolated compiler outputs, Wasm caches and evidence. Explicit caller
target selections retain Cargo's normal meaning. Build/release commands do not
clean automatically; `.cache/` and `.tools/` remain separate stores. Keep evidence
that must survive cleanup outside `target/`. Consumer fixtures use real Cargo
only in disposable cleanup workspaces and preserve a separate cache marker.
Native macOS qualification of this adoption remains outstanding. No package
versions or dependency selections changed during snapshot adoption, no release-group policy was enabled,
and no cleanup ran against this working checkout. Scheduled maintenance remains disabled.

The following earlier qualification records belong to their named source and
supplier identities, not to the updated root or its blocked Canic composition.

Earlier published Tooling 0.70.4 identifies
`b275fe7286115e4eb806e58d3fbbe6c22ab5ed2c`; Platform 0.8.3 identifies
`f69a759bb94e84f054d5f8701461d745b8f8eed4`; Metrics 0.17.6 identifies
`87759e723b101dea7b483806446c00eff1016c0a`. All declare Rust 1.91.
Their consumed interfaces remain supported: Host retains file/process APIs,
Build retains artifact admission, Platform consolidates external Account text
formatting, and Metrics refactors histogram bounds outside IcyDB's measurement
consumer. Tooling 0.70.4 changes only Host Process test-support implementation
relative to 0.70.3; those HTTP/socket helpers are not consumed here. No supplier
wrapper or new runtime mode is added.

Published Memory/Storage 0.56.0 identifies
`956b2b92728f82f75dd51d4d10ed52f325529e91`. Memory's bounded Wire allocation
ledger and Cell value reject prior CBOR images. The maintainer approved the
source hard cut: installations need fresh/reset memory or a separately reviewed
transition before installation, never a ledger-only reset over retained
collection bytes. No reset or deployment is performed by this adoption.
Memory's numeric slot/custody and manager/Cell substrate contracts remain
unchanged; IcyDB's separate artifact and Deep-job cuts are documented in the
[persisted-format inventory](../contracts/PERSISTED_FORMAT_INVENTORY.md).

Earlier published Tooling 0.70.0 archives identify source
`ecc8d61d19f0b711788f7e10e5dd728d0db6ff60`; Platform 0.8.1 identifies
`10c2610511d6a5525816185c999b25fd159c1d89`. Their declared MSRV remains Rust 1.91.
Tooling preserves the consumed file/process/build interfaces and Testkit's
external Principal interface. IcyDB does not consume Tools' changed ledger-value
identities. Platform's Principal packaging changes the public Rust identity
exchanged through read-authorization contexts and atom conversions: consumers
must align on Platform 0.8. Canonical Principal/Account text, bytes and native
Wire encodings are unchanged. See [0.291 adoption notes](../changelog/0.291.md).

Ordinary library consumers select the minimal owned Principal capability.
Facade tests explicitly select external conversion and self-authentication via a
development dependency. Authorization fixtures select self-authentication where
needed. Testkit remains the sole server setup/lifecycle owner; no new codec,
application protocol, host parser in canisters, or supplier wrapper is added.
The public IcyDB Wasm graph passes the explicit single-Platform identity and
Candid/CDK/bigint exclusions, including exclusion of the optional Principal
conversion/hash features. These graph checks cover normal/build edges; they do
not establish the dependency graph of a separately composed Canic application.
The authorization-actor graph still includes Candid through Canic's pinned
Platform 0.6.21 dependency, separately from IcyDB's minimal production graph.

The Host/Platform adoption qualified Wire 0.7.22 and Metrics 0.17.3 with
246 focused Linux tests: 23 Schema atom/Account tests,
8 facade authorization tests, 13 bootstrap tests, 2 endpoint-declaration tests,
179 CLI tests and 21 build-adapter tests. Strict affected library/test, integration
and guard-actor lint passes. The public Wasm library checks on Rust 1.91 with
SQL, metrics and migration, and the CLI checks on its declared Rust 1.96 minimum;
all three authorization actors check for Wasm with
their guarded features. Declaration pins, dependency boundaries, graph invariants,
manifest ordering and the substitute Testkit CLI selection fixture pass. Native
macOS, installed-IC and full workspace/release qualification are not rerun here;
substitute fixtures do not qualify a real PocketIC server. Wasm-size and IC
instruction/cycle deltas remain unmeasured. Subsequent incoming
Tooling/Platform/Wire/Metrics lockfile updates are preserved; that earlier adoption
matrix is not qualification of their host, browser or installed-IC behavior.

The Tooling 0.70.2 consumer checks pass strict CLI/build/Wasm-helper target lint,
integration test compilation against Testkit, 207 focused host tests and the CLI's
Rust 1.96 check. Those runtime results used Memory/Storage 0.55.2; they do not
establish compatibility of retained CBOR installations with 0.56.

Memory/Storage 0.56.0 passes 184 focused Linux tests covering runtime binding,
allocation diagnostics, database-format admission, Data/Index/Commit/Schema
stores, public error projection and host memory admission/reopening. Strict
Core/facade library/test lint and the public Rust 1.91 Wasm check with SQL,
metrics and migration pass on the new graph. Shared declaration boundaries,
dependency pins and graph invariants pass. The minimal and full-featured public
Wasm normal/build graphs each use one current Memory/Storage/Platform identity
and exclude Candid/CDK, bigint, Host packages, `serde_cbor` and `half`.
The full workspace and composed Canic graphs can still retain upstream packages.
Evidence is retained under `target/evidence/upstream-adoption-20261029`.
No native macOS, installed-IC, earlier-format transition, broad workspace/release
gate or Wasm-size/instruction/cycle qualification was run for this adoption.

The 0.70.4 / Platform 0.8.3 / Metrics 0.17.6 adoption passes strict selected
Core, CLI and build library/test lint; the shared adapter fixture, including
changed/missing input refusal; dependency declarations, pins and resolved-graph
checks; and grouped manifest ordering. Rust 1.91 public-library Wasm and Rust
1.96 CLI cross-version checks pass. Browser Node qualification preserves the
reviewed 12-method, 231-type contract and all 90 frames, rejecting 3,103
truncations plus malformed/value/limit controls. The public Wasm normal/build
feature graph excludes Candid/CDK, bigint, Host and CBOR, including Platform's
external Principal and self-authentication features. This is graph admission,
not final-artifact exclusion enforcement; #378 remains open. Focused runtime
results and exact source identities are retained under
`target/evidence/upstream-20261030`. No broad workspace/release gate, native
macOS, installed IC/Chromium, Wasm-size or IC instruction/cycle qualification is
claimed. No network lifecycle action was required.

### Retained earlier qualification

Wire 0.7.21 passes five endpoint/lifecycle tests, five generator tests, strict
generator lint, dependency-boundary checks and the Rust 1.91 Wasm check with
SQL/migration enabled. Node contract qualification preserves 90 exact frames
across twelve endpoints and rejects 3,112 truncated frames plus malformed/value/
limit controls. This does not qualify installed Chromium, installed IC or macOS;
the isolated Canic composition checkout still selects Wire 0.7.20.
Wire 0.7.20 and Timers 0.31.16 pass five generator tests and two installed
watchdog rearm/rollback-retry cases through Testkit 0.69.6 on Linux. This focused
consumer evidence does not qualify native macOS or the complete browser suite.
Memory 0.53 requires one runtime per physical backing and coordinated consumer
dependency identities. Full manager checks occur at construction, initial
bootstrap, lease opening and growth; ordinary IO trusts exclusive custody.
Published 0.53.0 passes 16 host admission cases, all nine installed grouping
cases and the full collection/upgrade case with unchanged budgets on Linux.
Focused strict lint, dependency policy and the public Rust 1.91 check also pass.
See the pending [adoption notes](../changelog/0.287.md) for source provenance
and the narrowed raw-alias detection contract.

Tooling 0.69.5 registry archives all identify source commit
`c5b6fae87c6785e831509667ca71617953174604`. Focused Linux qualification passes
strict CLI/build-helper target lint and the Testkit integration library check,
67 CLI rendering/diagnostic tests, 41 actual CLI-process tests and 21 build-helper
tests, including cache reuse and failed artifact publication. The CLI also checks
on its declared Rust 1.96 minimum. No separate Build/Testkit executable pin is
maintained by this consumer. Native macOS, installed browser/PocketIC workflows,
full gates and performance deltas have not been requalified for this graph;
the earlier release evidence below belongs to its recorded versions.

Platform 0.7.0 removes Candid frontend compiler exports that this workspace does
not use. Its Principal, Account, executor, ic0 and CDK entry modules match the
published 0.6.21 sources. Both runtime adapters use the same registry
`ic-cdk-executor` 2.0.0 identity. Memory/Storage 0.51.9, Timers 0.31.9 and
Tooling 0.69 select Platform 0.7.0. Canic still selects Platform 0.6.21
transitively; the two package identities are explicit in the lockfile.
The generated SQL/schema authorization seam normalizes canonical caller bytes
through IcyDB's checked Platform constructor, avoiding a shared nominal principal
type requirement with the pinned adapter. Updating that adapter is not a
prerequisite for the current authorization integration.
The selected Core Wasm normal graph has no Candid; the existing adapter's external
entropy boundary and host Testkit graphs retain their upstream Candid selection.

The earlier Tooling 0.69.0 qualification used sources published from
`f268aef5d205e51811e934648afcd13f026635f1`. Its seven consumed package archives
contain unchanged Rust sources relative to 0.68.12; the coordinated release
aligns their Platform-owned Principal identity with IcyDB's existing 0.7
selection. Earlier Tooling 0.69.3 was published from
`e3a71a922b1604f53b3e5a1530e154aec96e2a81`; all seven consumed
registry archives were prepared without changing their existing locked selection.
Its Testkit exposes mutable client access while retaining instance/server custody.
The three installed browser fixtures consume that API directly, removing local
client/server splitting. That qualification used Platform 0.7.0.

Shared Tooling 0.27.3 adds independently selected source-commit verification,
report-only unattended maintenance with restricted permissions, and an optional
sccache 0.19.0 catalog entry. IcyDB does not enable sccache or start scheduled
maintenance through this adoption. Any installed maintenance schedule requires
the upstream host-qualification procedure before resumption; snapshot checks
alone do not qualify an installed schedule.

The snapshot retains the structured runtime-scanner CLI
([#476](https://github.com/dragginzgame/icydb/issues/476)). The existing private
compiler package adds the inherited Clap dependency; its locked version and
features were already selected elsewhere in this workspace. No deployed package
gains that edge. The consumer fixture checks the actual binary's help, argument,
path and exit-status handoff while retaining IcyDB's source inventory. The full
shared scanner matrix remains upstream; the scanner source and guide are direct
snapshot roots. Its test runner is retained only to satisfy the canonical guide's
local link, not invoked by IcyDB's gate. The
snapshot also retains the shared hook and formatting driver; no standalone-file
formatting option is selected locally. The 0.26.26 driver uses its declared lexer
companion to distinguish balanced conditional attributes from actual module-path
metadata before reusing formatting results. IcyDB's real Make/hook fixture
includes an ordinary conditional attribute followed by an unrelated `path` name.
The scanner consumer fixture follows 0.26.25's native filesystem contract:
representable non-UTF8 filenames scan successfully; hosts rejecting creation
still qualify source-IO refusal through the raw argument. Native macOS
qualification remains with CI. The 0.27.2 snapshot also includes the canonical
fixture companion-closure correction and native prerequisite ordering. Shared
dependency findings belong in one supplier-owned remediation issue per root
cause; consumer issues require independently actionable local work.

Shared Tooling 0.26.23 owns Rust formatting orchestration. IcyDB selects
`FORMAT_RUST_ARGS := --sort-derives`, replacing its paired shell commands with
the shared grouped-manifest, derive and Rustfmt sequence
([#473](https://github.com/dragginzgame/icydb/issues/473)). The selected compiler,
repo-local Cargo home/target and prepared offline admission remain unchanged.
Manifest sorting now precedes derive sorting. Ordinary write mode retains
successful earlier writes on a later failure; the index-export hook isolates
those writes and refreshes selected files only after every pass succeeds.
The shared driver owns per-pass progress, retained failure logs and exact-input
success receipts; hook exports always run the passes. Progress durations are
informational, not performance qualification.

Focused Linux checks pass snapshot integrity, actual consumer formatting and
hook preservation, shared/release adapter dispatch, selected-source release
metadata, workstation orchestration and shell/Perl validation. The formatter
fixture qualifies retained exit-43
failure logs, manifest-before-derive partial writes, real-tool input reuse,
source invalidation and forced execution. The hook checks reject partial staging
and preserve role groups, lockfiles and unrelated edits, including unusual paths.
These script results do not qualify native macOS, installed IC/browser behavior
or the full release gate; the current installed-browser evidence is recorded
separately below. Locked dependency and own-package versions are unchanged.

The production report delegates gzip encoding to IC Tooling. Its shell custody
fixture uses a tiny level-1 member generated by the same published helper rather
than invoking another encoder
([#475](https://github.com/dragginzgame/icydb/issues/475)). The fixture does not
qualify production compression bytes or ratios.

The earlier Shared Tooling 0.26.22 adoption changed governance payloads, including
owned-type Candid restrictions and mechanical error-derive guidance. Its
executable helpers and tool catalogs were unchanged. Snapshot integrity,
dependency pins and the canonical Cargo dependency boundary pass. The owned
Rust sources use no CandidType derives
or retired Platform Candid compiler APIs. The Tooling 0.69 / Memory 0.51.9 /
Timers 0.31.9 graph passed strict
core/schema/CLI/build/integration all-target Clippy, 21 build-tool consumer tests,
18 CLI process-boundary tests and Rust 1.91 facade SQL/migration Wasm compilation.
Selected Core and audit-actor normal/build graph checks retain host-tool/parser
exclusions, one Core Platform identity and one actor executor identity.

The installed deterministic post-link/upgrade contract passed on that graph
under Testkit-managed PocketIC. Its compiler artifact was 4,736,891 raw bytes
and its final deployable was 4,168,823 bytes. The test checks retained rows
and query results across upgrade from the compiler artifact to its transformed
artifact on this same dependency graph. It does not qualify an upgrade from an
older dependency release or establish matched dependency-adoption savings.
Testkit 0.69's CLI and server admission were prepared through the normal setup
target; Testkit retained all local server startup and cleanup ownership.

Memory/Storage 0.51.10 and Timers 0.31.10 were selected concurrently during
the subsequent text-construction correction. Those locked inputs were fetched
without editing their selections. Focused Core SQL/migration Clippy and Rust
1.91 facade Wasm compilation pass on that graph; the earlier installed result
does not qualify these later runtime package selections.

The incoming lock selected Timers 0.31.11 during the Shared 0.26.26 work.
Its locked source was prepared without changing Cargo metadata. Scanner and
formatter qualification do not establish runtime qualification of that update.

Earlier scalar/Wire and focused Core budget/traversal/memory-diagnostic results
belong to their recorded dependency graphs. This tooling adoption does not
claim those complete test families were rerun.
Native macOS, browser and full release gates remain separate qualification
obligations. Deployment artifact dependency exclusion enforcement remains
tracked separately in #378.

Tooling 0.69.3 was qualified locally on Linux through strict Core, CLI, build
and affected integration-target linting; focused index, build-cache and CLI
process tests; and the actual Shared formatting/hook, setup and release-adapter
fixtures. All three maintained installed Chromium subjects pass with
Testkit-managed PocketIC 16.1.0: the endpoint roster makes 25 calls across
12 methods, rejected-migration findings make two calls, and physical-integrity
findings make two calls. Generation also checks 90 exact frames, 3,112
truncations and malformed/value/limit controls. Testkit retains instance/server
custody through gateway setup and cleans up the local server. These results
include the actors' Wasm builds; they do not qualify native macOS, the declared
MSRV, a live IC deployment or the full repository/release gates. No matched
Wasm-size, cycle or instruction delta was measured.

The new [RPC-budget rule](../../rules/rpc-budgets.md) applies to browser contracts.
IcyDB already projects schema-owned policy through Wire generation and checks
configuration mutations. This is not full runtime-boundary qualification:
below/exact/above cases for every request/reply resource and an exercised
intentional asymmetry remain unqualified by the current browser harness.
Native allocation budgets are not browser heap guarantees.

### Earlier runtime qualification (0.51.0 / 0.31.0)

The following evidence belongs to the older selections stated below; it does
not qualify the current patch releases.

IC Memory/Storage 0.51.0 were selected together from published archives at
`e3939cd1b33a4c6839ff3a793b42c61a977f8f16`. Their library Rust sources match
0.50.17; collection formats, allocation leases and retained data are unchanged.
The public facade now exposes Memory 0.51 declaration/authority/pool identities,
so consumers exchanging those types must align their direct Memory dependency.
IcyDB does not use Persistence's stable Metrics adapter; no storage reset is
required by this adoption.

Combined Memory/Storage 0.51 and Timers 0.31 qualification passes seventeen
native memory/admission/lifecycle tests and all three installed lifecycle cases.
Strict facade/integration and four actor Wasm Clippy selections pass. The actual
Rust 1.91 generated endpoint consumer builds for Wasm with SQL/migration/metrics;
its external packages match the selected workspace lock. Graph invariants pass.
Testkit owns local PocketIC startup/cleanup. Init uses 5,773,664 instructions;
empty upgrade uses 3,414,275 and populated/converged upgrades use 3,421,834,
below the unchanged 12,750,000 ceiling. Stable extents remain 23,134,208 bytes.
The first installed attempt supplied the launcher instead of its admitted server;
correcting the existing check handoff resolves that pre-startup failure. These
are Linux correctness/current-cost observations, not matched savings. Native
macOS, browser and broad release gates were not run; Wasm-size, cycle and
instruction deltas remain unmeasured.

IC Timers 0.31.0 is selected from the published archive at
`c7ff54cbcee9c2b802f461b7a61f478d18699df3`. Its library Rust sources match
0.30.13; its public arithmetic identity is Metrics 0.16. The Canic adapter is
pinned to committed `06fd517254cc31531f165c63216b5a02c24ceebd`, which admits
Timers 0.31 while preserving the existing facade binding and lifecycle API.
It retains the Platform-based adapter at `550a92efd4adc3e09151bf4e3da771892e5002cf`;
the newer unfinished bootstrap API is excluded. Platform owns raw system calls,
the executor and bounded external management entropy; direct adapter CDK and
management-helper dependencies are removed. The selected graph has one timer
runtime and one Metrics arithmetic identity. This resolves IcyDB's recurrence
of [Canic #581](https://github.com/dragginzgame/canic/issues/581); integration of
the [isolated adapter contribution](https://github.com/dragginzgame/canic/pull/629)
and wider Canic publication remain upstream.

IC Tooling 0.68.6 is selected from published archives at
`dede95dd1d6ea1cb79367ecd3957233fcf3a2c81`; its
[migration guide](https://github.com/dragginzgame/ic-tooling/blob/dede95dd1d6ea1cb79367ecd3957233fcf3a2c81/docs/changelog/0.68.md)
owns API changes. Native executable acquisition now requires Cargo's successful
`build-finished` event as well as the artifact and process success. Host process
capture retries interrupted reads and rejects impossible read counts. IcyDB
retains its existing Build/Testkit interfaces without a local acquisition path.

IC Wire/derive 0.7.15 is selected from published archives at
`47f011702edd0fce99d4272fd80b02991ee5f94f`. IcyDB already supplies the mandatory
registry diagnostic callback and explicit bulk acknowledgements, rejecting
unclean diagnostics. The generator additionally rejects duplicate numeric method
identities. Valid frame bytes, endpoint identities and product limits remain
unchanged; explicit persisted database codecs remain separate. Unpublished
sibling changes are not consumer dependencies.

Current Linux qualification passes 236 CLI/build tests (one expensive artifact
contract remains explicitly ignored), strict CLI/build Clippy, Rust 1.91 facade
Wasm compilation with SQL/migration/metrics, dependency pins, declaration and
graph boundaries, and documentation checks. Generated TypeScript parity passes
90 native frames across twelve endpoints, 3,108 truncations and malformed/value/
limit controls. This Node check does not qualify installed Chromium or transport.
Native macOS, exact Rust 1.96 host-floor, installed-IC/browser execution and broad
release gates are separate obligations. Wasm-size/cycle/instruction deltas are
unmeasured.

The recorded 2026-10-24 Linux 0.67.0 adoption checks passed 233 CLI/build tests, strict
selected host/build/integration linting, public Rust 1.91 SQL/migration Wasm,
dependency checks, offline tool admission and formatting/release fixtures.
Installed startup and Chromium qualification passed 15 calls across twelve
methods, 90 native/browser frames and 3,111 truncations plus malformed/limit
controls. That graph included Memory/Storage 0.50.15 and Timers 0.30.11.
These are retained adoption results, not a fresh full gate for later changes.

| Consumer obligation | Maintained proof owner |
| --- | --- |
| Selected payload integrity | Shared's verifier through `shared-tooling-check` |
| Setup ordering and Rust tool selection | `scripts/ci/test-workstation-setup.sh`, `scripts/ci/test-shared-tooling-adapters.sh` |
| Dependency declarations and grouped release metadata | `scripts/ci/test-cargo-metadata-adoption.sh` (aggregate); `--case ordering` selects only grouped/selected-source ordering |
| IcyDB workflow selection and execution policy | `scripts/ci/test-ci-workflow-invariants.sh`, `scripts/ci/check-ci-workflow-invariants.sh` |
| IcyDB structural scanner integration | `scripts/ci/test-invariant-scanners.sh` |
| Testkit CLI/server integration | `scripts/ci/test-testkit-tooling.sh`, `scripts/ci/test-pocketic-server-wrapper.sh` |
| Formatting-hook preservation | `scripts/ci/test-pre-commit.sh` |
| Release Make dispatch, rejection and recovery routing | `scripts/release/test-standard-release.sh` |
| Group transactions, lock selection, receipts and recovery | `scripts/release/test-release-groups.sh` |
| Publication preflight, receipts and exact-version retries | `scripts/ci/test-publish-workspace.sh` |
| Failure evidence and fixture completion | `scripts/ci/test-tooling-fixture-completion.sh` and validation-adapter fixtures |
| Tag-maintenance command selection without deletion | Shared's `scripts/dev/delete-github-tags-up-to.pl` default dry run |
| IcyDB document inventory, links and version facts | `scripts/ci/test-documentation.pl`, `scripts/ci/check-documentation.pl` |

Shared owns generic installer and dependency-check regression matrices. IcyDB
qualifies its actual selections and consumer behavior rather than duplicating
those producer matrices. Substitute fixtures do not qualify installed tools.
The generic CI-inspection matrix likewise stays upstream
([#474](https://github.com/dragginzgame/icydb/issues/474)); IcyDB retains its actual
Make/workflow/release-adapter checks and explicitly selects the interactive
`scripts/dev/gh-ci.sh` helper.

The generic release-runner matrix likewise remains at Shared Tooling's
`scripts/ci/test-portable-tools.sh` caller
([#598](https://github.com/dragginzgame/icydb/issues/598)). IcyDB's routine
`check-portable-automation` roster retains the consumer proofs above, snapshot
admission and all product/native obligations. The canonical exporter retired
only the generic test entrypoint at the same reviewed revision; production
runner, input-admission, changelog and fixture helpers remain selected. When
changing the Shared runner contract, explicitly select its upstream fixture
from a clean checkout of that revision. The consumer dispatch fixture checks
that retained release proofs run and their failures stop the real Make caller.

## Structural Rust scanner source

The root snapshot owns the Syn-based panic scanner and its semantic fixtures.
[The private source-check package](../../testing/source-checks/Cargo.toml)
compiles that source under the root workspace/lockfile, with no database or
simulator dependency. IcyDB owns required source roots and nonempty inventories;
Shared owns Rust parsing, cfg/macro handling and scanner regressions.

## Consumer-owned instruction measurement

IcyDB selects published IC Metrics 0.16.0 at
`c0f033a8f41c280fd0fc77b2ec934d2f2b8176ed`. Its Rust arithmetic sources match
0.15.8, but the public `MeasurementAggregate` conversion now uses the 0.16 type
identity. IcyDB owns report projection and Wire shape; it does not use the stable
adapter extracted to Persistence. Timers 0.31.0 also selects Metrics 0.16.0,
so the resolved graph contains one arithmetic crate identity.

Metrics 0.16 qualification passes eleven metrics-state tests and fifteen host
reporting tests, strict Core/build Clippy and Rust 1.91 Wasm compilation with
metrics enabled. These are Linux correctness checks, not IC cost measurements;
native macOS and installed-IC execution were not rerun.

[The runtime reader](../../crates/icydb-core/src/runtime.rs) reads IC performance
counter 1 through Platform. IC Metrics owns arithmetic; IcyDB owns execution
spans, phase/entity attribution, reset semantics and journal-debt reporting.
Native zero counters and passing native tests are not IC cost measurements.
Use matched source, dependency, compiler, feature and artifact identities for
raw Wasm bytes, IC instructions and cycles. Raw Wasm takes priority over gzip;
wall-clock/native timing is not a supported performance metric.

## Audit and verification ownership

[Shared methods](../../audits/README.md) own audit procedure;
[IcyDB's catalog](../audits/README.md) selects product scopes and evidence.
[Archived local methods](../audits/archive/shared-adoption/README.md) and old
reports retain their original identities; adoption does not rescore them or
create an automatic broad gate. GitHub issues own follow-up tracking.

[Shared verification helpers](../verification-helpers.md) own Markdown navigation,
release-command smoke mechanics, registry observation and portable digests.
IcyDB retains document selection, product facts, release receipts/publication
policy and artifact subjects. An absent registry version and an unavailable
registry are distinct observations; neither authorizes publication.

## Host qualification

The [CI workflow](../../.github/workflows/ci.yml) declares Ubuntu 24.04 x86-64
and native macOS 15 ARM64/x86-64 lanes. Bash 3.2 remains the portable shell target.
The workflow and package manifests own compiler selections; Linux compilation,
cross-compilation, archive hashes and substitute fixtures do not qualify macOS.

The latest recorded Tooling adoption above has Linux evidence. Native macOS,
exact Rust 1.96 host-floor execution and installed-IC qualification were unrun
for that adoption; broad workspace/release gates were also unrun. Earlier
macOS successes in the committed history apply only to their named source and
graph. Verify the relevant CI run against the actual candidate before claiming
current native-host qualification. Local PocketIC execution is distinct from
live-IC deployment. Wasm-size, cycle and instruction deltas remain unmeasured.

## Standard releases and extraction

[The common release contract](../releases.md) owns execution and recovery.
IcyDB callbacks own exact metadata, dependency selections and source/tag-bound
receipts. Release preparation fetches locked dependencies before offline gates.
The shared runner owns version/staging/commit/tag/push effects; ordinary coding
and cleanup do not authorize them. Publication, deployment and cleanup remain
separately authorized. The local broad-gate exception in
[the operating manual](agent-operating-manual.md#workflow) remains in force.

## Independent CLI release group

The maintainer approved independent CLI releases on 2026-11-02.
`ci/cargo-release-groups.json` assigns every root member to `database` or `cli`.
The database group keeps engine, schema, model, macros, diagnostics and root
qualification packages coordinated; only `icydb-cli` belongs to `cli`. Numeric
versions are unchanged by this layout adoption. All members own literal package
versions, with a complete root dependency catalog and one shared product lock.
No package moves or new workspaces are introduced.

Release, resume and publication require explicit `RELEASE_GROUP=database` or
`RELEASE_GROUP=cli`, including direct consumer script calls. Group-specific tags,
journals, receipts and ledgers prevent accidental cross-group delivery. Root
`CHANGELOG.md` is an index; coordinated history is retained once. Existing
completed journals remain historical; recover old releases from their recorded
source/tooling instead of interpreting them under the new group policy.

The standard release runner and IC Build editor own metadata transactions.
Consumer callbacks project only selected package versions and dependency pins,
finalize the selected notes and bind receipts to that exact group transition.
The Canic workspace remains outside the product release file plan.

Release validation still runs the complete existing gate. The opt-in
[group-plan qualification path](release-validation.md) retains source-bound
evidence; release omissions remain disabled pending #603 qualification.
Publication checks the complete package inventory before
selecting the requested group in dependency order. A CLI release requires its
selected database dependencies to be available in the registry. Updating those
requirements does not silently publish a new CLI version.
