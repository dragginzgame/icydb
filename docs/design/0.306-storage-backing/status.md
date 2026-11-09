# IcyDB 0.306 — Storage bucket backing adoption

The maintainer authorized starting the next minor after the 0.305 cleanup
closeout. Adopt published Storage 0.71.0 under the existing hard-cut contract:
fresh backing, reject incompatible images without writes, and no automatic
migration/reset. No deployment, package-version changes or sibling work.

## Landing slice

1. Update only root Storage selection and its Cargo-managed lock graph; remove
   the retired Wire error projection. Qualify root facade/Core bootstrap,
   allocation diagnostics, growth refusal, same-format cold reopen and retained
   commit/recovery consumers. Carry pending tooling/cleanup notes to 0.306.0,
   preserve published 0.305 notes and the deferred Canic 0.64 graph.

Published 0.71.0 is non-yanked with Rust 1.99.1. Its cached archive matches the
fresh crates.io index checksum
`f278c72c95e2819c20e6c6e59931b3a8891ccf38d1f258af28e599c828d38417`.
Evidence is under `target/evidence/storage-071-adoption/`.
Use focused checks only; broad release/workspace and installed IC/macOS gates
remain user-owned. IC cycles/instructions and Wasm sizes are unmeasured.

## Adoption and focused qualification

The Cargo-managed root lock changes only the Storage record: 0.70.0 to 0.71.0
and removal of its production Wire edge. Every other complete package record
matches the saved incoming lock. The published archive names source
`8ae045a4f2dff37261cda0ae99095fd4c032e936`. Cargo's updated cached index was copied
into the repository-local Cargo home after offline admission found its old
index; the exact scoped offline update then passed. No lock records were edited
by hand, and no supplier source was patched.

The facade drops the removed DirectoryError::Wire match. Its integration tests
cover fresh bootstrap, warm adoption, old/foreign discriminator rejection with
unchanged backing, and same-format cold reopen with retained allocation ID and
bytes. The first commit gate found a fixture assumption that a four-page
request must force physical growth; 0.71's reserved bucket slack can satisfy it.
The fixture now requests more than the entire capped physical backing, retaining
its original no-write/no-metadata-change assertions without naming a bucket size.
The retained initial failure is `core-commit.log`; corrected qualification is
`core-commit-final.log` in the evidence directory.

All 194 selected tests pass: four Storage facade integration cases, 34 facade
error cases, 47 facade database cases, two runtime-binding cases, three allocation
diagnostic cases, 24 startup cases and 80 commit/recovery cases. Strict affected
all-feature library/test-target and default-feature library Clippy pass on Rust
1.99.1. Current tooling admission verifies the unchanged 0.36.1 selection and all
170 snapshot payloads. CLI consumer qualification is recorded separately below.

CLI all-target consumer compilation passes. Formatting and diff whitespace
checks pass. The adoption slice is ready for user-owned integration; full
workspace/release gates, installed IC and macOS qualification were not requested
and remain skipped. IC cycles/instructions and uncompressed total/code-section
Wasm changes remain unmeasured. No deployment, package-version edits, network
lifecycle actions or sibling changes occurred. Canic's 0.64 runtime graph and
incoming tooling edits remain untouched. Pending cleanup/tooling outcomes now
live in the 0.306 ledger and detail notes; published 0.305.0 notes are unchanged.

## Authorized live-page cleanup

The maintainer requests further complexity/duplication reduction in 0.306.
2. Route the remaining ordinary catalogue, public-SQL, full-row, scoped-order
   and secondary-order budget traversals through the existing bounded live-page
   test helper. Preserve session lifetimes, page ceilings, work assertions,
   diagnostics and datasets. Exhaustive-proof and staged diagnostic loops retain
   their separate state; introduce no new helper mode or product behavior.

Use the paging-family tests and strict Core test-target lint. Record measured
non-test/test code deltas relative to the completed Storage adoption. Preserve
all incoming edits and pending release outcomes.

### Live-page cleanup closeout

Completed the five bounded traversal replacements without changing the shared
helper or any runtime implementation. All 23 test functions in the five affected
files remain. Same-session catalogue/scoped-order execution and fresh-request
SQL/full-row/secondary execution retain their original lifetimes, page ceilings,
work assertions, empty-page observations and diagnostic context.

All 297 paging-family tests pass on Storage 0.71.0. Strict Core all-feature
library/test-target Clippy, formatting and diff whitespace checks pass. Shared
LOC classification measures 39 fewer test code lines, zero non-test change,
and no unclassified source. Evidence and the incoming diff are retained under
`target/evidence/live-page-cleanup-0306/`. Pending group and minor notes retain
all previous outcomes and record this additional cleanup. No broad gates,
package versions, commits, deployment, network lifecycle or sibling changes;
IC cycles/instructions and total/code-section Wasm sizes remain unmeasured.

## Authorized Toko Tooling 0.37 adoption

3. Adopt the published 0.37.0 naming hard cut through authenticated preview and
   snapshot-only refresh. Migrate both tooling pins, LOC metadata, the baseline,
   receipt and consumer-owned Make/CI/release callers as one bounded outcome.
   Preserve all product code and historical release qualification records.
   Qualify root/nested admission, affected deployment refusal ordering, actual
   CI and documentation callers, and LOC metadata recognition. Reuse the approved
   tooling-only nested-lock exception while Canic remains deferred.

### Toko Tooling adoption closeout

Published 0.37.0 is adopted in both tooling selections, with all 171 authenticated
payloads admitted offline. Canonical tool preparation and offline tool checks
pass on Linux. Root and nested locks preserve every non-tooling package record.
The 2,169 Rust files retain identical source hashes and LOC classification, with
zero unclassified source. Consumer shell/workflow lint, nested dispatch ordering,
deployment refusal ordering, standard release adapters, CI invariants and local
documentation checks pass. Current validation mapping passes its 53 substitute
rejection/expansion cases; historical native certificates remain unchanged.

The dependency-pin gate passes against a disposable prospective Git index and
object store containing the renamed governance evidence. The ordinary checkout
gate requires that new evidence path to be staged by the maintainer; the real
index was not modified. Initial shell lint needed source-following enabled to
resolve its existing shared fixture helpers; the resulting focused check passes.
Evidence is retained under `target/evidence/toko-tooling-0370/`.

Canic's pre-existing runtime dependency conflict remains deferred; tooling
admission does not qualify that graph. Broad product/release gates and macOS
qualification were not requested and remain user-owned. No package-version,
commit, push, deployment, sibling or runtime-network lifecycle changes were
made. IC cycles/instructions and total/code-section Wasm sizes are unmeasured.

### Authorized 0.37.1 follow-up

Adopt the requested published Toko Tooling 0.37.1 patch in both existing tooling
selections. Keep the 0.37 naming migration and its validated caller wiring;
the authenticated preview changes only the LOC classifier payload. This patch
rejects policies under unsupported metadata namespaces before counting source.
Preserve unrelated lock selections and the deferred Canic runtime graph.

Focused closeout passes: root/nested offline admission, installed-tool checks,
documentation and dependency pins (using the same disposable prospective index
for the still-unstaged governance rename). All non-tooling locked package records
are unchanged; all 2,169 Rust source records and LOC totals match the 0.37.0
baseline, with no unclassified source. Evidence is retained under
`target/evidence/toko-tooling-0371/`. Broad product gates/macOS qualification were
not run; runtime qualification for Canic remains deferred. No staging, commit,
push, deployment or sibling changes. IC cycles/instructions and Wasm sizes remain
unmeasured.

### Authorized 0.37.2 follow-up

Adopt published Toko Tooling 0.37.2 in both existing tooling selections, retaining
the approved tooling-only nested-lock repair. The authenticated preview and
exporter preserve all 171 selected payload hashes. Upstream dependency-report
classification fixes are outside IcyDB's selected roots; no local caller, LOC
policy or tool installation changes are needed. Preserve the preceding focused
caller/tool qualification, all unrelated package records and product changes.
Evidence is under `target/evidence/toko-tooling-0372/`.

Root/nested offline admission, documentation, dependency-pin and whitespace
checks pass. Pin validation again uses a disposable prospective index for the
unstaged governance rename; the real index is untouched. All non-tooling locked
records are unchanged. Broad product/release and macOS gates were not run;
Canic runtime qualification remains deferred. No commits, deployment or sibling
changes; IC cycles/instructions and Wasm sizes remain unmeasured.

### Authorized 0.37.4 follow-up and closeout

Adopt published 0.37.4 in both tooling selections. Its standalone baseline no
longer implicitly exports every detailed guide: explicitly retain engineering
details, RPC budgets and the Candid boundary alongside the existing roots. All
171 payload paths remain; only five Markdown payloads change. The dependency
policy applies current stable direct selections during authorized maintenance,
without unrelated graph upgrades or weakening shared type/runtime contracts.
Existing local choices and approved exceptions remain recorded in the adoption
policy; no new exception is introduced.

Authenticated refresh, root/nested offline admission, documentation, dependency
pins and whitespace checks pass. The pin gate uses the same temporary prospective
index for the unstaged governance rename; the real index is unchanged. Every
non-tooling locked record and all executable payloads remain unchanged, so prior
focused caller/tool checks still apply. Evidence is retained under
`target/evidence/toko-tooling-0374/`. Broad product/release and macOS qualification
were not requested; Canic runtime qualification remains deferred. No commits,
deployment or sibling changes; IC cycles/instructions and Wasm sizes remain
unmeasured.

## Authorized SHA-2 adoption

4. Adopt the latest stable direct SHA-2 implementation, 0.11.1, for schema,
   Core and model-build hashing. Preserve hash bytes, domain separation and
   public schema contracts; retain older transitive implementations where needed.
   Qualify digest/fingerprint behavior and affected native/Wasm compilation.
   Preserve unrelated pending work, package versions and the deferred Canic stack.

### SHA-2 migration and contract evidence

The root now selects SHA-2 0.11.1 without default allocation/OID features. It
reuses the published implementation already selected by Platform Types; Cargo
changes only the three direct consumer edges (Core, Model, Schema). Every
external package record and the independent Canic lock remain unchanged. No
runtime source or public API changes were required.

Two test fixtures used the retired digest-output LowerHex implementation. Keep
the Unicode contract's original bytes as an array and preserve canonical-ordering
receipt text through explicit byte formatting. Fixed independently calculated
vectors now protect a persisted targeted-rule source key and a canonical value
fingerprint, rather than comparing only two calls through the current provider.
Formatting, schema/model checks and the selected Core hash, fingerprint, receipt,
cursor, integrity and Unicode tests pass. Two pre-existing Tier C receipt tests
remain ignored because their owning Make shard/merge gates supply the required
evidence. The fixture formatting initially triggered Clippy's format_collect;
direct String writes resolve it and strict affected library/test-target lint
passes. First failures and final results are retained under
`target/evidence/sha2-0111/`.

Final closeout passes the unchanged canonical-ordering receipt and the facade's
Wasm SQL/migration/metrics compilation on Rust 1.99.1. Dependency-pin admission
passes using the existing disposable prospective-index treatment for the unstaged
governance rename; documentation and whitespace checks pass. No new external
versions, package-version changes, commits, deployment or network lifecycle
changes. Broad product/release and macOS gates were not requested. Canic runtime
qualification remains deferred. IC cycles/instructions and total/code-section
Wasm sizes are unmeasured; Wasm compilation is not a size qualification.

## Authorized Platform Types 0.19 adoption

5. Adopt published Types 0.19.0 for IcyDB's canonical Principal/Account contracts.
   Remove the retired external-conversion feature and its conversion-only fixture;
   retain checked caller-byte and canonical identity tests. Preserve explicitly
   byte-oriented older transport boundaries and the deferred Canic workspace.
   Qualify schema, identity, guard and native/Wasm consumer boundaries without
   changing IcyDB package versions or deploying.

### Platform Types migration and focused evidence

The registry-verified 0.19.0 archive removes external-Principal aliases and
conversions; retained Principal/Account implementations and Serde bytes are
unchanged. Root/schema/Core/facade canonical identities now use 0.19. Remove the
facade's retired dev feature and conversion-only fixture while retaining caller
byte coverage for every admitted length and invalid/anonymous rejection.
Unchanged transport suppliers retain 0.18.3 across existing byte boundaries.
The graph invariant now admits only 0.18/0.19 families with one version each;
negative cases reject a third family and duplicate versions in either family.
The fixture also copies the current renamed governance evidence before admission.

Facade/CLI all-feature target compilation, 187 schema tests, seven guard tests,
and selected Account (59), Principal (17) and identity (six) test runs pass.
The graph/inheritance/release-order fixture, dependency boundaries, shell lint,
documentation and whitespace checks pass. Cargo adds only Types 0.19.0 and updates
Types edges; the old Types SHA feature edge disappears because its direct consumer
moved to 0.19. Existing package versions/checksums and the Canic lock are preserved.
Evidence is retained under `target/evidence/platform-types-019/`, including the
initial fixture evidence-path failure and corrected result.

Final closeout passes strict affected all-feature library/test-target Clippy and
facade Wasm compilation with SQL, migration and metrics on Rust 1.99.1. Broad
workspace/release, installed-network and macOS qualification were not requested.
Canic runtime qualification remains deferred; no package-version edits, staging,
commits, deployment, sibling or network lifecycle changes. IC cycles/instructions
and total/code-section Wasm sizes are unmeasured.

## Authorized Platform 0.28 retry

6. Adopt Platform 0.28.0 through newly published Testkit 0.96.0, the owner of
   IcyDB's native Platform dependency. Retain Types 0.19 and runtime selection,
   propagate native-client contract changes where consumed, and remove the now
   unnecessary dual-Types graph allowance. Qualify actual facade testing/CLI
   consumers and graph admission; preserve the independent Canic stack.

### Platform/Testkit migration evidence

Fresh registry checks found Testkit 0.96.0 now admits Platform 0.28.0 and Types
0.19.0. Root adoption requires only the Testkit catalog change: Cargo updates
Testkit and Platform, adds Platform's num-bigint 0.5.1, removes the old management
DTO package and Types 0.18.3, and prunes no-longer-enabled dependency edges.
The independent Canic lock is unchanged. No new direct Platform dependency,
supplier patch, external-identity bridge or runtime-version change is needed.

Remove the temporary 0.18/0.19 graph coexistence allowance: sensitive Types
selection again requires one version. Keep negative tests for old/new unexpected
families and duplicate versions. The root facade/CLI all-feature targets compile
unchanged. Two typed startup tests and public constructor-budget admission pass;
the live PocketIC progression test remains explicitly ignored without a prepared
server. Strict facade/CLI library/test-target Clippy and Wasm compilation with
SQL, migration, metrics and testing pass. No local network lifecycle action was
performed. Registry-selected archive preparation resolved the initial offline
cache miss for num-bigint; no source workaround was necessary.

Closeout also passes graph/inheritance/release-order fixtures, dependency
boundaries, shell lint, documentation and whitespace checks. Evidence is under
`target/evidence/platform-028/`. Broad workspace/release, live IC and macOS gates
were not requested. Canic runtime qualification remains deferred. Package
versions, staging, commits, deployment and sibling repositories are unchanged;
IC cycles/instructions and total/code-section Wasm sizes are unmeasured.

## Authorized Runtime 0.21 and Tooling 0.37.5 adoption

7. Adopt direct Runtime 0.21.0 and published Toko Tooling 0.37.5 in both tooling
   selections. Retain Storage's dependency-free Runtime 0.20.3 raw IC0 closure
   under a narrow graph allowance, with executor/guard dependency rejection.
   Qualify native/Wasm runtime consumers and actual updated dependency-boundary
   admission. Preserve the independent Canic runtime graph and all pending work.

### Runtime/Tooling adoption status

Both requested selections are updated: direct Runtime 0.21.0 and Toko Tooling
0.37.5. The authenticated exporter retains all 171 payloads, changing only the
Candid dependency checker and its ownership policy. The Wire-benchmark oracle
exception grants no direct Candid ownership to IcyDB. Root/nested tooling
admission, actual dependency boundaries, documentation and shell lint pass.
Canic's non-tooling package records are unchanged.

Storage still selects Runtime 0.20.3 only for raw IC0 on Wasm; its lock record
contains no dependencies and therefore no executor, guard or Types closure.
Graph admission permits that exact dependency-free record beside the direct
Runtime and rejects an old executor closure or duplicate current/other-old
versions. Focused synthetic graph cases and current graph admission pass. No
runtime singleton guard or supplier source was patched.

The facade/CLI all-feature native targets compiled successfully with Runtime
0.21.0 before the separately authorized IC Entropy work changed its sibling
package version. The next selected test stopped during resolution: IcyDB still
requires ic-entropy 0.1.0 while the active sibling reports 0.0.0. The maintainer
confirmed that session is still working. Preserve its inputs; do not change its
version to force validation. Storage-bootstrap tests, strict affected lint and
Wasm qualification must resume after that dependency settles. No adoption-ready
verdict is claimed for Runtime yet. Evidence is under
`target/evidence/runtime-021-tooling-0375/`.

No broad gates, deployment, commits, sibling edits or network lifecycle actions.
Native compilation does not qualify IC execution or macOS. IC cycles/instructions
and total/code-section Wasm sizes remain unmeasured.

## Authorized Storage 0.71.1 adoption

8. Require published Storage 0.71.1, already selected by the current lock, and
   retain the Runtime 0.21 alignment. Qualify changed directory lookup and
   admission/verification through IcyDB bootstrap/reopen and commit/recovery
   consumers. Remove the now-obsolete old Runtime graph allowance. Preserve
   concurrent Entropy work and the deferred Canic graph; no deployment.

### Storage 0.71.1 adoption evidence

The authenticated patch retains the 0.71 storage format and public API while
changing stable-key classification, directory key lookup and bounded control-row
reads/verification. Its Wasm dependency now selects Runtime 0.21. The lock had
already advanced when this slice started; the root manifest now requires 0.71.1.
No further locked records or Canic dependencies changed. Remove the exact old
Runtime exemption and restore single-version Runtime admission; positive and
negative graph cases pass, including executor-closure and duplicate rejection.

All four facade bootstrap cases pass, including old/foreign image rejection,
warm reuse and same-format cold reopen preserving allocation identity and bytes.
Dependency boundaries, pins (with the existing temporary prospective index for
the unstaged governance rename), documentation, shell lint and whitespace checks
pass. Evidence is retained under `target/evidence/storage-0711/`. The Entropy path
currently resolves again; its independent development work remains untouched.

Final closeout passes 108 selected tests (four bootstrap, 80 commit/recovery and
24 startup), strict Core/facade all-feature library/test-target Clippy, and facade
Wasm SQL/migration/metrics/testing compilation. This also completes the Runtime
0.21 bootstrap/lint/Wasm checks previously blocked by the Entropy version mismatch,
against the currently resolving approved local development source. Entropy's
separate registry publication qualification remains required before release.
No broad workspace/release or macOS gates were run. No package-version changes,
commits, deployment, sibling edits or network lifecycle actions. IC cycles,
instructions and total/code-section Wasm sizes remain unmeasured.

## Authorized issue #633 completion

9. Complete the existing IC Entropy extraction against verified published
   0.1.0. Replace only the approved local supplier selection, remove its temporary
   exception and overlay, and preserve database boot entropy policy. Qualify
   successive legacy u128 vectors, exhaustion/no-reseed behavior, monotonic ULIDs
   and retained boot-key/cursor binding tests. Keep Canic and macOS qualification
   limits explicit; no delivery is requested.

### Issue #633 registry migration and evidence

Registry 0.1.0 is published and non-yanked with Rust 1.99.1. Its downloaded
archive matches SHA-256
`d5a7386a4f8a63fc339bd18b773c49c078427a0273f66b708466852336045297`
and producer source `0caf4376a98e6c9f0dbd2231900a0d5fb932a83d`. The root now uses
that registry requirement; Cargo's resolved lock changes only this package's
source/checksum. Remove the temporary external-path policy and AGENTS overlay.
No sibling files or supplier implementation changed.

Retain the existing Generator-based implementation and native OS acquisition;
no second generator, fallback, background reseed or database-key policy was added.
Four old ChaCha20 0.10 u128 values captured with the original high-word/low-word
construction now pin successive consumer draws. A one-draw budget test verifies
repeated exhaustion maps to the existing error without reacquiring/reseeding.
The current 18 focused Linux tests pass: four random, ten ULID and four boot
entropy/cursor binding cases. The latter retain nonzero admission, rotation
refusal, retry behavior and database-incarnation separation.

Both configured macOS hosts now run these three focused families. Workflow
invariants and actionlint pass, but no macOS result is claimed from this Linux
session. The issue explicitly requires supported-native-host qualification, so
#633 remains open for those runs after user-owned integration. Existing Canic
installed acceptance remains separate. Evidence is under `target/evidence/issue-633/`.

Strict Core all-feature library/test-target Clippy and Wasm library compilation
also pass against the registry source. Dependency pins, graph/boundary admission,
documentation and whitespace checks pass. Native tests do not claim Wasm execution
or macOS qualification. Broad workspace/release and installed-network gates were
not requested. The fix is uncommitted, unpushed and undeployed; no package-version
or network lifecycle changes. IC cycles/instructions and complete/code-section
Wasm-size deltas remain unmeasured.
