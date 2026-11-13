# 0.311 migration completion

The maintainer authorized this next minor on 2026-11-13 after the 0.310
schema-construction cleanup passed its focused closeout (399 tests).
Package versions and delivery remain unchanged.

## Landing slice 1 — Direct rewrite completion

Status: complete; ready under focused qualification.

Remove the accounting-only `RebuildingIndexes` public/persisted state and its
separate continuation. Complete derived-domain accounting and reset the row
cursor in the same journal operation as the final rewrite page, then enter
`FinalValidation` directly. Preserve final proof, publication, counts, bounded
abort cleanup, terminal replay and same-contract interruption recovery.

The migration-record envelope receives a distinct magic discriminator; retain
one current reader, not a legacy decoder. Old migration records (including
terminal records) are rejected intact. Existing installations carrying those
records must finish/recover using their original release and explicitly retire
or reset them before adopting this hard cut. No automatic deletion or conversion
is authorized. Schema bundles and row layouts do not change.

Qualification: affected Core migration/application and record-codec tests,
CLI migration command tests, focused strict Clippy, no-default Core compilation,
and formatting. Full gates and macOS qualification remain user-owned.

Expected complexity reduction: one fewer durable/public state, one fewer
continuation and plan recompilation per migration; no new state or abstraction.
Derived-domain preparation remains bounded and shares existing projection
admission. Wasm total/code bytes and IC cycles/instructions are unmeasured.

## Slice 1 closeout

Passed: 178 Core application tests, 12 migration-record tests and 12 CLI migration
command tests (202 total); strict focused Core/CLI Clippy; Core no-default build;
`make fmt-check`; diff whitespace check. The selection covers interrupted final
rewrite recovery with retained counts, multi-page and empty migrations, missing
derived-entry rejection, bounded abort cleanup and terminal replay. Checksum-valid
old-envelope rejection and current-envelope round trips pass.

Net Rust source lines: non-test -12, tests +60, total +48 (including comments and
blank lines). The reduction is in lifecycle states/continuations, not total lines.
No Wasm or IC measurements were run. Broad/release gates and macOS qualification
were skipped as user-owned validation. No package version changes, commits, pushes
or deployment were performed. The accepted slice is exhausted; further independent
implementation scope needs maintainer direction.

## Landing slice 2 — Migration domain discovery

Authorized by the maintainer's continuation after the domain-discovery finding.
Status: complete; ready under focused qualification. Share candidate-generation selection between row projection,
completion counting and abort inventory. Metadata consumers must not compile row
constraints or projections. Keep catalog/head admission, target store capability
checks, logical counts before physical-domain deduplication, bounded abort cursors,
and final row proof. No new public or persisted contract is needed.

Qualify migration execution/application and relation binding with focused tests,
strict Clippy, no-default compilation and formatting. Broad gates remain unselected.


### Slice 2 closeout

Passed 217 focused tests: 179 schema-application, 2 migration-execution and 36
relation tests. Strict Core library/test Clippy, no-default Core compilation,
`make fmt-check` and diff whitespace checks pass. Added checks preserve logical
counts before/during staging, exact reverse-domain generation/store identity and
rejection of missing live source/target authority. Existing migration recovery,
unchanged-generation preservation, abort paging and final-proof tests pass.

Source delta: +16 non-test / +38 test Rust lines, net +54 including comments and
blank lines. Removed metadata consumers' full projection preparation, the temporary
domain representation, duplicate sort/dedup and unused projection identity storage.
No new persisted state or public contract. Wasm total/code-section bytes and IC
cycles/instructions are unmeasured. Broad/release/macOS gates remain skipped as
user-owned validation. No dependency/version changes or delivery occurred.
The accepted scope is complete; subsequent continuation begins with closeout audit.


## Tooling/compiler adoption follow-up — #637

The maintainer requested Toko Tooling 0.40.1 and Rust 1.100.0 after the two migration
slices. Pins, all three workspace floors, the toolchain and inherited common lint
tables are updated. Unrelated lock selections are preserved; requested Platform Runtime 0.22.0 and
Types 0.20.0 are adopted in root and native qualification. The installed 109-file
snapshot and actual compiler identities pass admission; Schema strict Clippy,
Rustdoc, Wasm compilation and four bounded-codec tests pass.
A disposable inherited-policy fixture rejects unsafe code and ignored must-use
results. Obsolete line-count expectations are removed without splitting operations.

All 611 Diagnostic Code missing-doc errors are repaired and its strict library/test
Clippy gate passes. SQL generator library/test strict Clippy also passes. Core
SQL/migration/metrics strict Clippy now clears all 460 Core missing-doc findings,
wildcard imports, redundant clones and map/boolean simplifications. The strict
Core library/test gate now passes with zero warnings and errors; see the Core
ownership closeout below.
SQLite support's prior three findings are cleared in this gate. No lint policy was
weakened. Source changes preserve conditional DISTINCT checks, move owned rejected
values into diagnostics without cloning, consume constraint-compilation inputs and
borrow relation-merge inputs. The follow-up removes 40 further ownership findings:
borrow inspecting assertion/error helpers and accepted index contracts; move the
diagnostic fingerprint rather than clone it; delete redundant owned SQL and index
encoding forwarding helpers. A further 19 findings are removed by borrowing schema
publication identities, field/default/index mutation inputs and inspected SQL
syntax. Live-authority and strict aggregate admission checks remain unchanged.
The planner/fixture pass removes eight more findings by borrowing read-only schema
metadata, logical input views, validation dependency fields and comparison values.
Owned planning-state handles and finding payloads remain owned.
Endpoint exceptions remain the maintainer's separate
Canic work. No suppression was added. Formatting and diff whitespace checks pass.
Core focused runtime checks are recorded in the closeout below; broader validation
and the separate Model Macros/CLI lint findings remain outstanding.
Wasm total/code-section sizes and IC cycles/instructions remain unmeasured.
The new compiler/lint adoption is **not ready**. #637 remains open; this is not waived by the
prior compiler's successful migration checks. No broad gate, release, package-version
change, sibling mutation or publication was performed. See the current adoption
section in `docs/governance/toko-tooling.md` for the evidence boundary and the new
release rules. Completing #637 needs real documentation/debt treatment and the
remaining native/Wasm/generated-consumer and required-host checks.

## Compiler text-contract hard cut — #637

The requested Rust 1.100 adoption uses Unicode 18, changing actual scalar case
mappings and contextual sigma classification. Under the maintainer-approved
fresh-installation contract, the boot discriminator changes to `ICYDBU18`.
Earlier images, including `ICYDBID\0`, refuse unchanged before recovery; there
is no legacy reader, deletion or conversion. Current-contract recovery remains.
The scalar digest, mapping vectors, format corpus and retained-byte refusal
checks are written for this boundary but remain unexecuted while Core Clippy is
blocked. Lower/upper allocation algorithms match Rust 1.99.1;
exhaustive expansion bounds remain part of the focused text checks.

The requested Toko Tooling 0.41.0 follow-up installs offline `make deps-list`,
removes unused workspace dependency catalogs and enforces the new Cargo/Rustdoc
lints. Release membership is preserved. Two more diagnostic assertion helpers now
borrow inspected errors; no lint suppression was added. See current tooling
adoption for supplier-isolation and setup evidence.

## Rust 1.100 source review — #638

The requested review uses the [Rust 1.100 release notes](https://doc.rust-lang.org/releases.html#version-11000-2026-11-12)
and the adopted supplier guidance. Findings are evidence for
[#638](https://github.com/dragginzgame/icydb/issues/638), not additional deployment gates.
The subsequent authorized implementation replaces the four restore-only guards,
uses `trim_prefix` and adopts Cargo warning denial for ordinary Make Clippy
recipes. No allocator experiment was performed.

- `runtime::with_instruction_counter_for_tests` has a local restore-only struct
  and Drop implementation. `core::mem::DropGuard` can remove both. Existing
  `native_counter_scope_restores_after_nested_scope_and_unwind` already exercises
  nested normal return and unwind restoration; retain that behavior and proof.
- `CurrentRequestScopeGuard` in `db/resource/request` is also a single-use
  restoration helper. A local DropGuard can remove its type, enter method and Drop
  implementation. Keep the old scope owned by the guard and qualify nested and
  unwinding scopes before replacing this production boundary. Test-only
  `StartupRecoveryWakeupRestore` and `ScalarPageResultBytesLimitGuard` are similar
  candidates, with their callers propagated together.
- Normal Clippy adapters in Makefile currently append `-D warnings`.
  `CARGO_BUILD_WARNINGS=deny` is a possible artifact-reuse improvement only after
  proving fresh and reused warning refusal and updating command-contract fixtures.
  Keep explicit compiler denial for plain JSON collectors; do not weaken #637.
- `Subaccount::from_hex` can express optional lowercase `0x` removal as
  `trim_prefix("0x")`; its current strip/unwrap-or sequence is equivalent. This
  is a small readability change, not an architectural or performance gain.
- Bounded ordering already stores small order tuples inline, precharges temporary
  bytes and caps initial top-k capacity. Custom Vec/Box allocators therefore need
  evidence of an additional benefit before introducing allocator/lifetime machinery.
  IC cycles/instructions and total/code-section Wasm bytes are unmeasured. Do not
  substitute native timing or introduce unsafe allocation code speculatively.
- Keep CommitGuard, cache budget owners, entropy cancellation guards and iterative
  expression/SQL destructors: they protect domain-specific lifecycle or stack
  bounds. Keep Infallible spelling; no duplicate implementation was found that
  switching to its new never-type alias would remove. No useful Box/Rc/Arc map
  conversion was established by the targeted transformation search.

The Host/Build/Testkit adoption preceding this review preserves unrelated locks.
CLI strict Clippy exposes 39 Model Macros ownership failures alongside Core's 77
library failures; those are additional #637 qualification evidence, not a supplier
API conflict. Broader compilation/tests and macOS qualification remain outstanding.

### Rust 1.100 implementation follow-up

Deleted RestoreCounter, CurrentRequestScopeGuard, StartupRecoveryWakeupRestore and
ScalarPageResultBytesLimitGuard; their exact previous state now lives in local
DropGuard values. No commit/resource ownership guard or iterative destructor was
changed. Added nested normal-return/unwind request-scope coverage and retained
the existing instruction-counter restoration test. Prefix tests also reject a
doubled prefix; Schema strict Clippy and both hex-prefix tests pass. Two Schema
assertion helpers now borrow inspected inputs to clear the selected lint gate.

The tooling-adapter fixture proves warning refusal on fresh and reused Cargo
artifacts, with JSON-rendered and plain output, and admits the actual Make
warning-denial environment. All adapter regressions and the 69 database-dispatch
assertions pass. Plain JSON collectors retain compiler-level denial. Recipe/group
hashes are refreshed without rewriting native qualification certificates or release
receipts. The release-plan fixture passes all 53 rejection/expansion cases, including
drift, interruption and fresh recovery with substitute stage bodies. This is
automation evidence, not native product qualification. Core SQL/migration/metrics library and test targets reach lint checking with
no remaining guard/type errors. The strict gate still rejects 77 library / 112
test-build ownership findings (112 unique), all needless-pass-by-value; evidence
is `/tmp/icydb-rust100-final2.jsonl`. Runtime restoration tests remain unexecuted
until that gate passes. No lint allowance was added.
IC cycles/instructions and total/code-section Wasm effects remain unmeasured.

### Recovery/schema/projection fixture ownership — #637

Removed 12 further findings by matching helper arguments to their actual use.
Raw commit/journal helpers accept byte slices, index-delta fixtures reuse shared
slices, and assertions borrow their expected errors/kinds/values. Historical
backfills accept OutputValue directly and consume it into expected rows instead
of converting from Value through a fixture-only panic branch. Assertions and
retained-corruption checks are unchanged; no lint suppression was added.

Formatting and diff whitespace checks pass. The selected Core SQL/migration/metrics
strict Clippy gate reports 77 library / 100 test-build ownership findings (100
unique), with no other diagnostic category or type errors. Evidence:
`/tmp/icydb-637-test-borrows-final.jsonl`. Runtime tests and broader qualification
remain stopped until that gate passes. Wasm/cycle/instruction changes are unmeasured.

### Cardinality query-fixture ownership — #637

Removed 12 further ownership findings from the cardinality query fixtures.
Helpers borrow inspected filters, candidates, field kinds, input values and
expected rows; group-order cases no longer allocate temporary vectors. Covering
assertions preserve exact expected source comparisons through optional references.
Schema-building inputs that are consumed remain owned. No assertions or lint
policies were weakened.

Formatting and diff whitespace checks pass. The selected Core SQL/migration/metrics
strict Clippy gate reports 77 library / 88 test-build findings (88 unique), all
needless-pass-by-value, with no type errors or findings in this test family.
Evidence: `/tmp/icydb-637-cardinality-final.jsonl`. The initial offline attempt
needed the locked cc 1.11.2 downloaded; final validation ran offline and locked.
Runtime tests and broader qualification remain stopped until the selected gate
passes. Wasm/cycle/instruction changes are unmeasured.

### Core ownership closeout — #637

The selected Core gate is now clean on Rust 1.100: `cargo clippy --offline
--locked -p icydb-core --lib --tests --features sql,migration,metrics
--message-format=json -- -D warnings` reports zero warnings and errors, including
the compile-test harness. Evidence: `/tmp/icydb-637-zero-final.jsonl` and
`/tmp/icydb-637-zero-final.log`. Formatting and diff whitespace checks pass.

Read-only catalog, schema, journal, route and projection inputs borrow their
owners; covering decoders take slices and forwarding callers drop redundant
clones. Prepared commit-control replacement remains a consuming capability,
now published through `apply(self)` rather than changing it into a repeatable
borrowed operation. The public catalog-description constructor borrows counts;
its Core and CLI fixture callers and the pending breaking notes are updated.
No lint suppression, Copy workaround or weakened assertion was introduced.

Focused runtime coverage passed 609 checks initially; one frozen marker hash
fixture still described the earlier migration envelope. Its first three hashes
were unchanged, while the cases containing the migration record reflected the
already-authorized direct-completion hard cut. Updated only those two expected
hashes, retaining byte identity, capacity, batch range, round-trip and corruption
assertions. The repaired fixture and all 12 migration-record tests then passed:
622 distinct passing focused tests in total. Evidence:
`/tmp/icydb-637-zero-focused-tests.log` (initial failure retained) and
`/tmp/icydb-637-zero-migration-tests.log` (13 passing checks).

The focused selection covers commit/control publication and convergence, journal
inspection, persisted rows, covering projections and physical streams, index
storage, CASE admission, schema integrity/application/migration, resumable jobs,
Deep integrity, the persisted-format corpus and cardinality query fixtures.
The externally updated Metrics 0.30.2 lock selection is preserved; final evidence
uses it (Cargo.lock SHA-256
`e6f1a355b1e68ffeff6b8091f0b8b29e1dd1be2039cf38ef366d75be589750d6`).

This closes the Core lint selection, not #637's repository-wide compiler adoption.
The earlier Model Macros/CLI findings and other qualification remain outstanding;
no broad workspace gate, release, macOS qualification or network lifecycle was run.
Wasm/code-section bytes and IC cycles/instructions remain unmeasured.

## Tooling 0.41.1 and Model/facade/CLI lint closeout — #637

Adopted the published 0.41.1 payload through its installer, retaining the 109-file
snapshot receipt. All 54 real packages use the newly supported
`target.'cfg(any())'.dev-dependencies` source-bundle marker, with the exact version
selected by their workspace. Cargo refreshed only tooling in the root and native
qualification locks; the deferred Canic lock received the verified tooling-only
repair because its existing executor-links conflict prevents normal resolution.
Unrelated incoming Host/Metrics selections were preserved. Snapshot, nested
admission, tool setup and formatting checks pass. The locked IC Build release
metadata editor is now admitted from the local cache. This patch adds no new
release stage; the adopted sequencing and supplier-isolation rules still apply.

The selected strict Clippy gates now pass with zero warnings/errors:

- Model and Model Macros libraries and test targets:
  `/tmp/icydb-637-model5.jsonl`.
- Facade SQL/migration/metrics library and test targets, plus the subsequently
  changed entity generator: `/tmp/icydb-637-facade4.jsonl`.
- CLI all targets: `/tmp/icydb-637-cli5.jsonl`.

Generator helpers borrow inspected token fragments; generated fields, variants
and typed-write inputs receive documentation at their generator owner. Model
visitor and numeric APIs document their contracts. ErrorTree moves owned strings
and borrows traversal prefixes. Three numeric constructors retain narrow,
reasoned `needless_pass_by_value` expectations: generated attachments supply
literal, constant and function-result arguments by value. Expanding or changing
that authoring contract solely for a lint would add complexity. All other findings
in these selected gates were fixed directly, without added Copy bounds or weakened
assertions.

The facade borrows column/scale metadata and diagnostics through their forwarding
chains; public borrowed-signature changes are recorded in the pending 0.311 notes.
CLI command/report adapters and test assertions borrow inspected inputs. Genuine
owned output rows, strings and diagnostic facts still transfer ownership.

The final focused runtime selections pass 353 checks: 25 Model unit tests,
17 Model hook integration tests, 55 generator tests, 78 facade tests, 109 CLI
unit tests and 69 actual-process exit-status fixtures. After the final typed-write
documentation change, all 38 entity-generator tests were rerun and pass; these
are included in the generator count, not added again. Evidence:

- `/tmp/icydb-637-model-tests.log`
- `/tmp/icydb-637-model-integration.log`
- `/tmp/icydb-637-model-generated-tests.log`
- `/tmp/icydb-637-typed-write-doc-tests.log`
- `/tmp/icydb-637-facade-tests.log`
- `/tmp/icydb-637-cli-focused-tests.log`
- `/tmp/icydb-637-cli-exit-tests.log`
- `/tmp/icydb-637-adoption-final-fmt-check.log`
- `/tmp/icydb-637-adoption-final-tooling-check.log`

This closes these selected lint families, not repository-wide #637 acceptance.
Broad workspace/release gates remain skipped user-owned validation. Native
qualification consumers, macOS and the deferred Canic runtime are not qualified
by these Linux checks. No release, package-version change, sibling update or
local network lifecycle action was performed. Wasm/code-section size and IC
cycles/instructions remain unmeasured.

## Model build-feature closeout — #637

The optional build feature is now qualified by strict all-target Clippy and
Rustdoc with warnings denied. The initial selection exposed three parser-error
callers missed by the earlier ErrorTree string-ownership change; those callers
now use the existing formatted-error path. It then exposed 131 library / 135
test-build lint findings and, after the library was clean, ten undocumented
feature-gated fixture declarations. All are repaired.

Authoring APIs now document declaration identity, field/default/write policies,
index expressions, durable memory keys, and graph lookup semantics. Actor code
generation borrows schema fragments, migration plans and token fragments that it
only inspects, while retaining owned encoded bytes and registry state. Caller
propagation includes generator fixtures. No new lint suppression, Copy workaround,
public API expansion or weakened assertion was introduced. Strict Rustdoc also
caught three existing bare URLs in international-type documentation; those now
render as links.

Validation passes:

- Strict all-target Model Clippy with `build`:
  `/tmp/icydb-637-model-build-final.jsonl`.
- 53 focused actor-generation, schema-validation and declaration tests:
  `/tmp/icydb-637-model-build-tests.log`.
- Five integration tests covering Boolean checks, literal preservation, durable
  namespaces and index-name resolution:
  `/tmp/icydb-637-model-build-fixtures.log`.
- Strict Model Rustdoc with `build`:
  `/tmp/icydb-637-model-build-rustdoc-final.log`.
- Formatting and diff whitespace checks pass:
  `/tmp/icydb-637-model-build-final-fmt.log`.

Repository-wide #637 acceptance is still incomplete. Remaining feature/qualification
selections and macOS evidence are not established by this focused Linux pass.
Broad workspace/release gates remain skipped user-owned validation; deferred Canic
was not selected. No deployment, package-version change or network lifecycle
action was performed. Wasm/code-section bytes and IC cycles/instructions remain
unmeasured.

## Plain Cargo native dependency admission — #637

The maintainer's plain `cargo clippy` invocation exposed a gap in the earlier
selected JSON-output evidence: native manifest finalization rejected Core's
unconditional `ic-platform-runtime` dependency. After that repair, the same root
invocation identified the facade's equivalent declaration. Every direct runtime
call in these packages is guarded by `cfg(target_arch = "wasm32")`; both dependency
edges now use that exact target condition. Wasm keeps the `ic0` feature. No unused
imports, dummy calls or lint suppressions were added, and no version update was
needed.

The root default-target `cargo clippy --offline --locked` invocation now passes
with `CARGO_BUILD_WARNINGS=deny`, including CLI compilation and Cargo manifest
finalization (`/tmp/icydb-637-native-plain-clippy2.log`). The facade and its Core
dependency also pass Wasm library checking with SQL, migration and metrics enabled
(`/tmp/icydb-637-runtime-target-wasm.log`). This is compile/dependency evidence,
not a Wasm performance measurement. Formatting and diff whitespace checks pass.

The plain native check does not replace remaining all-target/feature qualification
or macOS evidence. No full runtime suite or release gate was run; those remain
user-owned validation. No deployment or network lifecycle action occurred.

## Wire 0.22 / Build and Platform patch adoption

Published registry inspection and checksum-verified archives select Wire/Derive
0.22.0, Build 0.84.1, Platform 0.32.1, Runtime 0.22.1 and Types 0.20.1. Build and
Platform were already selected by the incoming maintained locks; their direct
manifest floors now match. Root and native qualification locks change only
Wire/Derive 0.21.5 to 0.22.0, preserving every unrelated package selection. The
source comparison found lint/API annotations and derive cleanup, with no codec
frame-format change. Public Wire Rust identities now come from 0.22; the pending
0.311 breaking notes require direct consumers to align their dependency version.

The selected Build library gate exposed four existing findings. Build completion
now borrows inspected configuration/plans, progress rendering borrows events, and
PATH discovery uses `map_or_default`. Ownership of retained artifacts and actual
build results is unchanged; no lint suppression was added. The local release
metadata executable is admitted at Build 0.84.1 through verified cache reuse.
The facade's tooling marker is grouped before test tables so manifest formatting
is stable; dependency selections and test membership are unchanged by that move.

Validation passes:

- Strict facade library/test Clippy with Wire schema, SQL, migration and metrics:
  `/tmp/icydb-wire22-facade-clippy.log`.
- Strict CLI all-target Clippy: `/tmp/icydb-wire22-cli-clippy.log`.
- Strict native Build library Clippy: `/tmp/icydb-wire22-build-clippy2.log`.
- 63 facade codec/reply/error tests and one Platform interoperability test:
  `/tmp/icydb-wire22-facade-tests.log` and
  `/tmp/icydb-wire22-platform-types-tests.log`.
- Eight selected native Build tests covering plan selection, Wire artifact
  agreement, schema metadata and optimizer failure boundaries:
  `/tmp/icydb-wire22-build-tests.log`.
- Wasm facade/Core compilation with SQL, migration and metrics:
  `/tmp/icydb-wire22-wasm-check.log`.
- Tooling/tool admission, formatting and diff whitespace checks:
  `/tmp/icydb-wire22-final-tooling-check.log` and
  `/tmp/icydb-wire22-final-fmt-check3.log`.

This is dependency adoption and focused qualification, not deployment or complete
#637 acceptance. The deferred Canic graph and sibling repositories are untouched.
No full workspace/runtime suite, release gate or macOS qualification was selected;
those remain user-owned validation. No network lifecycle action was needed.
Wasm size, code-section bytes and IC cycles/instructions remain unmeasured.

### Toko Tooling 0.42.2 vendor-layout adoption

Selected the checksum-verified published 0.42.2 payload in the root,
qualification and deferred Canic workspaces. Only the tooling package changed
in each lock. Cargo updated the maintained locks; the previously authorized
Canic tooling-only repair preserves its deferred runtime graph.

The installer previewed and migrated 106 managed files, retiring verified old
locations. Consumer Make, CI actions, setup, release, hook, native scanner and
fixture callers now use `vendor/toko-tooling/`. Release/publication entrypoints
are explicit; producer test scripts and the governance inventory are no longer
selected. The stock local Git hook points at the vendor hook. Snapshot-owned
bytes and receipts were changed only by the installer.

The release mapping now inventories vendor helpers, including transitive helper
drift. Its declaration hashes reflect the preserved stage roster and migrated
callers; the old native qualification certificate remains untouched. Focused
fixtures use disposable source and substituted effectful commands, and do not
qualify product release omissions. Broad product/release gates and macOS remain
user-owned validation; no package versions, sibling repositories or deployments
changed. Runtime Wasm/cycle/instruction effects are unmeasured.

Validation passed: snapshot and all three workspace tooling admissions, exact
pin/inheritance admission, tool setup/checks, formatting, actionlint, selected
ShellCheck, CI policy and its 159 fixtures, 69 database-selection cases,
25 documentation cases, 33 Wasm-report caller cases, compiler-policy projection,
Testkit/setup/automation/deployment callers, stock-hook preservation, release
routing and group transactions, publication ordering, and release-selection,
receipt drift and recovery fixtures. Effectful release/Cargo stage bodies were
substitutes in the release-selection fixtures. Logs are retained under
`/tmp/icydb-0422-*.log`; no product execution evidence is inferred from them.

### Toko Tooling 0.42.4 patch adoption

Updated the three exact tooling selections and their lock records, preserving
all unrelated package records. The published installer refreshed the baseline,
release-command checker and Cargo-tool maintenance helpers. Setup and tool
admission pass; the release-routing fixture has a focused `--case routing`
selection and passes with substituted runners. Snapshot, nested tooling,
dependency-pin admission and selected ShellCheck pass. The previous broad
release-selection fixtures were not repeated for this patch; product release
qualification and deferred Canic runtime qualification remain separate.
