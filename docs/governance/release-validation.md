# Release validation qualification

## Receipt custody

Candidate-transition and completed-release receipts default to
`release-state/receipts/` beneath the checkout's actual Git directory, beside
the shared runner's recovery journal. Cargo and executable caches are not their
owner. `RELEASE_RECEIPT_DIR` remains an explicit caller-owned selection for
isolated fixtures or recovery; every candidate, commit and publication consumer
uses the same selected directory.

Missing qualification still refuses receipt reuse. Earlier cache receipts are
not discovered, copied or recreated automatically. Keep their original bytes
and select their owning directory explicitly for an admitted recovery attempt.
Publication without an admitted completed-release receipt retains Cargo package
verification; it cannot infer a pass from a cache file or a release tag alone.

## Qualification boundary

[#603](https://github.com/dragginzgame/icydb/issues/603) owns the group mapping
and its native qualification. Release selection requires a reviewed matched
full/selected comparison for the actual host, compiler, Cargo configuration and
complete mapping. Missing or changed qualification retains `validate`; a selected
failure blocks release and cannot fall back to a successful full gate.

`check-product-invariants` owns live source/contract checks, documentation and
maintained-source boundary regressions. `check-automation` owns workstation,
release, publication and runner regression fixtures. `check-invariants`, normal
product CI and standalone portable-host qualification still execute their complete
applicable coverage. Splitting ownership does not delete any existing check.

For existing Rust source/tests in the six database library packages, selected
database validation retains the entire product workspace: all-target strict lint,
model/build and SQL lint, compilation, feature checks, model/runtime separation,
no-default facade/core tests, and full workspace tests with generated-code,
CLI process/library and Build consumers. Live invariants remain required. Only
unchanged release/workstation automation fixtures are omitted; supplemental CLI
lint/build/test stages are already covered by the required workspace aggregates.
The supplemental CLI archive belongs to CLI delivery, not database source admission.
Final selected-group archives still require normal Cargo verification at publication.

New/deleted/moved Rust files, other paths, tooling, manifests/features/dependencies,
compiler or mapping changes expand to full validation or refuse. The exact existing
IC Build release projection recognizes only owned database/CLI version metadata,
including an interleaved CLI patch after the database baseline. External dependency
identities, feature changes and other manifest contents remain bound. Version
projection maintains mapping identity; it never reuses a stage pass. Every selected
required stage executes freshly (`reused: 0`). Minor-line policy changes still
invalidate qualification.

CLI-private source retains CLI lint, build, process/library tests and archive
verification plus live product checks. It omits unchanged workspace aggregates
and automation regressions. All other CLI changes retain the complete gate.
Both groups carry selected proof through exact metadata, staging, commit, tag,
push and interrupted recovery. Selected receipts cannot authorize `--no-verify`
publication. Historical full-gate receipts keep their original meaning.

The ordinary Linux/macOS CI matrix and the independent Canic acceptance contract
remain unchanged. A Linux comparison does not admit macOS omissions. Each host
keeps its full release gate until its own exact comparison is reviewed. Selection
and fallback diagnostics are written to the command log and Actions summary.

## Bounded database feedback

The explicitly dispatched **Database subsystem validation** workflow accepts an
annotated `database-vVERSION` baseline and `auto` or `workspace` scope. It runs
Linux x86-64 and both supported macOS hosts, retaining the selection/reason and
Shared runner logs. This is an additional focused feedback entry point; it does
not replace automatic product CI or produce a release-qualification receipt.
Ordinary push/PR CI keeps its complete obligations without duplicating these runs.

| Changed source since baseline | Selected target | Mandatory cross-subsystem acceptance |
| --- | --- | --- |
| Core SQL parser only | `ci-database-parser` | SQL lowering, session pagination/order, SQLite query and mutation references, public SQL facade |
| Core query planner only | `ci-database-planner` | The same acceptance boundaries |
| Parser and planner together | `ci-database-query` | Both unit families and the same acceptance boundaries once |
| Any other/shared input, missing/untrusted baseline, dirty source, or no source change | `validate` | Complete product gate |

Compiler admission and strict SQL Clippy precede selected tests. Every filtered
test command must execute passing tests; zero matches or any required failure
stop the selection. The combined target deliberately accepts the parser/planner
boundary through session/reference/facade tests. It does not infer that arbitrary
subsystem combinations are safe. Database notes may accompany a bounded change;
notes alone cannot select one. Manifests, locks, CI/tooling, schema/model/codegen,
executor/storage and other unknown paths retain full validation.

The targets are also explicit focused development commands. For example,
`make ci-database-parser` runs parser and downstream acceptance regardless of
the local diff. Automatic workflow selection uses the complete annotated-baseline
diff, not only the latest commit. Existing Shared baseline observation and stage
execution remain the owners; the new selector grants no release authority.

Before release omissions can use these selections, #603 still requires reviewed
matched full/selected evidence, feature/retained-format/installed obligations,
native host qualification and integration with release receipt recovery. These bounded subsystem targets do not qualify product-suite omissions. The
selected database release mapping retains whole-workspace product coverage and
omits only unchanged automation regression execution.

## Mapping and selection

`ci/release-validation.json` is the single complete stage roster. Its mapping
contract classifies four supplemental CLI stages and two collecting stages;
`validation-stages.pl` derives the full gate's fail-fast and collecting selections
from that same roster. `validate` and the selector no longer independently list
the twelve full-gate stages. Both phases checkpoint the declarations. Tools and
compiler admission remain first, and Clippy still precedes builds and tests.
The normal full gate has twelve stages. The qualification comparison adds the
four supplemental CLI stages, for sixteen; either selected plan has eleven.
These are command counts, not measured performance savings.
The full entry point admits the reviewed recipe, roster and group fingerprints
before dispatch; stale declarations refuse before Cargo gates can execute.

The reviewed mapping contract pins the complete Makefile, roster and Cargo group
policy. A recipe, aggregation, release dispatch
or group change refuses until that contract and its qualification are reviewed;
updating a digest alone is not qualification. Nested gates retain generated-code,
facade, Core SQL, no-default-feature and workspace/reverse-consumer coverage.
Workspace lint keeps runtime selection and separately lints explicit model
generation; workspace tests enable generation for the graph integration targets.

| Selection | Required stages | Exclusions |
| --- | --- | --- |
| Existing database product Rust and owned version metadata | Product/consumer coverage and live scans (11) | Automation regressions and supplemental CLI duplicates/archive |
| Explicit workspace or uncertain/shared inputs | All 16 | None |
| CLI-private Rust command/presentation/tests and CLI notes only | Common tools, compiler, formatting, workflow/shell lint, dependency policy, invariants; CLI lint, all-target check, all-target unit/process tests and verified Cargo package | Workspace clippy, check, feature matrix, test and automation regressions; engine and automation contracts must be unchanged since the admitted CLI baseline |

The supplemental stages are `clippy-cli`, `check-cli`, `test-cli` and
`package-cli`. The full qualification plan deliberately includes them in addition
to the old gate, so that the comparison also proves the narrow recipes. Existing
workspace checks already include CLI compilation/tests; no duplicate work is
added to the default release gate.

The selector requires clean committed HEAD and an annotated ancestor tag from
the selected group. Shared observes the baseline and complete diff; unavailable,
lightweight and non-ancestor baselines remain consumer refusals. Shared also
checks roster/group equality; product mapping and recipe admission remain local.
CLI changes outside its private command/presentation/test surface expand to
workspace. Database changes outside the six existing library source/test trees,
allowed private CLI consumers, group notes and exact owned version projection
also expand. Other manifest/lock changes, features, shared/compiler/CI/release
files, build scripts, fixture packages and unknown paths retain the full gate.
No change also selects workspace. Every stage has a reason. Version projection
does not authorize version edits; the normal release adapter still owns them.

All tracked regular files, index entries, source and baseline/tag object,
configuration environment hashes, host and actual compiler identities enter the
checkpoint/context. Symlinks, gitlinks, unmerged files, untracked source and
redirected evidence paths refuse. Environment values are hashed to avoid writing
credentials into evidence. These observations bind inputs; they do not by
themselves establish correctness or admit a compiler/tool version.

## Running and retaining qualification

From a clean source, run `bash scripts/release/qualify-validation-plan.sh GROUP
GROUP-vVERSION auto` for selected scope, or use `workspace` for the full
comparison. This explicitly selects broad checks when the resulting plan is
workspace scope; ordinary development should use the focused fixture target
`test-release-validation-plan`. Broad execution still requires the maintainer's
request under the local operating manual.

Each invocation creates a fresh `target/release-validation/qualification.*`
attempt and calls Shared's existing runner through IcyDB's adapter. Plan mode
selects the unique run that retained the exact submitted plan, so nested
positional runners cannot supply or obscure its evidence. It fails fast,
retaining lint-before-tests order. Shared owns checkpoints, stage
execution and raw logs. Shared verifies plan/policy bytes, ordered stage events, before/after checkpoints
and retained logs, publishing an immutable evidence component through its atomic
receipt helper. Consumer receipts re-admit that saved proof and the same product
selector, source/index and host/compiler/configuration. Their JSON is published
through the same shared byte-custody helper; local event/timing parsers are removed. Missing, failed, interrupted, stale or
unknown stage evidence cannot produce a receipt. Failed attempts are retained;
retry creates a new attempt. Readmission can recheck the same evidence, but never
replaces or upgrades a failed stage. Stage reuse is disabled: every selected
stage is recorded as `executed`, with `reused: 0`.

Receipts use `icydb-release-validation-qualification/2`, report executed/reused
Make-target invocation counts and explicitly set `release_omission_admitted: false`. Existing
full-gate `GROUP-vVERSION.commit` release receipts retain their original semantics.
Version 1 qualification receipts are historical evidence; the new caller requires
its own saved Shared proof and never reinterprets those older bytes. Raw
Shared logs include execution durations for troubleshooting; neither qualification
nor acceptance uses elapsed time as a performance metric.

Shared's UTF-8 text contract applies to selection/event/timing records, including
non-ASCII reasons and checkout paths. Evidence digests still bind the original
bytes. Invalid UTF-8 or changed text cannot replace an admitted consumer receipt;
restoring the exact saved bytes permits identical readmission. The consumer
fixture exercises that boundary from a path containing spaces and a non-ASCII
character.

## Activation obligations

### 6 November comparison and subsequent tooling adoption

Both Linux x86-64 working-tree comparisons passed on Rust/Cargo 1.99.1 with
Shared Tooling 0.30.4 and the settled Platform 0.23.3 / Wire 0.17.4 lockfile.
Each ran sixteen full and eleven selected stages, with zero stage reuse.
Both saved proofs and receipts were re-admitted; original source/index custody
passed. The CLI selected/full archives are byte-identical, and all retained
archives have the admitted VCS revision and original manifest.

| Group | Controlled source | Retained evidence |
| --- | --- | --- |
| Database | `6307a537ceb04ca784280457b14bc3c8e768d440` | `target/release-validation-native.xVGIr5/evidence/` |
| CLI | `7bffd590ee7c97b5e9229bd84d046ec20fc10044` | `target/release-validation-native.Zp7J2h/evidence/` |

The reviewed records bind mapping
`24e0183670c5634d420b12967f2a9dd104bf1fe8f9981fa0dfe7bea1fb7a5b93`.
Separate admission-only controls passed for both actual records: eligible source
selects eleven stages, canonical consumer version preparation preserves the
mapping, and configuration/automation drift refuses. Database preparation includes
the consumer's extra exact-pin update as well as generic group preparation.
These controls execute no product stages or release/publication effects.

The subsequent requested Shared Tooling 0.30.8 adoption changes the snapshot,
release interface and ownership checks. Those reviewed records are therefore
historical for the current mapping; both groups retain full release validation.
Refreshing recipe fingerprints admits the full gate, not omissions. No further
full/selected comparison was run for this adoption. macOS omissions remain
unqualified, and ordinary CI is unchanged. [#603](https://github.com/dragginzgame/icydb/issues/603)
remains open for current-mapping and delivered-source acceptance.

### Frozen hook qualification

The [frozen release batch](../../docs/frozen-releases.md) interface preserves
captured committed delivery source while later normal-checkout dependency edits
remain deferred. IcyDB defaults ordinary release entrypoints to the explicitly
selected group; complete multi-group rosters remain explicit and supplier-first.
Publication remains a separate explicit selection. A batch applies one increment
to every group; mixed increments use the standard group workflows.

The actual-hook Linux fixture passed with evidence retained in
`target/tmp/frozen-icydb.Nlyf7s4K/` and
`target/tmp/deploy-0299-1/frozen-hooks-immutable.log`. It runs IcyDB's real Make,
metadata preparation, source/transition receipts, Git/tag and publication adapters
against a local bare remote. Its bounded gate builds verified native CLI and
library archives and checks their clean captured source and original manifests.
A real `cargo update` changes only the normal fixture checkout during the first
group's gate; both groups retain the captured Testkit and transitive dependency
versions. Interruption, uncertain-publication refusal and reconciliation,
completed resume without replay, supplier-order refusal and remote-conflict
refusal pass. Only registry effects are substitutes. The normal fixture source
and its deferred lock edit survive delivery.

This qualifies the frozen integration hooks on Linux, not a replacement product
gate, real registry acceptance, macOS, or installed-canister behavior. The current
mapping still selects the full product release gate as explained above.

### Prepared post-cut native comparison

The manual **Release mapping qualification** workflow
(`.github/workflows/release-validation.yml`) is ready for explicit dispatch once
the hard cut is settled and committed. It runs Linux x86-64, macOS ARM64 and macOS
x86-64. It is not triggered by ordinary pushes, PRs or deployment, and no job has
been added to the required product CI matrix.

The same driver can be selected explicitly on a prepared native workstation:

```bash
make fetch
bash scripts/release/qualify-native-validation.sh --group database
```

First review the settled Makefile, group policy and roster against the mapping
contract, updating only their reviewed fingerprints when necessary. A digest
refresh is not qualification. The driver refuses stale mapping inputs rather
than repairing them. If CLI archive verification needs features or APIs absent
from its declared published database dependencies, publish that matching family
through the existing full release workflow first. A successful archive against
older declared dependencies does not qualify pending database artifacts. The hard cut itself keeps
the full production gate; this preparation does not make it depend on a
not-yet-qualified selected plan.

This is broad qualification. The driver refuses a dirty checkout before setup
or execution; do not run it while another session owns changing inputs. It
creates a disposable product fixture under `target/`, never a delivery checkout.
A deterministic synthetic annotated baseline and a controlled database comment or CLI presentation
edit exercise the selected group without changing engine/schema/codegen,
manifest/lock or release inputs. Package versions stay unchanged. Synthetic
history has no push remote, disables hooks/signing, and is retained in a Git
bundle with its real input revision. The original checkout is never staged or
committed. Existing installers prepare fixture executables; only the locked
source acquisition cache is shared, not build output or validation evidence.

For an explicitly settled but uncommitted local candidate, select
`bash scripts/release/qualify-native-validation.sh --working-tree --group database`. This mode
exports the actual working files into the disposable fixture, preserving the
original index and refs. Shared checkpoints verify the export and final source
contents; file inventory, index and complete tracked diff must also stay fixed.
The artifact labels this as a working-tree snapshot and retains the candidate
patch, source inventory and snapshot identity. Its result qualifies that exact
candidate, not a different later commit or a published release. The hosted
workflow continues to require committed source. Neither mode admits omissions.

Both plans execute real product stages on the identical source, compiler and
configuration: full scope requires all 16 stages and selected scope requires 11. CLI selection
excludes four workspace aggregates and automation regressions; database selection
excludes automation regressions and four supplemental CLI obligations already
covered or owned by the independent CLI/archive boundary. Selected scope runs first.
For CLI qualification, this puts archive/dependency refusal before additional
engine gates; database selection already retains those engine gates. The comparison
re-admits both
existing consumer receipts through Shared's saved-proof checks; an excluded-stage
failure probe with different configuration cannot stand in for a matched run.
The report retains required/excluded commands, reasons, executed/reused counts
and a fingerprint of the declared mapping/dependency/compiler inputs. It never
uses duration as an acceptance metric.

Each host retains setup/full/selected logs, raw Shared proof and receipts,
`input-source`, `source.bundle` and `comparison.json`. Successful fixture build
trees are removed only after evidence is retained; failed attempts stay intact.
The full comparison retains a verified CLI archive. CLI qualification also
retains the selected archive and compares both byte for byte. Their VCS
revision and original manifest must match the admitted source before comparison
can succeed.
Review each native result on its matching input and synthetic source before
admitting that host. Linux qualification does not admit macOS omissions; macOS
continues through the full gate until its own comparison is reviewed. Every comparison still reports
`release_omission_admitted: false`.

The 4 November Linux working-tree comparison passed on Rust/Cargo 1.99.1:
15 full and 11 selected stages executed, with zero reuse. Both receipts were
re-admitted on matching source, baseline, compiler and configuration. Original
HEAD, index, file inventory and source bytes remained fixed. Local evidence is
under `target/release-validation-native.io439Q/evidence/`, including the source
bundle, raw proofs and comparison. Both verified CLI archives are retained and
byte-identical; their packaged source and original manifest match the admitted
Git tree. They use declared published database 0.297.0 dependencies, not final
database 0.298 artifacts. The ordinary complete local product gate also passed.
That comparison predates production receipt wiring and is retained as historical
evidence, not its qualification. New mapping code requires a fresh comparison.

### Production receipt integration

The reviewed comparison binds the recipe, policy, dependency/compiler inputs and
release adapter. IC Build's existing metadata editor normalizes release-family-owned
versions in member manifests, the root catalog, lock and owned pin exceptions.
Database/CLI version preparation does not invalidate qualification; changes to other declarations or release mechanics
do. Compiler flags, wrappers, profiles and Cargo configuration remain bound.

Every selected release executes a fresh plan against its committed source. Shared
admits the original run before IcyDB retains its exact input and raw log bytes
beside the Git release journal. Recovery uses Shared's input checkpoints and
plan parser on that sealed view; absolute event paths are preserved, never
rewritten or readmitted as a run at another physical location. The consumer
rechecks the original annotated baseline, product/CLI ownership and exact omissions.

A format-3 candidate receipt binds this retained gate to the existing canonical
metadata projection. Staged, committed, tagged and push checks preserve that
binding, including after deletion of the original Cargo validation cache. Missing
or altered evidence refuses recovery. The completed selected receipt has a
distinct format and cannot authorize publication with `--no-verify`: Cargo still
verifies the final-version archive against its declared published dependencies.
Full-gate format-2 candidate and one-line completed receipts retain their meaning.

The post-wiring Linux comparison passed on 4 November with Rust/Cargo 1.99.1:
15 full and 11 selected stages, zero reuse, identical verified archives and
unchanged original source/index custody. Evidence is retained under
`target/release-validation-native.1ao25U/evidence/`; the reviewed comparison is
recorded in `ci/release-validation-qualified.json`. Its mapping fingerprint is
`106ac97aac931f3e7ea03a14763f3ed651ad5ad1939017bb11183fb644d41126`.
Actual admission of that record on a clean committed CLI-private source view
passes; changed Cargo configuration or mapping inputs refuse. This admission
fixture executes no product stages and does not replace the real comparison.
That record admitted Linux x86-64 for its exact earlier inputs. The subsequent
bounded database targets and fallback diagnostics change the Makefile and release
adapter fingerprints, so this record is now historical and does not admit the
current mapping. Both group paths are implemented; fresh reviewed native
comparisons are required before this mapping can omit stages. Updating the reviewed
Makefile digest keeps the full gate dispatchable; it does not renew qualification.
macOS keeps full validation and its normal CI jobs.
The batch introducing this shared mapping selects full validation; qualified
narrowing applies to later eligible source changes against their own annotated
group baseline.

IcyDB integration is the scope of this work. Query/Canic/Auth sibling source,
publication and release authority remain separate. The hard cut is settled;
the independent adapter manifest/lock remain unchanged because the compatible
adapter is not published. Its current composed acceptance stays in #373;
these root/CLI comparisons do not claim an installed Canic pass.

Fixture qualification exercises the actual selector, plan runner and receipt
with substitute stage bodies. It proves selection, exact target dispatch,
required/excluded failures, conservative expansion, recipe/roster drift, unavailable baselines, source/configuration drift,
interruption, fresh retry and failure to replace a receipt after log tampering. It does not qualify
product, archive or native-host behavior.

Before admitting a host and group, run matched real full/selected plans on the
same admitted source and review all omissions. Both groups keep full validation
until their matching record is reviewed. Both native macOS hosts retain full
validation. Final archive dependencies/provenance,
registry order, public/generated identities, Wasm, retained persistence/recovery
and applicable installed contracts remain independent requirements. The isolated
Canic workspace retains its own lock and qualification; this plan neither adds
it to root Cargo commands nor waives its existing blockers. `package-cli` verifies
the packaged crate, but does not substitute for final release metadata, registry
availability or archive/provenance review. Current dependency requirements may
correctly block package verification until database dependencies are published.

Only reviewed native comparisons activate omissions. A missing or stale record
automatically retains full validation. Ordinary focused checks do not authorize
a full comparison or a release.
