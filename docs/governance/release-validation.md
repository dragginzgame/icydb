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

The 0.308 layout separates native qualification resolution. The existing full
product lint/check/feature/test leaves also select the qualification workspace,
so moving packages does not omit their CI coverage. Product dispatch checkpoints
both native manifests and locks; Canic adds only its own independent inputs.
The mapping contract includes the native lock and tooling-admission adapter.
A reviewed digest refresh admits these recipes, without renewing any stored
selected-stage qualification evidence or enabling release omissions.

The 0.301 pending-note move changes the database group's `metadata_files` from
`releases/database/0.300.md` to `releases/database/0.301.md`. Review confirms
that this is the only group-policy difference: package ownership, dependency
groups, the complete stage roster and Make recipes are unchanged. The mapping
contract readmits that declaration's current digest so the full gate can start.
Existing native comparison records are preserved and are not renewed by this
metadata review or its substituted regression fixtures. The current dependency,
workflow and other shared changes still require full validation; no receipt or
selected-stage omission is inferred from the digest repair.

The 0.305 deployment review readmits the database detail-note move to
`releases/database/0.305.md` and the Makefile's independent Canic-tooling
admission prerequisites and focused test target. None changes the product
validation stages, fail-fast/collect split or release dispatch. Refresh only
the reviewed Make/group declaration digests so full validation can start.
Preserve native comparison certificates: the changed inputs still select the
full gate, and this metadata correction authorizes no selected-stage omission.

The 0.309 deployment review readmits only the database detail-note move from
`releases/database/0.308.md` to `releases/database/0.309.md`. Group membership,
dependency ownership, Make recipes and the full stage roster are unchanged.
Refresh only the reviewed group declaration digest so full validation can start;
preserve the existing native certificates without granting selected omissions.

The 0.309 release closeout aligns qualification's test command with its existing
CI model-generation profile. That independent workspace contains no Testkit
callers or facade-owned tests. The root workspace continues to enable both
`icydb-model/build` and `icydb/testing`, including facade contracts, native API
checks and startup polling. Qualification retains every member and all-target
tests with `icydb-model/build`. Its lock remains on its independent Types and
thiserror selections. The reviewed Makefile digest admits this ownership repair;
existing native certificates remain unchanged and full validation stays required.

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

### Current selection after Build/Host/Storage adoption

The subsequent requested Build 0.80 / direct Host Foundation 0.76 / Host Tools
0.88 / Storage 0.69 / Testkit 0.94 migration changes `Cargo.toml` and `Cargo.lock`. Those are
mapping inputs, so the current candidate uses full
validation. Preserve the reviewed comparisons below for their exact Host Tools
0.86 source; changing a dependency does not refresh their omission authority.
Focused adoption checks are not a replacement full/selected comparison, and no
new broad comparison is requested by this dependency update. Existing captured
release inputs and their evidence keep their original identities.

The latest Toko Tooling 0.36 migration selects repository tooling through the
exact root workspace dependency and every member's inherited dev-dependency.
Cargo.lock and the snapshot receipt bind it; the superseded metadata selector
and dedicated tooling lock are removed, including from qualification inputs.
Snapshot, manifest, Make/caller and policy changes retain full validation;
historical records are unchanged. Tooling admission precedes repository CI/build,
captured setup, ICP staging and direct publication.

### Reviewed Linux admission — 8 November 2026

Fresh database and CLI working-tree comparisons pass on Linux x86-64 with
Rust/Cargo 1.99.1, Shared 0.31.12 and the settled Build 0.79.1, Host Foundation
0.75.1, Platform 0.26.1, Storage 0.68.11, Wire 0.17.14 and Testkit 0.93.2 graph.
Both capture candidate `0b7107d6de1b2b8187615cc1e215ba918dc33957` and each execute
sixteen full and eleven selected stages, with zero stage reuse.

| Group | Controlled source | Retained evidence |
| --- | --- | --- |
| Database | `57099eb6dada2852a5773cbe43433c0ba55d28ad` | `target/release-validation-native.KTTVhe/evidence/` |
| CLI | `b6727c9c5137fe67bbcd7fd61fe2d8827bff1042` | `target/release-validation-native.UIz4pz/evidence/` |

Both Shared proofs and consumer receipts re-admit. Source bundles verify, and
both final original HEAD/index/inventory/working-byte custody checks pass. The
CLI selected/full archives are byte-identical; every retained archive has the
admitted VCS source and original manifest. Archives use the existing declared
published database 0.303.0 family and CLI 0.296.5; final-version publication
verification remains required.

The appended reviewed records in `ci/release-validation-qualified.json` bind
mapping `632a816802b18d9c3e3ab58ae4b6d4371050fcb5a0ef7ce7511c372556810c0f`.
Actual record admission and selection pass in the separate admission-only control
`target/release-validation-admission.DVf0Br/`: eligible future source selects
eleven stages for either group, this mixed batch selects all sixteen, and
configuration/automation drift refuses. That control runs no product stages or
release effects. The bounded substitute-Cargo dispatch fixture also passes all
69 selection/ordering/failure controls without inheriting product checkpoints;
production checkpoint ownership is unchanged.

Earlier attempts `nsAhSO` and `ZRlP0U` passed their captured product stages but
correctly refused final custody after the normal lock changed. Retain those
failures; they supplied neither current records nor omission authority. The
maintainer confirmed the update settled before the fresh captures above.

These comparisons completed #603's Linux mapping qualification locally. The records
are uncommitted, unpushed and unpublished. macOS narrowing is not admitted;
both macOS hosts keep full validation and normal CI is unchanged. Canic #373
remains independent. The unreproduced hosted static adapter assertion is tracked
under #632; local/full-comparison success does not establish its root cause.
IC cycles/instructions and total/code-section Wasm deltas are unmeasured.

All comparisons below retain their exact input identities. Current selection is
described above; historical activation gaps and successes do not override it.

### Historical 6 November comparison and subsequent tooling adoption

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
unqualified, and ordinary CI is unchanged. That adoption left
[#603](https://github.com/dragginzgame/icydb/issues/603) open for renewed mapping
qualification; the current result is recorded above.

### Frozen hook qualification

The [frozen release batch](https://github.com/dragginzgame/toko-tooling/blob/5806e6e4943f6be600cd53385498ce08a5f804c4/docs/releases/release-batches.md) interface preserves
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

Database 0.300.0 subsequently passed that full Linux gate and was pushed and
published at `ead6a522155770bf4c5d289a4d33c90a627ec798`. All six official archives
match registry checksums, clean release source and original manifests. This real
registry acceptance used the standard workflow for the mixed-increment delivery;
it does not claim a real frozen publication run. The frozen default is enabled in
the delivered source. See the
[0.300 status](../design/0.300-testkit-frozen/0.300-status.md).

### Shared 0.30.9 setup and custody acceptance

The subsequent 0.30.9 refresh uses Shared's saved setup context and batch
status/resume/reconcile/retire interfaces, removing the old local wrappers.
Actual IcyDB hooks passed on Linux with evidence in
`target/tmp/frozen-icydb.3nZ3GS56/` and
`target/tmp/shared-0309/frozen-hooks-prepared.log`. Shared setup and saved-source
offline recovery preserve captured input bytes. Both groups retain the captured
Testkit/Wire graph after a real dependency update in the normal fixture.
Verified native archives, metadata transitions, exact receipts, uncertain
publication reconciliation, completed resume and remote-conflict refusal pass.
Status preserves intent; dirty normal-tree reconciliation refuses; clean
reconciliation fast-forwards; retirement refuses the ignored `.cache/` directory
and retains state/evidence. Only registry effects are substitutes.

The initial attempt stopped because the fixture updater's acquisition cache
lacked a newly locked Wire version. The fixture now prepares that captured graph
through its existing fetch owner; the corrected run passes. Dispatch, snapshot
source integrity, documentation and shell checks pass. No production release or
publication was executed for this refresh, and no further broad product gate or
native full/selected pair was run. Current-mapping omissions and macOS remain
unqualified. The real incoming Wire 0.17.5 lock edit was preserved byte-for-byte
through adoption; the maintainer subsequently authorized including it in the
pending CLI 0.296.2 deployment. That release requires its own actual gate.

### Delivered CLI 0.296.2 frozen acceptance

The real `batch-kRCC00xv` captured source
`fc2dd259c2cb04559b4cc0e2a1c2aa27da4f8650`, passed the complete Linux product
release gate and published CLI 0.296.2 at
`2db84355523b0af29d4434d718a1e03a4749e528`. Its official archive matches the
registry checksum, clean release source and original manifest; it selects
published IcyDB 0.300.0 and Wire 0.17.5. This is actual frozen Git and registry
acceptance, distinct from the earlier substituted-registry hook qualification.
A transient shared host-tool installation lock stopped the first preflight;
resuming the same captured batch after its owner finished completed delivery.

A real normal-checkout Testkit 0.89.3 lock edit appeared during validation. The
batch retained Testkit 0.89.2 through release and publication, and preserved that
normal edit unchanged. The completed batch, receipts and evidence remain under
`.git/release-state/frozen/batch-kRCC00xv/`,
`target/release-frozen/batch-kRCC00xv/` and `target/tmp/deploy-0300/`.
Normal-branch reconciliation remains pending because its deferred lock is dirty;
no reset, stash or cleanup was performed. Current-mapping omissions and macOS
qualification remain separate. See the
[delivery status](../design/0.300-testkit-frozen/0.300-status.md).

### Shared 0.31 release-batch hard-cut acceptance

The 0.31 snapshot at `74313941af2fce4399bec0e6909ca6229673c3f8` replaces the old
settings/runner with `RELEASE_GROUPS`, `RELEASE_PUBLISH_TARGET`,
`RELEASE_SETUP_TARGET`, `RELEASE_SETUP_MODE`, `RELEASE_BATCH_ACTIVE` and
`run-release-batch.pl`. Shared owns automatic capture; clearing a selected roster
and using retired settings refuse. IcyDB preserves explicit group ownership and
supplier-first admission while removing its duplicate capture default.

Actual consumer hooks pass on Linux with Rust/Cargo 1.99.1. Evidence is retained
in `target/tmp/frozen-icydb.2roRMAMs/` and
`target/tmp/shared-0310/batch-hooks.log`. The fixture uses real setup, locked
native builds, verified archives/original manifests, metadata, Git and receipts;
only registry effects are substitutes. Schema-2 intent retains the captured graph
after a real normal-fixture dependency edit. Interruption, uncertain publication,
completed resume, status, remote refusal and guarded reconciliation pass. Schema-1
journals and missing recorded publication commits refuse without migration or
reconstruction. Explicit retirement refuses the ignored cache and preserves state.

All 159 annotated snapshot payloads match the reviewed source. Portable checksum,
NUL-output/backend failure, inert-helper corruption and missing-source controls
pass against copies of the actual consumer helpers. Dispatch controls cover old
setting refusal, empty rosters and saved-batch operations. Browser declarations
pass schema 3 and a synthetic schema-2 lock is rejected; documentation, shell,
compiler admission and the declared stage contract pass.

The real completed `batch-kRCC00xv` remains schema 1; its retained original 0.30.9
snapshot verifies independently. Any remaining operations use that original
reviewed runner, not the new interface. No active production batch was migrated,
replayed or retired. Deferred normal-checkout Testkit 0.89.3 and prior local
status edits remain preserved. Package versions and published notes are unchanged;
new notes use pending database 0.300.1 and CLI 0.296.3 entries.

No full product suite, native full/selected comparison or deployment was requested
or run for this adoption. Current-mapping omissions and native macOS remain
unqualified. IC cycles/instructions and total/code-section Wasm deltas are
unmeasured.

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
then-current mapping. Both group paths were implemented; fresh reviewed native
comparisons were required before that mapping could omit stages. Updating the reviewed
Makefile digest keeps the full gate dispatchable; it does not renew qualification.
The current reviewed comparison is recorded in the activation section above.
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
