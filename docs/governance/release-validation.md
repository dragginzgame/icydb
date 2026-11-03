# Release validation qualification

[#603](https://github.com/dragginzgame/icydb/issues/603) owns qualification and
activation. `release-verify` still executes the full `validate` gate. The new
consumer selector and receipts are an opt-in qualification path, not permission
to omit release checks or reuse historical receipts. Workspace CI is unchanged.

## Mapping and selection

`ci/release-validation.json` exhaustively lists the eleven existing `validate`
stages and four supplemental CLI stages. The reviewed mapping contract pins the
complete Makefile and Cargo group policy. A recipe, aggregation, release dispatch
or group change refuses until that contract and its qualification are reviewed;
updating a digest alone is not qualification. The existing nested clippy/test,
feature and invariant recipes remain intact, including generated-code, facade,
Core SQL, no-default-feature and workspace/reverse-consumer tests.

| Selection | Required stages | Exclusions |
| --- | --- | --- |
| Database, explicit workspace or uncertain/shared inputs | All 15 | None |
| CLI-private Rust command/presentation/tests and CLI notes only | Common tools, compiler, formatting, workflow/shell lint, dependency policy, invariants; CLI lint, all-target check, all-target unit/process tests and verified Cargo package | Workspace clippy, check, feature matrix and test; all engine inputs must be unchanged since the admitted CLI baseline |

The supplemental stages are `clippy-cli`, `check-cli`, `test-cli` and
`package-cli`. The full qualification plan deliberately includes them in addition
to the old gate, so that the comparison also proves the narrow recipes. Existing
workspace checks already include CLI compilation/tests; no duplicate work is
added to the default release gate.

The selector requires clean committed HEAD and an annotated ancestor tag from
the selected group. Shared observes the baseline and complete diff; unavailable,
lightweight and non-ancestor baselines remain consumer refusals. Shared also
checks roster/group equality; product mapping and recipe admission remain local. Manifests,
locks, features, shared/compiler/CI/release files, generators, fixtures, public
library/build surfaces, database sources and unknown paths expand to workspace.
No change also selects workspace. Every stage has a reason. CLI-only release
versions, other-group ledgers and package versions remain governed by the normal
release adapter, not this selector.

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
`GROUP-vVERSION.commit` release receipts retain their original semantics.
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

Fixture qualification exercises the actual selector, plan runner and receipt
with substitute stage bodies. It proves selection, exact target dispatch,
required/excluded failures, conservative expansion, recipe/roster drift, unavailable baselines, source/configuration drift,
interruption, fresh retry and failure to replace a receipt after log tampering. It does not qualify
product, archive or native-host behavior.

Before release activation, run matched real full/selected plans on the same
admitted CLI presentation source and review all omissions. Database keeps the
full roster. Both native macOS hosts, final archive dependencies/provenance,
registry order, public/generated identities, Wasm, retained persistence/recovery
and applicable installed contracts remain independent requirements. The isolated
Canic workspace retains its own lock and qualification; this plan neither adds
it to root Cargo commands nor waives its existing blockers. `package-cli` verifies
the packaged crate, but does not substitute for final release metadata, registry
availability or archive/provenance review. Current dependency requirements may
correctly block package verification until database dependencies are published.

No release callback consumes these qualification receipts yet. Activate omissions
only after the real mapping, receipt integration and required host/artifact
qualification in #603 are complete. Ordinary focused checks do not authorize a
full comparison or a release.
