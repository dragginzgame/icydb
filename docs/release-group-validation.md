# Release group validation
<!-- Shared companions: scripts/ci/run-validation-targets.sh docs/validation-runner.md -->

Independent versions should not force unrelated release checks. A consumer may
qualify a complete gate for one release group, including its affected integration
boundaries, while keeping repository-wide CI. Until that mapping is qualified,
release adapters retain their existing full gate. This is the opt-in contract for
[#345](https://github.com/dragginzgame/shared-tooling/issues/345).

## Ownership and selection

The consumer owns the exhaustive stage roster and a reviewed selector. Shared
admits the resulting plan and runs its targets through the existing
[validation runner](validation-runner.md). This is not a dependency resolver or
an automatic interpretation of a Git diff. A file extension, group name or
unchanged version alone cannot establish that a check is irrelevant.

The selector must account for the complete proposed release relative to an
admitted baseline, including outstanding changes since the last relevant
qualification. It declares applicable packages, features, targets, native hosts,
direct/reverse consumer contracts, generated inputs, public identities, runtime
singletons, ABI/retained formats and installed behavior. Every omitted stage
gets a concrete reason. The roster must correspond to the existing full gate;
adding a full-gate stage also updates that roster and its selection fixtures.
The consumer must check that its group roster matches its Cargo release policy.

Shared implementation, compiler, CI/release scripts, relevant dependency locks,
features, generators and public identity changes broaden the required stages.
If the selector cannot prove the affected scope, it must refuse or select the
full roster. Do not infer completeness from hashes: they preserve the selected
inputs, not the correctness of the selection. Keep the selector, baseline
identity, configuration and actual stage recipes in the checkpoint inventory.
Source admission, exclusive checkout ownership and tools/configuration admission
remain with the existing release adapter.

## Runner interface

After normal source and group admission, the adapter produces canonical JSON
with `jq -cS` (one final newline). Both documents are bounded to 1 MiB. The
policy is version-controlled; the generated plan belongs under `target/` or the
existing release attempt directory. All paths passed in the plan are relative
to the repository root and must resolve through regular, non-symlink routes.

For example, a consumer policy before canonical serialization is:

```json
{
  "schema": 1,
  "groups": ["client", "runtime"],
  "stages": ["lint", "client-check", "runtime-check"]
}
```

The selector emits the following shape, filling `source` with the admitted
`RELEASE_SOURCE`, rather than the placeholder shown here:

```json
{
  "schema": 1,
  "source": "<admitted Git HEAD>",
  "group": "client",
  "scope": "group",
  "policy": "ci/release-validation.json",
  "inputs": ["Makefile", "Cargo.lock", "ci/cargo-release-groups.json", "ci/selection-context.json", "scripts/release/select-validation.sh"],
  "stages": [
    {"target": "lint", "required": true, "reason": "Common release policy"},
    {"target": "client-check", "required": true, "reason": "Client and its facade consumer boundary"},
    {"target": "runtime-check", "required": false, "reason": "Admitted client-only change does not affect runtime inputs or contracts"}
  ]
}
```

The example stage names and inputs are illustrative, not an admitted Platform
mapping. Actual recipes include their complete input inventories. Preserve
lint-before-expensive-work ordering and native host obligations. A host-specific
plan may exclude another host's stage only when the release adapter separately
requires that host's admitted evidence before delivery.

```bash
# RELEASE_SOURCE and RELEASE_GROUP come from the existing release runner.
VALIDATION_LOG_DIR=target/release-validation \
  bash scripts/ci/run-validation-targets.sh --plan target/release-plan.json
```

Plan mode requires a nonempty `RELEASE_GROUP`, the exact current Git
`RELEASE_SOURCE`, retained `VALIDATION_LOG_DIR`, and no positional Make targets.
It always stops on the first failure. The policy must name the group; the plan
must classify every policy stage exactly once, with at least one required stage.
Unknown schemas/fields/stages, duplicates, missing inputs or redirected paths
refuse. `scope: "workspace"` requires every stage; it cannot contain omissions.
Normal positional-target callers keep their existing behavior.

The runner checkpoints the plan before parsing, retains parsed plan/policy
bytes and their hashes, then checkpoints the expanded inventory. It checks those
inputs and Git HEAD before and after every target. A changed plan or policy
cannot become a fresh baseline midway through selection. Persistent drift
stops later work. Checkpoints do not isolate running commands from concurrent
writes; the release adapter must still admit and hold the selected source.

## Evidence and recovery

Each unique runner directory retains:

- `parsed-inputs.json.plan.json` and `.policy.json`: admitted selection and policy.
- `plan-selection.tsv`: complete input/target selection and exclusion reasons.
- `plan-inputs.json`: selection/configuration checkpoint, alongside observations.
- `plan-events.tsv`: append-only target states. Required stages initially read
  `blocked`; excluded stages read `not-applicable`. Completed attempts append
  `PASS` or `FAIL` and their log path. The last event for a target is its state.
- Normal raw logs and `timings.tsv`: actual Make execution and durations.

A failed input check can produce `FAIL` before a target executes; consult its
log. Interrupted or never-started stages retain `blocked`, never a synthetic
pass. These files describe runner execution, not an authorized release receipt.
The consumer's versioned receipt must bind the plan/policy bytes, complete
input checkpoint, source, group, configuration, host and actual successful
obligations. It must reject missing/failed evidence and re-admit the same plan
on recovery. A new source or scope requires a fresh plan and gate; never overwrite
old attempts or reinterpret historical full-workspace receipts.

A target may delegate an applicable stage to the existing
[exact-input reuse interface](release-validation-reuse.md). Its PASS means the
wrapper succeeded, not that the underlying check executed again. The consumer
receipt must retain that stage's `executed`/`reused` admission and original
execution proof separately. A failed or missing reuse proof cannot be counted as
a pass. Plan selection itself never grants reuse or projects package versions.

Fresh source/index/group-policy admission, unchanged unrelated versions and
ledgers, final archive qualification, registry/provenance checks, publication
order, enforced protections and recovery all remain required. Applicable native
and installed proofs remain required. No publication or sibling release is
performed by this helper.

## Platform pilot and subsequent adoption

Start with the approved four-group Platform boundary, after Shared delivery and
explicit consumer adoption. Keep Platform's present full release gate until its
real mapping and receipt integration are qualified.

| Selected change | Required boundary review |
| --- | --- |
| PocketIC client only | Client, facade re-export and external DTO identities, installed client behavior and supported native hosts. Exclude browser/runtime only with explicit unaffected-input evidence. |
| Types | Principal/public identity consumers, conversions, native/Wasm features, archives and affected Runtime/facade consumers. |
| Runtime | Macros/guard coordination, singleton composition, affected Timers/Endpoint consumers, runtime ABI and installed behavior. |
| Facade | Selected crypto/external adapters and their actual native/browser/Wasm consumers. |
| Shared or uncertain inputs | Full existing roster unless a reviewed selector proves a narrower complete scope. |

Qualify each group and shared-input expansion against the existing full gate on
the same source, with an explanation for every omitted stage. Inject a required
failure (must block) and an unrelated excluded failure (must not block the narrow
release). Cover unknown groups, omitted/new roster entries, stale plans, changed
source/inputs, missing evidence, interruption/recovery and preservation of other
groups' versions and ledgers. Keep all required native hosts. Measure commands
and elapsed time; do not promise a saving before real consumer qualification.

Shared's `test-release-validation-plan.sh` exercises two synthetic groups,
shared-input expansion, full-roster command equivalence and refusal/recovery.
It does not compile Platform or establish its macOS/installed qualification.
Platform's adoption is the next step; Tooling and other grouped workspaces follow
with their own mappings. Keep repository-wide CI throughout.
