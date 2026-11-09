# Content-bound release validation reuse

This is the opt-in stage contract for
[#334 — Review content-bound validation proof reuse across independent release groups](https://github.com/dragginzgame/toko-tooling/issues/334).
`scripts/ci/run-validation-stage.pl` implements exact-input execution and reuse
with new stage evidence. Existing release runners, whole-gate receipts and
complete gates keep their current behavior. A consumer must explicitly select
and qualify a bounded stage before using this interface in its release gate.
The exact-input runner never normalizes Cargo versions. An explicitly qualified
consumer can instead prepare the final candidate described below and run its
stages against those exact prospective versions.

[Group validation plans](release-group-validation.md) separately decide which
obligations apply. An applicable stage may reuse evidence only through this
exact-input contract; a not-applicable stage is neither executed nor reused.

## Executable interface

The caller supplies an executable observer, an explicit bounded environment as
canonical JSON, a byte allowance for each input/log, the exact command, a trusted
exclusive state directory and fresh source/group identities. For example:

The source identifier is a caller-admitted 40- or 64-digit hexadecimal content
identity, such as a Git commit or a retained source-projection digest. This tool
does not authenticate or admit that identity on the caller's behalf.

```bash
perl scripts/ci/run-validation-stage.pl \
  --state target/validation-stages --stage shellcheck \
  --contract consumer-shellcheck/1 --source "$(git rev-parse HEAD)" \
  --group persistence --observe scripts/ci/observe-shellcheck.sh \
  --environment target/stage-environment.json --max-bytes 33554432 \
  -- bash scripts/ci/check-shell.sh
```

The environment object contains only string values and must declare `PATH`.
Observer and command children receive exactly that environment, including the
absence of every undeclared variable. The observer writes one canonical JSON
object, with exactly `schema: 1`, a nonempty `files` array and a `configuration`
object. Source file paths are canonical relative paths; absolute tool paths may
use managed selectors. Include the complete expanded file inventory, every
external tool the stage/observer reads, and relevant configuration. A new matching
file then changes the inventory. Include generated inputs and native settings
where applicable. Canonical JSON uses sorted keys, compact encoding and one final
newline (the fixtures qualify `jq -cnS` for their selected values).

The helper records resolved paths and SHA-256 identities, the observer and runner,
the interpreter and first command executable, argv, bounded environment, physical
working directory and native host. It re-observes inputs after child execution
and immediately before admission, and checks original proof/log bytes again after
the final observer. It never projects versions or treats an old whole-gate receipt
as a stage proof. All evidence remains under the printed attempt directory.

Every call produces a separate `admission.json` on success, identifying `executed`
or `reused`, current source/group, context digest and the original execution-proof
path/digest. Original executions live in `proofs/`; reuse never rewrites their
origin. Unknown, incomplete or stale candidate records take ordinary execution;
unsafe state routes refuse. Failed commands keep their primary status even when
evidence publication also fails. Drift, interrupted execution or failed observation
cannot create an admitted stage. This interface does not admit a release source,
index, group policy, package archive, registry delivery or a complete gate.

## Explain execution and reuse decisions

For each reusable stage, report whether it executed or reused admitted evidence.
When reuse is unavailable, report the specific reason alongside the stage result:
for example, an unqualified group scope, host, jobserver/parallel setting, custom
target directory, simulator selection, environment, missing proof or changed
input identity. Emit this during normal release output and retain it with the
attempt; do not require users to read the implementation to explain a full rerun.
Keep paths and values redacted where they could expose secrets.

Review recurring fallbacks against the configurations consumers actually use.
Qualify useful group scopes and common parallel/target/simulator configurations
when their complete inputs can be bounded. Never remove an eligibility condition,
unset a user selection or normalize configuration just to force a cache hit.
Source-only analysis identifies opportunities; compare matching executed/admitted
attempt timings before claiming savings. Missing or invalid evidence still requires
execution or refusal under the existing contract.

## Decision and existing boundary

Reuse belongs to a validation **stage**, with fresh admission by every release
group. A successful earlier release is not an interchangeable whole-gate proof.
Keep the existing runner, group selection, saved intent and supplier-first
publication. A stage can satisfy its obligation through execution or through an
admitted original execution; all other obligations still execute normally.

Persistence's `Repository::verify` currently invokes `make validate` once and
records `ValidationEvidence` containing the selected release, full lock bytes,
tool identities, configuration and command. That format establishes the executed
whole gate at its original source. It has no stage inventory or reusable-stage
input contract. Existing records must never be converted to stage passes by
comparing Rust files, changing their source field or removing group identity.

The measured 694.592 seconds across three gates establishes repeated cost, not
which stages can safely be reused. Start with a stage whose complete inputs can
be bounded. Persistence's `lint-tooling` is a plausible first candidate: it runs
Actionlint on workflow YAML and ShellCheck on selected environment files, shell
scripts and the hook. Its Make recipe, tools, configuration, source closure,
directory enumeration and relevant environment must all be covered. This is a
candidate for qualification, not an admitted omission of Cargo manifests from
another stage. No saving is claimed before measurement.

## Ownership and closed input selection

The consumer owns the stage name/revision, exact argv and working directory,
complete input closure, environment policy, tool identities and native scope.
Shared owns schema admission, comparison and preservation mechanics. IC Build's
managed editor remains the only owner of any Cargo version projection; a shell
or JSON comparator must not implement a second TOML/lock editor.

An opt-in stage contract must account for:

- Source, tests, scripts, generated inputs, manifests, selected independent
  locks and feature/target selections actually read by that stage. Include the
  inventory itself: a new file matching a glob must invalidate an old proof.
- Compiler, Cargo, formatter/linter and child executable identities; executable
  bytes as well as version strings where the stage's tool admission requires it.
  Include flags, wrappers, Cargo configuration and configuration precedence.
- Complete resolved external package identities/checksums and relevant caches or
  generated/native inputs. A path crate's unchanged Rust files do not establish
  unchanged public crate identity or `CARGO_PKG_VERSION` behavior.
- Host OS/architecture and native configuration, server/optimizer identities,
  working-directory semantics and all relevant environment. Either capture
  relevant variables including absence, or execute under an admitted bounded
  environment. Ambient unknown inputs make the stage ineligible.
- VCS data or archive provenance if the command can observe them. Exact final
  archives, registry availability and publication provenance always receive
  their own fresh admission, even if an earlier compilation stage is reused.

Prefer exact byte equality. Hashes bind a complete admitted inventory; they do
not discover its completeness. If input completeness cannot be demonstrated,
execute the existing stage. Do not add a consumer skip flag or infer eligibility
from file extensions, a Git diff summary or a "metadata-only" label.

## New evidence, separate from retained receipts

Use two new, strictly decoded formats, with numeric `schema: 1` and distinct
`kind` fields. Existing receipts are not input formats of this command. Unknown schemas,
unknown fields, duplicate keys, invalid identities and incomplete records refuse.

An **executed stage proof**, kind `release-stage-execution`, contains:

| Field | Meaning |
| --- | --- |
| `origin` | Immutable original repository, source/tree and release attempt identity. |
| `context` | Stage/contract, command/cwd, complete input inventory/digests, tools, bounded environment and host. |
| `result` | Completed successful child status. |
| `logs` | Retained stdout/stderr paths and byte identities in the trusted attempt. |
| `elapsed_seconds` | Monotonic elapsed execution and post-execution observation time. |

Only the execution path writes this proof, after the command exits successfully
and its inputs are re-observed unchanged. A command that emits plausible output
then fails cannot produce a successful proof. Failed, cancelled, interrupted or
unfinished execution is retained as attempt evidence, never a reuse candidate.
Do not overwrite an earlier execution proof or replace its origin on reuse.

A **stage admission**, kind `release-stage-admission`, contains the supplied
fresh source/group identity, required stage contract, current context digest,
exact original execution-proof path/digest, comparison mode and elapsed time.
Source/index/policy admission remains with the existing caller. It references the original proof directly; never build a
chain of reuse receipts that loses the executed origin. Current group receipts
must distinguish `executed` from `reused` obligations and record each stage's
evidence. Introduce a new consumer receipt schema for that composition; the
current whole-gate format is not silently broadened.

Digests do not authenticate arbitrary user-provided proof files. Admission must
retain the consumer's existing trusted-state custody and exclusive source/state
ownership requirements. Reject symlinks, missing proof/log content and ambiguous
provenance according to those boundaries.

## Admission and recovery

1. Admit the current source, index, explicit group policy, configuration and
   saved intent through the existing consumer checks. Reuse never bypasses them.
2. Enumerate the full required stage roster. A consumer cannot select only the
   stages for which it happens to have old evidence.
3. Re-observe one stage's current inputs and compare its contract, command,
   tools, host and complete input inventory with an original successful proof.
   Missing, stale or unrecognized evidence takes the ordinary execution path;
   corruption or a custody error retains its exact error and evidence.
4. On a match, write separate admission evidence. On a mismatch, execute the
   existing command and record a new immutable execution proof only on success.
   Preserve the initial failure, partial output and all earlier receipts/logs.
5. Re-observe source and relevant inputs after subprocesses and before recording
   the group's composed validation receipt. Drift invalidates admission; a
   cached comparison before a helper ran is insufficient.
6. Continue normal preparation, exact final archive qualification, registry and
   provenance checks. Every group keeps its own receipts and recovery state.

Use the existing private temporary-file/atomic-publication and evidence-retention
mechanics. No second batch release engine, automatic rollback, cross-group receipt
rebinding, implicit cleanup or publication is part of this design. Resume must
re-admit the saved attempt and original evidence; a partial admission record is
not a completed stage. Fresh group policy also checks unrelated-group preservation.

## Cargo version projection

Version projection is a separate, explicitly qualified capability, disabled by
default. It is unnecessary for an exact-input stage whose admitted closure does
not read release metadata. For any stage that does, start by executing normally.

A future projection must render from retained original bytes through the
canonical editor, prove the complete projected file set equals the destination
bytes and reject every change outside the admitted group plan. It must preserve
unrelated groups, dependency identities/checksums, requirements and independent
locks. Keeping Cargo resolution equal is necessary but insufficient: package
versions can affect generated code, environment, tests, public identities and
archives. Qualify those effects for the particular stage before admitting an
equivalence rule. Ambiguous projection or an unsupported input forces execution.

## Qualification before activation

Exercise exact matches and drift in code/tests/features, external IDs/checksums,
tools, host, environment, generated inputs and directory inventories. Prove that
missing/stale/unknown evidence, plausible failed output and cancelled execution
cannot satisfy a stage. Mutate inputs between observation, subprocess return,
receipt publication and resume; preserve the primary and retention failures.

Use a real consumer stage through its existing Make/adapter dispatch, with
separate group receipts and all required stages still accounted for. Demonstrate
that a group change preserves unrelated groups and that every group still runs
fresh source/index/policy and archive/publication admission. Qualify Linux and
each required native macOS host separately; proof reuse cannot cross hosts by
assuming equal tool version strings.

Measure actual executed-stage counts and monotonic elapsed time for execution
and admission at the same selected inputs. Report comparison overhead, which
stages still execute and observed savings without promising the original full
11.6 minutes will disappear. Substituted command fixtures prove ordering and
failure propagation, not native acceptance, archive qualification or a speedup.

## Prepared final Cargo candidates

`prepare-cargo-stage-source.pl --root DIR --state target/STAGE-CANDIDATES
--source COMMIT --editor ABSOLUTE` is an opt-in source producer, not a release
runner or an equivalence rule for old binaries. It requires clean committed
source/index, regular tracked files, the admitted IC Build editor, locked offline
resolution and the ordinary Cargo configuration. Build/configuration overrides
require ordinary execution rather than being silently discarded.

The producer exports the selected source and uses the canonical group metadata
transaction/editor to render each numbered `Unreleased` candidate. Finalized
groups keep their current version. The resulting disposable source therefore
compiles and tests the actual intended final versions, including package-version
constants, checkpoints, build scripts and retained document fields. A later group
can reach the same candidate bytes after earlier supplier groups have shipped.
It cannot reuse evidence merely because a diff appears to contain only versions.

Candidate-only ledgers and group metadata files are replaced with fixed,
non-authorizing stage fixtures. Those inputs must not carry application data.
The consumer must qualify that its selected stages do not require their original
contents, and must retain fresh real metadata/receipt/rollback admission. A
candidate's empty receipt cannot authorize delivery. Never deploy, package for
publication or copy these stage-only records into the real checkout. Exact mixed-
version package archives, source/index/group-policy and registry admission remain
fresh in the owning checkout. Consumers also keep a small fresh check of actual
intermediate version-sensitive integration surfaces where those differ from the
final candidate; caching the prospective integration suite does not prove them.

The identity binds the complete candidate file inventory, bytes and modes.
Rendering changes no parent source/index. A retained candidate is checked for
source/mode drift and dirty state before use; it is never silently repaired.
Source, failed preparations and execution logs remain under the declared target
state. This is reproducible validation scratch, not an additional implementation
or delivery checkout. Managed tool selection stays with the caller through an explicit read-only
tools root. The producer creates no tool-root symlink and performs no setup.

`observe-cargo-stage.pl` inventories exact candidate files, expanded dependency
sources, native compiler tools/headers and Rust toolchain payloads. It runs in an
explicit bounded environment with caller-supplied tool inputs. Its current native
contract is Linux; other hosts retain ordinary execution until separately
qualified. The normal exact-input runner retains immutable executed proof and
fresh admissions, including native host, tool, environment, command and working-
directory bindings. `--admission-output FILE` writes a separate immutable machine-
readable admission for thin callers; occupied or redirected destinations refuse.
Executable aliases preserve their invocation name (for example the Cargo rustup
proxy) while the resolved payload is hashed and re-admitted.

`seal-validation-stage.pl ADMISSION SOURCE GROUP CANDIDATE_ID` verifies the original
execution proof/log identities and produces compact self-contained custody
bindings. Consumers embed those bindings in a new receipt kind, distinguish
execution/reuse, and retain fresh source/selection and product evidence. This does
not convert old whole-gate receipts or grant permission to reuse unrelated stages.

Before publication can consume candidate lint, run
`check-cargo-stage-closure.pl ACTUAL CANDIDATE PRIMARY`. It requires equality of
the actual selected package's complete resolved package/target/feature/dependency
closure, retaining versions and source identities. Different physical source roots
are rebased only for that comparison. A not-yet-released dependency, changed
feature or mismatched primary refuses equivalence; use ordinary current-source
lint. The consumer must qualify its lint command's coverage of that primary.
Actual archive creation/verification, upload admission and registry provenance
never become cached stages.

Publication may consume an existing completed lint qualification without treating
it as a new execution on today's host. Verify its original proof/log custody and
run `prepare-cargo-stage-source.pl --verify-projection ABSOLUTE_JSON` to recheck
the retained candidate's complete file inventory, modes and clean state without
rendering or tool setup. The projection must come from the admitted source-bound
receipt, never an untrusted lookup. Re-admit the actual release source and exact
primary closure as above; missing/damaged local evidence requires ordinary lint.
This does not create a new generic exact-input admission, change the original
qualification host or cache archive/registry checks.
