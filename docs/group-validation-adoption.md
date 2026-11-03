# Shared group-validation interfaces
<!-- Shared companions: scripts/ci/validation-evidence.pl scripts/ci/run-validation-targets.sh scripts/ci/release-receipts.sh -->

Use `scripts/ci/validation-evidence.pl` for common observations around the existing
validation runner. It writes one result to stdout only after successful admission;
failures return nonzero and diagnostics on stderr. It never selects product tests,
executes a gate, grants release authority, installs tools or updates receipts.
Consumers retain exclusive custody of source and evidence during observation.

## One full stage roster

Keep the existing canonical policy shape:

```json
{"groups":["client","runtime"],"schema":1,"stages":["lint","client","runtime"]}
```

Project group names from the already admitted Cargo group policy into a canonical
JSON array. This interface checks equality with the validation policy, not Cargo
policy validity or package membership. Keep canonical group admission and Cargo
metadata as those authorities; do not parse Cargo.toml again.

```bash
jq -cS '[.groups[].name]' ci/cargo-release-groups.json > target/group-names.json
perl scripts/ci/validation-evidence.pl roster "$PWD" \
  ci/release-validation.json target/group-names.json > target/full-stages.txt
```

The output lists each stage once, in policy order. Full CI can read that file into
a Bash array and pass it to `run-validation-targets.sh --fail-fast`. Check the
roster command's status before dispatch. Local release selectors use the same
declaration to produce complete plans; they do not regex-parse Make recipes or
maintain another full-gate list. Account for separate release-only obligations
explicitly rather than implying that every CI roster is the entire release gate.

## Conservative baseline observation

```bash
perl scripts/ci/validation-evidence.pl baseline "$PWD" "$RELEASE_SOURCE" \
  "refs/tags/$RELEASE_GROUP-v$RELEASE_PREVIOUS" > target/baseline.json
```

The result has `schema`, `source`, `reference`, `available`, `reason` and `changes`.
Existing references also include `object`; annotated commit tags include
`baseline`. Missing, lightweight and non-ancestor tags return `available:false`
and an empty change list. Git observation failures refuse. Only an annotated
ancestor yields the complete no-renames changed-path list; an equal source yields
an empty list. An empty list with an unavailable baseline is not “no changes.”
Reference movement during observation refuses. No network lookup is performed.

The caller derives and admits the expected group/tag/version, trusts or refuses
the tag, qualifies host/configuration and interprets product impact. `available`
means an observation exists, never that narrow validation is qualified. Unknown
scope keeps the full gate. This helper does not replace Tooling's hosted-baseline
qualification or other product-specific baseline policy.

## Verify and retain one run

After successful execution through `run-validation-targets.sh --plan`, select the
exact retained run directory. Keep it relative to the physical source root and
unambiguous; do not choose the first result of a glob. A consumer-owned canonical
JSON context object must be among the declared plan inputs before execution.
It records the actual host/compiler/configuration and any product observations
needed by that consumer. Shared binds its bytes; it cannot prove that the caller
observed those values correctly or that the input inventory is complete.
Include the selected runner/helpers, recipes, selection policy and tool/config
observations in that inventory; the generic schema is not a substitute for it.

```bash
perl scripts/ci/validation-evidence.pl verify "$PWD" \
  target/release-validation/plan.json "$RELEASE_SOURCE" "$RELEASE_GROUP" \
  target/release-validation/logs/validation.ABC123 \
  target/release-validation/context.json > target/release-validation/proof.json
```

For a protected receipt, invoke the verifier through the existing
`release_receipt_write` helper instead of redirecting over an old file. It
publishes only a successful result and preserves previous evidence on failure.
The generic proof is a component of the consumer's receipt, not that receipt's
release authorization. Fresh source/index/intent, group-policy, archive and
publication admission remain with the existing adapter.

The verifier uses the same plan parser as the dispatcher. It checks exact
plan/policy and selection bytes, initial/expanded input bindings, every required
stage's before/after checkpoints, ordered initial and terminal events, one
successful timing per required stage and each retained regular log. Excluded
stages must remain not-applicable. It reobserves current declared input bytes and
evidence before returning. Unknown, missing, failed, interrupted, duplicate,
reordered or redirected records refuse. The caller must preserve exclusive
custody; this is not a filesystem transaction or protection against a malicious
writer replacing all trusted evidence consistently.

The canonical result is `schema:1`, `kind:"group-validation-evidence"`, with:

- Source, group, scope and relative run identity.
- SHA-256 bindings for plan, policy and caller context.
- The complete runner input checkpoint, including inherited extra inputs.
- Hashes of admitted runner records, checkpoints and logs under `files`.
- Ordered stage results under `stages`: `PASS` with seconds/log/hash, or
  `not-applicable` with its reason.

`PASS` proves the recorded Make target passed. It does **not** claim its underlying
check executed again. A target using exact-input stage reuse must retain and admit
that separate execution/reuse proof in the consumer receipt. This interface does
not grant reuse, normalize package versions or convert old full-gate receipts.

To re-admit retained evidence without replacing its original proof:

```bash
perl scripts/ci/validation-evidence.pl check "$PWD" \
  target/release-validation/plan.json "$RELEASE_SOURCE" "$RELEASE_GROUP" \
  target/release-validation/logs/validation.ABC123 \
  target/release-validation/context.json target/release-validation/proof.json
```

`check` recomputes and compares the complete canonical result. Tampered logs or
context cannot silently become a new baseline. It requires the original physical
source/evidence location because the runner's timing/event logs contain absolute
paths. Do not rewrite those paths to relocate proof. Publication in a fresh clone
and recovery after version preparation retain their original consumer custody
contracts; reconstruct/admit the original source through that owner, or use its
qualified self-contained receipt. These commands alone cannot qualify relocation
or release-version projection.

Each JSON/TSV record is bounded to 1 MiB, subprocess observations to 4 MiB, and
each hashed log/input to 64 MiB. Unsupported sizes refuse; nothing is truncated.
Paths must be regular, contained and free of traversal/control characters or
symlink components below the physical root. JSON results are bounded to 1 MiB,
so a generated proof is always within the reader's allowance. Consumers must
also bound and check subprocess output/status.

## Migration and qualification

Keep local product declarations, source/configuration admission and thin callers.
Replace generic event/timing/path parsing with the shared verifier; remove the
superseded code only after the actual consumer mapping and recovery path pass.
Rust or JavaScript callers invoke the same executable and deserialize its JSON;
they do not translate the protocol into another implementation. The Shared
fixture exercises a shell pointer caller and a dependency-free compiled Rust
process caller over real retained runner evidence, including required failure
and changed-proof refusal. It is an interoperability fixture, not a claim that
Storage or Tooling adopted the helper or passed product/native qualification.

Full-versus-selected comparisons establish a mapping and renew it when relevant
inputs change. Keep required native acceptance, but do not permanently run the
full gate plus every selected gate on every ordinary CI change. Update baseline
discovery when changing qualification jobs. Common parser/roster/evidence
regressions stay in Shared; consumer fixtures cover actual policy and wiring.
