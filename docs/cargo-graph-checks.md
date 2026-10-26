# Resolved Cargo graph checks

`--json` emits a complete structured result after successful Cargo observation
and parsing: selected consumer, exact command, target, roots, pass/fail and
package-labelled violations. A policy failure returns 1 in JSON mode. Failed or
malformed observations emit no JSON result and still return nonzero.

Add `--explain` to ask the same Cargo invocation for ASCII tree prefixes. Violations
then include the first observed package or feature path in that selected tree;
text mode also prints the path. Targets, features, depth limits and edge kinds
remain unchanged. These are representative paths, not every introducing path
or a new resolver; Cargo can deduplicate repeated subtrees. Missing required
packages have no path. For example:

```bash
perl scripts/ci/check-cargo-graph.pl --consumer . --package example \
  --target wasm32-unknown-unknown --forbid candid --json --explain
```

Shared Tooling owns the observation/checking mechanics. Consumers own the roots,
targets, features, forbidden packages and required identity counts. This helper
complements direct declaration checks; it does not replace them or infer policy
from dependency-role comments.

```sh
perl scripts/ci/check-cargo-graph.pl --consumer . --package production-package \
  --target wasm32-unknown-unknown --all-features \
  --forbid candid_parser --forbid pocket-ic --require ic-wire=1
```

This example selects a Wire application root. Choose exclusions and counts for
the actual product under the [encoding policy](https://github.com/dragginzgame/shared-tooling/blob/main/DRAGGINZGAME.md#ic-application-encoding);
required Candid interoperability is not a blanket graph violation.

Pass one or more explicit `--package NAME` selections and one `--target TRIPLE`
(or `all` for a deliberate cross-target union). Match the actual build's default
feature behavior: use `--no-default-features` and `--features LIST` when needed,
or `--all-features`. Select at least one exclusion, exact count or maximum count.
Repeated literal exclusions compose idempotently across consumer policy lists. Counts are per package name; zero requires absence.
`--max-count NAME=COUNT` admits an absent optional package while limiting observed
identities (for example, at most one crypto generation). It conflicts with an
exact count or exclusion for the same name. `--forbid-pattern REGEX` evaluates a
caller-owned Perl regex against canonical package names, preserving family rules;
invalid patterns fail before observation. Prefer literal names for exact policy.
`--depth COUNT` selects Cargo's package-edge depth for direct-dependency checks;
the default inspects the complete graph. `--forbid-feature PACKAGE/FEATURE`
adds Cargo's feature edges and refuses matching rendered dependency-feature edges.
Overlapping selected roots may carry Cargo's `(command-line)` feature annotation;
it is admitted without changing forbidden-feature checks or package identity counts.
This preserves dependency feature-edge policies; it is not an exhaustive inventory
of every enabled/root feature. Package identities still come only from package
labels, and normal/build edges continue to exclude development dependencies. Roots, target and features stay explicit. Invalid/conflicting
policy and CR/LF directories fail before Cargo, including resolved path aliases.

The helper runs locked, offline Cargo tree with normal/build edges and flat
`{p}` labels. It admits output only after successful completion; a failed or
signalled child cannot supply a successful observation. Empty/malformed output
or missing selected roots fails. Names are Cargo's canonical package names,
including dependencies renamed by callers. Development edges are excluded.

Identity counting retains each full rendered label, including version and any
source qualifier Cargo prints; only the repeated-node `(*)` marker is removed.
Two versions or differently qualified sources count separately. This deliberately
matches existing consumer tree checks, not a source-authentication or artifact
identity guarantee. Registry/source admission, lock/source custody and exact bytes
remain separate obligations. Do not substitute name/version-only identity parsing.

Consumers may replace their repeated tree invocation, name matching and rendered
identity collectors with this checker, keeping feature matrices, cryptographic
policy, timer placement and lifecycle/recovery tests local. Prefer explicit calls
over new wrappers that only rename this command. Snapshot consumers select this
script as an entrypoint through the canonical exporter; do not copy/edit it.

A passing graph check does not compile or deploy anything. Bind successful locked
build evidence, source/lock identities, actual target/features and final artifact
digest to any deployment. Native hosts and installed IC behavior retain separate
qualification.

## Coordinated release adoption

A coordinated workspace version simplifies selection; it does not prove that
consumers have adopted the same dependency identities. After an authorized
upgrade, inspect each affected consumer's selected locked graph, including
public types exchanged with other libraries. A commit label or pending changelog
heading is not proof of a finalized version, registry publication or adoption.

Use targeted identity counts where multiple generations would violate an actual
consumer contract. Reuse `--require NAME=1` when the package must be present, or
`--max-count NAME=1` when it is optional. Match the production roots, target and
features, and qualify native tooling separately. Do not infer duplicate Wasm
code or a defect from multiple entries in a whole-workspace lockfile; independent
test, host and runtime graphs can legitimately select different versions.

Finish an accepted migration before treating its temporary mixed selections as
a permanent design defect. Add or tighten a consumer guard with the completed
adoption when justified; do not create a blanket duplicate-dependency ban or
restructure workspaces merely to make a lockfile uniform. Existing ownership,
focused validation and release authority still apply.

## Focused dependency-adoption contract

After an authorized dependency change, qualify every affected independent graph
before expensive artifact checks. This is a consumer-owned opt-in check, separate
from dependency preparation and the complete release gate. The
[dependency preparation rules](../rules/cargo-dependencies.md#preparing-authorized-dependency-changes)
still own lockfile updates and cache preparation.

Keep the orchestration local. Cargo already owns resolution and target/feature
selection; Shared supplies declaration/graph checks and the existing fail-fast
Make runner. A new shared adoption script or automatic affected-package detector
is unnecessary. Consumers declare:

- The root and independent workspace manifests, including fixtures that consume
  changed packages through paths. Inventory each selected lockfile explicitly.
- The affected package roots, actual deployed targets/features and any separate
  native test/tool profiles. Do not infer these from dependency-role comments.
- Which identities must be singletons within each selected graph. Independent
  graphs may legitimately select different versions; do not enforce fleet-wide
  version equality or treat a repeated `(*)` node as a second identity.
- The affected-package Clippy selections and read-only artifact freshness checks.
  Product fixture rosters, embedded bytes and refresh commands remain local.

Run these phases sequentially, stopping at the first failure:

1. **Admit all selected graphs cheaply.** Run full locked metadata for each
   manifest with its selected compiler and prepared cache, for example:

   ```sh
   cargo metadata --locked --offline --format-version 1 \
     --manifest-path integrations/example/Cargo.toml > metadata.json
   ```

   Do not use `--no-deps` as the only lock-consistency proof. Do not update a
   stale lock, fetch missing inputs or retry unlocked inside this check. Identify
   the manifest and retain stderr; a failed command's plausible JSON must never
   become admitted input. Establish success before parsing redirected output.
2. **Apply explicit dependency policy to each graph.** Reuse
   `check-cargo-dependency-boundaries.sh` where the consumer has a declaration
   policy, then the graph checker above where its normal/build scope matches.
   Cargo owns renamed packages, target-specific edges and selected optional
   features. A runtime-only singleton rule needs a separate Cargo observation
   with `--edges normal`; the shared checker includes build dependencies and
   must not be presented as runtime-only evidence. On a conflict, report the
   manifest, selected root/target/features, identities and their dependency paths
   from the same selection. A failed diagnostic command remains failed evidence.
3. **Lint every affected selection.** Run the consumer's explicit affected-package
   Clippy checks, including relevant tests/features and independent fixtures.
   Root-workspace Clippy does not compile another workspace. Preserve its lint
   policy, locked inputs and feature selection; do not silently substitute a
   default-feature build for the selected deployed profile.
4. **Check artifacts without refreshing them.** Only after all lint passes, run
   the consumer's existing verifier. A mismatch requires separate authorized
   regeneration and requalification, never automatic replacement by this check.

Consumer Make targets can reuse the existing logger without another scheduler:

```sh
bash scripts/ci/run-validation-targets.sh --fail-fast \
  adoption-metadata adoption-policy adoption-clippy adoption-artifacts
```

These are illustrative consumer-owned targets, not new Shared entrypoints. Each
must propagate child failure; use sequential recipes/dependencies that also hold
under parallel Make. Direct Bash orchestration can instead use strict mode and
explicit failure checks. Avoid pipelines that authenticate a failed producer's
partial output through a later successful reader.

Qualify the consumer wiring with recorded-command fixtures: all cheap graph
observations precede Clippy, and any metadata/policy/lint failure withholds later
phases and artifacts. Exercise failed commands that emit plausible output, stale
locks without mutation, renamed/target/optional dependencies, repeated nodes,
true identity conflicts with paths, and legitimate independent versions. Use real
small Cargo graphs for resolution/lock behavior; command substitutes prove only
ordering and failure propagation. Shared retains its generic checker fixtures;
consumers retain their actual roster, policy and artifact integration proofs.

A passing adoption check establishes only its declared graph, compilation/lint
and read-only artifact scope. Managed Wasm builds, installed IC/upgrade behavior,
native macOS execution and full release validation remain separately qualified.
