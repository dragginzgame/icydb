# Resolved Cargo graph checks
<!-- Shared companions: rules/contributions.md rules/rust-workspaces.md -->

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

Package violations also include `identity_paths`: one observed introducing path
for each distinct rendered identity of that package. This makes both sides of a
singleton conflict visible, including their versions and source qualifiers. The
existing `path` remains the first package path; feature violations retain their
feature path. These paths describe the same successful observation as the policy
failure, without another diagnostic command or different feature selection.

Toko Tooling owns the observation/checking mechanics. Consumers own the roots,
targets, features, forbidden packages and required identity counts. This helper
complements direct declaration checks; it does not replace them or infer policy
from dependency-role comments.

```sh
perl scripts/ci/check-cargo-graph.pl --consumer . --package production-package \
  --target wasm32-unknown-unknown --all-features \
  --forbid candid_parser --forbid pocket-ic --require ic-wire=1
```

This example selects a Wire application root. Choose exclusions and counts for
the actual product under the [encoding policy](https://github.com/dragginzgame/toko-tooling/blob/main/TOKO-TOOLING.md#ic-application-encoding);
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

Use `--edges normal` for a runtime/public-type policy that deliberately excludes
build dependencies. The default `--edges normal,build` preserves combined
normal/build checking; no dev or whole-lock duplicate policy is inferred. JSON
records the selected `edges`. Feature-edge policy composes with either selection.
Select the edges according to the actual contract: a runtime-only pass says
nothing about a separately selected host tool graph.

The helper runs locked, offline Cargo tree with the selected edges and flat
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

## Platform owner graphs

Apply the [owner declaration policy](../rules/cargo-dependencies.md#platform-package-owners)
first. Then select each actual package's runtime graph separately. For the
example policy's package names and the published Types/Runtime generation:

```sh
perl scripts/ci/check-cargo-graph.pl --consumer . --package identity-model \
  --target wasm32-unknown-unknown --edges normal --no-default-features \
  --require ic-platform-types=1 --forbid ic-platform --forbid ic-platform-runtime \
  --forbid candid --forbid ic-platform-runtime-guard

perl scripts/ci/check-cargo-graph.pl --consumer . --package ic0-adapter \
  --target wasm32-unknown-unknown --edges normal --no-default-features \
  --require ic-platform-runtime=1 --forbid ic-platform-types --forbid ic-platform \
  --forbid candid --forbid ic-cdk --forbid ic-cdk-executor --forbid ic0 \
  --forbid ic-platform-macros --forbid ic-platform-runtime-guard \
  --observations --json
```

The member manifest selects Runtime's `ic0` feature; the second command records
enabled features so the caller can check that only `ic0` is selected. Owned CDK
is a Runtime feature, not a separately named registry package: an absence check
for `ic-cdk` alone cannot prove CDK is disabled. For the IC0-only graph, the
recorded package closure should contain only the selected root and Runtime.
Types also needs its ordinary encoding, Serde and error dependencies; those are
not an execution dependency or a reason to reject the identity-only graph.

For actual executor users require one `ic-platform-runtime` and one
`ic-platform-runtime-guard`; require Types only when the selected API needs it.
For crypto/external-adapter packages admit the facade and its deliberately selected
capabilities instead of applying the IC0-only exclusions. Check shared Types at
the combined application boundary, including facade re-exports. Compile the
actual public-value exchange before claiming interoperability. Independent
host/dev graphs may have different legitimate selections and need their own
policies; neither a whole-lock duplicate count nor these examples constitutes
fleet-wide version lockstep. Preserve the qualified MSRV and installed/native
acceptance separately from source-only graph admission.

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
Apply the [current dependency selection rule](../TOKO-TOOLING.md#current-dependencies-and-upstream-independence)
to independent general-purpose libraries: older upstream `num-bigint` or `sha2`
selections do not by themselves block newer direct selections. Assess material
cost in the actual consumer artifact, rather than rejecting duplicates by count.

## Dependency adoption planning

The [product/qualification layout](../rules/rust-workspaces.md#standard-layout)
gives separate qualification packages an independent workspace and lock by
default. Inspect both selected graphs; a product release group does not own the
qualification packages or their versions. Qualification references the actual
product source under test, and required composed checks remain explicit. Product
normal/build/dev edges must not pull qualification packages back into its graph.

Before planning downstream version bumps or publication order, identify the
destination application's selected roots, targets/features and independent
manifests/locks. In the existing issue or change notes, distinguish the requested
upgrade and required runtime/public-type/security/minimum-capability alignments
from optional refreshes. Explain required extra selections with their introducing
dependency path or contract; do not expand a scoped update into unrelated
package refreshes. Within the selected update scope, use the baseline's latest
stable selection rule and document any required older-version exception.

Optional refreshes are deferred by default and batched with requested maintenance
or a natural release boundary. Do not start an update-everything pass because a
supplier published another version. Keep compatible minimum requirements where
they still describe the supported contract; a newer tested lock selection does
not itself require raising every manifest minimum or publishing the consumer.
Required API/security/compatibility changes select the affected consumer-boundary
checks once. Adoption alone does not justify replaying upstream generic fixtures
or starting downstream release procedures.

Admit the proposed combined graph cheaply in disposable preparation before
expensive artifact work. Reuse the focused checks below and existing source/archive
qualification for unpublished suppliers. Candidate source evidence is not registry
availability: verify publication separately before registry-dependent delivery.
A successful resolution alone does not prove public-type compatibility; compile
the affected consumer boundary before claiming compatibility. Keep failures tied
to their actual graph and distinguish independent host/dev selections.

Use the resulting dependency constraints to choose supplier-first delivery order.
That order is advice, not authority to release other repositories. Each receiving
owner performs its own adoption; do not open unsolicited version, lock or snapshot
PRs or start sibling deployment procedures. Follow the
[repository-scoped contribution rules](../rules/contributions.md#cross-repository-pull-requests).
Do not weaken singleton checks, rewrite retained locks in a read-only check, or
publish intermediate consumers merely to discover whether the final graph works.
Optional refreshes may be batched when explicitly in scope; they must not obscure
the minimum required migration. An unrelated pending adapter must not block
independent core qualification under the
[core/integration rule](../rules/rust-workspaces.md#core-and-integration-graph-boundary).

Enforce the declared graph constraints through existing consumer metadata/graph
checks and their failure propagation. Reviewers must reject an unexplained
downstream bump or a compatibility claim without the corresponding evidence.
Shared does not infer semantic compatibility or automatically decide affected
packages; no new mandatory report file or universal fleet version set is required.
The focused checks and complete release gates retain their separate scope.

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
   with `--edges normal`; the shared checker defaults to including build
   dependencies, so select that option explicitly for runtime-only evidence.
   On a conflict, report the
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

Use `--json --observations` to include each rendered package identity and its
sorted effective feature union in the same successful Cargo observation. This
uses Cargo's `{f}` field, including root/command-line features; feature-edge
labels alone are insufficient. The optional `observations` array contains
`name`, `identity` and `features`. Failed or malformed observations emit no
admission JSON. Existing policy options remain required; this is evidence for
consumer policy, not deployment or artifact qualification.
