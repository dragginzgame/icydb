# Frozen release batches
<!-- Shared companions: scripts/ci/run-frozen-release.pl -->

A release batch captures clean committed source once. It runs real release and
optional publication hooks in a managed delivery checkout, preserving the real
manifests, locks, ledgers and receipt contracts. Later `cargo update` or source
edits in the normal checkout belong to the next batch and do not change this
batch's validation or archives. This is distinct from the stage-only prospective
candidates in [validation reuse](release-validation-reuse.md), which must never
be published.

## Consumer adoption

This is opt-in until a consumer qualifies its actual release and publication
hooks in the independent checkout. After adoption, its ordinary `release-patch`,
`release-minor` and `release-major` targets can set:

```make
RELEASE_FROZEN_GROUPS := types,runtime,platform
RELEASE_FROZEN_PUBLISH_TARGET := publish
```

Select the complete affected roster in supplier-first order, not all groups by
default. The same requested increment applies to each selected group. Omit the
publication target for Git-only delivery. Explicit publication selection carries
real registry effects and requires the normal delivery authority. The initial
implementation supports direct delivery, not PR integration or mixed increments.
Do not select it for an ungrouped VERSION-only repository.

The supplying command is:

```sh
perl scripts/ci/run-frozen-release.pl --root "$PWD" \
  --groups types,runtime,platform --kind patch --publish-target publish
```

One queue per normal checkout serializes batches; groups within a batch run in
order and observe earlier supplier release commits. Parallel Cargo compilation
inside an admitted hook is independent. A concurrent second batch must still
admit its own clean source after acquiring the queue; it does not silently absorb
pending edits or attach itself to another batch. Do not loop independent commands
as a substitute for selecting the complete frozen batch.

## Custody and deferred changes

The disposable checkout is `target/release-frozen/BATCH`. Its Git object store,
release journals and batch intent live under `.git/release-state/frozen/BATCH`.
The base source is pinned by `refs/releases/frozen/BATCH`; the candidate borrows
base objects instead of duplicating repository history. Newly created release
commits remain in the durable candidate Git store. No symlinked tools or live
lockfiles are imported from the normal checkout. Hooks prepare their own tools
and locked sources through their existing owners; do not add unconditional
reinstallation or copy mutable caches to compensate for missing adoption.

The runner never rewrites, stages, stashes or resets the normal checkout, even
after delivery. It prints the delivered commit and recovery locations. Deferred
lock edits remain where the user made them. Reconcile the normal branch with the
published release and then reapply/refresh the dependency update for the next
candidate; a dirty lock must not be silently replaced or treated as released.
This reconciliation is currently explicit: the helper does not promise automatic
merging of a concurrently written lockfile. The active deployment still finishes
from the candidate when normal-checkout edits occur.

Resume using `--root DIR --resume BATCH`. An interrupted release with a saved
standard plan uses its exact-version resume; a clean pre-mutation failure may
retry the same increment. Missing plans after mutation refuse. Uncertain
publication requires the consumer's receipt reconciliation, never automatic
redispatch. After verifying the registry and consumer receipts, resume with
`--publication-result published` to advance, or `--publication-result not-published`
to retry the publication target. These are explicit assertions of verified remote
state, not ways to skip checking it. Completed batches do not republish on resume. Initialization failure
or checkout deletion requires inspecting retained state before recovery; do not
remove the durable Git store or source pin. `cargo clean` can interrupt active
work by removing the disposable checkout; this feature isolates `cargo update`,
not arbitrary filesystem deletion.

## Qualification and limits

Qualify actual hooks, clean locked builds, selected archive identities, all groups
and publication receipts. Tests must mutate the normal lock while a candidate is
running and prove later groups still use the frozen graph, preserve the edit,
keep durable commits and avoid repeat publication on completed resume. Test
interruption/recovery, supplier-order conflicts and remote advancement separately.
A synthetic runner fixture is not consumer or installed qualification.

Git/Cargo context overrides are rejected rather than inherited into the candidate.
Make command-line overrides are not replayed inside it: keep consumer hook
configuration in the committed Makefiles. Snapshot-owned exported tooling paths
are rebased to the candidate. Cargo also searches ancestor configuration, so
`.cargo/config.toml` above the candidate must remain stable during delivery;
this mechanism specifically isolates normal lockfile updates, not host changes.
Absolute source paths, external path dependencies, inherited Cargo configuration,
credential helpers and tool selection need explicit consumer qualification. A
snapshot is not a sandbox: its hooks must not read mutable source from the normal
checkout. Do not claim isolation of such inputs solely because Git files were
copied. Remote divergence or a real candidate failure still refuses delivery;
never force-push, rewrite evidence or suppress a failed gate to preserve speed.

Do not start another persistent development checkout here. These managed paths
are for release execution only. Retain incomplete/uncertain delivery state under
the existing [retention contract](../rules/engineering-details.md); retire completed
batch state, source pins and disposable artifacts only after delivery and recovery
references are reconciled. No automatic cleanup of other batches is authorized.
Batch admission applies the shared retained-state size/age limits, so these
directories cannot accumulate indefinitely without a visible owner review.
