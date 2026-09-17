# Typed binding instruction attribution

2026-09-17 · bounded follow-up to the user's request to explain the instruction
gap. Baseline: published `355bca1f7` plus the approved pagination repair in
[receipt 02](../02/report.md). No production optimization lands in this outcome.

## Finding

Repeated accepted-authority work dominates this fixture, not allocation of
the typed binding's maps. The earlier roughly 298,000-instruction "binding"
interval includes current catalog selection, a verified bundle read and a
durable database-incarnation read. It is not a measurement of map construction.

The refined instrumented primary-key workload's first warm call reports:

| Binding issuance stage | IC instructions |
| --- | ---: |
| Source validation and accepted catalog selection | 73,910 |
| Identity check and recovered store selection | 5,978 |
| Borrow current verified bundle, including job-closure validation | 87,230 |
| Source/primary-key/field compatibility and field mapping | 22,795 |
| Named-type/variant/member name projection | 11,705 |
| Durable database incarnation | 86,338 |
| Remaining constructor arguments, owned binding and uniqueness checks | 9,511 |

Terminal binding validation separately costs 265,190 instructions: recovery
3,357; incarnation 86,683; catalog selection 71,925; binding/catalog matching
103,225. Matching includes another verified bundle read and field-identity
checks; its internal split is not measured. Scan/sort and grouped COUNT show
the same broad distribution; all nine samples are in [samples](samples.txt).

The two measured incarnation intervals total 173,021 instructions in this
warm invocation. These are durable reads, boot/frame validation and control
inspection, not merely copying 16 bytes. Do not treat their cost as proven
removable work. All stages include counter overhead and instrumentation can
change generated code. Logs are emitted after each measured stage group.

SQL also checks current authority. Its execution context reuses
`SchemaStore::current_accepted_schema_authority_matches`, whose existing
verified bundle cache is invalidated by root-writing primitives. Typed issuance
and validation instead use `borrow_current_accepted_schema_bundle`, which
reselects the durable root and checks current constraint-validation jobs.
Together with the existing SQL compiled-command cache, this explains a
substantial source-level asymmetry. It does not establish that the stronger
typed checks can simply be deleted or that SQL has a correctness failure.

## Bounded next candidate

Audit reuse of the existing accepted catalog/authority within each synchronous
binding operation before considering a new binding cache. Keep issuance and
later execution as separate validity boundaries: a builder can outlive its
schema. Preserve store/domain selection, incarnation, current root, revision,
fingerprint, layout, source/slot compatibility and constraint-job closure.
Include same-revision/different-store and corrupt-control cases in any change.

This is a follow-up candidate, not an implemented authority shortcut or an
approved broad cache/recovery redesign. Map-sharing alone cannot remove most
of the measured interval. No cost gate or accepted deferral changes.

## Experiment and validation

- Two focused PocketIC probe runs pass, each comparing all nine cold/warm
  reports against the unchanged typed actor. The second splits incarnation
  from construction and recovery; counters use `performance_counter(1)`.
- Four disposable PocketIC fixtures were created and released; shared local
  networks were not changed. Warm means later invocations inside one request.
- Uninstrumented warm equality remains 660,963 planning plus 230,514 rendering
  instructions, versus the previously measured SQL total 447,202. This run
  rechecks the typed baseline, not SQL. Refined instrumentation adds 21,623
  warm planning instructions; it is not a candidate performance regression.
- Initial temporary host wiring failed because integration disables automatic
  test discovery, then because the child file was in the wrong directory.
  Corrected focused probes pass; no production test failure was discovered.
- Instrumentation and temporary test wiring were removed. Runtime and maintained
  integration sources exactly match their pre-investigation state. Formatting
  and whitespace checks pass. No full suite, fresh SQL qualification or charged
  cycle measurement ran; the existing diagnostic gate remains unresolved.

Reproduction inputs: Rust 1.98.1, locked/offline dependencies, package
`canister_audit_one_entity_typed_query`, no default features,
`typed-explain-measurement`, `wasm-release`, `wasm32-unknown-unknown`.
Cargo.lock SHA-256:
`2861ffbeb525446769bda64557a70e263e750ebfb547b2a6d4e3323f9d3d9562`.
Binaryen 132 and flags are unchanged from [receipt 01](../01/report.md).

Apply the archived [instrumentation patch](instrumentation.patch.txt) to this
baseline, temporarily wire [the host probe](probe.rs.txt) as `mod
binding_stage_probe;` in the maintained `typed_explain_measurement` target,
and set `ICYDB_BINDING_PROBE_DIR` to a directory containing `before.wasm` and
the optimized instrumented `probe.wasm`. Run only
`binding_stages_preserve_explain_reports`. This wiring is not retained.
Logs and artifacts: `target/typed-binding-stages/`.

| Artifact | Raw Wasm bytes | SHA-256 |
| --- | ---: | --- |
| Unchanged baseline | 2,510,157 | `f8581ce124529dd63ec84e08febd66163c46f876b5e8f6cd6960de4795d02d03` |
| Refined temporary probe | 2,510,794 | `960d5fbd52b4d730576d3d6880513e3c9e468c47f553f1cb48c1eec0c87f7a0c` |

The extra 637 bytes belong only to instrumentation. No production Wasm or
instruction improvement is claimed. Complexity: seven documentation/evidence
files, approximately 280 added lines; zero retained runtime/test changes,
new caches, modes, budgets or authority lifetimes. Implementation shape is
unchanged. Cost acceptance and final minor-line closeout remain open.
