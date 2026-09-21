# ICYDB-038 — Actual application catalogue reads

2026-09-21. User-authorised I2 investigation on the existing 0.261 line.

## Scope and disposition

The concrete integration candidates belong to Toko Miner's catalogue consumers:
Resource inventory projection, OrderCustomer presentation and NftToken order
eligibility. Existing generated field references and dynamic selection can
express all three required projections. No new executor, persisted cache,
schema format, runtime mode or query-attribution API is proposed.

The managed comparison passes: selected fields reduce Resource read/projection
instructions by **46.4–47.1%**, customer instructions by **51.4–52.0%**, and NFT
token instructions by **20.4–20.9%** across four lifecycle states. All returned
values and order match; same-release restoration preserves the complete result.
This completes the bounded I2 investigation. ICYDB-038 remains a candidate until
matched evidence identifies
the dominant actual action cost and qualifies a simpler change or establishes
that no upstream change is warranted. A read-only diagnostic endpoint does not
by itself establish savings in `execute_actions` or close that acceptance.

## Source findings

The frozen source, including dirty downstream work, is identified in
[sources.json](sources.json). The working snapshot is
`/tmp/icydb-038-investigation`; the sibling application is read-only.

| Consumer | Current work | Bounded alternative and constraint |
| --- | --- | --- |
| `rocket::read_resources` and inventory projection | Complete Resource rows in catalogue order, limit 257; reject an unfinished page. Inventory identity, validation and summaries use ID, key, name and maximum stack. | Compare those four selected fields with full rows reduced to the same values. Other Resource consumers need additional fields, so changing the shared full-row function globally is not justified. Reuse caller-owned rows first where the actual request already has them. |
| `outpost::orders::catalogue::summary` customer | Read every customer in catalogue order, limit 64, validate every row's two colour fields, then find one customer by ID. | Selecting ID, username, both colours and avatar preserves the all-customer validation scope. A primary-key-filtered query is a separate comparison: it no longer checks unrelated customer colours or the whole-catalogue overflow boundary. Equal successful results do not prove equivalent failure behavior. |
| Same Orders summary, eligible patterns | Complete NftToken rows for one collection in token-number order, limit 100; reject continuation; filter rarity and return IDs. | Select ID and rarity with the same filter, order, limit and continuation rejection. Decode rarity with the existing generated `TypedOutputValue` implementation and current NftToken binding. Retain the same ordered eligibility result. |

Resource has an accepted catalogue-order index; OrderCustomer has its primary
key and unique catalogue-order index; NftToken has the accepted unique
collection/token-number index. Inspection does not establish a missing index.
The probe preserves existing admission and uses generated field/entity names.

The outpost catalogue and map owners already retain rebuildable projections.
Several initialisation/reconciliation paths also call `read_resources`; merely
counting its source callers would overstate work in an ordinary warm action.
No additional persistent application cache is justified by this inspection.

## Retained gameplay evidence and attribution boundary

The existing September 20 action evidence measures 64 conserved inventory
actions. These are retained observations, not a new run of that action trace:

| Stationary workload | One action/request | Eight actions/request | Thirty-two actions/request |
| --- | ---: | ---: | ---: |
| Action-body instructions | 12,349,125,065 | 3,830,298,135 | 3,034,223,020 |
| Resource entity-window instructions | 1,304,327,193 | 146,238,631 | 35,929,609 |
| OrderCustomer entity-window instructions | 335,009,781 | 38,348,715 | 9,276,636 |
| NftToken entity-window instructions | 272,146,997 | 32,578,141 | 8,345,535 |

The entity windows overlap action spans and surrounding checkpoint/sampler
activity. Do not add them to action-body totals, infer per-call costs by division,
or attribute all batching savings to these reads. In addition,
`EntityMetricsSpan` records only replicated execution. Its projection span
starts after a prepared plan exists; the typed facade subsequently performs
generated row conversion. Thus retained entity metrics are not complete
construction/execution/conversion measurements and do not capture ordinary
query-only traffic durably. This is the documented existing scope, not a new
diagnostic defect or a reason to reopen ICYDB-013.

## Controlled comparison

[actor-probe.rs.txt](actor-probe.rs.txt) records the disposable diagnostic
endpoint; [host-probe.rs.txt](host-probe.rs.txt) records its focused managed
test appended to the existing action-cost owner. The application workspace
does not acquire either probe.

Both alternatives run in the same Game Shard actor with real application
declarations, accepted schema, authored fixtures and Canic lifecycle ownership.
Full typed rows and selected values reduce to identical scalar output tuples.
Selected output checks exact columns, arity, scalar types, numeric bounds and
the NftRarity type/variant contract through its accepted binding. Both reject
unfinished pages, retaining
the original bounded single-page callers rather than manufacturing cursors.

For each lifecycle state, the host brackets selected reads with full reads:
full, selected, selected, full, then typed single-customer and selected
single-customer controls. It compares every returned value and result order.
The states are install-ready before enrolment, after ordinary enrolment,
same-release restoration, and restoration followed by ordinary login. Normal
managed readiness runs before qualification; this is not a measurement of an
unready canister immediately after its raw post-upgrade hook. No artificial
application write, warm-up timer or migration is introduced.

Two additional queries immediately after the raw upgrade observe the current
admission boundary before the ordinary readiness callback runs. Both return
E259, `RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING`; neither reaches the
measured reads. After 48 ordinary simulated timer ticks, the existing application
readiness work completes and the restored-state comparison succeeds.

Instruction intervals are session opening, collection lookup, Resource read
and projection, customer read and projection, NFT read and projection, and the
enclosing diagnostic body. Collection lookup includes any lazily triggered
accepted-runtime preparation. The three read intervals include query
construction, execution, generated or checked conversion, and scalar projection;
they do not separately identify those sub-stages. Candid response encoding,
request-scope entry, actual action execution and receipt projection lie outside
the diagnostic body. Counter calls add uncalibrated instrumentation overhead;
matched alternatives retain the same outer counter layout.

The diagnostic returns all customer presentation tuples and all ordered
ID/rarity pairs to make value equivalence observable. The production Orders
summary instead keeps one customer and filters the token pairs to one rarity.
Consequently these are comparisons of its database read shapes, not exact
measurements of that complete presentation function. Resource IDs are rendered
to strings in both diagnostic alternatives; that shared projection cost is
also included. Production savings require an integrated endpoint comparison.

## Measurement and validation

The exact [measurements](measurements.json), [instruction messages](messages.txt),
[returned values](outputs.jsonl) and [artifact identities](artifacts.json) are
retained. Each admitted state has 64 Resources, 17 customers and nine tokens;
the primary-key customer control has one customer. All measured callers finish
without continuation. Overflow and multi-page traversal are outside this fixture.
The four common-collection token outputs exercise the authored Common rarity;
this is not an independent seven-variant enum qualification.

| After ordinary enrolment, instructions | Full typed rows | Selected fields | Reduction |
| --- | ---: | ---: | ---: |
| Resource read and inventory-field projection | 26,070,369 | 13,866,022 | 46.81% |
| All-customer read and presentation projection | 10,184,616 | 4,949,594 | 51.40% |
| Collection-token read and ID/rarity projection | 9,243,874 | 7,335,584 | 20.64% |
| Enclosing diagnostic body, including session/collection lookup | 56,248,398 | 36,900,739 | 34.40% |

These rows are nested scopes: do not add the enclosing body to its component
intervals. Full/selected/selected/full repeats are instruction-identical within
each lifecycle state. The single-customer control measures 5,137,680 typed versus
1,551,027 selected customer instructions after enrolment, but changes the
whole-catalogue validation scope described above and is not an approved replacement.

Repeated full diagnostic queries after normal restoration use 55,244,716
instructions; after ordinary login they use 54,936,434, a reduction of 308,282
(0.56%). The collection-lookup interval is 8,749,443 then 8,895,027. Thus this
ready-state workload does **not** reproduce I1's large query-only preparation
penalty that disappears after one replicated call. Source inspection explains
why this is plausible: Toko Miner's existing startup callback reconciles its
fixtures inside a replicated request after IcyDB becomes ready. This does not
disprove I1's migration-fixture result, isolate preparation sub-functions, or
qualify reads before readiness.

The final frozen Canic release build is
`609bb28830d3a967d259e0da4bb0589ec6cb67ee9cbe7a02bd16f5321b056ddd`.
All five application-role artifacts come from that one build. The diagnostic
Game Shard is **10,216,852 raw Wasm bytes**, SHA-256
`85b90e0282b34497b783f11a42f746f81cdb7274f38a24a1f049b0b98d79537f`.
The artifact receipt is authoritative for this identity. Canic fast profile
uses size optimisation, no LTO, 16 codegen units and stripped symbols; the normal
ic-wasm finalisation runs, with no Binaryen pass. Both read alternatives are
compiled into this same artifact. No production Wasm-size delta is measured.

The focused managed test passes: 24 admitted read queries, two rejected
pre-readiness queries, complete scalar value/order comparisons, and unchanged
results across same-release restoration. The canonical eight-artifact build,
snapshot-hash checks, formatting, documentation links and whitespace checks pass.
Full application CI, complete repository suites and production deployment are
outside this gate. Cycles and whole-action instruction deltas are unmeasured.

Initial qualification failures were corrected in the disposable probe:
direct Cargo Wasm builds are unsupported by Canic; automatic sccache startup
was unavailable in the sandbox, so its documented wrapper override was used;
the first source freeze caught a concurrently incomplete application
`From<AccessError>` conversion, then incorporated the owner's completed file;
probe trait-import/header errors were fixed. Mixing a diagnostic actor with
old companion artifacts failed Canic's E17 release-identity check before any
database reads, so the final fixture uses one canonical release set. The first
selected-enum probe incorrectly compared accepted catalogue names with a Rust
model path. Replacing that comparison and handwritten variant mapping with
`NftRarity::decode_typed_output` plus the current entity binding resolves it.
No runtime workaround or relaxed admission was added. Exact failed logs remain
under `/tmp/icydb-038-*`; successful evidence above supersedes their partial data.

Three disposable PocketIC server runs were started by the focused host tests
and exited; no investigation server remains. No application network was
started, stopped, reset or upgraded. The upgrades in this report concern only
the disposable managed test fixture.

To reproduce locally, the retained snapshot is `/tmp/icydb-038-investigation`.
Build it with `RUSTC_WRAPPER= canic --environment toko_miner build toko_miner
--profile fast`, then point the existing qualification build-ID/artifact-dir
environment variables at that exact release and run only the ignored
`qualification::opening::costs::icydb038_actual_catalogue_comparison` test in
`canister_toko_miner_user_hub`. Sources and artifacts are retained locally;
hashes do not by themselves reconstruct the original dirty application tree.

## Remaining acceptance

Even a passing same-output read comparison leaves whole-endpoint instruction
and charged-interval deltas, request-local reuse frequency, receipt-projection
attribution and independent per-stage preparation/conversion costs unmeasured.
The read alternatives share one diagnostic actor; its raw size is an artifact
measurement, not the deployment-size difference between implementations.
ICYDB-033 remains separate. Full repository suites remain user-owned.

The next useful application-owned change is selected projection at the measured
consumers, with the existing accepted-binding decoder for enum fields and
unchanged catalogue admission. First trace the current action request: its Field
receipt no longer requests Hangar projection, and several Resource consumers
already reuse cached catalogue rows. Do not infer current per-request duplication
or endpoint savings from September 20's historical totals. A matched integrated
action comparison must preserve inventory conservation, exact retries, receipts,
ordering and failure boundaries before ICYDB-038 can close. No new IcyDB runtime
feature is justified by the completed read comparison.

No IcyDB runtime or downstream production edits, dependency changes, version
bumps, commits, pushes, deployments or application-network resets are included.
The pre-existing IcyDB Cargo.lock change is left untouched.

Complexity delta: eleven IcyDB documentation/evidence files, approximately
1,465 net added lines including archived diagnostic Rust, source hashes and
machine-readable results. Production implementation shape and state space are
unchanged. The four diagnostic alternatives exist only in the disposable actor;
they reuse the current query and accepted-binding owners. Whole-endpoint cycle
and instruction deltas and production Wasm-size deltas remain unmeasured.
