# Selected catalogue labels — matched IC measurement

## Result and disposition

The existing selected-field surface is a useful option for label-only callers.
For 128 rows it uses **38.8–39.3% fewer explicit-update cycles** and
**45.5–46.2% fewer query/conversion instructions** than full typed rows reduced
to the same labels. For 16 rows the respective ranges are **24.5–26.4%** and
**53.9–59.7%**. All labels, IDs, ordering and page counts match independent
expected values. No IcyDB production code, public API or application was changed.

This completes the user-authorised R3 measurement after the
[R2 investigation](../01/report.md). It measures a caller's choice between
existing APIs, not a new runtime optimisation. Keep the existing selection
route. Whether checked DTO conversion warrants a new typed facade is a separate
ergonomics decision, not a missing-executor problem. No additional implementation
or downstream adoption is implied. Canic coordination remains deferred.

## Frozen subject and artifact

HEAD remains `4698cecb8dbba6786079b759b43494b020559997`, plus the dirty C1/R1
candidate and this R3 fixture extension. [sources.txt](sources.txt) records
lock, relevant dirty actor/host inputs, Rust 1.98.1 and Binaryen 132. Cargo
versions remain untouched at 0.259.6. The [R1 record](../../typed-catalogue-reads/01/report.md)
identifies the preceding fixture and runtime candidate; R3 changes only its
catalogue actor module, Candid return-type import and host measurement module.

Both query alternatives run in **one identical retained artifact**:

- Raw bytes: **4,309,787**.
- SHA-256: `5c7ff4ff362793d908b0b8d2ddfa72bfffb64811260775030dbeb5fc332385e2`.
- Defined functions: **10,212**; code payload **4,073,176** bytes; data payload
  **220,832** bytes.
- Existing `canister_test_sql` retained build owner; defaults off, explicit
  `candid-export,local-sql-query,test-admin-api`, LocalTest, SQL/Candid enabled,
  `wasm-release`, z/fat-LTO/one-codegen-unit/abort/stripped and existing -Oz
  post-link flow. PocketIC 16.0.0. Neither measured query uses SQL parsing.

The retained artifact lives through byte reading; the two fresh instances
install clones of those exact bytes. Exact path and all messages are retained
in [messages.txt](messages.txt). No cache/lock/build owner was added.

Relative to R1's post-change actor, raw size increases **8,236 bytes**, defined
functions **21**, code payload **7,832 bytes**, data payload **333 bytes**.
This is additional test-endpoint/DTO/measurement reachability. It is not the
Wasm difference between deploying the two query alternatives, which was not
isolated. No production footprint regression or reduction follows from it.

## Same-output comparison

Reuse R1's deterministic 16/128-row nested catalogue schema and seed. Each row
has ID/key/name plus description, capacity and optional placement with a
768-byte blob, four points, asset and boolean. Both variants return exactly
`CatalogLabel { id, key, name }`; final payload omission is identical.

The full path executes the ordinary typed complete-row query and then moves
the three fields into labels, dropping unused values. The selected path uses
ordinary `execute_live_page` with `DynamicQuery::select` and generated field
constants. It checks columns, exact arity and Nat64/Text/Text variants before
moving values into the same label DTO. No rendering, default fields, coercion,
trusted-lane substitution or generated-model fallback.

Both construct the query per page, order by the indexed key, request limit 257
and follow actual continuations. The existing public envelope yields one page
for 16 rows and two for 128. Four-page and 128-row caps fail closed. Decode
finishes before continuation adoption. The host checks every expected ID, key,
name and result order, not merely equality between implementations.

On each instance the existing four R1 full-row probes run first; labels then
run in **full, selected, selected, full** order. These are ordered observations,
not isolated cold/warm-cache experiments. Table percentages pair the first
selected call with the preceding full call and the second with the following
full call. They are not statistical confidence intervals or universal savings.

| Rows / pair | Full instructions | Selected instructions | Reduction | Full cycles | Selected cycles | Reduction |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 16 / first | 6,437,195 | 2,966,762 | 53.91% | 14,910,189 | 11,255,097 | 24.51% |
| 16 / second | 6,593,470 | 2,657,055 | 59.70% | 14,897,218 | 10,959,501 | 26.43% |
| 128 / first | 51,751,088 | 28,196,160 | 45.52% | 60,783,897 | 37,182,944 | 38.83% |
| 128 / second | 51,839,250 | 27,912,376 | 46.16% | 60,776,066 | 36,912,519 | 39.26% |

Type-1 instruction counters include query construction, execution, label
conversion/accumulation and dropping unused fields. Request scope/database
opening precede the interval; Candid serialization follows it. Balance-delta
cycles cover the broader explicit update, including those costs. Seed writes
are chunked at 16 with ordinary convergence; installation, startup, seeding,
virtual-time advances and 64 pre-call drain ticks are outside measured calls.
Co-scheduled work is not separately attributed. No native/wall-clock metric.

## Meaning and limits

The saving includes selecting fewer fields, avoiding the complete entity
binding/adapter path and not materialising unused nested output. This run does
not separately attribute each contribution. It does not prove index-only reads,
reduced stable bytes fetched, allocation totals, peak heap, smaller production
Wasm or Toko Miner deployment headroom. The representative fixture uses Nat64
IDs, not Toko Miner's ULIDs, and is not its complete endpoint workload.

R2's corruption boundary remains: selected reads validate required fields and
key consistency, not every unrelated stored field. Neither path nor its
validation was changed for this measurement. Label reads are not whole-row
integrity audits. Benefits require callers that only need labels to choose
selection; existing complete-row callers receive no automatic R3 saving.

## Validation and complexity

The focused catalogue integration test passed: **1 passed, 0 failed, 0 ignored,
100 filtered**. It validates eight new label measurements and eight existing
full-row observations. The enclosing command completed successfully. No
expensive failed measurement was rerun. Initial focused Clippy reported a
too-long host test; the required `make clippy` reproduced it, and extracting
the label-check helper resolved it. All `make clippy` lanes and the focused
actor/integration feature lane then passed. Formatting and diff checks passed.

Local lifecycle: the existing harness started one PocketIC server and created
two disposable instances; the server exited after the completed run. No manual
network stop/reset, deployed state, sibling source, dependency/version or
publication action. Full repository tests remain user-owned.

Three test-support source files, approximately **+149 net lines**, plus the
design/status, active notes and three evidence files. No production code,
behavior axis, executor, cache, format or public report type added. Runtime
structure is unchanged; test support gains one label DTO/result and endpoint.
