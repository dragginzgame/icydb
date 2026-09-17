# 0.258 L4 — Logical-memory qualification

Date: 2026-09-17. Verdict: the logical-memory implementation is ready for the
user-owned release gate, within the coverage limits below. No further production
change is required by this qualification. Existing physical-ID databases must be
recreated/reinstalled; this is not a migration implementation.

## Matched cost measurement

Baseline: `f3bd969be` / 0.257.22, registry `ic-memory 0.13.3`.
Candidate: the working 0.258 logical-memory cut, registry `ic-memory 0.14.1`.
Both use `icydb-testing-model-facade-only`, its default/general memory profile,
the same entity, write, lookup and same-version upgrade. Only the candidate uses
the logical declarations and explicit host pool. This compares the complete
dependency/integration change, not admission-hook cost in isolation.

Both actors include the identical test-only `open_database` endpoint from
[the maintained fixture](../../../../../../../../testing/model-facade-only/src/lib.rs).
It reads IC performance counters around request-scoped `db()` opening and
returns the instruction delta. It does not execute a row query. This adds no
production diagnostic endpoint, mode or per-request instrumentation.

Configuration: Rust 1.98.1, `wasm32-unknown-unknown`, `wasm-release` (size-oriented
optimization, fat LTO, one codegen unit), `ic-wasm 0.11.1 shrink`, no additional
Wasm optimization pass. Separate target directories avoid cross-tree Cargo
artifact reuse. PocketIC 16, default application subnet, identical funding and
driver calls. The [integration driver](../../../../../../../../testing/integration/tests/model_facade.rs)
records cycle balances; a separate fresh instance measures install from its
actual funded balance. No wall-clock/native timing is a performance metric.

| Metric | Baseline | Candidate | Change |
| --- | ---: | ---: | ---: |
| Raw Wasm bytes | 2,567,614 | 2,599,304 | +31,690 (+1.234%) |
| gzip bytes (`gzip -n`) | 839,543 | 848,915 | +9,372 (+1.116%) |
| `ic-wasm info` function count | 12,010 | 12,181 | +171 |
| Install cycles, including compilation/init | 14,983,012,147 | 15,169,035,909 | +1.242% |
| Fresh startup watchdog cycles | 116,450,980 | 116,634,711 | +0.158% |
| Warm-open instructions, median of three | 697,408 | 706,578 | +1.315% |
| Warm-open message cycles, median of three | 7,779,626 | 7,787,947 | +0.107% |
| Typed insert message cycles | 11,249,254 | 11,315,120 | +0.586% |
| Same-version upgrade cycles | 5,933,477,931 | 5,999,076,865 | +1.106% |
| Post-upgrade watchdog cycles | 56,243,989 | 56,364,881 | +0.215% |
| Allocated stable bytes after startup | 23,134,208 | 23,134,208 | Unchanged (353 Wasm pages) |

Raw samples and artifact digests: [measurements.csv](measurements.csv).
Warm-open cycles include message overhead; the instruction counter covers only
the test's request/open region. Install and upgrade include runtime compilation
and lifecycle work, not just allocation bootstrap. The watchdog windows use the
same bounded delivery helper on both sides. A successful post-upgrade lookup
proves the written row survives in each actor independently; no baseline-to-
candidate in-place upgrade is claimed.

Interpretation: this is an authoring/ownership simplification with small measured
cost increases, not a speed or memory-saving claim. One tiny actor does not
establish multi-database, many-store or full-application costs. Serialized ledger
metadata bytes and isolated cold-bootstrap instructions were not measured;
allocated pages are reported separately and are not a proxy for metadata size.
There were no predeclared numerical cost ceilings. The recommendation is to
accept these explicit costs, not to infer a universal performance guarantee.

## Functional matrix and evidence owners

| Boundary | Evidence |
| --- | --- |
| Rename Rust store/canister paths without changing keys | Extended model test checks all four role keys remain identical. This does not promise entity/schema rename compatibility. |
| Reorder/add declarations and namespaces | Production-runtime admission test preserves existing assignments, skips warm preparation, and adds only new allocations. |
| Store replacement | Admission test keeps only the old journal accessible and gives the new store empty data memory; no transfer is claimed. |
| Namespace replacement, including newly granted pool | Typed rejection before allocation commitment; backing bytes unchanged and original declaration retry succeeds. |
| Multiple namespaces / similarly prefixed / foreign owners | Exact-scoped journal selection and fixed-claim coexistence tests. |
| Missing/invalid roles, authority mismatch, revoked grants | Typed admission/resolution rejection; existing admission tests. |
| Exhausted pool and later expansion | New IcyDB test proves unchanged bytes/no capability on failure and unchanged existing slots after expanded-grant retry. |
| Host committed without omitted journal | New test proves policy cannot change through warm bootstrap, no extra preparation/commit occurs, and opening the journal still rejects. |
| Bucket profile mismatch | New test rejects persisted 1-page versus requested 4-page buckets without changing backing memory. Existing facade test confirms an already-committed host owns its profile. |
| Standalone/warm composed bootstrap | Four facade bootstrap tests, default-manager integration and lifecycle participant test pass. |
| Empty/debt journals and pending marker on real upgrade | L3's three generated PocketIC tests passed, including recovery controls and unchanged database-control bytes on rejection. |
| Reservation, generic retirement, failed persistence | Released dependency request/admission tests pass at the owning runtime. |
| Corrupt/inconsistent protected commit slots | Released dependency physical/ledger tests fail closed and retain current-format recovery contracts. |

Executed this turn: 12 IcyDB admission tests (three new), four facade bootstrap
tests, one default-manager test, one lifecycle-participant test, the extended
role-key model test, and two PocketIC measurement/lifecycle tests for each actor.
Dependency source copied unmodified from the registry 0.14.1 package into a
temporary directory: seven admission, five request, 15 ledger-commit and 12
physical-commit tests pass. These are focused selections, not full suites.

The dependency's packaged test lock referenced three unavailable cached versions;
its temporary lock was resolved offline to cached `cfg-if 1.0.4`, `syn 3.0.5`,
and `unicode-ident 1.0.24`. No repository lock or dependency source was changed
for those tests. IcyDB builds/tests use its locked graph. This is source-level
dependency regression evidence, not a bit-identical dependency test environment.

Limits: host-composition/exhaustion tests use the production runtime with native
backing, not a real Canic deployment. Reordering/renaming is qualified at model
and allocation owners rather than a combinatorial set of generated actors.
Persistence refusal and corrupt-slot recovery are tested; arbitrary process
termination at every physical write is not simulated. Existing applications and
all unrelated networks are unchanged. Local PocketIC instances were started for
the matched actor measurements.

## Reproduction and qualification notes

1. Export baseline `f3bd969be` to a temporary tree. Add exactly the maintained
   `open_database` test endpoint to its facade actor; retain its original schema
   and dependency lock. Do not add a legacy actor to maintained sources.
2. Build that actor in each tree with locked dependencies and the same
   `wasm-release` target settings, using **separate** Cargo target directories.
   Run `ic-wasm <compiler.wasm> -o <actor.wasm> shrink` for each.
3. Select each artifact via `ICYDB_MODEL_FACADE_WASM` and run the focused
   `model_facade` integration target with `--ignored --nocapture --test-threads=1`.
   Set `POCKET_IC_BIN` to the installed PocketIC 16 binary.
4. Compare raw bytes, optional deterministic gzip size, cycle outputs and
   instruction samples. The generated omitted-store reproducer remains in
   [its fixture README](../../../../../../../../testing/logical-memory/README.md).

Initial measurement setup errors were corrected before collecting the table:
assuming funding equalled total PocketIC balance caused subtraction overflow;
sharing target directories reused baseline macros in the candidate build.
The corrected explicit counter and isolated build passed. Neither failure was
a production runtime regression. No failed measurements enter the table.

Complexity of this qualification: four test/fixture Rust files plus report,
status/design and changelog documentation, approximately 350 added lines total.
Production execution/authority is unchanged. Tests gain three boundary cases,
one rename assertion and one small measurement endpoint; there is no new
allocator, recovery path, persisted mode or accounting framework.

Release boundary: focused lint, formatting, memory/dependency/deployment,
durability/endpoint invariants, documentation links and whitespace checks pass;
full workspace
validation and publication remain user-owned. Reclamation, data migration,
profile retuning, application changes and broad query optimisation remain out of
scope. Further implementation is not needed merely to chase these small costs.

## L5 — Approved redundancy cleanup

After L4, the user approved removing the duplicate commit-memory allocation
registry. The thread-local current-database selection remains; control IDs still
resolve from committed authority and actual memory opens still validate the
key/ID pair there. The process-global vector, mutex, collision checker and their
dedicated errors are deleted. This does not change bootstrap or recovery policy.

The `cleanup` CSV column uses the same actor, compiler profile, shrink pass and
PocketIC driver as the original candidate. The L4 table above remains the
pre-cleanup evidence, not the final artifact size.

| Metric | Before cleanup | After cleanup | Change |
| --- | ---: | ---: | ---: |
| Raw Wasm bytes | 2,599,304 | 2,598,260 | -1,044 (-0.040%) |
| gzip bytes | 848,915 | 848,699 | -216 |
| Function count | 12,181 | 12,179 | -2 |
| Median warm-open instructions | 706,578 | 705,555 | -1,023 (-0.145%) |
| Median warm-open message cycles | 7,787,947 | 7,788,759 | +812 (+0.010%) |
| Typed insert message cycles | 11,315,120 | 11,308,591 | -6,529 (-0.058%) |

Allocated stable bytes remain unchanged. These small mixed cost changes are not
a meaningful speedup claim. Versus the original 0.257.22 baseline, final raw
Wasm is +30,646 bytes (+1.194%), median warm-open instructions +1.168%, warm-open
message cycles +0.117%, and typed insert cycles +0.527%.

Request sorting and repeated request/snapshot construction belong upstream:
[ic-memory #6](https://github.com/dragginzgame/ic-memory/issues/6). Attribution
inspection found 4,610 shallow bytes across request-sorting helpers, but that is
not a measured removable saving. No dependency workaround or extra runtime mode
was added to IcyDB.

## L6 — Published ic-memory 0.14.2 adoption

The dependency update incorporates the #6 cleanup: selected requests reuse their
validated identity, unique-key request sorting no longer allocates, and historical
completion goes directly into the existing final resolver rather than building
an intermediate snapshot. API, durable format, admission/recovery preflights and
the single persistence boundary remain unchanged. No IcyDB Rust change is needed.

The `ic_memory_0_14_2` CSV column repeats the same Rust 1.98.1 actor build and
PocketIC workload against the published registry dependency. Relative to L5,
raw Wasm falls 513 bytes to 2,597,747 (-0.020%); gzip falls 91 bytes and function
count falls four. All three warm-open instruction and message-cycle samples,
typed-write cycles, watchdog cycles and stable bytes are exactly unchanged.
Install cycles fall 5,123,973 and same-version upgrade cycles fall 1,067,002;
these include compilation/lifecycle costs, not isolated allocation-bootstrap
execution. Historical-completion instruction/cycle savings remain unmeasured.
Final raw Wasm versus the original 0.257.22 baseline is +30,133 bytes (+1.174%).

All 18 focused admission/bootstrap/lifecycle native tests and five rebuilt-actor
PocketIC tests pass, including omitted-journal debt and pending-marker recovery.
Local PocketIC servers were started; unrelated networks were unchanged. This is
dependency adoption with unchanged IcyDB execution shape, not a new allocator or
recovery flow. Full-suite validation and publication remain user-owned.
