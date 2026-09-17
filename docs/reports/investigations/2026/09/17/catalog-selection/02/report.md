# Simple read queries — matched IC instruction qualification

Date: 2026-09-17. Baseline `7012352f13ec78bbc03b7224fb5f7fbb81d058a9`
(0.257.20); candidate is the shared accepted-snapshot handoff in 0.257.21.

## Verdict

Follow-up: the [whole-query investigation](../03/report.md) measures a smaller
real increase of 0.19–1.12% across all six warmed queries. The local measurements
below remain valid but do not establish whole-call gains or regressions.

**Mixed, not a general query-speed improvement.** The indexed range uses fewer
instructions, while warmed point, `IN` and count reads use 5.4–6.0% more in this
fixture. Other samples are essentially flat. These repeatable increases need
attribution before claiming an overall read-performance win. This measurement
does not identify their cause and does not change production code to fix them.

The earlier [schema-selection probe](../01/report.md) measured a different
boundary. Ordinary reads can already reuse complete runtime metadata. Its large
percentage reductions must not be extrapolated to these query results.

## Results

Local IC instructions; positive deltas mean **more** work. Each cell is repeated
three times with exactly the same instruction count. All 72 samples pass full
result equality across first/warmed calls and baseline/candidate artifacts.

| Query | First after load, baseline → candidate | Delta | Warmed, baseline → candidate | Delta |
| --- | ---: | ---: | ---: | ---: |
| Primary key | 3,048,168 → 3,047,684 | −0.016% | 2,730,497 → 2,893,890 | +5.984% |
| Indexed equality | 3,860,170 → 3,860,083 | −0.002% | 3,503,203 → 3,507,653 | +0.127% |
| Indexed range, limit 3 | 4,187,030 → 4,027,050 | −3.821% | 3,602,351 → 3,444,763 | −4.375% |
| Primary-key `IN` | 3,275,503 → 3,356,164 | +2.463% | 2,970,127 → 3,134,639 | +5.539% |
| Count | 2,934,301 → 2,934,752 | +0.015% | 3,058,461 → 3,224,031 | +5.414% |
| Grouped count | 3,649,628 → 3,650,175 | +0.015% | 3,502,605 → 3,509,288 | +0.191% |

Maintained audit actor, raw post-link Wasm: **4,485,070 → 4,483,015 bytes**
(−2,055 bytes, −0.046%). Defined functions: **10,489 → 10,490**.
Compiler-emitted sizes are 5,129,916 → 5,127,549 bytes. This actor includes SQL
and test/admin endpoints; these sizes do not predict a production application's
artifact. No whole-call charged-cycle or wall-clock measurement is reported.

## Method and limits

- Reuse the unmodified `canister_audit_sql_perf` actor and its six-row
  `PerfAuditUser` dataset (eight fields including timestamps; three user indexes).
  A fresh PocketIC fixture is installed for each query/artifact pair. Other
  maintained actor entities are present; this is not a single-entity canister.
- First-after-load queries run after installation, startup delivery, fixture
  reset/load and 64 zero-time ticks. **This is not a fully cold schema/startup
  measurement:** setup may already populate metadata caches.
- Three query messages exercise that same state, without persisting query-side
  cache changes. An update through `warm_user_query_with_perf` then retains
  caches; after 64 ticks, three further query messages measure the warmed state.
- Both states use `query_user_with_perf`. Its `performance_counter(1)` interval
  includes session acquisition, SQL parsing, planning and result execution;
  Candid request decoding and response encoding are outside the interval.
- Exact SQL is preserved in the [probe](probe.rs.txt). The calls are trusted
  audit reads, not public endpoint admission tests. Typed/dynamic terminal costs,
  larger datasets, writes and fully cold runtime construction remain unmeasured.
- No extra actor endpoint or unsafe boundary was added. The temporary native
  test module was removed after capture; its reproducible source is archived.

## Reproduction

Use separate baseline/candidate source copies, the same lockfile, Rust 1.98.1,
and `cargo build --locked --offline -p canister_audit_sql_perf --profile
wasm-release --target wasm32-unknown-unknown --features test-admin-api,candid-export`.
The compiler uses size optimization, fat LTO and one codegen unit. If sharing a
target directory between copies, explicitly invalidate the changed core crate:
Cargo's relative-path/mtime cache can otherwise reuse the prior snapshot. The
candidate core and its dependents were explicitly rebuilt for this capture.

Apply the repository's pinned Binaryen 132 transform: `-Oz
--enable-bulk-memory --enable-sign-ext --enable-nontrapping-float-to-int
--one-caller-inline-max-function-size=0`. Save as `baseline.wasm` and
`candidate.wasm`. Temporarily wire the archived probe as a normal child module
of the existing `sql_perf_audit` integration target; run only its named test with
`ICYDB_QUERY_MEASUREMENT_DIR` pointing to the two artifacts and PocketIC 16.0.0.

Post-link SHA-256:

- Baseline: `2d8e1671408533c73634ebc9f65029a45adeac35f99a8370bfa41eb4ad8847ed`
- Candidate: `052f2a36c9ab60643b11d958f79dff0fa54a404255330199d1969c6caf6cba61`

[Raw samples](samples.csv) retain every repeat. All 12 isolated instances were
released; shared networks were untouched. Full repository tests were not run.
