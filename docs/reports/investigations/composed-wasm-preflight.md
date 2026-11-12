> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# ICYDB-033 — Composed Wasm attribution preflight

Date: 2026-09-13. Result: **empty composed pair complete; application attribution open**.
No runtime, application, dependency or measurement-framework changes were made.

## Follow-up: Canic's empty composed pair is complete

The separately queued Canic experiment now supplies a matched host/empty
metrics-enabled participant pair. Its retained report is
`canic/docs/audits/working/icydb033-composed-wasm/report.md` in the sibling
repository; structured measurements and source identities are alongside it.
The final artifacts' raw sizes and SHA-256 hashes were independently rechecked
from IcyDB without rebuilding or changing Canic:

| Final artifact | Raw Wasm bytes | Code-section bytes | Defined functions |
| --- | ---: | ---: | ---: |
| Canic host | 2,396,348 | 2,195,334 | 3,989 |
| Host + empty metrics participant | 3,819,840 | 3,582,760 | 7,158 |
| Increment | +1,423,492 | +1,387,426 | +3,169 |

Host SHA-256: `c3cfc9d8a49f65d9f153359bb99e404c0f73e54259e3022f07e3cd286f5f1672`.
Participant SHA-256: `368021ada457ee05db9a3effb9265cc74fcf53fee622254e8939431dd1fcff77`.
Section/count figures above are the Canic report's measurements; raw sizes and
hashes are the independent checks here. This is feature cost, not a regression
between IcyDB releases or a measured Toko Miner increment.

The pair uses dirty Canic based on 0.110.15, published IcyDB 0.257.9 and
ic-memory 0.13.3. It is SQL-off and metrics-on, with one journaled store and zero
entities. Rust 1.98.1, ic-wasm 0.11.1 and Binaryen 132 are held constant. Neither
the earlier standalone capture below nor the Toko 0.257.10/0.13.2 reference is
an interchangeable baseline. See the Canic report for the frozen source manifest
and full profile/dependency identities.

## Current startup/recovery source audit

The focused follow-up inspects current IcyDB source, including existing dirty
changes, against the older-pin Canic attribution above. No runtime change or
new before/after cost measurement is made. Fifteen focused native startup,
pending-schema resumption and generated-watchdog tests pass.

The actual dependency path is generated watchdog -> generated driver attempt ->
shared startup coordinator -> journal recovery, schema reconciliation and
optional cardinality construction. These are runtime-authority operations,
not per-generated-entity engine copies:

| Owner | Finding | Disposition |
| --- | --- | --- |
| [Generated callback and driver](../../../crates/icydb-model/src/build/actor/db/store/mod.rs) | The callback calls `startup_state()` before entering the request; the shared driver observes readiness again. Memory bootstrap itself is memoized. | Repeated readiness traversal exists, but the callback's invariant-failure completion is not equivalent to the driver's generic terminal outcome. Do not delete the guard without preserving classification. |
| [Shared startup driver](../../../crates/icydb-core/src/db/startup/driver/mod.rs) | Both Ready and Recovering states enter recovery/convergence. Pending schema jobs resume from durable receipts; Ready can still have cardinality work. | Keep the shared driver. Ready is not proof that the watchdog can always be omitted. |
| [Recovery](../../../crates/icydb-core/src/db/commit/recovery/mod.rs) | Replay/fold reaches ordinary prepared row/index publication. A marker-free, empty-tail fast path already exists, selected from durable controls. | No new generated-empty shortcut. Row preparation remains necessary for persisted journal records even when no application write endpoint is linked. |
| [Generated schema facade](../../../crates/icydb/src/db/session/catalog.rs) | `ensure_generated_schema_fragment` and `apply_generated_schema_fragment` repeat proposal decoding/head selection, then differ in migration deferral and return contract. Generation selects one route. | Source-level DRY opportunity, not evidence that both flows contribute to this empty actor. Do not promote it to a measured Wasm fix. |

The startup tests include corruption classification, durable failure receipts,
schema-receipt readiness and post-Ready cardinality quiescence. A separate schema
test demonstrates pending activation resumption without the generated source
model. These constrain any reduction; an empty generated entity list cannot
replace accepted catalog, journal and schema-job authority.

## Startup-driver outlining result

The authorized follow-up tested exactly one source change: `#[inline(never)]`
on the existing generated `startup_driver_attempt` in
[the actor generator](../../../crates/icydb-model/src/build/actor/db/store/mod.rs).
Both maintained production subjects are unchanged byte-for-byte, at both
compiler-emitted and final deployable stages. **The annotation is discarded.**
No new driver, recovery shortcut, result mode or compiler policy remains.

| Production subject | Raw bytes, before = after | Defined functions, before = after | Code-section bytes, before = after |
| --- | ---: | ---: | ---: |
| Empty metrics | 1,829,471 | 4,644 | 1,719,170 |
| Entity-bearing lifecycle participant | 2,035,056 | 5,261 | 1,909,085 |

All raw-byte, code-section and function-count deltas are zero. Equality was
checked with direct binary comparison, not just sizes. Generated actor outputs
contain the annotation, and a traced lifecycle rebuild confirms Cargo work
with the candidate. This is not evidence that a named closure contains no
inlining; it establishes that this specific annotation produces no artifact
change for these two subjects under this build configuration.

## Initial standalone findings

The maintained reachable-operation pair adds 40,269 final raw Wasm bytes for
nine additional simple entities: 4,475 bytes/entity rounded up, below the
existing 8,192-byte ceiling. This is a local fixture observation, not a universal
per-entity price or a Toko Miner measurement. The fixture reaches typed paging,
exact-key reads, inserts, updates, batches, deletion and nested input binding.
Its first entity contains nested profiles; the nine added entities are simple.

The largest named function bodies are shared costs, not an engine copy for each
entity. Prioritise the shared prepared-page/projection and startup/schema owners
for investigation, but require a matched instruction and final-Wasm experiment
before changing them. Function size alone does not establish redundant logic.

Toko Miner's existing whole-actor totals cannot attribute bytes to IcyDB. The
Canic lifecycle probe inspected initially is not an interchangeable baseline either: it pins
IcyDB 0.257.9, does not enable IcyDB metrics and enables Canic internal test
fixtures. Toko Miner pins IcyDB 0.257.10 with metrics enabled in Game Shard and
Translation. Both siblings have unrelated dirty work, which was left untouched.

## Verification and next action

Production and named builds, standard report capture, Twiggy analysis and the
existing reachable-entity ceiling check passed during the initial capture.
The follow-up verifies Canic's raw artifact sizes/hashes without rebuilding.
There were no runtime edits, network lifecycle changes or full-suite run.
IC cycles/instructions and per-application increments remain unmeasured.

Next for ICYDB-033: decide on matched current-source composed qualification at
the existing Canic owner. The standalone driver annotation produced no change
and was removed; do not repeat it as an untested local optimization candidate.
Any composed candidate needs a matched before/after, not a subtraction from
the older-pin pair. The ICYDB-003 guide is complete separately;
its application interruption tests remain outside this measurement work.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/13/composed-wasm-preflight.md).
