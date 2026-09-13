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

Named diagnostics identify the generated startup watchdog and its recovery,
schema and storage dependencies as the first investigation owner. The named
watchdog subtree retains 897,805 bytes; this is overlapping diagnostic reachability,
not an additive or removable-size estimate. Named and canonical code ordering
differs despite matching section sizes and function counts. Do not remove
recovery just because the generated schema has no entities: accepted runtime
authority, persisted state and lifecycle obligations must still be established.

The requested empty-pair experiment is complete. Any optimization now needs a
matched before/after at that startup/recovery owner; no safe reduction is proved
yet. Query/write/per-entity composition and IC instructions/cycles remain
unmeasured. No new fixture framework or sibling implementation was added here.

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
| [Generated callback and driver](../../../../../../crates/icydb-model/src/build/actor/db/store.rs) | The callback calls `startup_state()` before entering the request; the shared driver observes readiness again. Memory bootstrap itself is memoized. | Repeated readiness traversal exists, but the callback's invariant-failure completion is not equivalent to the driver's generic terminal outcome. Do not delete the guard without preserving classification. |
| [Shared startup driver](../../../../../../crates/icydb-core/src/db/startup/driver.rs) | Both Ready and Recovering states enter recovery/convergence. Pending schema jobs resume from durable receipts; Ready can still have cardinality work. | Keep the shared driver. Ready is not proof that the watchdog can always be omitted. |
| [Recovery](../../../../../../crates/icydb-core/src/db/commit/recovery.rs) | Replay/fold reaches ordinary prepared row/index publication. A marker-free, empty-tail fast path already exists, selected from durable controls. | No new generated-empty shortcut. Row preparation remains necessary for persisted journal records even when no application write endpoint is linked. |
| [Generated schema facade](../../../../../../crates/icydb/src/db/session/catalog.rs) | `ensure_generated_schema_fragment` and `apply_generated_schema_fragment` repeat proposal decoding/head selection, then differ in migration deferral and return contract. Generation selects one route. | Source-level DRY opportunity, not evidence that both flows contribute to this empty actor. Do not promote it to a measured Wasm fix. |

The startup tests include corruption classification, durable failure receipts,
schema-receipt readiness and post-Ready cardinality quiescence. A separate schema
test demonstrates pending activation resumption without the generated source
model. These constrain any reduction; an empty generated entity list cannot
replace accepted catalog, journal and schema-job authority.

### Best next experiment

Test a compiler outlining boundary on the existing startup-driver body before
changing semantics or adding a new driver/result mode. The named timer closure's
131,017 shallow bytes are consistent with substantial inlining, but names and
dominator output do not prove exact source-to-byte attribution or duplication.
An explicit no-inlining boundary is a measurement hypothesis, not a committed
optimization. Keep it only if a controlled comparison justifies it.

Use the same current source/dependency graph for both sides and the maintained
empty-metrics and entity-bearing lifecycle subjects. Freeze profile, features,
tool versions and post-link flags. Measure final raw bytes and defined functions;
qualify startup/recovery instructions through the existing IC fixture, including
failure, pending schema activation and Ready convergence. Do not compare a
current candidate directly against the published-0.257.9 Canic pair. A subsequent
composed confirmation must reuse Canic's existing fixture, not add another host.

No safe large deletion or removable fraction of the 1,423,492-byte increment was
established. The duplicated readiness read is a separate possible instruction
optimization; combining it with an outlining experiment would obscure attribution.
Do not add an empty-database mode, bypass recovery or transfer ownership to
ic-timers from symbol names alone. Native checks do not establish IC cost savings.

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

## Local production-profile capture

Used the existing `wasm-audit-report.sh` and `check_wasm_entity_scale` owners.
Both actors use `wasm-release`, production build policy, SQL off, metrics off,
no default features and `candid-export`. Rust is 1.98.1 / LLVM 22.1.8;
`ic-wasm` is 0.11.1 and Binaryen is the repository-pinned 132. The canonical
post-link flags, path remapping, export checks and byte/hash checks were used.

| Final deployable measurement | One entity | Ten entities | Difference |
| --- | ---: | ---: | ---: |
| Raw Wasm bytes | 2,979,041 | 3,019,310 | +40,269 |
| Code-section payload bytes | 2,809,826 | 2,848,425 | +38,599 |
| Defined functions | 7,676 | 7,755 | +79 |
| Gzip bytes (secondary) | 1,206,330 | 1,210,714 | +4,384 |

IcyDB HEAD is `82b0d71c9346024825c1f6404b91e10361516f8c`, with the pending
memory-profile and mutation-diagnostic changes. Cargo.lock SHA-256:
`56a97a09a69fe5784d7d0f2dca7b03c9ae4cd32ef304f4c9052738217e88233f`.
The tracked code/manifest/lock diff digest was unchanged before and after:
`e9522e172d04cedde3ea66f56d585bdbb67b22065e12de304b2c110799da6c4a`.
This digest is corroboration, not a substitute for a clean source snapshot.
The standard report correctly marks this dirty capture **non-comparable** for
release-baseline/regression purposes. No historical improvement is claimed.

Final raw SHA-256, one then ten:

- `0f12845d962c4c2575c1c212d2324eba4d120b89f2cffb449726b1c25d962d72`
- `757817c6a5bb77d732de4bde4759bc2337a626653b516a97c7f547b9bb2bc0f1`

Full generated reports and Twiggy output are disposable local evidence under
`/tmp/icydb-033-standalone-audit/`; final artifacts are in `artifacts/wasm-size/`.
The hashes and selected measurements above remain the retained receipt.

Reproduce the production capture with a fresh output directory:

```sh
bash scripts/ci/wasm-audit-report.sh --sql-variant sql-off \
  --canister one_entity_reachable_operations \
  --canister ten_entity_reachable_operations --report-dir /tmp/icydb-033-new-run
```

## Named ownership signals — not deployable-size accounting

Built both packages with the existing `wasm-attribution` profile, no default
features and `candid-export`, then used `ic-wasm shrink --keep-name-section`
and Twiggy 0.8.0. Direct Cargo builds do not use the production path-remapping
wrapper, and this diagnostic shrink is not the canonical deployable pipeline.
Never subtract these artifacts from the production measurements above.

| Named function, shallow bytes | One entity | Ten entities |
| --- | ---: | ---: |
| Generated `startup_driver_attempt` | 68,815 | 68,815 |
| `execute_structural_projection_rows_inner` | 43,896 | 43,896 |
| `lower_initial_schema_proposal` | 26,353 | 26,353 |
| `fold_oldest_journal_batch` | 25,173 | 25,173 |
| `execute_trusted_live_page` | 24,828 | 24,828 |

The named live-page subtree retains about 743.5 KB in either diagnostic actor.
Retained subtrees overlap; do not sum them or treat them as removable bytes.
The ten-entity monomorphisation report also includes fixture helper instances,
drop glue and sorts. Its approximate bloat estimates are not predicted savings.
The 1.30/1.32 MB function-name sections are diagnostic metadata, not shipped cost.

Named artifact SHA-256, one then ten:

- `11bb1a06994ddaceb3e1462847333a4c705db1d3ef2e5982bb6bb987a5ead6ef`
- `7431aafe1d918ca516e2449242e3eadec997b0ad910abbb616189fece1fb8e43`

## Composed experiment scope

Keep lifecycle composition at its existing Canic owner; do not introduce a
Canic dependency into IcyDB's runtime or a second composition implementation.
Reuse the lifecycle probe's participant wiring in controlled audit subjects,
with one frozen dependency graph, SQL off and metrics on throughout.

| Successive subject | What the increment establishes |
| --- | --- |
| Canic host only | Host/control-plane baseline |
| Host + empty IcyDB participant | Fixed database/lifecycle/metrics cost |
| One generated entity + binding | Schema and binding cost |
| Same entity + typed page query | Query reachability cost |
| Same entity + typed insert | Write reachability cost |
| Same operations + bounded nested input | Additional codec/input-binding cost |
| Ten matching entities with those operations | Incremental entity specialisation |

The first two subjects are now delivered by Canic as recorded above; the later
rows remain prospective, not completed measurements or automatic authorization.

Keep host policy, lifecycle, instrumentation and non-database exports fixed.
Record final raw/code/data bytes, defined functions, resolved features, lockfile,
commit and optimizer identity. Pair optional named artifacts separately. Do not
use the full lifecycle test fixture's unrelated provisioning/export machinery
as the host-only side of a subtraction.

This synthetic matrix locates shared owners; it still does not attribute the
exact Game Shard or Translation workload. Closeout needs a matched application
capture or an explicit narrowing of ICYDB-033's acceptance. Existing lifecycle,
typed-constraint and relation tests must remain intact if implementation changes
follow. No sibling edits or new dependency pins are authorised by this report.

## Verification and next action

Production and named builds, standard report capture, Twiggy analysis and the
existing reachable-entity ceiling check passed during the initial capture.
The follow-up verifies Canic's raw artifact sizes/hashes without rebuilding.
There were no runtime edits, network lifecycle changes or full-suite run.
IC cycles/instructions and per-application increments remain unmeasured.

Next for ICYDB-033: inspect the current generated startup/watchdog and recovery
registration owner, preserving its accepted-schema and lifecycle obligations.
Any candidate reduction needs a matched current-source before/after, not a
subtraction from the older-pin pair. The ICYDB-003 guide is complete separately;
its application interruption tests remain outside this measurement work.
