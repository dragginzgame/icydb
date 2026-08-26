# Recurring Audit — Wasm Footprint (2026-08-26)

## Report Preamble

- Scope: the eight production actors required by the recurring Wasm-footprint
  audit, built with `wasm-release`, maintained production features and Candid
  metadata.
- Definition:
  [`docs/audits/recurring/crosscutting/crosscutting-wasm-footprint.md`](../../../../../../audits/recurring/crosscutting/crosscutting-wasm-footprint.md)
- Compared baseline:
  [`docs/reports/recurring/2026/06/15/wasm-footprint/01/report.md`](../../../../../06/15/wasm-footprint/01/report.md)
- Code snapshot: `12400d581c60e826e5ef8e6013366bbe18f2abd8`
  (tree `d242440ba158ef69121f32a5a3368c839353fbd5`).
- Capture: 2026-08-26 16:55 UTC on `main`; source worktree clean before
  report artifacts were added.
- Method: `WASM-3.0`; raw post-Binaryen `-Oz` bytes are authoritative and
  gzip bytes are secondary context.
- Toolchain: Rust 1.97.1, `ic-wasm` 0.11.1 and Binaryen `wasm-opt` 108.
- Comparability: the June baseline uses an incompatible target/method schema,
  so historical numeric deltas are unavailable. This clean eight-actor run is
  the current comparable baseline.
- Auditor: Codex.

## Verdict

**PASS WITH FINDINGS.** The required production artifacts and structural
breakdowns were captured from a clean source tree. The current one-entity SQL
actor is effectively unchanged from the 0.244 closeout measurement. Three
cross-cutting opportunities merit narrower measurement audits, but none is
evidence for an immediate production rewrite.

## Checklist Results

| Requirement | Status | Evidence |
| --- | --- | --- |
| All eight production actors captured | PASS | Per-actor size reports and summaries under `artifacts/` |
| Machine-readable raw/gzip measurements | PASS | Eight `size-report.json` artifacts |
| Twiggy top breakdown | PASS | Eight `twiggy-top.txt` artifacts |
| Twiggy retained ownership | PASS | Eight `twiggy-retained.csv` artifacts |
| Twiggy dominators | PASS | Eight `twiggy-dominators.txt` artifacts |
| Twiggy monomorphization | PASS | Eight `twiggy-monos.txt` artifacts |
| Current baseline selected | PASS | Clean `WASM-3.0` snapshot recorded above |
| Historical baseline deltas | PARTIAL | June target/method schema is incompatible |
| Production code unaffected by audit | PASS | Only this recurring report directory changed |

PASS=8, PARTIAL=1, FAIL=0.

## Authoritative Production Matrix

| Actor | Compiler raw | Final raw | Gzip | Post-link reduction | Functions | Data bytes | Exports |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `default_empty` | 2,221,621 | 1,946,039 | 742,918 | 275,582 (12.40%) | 4,881 | 95,601 | 5 |
| `default_empty_metrics` | 2,430,457 | 2,127,698 | 815,445 | 302,759 (12.45%) | 5,439 | 109,644 | 7 |
| `one_entity_dynamic_query` | 3,383,230 | 2,971,900 | 1,172,327 | 411,330 (12.15%) | 7,708 | 151,921 | 10 |
| `one_entity_typed_query` | 2,418,252 | 2,117,501 | 812,145 | 300,751 (12.43%) | 5,347 | 102,861 | 6 |
| `one_entity_sql_query` | 3,730,564 | 3,287,248 | 1,302,888 | 443,316 (11.88%) | 8,418 | 156,565 | 6 |
| `ten_entity_typed_query` | 3,237,318 | 2,847,161 | 1,121,863 | 390,157 (12.05%) | 7,260 | 140,622 | 6 |
| `sql` | 4,394,178 | 3,836,191 | 1,485,875 | 557,987 (12.69%) | 9,379 | 186,871 | 15 |
| `sql_perf` | 5,092,561 | 4,453,651 | 1,731,325 | 638,910 (12.54%) | 10,853 | 233,789 | 70 |

Every row has its complete machine-readable size report, size summary and four
required Twiggy views under [`artifacts/`](artifacts/).

### 0.244 Continuity

The current `one_entity_sql_query` final artifact is 3,287,248 raw bytes. The
documented 0.244 closeout measurement was 3,287,174 bytes: a difference of only
+74 bytes (+0.0023%). Its Candid hash is also unchanged. This is continuity
evidence, not a formal historical baseline comparison, but it rules out
meaningful unexplained growth in the active SQL surface.

## Directional Surface Comparisons

These subtractions identify surfaces worth inspecting. They are not isolated
owner measurements because actors can retain different generic instances and
linker closures.

| Directional comparison | Final raw delta | Context |
| --- | ---: | --- |
| Metrics minus empty | +181,659 (+9.33%) | Intended optional observability surface |
| One typed entity minus empty | +171,462 (+8.81%) | Smallest query-capable typed surface |
| One dynamic entity minus empty | +1,025,861 (+52.72%) | Dynamic proposal/query surface |
| One SQL entity minus empty | +1,341,209 (+68.92%) | SQL frontend and executor closure |
| One SQL entity minus one typed entity | +1,169,747 (+55.24%) | Broad SQL-specific retention signal |
| Ten typed entities minus one typed entity | +729,660 (+34.46%) | About 81,073 bytes per additional entity directionally |
| Mutation SQL actor minus one SQL entity | +548,943 (+16.70%) | Additional mutation and canister surface |
| SQL performance actor minus one SQL entity | +1,166,403 (+35.48%) | Audit-only endpoints and query matrix |

The empty floor is already 59.20% of the one-entity SQL actor, so a meaningful
cross-product reduction must examine shared startup/schema machinery as well as
SQL-specific code.

## Structural Attribution

The final deployable SQL artifact attributes 1,125,250 retained bytes (34.23%)
to `query_one_entity_sql`. That is the strongest authoritative SQL ownership
signal in this run.

The symbol-bearing diagnostic build further localizes candidates without
claiming deployable savings:

| Named diagnostic signal | Typed actor | SQL actor |
| --- | ---: | ---: |
| Generated `startup_driver_attempt` retained | 1,066,322 | 1,009,362 |
| `lower_application_candidates` retained | 271,360 | 247,799 |
| `lower_existing_store_candidate` shallow / retained | 55,212 / 124,104 | 55,225 / 124,118 |
| Public query export retained | 126,581 | 1,194,182 |

The near-identical schema-application values show a shared runtime floor rather
than SQL-local duplication. The much broader SQL query closure then supplies a
separate frontend/executor target. Full diagnostic context is recorded in
[`artifacts/named-attribution-summary.md`](artifacts/named-attribution-summary.md).

Twiggy also estimates 113,858 named bytes across four leading sort families in
the typed actor, 171,947 in SQL and 148,653 in the ten-entity actor. These are
generic-bloat signals, not predicted final savings. Sharing them through erased
types, allocation or indirect dispatch could reduce bytes while worsening
instructions and code complexity.

## Findings and Optimization Order

| ID | Risk | Finding | Disposition |
| --- | --- | --- | --- |
| `WASM-001` | MEDIUM | Startup and schema application dominate the shared runtime floor | Owner audit complete; bounded experiment only |
| `WASM-002` | MEDIUM | SQL ingress retains a 1.125 MB execution closure | Audit the complete SQL owner surface before design |
| `WASM-003` | LOW | Sort monomorphization is the largest generic-bloat family | Accept until a paired Wasm/performance experiment exists |

The optimization order is therefore:

1. If a future minor prioritizes footprint, measure one private generated
   no-removal lowering contract beneath the singular core application
   authority. Reject it below 32 KiB final raw savings per minimum actor.
2. Map the SQL ingress closure across session, frontend and executor boundaries
   before choosing another query optimization. Prefer a converged flow that
   benefits a broad query family over another edge-specific fast path.
3. Attempt sort sharing only as a paired experiment with final raw Wasm,
   allocation and instruction measurements. Reject it if it introduces dynamic
   dispatch, new state or material query regressions.

The metrics delta is intentional, `sql_perf` is an audit canister rather than a
production-minimum target, static data is not a dominant structural owner, and
gzip is not the project size authority. None is a priority cleanup from this
evidence.

The completed startup owner analysis, invalid alternatives and experiment
contract are recorded in
[`artifacts/startup-schema-application-owner-audit.md`](artifacts/startup-schema-application-owner-audit.md).

Machine-readable findings, owners and triggers are in
[`findings.json`](findings.json).

## Complexity and Validation

This audit changes documentation only: zero production lines, types, mutable
owners, caches, invalidation edges or behavior axes. It does not start another
0.244 landing patch or a new minor.

- Eight production Wasm builds and their post-link size pipelines passed.
- All required Twiggy top, retained, dominator and monomorphization captures
  passed.
- Clean-source provenance was preserved in every machine-readable report.
- The symbol-bearing attribution builds passed and remain diagnostic only.
- Historical numeric comparison is partial because the June baseline used an
  incompatible schema.
- No full test suite was run; this documentation-only audit did not alter code.
- No commit, tag, release or push was performed.

This run should be the comparison baseline for the next `WASM-3.0` footprint
audit. The next closeout opportunity is the read-only SQL-ingress owner audit;
implementation still requires a separately bounded and accepted slice.
