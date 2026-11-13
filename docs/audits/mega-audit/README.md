# IcyDB architectural reduction audit

The maintainer requested this dedicated directory so the original brief and its
findings can be read together. It is a one-off investigation, not a new recurring
audit procedure or a replacement for GitHub issue tracking.

- [Original brief](brief.md), moved unchanged from `docs/ mega-audit.md`.
- [Findings and architectural verdict](findings.md).
- [Source inventory and coverage](inventory.md).
- [Architecture, proposed programmes and qualification](programme.md).
- [Implementation evidence and dispositions](implementation.md).
- [Write, migration and aggregate follow-on investigation](follow-on.md).
- [Schema installation, reconciliation and rebuild audit](schema-lifecycle.md).
- [Schema lifecycle removal deep dive](schema-lifecycle-removal.md).
- [Journal recovery closeout investigation](recovery.md).
- [Relation preparation closeout investigation](relations.md).
- [Prepared-query cache closeout investigation](query-cache.md).

- [End-to-end query admission consolidation design](query-admission-design.md).
- [First slice: one public input-value contract](query-admission-first-slice.md).
- [Toko consumer impact of the filter contract](toko-filter-impact.md).
- [Query admission removal map and test obligations](query-admission-removal-map.md).

## Status

First source-backed investigation, 2026-11-11. Repository inventory is complete
for the source roots named in the inventory. Semantic review is **partial**;
neither exhaustive coverage nor implementation readiness is claimed for every
candidate. The report distinguishes observed duplication, conditional
simplifications, and measurement-dependent hypotheses.

The initial audit changed no production code. The maintainer subsequently
authorized working through the proposals; implementation evidence is linked
above. Existing local 0.310 work is preserved. The maintainer authorized the
0.310 query-admission programme on 2026-11-12; its first slice replaces the parallel
filter literal contract. No deployment is selected.

The cleanup batch, including follow-on, DISTINCT, schema lifecycle and recovery
and query-cache changes, totals 293 non-test code lines removed and
80 test code lines added (net reduction: 213). Remaining hypotheses have explicit
[reviewed dispositions](implementation.md#reviewed-disposition-of-the-initial-ten-findings).
They are not an instruction to remove useful specialization or recovery state.
Wasm/IC impact and repository-wide semantic coverage remain unmeasured/incomplete.

## Additional maintainer context

Prioritize query admission, access traversal, schema lifecycle, storage layers,
generated codecs, write staging, predicate representations, metadata/caches,
error conversions, and historical tests. These are hypotheses, not defects.

Organize eventual work into **A — Delete**, **B — Consolidate**, and
**C — Re-architect**. Every accepted slice reports both net test/non-test code
change and net complexity change: independently maintained mechanisms,
representations, transformations, states and ownership boundaries removed or
added. A generic abstraction that merely relocates complexity is not success.

Retain useful Wasm specialization until matched measurements support changing
it. Under the repository's measurement rules, report uncompressed Wasm total
and code-section bytes plus IC instructions/cycles. Native timing, query-latency
benchmarks and compressed sizes mentioned in the brief are not selected here.
Allocation and storage-operation observations may explain mechanisms, but do
not substitute for the permitted performance measurements.
