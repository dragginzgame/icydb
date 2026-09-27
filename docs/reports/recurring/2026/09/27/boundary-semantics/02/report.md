# Boundary / Envelope Semantics Audit — Storage Proof Follow-Up

## Metadata And Scope

- Authorization: implement BND-01's focused storage proof and rerun the audit.
- Definition: `docs/audits/recurring/range/boundary-envelope-semantics.md`.
- Method: **BOUNDARY-5 + DOMAIN-1**, unchanged from the daily baseline.
- Compared baseline: [2026-09-27 run 01](../01/report.md), the canonical daily
  baseline, preserved unchanged.
- Code snapshot: `b6111f5f7185136b56860ee35287b206d9debafa`, workspace
  `0.261.11`, plus the pre-existing dirty Cargo inputs and this run's addition
  to `crates/icydb-core/src/db/index/scan/tests.rs`.
- Comparability: **comparable**. Scope, invariants, method, features and
  verification families remain the same. The existing store-scan family gains
  one executable test for an already required obligation.
- Definition SHA-256:
  `2bb0a5c398ecd02b10f8fcd62c503d059eaac52d6c981079f8b20dd6a7ef10ca`.
- Changed test source SHA-256:
  `635996f907893ddb6d3ab4b2dd34930e2d1bed95707b2d5639940d1b0dd6f938`.
- Unchanged `Cargo.toml` SHA-256:
  `b34b059d3b555740d4d2ccdfb2a5ad39f102381294ed87313beee8d5cc78242c`.
- Unchanged `Cargo.lock` SHA-256:
  `695051feaba3c8de26eed05f5db590dddd2568f1f7493924e24b87ca08767716`.
- Daily baseline report SHA-256:
  `7a022f34e108bbcfe0fc2658435ba6cdcb0360a27a405a191d62fbe68d828610`.

The [frozen scope and exclusions](../01/report.md#frozen-scope) carry forward:
range lowering, directional containment and progress, suffix-aware ordering,
empty traversal suppression, and scalar query/authority/route binding.
Grouped, mutation-job and exhaustive-page proof lifecycles, hostile persisted
key ordering, all-type certification, performance and Wasm remain excluded.
No additional method change or runtime implementation was introduced.

## Invariants, Transformations And Ordering

The baseline's [invariant registry and transformation proof](../01/report.md#invariant-registry-and-transformation-proof),
[complete raw-bound mapping](../01/report.md#logical-to-complete-raw-mapping),
and [ordering/continuation analysis](../01/report.md#ordering-and-continuation-analysis)
remain applicable source evidence: all production files are unchanged at the
same HEAD, and the dirty Cargo inputs retain their recorded hashes. This reuse
covers those exact source mechanisms, not the baseline's blocked verdict or
historical test results. All selected behavioral cases are executed anew.

Reinspection of `index/scan/raw.rs::visit_raw_entries_in_range` and
`visit_canonical_raw_entries_in_range` confirms that `envelope_is_empty` returns
before backend selection and range construction. The new test exercises both
entry points. ASC still rewrites only the lower edge to `Excluded(anchor)`;
DESC rewrites only the upper edge. Current signature/authority/route admission,
full-key comparator, suffix encoding and logical post-filtering are unchanged.

## Adversarial Matrix And Empty-Envelope Proof

The baseline [adversarial matrix](../01/report.md#adversarial-matrix) remains
in scope with its explicit source-only limits. BND-01 now has the following
additional executed proof:

`db::index::scan::tests::empty_envelopes_skip_populated_store_traversal`

The fixture uses valid complete BigInt index keys. Heap storage contains four
keys. Journaled storage contains three folded canonical keys and a fourth live
overlay key. This prevents an empty-store result from masquerading as a guard.

| Case | Expected callbacks in live ASC / live DESC / canonical-only scan |
| --- | --- |
| Resume ASC at included upper endpoint | 0 / 0 / 0 |
| Resume DESC at included lower endpoint | 0 / 0 / 0 |
| Inverted included bounds | 0 / 0 / 0 |
| Equal lower excluded, upper included | 0 / 0 / 0 |
| Equal lower included, upper excluded | 0 / 0 / 0 |
| Equal bounds both excluded | 0 / 0 / 0 |
| Equal included singleton control | 1 / 1 / 1 |
| Unbounded populated control, heap | 4 / 4 / 4 |
| Unbounded populated control, journaled | 4 / 4 / 3 |

Both endpoint cases derive their effective bounds through the production
`resume_bounds_for_continuation` helper. All cases run against both populated
backends. The positive controls establish callback observability, preserved
singleton behavior, and distinct canonical/overlay contents.

Zero callbacks alone would not prove that an empty iterator was never
constructed. The executed callback checks are therefore paired with the
source proof that the shared guards precede range construction, as required
by BOUNDARY-5. Inverted and equal-exclusive intervals also exercise shapes
that must not reach invalid backend ranges. No test-only runtime branch or
instrumentation was added to production code.

The remaining containment, raw/logical ordering, suffix tie-break, cursor
binding and multi-page no-duplication/no-omission obligations retain their
baseline mechanisms and freshly executed tests. Their stable-data and sampled
value-domain limits are unchanged; this is not a claim of live-view snapshot
pagination or a whole-system correctness verdict.

## Findings And Verdict

**BND-01 — RESOLVED in this run.** The formerly missing populated-store proof
now covers endpoint collapse, inverted intervals and equal-exclusive intervals
in the live and canonical scanners. The original finding remains immutable
in run 01. No new finding was identified.

**Verdict: PASS for the declared scope.** Required source and behavioral
obligations have sufficient evidence. The baseline's drift triggers remain:
new encodings need independent ordering evidence; merged traversal callers must
preserve exact-prefix envelope preconditions; chunk-progress and public-token
changes need corresponding directional/binding proof.

## Verification Readout

The exact command wrapper bodies and selector sets are retained in the
[baseline verification readout](../01/report.md#verification-readout).
They are unchanged and were inspected before execution. Selection A includes
the complete `db::index::scan::tests::` family, which now selects the new test
as well. Selection B retains its eight named cases.

All commands ran at `/home/adam/projects/icydb`, with `rustc 1.98.1
(48a229cea 2026-09-01)`, repository Cargo paths from Makefile,
`RUST_TEST_THREADS=8`, `--locked -p icydb-core --lib --features sql`, no
ignored-test override and no property-case reduction. Source inputs are the
snapshot and hashes recorded above. No behavioral result from run 01 is
counted as a new execution or substituted for one.

| Exact command / check | Outcome | Selected / passed / failed / ignored |
| --- | --- | --- |
| `cargo fmt --all` | PASS | N/A |
| `bash /tmp/icydb-boundary-audit-selection.sh --list` | PASS | 62 / N/A / N/A / 0 |
| `bash /tmp/icydb-boundary-audit-selection.sh` | PASS | 62 / 62 / 0 / 0 |
| `bash /tmp/icydb-boundary-audit-admission.sh --list` | PASS | 8 / N/A / N/A / 0 |
| `bash /tmp/icydb-boundary-audit-admission.sh` | PASS | 8 / 8 / 0 / 0 |
| `git diff --check`, local-link and snapshot/hash verification | PASS | N/A |

**Newly executed behavioral tests: 70 passed, 0 failed, 0 ignored** (69 in the
daily baseline). Selection A covers envelope 14, semantic encoding 29, range
lowering 5, store scan 7 and selected session cases 7. Selection B covers
planner tightening 2, token admission 4, full-key order 1 and logical cursor
filtering 1. This includes the required empty-envelope storage proof; it is
no longer blocked.

The same 65 unused/dead-code compiler warnings remain in the `sql`-only core
build. No clippy failure occurred; clippy and full repository/workspace suites
were not run. The full suite remains user-owned. No IC/PocketIC service was
started, stopped or reconfigured.

## Delivery And Follow-Up

This follow-up changes four files: one owner-local test file, this report,
and root/detailed changelogs. The test adds approximately 83 net lines;
overall the follow-up adds approximately 264 lines including audit evidence
and release notes. Runtime implementation shape is unchanged, with no new
behavior axes, execution flows, formats or configuration. Verification debt
BND-01 is resolved.

Version `0.261.11` already has its release tag, so changelog-only notes open
`0.261.12` within the current minor line. Cargo versions and the pre-existing
Cargo dependency changes are untouched. The audit definition and daily
baseline report are also unchanged from run 01.

Raw Wasm size, IC cycles and instruction deltas are **unmeasured**; this change
adds test coverage and makes no runtime performance claim. No implementation
follow-up remains for BND-01. Future evidence belongs in a new audit run.
