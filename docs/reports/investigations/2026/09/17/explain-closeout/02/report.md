# Bounded range-continuation repair

2026-09-17 · active 0.257.22 candidate on published `355bca1f7`.
User explicitly approved fixing the correctness failure in [qualification 01](../01/report.md).

## Outcome and cause

The failing range now returns `[0,1]`, `[2,3]`, `[4,5]` and terminates.
An independent native regression first reproduced the same failure without
SQL or diagnostic warm-up, excluding `.explain()` as a necessary trigger.

Two parts of the existing continuation handoff needed correction:

1. Materialized access leaves sorted their keys but did not apply the supplied
   primary-key resume boundary. Bounded pages revisited the same earlier keys;
   a later empty page could lose logical progress and repeat returned rows.
2. Projection could publish a scan frontier beyond rows withheld by its output
   window. Applying the resume boundary alone exposed skipped rows. Physical
   progress is now published only when the output has not withheld rows;
   otherwise the existing last-emitted logical boundary owns resumption.

The physical leaf retains only keys strictly after the authenticated boundary,
using the existing ASC/DESC comparator and a single existing cursor-work charge.
It does not assume physical index order is primary-key order. Projection folds
scan-exhaustion into the final continuation decision once, after deciding which
frontier is safe. No cursor format, execution route, cache, limit, authority
lifetime or persisted representation changes. No migration is required.

## Validation

95 focused test executions pass:

- 64 cardinality/session tests, including both range directions without explain,
  cold/warm point/range/1,024-member explain parity, zero row visits by explain,
  and complete ordinary results;
- 10 physical leaf tests, including strict boundaries before/within/after a
  materialized key set in both directions;
- four live-page tests, one exhaustive-page test and six page-window tests;
- the same 10 physical leaf tests without default features.

Two native timing tests remain ignored by policy. Focused all-feature core/test
Clippy, repository invariants, formatting and whitespace checks pass. All 60
active-document links/anchors resolve. Full repository/release tests
remain user-owned. No local IC/PocketIC network was started for this repair.

The large membership case was previously unreachable because the earlier range
case failed. Its cold assertion now explicitly clears the shared test cache:
an earlier query can legitimately populate the same parameterized template.
Its traversal guard also covers missing keys under the four-entry native page
envelope, with one fresh request budget per resumed endpoint invocation. Query
values, limits and expected rows were not weakened. Empty pages must change the
continuation, and every returned row must extend the expected result prefix.

Intermediate evidence is retained in `target/range-continuation-fix/`: the
independent pre-fix failure, the insufficient boundary-only correction, and the
final passing logs. Initial test imports were corrected before reproduction.

## Cost and complexity

The SQL-free typed-explain audit actor rebuild is byte-identical to qualification
01: **2,510,157 raw Wasm bytes, 6,411 defined functions; delta zero**.
SHA-256 `f8581ce124529dd63ec84e08febd66163c46f876b5e8f6cd6960de4795d02d03`.
Rust 1.98.1, locked dependencies, feature/profile/target and Binaryen flags are
unchanged from that receipt. Artifact: `target/range-continuation-fix/typed.wasm`.
This actor does not exercise resumed reads. It is not a measurement of their
Wasm contribution or runtime cost. Repair-specific cycles/instructions and
actors retaining general paginated reads remain unmeasured; no speedup claimed.

Repair scope: three Rust files, +105 net lines relative to the prior qualification
handoff: +12 production lines and +93 test lines. Seven documentation/release
files are updated or added alongside them. Continuation behavior converges on
the existing boundary contract; no new behavior axis or alternate flow is added.

The correctness blocker is resolved. Qualification 01's diagnostic cost misses
and omitted-measurement dispositions remain separate closeout decisions. This
repair does not accept those costs, reopen deferred accounting, or start 0.258.
