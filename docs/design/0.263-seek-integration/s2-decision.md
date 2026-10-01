# S2 — Polling retirement decision and bounded qualification

S2 retains ordinary polling and removes the unused held-head seek subsystem.
The [design](0.263-design.md) records the decision and canonical owners;
[S1](s1-baseline.md) supplies the route qualification limit. No admitted resumed
intersection IC benefit was established to justify a new alignment protocol.
The smaller maintained execution surface is the reason for this change.

## Maintained behavior

Physical leaves retain bounded refills, authenticated primary-key resume and
index continuation anchors. Ordered composites retain alignment and duplicate
handling; traversal retains existing cardinality admission and cursorless
complete-prefix integrity checks. Remove the pending-target adapter, stream
variant, held-key fields, repositioning methods and seek-only accounting.
No public API, planner admission, cursor or persisted format changes.

Port useful assertions to ordinary polling: independent sparse suffixes and
limits, one-sided primary bounds, duplicate physical index occurrences, typed
corruption and hard-budget rejection before refill progress. Existing page,
composite and structural integrity tests remain. Prototype-only interruption,
logical overflow and physical-reposition tests are deleted with their subject.

## Matched IC controls

Reuse the S1 runner and populations 0 (spaced overlap) and 5 (rotated late
intersection): two/three children, ASC/DESC, exact outputs and repeated work.
Each artifact gets a fresh instance per population and identical setup. The
bounded runner also checks residuals and authored total limits. These small IC
controls have no continuation; native live-page tests cover resumed polling.
The signed dynamic endpoint selects a primary scan; SQL selects logical
intersections. Do not describe these controls as an IC resumed-intersection
comparison. S3 owns the complete matrix and its explicit qualification limits.

| Artifact | Raw Wasm bytes | SHA-256 |
| --- | ---: | --- |
| Frozen S1 baseline | 4,504,970 | `325db7585feb97e1ce1693d15081cd2fbebacf20c73058c6c88523ecc514c314` |
| S2 candidate | 4,500,351 | `1d2134d15f6bf614c57b1fc62bb5e17082682ff0d9b845c91c2c05b75ff6bf21` |

Raw size delta: **−4,619 bytes (−0.10%)**. Both artifacts use Cargo 0.262.2,
Rust 1.98.1, PocketIC 16.0.0, canonical wasm-release post-linking, SQL and Candid
on, local build profile. Candid files are identical. Frozen files are
`/tmp/icydb-0263-s1-baseline.wasm` and `/tmp/icydb-0263-s2-candidate.wasm`.

Instructions use the endpoint's existing windows; cycles use whole-update
balance debits after settling. Loader/folding/setup charges are outside samples.
The different SQL and dynamic windows must not be compared as a frontend ratio.
[Cost receipts](s2-costs.csv) retain 96 samples: 32 baseline, 32 candidate and
32 from an independent candidate repeat. Baseline reproduces S1 exactly;
all candidate instruction/cycle samples reproduce exactly in the repeat run.

The table compares each shape's second identical sample, candidate minus baseline.
Every retained row count and physical-entry work count agrees.

| Endpoint / shapes | Instruction delta | Charged cycle delta |
| --- | ---: | ---: |
| SQL: six shapes other than population 0, three children | 0 | 0 |
| SQL: population 0, three children, ASC | −28,022 | −30,244 |
| SQL: population 0, three children, DESC | +55,710 | −20,264 |
| Dynamic pages: six shapes other than population 0, three children | −3,394 | −3,394 |
| Dynamic page: population 0, three children, ASC | +63,546 | +63,554 |
| Dynamic page: population 0, three children, DESC | +122,629 | +44,616 |

The mixed three-child deltas are repeatable and remain unattributed; they are
not dismissed as noise or called a speedup. The largest second-sample instruction
increase is 0.95% (SQL DESC); the largest charged-cycle increase is 0.21%
(dynamic ASC). Accept retirement for its reduced maintained state and raw size,
with these observed cost tradeoffs recorded. No general query-speed improvement
or seeking benefit is claimed. Mainnet costs remain unmeasured.

## Validation and delivery boundary

All 143 focused native tests pass: 64 stream/physical/composite/structural checks
and 79 live session/cardinality checks. Bounded baseline, candidate and candidate
repeat PocketIC controls pass. Core all-feature/all-target and changed integration
Clippy gates, formatting, layer authority, docs and diff checks pass.

An initial new native fixture used anchors outside its declared index envelope;
the maintained guard correctly rejected them. Correcting the fixture envelope
resolved the failure; runtime envelope admission was not changed.

Three disposable PocketIC servers were started and stopped. No ICP network was
changed. Prior dirty worktree contents are preserved; Cargo versions, full suites,
commits and publication remain user-owned. S2 is complete; S3 has not started.

Complexity: 13 files, approximately −560 net lines; the stream subsystem alone
shrinks by 835 lines. One internal variant and the dormant protocol disappear.
Existing physical/composite owners converge on polling; the existing IC runner
is reused. Implementation structure is simpler and no new debt axis is added.
