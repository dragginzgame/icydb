# S1 — Signed lookup admission and bounded qualification

Accepted schema normalization now proves positive Eq/In comparisons exact for
signed Int8/Int16/Int32/Int64 query atoms, including projected newtypes and
relation key kinds. It carries Strict into the maintained planner and index
compiler; no downstream coercion gate, executor route or format is added.
Every inspected membership operand is charged before admission. Unproved
comparisons retain their existing coercion. Native tests establish equality at
extrema/null, type/operator boundaries, proof exhaustion, actual selected index
paths, duplicate membership and OR, directions, limits, residuals and issued
cursor suffixes. The signed intersection fixture now checks indexed execution.

## Matched artifacts and measurement boundary

Freeze the current source context, including the separate A1 cleanup, in a
detached measurement checkout. Replace only the candidate normalizer with the
published `60a4bd9ea` blob for the baseline, then restore its current source for
the candidate. Source inspection confirms that frozen candidate Rust inputs
match the real workspace. Implementation lives in the real workspace; the
isolated checkout exists only to build controlled artifacts. The earlier
shared-workspace baseline attempt was rejected by the input-mutation guard
during concurrent cleanup and supplies no measurement receipt.

Both builds use Cargo versions 0.263.0, Rust 1.98.1 and the canonical local
SQL/Candid-enabled wasm-release pipeline with Binaryen 132. Both use the same
frozen checkout and path trimming; Candid bytes match. These are new matched
artifacts, not a comparison with the earlier 0.263 artifact built under different
package/context inputs. Historical 0.263 receipts remain unchanged.

| Artifact | Raw Wasm bytes | SHA-256 |
| --- | ---: | --- |
| Baseline | 4,501,195 | `b6b5e86d012668157b0ba508108ff889c90cc408d646b68a3f1d75fc88d62ee8` |
| Candidate | 4,501,556 | `6e5140ce179087c952b40cec63d8ea428cc3f5bbded5525afec579c8d530349b` |

Raw delta: **+361 bytes (+0.008%)**. Local frozen modules are
`/tmp/icydb-0264-s1-baseline.wasm` and `/tmp/icydb-0264-s1-candidate.wasm`.
Reuse the existing bounded IC runner for cases 0 (spaced) and 5 (rotated late),
two/three predicates and ASC/DESC. Install fresh actors on PocketIC 16.0.0;
fixture writes, folds and deferred fees settle outside sample intervals.
Instructions and charged cycles use the existing endpoint-owned measurement
windows. SQL and dynamic endpoints remain separate controls.

Three runs pass: baseline, candidate and independent candidate repeat. Every
one of the candidate's 32 cost samples repeats exactly. The
[CSV](s1-costs.csv) retains 64 matched observations including both samples.
Exact IDs, repeated page work, residuals and authored limits pass. Native
accepted-catalog selection establishes signed index admission; SQL EXPLAIN
alone is not a proof of the dynamic route. Work counters are semantic evidence,
not an instruction/cycle proxy.

## Warmed results

Compare the second identical sample at each endpoint. All eight dynamic
controls reduce instructions by **49.8–74.0%** and charged cycles by
**35.4–52.3%**. Examples:

| Dynamic shape | Instructions before → after | Cycles before → after |
| --- | ---: | ---: |
| Spaced, two predicates, ASC | 21,048,184 → 10,463,860 | 29,607,024 → 19,022,551 |
| Spaced, three predicates, DESC | 21,236,909 → 6,338,975 | 29,803,436 → 15,064,588 |
| Rotated, two predicates, DESC | 20,355,374 → 5,349,222 | 28,839,373 → 13,913,079 |

SQL control instruction deltas range from −169,696 to +244,098; cycle deltas
range from −5,629 to +148,599. Two shapes are cost-identical. Maximum increases
are **4.08% instructions** (spaced three-predicate ASC) and **0.84% cycles**
(rotated two-predicate DESC). Results and logical intersection selection remain
correct. Those reproducible cost increases are recorded tradeoffs; their cause
is not isolated by these interleaved control samples. Do not claim uniform SQL
improvement or use cross-endpoint ratios as optimization evidence.

## Validation, complexity and remaining scope

All 120 focused native tests pass. Required make-Clippy recovery and refreshed
all-feature/all-target core Clippy pass; formatting, authority, documentation
and diff checks pass. Two new scalar-fixture metadata mistakes (text kind and
zero index ordinal) and an oversized test helper were corrected before the
passing gates. No unresolved validation failure remains. Full suites remain
user-owned. Three disposable PocketIC servers were started and stopped.

S1 touches ten files, approximately +670 net lines including tests, design and
receipts, excluding A1's separately authored portions of shared documentation.
Production normalization grows by 48 net lines. The rule adds a small semantic
proof to an existing owner; execution flows converge on existing Strict paths
without adding independent states, modes or strategies.

At S1 handoff, S2 was not started. The full dense/disjoint and wide-row resumed
IC matrix and scoped closeout are now recorded in the [S2 qualification](s2-qualification.md).
Mainnet costs and sustained write throughput are unmeasured by this work.
