# E3 — Generated entity-rename rehearsal

Recorded 2026-09-20. Scope: the [E3 contract](0.261-design.md), not a production
deployment or minor-line closeout. Both generated-actor scenarios pass.

## Workload and proof

The source has Item rows `(id, key, label, parent_id)` of `(1,101,201,NULL)` and
`(2,102,202,1)`, and Holder `(id,item_id)` of `(10,2)`. Both source versions are
1. The successor declares CatalogItem version 2 with `from_name = "Item"`,
and Holder version 2 with its explicit dependency-only transition. Store,
namespace, fields, indexes and relation semantics stay unchanged.

- A pending successor gates ordinary reads and retains the predecessor head.
  One scenario restarts the exact successor before advancing and retains the
  exact pending status.
- One typed Advance publishes Applied, changes the accepted head, preserves
  database identity and reports zero rows rewritten and zero indexes rebuilt.
- Repeating the exact predecessor-head command simulates a lost response.
  Its entire terminal status/receipt matches, both immediately and after an
  exact-successor restart. No response-derived replacement command is used.
- SQL rows/columns/count remain exact; the public entity label changes to
  CatalogItem. Generated Item/CatalogItem and Holder bindings decode the same
  values through exact-key reads. Unique-key lookup returns the preserved row.
- Self/inbound restrictive deletes, dangling-target updates and a duplicate
  unique-key update all reject with the maintained typed constraint error;
  both entities remain unchanged after those checks and after restart.

[Native E2](e2-findings.md) owns exact row/index/reverse byte and internal identity
comparisons and interrupted compound-marker recovery. These actor tests do not
claim to interrupt execution inside an atomic IC message.

## Build identity

Base HEAD: `0339efcd9fe35d9a27c4d4f2b809f9b8a6c557c6` plus the uncommitted
0.261 E1–E3 worktree. Cargo versions remain 0.260.0. Rust is
`1.98.1 (48a229cea 2026-09-01)`, LLVM 22.1.8; PocketIC is 16.0.0.
Cargo.lock SHA-256:
`b72698fe6f83be77180e84a3616cb22b5e76f6f8a1fc6ab4f734748d8e7d74ff`.

The existing retained build helper builds `canister_test_sql` for
`wasm32-unknown-unknown`, Debug/LocalTest: dev opt-level 1, no LTO, 16 codegen
units, debuginfo stripped, overflow checks enabled. Both variants enable
`test-admin-api,local-sql-query`; source adds `entity-rename`, successor adds
`entity-rename-successor`. The canonical Binaryen 132 post-link pipeline uses
`-Oz`, bulk-memory, sign-ext, nontrapping-float-to-int and one-caller inline
maximum 0. These are broad test actors, not release-profile application actors.

| Final deployable, non-gzipped actor | Raw bytes | BLAKE3 |
| --- | ---: | --- |
| Source | 8,919,185 | `cfc19d68ee29dd832a50bd2b2ebad02d3ae449e8cb96c8ca51f51084e5282250` |
| Successor | 8,950,738 | `d53f65136a9d4c83401148513f38824ef372996a1f7118acd30c18c552d772c1` |

Successor minus source: **+31,553 raw bytes**. This includes different generated
declarations/plan reachability, not an isolated runtime-library size change.
Gzip and production composed/release size are unmeasured.

Source SHA-256 identities:

- [Declarations](../../../schema/test/sql/src/entity_rename.rs):
  `54c7c5dd370941811d92f88d5fffc5ac10ae29da27784dc24f58b188f299ffb2`.
- [Fixed controls](../../../canisters/test/sql/src/entity_rename.rs):
  `1b5815b4c6ab03adce5516a234f9044cc357890a57f0048d3ed596b6e38870cc`.
- [Rehearsal](../../../testing/integration/tests/entity_rename.rs):
  `3f148b26564df747bc751e714c5c0042b93fe0c8a997011222491b5daa66ea7b`.

## IC measurements

Rows and assertions are identical in both scenarios. Cycle charges are canister
balance differences around the existing migration-command helper, including
ingress and any work scheduled during that call. They exclude separate install,
upgrade, seed, read and startup-delivery calls. They are not migration-body-only
charges; no absolute cost ceiling was proposed for this characterization.

| Command envelope (cycles) | Direct advance | Restart before advance |
| --- | ---: | ---: |
| Initial Advance | 405,294,975 | 405,445,154 |
| Immediate identical retry | 132,531,995 | 132,745,522 |
| Identical retry after successor restart | 152,194,485 | 152,104,427 |

The existing `measure_sql_query_instructions` probe wraps request execution,
session acquisition and a trusted SQL read with `performance_counter(1)`.
Readiness delivery is outside its interval; response encoding and host work
are not measured by that counter. Query text is
`SELECT id, key, label, parent_id FROM Item ORDER BY id`, substituting
CatalogItem in the successor. The lifecycle point and prior calls matter.

| Read interval (instructions) | Direct advance | Restart before advance |
| --- | ---: | ---: |
| Source | 1,818,723 | 1,818,723 |
| After publication/retry | 34,651,563 | 34,591,800 |
| After successor restart and constraint checks | 1,981,331 | 2,061,331 |
| Publication minus source | +32,832,840 | +32,773,077 |
| Restarted minus source | +162,608 | +242,608 |

The first post-publication interval is substantially larger. This is observed
lifecycle-specific cost, not a diagnosed cause, optimization claim or matched
runtime revision comparison. Migration-body instructions and causal attribution
were unmeasured at the E3 handoff. The subsequent authorised
[I1 investigation](i1-read-investigation.md) attributes the recurring query-only
cost to accepted runtime/catalog preparation and separates restart from ordinary
update effects. Migration-body instructions remain unmeasured.

Both scenarios emitted these BLAKE3 identities:

- Empty Candid arguments to the fixed seed control:
  `6755e93b2ea267fb63a1ca43907378ed3923f6416bab2555c3c82aa060f41cae`.
  Values are fixed in the hashed control source, not encoded in this request.
- Candid Advance arguments, with the same command reused for both retries:
  `0b92fe0e0f5a4015c482fa2bd0c05c80040b4f9bc44ee3e7394f3bf718c824c5`.
- Re-encoded source projection:
  `bedf56c3343890fc91f4a9a9b3b9f7ea763145fa8e41a7c5d62a39ceac121ee0`.
- Re-encoded successor and restarted projection:
  `234fe17427faf7b0cd1445bbc0ef0ae7d2d7900d887a0d16092974d6a1fa99ea`.
  Projection hashes differ because the entity label changes; exact values,
  columns and row count are asserted independently of that intended difference.

## Validation and limits

The final `entity_rename::` selection passes both tests. The preceding complete
five-test migration target passed all three maintained physical cast scenarios;
only the then-incorrect rename projection-label assertion failed. Its correction
changes only the rename test, and the two affected tests were rerun successfully.
All-feature actor/schema checks and focused actor/schema and integration Clippy
gates pass. Formatting, whitespace and local documentation links are checked
at handoff. Full repository suites remain user-owned.

Initial fixture failures were corrected: INSERT sent to the public UPDATE
endpoint, an inadmissible full-scan typed read, and expecting the source entity
label after a successful rename. No production runtime fix was required.
An initial sandbox localhost bind failed before server startup. Four approved
disposable server runs then covered the fixture corrections and final evidence;
each runner stopped its own server. No application network or data was touched.

E3 touches 16 files, approximately +600 net lines, all fixture/build-helper,
test or documentation work. Test configuration gains two related build features
and three fixed test controls, reusing the existing actor and lifecycle harness.
Test complexity increases modestly; production runtime/state/format complexity
is unchanged. No dependency/version changes, commits or pushes. Q1 remains the
next, separate read-only closeout slice.
