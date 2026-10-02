# Saved code review status

Updated 2026-10-02. This document tracks verification and repair of the saved
29 September 2026 review of commit `994715134` (`0.261.16`) against the current
`0.264` line. The original HTML review remains unchanged.

The review is open. Its 345 raw findings contain 307 distinct, non-refuted
items. An original status such as “verified” or “reproduced” describes the old
snapshot; it is not proof that the current code is fixed. The inventory below
keeps every distinct ID visible without declaring unchecked findings resolved.

Source: [saved review](IcyDB%20Code%20Review.html).
Release owner: [0.264 tracker](../design/0.264-signed-index-admission/0.264-status.md).

## Current progress

| Current status | Findings |
| --- | ---: |
| Verified fixed | 8 |
| In progress | 0 |
| Open | 5 |
| Partial | 1 |
| Needs verification | 293 |

“Verified fixed” requires current code evidence and focused semantic validation.
“Open” means the reviewed defect remains visible in current source; reproduction
is recorded separately. “Partial” identifies an implemented correction that
does not cover the whole original finding. “Needs verification” means no current
closure verdict has been established; it does not mean the finding is a confirmed
current defect. Performance claims require raw Wasm, IC cycles or instructions.

## Authorized work queue

The user authorized continuing the confirmed examples and maintaining this
status document on 2026-10-02. One bounded outcome is one handoff under the
repository rules. Generic continuation takes the next queued outcome in 0.264;
this queue does not authorize starting a different minor line.

| Order | Finding | Bounded outcome | Status |
| --- | --- | --- | --- |
| 1 | `value-types-error-4` | Normalize decimal multiplication operands in the existing schema numeric owner; qualify typed and checked results and SQL stored-field reads | Complete as A13 |
| 2 | `index-access-2` | Preserve signed timestamp ordering through current index encoding, decoding and range reads | Complete as A14 |
| 3 | `index-access-3` | Converge index suffix bounds on the maintained composite primary-key contract, including writes and reads | Complete as A15 |
| 4 | `r2-covering-projection-1` | Make hybrid component admission and decoding agree on supported kinds, with current projection results | Queued |
| 5 | `executor-aggregate-1` | Resolve owned group keys through existing hash buckets before creating groups | Queued |

Other findings retain their individual verification state below. In particular,
`model-schema-crates-1` (decimal excess precision incorrectly saturating) is a
separate outcome from operand padding; A13 must not claim it resolved.

## Validation record

The initial check passed 19 focused regressions for all five distinct critical
findings, plus three hybrid admission/ownership tests. Rust formatting and diff
checks passed. A disposable probe reproduced `value-types-error-4`: padded
scale-18 `20 × 20` returned `None`, while normalized values returned `400`.
The decimal division overflow probe returned `None` without a panic; this is
narrow evidence for `value-types-error-3`, not complete closure of that item.

A13 completes `value-types-error-4`. Operand normalization prevents padding
overflow without changing accepted stored field scales. All 44 focused tests
pass: 26 decimal tests, 17 runtime numeric tests and one SQL regression that
verifies admitted scale-18 storage and exact cold/warm multiplication results.
Two new decimal regressions and the SQL regression failed before the correction.
True magnitude overflow retains its existing typed errors and saturation.

Required maintainer Clippy recovery and strict schema/core all-feature
library/test lint pass. The first locked lint attempt met a concurrent
`ic-timers` update; the offline maintainer check then needed its new cached
version. Copying the already-downloaded dependency into the repository-local
cache resolved this without changing manifests or lockfile. The 44-test gate
was refreshed against the current lockfile. Formatting, schema/format guards,
documentation references, inventory uniqueness and diff checks pass.

Eight owned files add approximately 540 net lines, mostly this complete
inventory and direct regression coverage. Production arithmetic adds four
lines; the implementation shape is neutral, with no new behavior axis. Existing
hybrid cleanup and memory/timer dependency changes are preserved. A13 handed back with `index-access-2` queued;
other review findings retain their individual verification state below.

A14 completes `index-access-2`. Timestamp index components reuse the existing
signed 64-bit transform. The signed-byte check, primitive pairwise order check
and stored-range regression failed before correction: a full signed range omitted
all negative timestamps. Unique and non-unique accepted indexes now pass equality,
signed extrema, duplicate timestamp ordering, strict/inclusive ranges, ASC/DESC,
total limits and every issued continuation suffix. Full row reads verify decoded
timestamps against their inserted values; planner checks prove index-range
admission. Primary-key timestamp round trips retain their signed representation.
The separate hybrid unsupported-component finding is still queued.

All 41 distinct focused tests pass (42 executions across timestamp, ordered-key
and numeric range selections). Strict all-feature core library/test Clippy,
formatting, schema/format guards, documentation references and diff checks pass.
An initial fixture reset failed when changing index uniqueness under one retained
registry; separate unique/non-unique test cases provide isolated accepted schemas.
No production workaround was needed.

This is a pre-1.0 hard cut of current version-1 index bytes. Reinstall/recreate
stores with timestamp index components, regenerate indexes and discard retained
continuations. Opaque prior timestamp payloads have no encoding discriminator;
in-place retention is unsupported. No compatibility decoder or new format/mode
is added. Nine owned files add approximately 370 net lines, mainly regression
coverage and documentation. Production changes one expression with no line
increase; the implementation shape is neutral. Existing dirty work is preserved.
A14 handed back with `index-access-3` queued.

A15 completes `index-access-3`. The strict index codec reuses the primary-key
owner’s 254-byte composite bound. Four new regressions fail before correction:
maximum suffix encoding, maximum stable sizing and indexed inserts with two
full-width principals (64 bytes) or four accounts (254 bytes). All 48 focused
tests pass against the current lockfile. The 46 codec tests cover user/system
suffixes, complete borrowed decode, row witnesses, stable reopen, current maximum
sizes, scan bounds and malformed/truncated rejection. Two session regressions
prove accepted index prefix/range admission, indexed inserts, typed unique
conflicts, cold/warm lookups, full rows, ASC/DESC and every continuation suffix.

Required maintainer lint recovery and strict all-feature core library/test lint
pass. Formatting, schema/format guards, documentation references, inventory and
diff checks pass. Recovery corrected test-helper const/closure warnings and an
order-type import error. Concurrent memory/testkit updates required copying their
existing downloads into the local offline cache; current-lockfile checks pass,
and dependency edits remain outside this slice.

The stable key bound grows by 191 bytes. Reinstall/recreate persisted index stores
with current indexes and fresh continuations; in-place retention is unsupported.
Tuple and primary-key representations retain their current shape and version-1
posture, without a compatibility bridge. Nine owned files add approximately 440
net lines, chiefly regression coverage and notes. Production replaces the bound
expression and adjusts its import (+2 net lines); complexity is neutral with
one shared authority. Existing dirty work is preserved. The next queued finding
is `r2-covering-projection-1` (hybrid unsupported-component admission/decoding).

Full repository suites remain user-owned. Raw Wasm, IC cycles and instruction
deltas are unmeasured. No network lifecycle actions have been taken.

## Finding inventory

All 307 distinct, non-refuted findings are listed once. Duplicate aliases and the
four refuted raw findings remain in the source review and are excluded here.
Counts cover inventory states, not implementation or release readiness.

| Finding | Review severity | Current status | Review subject and current evidence |
| --- | --- | --- | --- |
| `index-access-1` | critical | Verified fixed | Filtered (partial) unique index: conflict re-check ignores the index predicate, so a committed batch cannot be folded or replayed (wedge risk) **Current evidence:** C76; four filtered-unique online fold/restart regressions pass. |
| `query-plan-1` | critical | Verified fixed | Two-sided PK range `pk >= a AND pk < b` has its predicate stripped although KeyRange end is inclusive: the row with pk == b is returned, counted, updated or deleted **Current evidence:** C72; five endpoint/read/count/mutation/pagination regressions pass. |
| `r2-cardinality-freshness-1` | critical | Verified fixed | Migration staging/abort folds the live overlay into canonical outside the journal protocol: durable prefix counts drift (rows vanish from indexed reads) and later folds wedge on count underflow **Current evidence:** C74; four migration staging/abort overlay regressions pass. |
| `r2-upgrade-lifecycle-1` | critical | Verified fixed | Renaming or moving a journaled store's Rust type is treated as STORE_CORRUPTION and permanently bricks the canister; reverting does not clear it **Current evidence:** C79; three store-path rejection and rollback regressions pass. |
| `schema-catalogs-1` | critical | Verified fixed | Record-member rename is metadata-only, but stored record values use member names as keys, so every existing row with that record becomes undecodable **Current evidence:** C77; three populated record rename/abort/recovery regressions pass. |
| `commit-1` | high | Needs verification | Online background folding routes batches through the LIVE accepted catalog, so a metadata-only entity rename published with retained debt makes older batches permanently unfoldable |
| `data-1` | high | Needs verification | Scalar fast path reports corruption for rows filled by a non-null historical default (ADD COLUMN ... DEFAULT) |
| `data-2` | high | Needs verification | Value-storage walker misreads canonical enum envelopes (tag 0x84), so nested record-path reads fail for records that contain an enum |
| `data-4` | high | Partial | Byte-level readers treat a legitimately absent historical slot as corruption (resumable UPDATE, nested field paths) **Current evidence:** C101 fixes resumable UPDATE; nested field-path readers remain unverified. |
| `executor-aggregate-1` | high | Open | Generic hash GROUP BY 'DirectOwned' probe path never looks up existing groups: one group per row for enum/unit/collection/composite keys **Current evidence:** Owned group resolution still inserts without probing existing groups. |
| `executor-aggregate-3` | high | Needs verification | Unbounded generic grouped finalization ignores DESC: groups always come back in ascending key order |
| `executor-aggregate-5` | high | Needs verification | Zero-key global DISTINCT aggregate fails with an invariant error on any NULL; its NULL semantics also diverge from the per-group path |
| `executor-stream-1` | high | Needs verification | Resumed secondary-order IN-list pages cap each branch at limit+1 with no resume anchor, silently dropping rows |
| `executor-stream-5` | high | Needs verification | Resumed non-PK-ordered pages never reposition physical streams: each page rescans from the range start (quadratic traversal, deep pages exhaust the budget) |
| `index-access-2` | high | Verified fixed | Timestamp index components use unbiased two's-complement bytes, so negative timestamps sort after positive ones **Current evidence:** A14 reuses signed encoding; primitive order and accepted unique/non-unique equality/range/pagination regressions pass. |
| `index-access-3` | high | Verified fixed | Index keys cap the primary-key suffix at 63 bytes but composite PKs can encode up to 254 bytes, so inserts fail on indexed entities **Current evidence:** A15 shares the 254-byte primary-key bound; full-width principal/account inserts, unique conflicts, admitted index ranges, resumed reads, bounded decode and stable reopen tests pass. |
| `model-schema-crates-1` | high | Needs verification | Decimal `Mul`/`MulAssign`/`Product`/`powu` return ~±1.7e10 when the exact product needs >28 fractional digits |
| `query-expr-2` | high | Needs verification | The FALSE set of a SQL NOT is compiled as a two-valued predicate NOT, so rows where the inner comparison is UNKNOWN (NULL operand) pass |
| `query-expr-4` | high | Open | In expression-lane filters, a missing nested path aborts the whole row, so an OR with a true sibling still rejects it **Current evidence:** Missing nested-path errors still reject the entire filter result. |
| `query-intent-1` | high | Open | SortRequiresMaterialization can never fire: the admission summary always reports materialized_sort=false, so public reads admit materialized ORDER BY **Current evidence:** Admission still constructs an empty materialization summary. |
| `query-intent-2` | high | Open | PublicRead counts every secondary-index route as bounded, including the planner's unbounded whole-index fallback, so public pages can full-scan the table **Current evidence:** Admission still treats every secondary-index route as satisfying the index requirement. |
| `query-intent-3` | high | Needs verification | The shared plan-cache key identifies literals only by an XXH3-128 digest with a public seed when a filter has no parameter template, so a forged collision would serve one caller another caller's plan |
| `query-plan-3` | high | Needs verification | Grouped canonical ORDER BY with mixed per-term directions is admitted, but grouped output is sorted with a single direction |
| `r2-covering-projection-1` | high | Open | Hybrid covering fails valid SELECTs with an invariant error whenever a projected index component is anything other than Bool/Int/Nat(<=64)/Text/Ulid/Unit **Current evidence:** Hybrid decoder still errors on kinds outside its six-tag decoder. |
| `r2-cross-message-concurrency-2` | high | Needs verification | Each migration Validating or abort page rewrites and heap-snapshots the whole index store, so large stores make both Advance and Abort trap while the database is gated |
| `r2-identity-allocation-1` | high | Needs verification | Dense field-ID renumbering orphans the Identity allocator: removing a field that sorts before the PK restarts Identity::next at 1 (reuse on empty entities; permanent insert failure and integrity corruption after a physical migration) |
| `r2-upgrade-lifecycle-2` | high | Needs verification | Store retirement or addition is committed irreversibly before the same release's schema reconciliation: store-key changes, entity moves and storage-mode switches leave neither the new nor the old wasm usable |
| `r2-upgrade-lifecycle-3` | high | Needs verification | Any store-topology change re-keys every TargetStoreIdentity, orphaning the entity source-lineage catalog; later migrations fail with Unadopted, and adoption is impossible once any entity is past version 1 |
| `schema-application-1` | high | Needs verification | Missing cardinality is treated as corruption; during upgrade startup this persists a terminal failure that blocks the rebuild that would clear it |
| `schema-application-2` | high | Needs verification | Capacity eviction can drop the deployed generated submission's receipt, flipping a Ready database to Recovering with no watchdog running |
| `schema-application-3` | high | Needs verification | Generated reconciliation rejects entities with any live activation, including SQL-DDL-owned ones, and persists it as terminal while the only way to finish the activation needs Ready |
| `schema-migration-1` | high | Needs verification | Entity-local `page.rows == 0` oversize check under a page-global leftover budget deterministically wedges RewritingRows/FinalValidation (non-abortable) and Validating |
| `schema-migration-2` | high | Needs verification | Lineage accepted_head is not re-stamped by SQL DDL, constraint-activation abort, or non-migration builds; every later migration/adoption fails with StaleAcceptedHead |
| `schema-mutation-1` | high | Needs verification | Aborting a SQL unique-index activation leaves staged candidate index entries behind for good, and they later break index DDL and generated schema removal |
| `schema-store-1` | high | Needs verification | Schema publication retain/position sweeps delete or overlay every cardinality record in a single message (unbounded work in DDL and in recovery fold) |
| `session-sql-1` | high | Needs verification | ILIKE/LOWER text predicates mean different things in the expression and predicate lanes; resumable UPDATE uses the expression lane and silently updates the wrong set |
| `value-types-error-3` | high | Needs verification | Decimal::checked_div panics on i128::MIN / -1 (integer division overflow), which callers can trigger through SQL arithmetic |
| `value-types-error-4` | high | Verified fixed | Decimal multiplication does not normalize its operands, so fixed-scale (e.g. e18) decimal fields overflow on tiny products like 20 × 20 **Current evidence:** A13 normalizes operands; all 44 focused tests and strict lint pass, including admitted scale-18 SQL reads. |
| `xc-architecture-1` | high | Needs verification | Filtered-index predicates are persisted as name-based SQL text and re-parsed by a second grammar, losing typed literals: membership, planner implication and uniqueness disagree |
| `xc-security-1` | high | Needs verification | Cursor HMAC key is deterministic on wasm32/IC, so continuation tokens can be forged |
| `canisters-testing-ci-1` | medium | Needs verification | The two Tier A SQLite and mutation oracle lanes match zero tests and pass on every PR |
| `canisters-testing-ci-2` | medium | Needs verification | PR CI never runs, or even compiles, the PocketIC tests for recovery, upgrade, migration, durable jobs and guard authorization |
| `canisters-testing-ci-3` | medium | Needs verification | The dependency_msrv job actually builds with 1.98.1, because rust-toolchain.toml overrides the toolchain the action sets |
| `canisters-testing-ci-4` | medium | Needs verification | Instruction-budget assertions can never fail (the 40B IC limit) or rely on a stale baseline; the 30B recovery allocation is not enforced for trapped recovery |
| `cli-1` | medium | Needs verification | `schema migration run` treats identical status pages as stalled, but core returns identical pages during legitimate multi-page FinalValidation and Idle journal draining |
| `cli-2` | medium | Needs verification | `canister refresh` reinstalls (wipes stable memory) with `--yes` on any environment, including one implicitly selected via ICP_ENVIRONMENT |
| `cli-3` | medium | Needs verification | One-shot `icydb sql` prints canister-returned errors to stdout and exits 0 |
| `cli-4` | medium | Needs verification | Migration `run`/`advance`/`abort` exit 0 on Rejected, and abort exits 0 when the migration was already Applied |
| `cli-5` | medium | Needs verification | Interactive shell executes a half-typed statement on Ctrl-D even though the banner advertises Ctrl-D as quit |
| `cli-9` | medium | Needs verification | Migration command Candid text leaves numeric literals untyped (`revision = N`), relying on icp-cli to recover types from canister metadata |
| `executor-aggregate-2` | medium | Needs verification | Grouped page cursor boundary is taken from the projected row, not the canonical group key |
| `executor-aggregate-6` | medium | Needs verification | Grouped continuation never seeks the access stream; resumed pages re-fold and re-count all pre-cursor groups against the cumulative max_groups |
| `executor-aggregate-7` | medium | Needs verification | Zero-key grouped aggregates return no row on empty input, while global-DISTINCT and SQL global aggregates return one row |
| `executor-aggregate-8` | medium | Needs verification | Grouped FIRST/LAST depend on traversal direction and access path (ORDER BY on group keys changes aggregate values) |
| `executor-stream-2` | medium | Needs verification | DISTINCT over a branch-ordered IN-list stream trips the primary-key monotonicity invariant ('HashMaterialize' is implemented as adjacent dedup) |
| `executor-stream-3` | medium | Needs verification | IntersectOrderedKeyStream reports no page access bound, so PK-ordered live pages over a general intersection fail with an invariant error |
| `facade-2` | medium | Needs verification | Public SQL renderers print stored user text raw, allowing terminal-escape injection and forged table rows in operator tooling |
| `index-access-4` | medium | Needs verification | Principal index-component order (content-lexicographic) differs from Principal::cmp (length first) |
| `index-access-5` | medium | Needs verification | Oversized equality/range literal on an indexed column returns InvariantViolation instead of a correct result |
| `index-access-6` | medium | Needs verification | Stable index B-tree page size is derived from the 16,477-byte maximum key (~133 KiB of stable memory per node) |
| `index-access-7` | medium | Needs verification | Heap-resident exact prefix-cardinality metadata grows with every distinct index prefix and is never trimmed while the canister runs |
| `integrity-relations-1` | medium | Needs verification | Durable startup failure receipts are bound only to database state, so a fixed upgrade cannot clear a failure caused by a code bug and the canister stays wedged |
| `integrity-relations-3` | medium | Needs verification | Delete-restrict re-projects and re-charges the same surviving source row once per deleted target, so 'update-away plus delete targets' batches hit the relation budget quadratically |
| `integrity-relations-4` | medium | Needs verification | Updates charge both old and new images against a batch limit equal to the per-image limit, so rows with more than about 2,730 references can be inserted but never updated |
| `integrity-relations-5` | medium | Needs verification | prove_empty_reverse_relation_domain scans the entire target index store, capped at 262,144 entries, so relation or entity removal fails on any large target store |
| `integrity-relations-6` | medium | Needs verification | Deep index and reverse phases livelock on an oversized (corrupt) source row: pages report InProgress forever with an unchanged checkpoint |
| `journal-jobs-1` | medium | Needs verification | Any catalog-lookup failure (migration gate, recovery-pending, internal error) permanently ends a mutation job as AcceptedSchemaChanged |
| `journal-jobs-2` | medium | Needs verification | Direct progress-store writes skip recovery admission and can invalidate a pending marker's MutationProgress `before`, making recovery fail permanently |
| `journal-jobs-3` | medium | Needs verification | Durable access-state revision rises by 2 on every startup, so no resumable job or exhaustive cursor over journaled stores survives an upgrade |
| `journal-jobs-4` | medium | Needs verification | Heap-store read-set revisions reset on upgrade (ABA), so a pre-upgrade exhaustive proof and cursor can be accepted against different data |
| `model-macros-1` | medium | Needs verification | Declared normalizers/validators are silently never run in many accepted positions (ty on entity/record/enum/tuple; item normalizers on list/set/map/tuple/enum payloads; item validators on tuple and enum payloads) |
| `model-macros-2` | medium | Needs verification | `IS TRUE` / `IS FALSE` in generated CHECK predicates are lowered to `= TRUE` / `= FALSE`, which lets NULL through under the three-valued check evaluator |
| `model-schema-crates-2` | medium | Needs verification | Account treats `subaccount: None` and `Some([0;32])` as different accounts; core stores and indexes both, and text parsing collapses them |
| `model-schema-crates-3` | medium | Needs verification | Decimal Add/Sub/Div saturate at the wrong scale when scale alignment overflows, returning values smaller than the dominant operand |
| `model-schema-crates-4` | medium | Needs verification | Decimal `Rem` returns ZERO on scale-alignment overflow, so the MultipleOf application validator accepts non-multiples |
| `model-schema-crates-5` | medium | Needs verification | Nested relation lowering silently drops relation leaves that are reachable only through a recursive type, contradicting the authoring guide |
| `model-schema-crates-6` | medium | Needs verification | Fragment and migration plan are never composed at build time; a mismatch appears only on the canister and blocks all DB work |
| `model-schema-crates-7` | medium | Needs verification | Numeric validator/normalizer constructors silently replace unrepresentable bounds with 0 (Clamp can rewrite every value to 0) |
| `query-expr-3` | medium | Needs verification | Rewriting a scalar-WHERE CASE into AND/OR drops CASE's lazy evaluation, so a CASE-guarded division errors on the rows it was meant to skip |
| `query-expr-9` | medium | Needs verification | REPLACE with an empty search string inserts the replacement between every character, and text functions can amplify output without being charged to the execution budget (including during plan-time folding) |
| `query-intent-4` | medium | Needs verification | The AND-constraint simplifier restarts at index 0 after every operator replacement, doing caller-driven O(R*N^2) work with no metering before admission |
| `query-plan-2` | medium | Needs verification | Residual stripping discharges case-insensitive `Ne` / `NotIn` using strict equality, removing live filter clauses |
| `r2-candid-stability-1` | medium | Needs verification | icydb_schema returns every entity's full description in one reply with no size guard or paging; a valid large schema makes every call trap |
| `r2-cardinality-freshness-3` | medium | Needs verification | Derived-cardinality inconsistency aborts the authoritative journal fold (STORE_CORRUPTION, terminal), although the 0.230 contract says it only makes evidence unavailable |
| `r2-covering-projection-2` | medium | Needs verification | Pure covering over an undecodable component kind scans and buffers the whole index range, throws it away, then the scalar path scans again under the same hard budget |
| `r2-cross-message-concurrency-3` | medium | Needs verification | A Building cardinality generation restarts on every journal fold, so it never finishes on large, busy stores; the watchdog runs forever and later stores never get a build |
| `r2-persisted-sql-text-1` | medium | Needs verification | DDL RENAME COLUMN reorders field-to-field predicates in generated filtered indexes, so the next generated schema release fails startup reconciliation |
| `r2-query-call-caches-2` | medium | Needs verification | Folding a schema batch after C1 was rebuilt leaves the store bundle cache empty; C1 hits never refill it, so admitted-root cardinality evidence is silently unavailable and query calls re-decode the bundle |
| `r2-recursive-bounds-1` | medium | Needs verification | Value-storage materializing decoder counts two depth units per nesting level, so nested-path reads reject (as Corruption) values the canonical write path accepted up to depth 64 |
| `r2-recursive-bounds-2` | medium | Needs verification | Recursive row decoders re-skip every subtree at each nesting level: decode cost is O(bytes x depth), up to ~63x write cost, while all row/page budgets are byte-based |
| `r2-replay-cost-1` | medium | Needs verification | Migration journal fold reloads and fully re-decodes the durable migration record twice per journal record, so large migrations produce rewrite pages that can never be folded |
| `r2-replay-cost-2` | medium | Needs verification | Unique-validation page fold re-reads, re-hashes, decodes and re-fingerprints the whole canonical accepted-schema bundle once per staged index key |
| `r2-replay-cost-3` | medium | Needs verification | DDL user-index replacement batches: the fold re-fingerprints the entity snapshot for every 64-key chunk, and admitted batches can hold 2x the 65,536-effect maximum that the convergence evidence proves and measures |
| `r2-upgrade-lifecycle-4` | medium | Needs verification | Deterministic registry-reconciliation rejections are non-terminal: the startup watchdog retries every second forever, re-reading the full control slot, while startup_state() reports Recovering |
| `r2-upgrade-lifecycle-5` | medium | Needs verification | A metadata-only migration touching only heap stores never wakes the stopped startup watchdog, so the canister stays Recovering after Applied until another upgrade |
| `repro-migration-1` | medium | Needs verification | Physical migration fails at Validating whenever dense field removal renumbers a retained pre-existing field |
| `schema-application-4` | medium | Needs verification | Pending application needs an exact precomputed final head; any intervening publication leaves the job unfinishable and unabortable, holding a record slot forever |
| `schema-application-6` | medium | Needs verification | Admission does not enforce the entity-name rules (64-byte limit, case-insensitive uniqueness) that the runtime root requires; accepted schemas can fail every runtime-root compile |
| `schema-application-7` | medium | Needs verification | Nested-leaf expansion during lowering has no budget; a record DAG with wide reuse can trap before any size check |
| `schema-catalogs-2` | medium | Needs verification | DESCRIBE / SHOW COLUMNS expand composite types with no depth limit or cycle check, so recursive (or heavily reused) composite types trap the call |
| `schema-migration-3` | medium | Needs verification | Plan-less generated source changes silently desynchronize lineage version/digest, later blocking unrelated migrations with MissingMigration |
| `schema-migration-4` | medium | Needs verification | Dangling lineage entries (removed or omitted entities) make current_proposal_lineage_is_applied permanently false: status never reports Applied and the successor fails startup with Downgrade |
| `schema-migration-5` | medium | Needs verification | A Rejected migration cannot be retried after fixing data: exact retry stays Aborted, generated submission identity cannot change, and findings beyond the first page are unobservable |
| `schema-mutation-2` | medium | Needs verification | Complete-domain staging charges its budget for every index entry and scans every row in the store, not just the target entity's |
| `schema-mutation-3` | medium | Needs verification | CHECK INTEGRITY reports a pending targeted-rule replacement as corruption of the accepted rule |
| `schema-mutation-4` | medium | Needs verification | Verify phase restarts on any write to any entity in the store, so activations can starve while unique activations block inserts |
| `schema-mutation-5` | medium | Needs verification | Check activations that depend on more than 32 fields can never persist findings: VALIDATE fails with a corruption error |
| `schema-store-3` | medium | Needs verification | SchemaStore::init_journaled uses StableBTreeMap::init, which silently reinitializes a non-empty schema allocation with a bad header |
| `schema-store-4` | medium | Needs verification | Heap-store identity allocation fully decodes, re-verifies and rewrites the whole live checkpoint (bundle up to 16 MiB plus identity inventory) on every write |
| `session-sql-2` | medium | Needs verification | SQL compiled-command cache is bounded by entry count only and keeps full SQL text twice plus literal payloads, so heap can be exhausted |
| `session-sql-3` | medium | Needs verification | Resumable job gets permanently stuck in Active when one matching row deterministically fails write admission (e.g. a row-local CHECK) |
| `session-write-3` | medium | Needs verification | Replacing an existing row regenerates non-PK generated Ulid/Timestamp fields and turns identical replaces into logical changes |
| `sql-parser-1` | medium | Needs verification | ORDER BY arithmetic sub-parser recurses on parentheses with no depth guard (stack overflow instead of ExpressionDepthLimit) |
| `sql-parser-2` | medium | Needs verification | Scope normalization rewrites a record path whose inner segment matches the entity or alias name to a top-level field (wrong column in SELECT/UPDATE/DELETE) |
| `sql-parser-3` | medium | Needs verification | Numbers in scientific or hex notation are silently split into a number plus an implicit projection alias |
| `sql-parser-4` | medium | Needs verification | DDL accepts dotted multi-segment column names in ADD COLUMN / RENAME COLUMN, persisting top-level fields that SQL cannot address consistently |
| `sql-parser-5` | medium | Needs verification | Filtered-index predicate identity is raw token text: IF NOT EXISTS and duplicate-contract detection break on formatting and after any RENAME COLUMN |
| `value-types-error-2` | medium | Needs verification | Numeric-widening compare/eq sends floats (and Nat128 values ≥ 2^127) through a lossy i128 Decimal; out-of-range values compare as None and rows silently drop out of WHERE filters |
| `value-types-error-6` | medium | Needs verification | Persisted LOWER/UPPER index expression keys depend on the Rust toolchain's Unicode tables, with no version pinning |
| `xc-performance-1` | medium | Needs verification | Every numeric ORDER BY / top-K / MIN-MAX comparison goes through Decimal, with u128 digit-string expansion (and float-to-text-to-parse for Float64) |
| `xc-performance-2` | medium | Needs verification | Bounded top-K window is O(N·K) and charges K+1 SortComparisons per row, so moderately large LIMIT/OFFSET without an index-backed order fails deterministically |
| `xc-performance-3` | medium | Needs verification | Validated full-row decode decodes each slot 2-3 times (validation results thrown away), against the row-contract rule |
| `xc-performance-5` | medium | Needs verification | Write amplification: each saved row is fully validated 4-6 times, re-encoded to canonical form at commit, and its bytes copied repeatedly |
| `xc-robustness-2` | medium | Needs verification | Scalar page kernel preallocates Vec::with_capacity(OFFSET+LIMIT+1) unclamped, so large OFFSET/LIMIT traps before any budget check |
| `xc-security-2` | medium | Needs verification | Startup watchdog replans deterministic migration-planning failures every round with no backoff, log or receipt |
| `canisters-testing-ci-10` | low | Needs verification | The SQL coverage manifest accepts `#[ignore]`d tests, and tests never run in CI, as satisfied evidence |
| `canisters-testing-ci-11` | low | Needs verification | The persisted-format version scan matches on names, so `*_POLICY_REVISION = 2` counters folded into a persisted identity slip through |
| `canisters-testing-ci-5` | low | Needs verification | The frozen canister endpoint policy checks only `icydb_*` exports; 'production' artifacts of test canisters still export unauthenticated trusted-write/SQL methods |
| `canisters-testing-ci-6` | low | Needs verification | CI downloads and runs PocketIC and actionlint with no checksum pinning, and the release-artifact job installs unpinned tools |
| `canisters-testing-ci-7` | low | Needs verification | The pre-commit hook re-stages only the root Cargo.toml, and CI's format gate skips the cargo-sort checks that the local fmt-check requires |
| `canisters-testing-ci-8` | low | Needs verification | TESTING.md's 'authoritative' taxonomy is stale and contradicts the repository |
| `canisters-testing-ci-9` | low | Needs verification | Contract tests assert source-file text, which TESTING.md prohibits and which can pass while the live constant has changed |
| `cli-10` | low | Needs verification | Statement splitter treats backslash as an escape inside strings, but the IcyDB lexer does not |
| `cli-11` | low | Needs verification | Substring error classification: any 'replica' error becomes 'local network not reachable', and other icp failures become 'not created' with stderr discarded |
| `cli-12` | low | Needs verification | Refresh silently downgrades from reinstall to upgrade when the status probe errors |
| `cli-13` | low | Needs verification | Dead string-matching recovery hint contradicts the 'don't match on error strings' rule |
| `cli-14` | low | Needs verification | NULL rendering is ambiguous (text 'null' shown as SQL NULL) and inconsistent between the query and UPDATE…RETURNING paths |
| `cli-15` | low | Needs verification | INSTALLING.md contradicts the code on the default environment and on refresh with a missing fixture method |
| `cli-16` | low | Needs verification | Diagnostic artifact lookups use binary search, but validation never enforces sorted IDs |
| `cli-17` | low | Needs verification | Shell history I/O errors are fatal and happen before the statement runs |
| `cli-18` | low | Needs verification | Abort loop has no iteration bound or progress guard |
| `cli-19` | low | Needs verification | Live-schema resolution failure in `diagnostic --canister` aborts the command instead of falling back |
| `cli-20` | low | Needs verification | CLI parsing tests depend on the ambient ICP_ENVIRONMENT variable |
| `cli-6` | low | Needs verification | Interactive line normalization rewrites the content of multi-line string literals before execution |
| `cli-8` | low | Needs verification | `canister upgrade` installs a wasm from a hard-coded, CWD-relative, environment-independent path instead of what `icp build` produced |
| `commit-3` | low | Needs verification | Marker encoding never checks the bytes it wrote against the precomputed envelope lengths |
| `commit-4` | low | Needs verification | CommitGuard has no Drop safety net, so the rule 'retained marker => wake-up registered' holds only by caller discipline, and one early return already bypasses it |
| `commit-6` | low | Needs verification | The schema-fingerprint check in commit preparation is tautological, and replay never validates recorded fingerprints despite the contracts |
| `data-12` | low | Needs verification | NULL for a nullable List/Set<Relation> has no working encode or decode path |
| `data-5` | low | Needs verification | Rows larger than 4 MiB are rejected with an Internal/Serialize error instead of a limit error |
| `data-6` | low | Needs verification | By-kind decode accepts non-canonical or out-of-contract persisted bytes without failing |
| `data-7` | low | Needs verification | Historical-fill validation uses a decoder that cannot read canonical-wire enum/composite payloads |
| `data-8` | low | Needs verification | Fresh-boot convergence writes store memory before later fallible steps; a failure leaves memory that can never be admitted |
| `data-9` | low | Needs verification | Database-format observation and admission disagree on what counts as uninitialized control memory |
| `executor-aggregate-10` | low | Needs verification | Grouped limit failures are surfaced as generic execution-budget errors that are not distinguishable from scalar DISTINCT exhaustion |
| `executor-aggregate-9` | low | Needs verification | Ordered DISTINCT group seek hard-codes MissingRowPolicy::Error regardless of the query's policy |
| `executor-core-1` | low | Needs verification | Instruction ceilings count from each tracker's own start, so every mutation-job advance or maintenance allowance gets a fresh 30B regardless of work already done in the message |
| `executor-core-2` | low | Needs verification | Read executions never set an instruction baseline at start, so work before the 64th charge (or the first charge of 1MiB or more) is counted by neither the execution nor the request scope |
| `executor-core-3` | low | Needs verification | Checks inside the commit window that can fail after marker publication return Err to the caller while the marker is kept, so the reported-failed write is later committed by recovery |
| `executor-core-4` | low | Needs verification | The commit path marks every touched store's index Ready after each write, which is a latent visibility hazard and an Err that can follow a durable commit |
| `executor-core-5` | low | Needs verification | Instruction-budget and production commit-apply behavior are structurally untestable in the native test suite |
| `executor-stream-10` | low | Needs verification | Branch-ordered prefix family skips construction charging and child-count validation that the merged path enforces |
| `executor-stream-6` | low | Needs verification | Lookahead row is scanned and then discarded; an exactly-full final page still returns a non-null continuation (contract mismatch) |
| `executor-stream-7` | low | Needs verification | Residual-retry stop condition `post_access_rows > keep_count` is unreachable, so full pages trigger redundant widened re-scans |
| `executor-stream-8` | low | Needs verification | PrimaryRangeKeyStream physical seek drops buffered and remaining keys when the target lies inside the loaded chunk (latent) |
| `executor-stream-9` | low | Needs verification | Effective offset uses logical-boundary presence while keep caps use has_progress (latent offset skip/over-skip) |
| `facade-1` | low | Needs verification | Participant lifecycle mode silently compiles when the app forgets the participant call; database stays Recovering indefinitely |
| `facade-3` | low | Needs verification | Grant and admission misconfiguration surfaces as an opaque E23, and the re-exported ic_memory_range! defaults to a Reserved (non-granting) range |
| `facade-4` | low | Needs verification | Production assert! in RequestExecutionFuture::poll can trap when a started future is polled under another request root |
| `facade-5` | low | Needs verification | SQL reply-size guard uses the 3 MiB non-replicated limit even when icydb_query runs in replicated mode |
| `facade-6` | low | Needs verification | crates/icydb/README.md contradicts the facade's model re-export and the workspace README |
| `facade-7` | low | Needs verification | Typed mixed batch repeats binding issuance per item and deep-clones every binding at execute (unmeasured instruction cost) |
| `index-access-10` | low | Needs verification | Raw index-key comparator falls back to raw byte order for undecodable keys, so the stable B-tree order is not transitive |
| `index-access-11` | low | Needs verification | Index-integrity audit doc still describes raw-byte lexicographic ordering; the implementation uses a decode-then-compare comparator |
| `index-access-8` | low | Needs verification | exact_child_prefixes_for_parent_set walks every multi-component prefix of the index on each call, without a budget |
| `index-access-9` | low | Needs verification | visit_raw_entries_in_merged_ranges calls BTreeMap::range without the empty-envelope guard |
| `integrity-relations-2` | low | Needs verification | Abandoned or expired Deep integrity jobs are never retired and permanently use per-owner and global progress capacity, including the slots that gate SQL mutation and resumable jobs |
| `integrity-relations-7` | low | Needs verification | Reverse keys from a heap (LiveSource) source into a journaled target exist only in the target's unjournaled live overlay; an online recovery pass resets them while the heap source rows survive |
| `integrity-relations-8` | low | Needs verification | A retention-page failure turns an already-persisted integrity result into an error; a key/payload job-id mismatch makes every integrity request fail permanently |
| `integrity-relations-9` | low | Needs verification | Storage report per-entity memory_bytes uses the maximum key size while store-level memory_bytes uses actual key bytes, so the two totals disagree |
| `journal-jobs-5` | low | Needs verification | append_batch_bytes has fallible steps after its first stable write; any Err leaves the tail in a state that identical replay cannot fix |
| `journal-jobs-6` | low | Needs verification | Prefix-repair branch in append is unreachable in production and loosens the sequence and commit-order checks |
| `journal-jobs-7` | low | Needs verification | Retirement preflight reads and reassembles the entire next batch (up to 16 MiB) just to get its header |
| `model-macros-10` | low | Needs verification | Derived index names can collide (slug normalization, predicate excluded), and the macro does not detect it |
| `model-macros-11` | low | Needs verification | `#[icydb::test]` puts the whole test under one request root, so per-message `with_request_execution` calls inside it share one aggregate budget |
| `model-macros-3` | low | Needs verification | Redundant-prefix index rejection is backwards for unique indexes and ignores filter predicates |
| `model-macros-4` | low | Needs verification | Runtime-adapter detection treats dev-dependencies and all target-specific dependencies as a usable `icydb` dependency |
| `model-macros-5` | low | Needs verification | Generated code uses unqualified prelude names and fixed derived item names, so common user aliases and names break compilation |
| `model-macros-6` | low | Needs verification | Validation and lowering disagree on predicate literals, and default parsing can panic, so proc-macro panics replace spanned errors |
| `model-macros-7` | low | Needs verification | Crate-path rewriting replaces every bare `icydb` / `icydb_model` identifier in the output, including user-authored names and path segments |
| `model-macros-8` | low | Needs verification | User attributes, doc comments, struct fields and generics on the annotated item are silently discarded |
| `model-macros-9` | low | Needs verification | Any string argument containing "::" is reinterpreted as a Rust path, so text defaults and args containing "::" cannot be written |
| `model-schema-crates-10` | low | Needs verification | NatBig/IntBig operators panic (subtraction underflow, division by zero); Decimal instead saturates and returns zero |
| `model-schema-crates-11` | low | Needs verification | Blob literal text means raw UTF-8 bytes for defaults but hex for migration fill literals |
| `model-schema-crates-12` | low | Needs verification | The persisted generated submission-key identity uses a 'v2' domain tag, contrary to the version-1 policy |
| `model-schema-crates-8` | low | Needs verification | Migration closure validation recurses without a depth bound and has superlinear cost on a 'bounded' public proposal |
| `model-schema-crates-9` | low | Needs verification | EntityFragment does not validate primary-key field shape (nullable/list/named/float) |
| `query-expr-10` | low | Needs verification | COALESCE and NULLIF evaluate every argument eagerly, unlike SQL's defined CASE equivalence |
| `query-expr-8` | low | Needs verification | The affine rewrite in grouped/UPDATE WHERE turns an integer literal into a Decimal, which switches Eq/Ne coercion to Strict and breaks non-Int64 field kinds |
| `query-intent-5` | low | Needs verification | The continuation signature and scalar token identify ORDER BY by display label, so order expressions differing only in literal type accept each other's cursors |
| `query-intent-6` | low | Needs verification | The READ_ADMISSION surface inventory omits public read surfaces that bypass QueryAdmissionPolicy |
| `query-plan-4` | low | Needs verification | Primary-key predicate strip drops the entire filter expression without checking predicate coverage |
| `query-plan-5` | low | Needs verification | index_covering_existing_rows_terminal_eligible returns true when predicate is None without checking for a residual filter expression |
| `r2-candid-stability-2` | low | Needs verification | Only the migration ABI is gated: other generated endpoint responses and public DTOs have no Candid golden or subtype check, and the CLI decodes strictly with no version handshake |
| `r2-candid-stability-3` | low | Needs verification | Recursive public input DTOs (FilterExpr, PublicValue/InputValue) have no decode-time depth bound on the IC, and FilterExpr's error wrapper re-formats the whole message at every level |
| `r2-persisted-sql-text-2` | low | Needs verification | Stored filtered-index semantics depend on current parser code, not on the stored text, so parser fixes silently invalidate already-built index contents |
| `r2-persisted-sql-text-3` | low | Needs verification | Migration planner rejects metadata-only enum-variant renames whenever a generated filtered-index predicate names the variant |
| `r2-query-call-caches-3` | low | Needs verification | Heap-store schema publications never wake the watchdog, so the database-wide runtime root stays cold for all query traffic until an unrelated update call |
| `r2-recursive-bounds-3` | low | Needs verification | Source check expressions reach bind_expression with nesting up to ~1020 levels; the binder recurses (and clones) before the 32-level check-tree bound is applied |
| `r2-recursive-bounds-4` | low | Needs verification | Generated-predicate depth bound in the model macros is looser than the accepted check-tree bound, so macro-accepted predicates are rejected at runtime schema acceptance |
| `r2-recursive-bounds-5` | low | Needs verification | Rendered partial-index predicate SQL can exceed the SQL predicate parser's source-depth budget even though the bound check tree is within its own limits |
| `schema-catalogs-4` | low | Needs verification | Catalog decoders accept states that construction rejects (zero-variant enums; invalid kind shapes inside enum payloads, tuples and newtypes) |
| `schema-catalogs-5` | low | Needs verification | DESCRIBE misreports nested structure: leaf names shown without their parent path, nested relation cardinality always 'single', partial unique shown as UNI |
| `schema-catalogs-6` | low | Needs verification | Store registry accepts registrations that share some, but not all, of their data/index/schema stores, or a journal store |
| `schema-catalogs-7` | low | Needs verification | IndexName derivation can give different field lists the same name; db::identity module docs are stale |
| `schema-migration-6` | low | Needs verification | Non-transform-slot ValueContract failures produce a finding with target FieldId(0), which try_new_transform rejects as store_invariant |
| `schema-migration-7` | low | Needs verification | Checked cast into a nullable target rejects every NULL source (NullSource); no transform can change a nullable column's type while preserving NULLs |
| `schema-migration-8` | low | Needs verification | Public migration findings drop the persisted source field, target field and transform reason |
| `schema-migration-9` | low | Needs verification | Pre-rewrite abort scans every index entry of each affected store (including unrelated entities) at 512 entries per call while the database stays gated |
| `schema-mutation-6` | low | Needs verification | A stale receipt acknowledgement (retried VALIDATE ... AFTER n) is reported as store corruption |
| `schema-mutation-7` | low | Needs verification | User-authored constraint names can take engine-reserved `__icydb_` names and block later NOT NULL additions |
| `schema-mutation-8` | low | Needs verification | Plain ADD CHECK is capped at one 256-row page but reports the SourceRows budget; heap-store VALIDATE over 256 rows is permanently unsupported |
| `schema-mutation-9` | low | Needs verification | RENAME COLUMN and SET DEFAULT derivations drop candidate owners but keep live activations, so they fail with misleading internal errors while an activation is pending |
| `schema-store-5` | low | Needs verification | Every accepted-bundle borrow re-scans and CRC-decodes the full identity-state inventory (retired records never shrink) |
| `schema-store-6` | low | Needs verification | Persisted-format inventory omits cardinality records and the ICYDBCAT frame, and misattributes identity-state storage |
| `schema-store-7` | low | Needs verification | Per-entity namespace-0 snapshot copies and their ICYDBCAT fingerprint header are written on every publication but only serve an orphan journal record kind |
| `schema-store-8` | low | Needs verification | Snapshot decode silently canonicalizes relation order instead of failing closed on noncanonical bytes |
| `session-sql-10` | low | Needs verification | Trusted mutation surface applies an undocumented 100-row / 1 MiB cap to INSERT but leaves DELETE unbounded; INSERT…SELECT materializes the whole source before the cap check |
| `session-sql-11` | low | Needs verification | "Public" UPDATE/DELETE policies only constrain statement shape; target selection runs as TrustedRead with no read admission or scan budget |
| `session-sql-4` | low | Needs verification | Hidden public SQL DELETE entry points skip the shared prepare/normalize phase, so the policy proof and the executed query can disagree |
| `session-sql-5` | low | Needs verification | SQL_SUBSET still says placeholders are unsupported, but the trusted query surface implements typed WHERE bindings |
| `session-sql-6` | low | Needs verification | Targetless `DROP INDEX name` is documented as supported but always rejected by the only DDL entry point |
| `session-sql-7` | low | Needs verification | Unknown column in UPDATE SET or INSERT column list is reported as an internal executor invariant |
| `session-sql-8` | low | Needs verification | Resumable Verify cannot finish on multi-page entities that keep receiving writes, and each drift restarts a full Forward sweep |
| `session-sql-9` | low | Needs verification | Positional INSERT without a column list uses an undocumented width heuristic that can map values to unexpected columns |
| `session-write-1` | low | Needs verification | Resumable update stamps UpdatedAt with a fresh clock reading on every forward advance, not the frozen continuation timestamp |
| `session-write-2` | low | Needs verification | Structural lane rejects explicit Default on database-owned insert fields (Identity PK, generated, CreatedAt/UpdatedAt) that SQL and the contract admit |
| `session-write-4` | low | Needs verification | Replace on a missing key lets callers author a database-generated Ulid/Timestamp primary key |
| `sql-parser-10` | low | Needs verification | INSERT table alias is parsed and discarded; alias-qualified column and RETURNING references are rejected, unlike UPDATE/DELETE |
| `sql-parser-11` | low | Needs verification | The token cursor's move-out helpers still clone every string, blob and identifier payload for error reporting |
| `sql-parser-6` | low | Needs verification | Flat AND/OR chains of more than ~127 terms are rejected as ExpressionDepthLimit, contradicting READ_ADMISSION; parentheses count double toward the depth limit |
| `sql-parser-7` | low | Needs verification | SQL_SUBSET.md has drifted from the implemented grammar (required clauses undocumented, extra shapes accepted) |
| `sql-parser-8` | low | Needs verification | Globally reserved keywords with no quoting escape make same-named fields and entities unaddressable |
| `sql-parser-9` | low | Needs verification | Integer literals above u64 are typed as Decimal (i128 mantissa); wide-int values are unrepresentable or rejected, and render_scalar_sql_value does not reliably round-trip |
| `value-types-error-7` | low | Needs verification | Identity-projection docs claim 'non-reversible' and 'correlation avoidance', but the hash is unkeyed over enumerable keys and ignores the entity |
| `value-types-error-8` | low | Needs verification | canonical_value_compare is not a total order across mixed numeric variants, which can break sort_by in ORDER BY |
| `value-types-error-9` | low | Needs verification | PublicValue::try_into_runtime_non_enum builds Value::Map without normalization or validation |
| `xc-architecture-10` | low | Needs verification | Oversized modules are mostly inline tests; the mutation coordinator sits in the session layer despite that module's own boundary header |
| `xc-architecture-2` | low | Needs verification | Migration planner relabels filtered-index predicates one rename at a time, so chained or swapped field renames compute a wrong expected predicate |
| `xc-architecture-3` | low | Needs verification | Resource-budget vocabulary is owned by the query/executor/session layers, inverting the module graph for index, access, schema and codec |
| `xc-architecture-4` | low | Needs verification | The catalog-native schema mutation layer depends on SQL-frontend DTOs, and some mutation semantics are SQL-owned |
| `xc-architecture-5` | low | Needs verification | cfg(test) forks production semantics: test-only AST/runtime variants and production-only recovery fast paths that unit tests never exercise |
| `xc-architecture-7` | low | Needs verification | IndexStore::clear is a fully public, uncalled, non-durable mutator exposed to generated/user code |
| `xc-architecture-9` | low | Needs verification | Index authoring limits are duplicated in the model macros and have drifted from the real bounds |
| `xc-contracts-2` | low | Needs verification | 1.0-TODO marks CLI SQL INSERT/DELETE complete, but the CLI routes them to the read-only `icydb_query` query method, which always rejects them |
| `xc-contracts-5` | low | Needs verification | Recovery traps on preflight/apply contradictions; a deterministic contradiction wedges startup with no failure receipt |
| `xc-contracts-7` | low | Needs verification | PERSISTED_FORMAT_POLICY says the current line adds no checksum bytes, while many persisted envelopes carry checksums |
| `xc-contracts-8` | low | Needs verification | The inventory says stale secondary-index entries are repaired at startup; no such repair exists, and the persisted `Missing` witness value is never written |
| `xc-contracts-9` | low | Needs verification | 1.0-FEATURES says NULL sorts before non-null values, but DESC reverses the comparator so NULL sorts last |
| `xc-docs-api-10` | low | Needs verification | The canonical INSTALLING example publishes metrics to anonymous callers without a caveat, exposing entity paths and per-entity instruction usage |
| `xc-docs-api-11` | low | Needs verification | The durability operator guide still uses pre-0.258 'memory ID' guidance that contradicts the logical-key model |
| `xc-docs-api-12` | low | Needs verification | Documented public surfaces are #[doc(hidden)]: the `icydb::types` module (Id, Ulid, ...) and `TypedEntityBinding::take_row_value` |
| `xc-docs-api-13` | low | Needs verification | icydb-model Store allocation builders panic for heap stores without `# Panics` docs |
| `xc-docs-api-2` | low | Needs verification | `.limit()` means a total-traversal cap for scalar reads but a page size (with a continuing cursor) for grouped reads; rustdoc describes neither |
| `xc-docs-api-5` | low | Needs verification | schema-authoring guide's 'complete starting declaration' link points to a README anchor that does not exist |
| `xc-docs-api-7` | low | Needs verification | SECURITY.md misdescribes the pre-commit hook and `make test` PocketIC download behavior, and has no vulnerability-reporting channel |
| `xc-docs-api-8` | low | Needs verification | The endpoints! declaration surface has constraints that user docs never state: SQL declaration name differs from the export, guarded syntax is order-sensitive, and fixtures need a hard-wired feature name |
| `xc-docs-api-9` | low | Needs verification | Typed-operation errors cannot be returned as the Candid `icydb::Error`, and the maintained endpoint template drops diagnostic facts that the diagnostics guide says must be preserved |
| `xc-performance-10` | low | Needs verification | Filtered raw-row lanes open and parse the same row twice |
| `xc-performance-11` | low | Needs verification | Opening a row reader has fixed O(field_count) setup: layout slot count recomputed, per-slot contract lookups, two allocations |
| `xc-performance-12` | low | Needs verification | Initial schema application checks store emptiness with a full O(N) count |
| `xc-performance-13` | low | Needs verification | Exact-key batch Candid-encodes every result row once just to measure its size |
| `xc-performance-4` | low | Needs verification | Mutation batch loop rebuilds and deep-clones the accepted row decode contract 4-5 times per item |
| `xc-performance-6` | low | Needs verification | Primary-range scans for full-row strategies walk keys, then do a separate point B-tree lookup per row; the single-pass path is rarely reachable |
| `xc-performance-7` | low | Needs verification | Journal publish reads and clones the full previous row only to learn whether it existed |
| `xc-performance-8` | low | Needs verification | Storage report scans each journaled data and index store three times |
| `xc-performance-9` | low | Needs verification | PrimaryRangeKeyStream allocates the raw key three times per scanned key |
| `xc-security-4` | low | Needs verification | Generated ULID primary keys on IC are fully predictable |
| `canisters-testing-ci-12` | info | Needs verification | CI invariant scanners have blind spots: rg errors are swallowed, and the no-panic scan misses several panic macros |
| `commit-7` | info | Needs verification | Production Gate-2 measurement and the marker-presence fast path have test-only replacements, so core unit tests never exercise them |
| `data-11` | info | Needs verification | Dead retired-slot machinery: writer and decoder would disagree on slot count if slot gaps ever appeared |
| `executor-aggregate-11` | info | Needs verification | Dedicated grouped COUNT(*) window selection sorts and heap-selects without charging sort budgets |
| `executor-core-6` | info | Needs verification | Verbose EXPLAIN always reports diag.p.order_pushdown=missing_model_context, even though the executor has accepted authority |
| `facade-8` | info | Needs verification | No compile-fail test for declaring a metrics endpoint without the icydb/metrics feature |
| `journal-jobs-10` | info | Needs verification | Application side effects inside compare_proof_and_advance are not atomic with progress persistence |
| `journal-jobs-8` | info | Needs verification | Batch validation repeats work: Identity-range check is O(ranges × records), records are validated twice, schema bundles are decoded several times |
| `journal-jobs-9` | info | Needs verification | Persisted-format inventory text for sequence-zero controls does not match the store implementation |
| `query-intent-7` | info | Needs verification | Explain gives no reliable admission or pushdown signal (typed order_pushdown is hard-coded) |
| `query-intent-8` | info | Needs verification | Production `.expect` in planner label rendering |
| `query-plan-6` | info | Needs verification | Secondary-index planning rejects NumericWiden equality even after normalization has canonicalized the literal to the field kind |
| `query-plan-7` | info | Needs verification | Branch-set constructor claims a proven PK-ascending suffix that the planner does not establish when ORDER BY is absent |
| `r2-candid-stability-4` | info | Needs verification | SQL RETURNING response-size check measures a hand-written one-variant copy of the response, not the delivered Result<SqlQueryResult, Error> envelope |
| `r2-query-call-caches-4` | info | Needs verification | Query calls pay cache-insertion work (retained-size walk, key and SQL clones, FIFO bookkeeping) whose result is always discarded |
| `schema-catalogs-8` | info | Needs verification | Per-entity commit and cache fingerprint does not cover the enum/composite catalogs |
| `schema-mutation-10` | info | Needs verification | Canonical check SQL rendering emits CARDINALITY(...) and LENGTH on nominal fields, which the SQL check binder cannot re-parse |
| `schema-store-9` | info | Needs verification | Production index `primary_key_slot_indices[0]` in the row-layout runtime contract |
| `session-write-6` | info | Needs verification | Normal structural writes run with no execution budget and are not charged to the request root |
| `session-write-7` | info | Needs verification | The insert-only dynamic batch has no result-size bound, so it can succeed natively but always fail on-chain |
| `session-write-8` | info | Needs verification | The request budget-bundle preflight does not aggregate a resource listed twice in one bundle |
| `value-types-error-10` | info | Needs verification | Value serde Deserialize recurses without a depth bound (no current untrusted decode path) |
| `value-types-error-11` | info | Needs verification | Fact-schema mismatch in with_diagnostic_facts turns the error into InvariantViolation instead of keeping its class |
| `xc-architecture-11` | info | Needs verification | The production panic CI gate scans only db/executor, not the recovery, commit and journal paths |
| `xc-architecture-8` | info | Needs verification | Persisted-format decoders still keep at least 7 private cursor readers alongside the shared ByteReader |
| `xc-contracts-10` | info | Needs verification | REF_INTEGRITY says relation diagnostics retain the constraint name; the code emits only numeric facts |
| `xc-performance-14` | info | Needs verification | Generic store visitors duplicate the full traversal and overlay-merge code for each visitor closure (wasm size, unmeasured) |
| `xc-performance-15` | info | Needs verification | Per-row budget and page-unit accounting repeats constant work |
| `xc-security-8` | info | Needs verification | Read guard gets no entity or statement context, so SQL authorization is all-entity |
