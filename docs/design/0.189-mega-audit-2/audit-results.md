• # IcyDB 0.187 Adversarial Audit

  ## 1. Executive Summary

  IcyDB 0.187.9 is not a SQLite-like embedded database. It is a Rust, Internet Computer-
  focused, schema-first stable-memory persistence and query runtime. Its durable mode
  uses commit markers plus marker-bound journal batches and recovery folding over IC
  stable memory, not POSIX files, fsync, WAL files, or file locks.

  Verdict: suitable for experimental or carefully bounded internal canister storage use,
  but not ready to be treated as SQLite-grade durable storage. The strongest parts are
  explicit contracts, catalog-native schema handling, and a credible marker/journal
  recovery design. The weakest parts are lack of true crash/failure-injection testing,
  no fuzzing of persisted formats, no full no-default test coverage, resource-exhaustion
  risks during recovery/index folding, and intentionally limited transaction/isolation
  semantics.

  No Critical data-corruption bug was proven in this audit. Several High and Medium
  risks remain before I would trust it with important production data.

  ## 2. Architecture and Promise Map

  IcyDB is closest to an embedded IC canister database/runtime over stable memory, not a
  POSIX file database.

  Architecture map:

   Area                Evidence
  ━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   Language/build      Rust workspace, version 0.187.9 in README.md:16.
  ──────────────────  ──────────────────────────────────────────────────────────────────
   Public facade       crates/icydb; core engine in crates/icydb-core.
  ──────────────────  ──────────────────────────────────────────────────────────────────
   Intended use        “schema-first persistence and query runtime for Internet
                       Computer canisters” in README.md:10.
  ──────────────────  ──────────────────────────────────────────────────────────────────
   Query model         Reduced single-entity SQL, not general relational SQL or joins,
                       documented at README.md:37.
  ──────────────────  ──────────────────────────────────────────────────────────────────
   Storage modes       journaled durable cached-stable mode and heap() volatile mode at
                       README.md:112.
  ──────────────────  ──────────────────────────────────────────────────────────────────
   Data store          Heap BTreeMap or journaled canonical StableBTreeMap plus live
                       heap/tombstone projection in crates/icydb-core/src/db/data/
                       store.rs:45.
  ──────────────────  ──────────────────────────────────────────────────────────────────
   Index store         Heap or journaled canonical StableBTreeMap plus materialized
                       live projection in crates/icydb-core/src/db/index/store.rs:133.
  ──────────────────  ──────────────────────────────────────────────────────────────────
   Commit protocol     Commit marker is persisted before applying commit-window effects
                       in crates/icydb-core/src/db/commit/guard.rs:146.
  ──────────────────  ──────────────────────────────────────────────────────────────────
   Journal protocol    Journaled row ops become marker-bound journal batches in crates/
                       icydb-core/src/db/executor/mutation/commit_window.rs:1225.
  ──────────────────  ──────────────────────────────────────────────────────────────────
   Recovery            Startup recovery loads marker, publishes marker-bound journal
                       batches, folds journal tails, rebuilds live projections/indexes,
                       validates, then clears marker in crates/icydb-core/src/db/
                       commit/recovery.rs:111.
  ──────────────────  ──────────────────────────────────────────────────────────────────
   Transactions        Explicitly not Postgres/SQLite transactions; no automatic
                       rollback of earlier writes when a canister update later returns
                       Err, documented at README.md:192.
  ──────────────────  ──────────────────────────────────────────────────────────────────
   Isolation           IC update calls are serialized, but SQL pagination is live-
                       state, not snapshot, documented at docs/contracts/
                       QUERY_CONTRACT.md:159.

  Promise map:

   Promise                           Evidence            Audit interpretation
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   Single IC update-call mutation    docs/contracts/     Strong but narrow promise.
   atomicity                         ATOMICITY.md:9
  ────────────────────────────────  ──────────────────  ────────────────────────────────
   No reliance on traps for          docs/contracts/     Good contract, but stable-
   normal correctness                ATOMICITY.md:39     memory allocation/trap
                                                         behavior still needs failure-
                                                         injection proof.
  ────────────────────────────────  ──────────────────  ────────────────────────────────
   Recovery before reads/writes      docs/contracts/     Implemented through recovered
                                     ATOMICITY.md:53     store access paths.
  ────────────────────────────────  ──────────────────  ────────────────────────────────
   No multi-message transactions     docs/contracts/     Important limitation versus
                                     ATOMICITY.md:202    SQLite.
  ────────────────────────────────  ──────────────────  ────────────────────────────────
   Heap store is not durable         README.md:124       Clear, but still a user
                                                         footgun.
  ────────────────────────────────  ──────────────────  ────────────────────────────────
   Cursor pagination is not          docs/contracts/     Correctly disclosed.
   snapshot isolation                QUERY_CONTRACT.m
                                     d:159

  ## 3. Build/Test/Tooling Results

   Command                                     Result
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   cargo fmt --all --check                     Passed.
  ──────────────────────────────────────────  ──────────────────────────────────────────
   cargo test --all --all-features             Passed. icydb-core: 4074 passed, 0
                                               failed, 4 ignored. Integration suites
                                               also passed.
  ──────────────────────────────────────────  ──────────────────────────────────────────
   cargo clippy --all-targets --all-           Passed.
   features -- -D warnings
  ──────────────────────────────────────────  ──────────────────────────────────────────
   make check-invariants                       Passed, including mutation atomicity,
                                               index-range spec, memory-id invariants,
                                               and production panic checks.
  ──────────────────────────────────────────  ──────────────────────────────────────────
   cargo audit                                 Passed for vulnerabilities. Reported
                                               allowed unmaintained advisories for
                                               backoff, instant, paste, and serde_cbor.
  ──────────────────────────────────────────  ──────────────────────────────────────────
   cargo check -p icydb --no-default-          Passed.
   features
  ──────────────────────────────────────────  ──────────────────────────────────────────
   cargo check -p icydb --no-default-          Passed.
   features --features sql
  ──────────────────────────────────────────  ──────────────────────────────────────────
   cargo check -p icydb-core --no-default-     Passed.
   features --features sql
  ──────────────────────────────────────────  ──────────────────────────────────────────
   cargo check -p icydb --no-default-          Passed.
   features --features diagnostics
  ──────────────────────────────────────────  ──────────────────────────────────────────
   cargo check -p icydb-core --no-default-     Passed.
   features --features diagnostics
  ──────────────────────────────────────────  ──────────────────────────────────────────
   cargo test --all --no-default-features      Failed to compile.
  ──────────────────────────────────────────  ──────────────────────────────────────────
   cargo test -p icydb-core --no-default-      Failed to compile.
   features
  ──────────────────────────────────────────  ──────────────────────────────────────────
   cargo miri --version                        Failed: Miri unavailable for the active
                                               toolchain.

  Representative no-default failures:

  - /home/adam/projects/icydb/canisters/audit/sql_perf/src/lib.rs references SQL/blob/
    diagnostics types unavailable under full workspace no-default tests.

  - crates/icydb-core/src/db/query/plan/mod.rs:139 re-exports a SQL-only symbol under
    #[cfg(any(test, feature = "sql"))], while the definition is only under #[cfg(feature
    = "sql")].

  - crates/icydb-core/src/db/schema/info.rs:8 imports SQL literal canonicalization under
    test cfg, but the re-export is gated by feature = "sql".

  Worktree note: I did not intentionally modify project files. At final check, local
  changes existed in /home/adam/projects/icydb/CHANGELOG.md, /home/adam/projects/icydb/
  crates/icydb-core/src/db/session/tests/sql_delete.rs, and untracked /home/adam/
  projects/icydb/docs/design/0.188-mutation-candidate-collector/.

  ## 4. SQLite-Style Best-Practices Comparison

   Area                      ACID semantics
   IcyDB evidence            Explicitly narrow atomicity, no transaction blocks, no
                             rollback on later Err
   SQLite-style expectation  Clear ACID or explicit non-ACID contract
   Risk                      Medium
   Recommendation            Keep disclaimers prominent; expose transaction-like API
                             only when semantics exist.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                      Atomic commit
   IcyDB evidence            Marker before apply, marker-bound journal batches
   SQLite-style expectation  Proven atomic commit with exhaustive crash tests
   Risk                      Medium
   Recommendation            Add failpoint/kill tests for every commit phase.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                      Crash recovery
   IcyDB evidence            Recovery folds marker/journal tails before access
   SQLite-style expectation  Deterministic, heavily tested recovery
   Risk                      High
   Recommendation            Add external crash harness and idempotence tests.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                      fsync/order
   IcyDB evidence            Not POSIX file-backed; IC stable memory
   SQLite-style expectation  Correct fsync/fdatasync and directory sync where relevant
   Risk                      N/A for IC, High if marketed as file DB
   Recommendation            Document non-POSIX storage boundary clearly.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                      Disk-full/I/O errors
   IcyDB evidence            Stable-memory operations largely infallible/trap-oriented
   SQLite-style expectation  Structured ENOSPC/EIO handling
   Risk                      Medium
   Recommendation            Add allocation/failure-injection model or document trap
                             boundary.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                      File locking
   IcyDB evidence            Not file-backed
   SQLite-style expectation  Cross-process locking if file DB
   Risk                      N/A
   Recommendation            State single-canister/runtime scope.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                      Isolation
   IcyDB evidence            Serialized IC updates, no snapshot pagination
   SQLite-style expectation  Defined isolation levels
   Risk                      Medium
   Recommendation            Add explicit snapshot/version API if needed.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                      Corruption detection
   IcyDB evidence            Magic/version/length/fingerprint checks for marker/journal
   SQLite-style expectation  Checksums, page validation, corruption taxonomy
   Risk                      Medium
   Recommendation            Add checksums/fuzzing for persisted bytes.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                      Format compatibility
   IcyDB evidence            Pre-1.0 hard-cut posture
   SQLite-style expectation  Documented file format and migration policy
   Risk                      Medium
   Recommendation            Publish persisted format docs before stability claim.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                      Fuzz testing
   IcyDB evidence            No fuzz harness found
   SQLite-style expectation  Persistent fuzzing of parsers/formats
   Risk                      High
   Recommendation            Add cargo fuzz or equivalent corpus tests.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                      Crash tests
   IcyDB evidence            Unit recovery tests, no kill/failpoint harness found
   SQLite-style expectation  Crash matrix across every write point
   Risk                      High
   Recommendation            Add child-process crash tests.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                      Error taxonomy
   IcyDB evidence            Strong logical error classes, little OS I/O taxonomy
   SQLite-style expectation  Distinguish corruption, I/O, full disk, locks
   Risk                      Medium
   Recommendation            Keep IC-specific errors, but model stable-memory failures.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                      Unsafe code
   IcyDB evidence            Limited unsafe decode/metrics callbacks
   SQLite-style expectation  Minimal unsafe, Miri/sanitizer coverage
   Risk                      Medium
   Recommendation            Add Miri where feasible and safety-focused tests.
  ──────────────────────────────────────────────────────────────────────────────────────
   Area                      CI coverage
   IcyDB evidence            Good all-features CI, missing full no-default tests
   SQLite-style expectation  Matrix across features/platforms
   Risk                      Medium
   Recommendation            Add failing no-default test jobs.

  ## 5. Correctness Findings

  F-001, High: Full no-default test configurations do not compile

  Evidence: cargo test --all --no-default-features and cargo test -p icydb-core --no-
  default-features fail. Representative cfg mismatch at crates/icydb-core/src/db/query/
  plan/mod.rs:139 and crates/icydb-core/src/db/schema/info.rs:8.

  Why it matters: feature combinations are part of the public support matrix. Untested
  cfg paths routinely hide correctness bugs.

  Reproduction: run cargo test -p icydb-core --no-default-features.

  Expected: either compile and run, or be explicitly unsupported and excluded.

  Actual: compile failure due SQL/test cfg leakage.

  Recommended fix: align #[cfg(test)] and #[cfg(feature = "sql")] gates, then add CI
  jobs for no-default tests or explicitly remove that support claim.

  Suggested regression test: CI job running cargo test -p icydb-core --no-default-
  features.

  Confidence: High.

  F-002, Medium: Strong relation validation in atomic batches does not see earlier
  staged rows

  Evidence: documented in docs/contracts/TRANSACTION_SEMANTICS.md:139.

  Why it matters: users expect one atomic batch to validate against its final staged
  state. This can reject logically valid parent+child inserts in the same batch or
  encourage unsafe ordering workarounds.

  Reproduction: create parent and child rows in the same atomic batch where child
  validation depends on parent visibility.

  Expected: either staged-state validation or explicit API naming that says batch
  validation uses pre-batch state.

  Actual: documented pre-batch validation.

  Recommended fix: implement staged relation validation or expose a dedicated API name/
  diagnostic for pre-state validation.

  Suggested regression test: parent+child same-batch relation validation.

  Confidence: High.

  ## 6. Durability and Crash-Safety Findings

  F-003, High: Crash-safety design is credible but not proven by SQLite-grade crash
  testing

  Evidence: commit marker and recovery logic are in crates/icydb-core/src/db/commit/
  guard.rs:146, crates/icydb-core/src/db/executor/mutation/commit_window.rs:1321, and
  crates/icydb-core/src/db/commit/recovery.rs:111. I found no child-process kill harness
  or failpoint matrix.

  Why it matters: recovery protocols fail in edge windows: marker set but batch append
  failed, fold watermark persisted but cleanup failed, index rebuild interrupted,
  allocation traps, and corrupted tail records.

  Scenario: kill process after marker persistence but before all marker-bound batches
  are published.

  Expected: recovery deterministically publishes/folds exactly once.

  Actual: code appears designed for this, but no adversarial crash harness proves it.

  Recommended fix: add failpoints before and after every durable write in marker,
  journal append, fold watermark, index fold, and marker clear.

  Suggested regression test: randomized kill/reopen model checking with committed-state
  oracle.

  Confidence: High for the test gap, Medium for runtime risk.

  F-004, Medium: Stable-memory allocation/trap behavior is not modeled as a structured
  commit failure

  Evidence: journal append uses StableBTreeMap::insert in crates/icydb-core/src/db/
  journal/store.rs:102. The atomicity contract says traps are catastrophic and not
  correctness mechanisms at docs/contracts/ATOMICITY.md:39.

  Why it matters: durable engines must define behavior under allocation failure, memory
  growth failure, and interrupted writes.

  Scenario: stable memory growth traps while appending a journal batch after marker
  persistence.

  Expected: either explicit structured error before mutation, or documented trap/retry
  recovery semantics with tests.

  Actual: not proven by failure injection.

  Recommended fix: introduce storage abstraction failpoints or allocation preflight for
  every durable write.

  Suggested regression test: simulated failure at marker set, journal append, fold
  watermark, index fold, and marker clear.

  Confidence: Medium.

  ## 7. Transaction and Isolation Findings

  F-005, Medium: API semantics are not database transactions despite atomic commit
  machinery

  Evidence: no transaction blocks, no rollback after later Err, and no multi-message
  commits are documented at docs/contracts/TRANSACTION_SEMANTICS.md:1 and docs/
  contracts/ATOMICITY.md:202.

  Why it matters: users comparing IcyDB to SQLite can accidentally build workflows
  assuming rollback or serializable transactions.

  Scenario: an update call writes row A, then returns Err after a later validation
  failure.

  Expected by SQLite users: whole transaction rolls back.

  Actual: earlier committed write remains, per documented contract.

  Recommended fix: avoid transaction-like naming unless a true transaction object
  exists; provide a staging transaction API if production users need this.

  Suggested regression test: canister update writes, returns Err, then read verifies
  documented persistence.

  Confidence: High.

  F-006, Medium: Pagination is explicitly live-state, not snapshot isolation

  Evidence: docs/contracts/QUERY_CONTRACT.md:159.

  Why it matters: cursor pagination over mutable datasets can miss or duplicate rows.

  Scenario: page 1 is read, then rows are inserted/deleted before page 2.

  Expected by many DB users: stable snapshot cursor.

  Actual: live-state continuation.

  Recommended fix: add snapshot/versioned pagination or make cursor type names/docs
  loudly indicate live continuation.

  Suggested regression test: mutate between pages and assert documented non-snapshot
  behavior.

  Confidence: High.

  ## 8. Concurrency Findings

  F-007, Medium: Concurrency model is IC-serialized, not general multi-thread/multi-
  process database concurrency

  Evidence: README states IC update calls are serialized at README.md:196. Db stores a
  &'static LocalKey<StoreRegistry> in crates/icydb-core/src/db/mod.rs:306.

  Why it matters: SQLite comparison implies multiple handles, processes, readers, and
  writers. IcyDB’s model is much narrower.

  Scenario: multiple host handles or test registries interact in one process.

  Expected: scoped recovery and locking semantics.

  Actual: the production model assumes canister serialization; general host concurrency
  guarantees are not established.

  Recommended fix: document exact Send/Sync, thread, process, and multi-handle
  guarantees in API docs.

  Suggested regression test: multiple handles to same registry/path equivalent,
  concurrent reads/writes under host harness.

  Confidence: Medium.

  F-008, Medium-Low: Process-global recovery fast path may be risky outside the intended
  single-canister domain

  Evidence: RECOVERED: OnceLock<()> is global in crates/icydb-core/src/db/commit/
  recovery.rs:52. Recovery also uses a commit-marker presence hint in crates/icydb-core/
  src/db/commit/store/mod.rs:301.

  Why it matters: process-global recovery state can be wrong if more than one logical
  database/registry exists in a process.

  Scenario: one database recovers and sets global recovered state; another database
  later opens with a pending marker but stale marker hint.

  Expected: recovery scoped to the storage domain.

  Actual: code appears partly scoped by registry, but the global fast path deserves
  proof.

  Recommended fix: scope recovery state by registry/memory-domain identity, or add tests
  proving multiple registries cannot skip recovery.

  Suggested regression test: two independent registries in one process, pending marker
  in the second after first recovery completes.

  Confidence: Medium-Low.

  ## 9. On-Disk Format and Recovery Findings

  IcyDB does not have a conventional on-disk file format. Persisted structures are
  stable-memory structures plus encoded commit markers and journal batches.

  Persistence risk matrix:

   Operation       Failure point       Expected          Actual code       Risk
                                       recovery          behavior
  ━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━
   Begin commit    Before marker       No durable        Marker set is     Low
   marker          write               mutation          guarded and
                                                         only set when
                                                         empty
  ──────────────  ──────────────────  ────────────────  ────────────────  ──────────────
   Begin commit    After marker        Replay marker/    Recovery loads    Medium until
   marker          write, before       journal           marker before     crash-tested
                   apply                                 access
  ──────────────  ──────────────────  ────────────────  ────────────────  ──────────────
   Publish         Marker written,     Marker should     Recovery          Medium
   marker-bound    batch append        republish         publishes
   journal         interrupted         missing           marker-bound
   batches                             batches           batches
                                                         idempotently
                                                         at crates/
                                                         icydb-core/
                                                         src/db/commit/
                                                         recovery.rs:16
                                                         7
  ──────────────  ──────────────────  ────────────────  ────────────────  ──────────────
   Fold journal    Before watermark    Refold            Fold watermark    Medium
   tail                                idempotently      applied after
                                                         records at
                                                         crates/icydb-
                                                         core/src/db/
                                                         commit/
                                                         recovery.rs:20
                                                         5
  ──────────────  ──────────────────  ────────────────  ────────────────  ──────────────
   Fold journal    After watermark,    Ignore or         Cleanup clears    Medium
   tail            before cleanup      clear old tail    folded batches
                                                         at crates/
                                                         icydb-core/
                                                         src/db/
                                                         journal/
                                                         store.rs:183
  ──────────────  ──────────────────  ────────────────  ────────────────  ──────────────
   Rebuild/fold    Interrupted         Derived           Recovery          Medium
   indexes         during rebuild      indexes           rebuilds
                                       rebuilt from      secondary
                                       rows              indexes before
                                                         marker clear
  ──────────────  ──────────────────  ────────────────  ────────────────  ──────────────
   Clear marker    Before clear        Recovery          Marker cleared    Low
                   completes           repeats           after
                                                         validation
  ──────────────  ──────────────────  ────────────────  ────────────────  ──────────────
   Corrupt         Decode malformed    Structured        Codec             Medium
   journal         batch               corruption        validates
   bytes                               error             magic/version/
                                                         length/count
  ──────────────  ──────────────────  ────────────────  ────────────────  ──────────────
   Heap store      Upgrade/reinit/     No recovery       Heap mode is      Low if
   write           crash               promised          volatile          documented,
                                                                           High if
                                                                           misused

  F-009, Medium: Raw journal batch storage is unbounded before decoder size checks

  Evidence: RawJournalBatch(Vec<u8>) uses Bound::Unbounded at crates/icydb-core/src/db/
  journal/codec.rs:209, while decode later enforces MAX_JOURNAL_BATCH_BYTES.

  Why it matters: a malformed stable-memory value can force allocation of a very large
  Vec<u8> before the codec rejects it.

  Scenario: corrupt or hostile stable memory contains an oversized journal value.

  Expected: bounded read or early rejection before allocation.

  Actual: stable structure may materialize the unbounded value first.

  Recommended fix: make the stable value bound explicit where possible, or store chunks/
  pages with bounded values.

  Suggested regression test: corrupted oversized journal value fails without OOM/panic.

  Confidence: High.

  F-010, Medium: No checksum on commit marker or journal envelope

  Evidence: marker and journal codecs validate magic/version/length in crates/icydb-
  core/src/db/commit/store/control_slot.rs:72 and crates/icydb-core/src/db/journal/
  codec.rs:252, but no checksum was found.

  Why it matters: length and magic detect many format errors, not all torn/corrupt-byte
  cases.

  Scenario: a bit flip changes a payload byte but preserves lengths and decode validity.

  Expected: corruption detected.

  Actual: may decode to wrong logical content unless schema/index validation catches it.

  Recommended fix: add CRC32C or stronger checksum to marker and journal envelopes.

  Suggested regression test: flip each byte in marker/journal payload and assert
  corruption or safe recovery.

  Confidence: Medium.

  ## 10. Error-Handling Findings

  F-011, Medium: Error taxonomy is strong for logical/storage corruption but weak for
  low-level durability failures

  Evidence: error taxonomy is documented in crates/icydb-core/src/error/mod.rs:53, with
  classes such as Corruption, IncompatiblePersistedFormat, Internal, and Conflict at
  crates/icydb-core/src/error/mod.rs:1746. There are no POSIX-style I/O, ENOSPC, or
  permission variants, which is understandable for IC stable memory but limits
  portability.

  Why it matters: mature storage engines distinguish corruption, unavailable storage,
  full storage, retryable failure, and programmer error.

  Scenario: stable-memory growth fails during commit.

  Expected: specific durable-write failure class or documented trap semantics.

  Actual: not clearly represented as structured error.

  Recommended fix: add explicit storage-resource failure classes if the stable-memory
  layer can expose them.

  Suggested regression test: injected memory-growth failure maps to a stable error.

  Confidence: Medium.

  F-012, Low: Public convenience API can panic on catalog read failure

  Evidence: show_entities uses .expect("session invariant") in crates/icydb-core/src/db/
  session/mod.rs:619, with try_show_entities available.

  Why it matters: library APIs should generally return structured errors when persisted
  state may be corrupt.

  Scenario: corrupted catalog causes show_entities to panic.

  Expected: user-facing API returns corruption error.

  Actual: convenience API panics.

  Recommended fix: keep try_show_entities as primary in examples; consider deprecating
  panic convenience for production paths.

  Suggested regression test: corrupt catalog read through both APIs.

  Confidence: High.

  ## 11. Security and Hostile-Input Findings

  F-013, Medium: Persisted-format hostile-input fuzzing is missing

  Evidence: no fuzz directory or cargo fuzz harness was found. Persisted decode paths
  include marker, journal, index key/envelope, structural field decoding, and raw rows.

  Why it matters: malformed database bytes should never panic, OOM, loop forever, or
  silently produce wrong rows.

  Scenario: journal batch with valid magic/version but adversarial record lengths and
  schema fingerprints.

  Expected: structured corruption or incompatible-format error.

  Actual: many checks exist, but no persistent fuzz proof.

  Recommended fix: add fuzz targets for commit marker, journal batch, index key/
  envelope, raw row decode, and schema snapshots.

  Suggested regression test: fuzz corpus plus deterministic corrupt/truncate/flip tests.

  Confidence: High.

  F-014, Medium: Unsafe decode callbacks need Miri/sanitizer coverage

  Evidence: unsafe callback/context casts exist in crates/icydb-core/src/db/data/
  structural_field/binary.rs:144, crates/icydb-core/src/db/data/structural_field/
  accepted.rs:317, and metrics sink lifetime erasure in crates/icydb-core/src/metrics/
  sink.rs:555.

  Why it matters: unsafe decode paths touching persisted input deserve extra
  verification.

  Scenario: malformed nested structural data drives callback order or state assumptions.

  Expected: no UB, structured error.

  Actual: safety comments exist, but Miri was unavailable and CI coverage was not found.

  Recommended fix: add Miri job where feasible and targeted tests for malformed nested
  structural data.

  Suggested regression test: structural decode fuzzing under Miri/nightly.

  Confidence: Medium.

  ## 12. API Design Findings

  F-015, Medium: Durable and volatile stores share similar write ergonomics

  Evidence: durable journaled and volatile heap() modes are documented together at
  README.md:112. Heap mode is explicitly “never durable” at README.md:124.

  Why it matters: users can prototype with heap mode, deploy accidentally, and lose data
  across upgrade/reinit.

  Scenario: application uses heap() in production because tests pass.

  Expected: strong compile-time or runtime warning for non-test volatile mode.

  Actual: documented but easy to misuse.

  Recommended fix: gate heap persistence behind an explicit volatile naming/API or
  diagnostic warning in production examples.

  Suggested regression test: docs/examples lint ensuring durable examples use journaled.

  Confidence: High.

  F-016, Medium: Non-atomic batch APIs intentionally preserve partial commits

  Evidence: non-atomic helpers loop per row and preserve prefix writes in crates/icydb-
  core/src/db/executor/mutation/save/batch.rs:55. Tests verify this at crates/icydb-
  core/src/db/executor/tests/mutation_save.rs:1871.

  Why it matters: the behavior is documented, but the risk is severe if callers use the
  wrong helper.

  Scenario: import job uses non-atomic batch and stops after first validation error.

  Expected: either all rows imported or none.

  Actual: prefix is committed.

  Recommended fix: keep non-atomic API names loud; add examples showing recovery/
  compensation.

  Suggested regression test: API-doc examples for atomic versus non-atomic failure.

  Confidence: High.

  ## 13. Test-Suite Assessment

  Existing strengths:

   Test class                           Evidence
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   Unit/integration tests               Large all-features suite passed.
  ───────────────────────────────────  ─────────────────────────────────────────────────
   Invariant checks                     make check-invariants passed.
  ───────────────────────────────────  ─────────────────────────────────────────────────
   Property tests                       proptest used in decimal, index key/envelope,
                                        and pagination components.
  ───────────────────────────────────  ─────────────────────────────────────────────────
   Recovery unit tests                  Recovery and commit modules have direct tests.
  ───────────────────────────────────  ─────────────────────────────────────────────────
   SQL/canister tests                   Integration suites passed under all-features.
  ───────────────────────────────────  ─────────────────────────────────────────────────
   Documentation of testing taxonomy    TESTING.md:13.

  Critical gaps:

   Gap                                         Risk
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   No full no-default test pass                Feature cfg regressions already present.
  ──────────────────────────────────────────  ──────────────────────────────────────────
   No external crash/kill harness              Commit/recovery protocol not SQLite-
                                               grade proven.
  ──────────────────────────────────────────  ──────────────────────────────────────────
   No storage failure injection                ENOSPC/memory-growth/trap windows not
                                               modeled.
  ──────────────────────────────────────────  ──────────────────────────────────────────
   No persisted-format fuzzing                 Hostile/corrupt bytes may panic or OOM.
  ──────────────────────────────────────────  ──────────────────────────────────────────
   No long-running model-based DB oracle       BTree/index/query interactions need
   test found                                  randomized checking.
  ──────────────────────────────────────────  ──────────────────────────────────────────
   No multi-handle/concurrency stress suite    Thread/process boundaries remain
   found                                       unclear.
  ──────────────────────────────────────────  ──────────────────────────────────────────
   No compatibility/migration suite across     Pre-1.0 may allow hard cuts, but
   old persisted versions found                upgrades need explicit validation.

  Highest ROI additions:

  1. Failpoint crash-recovery matrix.
  2. Model-based random operation test with reopen.
  3. Persisted-format fuzzing.
  4. No-default CI tests.
  5. Oversized/corrupt stable-memory value tests.

  ## 14. Performance and Scalability Assessment

  Performance risks:

   Area                              Evidence            Risk
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   Index recovery/fold               crates/icydb-       O(index size) memory/time
   materializes whole index          core/src/db/        during recovery.
                                     index/
                                     store.rs:427 and
                                     snapshot clone
                                     at crates/icydb-
                                     core/src/db/
                                     index/
                                     store.rs:548
  ────────────────────────────────  ──────────────────  ────────────────────────────────
   Pagination can full-evaluate      docs/contracts/     Large result sets can be
   candidates                        QUERY_CONTRACT.m    expensive despite cursors.
                                     d:177
  ────────────────────────────────  ──────────────────  ────────────────────────────────
   Bench coverage is partial         docs/audits/        Benchmarks do not fully
                                     reports/2026-       characterize large DB
                                     03/2026-03-31/      behavior.
                                     perf-
                                     audit.md:125
  ────────────────────────────────  ──────────────────  ────────────────────────────────
   Deletes/grouped paths heavy       docs/audits/        Mutation/commit costs can
                                     reports/2026-       dominate.
                                     03/2026-03-31/
                                     perf-
                                     audit.md:155

  Benchmark credibility: the project has useful performance audit artifacts, but not a
  comprehensive benchmark suite comparable to a storage engine’s durability/performance
  matrix. I found no standard benches/ suite covering large datasets, recovery time,
  write amplification, space amplification, cursor stability, or durability-level
  tradeoffs.

  ## 15. Documentation and Operational Readiness

  Strong documentation:

  - Scope and storage modes are clearly described.
  - Atomicity and transaction non-goals are unusually explicit.
  - Query cursor non-snapshot behavior is documented.
  - Generated endpoint policies are documented.

  Gaps:

   Gap                                         Why it matters
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   No single operator-facing durability        Users need one place explaining what
   page                                        survives upgrade/crash/trap.
  ──────────────────────────────────────────  ──────────────────────────────────────────
   No persisted-format specification           Hard to audit compatibility and
                                               corruption handling.
  ──────────────────────────────────────────  ──────────────────────────────────────────
   No backup/restore procedure found           Production storage needs operational
                                               playbooks.
  ──────────────────────────────────────────  ──────────────────────────────────────────
   No corruption recovery playbook found       Operators need “what to do when recovery
                                               fails.”
  ──────────────────────────────────────────  ──────────────────────────────────────────
   No max key/value/database/index size        Boundary behavior is part of API
   contract                                    stability.
  ──────────────────────────────────────────  ──────────────────────────────────────────
   No explicit thread/process/multi-handle     Prevents SQLite-like assumptions.
   support statement
  ──────────────────────────────────────────  ──────────────────────────────────────────
   No hostile-file/security posture page       Important if stable memory snapshots can
                                               be imported or restored.

  ## 16. Recommended Adversarial Test Plan

  1. Model-based random operation test:
      - Generate create/update/delete/get/range/query operations.
      - Mirror in BTreeMap plus reference secondary-index model.
      - Reopen/recover frequently.
      - Assert row and index equivalence after every operation.

  2. Crash/recovery simulation:
      - Add failpoints before/after marker set, journal append, fold watermark, index
        rebuild, marker clear.

      - Run child process, kill at failpoint, reopen.
      - Verify only committed states are visible.

  3. Corruption fuzzing:
      - Fuzz commit marker, journal batch, raw row, schema snapshot, index key/envelope.
      - Assert no panic, OOM, infinite loop, or silent wrong result.

  4. Concurrency stress:
      - Multiple handles where supported.
      - Long cursor while writes occur.
      - Concurrent host-thread misuse should either fail safely or be documented
        impossible.

  5. Boundary-value tests:
      - Empty keys/values, huge keys/values, binary values, Unicode, repeated overwrite,
        delete absent, range boundaries.

  6. Failure-injection tests:
      - Simulate memory allocation failure, stable-memory grow failure, interrupted
        fold, incompatible persisted version, and corrupted tail cleanup.

  ## 17. Top 10 Fixes

   Rank    Fix                                           Difficulty    Type
  ━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━
   1       Add failpoint crash-recovery harness for      High          Correctness-
           commit/journal/recovery                                     critical
  ──────  ────────────────────────────────────────────  ────────────  ──────────────────
   2       Fix no-default test cfg failures and add      Medium        Correctness-
           CI coverage                                                 critical
  ──────  ────────────────────────────────────────────  ────────────  ──────────────────
   3       Bound raw journal batch storage before        Medium        Correctness-
           allocation                                                  critical
  ──────  ────────────────────────────────────────────  ────────────  ──────────────────
   4       Add persisted-format checksums                Medium        Correctness-
                                                                       critical
  ──────  ────────────────────────────────────────────  ────────────  ──────────────────
   5       Add fuzz targets for marker/journal/raw       Medium        Correctness-
           row/index/schema decode                                     critical
  ──────  ────────────────────────────────────────────  ────────────  ──────────────────
   6       Make recovery state scoped to storage         Medium        Correctness-
           domain or prove global safety                               critical
  ──────  ────────────────────────────────────────────  ────────────  ──────────────────
   7       Add stable-memory allocation/failure          High          Correctness-
           injection                                                   critical
  ──────  ────────────────────────────────────────────  ────────────  ──────────────────
   8       Replace whole-index fold/rebuild with         High          Correctness-
           streaming/sliced recovery path                              critical at
                                                                       scale
  ──────  ────────────────────────────────────────────  ────────────  ──────────────────
   9       Strengthen volatile heap-mode API             Low           Quality-
           warnings/gating                                             improving
  ──────  ────────────────────────────────────────────  ────────────  ──────────────────
   10      Publish operator durability/recovery/         Medium        Quality-
           backup docs                                                 improving

  ## 18. Top 10 Tests to Add

  1. Kill after marker set, before journal append.
  2. Kill after some marker-bound journal batches append.
  3. Kill after data fold before watermark.
  4. Kill after watermark before tail cleanup.
  5. Kill during secondary-index rebuild/fold.
  6. Oversized/corrupt journal value must not OOM or panic.
  7. Model-based random operations with reopen after every N operations.
  8. Atomic batch parent+child relation visibility test.
  9. Multi-registry recovery test for process-global recovery state.
  10. Fuzz corpus for marker, journal, index key/envelope, raw row, and schema snapshot.

  ## 19. Scorecard

   Area                                   Score
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━
   Correctness                             7/10
  ─────────────────────────────────────  ───────
   Durability                              6/10
  ─────────────────────────────────────  ───────
   Crash safety                            5/10
  ─────────────────────────────────────  ───────
   Transaction semantics                   5/10
  ─────────────────────────────────────  ───────
   Concurrency safety                      5/10
  ─────────────────────────────────────  ───────
   Data-structure integrity                7/10
  ─────────────────────────────────────  ───────
   Error handling                          7/10
  ─────────────────────────────────────  ───────
   Security against malformed input        6/10
  ─────────────────────────────────────  ───────
   API design                              6/10
  ─────────────────────────────────────  ───────
   Test quality                            7/10
  ─────────────────────────────────────  ───────
   Documentation                           8/10
  ─────────────────────────────────────  ───────
   Performance engineering                 6/10
  ─────────────────────────────────────  ───────
   Maintainability                         7/10
  ─────────────────────────────────────  ───────
   SQLite-style reliability discipline     5/10

  ## 20. Unknowns and Limitations

  - I did not run Miri because it is unavailable for the active toolchain.
  - I did not run sanitizer builds.
  - I did not prove or disprove real crash safety with process kills because the
    repository lacks an obvious failpoint harness.

  - I did not manually corrupt IC stable-memory internals beyond static review.
  - I did not audit every generated canister endpoint in full.
  - Current dirty worktree files may affect future audit reproducibility; I did not
    modify them.

  Files changed by this audit: none.

  Validation status: normal all-features build, tests, fmt, clippy, invariants, feature
  checks, and cargo audit passed. Full no-default test commands failed to compile.

  ## 21. Final Verdict

  IcyDB 0.187.9 shows serious engineering intent and has better-than-average written
  contracts for a young storage engine, but it is not SQLite-like in reliability
  discipline yet. The commit-marker plus journal recovery design is plausible, but
  production-grade durability requires adversarial crash testing, persisted-format
  fuzzing, resource-failure modeling, and stronger compatibility/security hardening. I
  would not use this as high-stakes durable storage today unless the deployment accepts
  its IC-specific scope, narrow transaction semantics, and current recovery-test gaps.