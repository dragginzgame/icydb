# IcyDB Technical Debt Audit

Date: 2026-07-04

## Executive summary

IcyDB is not accumulating uncontrolled technical debt. The repository shows a
strong pattern of paying down planner, cursor, admission, schema-authority, and
durability debt as slices land. The current risk is different: the system is
moving fast enough that documentation, diagnostic schemas, perf attribution, and
test tripwires are lagging the architecture.

Top 10 technical debt findings:

1. TD-001: `QUERY_CONTRACT.md` and `CURSOR.md` still describe cursor handling as
   purely post-access, while 0.196 route evidence now exposes pushed ordered-read
   and limit-stop behavior.
2. TD-002: 0.197 primary-key equality and finite primary-key `IN`
   canonicalization is a real pre-1.0 cleanup opportunity because users already
   expect `WHERE pk = value` to behave like `by_id(value)`.
3. TD-003: Planner/admission/executor proof sharing remains high-interest debt
   around 0.197 unless one planner-owned access artifact is the only authority.
4. TD-004: Recursive structural-value decode still allocates owned `Value`
   lists/maps in the production decode path.
5. TD-005: Persisted-format governance has a policy and inventory, but not a
   byte-level stable-memory spec.
6. TD-006: Schema mutation has narrow runners and publication gates now, but no
   durable migration IDs, phases, watermarks, or reentry state.
7. TD-007: Recovery evidence is good as regression floor, but not yet a
   production recovery-size or streaming-recovery guarantee.
8. TD-008: The fast-path inventory still names three unguarded areas: stream
   precedence, grouped dedicated path ownership, and the bytes-terminal
   derivation exception.
9. TD-009: The SQL performance matrix classifier is a large test-side heuristic
   that currently classifies route families from SQL string fragments.
10. TD-010: Public EXPLAIN, SQL result, and perf attribution DTOs are useful but
    are moving quickly and need explicit pre-1.0 stability/version decisions.

Top 5 cleanup PRs to do immediately:

1. Update query/cursor contracts to describe semantic guarantees separately from
   route-dependent pushdown facts.
2. Add the three missing fast-path guard tests named by
   `docs/governance/fast-path-inventory.md`.
3. Split SQL perf matrix route classification into a structured helper with
   scenario metadata and unit tests, preserving the current dirty grouped
   aggregate classification.
4. Add 0.197 cache/proof tripwire tests before implementing primary-key
   canonicalization.
5. Refresh `docs/1.0-TODO.md` so closed items, intentional non-goals, and real
   blockers are not mixed.

Top 5 pre-1.0 cleanup blockers:

1. Byte-level persisted-format spec for stable-memory row, index, marker,
   journal, schema, and cursor-adjacent durable surfaces.
2. Durable schema migration lifecycle or an explicit 1.0 non-goal that keeps
   broad online migrations out of the contract.
3. Stable public DTO/version policy for EXPLAIN, SQL endpoint results, and
   diagnostics/perf attribution.
4. Recovery-size production policy: either streaming recovery/rebuild or a
   documented supported bound with fail-closed behavior.
5. Cursor contract closure: stable envelope fields, validation behavior, route
   pushdown wording, and mutation-between-pages examples.

Biggest architecture debt: planner/admission/executor proof ownership around
0.197 canonicalization. The design is clear, but the debt becomes expensive if
implemented as a late executor shortcut or as duplicated SQL/fluent rules.

Biggest test debt: missing tripwires for fast-path ownership plus missing
0.197 tests for same-shape/different-parameter cache reuse, wrong-type
fail-closed behavior, residual validation, `Empty` zero-IO behavior, and
SQL/fluent parity.

Biggest documentation debt: `docs/1.0-TODO.md` and query/cursor contracts do not
fully reflect the 0.196 closeout and later admission/durability contracts.

Biggest performance debt: recursive full `Value` materialization and absence of
allocation counters for row/field decode, projection, recursive decode, cache,
and covering paths.

Biggest public API debt: public EXPLAIN/access-decision DTOs and SQL result DTOs
are exposed through the facade while route facts and diagnostics are still being
actively refined.

Biggest storage/recovery debt: no byte-level persisted-format spec and no
production-scale recovery/streaming guarantee.

Overall: IcyDB is mostly managing debt, not accumulating serious uncontrolled
debt. The dangerous debt is hardening debt before 1.0: public schemas, durable
formats, cursor tokens, recovery bounds, and proof/admission artifacts.

## Scope

Repository commit observed at final audit point:
`1f4fb54ac062bbdb8538b4888251b444e6f0ebcd`.

The audit started with `ec8c119a2359921232ea36b6ca4a252314d19192`; the worktree
changed during the audit. I treated later changes as user work and did not
revert them.

Dirty worktree:

- Initial dirty files:
  - `CHANGELOG.md`
  - `docs/changelog/0.196.md`
  - `docs/design/0.196-sqlite-comparison-audit/implementation-results.json`
  - `docs/design/0.196-sqlite-comparison-audit/implementation-results.md`
  - `testing/integration/tests/sql_perf_matrix_audit.rs`
- Final pre-report dirty file before adding this audit:
  - `testing/integration/tests/sql_perf_matrix_audit.rs`
- Final validation dirty files after writing this audit:
  - `CHANGELOG.md`
  - `docs/changelog/0.196.md`
  - `docs/design/0.196-sqlite-comparison-audit/implementation-results.json`
  - `docs/design/0.196-sqlite-comparison-audit/implementation-results.md`
  - `testing/integration/tests/sql_perf_matrix_audit.rs`
  - `docs/audits/technical-debt/` (new audit artifacts from this task)
- The dirty test change adds grouped-aggregate route classification:
  `if sample.sql.contains(" GROUP BY ")` returns
  `materialized_order/materialized/grouped_aggregate_materialized`.

Current baseline assumptions:

- 0.196 is a diagnostic/route-classification line with full deterministic matrix
  evidence and no intended query, cache, cursor, persisted-format, public API, or
  public-read-admission change.
- 0.197 is a proposed deterministic rule-based primary-key canonicalization
  line, not a cost-based optimizer.
- Accepted schema snapshots are runtime authority; generated `EntityModel` and
  `IndexModel` are allowed for proposal, reconciliation, model-only convenience,
  and tests only.
- Pre-1.0 hard cuts are intentional and are not debt by themselves.

Inspected:

- `docs/design/`, including 0.196, 0.197, 0.193, 0.192, 0.191, 0.190, 0.189,
  and 0.184 audit/design lines.
- `docs/audits/`, `docs/contracts/`, `docs/governance/`,
  `docs/operations/`, `docs/1.0-TODO.md`, `docs/ROADMAP.md`, root and detailed
  changelogs.
- `crates/icydb-core/src/db/`, `crates/icydb/src/`,
  `crates/icydb-schema*`, `crates/icydb-build/`, `testing/`, `canisters/`, and
  Cargo feature/dependency surfaces.

Not inspected exhaustively:

- Historical audit artifacts under every `docs/audits/reports/*/artifacts`
  directory. I sampled relevant prior reports and used current source/docs as
  authority.
- Full perf matrices for this audit. I inspected checked-in matrix harnesses and
  recorded 0.196 artifacts, but did not rerun the expensive deterministic matrix.
- Wasm-size artifacts. No wasm-size measurement was run.

Relationship to 0.196 and 0.197:

- TD-001, TD-009, TD-014, and TD-018 are mostly 0.196 closeout/process debt.
- TD-002, TD-003, TD-008, TD-012, and TD-016 are 0.197 de-risking debt.
- TD-005, TD-006, TD-007, TD-010, TD-011, and TD-013 are pre-1.0 hardening debt.

## Commands run

| Command | Result | Notes |
| --- | --- | --- |
| `git status --short` | Pass | Initial dirty worktree recorded. |
| `git rev-parse HEAD` | Pass | Initial HEAD was `ec8c119a2359921232ea36b6ca4a252314d19192`. |
| `cargo metadata --format-version 1 --no-deps` | Pass | Initial package version output showed `0.196.4`; rerun later showed `0.196.5` after worktree moved. |
| `cargo tree --workspace --all-features --depth 1` | Pass | Baseline dependency surface captured. |
| `cargo fmt --check` | Pass | No formatting failures. |
| `cargo test --workspace --all-features` | Fail | Environmental failure: PocketIC binary missing for `fluent_perf_audit`; unit tests reached by the run passed before the integration failure. |
| `env IC_TESTKIT_ALLOW_POCKET_IC_DOWNLOAD=1 cargo test -p icydb-testing-integration --test fluent_perf_audit` | Pass | Approved network/download rerun; 2 passed, 1 ignored. |
| `cargo clippy --workspace --all-features --all-targets -- -D warnings` | Pass | No clippy failures. |
| `git status --short` | Pass | Later dirty worktree only showed `testing/integration/tests/sql_perf_matrix_audit.rs`. |
| `rg -n "^version = \"0\.196\.\"" Cargo.toml crates schema canisters testing` | Pass | Current workspace package version observed as `0.196.5`. |
| `rg -n "version = \"0\.196\.\"" Cargo.lock` | Pass | Lockfile observed at `0.196.5`. |
| `git diff -- testing/integration/tests/sql_perf_matrix_audit.rs` | Pass | Confirmed user dirty grouped-aggregate classifier/test. |
| `find docs/design -maxdepth 3 -type f` | Pass | Design docs enumerated. |
| `find docs/audits -maxdepth 4 -type f` | Pass | Audit docs enumerated. |
| `find docs/contracts -maxdepth 3 -type f` | Pass | Contract docs enumerated. |
| `find docs/operations -maxdepth 3 -type f` | Pass | Operation docs enumerated. |
| `sed` reads of 0.196, 0.197, 0.193, 0.192, 0.191, 0.190, 0.189, 0.184 docs | Pass | Used for design/audit context. |
| `sed` reads of query, cursor, admission, durability, persisted-format, roadmap, changelog docs | Pass | Used for contract and doc-debt context. |
| Broad `rg` for TODO/FIXME/HACK/XXX and debt markers | Pass | Too broad to use directly; narrowed searches used for findings. |
| `rg -o "TODO|FIXME|HACK|XXX" ... | wc -l` | Pass | Active scoped source marker count: 2. |
| `rg -n "TODO|FIXME|HACK|XXX" ...` | Pass | Both active scoped source TODOs are in structural value-storage decode. |
| `find ... -name '*.rs' ... | xargs wc -l | sort -nr | head -40` | Pass with broken-pipe note | Produced hotspot sizes; `sort: Broken pipe` came from `head` closing the pipe. |
| `rg -n "Cursor continuation|post-access|pushed down|index seek|runtime-only optimization|materialize" ...` | Pass | Found stale cursor/query contract lines. |
| `rg -n "stream|grouped|bytes|unguarded|guard|Remaining" docs/governance/fast-path-inventory.md` | Pass | Found remaining unguarded fast-path areas. |
| `rg -n "not a byte-level spec|byte-level|checksum|import|streaming|recovery|stable" ...` | Pass | Found persisted-format and recovery policy evidence. |
| `rg -n "SQL cache|cache|method version|shape|parameter|same-shape|different" ...` | Pass, truncated | Narrowed later to specific cache lines. |
| `rg -n "classif|GROUP BY|route_family|outcome|contains" testing/integration/tests/sql_perf_matrix_audit.rs` | Pass | Found test-side classifier and current dirty grouped aggregate line. |
| `sed -n` focused reads of `QUERY_CONTRACT.md`, value decode, cache, matrix classifier, fast-path inventory | Pass | Used for line-level evidence. |
| `rg -n "Full matrix|1,756|1,675|81|Route classification|materialized_order|wasm|/tmp|instructions" ...` | Pass | Found 0.196 matrix evidence and missing wasm-size artifact. |
| `rg -n "implementation started|pending|Closeout|0\.196\.5|Full-matrix" ...` | Pass | Found stale 0.196 design status line. |
| `rg -n "primary-key|planner-owned|executor shortcut|Admission|cache|same-shape|different-value|must|Fail-closed|parameter" ...` | Pass | Found 0.197 canonicalization requirements. |
| `rg -n "0\.197|Primary-key|canonical|planner|cursor|query planner|Unchecked|\[ \]|..." ...` | Pass, truncated | Used with focused reads for 1.0 TODO/doc debt. |
| `rg -n "schema mutation|migration|runner|durable|accepted schema|reconciliation|mutation" ...` | Pass, truncated | Used with focused reads for schema mutation debt. |
| `rg -n "allocation|alloc|row decode|projection|covering|wasm|benchmark|matrix|regression|instructions|stable-memory" ...` | Pass, truncated | Used for perf debt. |
| `sed -n` focused reads of schema mutation, runner, 0.192 audit, durability, 0.196 implementation | Pass | Used for migration/recovery debt. |
| `sed -n` focused reads of public facade, query, session load, SQL result, explain DTOs | Pass | Used for API debt. |
| `rg -n "pub enum|pub struct|pub use|pub type|non_exhaustive|must_use" ... crates/icydb-core/src/db/session/load.rs` | Fail | Bad path: `crates/icydb-core/src/db/session/load.rs` does not exist. Narrowed with actual facade/core files. |
| `cargo tree --workspace --all-features --duplicates` | Pass | Duplicate dependency versions found, mostly from dev/test and upstream IC/PocketIC stacks. |
| `find . -path './target' -prune -o -path './.git' -prune -o -path './.cache' -prune -o -name Cargo.toml -print` | Pass | 29 manifests found outside target/git/cache. |
| `rg -n "^\\[features\\]|^default|^sql|^diagnostics|sql-explain|..." ... canisters/*/Cargo.toml` | Fail | Bad glob for nested canister Cargo.tomls; useful hits still found for `icydb`/`icydb-core` and integration. |
| `rg -n "cfg\\(feature|cfg\\(all\\(feature|cfg\\(any\\(feature|feature = \\\"" ...` | Pass, truncated | Feature-gated surface sampled. |
| `rg -n "no-default|default-features|all-features|sql-explain|diagnostics" ...` | Pass, truncated | CI/Makefile no-default and feature checks found. |
| `find docs/audits -maxdepth 3 -type d` | Pass | Confirmed no existing technical-debt audit convention. |
| `find docs/audits -maxdepth 3 -type f -name '*debt*' -o -name '*audit*.md'` | Pass | No technical-debt convention found. |
| `git rev-parse HEAD` | Pass | Final pre-report HEAD: `1f4fb54ac062bbdb8538b4888251b444e6f0ebcd`. |
| `mkdir -p docs/audits/technical-debt` | Pass | Created requested audit directory. |
| `jq empty docs/audits/technical-debt/icydb-technical-debt-ledger-2026-07-04.json` | Pass | JSON syntax validated. |
| `git diff --check -- docs/audits/technical-debt/icydb-technical-debt-audit-2026-07-04.md docs/audits/technical-debt/icydb-technical-debt-ledger-2026-07-04.json` | Pass | No whitespace errors reported by git. |
| `wc -l docs/audits/technical-debt/icydb-technical-debt-audit-2026-07-04.md docs/audits/technical-debt/icydb-technical-debt-ledger-2026-07-04.json` | Pass | Final artifact counts: report 1,900 lines; ledger 1,003 lines; total 2,903 lines. |
| `git status --short` | Pass | Final status recorded modified pre-existing files plus new audit directory. |

## Debt taxonomy

Real debt: a current code, test, doc, API, perf, or process gap with concrete
evidence and a plausible future cost.

Intentional design choice: a suspicious-looking choice backed by current
contracts, design docs, or hard-cut policy.

Explicit non-goal: a consciously excluded product surface such as cost-based
optimization, raw stable-memory import support, or multi-message transactions.

First-slice deferral: a gap intentionally scoped out of a narrow slice, but
still requiring a tripwire, TODO ledger, or follow-up before hardening.

Pre-1.0 hard-cut opportunity: a breaking or cleanup change that should happen
before public APIs, durable formats, cursor tokens, or diagnostics are stable.

Post-1.0 breaking-risk item: debt that becomes much more expensive once users
depend on API shapes, byte layouts, cursor envelopes, or diagnostic schemas.

## Debt inventory

| Debt ID | Category | Title | Current location | Evidence | Debt type | Why it is debt | Why it may be intentional / not debt | Interest rate | Risk if ignored | Cleanup opportunity | Suggested owner area | Effort | Priority | Confidence | Blocks 1.0 | Blocks 0.196 | Blocks 0.197 | Follow-up patch prompt |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| TD-001 | N, B | Query/cursor performance contract lags 0.196 route facts | `docs/contracts/QUERY_CONTRACT.md`, `docs/contracts/CURSOR.md`, 0.196 implementation docs | `QUERY_CONTRACT.md` says cursor continuation is post-access and not pushed into index seek/range; 0.196 reports pushed ordered reads and limit-stop attribution for 1,675 common successes | stale doc / contract drift | Users and future maintainers cannot tell which behavior is semantic guarantee versus internal route optimization | It may be intentionally conservative if pushdown is not guaranteed | High | 0.197 may update planner/executor while relying on stale cursor wording | Rewrite performance model as semantics plus route-dependent optimization facts | Query docs / planner | S | P1 | High | Yes | No | Maybe | Update query/cursor contracts to separate pagination semantics from 0.196 route pushdown facts and link closeout tests. |
| TD-002 | B, K | Primary-key equality canonicalization gap | 0.197 design, query planner/admission/cache | 0.197 design requires strict scalar PK equality/finite PK `IN` canonicalization; current public exact admission is `by_id`/`by_ids` | first-slice deferral / API ergonomics | Users must know IcyDB-specific key APIs for shapes databases normally recognize | It is a deliberate 0.197 follow-up, not a 0.196 regression | High | Workarounds and executor shortcuts harden before 1.0 | Land planner-owned `ByKey`/`ByKeys`/`Empty` proof for strict scalar PK only | Query planner / admission | M | P0 | High | Maybe | No | Yes | Implement only 0.197 scalar PK canonicalization with fail-closed tests and no executor shortcut. |
| TD-003 | A, B, C | Planner/admission/executor proof drift risk | `query/admission.rs`, route planner, executor route contracts, 0.197 design | `query/admission.rs` is 2,205 lines; 0.197 says one artifact must feed admission, explain, cache, execution; fast-path inventory has unguarded areas | architecture / missing invariant tests | Proofs can drift if SQL, fluent, admission, explain, and executor each infer boundedness | Existing design explicitly calls for one artifact | High | Public read bypass, stale explain facts, or wrong route cache reuse | Add tests that assert execution cannot infer PK proof independently | Query planner / read admission | M | P0 | High | Yes | No | Yes | Add planner-owned proof-tripwire tests before adding any executor PK recognition. |
| TD-004 | F, O | Recursive structural-value decode allocates full owned values | `crates/icydb-core/src/db/data/structural_field/value_storage/decode/value.rs` | Only 2 active source TODOs, both at recursive list/map decode allocation comments | performance / serialization debt | Projection and validation paths may pay full-row/full-value allocation cost | Current behavior is correct and bounded | Medium | Projection-only and recursive-field workloads stay expensive; row-codec changes get harder | Add allocation measurement and a visitor boundary experiment | Data codec / row layout | M | P2 | High | Maybe | No | No | Add allocation counters for recursive decode and prototype a non-owning visitor path behind tests. |
| TD-005 | D, N, P | Persisted format inventory is not a byte-level spec | `docs/contracts/PERSISTED_FORMAT_INVENTORY.md`, `PERSISTED_FORMAT_POLICY.md`, `DURABILITY.md` | Inventory line says it is a checklist, not byte-level layout spec; policy covers classification but not bytes | documentation / persisted-format debt | 1.0 needs durable byte authority independent of source spelunking | Before 1.0, code can still own exact encodings | High | Byte changes become breaking or ambiguous after 1.0 | Write byte-level spec for row, schema, marker, journal, index, structural values | Storage / governance | M | P0 | High | Yes | No | No | Create a persisted byte-format spec that cites codecs and malformed-input tests. |
| TD-006 | E, D | Schema mutation lacks durable migration lifecycle | `schema/mutation/*`, 0.192 audit, `docs/1.0-TODO.md` | Current code says startup reconciliation can execute only no-rebuild plans; 0.192 says no durable migration IDs/phases/watermarks/reentry | architecture / recovery debt | Runners exist, but interrupted migrations do not have a durable phase authority like commit recovery | Broad online migration may be a non-goal for 1.0 | High | Partial schema/index work becomes hard to recover or diagnose | Either implement durable migration state or explicitly exclude broad online migrations from 1.0 | Schema / recovery | L | P0 | High | Yes | No | No | Define the minimum 1.0 migration lifecycle or document the non-goal and fail-closed boundary. |
| TD-007 | D, M | Recovery scale evidence is regression floor, not production bound | `DURABILITY.md`, `DURABILITY_GUIDE.md`, 0.191 docs | Durability contract says 1,024 host and 32-row PocketIC probes are proof shapes, not production IC guarantees | test/perf/recovery debt | Operators need supported recovery-size expectations | The current fail-closed stance is intentional and correct | High | Large recovery work may fail without a clear product bound | Add recovery-size matrix or streaming recovery design | Storage / operations | L | P0 | High | Yes | No | No | Produce recovery-size support matrix or streaming recovery plan with fail-closed tests. |
| TD-008 | M, P | Fast-path ownership tripwires remain incomplete | `docs/governance/fast-path-inventory.md` | Remaining unguarded areas: stream precedence helpers, grouped dedicated ownership, bytes-terminal exception | missing invariant tests | Fast paths are exactly where silent planner/executor drift is expensive | The inventory explicitly tracks follow-ups | Medium | Future optimization can bypass proof/admission boundaries | Add three narrow guard tests and close inventory entries | Executor / query tests | S | P1 | High | Maybe | No | Yes | Add guard tests for the three remaining fast-path inventory gaps. |
| TD-009 | L, O, M | SQL perf matrix route classifier relies on SQL string heuristics | `testing/integration/tests/sql_perf_matrix_audit.rs` | `route_classification_for_sample` branches on `sample.sql.contains(...)`; dirty change adds `GROUP BY` string branch | test/observability debt | Route attribution can drift from execution facts and misclassify performance evidence | It is test-only and closeout-focused, not production | Medium | Performance reports become misleading during 0.196/0.197 closeout | Move classification toward structured scenario metadata and execution route facts | Testing / perf harness | M | P1 | High | No | Maybe | Maybe | Refactor route classification into typed scenario metadata with unit tests preserving current labels. |
| TD-010 | K, L | Public EXPLAIN and SQL DTOs need stability policy | `crates/icydb/src/db/query/mod.rs`, `query/explain/plan.rs`, `db/sql/types.rs`, `db/session/mod.rs` | Facade re-exports `Explain*V1`; SQL result enum and perf attribution structs have public fields | public API risk | Public DTO shapes can freeze internal route/diagnostic evolution | V1 naming and schema_version on access decision show intent to version | High | Post-1.0 changes become breaking Candid/API changes | Decide opaque accessors vs versioned public structs before 1.0 | Public API / diagnostics | M | P0 | High | Yes | No | Maybe | Audit public DTOs and mark/stabilize/version them before 1.0. |
| TD-011 | N, P | `docs/1.0-TODO.md` mixes stale, closed, and real blockers | `docs/1.0-TODO.md`, ROADMAP, contracts | Many unchecked items now have contracts or changelog closure; some are true blockers | governance/doc debt | Release planning can rediscover old findings and miss current blockers | The file is a broad checklist, not a status dashboard | Medium | 1.0 readiness decisions become noisy | Refresh TODO into closed / blocker / non-goal / deferred buckets | Governance | S | P1 | High | Yes | No | No | Reconcile 1.0 TODO against current contracts and mark closed/non-goal/deferred items. |
| TD-012 | H, B | Query-plan cache identity must hard-cut for 0.197 if shape semantics change | `session/query/cache.rs`, 0.197 design | Cache method version is `3`; key includes accepted schema identity and normalized predicate fingerprint; 0.197 warns old generic/rejected plans must miss | cache-version risk | PK canonicalization changes shape meaning and admission/execution path | The code already has method-version machinery | High | Same-shape/different-value or stale generic plan reuse | Add tests first, bump versions only if needed | Query cache / SQL | S | P0 | High | Maybe | No | Yes | Add 0.197 cache identity tests for literals, parameters, wrong types, and stale generic/rejected plans. |
| TD-013 | D, L | Operator durability diagnostics remain narrower than recovery authority | `DURABILITY.md`, `DURABILITY_GUIDE.md`, 0.192 audit | 0.192 requested compact operator health contract; current docs warn direct raw access is out-of-contract | observability debt | Operators need compact marker/journal/fold/readiness facts without raw-store access | Existing fail-closed boundary is intentional | Medium | Debugging recovery failures requires source-level knowledge | Add bounded storage health report and troubleshooting guide | Storage diagnostics / ops | M | P1 | Medium | Maybe | No | No | Define operator durability health DTO with marker/journal/fold readiness facts. |
| TD-014 | O, M | Perf evidence lacks allocation counters and wasm-size deltas | 0.196 implementation results, perf matrix harness | 0.196 closeout says no wasm-size artifact; matrix captures instruction counters but not allocation counters | perf measurement debt | Optimization cleanup may move allocations/wasm size without visible gates | Full deterministic matrix is strong instruction evidence | Medium | Regressions shift to heap allocation or wasm size unnoticed | Add measurement-only counters and wasm-size capture | Perf harness | S | P1 | High | Maybe | Maybe | Yes | Add allocation and wasm-size measurement-only reports without changing execution. |
| TD-015 | J | Feature combination coverage exists but was not audited as fully closed | `Makefile`, `.github/workflows/ci.yml`, feature rg output | CI/Makefile check no-default, sql, diagnostics; this audit only ran all-features | needs more evidence | Feature cfg surface is broad across SQL/diagnostics/sql-explain | Existing CI shape likely covers important combinations | Low | Rare no-default or diagnostics-only drift | Run or document feature matrix in release readiness | Build/CI | S | P2 | Medium | No | No | No | Add a release-readiness note that maps features to CI checks and gaps. |
| TD-016 | B, M | SQL/fluent parity for 0.197 is not yet proven | 0.197 design, SQL lowering tests, fluent query API | 0.197 lists required SQL literal/parameter/fluent equivalence tests | missing regression tests | Canonicalization must not create SQL-only or fluent-only behavior | This is required by design before implementation | High | User-visible inconsistency and cache divergence | Add parity tests before or with canonicalization | Query tests | M | P0 | High | Maybe | No | Yes | Add SQL/fluent PK equality and PK IN parity tests for 0.197. |
| TD-017 | I, J | Macro/codegen public path assumptions need pre-1.0 smoke around no-default/SQL | `crates/icydb/src/lib.rs`, `__macro`, trybuild tests, build helper | Facade exposes narrow `__macro` generated-code surface; no-default checks exist | API/codegen debt | Generated code can accidentally rely on broad facade internals | `__macro` is doc-hidden and intentionally narrow | Medium | Downstream generated code breaks when facade cleans internals | Add trybuild/no-default generated-code fixture coverage for feature combos | Macro/codegen | S | P2 | Medium | Maybe | No | No | Add no-default and SQL-enabled generated-code compile fixtures for facade path assumptions. |
| TD-018 | N | 0.196 design status header is stale versus implementation closeout | `0.196-design.md`, `implementation-results.md` | Design says full-matrix closeout pending; implementation results show full-matrix evidence and pass | stale doc | Readers may think 0.196 lacks closeout evidence | The implementation results doc is the fresher authority | Low | Audit churn and duplicated findings | Update design status or add closeout pointer | Docs | XS | P2 | High | No | Maybe | No | Update 0.196 design status to point at implementation closeout. |

## Detailed findings

### TD-001: Query/cursor performance contract lags 0.196 route facts

Category:
- Documentation / query / observability

Severity:
- High

Priority:
- P1

Confidence:
- High

Debt type:
- Stale contract / documentation drift

Interest rate:
- High

Should block 1.0:
- Yes

Should block 0.196:
- No

Should block 0.197:
- Maybe

Evidence:
- `docs/contracts/QUERY_CONTRACT.md` says cursor continuation is applied in the
  post-access phase and cursor boundary conditions are not pushed down into
  index seek/range operations.
- `docs/contracts/CURSOR.md` repeats the post-access statement.
- `docs/design/0.196-sqlite-comparison-audit/implementation-results.md`
  records full deterministic matrix evidence, 1,756 generated scenarios,
  1,675 executed scenarios, route-family/outcome coverage, and explicit
  limit-stop attribution.

Current behavior:
- Pagination semantics remain live-state and non-snapshot. However, route facts
  now distinguish pushed ordered reads, residual scans, materialized order, and
  unsupported access kinds.

Why this is debt:
- The contract currently conflates semantic guarantee and implementation
  performance model. 0.197 depends on precise separation between planner proof,
  admission, explain, cache identity, and execution. Stale wording makes it
  easier to implement new proof behavior in the wrong layer.

Why this may be intentional:
- It is correct to avoid promising pushdown as a stable user-visible guarantee.
  The doc should keep that stance but update the performance model.

Risk if ignored:
- Future code may use the stale contract as evidence that route pushdown is not
  real, or users may treat route facts as stable cursor-token semantics.

Recommended cleanup:
- Minimal: rewrite query/cursor performance docs to say cursor semantics are
  stable but route implementations may apply bounded ordered-read pushdown when
  planner/admission proofs allow it.
- Optional: link each route family to closeout tests and matrix evidence.

Acceptance criteria:
- Docs cite 0.196 implementation results.
- Cursor semantic guarantees remain unchanged.
- Pushdown is explicitly described as route-dependent optimization, not public
  token semantics.

Estimated effort:
- S

Suggested patch order:
- First cleanup PR.

Follow-up Codex patch prompt:
- "Update `docs/contracts/QUERY_CONTRACT.md` and `docs/contracts/CURSOR.md` so
  the cursor semantics section remains live-state/post-result correct while the
  performance model reflects 0.196 route-dependent ordered-read pushdown and
  limit-stop facts. Link the 0.196 implementation-results evidence and avoid
  promising pushdown as a public guarantee."

### TD-002: Primary-key equality canonicalization gap

Category:
- Query / API / architecture

Severity:
- High

Priority:
- P0

Confidence:
- High

Debt type:
- First-slice deferral / API ergonomics debt

Interest rate:
- High

Should block 1.0:
- Maybe

Should block 0.196:
- No

Should block 0.197:
- Yes

Evidence:
- `docs/design/0.197-deterministic-optimizer-canonicalization/0.197-design.md`
  requires planner-owned strict scalar primary-key equality and finite
  primary-key `IN` canonicalization.
- The design requires accepted-schema key encoding, fail-closed validation,
  cache hard cuts if identity changes, and no executor shortcut.
- `docs/contracts/READ_ADMISSION.md` documents exact `by_id`/`by_ids` public
  admission, not natural `filter(pk = value)` equivalence.

Current behavior:
- Exact primary-key APIs are the admitted path. Natural SQL/fluent primary-key
  predicates are not yet canonicalized into the same proof.

Why this is debt:
- A database framework that exposes SQL and fluent predicates will surprise users
  if `WHERE id = ?` cannot use the same admitted key proof as `by_id`.

Why this may be intentional:
- 0.196 explicitly did not implement this. 0.197 is the design line for it.

Risk if ignored:
- Users learn IcyDB-specific query spellings, SQL/fluent parity debt grows, and
  later API behavior changes become more visible.

Recommended cleanup:
- Implement only strict scalar PK equality and finite PK `IN` as planner-owned
  `ByKey`/`ByKeys`/`Empty` artifacts.
- Reject wrong-type, malformed, partial composite, secondary unique, and
  over-budget inputs without scan fallback.

Acceptance criteria:
- SQL literal, SQL parameter, and fluent tests pass for same logical key.
- Wrong-type/missing parameter tests fail before scan.
- `Empty` performs zero row-store gets and zero index range scans.
- EXPLAIN, admission, cache, and executor consume the same artifact.

Estimated effort:
- M

Suggested patch order:
- After TD-012 and TD-016 tests are in place.

Follow-up Codex patch prompt:
- "Implement the first 0.197 primary-key canonicalization slice for strict
  scalar PK equality and finite PK `IN`, producing planner-owned
  `ByKey`/`ByKeys`/`Empty` artifacts consumed by admission, EXPLAIN, cache, and
  execution. Add SQL/fluent parity, parameter-cache, wrong-type, residual, and
  zero-IO `Empty` tests."

### TD-003: Planner/admission/executor proof drift risk

Category:
- Architecture / query / read admission / testing

Severity:
- High

Priority:
- P0

Confidence:
- High

Debt type:
- Abstraction leak / missing invariant tests

Interest rate:
- High

Should block 1.0:
- Yes

Should block 0.196:
- No

Should block 0.197:
- Yes

Evidence:
- 0.197 says primary-key canonicalization "must carry enough information for
  read admission, explain, cache identity, and execution" and must not be a
  late executor trick.
- `crates/icydb-core/src/db/query/admission.rs` is a large admission authority
  file.
- `docs/governance/fast-path-inventory.md` still lists unguarded fast-path
  owner boundaries.

Current behavior:
- Existing design is sound: planner proposes, executor revalidates, no public
  bypass. The debt is the missing tripwire before adding another proof family.

Why this is debt:
- If an executor path independently recognizes primary-key equality, EXPLAIN,
  admission, and execution can diverge.

Why this may be intentional:
- Current exact-read admission and 0.196 pushdown are intentionally bounded.

Risk if ignored:
- A future route optimization could become a public-read bypass or produce stale
  cache/explain facts.

Recommended cleanup:
- Add proof-drift tests before implementation: one asserts executor cannot
  upgrade a non-admitted shape; one asserts explain/admission/execution route
  identity for admitted shapes; one asserts residual validation remains complete.

Acceptance criteria:
- Tests fail if execution infers PK proof without planner artifact.
- Public read admission rejects unsupported fallback shapes.
- EXPLAIN route facts and executor route facts agree for canonicalized shapes.

Estimated effort:
- M

Suggested patch order:
- Same sprint as TD-012 and before TD-002 implementation.

Follow-up Codex patch prompt:
- "Add 0.197 proof-drift guard tests that prove admission, EXPLAIN, cache, and
  execution all consume one planner-owned access artifact and that executor code
  cannot independently upgrade primary-key equality predicates."

### TD-004: Recursive structural-value decode allocates full owned values

Category:
- Serialization / row layout / performance

Severity:
- Medium

Priority:
- P2

Confidence:
- High

Debt type:
- Performance workaround / stale TODO

Interest rate:
- Medium

Should block 1.0:
- Maybe

Should block 0.196:
- No

Should block 0.197:
- No

Evidence:
- The only two active scoped source TODO/FIXME/HACK/XXX markers are in
  `crates/icydb-core/src/db/data/structural_field/value_storage/decode/value.rs`.
- Both TODOs say recursive list/map decode allocates final owned runtime
  containers and should eventually use a streaming visitor.

Current behavior:
- Decode is bounded and fallible, but projection/validation paths cannot avoid
  owned recursive `Value` construction.

Why this is debt:
- IcyDB has covering/projection-oriented performance work. Full recursive
  decode allocation is the opposite shape and can hide row-layout wins.

Why this may be intentional:
- It is correct and simpler. The TODO is scoped to future projection-only paths.

Risk if ignored:
- Field-offset/row-codec experiments become harder because recursive value
  allocation remains the hidden cost.

Recommended cleanup:
- First add measurement: allocation counters or proxy counters for recursive
  decode, projection-only decode, and full-row decode.
- Then add a streaming visitor boundary if counters show material cost.

Acceptance criteria:
- Bench/report exposes recursive list/map decode counts.
- No persisted-format change.
- Existing decode-fail-closed tests continue to pass.

Estimated effort:
- M

Suggested patch order:
- Measurement-only PR after 0.197 proof tests.

Follow-up Codex patch prompt:
- "Add measurement-only instrumentation for structural value-storage recursive
  list/map decode allocation pressure, then document whether a visitor-based
  projection path is worth implementing. Do not change persisted bytes."

### TD-005: Persisted format inventory is not a byte-level spec

Category:
- Storage / persisted format / documentation / process

Severity:
- High

Priority:
- P0

Confidence:
- High

Debt type:
- Undocumented persisted contract

Interest rate:
- High

Should block 1.0:
- Yes

Should block 0.196:
- No

Should block 0.197:
- No

Evidence:
- `docs/contracts/PERSISTED_FORMAT_INVENTORY.md` says it is a review checklist,
  not a byte-level layout specification.
- `docs/contracts/PERSISTED_FORMAT_POLICY.md` governs classifications and
  hard-cut policy, but not exact stable-memory bytes.
- `docs/contracts/DURABILITY.md` explicitly defers checksums and import support.

Current behavior:
- Code and tests are the exact persisted-format authority.

Why this is debt:
- After 1.0, source-only byte authority makes compatibility review expensive and
  error-prone.

Why this may be intentional:
- Before 1.0, hard cuts and source-owned encoding are acceptable.

Risk if ignored:
- A patch can change durable bytes without a reviewer seeing the complete
  compatibility impact.

Recommended cleanup:
- Write a byte-level spec that covers stable memory IDs, row envelopes, schema
  snapshots, index keys/entries, marker envelopes, journal batches/chunks,
  structural values, and cursor/durable boundaries where relevant.

Acceptance criteria:
- Every inventory row links to exact codec files and malformed-input tests.
- Spec says whether unknown versions are incompatible format, corruption, or
  migration-required.
- Changelog/review checklist references the byte-level spec.

Estimated effort:
- M

Suggested patch order:
- Pre-1.0 storage hardening sprint.

Follow-up Codex patch prompt:
- "Create a byte-level persisted-format specification under `docs/contracts/`
  that expands the existing inventory into stable-memory layout details, version
  behavior, fail-closed decode expectations, and test links."

### TD-006: Schema mutation lacks durable migration lifecycle

Category:
- Schema / migration / storage / recovery

Severity:
- High

Priority:
- P0

Confidence:
- High

Debt type:
- Architecture debt / recovery state debt

Interest rate:
- High

Should block 1.0:
- Yes

Should block 0.196:
- No

Should block 0.197:
- No

Evidence:
- 0.192 audit identified no durable migration IDs, phases, watermarks, or
  reentry state.
- `crates/icydb-core/src/db/schema/mutation/mod.rs` says startup reconciliation
  can currently execute only no-rebuild plans.
- The same module stages index additions/drops/nullability changes, but
  publication remains blocked or narrow.
- `SchemaFieldPathIndexRunner` is a narrow physical runner for one supported
  field-path index rebuild.

Current behavior:
- Catalog-native mutation planning is stronger than the old audit: there are
  schema-owned plans, publication gates, and a narrow runner. Durable migration
  lifecycle is still missing.

Why this is debt:
- Physical schema work without durable phases is not equivalent to commit
  recovery. Interrupted migration semantics need their own authority.

Why this may be intentional:
- Broad online migration may be explicitly deferred before 1.0.

Risk if ignored:
- Future DDL work can accidentally publish accepted schema around incomplete
  physical state or require ad hoc recovery rules.

Recommended cleanup:
- Either implement minimal durable migration state for supported rebuild paths,
  or explicitly state that broad online migration is out of 1.0 and keep
  unsupported changes fail-closed.

Acceptance criteria:
- Migration status is durable and reentrant, or the 1.0 contract excludes it.
- Startup readiness fails closed on incomplete migration state.
- Tests cover interrupted migration/reentry for any admitted physical path.

Estimated effort:
- L

Suggested patch order:
- After format spec, before broad DDL stabilization.

Follow-up Codex patch prompt:
- "Define the 1.0 schema migration boundary: either add durable migration
  records/phases/watermarks for the supported field-path rebuild path, or update
  contracts to keep broad online migration out of 1.0 with fail-closed tests."

### TD-007: Recovery scale evidence is regression floor, not production bound

Category:
- Storage / durability / performance / operations

Severity:
- High

Priority:
- P0

Confidence:
- High

Debt type:
- Missing performance/recovery contract

Interest rate:
- High

Should block 1.0:
- Yes

Should block 0.196:
- No

Should block 0.197:
- No

Evidence:
- `docs/contracts/DURABILITY.md` says 0.190/0.191 recovery measurements are
  proof shapes and regression budgets, not production IC instruction-budget
  guarantees.
- `docs/operations/DURABILITY_GUIDE.md` tells operators not to claim arbitrary
  large-index recovery is budget-certified.

Current behavior:
- Recovery is guarded and fail-closed, but product recovery size is not bounded.

Why this is debt:
- Operators need to know what size of recovery/rebuild IcyDB supports before
  hardening durable storage claims.

Why this may be intentional:
- Fail-closed behavior is correct and safer than overclaiming.

Risk if ignored:
- A production canister may enter fail-closed recovery on a large state without
  a published support envelope.

Recommended cleanup:
- Produce either a supported recovery-size matrix or a streaming recovery/fold
  design with measurable budgets.

Acceptance criteria:
- Operations docs state supported row/index/journal sizes or explicitly require
  future streaming recovery for larger states.
- Tests prove idempotent reentry within the documented bound.
- Metrics/diagnostics expose enough state for operator triage.

Estimated effort:
- L

Suggested patch order:
- Pre-1.0 storage sprint.

Follow-up Codex patch prompt:
- "Produce a recovery-size support matrix or streaming recovery design, with
  PocketIC/host evidence, idempotent reentry tests, and operator documentation."

### TD-008: Fast-path ownership tripwires remain incomplete

Category:
- Testing / query / executor / process

Severity:
- Medium

Priority:
- P1

Confidence:
- High

Debt type:
- Missing invariant tests

Interest rate:
- Medium

Should block 1.0:
- Maybe

Should block 0.196:
- No

Should block 0.197:
- Yes

Evidence:
- `docs/governance/fast-path-inventory.md` lists remaining unguarded areas:
  stream fast-path precedence helpers, grouped dedicated fast-path ownership,
  and bytes-terminal derivation exception.

Current behavior:
- Some fast-path guards already exist, including terminal derivation and SQL
  count consumer-route guards.

Why this is debt:
- Unguarded fast paths are prone to becoming convention-based rather than
  proof-based.

Why this may be intentional:
- The inventory calls them follow-ups after remaining boundary story is clear.

Risk if ignored:
- 0.197 may add another proof family while old fast paths still rely on
  informal ownership.

Recommended cleanup:
- Add one narrow guard per remaining unguarded area and update the inventory.

Acceptance criteria:
- Tests fail if fast-path precedence order changes without explicit update.
- Tests fail if grouped dedicated runtime path owns planner strategy decisions.
- Tests fail if bytes-terminal derivation bypasses prepared execution state.

Estimated effort:
- S

Suggested patch order:
- Immediate cleanup PR.

Follow-up Codex patch prompt:
- "Add guard tests for the three remaining items in
  `docs/governance/fast-path-inventory.md`, then update the inventory to mark
  them guarded."

### TD-009: SQL perf matrix route classifier relies on SQL string heuristics

Category:
- Observability / performance / testing

Severity:
- Medium

Priority:
- P1

Confidence:
- High

Debt type:
- Test harness complexity / misleading diagnostics risk

Interest rate:
- Medium

Should block 1.0:
- No

Should block 0.196:
- Maybe

Should block 0.197:
- Maybe

Evidence:
- `testing/integration/tests/sql_perf_matrix_audit.rs` is about 5,563 lines.
- `route_classification_for_sample` branches on SQL text fragments such as
  `ORDER BY id ASC`, `collection_id =`, and `GROUP BY`.
- The current dirty worktree adds grouped aggregate classification with
  `sample.sql.contains(" GROUP BY ")`.

Current behavior:
- The classifier is test-only and produces useful closeout summaries, but it is
  heuristic.

Why this is debt:
- Performance evidence can be misclassified if SQL rendering changes while
  execution facts stay the same.

Why this may be intentional:
- It is a pragmatic matrix harness, not production code.

Risk if ignored:
- Route-family changes in 0.197 may be counted as route proof changes or hidden
  as classifier changes.

Recommended cleanup:
- Add structured scenario metadata for access/order/grouping family and use
  runtime route facts where available. Keep string fallback only for legacy
  saved reports.

Acceptance criteria:
- Existing matrix markdown labels stay stable.
- Unit tests cover grouped aggregate, primary order, secondary order, equality
  prefix ordered suffix, storage mirror, and unsupported expressions.
- Saved-report compatibility remains for old artifacts.

Estimated effort:
- M

Suggested patch order:
- Before next full matrix closeout.

Follow-up Codex patch prompt:
- "Refactor `route_classification_for_sample` so generated scenarios carry
  structured route-intent metadata, while saved reports keep a compatibility
  classifier. Preserve the current grouped aggregate classification and tests."

### TD-010: Public EXPLAIN and SQL DTOs need stability policy

Category:
- API / observability / documentation

Severity:
- High

Priority:
- P0

Confidence:
- High

Debt type:
- Public API risk

Interest rate:
- High

Should block 1.0:
- Yes

Should block 0.196:
- No

Should block 0.197:
- Maybe

Evidence:
- `crates/icydb/src/db/query/mod.rs` re-exports many `Explain*V1` types.
- `ExplainAccessDecisionV1` has public fields and `schema_version`.
- `SqlQueryResult` and SQL grouped/perf attribution DTOs have public fields and
  feature-gated variants/fields.

Current behavior:
- Public observability types are valuable and mostly intentionally versioned.

Why this is debt:
- Route facts, diagnostics, and perf attribution are still moving. Public DTOs
  can freeze internal names and fields before 1.0.

Why this may be intentional:
- V1 names and schema_version indicate planned versioning. Public structs make
  integration easy.

Risk if ignored:
- 0.197 route/proof additions or future diagnostics cleanup becomes public API
  breakage.

Recommended cleanup:
- Decide which DTOs are stable Candid/API surface, which are diagnostics-only,
  and which should be opaque with accessors before 1.0.

Acceptance criteria:
- API docs state stability guarantees per DTO.
- Versioned DTOs include version fields or names.
- Breaking shape changes are completed before 1.0 or explicitly gated.

Estimated effort:
- M

Suggested patch order:
- Pre-1.0 API sprint, before 0.197 public release if EXPLAIN changes.

Follow-up Codex patch prompt:
- "Audit public EXPLAIN, SQL result, and perf attribution DTOs. Decide stable,
  versioned, or opaque status for each, then update docs/tests before 1.0."

### TD-011: 1.0 TODO mixes stale, closed, and real blockers

Category:
- Documentation / governance

Severity:
- Medium

Priority:
- P1

Confidence:
- High

Debt type:
- Stale checklist / process debt

Interest rate:
- Medium

Should block 1.0:
- Yes

Should block 0.196:
- No

Should block 0.197:
- No

Evidence:
- `docs/1.0-TODO.md` still has unchecked items for transaction semantics,
  cursor hardening, query planner rules, storage invariants, accepted schema
  identity, diagnostics, and tests.
- Current contracts/changelogs close or clarify several of these areas, while
  others remain real blockers.

Current behavior:
- The TODO file is broad and useful, but not status-accurate.

Why this is debt:
- A noisy release checklist causes duplicated audits and can hide real blockers.

Why this may be intentional:
- It is a pre-1.0 umbrella checklist, not a release dashboard.

Risk if ignored:
- Release planning spends time re-proving already-closed design lines.

Recommended cleanup:
- Reclassify entries as closed, blocker, non-goal, deferred, or needs evidence.

Acceptance criteria:
- Every remaining unchecked item points to an owner doc, test gap, or follow-up.
- Closed items cite contract/changelog evidence.
- Non-goals are moved out of blocker sections.

Estimated effort:
- S

Suggested patch order:
- Immediate docs cleanup after TD-001.

Follow-up Codex patch prompt:
- "Reconcile `docs/1.0-TODO.md` against current contracts, changelog, and 0.196
  evidence. Mark closed items, split true blockers from non-goals, and link each
  remaining blocker to evidence or an owner doc."

### TD-012: Query-plan cache identity must hard-cut for 0.197 if shape semantics change

Category:
- Caching / query / SQL

Severity:
- High

Priority:
- P0

Confidence:
- High

Debt type:
- Cache-version risk

Interest rate:
- High

Should block 1.0:
- Maybe

Should block 0.196:
- No

Should block 0.197:
- Yes

Evidence:
- `crates/icydb-core/src/db/session/query/cache.rs` defines
  `SHARED_QUERY_PLAN_CACHE_METHOD_VERSION: u8 = 3`.
- The query-plan cache key includes method version, entity path, schema
  identity, visibility, and structural query.
- 0.197 says plan-cache identity, SQL-cache identity, method version, or
  admission semantics must hard-cut if meaning changes.

Current behavior:
- Cache identity machinery is already explicit.

Why this is debt:
- 0.197 changes the meaning of primary-key predicate shapes. Tests must prove
  old generic/rejected plans are not reused.

Why this may be intentional:
- Existing method versioning is the right mitigation.

Risk if ignored:
- Same-shape/different-parameter SQL could reuse stale key values, or old
  generic scans could remain cached after canonicalization.

Recommended cleanup:
- Add tests for parameterized SQL shape reuse, different values, wrong types,
  literal/parameter isolation, stale rejected/generic plans, and version bump
  behavior.

Acceptance criteria:
- Same cached parameterized shape with key A then key B returns B.
- Wrong-type parameter fails validation and does not scan.
- If method version changes, old entries miss.

Estimated effort:
- S

Suggested patch order:
- Before TD-002 implementation.

Follow-up Codex patch prompt:
- "Add 0.197 query/SQL cache identity tests proving parameterized PK predicates
  cache shape and parameter slot but not stale values, and that stale generic or
  rejected plans miss if method versions change."

### TD-013: Operator durability diagnostics remain narrower than recovery authority

Category:
- Observability / storage / operations

Severity:
- Medium

Priority:
- P1

Confidence:
- Medium

Debt type:
- Observability gap

Interest rate:
- Medium

Should block 1.0:
- Maybe

Should block 0.196:
- No

Should block 0.197:
- No

Evidence:
- 0.192 audit identified operator durability diagnostics as a design need.
- Durability docs warn raw stable-memory access bypasses guarded recovery and is
  out of contract.

Current behavior:
- Recovery authority is guarded, but operator-facing health is not as compact as
  the underlying marker/journal/fold model.

Why this is debt:
- Operators need durable readiness facts without directly reading raw stores.

Why this may be intentional:
- Rich raw repair/import is an explicit non-goal.

Risk if ignored:
- Debugging fail-closed recovery requires internal knowledge.

Recommended cleanup:
- Define a bounded operator health report with marker state, journal tail/fold
  state, schema readiness, and recovery-required flag.

Acceptance criteria:
- Health report is query-safe and does not mutate persistent state.
- It does not expose raw import/repair promises.
- Tests cover clean, marker-present, journal-tail, and fail-closed states.

Estimated effort:
- M

Suggested patch order:
- Storage observability sprint.

Follow-up Codex patch prompt:
- "Design and add a bounded durability health report that surfaces guarded
  recovery readiness facts without raw stable-memory access or repair/import
  promises."

### TD-014: Perf evidence lacks allocation counters and wasm-size deltas

Category:
- Performance / testing / observability

Severity:
- Medium

Priority:
- P1

Confidence:
- High

Debt type:
- Measurement debt

Interest rate:
- Medium

Should block 1.0:
- Maybe

Should block 0.196:
- Maybe

Should block 0.197:
- Yes for performance claims

Evidence:
- 0.196 implementation results record instruction deltas and route facts, but
  state that no wasm-size artifact was produced.
- The matrix harness records many instruction counters but not allocation
  counters.
- 0.197 performance closeout requires fresh focused and full matrix artifacts.

Current behavior:
- Instruction evidence is strong; allocation and wasm-size evidence is weaker.

Why this is debt:
- Query performance changes can trade instructions for allocation or wasm size.

Why this may be intentional:
- Full matrices are already expensive, and wasm-size was not changed by
  diagnostic-only code intentionally.

Risk if ignored:
- 0.197 could claim performance closeout while missing allocation or code-size
  regressions.

Recommended cleanup:
- Add measurement-only allocation proxy counters and a wasm-size capture hook for
  relevant matrix/report runs.

Acceptance criteria:
- Reports include wasm profile plus raw wasm byte size when available.
- Allocation/clone/decode proxy counters exist for row decode, recursive decode,
  projection, cache, and covering paths.
- Existing closeout gates remain unchanged unless explicitly widened.

Estimated effort:
- S

Suggested patch order:
- Before 0.197 perf closeout.

Follow-up Codex patch prompt:
- "Add measurement-only wasm-size and allocation/proxy-counter reporting to the
  SQL perf harness, without changing query behavior or closeout thresholds."

### TD-015: Feature combination coverage exists but was not audited as fully closed

Category:
- Feature flags / dependencies / testing

Severity:
- Low

Priority:
- P2

Confidence:
- Medium

Debt type:
- Needs more evidence

Interest rate:
- Low

Should block 1.0:
- No

Should block 0.196:
- No

Should block 0.197:
- No

Evidence:
- `crates/icydb` and `crates/icydb-core` have `default = []`, `sql`,
  `sql-explain`, and `diagnostics` features.
- `Makefile` and `.github/workflows/ci.yml` include no-default, SQL, and
  diagnostics checks.
- This audit ran all-features, not the full CI feature matrix.

Current behavior:
- Feature coverage appears intentional and improved, but this audit did not
  prove every combination.

Why this is debt:
- Feature-gated public APIs and diagnostics can drift outside all-features.

Why this may be intentional:
- CI already checks important no-default combinations.

Risk if ignored:
- A release readiness audit may assume all-features covers no-default behavior.

Recommended cleanup:
- Add a short feature-matrix contract that maps supported feature combinations
  to CI/Makefile checks.

Acceptance criteria:
- Document says which combinations are supported.
- CI/Makefile commands are linked.
- Release checklist includes no-default verification.

Estimated effort:
- S

Suggested patch order:
- Governance cleanup sprint.

Follow-up Codex patch prompt:
- "Document the supported `icydb`/`icydb-core` feature matrix and map each
  supported combination to CI or Makefile verification commands."

### TD-016: SQL/fluent parity for 0.197 is not yet proven

Category:
- Query / SQL / testing

Severity:
- High

Priority:
- P0

Confidence:
- High

Debt type:
- Missing regression tests

Interest rate:
- High

Should block 1.0:
- Maybe

Should block 0.196:
- No

Should block 0.197:
- Yes

Evidence:
- 0.197 lists explicit tests for SQL literal, SQL parameter, and fluent
  primary-key equality / `IN` equivalence.
- Current SQL lowering tests cover many canonical shape identities, but the
  0.197 behavior has not landed yet.

Current behavior:
- SQL/fluent parity is strong in many areas, but not for the proposed PK
  canonicalization behavior.

Why this is debt:
- Canonicalization should not be SQL-only or fluent-only.

Why this may be intentional:
- It belongs to the 0.197 implementation.

Risk if ignored:
- Public query surfaces diverge exactly where database users expect equivalence.

Recommended cleanup:
- Add parity tests before or in the same patch as canonicalization.

Acceptance criteria:
- Fluent, SQL literal, and SQL parameter variants produce same row results,
  route facts, admission behavior, and cache semantics.
- Wrong-type and missing-parameter variants fail the same way where applicable.

Estimated effort:
- M

Suggested patch order:
- Before TD-002 implementation or in the first 0.197 patch.

Follow-up Codex patch prompt:
- "Add SQL/fluent parity tests for 0.197 primary-key equality and finite
  primary-key `IN`, including literals, parameters, wrong types, missing
  parameters, and residual validation."

### TD-017: Macro/codegen public path assumptions need feature-combo smoke

Category:
- Macro-codegen / feature flags / API

Severity:
- Medium

Priority:
- P2

Confidence:
- Medium

Debt type:
- Generated-code assumption risk

Interest rate:
- Medium

Should block 1.0:
- Maybe

Should block 0.196:
- No

Should block 0.197:
- No

Evidence:
- `crates/icydb/src/lib.rs` exposes a narrow `#[doc(hidden)] __macro` generated
  code surface.
- `icydb-build` emits SQL-gated generated endpoint code.
- Trybuild coverage exists, and no-default checks exist, but this audit did not
  prove generated code under every feature combination.

Current behavior:
- Generated code targets explicit facade paths, which is the correct direction.

Why this is debt:
- Hidden facade paths can become de facto public if generated downstream code
  relies on them.

Why this may be intentional:
- Generated code needs hidden stable wiring; exposing it narrowly is appropriate.

Risk if ignored:
- A cleanup of facade internals can break generated canisters in a feature
  combination not covered by tests.

Recommended cleanup:
- Add compile fixtures for no-default generated canister, SQL generated
  canister, and diagnostics-enabled generated canister.

Acceptance criteria:
- Trybuild or integration compile tests cover generated code paths for supported
  feature combinations.
- Public docs keep `__macro` hidden and not semver-stable for hand-written code.

Estimated effort:
- S

Suggested patch order:
- Macro/codegen cleanup sprint.

Follow-up Codex patch prompt:
- "Add generated-code compile fixtures that exercise facade `__macro` paths in
  no-default, SQL, SQL-explain, and diagnostics-supported combinations."

### TD-018: 0.196 design status header is stale versus implementation closeout

Category:
- Documentation / process

Severity:
- Low

Priority:
- P2

Confidence:
- High

Debt type:
- Stale status text

Interest rate:
- Low

Should block 1.0:
- No

Should block 0.196:
- Maybe

Should block 0.197:
- No

Evidence:
- `docs/design/0.196-sqlite-comparison-audit/0.196-design.md` says
  implementation started and full-matrix closeout still pending.
- `docs/design/0.196-sqlite-comparison-audit/implementation-results.md` says
  the 0.196 slice now has full-matrix evidence and lists closeout results.

Current behavior:
- The implementation results doc is current; the design header is stale.

Why this is debt:
- It causes re-audit churn and makes it easy to duplicate already-known
  findings.

Why this may be intentional:
- Design docs often retain historical status, but this one is likely read as
  current.

Risk if ignored:
- Engineers may rerun or distrust already-recorded closeout work.

Recommended cleanup:
- Change the status line to "implemented; see implementation-results" or add a
  closeout pointer.

Acceptance criteria:
- Header no longer says closeout is pending.
- It links to implementation results.

Estimated effort:
- XS

Suggested patch order:
- Docs cleanup batch with TD-001 and TD-011.

Follow-up Codex patch prompt:
- "Update the 0.196 design status header to point at
  `implementation-results.md` as the closeout authority."

## Debt that is probably intentional

| Suspicious item | Why it looked suspicious | Why it is intentional | Keep from rediscovery |
| --- | --- | --- | --- |
| No cost-based optimizer | SQLite comparison could suggest cost-based planning | ROADMAP and 0.197 explicitly choose deterministic rule-based planning | Keep explicit non-goal in query contract and 0.197 docs. |
| Pre-1.0 hard cuts | Compatibility breaks can look risky | AGENTS rules and design docs prefer hard cuts before 1.0 | Persisted-format policy should keep hard-cut checklist visible. |
| Raw stable-memory import unsupported | Import/export appears missing | Durability and persisted-format docs explicitly exclude raw import | Operations docs should keep non-goal and future import requirements. |
| No persisted checksums now | Missing checksum can look like corruption debt | Current supported boundary is same-canister stable memory plus fail-closed decode | Keep checksum decision in DURABILITY and revisit before import support. |
| Heap stores not durable production storage | Heap/journaled parity gaps could look like bugs | Contracts state journaled is durable production lane; heap is volatile/test/demo | Keep tests/docs distinguishing semantic parity from durability parity. |
| Broad online schema migration unsupported | Mutation runners exist, but many paths block | Current code intentionally fail-closes rebuild/migration shapes | 1.0 TODO should decide non-goal versus blocker. |

## Stale TODO/FIXME report

Active scoped source markers found: 2.

| File | Line | Comment | Apparent age/context | Classification | Recommendation |
| --- | --- | --- | --- | --- | --- |
| `crates/icydb-core/src/db/data/structural_field/value_storage/decode/value.rs` | 484 | `TODO(value-storage zero-copy): recursive decode must allocate...` | Current structural value-storage decode path | Real debt | Convert to measurement task, then visitor experiment if counters justify it. |
| `crates/icydb-core/src/db/data/structural_field/value_storage/decode/value.rs` | 504 | `TODO(value-storage zero-copy): recursive map decode allocates...` | Same | Real debt | Same as above. |
| `docs/1.0-TODO.md` | many | unchecked pre-1.0 items | Mixed status | Real doc/process debt | Reconcile into closed/blocker/non-goal/deferred buckets. |
| `docs/design/0.196-sqlite-comparison-audit/0.196-design.md` | 3 | full-matrix closeout pending | Stale after implementation results | Stale/delete or update | Replace with closeout pointer. |

## Duplicated logic report

| Concept | Duplicated locations | Risk | Unify now? | Smallest unification slice | Tests needed |
| --- | --- | --- | --- | --- | --- |
| Route classification for perf evidence | SQL perf matrix classifier and runtime route/explain facts | Misleading closeout summaries | Yes, in test harness | Add typed scenario metadata and keep saved-report fallback | Matrix classifier unit tests. |
| Primary-key exact access proof | Existing `by_id`/`by_ids`, future SQL/fluent PK predicates, admission, explain, executor | 0.197 proof drift | Yes for 0.197 | Planner-owned `ByKey`/`ByKeys`/`Empty` artifact | SQL/fluent/cache/admission/explain parity tests. |
| Query cache identity shape | Structural query key, normalized predicate fingerprint, SQL parameter shape | Stale cache reuse | Yes before 0.197 | Add tests; bump method version only if semantics change | Same-shape/different-value tests. |
| Fast-path ownership | Planner route facts, executor terminal paths, bytes terminal exception | Boundary drift | Yes | Add three fast-path guards | Source/behavior guard tests. |
| Persisted format authority | Codecs, inventory, policy docs, changelog review | Review misses byte changes | Yes before 1.0 | Byte-level spec linked to tests | Malformed decode and compatibility tests. |
| Schema mutation lifecycle | Mutation plan, field-path runner, reconciliation, DDL admission | Incomplete migration recovery | Not broad rewrite | Decide durable state or explicit non-goal | Interrupted migration/reentry tests if admitted. |
| Public diagnostics schemas | EXPLAIN DTOs, SQL result enum, perf attribution DTOs | API freeze | Yes before 1.0 | Stability/version policy | Candid/API shape tests. |

## API debt report

| Public item | Risk | Breaking/non-breaking cleanup | Pre-1.0 recommendation | Tests/docs |
| --- | --- | --- | --- | --- |
| `ExplainAccessDecisionV1` and related `Explain*` DTOs | Public fields freeze route vocabulary | Breaking if fields become opaque | Decide stable V1 schema or opaque accessors | Public API docs and JSON/Candid shape tests. |
| `SqlQueryResult` | Feature-gated variants and string statuses can freeze SQL endpoint schema | Breaking to rename/remove variants or fields | Version or document endpoint result stability | Candid shape tests per feature combo. |
| `SqlQueryPerfAttribution` | Public counters can accumulate unstable fields | Non-breaking if new optional fields only; breaking otherwise | Version or keep diagnostics-only with explicit stability | Diagnostics feature tests. |
| `trusted_read_unchecked` | Public bypass can be misused | Non-breaking docs/tests | Keep, but docs must stay explicit about controller/admin policy | Read admission tests already important. |
| `__macro` facade module | Hidden generated-code surface can become de facto public | Breaking to generated downstream code | Add generated-code compile fixtures and keep doc-hidden | Trybuild/no-default/SQL fixtures. |
| `StructuralPatch::empty` | Public sparse mutation shape can be confusing | Non-breaking docs | Keep but ensure accepted-schema construction path is prominent | Mutation docs/examples. |

## Test debt report

| Invariant | Existing evidence found | Missing or weak coverage | Test type | Before 1.0? |
| --- | --- | --- | --- | --- |
| Committed row exists in exactly one primary logical location | Commit/data/index tests, durability contracts | Broader recovery-size state coverage | Fault-injection/integration | Yes |
| Secondary index entry points to live row | Index consistency and recovery tests | Operator health/reporting not compact | Integration/fault | Yes |
| Every indexed row has corresponding index entries | Rebuild/recovery tests | Large-state recovery bound | Perf/fault | Yes |
| Unique indexes cannot contain duplicates | Existing uniqueness tests and contracts | Wider property tests for schema mutation paths | Property/integration | Should |
| Insert/update/delete atomic across data and indexes | Atomicity and transaction contracts; commit tests | Migration/rebuild atomicity if admitted | Fault-injection | Yes if migrations admitted |
| Failed writes leave no partial changes | Commit-window tests | Cross-store fault matrix at larger scale | Fault-injection | Should |
| Decode failures fail closed | Codec tests and persisted-format policy | Byte-level spec cross-links | Unit/corpus | Yes |
| Schema mismatch fails clearly | Accepted schema transition tests | 1.0 TODO status drift | Unit/docs | Yes |
| Migration completes or leaves safe state | Field-path runner tests, publication gates | Durable migration phases/watermarks | Fault-injection | Yes if admitted |
| Index path equals scan/materialized fallback | SQL/fluent tests and perf matrix | 0.197 PK canonicalization parity | Differential | Yes |
| SQL and fluent equivalent queries match | Many SQL lowering/surface tests | PK equality/IN equivalence | Integration/unit | Yes for 0.197 |
| Query ordering deterministic where promised | 0.196 ordered-read evidence | Contract wording drift | Contract/test docs | Yes |
| Cursor pages do not skip/duplicate rows | Pagination tests and 0.196 cursor edge tests | Contract examples for mutation-between-pages | Integration/docs | Yes |
| Public read admission cannot be bypassed | 0.193 contract/tests | 0.197 proof path tests | Integration/admission | Yes |
| Cached plans do not embed liveness/generation facts | Cache key includes visibility/schema identity | 0.197 parameter/value tests | Unit/integration | Yes |
| Query calls do not mutate persistent metrics/state | Contracts imply query-safe metrics | Operator diagnostics health report tests missing | Unit/integration | Should |
| Macro schema matches accepted runtime schema | Trybuild and accepted-schema tests | Feature-combo generated fixtures | Compile-fail/trybuild | Should |
| Feature flags do not change persisted format | CI checks no-default builds | Explicit feature-format contract | Build/docs | Should |
| Heap and journaled stores preserve semantics | Storage mirror matrix and contracts | Clearer parity/non-durability docs | Integration/docs | Should |
| Recovery marker/journal state idempotent | 0.190/0.191 failpoint/corpus evidence | Production-size/streaming bound | Fault/perf | Yes |
| Diagnostics do not change behavior | 0.196 diagnostic-only deltas | Public DTO stability and wasm-size measurements | Integration/perf/API | Yes |

High-value tests to add before 1.0:

- `public_read_pk_filter_canonicalization_uses_same_admission_proof_as_by_id`
- `sql_parameterized_pk_equality_cache_reuses_shape_not_value`
- `sql_pk_equality_wrong_type_fails_without_scan`
- `pk_in_empty_result_performs_zero_row_and_index_io`
- `fast_path_stream_precedence_owner_guard`
- `grouped_dedicated_fast_path_owner_guard`
- `bytes_terminal_derivation_exception_guard`
- `persisted_format_byte_spec_examples_decode_fail_closed`
- `durable_migration_incomplete_state_fails_closed` if migration is admitted
- `recovery_health_report_marker_journal_fold_states_are_query_safe`

## Documentation debt report

| Doc | Debt | Recommendation |
| --- | --- | --- |
| `docs/contracts/QUERY_CONTRACT.md` | Cursor performance model stale versus 0.196 pushdown facts | Update semantics/performance split. |
| `docs/contracts/CURSOR.md` | Historical post-access wording | Point to updated query contract and add mutation-between-pages examples. |
| `docs/design/0.196-sqlite-comparison-audit/0.196-design.md` | Header says closeout pending | Update status pointer. |
| `docs/1.0-TODO.md` | Mixed stale/closed/blocker items | Reconcile into release-readiness buckets. |
| `docs/contracts/PERSISTED_FORMAT_INVENTORY.md` | Checklist but not byte spec | Promote to byte-level spec or add companion. |
| `docs/contracts/DURABILITY.md` and operations guide | Correctly cautious but no product recovery bound | Add recovery-size support matrix when evidence exists. |
| 0.197 design | Strong design, but implementation status not yet linked to tests | Keep as owner doc; add implementation results when slice lands. |

Docs to delete/archive:

- None identified as safe delete. Prefer status updates over deletion.

Docs to promote to contract:

- 0.197 cache/admission/proof rules after implementation.
- 0.196 route classification semantics that are intended to be stable
  diagnostics schema.
- Recovery-size/streaming follow-up once support envelope is decided.

Docs required before 1.0:

- Byte-level persisted-format spec.
- Public DTO/API stability policy for EXPLAIN, SQL results, diagnostics, and
  perf attribution.
- 1.0 migration boundary contract.
- Cursor envelope/validation contract closure.
- Operator durability troubleshooting guide with bounded health report.

## Performance debt report

| Area | Evidence | Debt | Cleanup |
| --- | --- | --- | --- |
| Recursive structural decode | Two active source TODOs | Allocation not measured or avoidable | Add counters, then visitor path if justified. |
| SQL matrix route attribution | 5,563-line harness and SQL string classifier | Classification may drift from execution facts | Use structured metadata/runtime facts. |
| Wasm size | 0.196 closeout says no wasm-size artifact | Code-size regressions not captured | Add raw wasm byte capture. |
| Allocation counters | Matrix has instruction counters only | Heap allocation regressions can hide | Add measurement-only counters/proxies. |
| Recovery scale | Durability docs say no production bound | Large recovery support unclear | Recovery-size matrix or streaming design. |
| Cache hit/miss attribution | Cache metrics exist | 0.197 value/shape risks need specific tests | Add parameter cache tests. |

Benchmark gap table:

| Scenario | Current evidence | Gap |
| --- | --- | --- |
| PK get existing/missing | Exact APIs tested; 0.197 design pending | Natural PK equality matrix missing. |
| Finite PK IN | Design pending | Dedup/order/zero-IO tests missing. |
| Ordered LIMIT | 0.196 matrix evidence | Contract wording stale. |
| Cursor continuation | Pagination tests and 0.196 edge tests | Pushdown vs semantics docs stale. |
| Materialized ORDER BY | 0.196 matrix route facts | Storage mirror materialized route is high instruction family. |
| Projection-only decode | Covering/projection counters exist partly | Allocation counters missing. |
| Recursive decode | TODOs | Allocation counters missing. |
| Covering index | Pure/hybrid counters in matrix | Public perf guide absent. |
| Cache hit/miss | Cache attribution exists | 0.197 parameter value/shape tests missing. |
| SQL parameter shape reuse | Lowering/cache tests exist | PK canonicalization-specific tests missing. |
| Heap/journaled parity | Storage mirror matrix | Docs should clarify semantic vs durability parity. |

Low-risk measurement-only PRs:

1. Add raw wasm byte size capture to matrix reports.
2. Add recursive decode allocation/proxy counters.
3. Add cache hit/miss reason report focused on parameterized PK shapes.
4. Add recovery-size report harness without changing recovery behavior.
5. Add matrix classifier metadata report while preserving old string classifier.

## Pre-1.0 cleanup checklist

| Item | Why before 1.0 | Risk after 1.0 | Recommended slice | Owner area | Acceptance criteria |
| --- | --- | --- | --- | --- | --- |
| Byte-level persisted-format spec | Durable bytes become compatibility contract | Breaking or ambiguous migrations | Write spec and link tests | Storage | Codec/test map complete. |
| Migration lifecycle boundary | Schema mutation API hardens | Recovery semantics become breaking | Implement durable state or non-goal | Schema | Fail-closed interrupted state. |
| Public DTO stability | API/Candid schemas harden | Breaking clients | Version/opaque/public policy | API | Shape tests and docs. |
| Cursor contract closure | Tokens and pagination harden | Token changes break users | Update docs/tests | Query | Envelope/validation documented. |
| Recovery-size support | Operators need product bound | Fail-closed surprises | Matrix or streaming plan | Storage | Supported bound documented. |
| 0.197 proof/cache tests | Prevents unsafe implementation | Planner/executor drift | Tests-first | Query | Tests fail without shared artifact. |
| 1.0 TODO reconciliation | Release planning needs signal | Stale blockers waste effort | Docs cleanup | Governance | Each item classified. |
| Fast-path guards | Prevents drift during optimizations | Hidden bypasses | Guard tests | Executor | Inventory updated. |
| Feature matrix contract | Public API depends on features | Unsupported combo breakage | Docs/CI map | Build | CI commands linked. |
| Perf allocation/wasm counters | Perf claims harden | Hidden regressions | Measurement-only | Perf | Reports include counters. |

## Prioritized cleanup backlog

| Rank | Title | Category | Severity | Priority | Effort | Confidence | Interest rate | Expected benefit | Files likely touched | Tests needed | Docs needed | Blocks 1.0? | Suggested patch order |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | Write byte-level persisted-format spec | Storage/docs | High | P0 | M | High | High | Stabilizes durable review | `docs/contracts/*` | Existing codec tests linked | New spec | Yes | Sprint 2 |
| 2 | Decide 1.0 schema migration boundary | Schema/storage | High | P0 | L | High | High | Avoids unsafe migration hardening | `docs/contracts`, `schema/mutation/*` | Fail-closed or reentry tests | Contract | Yes | Sprint 2 |
| 3 | Public DTO stability/version audit | API/observability | High | P0 | M | High | High | Prevents post-1.0 API breakage | `crates/icydb/src`, explain DTOs | API/Candid shape tests | API docs | Yes | Sprint 3 |
| 4 | Recovery-size support matrix or streaming plan | Storage/perf | High | P0 | L | High | High | Makes durability product-bound | recovery tests/docs | Perf/fault | Ops guide | Yes | Sprint 3 |
| 5 | Cursor contract closure | Query/docs | High | P1 | S | High | High | Aligns semantics and optimization facts | `QUERY_CONTRACT.md`, `CURSOR.md` | Existing tests linked | Contract update | Yes | Sprint 1 |
| 6 | 0.197 cache identity tests | Cache/query | High | P0 | S | High | High | Prevents stale plan reuse | cache/sql tests | Unit/integration | Design results | Maybe | Sprint 1 |
| 7 | 0.197 SQL/fluent parity tests | Query/testing | High | P0 | M | High | High | Prevents surface drift | SQL/fluent tests | Integration | Design results | Maybe | Sprint 1 |
| 8 | 0.197 proof-drift guard tests | Query/admission | High | P0 | M | High | High | Blocks executor shortcut | admission/executor tests | Unit/integration | Design results | Yes | Sprint 1 |
| 9 | Add fast-path guard tests | Executor/testing | Medium | P1 | S | High | Medium | Locks optimization ownership | fast-path guard tests | Unit | Inventory update | Maybe | Sprint 1 |
| 10 | Refactor SQL perf route classifier | Perf/testing | Medium | P1 | M | High | Medium | Makes matrix evidence trustworthy | `sql_perf_matrix_audit.rs` | Unit | Maybe report docs | No | Sprint 1 |
| 11 | Refresh 1.0 TODO | Governance/docs | Medium | P1 | S | High | Medium | Removes stale blockers | `docs/1.0-TODO.md` | None | Same | Yes | Sprint 1 |
| 12 | Update 0.196 design status | Docs/process | Low | P2 | XS | High | Low | Removes closeout confusion | 0.196 design | None | Same | No | Sprint 1 |
| 13 | Add wasm-size measurement | Perf/build | Medium | P1 | S | High | Medium | Catches code-size growth | testing scripts/harness | Smoke | Report docs | Maybe | Sprint 2 |
| 14 | Add allocation/proxy counters | Perf/codec | Medium | P1 | S | Medium | Medium | Reveals hidden allocation cost | diagnostics/perf harness | Perf smoke | Report docs | Maybe | Sprint 2 |
| 15 | Recursive decode visitor experiment | Codec/perf | Medium | P2 | M | Medium | Medium | Reduces projection decode cost if justified | value_storage decode | Unit/perf | Format note | Maybe | Sprint 3 |
| 16 | Operator durability health DTO | Observability/storage | Medium | P1 | M | Medium | Medium | Improves recovery triage | diagnostics/storage | Unit/integration | Ops guide | Maybe | Sprint 2 |
| 17 | Feature matrix contract | Build/governance | Low | P2 | S | Medium | Low | Prevents unsupported combo confusion | docs/CI/Makefile | Existing checks | New doc | No | Sprint 2 |
| 18 | Generated-code feature fixtures | Macro/testing | Medium | P2 | S | Medium | Medium | Locks hidden facade assumptions | trybuild/macro tests | Compile | Maybe | Maybe | Sprint 2 |
| 19 | Link contracts to tests | Docs/testing | Medium | P2 | S | High | Medium | Speeds future audits | contracts | None | Contract refs | Maybe | Sprint 2 |
| 20 | Public SQL result version note | API/docs | High | P1 | S | High | High | Clarifies Candid stability | SQL docs/types | Candid shape | API docs | Yes | Sprint 3 |
| 21 | Explain route facts stability note | Observability/API | High | P1 | S | High | High | Lets 0.197 add facts safely | explain docs/types | JSON shape | API docs | Yes | Sprint 3 |
| 22 | Recovery fail-closed operator examples | Ops/docs | Medium | P2 | S | Medium | Medium | Reduces support burden | operations guide | None | Examples | Maybe | Sprint 3 |
| 23 | Heap vs journaled parity note | Storage/docs | Low | P3 | XS | High | Low | Avoids durability confusion | contracts/docs | None | Note | No | Sprint 2 |
| 24 | Matrix saved-artifact archiving policy | Perf/process | Medium | P2 | S | High | Medium | Avoids `/tmp` evidence loss | perf docs | None | Policy | Maybe | Sprint 2 |
| 25 | Changelog/audit closeout checklist | Governance | Medium | P2 | S | Medium | Medium | Converts audits into backlog | governance docs | None | Checklist | Maybe | Sprint 3 |

## Cleanup sprint plan

### Sprint 1: Query proof and documentation de-risking

Goals:

- Make 0.196 docs current.
- Add tests that prevent 0.197 from being implemented as an executor shortcut.
- Improve matrix route evidence without changing production code.

PRs:

- Update query/cursor contracts and 0.196 status pointer.
- Add 0.197 cache identity, SQL/fluent parity, and proof-drift tests.
- Add the three fast-path guard tests.
- Refactor perf matrix route classifier into structured helper metadata.
- Refresh `docs/1.0-TODO.md`.

Risk:

- Low to medium. Mostly tests/docs/harness.

Tests:

- Focused core query/admission/cache tests.
- SQL perf matrix classifier unit tests.
- Existing all-features workspace checks.

Docs:

- Query contract, cursor contract, 0.196 status, 1.0 TODO.

What not to touch:

- No storage-format changes.
- No primary-key canonicalization implementation until tests are in place.
- No broad planner rewrite.

### Sprint 2: Storage hardening and measurement

Goals:

- Reduce pre-1.0 durable ambiguity.
- Improve measurement before performance cleanup.

PRs:

- Byte-level persisted-format spec.
- Feature matrix contract.
- Wasm-size and allocation/proxy measurement-only reports.
- Operator durability health DTO design, or at least contract and DTO skeleton
  if implementation is larger.
- Generated-code feature-combo compile fixtures.
- Matrix artifact archiving policy.

Risk:

- Medium. Avoid changing persisted bytes or recovery behavior.

Tests:

- JSON/doc lint where available.
- Existing codec/malformed tests linked.
- Compile fixtures for generated-code feature combos.

Docs:

- Persisted format spec, feature matrix, operations guide.

What not to touch:

- No migration durable-state implementation mixed with byte spec.
- No query planner changes.

### Sprint 3: 1.0 public boundary closure

Goals:

- Freeze or version public schemas before users depend on them.
- Decide recovery and migration product boundaries.

PRs:

- Public EXPLAIN/SQL/perf DTO stability audit and fixes.
- Recovery-size support matrix or streaming recovery design.
- Migration lifecycle decision: minimal durable state or explicit 1.0 non-goal.
- Recursive decode visitor experiment only if Sprint 2 counters justify it.
- Changelog/audit closeout checklist.

Risk:

- Medium to high if API or migration behavior changes. Keep each PR narrow.

Tests:

- Candid/API shape tests.
- Recovery fault/perf tests if recovery support changes.
- Migration fail-closed/reentry tests if migration support changes.

Docs:

- API stability docs, durability guide, migration contract.

What not to touch:

- Do not mix storage-format changes with query-planner cleanup.
- Do not add broad online migration support without durable phases and recovery
  tests.

## Final recommendation

Is IcyDB accumulating serious technical debt?

- Mostly no. The architecture is being actively simplified and documented. The
  serious debt is hardening debt that will become expensive at 1.0 if not closed.

Where is the debt most dangerous?

- Persisted byte formats, migration/recovery lifecycle, public DTO schemas,
  cursor token/contract closure, and 0.197 proof/cache/admission ownership.

What should be cleaned immediately?

- Query/cursor contract drift, fast-path guard gaps, 0.197 cache/proof tests,
  SQL perf classifier structure, and stale 1.0/0.196 documentation.

What should wait until after 0.196?

- Primary-key canonicalization implementation. 0.196 should remain diagnostic
  and closeout-clean.

What should wait until after 0.197?

- Deeper row-codec/visitor optimization and broad perf cleanup should wait for
  0.197 correctness and perf evidence unless they are measurement-only.

What should be fixed before 1.0?

- Byte-level persisted-format spec, public DTO stability/versioning, migration
  lifecycle or explicit non-goal, recovery-size policy, cursor contract closure,
  and feature matrix contract.

What looks like debt but should be kept?

- Deterministic rule-based planning instead of cost-based optimization.
- Pre-1.0 hard cuts.
- No raw import/checksum support until explicitly designed.
- Trusted read helpers for controller/admin use, with strict docs.
- Generated hidden facade paths, if covered by compile tests.

What should be deleted?

- No production code should be deleted based on this audit alone.
- Stale status text should be updated; stale TODO/checklist entries should be
  reclassified or removed from blocker lists.

Audit artifact complexity delta:

- Production files touched by this audit: 0.
- New audit artifacts: 2 files, 2,903 lines total.
- Implementation shape: unchanged.
- Perf delta: none measured.
- Wasm-size delta: none measured.
