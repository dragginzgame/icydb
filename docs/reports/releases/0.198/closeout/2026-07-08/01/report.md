# IcyDB 0.198 Closeout Audit

## Verdict

READY TO CLOSE 0.198.

0.198 read-intent ergonomics is coherent after 0.198.13. The public fluent load
surface now teaches and exposes intent-first reads without legacy public
`.limit(...)`, `.offset(...)`, `.one()`, `.all()`, public `PageRequest`, or
public `PagedLoadQuery` staging. Validation passed. The only dirty-worktree
item is unrelated lockfile drift for `memchr 2.8.2 -> 2.8.3`.

## Executive Summary

- Branch: `main`
- Commit: `1d2e3d652b4c5db9d61915bf39dde9fc0b139ce9`
- Commit title: `0.198.13`
- Working tree: dirty only because `Cargo.lock` updates `memchr` from `2.8.2`
  to `2.8.3`
- Overall assessment: implementation, docs, diagnostics, invariants, and tests
  support closing 0.198
- Highest-risk remaining issue: the dirty lockfile should be resolved before
  the next release operation, but it is not a 0.198 API/semantic blocker
- Recommended next step: close 0.198 and start 0.199 as the boundary and
  architecture audit line
- Confidence: high

## Scope Audited

Code areas:

- Public fluent load facade in `crates/icydb/src/db/session/load.rs`
- Core fluent paging in `crates/icydb-core/src/db/query/fluent/load/pagination.rs`
- Read-intent authority in `crates/icydb-core/src/db/query/read_intent.rs`
- Public re-exports in `crates/icydb/src/db/mod.rs` and
  `crates/icydb-core/src/db/mod.rs`
- Direct-query and generated/internal boundary docs in
  `crates/icydb/src/db/query/mod.rs` and `crates/icydb/src/db/session/mod.rs`
- Read-admission invariant script

Docs/examples:

- `README.md`
- `docs/guides/public-facade-api.md`
- `docs/guides/read-intent.md`
- `docs/contracts/READ_ADMISSION.md`
- `docs/contracts/QUERY_PRACTICE.md`
- `docs/design/0.198-read-intent-ergonomics/0.198-design.md`
- `docs/design/0.198-read-intent-ergonomics/0.198-supplemental-status.md`
- `docs/design/0.198-read-intent-ergonomics/0.198-api-feedback.md`
- `docs/changelog/0.198.md`
- `CHANGELOG.md`

Not reproduced:

- Full workspace test suite.
- Wasm builds and raw wasm-size deltas.
- SQL performance matrices.
- Runtime performance deltas; 0.198.13 is an API/read-intent cleanup, not a
  performance claim.

## Commands Run

| Command | Result | Notes |
| --- | --- | --- |
| `git status --short` | Pass | Dirty only in `Cargo.lock`. |
| `git branch --show-current` | Pass | `main`. |
| `git rev-parse HEAD` | Pass | `1d2e3d652b4c5db9d61915bf39dde9fc0b139ce9`. |
| `git log -1 --oneline` | Pass | `1d2e3d652 0.198.13`. |
| `git diff --stat` | Pass | Only `Cargo.lock` before audit artifacts. |
| `git diff -- Cargo.lock` | Pass | `memchr 2.8.2 -> 2.8.3`. |
| `rg` public-surface and docs searches | Pass | No active public fluent legacy surface found. |
| `cargo fmt --all --check` | Pass | Formatting clean. |
| `git diff --check` | Pass | No whitespace errors. |
| `bash scripts/ci/check-read-admission-invariants.sh` | Pass | New page vocabulary guarded. |
| `cargo check -p icydb-core -p icydb -p icydb-cli` | Pass | Uses current dirty `Cargo.lock`. |
| `cargo test -p icydb-core --test compile` | Pass | Trybuild pass/fail guards passed. |
| `cargo test -p icydb-core --all-features default_fluent_page` | Pass | Page admission tests passed. |
| `cargo test -p icydb-core --all-features fluent_paged_load` | Pass | Fluent page continuation tests passed. |
| `cargo test -p icydb-cli renders_query_read_admission` | Pass | CLI guidance tests passed. |
| `cargo test -p icydb-core --all-features` | Pass | 4336 passed, 5 ignored; trybuild passed. |
| `cargo test -p icydb --lib --all-features` | Pass | 70 passed. |
| `make clippy` | Pass | Invariants, feature matrix, and workspace Clippy passed. |

Note: three focused Cargo filters were initially started in parallel and waited
on Cargo locks. They all completed successfully. No further Cargo work was run
in parallel.

## Closeout Gate Summary

| Gate | Status | Evidence | Blocking? |
| --- | --- | --- | --- |
| Public fluent legacy surface | Pass | `FluentLoadQuery` exposes `page(limit)`, `next_page(limit, cursor)`, `partial_window`, semantic terminals; no public fluent load `.limit`, `.offset`, `.one`, `.all`. | No |
| Page ergonomics | Pass | `icydb` facade returns `PagedResponse<E>` directly; core returns `PagedLoadExecution<E>` directly. | No |
| Public `PageRequest` removal | Pass | `PageRequest` is `pub(in crate::db::query)` and no longer re-exported. | No |
| Public `PagedLoadQuery` removal | Pass | `PagedLoadQuery` is private in core pagination module and no longer re-exported. | No |
| Partial window boundary | Pass | `PartialWindowLoadQuery` remains the deliberate partial-read path and does not expose semantic page/complete/aggregate terminals. | No |
| Complete reads | Pass | `collect_complete()` is documented and guarded as complete-small-set terminal. | No |
| Exact aggregates | Pass | Exact aggregate helpers are documented and compile through the public surface. | No |
| Trusted/admin lane | Pass | `admin_batch(...)` requires `trusted_read_unchecked()` in core and docs keep trusted maintenance in Tier 2. | No |
| Direct query boundary | Pass | `Query` is `#[doc(hidden)]`; direct execution is documented as Tier 4/internal-generated, not endpoint recipe. | No |
| SQL/fluent boundary | Pass | Generated SQL remains controller/admin; read-admission invariants check no `sql.public_read` path. | No |
| Diagnostics | Pass | CLI guidance now says `page(...)`, `next_page(...)`, semantic terminals, and `partial_window(...)`, not raw `LIMIT`. | No |
| Docs/examples | Pass | Active README/guides/contracts teach final API. Historical changelog/review notes preserve old text as history. | No |
| Validation | Pass | Focused and broad checks passed. | No |
| Dirty worktree | Non-blocking issue | `Cargo.lock` has unrelated `memchr` update. | No for 0.198; yes before a clean release tree |

## Public Fluent Legacy Surface Check

| Legacy surface | Remaining hits? | Classification | Blocker? |
| --- | ---: | --- | --- |
| `.limit(...)` on public fluent load | No | Removed from public facade and production core fluent load. Low-level `Query<E>::limit` remains direct-query/SQL/planner primitive. | No |
| `.offset(...)` on public fluent load | No | Removed from public facade and production core fluent load. Low-level `Query<E>::offset` remains direct-query/SQL/planner primitive. | No |
| `.one()` | No | `try_one()` remains the exact row terminal. Other `one` hits are unrelated internals. | No |
| `.all()` | No | Remaining `.all(...)` hits are Rust iterator calls or historical docs. | No |
| Public `PageRequest` | No | Internal only. Historical changelog/review notes mention old state. | No |
| Public `PagedLoadQuery` | No | Private core staging type only. Historical notes mention old state. | No |

## Read-Intent Surface Check

| Intent | Preferred API | Verified? | Evidence | Notes |
| --- | --- | ---: | --- | --- |
| Exact row | `by_id(id).try_one()` | Yes | Public facade guide, read-intent guide, compile checks. | Exact primary-key filter canonicalization remains 0.197 authority. |
| Exact rows | `by_ids(ids).execute_rows()` | Yes | Public facade guide. | `execute_rows()` is still valid for exact key sets. |
| Public first page | `page(limit)?` | Yes | Facade/core method signatures and docs. | Executes directly. |
| Public next page | `next_page(limit, cursor)?` | Yes | Facade/core method signatures and tests. | Cursor is opaque. |
| Complete small set | `collect_complete()` | Yes | Docs and read-admission invariants. | Fails instead of truncating. |
| Deliberate partial rows | `partial_window(n).execute_rows()` | Yes | Public facade docs and wrapper type. | Not a page and not complete. |
| Exact aggregate | `*_exact(...)` helpers | Yes | Docs, compile-pass guard, core tests. | Fieldless ID extrema use `min_id_exact`/`max_id_exact`. |
| Trusted batch | `trusted_read_unchecked().admin_batch(...)` | Yes | Core runtime check and Tier 2 docs. | Batch size remains engine-owned. |

## SQL/Fluent Boundary

| Area | Result | Evidence | Risk |
| --- | --- | --- | --- |
| Generated SQL controller gate | Pass | `check-read-admission-invariants.sh` and `make clippy`. | Low |
| No `sql.public_read` | Pass | Invariant script forbids parser/generated public SQL config. | Low |
| Caller-controlled SQL guidance | Pass | `READ_ADMISSION.md`, public facade guide, read-intent guide. | Low |
| SQL/fluent diagnostic alignment | Pass | CLI diagnostic tests and read-admission common rejection table. | Low |

## Direct Query Surface

| Surface | Classification | Public risk | Recommendation |
| --- | --- | --- | --- |
| `Query<E>` | Hidden advanced type | Low | Keep hidden; normal endpoint docs should continue to teach fluent terminals. |
| `DbSession::execute_query` | Tier 4/internal-generated/direct-query surface | Low | Acceptable for 0.198 because it uses default bounded admission and is not taught as a recipe. |
| `Query<E>::limit/offset` | Low-level direct-query primitive | Low | Acceptable because public fluent load does not expose it. Recheck in 0.199 boundary audit. |

## Diagnostics And EXPLAIN

| Area | Result | Evidence | Risk |
| --- | --- | --- | --- |
| `QueryIntent` ambiguous-shape errors | Pass | Read-admission tests and diagnostics. | Low |
| `ReadIntentKind` reporting only | Pass | `read_intent.rs` docs state metadata only. | Low |
| Semantic EXPLAIN helpers | Pass | Public docs list exact aggregate/existence explain helpers. | Low |
| Admission diagnostic details | Pass | CLI diagnostic tests passed. | Low |

## Docs And Examples

| File/Area | Result | Issue | Blocking? |
| --- | --- | --- | --- |
| README | Pass | Uses `.page(10)` directly. | No |
| Public facade guide | Pass | Teaches Tier 1 read-intent API and keeps trusted/direct-query out of normal recipes. | No |
| Read-intent guide | Pass | Teaches exact row, page/next page, complete, partial window, exact aggregate, trusted batch. | No |
| Read-admission contract | Pass | Uses final public page vocabulary. | No |
| 0.198 supplemental status | Pass | Declares the closed surface. | No |
| 0.198 detailed changelog | Pass with history caveat | Older patch entries intentionally preserve intermediate `PageRequest` history; 0.198.13 supersedes at top. | No |
| Raw ChatGPT evaluation note | Pass with history caveat | Marked historical and not current API authority. | No |

## Priority Findings

| Priority | Finding | Risk | Size | Recommendation |
| --- | --- | --- | --- | --- |
| P0 | None | None | N/A | 0.198 has no closeout blockers. |
| P1 | None | None | N/A | No final 0.198.14 blocker patch needed. |
| P2 | Dirty `Cargo.lock` updates `memchr` from `2.8.2` to `2.8.3` | Could surprise the next release/check if left unclassified. | XS | Decide whether to keep or discard that lockfile drift before starting release or 0.199 work. |
| P3 | Historical 0.198 changelog/review notes still contain old `PageRequest` examples | Grep noise only; not active guidance. | N/A | Do not rewrite release history; rely on 0.198.13 section and active guides. |

## Required 0.198.14 Patches

No 0.198.14 blocker patches required.

## Deferred To 0.199

- Boundary audit for low-level `Query<E>` direct execution, public docs,
  generated/internal tooling, diagnostics, and cache/admission ownership.
- Optional docs polish: if old historical notes become confusing in generated
  documentation output, add a top-level "historical notes" banner to the
  detailed 0.198 changelog rather than rewriting previous patch entries.
- Optional API hygiene audit for whether direct-query surfaces should remain
  `#[doc(hidden)]` or move deeper behind generated/internal modules before 1.0.

## Final Recommendation

Close 0.198. The line achieved the intended hard-cut read-intent API:

- normal public reads use semantic terminals;
- public list endpoints use `page(limit)?` / `next_page(limit, cursor)?`;
- complete reads use `collect_complete()`;
- exact aggregate helpers are semantic;
- deliberate partial windows use `partial_window(...)`;
- trusted maintenance uses an explicit trusted lane;
- SQL and direct-query surfaces remain admin/internal/advanced.

The only action before continuing is housekeeping: resolve the current
`Cargo.lock` drift deliberately. It does not block closing 0.198.
