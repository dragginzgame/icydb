# IcyDB Changelog

All notable, and occasionally less notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).


## [0.86.x] 🧪 - 2026-04-16 - Grouped HAVING Expression Widening

- `0.86.2` closes the main `0.86` fallout pass by updating the new grouped `HAVING` continuation-signature snapshot, cleaning the small clippy follow-through exposed by the explain/fingerprint widening, and proving the grouped-focused `icydb-core` sweep is green again.
- `0.86.1` follows the grouped `HAVING` widening through the remaining observability seams, so `EXPLAIN` now shows grouped `HAVING` as the real expression tree instead of the old clause-only shape, and grouped continuation signatures now distinguish widened `HAVING` expression structure correctly.
- `0.86.0` widens grouped SQL `HAVING` from simple symbol-to-literal checks to bounded post-aggregate expressions like `ROUND(AVG(strength), 2) >= 10` and `COUNT(*) + 1 > 5`, and it also fixes grouped query cache identity so those widened `HAVING` shapes do not collide in the shared query cache.

See detailed breakdown:
[docs/changelog/0.86.md](docs/changelog/0.86.md)

---

## [0.85.x] 🧮 - 2026-04-16 - Grouped Post-Aggregate Projection Expressions

- `0.85.3` keeps the executor cleanup moving by making prepared execution inputs own stream resolution and single-attempt materialization more directly, so the kernel is now mostly just retry policy while the fast-path and scalar read boundaries read as clearer, more deliberate owners.
- `0.85.2` follows through on the grouped and executor cleanup by consolidating grouped aggregate/projection ownership, moving the compiled grouped projection contract back under the projection boundary, and tightening the scalar retry/materialization split so the post-`0.85.0` executor seams read as deliberate owners instead of fallout patches.
- `0.85.1` merges the old perf-attribution and structural-read feature flags into one gated `diagnostics` surface, so local attribution, read diagnostics, and perf harness builds now opt into one explicit instrumentation feature instead of two overlapping ones.
- `0.85.0` widens grouped SQL `SELECT` projection so you can now compute bounded scalar expressions over grouped keys and finished aggregate results, including forms like `COUNT(*) + MAX(age)` and `ROUND(AVG(age), 2)`, and it also breaks the scalar page materializer into focused planning, scan, post-scan, retained-slot, and metrics modules so the executor keeps the same behavior through clearer internal boundaries.

See detailed breakdown:
[docs/changelog/0.85.md](docs/changelog/0.85.md)

---

## [0.84.x] 🧊 - 2026-04-16 - Prepared Execution Residents

- `0.84.4` closes the line by finishing the shared scalar executor cleanup: scalar page reads and the cursorless short path now resolve explicit execution strategies once instead of re-reading retained-layout and residual-scan flags at each late phase, and the local bootstrap/perf harness follow-through keeps the new attribution tooling clean for recurring audit runs.
- `0.84.3` follows the grouped attribution line through the remaining hot repeated `GROUP BY age, COUNT(*)` row and also turns the scalar page executor into a resolved-strategy path, so grouped repeated rows get cheaper and scalar read behavior is now decided once through explicit direct-row, kernel-scan, post-access, and final-payload strategies instead of being re-derived across page execution.
- `0.84.2` follows the resident and grouped-attribution work by trimming a broad slice of duplicate query-planner, validation, expression, and fingerprint scaffolding, so the `db/query` planning boundary is smaller and easier to maintain without changing query behavior.
- `0.84.1` turns grouped runtime into a measured and then tightened path: SQL and fluent perf audits now expose grouped stream/fold/finalize plus grouped `COUNT(*)` fold metrics, and the follow-through trims repeated grouped fold, one-slot grouped row-read, and single-field grouped hash work enough to make the repeated grouped SQL row materially cheaper than its original baseline.
- `0.84.0` starts the line by freezing shared prepared SQL projection, scalar retained-slot, and grouped prep/layout residents onto prepared plans, which cuts repeated SQL setup work without widening query behavior.

See detailed breakdown:
[docs/changelog/0.84.md](docs/changelog/0.84.md)

---

## [0.83.x] 🧱 - 2026-04-15 - Serialization Boundary Cleanup

- `0.83.5` finishes the last cold fluent compile cleanup by moving access-choice ranking behind the explain boundary, narrowing scalar load route preparation, and then trimming more executor, query, fluent, and access-choice wrapper duplication so the remaining `0.83` compile-path cleanup leaves less internal indirection behind.
- `0.83.4` makes the local SQL shell easier to read by rendering `DESCRIBE` fields, indexes, and relations as padded ASCII tables, and it keeps trimming executor, query-cache, and explain-wrapper indirection so shared planning and runtime paths carry fewer redundant internal contracts.
- `0.83.3` follows the binary cutover by removing ambient `Serialize` policy from core and schema generation, hardening the new structural binary owners with broader malformed-payload coverage, cleaning a few remaining query encode/decode redundancies, and tightening several type-local codecs so `Ulid`, `Date`, `Timestamp`, `Principal`, and `Account` use smaller or less allocation-heavy runtime byte paths.
- `0.83.2` is the hard cut that replaces the last CBOR-shaped structural field contracts with engine-owned structural binary codecs, deletes the generic `serde_cbor` and `crate::serialize` layers from `icydb-core`, removes the final persisted-row CBOR semantics and stale CBOR naming, and tightens legacy `PersistedRow` so old-style derives must use explicit storage hints instead of silently falling back to CBOR.
- `0.83.1` continues the serialization cleanup by moving migration state onto an explicit binary codec, simplifying `Value` decoding and structural encoding ownership, teaching the legacy `PersistedRow` derive more exact field kinds plus explicit decimal scale hints so more old-style typed rows can avoid the CBOR fallback seam, and making executor `bytes()` sizing use the same owner-local value-storage encoding as the rest of the runtime instead of counting CBOR bytes separately.
- `0.83.0` starts the line by moving cursor tokens and several DB-owned persisted-value paths off the generic serializer, centralizing the remaining runtime CBOR helper behind one DB codec boundary, and proving that more of the storage runtime can use explicit engine-owned codecs instead of generic CBOR-by-default plumbing.

See detailed breakdown:
[docs/changelog/0.83.md](docs/changelog/0.83.md)

---

## [0.82.x] 🧭 - 2026-04-15 - Deterministic Compiled Query Reuse

- `0.82.10` makes the new covering SQL read path easier to trust and cheaper to inspect: the local SQL shell now separates engine work from shell render time, pure covering queries now show how much executor time is spent decoding covered values versus assembling rows, and several narrow covering-query cuts trim more overhead from ordered `SELECT id ...` and `SELECT id, name ...` rows without reopening row-store fetches.
- `0.82.9` widens two narrow SQL read slices: index-ordered SQL projections now stay on cheap selective or pure covering read paths much more often, including key-only ordered rows that can now avoid row-store fetches entirely, and grouped SQL now admits computed projections plus additive grouped-key ordering like `GROUP BY age ORDER BY age + 1`, with descending grouped pages and cursors now respecting that grouped order correctly instead of silently falling back to ascending key order.
- `0.82.8` restores the typed public write API so single-row writes return the saved entity directly, batch writes return plain `Vec<_>`, and plain typed deletes return row counts instead of a SQL-style mutation wrapper, bringing the facade back in line with the core typed write contract, and updates the published crate description so IcyDB is described as a schema-first typed query and persistence runtime rather than an ORM.
- `0.82.7` makes generated create-input names less awkward by moving the concrete generated type to `<Entity>_Create` while exposing a stable downstream `icydb::Create::<Entity>` generic alias, trims one more narrow executor hot path by making direct-field `ORDER BY` on raw-row direct lanes decode only the ordered slots instead of opening the general structural row reader, splits the local SQL shell perf footer into `c/p/s/e/d` so physical store/index access no longer hides inside the broader executor bucket, and adds built-in `?` / `help` shell commands so the CLI can explain that perf legend directly.
- `0.82.6` makes debug output easier to read by formatting ULIDs as their normal 26-character string, tightens the direct raw-row executor lane so simple route-ordered fluent reads stop scanning once they have the final `OFFSET/LIMIT` page window, and fixes typed save preflight so non-empty `List` / `Set` fields containing nested structured values no longer fail closed with a spurious field-type mismatch during insert or update.
- `0.82.5` keeps the executor cleanup line focused on real runtime ownership instead of broader cleverness: residual scan contracts are now explicit, more no-cursor fluent read families, including filtered ordered raw-row loads, can stay on direct raw-row paths, executor lane metrics now show much more precisely which fluent loads still need the heavier retained-row materialization path before the next perf cut, the local SQL shell separates successful commands with clearer whitespace and a denser perf footer bar, `EXPLAIN EXECUTION` now renders as a readable phase-and-tree report with visible SQL projection materialization hints, and grouped SQL typos like misspelled projected fields now report `unknown field` instead of a misleading generic `GROUP BY` shape error.
- `0.82.4` trims more warmed executor overhead by removing unnecessary projection-validation and retained-slot setup from scalar read paths, and adds a direct raw-row SQL projection short path so simple field-only `SELECT` queries do less projection work before rendering rows.
- `0.82.3` makes duplicate SQL global aggregate projections like repeated `COUNT(...)` columns reuse one compiled aggregate terminal instead of recomputing the same reduction for every repeated output slot, keeps the original SQL column order and labels unchanged, and makes blob values show up in raw debug/error output as `Blob(N bytes)` instead of an unreadable byte array.
- `0.82.2` makes SQL `COUNT(*)` and `COUNT(non-null_field)` reuse the same shared count route as fluent queries instead of materializing rows just to count them, and adds a small fast-path inventory plus guard test so those shared route boundaries are documented and harder to regress.
- `0.82.1` makes cache compatibility more explicit by versioning the shared and SQL cache keys, proves those version and query/update boundaries fail closed in tests, adds isolated cold-vs-warm PocketIC perf rows that show repeated query reuse now cuts the same logical read by about `22%` on fluent and about `30%` on SQL after an update warm, fixes the local Rust SQL shell so grouped queries render rows again instead of failing on the empty grouped continuation cursor JSON shape, and now lets bounded global aggregate lists like `SELECT MIN(age), MAX(age) ...` return one row instead of failing closed.
- `0.82.0` makes the shared lower query-plan cache use one explicit canonical structural cache key instead of a hidden manual hash walk, keeps SHA-256 on deterministic identity boundaries and `xxh3` on in-heap cache hashing, and records the remaining manual hashing cleanup targets for later follow-up instead of widening the cache model further.

See detailed breakdown:
[docs/changelog/0.82.md](docs/changelog/0.82.md)

---

## [0.81.x] ↕️ - 2026-04-14 - Bounded Computed ORDER BY Aliases

- `0.81.4` cleans up the shared lower query-plan cache so SQL and fluent now reuse one explicit canonical structural cache key instead of hiding that identity inside a manual hash walk, without widening the SQL surface again.
- `0.81.3` closes out the line with security-audit hygiene: the recurring security-boundary audit now points at the current test names, and the live SQL canister suite explicitly checks that malformed SQL is rejected before execution at the public boundary.
- `0.81.2` removes the last alias-only restriction from the same bounded scalar ordering slice, so direct SQL forms like `ORDER BY age + 1`, `ORDER BY age + rank`, and `ORDER BY ROUND(age / 3, 2)` now work on the existing canonical computed-order path, and the same line also adds the first recurring security-boundary audit plus CI-script updates so that audit follows the current module layout cleanly.
- `0.81.1` hardens the new bounded computed `ORDER BY <alias>` slice by proving alias normalization reaches the same cached internal query shape, widening tests to cover field-to-field arithmetic and `ROUND(field + field, scale)` alias ordering, and adding a matching SQL perf-audit row so that family is measured in the recurring PocketIC harness.
- `0.81.0` lets SQL `ORDER BY <alias>` reuse one small admitted computed scalar family, so aliases like `next_age` or `rounded_age` now work when they come from bounded arithmetic or `ROUND(...)` projection items without opening direct expression ordering or grouped computed ordering.

See detailed breakdown:
[docs/changelog/0.81.md](docs/changelog/0.81.md)

---

## [0.80.x] 🧠 - 2026-04-14 - Compiled SQL Cache

- `0.80.3` widens one small SQL projection boundary so scalar `SELECT` lists can now do bounded field-to-field arithmetic like `dexterity + charisma AS total`, and it keeps `ROUND(..., scale)` output fixed to the requested decimal places even when that projection is aliased.
- `0.80.2` extends the local Rust SQL shell perf footer to show `comp`, `plan`, and `exec` separately, moves the repeated-query caches into canister-lifetime heap state so update calls can warm later query calls, and updates the dedicated SQL/fluent perf audits so they now show which cache layer actually handled each query instead of only one combined instruction total.
- `0.80.1` moves the new repeated-query cache down to a shared lower query-plan boundary so SQL and fluent reads can reuse the same session-local plan work, and it adds a separate fluent PocketIC perf audit plus baseline so both frontends can be measured on the same fixture/index surface without turning one audit into a dual-purpose harness.
- `0.80.0` starts the compiled SQL cache line by splitting SQL work into an explicit compile step and execute step, so the engine now has one concrete semantic command artifact to reuse later without claiming any cache hits or repeated-query speedups yet.

See detailed breakdown:
[docs/changelog/0.80.md](docs/changelog/0.80.md)

---

## [0.79.x] ⚖️ - 2026-04-13 - Predicate Expressions

- `0.79.5` adds a dedicated SQL performance audit surface with its own canister, schema fixtures, and PocketIC harness, so recurring instruction checks can cover a broad stable matrix of SQL query shapes without reusing smoke or parity test fixtures for a second purpose.
- `0.79.4` adds two more bounded predicate SQL ergonomics wins on the existing surface: plain-field `IS NOT TRUE` / `IS NOT FALSE`, plus field-bound `BETWEEN` / `NOT BETWEEN` so sibling-field range checks like `strength BETWEEN dexterity AND charisma` reuse the same compare-fields path instead of opening generic expression predicates.
- `0.79.3` adds bounded prefix-only `ILIKE` and `NOT ILIKE` on the existing predicate SQL surface, so case-insensitive prefix filters can use familiar SQL spellings while still lowering to the same small casefolded starts-with path instead of opening broader pattern matching.
- `0.79.2` adds a few small SQL ergonomics wins on the existing bounded predicate surface: prefix-only `NOT LIKE`, `<>` as an alias for `!=`, one trailing comma allowed in `IN (...)` / `NOT IN (...)`, and plain-field `IS TRUE` / `IS FALSE`, all reusing the current canonical predicate paths instead of opening broader expression support.
- `0.79.1` widens the same bounded predicate slice a little further by adding `NOT BETWEEN` plus symmetric compare normalization on the shared SQL and fluent predicate path, so outside-range filters and ergonomic forms like `5 < strength` reuse the same canonical field-first predicate model without opening arithmetic or generic predicate expressions.
- `0.79.0` starts the next SQL/fluent slice by adding bounded field-to-field predicate comparison on the shared predicate path, keeping those comparisons residual-only in planning and grouped-fail-closed for now, surfacing them clearly in explain output, letting scalar field-vs-field compares stay on the lightweight scalar predicate runtime instead of falling back to the generic structural lane, and folding the local SQL shell over to a real Rust-backed CLI with multiline input, history, boxed tables, and instruction-count footers.

See detailed breakdown:
[docs/changelog/0.79.md](docs/changelog/0.79.md)

---

## [0.78.x] 🧮 - 2026-04-13 - Simple Scalar Projection Expressions

- `0.78.2` locks the new scalar arithmetic and `ROUND(..., n)` projection slice at the shipped edges too, by adding direct public SQL result-packaging coverage and a live PocketIC SQL canister smoke test for subtraction and rounding without changing the bounded feature surface.
- `0.78.1` widens the same bounded scalar projection slice so SQL and fluent now support `+`, `-`, `*`, `/`, and explicit `ROUND(..., n)` over a single field expression, while grouped arithmetic, predicate arithmetic, order-by arithmetic, and generic expression parsing still stay intentionally fail-closed.
- `0.78.0` starts the next SQL/fluent slice by adding one bounded computed projection shape, so both SQL and fluent can now project `field + numeric_literal` through the same shared expression and evaluation path without widening grouping, filtering, ordering, or generic expression support.

See detailed breakdown:
[docs/changelog/0.78.md](docs/changelog/0.78.md)

---

## [0.77.x] 🧭 - 2026-04-12 - SQL Contract Freeze

- `0.77.6` finishes the SQL ownership freeze by making duplicated semantic decisions a hard CI failure, collapsing SQL entity matching to one owner in lowering, narrowing the internal query-lane helper so only the public SQL read/write surfaces classify statement families, and then folding several small duplicated SQL/query helpers into shared owners so the frozen surface is easier to maintain without widening it.
- `0.77.5` hardens the frozen SQL ownership model by splitting the biggest parser and lowering hotspots into smaller owner-local modules, turning session SQL execution back into a thin router, moving `INSERT ... SELECT` semantic checks fully back into lowering, and locking SQL-facing projection labels and grouped SQL result shaping under the session SQL projection boundary with a stricter CI tripwire.
- `0.77.4` finishes the query-expression cleanup by routing the remaining `ORDER BY LOWER/UPPER(...)` lane through the same canonical `Expr` path as the rest of query planning, removing the last parallel order-expression runtime path, restoring SQL lowering parity for plain field and grouped projection shapes, and locking grouped function expressions to stay explicitly fail-closed until they can reuse the shared expression evaluator.
- `0.77.3` finishes the remaining SQL edge-surface proof by adding a small end-to-end PocketIC suite for the dedicated SQL test canister, locking public SQL payload packaging directly in the `icydb` crate, consolidating duplicated SQL and executor test helpers into smaller shared suites, removing one obsolete demo-only wasm profile shim, and hard-cutting the old session-only computed SQL projection lane so bounded SQL text functions now lower and explain through the same canonical expression path as the rest of SQL projection planning.
- `0.77.2` finishes the remaining query-surface cleanup by flattening fluent grouped execution onto one `execute()` result shape, moving the last public typed query grouping branch under one core boundary, and deleting the final dead grouped cursor residue instead of keeping helper-only compatibility code around.
- `0.77.1` finishes the SQL cleanup by deleting the last old lane-shaped SQL runtime wrappers, hard-cutting the remaining dead SQL scaffolding instead of silencing it, and leaving only `execute_sql_query::<E>(...)` plus `execute_sql_update::<E>(...)` as the live SQL executors.
- `0.77.0` removes public SQL dispatch entirely, replaces the older mixed SQL entrypoint story with one single-entity SQL query executor plus one matching SQL mutation executor, and deletes the broad SQL parity canister/test scaffolding that only existed to exercise the old routed SQL surface.

See detailed breakdown:
[docs/changelog/0.77.md](docs/changelog/0.77.md)

---

## [0.76.x] 🧩 - 2026-04-11 - SQL Surface Completion

- `0.76.14` makes mutation results follow one clearer rule: `SELECT` and every admitted row-producing mutation surface now share the same row payload family (`... RETURNING` on SQL dispatch, fluent delete returning, and typed create/insert/update returning), while non-returning writes share one mutation-result family across typed create/insert/update/replace/delete surfaces.
- `0.76.13` renames the new authored insert companion surface from `TypeInsert` / `insert_typed(...)` to `TypeCreate` / `create(...)`, keeping the same generated and managed-field ownership rules while making the authored create contract read more clearly next to the older full-entity `insert(...)` path.
- `0.76.12` adds a separate typed authored insert surface (`TypeInsert` plus `insert_typed(...)`) so generated fields and managed timestamps are structurally absent from the authored write type, authored omission reaches save preflight explicitly, and the older full-entity `insert(...)` path can stay in place until typed write provenance is designed more broadly.
- `0.76.11` hardens schema insert-generated fields on the authored write surfaces that can already prove caller intent, so typed-dispatch SQL and public structural writes now reject explicit values for `generated(insert = "...")` fields on both create and rewrite lanes instead of quietly letting system-owned generated values be overwritten.
- `0.76.10` keeps reduced SQL defaults explicit by widening schema-owned `generated(insert = "...")` only to a small allowlist, letting typed-dispatch inserts synthesize `Timestamp::now` as well as `Ulid::generate`, and then finishing the managed-timestamp cleanup so `created_at` and `updated_at` are still declared automatically but now flow through one shared save/preflight path instead of separate typed and SQL timestamp owners.
- `0.76.9` moves reduced SQL insert omission and auto-managed timestamps onto explicit schema/runtime field policy, so typed-dispatch inserts can omit only schema-marked generated fields, ordinary `default = ...` values no longer act like hidden SQL defaults, auto-managed `created_at` and `updated_at` stay boilerplate-free without runtime name checks, and explicit SQL writes to those managed fields are now rejected instead of silently overwritten.
- `0.76.8` freezes the reduced SQL write-result contract by rejecting `RETURNING` with one explicit stable unsupported-feature label across parser, typed dispatch, delete surfaces, and query-only generated SQL routes, instead of quietly drifting toward broader write-output semantics.
- `0.76.7` extends the same reduced SQL write lane again by admitting narrow same-entity `INSERT ... SELECT` on typed dispatch, so field-only and admitted scalar computed-projection copy-insert flows can reuse the existing SQL read path and structural insert path while grouped and aggregate source queries stay rejected and the generated `sql_dispatch` query surface stays read-only.
- `0.76.6` keeps the SQL write lane moving by adding deterministic `UPDATE ... ORDER BY ... LIMIT/OFFSET` windows on typed dispatch, admitting single-table aliases on `INSERT`, and letting `Ulid`-key entities generate their own primary key during narrow SQL inserts, with the same behavior covered in both core tests and the PocketIC harness.
- `0.76.5` broadens the typed SQL write lane so `UPDATE ... WHERE ...` can now target rows selected by the reduced predicate surface, single-table aliases now work on that narrowed `UPDATE` surface, and the remaining blocked write tails (`UPDATE ORDER BY/LIMIT/OFFSET`, `INSERT ... SELECT`, and write-table aliases outside the admitted lane) now fail with stable explicit unsupported-feature errors that are covered in both core tests and the PocketIC SQL harness.
- `0.76.4` keeps the SQL-only line moving by adding single-table aliases for `SELECT` and `DELETE`, widening narrow `INSERT` forms on the typed dispatch lane, and then tightening the session-owned SQL boundary plus the related session test suites so the new SQL work lands on clearer ownership seams without changing query semantics.
- `0.76.3` adds a first reduced SQL write surface by admitting narrow `INSERT` and primary-key-only `UPDATE` statements on the typed dispatch lane, and also pulls SQL projection/materialization shaping back behind the session boundary so shared executor contracts stay structural while the branch-level cleanup remains local.
- `0.76.2` keeps the same SQL-only completion line moving without changing the runtime model: grouped top-level `SELECT DISTINCT ... GROUP BY ...` now normalizes away during lowering, grouped `TRIM(...)`-style projection over grouped fields now runs through the session-owned grouped SQL lane, projection aliases now work as output-label syntax, and narrow `ORDER BY` aliases now work for already-supported field and `LOWER/UPPER(...)` order targets without widening planner or runtime semantics.
- `0.76.1` delivers the first SQL-surface feature slice by fixing scalar `SELECT DISTINCT` without primary-key projection, adding both global and grouped aggregate DISTINCT qualifiers, admitting grouped `MIN/MAX(field)`, and making ordered `DELETE ... OFFSET` work through the same normalized execution stack instead of parser-only restrictions or fake fallback lanes.
- `0.76.0` opens the `0.76` SQL-surface line by locking the remaining non-`JOIN` gaps into one explicit tracker, documenting which shapes are intentionally still fail-closed, and setting the boundary for later SQL-only follow-up patches without changing execution behavior yet.

See detailed breakdown:
[docs/changelog/0.76.md](docs/changelog/0.76.md)

---

## [0.75.x] 🧭 - 2026-04-10 - Cleanup & Audits

- `0.75.8` keeps the cleanup line moving by simplifying relation validation around one shared strong-relation model, renaming the prepared execution-plan boundary to match its actual role, flattening a few more grouped aggregate/runtime helper layers, and trimming stale hidden exports and tiny helper modules across the build, schema, and facade crates without changing query behavior.
- `0.75.7` keeps trimming executor and aggregate indirection by flattening continuation and terminal helper layers, so more query execution paths now run through one shared planning/runtime boundary instead of small wrapper-only contracts.
- `0.75.6` keeps the cleanup line moving by making executor planning a real ownership boundary, collapsing more duplicated SQL and pipeline wrapper flows, and simplifying diagnostics so incompatible stored bytes are reported as corruption instead of a fake compatibility bucket, without widening the query surface.
- `0.75.5` continues the cleanup line by hard-cutting fake internal cursor versioning, collapsing a few more aggregate/relation/explain wrapper seams, and deleting several tiny helper-only query modules so the repo carries less compatibility theater and less file-level indirection before `0.76`.
- `0.75.4` finishes the current cleanup slice by removing dead non-test projection-expression scaffolding, tightening several remaining SQL/delete/session wrappers onto shared paths, and leaving `icydb-core` clean under strict all-target clippy before the next `0.76` feature work.
- `0.75.3` keeps the audit line structural by collapsing duplicated SQL projection and covering-read flows onto one shared executor path, deleting obsolete single-component scan wrappers, and moving deep perf attribution behind an opt-in feature so the default runtime is simpler before the next optimization pass.
- `0.75.2` fixes the new sparse-versus-dense row reader split so full-row queries no longer pay the lazy slot-reader setup cost, while commit and projection boundaries go back to rejecting malformed unused fields immediately instead of deferring those corruption errors until first access.
- `0.75.1` reclaims the generic SQL execute-preparation regression that had crept back into tiny projected reads, so direct `SELECT id ... ORDER BY id LIMIT 1` queries return to the projected-row fast path and run materially cheaper again without adding new route-specific shortcuts.
- `0.75.0` starts the next crosscutting cleanup line by rerunning the complexity audit, splitting several of the largest internal DB hotspot modules into smaller owner-local pieces, and finishing the static `EntityModel` pass so executor runtime now relies more on planner-frozen query metadata and authority-owned row layout contracts instead of re-deriving schema facts during execution.

See detailed breakdown:
[docs/changelog/0.75.md](docs/changelog/0.75.md)

---

## [0.74.x] 🧹 - 2026-04-10 - Redundancy and Ownership Audit

- `0.74.12` finishes the remaining generic projection/materialization cleanup by consolidating shared prepared projection state, deleting duplicated structural SQL fallback orchestration, and keeping the tiny direct SQL projection benchmark about `7.9%` below the original `0.74` audit baseline without adding any new route-specific shortcuts.
- `0.74.11` finishes the tiny-query fixed-cost cleanup by moving SQL projection/source preparation out of execute and into prepared runtime state, so direct covering `SELECT id ... ORDER BY id LIMIT 1` reads do less repeated setup work and now land about `10%` below the original `0.74` audit baseline.
- `0.74.10` hard-cuts generated schema trust and the remaining runtime validation residue for entity/index/field metadata, moves filtered-index predicate parsing to build time, and trims more parser plus executor fixed cost, so the same tiny direct SQL projection query is already materially cheaper before the final prepared-state refactor.
- `0.74.9` finishes the grouped executor cleanup by collapsing grouped finalize onto one shared group table, fusing grouped paging into that path, and switching bounded grouped pages from full-result sorting to bounded selection, so grouped queries do less duplicate work before projection and cursor paging.
- `0.74.8` cuts several hot query-path costs in grouped execution, sorting, ranking, and SQL row materialization, so grouped lookups and grouped `COUNT(*)` spend less time building temporary keys, ordered reads do less repeated slot work, bounded queries avoid more wasted rescans and full-result sorting, and ranking terminals stop cloning whole rows just to compare one field.
- `0.74.7` expands the SQL perf-audit fixture cohort and removes a perf-harness wrapper tax that was making non-grouped queries pay grouped-metrics shaping cost even when no grouped execution happened, so the broader PocketIC benchmark surface is more representative and its non-grouped samples no longer carry that fake fixed overhead.
- `0.74.6` continues the cleanup line in commit and mutation execution by trimming commit-guard payload retention, collapsing more commit/recovery forwarding shells, and flattening several `commit_window` helper seams so rollback, preflight, and delete-metric plumbing now live on fewer duplicated wrappers.
- `0.74.5` finishes the explain and payload cleanup pass by removing more grouped and aggregate explain mirrors, centralizing descriptor projection, and trimming extra non-grouped route-payload wrappers so explain and runtime carry fewer duplicate copies of the same execution facts.
- `0.74.4` makes the grouped boundary harder to bypass by carrying the dedicated grouped `COUNT(*)` fold choice as an explicit planner-to-runtime contract, dropping planner strategy from grouped runtime stage payloads, and adding tripwires so grouped runtime cannot quietly drift back into direct planner-strategy inspection.
- `0.74.3` finishes the grouped boundary pass by centralizing planner-to-route grouped mode projection on one explicit route contract, adding assertions that grouped routes never drift from that projection, and locking the mode mapping directly in tests so grouped explain and metrics stay aligned with the same execution-mode vocabulary.
- `0.74.2` continues the grouped cleanup by tightening the planner-versus-route boundary, so grouped route observability and metrics now consume one explicit route-owned execution mode projected from planner strategy and route capabilities, and grouped routes fail closed instead of silently inventing missing grouped mode labels.
- `0.74.1` follows the grouped audit with the first real cleanup pass, trimming leftover grouped planner/runtime compatibility seams so grouped strategy, grouped aggregate-family routing, metrics, and fingerprint hashing now flow through fewer parallel abstractions without widening grouped query behavior.
- `0.74.0` starts the cleanup line by inventorying the grouped planning stack, classifying which planner/runtime/explain/fingerprint seams are canonical versus transitional, and naming the first grouped cleanup targets so later deletions stay surgical instead of becoming a vague refactor bucket.

See detailed breakdown:
[docs/changelog/0.74.md](docs/changelog/0.74.md)

---

## [0.73.x] 🧮 - 2026-04-09 - Grouped Aggregate Planning

- `0.73.2` finishes the next grouped-planning structural step by making the planner carry one explicit grouped aggregate-family profile into grouped runtime and continuation signatures, then locking grouped `SUM(field)` and `AVG(field)` cursor-window behavior on the broader parity and integration surfaces before widening to more grouped families.
- `0.73.1` widens the first admitted grouped aggregate cohort so indexed single-key grouped reads, including simple fully indexable filtered shapes, can keep `COUNT(field)`, `SUM(field)`, and `AVG(field)` on the ordered grouped planning path instead of falling back to the generic hash-group route.
- `0.73.0` starts the grouped aggregate planning line by making grouped strategy and grouped fallback reasons planner-owned artifacts that execution, fingerprints, and `EXPLAIN` all project from directly, and by exposing that grouped fallback story on the public grouped explain surfaces without widening grouped route-admission policy yet.

See detailed breakdown:
[docs/changelog/0.73.md](docs/changelog/0.73.md)

---

## [0.72.x] 🎯 - 2026-04-08 - Deterministic Planning & Index Exploitation

- `0.72.4` is a small follow-up that updates executor pipeline snapshots for the new ordered-route diagnostics introduced in `0.72.3`, keeping the deterministic-planning line green without changing planner behavior.
- `0.72.3` finishes the `0.72` line by making ordered-route fallback reasons explicit in `EXPLAIN`, so verbose planning now tells you when a chosen route stays direct, when it needs the shared materialized boundary, and when it must fail closed to a materialized sort, without widening planner policy or adding new route-admission heuristics.
- `0.72.2` broadens bounded ordered reads on the same single-entity planner line by keeping more admitted `ORDER BY ... LIMIT/OFFSET` windows, including the simple single-field fallback lane, on direct index-backed routes, mirrors those windows across core/parity/PocketIC coverage, and fixes one nasty session-test fixture bug where competing indexes accidentally shared the same physical runtime identity.
- `0.72.1` extends that deterministic-planning line to admitted composite ordered-read cohorts, locking both ascending and descending choice across planner/session/SQL surfaces and making the desc equality-prefix lane fail closed to materialized ordering instead of pretending it keeps the ascending fast path.
- `0.72.0` makes single-entity read planning choose between competing visible indexes with one stable ranking order, and locks that choice across planner tests, session explain, generated SQL parity, and PocketIC without changing index-visibility rules or reintroducing runtime correctness checks.

See detailed breakdown:
[docs/changelog/0.72.md](docs/changelog/0.72.md)

---

## [0.71.x] 🧱 - 2026-04-08 - Aggregate Execution Simplification

- `0.71.2` keeps aggregate behavior unchanged but makes more aggregate reads and aggregate `EXPLAIN` calls cheaper by keeping slot-only decoded rows sparse through projection, bytes, extrema, and numeric paths, cutting several typed SQL aggregate and fluent aggregate explain instruction counts by about `0.6%` to `1.6%`.
- `0.71.1` follows up the aggregate execution cleanup by adding public fluent `EXPLAIN` coverage for numeric and projection/distinct aggregate helpers, wiring the `icydb` facade back up to the same aggregate/planning introspection surface as core, and extending perf coverage for those public explain methods without changing aggregate semantics.
- `0.71.0` starts the aggregate execution simplification line by making typed SQL and fluent aggregate terminals prepare their runtime shape once and then project execution from that prepared strategy across scalar, numeric, order-sensitive, and projection/distinct families, without changing planner-visible index rules or aggregate semantics.

See detailed breakdown:
[docs/changelog/0.71.md](docs/changelog/0.71.md)

---

## [0.70.x] 🧭 - 2026-04-07 - Planner-Gated Index Visibility

- `0.70.7` closes the `0.68` to `0.70` design line in docs by marking the earlier covering-read plans as historical groundwork, recording `0.70` planner-gated visibility as the implemented current model, and clarifying that the discarded snapshot/authority notes are no longer active backlog.
- `0.70.6` finishes the cleanup around planner-visible `Ready` indexes by making the visible-index boundary explicit in planning, extending the same fallback rule to filtered aggregate `EXPLAIN` surfaces, and simplifying the aggregate terminal and numeric runtime shapes while the last matrix checks stayed effectively flat.
- `0.70.5` completes the new Option A direction for session-backed reads by hiding non-`Ready` indexes from planning entirely, so execution no longer carries index-correctness checks, `SHOW INDEXES` now reports `ready|building|dropping`, and building indexes now fall back to ordinary full-scan/materialized routes across core tests, generated SQL parity, and PocketIC.
- `0.70.4` keeps the current covering-authority rules unchanged but moves secondary-read authority resolution onto one immutable snapshot taken from the live registry/store boundary, while also broadening aggregate query coverage and PocketIC perf baselines so the typed aggregate lane now has locked success, empty-window, reject, and explain behavior without widening policy by analogy.
- `0.70.3` keeps the new `0.70.2` authority cohorts intact but finishes the refactor by making one resolved profile the only production source of probe-free covering behavior, leaving the flat authority labels as an optional derived view instead of a second behavior source.
- `0.70.2` centralizes the single-component covering line behind one shared flat classifier, extends that same flat authority surface to one proven witness-backed composite family, and keeps stale composite/storage-witness reads on their older conservative route instead of widening policy by resemblance.
- `0.70.1` does not change query execution, but it makes the new index-validity authority rules much easier to inspect and safer to regress-test by surfacing covering-route downgrade reasons on `EXPLAIN`, showing runtime index state on inspection surfaces, and locking the invalid-index fallback across core tests, generated SQL parity, and PocketIC.
- `0.70.0` starts the new read-path lifecycle line by giving indexes explicit `Building` / `Ready` / `Dropping` states and by requiring a `Ready` index before any probe-free covering read may use the newer witness-backed execution paths, while leaving aggregate index shortcuts on their older rules for now.

See detailed breakdown:
[docs/changelog/0.70.md](docs/changelog/0.70.md)

---

## [0.69.x] 🧭 - 2026-04-07 - True Covering Read Execution

- `0.69.9` finishes the next narrow stale `CustomerOrder WHERE priority = 20 ORDER BY status ...` storage-witness follow-up, so three more equality-prefix index-backed reads keep the same stale-row-filtered results but skip the extra row-presence probe and run about `12%–21%` cheaper on the exact same query.
- `0.69.8` broadens the new stale-row storage witness to a few narrow `CustomerOrder` index-backed reads, so those queries keep the same filtered output after stale index entries but skip the extra row-presence probe and run about `6.7%–7.6%` cheaper on the exact same query.
- `0.69.7` adds a narrow storage-level witness for two stale `Customer ORDER BY name` index-backed reads, so those queries keep the same filtered output but skip the extra row-presence probe and run about `5.5%` cheaper on the exact same query.
- `0.69.6` is a small follow-up to the `0.69.5` test-harness work that keeps the PocketIC SQL integration suite building against the newer `canic-testkit` fixture API, without changing query behavior or the covering-read authority rules.
- `0.69.5` finishes the current secondary covering authority audit by locking more already-truthful witness-backed SQL routes, instrumenting stale-row checks on the remaining fallback path, and confirming that the next real speed win needs a true storage-level existence witness rather than more executor-side reshuffling.
- `0.69.4` keeps the current covering-read routes intact, makes narrow row-backed SQL reads about `6%–10%` cheaper by validating every stored field without fully materializing untouched complex fields, and also widens one honest store-backed composite `ORDER BY` covering route so `CustomerOrder ORDER BY priority, status, id` now avoids the extra row check and runs about `7.5%` cheaper on the exact same query.
- `0.69.3` follows up the `0.69` executor work by fixing generated canister memory registration on the newer `canic-memory` API and by splitting the old mixed demo/test canister layout into clearer `demo`, `test`, and `audit` surfaces, so generated startup keeps working and routine test runs no longer depend on the heavyweight RPG demo canister.
- `0.69.2` keeps the shipped `0.69` witness-backed covering routes unchanged, makes persisted-row serializer/re-emission work cheaper, and cuts about `7%–13%` from several shared `Character` and `ActiveUser` index-covered SQL reads by letting the shared execution kernel materialize those covering windows directly instead of first building a generic ordered key stream.
- `0.69.1` keeps the new `0.69` covering-read work intact while cleaning up IcyDB’s memory bootstrap and commit-slot wiring around `canic-memory 0.25.10`, so generated canisters, commit storage, and facade SQL tests now use the smaller public memory API end to end instead of mixing public helpers with lower-level registry calls.
- `0.69.0` turns covering reads into a real execution route instead of planner-only metadata, proves when simple primary-key reads can skip row checks entirely, and adds the first explicit witness-backed secondary read cohorts so common `ORDER BY name` and filtered composite index reads can avoid the extra row-existence probe when the engine can prove the index and row stores are synchronized.

See detailed breakdown:
[docs/changelog/0.69.md](docs/changelog/0.69.md)

---

## [0.68.x] 📚 - 2026-04-04 - First-Class Covering Reads

- `0.68.7` extends the same bounded read path to canonical case-insensitive expression ranges such as `LOWER(field) >= 'a' AND LOWER(field) < 'b'`, keeps unsupported wrapped range shapes fail-closed, and cleans up the PocketIC SQL harness so it uses the newer fallible `canic-testkit` setup APIs instead of carrying its own local startup glue.
- `0.68.6` keeps more filtered `ORDER BY ... LIMIT` reads on the shared index-backed path, makes risky descending secondary-order shapes fail closed to the safer materialized route, shows composite equality-prefix values in `EXPLAIN EXECUTION`, and adds side-by-side generated-vs-typed perf coverage for `Character` and `ActiveUser` ordered reads.
- `0.68.5` removes the old `num-traits`-style numeric conversion layer, replaces it with one narrower `NumericValue` contract for numeric validation and sanitization, and switches wrapper types plus generated numeric newtypes to explicit conversions, so code using `NumCast`, `NumFromPrimitive`, or `NumToPrimitive` must move to the new explicit APIs.
- `0.68.4` makes equivalent text-prefix SQL forms such as `LIKE 'A%'`, `STARTS_WITH(name, 'A')`, and explicit `>= / <` bounds stay on the same bounded index-backed read path, keeps that same bounded covering route intact when the secondary index is filtered and the query proves the guard, and broadens canister and perf coverage around those cases.
- `0.68.3` keeps the new ordered read paths in place, fixes a text-index ordering bug so `ORDER BY LOWER(name)` and related expression-backed reads return rows in true lexical order, and extends canister perf coverage for those expression-backed routes.
- `0.68.2` makes more ordered SQL projections stay on the shared index-backed path, including more index-covered projections and composite `ORDER BY ... LIMIT` reads, and moves the PocketIC SQL harness onto a per-test `canic-testkit` lifecycle so those routes are exercised more reliably.
- `0.68.1` keeps the `0.68.0` planner groundwork in place and makes common cursorless SQL projection reads cheaper by teaching that lane to skip cursor output, skip some extra page bookkeeping, and avoid carrying full row payloads when retained slot rows are already enough.
- `0.68.0` starts the `0.68` read-path line by teaching the planner to recognize a narrow class of simple index-covered scalar projections and by sharing the direct-field projection rule between planner and executor, so later select-speed work can build on one authority without changing current query results yet.

See detailed breakdown:
[docs/changelog/0.68.md](docs/changelog/0.68.md)

---

## [0.67.x] 🧩 - 2026-04-01 - SQL Surface Coherence

- `0.67.7` closes the `0.67` line by switching `CandidType` wire-surface comments from Rustdoc to plain comments, trimming about `2.3 KB` from the `minimal` SQL-on wasm audit canister and making that lighter comment style the rule for `CandidType` surfaces going forward.
- `0.67.6` fixes a fresh-write structured-value bug, so nested blob, float, and big-int payloads no longer fail immediate row decode during insert/commit preparation, and it also locks the shipped bounded `STARTS_WITH(...)` family to the same accepted and fail-closed behavior on `DELETE`, `EXPLAIN DELETE`, and `EXPLAIN JSON DELETE`.
- `0.67.5` keeps the current SQL feature set intact but makes grouped and aggregate SQL queries cheaper by removing repeated aggregate compile work, trimming grouped page/cursor overhead, and cutting the instruction budget for common `COUNT`, `MIN`, `MAX`, and grouped `GROUP BY` paths without changing query results.
- `0.67.4` keeps the current SQL feature set intact but makes common SQL reads cheaper by removing repeated dispatch and executor setup work from the shared query path, while also moving the workspace onto `canic-cdk` / `canic-memory` `0.22.6`.
- `0.67.3` keeps the current SQL feature set intact but makes SQL-enabled canisters much smaller by removing a duplicated generated `query(sql)` router, so generated canister SQL now reuses the shared core route family instead of carrying its own extra copy of that logic.
- `0.67.2` keeps the SQL and storage surface unchanged but cuts much more write-path instruction cost, especially for inserts, updates, and batched inserts, by removing repeated save and commit-preflight work instead of changing any user-facing behavior.
- `0.67.1` keeps the SQL and storage feature surface unchanged but reduces fixed one-row write overhead in the shared commit and mutation path, so inserts, updates, and deletes use a bit less instruction budget before any row-specific work even starts.
- `0.67.0` adds one small SQL coherence slice instead of a broad rewrite: `STARTS_WITH(field, 'x')` now works directly, and the matching `STARTS_WITH(LOWER(field), 'x')` / `STARTS_WITH(UPPER(field), 'X')` forms use the same bounded prefix-search behavior already supported through `LIKE 'prefix%'`, while unsupported wrappers such as `TRIM(...)` still fail closed with stable errors across typed SQL, generated canister SQL, and `EXPLAIN`.

See detailed breakdown:
[docs/changelog/0.67.md](docs/changelog/0.67.md)

---

## [0.66.x] 🧭 - 2026-03-28 - Complexity Hotspot Decomposition

- `0.66.6` keeps the SQL and storage feature surface unchanged but trims more release canister wasm by narrowing the generated SQL query lane, removing a little more query-only wrapper flow, and simplifying retained value-rendering paths, then refreshes the recurring wasm-footprint audit so the current release matrix and Twiggy artifacts are recorded against the latest smaller baseline.
- `0.66.5` keeps the shipped SQL and storage behavior unchanged but wires the new crosscutting performance harness into the demo_rpg SQL canister and PocketIC integration flow, so the repo now records repeatable instruction-footprint baselines and optimization reruns instead of only structural perf guesses.
- `0.66.4` keeps the SQL and storage feature set unchanged but finishes the Canic `0.19.1` compatibility pass by moving the old shared case helpers into a new local `icydb-utils` crate, cleaning up the canister Candid-export wiring around the updated `canic-cdk` macro behavior, fixing a release-build query-attribute import regression that surfaced during wasm validation, and confirming that the standard SQL wasm audit fixtures are still smaller than the latest recorded benchmark.
- `0.66.3` keeps the current SQL and runtime feature surface intact but cleans up the release line by moving the remaining direct `canic-cdk` usage onto `canic::cdk`, splitting the reduced-SQL parser hotspot into a smaller root plus a projection child module, refreshing the complexity-accretion report after that parser cleanup, and updating one trybuild stderr expectation to match the new Canic diagnostic suggestion.
- `0.66.2` updates the canister/runtime integration line to Canic `0.18.6`, switches generated database bootstrap over to Canic's single public memory-registry bootstrap call, moves debug Candid export wiring onto `canic::export_candid`, and rebases the SQL fixture canister memory ranges so the test canisters no longer collide with Canic's own reserved stable-memory slots.
- `0.66.1` adds reduced-SQL text projection functions such as `trim`, `lower`, `length`, `substring`, and prefix/suffix helpers on the unified SQL dispatch lane, extends `EXPLAIN` for that same bounded lane, keeps `query_from_sql(...)` intentionally limited to structural queries, fixes shared-store full-scan `count()` so entity counts no longer include sibling rows from the same datastore, and finishes splitting the session SQL runtime into smaller owner-local modules so the new work lands against clearer boundaries.
- `0.66.0` keeps row format, SQL behavior, routing, and executor semantics unchanged but turns the main complexity-audit hotspots into smaller owner-local modules, splitting the large persisted-row, access-choice, and execution-descriptor roots into cleaner submodules so future work lands against narrower boundaries instead of continuing to accrete in single branch-heavy files.

See detailed breakdown:
[docs/changelog/0.66.md](docs/changelog/0.66.md)

---

## [0.65.x] 🧱 - 2026-03-26 - Canonical Row Invariant Hard-Cut

- `0.65.10` keeps rows, SQL behavior, and runtime semantics unchanged but trims more canister wasm by stopping most runtime-facing Rust doc comments from being embedded in normal builds, removing the last macro-emitted docs, and shortening a few especially long SQL/planning error messages that were still retained, cutting shrunk `wasm-release` size from `1,145,951` to `1,137,333` on `minimal` (`8,618` bytes) and from `1,277,482` to `1,268,864` on `one_simple` (`8,618` bytes).
- `0.65.9` keeps canonical rows, SQL behavior, and runtime semantics unchanged but trims more SQL canister wasm by flattening a few small internal registries and caches, dropping generated canister doc strings, tightening some long reduced-SQL and diagnostics strings, and removing dead schema comment plumbing, cutting shrunk `wasm-release` size from `1,157,161` to `1,145,951` on `minimal` (`11,210` bytes) and from `1,295,758` to `1,277,482` on `one_simple` (`18,276` bytes).
- `0.65.8` keeps canonical rows, SQL behavior, and runtime query semantics unchanged, finishes the internal orphaned-test cleanup by draining the stale executor aggregate backlog to zero and replacing the last dead matrix residue with live owner-local coverage, and also updates the workspace toolchain/MSRV pin from Rust `1.94.0` to `1.94.1`.
- `0.65.7` keeps the canonical-row and SQL behavior unchanged but continues the orphaned aggregate-test revival by moving more field-target, ranked-extrema, and secondary-index aggregate contracts into the live executor owner suite, trimming the stale aggregate backlog again from `9,002` lines to `8,156` while keeping those contracts actively exercised instead of stranded in dead matrix files.
- `0.65.6` keeps the canonical-row and SQL behavior unchanged but moves much more of the old dormant aggregate/session test backlog into the live owner-local harness, deletes the stale `aggregate/session_matrix.rs` and `aggregate/projection_matrix.rs` files, restores broad temporal, projection, and aggregate parity coverage so those contracts actually run again instead of sitting unlinked, and includes the structured-map persistence fix that lets record-valued map entries round-trip instead of failing row encode.
- `0.65.5` finishes moving the distinct pagination continuation checks into the live executor owner suite and fixes structured-field persistence bugs on the new storage/projection path, including nested custom records that could collapse to `null` and value-backed map fields whose structured entries were incorrectly rejected during row encode, with regression coverage that now locks required, optional, nested, list-shaped, and structured-map record persistence so values like `Profile::default()` and `Map<Principal, Record>` round-trip as full structured data instead of being lost or refused at save time.
- `0.65.4` expands strict SQL prefix matching so plain `LIKE 'prefix%'` works directly and both `LOWER(text)` and `UPPER(text)` can use the same prefix search path, while also reviving a large dormant executor test subset and fixing two real bugs that work exposed: singleton `Unit` rows were being written with the wrong scalar encoding, and diagnostics storage reports could collapse distinct runtime hook identities onto the same model name.
- `0.65.3` keeps the canonical-row behavior unchanged but trims more raw SQL canister wasm by replacing several small ordered-tree helpers in EXPLAIN, schema describe/info, ORDER BY validation, and predicate coercion metadata with lighter deterministic containers, cutting shrunk `wasm-release` size from `1,164,644` to `1,157,161` on `minimal` (`7,483` bytes) and from `1,303,241` to `1,295,758` on `one_simple` (`7,483` bytes).
- `0.65.2` keeps the new canonical-row behavior unchanged but further hardens fresh-install writes by making canonical rows the only production storage write token, routing save and commit replay back through the same canonical row emitter, and adding guard tests plus cleanup around the stricter row model.
- `0.65.1` closes the last read-side gap in the canonical row hard-cut by forcing every structural row to fully decode before projection, predicate, relation, or index consumers can use it, so malformed unused fields now fail immediately instead of only when a later query happens to touch them.
- `0.65.0` hard-cuts the persisted row format so every stored row must have one valid payload for every declared field, rejects missing slots and legacy raw-CBOR scalar bytes across commit/replay/query paths, and may require an explicit data rewrite or reset if existing stored data was relying on the older tolerant row shape.

See detailed breakdown:
[docs/changelog/0.65.md](docs/changelog/0.65.md)

---

## [0.64.x] 🧩 - 2026-03-25 - Structural Mutation API

- `0.64.6` closes the `0.64` line by auditing the public mutation wording, simplifying the remaining user-facing `UpdatePatch` error messages, and explicitly freezing the shipped surface at the single mode-driven `MutationMode` + `UpdatePatch` + `DbSession::mutate_structural(...)` API without extra wrapper layers.
- `0.64.5` keeps the SQL and mutation feature set unchanged, renames the public structural write-mode enum to cleaner `MutationMode`, registers generated actor cfgs so downstream crates do not trip `unexpected_cfgs`, trims more raw SQL canister wasm by simplifying reverse-index target handling plus the explain/access-choice path, and removes more duplicate explain projection, candidate-evaluation, and text-rendering work, cutting raw `wasm-release` size from `1,170,409` to `1,161,397` shrunk bytes on `minimal` (`9,012` bytes) and from `1,250,792` to `1,243,495` shrunk bytes on `one_simple` (`7,297` bytes).
- `0.64.4` keeps the SQL and mutation feature set unchanged, trims more raw SQL canister wasm by tightening structural ordering plus index-entry and unique-index validation ownership, and hardens commit preparation so malformed non-indexed row fields are rejected before storage instead of surfacing later at query time, cutting raw `wasm-release` size from `1,171,353` to `1,170,409` shrunk bytes on `minimal` (`944` bytes) and from `1,253,182` to `1,250,792` shrunk bytes on `one_simple` (`2,390` bytes).
- `0.64.3` keeps the SQL and mutation feature set unchanged but trims more raw SQL canister wasm by cleaning up grouped-query validation, reverse-relation index prep, structural ordering, and commit/index planning ownership, cutting raw `wasm-release` size from `1,178,818` to `1,171,353` shrunk bytes on `minimal` (`7,465` bytes) and from `1,265,115` to `1,253,182` shrunk bytes on `one_simple` (`11,933` bytes).
- `0.64.2` trims more raw SQL canister wasm by collapsing duplicated access canonicalization plus index planning and commit-preparation paths onto shared owners, cutting raw `wasm-release` size from `1,184,089` to `1,178,818` shrunk bytes on `minimal` (`5,271` bytes) and from `1,282,070` to `1,265,115` shrunk bytes on `one_simple` (`16,955` bytes) while keeping the SQL feature set unchanged.
- `0.64.1` trims more SQL canister wasm by collapsing duplicated predicate canonicalization, compare-list normalization, map-entry ordering, and framed sort-key encoding paths onto shared owners, cutting raw `wasm-release` size from `1,190,526` to `1,184,089` shrunk bytes on `minimal` (`6,437` bytes) and from `1,288,385` to `1,282,070` shrunk bytes on `one_simple` (`6,315` bytes) while keeping the SQL feature set unchanged.
- `0.64.0` adds one public mode-driven structural mutation API (`Insert`, `Update`, `Replace`) that still reuses the existing typed validation and commit pipeline, and it also isolates normal IcyDB cargo workflows into repo-local cargo state so sibling repos stop contending on the same filesystem.

See detailed breakdown:
[docs/changelog/0.64.md](docs/changelog/0.64.md)

---

## [0.63.x] 🧹 - 2026-03-23 - Post-De-Monomorphization Consolidation Audit

- `0.63.12` keeps the full SQL feature set but trims more canister wasm by narrowing the generated query path’s initial route and cursor setup, and it also fixes a flaky `make patch` clean-tree check that could fail once and pass on a second run without any real file change.
- `0.63.11` cleans up several public SQL, describe, query-expression, diagnostics, metrics, and error payload types by removing unused `serde::Serialize` derives where Candid decoding is the real contract, while keeping the runtime feature set and measured wasm size effectively unchanged.
- `0.63.10` keeps the full SQL feature set but cuts canister wasm size by making generated SQL query exports truly select-only, trimming retained explain/snapshot payloads, and restoring clean `sql-off` builds so SQL-on vs SQL-off footprint checks stay trustworthy.
- `0.63.9` closes this phase of the `0.63` line by running the crosscutting audit suite again, restoring the canonical wasm audit report layout to one top-level summary plus artifact-local per-canister details, and making the route feature budget guard an actually executed test instead of a dead source-only check.
- `0.63.8` keeps the `0.63` cleanup moving in the canister and test harness layer by deduplicating more demo_rpg SQL parity checks, sharing the repeated wasm-audit SQL stability test shell across the audit canisters, and simplifying the remaining Pocket-IC metadata-lane assertions, without changing SQL or runtime behavior.
- `0.63.7` keeps the `0.63` cleanup moving outside the core runtime by collapsing dead Pocket-IC canister-build wrappers, deduplicating repeated Pocket-IC canister setup and metadata-lane checks, and sharing the generated-actor SQL surface assertion across the wasm audit canisters, without changing SQL or runtime behavior.
- `0.63.6` keeps the `0.63` cleanup moving by tightening row-decode, data-key, executor missing-row, unique-validation, and commit-marker size-classification boundaries, then cutting more one-off local wrappers across scalar/grouped entrypoints, stream access, and aggregate helper files, without changing SQL or runtime behavior.
- `0.63.5` keeps the `0.63` cleanup moving by pushing more persisted-row, reverse-index, covering decode, and adjacent commit failure paths onto the types that already own those contracts, without changing SQL or runtime behavior.
- `0.63.4` keeps the `0.63` cleanup moving by pushing more executor, commit, migration, cursor, and index-planning failures onto the types that already own those contracts, without changing SQL or runtime behavior.
- `0.63.3` keeps the internal `0.63` cleanup moving by tightening aggregate, terminal, and access-path ownership boundaries so more invariant and decode logic lives on the types that actually own those contracts, without changing SQL or runtime behavior.
- `0.63.2` removes the last internal compatibility shells and dead test-support layers from the `0.63` audit line, tightens a few index ownership boundaries, and keeps the current SQL and runtime behavior unchanged.
- `0.63.1` continues the internal cleanup line by removing the old shared DB error-constructor layer, consolidating more planner/executor/session ownership boundaries, and keeping the current SQL and runtime behavior unchanged.
- `0.63.0` consolidates internal query, session, projection, cursor, mutation, and planner validation boundaries so repeated wrappers and drifted helper paths collapse onto shared owners without changing the current SQL/runtime feature surface.

See detailed breakdown:
[docs/changelog/0.63.md](docs/changelog/0.63.md)

---

## [0.62.x] 🧬 - 2026-03-21 - Structural Persisted-Row Decode

- `0.62.11` removes most of the remaining per-entity SQL and delete runtime duplication by moving dispatch, delete execution, and index/commit preparation onto shared runtime paths, drops the simple-entity wasm growth rate to about `1,669` raw bytes per added entity, and fixes runtime hook store-path wiring so live unique-index conflicts are classified the same way as replayed conflicts.
- `0.62.10` removes more generated per-entity API clutter by moving field-kind metadata onto one shared trait, dropping extra generated type constants, and keeping list/set/map wrappers ergonomic with `Deref` and `DerefMut` while the simple-entity wasm baseline stays flat.
- `0.62.9` cleans up the remaining per-entity metadata surface by making the generated runtime model the only source of primary-key and index metadata, simplifying generated SQL dispatch descriptors, and trimming redundant test-fixture macro inputs so future per-entity work builds on one consistent authority model.
- `0.62.8` continues the per-entity cleanup line by reorganizing the derive and build generators into smaller, clearer owner-local helpers, keeping generated behavior stable while leaving the macro/codegen layer in a cleaner state for future compression work.
- `0.62.7` cleans up the per-entity macro and build generators by splitting large derive/codegen emitters into smaller owner-local helpers, keeping the generated runtime shape intact while making the next per-entity compression work easier to do safely.
- `0.62.6` keeps the post-serde decoder cleanup moving by peeling recursive `ByKind` decode away from the structural-field root, tightening the remaining leaf and enum-value decode paths inside their owning modules, and landing another small debug wasm reduction without undoing the clearer module boundaries from `0.62.4` and `0.62.5`.
- `0.62.5` reorganizes the remaining explicit runtime decode and projection surfaces into clearer owner modules, splitting the structural field decoder and projection materialization/evaluation into smaller boundaries so follow-up work can stay coherent without undoing the post-serde architecture.
- `0.62.4` reorganizes the remaining explicit field decoder into clear owner modules (`cbor`, `scalar`, `kind`, `leaf`, `storage_key`, and `value_storage`) and updates the design/status docs so the post-serde cleanup can continue from a cleaner structure instead of one growing file.
- `0.62.3` keeps shaving the remaining explicit structural decoder by tightening storage-key and account leaf decode, reusing one slot-validation path across projection and recovery checks, and replacing the heavy date parser with a fixed-width parser, which lowers the simple-entity audit floor again to about `24,947` raw wasm bytes per added entity.
- `0.62.2` finishes the shared-runtime cut by removing the last runtime `serde_cbor` decode path, moving predicate/index/projection work onto shared scalar programs, shrinking the remaining structural field decoder, and lowering the simple-entity audit floor to about `24,987` raw wasm bytes per added entity.
- `0.62.1` hard-cuts legacy persisted-row and commit-marker formats, makes the slot-based row container the only live storage format, pushes commit/index/recovery and unique-validation work further onto slot readers plus scalar leaf codecs, and removes the runtime field-projection fallback that previously read persisted slots through generic `get_value`.
- `0.62.0` starts the structural persisted-row decode line by introducing one shared row/field decode path for non-boundary runtime work, narrowing enum payload fallback handling, and skipping typed commit-prep row decode entirely for entities with no secondary indexes, which lowers the audit cost to about `45,499` raw wasm bytes per added simple entity and about `56,265` raw wasm bytes per added complex entity.

See detailed breakdown:
[docs/changelog/0.62.md](docs/changelog/0.62.md)

---

## [0.61.x] 📦 - 2026-03-20 - Per-Entity WASM Compression

- `0.61.9` keeps flattening the simple-entity runtime floor by removing one more specialization axis from commit and index preparation, so the shared commit-prep path now depends on reader capabilities instead of concrete reader implementations and lands at about `47,349` raw wasm bytes per added simple entity.
- `0.61.8` keeps flattening the simple-entity runtime floor by moving the blocked-delete proof loop in strong-relation validation onto one shared helper and leaving only the final typed diagnostic key reconstruction at the error edge, which brings the audit cost down to about `48,233` raw wasm bytes per added simple entity and clears the old relation-validation family out of the top debug monos list.
- `0.61.7` keeps flattening the simple-entity runtime floor by moving delete execution onto the same pattern as commit prep, with structural plan/access/commit orchestration around one typed decode-and-filter leaf, which brings the audit cost down to about `51,491` raw wasm bytes per added simple entity and clears the old delete executor family out of the top debug monos list.
- `0.61.6` keeps flattening the simple-entity runtime floor by turning commit preparation into a typed-forward-index leaf plus one shared finalization path, which brings the audit cost down to about `55,243` raw wasm bytes per added simple entity and removes the old relation and commit-prep helper families from the top debug monos list.
- `0.61.5` keeps flattening the simple-entity runtime floor by shrinking the remaining lowered-SQL query fallback again, which removes the old SQL query wrapper families from the top debug monos list and lands at about `60,198` raw wasm bytes per added simple entity.
- `0.61.4` keeps flattening the simple-entity runtime floor by moving scalar execution, more of SQL dispatch, reverse-relation index preparation, global-aggregate `EXPLAIN`, and the lowered-SQL query lane onto shared runtime helpers, which brings the audit cost down to about `80,605` raw wasm bytes per added simple entity and leaves relation validation/preparation as the next big duplicated families instead of the earlier SQL wrappers.
- `0.61.3` is the already-shipped release cut for the `0.61.2` macro-compression checkpoint, so the deeper simple-entity flattening work now records under `0.61.4` instead of being mixed into the prior shipped `0.61.3` notes.
- `0.61.2` keeps the macro-compression line moving by replacing repeated generated list/set/map container conversion bodies with shared runtime helpers, rebuilding the wasm audit fixtures around an empty `minimal` plus `one_simple`, `one_complex`, `ten_simple`, and `ten_complex`, and recording the first clean baseline matrix for separating base entity cost from richer macro-heavy schema cost.
- `0.61.1` keeps the macro-compression line moving by lifting generated entity and record sanitization and validation onto shared field-descriptor loops, reshaping schema AST emission around local const tables for repeated slices, and confirming with a fresh `minimal` vs `twenty` wasm audit that the remaining per-entity growth is still large.
- `0.61.0` opens the post-`0.60` macro-compression line by shrinking repeated entity macro output in the runtime model and traversal paths: entity models now emit one direct `__MODEL_FIELDS` array, and generated entity and record traversal now runs through a shared field-descriptor loop instead of repeating inline per-field `drive` bodies.

See detailed breakdown:
[docs/changelog/0.61.md](docs/changelog/0.61.md)

---

## [0.60.x] ✂️ - 2026-03-20 - Generated View Removal

- `0.60.0` removes the generated entity `View`/`Create`/`Update` DTO families, makes entities the direct public read/write payloads, deletes the old internal patch/view layer, and cleans the remaining live API/docs surfaces so the crate no longer advertises the removed model.

See detailed breakdown:
[docs/changelog/0.60.md](docs/changelog/0.60.md)

---

## [0.59.x] ⚙️ - 2026-03-18 - Execution De-Monomorphization

- `0.59.8` removes another small typed-key decode loop from preflight index readers and replaces the old `minimal` vs `demo_rpg` wasm comparison with a dedicated `minimal` vs `twenty` audit pair, so wasm reports now use a controlled twenty-entity fixture instead of a mixed-purpose test canister.
- `0.59.7` removes another typed rebound from executor preparation by letting global DISTINCT grouped routing stay on prepared structural plans end-to-end, rather than rebuilding a fresh typed execution plan inside the executor.
- `0.59.6` removes more entity-type plumbing from the execution core by making scalar and grouped kernel dispatch run from prepared runtime bundles, replacing loose model/path/tag threading with one executor-owned authority contract, and closing the release checkpoint with green `icydb-core` tests plus workspace clippy.
- `0.59.5` keeps the execution engine on shared runtime paths by replacing more typed fast-path plumbing with traversal/runtime helpers, moving shared ordering logic into executor-owned helpers, and tightening store/index access so only scan boundaries touch the registry directly.
- `0.59.4` publishes the `0.59.3` execution-core checkpoint as a release cut without additional runtime behavior changes.
- `0.59.3` keeps pushing query execution toward one shared runtime by removing more typed access-plan baggage, moving projection, ranking, and `bytes(field)` materialization off full entity reconstruction, and keeping large test-only helpers out of production files.
- `0.59.2` keeps shrinking query execution by moving key handling, materialized aggregate loops, and final typed response assembly onto structural or outer-edge boundaries, reorganizing test-only helpers out of runtime files, and remeasuring both canister harnesses in `debug` and `wasm-release`.
- `0.59.1` moves scalar execution much further onto shared prepared runtime contracts by removing more plan-owned scalar branching, making scalar row decoding structural, and updating fresh developer setup to install the required `ripgrep` tool for invariant checks.
- `0.59.0` starts the execution-engine size-reduction line by making grouped aggregate state and grouped fold ownership structural, removing the old typed grouped fold hook layer, and landing the first measured grouped-kernel size drop before scalar execution work begins.

See detailed breakdown:
[docs/changelog/0.59.md](docs/changelog/0.59.md)

---

## [0.58.x] 📉 - 2026-03-18 - Runtime Size-Reduction

- `0.58.3` adds a default-on `sql` feature so non-SQL builds can compile out the SQL frontend, public SQL APIs, and generated canister SQL glue, moves shared reduced-SQL lexer/error plumbing onto one neutral core boundary, and adds an optional SQL-on / SQL-off mode to the wasm size scripts for side-by-side footprint checks.
- `0.58.2` removes dead executor pipeline scaffolding left behind by the size-reduction refactor, recovers cleanly from poisoned schema-cache locks during save validation, and keeps test-only cursor and entrypoint helpers out of release builds.
- `0.58.1` hardens the release with SQL dispatch consistency fixes (`DELETE` now works through the unified query-lane surface), safer generated code and Clippy cleanup, and PocketIC integration tests that no longer fail the whole workspace when the local PocketIC server binary is not configured.
- `0.58.0` completes the runtime size-reduction line by removing the highest-value generic orchestration and adapter duplication, flattening structural execution boundaries around entity tags and cursor/index identity, and closing the line once the remaining binary size was confirmed to come from typed execution kernels rather than more safe local cleanup.

See detailed breakdown:
[docs/changelog/0.58.md](docs/changelog/0.58.md)

---

## [0.57.x] 🔍 - 2026-03-16 - Reduced SQL Closure Follow-Ups

- `0.57.1` expands `bytes_by(field)` index-only/constant covering fast paths, adds `explain_bytes_by(field)` terminal metadata with stable projection-mode labels, and keeps strict-mode shapes fail-closed with dedicated BYTES-by diagnostics counters.
- `0.57.0` lets SQL users run `SHOW TABLES` as an alias for `SHOW ENTITIES` and adds bounded `LOWER(field) LIKE 'prefix%'` lowering to case-insensitive prefix matching while keeping out-of-scope SQL shapes fail-closed.

See detailed breakdown:
[docs/changelog/0.57.md](docs/changelog/0.57.md)

---

## [0.56.x] 🗂️ - 2026-03-16 - Reduced SQL Introspection and Entity Listing

- `0.56.6` hardens the new case-insensitive expression-prefix path with extra planner/access/runtime parity tests (including one-character and empty-prefix edges), locks residual-filter diagnostics for deterministic routing, and stabilizes PocketIC SQL integration runs by serializing test bodies and reusing one built test-canister WASM payload per process.
- `0.56.5` adds reduced-SQL `SHOW COLUMNS <entity>` end-to-end, expands grouped SQL `HAVING` with bounded `IS NULL` / `IS NOT NULL` support, and extends existing `LOWER(field)` expression indexes to support case-insensitive prefix filters (`starts_with_ci`) via bounded index-range planning with fail-closed unsupported-expression behavior.
- `0.56.4` consolidates duplicated reduced-SQL lane plumbing by centralizing core wrong-lane gating, simplifying generated `sql_dispatch` route branching, and moving core/facade wrong-lane SQL tests to matrix coverage, while keeping SQL behavior and fail-closed contracts unchanged.
- `0.56.3` makes `SHOW ENTITIES` a first-class reduced-SQL route across parser/lowering/session/generated dispatch and adds constrained grouped `HAVING` execution for SQL `GROUP BY` shapes, while keeping unsupported HAVING boolean forms fail-closed.
- `0.56.2` replaces split SQL canister helpers with one `query(...)` enum envelope (`SqlQueryResult`) across projection/explain/describe/show-indexes/show-entities, updates local shell rendering flows around that unified result, and fixes generated empty-entity route stubs so downstream builds do not emit unused-`sql` warnings.
- `0.56.1` hardens the introspection surfaces by locking `DESCRIBE`/`SHOW INDEXES` shell output vectors in tests, broadening canister integration coverage for mixed-case/schema-qualified/semicolon forms, and removing version-specific wording from generated helper error messages.
- `0.56.0` adds reduced-SQL `DESCRIBE <entity>` and `SHOW INDEXES <entity>` introspection lanes across parser/lowering/session/facade, expands generated `sql_dispatch` with typed and shell-friendly describe/index helpers plus `SHOW ENTITIES` support, and keeps `query_rows` projection-only with deterministic introspection-lane rejections.

See detailed breakdown:
[docs/changelog/0.56.md](docs/changelog/0.56.md)

---

## [0.55.x] 🧠 - 2026-03-15 - Expression Indexes

- `0.55.7` removes remaining legacy/shim compatibility aliases and stale wording (including `Date::get` and the PocketIC staging alias), while keeping persisted row and commit decode compatibility fallbacks in place.
- `0.55.6` continues load-pipeline containment by splitting post-access, orchestrator, reducer, distinct, and terminal execution seams into smaller owner modules, and keeps invariant gates green by fixing a pipeline planner-import leak in post-access coordination.
- `0.55.5` consolidates continuation-envelope semantics under one index-owned boundary, hardens layer-authority leak detection for generic helper signatures, makes PocketIC integration teardown keep primary failures visible, and publishes the 2026-03-15 canonical-semantic-authority and DRY-consolidation audit reports.
- `0.55.4` completes the predicate semantic-authority audit by centralizing OR-equality canonicalization and identifier rewrites in predicate-owned boundaries, and makes PocketIC canister tests deterministic in `make test`/CI by resolving `POCKET_IC_BIN` explicitly.
- `0.55.3` closes the line with unsupported-expression fail-closed recovery/runtime parity coverage and adds a recurring canonical semantic-authority crosscutting audit track.
- `0.55.2` expands expression-index planning/runtime parity for supported `LOWER(field)` case-insensitive `Eq`/`In` lookups, while keeping unsupported-expression and range-family shapes fail-closed.
- `0.55.1` hardens the line with filtered+expression composition locks, conditional+expression uniqueness coverage, and live-vs-replay expression-unique conflict parity checks.
- `0.55.0` ships the initial expression-index foundation line with canonical index key-item metadata, validated expression-key derivation, and shared planner/EXPLAIN eligibility for the first supported deterministic subset.

See detailed breakdown:
[docs/changelog/0.55.md](docs/changelog/0.55.md)

---

## [0.54.x] 🧪 - 2026-03-13 - Filtered Indexes Line Open

- `0.54.1` adds generated canister-local SQL dispatch helpers (`sql_dispatch`) with deterministic entity-keyed routing, shell-friendly `query` output plus structured `query_rows` payloads, clearer `EXPLAIN` rejection guidance for unordered pagination, and actionable reduced-SQL clause-order parse diagnostics.
- `0.54.0` ships filtered/conditional indexes end-to-end: optional index predicates in schema/runtime metadata, schema-time predicate validation, mutation-path membership gating, planner implication-based index eligibility, and replay/startup recovery parity tests that lock live-vs-replay behavior.

See detailed breakdown:
[docs/changelog/0.54.md](docs/changelog/0.54.md)

---

## [0.53.x] 🛟 - 2026-03-12 - Data Survivability Line Planned

- `0.53.2` lands the data-survivability runtime hardening pass with versioned row and commit-marker envelopes, durable migration-step resume state bound to commit markers, integrity reporting, and fail-closed startup recovery checks that prevent silent row orphaning/discard.
- `0.53.1` splits the session runtime into smaller facades and tightens ownership boundaries so canonicalization, continuation, and index-range validation each stay in one authority layer with stronger guard coverage.
- `0.53.0` reframes the `0.53` design contract around data survivability and upgrade safety, with explicit persisted-format compatibility-window guarantees, migration resume/recovery safety language, and clearer corruption-vs-compatibility error-taxonomy boundaries.

See detailed breakdown:
[docs/changelog/0.53.md](docs/changelog/0.53.md)

---

## [0.52.x] 📝 - 2026-03-12 - Reduced SQL Parser Line Open

- `0.52.3` expands SQL-first matrix coverage across scalar/projection/grouped/aggregate/explain paths, adds grouped cursor fail-closed regression checks for invalid payloads and query-signature mismatch, extends facade SQL matrix coverage for `query_from_sql` and `explain_sql` surfaces, and adds a test-canister-only `sql(...)` query endpoint for quick ad-hoc SQL checks.
- `0.52.2` adds reduced SQL grouped execution (`execute_sql_grouped`), a constrained global aggregate SQL surface (`execute_sql_aggregate` for `COUNT(*)`/`COUNT(field)`/`SUM(field)`/`AVG(field)`/`MIN(field)`/`MAX(field)`), `EXPLAIN` support for those constrained aggregate selects, constrained scalar `SELECT DISTINCT` support (`DISTINCT *` or field lists including primary key), qualified identifier normalization (`schema.Entity`, `Entity.field`) for executable SQL surfaces, and tighter fail-closed parser boundaries with stable unsupported-feature labels (table aliases, quoted identifiers, and `COUNT(DISTINCT ...)` plus other out-of-scope grammar branches).
- `0.52.1` adds executable SQL field-list projection lowering, a new projection-shaped SQL session API (`execute_sql_projection`), facade projection-response iteration support (`IntoIterator`), parity tests that lock SQL/fluent projection identity and fingerprints, and clearer SQL subset docs for what remains gated.
- `0.52.0` opens the reduced SQL parser line with deterministic reduced-SQL parsing plus initial SQL-to-query lowering and session entrypoints (`query_from_sql`, `execute_sql`, `explain_sql`) for the minimum executable subset (`SELECT *` and constrained `DELETE`) while broader SQL projection/grouping semantics remain gated for follow-up patches.

See detailed breakdown:
[docs/changelog/0.52.md](docs/changelog/0.52.md)

---

## [0.51.x] 🧭 - 2026-03-12 - Engine Contract Stabilization Line Open

- `0.51.2` removes another internal execution-planning wrapper by routing key-stream execution through one `ExecutableAccess` contract (instead of a separate descriptor layer), reducing route/executor indirection while keeping query behavior unchanged.
- `0.51.1` completes stabilization work through Slice F by locking projection and aggregate strategy surfaces, continuation and ordering determinism, and numeric comparison/arithmetic authority alignment across planner, executor, grouped ordering, and index-ordering contracts.
- `0.51.0` starts the `0.51` stabilization line by freezing deterministic query-plan snapshot surfaces (`query -> executable plan -> explain`), adding baseline snapshots for core execution shapes, locking ordering-contract guards for `ORDER`/`LIMIT`/cursor behavior, and adding continuation-envelope replay/version fail-closed regression coverage on fluent pagination paths.

See detailed breakdown:
[docs/changelog/0.51.md](docs/changelog/0.51.md)

---

## [0.50.x] 🔧 - 2026-03-11 - Executor Simplification Line Open

- `0.50.2` records the pre-`0.51` contract lock by explicitly freezing `is_not_null()` and `between(...)`, planner projection selection shapes (`All`/`Fields`/`Expression`), and grouped `AVG` planner support (including global DISTINCT policy/invariant alignment).
- `0.50.1` expands Slice E mechanical cleanup across aggregate terminals/projection helpers, runtime payload-window helpers, post-access operator plumbing, commit-window metrics wiring, and EXPLAIN descriptor helper paths, reducing executor maintenance surface without changing query behavior.
- `0.50.0` opens the `0.50` line with design and preparation artifacts (slice plans, invariants, and baseline trackers) to guide executor simplification work; runtime behavior is unchanged.

See detailed breakdown:
[docs/changelog/0.50.md](docs/changelog/0.50.md)

---

## [0.49.x] 🏗️ - 2026-03-11 - Executor Architecture Stabilization

- `0.49.3` hardens EXPLAIN execution consistency by adding cross-node-family JSON schema guards, locking text/JSON parity for additive execution metadata, and tightening explain-to-diagnostics node correlation checks without changing query behavior.
- `0.49.2` stabilizes executor observability and orchestration internals by splitting EXPLAIN JSON/node ownership, adding executor diagnostics node/counter contracts, and moving load orchestration to a deterministic stage-descriptor loop while keeping query behavior unchanged.
- `0.49.1` removes the remaining internal `db::executor::shared` bucket by moving contracts and helpers to owner modules (`pipeline::contracts`, `projection`, and `context`) and adds guardrails so the old namespace cannot return.
- `0.49.0` stabilizes the post-`load` executor architecture by locking scan/pipeline/aggregate/terminal boundaries, expanding EXPLAIN execution metadata (including deterministic node IDs and layer/fast-path/pushdown visibility), hardening continuation fail-closed checks across grouped shape drift, and adding additive row-flow metrics (`rows_filtered`, `rows_aggregated`, `rows_emitted`) without introducing new query language features.

See detailed breakdown:
[docs/changelog/0.49.md](docs/changelog/0.49.md)

---

## [0.48.x] 💡 - 2026-03-11 - EXPLAIN and Other Features

- `0.48.7` hardens the new executor architecture by adding stricter cross-layer guard checks, fail-closed grouped cursor compatibility tests for projection-shape drift, richer EXPLAIN execution metadata (including stable node IDs and fast-path/pushdown mode fields), and additive row-flow metrics (`rows_filtered`, `rows_aggregated`, `rows_emitted`).
- `0.48.6` finishes the executor-load refactor by removing the old `db::executor::load` module tree, moving internals to clearer modules (`stream/access`, `scan`, `pipeline` plus owner-named contracts/projection modules, and `terminal`), and updating invariant checks and structural tests to the new layout.
- `0.48.5` continues boundary cleanup by making `value` own `StorageKey` (removing the `value -> db` dependency), moving DB-only executor/query/planner/cursor error constructors out of `InternalError`, and organizing `db::error` into smaller subsystem modules.
- `0.48.4` splits EXPLAIN into smaller focused modules, consolidates runtime schema contracts under `db::schema`, replaces ad-hoc EXPLAIN JSON object assembly with one deterministic writer, and renames the advanced metrics namespace from `obs` to `metrics`.
- `0.48.3` removes the internal `db::error` wrapper module so DB runtime code now constructs invariant errors directly through `InternalError` constructors with unchanged formatting and behavior.
- `0.48.2` adds clearer EXPLAIN execution diagnostics (covering-scan eligibility, cursor resume metadata, and fast-path reason codes) and fixes a DB codegen bug so generated `icydb_snapshot()` uses the active database handle correctly.
- `0.48.1` expands EXPLAIN observability for index routes with clearer predicate/order/fetch metadata and planner-derived access-choice reason codes, without changing query execution behavior.
- `0.48.0` makes plan hashes and continuation signatures independent from EXPLAIN formatting, keeps cursor continuation safety checks fail-closed under one cursor-owned contract, and preserves explain/runtime parity with expanded regression coverage.

See detailed breakdown:
[docs/changelog/0.48.md](docs/changelog/0.48.md)

---

## [0.47.x] 🔎 - 2026-03-10 - Audit Pass


- `0.47.8` removes internal shim/wrapper layers in planner and executor paths so grouped policy/projection validation and access-descriptor wiring now call their semantic owners directly, reducing indirection without changing query behavior.
- `0.47.7` ⚠️ introduces breaking temporal API-boundary contract enforcement: `Date` now must be `"YYYY-MM-DD"`, `Timestamp` must be RFC3339 text, and `Duration` remains integer milliseconds in JSON payloads.
- `0.47.6` continues load-hub decomposition by splitting scalar/orchestrate/fast-path/fast-stream-route/candidate/projection operator modules, updates audit governance so same-day reruns always compare to the day baseline, and re-runs crosscutting audits with stable risk scores (`complexity 6/10`, `velocity 5/10`, `module 5/10`, `layer 4/10`, `DRY 5/10`).
- `0.47.5` continues structural containment by reducing route/validation branch pressure (including another route-planner/cursor-policy trim pass), splitting load projection/ranking terminals and grouped validation modules into clearer runtime boundaries, tightening route fast-path guard contracts, and requiring explicit DRY follow-ups whenever high-risk divergence seams are detected.
- `0.47.4` adds a live-vs-replay unique-conflict parity lock, hardens recurring index-integrity parity checks, and reduces route/planner structural pressure by splitting planner type inference and simplifying aggregate route-hint branching.
- `0.47.3` closes the cursor edge-case checklist with explicit composite-anchor and DESC-resume verification, reducing continuation regression risk without changing query behavior.
- `0.47.2` continues structural containment by further splitting route execution, route hinting, and grouped runtime/load contract hubs, making ownership boundaries clearer while keeping query behavior unchanged.
- `0.47.1` continues audit-pass hardening by splitting load and grouped-contract execution hotspots, tightening grouped DISTINCT planner/runtime contracts, and updating invariant/guard checks so CI stays aligned with the refactor.
- `0.47.0` establishes the audit-pass baseline with prep work, expanded recurring audit coverage, an initial risk summary, and a cleaner docs layout for audits/design tracking.

See detailed breakdown:
[docs/changelog/0.47.md](docs/changelog/0.47.md)

---

## [0.46.x] 📏 - 2026-03-08 - Standards Alignment

- `0.46.13` finalizes version-sequence alignment after that script error; runtime behavior is unchanged.
- `0.46.12` is a metadata-only patch created during release-script correction; runtime behavior is unchanged.
- `0.46.11` adds temporal projection, ranked-row terminal, and explain parity locks (including `explain_first`/`explain_last`) so first/last, id/value, and ranked outputs keep semantic `Date`/`Timestamp`/`Duration` values, and closes the `0.46` standards-alignment track matrix.
- `0.46.10` adds temporal grouped-key and distinct-projection locks so `Date`/`Timestamp`/`Duration` values stay semantic at runtime boundaries.
- `0.46.9` locks strict `starts_with` index-lowering parity, adds clearer verbose text-operator fallback reasons, and keeps non-strict fallback precedence stable.
- `0.46.8` aligns `exists`/`not_exists`/`is_empty` behavior across load APIs, expands date/time edge coverage, and improves verbose fallback diagnostics.
- `0.46.7` renames query execution errors for clearer ownership and adds stronger complexity-drift guard coverage.
- `0.46.6` centralizes deterministic `IN (...)` planning helpers and renames route execution mode types for clearer ownership.
- `0.46.5` speeds up safe `sum_by`/`avg_by` streaming paths and adds stronger fail-closed checks for duplicate-risk routes.
- `0.46.4` makes ordered `LIMIT` pushdown safer with deterministic `ORDER BY` checks and short-circuits strict empty `IN ()`.
- `0.46.3` tightens executor boundaries and structural guards so internal APIs do not widen accidentally.
- `0.46.2` removes duplicate window/covering logic between planner and executor, including shared `count_distinct_by` reuse.
- `0.46.1` adds `select_one` for constant scalar reads and tightens `exists` early-stop behavior for offset windows.
- `0.46.0` adds early planner/runtime shortcuts for queries that cannot return rows, reducing work while keeping query results unchanged.

See detailed breakdown:
[docs/changelog/0.46.md](docs/changelog/0.46.md)

---

## [0.45.x] 🧼 - 2026-03-07 - Feature Cleanup

- `0.45.8` unifies fast-stream route execution and explain traversal helpers, and deduplicates shared invariant/resume-boundary logic.
- `0.45.7` reduces access-path fan-out by consolidating executor dispatch behind shared capability checks and adds a drift-audit metric.
- `0.45.6` finishes shared SHA256 helper cleanup and reduces planner/runtime overlap in post-access and residual-filter routing.
- `0.45.5` moves almost all schema-node public fields behind constructors/accessors, reducing coupling while keeping behavior unchanged.
- `0.45.4` keeps equivalent numeric/text coercion filters on the same fingerprint/cursor signature and adds compile-fail relation naming tests.
- `0.45.3` tightens grouped-query planner validation, locks equivalent cursor replay behavior, and reduces response DTO field coupling.
- `0.45.2` continues module splitting, adds DTO serialization-shape regression tests, and fixes CI invariant paths after file moves.
- `0.45.1` stabilizes cursor and EXPLAIN behavior under refactors, consolidates hashing/encoding helpers, and splits large executor modules.
- `0.45.0` tightens internal API and error/serialization contracts, enforces schema relation naming checks, and reduces duplicate planner/executor routing.

See detailed breakdown:
[docs/changelog/0.45.md](docs/changelog/0.45.md)

---

## [0.44.x] 🚀 - 2026-03-07 - Optimization Closure

- `0.44.3` centralizes access-capability checks and expands `count()`/`bytes()` `.by_ids(...)` speedups for both unordered and `ORDER BY id` windows.
- `0.44.2` expands safe `bytes()` fast paths for unordered secondary-index windows, while ordered/predicate-heavy shapes stay on canonical fallback execution.
- `0.44.1` expands safe index-backed fast paths for `count()` and `exists()`, with unchanged fallback behavior when fast-path eligibility is not met.
- `0.44.0` speeds up common `count()`/`bytes()` queries on primary-key scans and key ranges, improves LIMIT/covering diagnostics, and moves the workspace to Rust `1.94.0`.

See detailed breakdown:
[docs/changelog/0.44.md](docs/changelog/0.44.md)

---

## [0.43.x] 📐 - 2026-03-07 - BYTES and Audits

- `0.43.4` tightens internal API boundaries by moving diagnostics/trace/explain DTO reads to accessor methods and reducing direct field coupling, with no query behavior changes.
- `0.43.3` continues the architecture cleanup by removing duplicate access-shape type definitions, eliminating dead plan-shape storage, and dropping no-op wrapper layers while keeping query and cursor behavior unchanged.
- `0.43.2` improves runtime plan stability by centralizing access-shape capability checks behind one shared access boundary, reducing duplicate routing logic and tightening guard coverage for raw access-path usage.
- `0.43.1` adds scalar field-size measurement with `BYTES(field)` (`bytes_by("field")`) so you can identify which field dominates storage in a filtered query window.

```rust
let total_bytes = session.load::<Event>().bytes()?;
let payload_bytes = session.load::<Event>().bytes_by("payload")?;
```

- `0.43.0` adds scalar load-query `bytes()` so you can measure total persisted payload size for the same filtered/ordered/limited window returned by `execute()`, and starts a dated crosscutting audit cycle for this line.


See detailed breakdown:
[docs/changelog/0.43.md](docs/changelog/0.43.md)

---

## [0.42.x] 🧾 - 2026-03-06 - EXPLAIN

- `0.42.3` keeps the EXPLAIN release line warning-clean by removing an unused internal explain helper that broke strict `-D warnings` test builds.
- `0.42.2` strengthens EXPLAIN stability by refactoring runtime explain tests around stable output surfaces and broadening aggregate-path regression matrices.
- `0.42.1` completes the EXPLAIN follow-up by freezing descriptor vocabulary, adding fluent/runtime explain adapters (text/json/verbose), and surfacing deterministic verbose route diagnostics with guard coverage for deferred scalar node families.
- `0.42.0` adds `EXPLAIN` execution output so you can see how a query will run (index/path, filtering stage, ordering, and limit handling) before execution.

See detailed breakdown:
[docs/changelog/0.42.md](docs/changelog/0.42.md)

---

## [0.41.x] 🌱 - 2026-03-05 - Minor Features & Pre-EXPLAIN

- `0.41.4` finished the last pre-EXPLAIN execution-shape contracts from the `0.41` design scope, including strict `COUNT(predicate)` prefilter pushdown, secondary `IN` multi-lookup, index-range order satisfaction, and top-N seek routing.
- `0.41.3` added pre-EXPLAIN developer diagnostics and introspection surfaces, including stable execution descriptors, plan-hash/trace output, execution metrics, and schema inspection helpers.
- `0.41.2` improved ordered-query execution and predicate pushdown groundwork for the next observability slice.
- `0.41.1` sped up index-backed aggregate terminals (`count`, `exists`, and targeted `min`) while keeping strict correctness and stale-key safety checks.
- `0.41.0` improved basic `LIMIT` handling and added the first index-covering projection fast paths for common scalar value terminals.

See detailed breakdown:
[docs/changelog/0.41.md](docs/changelog/0.41.md)

---

## [0.40.x] 🔬 - 2026-03-05 - AUDIT ALL THE THINGS!!!!1

- `0.40.0` split very large runtime/planner files into smaller modules to make the codebase easier to navigate and review.
- `0.40.1` made load routing use one shared executable access shape at runtime and added a guard test so old path-specific branching does not come back.
- `0.40.2` unified cursor continuation handling and merged scalar/grouped load dispatch into one internal execution pipeline.
- `0.40.3` added guard tests that lock load pipeline structure and reduced duplicate grouped cursor-policy checks by keeping that rule behind one continuation boundary.
- `0.40.4` tightened the unified load pipeline by using stage-typed state handoffs, keeping grouped policy decisions behind planner-owned wrappers, and adding fail-closed grouped DISTINCT guards plus planner-bypass tests.
- `0.40.5` continued the load/executor cleanup by bundling page and physical stream inputs into typed request contracts, converging aggregate fast-path helper inputs, and splitting grouped route stage state into explicit ownership bundles.
- `0.40.6` split `query::plan::semantics`, `query::intent`, and `executor::load::projection` into domain modules to reduce file size and clarify ownership, without changing behavior.
- `0.40.7` boundary-semantics audit completed.
- `0.40.8` complexity-accretion audit reduced planner/route/load branch pressure by centralizing continuation and pushdown policy decisions, isolating runtime access execution behind one descriptor contract, and narrowing plan errors to semantic vs cursor domains.
- `0.40.9` completed the cursor ordering and dry consolidation audits with new ordering/continuation property tests and stricter planner-vs-executor grouped policy and cursor-error boundary guards.
- `0.40.10` completes the error taxonomy and index-integrity audit slice by tightening cursor error classification and adding crash/recovery guards for unique constraints, reverse indexes, and prefix-range key bounds.
- `0.40.11` upgrades layer auditing from import-direction checks to semantic authority checks, adds forward-vs-replay state-equivalence coverage, and adds grouped planner/handoff/route policy snapshot guards.
- `0.40.12` records the complexity-accretion v2 rerun with risk still at `6/10`: runtime `.as_inner()` calls are now zero and access-path fan-out remains lower, but continuation spread and planner/route/load branch pressure remain the main complexity risks.


```text
scripts/dev/cloc.sh (2026-03-03)
crate                 runtime_loc     test_loc     test_%
-------------------- ------------ ------------   --------
icydb                        3147            0       0.0%
icydb-build                   208            0       0.0%
icydb-core                  46287        37120      44.5%
icydb-derive                  418            0       0.0%
icydb-primitives              364            0       0.0%
icydb-schema                 1631            0       0.0%
icydb-schema-derive          6414            0       0.0%
icydb-schema-tests           2450           73       2.9%
```

- `0.40.13` finalizes the highest-ROI complexity-accretion v2 follow-ups by formalizing planner-to-route pushdown contracts, centralizing access-capability evaluation, and removing remaining compatibility/shim surfaces.
- `0.40.14` closes the remaining `0.40` audit work by adding cursor parity/invalidation, persisted decode-boundary, and origin-preservation matrix coverage, then reconciling status/design trackers to a closed state.
- `0.40.15` continues `0.40` boundary cleanup by moving commit/response ownership out of `db.rs`, tightening `db`/`session` visibility, centralizing continuation contracts between planning and execution, replacing ambiguous response aliases with explicit `EntityResponse` types, and removing repeated response `ids()`/`views()` allocation in favor of iterator-based access.
- `0.40.16` finishes response-surface cleanup by switching repeated `ids()`/`views()` materialization to iterator APIs, adding direct iteration over paged execution payloads, moving execution trace contracts under diagnostics ownership, and removing the ambiguous `Response` alias in favor of explicit `EntityResponse` typing.
- `0.40.17` hardens API/layer boundaries by sealing cardinality and index-planning extension traits behind private modules, moving response sealing to `private.rs`, relocating executable access contracts and lowering helpers under `db::access`, replacing executor-side planner-type extension helpers with route-owned helpers, shifting grouped projection-layout validity into the planner handoff contract (so executor no longer calls planner validators), moving grouped HAVING compare semantics under predicate ownership, and adding a layered compile-fail/compile-pass guard suite that locks allowed root import paths and blocks deep internal imports.
- `0.40.18` consolidates projection comparison semantics by routing load projection equality/ordering through shared predicate helpers, while keeping grouped `HAVING` fingerprint and index-predicate compilation contracts stable.
- `0.40.19` rejects `delete().offset(...)` at intent validation instead of silently ignoring it, and removes unused marker types from query wrappers to keep those APIs simpler and clearer.
- `0.40.20` completes the query intent migration by making `query/intent` the approved intent-state authority and moving planning handoff to explicit access/logical DTO contracts without changing query behavior.
- `0.40.21` continues planner cleanup by splitting validation and planner internals into smaller modules (`validate/*`, `planner/*`) to reduce branch pressure and make responsibility boundaries clearer while keeping query behavior unchanged.
- `0.40.22` splits planner expression ownership into `expr/{ast,projection,type_inference}` and tightens continuation-signature wiring so runtime paths consume signature authority from the continuation contract.
- `0.40.23` keeps query behavior stable while splitting large fluent/predicate files into focused modules, moving cursor/order execution routing behind one planner-owned `ExecutionOrderContract`, and reducing predicate hash drift risk via shared predicate encoding authority plus canonical normalization before fingerprint/signature hashing.
- `0.40.24` reduces execution-router branching by dispatching through explicit route shapes, moving pushdown/index-range eligibility checks behind one access capability contract, and tightening continuation-envelope debug guards while keeping query behavior unchanged.
- `0.40.25` continues continuation wiring by making runtime paths consume `ContinuationContract` accessors directly, replacing scalar runtime naming with `ScalarContinuationContext`, routing executor continuation reads and continuation-activation/policy/anchor gates through one `RouteContinuationPlan` projection boundary (without free gate wrappers), removing remaining load fast-path order derivation from plan internals, and hardening grouped fold/stream stage boundaries behind constructor/accessor APIs.
- `0.40.26` continues continuation cleanup by splitting the access-stream hub into smaller modules, making load entrypoint leaf handlers consume pre-resolved continuation context instead of resolving tokens themselves, and adding structural guard tests to keep those boundaries from drifting.
- `0.40.27` pre-resolves access execution contracts once in the access layer, so planner/query/executor runtime paths consume one shared access strategy instead of repeatedly re-lowering access shape details.

See detailed breakdown:
[docs/changelog/0.40.md](docs/changelog/0.40.md)

---

## [0.39.x] 🔢 - 2026-03-02 - Numeric Consolidation

- `0.39.0` consolidates numeric capability checks under shared helpers and tightens planner expression typing so numeric operators/aggregates fail early on known non-numeric fields while mixed numeric expressions still work when subtype cannot be resolved yet.
- `0.39.1` starts runtime numeric convergence by routing projection and aggregate decimal coercion through one shared helper, so mixed numeric comparisons/arithmetic behave consistently and mixed numeric-vs-non-numeric equality now fails as an invariant error instead of silently returning false.
- `0.39.2` consolidates numeric runtime semantics by formalizing the shared arithmetic contract (promotion/coercion/overflow/division), converging predicate/HAVING/range-bound numeric comparison paths on shared numeric compare authority, and aligning aggregate `sum/avg` plus grouped global `SUM(DISTINCT field)` arithmetic with that same contract while keeping strict index pushdown boundaries explicit.
- `0.39.3` adds numeric identity drift guards for continuation signatures and plan fingerprints, including literal/promotion stability checks (`1 + 2` vs mixed numeric literal forms), aggregate/DISTINCT no-op promotion-path checks, and alias-only continuation decode/resume stability coverage.

See detailed breakdown:
[docs/changelog/0.39.md](docs/changelog/0.39.md)

---

## [0.38.x] 🪢 - 2026-03-02 - Projection Expression Spine

- `0.38.0` cleans up query and error internals before unified expressions: projection building now follows one path, planner/executor duplicate rule checks are reduced, aggregate fingerprints ignore alias/explain-only metadata, runtime error class/origin are preserved at the public boundary, and architecture guards now use structural tests instead of source-text scans.
- `0.38.1` hardens fluent field-target terminal dispatch by requiring planner slot routing, removing runtime fallback from production query paths, and adding guard coverage so slot-first behavior cannot silently drift back.
- `0.38.2` makes `ProjectionSpec` the grouped output authority, evaluates grouped rows through expression projections, and ties continuation/fingerprint identity to projection semantics (alias-only changes stay stable while semantic changes invalidate).
- `0.38.3` removes the remaining route-layer `include_str!` architectural-policing tests and replaces continuation-profile `ProjectionDefault` sectioning with explicit grouped-shape hashing.

See detailed breakdown:
[docs/changelog/0.38.md](docs/changelog/0.38.md)

---

## [0.37.x] 🧮 - 2026-03-01 - Aggregate Fluent API Consolidation

- `0.37.0` added a simpler grouped query style with `.aggregate(...)` plus reusable aggregate builders like `count()`, `sum("field")`, and `distinct()`.
- `0.37.0` removed older `group_*` aggregate helper methods so grouped aggregation uses one consistent API path.
- `0.37.1` added migration safety checks and tests so removed grouped helpers fail clearly and `.aggregate(...)` replacements are verified.
- `0.37.2` cleaned up grouped route labeling and DISTINCT helper ownership so grouped execution behavior is easier to reason about.
- `0.37.3` reduced grouped execution complexity by splitting responsibilities into clearer stages and centralizing key routing/validation paths.
- `0.37.4` unified cursor paging and access-normalization handling to reduce inconsistent behavior across query paths.
- `0.37.5` added stronger invariant and regression coverage to lock core execution behavior and catch drift earlier.
- `0.37.6` preserved runtime error class and origin details at public boundaries so client-side error handling can be more precise.

```rust
let page = session
    .load::<Order>()
    .group_by("user_id")?
    .aggregate(count())
    .aggregate(sum("rank").distinct())
    .execute_grouped()?;
```

See detailed breakdown:
[docs/changelog/0.37.md](docs/changelog/0.37.md)

---

## [0.36.x] 🪜 - 2026-03-01 - Ordered Group Strategy Foundations

- `0.36.0` established grouped strategy routing (`HashGroup` vs `OrderedGroup`) and conservative grouped `HAVING`, with deterministic downgrade and explain/fingerprint coverage.
- `0.36.1` and `0.36.2` delivered grouped DISTINCT contracts, including zero-key global `COUNT(DISTINCT field)` / `SUM(DISTINCT field)` routed through grouped budget accounting with typed limit failures.
- `0.36.3` closes grouped hardening with explicit grouped cursor-`LIMIT` gating, centralized grouped DISTINCT admission, bounded grouped-buffer assertions, fingerprint-snapshot locking, and deterministic `SUM(DISTINCT)`/continuation-matrix regression coverage.

```rust
let page = session
    .load::<Order>()
    .group_sum_distinct_by("rank")
    .execute_grouped()?;
```

See detailed breakdown:
[docs/changelog/0.36.md](docs/changelog/0.36.md)

---

## [0.35.x] 👥 - 2026-03-01 - GROUP BY

- Added fluent `GROUP BY` builders and grouped execution entrypoints, including grouped `min/max` id terminals and grouped pagination without requiring explicit `ORDER BY`.
- Closed the `0.35.1` grouped hardening checklist with streaming predicate folding, bounded grouped `LIMIT` paging, deterministic grouped strategy metrics, grouped `DISTINCT`/`ORDER BY` policy gates, and grouped continuation anti-split coverage.
- Added clearer grouped-vs-scalar API boundary errors, while keeping grouped field-target extrema deferred in grouped v1.

```rust
let page = db
    .load_query::<Order>()
    .group_by("customer_id")
    .group_count()
    .execute_grouped(50)?;
```

See detailed breakdown:
[docs/changelog/0.35.md](docs/changelog/0.35.md)

---

## [0.34.x] 🧱 - 2026-03-01 - Boundary Cleanup and Readiness Track

- Consolidated major executor/query/cursor boundaries across the line, including route contract cleanup, staged planner ownership, and stream-access physical resolution ownership.
- Tightened cursor and continuation authority through explicit spine/window/signature invariants and clearer token/error construction boundaries.
- Continued structural decomposition work ahead of `0.35`, including aggregate contract modularization and no-shim cleanup in commit/recovery paths.

```text
query intent -> route intent -> route capability -> execution mode
cursor token -> codec decode -> cursor invariant checks -> resume window
```

See detailed breakdown:
[docs/changelog/0.34.md](docs/changelog/0.34.md)

---

## [0.33.x] 🔌 - 2026-02-28 - Grouped Runtime Activation and Surface Consolidation

- Activated grouped query runtime from planning through execution, including grouped cursor continuation/resume support, grouped route observability, stronger grouped continuation-signature safety across query-shape changes, and explicit cursor-variant handling in load paths (without scalar panic accessors).
- Reduced internal `db/` surface complexity with stream ownership cleanup, load-terminal dispatch consolidation, full predicate-spine convergence under one dedicated `db::predicate` boundary, access-plan canonicalization ownership moved into `db::access`, aggregate taxonomy unified under one query-owned enum authority across explain/fingerprint/executor, query planning decoupled from executor runtime shape through an explicit query-owned compile handoff, cursor planning/execution contracts decoupled from concrete query-plan carriers, query-shape policy ownership converged under `db::query::policy`, and grouped spec consumption converged on query-owned grouped spec contracts without executor-side projection types.
- Hardened execution correctness and recovery by enforcing grouped hard limits against unique canonical groups, keeping commit replay/index rebuild semantics schema-fingerprint guarded and row-authoritative, and removing pre-marker store mutation from commit-window preflight via in-memory simulation.

```rust
let grouped = db
    .load_query::<Event>()
    .group_by("type")
    .group_last("created_at")
    .execute_grouped(25)?;
```

See detailed breakdown:
[docs/changelog/0.33.md](docs/changelog/0.33.md)

---

## [0.32.x] 🛤️ - 2026-02-27 - Grouped Substrate and Readiness Track

- Built grouped planning and executor substrate foundations, including grouped budgets, route scaffolding, and determinism/observability guardrails.
- Kept grouped runtime intentionally disabled across this line while hardening contracts and readiness boundaries ahead of runtime enablement.

See detailed breakdown:
[docs/changelog/0.32.md](docs/changelog/0.32.md)

---

## [0.31.x] 🔑 - 2026-02-27 - Deterministic Key Substrate

- Hardened pre-`GROUP BY` key/equality substrate with canonical group-key semantics and stable hashing contracts to keep DISTINCT behavior deterministic.
- Aligned ordering/equality contracts and field-target distinct reducers around one canonical key model to reduce runtime drift across execution paths.

```text
canonical_key(value) -> stable_hash
canonical_eq(lhs, rhs) => hash(lhs) == hash(rhs)
```

See detailed breakdown:
[docs/changelog/0.31.md](docs/changelog/0.31.md)

---

## [0.30.x] ⚡ - 2026-02-27 - Execution Kernel Consolidation and Boundary Cleanup

- Consolidated read and aggregate execution ownership into kernel-centered paths, with focused parity work across routing, cursor continuation, DISTINCT handling, and fallback behavior.
- Completed broad `db/` boundary cleanup across plan, cursor, direction, access, and commit ownership to tighten layering and reduce structural duplication.
- Expanded invariant guards and structural/regression coverage across the line to catch architectural drift earlier while preserving query semantics.

```text
plan -> preparation -> route -> kernel -> post_access -> response
```

See detailed breakdown:
[docs/changelog/0.30.md](docs/changelog/0.30.md)

---

## [0.29.x] 🛡️ - 2026-02-25 - Pre-GROUP BY Hardening and Query Boundary Cleanup

- Delivered the pre-`GROUP BY` hardening line with deterministic behavior, stronger cursor/recovery safeguards, and expanded invariant coverage.
- Completed the `db/query` Audit I-III boundary work, including planning/lowering separation, explicit routing contracts, and tighter cursor/runtime ownership.
- Optimized routed key-stream and fast-path execution while preserving scan-budget and result parity across streaming/materialized paths.

```text
if route.streaming_eligible { stream() } else { materialize() }
```

See detailed breakdown:
[docs/changelog/0.29.md](docs/changelog/0.29.md)

---

## [0.28.x] 🖼️ - 2026-02-24 - Projection Terminal Expansion and Routing Hardening

- Added a full projection/ranking terminal family (`values_by*`, `distinct_values_by`, `first/last_value_by`, `top_k_by`, `bottom_k_by`, and id/value variants) with deterministic ordering contracts.
- Kept load execution semantics stable by applying projection/ranking as terminal reductions over canonical effective windows, without introducing new routing or cursor behavior.
- Hardened continuation and route-layer invariants with boundary cleanup plus parity/regression coverage for scan budgets, direction invariance, unknown-field fail-before-scan, and execute-equivalence.

```rust
let top = db.load_query::<User>().top_k_by_values("score", 10)?;
let pairs = db.load_query::<User>().top_k_by_with_ids("score", 10)?;
```

See detailed breakdown:
[docs/changelog/0.28.md](docs/changelog/0.28.md)

---

## [0.27.x] 🔐 - 2026-02-23 - Index-Only and Runtime Hardening

- Introduced index-only predicate execution for eligible load queries, reducing row reads while preserving result parity.
- Hardened runtime safety across commit/index/data/cursor boundaries with stricter validation, clearer error/reporting paths, and fail-closed guardrails.
- Expanded observability and regression coverage for routing, continuation behavior, DISTINCT handling, and fallback parity.

See detailed breakdown:
[docs/changelog/0.27.md](docs/changelog/0.27.md)

---
## [0.26.x] 🎯 - 2026-02-23 - Compiled Field Projection Hardening

- `0.26.0` moved runtime field projection from name lookups to slot/index access and removed `FieldProjection::get_value(...)` in favor of `get_value_by_index(...)`.
- `0.26.1` and `0.26.2` hardened this rollout with stricter index invariants, CI projection guardrails, and full slot-resolved ordering/predicate runtime paths.
- The line closes with hot-loop field access consistently using pre-resolved slots, reducing runtime lookup drift and keeping behavior deterministic.

See detailed breakdown:
[docs/changelog/0.26.md](docs/changelog/0.26.md)

---

## [0.25.x] ➕ - 2026-02-23 - Field Aggregate Expansion

- `0.25.0` introduced field aggregate terminals (`min_by`, `max_by`, `nth_by`, `sum_by`, `avg_by`) with deterministic ordering and fail-fast target validation.
- `0.25.1` and `0.25.2` focused on parity and eligibility hardening so fallback/fast-path behavior and error classification stay stable.
- `0.25.2` expanded the surface with `median_by`, `count_distinct_by`, and `min_max_by`, while preserving canonical effective-window semantics.

See detailed breakdown:
[docs/changelog/0.25.md](docs/changelog/0.25.md)

---

## [0.24.x] 🛣️ - 2026-02-22 - Aggregate Route Foundation

- `0.24.0` introduced composite aggregate direct-path routing with parity-focused behavior locking against fallback execution.
- `0.24.1` added `first()` and `last()` terminals with explicit cursor and bounded-scan guardrails.
- `0.24.2` through `0.24.7` hardened route capability ownership, descending/scan-hint safety, secondary extrema probe fallback behavior, and pre-`0.25` field-aggregate validation boundaries.

See detailed breakdown:
[docs/changelog/0.24.md](docs/changelog/0.24.md)

---

## [0.23.6] 🧱 – 2026-02-21 - IndexRange Aggregate Direct Path

### 📝 Summary

* Added a direct aggregate fast path for `IndexRange` query shapes.
* Kept aggregate results unchanged while reducing aggregate routing overhead for index-range reads.

### 🔧 Changed

* Aggregate execution now short-circuits eligible `AccessPath::IndexRange` plans through the existing bounded index-range traversal path.
* Added explicit spec-boundary guards for index-range aggregate fast-path usage (exact range-spec arity and no prefix-spec mixing).

### 🧹 Cleanup

* Reduced `clippy::too_many_arguments` pressure in executor hot paths by bundling related stream and fast-path inputs into small internal state/input structs, lowering drift risk without changing query behavior.

### 🧪 Testing

* Added index-range aggregate scan-budget coverage for windowed `exists` (`offset + 1`).
* Added invariant tests for index-range aggregate fast-path spec assumptions.

### 🛣️ Roadmap

* This patch closes the aggregate fast-path expansion for the `0.23.x` line while keeping semantics parity-first and boundary-safe.
* It finishes the patch-scope step that this series was building toward: direct aggregate handling for index-range traversal.
* This work unlocks a cleaner `0.24` decision point, focused on a new feature track rather than more patch-level routing hardening.

---

## [0.23.5] ⚡ – 2026-02-21 - Aggregate Access-Path Fast Paths

### 📝 Summary

* Added small aggregate fast paths for primary-key point, key-batch, index-prefix, and primary-data range/scan shapes.
* Kept aggregate results unchanged while reducing work for `by_id`/`by_ids`, eligible index-prefix, and PK-ordered `KeyRange`/`FullScan` aggregate terminals.

### 🔧 Changed

* Aggregate execution now short-circuits `AccessPath::ByKey`, `AccessPath::ByKeys`, eligible `AccessPath::IndexPrefix`, `AccessPath::KeyRange`, and `AccessPath::FullScan` plans directly instead of building the generic ordered key stream.
* Preserved existing consistency and window semantics for `count`, `exists`, `min`, and `max`.
* Added a defensive fast-path arity guard for secondary aggregate prefix specs to fail fast on planner/executor drift.

### 🧪 Testing

* Added regressions for windowed `by_id`/`by_ids` aggregate parity, dedup-before-window behavior, secondary index-prefix `MissingOk` scan safety, `KeyRange`/`FullScan` scan budgeting, and strict missing-row classification (including secondary-prefix traversal).

---

## [0.23.4] 🛠️ – 2026-02-21 - Boundary Hardening

### 📝 Summary

* Hardened aggregate execution parity and execution boundaries so optimized and fallback paths stay aligned.
* Focused this release on safety and drift resistance, without introducing new query features.

### 🔧 Changed

* Tightened aggregate-path parity checks across `ASC`/`DESC`, `DISTINCT`, and windowed reads (`offset`/`limit`).
* Disabled bounded COUNT probe hints for `DISTINCT + offset` aggregate windows so deduplication and offset application stay parity-safe under future access-path changes.
* Kept planner/executor boundary hardening explicit: index traversal remains spec-driven with strict invariant enforcement against semantic fallback drift.
* Added runtime fast-path spec-arity guards so secondary and index-range optimizations fail fast if multiple lowered specs are unexpectedly present.

### 🧪 Testing

* Expanded targeted regressions for aggregate parity across direction, distinctness, paging, and continuation behavior.
* Kept drift-guard coverage in place for planner/executor boundary invariants so semantic handling does not leak back into execution index paths.

---

## [0.23.3] 📐 – 2026-02-21 - Index Range Spec Boundary

### 📝 Summary

* Moved index-range execution to pre-built key-range specs.
* Planner now prepares the range bytes up front, and execution just applies them.

### 🔧 Changed

* Added `IndexRangeSpec` as the concrete planner output for index range traversal.
* Executor index-range paths now require that pre-lowered spec instead of interpreting value bounds at runtime.
* Composite access traversal now consumes specs in order and fails fast on missing or unused specs.
* Planner now owns index literal canonicalization for both range and prefix plans, so execution stays byte-only for index traversal.
* Tightened index key builder boundaries so non-index modules use higher-level raw key helpers instead of calling low-level prefix/range builders directly.

### 🧪 Testing

* Added executor invariant regressions for missing index-range specs and unused lowered specs.
* Added a CI and pre-push invariant check that blocks index-path executor code from drifting back to semantic `Value` handling.
* Revalidated prefix-bound traversal behavior and kept the current closed-bound contract using canonical sentinel keys.

---

## [0.23.2] 🧬 – 2026-02-21 - EncodedValue Consolidation

### 📝 Summary

* Consolidated index encoding around one shared `EncodedValue` path.
* Kept query behavior the same while reducing repeated encode work in planning and lookup.

### 🔧 Changed

* `RawIndexKey` builders now accept only `EncodedValue`, making canonical index bytes a hard input invariant instead of a convention.
* Added `EncodedValue` as the shared wrapper for canonical index bytes used by lookup and range bound construction.
* Planner index prefix/range selection now caches encoded literals and reuses them across candidate evaluation instead of re-encoding.
* Added a compile-time derive guard so `prim = "Decimal"` requires `item(scale = N)`.

### 🧪 Testing

* Added a compile-fail regression for missing decimal `scale` on `Decimal` schema fields.

---

## [0.23.1] 🧾 – 2026-02-21 - Decimal Scale Boundary Hardening

### 📝 Summary

* Follow-up hardening for the decimal consolidation release.
* Tightened write and decode checks so decimal scale mismatches fail early and clearly.

### 🔧 Changed

* Decimal fields now require an explicit schema scale (`item(scale = N)`).
* Save paths now reject mixed-scale decimal writes before persistence.
* Update/decode paths now reject persisted rows that violate decimal scale rules.
* Aligned decimal roadmap/status doc references.

### 🧪 Testing

* Added regressions for mixed-scale write rejection, persisted-row scale drift rejection, and invalid decimal binary scale decode.

---

## [0.23.0] 🔢 – 2026-02-21 - Decimal Consolidation

### 📝 Summary

* Unified decimal handling behind one internal decimal implementation.
* Removed split fixed-point paths so runtime behavior is simpler and more consistent.

### 🔧 Changed

* Query/value runtime paths now use one decimal representation.
* Removed runtime reliance on `rust_decimal`.
* Added schema support for explicit decimal scale metadata with `item(prim = "Decimal", scale = N)`.
* Added decimal-backed compatibility wrappers for common fixed-scale finance usage (`E8s`, `E18s`).

### ⚠️ Breaking

* Removed `Primitive::E8s` and `Primitive::E18s` from schema/runtime primitive surfaces.

### 🧭 Migration Notes

* Replace primitive `E8s`/`E18s` usage with decimal fields plus explicit scale metadata, or use wrappers in `base::types::finance`.

---

## [0.22.2] ✅ – 2026-02-21 - Aggregate Sealing + DESC Early-Stop Parity

### 📝 Summary

* Hardened aggregate execution so DESC traversal keeps early-stop parity with ASC, and aggregate streaming now runs through one shared fold engine.
* Kept aggregate results unchanged while reducing drift risk between `count`, `exists`, `min`, and `max` paths.

### 🔧 Changed

* Added directional aggregate probe hints so `max()` on DESC and `min()` on ASC can stop key production early on eligible paths.
* Unified aggregate streaming under one fold path, including `count()` pushdown through key-only fold mode instead of a separate streaming engine.
* Kept COUNT pushdown as a strict subset of streaming eligibility and preserved canonical fallback behavior for unsupported shapes.
* Kept DISTINCT + offset probe safety guards aligned with aggregate streaming rules.

### 🧪 Testing

* Added targeted aggregate regressions for DESC early-stop scan budgets, `distinct + offset` probe-hint suppression, stale-leading-key behavior under `MissingOk`, and count-pushdown eligibility matrix parity.

---

## [0.22.1] 🧮 – 2026-02-20 - Aggregate Count Pushdown

### 📝 Summary

* Improved `count()` performance for eligible queries while keeping results unchanged.

```rust
let total = session.load::<MyEntity>().order_by("id").offset(10).limit(25).count()?;
```

### 🔧 Changed

* `count()` now uses a faster path for safe query shapes, and page-aware counting can stop earlier when possible.
* `exists()` now exits earlier in more cases, with a safety guard to keep `distinct + offset` behavior correct.
* Reorganized the entire db/ module tree so responsibilities are clearer.
* Single-row and batch save paths now share one pre-check flow, and save validation error classification is more consistent.
* Did a pass to remove the number of generics on types and functions.

---

## [0.22.0] 🌊 – 2026-02-20 - Streaming Aggregates

### 📝 Summary

* Added aggregate query terminals for loads: `count`, `exists`, `min`, and `max`.
* `min` and `max` return primary keys (`Option<Id<E>>`).

```rust
let count = session.load::<MyEntity>().order_by("id").limit(50).count()?;
```

### 🔧 Changed

* Aggregates now follow the same internal key path as normal loads, so behavior stays consistent across paths.
* Load execution now resolves one key stream first and then runs shared paging/finalization steps, which keeps normal-load and aggregate behavior aligned.
* `min`/`max` now take advantage of sort direction to return earlier when safe.
* Aggregate results now respect pagination windows (`offset`/`limit`) the same way normal loads do.
* Added a small internal probe for `exists` and wired fetch hints across PK, secondary-prefix, and index-range fast paths so they can stop earlier.
* Expanded parity coverage so aggregate results match regular query results across key query shapes.

### 🧹 Cleanup

* Unified the safety check used by load and aggregate streaming to keep rules aligned.
* Added a clear hook for upcoming `count` pushdown work without changing behavior in `0.22.0`.

---

## [0.21.1] 🪴 – 2026-02-20 - Post-Release Cleanup

### 🧹 Cleanup

* Reduced repeated index-plan corruption mapping by introducing origin-specific `InternalError` helpers and simplified index-plan modules to use those helpers directly.
* Split `executor/tests/pagination.rs` into focused pagination submodules with shared helpers in `pagination/mod.rs` to reduce test maintenance load.
* Split `query/plan/planner.rs` into focused internal submodules (`planner/range.rs`, `planner/normalize.rs`) to reduce planner file pressure without changing planning behavior.
* Split `query/plan/logical.rs` helpers into focused internal submodules (`logical/order_cursor.rs`, `logical/window.rs`) to reduce logical-plan file pressure without changing execution semantics.
* Normalized index lookup invariant-message construction so executor-invariant prefixes are generated through one path.
* De-duplicated ASC/DESC index-range continuation-advance checks into one helper to keep guard behavior consistent.
* Simplified `RawRowError -> InternalError` mapping to use canonical store-unsupported construction.
* Added focused regression checks for index-plan corruption helper origins and raw-row error mapping.
* Updated `docs/status/0.21-status.md` to reflect that `0.21.0` is shipped and complete.

```rust
InternalError::index_plan_index_corruption(message)
```

---

## [0.21.0] 📍 – 2026-02-20 - Cursor Offset

### 📝 Summary

* Added cursor paging support for offset queries.
* Offset is now applied on the first page only, and continuation pages resume correctly without re-applying it.

```rust
validate_cursor_window_offset(expected_initial_offset, actual_initial_offset)?;
```

* Latest audit snapshot (risk index, lower is better):

```text
Invariant Integrity  4/10
Recovery Integrity   4/10
Cursor/Ordering      3/10
Index Integrity      3/10
State-Machine        4/10
Structure Integrity  4/10
Complexity           6/10
Velocity             6/10
DRY                  4/10
Taxonomy             4/10
```

### 🔧 Changed

* Cursor validation and anchor handling were consolidated so resume rules are enforced in one place.
* Pagination coverage was expanded for offset + continuation, including DISTINCT and ASC/DESC parity checks.
* Fixed ten of the lowest hanging fruit from the recent audit, including cleaner cursor checks, shared pagination helpers, and more consistent internal error handling.

---

## [0.20.1] 🧯 – 2026-02-20 - Error Mapping Consolidation

* Consolidated internal error construction so executor/query/store origin mapping is more consistent and easier to audit.

```rust
InternalError::serialize_corruption(format!("{payload_label} decode failed: {source}"))
```

---

## [0.20.0] ✨ – 2026-02-20 - DISTINCT Row Deduplication

### 📝 Summary

* Added `DISTINCT` for full-row query results.
* DISTINCT now runs as an ordered stream step, so paging and continuation stay consistent across fast-path and fallback execution.

```rust
let query = Query::<DistinctEntity>::new(ReadConsistency::MissingOk).distinct();
```

### 🔧 Changed

* Added `Query::distinct()` planning support via a `distinct` flag on `LogicalPlan`.
* Added `DistinctOrderedKeyStream` to suppress adjacent duplicate keys from ordered streams.
* Wired DISTINCT into both fast-path and fallback load execution before row materialization.
* Included DISTINCT in explain/fingerprint/continuation-signature shape so continuation tokens stay query-compatible.
* Completed DISTINCT pagination/resume matrix coverage across ASC and DESC traversal shapes.

---

## [0.19.1] 🧹 – 2026-02-20 - Execution Structure Cleanup

### 📝 Summary

* Reduced executor complexity with mechanical refactors that keep behavior the same.
* Tightened module boundaries so routing, stream composition, physical key resolution, and tracing are easier to read and maintain.

```rust
// Routing now follows one decision step, then one materialization step.
let decision = Self::evaluate_fast_path(inputs, fast_path_plan)?;
```

### 🔧 Changed

* Moved physical access-path key resolution into `db::executor::physical_path` (`resolve_physical_key_stream`).
* Moved composite stream reduction logic (`AccessPlan` union/intersection stream building) into `db::executor::composite_stream`.
* Added `ExecutionInputs` in load execution to group shared execution inputs in one place.
* Split fast-path routing from execution with `evaluate_fast_path(...)` and `FastPathDecision`.
* Unified fast-path and fallback finalization through one `finalize_execution(...)` helper.
* Moved index-range limit pushdown assessment next to routing in `load/route.rs`.
* Moved trace-only access-shape projection logic into `load/trace.rs`.
* Simplified boundary/envelope API by using `KeyEnvelope` directly instead of thin wrapper helpers.

### 🧹 Cleanup

* Reduced branch-heavy routing code in `load/execute`.
* Reduced `context.rs` and `load/mod.rs` fan-out by moving domain-specific blocks into focused submodules.

---

## [0.19.0] ↕️ – 2026-02-20 - Mixed-Direction ORDER BY

### 📝 Summary

* Added full support for mixed-direction `ORDER BY` (for example, one field descending and the next ascending).
* Kept paging behavior stable, with no cursor format or storage changes.

```rust
let query = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
    .order_by_desc("rank")
    .order_by("id");
```

### 🔧 Changed

* Updated internal stream ordering so `Union` and `Intersection` follow mixed-direction sort rules correctly.
* Unified paging resume checks so continuation boundaries are handled the same way across paths.
* Expanded mixed-direction paging coverage to include resume-from-each-boundary cases for two-field and three-field order patterns.
* Moved physical access-path key resolution into `db::executor::physical_path` and renamed the resolver to `resolve_physical_key_stream` without changing behavior.

---

## [0.18.2] 🔍 – 2026-02-20 - Complexity Audit Follow-Through

### 📝 Summary

* Continued the complexity audit with low-risk cleanup focused on consistency.
* Reduced code spread in save flow, boundary handling, pagination tests, and internal error mapping.

### 🔧 Changed

* Simplified save-path wiring while keeping mode and lane behavior explicit.
* Finished boundary handling cleanup so continuation/range checks use one consistent path.
* Reduced pagination test boilerplate with shared helpers (`4911` lines to `4819` lines).
* Moved repeated planner/index error-mapping helpers into one canonical `InternalError` location.

### 🧹 Cleanup

* Removed duplicate internal error-mapping blocks in plan modules.
* Replaced repeated terminal resume assertions in pagination tests with shared checks.

---

## [0.18.1] 🧪 – 2026-02-19 - Composite Pagination Closure + Test Macro Helpers

### 📝 Summary

* Folded composite pagination correctness tracking into `0.18` so budgeting and continuation invariants live under one milestone record.

### 🔧 Changed

* Added a suite of macro test helpers to reduce repeated test boilerplate and keep test schemas consistent:

```rust
test_canister!(ident = RecoveryTestCanister);
test_store!(ident = RecoveryTestDataStore, canister = RecoveryTestCanister);
test_entity!(ident = RecoveryTestEntity { id: Ulid });
test_entity_schema!(ident = RecoveryTestEntity, id = Ulid, id_field = id, ...);
```

### 🧪 Testing

* Added explicit composite child-order permutation tests for both `Union` and `Intersection` to lock row-sequence and decoded continuation-boundary invariance.

---

## [0.18.0] 📏 – 2026-02-19 - Execution Scan Budgeting

### 📝 Summary

* Added a safe internal scan cap so eligible composite queries can stop reading keys earlier while returning the same rows and the same continuation cursor behavior.
* This is an internal optimization only. If a query shape is not proven safe, icyDB keeps the previous full-scan behavior.

```rust
let budget = offset.saturating_add(limit).saturating_add(1);
```

### 🔧 Changed

* Added `BudgetedOrderedKeyStream` to cap key polling when a plan is known to be safe.
* Added guarded scan budget derivation (`offset + limit + 1`) for eligible load plans.
* Added explicit budget-safety checks on `LogicalPlan` so the executor can make one clear yes/no decision before applying the optimization.
* Kept budget wrapping at one boundary (`LoadExecutor::materialize_key_stream_into_page`) to avoid semantic drift.

### 🧪 Testing

* Added tests that confirm budgeted streams stop correctly and never over-poll.
* Added ASC/DESC composite coverage for safe budgeted paths.
* Added guard tests for cursor-present, residual-filter, and post-sort cases to confirm fallback behavior stays unchanged.
* Added parity tests to confirm budgeted and non-budgeted paths produce the same page rows and continuation boundaries.

---

## [0.17.0] 🧩 – 2026-02-19 - Composite Intersection Stream Execution

### 📝 Summary

* Completed the `0.17` composite streaming milestone by moving `AccessPlan::Intersection` from materialized set intersection to stream-native pairwise execution.
* Composite `Union` and `Intersection` now share the same pull-based execution model, with ordering and invariant behavior preserved.

```rust
let intersected: OrderedKeyStreamBox =
    Box::new(IntersectOrderedKeyStream::new(left, right, direction));
```

### 🔧 Changed

* Added `IntersectOrderedKeyStream` and wired `AccessPlan::Intersection` to reduce child streams pairwise instead of materializing candidate key sets.
* Removed the old intersection candidate-set path (`collect_candidate_keys`) from composite executor flow.
* Preserved ordering semantics, continuation behavior, error classification, index encoding, and public API surface.
* Simplified `MergeOrderedKeyStream` internals to single-item lookahead state, matching intersection stream structure without changing output behavior.

### 🧪 Testing

* Added/expanded intersection coverage for ASC, DESC, no-overlap, duplicate suppression (including duplicates on both sides), and monotonic-direction violation handling.
* Added executor-level regression coverage for nested composite intersection plans and `DESC + LIMIT + continuation` traversal with no duplicates or omissions.

### 📚 Documentation

* Updated `docs/status/0.17-status.md` to mark `0.17` intersection-streaming scope complete with current verification results.

---

## [0.16.4] 🎛️ – 2026-02-19 - Enum Filter Path Normalization

### 📝 Summary

* Refined enum filter construction so loose enum literals are normalized to the schema enum path before validation and execution.
* Strict enum comparison remains strict; this change removes filter-construction ambiguity without relaxing equality behavior.

```rust
// Loose filter input:
Value::Enum(ValueEnum::loose("Active"))
// Normalized predicate literal:
Value::Enum(ValueEnum::new("Active", Some("entity::Stage")))
```

### 🔧 Changed

* Added a dedicated query-boundary normalizer (`db::query::enum_filter`) that resolves enum paths per field schema.
* Added enum path metadata to runtime `FieldKind::Enum { path: ... }` and threaded it through schema info for field-scoped resolution.
* Applied enum normalization in both dynamic `filter_expr` lowering and typed query filter planning.
* Kept `Value` equality semantics, index encoding, storage layout, and wire formats unchanged.

### 🧪 Testing

* Added tests for strict enum success, strict wrong-path rejection, loose enum path resolution, and `IN` stage filter normalization.
* Added audit tests proving enum normalization is idempotent and field-scoped when different enum fields share the same variant names.

---

## [0.16.3] ⏱️ – 2026-02-19 - Temporal Millisecond Consistency

### 📝 Summary

* Hardened `Timestamp`/`Duration` temporal semantics so arithmetic and encoding are consistently millisecond-based with no unit drift.

```rust
let a = Timestamp::from_millis(5_000);
let b = Timestamp::from_millis(2_000);
assert_eq!(a - b, Duration::from_millis(3_000));
```

### 🔧 Changed

* Standardized timestamp arithmetic to `Timestamp +/- Duration` and `Timestamp - Timestamp -> Duration`, all in `u64` milliseconds.
* `Timestamp::parse_flexible` now interprets bare integer input as milliseconds (matching internal/storage semantics) instead of implicitly treating integers as seconds.
* Kept ordered index encoding unchanged (`u64` big-endian) while adding monotonicity coverage for timestamp ordered bytes.

### 🧪 Testing

* Added contract tests for millisecond precision, no-truncation arithmetic, timestamp-difference duration semantics, and timestamp ordered-encoding monotonicity.

---

## [0.16.2] 🧾 – 2026-02-19 - DRY Survey

### 📝 Summary

* Reduced direction-handling drift in executor key production by routing ordered key normalization through one shared helper.

```rust
pub(crate) fn normalize_ordered_keys(
    keys: &mut Vec<DataKey>,
    direction: Direction,
    already_sorted: bool,
)
```

### 🔧 Changed

* Centralized executor ordered-key direction normalization behind `normalize_ordered_keys(...)`, so PK stream, secondary index stream, and access-path key production all use one sort/reverse path.
* Added shared index-range bound encode reason mapping (`map_bound_encode_error(...)`) and reused it in planner cursor validation and index-store range lookup without changing boundary-specific error classes.
* Added relation-owned error helpers (`target_key_mismatch_error(...)`, `incompatible_store_error(...)`) and routed save-time strong-relation checks through them to keep operator-facing wording consistent.
* Added domain-specific `InternalError` constructors for repeated `(ErrorClass, ErrorOrigin)` pairs and routed repeated call sites through those constructors to reduce taxonomy drift while preserving exact classes, origins, and messages.

---

## [0.16.1] ⏲️ – 2026-02-19 - Timestamps in Milliseconds

### 📝 Summary

* Finalized an internal representation-boundary refactor and migrated `Timestamp` to millisecond-backed semantics.
* This release keeps query planner/executor flow intact while changing timestamp unit semantics in storage and wire representations.

```rust
assert_eq!(Timestamp::from_secs(42).as_millis(), 42_000);
assert_eq!(serde_json::to_string(&Timestamp::from_secs(42))?, "42000");
```

### 🔧 Changed

* Added internal `Repr` representation boundary usage for timestamp- and duration-adjacent internals.
* Introduced internal `OrderedEncode` delegation for fixed-width ordered index component encoding.
* Updated timestamp-related value normalization and numeric conversion paths to operate on millisecond representation.
* Updated timestamp parsing and `now()` behavior to produce millisecond-backed values.

### ⚠️ Breaking

* `Timestamp` now serializes and persists as milliseconds instead of seconds. Existing second-based persisted timestamp bytes require migration before rollout.

---

## [0.16.0] 🔀 – 2026-02-19 - Composite Union Stream Execution

### 📝 Summary

* Completed the `0.16.0` union-stream execution milestone for composite `Union` paths.
* `Union` execution now uses deterministic stream merge with explicit direction safety, while continuation, limit, and filtering ownership remain unchanged.

```rust
// Pairwise union composition in executor context:
let merged: OrderedKeyStreamBox =
    Box::new(MergeOrderedKeyStream::new(left, right, direction));
```

### ➕ Added

* added `delete-tags.sh` ci script.
* Added a new `0.16` milestone status tracker at `docs/status/0.16-status.md`.

### 🔧 Changed

* Updated executor composite planning path so `AccessPlan::Union` now composes child streams with pairwise merge instead of materializing one candidate key set first.
* Added explicit merge-direction construction for union stream composition and invariant failure on child stream direction mismatch.
* Kept `AccessPlan::Intersection` behavior unchanged for now (still materialized and intentionally out of current 0.16 scope).

### 🧪 Testing

* Added semantic coverage for overlapping PK `OR` predicates to lock union de-dup and stable ordering behavior.
* Added targeted coverage for `Union × DESC × LIMIT × Continuation` to catch duplicate/omission regressions in paged descending traversal.
* Added merge-stream mismatch coverage to verify incorrect child direction is rejected as `InvariantViolation`.
* Re-ran focused stream merge, semantics, and composite trace tests with passing results.

---

## [0.15.0] 🪜 – 2026-02-19 - Ordered Key Stream Abstraction

### 📝 Summary

* Completed the `0.15` internal ordered-key stream milestone by moving key collection behind one shared stream interface.
* This is an internal cleanup release. Query results, cursors, pagination behavior, explain output, and metrics behavior are unchanged.

```rust
pub(crate) trait OrderedKeyStream {
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError>;
}
```

### 🔧 Changed

* Added internal `OrderedKeyStream` and `VecOrderedKeyStream` so load execution uses one key-stream model.
* Updated query key production to return stream producers (`produce_key_stream(...)`) instead of exposing direct key vectors.
* Updated load execution (normal path and fast paths) to read keys from streams through one shared row-loading path.
* Added internal `MergeOrderedKeyStream` support so two ordered key streams can be merged in canonical order with duplicate suppression.
* Kept responsibilities in the same places as before: cursor resume/validation logic stays in cursor and range helpers, and filtering/sorting/paging still runs in post-access execution.

### 🧪 Testing

* Added focused stream abstraction unit coverage for deterministic key order and exhaustion behavior.
* Added merge-stream unit coverage for ascending/descending merge order, duplicate suppression, and error propagation.
* Added explicit regression coverage that duplicate `by_ids(...)` input keys are still de-duplicated with stream-backed key production.

### 📚 Documentation

* Added `docs/status/0.15-status.md` and marked the `0.15` completion matrix as complete with verification notes.

---

## [0.14.2] 📐 – 2026-02-18 - Execution Phase Alignment

### 🧹 Cleanup

* Aligned the PK fast path with the same execution phase structure used by other load paths: ordered key iteration, row fetch, then shared row deserialization.
* Removed the PK-only inline decode loop so fast-path execution is easier to reason about and less likely to drift from the shared load pipeline.

### 🔧 Changed

* This is an internal cleanup only; query behavior and pagination semantics remain unchanged.

---

## [0.14.1] ✅ – 2026-02-18 - Validation Unification I

### ➕ Added

* Added a minimal, opt-in load execution trace surface in `icydb-core` for cursor-paged queries via `PagedLoadQuery::execute_with_trace()`.

### 🧹 Cleanup

* Cleaned up cursor checks so the same validation rules are applied in one place, with runtime failures treated as internal invariant issues.
* Kept plan-shape checks at planning time and reduced duplicate runtime re-checks to lightweight safety guards.
* Simplified store-side lookup checks so store code focuses on safe reads and decode integrity instead of user input rules.
* Simplified pushdown checks so they only decide optimization eligibility, not query validity.
* Standardized boundary-arity runtime failures as `InvariantViolation` instead of `Unsupported`.

### 🔧 Changed

* Trace output is debug-only and semantics-neutral: it reports access path variant, direction, pushdown and fast-path decisions, keys scanned, rows returned, and whether continuation was applied.

---

## [0.14.0] 🔽 – 2026-02-18 - DESC Support Complete

### 📝 Summary

* Completed the `0.14` single-path `IndexRange` DESC milestone end-to-end, including planner direction derivation, reverse index-range traversal, directional continuation advancement, and cursor direction validation.
* Closed the `0.14` completion audit with passing workspace gate checks and a fully completed status matrix in `docs/status/0.14-status.md`.

```rust
let page1 = session
    .load::<PhaseEntity>()
    .order_by_desc("rank")
    .limit(20)
    .execute_paged()?;

if let Some(cursor) = page1.next_cursor.clone() {
    let page2 = session
        .load::<PhaseEntity>()
        .order_by_desc("rank")
        .limit(20)
        .cursor(cursor)
        .execute_paged()?;
}
```

### 🔧 Changed

* Wired executable direction selection from canonical `ORDER BY` direction across plan shapes, so execution now carries `Direction::Desc` whenever the leading ordered field is descending.
* Activated reverse store traversal for single-path `IndexRange` DESC execution by iterating raw-key ranges in reverse while preserving the same canonical bound envelope.
* Made continuation advancement checks direction-aware in index range scans (`candidate > anchor` for ASC, `candidate < anchor` for DESC).
* Removed ASC-only gating from secondary-order pushdown eligibility by accepting direction-uniform order specs (`Asc`-uniform or `Desc`-uniform) instead of only ascending specs.
* Enabled descending PK fast-path scans for PK-ordered load plans (`ORDER BY id DESC`) while preserving the same cursor-boundary semantics and fallback parity.
* Enabled descending `IndexRange` limit pushdown for direction-uniform DESC order specs (for example `ORDER BY tag DESC, id DESC`) while keeping mixed-direction shapes on safe fallback paths.

### 🧪 Testing

* Added explicit DESC `IndexRange` edge-case coverage for boundary resume behavior (upper-anchor continuation, lower-boundary exhaustion, and single-element range exhaustion).
* Added explicit multi-page DESC continuation coverage (`E,D` -> `C,B` -> `A`) with no-duplicate and no-omission assertions.
* Added full-result directional symmetry coverage asserting `reverse(ASC) == DESC` on a single-field `IndexRange` dataset.
* Added full-result directional symmetry coverage for composite and unique `IndexRange` paths, asserting `reverse(ASC) == DESC` on deterministic datasets.
* Added explicit DESC continuation coverage for duplicate tie-groups under mixed envelopes (`> lower`, `<= upper`) for both single-field and composite `IndexRange` paths.
* Confirmed duplicate-group DESC ordering keeps canonical PK tie-break stability within equal order values, and validated this alongside DESC continuation edge cases.
* Added descending secondary-order pushdown eligibility coverage for explicit PK-desc tie-break ordering.
* Added descending PK fast-path parity coverage against non-fast execution paths.
* Added descending `IndexRange` limit-pushdown trace coverage for direction-uniform DESC plans.

### 📚 Documentation

* Added `docs/status/0.14-status.md` with milestone-alignment progress, current risk points, and next implementation checkpoints.
* Clarified 0.14 design symmetry policy for duplicate groups: DESC preserves canonical PK tie-break order within equal-value groups, while strict `reverse(ASC) == DESC` assertions apply to deterministic non-duplicate datasets.

---

## [0.13.3] 🔎 – 2026-02-18 - Audits & DESC Preparation

### 📝 Summary

* Prepared the query/execution stack for future DESC support without enabling DESC behavior, while preserving current ASC semantics.
* Completed an initial audit baseline pass and documented outcomes under `docs/audits` and `docs/audit-results`.

### 🔧 Changed

* Added execution-layer `Direction` plumbing (currently `Asc` only) so ordering direction is carried as data without expanding `AccessPath` variants.
* Centralized cursor continuation range rewrites into one helper (`resume_bounds`) and centralized raw-anchor envelope validation into one helper (`anchor_within_envelope`).
* Added a store traversal containment point (`index_range_stream(bounds, direction)`) and threaded direction through planner cursor validation, executor paging, and continuation token encoding.
* Cursor tokens now include direction (`Asc` for now) to keep wire format ready for future DESC execution support without changing current behavior.
* Reduced non-test `AccessPath`/`AccessPlan` branch fan-out by moving dispatch into enum impl methods for planner normalization, canonical ordering, projection, debug summaries, and executor access-plan execution.
* Standardized error-construction discipline so constructor helpers live on owning error types (`impl` associated functions) instead of free-floating `fn ... -> *Error` helpers.
* Moved plan, cursor, relation, index, and predicate error helper constructors into their owning error types while preserving variant payloads and classification semantics.

### 🩹 Fixed

* Kept encoded cursor-token validation strict for `IndexRange` resumes, but restored boundary-only resume support for executor-internal planned cursors so manual continuation boundaries continue to work.
* Restored stable executor invariant messages for PK cursor boundary failures (missing slot, type mismatch, and arity mismatch) after cursor-spine revalidation.
* Resolved the pagination regressions introduced by cursor-spine consolidation; `cargo test -p icydb-core --lib` now passes again.

### 🧪 Testing

* Re-ran targeted pagination regressions plus full library tests after cursor and dispatch containment changes.
* Baseline audit sweep completed across the current tracks:

```text
cursor ordering / boundary semantics
complexity
error taxonomy
invariant preservation
complexity accretion
dry consolidation
```

---

## [0.13.2] 🗂️ – 2026-02-18 - docs/audits

### 📝 Summary

* This release starts a first-pass audit sweep across each defined audit track to establish a consistent baseline before deeper follow-up passes.

### 🧹 Cleanup

* Restored generated companion type names to stable entity-prefixed forms (`TypeView`, `TypeCreate`, `TypeUpdate`) while keeping them in `<type>_views` submodules, avoiding frontend-facing numeric disambiguation names.

### 🥾 Governance

* Expanded the audit framework with additional audit definitions and a runnable guide (`docs/audits/AUDIT-HOWTO.md`) to make review and release checks more repeatable.

### 📚 Documentation

* Reorganized long-form docs into clearer `docs/contracts`, `docs/meta`, and `docs/archive` sections to make it easier to find normative contracts vs reference material.

---

## [0.13.1] 🛠️ – 2026-02-17

### 🧹 Cleanup

* Reduced duplication in `db/executor/tests/pagination.rs` by introducing shared local helpers for pushdown parity, page collection, limit matrices, and boundary resume checks.
* Simplified pagination test setup and repeated assertions with shared helpers (`setup_pagination_test`, ID extraction, cursor-boundary decode), which makes the file easier to extend for 0.14 DESC coverage.
* Split entity integrity decode from storage codec policy by moving key-validation decode logic into `db::entity_decode` (`decode_and_validate_entity_key`).
* Kept behavior and error classification stable while clarifying boundaries: `db::codec` now stays focused on storage decode policy, and executor/entity key checks live in a dedicated integrity layer.

### 🥾 Governance

* Added sample audit documents under `docs/` to make architecture and error-taxonomy review workflows easier to follow and repeat.

---

## [0.13.0] 🚦 – 2026-02-17 - IndexRange LIMIT Pushdown

### 📝 Summary

* `0.13.0` focuses on `LIMIT` pushdown for `AccessPath::IndexRange` to stop scans earlier without changing query semantics.
* It keeps 0.12 cursor behavior intact while reducing unnecessary traversal for large range windows.
* Result ordering, continuation behavior, and plan semantics remain unchanged.

### 🔧 Changed

* Added a limited index-range resolver in `IndexStore` and kept the existing range resolver behavior by delegating through the same path.
* Added executor wiring to use limited range traversal for eligible `IndexRange` + `LIMIT` plans.
* Kept eligibility conservative (no residual predicate, compatible order shape) to avoid semantic drift while rollout is in progress.
* Eligible `IndexRange` plans with `limit=0` now short-circuit without scanning index entries.

### 🧪 Testing

* Added limit-matrix pagination tests for single-field and composite `IndexRange` paths.
* Covered `limit=0`, `limit=1`, bounded page sizes, and larger-than-result windows.
* Verified paginated collect-all results still match unbounded execution order and remain duplicate-free.
* Added exact-size and terminal-page assertions to confirm continuation cursors are suppressed when paging is complete.
* Added a trace assertion for eligible `IndexRange` + `LIMIT` plans to verify access-phase row scans are capped to `offset + limit + 1`.
* Added a trace assertion that `limit=0` eligible plans report zero access-phase rows scanned.
* Added explicit `limit=0` + non-zero `offset` coverage to verify the same zero-scan behavior.
* Isolated executor trace tests with thread-local event buffers so trace assertions stay deterministic under parallel test execution.

### 🧭 Migration Notes

* No public API migration required.
* Binary cursor envelope work is explicitly out of scope for `0.13` and deferred to a later milestone.

---

## [0.12.0] 📜 – 2026-02-17 - Cursor Pagination

### 📝 Summary

* `0.12.0` completes the `IndexRange` cursor hardening work.
* Continuation now resumes from exact raw index-key position, stays inside original range bounds, and keeps page traversal deterministic.
* This reduces duplicate/skip risk and makes pagination behavior more predictable for range-heavy queries.

### 🔧 Changed

* `IndexRange` cursors now carry a raw-key anchor (`last_raw_key`) and resume by rewriting only the lower bound to `Bound::Excluded(last_raw_key)`.
* Cursor validation now checks raw-key decode, index identity/namespace/arity, and envelope membership against the original `(prefix, lower, upper)` range.
* Planning and execution now share one canonical raw-bound builder to prevent drift between validation and store traversal.
* Load execution now passes planned cursor state (boundary + optional raw anchor) through to store-level range traversal.

### 🧪 Testing

* Added multi-page parity coverage that compares paginated results to unbounded execution, including byte-for-byte row parity checks.
* Added strict monotonic anchor-progression assertions across continuation pages.
* Added explicit unique-index continuation coverage for `IndexRange` (design Case F).

### 🧭 Migration Notes

* No public API migration required yet.
* Cursor format stability and binary cursor commitments are planned for a later milestone.

---

## [0.11.2] 👀 – 2026-02-17 - Rust Visibility Pass

### 📝 Summary

* This release completes a visibility pass across the DB internals based on `docs/VISIBILITY.md`. We tightened module boundaries, removed deep internal imports, and moved callers to clear subsystem root surfaces. This is better because internal refactors are safer, accidental API leakage is reduced, and privacy rules are now enforced by both module visibility and compile-fail tests.

### 🗑️ Removed

* Removed the hidden `__internal` module from `icydb`.

### 🧪 Testing

* Updated privacy tests for `db::data`, `db::index`, and `db::executor`.
* Updated sanitize/validate compile-fail tests to check private `visitor` internals.

---

## [0.11.1] 🧽 – 2026-02-17

### 🔧 Changed

* Added a dedicated hidden macro wiring surface (`__macro`) in `icydb` so generated code no longer has to depend on long `__internal::core::...` DB paths.
* Updated actor/store codegen to use `::icydb::__macro::{Db, DataStore, IndexStore, StoreRegistry, EntityRuntimeHooks, ...}` for cleaner internal codegen boundaries.

### 🧹 Cleanup

* Tightened several internal module boundaries from `pub` to `pub(crate)` in `db` and `db::query` where external visibility was not needed.
* Removed dead predicate/query helpers and unused wrappers, including `IndexIdError`, unused text-op variants (`Eq`, `Contains`), unused typed visitor wrappers, and unused diagnostics helper constructors.
* Simplified infallible merge and conversion helpers by removing unnecessary return values and dead branches.
* Removed additional unused plan helpers while keeping current access-path behavior unchanged.

## [0.11.0] ↔️ – 2026-02-16 - Range Pushdown

### 🔧 Changed

* Secondary index scans can now push down bounded range predicates (`>`, `>=`, `<`, `<=`) into index traversal instead of relying on full-scan filtering.
* Composite indexes now support prefix-equality plus one ranged component (for example `a = ?` with a range on `b`) with deterministic ordering through the primary-key tie-break.
* Range-compatible predicate pairs (`>=` + `<=`) now plan as one bounded `IndexRange`, which keeps pagination behavior aligned with fallback execution.
* Single-predicate one-sided ranges on single-field indexes (for example `tag > x` or `tag <= y`) now plan directly to `IndexRange` instead of falling back.

### 🧹 Cleanup

* Added explicit `AccessPath::IndexRange` guardrails for secondary-range planning prep, and wired explain/trace/hash/canonical/debug handling so new access variants cannot be silently skipped.

### 🧪 Testing

* Added planner tests for valid and invalid range extraction shapes, stricter-bound merging, and empty-range rejection.
* Added parity tests that compare range pushdown results against `by_ids` fallback for single-field and composite-prefix range queries.
* Added pagination boundary tests for range windows to verify no-duplicate resume behavior at lower and upper edges.
* Added edge-value tests around `0` and `u32::MAX` to verify inclusive and exclusive bound correctness.
* Added table-driven parity matrices across `>`, `>=`, `<`, `<=`, and `BETWEEN`-equivalent forms, including descending and no-match/all-match cases.
* Added composite duplicate-edge cursor boundary tests to verify strict resume behavior when lower/upper boundary groups contain multiple rows.

Example (shape only):

```rust
AccessPath::IndexRange {
    index,
    prefix: vec![Value::Uint(7)],
    lower: Bound::Included(Value::Uint(100)),
    upper: Bound::Excluded(Value::Uint(200)),
}
```

---

## [0.10.2] 🧼 – 2026-02-16

### 🧹 Cleanup

* `AccessPlan` projection now uses one shared path for explain, trace, metrics, and hashing. This reduces repeated edits when access types change.
* Plan-shape rules now live in `query::policy`, and other layers wrap those errors instead of re-implementing them. This keeps behavior aligned.
* Save and delete commit-window setup now uses one shared helper. This keeps single, batch, and delete flows consistent.
* Explain and trace now share one pushdown mapping. New pushdown outcomes only need to be added in one place.
* Value type tags now come from one canonical source across normalization, ordering, and fingerprinting. This removes duplicated tag tables.
* Index-prefix compatibility checks and cursor primary-key decoding now use shared helpers. This lowers planner/executor drift risk.
* Reduced temporary allocations in cursor filtering and predicate sort-key encoding for cleaner hot-path behavior.
* `apply_post_access_with_cursor` is now split into clear phase helpers for filter, order, cursor, pagination, and delete limits. This makes future changes safer and easier to review.
* Pushdown validation tests now use table-driven cases for core and applicability scenarios. This removes repeated setup and keeps matrix coverage easier to extend.
* Changelog section headers are now normalized to one fixed emoji mapping for standard section types. This keeps release notes consistent across versions.

## [0.10.1] 🧱 – 2026-02-16

* Macro codegen #[expect] changed to #[allow], oops.

## [0.10.0] 🗝️ – 2026-02-16 - Index Key Ordering

### 📝 Summary

* `0.10.0` begins IndexKey v2.  It was hashed before, now it's a canonical byte slice that can be ordered.
* Goal: keep index key bytes and key ordering stable across upgrades.
* Coming Next: all the cool stuff orderable indexes bring.

### 🔧 Changed

* Index keys now use a framed format with explicit lengths for each part.
* Index component encoding is now fully canonical and deterministic.
* User and system index keys are clearly separated by key kind.
* Startup recovery now rebuilds secondary indexes from saved rows, so stale index entries are corrected before normal reads and writes continue.
* Rebuild is fail-closed: if rebuild hits bad row bytes or hook wiring issues, recovery restores the previous index snapshot and returns a classified error.

Index key format (`v0.10`):

```text
[key_kind:u8][index_id:fixed][component_count:u8]
[component_len:u16be][component_bytes]...
[pk_len:u16be][pk_bytes]
```

### 🧪 Testing

* Added golden-byte tests that fail if key encoding changes.
* Added corruption tests for invalid lengths, truncation, and trailing bytes.
* Added ordering tests to ensure value order and byte order stay aligned.
* Added prefix-scan isolation tests for namespace and index boundaries.
* Added unique-index behavior tests for insert, update, and delete/reinsert flows.
* Added recovery tests to confirm index key bytes stay stable after replay.
* Added startup-rebuild tests that prove stale index entries are replaced by canonical entries rebuilt from row data.
* Added fail-closed rebuild tests that prove index state is rolled back if rebuild encounters corrupt rows.

### 🧹 Cleanup

* Replaced many `#[allow(...)]` attributes with `#[expect(...)]` where valid, and removed unfulfilled expects.

Example (simplified):

```rust
let key = IndexKey::new(&entity, index)?.expect("indexable");
let raw = key.to_raw();
let decoded = IndexKey::try_from_raw(&raw)?;
assert_eq!(decoded.to_raw().as_bytes(), raw.as_bytes());
```

---

## [0.9.0] 💪 – 2026-02-15 - Strengthening Release

### 📝 Summary

* `0.9.0` focuses on safer deletes, clearer batch-write behavior, and stronger query execution checks.
* Existing `0.8.x` user-facing behavior stays the same in key areas (cursor format, storage format, and default write semantics).

### 🔧 Changed

* Strong relation checks now block deletes that would leave broken references.
* Batch writes now have clear lanes: atomic (`*_many_atomic`) and non-atomic (`*_many_non_atomic`).
* Ordered pagination does less unnecessary work while keeping the same results.
* Planner and executor checks were tightened to catch invalid states earlier.

### 🩹 Fixed

* Recovery replay for interrupted writes is now more reliable and repeat-safe.
* Error categories are clearer (`Unsupported`, `Corruption`, `Internal`) across relation/index paths.
* Metrics and trace coverage improved for key read/write phases.
* Storage diagnostics now clearly separate user index data from system index data.

Example (simplified):

```rust
let saved = db.session().insert_many_atomic(users)?;
assert_eq!(saved.len(), users.len()); // all-or-nothing for this batch
```

---

## [0.8.5] 🔒 – 2026-02-15 - Transaction Semantics Hardening

### 📝 Summary

* This release tightens and clarifies the batch write behavior introduced in `0.8.4`.
* `_many_atomic` is confirmed as all-or-nothing for one entity type.
* `_many_non_atomic` remains fail-fast with partial commits allowed.

### 🧪 Testing

* Added more conflict tests for atomic and non-atomic update/replace batch flows.
* Added tests that confirm invalid strong relations fail atomic batches without partial writes.
* Added empty-batch tests for both lanes.
* Added recovery tests for unknown entity paths and miswired hooks.
* Added tests for reserved index namespaces and storage corruption counters.
* Added tests to confirm delete `limit` is applied in the correct execution phase.

### 🔧 Changed

* Updated docs with simpler guidance on choosing atomic vs non-atomic batch writes.
* Improved ordered pagination performance for common first-page queries.
* Added a faster path for primary-key ordered scans, including key-range scans.

Example (simplified):

```rust
let result = db.session().update_many_non_atomic(batch);
if result.is_err() {
    // By design, earlier rows in this batch may already be committed.
}
```

---

## [0.8.4] 📜 – 2026-02-15 - Explicit Transaction Semantics Milestone

### 📝 Summary

* Added opt-in atomic batch APIs: `insert_many_atomic`, `update_many_atomic`, and `replace_many_atomic`.
* These are atomic only within one entity type.
* They are not full multi-entity transactions.
* Existing non-atomic batch APIs were kept as-is.

### 🔧 Changed

* Added an explicit all-or-nothing batch lane for single-entity writes.
* Updated docs to clearly explain atomic vs non-atomic behavior.

Example (single entity type only):

```rust
let users = vec![
    User { id: user_a, email: "a@example.com".into() },
    User { id: user_b, email: "b@example.com".into() },
];

let saved = db.session().insert_many_atomic(users)?;
assert_eq!(saved.len(), 2);
```

---

## [0.8.3] 🔗 – 2026-02-15 - Strong RI Milestone

### 📝 Summary

* Completed the strong referential integrity milestone for the `0.9` plan.
* Deletes now better protect against broken strong references, and related replay/diagnostic paths are better covered by tests.

Example (simplified):

```rust
let err = db
    .session()
    .delete::<TargetEntity>()
    .by_id(target_id)
    .execute()
    .unwrap_err();
assert!(err.to_string().contains("strong relation"));
```

---

## [0.8.2] ♻️ – 2026-02-15 - Reverse Index Integrity

### 🔧 Changed

* Strong-relation delete checks now use reverse indexes instead of full source scans.
* Reverse-index updates now follow the same commit/recovery path as row updates.
* Metrics now report reverse-index and relation-validation deltas more clearly.
* Storage snapshots now separate user index entries from system index entries.

### 🧹 Cleanup

* Simplified runtime dispatch by moving to one shared hook registry per entity.

### ⚠️ Breaking

* User index names in the reserved `~` namespace are now rejected at derive time.

Example (simplified):

```rust
// This now fails during schema derive/validation:
#[index(name = "~custom", fields = ["email"])]
```

---

## [0.8.1] 🧭 – 2026-02-13 - Cursor Boundary Hardening

### 🧪 Testing

* Added stronger tests for invalid cursor tokens (empty, bad hex, odd length).
* Added live-state pagination tests for insert/delete changes between page requests.
* Added more cursor codec roundtrip and edge-case tests.

### 🩹 Fixed

* Schema validation now catches data/index memory ID collisions earlier.

### 🧹 Cleanup

* Broke index code into smaller modules and kept tests close to those modules.
* Simplified index fingerprint storage to one inline value next to each index entry.
* Removed no-longer-needed fingerprint memory config from schema metadata.

### 🔧 Changed

* Store access now goes through one shared registry handle.
* Index metrics now emit one delta event per commit apply.
* Added replay tests for mixed save/save/delete flows on shared index keys.

### ⚠️ Breaking

* Duplicate store path registration is now rejected instead of silently replaced.
* Store schema/runtime now uses a single combined store model instead of split data/index registries.
* Commit markers no longer store `kind`; mutation shape is derived from `before` and `after`.

Example (simplified):

```rust
let err = Query::<User>::new(ReadConsistency::MissingOk)
    .page()
    .cursor("not-hex")
    .limit(20)
    .plan()
    .unwrap_err();
```

---

## [0.8.0] 🏛️ – 2026-02-13 - Structural Correctness Baseline

### 📝 Summary

* `0.8.0` focuses on making core query and pagination behavior predictable.
* Goal: same input should reliably produce the same output.
* Strong delete-side relation checks were planned for later `0.8.x` updates.

### 🔧 Changed

* Pagination rules are now clearer and consistently enforced.
* Collection behavior is now clearly documented for `List`, `Set`, and `Map`.
* Added `icydb-primitives` to centralize scalar metadata.
* Updated docs and roadmap language to reduce ambiguity.

### ⚠️ Breaking

* Generated view/create/update payload types now live in entity-local modules.
* Call sites should use prelude aliases or explicit entity module paths.

### 🩹 Fixed

* Added wider regression coverage for cursor paging and uniqueness behavior.
* Improved planner/query error and lint hygiene without changing user-facing query behavior.

### 🧹 Cleanup

* Reduced duplicate internal logic in planning and mutation paths.
* Centralized canonical value ordering/tagging behavior in shared modules.
* Split `Unit` coercion behavior from `Bool` to make type handling clearer.

Example (simplified):

```rust
let page1 = query.order_by("created_at").limit(20).execute()?;
let page2 = query.cursor(page1.next_cursor.unwrap()).execute()?;
```

---

## [0.7.21] 🧭 – 2026-02-11 - Cursor Pagination, Part I

### 🔧 Changed

* Cursor pagination now follows one clear execution order for filtering, ordering, cursor skip, and limits.
* Cursor payloads are now encoded and validated earlier in planning.
* Added typed pagination with `.page()`, which requires explicit order and limit.
* Documented expected pagination consistency when data changes between requests.

### 🩹 Fixed

* Schema validation cache now stays isolated per entity type.
* Singleton unit-key save/load behavior was tightened and covered with tests.
* `next_cursor` is now based on the last row returned, reducing cursor drift.
* Added stronger validation for malformed or mismatched cursor tokens.

### 🧹 Cleanup

* Removed unused query error layers and unused error variants.
* Removed dead missing-key patch error branches after no-op missing-key behavior.
* Reduced `QueryError::Plan` size while preserving diagnostics.

Example (simplified):

```rust
let page = Query::<User>::new(ReadConsistency::MissingOk)
    .page()
    .order_by("created_at")
    .limit(20)
    .execute()?;
```

## [0.7.20] 🌤️ – 2026-02-11 - Calm After the Storm

### 🔧 Changed

* Read paths now quickly check and replay pending commit markers before loading data.
* Write recovery now uses the same recovery path as reads for consistency.
* Saves now enforce that the declared primary key matches the entity identity.
* Facade query errors are grouped more clearly, including a dedicated unordered pagination error.
* Facade query exports were narrowed to safer boundary types.
* Map patch behavior now matches list/set behavior: missing-key remove/replace is a no-op.
* Removed disabled internal map-predicate branches.

### 🩹 Fixed

* Derive validation now rejects unsupported map value shapes earlier.
* Map value conversion avoids panic on invalid entries and reports issues safely.
* Fixed recursive map type inference issues in nested map-like value trees.
* Row decode errors now keep underlying deserialize details for easier debugging.
* Added more regression tests for map validation and incomplete marker replay.

Example (simplified):

```rust
let update = UserUpdate::default()
    .with_settings(MapPatch::remove("missing_key"));
db.session().patch_by_id(user_id, update)?; // remove on missing key is a no-op
```

## [0.7.19] 🛠️ – 2026-02-10

### 🔧 Changed

* `icydb-schema-derive` now treats field visibility as an entity/record responsibility: base `Field`/`FieldList` emission no longer hardcodes `pub(crate)`, entity fields are emitted as `pub(crate)`, and record fields are emitted as `pub`.

## [0.7.18] 🧪 – 2026-02-10

### ➕ Added

* Icrc1::TokenAmount and Icrc1::Tokens provide a .units() -> u64 call

### 🔧 Changed

* `Timestamp` now supports signed and unsigned scalar arithmetic (`u64`/`i64`) via `+`, `-`, `+=`, and `-=`, using saturating behavior for underflow/overflow and negative deltas.
* `Duration` now supports the same signed and unsigned scalar arithmetic ergonomics (`u64`/`i64`) with saturating semantics.
* `Timestamp` arithmetic with `Duration` is now directly supported (`Timestamp +/- Duration` and assign variants), applying duration values in whole seconds.
* `Timestamp` and `Duration` now support direct scalar comparisons against `u64` and `i64` (`<`, `<=`, `>`, `>=`, `==`) in both directions.
* Scalar-left subtraction is now supported for both time types (`u64/i64 - Timestamp` and `u64/i64 - Duration`) so raw numeric timestamps and durations can be subtracted from wrapped values without manual conversion.

## [0.7.15] 🧱 – 2026-02-09

### ➕ Added

* Any Id<E> can now be turned into a ledger subaccount with `.subaccount()`
* Added facade-level `UpdateView::merge` error promotion so patch failures are surfaced as `icydb::Error` with `ErrorKind::Update(UpdateErrorKind::Patch(...))`.

### 🔧 Changed

* Generated relation `*_ids()` accessors for `many` cardinality now return `impl Iterator<Item = Id<Relation>> + '_` instead of allocating a `Vec<Id<Relation>>`, while preserving key-to-`Id` projection behavior.

### ⚠️ Breaking

* `icydb::patch` no longer exports `MergePatch` or `MergePatchError`; callers should use `UpdateView::merge` and handle facade `icydb::Error`.

---

## [0.7.12] 🧬 – 2026-02-09

### ➕ Added

* Added `UpdateView` trait generation for schema-derived list/set/map/newtype/record/tuple/enum/entity types so patch payload typing is explicit at the view boundary.
* Added `UpdateView` coverage for core container wrappers (`OrderedList`, `IdSet`) and structural containers (`Option`, `Vec`, `HashMap`, `HashSet`, `BTreeMap`, `BTreeSet`) using `ListPatch`/`SetPatch`/`MapPatch` payload shapes.

### 🔧 Changed

* Schema derives now route patch generation through `MergePatch` end-to-end (trait wiring, node dispatch, and emitted merge calls), while preserving existing `*Update` payload type names and patch shapes.
* Merge payload typing now resolves through `<T as UpdateView>::UpdateViewType`; `MergePatch` implementations no longer define or consume a separate `Patch` associated type.
* Atomic merge semantics now consistently use `traits::Atomic` in type modules, and the blanket `MergePatch` path applies full-replacement updates from `UpdateViewType = Self`.

---

## [0.7.10] 🚦 – 2026-02-09 - Facade Error Kinds

### ➕ Added

* Added structured facade error categories in `icydb::error` via `ErrorKind`, `QueryErrorKind`, `UpdateErrorKind`, `PatchError`, and `StoreErrorKind` so callers can branch on stable semantic error kinds instead of parsing messages.
* Added explicit patch error lowering from `ViewPatchError` into facade `PatchError` variants, keeping patch failure handling user-facing and predictable.
* Added `DbSession::patch_by_id` in the facade to execute load-merge-save in one boundary-owned operation, mapping merge failures into `ErrorKind::Update(UpdateErrorKind::Patch)` without exposing core patch errors to callers.
* Added a dedicated `types::identity::GenerateKey` module trait so key generation capability is explicitly modeled at the identity layer.

### 🔧 Changed

* Query error mapping in the facade now classifies validation/planning/intent failures as `Query(Invalid)`, unsupported features as `Query(Unsupported)`, and response cardinality failures as `Query(NotFound|NotUnique)`.
* Internal execution failures continue to cross the facade as `ErrorKind::Internal` with preserved origin and message context.

### ⚠️ Breaking

* `icydb::Error` now exposes `kind` instead of the previous class-style taxonomy field, and the old facade `ErrorClass` surface is replaced by the new structured `ErrorKind` family.

---

## [0.7.9] 🆔 – 2026-02-09 - Relation ID Accessors

### ➕ Added

* Added generated relation ID accessors on entity and record inherent impls for relation-backed fields, including `*_id()` for single/optional relations and `*_ids()` for many relations.
* Accessors now return typed IDs (`Id<Relation>`) derived from stored primitive relation keys, so relation fields can remain `pub(crate)` without losing ergonomic read access.

### 🔧 Changed

* Split inherent code generation into smaller focused modules (`entity`, `record`, `collection`, and relation accessor generation) to reduce coupling and make future schema macro changes easier to review.
* Split view/mutation traits into dedicated modules so behavior contracts are clearer: `AsView` stays in `traits::view`, `CreateView` moved to `traits::create`, and `UpdateView` + `ViewPatchError` now live in `traits::update`.

### ⚠️ Breaking

* `UpdateView::merge` now returns `ViewPatchError` directly instead of `InternalError`, and patch classification (`NotFound`/`Unsupported`) is now applied at the error boundary via explicit conversion.
* Removed `view` type aliases (`View<T>`, `Create<T>`, `Update<T>`); call sites now use associated types (`<T as AsView>::ViewType`, `<T as CreateView>::CreateViewType`, `<T as UpdateView>::UpdateViewType`).

---

## [0.7.7] 🧯 – 2026-02-08 - Error Boundary and ID Naming

### 🔧 Changed

* `UpdateView::merge` now returns `Error` instead of `ViewPatchError`, with patch failures bubbled through `InternalError` via `ErrorDetail::ViewPatch` while preserving contextual path/leaf diagnostics.
* Standardized a broad set of accessor methods from `key()` to `id()` to align naming with typed identity usage across the public API.

---

## [0.7.4] 🧰 – 2026-02-08

### ➕ Added

* Added contextual merge patch errors via `ViewPatchError::Context`, including `path()` and `leaf()` helpers, so callers can locate and classify update failures without depending on internal patch details.
* Added explicit executor-level phase-order tests covering optional-field equality, `IN`/`CONTAINS`, and text predicates.
* Added a structural post-access guard test plus `TracePhase::PostAccess` diagnostics so regressions in filter/order/pagination execution are detected at the executor boundary.

### 🔧 Changed

* Restored post-access query execution in load/delete paths so predicate evaluation, ordering, pagination, and delete limits are applied deterministically from the logical plan.

---

## [0.7.3] 🧼 – 2026-02-08
* Added `EntityValue` back to the public prelude re-exports for easier trait access in downstream code.

---

## [0.7.2] 🔐 – 2026-02-08 - Key Byte Contracts

### 📝 Summary

0.7.1 standardizes primary-key byte encoding through `EntityKeyBytes` and simplifies external identity projection to hash canonical key bytes directly.
This release also removes namespace-based projection metadata and makes key-byte encoding an explicit compile-time contract.

### ➕ Added

* Added `EntityKeyBytes` with explicit `BYTE_LEN` and `write_bytes` requirements for primary-key encoding.
* Added `Id<E>::KEY_BYTES` and `Id<E>::into_key()` for explicit key-size introspection and key extraction.

### 🔧 Changed

* `EntityKey::Key` now requires `EntityKeyBytes`, so key-encoding compatibility is checked at compile time.
* `Id<E>::project()` is now a direct projection path over canonical key bytes using the projection domain tag.
* Relaxed the docs so Codex stops faffing about the ID being a secret in a capability-first system

### 🗑️ Removed

* Removed `Subaccount::from_ulid` in favor of explicit subaccount byte construction paths.

---

## [0.7.0] ❄️ – 2026-02-08 - Contract Freeze

### 📝 Summary

0.7.0 freezes the core engine contracts for identity, query behavior, atomicity, and referential integrity.

Identity is now explicitly typed (`Id<E>`), query intent/planning boundaries are formally locked, commit-marker discipline is specified as the atomicity source of truth, and RI remains explicit strong-only save-time validation with weak-by-default relations.

This release is the 0.7 baseline for deterministic behavior, compile-time schema rejection of illegal identity shapes, and bounded write-path enforcement without cascades.

### ➕ Added

* Added `strong`/`weak` relation flags in the schema DSL, with `weak` as the default.
* Added a `Display` derive in `icydb-derive` for tuple newtypes.
* Added collection types `OrderedList` and `IdSet` for explicit many-field semantics.
* Added `OrderedList::retain` plus `apply_patches` helpers on `OrderedList` and `IdSet` for explicit patch application.
* Added `docs/collections.md` as the contract reference for collection and patch semantics.
* Added `docs/IDENTITY_CONTRACT.md` as the normative identity and primary-key contract for `Id<E>`, explicit construction, and declared-type authority.
* Added `docs/QUERY_CONTRACT.md` as the intent/planning/execution boundary contract for query determinism and explicit missing-row policy.
* Added `docs/ATOMICITY.md` as the normative single-message commit and recovery contract for write safety.
* Added `docs/REF_INTEGRITY.md` as the normative RI contract for strong/weak relation behavior and bounded save-time validation.
* Added `saturating_add`/`saturating_sub` helpers to arithmetic newtypes for explicit saturating math.
* Added `Id<E>` as a typed primary-key value that preserves entity-kind correctness.
* Added parity coverage to keep keyability conversion paths aligned across `ScalarType::is_keyable`, `Value::as_storage_key`, and `StorageKey::try_from_value`.

### 🔧 Changed

* Save operations now enforce referential integrity for `RelationStrength::Strong` fields and fail if targets are missing.
* Write executors now perform a fast commit-marker check and replay recovery before mutations when needed; read recovery remains startup-only.
* Entity macros now allow primary keys to be relations for identity-borrowing singleton entities.
* Primary-key derivation now follows only the declared primary-key field type; relation metadata does not infer PK storage shape.
* Illegal or ambiguous identity/primary-key schema shapes are now treated as compile-time derive failures instead of runtime checks.
* ORDER BY and model key-range validation now use a shared canonical value comparator instead of `Value::partial_cmp`, keeping query ordering behavior consistent for all orderable key types.
* Documented that `Value::partial_cmp` is not the canonical database ordering path and should not be used for ORDER BY or key-range semantics.

### ⚠️ Breaking

* Entity and record fields with `many` cardinality now emit `OrderedList<T>` instead of `Vec<T>`.
* Relation fields with `many` cardinality now emit `IdSet<T>` instead of list types like `Vec<Id<T>>`.
* Entity primary-key fields now emit `Id<E>` instead of raw key values, and `EntityValue::set_id` wraps raw keys into `Id<E>` so call sites must pass the raw key type.
* Storage key admission is now registry-driven via `is_storage_key_encodable`; the encodable scalar set is unchanged, but the contract is now explicit and auditable.

---

## [0.6.20] 🛠️ – 2026-02-04

### ➕ Added

* Added `Blob::as_bytes()` and `Blob::as_mut_bytes()` for explicit byte access without deref.

### 🔧 Changed

* Relation/external field suffix bans now apply only to relation and external fields (not arbitrary primitives like `my_api_id`).

### 🩹 Fixed

* Made `Id<T>` `Sync + Send` to fix the `*const` variant.

---

## [0.6.17] 🌿 – 2026-02-03 - Query Ergonomics

### ➕ Added

* Added `WriteResponse`/`WriteBatchResponse` helpers for write results, including key and view accessors.
* Added `Nat::to_i64`/`to_u64` and `Int::to_i64`/`to_u64` for explicit integer conversion without deref.
* Added `by_ref()` for query flow (later removed and replaced by `by_id()`/`by_ids()`).
* Added `many_refs()` for query flow (later removed and replaced by `by_ids()`).

### 🔧 Changed

* id_strict and key_strict to require_id and require_key to match other methods
* Clarified schema error messaging for banned suffixes on field names

### ⚠️ Breaking

* Schema field names ending in `_id`, `_ids`, `_ref`, `_refs`, `_key`, or `_keys` now fail at compile time; relation fields were renamed to base nouns.
* Singleton query `only()` no longer accepts an explicit ID and always uses the default singleton key.

---

## [0.6.11] 🧮 – 2026-02-03 - Decimals, Collections and Stuff

### ➕ Added

* Added a `get()` accessor to map collection inherent traits for explicit lookup without deref.
* Added `Decimal::abs()` to expose absolute value math without deref.
* Added `Blob::to_vec()` for explicit byte cloning without deref.

### 🔧 Changed

* Planner access planning no longer re-validates predicates; validation is now owned by the intent/executor boundaries.
* Consolidated primary-key compatibility checks to the shared `FieldType::is_keyable` rule to avoid drift across planner/validator layers.
* Renamed primary_key() and similar methods in Response to key() for consistency

### ⚠️ Breaking

* `MapCollection::iter` now returns a GAT-backed iterator instead of a boxed trait object, so implementations and type annotations must update.
* `Collection::iter` now returns a GAT-backed iterator instead of a boxed trait object, so implementations and type annotations must update.
* `DbSession::insert`/`replace`/`update` now return `WriteResponse<E>` (and batch variants return `Vec<WriteResponse<E>>`).

---

## [0.6.6] ✅ – 2026-02-03 - Diagnostic Test Reenablement

### 📝 Summary

* Re-enabled query plan explain, fingerprint, and validation tests to guard planner determinism and invariants after the refactor.

### ➕ Added

* Added `ByKeys` determinism checks for `ExplainPlan` and `PlanFingerprint` to lock in set semantics for key batches.
* Added a typed-vs-model planning equivalence test to anchor `QueryModel`/`Query<E>` parity post-refactor.

---

## [0.6.5] 📦 – 2026-02-03 - Derive Consolidation & Explicit Collections

### 📝 Summary

* Introduced `QueryModel` to separate model-level intent, validation, and planning from typed `Query<E>` wrappers, reducing trait coupling in query logic.
* Added the `icydb-derive` proc-macro crate for arithmetic and ordering derives on schema-generated types.
* Relocated canister-centric tests to PocketIC-backed flows and removed canister builds from default `make test` runs.

### ➕ Added

* Added the `icydb-derive` proc-macro crate with `Add`, `AddAssign`, `Sub`, `SubAssign`, `Mul`, `MulAssign`, `Div`, `DivAssign`, and `Sum` derives for tuple newtypes.
* Added a `Rem` derive for tuple newtypes and re-exported the `Rem` trait from `traits`.
* Added a `PartialOrd` derive in `icydb-derive` and routed schema-generated types to it.
* Added `Decimal` helpers `is_sign_negative`, `scale`, and `mantissa` for explicit access without deref.
* Added `MulAssign` and `DivAssign` impls for `Decimal` to match arithmetic derives.
* Added `Blob::as_slice` for explicit byte access in validators.
* Added `Mul`/`Div` and assignment ops for `E8s` and `E18s` to satisfy fixed-point newtype arithmetic derives.
* Added `Mul`/`Div` and assignment ops for `Nat` and `Nat128` to support arithmetic newtype derives.
* Added `Mul`/`Div` and assignment ops for `Int` and `Int128` to support arithmetic newtype derives.
* Added `Collection` and wired list/set wrapper types to explicit iteration and length access without deref.
* Added `MapCollection` for explicit, read-only iteration over map wrapper types without deref.
* Added explicit mutation APIs on list/set/map wrapper types (`push`, `insert`, `remove`, `clear`) without implicit container access.
* Moved `PartialEq` derives to `icydb-derive` for schema-generated types.

### 🔧 Changed

* Newtype arithmetic derives now route through `icydb-derive` (including `Div`/`DivAssign`) instead of `derive_more`.
* `test_entity!` now requires an explicit `struct` block and derives `EntityKind::Id` from the primary key field’s Rust type, failing at compile time if the PK is missing from the struct or `fields {}`.
* `FieldProjection` is now derived via `icydb-derive` and no longer implemented by schema-specific `imp` code.
* `DbSession::diagnose_query` now requires `EntityKind` only, keeping diagnostics schema-level.
* Public query builders now accept `EntityKind` for intent construction; execution continues to require `EntityValue`.
* Updated `canic` to `0.9.17`.
* `make test` no longer runs canister builds; `test-canisters` is now a no-op.

### 🗑️ Removed

* Removed schema-derive `imp` implementations for `Add`/`AddAssign`/`Sub`/`SubAssign` in favor of derives.
* Removed `Display` trait from schema-derive

### 🩹 Fixed

* Exported `Div`/`DivAssign` through `traits` so generated arithmetic derives resolve cleanly.
* Session write APIs and query execution now require `EntityValue`, aligning runtime execution with value-level access.
* `#[newtype]` now derives `Rem` only for primitives that support remainder, and `Int128`/`Nat128` implement `Rem` to match numeric newtype expectations.

---

## [0.6.4] 🗝️ – 2026-02-01 - Explicit Key Boundaries

### 🔧 Changed

* Removed `Into<...>` from `by_key` functions to keep primary key boundaries explicit (`by_key` was later replaced by `by_id`/`by_ids`).

---

## [0.6.3] 🧷 – 2026-02-01 - Primary Key Guardrails

### 🩹 Fixed

* Entity macros now reject relation fields as primary keys, preventing relation identities from being used as primary key types.
* Primary key fields must have cardinality `One`; optional or many primary keys now fail at macro expansion time.
* Local schema invariants now fail fast during macro expansion, including field identifier rules, enum variant ordering, and redundant index prefix checks.
* Added compile-fail tests covering relation and non-One primary key shapes in the entity macro.

### 📝 Summary

* Locked primary key invariants at macro expansion time to avoid downstream RI violations.

---

## [0.6.1] 🔗 – 2026-02-01 - Referential Integrity, Part II

### ➕ Added

* **Save-time referential integrity (RI v2)**: direct `Id<T>` and `Option<Id<T>>` relation fields are now validated pre-commit; saves fail if the referenced target row is missing.
* Added `docs/REF_INTEGRITY_v2.md`, defining the v2 RI contract, including:

  * strong vs weak reference shapes,
  * atomicity boundaries,
  * and explicit non-recursive enforcement rules.
* Added targeted RI tests covering:

  * strong reference failure on missing targets,
  * allowance of weak reference shapes,
  * and non-enforcement of references during delete operations.

### 🔧 Changed

* Nested and collection reference shapes (`Id<T>` inside records/enums, and `Vec`/`Set`/`Map<Id<T>>`) are now **explicitly treated as weak** at runtime and no longer trigger invariant violations during save.
* Clarified that schema-level relation validation is **advisory only** and does not imply runtime RI enforcement.
* Aligned runtime behavior, schema comments, and documentation with the RI v2 contract.

### 📝 Summary

* Introduced **minimal, explicit save-time referential integrity** for direct references only, while formally defining and locking the weak-reference contract for all other shapes.

---

## [0.6.0] 🧱 – 2026-01-31 - Referential Integrity, Part I

### ⚠️ Breaking
* Index storage now splits data and index stores explicitly; index stores require separate entry and fingerprint memories.
* `IndexStore::init` now requires both entry and fingerprint memories; constructing an index store without fingerprint memory is no longer possible.

### ➕ Added
* Added dedicated index fingerprint storage to keep verification data independent from index routing entries.
* Added a cross-canister relation validation test with a dedicated relation canister to lock in the new schema invariant.

### 🩹 Fixed
* ORDER BY now preserves input order deterministically for incomparable values.
* Commit marker apply now rejects malformed index ops or unexpected delete payloads in release builds.
* Commit marker decoding now rejects unknown fields instead of silently ignoring them.
* Commit marker decoding now honors the marker size limit instead of the default row size cap.
* Oversized commit markers now surface invariant violations instead of corruption.

### 🔧 Changed
* Documented that `FieldRef` and `FilterExpr` use different coercion defaults for ordering; see `docs/QUERY_BUILDER.md`.
* Consolidated build-time schema validation behind `validate::validate_schema` so all passes run through a single entrypoint.

### 📝 Summary
* Logged the 0.6 atomicity audit results, including the read-path recovery mismatch, for follow-up.

---

## [0.5.25] 🧰 – 2026-01-30

### ⚠️ Breaking
* Case-insensitive coercions are now rejected for non-text fields, including identifiers and numeric types.
* Text substring matching must use `TextContains`/`TextContainsCi`; `CompareOp::Contains` on text fields is invalid.
* ORDER BY now rejects unsupported or non-orderable fields instead of silently preserving input order.

### 🔧 Changed
* Executor ordering tests now sort only on orderable fields while preserving tie stability and secondary ordering guarantees.
* Conducted a DRY / legacy sweep across query session, executor, and plan layers to remove duplicated or misleading APIs.

---

## [0.5.24] 🧪 – 2026-01-30

### 🩹 Fixed
- replaced FilterExpr helpers that were accidentally removed

---

## [0.5.23] 🧼 – 2026-01-30

### 🩹 Fixed

* Insert now decodes existing rows and surfaces row-key mismatches as **corruption** instead of conflicts.
* `SaveExecutor` update/replace detects row-key mismatches as corruption, preventing index updates from amplifying bad rows.
* Unique index validation now treats stored entities missing indexed fields as **corruption**.
* Executors validate logical plan invariants at execution time to protect erased plans:

  * delete limits require ordering
  * delete plans cannot carry pagination
* Recovery validates commit marker kind semantics:

  * delete markers with payloads are rejected
  * save markers missing payloads are rejected
* Load execution performs recovery before reads when a commit marker exists, eliminating read-after-crash exposure to partial state.
* `NotIn` comparisons now return `false` for invalid inputs, matching the “unsupported comparisons are false” contract.
* **ORDER BY now permits opaque primary-key fields; incomparable values sort stably and preserve input order.**

### 🔧 Changed

* Recovery-guarded read access is now enforced via `Db::recovered_context`; raw store accessors are crate-private.
* `storage_report` now enforces recovery before collecting snapshots.
* `FilterExpr` now represents null / missing / empty checks explicitly, matching core predicate semantics.
* Dynamic filters now expose case-insensitive comparisons and text operators without embedding coercion flags in values.
* Map and membership predicates (`not_in`, map-contains variants) are now available via `FilterExpr`.

### 🗑️ Removed

* Dropped the unused projection surface (`ProjectionSpec` and related plan/query fields) to avoid false affordances.

### ⚠️ Breaking

* `obs::snapshot::storage_report` now returns `Result<StorageReport, InternalError>` instead of `StorageReport`.

---


## [0.5.22] 🪄 - 2026-01-29

### 🩹 Fixed
* Unique index validation now treats index/data key mismatches as corruption, preventing hash-collision or conflict misclassification.
* Delete limits now treat empty sort expressions as missing ordering, avoiding nondeterministic delete ordering.

### 🔧 Changed
* Empty `many([])` / `ByKeys([])` is now a defined no-op that returns an empty result set.

### 🗑️ Removed
* Removed legacy index mutation helpers (`IndexStore::insert_index_entry`, `IndexStore::remove_index_entry`) and the unused `load_existing_index_entry` helper.

---

## [0.5.21] 🧭 - 2026-01-29

### ➕ Added
* Added enum filter helpers (`EnumValue`, `Value::from_enum`, `Value::enum_strict`) and `FieldRef::eq_none` to make enum/null predicates ergonomic without changing planners or wire formats.
* Added ergonomic helpers to FilterExpr, ie. `FilterExpr::eq()`

---

## [0.5.15] 🧱 - 2026-01-29

### 🩹 Fixed
* `only()` now works for singleton entities whose primary key is `()` or `types::Unit`, keeping unit keys explicit without leaking internal representations.

### ➕ Added
* Session load/delete queries now expose `Response` terminal helpers directly (for example `row`, `keys`, `primary_keys`, and `require_one`), so applications can avoid handling `Response` explicitly.

### 🔧 Changed
* Load query offsets now use `u32` across intent, planning, and session APIs.
* Also count is u32

---

## [0.5.13] 🧬 - 2026-01-29

### ➕ Added
* Added dynamic query expressions (`FilterExpr`, `SortExpr`) that lower into validated predicates and order specs at the intent boundary.
* Session load/delete queries now expose `filter_expr` and `sort_expr` to attach dynamic filters and sorting safely.
* Re-exported expression types in the public query module for API endpoints that accept user-supplied filters or ordering.
* Facade versions of FilterExpr and SortExpr

---

## [0.5.11] 📦 - 2026-01-29

### 🔧 Changed
* View-to-entity conversions are now infallible; view values are treated as canonical state.
* Create/view-derived entity conversions now use `From` instead of `TryFrom`.
* Float view inputs now normalize `NaN`, infinities, and `-0.0` to `0.0` during conversion.
* Removed `ViewError` plumbing from view conversion and update merge paths.

### ⚠️ Breaking
* `View::from_view` and `UpdateView::merge` no longer return `Result`, and conversion errors are no longer surfaced at the view boundary.

---

## [0.5.10] 🔌 - 2026-01-29

### ➕ Added
* Restored key-only query helpers: `only()` for singleton entities and `many()` for primary-key batch access.
* Added `text_contains` and `text_contains_ci` predicates for explicit substring searches on text fields.
* Session query execution now returns the facade `Response`, keeping core response types out of the public API.

### 🩹 Fixed
* Cardinality errors now surface as `NotFound`/`Conflict` instead of internal failures when interpreting query responses.

---

## [0.5.7] 🗂️ - 2026-01-28

### ➕ Added
* Generated entity field constants now use `FieldRef`, enabling predicate helpers like `Asset::ID.in_list(&ids)` without changing planner or executor behavior.
* Load and delete queries now support `many` for primary-key batch lookups, using key-based access instead of predicate scans.
* Singleton entities with unit primary keys can use `only()` on load/delete queries for key-only access.

### 🩹 Fixed
* The `icydb` load facade now exposes `count()` and `exists()` terminals.
* Delete queries now treat zero affected rows as a valid, idempotent outcome in the session facade.

---

## [0.5.6] 🧾 - 2026-01-28

### ➕ Added
* Load queries now expose view terminals (`views`, `view`, `view_opt`) so callers can materialize read-only views directly.
* `Response` now provides view helpers (`views`, `view`, `view_opt`) to keep view materialization explicit at the terminal.
* Predicates now support `&` composition for building conjunctions inline.

### 🔧 Changed
* `key()` on load and delete session queries now accepts any type convertible into `Key`.

---

## [0.5.4] 🧹 - 2026-01-28

### ➕ Added
* `key()` is now available on both session query types for consistent access to key-based lookups.

---

## [0.5.2] 🚪 - 2026-01-28 - Public Facade Boundary

### 🩹 Fixed
* Public query methods now return `icydb::Error`, so low-level internal errors no longer leak into app code.
* You can no longer call executors or internal query execution paths from the public `icydb` API.
* Removed `core_db()` and similar test-only backdoors that skipped the public API entirely.
* Removed cross-canister query plumbing and erased-plan interfaces that exposed internal execution details.

### 🔧 Changed
* `db!()` now always returns the public `icydb` session wrapper, not the internal core session.
* Queries must be executed through the session’s load/delete helpers; executors are now core-only.
* Low-level executor corruption tests were removed from the public test suite.

### 🗑️ Removed
* Entity-based query dispatch (`EntityDispatch`, `dispatch_load/save/delete`) and canister-to-canister query handling.
* “Save query” abstractions — writes are now only done via explicit insert/replace/update APIs.
* Tests that depended on calling executors directly outside of `icydb-core`.
* Dropped `upsert` support and the related code paths (~800 lines).

---

## [0.5.1] 🧭 - 2026-01-28 - Redesigned Query Builder

### 🩹 Fixed
* Executors now reject mismatched plan modes (load vs delete) with a typed `Unsupported` error instead of trapping.

### 🔧 Changed
* Query diagnostics now surface composite access shapes in trace access (union/intersection).
* Executor trace events include per-phase row counts (access, filter, order, page/delete limit).
* Fluent queries now start with explicit `DbSession::load`/`DbSession::delete` entry points (no implicit mode switching).
* Pagination and delete limits are expressed via `offset()`/`limit()` on mode-specific intents.

---

## [0.5.0] 🏛️ – 2026-01-24 – Query Engine v2 (Stabilization Release)

This release completes the **Query Engine v2 stabilization** effort. It introduces a typed, intent-driven query facade, seals executor boundaries, and formalizes correctness, atomicity, and testing contracts.

The focus is **correctness, determinism, and architectural hardening**, not new end-user features.

---

### ➕ Added

**Query Facade**
* Typed query intent (`Query<E>`), making it impossible to plan or execute a query against the wrong entity.
* Executable plan boundary: `ExecutablePlan<E>` is the sole executor input; executor-invalid plans are mechanically unrepresentable.
* Formal query facade contract defining responsibilities of intent construction, planning, and execution.

**Query Semantics**
* Intent-level pagination via `Page` and `Query::page(limit, offset)`.
* Explicit delete intent with `QueryMode::Delete` and `Query::delete_limit(max_rows)`.
* Explicit read consistency (`MissingOk` vs `Strict`) required for all queries.

**Testing & Guarantees**
* Compile-fail (trybuild) tests for facade invariants, preventing construction or execution of internal plan types by user code.
* Query facade testing guide for invariant-driven strategy and when to use compile-fail vs runtime tests.
* Write-unit rollback discipline enforcing “no fallible work after commit window” across mutation paths.

---

### 🩹 Fixed

**Planner / Executor Correctness**
* Missing-row behavior no longer varies based on index vs scan access paths.
* Planners no longer emit plans that executors cannot legally execute.
* Removed duplicated predicate and schema validation between builder, planner, and executor layers.
* Queries can no longer be planned against arbitrary schemas or entities.
* Replaced release assert!-based planner invariant checks with non-panicking error paths to avoid production traps.

**Storage & Indexing**
* Fixed full-scan lower-bound ordering for non-integer primary keys (e.g., Account PK), preventing empty result sets on scans and set operations.
* Eliminated executor panic on empty principals by aligning Key::Principal encoding with IC principal semantics (anonymous/empty principal).
* Index store now surfaces corruption when index entries diverge from entity keys, rather than silently reporting removal.
* Increased commit marker size cap to avoid rejecting valid commits with large index entries.

**Identity & Documentation**
* Removed panicking public `Id<T>` constructors in favor of fallible APIs; unchecked constructors are crate-private for generated models.
* Updated README and internal docs to reflect the actual query execution and atomicity model.

---

### 🔧 Changed

**API & Planning**
* Query API redesign: replaced untyped `QuerySpec` / v1-style DSL with a typed, intent-only `Query<E>` → `ExecutablePlan<E>` flow.
* Pagination is now an intent-level concern; response-level pagination helpers are removed to avoid ambiguity and post-hoc slicing.
* Executors now accept only `ExecutablePlan<E>` and no longer perform planner-style validation.
* `LogicalPlan` is sealed/internal and cannot be constructed or executed outside the planner.
* Planning is deterministic, entity-bound, and side-effect free; repeated planning of the same intent yields equivalent plans.

**Errors, Docs, Tooling**
* Clarified and enforced separation between `Unsupported`, `Corruption`, and `Internal` error classes.
* Improved index store error typing and auditing by preserving error class/origin for index resolution failures.
* Documented unique index NULL/Unsupported semantics: non-indexable values skip indexing and do not participate in uniqueness.
* Removed legacy integration docs and consolidated guidance into README and contract-level documents.
* Updated minimum supported Rust version to **1.94.0** (edition 2024).

---

### 🗑️ Removed

* v1 query DSL and legacy builder APIs.
* Public execution or construction of logical plans.
* Implicit read semantics.
* Executor-side validation and planning logic.
* Schema-parameterized planning APIs.
* Response-level pagination helpers (`Page`, `into_page`, `has_more`).
* Internal plan re-exports from the public facade.
* Plan cache, removed as a premature optimization; planning is deterministic and cheap.

---

### 🧭 Migration Notes

This release contains **intentional breaking changes**:

* All queries must be rewritten using `Query<E>` and explicitly planned before execution.
* Direct use of `LogicalPlan` or untyped query builders is no longer supported.
* Code relying on implicit missing-row behavior must now choose a consistency policy.
* Pagination must be expressed at intent time, not derived from execution results.

These changes are foundational. Future releases are expected to be **additive or performance-focused**, not corrective.

---

### 📝 Summary

0.5.0 marks the point where the query engine is considered *correct by construction*.
Subsequent releases should not re-litigate query correctness, atomicity, or executor safety.


---

## [0.4.7] 🧰 - 2026-01-22
- 🔁 Renamed `ensure_exists_many` to `ensure_exists_all` for clarity.
- ✅ `ensure_exists_all` is now a true existence-only guard (no deserialization).
- 🧭 Insert no longer loads existing rows during index planning; missing rows are treated as expected.
- 🐛 Debug sessions now emit logs across load/exists, save, delete, and upsert executors.

---

## [0.4.6] 🧪 - 2026-01-22
- 🧭 Existence checks now treat missing rows as normal and avoid false corruption on scans.
- 🧹 Deletes by primary key are idempotent; missing rows are skipped during pre-scan.
- 🧾 Store not-found is now typed (`StoreError::NotFound`) with `ErrorClass::NotFound`.

---

## [0.4.5] ⚛️ - 2026-01-21 - Atomicity, Part 1
- Moved `FromKey` into `db::traits` and relocated `FromKey` impls into `db/types/*` to keep core types DB-agnostic.
- Moved `Filterable` and `FilterView` into `db::traits` (still re-exported via `traits`).
- Moved index fingerprint hashing out of `Value` into `db::index::fingerprint`.
- Atomicity - commit markers and recovery gating

---

## [0.4.4] 🛡️ - 2026-01-20 - Localized CBOR safety checks and panic containment
- CBOR serialization is now internalized in `icydb-core`, with local decode bounds and structural validation.
- Deserialization rejects oversized payloads before decode and contains any decode panics as typed errors.
- Added targeted CBOR tests for oversized, truncated, and malformed inputs.
- Macro validation now reports invalid schema annotations as compile errors instead of panicking (including trait removal checks and item config validation).

---

## [0.4.3] 📣 - 2026-01-20 - Explicit, classified, and localized error propagation at the Disco!
- Storable encoding and decoding no longer panics
- Persisted rows and index entries now use raw, bounded value codecs (`RawRow`, `RawIndexEntry`); domain types no longer decode directly from stable memory.
- Added explicit size limits and corruption checks for row payloads and index entry key sets; invalid bytes surface as corruption instead of panics.
- Domain types no longer implement `Storable`; decoding uses explicit `try_from_bytes`/`TryFrom<&[u8]>` APIs.
- Added targeted raw codec tests for oversized payloads, truncated buffers, corrupted length fields, and duplicate index keys.
- Storage snapshots now count corrupted index entries via value decode checks.
- Fixed executor candidate scans to propagate decode errors from store range reads.

---

## [0.4.2] 🧹 - 2026-01-19
- Increased `EntityName` and index field limits to 64 chars; `IndexName` length now uses a 2-byte prefix, widening `IndexKey` size.
- `DataKey` now reuses canonical `EntityName` decoding, and `IndexKey` rejects non-zero fingerprint padding beyond `len`.
- Standardized corruption error messages for strict decoders across keys and core types.

---

## [0.4.0] 💥 – 2026-01-18 – ⚠️ Very Breaky Things ⚠️

This release finalizes a major internal storage and planning refactor. It hardens corruption detection, fixes long-standing key-space ambiguities, and establishes strict invariants for ordered storage.

---

### ⚠️ Breaking

* **Entity identity is now name-based**
  Storage and index keys now use the per-canister `ENTITY_NAME` directly.
  This replaces the previous hashed `ENTITY_ID` representation.

  * Improves debuggability and introspection
  * Removes hash collision risk
  * Changes on-disk key layout

* **Key serialization invariants enforced**

  * `Key`, `DataKey`, and `IndexKey` are now *strictly fixed-size* and canonical
  * Variable-length encodings are no longer permitted for ordered keys
  * Any deviation is treated as corruption and surfaced immediately
  * `Account` encoding is now canonical (`None` ≠ `Some([0; 32])`)
  * `EntityName`/`IndexName` ordering now matches serialized bytes, with ASCII + padding validation on decode

---

### 🔧 Changed

* **Index executors decoupled from error/metrics plumbing**

  * Index stores no longer emit executor-level errors
  * Executors now:

    * Emit index metrics
    * Surface uniqueness conflicts explicitly

* **Strict read semantics expanded**

  * Missing or malformed rows are now treated as corruption
  * `delete`, `exists`, and `unique` paths use strict scans by default
  * Silent partial reads are no longer allowed

* **Unique index lookups re-validated**

  * Indexed field values are re-read and compared
  * Hash or value mismatches are surfaced as corruption
  * Prevents stale or inconsistent unique entries from going unnoticed

---

### 🔧 Changed

* **Planner is now side-effect free**

  * Planning no longer mutates state or emits metrics
  * All plan-kind metrics are emitted during execution only
  * Enables deterministic planning and easier reasoning about execution paths

---

### 🔧 Changed

* **IndexName sizing is now derived and validated**

  * Computed from:

    * Entity name (≤ 48 chars)
    * Up to 4 indexed field names (≤ 48 chars each)
  * Boundary checks enforced in:

    * Core storage
    * Schema validators
  * Prevents silent truncation and oversized index identifiers

---

### 🔧 Changed

* **Fixed-size key enforcement**

  * Ordered keys (`Key`, `DataKey`, `IndexKey`) now guarantee:

    * Deterministic byte layout
    * Total ordering equivalence between logical and serialized forms
  * Stable memory corruption is detected early and fails fast

* **Explicit size invariants**

  * All bounded `Storable` implementations now:

    * Enforce exact serialized size
    * Validate input on decode
    * Reject malformed or undersized buffers

---

### 🧭 Migration Notes

* Existing stable data **must be migrated**
* Custom storage code relying on:

  * Variable-length keys
  * Hashed entity identifiers
  * Lenient reads
    will need to be updated
* In return, the storage layer now has **database-grade guarantees** around ordering, identity, and corruption detection

---

This release lays the foundation for:

* Safer upgrades
* More aggressive validation
* Long-term storage stability

Future versions will build on these invariants rather than revisiting them.


## [0.3.3] 🧪 - 2026-01-14
- fixed a CI issue where clippy errors broke things
- #mission70 is retarded

## [0.3.2] 📊 - 2026-01-14 - Metrics Decoupling
- Public `Error` now exposes `class` and `origin` alongside the message.
- Observability: unbundled metrics + query instrumentation via `obs::sink` dependency inversion, keeping executors/planner/storage metrics-agnostic while preserving global default and scoped overrides.
- Metrics: route report/reset through `obs::sink` helpers to keep metrics ingress sealed.
- Metrics: avoid double-counting plan kinds on pre-paginated loads.
- Docs: clarify metrics are update-only by design, instruction deltas are pressure indicators, and executor builders bypass session metrics overrides.
- updated canic to 0.8.4

## [0.3.1] 🛠️ - 2026-01-12
- fixed stupid bug

## [0.3.0] 🚪 – 2026-01-12 – Public Facade Rewrite
### 🔧 Changed
- 🧱 Major layering refactor: icydb is now a strict public facade over icydb-core, with internal subsystems depending directly on core rather than facade modules.
- 🔌 Clear API boundaries: Engine internals (execution, queries, serialization, validation) are fully isolated in icydb-core; icydb exposes only intentional, stable entry points.
- 📦 Public query surface: icydb::db::query is now a supported public API and re-exports core query types for direct use.
- 🛠️ New facade utilities: Added top-level serialize, deserialize, sanitize, and validate helpers with normalized public errors.
- 🔒 Hardened macros & executors: Generated code now targets canonical core paths, preventing accidental API leakage.

### 📝 Summary
- ⚠️ Downstream crates using icydb-core internals may need import updates.
- 🚀 Future internal refactors should now cause far fewer breaking changes.

## [0.2.5] 🚨 - 2026-01-11 - Error Upgrade
- Runtime errors are now unified under `RuntimeError` with class + origin metadata (internal taxonomy, not a stable API).
- Public `Error` values are produced only at API boundaries and now stringify with `origin:class:` prefixes.
- Added `REFACTOR.md` to document the maintainer-facing runtime contract and refactor baseline.

## [0.2.3] 🧰 - 2026-01-04
- Added issue() and issue_at() for sanitizer and validators so you can pass Into<Issue>.  You couldn't before because
it's a dynamic trait.

## [0.2.2] 🧪 - 2026-01-04
- Been working on Canic since Boxing Day, so pushing a new release with the latest [0.7.6] version

## [0.2.1] 📦 - 2025-12-26 - 📦 Boxing Day 📦
- Float32/Float64 deserialization rejects non-finite values; `from_view` now panics on non-finite inputs to enforce invariants.
- more tests!

## [0.2.0] 🎄 - 2025-12-25 - 🎄 Christmas Cleanup 🎄
- 3 crates removed: icydb_error, icydb_paths, icydb_base.  Much simpler dependency graph.
- Goodbye 1100+ lines of code
- Refactored Sanitize/Validate so that creating Validators and Sanitizers cannot panic, but instead Validator::new() errors get added to the error tree
- Visitor method now uses a context instead of recursive trees
- Visitor method now has a generic return Error method via the VisitorCore / VisitorAdapter pattern
- Paths are now automatically ::icydb because we do an `extern crate self as icydb`
- Merry Christmas!

---

## [0.1.x] 🌱 - 2025-12-24 - Query Ergonomics and Safety Foundations

- `0.1.0` updates the workspace baseline to Rust `1.92.0`, adapts to canic crate layout changes, and restores clean lint/tooling checks.
- `0.1.1` removes `msg_caller` from `Principal` and adds `WrappedPrincipal::from_text` passthrough support.
- `0.1.5` expands `FilterExpr` method coverage, tightens CI/clippy workflow stability, and fixes `UpdateView<T>` clearing for `Some(None)`.
- `0.1.7` simplifies and hardens single-result versus multi-result query response handling.
- `0.1.10` introduces `ResponseExt` convenience methods for common first/view/count/pk extraction paths.
- `0.1.11` improves query ergonomics by allowing direct reuse of pre-built filters and direct response interpretation helpers.
- `0.1.12` makes `Ulid::generate` and `Subaccount::random` fall back to zeroed randomness when RNG is unseeded.
- `0.1.13` expands `ResponseExt` with additional existence checks.
- `0.1.14` begins grouped aggregation groundwork with `group_count_by` in `LoadExecutor`.
- `0.1.15` adds response cardinality guards plus delete-side `ensure_deleted_*` helpers for stricter invariants.
- `0.1.16` removes unused save-operation generics and adds batch save helpers (`insert_many`, `create_many`, `replace_many`).
- `0.1.17` adds explicit `many_by_field` helpers for load/delete while keeping primary-key many helpers as convenience wrappers.
- `0.1.18` adds `Row`/`Page` response helpers, fixes index-planned `exists` filter handling, and introduces unique-index upsert execution.
- `0.1.19` stabilizes unique-index upsert/delete behavior, primary-key `IN` planning, deterministic index-load ordering, and exists-window semantics.
- `0.1.20` hardens metrics counters and numeric/date/time edge-case handling, with focused regression coverage.

See detailed breakdown:
[docs/changelog/0.1.md](docs/changelog/0.1.md)

---

## [0.0.x] 🧊 - 2025-12-09 - ICYDB REBOOT - KEEP DATA COOL

```text
  _________
 /        /|
/  DATA  / |
/________/  |
|  COOL  |  /
|  ❄❄❄ | /
|________|/
keep data cool
```

- New name, same mission: IcyDB takes over from Mimic with the public meta-crate exposed at `icydb`.

- `0.0.1` launches the IcyDB reboot line with the `icydb` crate surface, refreshed docs, path resolution updates, and aligned `icydb_*` endpoint/codegen naming.
- `0.0.6` adds early finance/sanitizer support, cleans dependencies, fixes boxed-view type alignment, and hardens enum matching behavior.
- `0.0.8` updates to canic `0.4`, resolves the `darling` yank follow-up, and improves public API rustdoc coverage.
- `0.0.9` updates to canic `0.4.8` and audits public endpoint visibility (`pub` vs `pub(crate)`).
- `0.0.10` removes unauthenticated `icydb_query_*` endpoints and switches codegen to internal dispatch helpers so auth stays caller-owned.
- `0.0.11` expands `Timestamp` support and tests across seconds/millis/micros/nanos plus RFC3339 parsing.
- `0.0.13` adds tests for identifier/path/metrics/hash behavior, documents public codegen macros, and clarifies confusing payment/amount type naming.
- `0.0.14` removes the direct canic dependency after `canic-core` and `canic-memory` split into separate crates.
- `0.0.15` adds payload-aware enum storage, broadens boxed/vector field-value support, adds regression coverage, and moves `build!` into `icydb-build`.
- `0.0.20` fixes delete-window correctness (`offset`/`limit`) and shares scan/deserialization helpers between load/delete planning paths.

See detailed breakdown:
[docs/changelog/0.0.md](docs/changelog/0.0.md)
