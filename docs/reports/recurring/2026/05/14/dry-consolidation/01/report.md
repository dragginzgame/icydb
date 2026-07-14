# DRY Consolidation Audit - 2026-05-14

## Report Preamble

- scope: duplication and consolidation pressure across `crates/icydb-core/src`
  runtime boundaries; tests are excluded unless explicitly called out
- compared baseline report path:
  `docs/audits/reports/2026-05/2026-05-13/dry-consolidation.md`
- rerun basis: clean post-`0.154.9` release baseline before this report and
  artifacts were generated
- code snapshot identifier: `499a8478a`
- method tag/version: `DRY-1.2`
- method manifest:
  - `method_version = DRY-1.2`
  - `duplication_taxonomy = DT-1`
  - `owner_layer_taxonomy = OL-1`
  - `invariant_role_model = IR-1`
  - `facade_inclusion_rule = FI-1`
  - `consolidation_safety_model = CS-1`
- comparability status: `comparable`; method, scope, and taxonomy match the
  2026-05-13 baseline

## Evidence Artifacts

- `docs/audits/reports/2026-05/2026-05-14/artifacts/dry-consolidation/dry-consolidation-runtime-metrics.tsv`
- `docs/audits/reports/2026-05/2026-05-14/artifacts/dry-consolidation/dry-consolidation-pattern-counts.tsv`
- `docs/audits/reports/2026-05/2026-05-14/artifacts/dry-consolidation/dry-consolidation-module-pressure.tsv`
- `docs/audits/reports/2026-05/2026-05-14/artifacts/dry-consolidation/dry-consolidation-sql-decisions.tsv`

## STEP 0 - Run Metadata + Scope Capture

Evidence mode: `semi-mechanical`

| Item [M/C] | Previous [M/C] | Current [M/C] | Delta [D] | Comparable? [C] |
| ---- | ---- | ---- | ---- | ---- |
| baseline report path | `docs/audits/reports/2026-05/2026-05-07/dry-consolidation.md` | `docs/audits/reports/2026-05/2026-05-13/dry-consolidation.md` | one report forward | yes |
| method version | `DRY-1.2` | `DRY-1.2` | none | yes |
| duplication taxonomy | `DT-1` | `DT-1` | none | yes |
| owner-layer taxonomy | `OL-1` | `OL-1` | none | yes |
| invariant role model | `IR-1` | `IR-1` | none | yes |
| facade inclusion rule | `FI-1` | `FI-1` | none | yes |
| consolidation safety model | `CS-1` | `CS-1` | none | yes |
| in-scope roots | `crates/icydb-core/src` | same | none | yes |
| exclusions | tests/bench/examples/generated | same | none | yes |
| snapshot state | dirty tree at `2008c4809` | clean post-`0.154.9` code at `499a8478a` before report generation | release-baseline evidence replaces dirty-tree qualification | yes |

## STEP 1A - Structural Duplication Scan

Evidence mode: `mechanical`

| Pattern [M] | Files [M] | Duplication Type [C] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Safety Critical? [C] | Behavioral Equivalence Confidence [C] | Drift Risk [C] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| field-path schema mutation runner publication pipeline | `db/schema/reconcile.rs`; `db/schema/mutation/field_path/runner.rs`; `field_path/staging.rs`; `field_path/staged_store.rs`; `field_path/isolated_store.rs`; `field_path/publication.rs` | Intentional boundary duplication | yes | yes | yes | yes | high | low-medium | low-medium |
| startup field-path reconciliation gate/publication adapter | `db/schema/reconcile.rs` | Same-owner startup adapter cluster | yes | yes | yes | yes | high | medium-low | medium-low |
| compiled expression IR vs planner-to-IR lowering | `db/query/plan/expr/compiled_expr/mod.rs`; `db/query/plan/expr/compiled_expr/compile.rs`; `db/query/plan/expr/compiled_expr/evaluate.rs`; `db/query/plan/expr/scalar.rs` | Intentional boundary duplication | yes | yes | yes | yes | high | low | low |
| predicate capability classification plus consumption | `db/predicate/capability.rs`; `db/predicate/runtime/mod.rs`; `db/index/predicate/compile.rs` | Intentional boundary duplication | no | yes | yes | yes | high | low | low |
| continuation cursor contract transport | `db/cursor/mod.rs`; `db/cursor/spine.rs`; `db/query/plan/continuation.rs`; `db/executor/planning/continuation/scalar.rs` | Intentional boundary duplication | no | yes | yes | yes | high | medium-low | medium-low |
| route capability snapshot forwarding | `db/executor/planning/route/capability.rs`; `db/executor/planning/route/contracts/capabilities.rs`; `db/executor/planning/route/hints/load.rs`; `db/executor/planning/route/hints/aggregate.rs` | Intentional boundary duplication | no | yes | yes | yes | high | medium | medium |
| commit marker envelope plus size guards | `db/commit/store/mod.rs`; `db/commit/marker.rs` | Defensive duplication | no | yes | yes | yes | high | low-medium | low-medium |
| index-store reader bridge and preflight overlay access | `db/index/readers.rs`; `db/registry/readers.rs`; `db/executor/runtime_context/index_readers.rs`; `db/executor/mutation/commit_window.rs`; `db/commit/prepare.rs` | Boundary bridge duplication | no | yes | yes | yes | high | low-medium | low-medium |
| accepted schema generated-compatible row-decode bridge | `db/schema/runtime.rs`; `db/session/mod.rs`; `db/executor/authority/entity.rs`; `db/executor/terminal/row_decode/mod.rs`; `db/data/structural_row.rs` | Boundary bridge duplication | no | yes | yes | yes | high | low-medium | low-medium |
| fluent non-paged terminal public wrappers | `db/query/fluent/load/terminals.rs` | Boilerplate duplication | yes | yes | no | no | high | low-medium | low-medium |
| SQL projection and result finalization shell | `db/session/sql/execute/mod.rs`; `db/session/sql/mod.rs`; `db/session/sql/compiled.rs` | Boilerplate duplication | yes | yes | no | no | high | low-medium | low-medium |

## STEP 1B - Closed High-Risk Seam Verification

Evidence mode: `mechanical`

| Former Risk [M] | Current Evidence [M] | Classification [C] | Residual Risk [C] |
| ---- | ---- | ---- | ---- |
| duplicate SQL grouped/projection shape decisions across lowering and execution | `scripts/audit/sql_duplicate_decision_count.sh` reported `recomputed_decision_count=0`, `propagated_decision_count=0`, `tracked_decisions=5` | closed | low |
| schema mutation publication widened before runner execution | startup execution now calls `SchemaMutationRunnerInput::new`, `SchemaFieldPathIndexRunner::run`, `StartupFieldPathRebuildGate`, `StartupFieldPathPublicationDecision`, and final physical-store revalidation before inserting `accepted_after` | closed for the single supported path | low |
| same-owner schema reconciliation publication tail duplication | `accept_reconciled_schema_snapshot` is shared by metadata-only and runtime-store reconciliation paths | closed | low |
| accepted row-decode contract attached without generated-compatible proof | proof propagation remains schema-runtime-owned and executor-consumed | closed in production | low |

## STEP 2A - Semantic Redundancy Scan

Evidence mode: `classified`

| Pattern Family [M] | Occurrences [M] | Layers Involved [M] | Cross-Layer? [D] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Difficulty [C] | Suggested Owner Layer [C] | Risk [C] |
| ---- | ----: | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| field-path startup runner and publication contracts | 6 coordinated surfaces | reconcile startup adapter plus mutation field-path runner phases | no | yes | yes | yes | yes (`SchemaFieldPathIndexRunner` and startup reconcile adapter) | high | medium | keep runner contracts; only extract adapter helpers locally if split pressure grows | low-medium |
| startup field-path gate and publication adapter | 1 owner cluster | runtime startup reconciliation helpers | no | yes | yes | yes | yes (`db/schema/reconcile.rs`) | high | low-medium | reconcile startup adapter | medium-low |
| compiled expression evaluation vs compile lowering | 3 coordinated surfaces | planner scalar compile tree, compile lowering, compiled evaluator | no | yes | yes | yes | yes (`CompiledExpr::evaluate`, with lowering in `compiled_expr/compile.rs`) | high | low | keep compile/evaluate split | low |
| predicate capability classification | 3 | predicate authority, runtime compilation, index predicate compile | yes | no | yes | yes | yes (`db/predicate/capability.rs`) | high | low | predicate boundary | low |
| continuation meaning transport and revalidation | 4 | cursor, planner continuation, executor continuation, session transports | yes | no | yes | yes | yes (`db/cursor/mod.rs` plus planner contract) | high | medium | cursor/planner continuation boundary | medium-low |
| route capability snapshot propagation | 4 | route capability, route contracts, load hints, aggregate hints | yes | no | yes | yes | yes (`db/executor/planning/route/capability.rs`) | high | medium | route boundary | medium |
| commit marker envelope enforcement | 2 logical owners | commit store, commit marker payload codec | yes | no | yes | yes | yes (`db/commit/store/mod.rs`) | high | low | commit store boundary | low-medium |
| index-store reader bridge and preflight overlay access | 5 | index reader trait, registry handle reader, runtime context reader, commit preflight overlay, commit preparation | yes | no | yes | yes | yes (`db/index/readers.rs` trait boundary) | high | medium | keep reader contract in index; keep overlay in mutation commit-window | low-medium |
| accepted generated-compatible row-decode proof propagation | 5 | schema runtime descriptor, session authority, entity authority, executor row layout, structural row contract | yes | no | yes | yes | yes (`AcceptedRowLayoutRuntimeDescriptor::from_generated_compatible_schema`) | high | medium | schema runtime owns proof; executor consumes proof | low-medium |
| fluent non-paged terminal public wrappers | 1 owner family | fluent load terminal public methods and projection output shaping | no | yes | yes | no | yes (`db/query/fluent/load/terminals.rs`) | high | low | fluent load boundary | low-medium |
| SQL projection execution and response finalization shells | 1 owner family | SQL session compile contract, plan-cache binding, statement-result shaping | no | yes | yes | no | yes (`db/session/sql`) | high | low | session SQL boundary | low-medium |

## STEP 3A - Duplication-Driven Split Pressure Only

Evidence mode: `semi-mechanical`

| Module [M] | Size [M] | Duplication Families [M] | Same Owner Layer? [C] | Pressure Type [C] | Duplication Amplification [C] | Risk [C] |
| ---- | ----: | ----: | ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/schema/reconcile.rs` | `843` runtime LOC | `2` | yes | startup adapter growth | 0.154 closeout moved the accepted snapshot tail into a shared helper, but startup field-path gates, diagnostics, tests, and physical publication checks now make this file the main local split-pressure point | medium-low |
| `crates/icydb-core/src/db/schema/mutation/mod.rs` | `1478` runtime LOC | `1` | yes | broad contract surface | mutation-plan and runner contracts are broad but remain a centralized schema-owned authority | low-medium |
| `crates/icydb-core/src/db/schema/mutation/field_path/publication.rs` | `639` runtime LOC | `1` | yes | publication phase reports | validation/invalidation/snapshot/physical publication reports repeat phase checks intentionally | low-medium |
| `crates/icydb-core/src/db/schema/runtime.rs` | `510` runtime LOC | `2` | yes | descriptor proof authority | generated-compatible proof construction remains centralized but descriptor checks are a growing authority surface | low-medium |
| `crates/icydb-core/src/db/session/sql/execute/mod.rs` | `621` runtime LOC | `1` | yes | execution shell repetition | cached and bypass paths still repeat prepared-plan/projection-contract shells | low-medium |
| `crates/icydb-core/src/db/query/fluent/load/terminals.rs` | `778` runtime LOC | `1` | yes | partially compressed | remaining repetition is public method shape and output conversion | low-medium |

## STEP 4A - Invariant Repetition Classification

Evidence mode: `classified`

| Invariant [M] | Canonical Owner [C] | Enforcement Sites [M] | Site Roles [C] | Boundary-Protected? [C] | Sites Count [D] | Classification [C] | Divergence Risk [C] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ----: | ---- | ---- | ---- |
| supported field-path schema mutation publication | `SchemaFieldPathIndexRunner` plus startup adapter in `db/schema/reconcile.rs` | transition admission; runner input binding; accepted row decode; rebuild gate; staged physical validation; physical publication readiness; final target-index revalidation; schema snapshot insert | defining plus staging plus startup application plus defensive re-checking | yes | 8 | Safety-enhancing | low-medium | low-medium |
| accepted schema reconciliation publication/metrics tail | `db/schema/reconcile.rs` | schema-store path; runtime-store path | metadata-only and runtime-startup variants through shared tail helper | yes | 2 | Consolidated same-owner helper | low | low |
| scalar expression evaluation semantics | `db/query/plan/expr/compiled_expr/evaluate.rs` | `compiled_expr/evaluate.rs`; executor reader adapters | defining plus value-source adaptation | yes | 2 | Boundary-protected | low | low |
| predicate capability meaning | `db/predicate/capability.rs` | `predicate/capability.rs`; `predicate/runtime/mod.rs`; `index/predicate/compile.rs` | defining plus runtime admission plus index admission | yes | 3 | Safety-enhancing | low | low |
| continuation contract meaning | `db/cursor/mod.rs` and `db/query/plan/continuation.rs` | `cursor/mod.rs`; `cursor/spine.rs`; `query/plan/continuation.rs`; `executor/planning/continuation/scalar.rs` | defining plus validating plus transport plus defensive re-checking | yes | 4 | Safety-enhancing | medium-low | medium-low |
| route capability snapshot interpretation | `db/executor/planning/route/capability.rs` | `route/capability.rs`; `route/contracts/capabilities.rs`; `hints/load.rs`; `hints/aggregate.rs` | defining plus transport plus application | yes | 4 | Safety-enhancing | medium | medium |
| commit marker canonical envelope | `db/commit/store/mod.rs` | `commit/store/mod.rs`; `commit/marker.rs` | defining plus defensive re-checking | yes | 2 | Safety-enhancing | low-medium | low-medium |
| index-store reader authority | `db/index/readers.rs` | `index/readers.rs`; `runtime_context/index_readers.rs`; `registry/readers.rs`; `commit_window.rs`; `commit/prepare.rs` | defining plus runtime implementation plus registry implementation plus preflight overlay plus preparation consumer | yes | 5 | Safety-enhancing | low-medium | low-medium |
| accepted generated-compatible row-decode authority | `db/schema/runtime.rs` | descriptor proof construction; session authority handoff; entity authority proof forwarding; row-layout proof consumption; structural row contract construction | defining plus validating plus transport plus consumption | yes | 5 | Boundary bridge | low-medium | low-medium |

## STEP 5A - Error Mapping / Construction Drift

Evidence mode: `semi-mechanical`

| Error Family [M] | Files [M] | Mapping Logic Duplication [C] | Classification Drift? [C] | Origin Drift? [C] | Same Owner Layer? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Safety Class [C] | Drift Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| schema mutation startup runner failures | `db/schema/reconcile.rs`; `db/schema/mutation/field_path/runner.rs`; `field_path/publication.rs` | medium | no | no | yes | yes | high | boundary-sensitive | medium-low |
| projection expression evaluation failures | `db/query/plan/expr/compiled_expr/mod.rs`; `db/executor/projection/eval/scalar.rs` | low-medium | no | no | no | yes | high | boundary-sensitive | medium-low |
| commit marker corruption constructors and envelope decode guards | `db/commit/marker.rs`; `db/commit/store/mod.rs` | low | no | no | no | yes | high | protective duplication | low-medium |
| index-store reader failures | `db/index/readers.rs`; `db/executor/runtime_context/index_readers.rs`; `db/registry/readers.rs`; `db/executor/mutation/commit_window.rs` | low | no | no | no | yes | high | boundary-sensitive | low-medium |
| accepted generated-compatible row-shape failures | `db/schema/runtime.rs`; `db/session/mod.rs`; `db/executor/authority/entity.rs`; `db/executor/terminal/row_decode/mod.rs` | low | no | no | no | yes | high | protective duplication | low-medium |

## STEP 6B - Protective Redundancy Review

Evidence mode: `classified`

| Pattern Family [M] | Files [M] | Same Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Why It Must Stay Split [C] | Behavioral Equivalence Confidence [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| field-path runner stages, validates, invalidates, publishes snapshot, publishes physical store, and final-revalidates startup visibility | field-path runner modules plus startup adapter | yes | yes | yes | each phase has a different failure/rollback/visibility boundary and should stay observable until supported multi-index publication exists | high |
| runtime-store startup execution and metadata-only schema-store reconciliation | `db/schema/reconcile.rs` | yes | yes | partially | runtime-store path may execute physical runner work; metadata-only path must stay fail-closed for physical work | high |
| compiled expression IR vs planner-to-IR compile lowering | `compiled_expr/mod.rs`; `compiled_expr/compile.rs`; `compiled_expr/evaluate.rs`; `scalar.rs` | yes | yes | yes | evaluator purity depends on keeping planner structures out of runtime evaluation while compile modules own late lowering from planner trees into resolved IR leaves | high |
| predicate capability meaning vs runtime/index application | `db/predicate/capability.rs`; `db/predicate/runtime/mod.rs`; `db/index/predicate/compile.rs` | no | yes | yes | capability ownership stays in predicate authority while runtime and index compilation consume classified results locally | high |
| cursor contract definition vs planner/runtime transports | `db/cursor/mod.rs`; `db/cursor/spine.rs`; `db/query/plan/continuation.rs`; `db/executor/planning/continuation/scalar.rs` | no | yes | yes | preserves one continuation meaning while keeping wire decode, planner validation, and executor revalidation distinct | high |
| route capability derivation vs route hint consumption | `db/executor/planning/route/capability.rs`; `db/executor/planning/route/contracts/capabilities.rs`; `db/executor/planning/route/hints/*` | no | yes | yes | keeps route-owned capability reasoning separate from hint application and downstream execution contracts | high |
| commit marker stable envelope vs payload codec | `db/commit/store/mod.rs`; `db/commit/marker.rs` | no | yes | yes | store owns the persisted trust boundary while marker owns the row-op payload shape | high |
| index reader contract vs runtime/preflight implementations | `db/index/readers.rs`; `db/executor/runtime_context/index_readers.rs`; `db/registry/readers.rs`; `db/executor/mutation/commit_window.rs` | no | yes | yes | preserves one reader contract while runtime contexts and preflight overlays implement their own visibility and staging semantics | high |
| accepted schema runtime descriptor vs generated-compatible row decode bridge | `db/schema/runtime.rs`; `db/session/mod.rs`; `db/executor/authority/entity.rs`; `db/executor/terminal/row_decode/mod.rs` | no | yes | yes | schema runtime owns generated-compatible proof construction while executor row decode only consumes the proof and accepted decode facts | high |

## STEP 7B - Consolidation Candidates (Post-Protection Gate)

Evidence mode: `classified`

| Area [M] | Files [M] | Duplication Type [C] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Consolidation Safety Class [C] | Suggested Owner Layer [C] | Difficulty [C] | Drift Surface Reduction [C] | Estimated LoC Reduction [D] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- |
| startup field-path reconciliation adapter split | `db/schema/reconcile.rs` | Same-owner adapter concentration | yes | yes | yes | yes | safe local module split after audit approval | schema reconcile boundary | low-medium | medium-low | 0-8 | medium-low |
| route capability snapshot call-site compression | `db/executor/planning/route/hints/load.rs`; `db/executor/planning/route/hints/aggregate.rs` | Boilerplate duplication | yes | yes | partially | yes | safe local unification | route boundary | low-medium | low | 4-8 | low-medium |
| SQL projection/result shell compression | `db/session/sql/execute/mod.rs`; `db/session/sql/mod.rs` | Boilerplate duplication | yes | yes | no | yes | safe local unification | session SQL boundary | low | low | 6-12 | low-medium |

## STEP 8B - Dangerous Consolidations (Do NOT Merge)

Evidence mode: `classified`

| Area [M] | Why Duplication Is Protective [C] | Boundary-Protected? [C] | Consolidation Safety Class [C] | Risk If Merged [C] |
| ---- | ---- | ---- | ---- | ---- |
| schema mutation runner phase contracts | staging, physical validation, runtime invalidation, snapshot handoff, physical-store publication, final startup revalidation, and schema-store insertion carry different rollback and visibility meanings | yes | do not merge phases | high |
| startup runtime-store reconciliation and metadata-only schema-store reconciliation | runtime-store path is allowed to execute physical runner work; metadata-only path must stay fail-closed for physical work | partially | do not merge execution paths | medium-high |
| `CompiledExpr` evaluator and planner compile lowering | evaluator purity depends on keeping planner structures out of evaluation; only compile modules should know planner expression shapes | yes | do not merge | high |
| executor row/value readers and compiled expression semantics | readers are the execution-context boundary; moving row decoding or aggregation mechanics into expression evaluation would break the single-IR layering contract | yes | do not merge | high |
| cursor contract definition and planner/runtime/session transport code | split preserves one semantic owner while keeping decode, validation, and transport boundaries explicit | yes | do not merge | high |
| route capability derivation and planner/hint consumption | split preserves route-owned capability reasoning and prevents policy re-derivation in consumers | yes | do not merge | high |
| commit marker store envelope and payload codec | split preserves the persistence trust boundary and failure classification edge | yes | do not merge | high |
| predicate capability meaning and runtime/index application | split preserves one semantic authority while keeping consumer admission policy local | yes | do not merge | medium |
| index reader contract and preflight overlay implementation | the overlay must remain commit-window local because it observes staged mutations that normal runtime/registry readers must not see | yes | do not merge | medium-high |
| accepted runtime schema descriptor and generated row decode bridge | generated-compatible proofs must remain schema-runtime-owned and executor-consumed; making generated codecs the authority would hide accepted-layout drift | yes | do not merge | medium-high |

## STEP 9 - Quantitative Summary + High-Risk Ledger

Evidence mode: `semi-mechanical`

| Metric [M/C/D] | Previous [M/C/D] | Current [M/C/D] | Delta [D] | Interpretation [C] |
| ---- | ---- | ---- | ---- | ---- |
| total duplication patterns found | `11` | `11` | `0` | the surface count is stable after 0.154.9; the former reconcile tail duplication is closed, while startup publication revalidation remains tracked as a protective adapter cluster |
| total high-risk divergence patterns | `0` | `0` | `0` | no duplicate evaluator, SQL shape decision, generated-as-authority path, or unsupported schema mutation publication path reopened |
| same-layer accidental duplication count | `1` | `0` | `-1` | accepted schema reconciliation tail duplication was consolidated through the shared reconcile helper |
| cross-layer intentional duplication count | `3` | `3` | `0` | predicate, continuation, and route duplication remain intentionally boundary-protective |
| defensive duplication count | `1` | `1` | `0` | commit marker envelope checks remain intentionally duplicated at the store/payload boundary |
| boundary-protected duplication count | `8` | `8` | `0` | field-path runner publication phases remain protected duplication, not a merge target |
| invariants with `>3` enforcement sites | `7` | `7` | `0` | the supported-publication invariant remains multi-site by design |
| error-construction families with `>3` custom mappings | `2` | `2` | `0` | schema mutation startup runner errors remain typed and owner-local |
| drift surface reduction estimate | `medium-low` | `medium-low` | stable | current pressure is local split pressure and small boilerplate, not divergent semantics |
| estimated LoC reduction range (conservative) | `22-44` | `10-20` | `-12-24` | the previous reconcile tail helper candidate has already landed; remaining reductions are small |

High-risk ledger:

| Pattern [M] | Primary Locations [M] | Owner Boundary [C] | Canonical Owner Known? [C] | Worth Fixing This Cycle? [C] | Consolidation Safety Class [C] | Rationale [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| none | n/a | n/a | n/a | n/a | n/a | production scalar, grouped, SQL shape, accepted row-decode, and supported schema mutation runner paths remain under explicit owner boundaries |

## STEP 9A - Analyst Verification Readout

Evidence mode: `semi-mechanical`

| Verification Metric [M/C/D] | Count [D] | Definition [M/C] |
| ---- | ----: | ---- |
| mechanical findings count | 11 | STEP 1A rows backed by direct pattern/file anchors |
| classified findings count | 40 | STEP 2A + STEP 4A + STEP 5A + STEP 6B + STEP 7B + STEP 8B rows requiring owner/safety judgment |
| high-confidence candidate count | 3 | STEP 7B candidates with high behavioral-equivalence confidence and safe local boundaries |
| boundary-protected findings count | 18 | rows where `Boundary-Protected? = yes` or `partially` across Steps 1A/4A/6B/8B |

## 1. Run Metadata + Comparability Note

- `DRY-1.2` method manifest applied; this run is method-comparable to the
  2026-05-13 baseline.
- Unlike the May 13 run, this report is not dirty-tree qualified for runtime
  code. It was generated after the `0.154.9` closeout push from snapshot
  `499a8478a`.
- The 0.154 supported schema-runner path is now part of the release baseline,
  so the audit treats its phase split as protective redundancy rather than a
  new high-risk duplication seam.

## 2. Mode A Summary: High-Impact Consolidation Opportunities

- No high-risk production DRY seam is open.
- SQL grouped/projection decision duplication remains closed: the audit helper
  reported `recomputed_decision_count=0` and `propagated_decision_count=0`.
- Supported schema mutation startup execution is not publishing from duplicated
  semantics: it consumes schema-owned runner input, accepted row contracts,
  the field-path runner, startup rebuild gate, publication decision, and final
  physical-store revalidation before inserting the accepted-after snapshot.
- The May 13 reconcile-tail consolidation candidate is closed by the shared
  accepted snapshot reconciliation helper.

## 3. Mode A Summary: Medium Opportunities

- The only medium-low follow-up is structural: `db/schema/reconcile.rs` now
  carries the 0.154 startup field-path adapter, gate, publication decision, and
  tests. A local module split could improve scanability, but it should not merge
  runtime-store physical execution with metadata-only reconciliation.
- Route capability snapshot call-site compression remains a small owner-local
  cleanup candidate.
- SQL projection/result shell repetition remains a small session-SQL cleanup
  candidate.

## 4. Mode A Summary: Low/Cosmetic Opportunities

- Fluent non-paged terminal public wrappers remain local boilerplate.
- Field-path publication report phases repeat status checks, but this is
  intentional while rollback, invalidation, snapshot handoff, and physical
  store publication remain distinct failure boundaries.

## 5. Mode B Summary: Protective Redundancies (Keep Separate)

- Keep field-path runner publication phases split through DDL and multi-index
  work.
- Keep runtime-store startup execution separate from metadata-only schema-store
  reconciliation.
- Keep `CompiledExpr` evaluator purity separate from planner compile lowering.
- Keep executor readers separate from compiled-expression semantics.
- Keep cursor contract definition separate from planner/runtime/session
  transport code.
- Keep route capability derivation separate from hint consumers.
- Keep commit marker store-envelope checks separate from marker payload codec.
- Keep index reader contracts separate from runtime/preflight implementations.
- Keep accepted schema runtime descriptors and generated-compatible proof
  construction separate from executor row decode consumption.

## 6. Dangerous Consolidations (Do Not Merge)

- Do not collapse the schema mutation runner phases into one untyped startup
  write path.
- Do not allow metadata-only reconciliation to execute physical schema mutation
  work.
- Do not move planner expression types into `compiled_expr/evaluate.rs`.
- Do not move executor row decoding, grouped row state, projection
  materialization, or aggregation mechanics into compiled-expression
  evaluation.
- Do not merge cursor semantic ownership into planner/runtime/session transport
  code.
- Do not merge route capability derivation into hint or execution consumers.
- Do not merge commit store envelope validation into payload codec logic.
- Do not collapse `PreflightStoreOverlay` into the normal runtime/registry index
  readers.
- Do not make generated codecs the accepted layout authority.

## 7. Quantitative Summary

- patterns found: `11`
- high-risk divergence patterns: `0`
- boundary-protected patterns: `8`
- drift surface reduction estimate: `medium-low`
- conservative LoC reduction: `10-20`
- DRY risk index: `3.6/10`
