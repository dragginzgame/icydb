# DRY Consolidation Audit - 2026-05-04

## Report Preamble

- scope: duplication and consolidation pressure across `crates/icydb-core/src` runtime boundaries; tests are excluded unless explicitly called out
- compared baseline report path: `docs/audits/reports/2026-05/2026-05-03/dry-consolidation.md`
- rerun basis: clean `0.147.4` snapshot after accepted schema/write descriptor bridge closeout
- code snapshot identifier: `13ec2bef4`
- method tag/version: `DRY-1.2`
- method manifest:
  - `method_version = DRY-1.2`
  - `duplication_taxonomy = DT-1`
  - `owner_layer_taxonomy = OL-1`
  - `invariant_role_model = IR-1`
  - `facade_inclusion_rule = FI-1`
  - `consolidation_safety_model = CS-1`
- comparability status: `comparable`; this run is cleaner than the 2026-05-03 scan because it was taken from a clean working tree

## Evidence Artifacts

- `docs/audits/reports/2026-05/2026-05-04/artifacts/dry-consolidation/dry-consolidation-pattern-counts.tsv`
- `docs/audits/reports/2026-05/2026-05-04/artifacts/dry-consolidation/dry-consolidation-module-pressure.tsv`

## STEP 0 - Run Metadata + Scope Capture

Evidence mode: `semi-mechanical`

| Item [M/C] | Previous [M/C] | Current [M/C] | Delta [D] | Comparable? [C] |
| ---- | ---- | ---- | ---- | ---- |
| baseline report path | `docs/audits/reports/2026-04/2026-04-30/dry-consolidation.md` | `docs/audits/reports/2026-05/2026-05-03/dry-consolidation.md` | one report forward | yes |
| method version | `DRY-1.2` | `DRY-1.2` | none | yes |
| duplication taxonomy | `DT-1` | `DT-1` | none | yes |
| owner-layer taxonomy | `OL-1` | `OL-1` | none | yes |
| invariant role model | `IR-1` | `IR-1` | none | yes |
| facade inclusion rule | `FI-1` | `FI-1` | none | yes |
| consolidation safety model | `CS-1` | `CS-1` | none | yes |
| in-scope roots | `crates/icydb-core/src` | same | none | yes |
| exclusions | tests/bench/examples/generated | same | none | yes |
| snapshot state | dirty tree at `0eaa42bf5` | clean tree at `13ec2bef4` | cleaner evidence base | yes |

## STEP 1A - Structural Duplication Scan

Evidence mode: `mechanical`

| Pattern [M] | Files [M] | Duplication Type [C] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Safety Critical? [C] | Behavioral Equivalence Confidence [C] | Drift Risk [C] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| compiled expression IR vs planner-to-IR lowering | `db/query/plan/expr/compiled_expr/mod.rs`; `db/query/plan/expr/compiled_expr/compile.rs`; `db/query/plan/expr/compiled_expr/evaluate.rs`; `db/query/plan/expr/scalar.rs` | Intentional boundary duplication | yes | yes | yes | yes | high | low | low |
| predicate capability classification plus consumption | `db/predicate/capability.rs`; `db/predicate/runtime/mod.rs`; `db/index/predicate/compile.rs` | Intentional boundary duplication | no | yes | yes | yes | high | low | low |
| continuation cursor contract transport | `db/cursor/mod.rs`; `db/cursor/spine.rs`; `db/query/plan/continuation.rs`; `db/executor/planning/continuation/scalar.rs` | Intentional boundary duplication | no | yes | yes | yes | high | medium-low | medium-low |
| route capability snapshot forwarding | `db/executor/planning/route/capability.rs`; `db/executor/planning/route/contracts/capabilities.rs`; `db/executor/planning/route/hints/load.rs`; `db/executor/planning/route/hints/aggregate.rs` | Intentional boundary duplication | no | yes | yes | yes | high | medium | medium |
| commit marker envelope plus size guards | `db/commit/store/mod.rs`; `db/commit/marker.rs` | Defensive duplication | no | yes | yes | yes | high | low-medium | low-medium |
| index-store reader bridge and preflight overlay access | `db/index/readers.rs`; `db/registry/readers.rs`; `db/executor/runtime_context/index_readers.rs`; `db/executor/mutation/commit_window.rs`; `db/commit/prepare.rs` | Boundary bridge duplication | no | yes | yes | yes | high | low-medium | low-medium |
| SQL accepted write descriptor bridge helpers | `db/session/sql/execute/write.rs`; `db/session/sql/execute/write_returning.rs`; `db/session/sql/projection/labels.rs`; `db/session/mod.rs` | Boilerplate duplication | yes | yes | partially | yes | high | low-medium | low-medium |
| fluent non-paged terminal public wrappers | `db/query/fluent/load/terminals.rs` | Boilerplate duplication | yes | yes | no | no | high | low-medium | low-medium |
| SQL projection and result finalization shell | `db/session/sql/execute/mod.rs`; `db/session/sql/mod.rs`; `db/session/sql/compiled.rs` | Boilerplate duplication | yes | yes | no | no | high | low-medium | low-medium |

## STEP 1B - Closed High-Risk Seam Verification

Evidence mode: `mechanical`

| Former Risk [M] | Current Evidence [M] | Classification [C] | Residual Risk [C] |
| ---- | ---- | ---- | ---- |
| row/scalar projection evaluating `ScalarProjectionExpr` directly at runtime | `rg "ScalarProjectionExpr\|eval_scalar_projection_expr\|eval_canonical_scalar_projection_expr\|ScalarProjectionExpr::\|match expr" crates/icydb-core/src/db/executor -n` returns production adapter calls in `db/executor/projection/eval/scalar.rs` plus tests | closed in production | low |
| duplicate scalar operator evaluator under executor projection | `rg "GroupedCompiledExpr\|GroupedProjectionExpr\|eval_grouped_projection_expr\|eval_binary_expr\|eval_unary_expr\|projection/eval/operators\|ScalarProjectionExpr" crates/icydb-core/src/db -g '!**/tests/**' -g '!**/tests.rs' -n` returns planner compile surfaces only for `ScalarProjectionExpr` | closed | low |
| runtime expression paths bypassing `CompiledExpr::evaluate` | production `rg "\.evaluate\(" crates/icydb-core/src/db/executor crates/icydb-core/src/db/query/plan/expr -g '!**/tests/**' -g '!**/tests.rs' -n` shows executor projection, grouped, and aggregate paths calling `CompiledExpr::evaluate` through reader adapters | closed | low |

## STEP 2A - Semantic Redundancy Scan

Evidence mode: `classified`

| Pattern Family [M] | Occurrences [M] | Layers Involved [M] | Cross-Layer? [D] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Difficulty [C] | Suggested Owner Layer [C] | Risk [C] |
| ---- | ----: | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| compiled expression evaluation vs compile lowering | 3 coordinated surfaces | planner scalar compile tree, compile lowering, compiled evaluator | no | yes | yes | yes | yes (`CompiledExpr::evaluate`, with lowering in `compiled_expr/compile.rs`) | high | low | keep split between compile and evaluate modules | low |
| predicate capability classification | 3 | predicate authority, runtime compilation, index predicate compile | yes | no | yes | yes | yes (`db/predicate/capability.rs`) | high | low | predicate boundary | low |
| continuation meaning transport and revalidation | 4 | cursor, planner continuation, executor continuation, session transports | yes | no | yes | yes | yes (`db/cursor/mod.rs` plus planner contract) | high | medium | cursor/planner continuation boundary | medium-low |
| route capability snapshot propagation | 4 | route capability, route contracts, load hints, aggregate hints | yes | no | yes | yes | yes (`db/executor/planning/route/capability.rs`) | high | medium | route boundary | medium |
| commit marker envelope enforcement | 2 logical owners | commit store, commit marker payload codec | yes | no | yes | yes | yes (`db/commit/store/mod.rs`) | high | low | commit store boundary | low-medium |
| index-store reader bridge and preflight overlay access | 5 | index reader trait, registry handle reader, runtime context reader, commit preflight overlay, commit preparation | yes | no | yes | yes | yes (`db/index/readers.rs` trait boundary) | high | medium | keep reader contract in index; keep overlay in mutation commit-window | low-medium |
| SQL accepted write descriptor bridge helpers | 4 | accepted runtime descriptor, SQL write staging, SQL write `RETURNING`, projection labels | no | yes | yes | partially | yes (`AcceptedRowLayoutRuntimeDescriptor` consumed by `db/session/sql/execute`) | high | low | session SQL execution boundary | low-medium |
| fluent non-paged terminal public wrappers | 1 owner family | fluent load terminal public methods and projection output shaping | no | yes | yes | no | yes (`db/query/fluent/load/terminals.rs`) | high | low | fluent load boundary | low-medium |
| SQL projection execution and response finalization shells | 1 owner family | SQL session compile contract, plan-cache binding, statement-result shaping | no | yes | yes | no | yes (`db/session/sql`) | high | low | session SQL boundary | low-medium |

## STEP 3A - Duplication-Driven Split Pressure Only

Evidence mode: `semi-mechanical`

| Module [M] | Size [M] | Duplication Families [M] | Same Owner Layer? [C] | Pressure Type [C] | Duplication Amplification [C] | Risk [C] |
| ---- | ----: | ----: | ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/session/sql/execute/write.rs` | `744` | `1` | yes | boundary-bridge growth | accepted write field admission, patch staging, insert key selection, and generated value extraction are co-located | medium-low |
| `crates/icydb-core/src/db/session/sql/execute/write_returning.rs` | `303` | `1` | yes | boundary-bridge result shaping | statement result labels and slot counts now come from accepted descriptor fields before generated value extraction | low-medium |
| `crates/icydb-core/src/db/session/sql/execute/mod.rs` | `715` | `1` | yes | execution shell repetition | cached and bypass paths still repeat prepared-plan/projection-contract shells | low-medium |
| `crates/icydb-core/src/db/session/sql/projection/labels.rs` | `145` | `1` | yes | label helper ownership | projection label helpers are small and now include accepted write descriptor labels | low |
| `crates/icydb-core/src/db/schema/runtime.rs` | `418` | `1` | yes | descriptor authority | accepted runtime descriptors remain the schema-side authority for layout fields and compatibility checks | low |
| `crates/icydb-core/src/db/data/persisted_row/types.rs` | `367` | `1` | yes | slot wrapper contract | `FieldSlot::from_validated_index` documents accepted descriptor proof before slot construction | low |
| `crates/icydb-core/src/db/executor/terminal/row_decode/mod.rs` | `512` | `1` | no | generated-compatible bridge | generated row decoders remain a bridge after accepted descriptor compatibility is proven | low-medium |
| `crates/icydb-core/src/db/query/plan/expr/compiled_expr/evaluate.rs` | `701` | `1` | yes | boundary-owned growth | evaluator remains the single runtime expression engine | medium |
| `crates/icydb-core/src/db/query/fluent/load/terminals.rs` | `1073` | `1` | yes | partially-compressed | remaining repetition is public method shape and output conversion | low-medium |

## STEP 4A - Invariant Repetition Classification

Evidence mode: `classified`

| Invariant [M] | Canonical Owner [C] | Enforcement Sites [M] | Site Roles [C] | Boundary-Protected? [C] | Sites Count [D] | Classification [C] | Divergence Risk [C] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ----: | ---- | ---- | ---- |
| scalar expression evaluation semantics | `db/query/plan/expr/compiled_expr/evaluate.rs` | `compiled_expr/evaluate.rs`; executor reader adapters | defining plus value-source adaptation | yes | 2 | Boundary-protected | low | low |
| predicate capability meaning | `db/predicate/capability.rs` | `predicate/capability.rs`; `predicate/runtime/mod.rs`; `index/predicate/compile.rs` | defining plus runtime admission plus index admission | yes | 3 | Safety-enhancing | low | low |
| continuation contract meaning | `db/cursor/mod.rs` and `db/query/plan/continuation.rs` | `cursor/mod.rs`; `cursor/spine.rs`; `query/plan/continuation.rs`; `executor/planning/continuation/scalar.rs` | defining plus validating plus transport plus defensive re-checking | yes | 4 | Safety-enhancing | medium-low | medium-low |
| route capability snapshot interpretation | `db/executor/planning/route/capability.rs` | `route/capability.rs`; `route/contracts/capabilities.rs`; `hints/load.rs`; `hints/aggregate.rs` | defining plus transport plus application | yes | 4 | Safety-enhancing | medium | medium |
| commit marker canonical envelope | `db/commit/store/mod.rs` | `commit/store/mod.rs`; `commit/marker.rs` | defining plus defensive re-checking | yes | 2 | Safety-enhancing | low-medium | low-medium |
| index-store reader authority | `db/index/readers.rs` | `index/readers.rs`; `runtime_context/index_readers.rs`; `registry/readers.rs`; `commit_window.rs`; `commit/prepare.rs` | defining plus runtime implementation plus registry implementation plus preflight overlay plus preparation consumer | yes | 5 | Safety-enhancing | low-medium | low-medium |
| accepted write descriptor authority | `db/schema/runtime.rs`; `db/session/sql/execute` | schema descriptor lookup; SQL write accepted field lookup; `RETURNING` label construction; generated-compatible guard | accepted shape plus write bridge plus result shape | partially | 4 | Boundary bridge | low-medium | low-medium |
| fluent non-paged terminal wrapper contract | `db/query/fluent/load/terminals.rs` | `with_non_paged`; typed field terminals; projection terminal families; explain terminal families | typed adaptation plus payload shaping plus explain forwarding | no | 4 | Partially consolidated | low-medium | low-medium |

## STEP 5A - Error Mapping / Construction Drift

Evidence mode: `semi-mechanical`

| Error Family [M] | Files [M] | Mapping Logic Duplication [C] | Classification Drift? [C] | Origin Drift? [C] | Same Owner Layer? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Safety Class [C] | Drift Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| projection expression evaluation failures | `db/query/plan/expr/compiled_expr/mod.rs`; `db/executor/projection/eval/scalar.rs` | low-medium | no | no | no | yes | high | boundary-sensitive | medium-low |
| commit marker corruption constructors and envelope decode guards | `db/commit/marker.rs`; `db/commit/store/mod.rs` | low | no | no | no | yes | high | protective duplication | low-medium |
| index-store reader failures | `db/index/readers.rs`; `db/executor/runtime_context/index_readers.rs`; `db/registry/readers.rs`; `db/executor/mutation/commit_window.rs` | low | no | no | no | yes | high | boundary-sensitive | low-medium |
| typed save guard failures | `db/session/mod.rs`; `db/data/persisted_row/mod.rs`; `db/metrics/sink.rs` | low | no | no | no | yes | high | boundary-sensitive | low |
| SQL accepted write descriptor failures | `db/session/sql/execute/write.rs`; `db/session/sql/execute/write_returning.rs`; `db/schema/runtime.rs` | low | no | no | yes | yes | high | safe local ownership | low-medium |

## STEP 6B - Protective Redundancy Review

Evidence mode: `classified`

| Pattern Family [M] | Files [M] | Same Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Why It Must Stay Split [C] | Behavioral Equivalence Confidence [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| compiled expression IR vs planner-to-IR compile lowering | `compiled_expr/mod.rs`; `compiled_expr/compile.rs`; `compiled_expr/evaluate.rs`; `scalar.rs` | yes | yes | yes | evaluator purity depends on keeping planner structures out of runtime evaluation while compile modules own late lowering from planner trees into resolved IR leaves | high |
| predicate capability meaning vs runtime/index application | `db/predicate/capability.rs`; `db/predicate/runtime/mod.rs`; `db/index/predicate/compile.rs` | no | yes | yes | capability ownership stays in predicate authority while runtime and index compilation consume classified results locally | high |
| cursor contract definition vs planner/runtime transports | `db/cursor/mod.rs`; `db/cursor/spine.rs`; `db/query/plan/continuation.rs`; `db/executor/planning/continuation/scalar.rs` | no | yes | yes | preserves one continuation meaning while keeping wire decode, planner validation, and executor revalidation distinct | high |
| route capability derivation vs route hint consumption | `db/executor/planning/route/capability.rs`; `db/executor/planning/route/contracts/capabilities.rs`; `db/executor/planning/route/hints/*` | no | yes | yes | keeps route-owned capability reasoning separate from hint application and downstream execution contracts | high |
| commit marker stable envelope vs payload codec | `db/commit/store/mod.rs`; `db/commit/marker.rs` | no | yes | yes | store owns the persisted trust boundary while marker owns the row-op payload shape | high |
| index reader contract vs runtime/preflight implementations | `db/index/readers.rs`; `db/executor/runtime_context/index_readers.rs`; `db/registry/readers.rs`; `db/executor/mutation/commit_window.rs` | no | yes | yes | preserves one reader contract while runtime contexts and preflight overlays implement their own visibility and staging semantics | high |
| accepted schema runtime descriptor vs generated-compatible row decoder bridge | `db/schema/runtime.rs`; `db/executor/terminal/row_decode/mod.rs`; `db/session/sql/execute/write.rs` | no | yes | yes | accepted schema owns the runtime layout, while generated codecs are only the current extraction bridge after compatibility is proven | high |
| runtime `Value` boundary adapters vs typed persisted-field slot codecs | `db/data/contract.rs`; `db/data/persisted_row/slot.rs`; schema runtime descriptors | no | yes | yes | `Value` remains a runtime boundary transport, while persisted field slot codecs stay typed and trait-owned | high |

## STEP 7B - Consolidation Candidates (Post-Protection Gate)

Evidence mode: `classified`

| Area [M] | Files [M] | Duplication Type [C] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Consolidation Safety Class [C] | Suggested Owner Layer [C] | Difficulty [C] | Drift Surface Reduction [C] | Estimated LoC Reduction [D] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- |
| SQL accepted descriptor helper compression | `db/session/sql/execute/write.rs`; `db/session/sql/execute/write_returning.rs`; `db/session/sql/projection/labels.rs` | Boilerplate duplication | yes | yes | partially | yes | safe local unification | session SQL execution boundary | low | low-medium | 8-16 | low-medium |
| route capability snapshot call-site compression | `db/executor/planning/route/hints/load.rs`; `db/executor/planning/route/hints/aggregate.rs` | Boilerplate duplication | yes | yes | partially | yes | safe local unification | route boundary | low-medium | low | 4-8 | low-medium |
| SQL projection/result shell compression | `db/session/sql/execute/mod.rs`; `db/session/sql/mod.rs` | Boilerplate duplication | yes | yes | no | yes | safe local unification | session SQL boundary | low | low | 6-12 | low-medium |

## STEP 8B - Dangerous Consolidations (Do NOT Merge)

Evidence mode: `classified`

| Area [M] | Why Duplication Is Protective [C] | Boundary-Protected? [C] | Consolidation Safety Class [C] | Risk If Merged [C] |
| ---- | ---- | ---- | ---- | ---- |
| `CompiledExpr` evaluator and planner compile lowering | evaluator purity depends on keeping planner structures out of evaluation; only compile modules should know planner expression shapes | yes | do not merge | high |
| executor row/value readers and compiled expression semantics | readers are the execution-context boundary; moving row decoding or aggregation mechanics into expression evaluation would break the single-IR layering contract | yes | do not merge | high |
| cursor contract definition and planner/runtime/session transport code | split preserves one semantic owner while keeping decode, validation, and transport boundaries explicit | yes | do not merge | high |
| route capability derivation and planner/hint consumption | split preserves route-owned capability reasoning and prevents policy re-derivation in consumers | yes | do not merge | high |
| commit marker store envelope and payload codec | split preserves the persistence trust boundary and failure classification edge | yes | do not merge | high |
| predicate capability meaning and runtime/index application | split preserves one semantic authority while keeping consumer admission policy local | yes | do not merge | medium |
| index reader contract and preflight overlay implementation | the overlay must remain commit-window local because it observes staged mutations that the normal runtime/registry readers must not see | yes | do not merge | medium-high |
| accepted runtime schema descriptor and generated row decode bridge | current generated codecs are compatibility-guarded extraction adapters; merging them into accepted descriptor authority would hide the remaining schema part-two work | yes | do not merge | medium-high |
| runtime `Value` adapters and persisted-field slot codecs | runtime boundary conversion is descriptor validated; typed persistence codecs must not regain dynamic `Value` persistence | yes | do not merge | high |

## STEP 9 - Quantitative Summary + High-Risk Ledger

Evidence mode: `semi-mechanical`

| Metric [M/C/D] | Previous [M/C/D] | Current [M/C/D] | Delta [D] | Interpretation [C] |
| ---- | ---- | ---- | ---- | ---- |
| total duplication patterns found | `8` | `9` | `+1` | the SQL accepted descriptor bridge is now visible as same-owner helper repetition, not as competing storage policy |
| total high-risk divergence patterns | `0` | `0` | `0` | no production duplicate evaluator or cross-layer policy seam reopened |
| same-layer accidental duplication count | `0` | `0` | `0` | remaining same-owner targets are boilerplate wrappers rather than semantic duplication |
| cross-layer intentional duplication count | `3` | `3` | `0` | predicate, continuation, and route duplication remain intentionally boundary-protective |
| defensive duplication count | `1` | `1` | `0` | commit marker envelope checks remain intentionally duplicated at the store/payload boundary |
| boundary-protected duplication count | `6` | `7` | `+1` | accepted descriptor/generation bridge is now explicitly protected by generated-compatible validation |
| invariants with `>3` enforcement sites | `5` | `6` | `+1` | accepted write descriptor authority joins continuation, route, index reader, fluent wrapper, and prepared parameter admission families |
| error-construction families with `>3` custom mappings | `1` | `1` | `0` | projection evaluation mapping remains the main custom mapping family |
| drift surface reduction estimate | `medium-low` | `medium-low` | stable | current pressure is local wrapper/bridge repetition, not divergent semantics |
| estimated LoC reduction range (conservative) | `14-32` | `18-36` | `+4` | one more small helper-compression candidate is available after SQL accepted descriptor closeout |

High-risk ledger:

| Pattern [M] | Primary Locations [M] | Owner Boundary [C] | Canonical Owner Known? [C] | Worth Fixing This Cycle? [C] | Consolidation Safety Class [C] | Rationale [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| none | n/a | n/a | n/a | n/a | n/a | production scalar, grouped, HAVING, ordering, aggregate terminal, SQL write, and runtime persistence boundary paths remain under a single visible authority per contract |

## STEP 9A - Analyst Verification Readout

Evidence mode: `semi-mechanical`

| Verification Metric [M/C/D] | Count [D] | Definition [M/C] |
| ---- | ----: | ---- |
| mechanical findings count | 9 | STEP 1A rows backed by direct pattern/file anchors |
| classified findings count | 34 | STEP 2A + STEP 4A + STEP 5A + STEP 6B + STEP 7B + STEP 8B rows requiring owner/safety judgment |
| high-confidence candidate count | 3 | STEP 7B candidates with high behavioral-equivalence confidence and safe local unification |
| boundary-protected findings count | 15 | rows where `Boundary-Protected? = yes` or `partially` across Steps 1A/4A/6B/8B |

## 1. Run Metadata + Comparability Note

- `DRY-1.2` method manifest applied; this run is method-comparable to the 2026-05-03 baseline.
- The scan used clean committed snapshot `13ec2bef4`, so conclusions are not dirty-tree qualified.
- `0.147.4` moves SQL structural patch slot selection and SQL write `RETURNING` all-column shapes onto accepted descriptor fields before generated codec handoff. That is a DRY improvement because the generated model is now a compatibility-guarded bridge, not a second shape authority for those write surfaces.

## 2. Mode A Summary: High-Impact Consolidation Opportunities

- No high-risk production DRY seam is open.
- `db/executor/projection/eval/scalar.rs` still adapts value sources into `CompiledExprValueReader` and calls `CompiledExpr::evaluate`.
- `db/query/plan/expr/compiled_expr/evaluate.rs` remains the single runtime expression engine; `db/query/plan/expr/compiled_expr/compile.rs` remains the planner-to-IR lowering boundary.
- SQL write descriptor work is healthier than the previous baseline: accepted descriptor fields now select write slots and `RETURNING` labels, while generated codecs only extract values after generated-compatible validation.

## 3. Mode A Summary: Medium Opportunities

- No new medium-risk consolidation candidate was found.
- The largest visible growth area is `db/session/sql/execute/write.rs`, but the repetition is local bridge code around accepted descriptor fields, structural patch staging, and generated-compatible extraction.
- The accepted descriptor bridge should be compressed only inside the SQL execution boundary; do not collapse it into schema runtime descriptors or generated row decoding while schema part two is still open.

## 4. Mode A Summary: Low/Cosmetic Opportunities

- SQL accepted descriptor helpers can probably share a small local helper for descriptor field lookup, validated slot creation, and label extraction.
- Route hint helpers in `db/executor/planning/route/hints/load.rs` and `aggregate.rs` still repeat small same-owner bounded-fetch plumbing.
- SQL projection/result shell repetition remains a small local cleanup candidate, though `0.147.4` reduced write-result shape drift.
- Fluent non-paged terminal public wrappers remain local boilerplate.

## 5. Mode B Summary: Protective Redundancies (Keep Separate)

- Keep `CompiledExpr` evaluator purity separate from planner compile lowering.
- Keep executor readers separate from compiled-expression semantics.
- Keep cursor contract definition separate from planner/runtime/session transport code.
- Keep route capability derivation separate from hint consumers.
- Keep commit marker store-envelope checks separate from marker payload codec.
- Keep index reader contracts separate from runtime/preflight implementations.
- Keep accepted schema runtime descriptors separate from generated-compatible row decoder extraction until accepted field decode owns payload materialization.
- Keep runtime `Value` boundary adapters separate from typed persisted-field slot codecs.

## 6. Dangerous Consolidations (Do Not Merge)

- Do not move planner expression types into `compiled_expr/evaluate.rs`.
- Do not move executor row decoding, grouped row state, projection materialization, or aggregation mechanics into compiled-expression evaluation.
- Do not merge cursor semantic ownership into planner/runtime/session transport code.
- Do not merge route capability derivation into hint or execution consumers.
- Do not merge commit store envelope validation into payload codec logic.
- Do not collapse `PreflightStoreOverlay` into the normal runtime/registry index readers.
- Do not make generated codecs the accepted layout authority.
- Do not make runtime `Value` persistable again through slot codecs.

## 7. Quantitative Summary

- patterns found: `9`
- high-risk divergence patterns: `0`
- boundary-protected patterns: `7`
- drift surface reduction estimate: `medium-low`
- conservative LoC reduction: `18-36`
- DRY risk index: `3.7/10`

## 8. Validation Commands

- `rg "ScalarProjectionExpr|eval_scalar_projection_expr|eval_canonical_scalar_projection_expr|ScalarProjectionExpr::|match expr" crates/icydb-core/src/db/executor -n`
- `rg "GroupedCompiledExpr|GroupedProjectionExpr|eval_grouped_projection_expr|eval_binary_expr|eval_unary_expr|projection/eval/operators|ScalarProjectionExpr" crates/icydb-core/src/db -g '!**/tests/**' -g '!**/tests.rs' -n`
- `rg "\.evaluate\(" crates/icydb-core/src/db/executor crates/icydb-core/src/db/query/plan/expr -g '!**/tests/**' -g '!**/tests.rs' -n`
- `bash scripts/ci/check-layer-authority-invariants.sh`
- `scripts/audit/runtime_metrics.sh`
- `cargo check -p icydb-core --tests --features sql`

## 9. Architectural Risk Summary

The current tree is DRY-stable. The main previous high-risk concern, duplicate runtime expression evaluation, remains closed. The latest SQL write work improves authority shape by pushing write slots and all-column `RETURNING` labels through accepted descriptors before generated extraction. What remains is mostly local bridge boilerplate and public wrapper repetition.

The best next cleanup, if desired, is a small SQL execution helper pass around accepted descriptor field lookup and label/slot construction. That cleanup is optional and should stay inside `db/session/sql/execute`; it should not merge schema runtime descriptors, generated row decoders, or runtime `Value` adapters.
