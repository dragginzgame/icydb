# UI Architecture Guards

This file maps each UI test to exactly one architecture rule.

## Compile-Fail Guards (`tests/ui/**/*.rs`)

| Test | Guarded Rule |
| --- | --- |
| `db/access_module_private.rs` | `db::access` is internal and must not be imported via deep paths. |
| `db/codec_module_private.rs` | `db::codec` decode/encoding internals are not public API. |
| `db/commit_module_private.rs` | `db::commit` recovery/marker internals are not public API. |
| `db/contracts_module_private.rs` | `db::contracts` planner/executor contract internals remain private. |
| `db/cursor_module_private.rs` | `db::cursor` continuation/token internals are not public API. |
| `db/canonical_row_not_public.rs` | `CanonicalRow` stays internal so arbitrary callers cannot acquire row-write capability. |
| `db/diagnostics_module_private.rs` | `db::diagnostics` module internals are not imported directly; use root re-exports. |
| `db/executor/aggregate_terminals_module_private.rs` | Executor aggregate terminal internals are not public API. |
| `db/executor/grouped_budget_observability_module_private.rs` | Grouped budget observability internals are not publicly importable. |
| `db/executor/grouped_route_observability_module_private.rs` | Grouped route observability internals are not publicly importable. |
| `db/executor/cannot_import_query_plan_planner.rs` | Executor boundary must not import planner internals (`db::query::plan::planner`). |
| `db/executor/cannot_import_query_intent.rs` | Executor boundary must not import query intent internals (`db::query::intent`). |
| `db/executor/kernel_module_private.rs` | `db::executor::kernel` internals are not public API. |
| `db/executor/load_module_private.rs` | `db::executor::load` internals are not public API. |
| `db/index_module_private.rs` | `db::index` internals are not imported directly. |
| `db/logical_plan_not_reexported.rs` | Internal planner symbol `LogicalPlan` must not be re-exported at `db` root. |
| `db/registry_module_private.rs` | `db::registry` internals are not imported directly. |
| `db/response/cardinality_methods_require_extension_trait.rs` | Cardinality helpers require explicit `ResponseCardinalityExt` import. |
| `db/response_alias_removed.rs` | Removed alias `db::Response` must stay absent. |
| `db/response_module_private.rs` | `db::response` module stays private; callers use root response types. |
| `db/session_module_private.rs` | `db::session` module internals are private; callers use `db::DbSession`. |
| `db/store_module_private.rs` | `db::data` store internals are not public API. |
| `db/trace_module_absent.rs` | Removed `db::trace` path must remain absent after trace move to diagnostics. |
| `query/grouped/handoff_module_private.rs` | Grouped handoff internals are not publicly importable. |
| `query/logical_plan_private.rs` | Query logical plan internals are not public via deep paths. |
| `query/plan_module_private.rs` | Query plan internals remain private; only root re-exports are public. |
| `visitor/sanitize_module_private.rs` | Visitor sanitize internals stay private. |
| `visitor/validate_module_private.rs` | Visitor validate internals stay private. |

## Compile-Pass Guards (`tests/pass/**/*.rs`)

| Test | Allowed Path Contract |
| --- | --- |
| `db/entity_response_root_import.rs` | `EntityResponse` is available via `db` root public API. |
| `db/execution_trace_root_import.rs` | `ExecutionTrace` diagnostics surface is available via `db` root re-exports. |
| `db/session_root_import.rs` | `DbSession` is available via `db` root public API. |
