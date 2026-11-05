> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Complexity / Surface Hygiene Audit - icydb-schema + derive - 2026-05-21

## Runtime Authority Drift Findings

| Area [C] | Runtime Authority [C] | Alternate Authority Found? [C] | Evidence [M/C] | Allowed Role? [C] | Finding [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| schema acceptance | accepted runtime schema snapshots | No | schema crate builds proposal-time nodes; core owns accepted schema runtime | yes | no runtime fallback authority in schema crate | Low |
| generated models | proposal/codegen metadata only | No | derive emits `EntityModel`/`IndexModel`; no schema crate runtime reconstruction path found | yes | generated model use is macro/proposal boundary, not runtime authority | Low |
| stable memory declarations | `ic-memory` runtime validation plus generated declarations | Partial | schema/derive still locally validate memory IDs and stable-key syntax; build emits `ic_memory_*` declarations | yes, but duplicated | early diagnostics remain useful, but local copies should be centralized or guarded against `ic-memory` behavior drift | Medium |
| Canic coupling | Canic/CDK runtime lifecycle outside schema | Partial | `icydb-schema` imports `canic_cdk::utils::time::now_secs` only for schema timestamp | unclear | schema crate still has a Canic dependency unrelated to memory declarations | Medium |
| SQL DDL | catalog-native mutation in core | No | schema/derive have no SQL DDL lowering code | yes | no drift found | Low |
| endpoint names | generated `__icydb_*` endpoint names | No | schema/derive do not expose endpoint override metadata | yes | no endpoint-name shim surface | Low |
| persisted decoding | core codecs remain bounded/fallible | No | derive calls core `__macro` encode/decode helpers for generated defaults/codecs | yes | no ad hoc persisted decoder found in schema crates | Low |

## Facade / Generated-Boundary Findings

| Surface [M] | Boundary Type [C] | Generated Consumer Evidence [M/C] | Could Narrow? [C] | Required Replacement [C] | Deletion Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| `schema::node::*` | generated code target | derive emits concrete `::icydb::schema::node::*` constructors | no broad narrowing without derive rewrite | none | low | High |
| `schema::build::schema_write` | generated registration | derive emits `schema_write().insert_node(...)` | no | none | low | High |
| `schema::build::get_schema` | build codegen input | `icydb-build` calls `get_schema()` | no | none | low | High |
| derive proc-macro functions | user macro API | public attribute macros | no | none | low | High |
| derive `node::*` / `imp::*` internal facades | proc-macro implementation convenience | crate-local prelude and generator modules consume broad re-exports | yes | narrower crate-local imports by owner | medium | Medium |
| memory policy helpers | schema/derive policy helpers | store/canister validation and tests | yes | shared policy module or parity tests | medium | Medium |
| generated `EntityModel` / `IndexModel` quote output | generated proposal metadata | generated entity/index model constants | no for current architecture | accepted schema reconciliation still needs proposal metadata | low | High |
| `schema::visit::Visitor` | possible schema traversal facade | schema nodes implement traversal through trait | unclear | owner decision on public traversal API | blocked | Medium |

## Follow-Up Actions

1. Memory policy owner: either centralize derive/schema memory ID and stable-key validation around one shared policy source, or add explicit parity guards against `ic-memory` declaration validation.
2. Stable key ABI: add a parity test proving derive `COMMIT_STABLE_KEY` generation matches schema `Canister::commit_stable_key()` for the same namespace.
3. Dependency surface: decide whether `icydb-schema` still needs `canic-cdk` for `now_secs()`; if not, replace it with a schema/local time helper and remove that dependency edge.
4. Schema facade narrowing: make `schema::validate` private and remove/narrow unused schema graph helpers if no public facade owner claims them.
5. Visitor narrowing: keep `Visitor` public only if direct schema traversal is intended; make the concrete `ValidateVisitor` crate-private.
6. Derive modularity: split `node/field.rs` by field parsing, Rust defaults, and database-default encoding in a dedicated behavior-preserving slice if 0.160/0.161 continues schema-default work.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/05/21/complexity-surface-hygiene-icydb-schema/01/report.md).
