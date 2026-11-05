> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Focused Audit - Query Export Wasm Footprint (2026-04-03)

## Scope

- target canister: `minimal`

- profile: `wasm-release`

- SQL variant: `sql-on`

- primary question: where is the fixed wasm cost for the main query types now that per-entity growth is no longer the dominant problem?

### 5. Public SQL Result and Rendering Surface

Primary owner:

- `/home/adam/projects/icydb/crates/icydb/src/db/sql/mod.rs`

Current size signal:

- `574` source lines

This module is thinner than before, but the public result type is still one wide union:

- `Projection`

- `Explain`

- `Describe`

- `ShowIndexes`

- `ShowColumns`

- `ShowEntities`

And the same root retains all of the render helpers for those variants.

This is legitimate facade work, but it means the single public query export still pulls projection, explain, describe, and show rendering into one retained result family.

### Candidate 3. Reduce The Number Of Intermediate Row/Result Shapes

Highest-value files:

- `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/projection/payload.rs`

- `/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/surface/route.rs`

- `/home/adam/projects/icydb/crates/icydb/src/db/sql/mod.rs`

Current problem:

The query root still carries multiple adjacent wrappers for roughly the same projection payload:

- `SqlProjectionPayload`

- `SqlDispatchResult::Projection`

- `SqlQueryRowsOutput`

- `SqlProjectionRows`

That is not necessarily large by itself, but it is one of the few places where the generated query root still obviously does repeated packaging for the same result family.

Best target shape:

- one canonical structural row payload from core to facade

## Recommended Order

1. Collapse generated query routing and route-family ownership first.
2. Then collapse dual generated-query lowering/dispatch paths.
3. Only then look at row/result wrapper reduction.

This order matters because the first two are much more likely to reduce the retained `query(sql)` code root by tens of KB than string cleanup or helper reshuffling.

## Verification Readout

- `WASM_CANISTER_NAME=minimal WASM_PROFILE=wasm-release WASM_SQL_VARIANTS=sql-on bash scripts/ci/wasm-size-report.sh`

- `twiggy top -n 30 artifacts/wasm-size/minimal.wasm-release.dfx-shrunk.wasm`

- `twiggy dominators artifacts/wasm-size/minimal.wasm-release.dfx-shrunk.wasm`

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/04/03/wasm-query-audit/01/report.md).
