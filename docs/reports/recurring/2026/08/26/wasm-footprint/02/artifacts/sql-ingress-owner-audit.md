# SQL Ingress Owner Audit

## Scope and provenance

This is the evidence artifact for IcyDB 0.246 Patch 1. It audits the exact
released predecessor `v0.245.0` at revision
`b6b5c287de5d3fcca3fc6dbe9acbe7c4026236b9` and tree
`3d53872ddf71791bee02695e983f767c7049f51a`. All production size reports record
`source_dirty=false`, the same lockfile, Rust 1.97.1, `ic-wasm` 0.11.1 and
Binaryen 108 `-Oz`.

## Final production artifacts

| Actor | Raw Wasm | Gzip | Functions | Candid hash prefix | Exports |
| --- | ---: | ---: | ---: | --- | ---: |
| Typed direct lookup | 2,010,198 | 780,618 | 5,140 | `1005c63f` | 6 |
| Dynamic structural query | 2,864,615 | 1,137,805 | 7,502 | `14ff87ea` | 10 |
| SQL query | 3,179,832 | 1,274,179 | 8,211 | `a22d2bfd` | 6 |

| Directional comparison | Raw delta | Interpretation |
| --- | ---: | --- |
| SQL minus typed | +1,169,634 (+58.19%) | SQL frontend plus the general structural planner/executor versus a planner-free `Database::get` |
| Dynamic minus typed | +854,417 (+42.50%) | General structural query engine plus four measurement endpoints and diagnostics |
| SQL minus dynamic | +315,217 (+11.00%) | Closer SQL frontend/surface signal, still unmatched because endpoints and contracts differ |

The typed and SQL actors use the same dependency packages. SQL adds the
existing `icydb/sql -> icydb-core/sql` feature chain; it does not introduce an
SQL-only third-party dependency.

## Named ownership evidence

The symbol-bearing artifacts are diagnostic and larger than the deployable
artifacts. Shallow module-family sums and retained dominators overlap and are
not additive.

| Owner signal | Typed | Dynamic | SQL |
| --- | ---: | ---: | ---: |
| Named artifact | 3,021,178 | 4,300,137 | 4,757,027 |
| Public query export retained | 126,775 | not used as a matched owner | 1,193,817 |
| Parser shallow family | 0 | 0 | 49,539 |
| Semantic lowering shallow family | 0 | 0 | 77,132 |
| Executor shallow family | 831 | 351,653 | 492,774 |
| Query-plan shallow family | 772 | 166,912 | 209,878 |
| Predicate shallow family | 38,267 | 52,694 | 54,068 |

Additional SQL dominators:

| Symbol / family | Retained bytes | Meaning |
| --- | ---: | --- |
| Trusted SQL query with entity routing | 1,157,551 | Full downstream request closure |
| SQL projection inner | 156,746 | SQL-shaped result materialization and dependencies |
| Compiled SQL query execution | 129,310 | Immutable compiled-command execution closure |
| Shared grouped plan | 92,969 | Maintained aggregate/group execution |
| Shared grouped fold | 82,299 | Maintained aggregate/group execution |
| Access-stream paths | approximately 55,000–60,000 | Shared physical index/row access |
| `parse_sql_with_attribution` | 37,345 | Required complete parser and dependencies |
| Parser statement dispatch | 35,960 | Required current grammar dispatch |
| Select-shape lowering | 35,236 | Required accepted-schema semantic lowering |

Mutation lowering symbols visible in the query actor total only about 1,123
shallow named bytes. The parser must recognize state-changing statements so the
query surface can reject them with the maintained typed error. A separate
query-only parser would duplicate grammar ownership and does not have a 32 KiB
removable ceiling.

## Owner and authority chain

| Stage | Owner | Input | Output / authority rule |
| --- | --- | --- | --- |
| Endpoint | audit canister / generated actor | authored SQL string | trusted session call; no schema authority |
| Routing parse | SQL surface parser | SQL string | entity name only; cannot establish acceptance |
| Catalogue selection | pinned request/session | entity name | accepted catalogue context from current runtime authority |
| Compile cache | session SQL | SQL text, surface and catalogue fingerprint | one `CompiledSqlCommand`; a hit cannot establish freshness |
| Semantic compilation | SQL compiler | AST and accepted schema info | immutable resolved command |
| Execution context | session SQL | command, catalogue and accepted authority | binds execution to the pinned accepted snapshot |
| Planning/execution | shared structural engine | resolved command and request budgets | physical rows/aggregate; no SQL reparsing or schema reselection |
| Projection | SQL result owner | physical result | canonical SQL response values |

This chain already converges on the shared structural planner/executor after
SQL-specific parsing and semantic resolution. There is one cached compiled
artifact and one accepted-schema authority chain. No parallel schema,
preparation or execution authority was found.

## Repeated parse and attribution gap

The query miss path is:

```text
execute_trusted_sql_query_with_entity_name
  -> sql_statement_entity_name
     -> parse_sql_with_attribution                 # routing parse
  -> compile_sql_query_with_execution_context
     -> compiled-command cache lookup
     -> parse_sql_with_attribution on miss         # compilation parse
     -> semantic compilation
  -> compiled execution
```

The cache-hit path still performs the routing parse. The diagnostics facade
also calls `sql_statement_entity_name` before the core attribution method,
which calls it again. Existing comments describe the returned entity as coming
from the same canonical parse used for compilation, but only the parser
implementation is shared; the AST value is not.

The core compile timer starts after its routing parse, and the outer facade
starts after its additional parse. Reported compile and total instructions
therefore omit real frontend work.

A focused exact-predecessor run of
`sql_perf_shared_floor_queries_report_phase_breakdown` passed. On cold
cache-miss cases it measured:

| Query family | Counted compile | Counted parse | Execute | Reported total |
| --- | ---: | ---: | ---: | ---: |
| Primary-key key-only | 375,057 | 21,936 | 374,571 | 749,628 |
| Primary-key ordered limit 1 | 381,370 | 24,694 | 600,049 | 981,419 |
| Lower-text ordered limit 3 | 402,103 | 36,250 | not isolated here | 1,420,324 |
| Grouped count | 418,883 | 29,776 | not isolated here | 1,890,062 |
| `IN` predicate | 440,902 | 42,079 | not isolated here | 1,546,336 |
| `NOT IN` predicate | 435,330 | 36,861 | not isolated here | 1,573,476 |

The uncounted routing parse invokes the same parser, so similar order of cost is
plausible, but it was not separately instrumented and no exact saving is
claimed. The finding is sufficient to reject current attribution totals as a
complete SQL frontend measurement.

## Removable-cost and complexity decision

Parse-once would keep the parser, AST, semantic compiler and compiled command.
It could remove only repeat invocation and adapter/control work. The entire
required parser dominator is 37,345 named bytes, so removing the second call
cannot conservatively prove 32 KiB of final raw savings. The routing helper and
visible adapters are much smaller still.

Narrowing SQL execution to the audit actor's point lookup would remove
supported aggregate, grouping, ordering, explain and introspection behavior or
introduce a second runtime mode. Type erasure or dynamic dispatch would trade
code size for allocation, indirect calls and instruction risk. Neither is a
valid converged-flow cleanup.

Verdict: `NO-BUILD — SQL PREMIUM IS CURRENTLY JUSTIFIED`.

The repeated parse should be considered only as a separately bounded broad
performance and measurement-correctness slice. Its least-complex shape would
move one already parsed statement through the existing catalogue-selection and
compile-cache flow. A nested AST cache, separate prepared route, runtime mode
or alternative authority is out of scope.
