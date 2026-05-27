# SQL Semantic Compiler Naming

## Status

Accepted.

## Family

SQL compile-stage semantic compiler.

## Problem

The SQL compile stage used a private `core` module and
`compile_sql_statement_core(...)` helper for cache-independent statement
compilation. The module does not own a generic SQL compile core. It validates a
prepared surface and lowers parsed SQL statements into session-owned semantic
compile artifacts.

Under the 0.165 naming policy, `Core` is kept only for genuine invariant
payloads. This owner is better named after the semantic compiler role it
performs.

## Accepted Renames

```text
session::sql::compile::core -> session::sql::compile::semantic_compiler
compile_sql_statement_core(...) -> compile_sql_statement_semantic_artifacts(...)
compile core artifact comments -> semantic artifact comments
```

## Kept Names

- `SqlCompileArtifacts` remains accurate because it names the complete command
  artifact plus stage-local compile counters.
- `SqlQueryShape` remains accurate because it records query-shape facts used by
  command dispatch and attribution.
- `compile_sql_statement_measured(...)` remains accurate because it owns the
  attribution wrapper around the semantic compiler.

## Old-Vocabulary Scan Terms

```text
session::sql::compile::core|compile::core|compile_sql_statement_core|core artifact
```
