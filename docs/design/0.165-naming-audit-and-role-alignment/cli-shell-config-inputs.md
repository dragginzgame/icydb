# CLI Shell Config Input Naming

## Status

Accepted.

## Family

CLI SQL shell test-support config inputs.

## Problem

The CLI SQL shell test-support seam exposed `SqlConfigParts` and
`sql_config_parts(...)` even though the helper returns the concrete shell config
inputs parsed from `SqlArgs`: canister, environment, history file, and optional
SQL text.

Under the 0.165 naming policy, `Parts` is acceptable only for temporary
construction decomposition, not for stable test-support helpers that expose
named command inputs.

## Accepted Renames

```text
SqlConfigParts -> SqlShellConfigInputs
sql_config_parts(...) -> sql_shell_config_inputs(...)
CLI shell config parts tests -> shell config input tests
```

## Kept Names

- `ShellConfig` remains the production configuration owner.
- `SqlArgs` remains the parsed CLI argument DTO.
- Tuple destructuring remains local to tests because the helper is test-only
  and exists only to inspect parsed shell config inputs.

## Old-Vocabulary Scan Terms

```text
SqlConfigParts|sql_config_parts|shell config parts|config parts
```
