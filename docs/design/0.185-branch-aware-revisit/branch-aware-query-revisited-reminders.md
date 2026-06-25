# 0.185 Branch-Aware Query Revisited Reminders

This started as a parking note. The active 0.185 baseline is now documented in
`0.185-design.md` and `status.md`.

## Reminder List

- Revisit branch-set route representation after the 0.184 audit cleanup.
- Confirm SQL and fluent both feed the same branch-aware planner path.
- Add continuation and cursor design for branch merges.
- Add cost or adaptive route choice for small versus large `IN` predicates.
- Expand perf matrix coverage around branch-heavy fluent and SQL queries.
- Decide whether shared branch-tree machinery should replace any remaining
  special-case `IN` flows.
