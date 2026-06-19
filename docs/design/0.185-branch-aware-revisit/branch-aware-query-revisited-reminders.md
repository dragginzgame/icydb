# 0.185 Branch-Aware Query Revisited Reminders

This is a parking note, not a full design doc.

## Reminder List

- Revisit branch-set route representation after the 0.184 audit cleanup.
- Confirm SQL and fluent both feed the same branch-aware planner path.
- Add continuation and cursor design for branch merges.
- Add cost or adaptive route choice for small versus large `IN` predicates.
- Expand perf matrix coverage around branch-heavy fluent and SQL queries.
- Decide whether shared branch-tree machinery should replace any remaining
  special-case `IN` flows.
