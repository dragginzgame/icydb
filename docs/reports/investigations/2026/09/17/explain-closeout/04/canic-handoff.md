# ICYDB-033 — bounded Canic attribution handoff

Ready-to-queue task for the Canic owner; not executed by this IcyDB change.
Toko Miner and Canic were inspected read-only on 2026-09-17. The installed
composition audit now names IcyDB 0.257.21, but its retained empty-pair receipt
measures 0.257.9. Updating the dependency alone is not new measurement evidence.

## Task

Extend the existing `canisters/audit/icydb_composed` subjects and
`crates/canic-host/examples/icydb_composed_audit.rs`, not a second composition
or measurement framework. Keep Toko Miner read-only, SQL off, metrics on,
Canic's existing synchronous participant owner, and the same store/memory profile.

Freeze one exact Canic source snapshot, one published IcyDB version, one
resolved dependency graph, Rust 1.98.1 and the canonical post-link toolchain.
Do not bump production pins or compare subjects built from changing worktrees.

Capture these matched subjects:

1. Existing host-only and empty metrics participant controls.
2. One generated entity with binding only.
3. The same entity with one fixed typed page query.
4. The same entity with query and typed insert reachable.
5. Ten matching entities with the same reachable operations.

Report raw Wasm bytes first, code/data section bytes and defined function count;
gzip is secondary. Separate fixed participant cost, binding/query/write
reachability and the one-to-ten entity increment. Retain hashes, features,
profile, lockfile and source identity with the result. No wall-clock metric.
Use named diagnostic artifacts only to identify concrete owners; do not add
overlapping retained sizes or call them removable byte budgets.

Keep ordinary schema, relation, lifecycle and typed-input semantics intact.
Do not remove recovery, metrics, entity types or database ownership to improve
the result. Do not repeat the rejected startup outlining experiment.
This outcome is attribution, not permission for unrelated runtime optimization.

State explicitly that these controlled subjects are not the complete Toko Miner
application. Finishing ICYDB-033 still requires either matched application
qualification or an explicit narrowing of its acceptance. Return the smallest
demonstrated owner-specific reduction candidate, or report that none is proven.
