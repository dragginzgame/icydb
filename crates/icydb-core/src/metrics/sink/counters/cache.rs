//! Query-plan and compiled-SQL cache counter mutation helpers.
//! Does not own cache identity, storage, or metrics event dispatch.

use crate::metrics::{
    sink::{CacheKind, CacheMissReason, CacheOutcome},
    state as metrics,
};

// Cache counters are intentionally cache-family specific and outcome specific
// so the report can distinguish a cold cache from a warmed cache that inserts
// successfully after misses.
#[remain::check]
pub(in crate::metrics::sink) const fn record_global_cache_outcome(
    ops: &mut metrics::EventOps,
    kind: CacheKind,
    outcome: CacheOutcome,
) {
    #[remain::sorted]
    match kind {
        CacheKind::SharedQueryPlan => {
            record_global_shared_query_plan_cache_outcome(ops, outcome);
        }
        CacheKind::SqlCompiledCommand => {
            record_global_sql_compiled_command_cache_outcome(ops, outcome);
        }
    }
}

// Shared query-plan cache outcomes update only the query-plan cache family so
// cache dashboards can distinguish planner reuse from SQL command reuse.
#[remain::check]
pub(in crate::metrics::sink) const fn record_global_shared_query_plan_cache_outcome(
    ops: &mut metrics::EventOps,
    outcome: CacheOutcome,
) {
    #[remain::sorted]
    match outcome {
        CacheOutcome::Hit => {
            ops.cache_shared_query_plan_hits = ops.cache_shared_query_plan_hits.saturating_add(1);
        }
        CacheOutcome::Insert => {
            ops.cache_shared_query_plan_inserts =
                ops.cache_shared_query_plan_inserts.saturating_add(1);
        }
        CacheOutcome::Miss => {
            ops.cache_shared_query_plan_misses =
                ops.cache_shared_query_plan_misses.saturating_add(1);
        }
    }
}

// SQL compiled-command cache outcomes update only the SQL cache family so the
// same hit/miss vocabulary remains separated by cache owner.
#[remain::check]
pub(in crate::metrics::sink) const fn record_global_sql_compiled_command_cache_outcome(
    ops: &mut metrics::EventOps,
    outcome: CacheOutcome,
) {
    #[remain::sorted]
    match outcome {
        CacheOutcome::Hit => {
            ops.cache_sql_compiled_command_hits =
                ops.cache_sql_compiled_command_hits.saturating_add(1);
        }
        CacheOutcome::Insert => {
            ops.cache_sql_compiled_command_inserts =
                ops.cache_sql_compiled_command_inserts.saturating_add(1);
        }
        CacheOutcome::Miss => {
            ops.cache_sql_compiled_command_misses =
                ops.cache_sql_compiled_command_misses.saturating_add(1);
        }
    }
}

// Cache size is a gauge for the current scope, not an event count. Cache owners
// refresh it after lookups and insertions so the metrics report can show memory
// pressure alongside reuse outcomes.
#[remain::check]
pub(in crate::metrics::sink) const fn record_global_cache_entries(
    ops: &mut metrics::EventOps,
    kind: CacheKind,
    entries: u64,
) {
    #[remain::sorted]
    match kind {
        CacheKind::SharedQueryPlan => {
            ops.cache_shared_query_plan_entries = entries;
        }
        CacheKind::SqlCompiledCommand => {
            ops.cache_sql_compiled_command_entries = entries;
        }
    }
}

// Cache miss reasons are scoped below the coarse miss counter. They explain
// whether misses are healthy first-contact behavior or drift in one identity
// dimension without expanding labels by query text or schema fingerprint.
#[remain::check]
pub(in crate::metrics::sink) const fn record_global_cache_miss_reason(
    ops: &mut metrics::EventOps,
    kind: CacheKind,
    reason: CacheMissReason,
) {
    #[remain::sorted]
    match kind {
        CacheKind::SharedQueryPlan => {
            record_global_shared_query_plan_miss_reason(ops, reason);
        }
        CacheKind::SqlCompiledCommand => {
            record_global_sql_compiled_command_miss_reason(ops, reason);
        }
    }
}

// Shared query-plan cache misses cannot vary by SQL surface. If that impossible
// reason reaches this boundary, fold it into the distinct-key bucket rather than
// creating a nonsensical public counter.
#[remain::check]
pub(in crate::metrics::sink) const fn record_global_shared_query_plan_miss_reason(
    ops: &mut metrics::EventOps,
    reason: CacheMissReason,
) {
    #[remain::sorted]
    match reason {
        CacheMissReason::Cold => {
            ops.cache_shared_query_plan_miss_cold =
                ops.cache_shared_query_plan_miss_cold.saturating_add(1);
        }
        CacheMissReason::DistinctKey | CacheMissReason::Surface => {
            ops.cache_shared_query_plan_miss_distinct_key = ops
                .cache_shared_query_plan_miss_distinct_key
                .saturating_add(1);
        }
        CacheMissReason::SchemaFingerprint | CacheMissReason::SchemaVersion => {
            ops.cache_shared_query_plan_miss_schema_fingerprint = ops
                .cache_shared_query_plan_miss_schema_fingerprint
                .saturating_add(1);
        }
        CacheMissReason::Visibility => {
            ops.cache_shared_query_plan_miss_visibility = ops
                .cache_shared_query_plan_miss_visibility
                .saturating_add(1);
        }
    }
}

// SQL compiled-command cache misses cannot vary by planner visibility. Fold
// that impossible reason into distinct-key so the public report stays aligned
// with the cache family's real identity dimensions.
#[remain::check]
pub(in crate::metrics::sink) const fn record_global_sql_compiled_command_miss_reason(
    ops: &mut metrics::EventOps,
    reason: CacheMissReason,
) {
    #[remain::sorted]
    match reason {
        CacheMissReason::Cold => {
            ops.cache_sql_compiled_command_miss_cold =
                ops.cache_sql_compiled_command_miss_cold.saturating_add(1);
        }
        CacheMissReason::DistinctKey | CacheMissReason::Visibility => {
            ops.cache_sql_compiled_command_miss_distinct_key = ops
                .cache_sql_compiled_command_miss_distinct_key
                .saturating_add(1);
        }
        CacheMissReason::SchemaFingerprint | CacheMissReason::SchemaVersion => {
            ops.cache_sql_compiled_command_miss_schema_fingerprint = ops
                .cache_sql_compiled_command_miss_schema_fingerprint
                .saturating_add(1);
        }
        CacheMissReason::Surface => {
            ops.cache_sql_compiled_command_miss_surface = ops
                .cache_sql_compiled_command_miss_surface
                .saturating_add(1);
        }
    }
}

// Mirror cache activity to the owning entity so global cache movement can be
// traced back to the model whose schema/query identity produced it.
#[remain::check]
pub(in crate::metrics::sink) const fn record_entity_cache_outcome(
    ops: &mut metrics::EntityCounters,
    kind: CacheKind,
    outcome: CacheOutcome,
) {
    #[remain::sorted]
    match kind {
        CacheKind::SharedQueryPlan => {
            record_entity_shared_query_plan_cache_outcome(ops, outcome);
        }
        CacheKind::SqlCompiledCommand => {
            record_entity_sql_compiled_command_cache_outcome(ops, outcome);
        }
    }
}

// Entity-scoped query-plan cache outcomes mirror global counters so one model's
// planner cache churn can be isolated from aggregate cache totals.
#[remain::check]
pub(in crate::metrics::sink) const fn record_entity_shared_query_plan_cache_outcome(
    ops: &mut metrics::EntityCounters,
    outcome: CacheOutcome,
) {
    #[remain::sorted]
    match outcome {
        CacheOutcome::Hit => {
            ops.cache_shared_query_plan_hits = ops.cache_shared_query_plan_hits.saturating_add(1);
        }
        CacheOutcome::Insert => {
            ops.cache_shared_query_plan_inserts =
                ops.cache_shared_query_plan_inserts.saturating_add(1);
        }
        CacheOutcome::Miss => {
            ops.cache_shared_query_plan_misses =
                ops.cache_shared_query_plan_misses.saturating_add(1);
        }
    }
}

// Entity-scoped SQL cache outcomes keep SQL command reuse attributable to the
// entity path that owns the compiled statement context.
#[remain::check]
pub(in crate::metrics::sink) const fn record_entity_sql_compiled_command_cache_outcome(
    ops: &mut metrics::EntityCounters,
    outcome: CacheOutcome,
) {
    #[remain::sorted]
    match outcome {
        CacheOutcome::Hit => {
            ops.cache_sql_compiled_command_hits =
                ops.cache_sql_compiled_command_hits.saturating_add(1);
        }
        CacheOutcome::Insert => {
            ops.cache_sql_compiled_command_inserts =
                ops.cache_sql_compiled_command_inserts.saturating_add(1);
        }
        CacheOutcome::Miss => {
            ops.cache_sql_compiled_command_misses =
                ops.cache_sql_compiled_command_misses.saturating_add(1);
        }
    }
}

// Keep per-entity miss reason buckets aligned with the global cache report so
// one drifting entity can be found without reverse-engineering aggregate totals.
#[remain::check]
pub(in crate::metrics::sink) const fn record_entity_cache_miss_reason(
    ops: &mut metrics::EntityCounters,
    kind: CacheKind,
    reason: CacheMissReason,
) {
    #[remain::sorted]
    match kind {
        CacheKind::SharedQueryPlan => {
            record_entity_shared_query_plan_miss_reason(ops, reason);
        }
        CacheKind::SqlCompiledCommand => {
            record_entity_sql_compiled_command_miss_reason(ops, reason);
        }
    }
}

#[remain::check]
pub(in crate::metrics::sink) const fn record_entity_shared_query_plan_miss_reason(
    ops: &mut metrics::EntityCounters,
    reason: CacheMissReason,
) {
    #[remain::sorted]
    match reason {
        CacheMissReason::Cold => {
            ops.cache_shared_query_plan_miss_cold =
                ops.cache_shared_query_plan_miss_cold.saturating_add(1);
        }
        CacheMissReason::DistinctKey | CacheMissReason::Surface => {
            ops.cache_shared_query_plan_miss_distinct_key = ops
                .cache_shared_query_plan_miss_distinct_key
                .saturating_add(1);
        }
        CacheMissReason::SchemaFingerprint | CacheMissReason::SchemaVersion => {
            ops.cache_shared_query_plan_miss_schema_fingerprint = ops
                .cache_shared_query_plan_miss_schema_fingerprint
                .saturating_add(1);
        }
        CacheMissReason::Visibility => {
            ops.cache_shared_query_plan_miss_visibility = ops
                .cache_shared_query_plan_miss_visibility
                .saturating_add(1);
        }
    }
}

#[remain::check]
pub(in crate::metrics::sink) const fn record_entity_sql_compiled_command_miss_reason(
    ops: &mut metrics::EntityCounters,
    reason: CacheMissReason,
) {
    #[remain::sorted]
    match reason {
        CacheMissReason::Cold => {
            ops.cache_sql_compiled_command_miss_cold =
                ops.cache_sql_compiled_command_miss_cold.saturating_add(1);
        }
        CacheMissReason::DistinctKey | CacheMissReason::Visibility => {
            ops.cache_sql_compiled_command_miss_distinct_key = ops
                .cache_sql_compiled_command_miss_distinct_key
                .saturating_add(1);
        }
        CacheMissReason::SchemaFingerprint | CacheMissReason::SchemaVersion => {
            ops.cache_sql_compiled_command_miss_schema_fingerprint = ops
                .cache_sql_compiled_command_miss_schema_fingerprint
                .saturating_add(1);
        }
        CacheMissReason::Surface => {
            ops.cache_sql_compiled_command_miss_surface = ops
                .cache_sql_compiled_command_miss_surface
                .saturating_add(1);
        }
    }
}
