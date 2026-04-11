//! Module: db::query::fingerprint::tests
//! Covers query fingerprint stability and frozen hash-profile behavior.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod coercion;
mod grouped;
mod numeric_projection;
mod shape;
mod stability;

use crate::{
    db::{
        access::AccessPath,
        codec::cursor::encode_cursor,
        predicate::{CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::{
            builder::{field::FieldRef, sum},
            explain::ExplainGrouping,
            fingerprint::{
                finalize_sha256_digest, hash_parts, new_continuation_signature_hasher,
                new_plan_fingerprint_hasher,
            },
            intent::{KeyAccess, build_access_plan_from_keys},
            plan::{
                AccessPlannedQuery, AggregateKind, DeleteLimitSpec, DeleteSpec, FieldSlot,
                GroupAggregateSpec, GroupSpec, GroupedExecutionConfig, LoadSpec, LogicalPlan,
                PageSpec, QueryMode, ScalarPlan,
                expr::{
                    Alias, BinaryOp, Expr, FieldId, ProjectionField, ProjectionSelection,
                    ProjectionSpec,
                },
            },
        },
    },
    model::index::IndexModel,
    types::{Decimal, Ulid},
    value::Value,
};
use sha2::{Digest, Sha256};
use std::ops::Bound;

fn fingerprint_with_projection(plan: &AccessPlannedQuery, projection: &ProjectionSpec) -> [u8; 32] {
    let explain = plan.explain();
    let mut hasher = new_plan_fingerprint_hasher();
    hash_explain_plan_profile_with_projection(
        &mut hasher,
        &explain,
        hash_parts::ExplainHashProfile::Fingerprint,
        projection,
    );

    finalize_sha256_digest(hasher)
}

fn hash_explain_plan_profile_with_projection(
    hasher: &mut Sha256,
    plan: &crate::db::query::explain::ExplainPlan,
    profile: hash_parts::ExplainHashProfile<'_>,
    projection: &ProjectionSpec,
) {
    hash_parts::hash_explain_plan_profile_internal(hasher, plan, profile, Some(projection));
}

fn full_scan_query() -> AccessPlannedQuery {
    AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
}

fn index_prefix_query(index: IndexModel, values: Vec<Value>) -> AccessPlannedQuery {
    AccessPlannedQuery::new(
        AccessPath::IndexPrefix { index, values },
        MissingRowPolicy::Ignore,
    )
}

fn index_range_query(
    index: IndexModel,
    prefix: Vec<Value>,
    lower: Bound<Value>,
    upper: Bound<Value>,
) -> AccessPlannedQuery {
    AccessPlannedQuery::new(
        AccessPath::index_range(index, prefix, lower, upper),
        MissingRowPolicy::Ignore,
    )
}

fn grouped_query_with_fixed_shape() -> AccessPlannedQuery {
    AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore).into_grouped(
        GroupSpec {
            group_fields: vec![FieldSlot::from_parts_for_test(1, "rank")],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(64, 4096),
        },
    )
}

fn grouped_explain_with_fixed_shape() -> crate::db::query::explain::ExplainPlan {
    grouped_query_with_fixed_shape().explain()
}
