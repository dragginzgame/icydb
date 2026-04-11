//! Module: db::query::fingerprint::tests
//! Covers query fingerprint stability and frozen hash-profile behavior.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod coercion;
mod grouped;
mod numeric_projection;
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

#[test]
fn fingerprint_changes_with_index_choice() {
    const INDEX_FIELDS: [&str; 1] = ["idx_a"];
    const INDEX_A: IndexModel = IndexModel::generated(
        "fingerprint::idx_a",
        "fingerprint::store",
        &INDEX_FIELDS,
        false,
    );
    const INDEX_B: IndexModel = IndexModel::generated(
        "fingerprint::idx_b",
        "fingerprint::store",
        &INDEX_FIELDS,
        false,
    );

    let plan_a: AccessPlannedQuery =
        index_prefix_query(INDEX_A, vec![Value::Text("alpha".to_string())]);
    let plan_b: AccessPlannedQuery =
        index_prefix_query(INDEX_B, vec![Value::Text("alpha".to_string())]);

    assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
}

#[test]
fn fingerprint_changes_with_pagination() {
    let mut plan_a: AccessPlannedQuery = full_scan_query();
    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(10),
        offset: 0,
    });
    plan_b.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(10),
        offset: 1,
    });

    assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
}

#[test]
fn fingerprint_changes_with_delete_limit() {
    let mut plan_a: AccessPlannedQuery = full_scan_query();
    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().mode = QueryMode::Delete(DeleteSpec::new());
    plan_b.scalar_plan_mut().mode = QueryMode::Delete(DeleteSpec::new());
    plan_a.scalar_plan_mut().delete_limit = Some(DeleteLimitSpec {
        limit: Some(2),
        offset: 0,
    });
    plan_b.scalar_plan_mut().delete_limit = Some(DeleteLimitSpec {
        limit: Some(3),
        offset: 0,
    });

    assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
}

#[test]
fn fingerprint_changes_with_distinct_flag() {
    let plan_a: AccessPlannedQuery = full_scan_query();
    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_b.scalar_plan_mut().distinct = true;

    assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
}

#[test]
fn fingerprint_changes_when_index_range_bound_discriminant_changes() {
    const INDEX_FIELDS: [&str; 2] = ["group", "rank"];
    const INDEX: IndexModel = IndexModel::generated(
        "fingerprint::group_rank",
        "fingerprint::store",
        &INDEX_FIELDS,
        false,
    );

    let plan_included: AccessPlannedQuery = index_range_query(
        INDEX,
        vec![Value::Uint(7)],
        Bound::Included(Value::Uint(100)),
        Bound::Excluded(Value::Uint(200)),
    );
    let plan_excluded: AccessPlannedQuery = index_range_query(
        INDEX,
        vec![Value::Uint(7)],
        Bound::Excluded(Value::Uint(100)),
        Bound::Excluded(Value::Uint(200)),
    );

    assert_ne!(plan_included.fingerprint(), plan_excluded.fingerprint());
}

#[test]
fn fingerprint_changes_when_index_range_bound_value_changes() {
    const INDEX_FIELDS: [&str; 2] = ["group", "rank"];
    const INDEX: IndexModel = IndexModel::generated(
        "fingerprint::group_rank",
        "fingerprint::store",
        &INDEX_FIELDS,
        false,
    );

    let plan_low_100: AccessPlannedQuery = index_range_query(
        INDEX,
        vec![Value::Uint(7)],
        Bound::Included(Value::Uint(100)),
        Bound::Excluded(Value::Uint(200)),
    );
    let plan_low_101: AccessPlannedQuery = index_range_query(
        INDEX,
        vec![Value::Uint(7)],
        Bound::Included(Value::Uint(101)),
        Bound::Excluded(Value::Uint(200)),
    );

    assert_ne!(plan_low_100.fingerprint(), plan_low_101.fingerprint());
}
