use super::*;
use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        cursor::GroupedContinuationToken,
        direction::Direction,
        predicate::{CompareOp, ComparePredicate},
        query::{
            builder::{FieldRef, count, count_by, exists, first, last, max, max_by, min, sum},
            expr::FilterExpr,
            plan::{
                AccessPlannedQuery, AggregateKind, LogicalPlan, ScalarPlan,
                expr::{Expr, ProjectionField},
            },
        },
    },
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel},
        index::IndexModel,
    },
    testing::entity_model_from_static,
    traits::{EntitySchema, FieldProjection, FieldValue},
    types::{Ulid, Unit},
    value::{Value, ValueEnum},
};
use serde::{Deserialize, Serialize};

// Helper for intent tests that need the typed model snapshot.
fn basic_model() -> &'static EntityModel {
    <PlanEntity as EntitySchema>::MODEL
}

fn query_error_is_group_plan_error(
    err: &QueryError,
    predicate: impl FnOnce(&crate::db::query::plan::validate::GroupPlanError) -> bool,
) -> bool {
    let QueryError::Plan(plan_err) = err else {
        return false;
    };

    match plan_err.as_ref() {
        crate::db::query::plan::PlanError::User(inner) => match inner.as_ref() {
            crate::db::query::plan::PlanUserError::Group(inner) => predicate(inner.as_ref()),
            _ => false,
        },
        crate::db::query::plan::PlanError::Policy(inner) => match inner.as_ref() {
            crate::db::query::plan::PlanPolicyError::Group(inner) => predicate(inner.as_ref()),
            crate::db::query::plan::PlanPolicyError::Policy(_) => false,
        },
        crate::db::query::plan::PlanError::Cursor(_) => false,
    }
}

fn query_error_is_policy_plan_error(
    err: &QueryError,
    predicate: impl FnOnce(&crate::db::query::plan::validate::PolicyPlanError) -> bool,
) -> bool {
    let QueryError::Plan(plan_err) = err else {
        return false;
    };

    match plan_err.as_ref() {
        crate::db::query::plan::PlanError::Policy(inner) => match inner.as_ref() {
            crate::db::query::plan::PlanPolicyError::Policy(inner) => predicate(inner.as_ref()),
            crate::db::query::plan::PlanPolicyError::Group(_) => false,
        },
        crate::db::query::plan::PlanError::User(_)
        | crate::db::query::plan::PlanError::Cursor(_) => false,
    }
}

fn query_error_is_order_plan_error(
    err: &QueryError,
    predicate: impl FnOnce(&crate::db::query::plan::validate::OrderPlanError) -> bool,
) -> bool {
    let QueryError::Plan(plan_err) = err else {
        return false;
    };

    match plan_err.as_ref() {
        crate::db::query::plan::PlanError::User(inner) => match inner.as_ref() {
            crate::db::query::plan::PlanUserError::Order(inner) => predicate(inner.as_ref()),
            _ => false,
        },
        crate::db::query::plan::PlanError::Policy(_)
        | crate::db::query::plan::PlanError::Cursor(_) => false,
    }
}

fn query_error_is_predicate_validation_error(
    err: &QueryError,
    predicate: impl FnOnce(&crate::db::predicate::ValidateError) -> bool,
) -> bool {
    let QueryError::Plan(plan_err) = err else {
        return false;
    };

    match plan_err.as_ref() {
        crate::db::query::plan::PlanError::User(inner) => match inner.as_ref() {
            crate::db::query::plan::PlanUserError::PredicateInvalid(inner) => {
                predicate(inner.as_ref())
            }
            _ => false,
        },
        crate::db::query::plan::PlanError::Policy(_)
        | crate::db::query::plan::PlanError::Cursor(_) => false,
    }
}

// Test-only entity to compare typed vs model planning without schema macros.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct PlanEntity {
    id: Ulid,
    name: String,
}

static MAP_PLAN_FIELDS: [FieldModel; 2] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        name: "attributes",
        kind: FieldKind::Map {
            key: &FieldKind::Text,
            value: &FieldKind::Uint,
        },
    },
];
static MAP_PLAN_INDEXES: [&IndexModel; 0] = [];
static MAP_PLAN_MODEL: EntityModel = entity_model_from_static(
    "intent_tests::MapPlanEntity",
    "MapPlanEntity",
    &MAP_PLAN_FIELDS[0],
    &MAP_PLAN_FIELDS,
    &MAP_PLAN_INDEXES,
);

static ENUM_PLAN_FIELDS: [FieldModel; 2] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        name: "stage",
        kind: FieldKind::Enum {
            path: "intent_tests::Stage",
        },
    },
];
static ENUM_PLAN_INDEXES: [&IndexModel; 0] = [];
static ENUM_PLAN_MODEL: EntityModel = entity_model_from_static(
    "intent_tests::EnumPlanEntity",
    "EnumPlanEntity",
    &ENUM_PLAN_FIELDS[0],
    &ENUM_PLAN_FIELDS,
    &ENUM_PLAN_INDEXES,
);

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct PlanSingleton {
    id: Unit,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct PlanNumericEntity {
    id: Ulid,
    rank: i32,
}

impl FieldProjection for PlanSingleton {
    fn get_value_by_index(&self, index: usize) -> Option<Value> {
        match index {
            0 => Some(self.id.to_value()),
            _ => None,
        }
    }
}

crate::test_canister! {
    ident = PlanCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = PlanDataStore,
    canister = PlanCanister,
}

crate::test_entity_schema! {
    ident = PlanEntity,
    id = Ulid,
    entity_name = "PlanEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("name", FieldKind::Text),
    ],
    indexes = [],
    store = PlanDataStore,
    canister = PlanCanister,
}

crate::test_entity_schema! {
    ident = PlanSingleton,
    id = Unit,
    id_field = id,
    singleton = true,
    entity_name = "PlanSingleton",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Unit),
    ],
    indexes = [],
    store = PlanDataStore,
    canister = PlanCanister,
}

crate::test_entity_schema! {
    ident = PlanNumericEntity,
    id = Ulid,
    entity_name = "PlanNumericEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("rank", FieldKind::Int),
    ],
    indexes = [],
    store = PlanDataStore,
    canister = PlanCanister,
}

#[test]
fn intent_rejects_by_ids_with_predicate() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore)
        .by_ids([Ulid::generate()])
        .filter(Predicate::True);

    assert!(matches!(
        intent.build_plan_model(),
        Err(QueryError::Intent(IntentError::ByIdsWithPredicate))
    ));
}

#[test]
fn intent_rejects_only_with_predicate() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore)
        .only(Ulid::generate())
        .filter(Predicate::True);

    assert!(matches!(
        intent.build_plan_model(),
        Err(QueryError::Intent(IntentError::OnlyWithPredicate))
    ));
}

#[test]
fn intent_rejects_delete_limit_without_order() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore)
        .delete()
        .limit(1);

    assert!(matches!(
        intent.build_plan_model(),
        Err(QueryError::Intent(IntentError::PlanShape(
            crate::db::query::plan::validate::PolicyPlanError::DeleteLimitRequiresOrder
        )))
    ));
}

#[test]
fn intent_rejects_delete_offset_modifier() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore)
        .delete()
        .offset(10);

    assert!(matches!(
        intent.build_plan_model(),
        Err(QueryError::Intent(IntentError::PlanShape(
            crate::db::query::plan::validate::PolicyPlanError::DeletePlanWithOffset
        )))
    ));
}

#[test]
fn intent_rejects_offset_then_delete_shape() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore)
        .offset(10)
        .delete();

    assert!(matches!(
        intent.build_plan_model(),
        Err(QueryError::Intent(IntentError::PlanShape(
            crate::db::query::plan::validate::PolicyPlanError::DeletePlanWithOffset
        )))
    ));
}

#[test]
fn delete_query_rejects_grouped_shape_during_intent_validation() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .group_by("name")
        .expect("group field should resolve")
        .plan()
        .expect_err("delete queries must reject grouped logical shape during intent validation");

    assert!(matches!(
        err,
        QueryError::Intent(IntentError::PlanShape(
            crate::db::query::plan::validate::PolicyPlanError::DeletePlanWithGrouping
        ))
    ));
}

#[test]
fn load_limit_without_order_rejects_unordered_pagination() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .limit(1)
        .plan()
        .expect_err("limit without order must fail");

    assert!(query_error_is_policy_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::PolicyPlanError::UnorderedPagination
        )
    }));
}

#[test]
fn grouped_load_limit_without_order_is_allowed() {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .expect("grouped pagination should use canonical grouped-key order");
}

#[test]
fn grouped_load_distinct_is_rejected_without_adjacency_eligibility() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .distinct()
        .plan()
        .expect_err("grouped distinct should be rejected until adjacency eligibility exists");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::DistinctAdjacencyEligibilityRequired
        )
    }));
}

#[test]
fn grouped_load_order_prefix_mismatch_is_rejected() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .expect_err("grouped order should be rejected when group keys are not the order prefix");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::OrderPrefixNotAlignedWithGroupKeys
        )
    }));
}

#[test]
fn grouped_load_order_prefix_alignment_is_allowed() {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .expect("grouped order should be accepted when grouped keys lead ORDER BY and LIMIT is explicit");
}

#[test]
fn grouped_load_order_without_limit_is_rejected() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .plan()
        .expect_err("grouped order should reject missing LIMIT");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::OrderRequiresLimit
        )
    }));
}

#[test]
fn grouped_load_distinct_count_terminal_is_allowed() {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count().distinct())
        .plan()
        .expect("grouped distinct count terminal should plan in grouped v1");
}

#[test]
fn grouped_aggregate_builder_count_shape_matches_helper_terminal() {
    let helper_explain = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .plan()
        .expect("helper grouped count should plan")
        .into_inner()
        .explain();
    let builder_explain = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(count())
        .plan()
        .expect("builder grouped count should plan")
        .into_inner()
        .explain();

    assert_eq!(
        helper_explain, builder_explain,
        "aggregate(count()) should preserve grouped count logical shape",
    );
}

#[test]
fn grouped_global_distinct_count_field_without_group_by_is_allowed() {
    let plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::count_by("name").distinct())
        .plan()
        .expect("global grouped count(distinct field) should plan");

    let Some(grouped) = plan.into_inner().grouped_plan().cloned() else {
        panic!("global grouped distinct field aggregate must compile to grouped logical plan");
    };
    assert!(
        grouped.group.group_fields.is_empty(),
        "global grouped distinct aggregate should use zero group keys"
    );
    assert_eq!(
        grouped.group.aggregates.len(),
        1,
        "global grouped distinct aggregate should declare exactly one terminal"
    );
    assert_eq!(
        grouped.group.aggregates[0].target_field(),
        Some("name"),
        "global grouped distinct count should preserve target field"
    );
    assert!(
        grouped.group.aggregates[0].distinct(),
        "global grouped distinct count should preserve DISTINCT modifier"
    );
}

#[test]
fn grouped_aggregate_builder_global_distinct_count_shape_matches_helper_terminal() {
    let helper_explain = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::count_by("name").distinct())
        .plan()
        .expect("helper global count(distinct field) should plan")
        .into_inner()
        .explain();
    let builder_explain = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(count_by("name").distinct())
        .plan()
        .expect("builder global count(distinct field) should plan")
        .into_inner()
        .explain();

    assert_eq!(
        helper_explain, builder_explain,
        "aggregate(count_by(field).distinct()) should preserve global distinct-count logical shape",
    );
}

#[test]
fn grouped_aggregate_builder_global_distinct_sum_shape_matches_helper_terminal() {
    let helper_explain = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::sum("rank").distinct())
        .plan()
        .expect("helper global sum(distinct field) should plan")
        .into_inner()
        .explain();
    let builder_explain = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(sum("rank").distinct())
        .plan()
        .expect("builder global sum(distinct field) should plan")
        .into_inner()
        .explain();

    assert_eq!(
        helper_explain, builder_explain,
        "aggregate(sum(field).distinct()) should preserve global distinct-sum logical shape",
    );
}

#[test]
fn grouped_aggregate_builder_fingerprint_parity_preserves_grouping_and_order_shape() {
    let helper_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .expect("helper grouped count plan should build")
        .into_inner();
    let builder_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(count())
        .limit(1)
        .plan()
        .expect("builder grouped count plan should build")
        .into_inner();

    assert_eq!(
        helper_plan.fingerprint(),
        builder_plan.fingerprint(),
        "helper and builder grouped count plans must have identical fingerprints",
    );
    assert_eq!(
        helper_plan.continuation_signature("intent::tests::PlanEntity"),
        builder_plan.continuation_signature("intent::tests::PlanEntity"),
        "helper and builder grouped count plans must have identical continuation signatures",
    );
}

#[test]
fn grouped_aggregate_builder_terminal_matrix_matches_helper_fingerprints() {
    for terminal in ["exists", "first", "last", "min", "max"] {
        let helper_plan = match terminal {
            "exists" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(crate::db::exists())
                .limit(1)
                .plan()
                .expect("helper grouped exists plan should build")
                .into_inner(),
            "first" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(crate::db::first())
                .limit(1)
                .plan()
                .expect("helper grouped first plan should build")
                .into_inner(),
            "last" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(crate::db::last())
                .limit(1)
                .plan()
                .expect("helper grouped last plan should build")
                .into_inner(),
            "min" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(crate::db::min())
                .limit(1)
                .plan()
                .expect("helper grouped min plan should build")
                .into_inner(),
            "max" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(crate::db::max())
                .limit(1)
                .plan()
                .expect("helper grouped max plan should build")
                .into_inner(),
            _ => unreachable!("terminal matrix is fixed"),
        };
        let builder_plan = match terminal {
            "exists" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(exists())
                .limit(1)
                .plan()
                .expect("builder grouped exists plan should build")
                .into_inner(),
            "first" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(first())
                .limit(1)
                .plan()
                .expect("builder grouped first plan should build")
                .into_inner(),
            "last" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(last())
                .limit(1)
                .plan()
                .expect("builder grouped last plan should build")
                .into_inner(),
            "min" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(min())
                .limit(1)
                .plan()
                .expect("builder grouped min plan should build")
                .into_inner(),
            "max" => Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
                .group_by("name")
                .expect("group field should resolve")
                .aggregate(max())
                .limit(1)
                .plan()
                .expect("builder grouped max plan should build")
                .into_inner(),
            _ => unreachable!("terminal matrix is fixed"),
        };

        assert_eq!(
            helper_plan.fingerprint(),
            builder_plan.fingerprint(),
            "terminal `{terminal}` helper/builder fingerprints must match",
        );
        assert_eq!(
            helper_plan.continuation_signature("intent::tests::PlanEntity"),
            builder_plan.continuation_signature("intent::tests::PlanEntity"),
            "terminal `{terminal}` helper/builder continuation signatures must match",
        );
    }
}

#[test]
fn grouped_aggregate_builder_fingerprint_parity_preserves_distinct_flag_shape() {
    let helper_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::count_by("name").distinct())
        .limit(1)
        .plan()
        .expect("helper grouped global distinct count plan should build")
        .into_inner();
    let builder_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(count_by("name").distinct())
        .limit(1)
        .plan()
        .expect("builder grouped global distinct count plan should build")
        .into_inner();

    assert_eq!(
        helper_plan.fingerprint(),
        builder_plan.fingerprint(),
        "helper and builder global distinct-count plans must have identical fingerprints",
    );
    assert_eq!(
        helper_plan.continuation_signature("intent::tests::PlanEntity"),
        builder_plan.continuation_signature("intent::tests::PlanEntity"),
        "helper and builder global distinct-count plans must have identical continuation signatures",
    );
}

#[test]
fn grouped_aggregate_builder_fingerprint_parity_preserves_distinct_sum_shape() {
    let helper_plan = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::sum("rank").distinct())
        .limit(1)
        .plan()
        .expect("helper grouped global distinct sum plan should build")
        .into_inner();
    let builder_plan = Query::<PlanNumericEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(sum("rank").distinct())
        .limit(1)
        .plan()
        .expect("builder grouped global distinct sum plan should build")
        .into_inner();

    assert_eq!(
        helper_plan.explain().grouping,
        builder_plan.explain().grouping,
        "helper and builder global distinct-sum plans must have identical grouped projection shapes",
    );
    assert_eq!(
        helper_plan.fingerprint(),
        builder_plan.fingerprint(),
        "helper and builder global distinct-sum plans must have identical fingerprints",
    );
    assert_eq!(
        helper_plan.continuation_signature("intent::tests::PlanNumericEntity"),
        builder_plan.continuation_signature("intent::tests::PlanNumericEntity"),
        "helper and builder global distinct-sum plans must have identical continuation signatures",
    );
}

#[test]
fn grouped_aggregate_builder_fingerprint_parity_preserves_projection_order_shape() {
    let helper_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .aggregate(crate::db::max())
        .limit(1)
        .plan()
        .expect("helper grouped multi-aggregate plan should build")
        .into_inner();
    let builder_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(count())
        .aggregate(max())
        .limit(1)
        .plan()
        .expect("builder grouped multi-aggregate plan should build")
        .into_inner();

    assert_eq!(
        helper_plan.explain().grouping,
        builder_plan.explain().grouping,
        "helper and builder grouped multi-aggregate projection shapes must match",
    );
    assert_eq!(
        helper_plan.fingerprint(),
        builder_plan.fingerprint(),
        "helper and builder grouped multi-aggregate plans must have identical fingerprints",
    );
}

#[test]
fn grouped_aggregate_builder_continuation_token_bytes_match_helper_shape() {
    let helper_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .plan()
        .expect("helper grouped continuation plan should build")
        .into_inner();
    let builder_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(count())
        .limit(1)
        .plan()
        .expect("builder grouped continuation plan should build")
        .into_inner();
    let helper_signature = helper_plan.continuation_signature("intent::tests::PlanEntity");
    let builder_signature = builder_plan.continuation_signature("intent::tests::PlanEntity");
    assert_eq!(
        helper_signature, builder_signature,
        "helper and builder grouped continuation signatures must match",
    );

    let helper_token = GroupedContinuationToken::new_with_direction(
        helper_signature,
        vec![Value::Text("alpha".to_string())],
        Direction::Asc,
        0,
    )
    .encode()
    .expect("helper grouped continuation token should encode");
    let builder_token = GroupedContinuationToken::new_with_direction(
        builder_signature,
        vec![Value::Text("alpha".to_string())],
        Direction::Asc,
        0,
    )
    .encode()
    .expect("builder grouped continuation token should encode");
    assert_eq!(
        helper_token, builder_token,
        "helper and builder grouped continuation token bytes must match for equivalent shapes",
    );
}

#[test]
fn grouped_global_distinct_mixed_terminal_shape_without_group_by_is_rejected() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::count_by("name").distinct())
        .aggregate(crate::db::count())
        .plan()
        .expect_err(
            "global grouped distinct without group keys should reject mixed aggregate shape",
        );

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::GlobalDistinctAggregateShapeUnsupported
        )
    }));
}

#[test]
fn grouped_aggregate_builder_rejects_distinct_for_unsupported_kind() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(exists().distinct())
        .plan()
        .expect_err("grouped distinct exists should remain rejected");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::DistinctAggregateKindUnsupported { index, kind }
                if *index == 0 && kind == "Exists"
        )
    }));
}

#[test]
fn grouped_aggregate_builder_rejects_field_target_terminal_in_grouped_v1() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(max_by("name"))
        .plan()
        .expect_err("grouped max(field) should remain unsupported in grouped v1");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::FieldTargetAggregatesUnsupported { index, kind, field }
                if *index == 0 && kind == "Max" && field == "name"
        )
    }));
}

#[test]
fn grouped_aggregate_builder_rejects_global_distinct_sum_on_non_numeric_target() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(sum("name").distinct())
        .plan()
        .expect_err("global sum(distinct non-numeric field) should fail");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::GlobalDistinctSumTargetNotNumeric { index, field }
                if *index == 0 && field == "name"
        )
    }));
}

#[test]
fn grouped_having_requires_group_by() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .having_group("name", CompareOp::Eq, Value::Text("alpha".to_string()))
        .expect_err("having should fail when group_by is missing");

    assert!(matches!(
        err,
        QueryError::Intent(IntentError::HavingRequiresGroupBy)
    ));
}

#[test]
fn grouped_having_with_distinct_is_rejected() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .having_aggregate(0, CompareOp::Gt, Value::Uint(0))
        .expect("having aggregate clause should append on grouped query")
        .distinct()
        .plan()
        .expect_err("grouped having with distinct should be rejected in this release");

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::DistinctHavingUnsupported
        )
    }));
}

#[test]
fn grouped_having_with_distinct_is_rejected_for_ordered_eligible_shape() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .group_by("name")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .having_aggregate(0, CompareOp::Gt, Value::Uint(0))
        .expect("having aggregate clause should append on grouped query")
        .distinct()
        .plan()
        .expect_err(
            "grouped having with distinct should be rejected even when grouped order prefix is aligned",
        );

    assert!(query_error_is_group_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::GroupPlanError::DistinctHavingUnsupported
        )
    }));
}

#[test]
fn load_rejects_duplicate_non_primary_order_field() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .order_by_desc("name")
        .limit(1)
        .plan()
        .expect_err("duplicate non-primary order field must fail");

    assert!(query_error_is_order_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::OrderPlanError::DuplicateOrderField { field }
                if field == "name"
        )
    }));
}

#[test]
fn load_offset_without_order_rejects_unordered_pagination() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .offset(1)
        .plan()
        .expect_err("offset without order must fail");

    assert!(query_error_is_policy_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::PolicyPlanError::UnorderedPagination
        )
    }));
}

#[test]
fn load_limit_and_offset_without_order_rejects_unordered_pagination() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .limit(10)
        .offset(2)
        .plan()
        .expect_err("limit+offset without order must fail");

    assert!(query_error_is_policy_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::PolicyPlanError::UnorderedPagination
        )
    }));
}

#[test]
fn load_ordered_pagination_is_allowed() {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .limit(10)
        .offset(2)
        .plan()
        .expect("ordered pagination should plan");
}

#[test]
fn ordered_plan_appends_primary_key_tie_break() {
    let plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by("name")
        .plan()
        .expect("ordered plan should build")
        .into_inner();
    let order = plan
        .scalar_plan()
        .order
        .as_ref()
        .expect("ordered query should carry order spec");

    assert_eq!(
        order.fields,
        vec![
            ("name".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
        "canonical order should append primary key as terminal tie-break"
    );
}

#[test]
fn ordered_plan_moves_primary_key_to_terminal_position() {
    let plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("id")
        .order_by("name")
        .plan()
        .expect("ordered plan should build")
        .into_inner();
    let order = plan
        .scalar_plan()
        .order
        .as_ref()
        .expect("ordered query should carry order spec");

    assert_eq!(
        order.fields,
        vec![
            ("name".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Desc),
        ],
        "canonical order must keep exactly one terminal PK tie-break with requested direction"
    );
}

#[test]
fn intent_rejects_empty_order_spec() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore)
        .order_spec(OrderSpec { fields: Vec::new() });

    assert!(matches!(
        intent.build_plan_model(),
        Err(QueryError::Intent(IntentError::PlanShape(
            crate::db::query::plan::validate::PolicyPlanError::EmptyOrderSpec
        )))
    ));
}

#[test]
fn intent_rejects_conflicting_key_access() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore)
        .by_id(Ulid::generate())
        .by_ids([Ulid::generate()]);

    assert!(matches!(
        intent.build_plan_model(),
        Err(QueryError::Intent(IntentError::KeyAccessConflict))
    ));
}

#[test]
fn typed_by_ids_matches_by_id_access() {
    let key = Ulid::generate();

    let by_id = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .by_id(key)
        .plan()
        .expect("by_id plan")
        .into_inner();
    let by_ids = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .by_ids([key])
        .plan()
        .expect("by_ids plan")
        .into_inner();

    assert_eq!(by_id, by_ids);
}

#[test]
fn by_id_limit_one_without_order_simplifies_paging_shape() {
    let key = Ulid::generate();
    let plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .by_id(key)
        .limit(1)
        .plan()
        .expect("by_id + limit(1) plan should build")
        .into_inner();

    assert!(
        plan.scalar_plan().page.is_none(),
        "by_id + limit(1) with no offset should remove redundant page metadata"
    );
    assert!(
        matches!(
            plan.access,
            AccessPlan::Path(path)
                if matches!(path.as_ref(), AccessPath::ByKey(by_key) if *by_key == key)
        ),
        "by_id + limit(1) should keep exact ByKey access",
    );
}

#[test]
fn by_key_access_strips_redundant_primary_key_equality_predicate() {
    let key = Ulid::generate();
    let model_plan = QueryModel::<Ulid>::new(PlanEntity::MODEL, MissingRowPolicy::Ignore)
        .by_id(key)
        .filter(FieldRef::new("id").eq(key))
        .build_plan_model()
        .expect("model by_id + id == literal plan should build");
    let (logical, access) = model_plan.into_parts();
    let typed_access = access_plan_to_entity_keys::<PlanEntity>(PlanEntity::MODEL, access)
        .expect("typed access conversion should succeed");
    let typed_plan = AccessPlannedQuery::from_parts(logical, typed_access);

    assert!(
        typed_plan.scalar_plan().predicate.is_none(),
        "by_id + id == literal should strip redundant scalar predicate"
    );
    assert!(
        matches!(
            typed_plan.access,
            AccessPlan::Path(path)
                if matches!(path.as_ref(), AccessPath::ByKey(by_key) if *by_key == key)
        ),
        "redundant predicate stripping must keep the exact ByKey path"
    );
}

#[test]
fn singleton_only_uses_default_key() {
    let plan = Query::<PlanSingleton>::new(MissingRowPolicy::Ignore)
        .only()
        .plan()
        .expect("singleton plan")
        .into_inner();

    assert!(matches!(
        plan.access,
        AccessPlan::Path(path) if matches!(path.as_ref(), AccessPath::ByKey(Unit))
    ));
}

#[test]
fn build_plan_model_full_scan_without_predicate() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore);
    let plan = intent.build_plan_model().expect("model plan should build");

    assert!(matches!(
        plan.access,
        AccessPlan::Path(path) if matches!(path.as_ref(), AccessPath::FullScan)
    ));
}

#[test]
fn build_plan_model_limit_zero_lowers_to_empty_by_keys() {
    let plan = QueryModel::<Ulid>::new(PlanEntity::MODEL, MissingRowPolicy::Ignore)
        .order_by("id")
        .limit(0)
        .build_plan_model()
        .expect("ordered limit(0) plan should build");

    assert!(matches!(
        &plan.access,
        AccessPlan::Path(path)
            if matches!(path.as_ref(), AccessPath::ByKeys(keys) if keys.is_empty())
    ));
}

#[test]
fn build_plan_model_constant_false_lowers_to_empty_by_keys() {
    let plan = QueryModel::<Ulid>::new(PlanEntity::MODEL, MissingRowPolicy::Ignore)
        .filter(Predicate::False)
        .build_plan_model()
        .expect("constant false plan should build");

    assert!(
        matches!(
            &plan.access,
            AccessPlan::Path(path)
                if matches!(path.as_ref(), AccessPath::ByKeys(keys) if keys.is_empty())
        ),
        "constant-false filter should lower to empty by-keys access"
    );
    assert!(
        matches!(plan.scalar_plan().predicate, Some(Predicate::False)),
        "constant-false filter should remain visible in logical predicate for explain stability"
    );
}

#[test]
fn build_plan_model_constant_true_elides_logical_predicate() {
    let plan = QueryModel::<Ulid>::new(PlanEntity::MODEL, MissingRowPolicy::Ignore)
        .filter(Predicate::True)
        .build_plan_model()
        .expect("constant true plan should build");

    assert!(
        plan.scalar_plan().predicate.is_none(),
        "constant-true filter should be folded away before logical planning"
    );
    assert!(
        matches!(
            &plan.access,
            AccessPlan::Path(path) if matches!(path.as_ref(), AccessPath::FullScan)
        ),
        "constant-true filter should not force access routing changes",
    );
}

#[test]
fn typed_plan_matches_model_plan_for_same_intent() {
    let predicate = FieldRef::new("id").eq(Ulid::default());

    let model_intent = QueryModel::<Ulid>::new(PlanEntity::MODEL, MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("name")
        .limit(10)
        .offset(2);

    let model_plan = model_intent.build_plan_model().expect("model plan");
    let (model_logical, model_access) = model_plan.into_parts();
    let LogicalPlan::Scalar(ScalarPlan {
        mode,
        predicate: plan_predicate,
        order,
        distinct,
        delete_limit,
        page,
        consistency,
    }) = model_logical
    else {
        panic!("typed/model intent parity test expects scalar logical plan");
    };

    let access = access_plan_to_entity_keys::<PlanEntity>(PlanEntity::MODEL, model_access)
        .expect("convert access plan");
    let model_as_typed = AccessPlannedQuery::from_parts(
        LogicalPlan::Scalar(ScalarPlan {
            mode,
            predicate: plan_predicate,
            order,
            distinct,
            delete_limit,
            page,
            consistency,
        }),
        access,
    );

    let typed_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("name")
        .limit(10)
        .offset(2)
        .plan()
        .expect("typed plan")
        .into_inner();

    assert_eq!(model_as_typed, typed_plan);
}

#[test]
fn query_distinct_defaults_to_false() {
    let plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .plan()
        .expect("typed plan")
        .into_inner();

    assert!(
        !plan.scalar_plan().distinct,
        "distinct should default to false for new query intents"
    );
}

#[test]
fn query_distinct_sets_logical_plan_flag() {
    let plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .distinct()
        .plan()
        .expect("typed plan")
        .into_inner();

    assert!(
        plan.scalar_plan().distinct,
        "distinct should be true when query intent enables distinct"
    );
}

#[test]
fn compiled_query_projection_spec_lowers_scalar_fields_in_model_order() {
    let compiled = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .plan()
        .expect("plan should build");
    let field_names = compiled
        .projection_spec()
        .fields()
        .map(|field| match field {
            ProjectionField::Scalar {
                expr: Expr::Field(field),
                alias: None,
            } => field.as_str().to_string(),
            other @ ProjectionField::Scalar { .. } => {
                panic!("scalar projection should lower to plain field exprs: {other:?}")
            }
        })
        .collect::<Vec<_>>();

    assert_eq!(field_names, vec!["id".to_string(), "name".to_string()]);
}

#[test]
fn compiled_query_projection_spec_lowers_grouped_shape_in_declaration_order() {
    let compiled = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("group by should resolve")
        .aggregate(count())
        .plan()
        .expect("grouped plan should build");
    let projection = compiled.projection_spec();
    let fields = projection.fields().collect::<Vec<_>>();
    assert_eq!(
        fields.len(),
        2,
        "grouped projection should include key + aggregate"
    );

    match fields[0] {
        ProjectionField::Scalar {
            expr: Expr::Field(field),
            alias: None,
        } => assert_eq!(field.as_str(), "name"),
        other @ ProjectionField::Scalar { .. } => {
            panic!("first grouped projection field should be grouped key expr: {other:?}")
        }
    }
    match fields[1] {
        ProjectionField::Scalar {
            expr: Expr::Aggregate(aggregate),
            alias: None,
        } => {
            assert_eq!(aggregate.kind(), AggregateKind::Count);
            assert_eq!(aggregate.target_field(), None);
            assert!(!aggregate.is_distinct());
        }
        other @ ProjectionField::Scalar { .. } => {
            panic!("second grouped projection field should be grouped aggregate expr: {other:?}")
        }
    }
}

#[test]
fn compiled_query_projection_spec_preserves_global_distinct_aggregate_semantics() {
    let compiled = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(count_by("name").distinct())
        .plan()
        .expect("global distinct grouped plan should build");
    let projection = compiled.projection_spec();
    let fields = projection.fields().collect::<Vec<_>>();
    assert_eq!(
        fields.len(),
        1,
        "global distinct grouped projection should only include one aggregate"
    );

    match fields[0] {
        ProjectionField::Scalar {
            expr: Expr::Aggregate(aggregate),
            alias: None,
        } => {
            assert_eq!(aggregate.kind(), AggregateKind::Count);
            assert_eq!(aggregate.target_field(), Some("name"));
            assert!(aggregate.is_distinct());
        }
        other @ ProjectionField::Scalar { .. } => {
            panic!("global distinct projection should lower to aggregate expr: {other:?}")
        }
    }
}

#[test]
fn build_plan_model_rejects_map_field_predicates_before_planning() {
    let intent = QueryModel::<Ulid>::new(&MAP_PLAN_MODEL, MissingRowPolicy::Ignore).filter(
        Predicate::Compare(ComparePredicate::with_coercion(
            "attributes",
            CompareOp::Eq,
            Value::Map(Vec::new()),
            crate::db::predicate::CoercionId::Strict,
        )),
    );

    let err = intent
        .build_plan_model()
        .expect_err("map field predicates must be rejected before planning");
    assert!(query_error_is_predicate_validation_error(&err, |inner| {
        matches!(
            inner,
            crate::db::predicate::ValidateError::UnsupportedQueryFeature(
                crate::db::predicate::UnsupportedQueryFeature::MapPredicate { field }
            ) if field == "attributes"
        )
    }));
}

#[test]
fn filter_expr_resolves_loose_enum_stage_filters() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "stage",
        CompareOp::Eq,
        Value::Enum(ValueEnum::loose("Active")),
        crate::db::predicate::CoercionId::Strict,
    ));

    let intent = QueryModel::<Ulid>::new(&ENUM_PLAN_MODEL, MissingRowPolicy::Ignore)
        .filter_expr(FilterExpr(predicate))
        .expect("filter expr should lower");
    let plan = intent.build_plan_model().expect("plan should build");

    let Some(Predicate::Compare(cmp)) = plan.scalar_plan().predicate.as_ref() else {
        panic!("expected compare predicate");
    };
    let Value::Enum(stage) = &cmp.value else {
        panic!("expected enum literal");
    };
    assert_eq!(stage.path(), Some("intent_tests::Stage"));
}

#[test]
fn filter_expr_rejects_wrong_strict_enum_path() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "stage",
        CompareOp::Eq,
        Value::Enum(ValueEnum::new("Active", Some("wrong::Stage"))),
        crate::db::predicate::CoercionId::Strict,
    ));

    let err = QueryModel::<Ulid>::new(&ENUM_PLAN_MODEL, MissingRowPolicy::Ignore)
        .filter_expr(FilterExpr(predicate))
        .expect_err("strict enum with wrong path should fail");
    assert!(matches!(
        err,
        QueryError::Validate(crate::db::predicate::ValidateError::InvalidLiteral {
            field,
            ..
        }) if field == "stage"
    ));
}

#[test]
fn direct_stage_filter_resolves_loose_enum_path() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "stage",
        CompareOp::Eq,
        Value::Enum(ValueEnum::loose("Draft")),
        crate::db::predicate::CoercionId::Strict,
    ));

    let plan = QueryModel::<Ulid>::new(&ENUM_PLAN_MODEL, MissingRowPolicy::Ignore)
        .filter(predicate)
        .build_plan_model()
        .expect("direct filter should build");
    let Some(Predicate::Compare(cmp)) = plan.scalar_plan().predicate.as_ref() else {
        panic!("expected compare predicate");
    };
    let Value::Enum(stage) = &cmp.value else {
        panic!("expected enum literal");
    };
    assert_eq!(stage.path(), Some("intent_tests::Stage"));
}
