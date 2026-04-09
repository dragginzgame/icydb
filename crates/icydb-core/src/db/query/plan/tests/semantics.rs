//! Module: db::query::plan::tests::semantics
//! Responsibility: module-local ownership and contracts for db::query::plan::tests::semantics.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        access::{AccessPath, AccessPlan, AccessPlanError},
        predicate::{
            CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate,
            UnsupportedQueryFeature,
        },
        query::{
            plan::validate::{
                CursorPagingPolicyError, OrderPlanError, PlanError, PolicyPlanError,
                validate_cursor_paging_requirements, validate_query_semantics,
            },
            plan::{
                AccessPlannedQuery, AggregateKind, DeleteLimitSpec, DeleteSpec,
                DistinctExecutionStrategy, ExecutionOrdering, FieldSlot, GroupAggregateSpec,
                GroupSpec, GroupedExecutionConfig, LoadSpec, LogicalPlan, LogicalPlanningInputs,
                OrderDirection, OrderSpec, PageSpec, PlanPolicyError, PlanUserError, QueryMode,
                build_logical_plan, logical_query_from_logical_inputs,
            },
        },
        schema::{SchemaInfo, ValidateError},
    },
    model::{
        entity::EntityModel,
        field::FieldKind,
        index::{IndexExpression, IndexKeyItem, IndexModel},
    },
    traits::{EntitySchema, Path},
    types::Ulid,
    value::Value,
};

const INDEX_FIELDS: [&str; 1] = ["tag"];
const INDEX_MODEL: IndexModel =
    IndexModel::generated("test::idx_tag", "test::IndexStore", &INDEX_FIELDS, false);
const EXPRESSION_INDEX_FIELDS: [&str; 1] = ["name"];
const EXPRESSION_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Lower("name"))];
const EXPRESSION_INDEX_MODEL: IndexModel = IndexModel::generated_with_key_items(
    "test::idx_name_lower",
    "test::ExpressionIndexStore",
    &EXPRESSION_INDEX_FIELDS,
    &EXPRESSION_INDEX_KEY_ITEMS,
    false,
);

crate::test_entity! {
    ident = PlanValidateIndexedEntity,
    id = Ulid,
    entity_name = "IndexedEntity",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tag", FieldKind::Text),
        ("rank", FieldKind::Int),
    ],
    indexes = [&INDEX_MODEL],
}

crate::test_entity! {
    ident = PlanValidateListEntity,
    id = Ulid,
    entity_name = "ListEntity",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tags", FieldKind::List(&FieldKind::Text)),
    ],
    indexes = [],
}

crate::test_entity! {
    ident = PlanValidateMapEntity,
    id = Ulid,
    entity_name = "MapEntity",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("metadata", FieldKind::Map {
            key: &FieldKind::Text,
            value: &FieldKind::Text,
        }),
    ],
    indexes = [],
}

crate::test_entity! {
    ident = PlanValidateExpressionIndexedEntity,
    id = Ulid,
    entity_name = "ExpressionIndexedEntity",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("name", FieldKind::Text),
        ("rank", FieldKind::Int),
    ],
    indexes = [&EXPRESSION_INDEX_MODEL],
}

fn model_with_index() -> &'static EntityModel {
    <PlanValidateIndexedEntity as EntitySchema>::MODEL
}

fn model_with_expression_index() -> &'static EntityModel {
    <PlanValidateExpressionIndexedEntity as EntitySchema>::MODEL
}

#[test]
fn plan_rejects_unorderable_field() {
    let model = <PlanValidateListEntity as EntitySchema>::MODEL;

    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("tags".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::FullScan),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };

    let err = validate_query_semantics(&schema, model, &plan).expect_err("unorderable field");
    assert!(matches!(err, PlanError::User(inner) if matches!(
        inner.as_ref(),
        PlanUserError::Order(inner)
            if matches!(inner.as_ref(), OrderPlanError::UnorderableField { .. })
    )));
}

#[test]
fn plan_rejects_duplicate_non_primary_order_field() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![
                    ("rank".to_string(), OrderDirection::Asc),
                    ("rank".to_string(), OrderDirection::Desc),
                    ("id".to_string(), OrderDirection::Asc),
                ],
            }),
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::FullScan),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };

    let err = validate_query_semantics(&schema, model, &plan)
        .expect_err("duplicate non-primary order field must fail");
    assert!(matches!(err, PlanError::User(inner) if matches!(
        inner.as_ref(),
        PlanUserError::Order(inner)
            if matches!(
                inner.as_ref(),
                OrderPlanError::DuplicateOrderField { field } if field == "rank"
            )
    )));
}

#[test]
fn plan_rejects_index_prefix_too_long() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::IndexPrefix {
            index: INDEX_MODEL,
            values: vec![Value::Text("a".to_string()), Value::Text("b".to_string())],
        }),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };

    let err = validate_query_semantics(&schema, model, &plan).expect_err("index prefix too long");
    assert!(matches!(err, PlanError::User(inner) if matches!(
        inner.as_ref(),
        PlanUserError::Access(inner)
            if matches!(inner.as_ref(), AccessPlanError::IndexPrefixTooLong { .. })
    )));
}

#[test]
fn plan_rejects_empty_index_prefix() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::IndexPrefix {
            index: INDEX_MODEL,
            values: vec![],
        }),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };

    let err = validate_query_semantics(&schema, model, &plan).expect_err("index prefix empty");
    assert!(matches!(err, PlanError::User(inner) if matches!(
        inner.as_ref(),
        PlanUserError::Access(inner)
            if matches!(inner.as_ref(), AccessPlanError::IndexPrefixEmpty)
    )));
}

#[test]
fn plan_accepts_model_based_validation() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::ByKey(Value::Ulid(Ulid::nil()))),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };

    validate_query_semantics(&schema, model, &plan).expect("valid plan");
}

#[test]
fn plan_rejects_empty_order_spec() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec { fields: vec![] }),
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::FullScan),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };

    let err = validate_query_semantics(&schema, model, &plan).expect_err("empty order must fail");
    assert!(matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PlanPolicyError::Policy(inner)
            if matches!(inner.as_ref(), PolicyPlanError::EmptyOrderSpec)
    )));
}

#[test]
fn delete_limit_requires_order() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Delete(DeleteSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: Some(DeleteLimitSpec { max_rows: 10 }),
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::FullScan),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };

    let err = validate_query_semantics(&schema, model, &plan)
        .expect_err("delete limit without order must fail");
    assert!(matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PlanPolicyError::Policy(inner)
            if matches!(inner.as_ref(), PolicyPlanError::DeleteLimitRequiresOrder)
    )));
}

#[test]
fn scalar_shorthand_helpers_remain_explicit_without_deref() {
    let mut plan: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);

    assert!(
        matches!(plan.logical.scalar().mode, QueryMode::Load(_)),
        "logical scalar helper should expose the scalar mode view",
    );
    assert!(
        !plan.scalar().distinct,
        "access-planned scalar helper should expose scalar semantics explicitly",
    );

    plan.scalar_mut().distinct = true;
    assert!(plan.scalar().distinct);

    plan.logical.scalar_mut().distinct = false;
    assert!(!plan.scalar().distinct);
}

#[test]
fn scalar_distinct_execution_strategy_is_planner_lowered_from_access_shape() {
    let mut path_plan: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    path_plan.scalar_mut().distinct = true;
    assert_eq!(
        path_plan.distinct_execution_strategy(),
        DistinctExecutionStrategy::None,
        "duplicate-safe single-path DISTINCT should lower to no-op strategy",
    );

    let mut composite_plan: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    composite_plan.access = AccessPlan::Union(vec![
        AccessPlan::path(AccessPath::ByKey(Value::Ulid(Ulid::from_u128(1)))),
        AccessPlan::path(AccessPath::ByKey(Value::Ulid(Ulid::from_u128(2)))),
    ]);
    composite_plan.scalar_mut().distinct = true;
    assert_eq!(
        composite_plan.distinct_execution_strategy(),
        DistinctExecutionStrategy::PreOrdered,
        "union DISTINCT should lower to streaming preordered dedup strategy",
    );

    composite_plan.access = AccessPlan::path(AccessPath::IndexMultiLookup {
        index: INDEX_MODEL,
        values: vec![Value::from(7_u64), Value::from(8_u64)],
    });
    assert_eq!(
        composite_plan.distinct_execution_strategy(),
        DistinctExecutionStrategy::HashMaterialize,
        "index multi-lookup DISTINCT should retain materialized dedup strategy",
    );

    composite_plan.scalar_mut().distinct = false;
    assert_eq!(
        composite_plan.distinct_execution_strategy(),
        DistinctExecutionStrategy::None,
        "disabled DISTINCT should lower to no-op strategy",
    );
}

#[test]
fn delete_plan_rejects_pagination() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Delete(DeleteSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(1),
                offset: 0,
            }),
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::FullScan),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };

    let err = validate_query_semantics(&schema, model, &plan)
        .expect_err("delete plans must not carry pagination");
    assert!(matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PlanPolicyError::Policy(inner)
            if matches!(inner.as_ref(), PolicyPlanError::DeletePlanWithPagination)
    )));
}

#[test]
fn load_plan_rejects_delete_limit() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: Some(DeleteLimitSpec { max_rows: 1 }),
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::FullScan),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };

    let err = validate_query_semantics(&schema, model, &plan)
        .expect_err("load plans must not carry delete limits");
    assert!(matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PlanPolicyError::Policy(inner)
            if matches!(inner.as_ref(), PolicyPlanError::LoadPlanWithDeleteLimit)
    )));
}

#[test]
fn plan_rejects_unordered_pagination() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(10),
                offset: 2,
            }),
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::FullScan),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };

    let err = validate_query_semantics(&schema, model, &plan)
        .expect_err("pagination without ordering must be rejected");
    assert!(matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PlanPolicyError::Policy(inner)
            if matches!(inner.as_ref(), PolicyPlanError::UnorderedPagination)
    )));
}

#[test]
fn plan_rejects_limit_without_order() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(10),
                offset: 0,
            }),
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::FullScan),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };

    let err = validate_query_semantics(&schema, model, &plan)
        .expect_err("limit without ordering must be rejected");
    assert!(matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PlanPolicyError::Policy(inner)
            if matches!(inner.as_ref(), PolicyPlanError::UnorderedPagination)
    )));
}

#[test]
fn continuation_cursor_paging_requires_order_and_limit() {
    let requires_order = validate_cursor_paging_requirements(
        false,
        LoadSpec {
            limit: Some(10),
            offset: 0,
        },
    );
    assert_eq!(
        requires_order,
        Err(CursorPagingPolicyError::CursorRequiresOrder),
        "cursor paging must require explicit ORDER BY in planner policy",
    );

    let requires_limit = validate_cursor_paging_requirements(
        true,
        LoadSpec {
            limit: None,
            offset: 0,
        },
    );
    assert_eq!(
        requires_limit,
        Err(CursorPagingPolicyError::CursorRequiresLimit),
        "cursor paging must require explicit LIMIT in planner policy",
    );
}

#[test]
fn plan_accepts_ordered_pagination() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(10),
                offset: 2,
            }),
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::FullScan),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };

    validate_query_semantics(&schema, model, &plan).expect("ordered pagination is valid");
}

#[test]
fn plan_accepts_expression_order_when_access_satisfies_matching_index() {
    let model = model_with_expression_index();
    let schema = SchemaInfo::from_entity_model(model).expect("valid expression-indexed model");
    let mut plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![
                    ("LOWER(name)".to_string(), OrderDirection::Asc),
                    ("id".to_string(), OrderDirection::Asc),
                ],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(2),
                offset: 0,
            }),
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::index_range(
            EXPRESSION_INDEX_MODEL,
            Vec::new(),
            std::ops::Bound::Unbounded,
            std::ops::Bound::Unbounded,
        )),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };
    plan.finalize_planner_route_profile_for_model(model);

    validate_query_semantics(&schema, model, &plan).expect(
        "expression order should validate when a matching index path already satisfies order",
    );
}

#[test]
fn plan_rejects_expression_order_without_access_satisfied_index_contract() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let mut plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![
                    ("LOWER(tag)".to_string(), OrderDirection::Asc),
                    ("id".to_string(), OrderDirection::Asc),
                ],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(2),
                offset: 0,
            }),
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::FullScan),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };
    plan.finalize_planner_route_profile_for_model(model);

    let err = validate_query_semantics(&schema, model, &plan)
        .expect_err("expression order must fail closed when access does not satisfy ordering");
    assert!(matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PlanPolicyError::Policy(inner)
            if matches!(
                inner.as_ref(),
                PolicyPlanError::ExpressionOrderRequiresIndexSatisfiedAccess
            )
    )));
}

#[test]
fn planner_build_logical_plan_appends_primary_key_tie_break_for_non_unique_order_keys() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let inputs = LogicalPlanningInputs::new(
        QueryMode::Load(LoadSpec::new()),
        Some(OrderSpec {
            fields: vec![("tag".to_string(), OrderDirection::Asc)],
        }),
        false,
        None,
        None,
    );
    let logical_query = logical_query_from_logical_inputs(inputs, None, MissingRowPolicy::Ignore);
    let logical_plan = build_logical_plan(model, logical_query);
    let order = logical_plan
        .scalar_semantics()
        .order
        .as_ref()
        .expect("logical plan should carry canonicalized order");

    assert_eq!(
        order.fields,
        vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
        "planner must append primary key as deterministic terminal tie-break",
    );
}

#[test]
fn grouped_plan_without_order_uses_grouped_canonical_ordering_contract() {
    let group_field =
        FieldSlot::resolve(<PlanValidateIndexedEntity as EntitySchema>::MODEL, "rank")
            .expect("group field must resolve");
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: vec![group_field],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                target_field: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        });
    let continuation = grouped
        .continuation_contract(<PlanValidateIndexedEntity as Path>::PATH)
        .expect("grouped plan should project continuation contract");

    assert_eq!(
        continuation.order_contract().ordering(),
        &ExecutionOrdering::Grouped(None),
        "grouped plans without explicit ORDER BY should use canonical grouped ordering contract",
    );
}

#[test]
fn plan_rejects_order_without_terminal_primary_key_tie_break() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("tag".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::FullScan),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };

    let err = validate_query_semantics(&schema, model, &plan).expect_err("missing PK tie-break");
    assert!(matches!(err, PlanError::User(inner) if matches!(
        inner.as_ref(),
        PlanUserError::Order(inner)
            if matches!(inner.as_ref(), OrderPlanError::MissingPrimaryKeyTieBreak { .. })
    )));
}

#[test]
fn plan_rejects_map_field_predicates_during_planning_validation() {
    let model = <PlanValidateMapEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: Some(Predicate::Compare(ComparePredicate::with_coercion(
                "metadata",
                CompareOp::Eq,
                Value::Text("payload".to_string()),
                CoercionId::Strict,
            ))),
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::FullScan),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
    };

    let err = validate_query_semantics(&schema, model, &plan)
        .expect_err("map field predicates must fail during planner validation");
    assert!(matches!(err, PlanError::User(inner) if matches!(
        inner.as_ref(),
        PlanUserError::PredicateInvalid(inner)
            if matches!(
                inner.as_ref(),
                ValidateError::UnsupportedQueryFeature(UnsupportedQueryFeature::MapPredicate { field })
                    if field == "metadata"
            )
    )));
}
