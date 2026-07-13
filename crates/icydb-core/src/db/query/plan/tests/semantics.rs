//! Module: db::query::plan::tests::semantics
//! Covers semantic query-plan behavior and planning outcomes.
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
            intent::QueryModel,
            plan::validate::{
                CursorPagingPolicyError, ExprPlanError, OrderPlanError, PlanError, PolicyPlanError,
                validate_cursor_paging_requirements, validate_query_semantics,
            },
            plan::{
                AccessChoiceExplainSnapshot, AccessPlannedQuery, AggregateKind, DeleteLimitSpec,
                DeleteSpec, DistinctExecutionStrategy, ExecutionOrdering, FieldSlot,
                GroupAggregateSpec, GroupSpec, GroupedExecutionConfig, LoadSpec, LogicalPlan,
                LogicalPlanningInputs, OrderDirection, OrderSpec, PageSpec, PlanPolicyError,
                PlanUserError, QueryMode, ResidualFilterShape, VisibleIndexes, build_logical_plan,
                build_query_model_plan_with_indexes_from_scalar_planning_state,
                expr::{BinaryOp, Expr, FieldId, FieldPath, Function},
                logical_query_from_logical_inputs,
                prepare_query_model_scalar_planning_state_for_model_only,
                try_build_trivial_scalar_load_plan_for_model_only,
            },
        },
        schema::{SchemaInfo, ValidateError},
    },
    entity::EntityDeclaration,
    model::{
        entity::EntityModel,
        field::FieldKind,
        index::{IndexExpression, IndexKeyItem, IndexModel},
    },
    traits::Path,
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

crate::test_schema_entity! {
    ident = PlanValidateIndexedEntity,
    entity_name = "IndexedEntity",
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { tag: () => FieldKind::Text { max_len: None } },
        crate::test_field! { rank: () => FieldKind::Int64 },
    ],
    indexes = [&INDEX_MODEL],
}

crate::test_schema_entity! {
    ident = PlanValidateRecordFieldPathEntity,
    entity_name = "RecordFieldPathEntity",
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { profile: () => FieldKind::Structured { queryable: false } },
    ],
    indexes = [],
}

crate::test_schema_entity! {
    ident = PlanValidateListEntity,
    entity_name = "ListEntity",
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { tags: () => FieldKind::List(&FieldKind::Text { max_len: None }) },
    ],
    indexes = [],
}

crate::test_schema_entity! {
    ident = PlanValidateMapEntity,
    entity_name = "MapEntity",
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { metadata: () => FieldKind::Map {
            key: &FieldKind::Text { max_len: None },
            value: &FieldKind::Text { max_len: None },
        } },
    ],
    indexes = [],
}

crate::test_schema_entity! {
    ident = PlanValidateExpressionIndexedEntity,
    entity_name = "ExpressionIndexedEntity",
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { name: () => FieldKind::Text { max_len: None } },
        crate::test_field! { rank: () => FieldKind::Int64 },
    ],
    indexes = [&EXPRESSION_INDEX_MODEL],
}

fn model_with_index() -> &'static EntityModel {
    <PlanValidateIndexedEntity as EntityDeclaration>::MODEL
}

fn model_with_expression_index() -> &'static EntityModel {
    <PlanValidateExpressionIndexedEntity as EntityDeclaration>::MODEL
}

fn assert_trivial_scalar_fast_path_matches_general(query: QueryModel<'static, Value>) {
    let visible_indexes = VisibleIndexes::generated_model_only_for_test(query.model().indexes());
    let fast = try_build_trivial_scalar_load_plan_for_model_only(&query)
        .expect("trivial fast path should build")
        .expect("query should be fast-path eligible");
    let planning_state = prepare_query_model_scalar_planning_state_for_model_only(&query)
        .expect("general state should prepare");
    let general = build_query_model_plan_with_indexes_from_scalar_planning_state(
        &query,
        &visible_indexes,
        planning_state,
    )
    .expect("general plan should build");

    assert_eq!(
        fast, general,
        "trivial scalar fast path must preserve the general planner output",
    );
}

fn scalar_load_plan_with_predicate(
    predicate: Option<Predicate>,
    access: AccessPlan<Value>,
) -> AccessPlannedQuery {
    AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: predicate.is_some(),
            predicate,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access,
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_execution_planning_contract: None,
    }
}

fn finalized_predicate_pushdown_labels(
    predicate: Option<Predicate>,
    access: AccessPlan<Value>,
) -> (String, &'static str, &'static str) {
    let model = model_with_index();
    let mut plan = scalar_load_plan_with_predicate(predicate, access);

    plan.finalize_planner_route_profile_for_model(model);
    plan.finalize_static_execution_planning_contract_for_model_only(model)
        .expect("predicate pushdown diagnostics contract should finalize");

    (
        plan.predicate_pushdown_label(),
        plan.predicate_pushdown_outcome_label(),
        plan.predicate_pushdown_reason_label(),
    )
}

fn lower_name_order_term(direction: OrderDirection) -> crate::db::query::plan::OrderTerm {
    crate::db::query::plan::OrderTerm::new(
        Expr::FunctionCall {
            function: Function::Lower,
            args: vec![Expr::Field(FieldId::new("name"))],
        },
        direction,
    )
}

fn lower_tag_order_term(direction: OrderDirection) -> crate::db::query::plan::OrderTerm {
    crate::db::query::plan::OrderTerm::new(
        Expr::FunctionCall {
            function: Function::Lower,
            args: vec![Expr::Field(FieldId::new("tag"))],
        },
        direction,
    )
}

#[test]
fn finalized_static_contract_owns_predicate_pushdown_none_label() {
    assert_eq!(
        finalized_predicate_pushdown_labels(None, AccessPlan::path(AccessPath::FullScan)),
        ("none".to_string(), "none", "no_filter"),
    );
}

#[test]
fn finalized_static_contract_owns_applied_predicate_pushdown_label() {
    let key = Value::Ulid(Ulid::from_u128(4_201));
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "id",
        CompareOp::Eq,
        key.clone(),
        CoercionId::Strict,
    ));

    assert_eq!(
        finalized_predicate_pushdown_labels(
            Some(predicate),
            AccessPlan::path(AccessPath::ByKey(key)),
        ),
        (
            "applied(by_key)".to_string(),
            "partial",
            "residual_after_access",
        ),
    );
}

#[test]
fn finalized_static_contract_skips_preparation_compile_for_access_proven_predicate() {
    let values = vec![Value::from("alpha"), Value::from("beta")];
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "tag",
        CompareOp::In,
        Value::List(values.clone()),
        CoercionId::Strict,
    ));
    let access = AccessPlan::path(AccessPath::IndexMultiLookup {
        index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
            INDEX_MODEL,
        ),
        values,
    });
    let mut plan = scalar_load_plan_with_predicate(Some(predicate), access);

    plan.finalize_planner_route_profile_for_model(model_with_index());
    plan.finalize_static_execution_planning_contract_for_model_only(model_with_index())
        .expect("access-proven predicate should finalize");

    assert!(
        plan.execution_preparation_predicate().is_some(),
        "diagnostic preparation predicate should remain visible",
    );
    assert!(
        !plan.has_any_residual_filter(),
        "index multi-lookup should prove the IN predicate",
    );
    assert!(
        plan.execution_preparation_compiled_predicate().is_none(),
        "access-proven predicates should not compile unused preparation programs",
    );
    assert!(
        plan.effective_runtime_filter_program().is_none(),
        "access-proven predicates should not leave a runtime filter",
    );
}

#[test]
fn finalized_static_contract_owns_full_scan_fallback_pushdown_reason_labels() {
    let full_scan = || AccessPlan::path(AccessPath::FullScan);
    let non_strict = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Gt,
        Value::Int64(7),
        CoercionId::NumericWiden,
    ));
    let empty_starts_with = Predicate::Compare(ComparePredicate::with_coercion(
        "tag",
        CompareOp::StartsWith,
        Value::Text(String::new()),
        CoercionId::Strict,
    ));
    let is_null = Predicate::IsNull {
        field: "tag".to_string(),
    };
    let text_scan = Predicate::TextContains {
        field: "tag".to_string(),
        value: Value::Text("blue".to_string()),
    };

    assert_eq!(
        finalized_predicate_pushdown_labels(Some(non_strict), full_scan()),
        (
            "fallback(non_strict_compare_coercion)".to_string(),
            "fallback",
            "non_strict_compare_coercion",
        ),
    );
    assert_eq!(
        finalized_predicate_pushdown_labels(Some(empty_starts_with), full_scan()),
        (
            "fallback(starts_with_empty_prefix)".to_string(),
            "fallback",
            "starts_with_empty_prefix",
        ),
    );
    assert_eq!(
        finalized_predicate_pushdown_labels(Some(is_null), full_scan()),
        (
            "fallback(is_null_full_scan)".to_string(),
            "fallback",
            "is_null_full_scan",
        ),
    );
    assert_eq!(
        finalized_predicate_pushdown_labels(Some(text_scan), full_scan()),
        (
            "fallback(text_operator_full_scan)".to_string(),
            "fallback",
            "text_operator_full_scan",
        ),
    );
}

#[test]
fn trivial_scalar_load_fast_path_matches_general_plan_without_order() {
    let query = QueryModel::<Value>::new(model_with_index(), MissingRowPolicy::Ignore);

    assert_trivial_scalar_fast_path_matches_general(query);
}

#[test]
fn trivial_scalar_load_fast_path_matches_general_plan_for_primary_order() {
    let query = QueryModel::<Value>::new(model_with_index(), MissingRowPolicy::Ignore)
        .order_term(crate::db::asc("id"))
        .limit(2);

    assert_trivial_scalar_fast_path_matches_general(query);
}

#[test]
fn trivial_scalar_load_fast_path_rejects_secondary_order() {
    let query = QueryModel::<Value>::new(model_with_index(), MissingRowPolicy::Ignore)
        .order_term(crate::db::asc("tag"))
        .limit(2);
    let fast = try_build_trivial_scalar_load_plan_for_model_only(&query)
        .expect("eligibility check should not fail");

    assert!(
        fast.is_none(),
        "secondary-order loads must stay on the general planner path",
    );
}

#[test]
fn finalized_static_contract_carries_explicit_expression_only_residual_filter_state() {
    let model = model_with_expression_index();
    let mut plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: Some(Expr::FunctionCall {
                function: Function::StartsWith,
                args: vec![
                    Expr::FunctionCall {
                        function: Function::Replace,
                        args: vec![
                            Expr::Field(FieldId::new("name")),
                            Expr::Literal(Value::Text("a".to_string())),
                            Expr::Literal(Value::Text("A".to_string())),
                        ],
                    },
                    Expr::Literal(Value::Text("Al".to_string())),
                ],
            }),
            predicate_covers_filter_expr: false,
            predicate: None,
            order: None,
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
        static_execution_planning_contract: None,
    };

    plan.finalize_planner_route_profile_for_model(model);
    plan.finalize_static_execution_planning_contract_for_model_only(model)
        .expect("expression-only residual filter should finalize into static execution planning contract");

    assert!(
        plan.has_any_residual_filter(),
        "finalized plans should carry explicit residual state for expression-only filters",
    );
    assert!(
        plan.residual_filter_expr().is_some(),
        "finalized plans should expose one explicit residual filter expression",
    );
    assert!(
        plan.effective_execution_predicate().is_none(),
        "expression-only residual filters should not invent one derived residual predicate",
    );
    assert!(
        plan.effective_runtime_compiled_filter_expr().is_some(),
        "expression-only residual filters should compile onto the explicit expression runtime lane",
    );
    assert_eq!(
        plan.residual_filter_shape(),
        ResidualFilterShape::Expression,
        "expression-only residual filters should carry an explicit diagnostics shape",
    );
    assert_eq!(
        (
            plan.predicate_pushdown_label(),
            plan.predicate_pushdown_outcome_label(),
            plan.predicate_pushdown_reason_label(),
        ),
        ("none".to_string(), "fallback", "no_predicate_subset"),
        "expression-only filters should keep the compact label while carrying an explicit planner-owned fallback reason",
    );
}

#[test]
fn non_index_access_choice_seed_survives_finalize_access_choice_with_indexes() {
    let model = model_with_index();
    let mut plan = AccessPlannedQuery::from_logical_access_and_projection(
        LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        AccessPlan::path(AccessPath::ByKey(Value::Ulid(Ulid::from_u128(9_991)))),
        crate::db::query::plan::expr::ProjectionSelection::All,
    );

    assert_eq!(
        plan.access_choice().chosen_reason.code(),
        "by_key_access",
        "seeded by-key plans should start with the concrete non-index chosen reason",
    );

    plan.finalize_access_choice_for_model_only_with_indexes(model, model.indexes());

    assert_eq!(
        plan.access_choice().chosen_reason.code(),
        "by_key_access",
        "finalizing access-choice with visible indexes must preserve the seeded non-index chosen reason instead of reprojection through the generic fallback",
    );
}

#[test]
fn finalize_access_choice_prefers_stored_non_index_snapshot_over_shape_projection() {
    let model = model_with_index();
    let mut plan = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::ByKey(Value::Ulid(Ulid::from_u128(9_992)))),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_execution_planning_contract: None,
    };

    plan.finalize_access_choice_for_model_only_with_indexes(model, model.indexes());

    assert_eq!(
        plan.access_choice().chosen_reason.code(),
        "non_index_access",
        "finalize_access_choice_for_model_only_with_indexes should now trust the stored planner-owned non-index snapshot instead of reprojecting from the selected access shape",
    );
}

#[test]
fn finalized_static_contract_keeps_expression_residual_when_predicate_subset_also_exists() {
    let model = model_with_expression_index();
    let key = Value::Ulid(Ulid::from_u128(9_900));
    let mut plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: Some(Expr::Binary {
                op: BinaryOp::And,
                left: Box::new(Expr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(Expr::Field(FieldId::new("id"))),
                    right: Box::new(Expr::Literal(key.clone())),
                }),
                right: Box::new(Expr::FunctionCall {
                    function: Function::StartsWith,
                    args: vec![
                        Expr::FunctionCall {
                            function: Function::Replace,
                            args: vec![
                                Expr::Field(FieldId::new("name")),
                                Expr::Literal(Value::Text("a".to_string())),
                                Expr::Literal(Value::Text("A".to_string())),
                            ],
                        },
                        Expr::Literal(Value::Text("A".to_string())),
                    ],
                }),
            }),
            predicate_covers_filter_expr: false,
            predicate: Some(Predicate::Compare(ComparePredicate::with_coercion(
                "id",
                CompareOp::Eq,
                key.clone(),
                CoercionId::Strict,
            ))),
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::ByKey(key)),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_execution_planning_contract: None,
    };

    plan.finalize_planner_route_profile_for_model(model);
    plan.finalize_static_execution_planning_contract_for_model_only(model)
        .expect("mixed semantic filter should finalize into explicit residual state");

    assert!(
        plan.residual_filter_expr().is_some(),
        "semantic expression remainders must still survive even when one derived predicate subset also exists",
    );
    assert!(
        plan.effective_runtime_filter_program().is_some(),
        "mixed predicate-plus-expression filters should still compile one explicit residual runtime filter program",
    );
    assert_eq!(
        plan.residual_filter_shape(),
        ResidualFilterShape::ExpressionAndPredicate,
        "mixed predicate-plus-expression filters should carry an explicit diagnostics shape",
    );
    assert_eq!(
        (
            plan.predicate_pushdown_label(),
            plan.predicate_pushdown_outcome_label(),
            plan.predicate_pushdown_reason_label(),
        ),
        (
            "applied(by_key)".to_string(),
            "partial",
            "predicate_subset_does_not_cover_expression",
        ),
        "mixed expression/predicate filters should expose partial predicate pushdown with an explicit residual-expression reason",
    );
}

#[test]
fn plan_rejects_unorderable_field() {
    let model = <PlanValidateListEntity as EntityDeclaration>::MODEL;

    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![crate::db::query::plan::OrderTerm::field(
                    "tags",
                    OrderDirection::Asc,
                )],
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
        static_execution_planning_contract: None,
    };

    let err = validate_query_semantics(schema, model, &plan).expect_err("unorderable field");
    std::assert_matches!(err, PlanError::User(inner) if matches!(
        inner.as_ref(),
        PlanUserError::Order(inner)
            if matches!(
                inner.as_ref(),
                OrderPlanError::UnorderableField { term_index: 0 }
            )
    ));
}

#[test]
fn plan_accepts_nested_path_order_field() {
    let model = <PlanValidateRecordFieldPathEntity as EntityDeclaration>::MODEL;
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![
                    crate::db::query::plan::OrderTerm::new(
                        Expr::FieldPath(FieldPath::new("profile", vec!["rank".to_string()])),
                        OrderDirection::Desc,
                    ),
                    crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
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
        static_execution_planning_contract: None,
    };

    validate_query_semantics(schema, model, &plan).expect("nested order path should be accepted");
}

#[test]
fn plan_rejects_nested_path_inside_order_expression() {
    let model = <PlanValidateRecordFieldPathEntity as EntityDeclaration>::MODEL;
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![crate::db::query::plan::OrderTerm::new(
                    Expr::FunctionCall {
                        function: Function::Abs,
                        args: vec![Expr::FieldPath(FieldPath::new(
                            "profile",
                            vec!["rank".to_string()],
                        ))],
                    },
                    OrderDirection::Desc,
                )],
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
        static_execution_planning_contract: None,
    };

    let err = validate_query_semantics(schema, model, &plan).expect_err("nested order expression");
    assert!(
        matches!(&err, PlanError::User(inner) if matches!(
        inner.as_ref(),
        PlanUserError::Expr(inner)
            if matches!(
                inner.as_ref(),
                ExprPlanError::InvalidFunctionArgument { .. }
            )
        )),
        "{err:?}"
    );
}

#[test]
fn plan_rejects_duplicate_non_primary_order_field() {
    let model = <PlanValidateIndexedEntity as EntityDeclaration>::MODEL;
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![
                    crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
                    crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Desc),
                    crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
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
        static_execution_planning_contract: None,
    };

    let err = validate_query_semantics(schema, model, &plan)
        .expect_err("duplicate non-primary order field must fail");
    std::assert_matches!(err, PlanError::User(inner) if matches!(
        inner.as_ref(),
        PlanUserError::Order(inner)
            if matches!(
                inner.as_ref(),
                OrderPlanError::DuplicateOrderField {
                    first_term_index: 0,
                    duplicate_term_index: 1,
                }
            )
    ));
}

#[test]
fn plan_rejects_index_prefix_too_long() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                INDEX_MODEL,
            ),
            values: vec![Value::Text("a".to_string()), Value::Text("b".to_string())],
        }),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_execution_planning_contract: None,
    };

    let err = validate_query_semantics(schema, model, &plan).expect_err("index prefix too long");
    std::assert_matches!(err, PlanError::User(inner) if matches!(
        inner.as_ref(),
        PlanUserError::Access(inner)
            if matches!(inner.as_ref(), AccessPlanError::IndexPrefixTooLong { .. })
    ));
}

#[test]
fn plan_rejects_empty_index_prefix() {
    let model = model_with_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                INDEX_MODEL,
            ),
            values: vec![],
        }),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_execution_planning_contract: None,
    };

    let err = validate_query_semantics(schema, model, &plan).expect_err("index prefix empty");
    std::assert_matches!(err, PlanError::User(inner) if matches!(
        inner.as_ref(),
        PlanUserError::Access(inner)
            if matches!(inner.as_ref(), AccessPlanError::IndexPrefixEmpty)
    ));
}

#[test]
fn plan_accepts_model_based_validation() {
    let model = <PlanValidateIndexedEntity as EntityDeclaration>::MODEL;
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
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
        static_execution_planning_contract: None,
    };

    validate_query_semantics(schema, model, &plan).expect("valid plan");
}

#[test]
fn plan_rejects_empty_order_spec() {
    let model = <PlanValidateIndexedEntity as EntityDeclaration>::MODEL;
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
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
        static_execution_planning_contract: None,
    };

    let err = validate_query_semantics(schema, model, &plan).expect_err("empty order must fail");
    std::assert_matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PlanPolicyError::Policy(inner)
            if matches!(inner.as_ref(), PolicyPlanError::EmptyOrderSpec)
    ));
}

#[test]
fn delete_limit_requires_order() {
    let model = <PlanValidateIndexedEntity as EntityDeclaration>::MODEL;
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Delete(DeleteSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: Some(DeleteLimitSpec {
                limit: Some(10),
                offset: 0,
            }),
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::FullScan),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_execution_planning_contract: None,
    };

    let err = validate_query_semantics(schema, model, &plan)
        .expect_err("delete limit without order must fail");
    std::assert_matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PlanPolicyError::Policy(inner)
            if matches!(inner.as_ref(), PolicyPlanError::DeleteWindowRequiresOrder)
    ));
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
        index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
            INDEX_MODEL,
        ),
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
    let model = <PlanValidateIndexedEntity as EntityDeclaration>::MODEL;
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Delete(DeleteSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![crate::db::query::plan::OrderTerm::field(
                    "id",
                    OrderDirection::Asc,
                )],
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
        static_execution_planning_contract: None,
    };

    let err = validate_query_semantics(schema, model, &plan)
        .expect_err("delete plans must not carry pagination");
    std::assert_matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PlanPolicyError::Policy(inner)
            if matches!(inner.as_ref(), PolicyPlanError::DeletePlanWithPagination)
    ));
}

#[test]
fn load_plan_rejects_delete_limit() {
    let model = <PlanValidateIndexedEntity as EntityDeclaration>::MODEL;
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![crate::db::query::plan::OrderTerm::field(
                    "id",
                    OrderDirection::Asc,
                )],
            }),
            distinct: false,
            delete_limit: Some(DeleteLimitSpec {
                limit: Some(1),
                offset: 0,
            }),
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::path(AccessPath::FullScan),
        projection_selection: crate::db::query::plan::expr::ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_execution_planning_contract: None,
    };

    let err = validate_query_semantics(schema, model, &plan)
        .expect_err("load plans must not carry delete limits");
    std::assert_matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PlanPolicyError::Policy(inner)
            if matches!(inner.as_ref(), PolicyPlanError::LoadPlanWithDeleteLimit)
    ));
}

#[test]
fn plan_rejects_unordered_pagination() {
    let model = <PlanValidateIndexedEntity as EntityDeclaration>::MODEL;
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
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
        static_execution_planning_contract: None,
    };

    let err = validate_query_semantics(schema, model, &plan)
        .expect_err("pagination without ordering must be rejected");
    std::assert_matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PlanPolicyError::Policy(inner)
            if matches!(inner.as_ref(), PolicyPlanError::UnorderedPagination)
    ));
}

#[test]
fn plan_rejects_limit_without_order() {
    let model = <PlanValidateIndexedEntity as EntityDeclaration>::MODEL;
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
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
        static_execution_planning_contract: None,
    };

    let err = validate_query_semantics(schema, model, &plan)
        .expect_err("limit without ordering must be rejected");
    std::assert_matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PlanPolicyError::Policy(inner)
            if matches!(inner.as_ref(), PolicyPlanError::UnorderedPagination)
    ));
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
    let model = <PlanValidateIndexedEntity as EntityDeclaration>::MODEL;
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![crate::db::query::plan::OrderTerm::field(
                    "id",
                    OrderDirection::Asc,
                )],
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
        static_execution_planning_contract: None,
    };

    validate_query_semantics(schema, model, &plan).expect("ordered pagination is valid");
}

#[test]
fn plan_accepts_expression_order_when_access_satisfies_matching_index() {
    let model = model_with_expression_index();
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let mut plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![
                    lower_name_order_term(OrderDirection::Asc),
                    crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
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
        static_execution_planning_contract: None,
    };
    plan.finalize_planner_route_profile_for_model(model);

    validate_query_semantics(schema, model, &plan).expect(
        "expression order should validate when a matching index path already satisfies order",
    );
}

#[test]
fn plan_accepts_materialized_expression_order_without_access_satisfied_index_contract() {
    let model = <PlanValidateIndexedEntity as EntityDeclaration>::MODEL;
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let mut plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![
                    lower_tag_order_term(OrderDirection::Asc),
                    crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
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
        static_execution_planning_contract: None,
    };
    plan.finalize_planner_route_profile_for_model(model);

    validate_query_semantics(schema, model, &plan)
        .expect("materialized expression order should validate when access cannot satisfy order");
}

#[test]
fn planner_build_logical_plan_appends_primary_key_tie_break_for_non_unique_order_keys() {
    let model = <PlanValidateIndexedEntity as EntityDeclaration>::MODEL;
    let inputs = LogicalPlanningInputs::new(
        QueryMode::Load(LoadSpec::new()),
        None,
        false,
        Some(OrderSpec {
            fields: vec![crate::db::query::plan::OrderTerm::field(
                "tag",
                OrderDirection::Asc,
            )],
        }),
        false,
        None,
        None,
    );
    let logical_query = logical_query_from_logical_inputs(inputs, None, MissingRowPolicy::Ignore);
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let logical_plan = build_logical_plan(schema, logical_query);
    let order = logical_plan
        .scalar_semantics()
        .order
        .as_ref()
        .expect("logical plan should carry canonicalized order");

    assert_eq!(
        order.fields,
        vec![
            crate::db::query::plan::OrderTerm::field("tag", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
        "planner must append missing primary key tie-break components",
    );
}

#[test]
fn planner_build_logical_plan_preserves_composite_primary_key_order_component() {
    let model = super::model_with_composite_primary_key();
    let inputs = LogicalPlanningInputs::new(
        QueryMode::Load(LoadSpec::new()),
        None,
        false,
        Some(OrderSpec {
            fields: vec![crate::db::query::plan::OrderTerm::field(
                "local_id",
                OrderDirection::Desc,
            )],
        }),
        false,
        None,
        None,
    );
    let logical_query = logical_query_from_logical_inputs(inputs, None, MissingRowPolicy::Ignore);
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let logical_plan = build_logical_plan(schema, logical_query);
    let order = logical_plan
        .scalar_semantics()
        .order
        .as_ref()
        .expect("logical plan should carry canonicalized order");

    assert_eq!(
        order.fields,
        vec![
            crate::db::query::plan::OrderTerm::field("local_id", OrderDirection::Desc),
            crate::db::query::plan::OrderTerm::field("tenant_id", OrderDirection::Asc),
        ],
        "planner must preserve explicit composite primary-key order terms before appending missing tie-break components",
    );
}

#[test]
fn planner_build_logical_plan_preserves_grouped_order_without_primary_key_tie_break() {
    let model = <PlanValidateIndexedEntity as EntityDeclaration>::MODEL;
    let group_field =
        FieldSlot::resolve(model, "tag").expect("group field must resolve for grouped order test");
    let inputs = LogicalPlanningInputs::new(
        QueryMode::Load(LoadSpec::new()),
        None,
        false,
        Some(OrderSpec {
            fields: vec![
                crate::db::query::plan::OrderTerm::field("tag", OrderDirection::Asc),
                crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            ],
        }),
        false,
        Some(GroupSpec {
            group_fields: vec![group_field],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                input_expr: None,
                filter_expr: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        }),
        None,
    );
    let logical_query = logical_query_from_logical_inputs(inputs, None, MissingRowPolicy::Ignore);
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let LogicalPlan::Grouped(grouped) = build_logical_plan(schema, logical_query) else {
        panic!("grouped logical inputs should assemble one grouped logical plan");
    };
    let order = grouped
        .scalar
        .order
        .as_ref()
        .expect("grouped logical plan should carry explicit grouped order");

    assert_eq!(
        order.fields,
        vec![
            crate::db::query::plan::OrderTerm::field("tag", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
        ],
        "grouped logical plans must preserve declared grouped ordering without appending the row-level primary key tie-break",
    );
}

#[test]
fn grouped_plan_without_order_uses_grouped_canonical_ordering_contract() {
    let group_field = FieldSlot::resolve(
        <PlanValidateIndexedEntity as EntityDeclaration>::MODEL,
        "rank",
    )
    .expect("group field must resolve");
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: vec![group_field],
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Count,
                input_expr: None,
                filter_expr: None,
                distinct: false,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        });
    let continuation = grouped
        .planned_continuation_contract(<PlanValidateIndexedEntity as Path>::PATH)
        .expect("grouped plan should project continuation contract");

    assert_eq!(
        continuation.order_contract().ordering(),
        &ExecutionOrdering::Grouped(None),
        "grouped plans without explicit ORDER BY should use canonical grouped ordering contract",
    );
}

#[test]
fn plan_rejects_order_without_primary_key_tie_break() {
    let model = <PlanValidateIndexedEntity as EntityDeclaration>::MODEL;
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![crate::db::query::plan::OrderTerm::field(
                    "tag",
                    OrderDirection::Asc,
                )],
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
        static_execution_planning_contract: None,
    };

    let err = validate_query_semantics(schema, model, &plan).expect_err("missing PK tie-break");
    std::assert_matches!(err, PlanError::User(inner) if matches!(
        inner.as_ref(),
        PlanUserError::Order(inner)
            if matches!(
                inner.as_ref(),
                OrderPlanError::MissingPrimaryKeyTieBreak {
                    primary_key_index: 0,
                }
            )
    ));
}

#[test]
fn plan_rejects_map_field_predicates_during_planning_validation() {
    let model = <PlanValidateMapEntity as EntityDeclaration>::MODEL;
    let schema = SchemaInfo::cached_for_generated_entity_model(model);
    let plan: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate_covers_filter_expr: false,
            predicate: Some(Predicate::Compare(ComparePredicate::with_coercion(
                "metadata",
                CompareOp::Eq,
                Value::Text("payload".to_string()),
                CoercionId::Strict,
            ))),
            order: Some(OrderSpec {
                fields: vec![crate::db::query::plan::OrderTerm::field(
                    "id",
                    OrderDirection::Asc,
                )],
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
        static_execution_planning_contract: None,
    };

    let err = validate_query_semantics(schema, model, &plan)
        .expect_err("map field predicates must fail during planner validation");
    std::assert_matches!(err, PlanError::User(inner) if matches!(
        inner.as_ref(),
        PlanUserError::PredicateInvalid(inner)
            if matches!(
                inner.as_ref(),
                ValidateError::UnsupportedQueryFeature(UnsupportedQueryFeature::MapPredicate { field })
                    if field == "metadata"
            )
    ));
}
