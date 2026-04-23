use super::*;
use crate::traits::ValueSurfaceEncode;

#[test]
fn plan_fingerprint_hasher_profile_seed_matches_manual_contract() {
    let mut helper = new_plan_fingerprint_hasher();
    helper.update(b"payload");

    let mut manual = Sha256::new();
    manual.update(b"planfp");
    manual.update(b"payload");

    assert_eq!(helper.finalize(), manual.finalize());
}

#[test]
fn continuation_signature_hasher_profile_seed_matches_manual_contract() {
    let mut helper = new_continuation_signature_hasher();
    helper.update(b"payload");

    let mut manual = Sha256::new();
    manual.update(b"contsig");
    manual.update(b"payload");

    assert_eq!(helper.finalize(), manual.finalize());
}

#[test]
fn fingerprint_is_deterministic_for_equivalent_predicates() {
    let id = Ulid::default();

    let predicate_a = Predicate::And(vec![
        Predicate::eq("id".to_string(), id.to_value()),
        Predicate::eq("other".to_string(), Value::Text("x".to_string())),
    ]);
    let predicate_b = Predicate::And(vec![
        Predicate::eq("other".to_string(), Value::Text("x".to_string())),
        Predicate::eq("id".to_string(), id.to_value()),
    ]);

    let mut plan_a: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().predicate = Some(predicate_a);

    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_b.scalar_plan_mut().predicate = Some(predicate_b);

    assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
}

#[test]
fn fingerprint_is_deterministic_for_by_keys() {
    let a = Ulid::from_u128(1);
    let b = Ulid::from_u128(2);

    let access_a = build_access_plan_from_keys(&KeyAccess::Many(vec![a, b, a]));
    let access_b = build_access_plan_from_keys(&KeyAccess::Many(vec![b, a]));

    let plan_a: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: access_a,
        projection_selection: ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_planning_shape: None,
    };
    let plan_b: AccessPlannedQuery = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            filter_expr: None,
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: access_b,
        projection_selection: ProjectionSelection::All,
        access_choice: crate::db::query::plan::AccessChoiceExplainSnapshot::non_index_access(),
        planner_route_profile: crate::db::query::plan::PlannerRouteProfile::seeded_unfinalized(
            false,
        ),
        static_planning_shape: None,
    };

    assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
}

#[test]
fn fingerprint_is_stable_for_full_scan() {
    let plan: AccessPlannedQuery = full_scan_query();
    let fingerprint_a = plan.fingerprint();
    let fingerprint_b = plan.fingerprint();
    assert_eq!(fingerprint_a, fingerprint_b);
}

#[test]
fn fingerprint_is_stable_for_equivalent_index_range_bounds() {
    const INDEX_FIELDS: [&str; 2] = ["group", "rank"];
    const INDEX: IndexModel = IndexModel::generated(
        "fingerprint::group_rank",
        "fingerprint::store",
        &INDEX_FIELDS,
        false,
    );

    let plan_a: AccessPlannedQuery = index_range_query(
        INDEX,
        vec![Value::Uint(7)],
        Bound::Included(Value::Uint(100)),
        Bound::Excluded(Value::Uint(200)),
    );
    let plan_b: AccessPlannedQuery = index_range_query(
        INDEX,
        vec![Value::Uint(7)],
        Bound::Included(Value::Uint(100)),
        Bound::Excluded(Value::Uint(200)),
    );

    assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
}

#[test]
fn fingerprint_and_signature_distinguish_different_expression_owned_filter_expr() {
    let mut plan_a: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().filter_expr = Some(Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("other"))),
        right: Box::new(Expr::Literal(Value::Text("Ada".to_string()))),
    });

    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_b.scalar_plan_mut().filter_expr = Some(Expr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("other"))),
        right: Box::new(Expr::Literal(Value::Text("Bea".to_string()))),
    });

    assert_ne!(
        plan_a.fingerprint(),
        plan_b.fingerprint(),
        "distinct expression-owned scalar filters must not alias on plan fingerprint",
    );
    assert_ne!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity"),
        "distinct expression-owned scalar filters must not alias on continuation identity",
    );
}

#[test]
fn fingerprint_and_signature_follow_canonical_searched_case_filter_identity() {
    let case_expr = Expr::Case {
        when_then_arms: vec![crate::db::query::plan::expr::CaseWhenArm::new(
            Expr::Field(FieldId::new("flag")),
            Expr::Literal(Value::Bool(true)),
        )],
        else_expr: Box::new(Expr::Field(FieldId::new("other_flag"))),
    };
    let boolean_expr = Expr::Binary {
        op: BinaryOp::Or,
        left: Box::new(Expr::FunctionCall {
            function: crate::db::query::plan::expr::Function::Coalesce,
            args: vec![
                Expr::Field(FieldId::new("flag")),
                Expr::Literal(Value::Bool(false)),
            ],
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::Unary {
                op: crate::db::query::plan::expr::UnaryOp::Not,
                expr: Box::new(Expr::FunctionCall {
                    function: crate::db::query::plan::expr::Function::Coalesce,
                    args: vec![
                        Expr::Field(FieldId::new("flag")),
                        Expr::Literal(Value::Bool(false)),
                    ],
                }),
            }),
            right: Box::new(Expr::Field(FieldId::new("other_flag"))),
        }),
    };

    let mut plan_a: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().filter_expr = Some(canonicalize_scalar_where_bool_expr(case_expr));

    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_b.scalar_plan_mut().filter_expr = Some(canonicalize_scalar_where_bool_expr(boolean_expr));

    assert_eq!(
        plan_a.fingerprint(),
        plan_b.fingerprint(),
        "canonical-equivalent searched CASE filters should share one plan fingerprint",
    );
    assert_eq!(
        plan_a.continuation_signature("tests::Entity"),
        plan_b.continuation_signature("tests::Entity"),
        "canonical-equivalent searched CASE filters should share one continuation identity",
    );
}

#[test]
fn explain_hash_matches_plan_fingerprint_for_expression_owned_filter_expr() {
    let mut plan: AccessPlannedQuery = full_scan_query();
    plan.scalar_plan_mut().filter_expr = Some(Expr::Binary {
        op: BinaryOp::And,
        left: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("other"))),
            right: Box::new(Expr::Literal(Value::Text("Ada".to_string()))),
        }),
        right: Box::new(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("id"))),
            right: Box::new(Expr::Literal(Ulid::default().to_value())),
        }),
    });

    let explain_fingerprint = encode_cursor(&fingerprint_with_projection(
        &plan,
        &plan.projection_spec_for_identity(),
    ));

    assert_eq!(
        plan.fingerprint().as_hex(),
        explain_fingerprint,
        "planned fingerprint and explain fingerprint must use the same canonical scalar filter authority",
    );
}
