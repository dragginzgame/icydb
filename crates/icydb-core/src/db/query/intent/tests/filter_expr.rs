use super::support::*;

#[test]
fn filter_expr_build_plan_model_preserves_scalar_filter_expression_ownership() {
    let plan = QueryModel::<Ulid>::new(basic_model(), MissingRowPolicy::Ignore)
        .filter(FieldRef::new("name").eq("Ada"))
        .build_plan_model()
        .expect("fluent filter expression plan should build");

    assert!(
        matches!(
            plan.scalar_plan().filter_expr.as_ref(),
            Some(Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Eq,
                left,
                right,
            }) if left.as_ref() == &Expr::Field(crate::db::query::plan::expr::FieldId::new("name"))
                && right.as_ref() == &Expr::Literal(Value::Text("Ada".to_string()))
        ),
        "scalar plans should now preserve one planner-owned semantic filter expression alongside the derived predicate",
    );
    assert!(
        plan.scalar_plan().predicate.is_some(),
        "the first 0.100 slice should preserve the existing derived predicate contract while expression ownership is being threaded through planning",
    );
}

#[test]
fn build_plan_model_rejects_map_field_predicates_before_planning() {
    let intent = QueryModel::<Ulid>::new(&MAP_PLAN_MODEL, MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "attributes",
            CompareOp::Eq,
            Value::Map(Vec::new()),
            crate::db::predicate::CoercionId::Strict,
        )));

    let err = intent
        .build_plan_model()
        .expect_err("map field predicates must be rejected before planning");
    assert!(query_error_is_predicate_validation_error(&err, |inner| {
        matches!(
            inner,
            crate::db::schema::ValidateError::UnsupportedQueryFeature(
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
        .filter_predicate(predicate);
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
        .filter_predicate(predicate)
        .build_plan_model()
        .expect_err("strict enum with wrong path should fail");
    assert!(matches!(
        err,
        QueryError::Validate(err)
            if matches!(
                err.as_ref(),
                crate::db::schema::ValidateError::InvalidLiteral {
                    field,
                    ..
                } if field == "stage"
            )
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
        .filter_predicate(predicate)
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
