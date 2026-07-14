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
        "query intent should preserve the derived predicate contract while expression ownership is threaded through planning",
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
