use super::*;

#[test]
fn fingerprint_numeric_projection_alias_only_change_does_not_invalidate() {
    let plan: AccessPlannedQuery = full_scan_query();
    let numeric_projection = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Int(1))),
        },
        alias: None,
    }]);
    let alias_only_numeric_projection =
        ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Alias {
                expr: Box::new(Expr::Binary {
                    op: crate::db::query::plan::expr::BinaryOp::Add,
                    left: Box::new(Expr::Field(FieldId::new("rank"))),
                    right: Box::new(Expr::Literal(Value::Int(1))),
                }),
                name: Alias::new("rank_plus_one_expr"),
            },
            alias: Some(Alias::new("rank_plus_one")),
        }]);

    let semantic_fingerprint = fingerprint_with_projection(&plan, &numeric_projection);
    let alias_fingerprint = fingerprint_with_projection(&plan, &alias_only_numeric_projection);

    assert_eq!(
        semantic_fingerprint, alias_fingerprint,
        "numeric projection alias wrappers must not affect fingerprint identity",
    );
}

#[test]
fn fingerprint_numeric_projection_semantic_change_invalidates() {
    let plan: AccessPlannedQuery = full_scan_query();
    let projection_add_one = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Int(1))),
        },
        alias: None,
    }]);
    let projection_mul_one = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Mul,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Literal(Value::Int(1))),
        },
        alias: None,
    }]);

    let add_fingerprint = fingerprint_with_projection(&plan, &projection_add_one);
    let mul_fingerprint = fingerprint_with_projection(&plan, &projection_mul_one);

    assert_ne!(
        add_fingerprint, mul_fingerprint,
        "numeric projection semantic changes must invalidate fingerprint identity",
    );
}

#[test]
fn fingerprint_numeric_literal_decimal_scale_is_canonicalized() {
    let plan: AccessPlannedQuery = full_scan_query();
    let decimal_one_scale_1 = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Literal(Value::Decimal(Decimal::new(10, 1))),
        alias: None,
    }]);
    let decimal_one_scale_2 = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Literal(Value::Decimal(Decimal::new(100, 2))),
        alias: None,
    }]);

    assert_eq!(
        fingerprint_with_projection(&plan, &decimal_one_scale_1),
        fingerprint_with_projection(&plan, &decimal_one_scale_2),
        "decimal scale-only literal changes must not fragment fingerprint identity",
    );
}

#[test]
fn fingerprint_literal_numeric_subtype_remains_significant_when_observable() {
    let plan: AccessPlannedQuery = full_scan_query();
    let int_literal = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Literal(Value::Int(1)),
        alias: None,
    }]);
    let decimal_literal = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Literal(Value::Decimal(Decimal::new(10, 1))),
        alias: None,
    }]);

    assert_ne!(
        fingerprint_with_projection(&plan, &int_literal),
        fingerprint_with_projection(&plan, &decimal_literal),
        "top-level literal subtype remains observable and identity-significant",
    );
}

#[test]
fn fingerprint_numeric_promotion_paths_do_not_fragment() {
    let plan: AccessPlannedQuery = full_scan_query();
    let int_plus_int = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Literal(Value::Int(1))),
            right: Box::new(Expr::Literal(Value::Int(2))),
        },
        alias: None,
    }]);
    let int_plus_decimal = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Literal(Value::Int(1))),
            right: Box::new(Expr::Literal(Value::Decimal(Decimal::new(20, 1)))),
        },
        alias: None,
    }]);
    let decimal_plus_int = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Literal(Value::Decimal(Decimal::new(10, 1)))),
            right: Box::new(Expr::Literal(Value::Int(2))),
        },
        alias: None,
    }]);

    let fingerprint_int_plus_int = fingerprint_with_projection(&plan, &int_plus_int);
    let fingerprint_int_plus_decimal = fingerprint_with_projection(&plan, &int_plus_decimal);
    let fingerprint_decimal_plus_int = fingerprint_with_projection(&plan, &decimal_plus_int);

    assert_eq!(fingerprint_int_plus_int, fingerprint_int_plus_decimal);
    assert_eq!(fingerprint_int_plus_int, fingerprint_decimal_plus_int);
}

#[test]
fn fingerprint_commutative_operand_order_remains_significant_without_ast_normalization() {
    let plan: AccessPlannedQuery = full_scan_query();
    let rank_plus_score = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("rank"))),
            right: Box::new(Expr::Field(FieldId::new("score"))),
        },
        alias: None,
    }]);
    let score_plus_rank = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Field(FieldId::new("score"))),
            right: Box::new(Expr::Field(FieldId::new("rank"))),
        },
        alias: None,
    }]);

    assert_ne!(
        fingerprint_with_projection(&plan, &rank_plus_score),
        fingerprint_with_projection(&plan, &score_plus_rank),
        "fingerprint preserves AST operand order for commutative operators in v2",
    );
}

#[test]
fn fingerprint_aggregate_numeric_target_field_remains_significant() {
    let plan: AccessPlannedQuery = full_scan_query();
    let sum_rank = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Aggregate(sum("rank")),
        alias: None,
    }]);
    let sum_score = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Aggregate(sum("score")),
        alias: None,
    }]);

    assert_ne!(
        fingerprint_with_projection(&plan, &sum_rank),
        fingerprint_with_projection(&plan, &sum_score),
        "aggregate target field changes must invalidate fingerprint identity",
    );
}

#[test]
fn fingerprint_distinct_numeric_noop_paths_stay_stable() {
    let plan: AccessPlannedQuery = full_scan_query();
    let sum_distinct_plus_int_zero =
        ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Aggregate(sum("rank").distinct())),
                right: Box::new(Expr::Literal(Value::Int(0))),
            },
            alias: None,
        }]);
    let sum_distinct_plus_decimal_zero =
        ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Aggregate(sum("rank").distinct())),
                right: Box::new(Expr::Literal(Value::Decimal(Decimal::new(0, 1)))),
            },
            alias: None,
        }]);

    assert_eq!(
        fingerprint_with_projection(&plan, &sum_distinct_plus_int_zero),
        fingerprint_with_projection(&plan, &sum_distinct_plus_decimal_zero),
        "distinct numeric no-op literal subtype differences must not fragment fingerprint identity",
    );
}
