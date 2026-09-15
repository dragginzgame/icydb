use crate::db::{
    codec::new_hash_sha256,
    query::{
        builder::{count, count_by, min_by, sum},
        fingerprint::projection_hash::hash_projection_structural_fingerprint,
        plan::expr::{Alias, BinaryOp, Expr, FieldId, ProjectionField, ProjectionSpec},
    },
};
use crate::{types::Decimal, value::Value};
use sha2::Digest;

#[test]
fn projection_hash_propagates_nested_aggregate_failure() {
    use crate::value::{test_hash_budget_error, with_test_hash_override};
    let spec = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Aggregate(count().with_filter_expr(Expr::Literal(Value::Bool(true)))),
        alias: None,
    }]);
    with_test_hash_override(Err(test_hash_budget_error), || {
        let error =
            hash_projection_structural_fingerprint(&mut new_hash_sha256(), &spec).unwrap_err();
        assert_eq!(error.diagnostic(), test_hash_budget_error().diagnostic());
        assert_eq!(
            error.diagnostic_facts(),
            test_hash_budget_error().diagnostic_facts()
        );
    });
}

fn hash_projection(spec: &ProjectionSpec) -> [u8; 32] {
    let mut hasher = new_hash_sha256();
    hash_projection_structural_fingerprint(&mut hasher, spec).unwrap();
    super::super::finalize_sha256_digest(hasher)
}

#[test]
fn alias_is_excluded_from_projection_semantic_hash_identity() {
    let base = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Field(FieldId::new("rank")),
        alias: None,
    }]);
    let aliased = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Alias {
            expr: Box::new(Expr::Field(FieldId::new("rank"))),
            name: Alias::new("rank_expr"),
        },
        alias: Some(Alias::new("rank_column")),
    }]);

    assert_eq!(hash_projection(&base), hash_projection(&aliased));
}

#[test]
fn projection_field_order_remains_hash_significant() {
    let in_order = ProjectionSpec::from_fields_for_test(vec![
        ProjectionField::Scalar {
            expr: Expr::Field(FieldId::new("id")),
            alias: None,
        },
        ProjectionField::Scalar {
            expr: Expr::Field(FieldId::new("rank")),
            alias: None,
        },
    ]);
    let swapped_order = ProjectionSpec::from_fields_for_test(vec![
        ProjectionField::Scalar {
            expr: Expr::Field(FieldId::new("rank")),
            alias: None,
        },
        ProjectionField::Scalar {
            expr: Expr::Field(FieldId::new("id")),
            alias: None,
        },
    ]);

    assert_ne!(hash_projection(&in_order), hash_projection(&swapped_order));
}

#[test]
fn aggregate_identity_is_hash_significant() {
    let sum_rank = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Aggregate(sum("rank")),
        alias: None,
    }]);
    let count_all = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Aggregate(count()),
        alias: None,
    }]);

    assert_ne!(hash_projection(&sum_rank), hash_projection(&count_all));
}

#[test]
fn extrema_distinct_modifier_is_not_projection_hash_significant() {
    let min_rank = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Aggregate(min_by("rank")),
        alias: None,
    }]);
    let min_distinct_rank = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Aggregate(min_by("rank").distinct()),
        alias: None,
    }]);

    assert_eq!(
        hash_projection(&min_rank),
        hash_projection(&min_distinct_rank)
    );
}

#[test]
fn count_distinct_modifier_remains_projection_hash_significant() {
    let count_rank = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Aggregate(count_by("rank")),
        alias: None,
    }]);
    let count_distinct_rank = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Aggregate(count_by("rank").distinct()),
        alias: None,
    }]);

    assert_ne!(
        hash_projection(&count_rank),
        hash_projection(&count_distinct_rank),
    );
}

#[test]
fn numeric_literal_decimal_scale_is_canonicalized_for_hash_identity() {
    let decimal_one_scale_1 = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Literal(Value::Decimal(Decimal::new(10, 1))),
        alias: None,
    }]);
    let decimal_one_scale_2 = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Literal(Value::Decimal(Decimal::new(100, 2))),
        alias: None,
    }]);

    assert_eq!(
        hash_projection(&decimal_one_scale_1),
        hash_projection(&decimal_one_scale_2),
        "decimal literal scale-only differences must not fragment identity",
    );
}

#[test]
fn literal_numeric_subtype_remains_hash_significant_when_observable() {
    let int_literal = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Literal(Value::Int64(1)),
        alias: None,
    }]);
    let decimal_literal = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Literal(Value::Decimal(Decimal::new(10, 1))),
        alias: None,
    }]);

    assert_ne!(
        hash_projection(&int_literal),
        hash_projection(&decimal_literal),
        "top-level literal subtype is observable and remains identity-significant",
    );
}

#[test]
fn raw_arithmetic_literal_subtypes_remain_hash_significant() {
    let int_plus_int = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Literal(Value::Int64(1))),
            right: Box::new(Expr::Literal(Value::Int64(2))),
        },
        alias: None,
    }]);
    let int_plus_decimal = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Literal(Value::Int64(1))),
            right: Box::new(Expr::Literal(Value::Decimal(Decimal::new(20, 1)))),
        },
        alias: None,
    }]);
    let decimal_plus_int = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Literal(Value::Decimal(Decimal::new(10, 1)))),
            right: Box::new(Expr::Literal(Value::Int64(2))),
        },
        alias: None,
    }]);

    let hash_int_plus_int = hash_projection(&int_plus_int);
    let hash_int_plus_decimal = hash_projection(&int_plus_decimal);
    let hash_decimal_plus_int = hash_projection(&decimal_plus_int);

    assert_ne!(hash_int_plus_int, hash_int_plus_decimal);
    assert_ne!(hash_int_plus_int, hash_decimal_plus_int);
}

#[test]
fn commutative_operand_order_remains_hash_significant_without_ast_normalization() {
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
        hash_projection(&rank_plus_score),
        hash_projection(&score_plus_rank),
        "projection hash preserves AST operand order for commutative operators in the current profile",
    );
}

#[test]
fn aggregate_numeric_target_field_remains_hash_significant() {
    let sum_rank = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Aggregate(sum("rank")),
        alias: None,
    }]);
    let sum_score = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Aggregate(sum("score")),
        alias: None,
    }]);

    assert_ne!(
        hash_projection(&sum_rank),
        hash_projection(&sum_score),
        "aggregate target-field semantic changes must invalidate identity",
    );
}

#[test]
fn aggregate_input_expression_shape_remains_hash_significant() {
    let avg_rank_plus_score = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Aggregate(
            crate::db::query::builder::aggregate::AggregateExpr::from_expression_input(
                crate::db::query::plan::AggregateKind::Avg,
                Expr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(Expr::Field(FieldId::new("rank"))),
                    right: Box::new(Expr::Field(FieldId::new("score"))),
                },
            ),
        ),
        alias: None,
    }]);
    let avg_score_plus_rank = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Aggregate(
            crate::db::query::builder::aggregate::AggregateExpr::from_expression_input(
                crate::db::query::plan::AggregateKind::Avg,
                Expr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(Expr::Field(FieldId::new("score"))),
                    right: Box::new(Expr::Field(FieldId::new("rank"))),
                },
            ),
        ),
        alias: None,
    }]);

    assert_ne!(
        hash_projection(&avg_rank_plus_score),
        hash_projection(&avg_score_plus_rank),
        "aggregate input expression structure must remain part of projection identity",
    );
}

#[test]
fn raw_literal_subtypes_around_aggregates_remain_hash_significant() {
    let sum_plus_int_zero = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Aggregate(sum("rank"))),
            right: Box::new(Expr::Literal(Value::Int64(0))),
        },
        alias: None,
    }]);
    let sum_plus_decimal_zero =
        ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Aggregate(sum("rank"))),
                right: Box::new(Expr::Literal(Value::Decimal(Decimal::new(0, 1)))),
            },
            alias: None,
        }]);

    assert_ne!(
        hash_projection(&sum_plus_int_zero),
        hash_projection(&sum_plus_decimal_zero),
        "structural hashing preserves operand types even for numerically equal literals",
    );
}

#[test]
fn raw_literal_subtypes_around_distinct_aggregates_remain_hash_significant() {
    let sum_distinct_plus_int_zero =
        ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Aggregate(sum("rank").distinct())),
                right: Box::new(Expr::Literal(Value::Int64(0))),
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

    assert_ne!(
        hash_projection(&sum_distinct_plus_int_zero),
        hash_projection(&sum_distinct_plus_decimal_zero),
        "DISTINCT does not change structural literal encoding",
    );
}

#[test]
fn projection_filter_and_mutation_scope_preserve_structural_literal_bytes() {
    use crate::db::codec::{write_hash_tag_u8, write_hash_u32};

    let left = Value::Int64(1);
    let right = Value::Decimal(Decimal::new(20, 1));
    let expr = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Literal(left.clone())),
        right: Box::new(Expr::Literal(right.clone())),
    };
    // Current binary-add framing: preserve each admitted operand's value
    // digest. Preparation, not this encoder, owns numeric normalization.
    let mut bytes = vec![0x23, 0x01, 0x21];
    bytes.extend_from_slice(&crate::value::hash_value(&left).unwrap());
    bytes.push(0x21);
    bytes.extend_from_slice(&crate::value::hash_value(&right).unwrap());

    let mut expected = new_hash_sha256();
    write_hash_tag_u8(&mut expected, 0x01);
    write_hash_u32(&mut expected, 1);
    write_hash_tag_u8(&mut expected, 0x10);
    expected.update(&bytes);
    let spec = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
        expr: expr.clone(),
        alias: None,
    }]);
    assert_eq!(
        hash_projection(&spec),
        super::super::finalize_sha256_digest(expected)
    );

    let mut expected = new_hash_sha256();
    expected.update(&bytes);
    let mut actual = new_hash_sha256();
    super::hash_scalar_filter_expr_structural_fingerprint(&mut actual, &expr).unwrap();
    assert_eq!(
        super::super::finalize_sha256_digest(actual),
        super::super::finalize_sha256_digest(expected)
    );

    #[cfg(feature = "sql")]
    {
        let mut expected = crate::db::codec::new_hash_sha256_prefixed(b"resumable_update_scope_v1");
        expected.update(bytes);
        assert_eq!(
            super::super::resumable_update_scope_fingerprint(&expr).unwrap(),
            super::super::finalize_sha256_digest(expected),
        );
    }
}

#[test]
fn borrowed_aggregate_identity_preserves_field_filter_and_count_framing() {
    use crate::{
        db::{
            codec::{write_hash_str_u32, write_hash_u32},
            query::{builder::AggregateExpr, plan::AggregateKind},
        },
        value::{test_hash_budget_error, with_test_hash_override},
    };

    let projection = |aggregate| {
        ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Aggregate(aggregate),
            alias: None,
        }])
    };
    let filtered = sum("rank").with_filter_expr(Expr::Literal(Value::Bool(true)));
    let mut expected = new_hash_sha256();
    expected.update([0x01]);
    write_hash_u32(&mut expected, 1);
    // Scalar projection, aggregate, SUM, direct target, non-distinct, filter.
    expected.update([0x10, 0x24, 0x02, 0x01]);
    write_hash_str_u32(&mut expected, "rank");
    expected.update([0x03, 0x05, 0x21]);
    expected.update(crate::value::hash_value(&Value::Bool(true)).unwrap());
    assert_eq!(
        hash_projection(&projection(filtered)),
        super::super::finalize_sha256_digest(expected)
    );

    let count_literal = AggregateExpr::from_expression_input(
        AggregateKind::Count,
        Expr::Literal(Value::List(vec![Value::Text("value".repeat(1024))])),
    );
    let mut expected = new_hash_sha256();
    expected.update([0x01]);
    write_hash_u32(&mut expected, 1);
    // COUNT(non-null literal): no input, non-distinct, no filter.
    expected.update([0x10, 0x24, 0x01, 0x00, 0x03, 0x04]);
    let expected = super::super::finalize_sha256_digest(expected);
    let spec = projection(count_literal);
    let before = spec.clone();
    with_test_hash_override(Err(test_hash_budget_error), || {
        for _ in 0..2 {
            assert_eq!(hash_projection(&spec), expected);
        }
    });
    assert_eq!(spec, before);
}
