//! Module: query::fingerprint::projection_hash
//! Responsibility: projection structural hash encoding over planner semantic trees.
//! Does not own: planner projection lowering or continuation profile ordering.
//! Boundary: semantic-only projection hash bytes independent from alias/explain metadata.

#[cfg(test)]
use crate::db::codec::new_hash_sha256;
#[cfg(test)]
use crate::db::numeric::coerce_numeric_decimal;
#[cfg(all(test, feature = "sql"))]
use crate::db::query::fingerprint::finalize_sha256_digest;
use crate::db::query::fingerprint::hash_parts::write_value;
use crate::db::query::plan::expr::UnaryOp;
use crate::db::query::{
    builder::aggregate::AggregateExpr,
    fingerprint::hash_parts::{write_str, write_tag, write_u32},
    plan::expr::{BinaryOp, Expr, ProjectionField, ProjectionSpec},
};
#[cfg(test)]
use crate::value::Value;
use sha2::Sha256;

const PROJECTION_STRUCTURAL_FINGERPRINT_TAG: u8 = 0x01;

const PROJECTION_FIELD_SCALAR_TAG: u8 = 0x10;

const EXPR_FIELD_TAG: u8 = 0x20;
const EXPR_LITERAL_TAG: u8 = 0x21;
const EXPR_UNARY_TAG: u8 = 0x22;
const EXPR_BINARY_TAG: u8 = 0x23;
const EXPR_AGGREGATE_TAG: u8 = 0x24;
const EXPR_FUNCTION_CALL_TAG: u8 = 0x25;
const EXPR_CASE_TAG: u8 = 0x26;

#[cfg(test)]
const NUMERIC_LITERAL_CANONICAL_DECIMAL_TAG: u8 = 0xA1;

const AGGREGATE_TARGET_ABSENT_TAG: u8 = 0x00;
const AGGREGATE_TARGET_PRESENT_TAG: u8 = 0x01;
const AGGREGATE_DISTINCT_TAG: u8 = 0x02;
const AGGREGATE_NON_DISTINCT_TAG: u8 = 0x03;
const AGGREGATE_FILTER_ABSENT_TAG: u8 = 0x04;
const AGGREGATE_FILTER_PRESENT_TAG: u8 = 0x05;

const UNARY_OP_NOT_TAG: u8 = 0x02;

const BINARY_OP_OR_TAG: u8 = 0x00;
const BINARY_OP_ADD_TAG: u8 = 0x01;
const BINARY_OP_SUB_TAG: u8 = 0x02;
const BINARY_OP_MUL_TAG: u8 = 0x03;
const BINARY_OP_DIV_TAG: u8 = 0x04;
const BINARY_OP_AND_TAG: u8 = 0x05;
const BINARY_OP_NE_TAG: u8 = 0x06;
const BINARY_OP_EQ_TAG: u8 = 0x07;
const BINARY_OP_LT_TAG: u8 = 0x08;
const BINARY_OP_LTE_TAG: u8 = 0x09;
const BINARY_OP_GT_TAG: u8 = 0x0A;
const BINARY_OP_GTE_TAG: u8 = 0x0B;

///
/// ProjectionHashShape
///
/// Canonical semantic projection hash shape that borrows one `ProjectionSpec`.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ProjectionHashShape<'a> {
    projection: &'a ProjectionSpec,
}

impl<'a> ProjectionHashShape<'a> {
    /// Build one semantic projection hash shape.
    #[must_use]
    pub(in crate::db) const fn semantic(projection: &'a ProjectionSpec) -> Self {
        Self { projection }
    }
}

impl ProjectionSpec {
    /// Compute one projection structural hash for SQL-facing tests.
    #[must_use]
    #[cfg(all(test, feature = "sql"))]
    pub(in crate::db) fn structural_hash_for_test(&self) -> [u8; 32] {
        let mut hasher = new_hash_sha256();
        hash_projection_structural_fingerprint(&mut hasher, self);
        finalize_sha256_digest(hasher)
    }
}

/// Hash one projection semantic shape using the current structural encoding.
#[expect(clippy::cast_possible_truncation)]
pub(in crate::db) fn hash_projection_structural_fingerprint(
    hasher: &mut Sha256,
    projection: &ProjectionSpec,
) {
    let shape = ProjectionHashShape::semantic(projection);

    write_tag(hasher, PROJECTION_STRUCTURAL_FINGERPRINT_TAG);
    write_u32(hasher, shape.projection.fields().count() as u32);
    for field in shape.projection.fields() {
        hash_projection_field(hasher, field);
    }
}

///
/// Hash one canonical scalar filter expression into the shared identity stream.
///
/// This is reused by fingerprint and continuation-signature hashing so those
/// surfaces consume the same planner-owned semantic filter shape as projection
/// hashing instead of inventing a second expression walker.
///
pub(in crate::db) fn hash_scalar_filter_expr_structural_fingerprint(
    hasher: &mut Sha256,
    expr: &Expr,
) {
    hash_expr(hasher, expr, false);
}

fn hash_projection_field(hasher: &mut Sha256, field: &ProjectionField) {
    // Field aliases are explain/display metadata and must not affect
    // projection semantic identity.
    write_tag(hasher, PROJECTION_FIELD_SCALAR_TAG);
    hash_expr(hasher, field.expr(), false);
}

fn hash_expr(hasher: &mut Sha256, expr: &Expr, numeric_literal_context: bool) {
    #[cfg(not(test))]
    let _ = numeric_literal_context;

    match expr {
        Expr::Field(field) => {
            write_tag(hasher, EXPR_FIELD_TAG);
            write_str(hasher, field.as_str());
        }
        Expr::Literal(value) => {
            write_tag(hasher, EXPR_LITERAL_TAG);
            #[cfg(test)]
            if numeric_literal_context {
                let Some(decimal) = coerce_numeric_decimal(value) else {
                    write_value(hasher, value);
                    return;
                };

                write_tag(hasher, NUMERIC_LITERAL_CANONICAL_DECIMAL_TAG);
                write_value(hasher, &Value::Decimal(decimal));
            } else {
                write_value(hasher, value);
            }
            #[cfg(not(test))]
            write_value(hasher, value);
        }
        Expr::FunctionCall { function, args } => {
            write_tag(hasher, EXPR_FUNCTION_CALL_TAG);
            write_str(hasher, function.sql_label());
            write_u32(hasher, u32::try_from(args.len()).unwrap_or(u32::MAX));
            for arg in args {
                hash_expr(hasher, arg, numeric_literal_context);
            }
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            write_tag(hasher, EXPR_CASE_TAG);
            write_u32(
                hasher,
                u32::try_from(when_then_arms.len()).unwrap_or(u32::MAX),
            );
            for arm in when_then_arms {
                hash_expr(hasher, arm.condition(), false);
                hash_expr(hasher, arm.result(), numeric_literal_context);
            }
            hash_expr(hasher, else_expr.as_ref(), numeric_literal_context);
        }
        Expr::Unary { op, expr } => {
            write_tag(hasher, EXPR_UNARY_TAG);
            write_tag(hasher, unary_op_tag(*op));
            hash_expr(hasher, expr.as_ref(), numeric_literal_context);
        }
        Expr::Binary { op, left, right } => {
            write_tag(hasher, EXPR_BINARY_TAG);
            write_tag(hasher, binary_op_tag(*op));
            // Expression hashing preserves AST operand order. Commutative
            // normalization is intentionally out-of-scope for structural identity.
            let binary_numeric_literal_context =
                numeric_literal_context || binary_op_uses_numeric_widen_semantics(*op);
            hash_expr(hasher, left.as_ref(), binary_numeric_literal_context);
            hash_expr(hasher, right.as_ref(), binary_numeric_literal_context);
        }
        Expr::Aggregate(aggregate) => {
            write_tag(hasher, EXPR_AGGREGATE_TAG);
            hash_aggregate_expr(hasher, aggregate);
        }
        #[cfg(test)]
        Expr::Alias { expr, name: _ } => {
            // Expression alias wrappers are presentation metadata only.
            hash_expr(hasher, expr.as_ref(), numeric_literal_context);
        }
    }
}

const fn binary_op_uses_numeric_widen_semantics(op: BinaryOp) -> bool {
    match op {
        BinaryOp::Or | BinaryOp::And => false,
        BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div => true,
    }
}

fn hash_aggregate_expr(hasher: &mut Sha256, aggregate: &AggregateExpr) {
    write_tag(hasher, aggregate.kind().fingerprint_tag());
    match (aggregate.target_field(), aggregate.input_expr()) {
        (Some(target_field), Some(Expr::Field(field_id))) if field_id.as_str() == target_field => {
            write_tag(hasher, AGGREGATE_TARGET_PRESENT_TAG);
            write_str(hasher, target_field);
        }
        (_, Some(input_expr)) => {
            write_tag(hasher, AGGREGATE_TARGET_PRESENT_TAG);
            hash_expr(hasher, input_expr, false);
        }
        (_, None) => write_tag(hasher, AGGREGATE_TARGET_ABSENT_TAG),
    }
    write_tag(
        hasher,
        if aggregate.is_distinct() {
            AGGREGATE_DISTINCT_TAG
        } else {
            AGGREGATE_NON_DISTINCT_TAG
        },
    );
    if let Some(filter_expr) = aggregate.filter_expr() {
        write_tag(hasher, AGGREGATE_FILTER_PRESENT_TAG);
        hash_expr(hasher, filter_expr, false);
    } else {
        write_tag(hasher, AGGREGATE_FILTER_ABSENT_TAG);
    }
}

const fn unary_op_tag(op: UnaryOp) -> u8 {
    match op {
        UnaryOp::Not => UNARY_OP_NOT_TAG,
    }
}

const fn binary_op_tag(op: BinaryOp) -> u8 {
    match op {
        BinaryOp::Or => BINARY_OP_OR_TAG,
        BinaryOp::And => BINARY_OP_AND_TAG,
        BinaryOp::Eq => BINARY_OP_EQ_TAG,
        BinaryOp::Ne => BINARY_OP_NE_TAG,
        BinaryOp::Lt => BINARY_OP_LT_TAG,
        BinaryOp::Lte => BINARY_OP_LTE_TAG,
        BinaryOp::Gt => BINARY_OP_GT_TAG,
        BinaryOp::Gte => BINARY_OP_GTE_TAG,
        BinaryOp::Add => BINARY_OP_ADD_TAG,
        BinaryOp::Sub => BINARY_OP_SUB_TAG,
        BinaryOp::Mul => BINARY_OP_MUL_TAG,
        BinaryOp::Div => BINARY_OP_DIV_TAG,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::{
        codec::new_hash_sha256,
        query::{
            builder::{count, sum},
            fingerprint::projection_hash::hash_projection_structural_fingerprint,
            plan::expr::{Alias, BinaryOp, Expr, FieldId, ProjectionField, ProjectionSpec},
        },
    };
    use crate::{types::Decimal, value::Value};

    fn hash_projection(spec: &ProjectionSpec) -> [u8; 32] {
        let mut hasher = new_hash_sha256();
        hash_projection_structural_fingerprint(&mut hasher, spec);
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
    fn aggregate_semantics_are_hash_significant() {
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
    fn numeric_literal_decimal_scale_is_canonicalized_for_hash_identity() {
        let decimal_one_scale_1 =
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
                expr: Expr::Literal(Value::Decimal(Decimal::new(10, 1))),
                alias: None,
            }]);
        let decimal_one_scale_2 =
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
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
            expr: Expr::Literal(Value::Int(1)),
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
    fn numeric_promotion_paths_do_not_fragment_hash_identity() {
        let int_plus_int = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Literal(Value::Int(1))),
                right: Box::new(Expr::Literal(Value::Int(2))),
            },
            alias: None,
        }]);
        let int_plus_decimal =
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
                expr: Expr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(Expr::Literal(Value::Int(1))),
                    right: Box::new(Expr::Literal(Value::Decimal(Decimal::new(20, 1)))),
                },
                alias: None,
            }]);
        let decimal_plus_int =
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
                expr: Expr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(Expr::Literal(Value::Decimal(Decimal::new(10, 1)))),
                    right: Box::new(Expr::Literal(Value::Int(2))),
                },
                alias: None,
            }]);

        let hash_int_plus_int = hash_projection(&int_plus_int);
        let hash_int_plus_decimal = hash_projection(&int_plus_decimal);
        let hash_decimal_plus_int = hash_projection(&decimal_plus_int);

        assert_eq!(hash_int_plus_int, hash_int_plus_decimal);
        assert_eq!(hash_int_plus_int, hash_decimal_plus_int);
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
        let avg_rank_plus_score =
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
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
        let avg_score_plus_rank =
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
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
    fn aggregate_numeric_promotion_noop_paths_stay_hash_stable() {
        let sum_plus_int_zero =
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
                expr: Expr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(Expr::Aggregate(sum("rank"))),
                    right: Box::new(Expr::Literal(Value::Int(0))),
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

        assert_eq!(
            hash_projection(&sum_plus_int_zero),
            hash_projection(&sum_plus_decimal_zero),
            "numeric no-op literal subtype differences must not fragment aggregate identity",
        );
    }

    #[test]
    fn distinct_numeric_promotion_noop_paths_stay_hash_stable() {
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
            hash_projection(&sum_distinct_plus_int_zero),
            hash_projection(&sum_distinct_plus_decimal_zero),
            "distinct numeric no-op literal subtype differences must not fragment identity",
        );
    }
}
