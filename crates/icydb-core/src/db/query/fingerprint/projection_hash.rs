//! Module: query::fingerprint::projection_hash
//! Responsibility: projection structural hash encoding over planner semantic trees.
//! Does not own: planner projection lowering or continuation profile ordering.
//! Boundary: semantic-only projection hash bytes independent from alias/explain metadata.

#[cfg(test)]
use crate::{db::codec::new_hash_sha256, db::query::fingerprint::finalize_sha256_digest};
use crate::{
    db::numeric::coerce_numeric_decimal,
    db::query::{
        builder::aggregate::AggregateExpr,
        fingerprint::hash_parts::{write_str, write_tag, write_u32, write_value},
        plan::{
            AggregateKind,
            expr::{BinaryOp, Expr, ProjectionField, ProjectionSpec, UnaryOp},
        },
    },
    value::Value,
};
use sha2::Sha256;

///
/// ProjectionHashShape
///
/// Canonical semantic projection hash shape that borrows one `ProjectionSpec`.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ProjectionHashShape<'a> {
    projection: &'a ProjectionSpec,
}

impl<'a> ProjectionHashShape<'a> {
    /// Build one semantic projection hash shape.
    #[must_use]
    pub(super) const fn semantic(projection: &'a ProjectionSpec) -> Self {
        Self { projection }
    }
}

/// Hash one projection semantic shape using the v1 structural encoding.
#[expect(clippy::cast_possible_truncation)]
pub(super) fn hash_projection_structural_fingerprint_v1(
    hasher: &mut Sha256,
    projection: &ProjectionSpec,
) {
    const PROJECTION_STRUCTURAL_FINGERPRINT_V1: u8 = 0x01;
    let shape = ProjectionHashShape::semantic(projection);

    write_tag(hasher, PROJECTION_STRUCTURAL_FINGERPRINT_V1);
    write_u32(hasher, shape.projection.fields().count() as u32);
    for field in shape.projection.fields() {
        hash_projection_field_v1(hasher, field);
    }
}

fn hash_projection_field_v1(hasher: &mut Sha256, field: &ProjectionField) {
    match field {
        ProjectionField::Scalar { expr, alias: _ } => {
            // Field aliases are explain/display metadata and must not affect
            // projection semantic identity.
            write_tag(hasher, 0x10);
            hash_expr_v1(hasher, expr, false);
        }
    }
}

fn hash_expr_v1(hasher: &mut Sha256, expr: &Expr, numeric_literal_context: bool) {
    match expr {
        Expr::Field(field) => {
            write_tag(hasher, 0x20);
            write_str(hasher, field.as_str());
        }
        Expr::Literal(value) => {
            write_tag(hasher, 0x21);
            if numeric_literal_context {
                hash_numeric_literal_semantic_v1(hasher, value);
            } else {
                write_value(hasher, value);
            }
        }
        Expr::Unary { op, expr } => {
            write_tag(hasher, 0x22);
            write_tag(hasher, unary_op_tag_v1(*op));
            hash_expr_v1(
                hasher,
                expr.as_ref(),
                matches!(op, UnaryOp::Neg) || numeric_literal_context,
            );
        }
        Expr::Binary { op, left, right } => {
            write_tag(hasher, 0x23);
            write_tag(hasher, binary_op_tag_v1(*op));
            // Expression hashing preserves AST operand order. Commutative
            // normalization is intentionally out-of-scope for v1 identity.
            let binary_numeric_literal_context =
                numeric_literal_context || binary_op_uses_numeric_widen_semantics(*op);
            hash_expr_v1(hasher, left.as_ref(), binary_numeric_literal_context);
            hash_expr_v1(hasher, right.as_ref(), binary_numeric_literal_context);
        }
        Expr::Aggregate(aggregate) => {
            write_tag(hasher, 0x24);
            hash_aggregate_expr_v1(hasher, aggregate);
        }
        Expr::Alias { expr, name: _ } => {
            // Expression alias wrappers are presentation metadata only.
            hash_expr_v1(hasher, expr.as_ref(), numeric_literal_context);
        }
    }
}

// Canonicalize numeric-coercible literal leaves when they appear under numeric
// operators so promotion-path representation differences do not fragment identity.
fn hash_numeric_literal_semantic_v1(hasher: &mut Sha256, value: &Value) {
    let Some(decimal) = coerce_numeric_decimal(value) else {
        write_value(hasher, value);
        return;
    };

    write_tag(hasher, 0xA1);
    write_value(hasher, &Value::Decimal(decimal));
}

const fn binary_op_uses_numeric_widen_semantics(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Add
            | BinaryOp::Sub
            | BinaryOp::Mul
            | BinaryOp::Div
            | BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::Lt
            | BinaryOp::Lte
            | BinaryOp::Gt
            | BinaryOp::Gte
    )
}

fn hash_aggregate_expr_v1(hasher: &mut Sha256, aggregate: &AggregateExpr) {
    write_tag(hasher, aggregate_kind_tag_v1(aggregate.kind()));
    match aggregate.target_field() {
        Some(field) => {
            write_tag(hasher, 0x01);
            write_str(hasher, field);
        }
        None => write_tag(hasher, 0x00),
    }
    write_tag(hasher, if aggregate.is_distinct() { 0x02 } else { 0x03 });
}

const fn unary_op_tag_v1(op: UnaryOp) -> u8 {
    match op {
        UnaryOp::Neg => 0x01,
        UnaryOp::Not => 0x02,
    }
}

const fn binary_op_tag_v1(op: BinaryOp) -> u8 {
    match op {
        BinaryOp::Add => 0x01,
        BinaryOp::Sub => 0x02,
        BinaryOp::Mul => 0x03,
        BinaryOp::Div => 0x04,
        BinaryOp::And => 0x05,
        BinaryOp::Or => 0x06,
        BinaryOp::Eq => 0x07,
        BinaryOp::Ne => 0x08,
        BinaryOp::Lt => 0x09,
        BinaryOp::Lte => 0x0A,
        BinaryOp::Gt => 0x0B,
        BinaryOp::Gte => 0x0C,
    }
}

const fn aggregate_kind_tag_v1(kind: AggregateKind) -> u8 {
    match kind {
        AggregateKind::Count => 0x01,
        AggregateKind::Sum => 0x02,
        AggregateKind::Exists => 0x03,
        AggregateKind::Min => 0x04,
        AggregateKind::Max => 0x05,
        AggregateKind::First => 0x06,
        AggregateKind::Last => 0x07,
        AggregateKind::Avg => 0x08,
    }
}

#[cfg(test)]
pub(in crate::db) fn projection_hash_for_test(projection: &ProjectionSpec) -> [u8; 32] {
    let mut hasher = new_hash_sha256();
    hash_projection_structural_fingerprint_v1(&mut hasher, projection);
    finalize_sha256_digest(hasher)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::query::{
        builder::{count, sum},
        fingerprint::projection_hash::hash_projection_structural_fingerprint_v1,
        plan::expr::{Alias, BinaryOp, Expr, FieldId, ProjectionField, ProjectionSpec},
    };
    use crate::{types::Decimal, value::Value};
    use sha2::Sha256;

    fn hash_projection(spec: &ProjectionSpec) -> [u8; 32] {
        let mut hasher = crate::db::codec::new_hash_sha256();
        hash_projection_structural_fingerprint_v1(&mut hasher, spec);
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
            "projection hash preserves AST operand order for commutative operators in v1",
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

    #[test]
    fn projection_hash_encoder_signature_accepts_projection_semantics_only() {
        let hash: fn(&mut Sha256, &ProjectionSpec) = hash_projection_structural_fingerprint_v1;

        let _ = hash;
    }
}
