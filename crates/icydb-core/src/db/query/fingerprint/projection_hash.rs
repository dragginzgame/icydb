//! Module: query::fingerprint::projection_hash
//! Responsibility: projection structural hash encoding over planner semantic trees.
//! Does not own: planner projection lowering or continuation profile ordering.
//! Boundary: semantic-only projection hash bytes independent from alias/explain metadata.

use crate::{
    db::query::{
        builder::aggregate::AggregateExpr,
        plan::{
            AggregateKind,
            expr::{BinaryOp, Expr, ProjectionField, ProjectionSpec, UnaryOp},
        },
    },
    value::{Value, hash_value},
};
use sha2::{Digest, Sha256};

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
            hash_expr_v1(hasher, expr);
        }
    }
}

fn hash_expr_v1(hasher: &mut Sha256, expr: &Expr) {
    match expr {
        Expr::Field(field) => {
            write_tag(hasher, 0x20);
            write_str(hasher, field.as_str());
        }
        Expr::Literal(value) => {
            write_tag(hasher, 0x21);
            write_value(hasher, value);
        }
        Expr::Unary { op, expr } => {
            write_tag(hasher, 0x22);
            write_tag(hasher, unary_op_tag_v1(*op));
            hash_expr_v1(hasher, expr.as_ref());
        }
        Expr::Binary { op, left, right } => {
            write_tag(hasher, 0x23);
            write_tag(hasher, binary_op_tag_v1(*op));
            hash_expr_v1(hasher, left.as_ref());
            hash_expr_v1(hasher, right.as_ref());
        }
        Expr::Aggregate(aggregate) => {
            write_tag(hasher, 0x24);
            hash_aggregate_expr_v1(hasher, aggregate);
        }
        Expr::Alias { expr, name: _ } => {
            // Expression alias wrappers are presentation metadata only.
            hash_expr_v1(hasher, expr.as_ref());
        }
    }
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
    }
}

fn write_tag(hasher: &mut Sha256, tag: u8) {
    hasher.update([tag]);
}

fn write_u32(hasher: &mut Sha256, value: u32) {
    hasher.update(value.to_be_bytes());
}

#[expect(clippy::cast_possible_truncation)]
fn write_str(hasher: &mut Sha256, value: &str) {
    write_u32(hasher, value.len() as u32);
    hasher.update(value.as_bytes());
}

fn write_value(hasher: &mut Sha256, value: &Value) {
    match hash_value(value) {
        Ok(digest) => hasher.update(digest),
        Err(err) => {
            write_tag(hasher, 0xEE);
            write_str(hasher, &err.display_with_class());
        }
    }
}

#[cfg(test)]
pub(in crate::db) fn projection_hash_for_test(projection: &ProjectionSpec) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hash_projection_structural_fingerprint_v1(&mut hasher, projection);
    let digest = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);

    out
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::query::{
        builder::{count, sum},
        fingerprint::projection_hash::hash_projection_structural_fingerprint_v1,
        plan::expr::{Alias, Expr, FieldId, ProjectionField, ProjectionSpec},
    };
    use sha2::{Digest, Sha256};

    fn hash_projection(spec: &ProjectionSpec) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hash_projection_structural_fingerprint_v1(&mut hasher, spec);
        let digest = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&digest);
        out
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
    fn projection_hash_encoder_signature_accepts_projection_semantics_only() {
        let hash: fn(&mut Sha256, &ProjectionSpec) = hash_projection_structural_fingerprint_v1;

        let _ = hash;
    }
}
