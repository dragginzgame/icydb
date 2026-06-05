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
use crate::db::query::fingerprint::hash_sections::write_value;
use crate::db::query::plan::expr::UnaryOp;
use crate::db::query::{
    builder::aggregate::AggregateExpr,
    fingerprint::hash_sections::{write_str, write_tag, write_u32},
    plan::{
        AggregateIdentity,
        expr::{BinaryOp, Expr, ProjectionField, ProjectionSpec},
    },
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
const EXPR_FIELD_PATH_TAG: u8 = 0x27;

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

/// Hash one projection identity shape using the current structural encoding.
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
        Expr::FieldPath(path) => {
            write_tag(hasher, EXPR_FIELD_PATH_TAG);
            write_str(hasher, path.root().as_str());
            write_u32(
                hasher,
                u32::try_from(path.segments().len()).unwrap_or(u32::MAX),
            );
            for segment in path.segments() {
                write_str(hasher, segment);
            }
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
            write_str(hasher, function.canonical_label());
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
    let identity = AggregateIdentity::from_aggregate_expr(aggregate);

    write_tag(hasher, identity.kind().fingerprint_tag());
    match (identity.target_field(), identity.input_expr()) {
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
        if identity.distinct() {
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
mod tests;
