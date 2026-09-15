//! Module: query::fingerprint::projection_hash
//! Responsibility: projection structural hash encoding over planner semantic trees.
//! Does not own: planner projection lowering or continuation profile ordering.
//! Boundary: semantic-only projection hash bytes independent from alias/explain metadata.

pub(in crate::db::query::fingerprint) mod admission;

use crate::db::query::fingerprint::hash_sections::write_value;
use crate::db::query::plan::expr::UnaryOp;
use crate::db::query::{
    builder::aggregate::AggregateExpr,
    fingerprint::hash_sections::{write_str, write_tag, write_u32},
    plan::{
        AggregateSemanticKeyRef,
        expr::{BinaryOp, Expr, ProjectionField, ProjectionSpec},
    },
};
use crate::error::InternalError;
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

const AGGREGATE_TARGET_ABSENT_TAG: u8 = 0x00;
const AGGREGATE_TARGET_PRESENT_TAG: u8 = 0x01;
const AGGREGATE_DISTINCT_TAG: u8 = 0x02;
const AGGREGATE_NON_DISTINCT_TAG: u8 = 0x03;
const AGGREGATE_FILTER_ABSENT_TAG: u8 = 0x04;
const AGGREGATE_FILTER_PRESENT_TAG: u8 = 0x05;

const UNARY_OP_NOT_TAG: u8 = 0x01;

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

/// Hash one projection identity shape using the current structural encoding.
pub(in crate::db::query::fingerprint) fn hash_projection_structural_fingerprint(
    hasher: &mut Sha256,
    projection: &ProjectionSpec,
) -> Result<(), InternalError> {
    write_tag(hasher, PROJECTION_STRUCTURAL_FINGERPRINT_TAG);
    write_u32(
        hasher,
        u32::try_from(projection.fields().len()).unwrap_or(u32::MAX),
    );
    for field in projection.fields() {
        hash_projection_field(hasher, field)?;
    }
    Ok(())
}

///
/// Hash one canonical scalar filter expression into the shared identity stream.
///
/// This is reused by fingerprint and continuation-signature hashing so those
/// surfaces consume the same planner-owned semantic filter shape as projection
/// hashing instead of inventing a second expression walker.
///
pub(in crate::db::query::fingerprint) fn hash_scalar_filter_expr_structural_fingerprint(
    hasher: &mut Sha256,
    expr: &Expr,
) -> Result<(), InternalError> {
    hash_expr(hasher, expr)?;
    Ok(())
}

fn hash_projection_field(
    hasher: &mut Sha256,
    field: &ProjectionField,
) -> Result<(), InternalError> {
    // Field aliases are explain/display metadata and must not affect
    // projection semantic identity.
    write_tag(hasher, PROJECTION_FIELD_SCALAR_TAG);
    hash_expr(hasher, field.expr())?;
    Ok(())
}

fn hash_expr(hasher: &mut Sha256, expr: &Expr) -> Result<(), InternalError> {
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
            // Structural identity preserves admitted literal types. Numeric
            // normalization belongs to preparation, not the hash encoder.
            write_value(hasher, value)?;
        }
        Expr::FunctionCall { function, args } => {
            write_tag(hasher, EXPR_FUNCTION_CALL_TAG);
            write_str(hasher, function.canonical_label());
            write_u32(hasher, u32::try_from(args.len()).unwrap_or(u32::MAX));
            for arg in args {
                hash_expr(hasher, arg)?;
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
                hash_expr(hasher, arm.condition())?;
                hash_expr(hasher, arm.result())?;
            }
            hash_expr(hasher, else_expr.as_ref())?;
        }
        Expr::Unary { op, expr } => {
            write_tag(hasher, EXPR_UNARY_TAG);
            write_tag(hasher, unary_op_tag(*op));
            hash_expr(hasher, expr.as_ref())?;
        }
        Expr::Binary { op, left, right } => {
            write_tag(hasher, EXPR_BINARY_TAG);
            write_tag(hasher, binary_op_tag(*op));
            // Expression hashing preserves AST operand order. Commutative
            // normalization is intentionally out-of-scope for structural identity.
            hash_expr(hasher, left.as_ref())?;
            hash_expr(hasher, right.as_ref())?;
        }
        Expr::Aggregate(aggregate) => {
            write_tag(hasher, EXPR_AGGREGATE_TAG);
            hash_aggregate_expr(hasher, aggregate)?;
        }
        #[cfg(test)]
        Expr::Alias { expr, name: _ } => {
            // Expression alias wrappers are presentation metadata only.
            hash_expr(hasher, expr.as_ref())?;
        }
    }
    Ok(())
}

fn hash_aggregate_expr(
    hasher: &mut Sha256,
    aggregate: &AggregateExpr,
) -> Result<(), InternalError> {
    // Hash borrowed canonical meaning, without copying then normalizing the
    // input tree. COUNT input and DISTINCT normalization stay planner-owned.
    let identity = AggregateSemanticKeyRef::from_aggregate_expr(aggregate);

    write_tag(hasher, identity.kind().fingerprint_tag());
    match identity.input_expr() {
        Some(Expr::Field(field)) => {
            write_tag(hasher, AGGREGATE_TARGET_PRESENT_TAG);
            write_str(hasher, field.as_str());
        }
        Some(input_expr) => {
            write_tag(hasher, AGGREGATE_TARGET_PRESENT_TAG);
            hash_expr(hasher, input_expr)?;
        }
        None => write_tag(hasher, AGGREGATE_TARGET_ABSENT_TAG),
    }
    write_tag(
        hasher,
        if identity.distinct() {
            AGGREGATE_DISTINCT_TAG
        } else {
            AGGREGATE_NON_DISTINCT_TAG
        },
    );
    if let Some(filter_expr) = identity.filter_expr() {
        write_tag(hasher, AGGREGATE_FILTER_PRESENT_TAG);
        hash_expr(hasher, filter_expr)?;
    } else {
        write_tag(hasher, AGGREGATE_FILTER_ABSENT_TAG);
    }
    Ok(())
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
