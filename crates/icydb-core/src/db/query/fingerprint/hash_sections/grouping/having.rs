#[cfg(test)]
mod tests;

use crate::db::query::{
    builder::{AggregateExpr, scalar_projection::render_scalar_projection_expr_plan_label},
    fingerprint::hash_sections::{
        GROUP_HAVING_ABSENT_TAG, GROUP_HAVING_AND_TAG, GROUP_HAVING_COMPARE_TAG,
        GROUP_HAVING_PRESENT_TAG, GROUP_HAVING_VALUE_AGGREGATE_INDEX_TAG,
        GROUP_HAVING_VALUE_BINARY_TAG, GROUP_HAVING_VALUE_CASE_ARM_TAG,
        GROUP_HAVING_VALUE_CASE_TAG, GROUP_HAVING_VALUE_EXPR_TAG,
        GROUP_HAVING_VALUE_FIELD_PATH_TAG, GROUP_HAVING_VALUE_FUNCTION_TAG,
        GROUP_HAVING_VALUE_GROUP_FIELD_TAG, GROUP_HAVING_VALUE_LITERAL_TAG,
        GROUP_HAVING_VALUE_UNARY_TAG, write_str, write_tag, write_u32, write_value,
    },
    plan::{
        AggregateIdentity, AggregateSemanticKeyRef, GroupAggregateSpec, GroupFieldSet,
        expr::{BinaryOp, CaseWhenArm, Expr, UnaryOp},
    },
};
use crate::error::InternalError;
use sha2::Sha256;

const GROUP_HAVING_MISSING_SLOT_SENTINEL: u32 = u32::MAX;

/// Borrowed planner semantics used by continuation HAVING identity.
pub(super) struct GroupHavingFingerprintSource<'a> {
    pub(super) expr: &'a Expr,
    pub(super) group_fields: &'a GroupFieldSet,
    pub(super) aggregates: &'a [GroupAggregateSpec],
}
impl GroupHavingFingerprintSource<'_> {
    fn group_field<'a>(&'a self, expr: &Expr) -> Option<(u32, &'a str)> {
        self.group_fields
            .iter()
            .find(|field| field.matches_expr(expr))
            .map(|field| (field.root_slot() as u32, field.field()))
    }

    // Matched slots use borrowed semantic keys; only missing slots render labels.
    fn hash_aggregate_expr(&self, hasher: &mut Sha256, aggregate_expr: &AggregateExpr) {
        write_tag(hasher, GROUP_HAVING_VALUE_AGGREGATE_INDEX_TAG);

        let semantic_key = AggregateSemanticKeyRef::from_aggregate_expr(aggregate_expr);
        if let Some(index) = self
            .aggregates
            .iter()
            .position(|aggregate| aggregate.semantic_key() == semantic_key)
        {
            write_u32(hasher, index as u32);
            return;
        }

        let semantic_distinct = AggregateIdentity::normalize_distinct_for_kind(
            aggregate_expr.kind(),
            aggregate_expr.is_distinct(),
        );
        let input_expr = aggregate_expr
            .input_expr()
            .map(render_scalar_projection_expr_plan_label);
        let filter_expr = aggregate_expr
            .filter_expr()
            .map(render_scalar_projection_expr_plan_label);

        write_u32(hasher, GROUP_HAVING_MISSING_SLOT_SENTINEL);
        write_tag(hasher, aggregate_expr.kind().fingerprint_tag());
        write_optional_str(hasher, aggregate_expr.target_field());
        write_optional_str(hasher, input_expr.as_deref());
        write_optional_str(hasher, filter_expr.as_deref());
        write_bool(hasher, semantic_distinct);
    }
}

pub(super) fn hash_group_having_projection(
    hasher: &mut Sha256,
    expr: Option<&GroupHavingFingerprintSource<'_>>,
) -> Result<(), InternalError> {
    let Some(expr) = expr else {
        write_tag(hasher, GROUP_HAVING_ABSENT_TAG);
        return Ok(());
    };

    write_tag(hasher, GROUP_HAVING_PRESENT_TAG);
    hash_group_having_expr(hasher, expr.expr, expr)?;
    Ok(())
}

fn hash_group_having_expr(
    hasher: &mut Sha256,
    expr: &Expr,
    context: &GroupHavingFingerprintSource<'_>,
) -> Result<(), InternalError> {
    match expr {
        Expr::Binary {
            op:
                op @ (BinaryOp::Eq
                | BinaryOp::Ne
                | BinaryOp::Lt
                | BinaryOp::Lte
                | BinaryOp::Gt
                | BinaryOp::Gte),
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr(hasher, left, context)?;
            write_tag(hasher, grouped_having_binary_op_tag(*op));
            hash_group_having_value_expr(hasher, right, context)?;
        }
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_AND_TAG);
            write_u32(hasher, 2);
            hash_group_having_expr(hasher, left, context)?;
            hash_group_having_expr(hasher, right, context)?;
        }
        _ => {
            write_tag(hasher, GROUP_HAVING_VALUE_EXPR_TAG);
            hash_group_having_value_expr(hasher, expr, context)?;
        }
    }
    Ok(())
}

fn hash_group_having_value_expr(
    hasher: &mut Sha256,
    expr: &Expr,
    context: &GroupHavingFingerprintSource<'_>,
) -> Result<(), InternalError> {
    match expr {
        Expr::Field(field_id) => {
            write_tag(hasher, GROUP_HAVING_VALUE_GROUP_FIELD_TAG);
            if let Some((slot_index, field)) = context.group_field(expr) {
                write_u32(hasher, slot_index);
                write_str(hasher, field);
            } else {
                write_u32(hasher, GROUP_HAVING_MISSING_SLOT_SENTINEL);
                write_str(hasher, field_id.as_str());
            }
        }
        Expr::FieldPath(path) => {
            write_tag(hasher, GROUP_HAVING_VALUE_FIELD_PATH_TAG);
            write_str(hasher, path.root().as_str());
            write_u32(hasher, path.segments().len() as u32);
            for segment in path.segments() {
                write_str(hasher, segment);
            }
        }
        Expr::Aggregate(aggregate_expr) => {
            context.hash_aggregate_expr(hasher, aggregate_expr);
        }
        Expr::Literal(value) => {
            write_tag(hasher, GROUP_HAVING_VALUE_LITERAL_TAG);
            write_value(hasher, value)?;
        }
        Expr::FunctionCall { function, args } => {
            write_tag(hasher, GROUP_HAVING_VALUE_FUNCTION_TAG);
            write_str(hasher, function.canonical_label());
            write_u32(hasher, args.len() as u32);
            for arg in args {
                hash_group_having_value_expr(hasher, arg, context)?;
            }
        }
        Expr::Unary { op, expr } => {
            write_tag(hasher, GROUP_HAVING_VALUE_UNARY_TAG);
            write_tag(hasher, grouped_having_unary_op_tag(*op));
            hash_group_having_value_expr(hasher, expr, context)?;
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            write_tag(hasher, GROUP_HAVING_VALUE_CASE_TAG);
            write_u32(hasher, when_then_arms.len() as u32);
            for arm in when_then_arms {
                hash_group_having_case_arm(hasher, arm, context)?;
            }
            hash_group_having_value_expr(hasher, else_expr, context)?;
        }
        Expr::Binary { op, left, right } => {
            write_tag(hasher, GROUP_HAVING_VALUE_BINARY_TAG);
            write_tag(hasher, grouped_having_binary_op_tag(*op));
            hash_group_having_value_expr(hasher, left, context)?;
            hash_group_having_value_expr(hasher, right, context)?;
        }
        #[cfg(test)]
        Expr::Alias { expr, .. } => {
            hash_group_having_value_expr(hasher, expr, context)?;
        }
    }
    Ok(())
}

fn write_optional_str(hasher: &mut Sha256, value: Option<&str>) {
    if let Some(value) = value {
        write_tag(hasher, 1);
        write_str(hasher, value);
    } else {
        write_tag(hasher, 0);
    }
}

fn write_bool(hasher: &mut Sha256, value: bool) {
    write_tag(hasher, u8::from(value));
}

fn hash_group_having_case_arm(
    hasher: &mut Sha256,
    expr: &CaseWhenArm,
    context: &GroupHavingFingerprintSource<'_>,
) -> Result<(), InternalError> {
    write_tag(hasher, GROUP_HAVING_VALUE_CASE_ARM_TAG);
    hash_group_having_value_expr(hasher, expr.condition(), context)?;
    hash_group_having_value_expr(hasher, expr.result(), context)?;
    Ok(())
}

const fn grouped_having_unary_op_tag(op: UnaryOp) -> u8 {
    match op {
        UnaryOp::Not => 0x01,
    }
}

const fn grouped_having_binary_op_tag(op: BinaryOp) -> u8 {
    match op {
        BinaryOp::Or => 0x01,
        BinaryOp::And => 0x02,
        BinaryOp::Eq => 0x03,
        BinaryOp::Ne => 0x04,
        BinaryOp::Lt => 0x05,
        BinaryOp::Lte => 0x06,
        BinaryOp::Gt => 0x07,
        BinaryOp::Gte => 0x08,
        BinaryOp::Add => 0x09,
        BinaryOp::Sub => 0x0A,
        BinaryOp::Mul => 0x0B,
        BinaryOp::Div => 0x0C,
    }
}
