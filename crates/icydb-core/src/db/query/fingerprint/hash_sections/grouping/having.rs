mod comparison;

#[cfg(test)]
mod tests;

use crate::db::query::{
    builder::AggregateExpr,
    construction::ConstructionBudget,
    fingerprint::hash_sections::{
        GROUP_HAVING_ABSENT_TAG, GROUP_HAVING_AND_TAG, GROUP_HAVING_COMPARE_TAG,
        GROUP_HAVING_PRESENT_TAG, GROUP_HAVING_VALUE_AGGREGATE_INDEX_TAG,
        GROUP_HAVING_VALUE_BINARY_TAG, GROUP_HAVING_VALUE_CASE_ARM_TAG,
        GROUP_HAVING_VALUE_CASE_TAG, GROUP_HAVING_VALUE_EXPR_TAG,
        GROUP_HAVING_VALUE_FIELD_PATH_TAG, GROUP_HAVING_VALUE_FUNCTION_TAG,
        GROUP_HAVING_VALUE_GROUP_FIELD_TAG, GROUP_HAVING_VALUE_LITERAL_TAG,
        GROUP_HAVING_VALUE_UNARY_TAG,
        grouping::{hash_field_path, having::comparison::admit_semantic_key_comparison},
        write_expr_label, write_str, write_tag, write_u32,
    },
    plan::{
        AggregateIdentity, AggregateSemanticKeyRef, GroupAggregateSpec, GroupFieldSet,
        expr::{BinaryOp, CaseWhenArm, Expr, UnaryOp},
    },
};
use crate::error::InternalError;
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use sha2::{Digest, Sha256};

const GROUP_HAVING_MISSING_SLOT_SENTINEL: u32 = u32::MAX;

/// Borrowed planner semantics used by continuation HAVING identity.
pub(super) struct GroupHavingFingerprintSource<'a> {
    pub(super) expr: &'a Expr,
    pub(super) group_fields: &'a GroupFieldSet,
    pub(super) aggregates: &'a [GroupAggregateSpec],
}
impl GroupHavingFingerprintSource<'_> {
    fn group_field<'a>(
        &'a self,
        expr: &Expr,
        budget: &dyn ConstructionBudget,
    ) -> Result<Option<(u32, &'a str)>, InternalError> {
        for field in self.group_fields.iter() {
            if field.try_matches_expr(expr, &mut |steps| {
                budget.charge(Resource::PredicateExpressionSteps, steps)
            })? {
                return Ok(Some((field.root_slot() as u32, field.field())));
            }
        }
        Ok(None)
    }

    // Matched slots use borrowed semantic keys; only missing slots render labels.
    fn hash_aggregate_expr(
        &self,
        hasher: &mut Sha256,
        aggregate_expr: &AggregateExpr,
        budget: &dyn ConstructionBudget,
    ) -> Result<(), InternalError> {
        write_tag(hasher, GROUP_HAVING_VALUE_AGGREGATE_INDEX_TAG);

        let semantic_key = AggregateSemanticKeyRef::from_aggregate_expr(aggregate_expr);
        for (index, aggregate) in self.aggregates.iter().enumerate() {
            budget.charge(Resource::PredicateExpressionSteps, 1)?;
            let candidate = aggregate.semantic_key();
            // Scalar mismatches cannot inspect operand trees. Matching headers
            // admit one side's complete extent before the existing equality;
            // exhaustion is not a missing slot and must propagate unchanged.
            if candidate.kind() != semantic_key.kind()
                || candidate.distinct() != semantic_key.distinct()
                || candidate.input_expr().is_some() != semantic_key.input_expr().is_some()
                || candidate.filter_expr().is_some() != semantic_key.filter_expr().is_some()
            {
                continue;
            }
            admit_semantic_key_comparison(semantic_key, budget)?;
            if candidate == semantic_key {
                write_u32(hasher, index as u32);
                return Ok(());
            }
        }

        let semantic_distinct = AggregateIdentity::normalize_distinct_for_kind(
            aggregate_expr.kind(),
            aggregate_expr.is_distinct(),
        );
        write_u32(hasher, GROUP_HAVING_MISSING_SLOT_SENTINEL);
        write_tag(hasher, aggregate_expr.kind().fingerprint_tag());
        write_optional_str(hasher, aggregate_expr.target_field(), budget)?;
        // Preserve the HAVING-specific framing, but render/hash each operand
        // independently so input and filter labels never need joint retention.
        for expr in [aggregate_expr.input_expr(), aggregate_expr.filter_expr()] {
            write_tag(hasher, u8::from(expr.is_some()));
            if let Some(expr) = expr {
                write_expr_label(hasher, expr, budget)?;
            }
        }
        write_bool(hasher, semantic_distinct);
        Ok(())
    }
}

pub(super) fn hash_group_having_projection(
    hasher: &mut Sha256,
    expr: Option<&GroupHavingFingerprintSource<'_>>,
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    let Some(expr) = expr else {
        write_tag(hasher, GROUP_HAVING_ABSENT_TAG);
        return Ok(());
    };

    write_tag(hasher, GROUP_HAVING_PRESENT_TAG);
    hash_group_having_expr(hasher, expr.expr, expr, budget)?;
    Ok(())
}

fn hash_group_having_expr(
    hasher: &mut Sha256,
    expr: &Expr,
    context: &GroupHavingFingerprintSource<'_>,
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    budget.charge(Resource::PredicateExpressionSteps, 1)?;
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
            hash_group_having_value_expr(hasher, left, context, budget)?;
            write_tag(hasher, grouped_having_binary_op_tag(*op));
            hash_group_having_value_expr(hasher, right, context, budget)?;
        }
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_AND_TAG);
            write_u32(hasher, 2);
            hash_group_having_expr(hasher, left, context, budget)?;
            hash_group_having_expr(hasher, right, context, budget)?;
        }
        _ => {
            write_tag(hasher, GROUP_HAVING_VALUE_EXPR_TAG);
            hash_group_having_value_expr(hasher, expr, context, budget)?;
        }
    }
    Ok(())
}

fn hash_group_having_value_expr(
    hasher: &mut Sha256,
    expr: &Expr,
    context: &GroupHavingFingerprintSource<'_>,
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    // Charge each recursive dispatch before descending; literal encoding shares
    // the caller's value admission rather than opening another budget scope.
    budget.charge(Resource::PredicateExpressionSteps, 1)?;
    match expr {
        Expr::Field(field_id) => {
            write_tag(hasher, GROUP_HAVING_VALUE_GROUP_FIELD_TAG);
            if let Some((slot_index, field)) = context.group_field(expr, budget)? {
                write_u32(hasher, slot_index);
                budget.charge(Resource::PredicateExpressionSteps, field.len() as u64)?;
                write_str(hasher, field);
            } else {
                write_u32(hasher, GROUP_HAVING_MISSING_SLOT_SENTINEL);
                budget.charge(
                    Resource::PredicateExpressionSteps,
                    field_id.as_str().len() as u64,
                )?;
                write_str(hasher, field_id.as_str());
            }
        }
        Expr::FieldPath(path) => {
            write_tag(hasher, GROUP_HAVING_VALUE_FIELD_PATH_TAG);
            hash_field_path(hasher, path.path_spec(), budget)?;
        }
        Expr::Aggregate(aggregate_expr) => {
            context.hash_aggregate_expr(hasher, aggregate_expr, budget)?;
        }
        Expr::Literal(value) => {
            write_tag(hasher, GROUP_HAVING_VALUE_LITERAL_TAG);
            hasher.update(budget.hash_value(value)?);
        }
        Expr::FunctionCall { function, args } => {
            write_tag(hasher, GROUP_HAVING_VALUE_FUNCTION_TAG);
            budget.charge(
                Resource::PredicateExpressionSteps,
                function.canonical_label().len() as u64,
            )?;
            write_str(hasher, function.canonical_label());
            write_u32(hasher, args.len() as u32);
            for arg in args {
                hash_group_having_value_expr(hasher, arg, context, budget)?;
            }
        }
        Expr::Unary { op, expr } => {
            write_tag(hasher, GROUP_HAVING_VALUE_UNARY_TAG);
            write_tag(hasher, grouped_having_unary_op_tag(*op));
            hash_group_having_value_expr(hasher, expr, context, budget)?;
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            write_tag(hasher, GROUP_HAVING_VALUE_CASE_TAG);
            write_u32(hasher, when_then_arms.len() as u32);
            for arm in when_then_arms {
                hash_group_having_case_arm(hasher, arm, context, budget)?;
            }
            hash_group_having_value_expr(hasher, else_expr, context, budget)?;
        }
        Expr::Binary { op, left, right } => {
            write_tag(hasher, GROUP_HAVING_VALUE_BINARY_TAG);
            write_tag(hasher, grouped_having_binary_op_tag(*op));
            hash_group_having_value_expr(hasher, left, context, budget)?;
            hash_group_having_value_expr(hasher, right, context, budget)?;
        }
        #[cfg(test)]
        Expr::Alias { expr, .. } => {
            hash_group_having_value_expr(hasher, expr, context, budget)?;
        }
    }
    Ok(())
}

fn write_optional_str(
    hasher: &mut Sha256,
    value: Option<&str>,
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    if let Some(value) = value {
        write_tag(hasher, 1);
        budget.charge(Resource::PredicateExpressionSteps, value.len() as u64)?;
        write_str(hasher, value);
    } else {
        write_tag(hasher, 0);
    }
    Ok(())
}

fn write_bool(hasher: &mut Sha256, value: bool) {
    write_tag(hasher, u8::from(value));
}

fn hash_group_having_case_arm(
    hasher: &mut Sha256,
    expr: &CaseWhenArm,
    context: &GroupHavingFingerprintSource<'_>,
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    budget.charge(Resource::PredicateExpressionSteps, 1)?;
    write_tag(hasher, GROUP_HAVING_VALUE_CASE_ARM_TAG);
    hash_group_having_value_expr(hasher, expr.condition(), context, budget)?;
    hash_group_having_value_expr(hasher, expr.result(), context, budget)?;
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
