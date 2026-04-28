use crate::db::query::{
    builder::scalar_projection::render_scalar_projection_expr_plan_label,
    explain::{ExplainGroupAggregate, ExplainGroupField},
    fingerprint::hash_parts::{
        GROUP_HAVING_ABSENT_TAG, GROUP_HAVING_AND_TAG, GROUP_HAVING_COMPARE_TAG,
        GROUP_HAVING_PRESENT_TAG, GROUP_HAVING_VALUE_AGGREGATE_INDEX_TAG,
        GROUP_HAVING_VALUE_BINARY_TAG, GROUP_HAVING_VALUE_CASE_ARM_TAG,
        GROUP_HAVING_VALUE_CASE_TAG, GROUP_HAVING_VALUE_EXPR_TAG, GROUP_HAVING_VALUE_FUNCTION_TAG,
        GROUP_HAVING_VALUE_GROUP_FIELD_TAG, GROUP_HAVING_VALUE_LITERAL_TAG,
        GROUP_HAVING_VALUE_UNARY_TAG, write_str, write_tag, write_u32, write_value,
    },
    plan::{
        AggregateIdentity, FieldSlot, GroupAggregateSpec,
        expr::{BinaryOp, CaseWhenArm, Expr, UnaryOp},
    },
};
use sha2::Sha256;

/// Canonical grouped HAVING expression source shared by plan and explain hashing.
pub(super) enum GroupHavingFingerprintSource<'a> {
    Explain {
        expr: &'a Expr,
        group_fields: &'a [ExplainGroupField],
        aggregates: &'a [ExplainGroupAggregate],
    },
    PlanBorrowed {
        expr: &'a Expr,
        group_fields: &'a [FieldSlot],
        aggregates: &'a [GroupAggregateSpec],
    },
    PlanOwned {
        expr: Expr,
        group_fields: &'a [FieldSlot],
        aggregates: &'a [GroupAggregateSpec],
    },
}

pub(super) fn hash_group_having_projection(
    hasher: &mut Sha256,
    expr: Option<&GroupHavingFingerprintSource<'_>>,
) {
    let Some(expr) = expr else {
        write_tag(hasher, GROUP_HAVING_ABSENT_TAG);
        return;
    };

    write_tag(hasher, GROUP_HAVING_PRESENT_TAG);
    match expr {
        GroupHavingFingerprintSource::Explain {
            expr,
            group_fields,
            aggregates,
        } => hash_group_having_expr_explain(hasher, expr, group_fields, aggregates),
        GroupHavingFingerprintSource::PlanBorrowed {
            expr,
            group_fields,
            aggregates,
        } => hash_group_having_expr_plan(hasher, expr, group_fields, aggregates),
        GroupHavingFingerprintSource::PlanOwned {
            expr,
            group_fields,
            aggregates,
        } => hash_group_having_expr_plan(hasher, expr, group_fields, aggregates),
    }
}

fn hash_group_having_expr_plan(
    hasher: &mut Sha256,
    expr: &Expr,
    group_fields: &[FieldSlot],
    aggregates: &[GroupAggregateSpec],
) {
    match expr {
        Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr_plan(hasher, left, group_fields, aggregates);
            write_tag(hasher, 0x03);
            hash_group_having_value_expr_plan(hasher, right, group_fields, aggregates);
        }
        Expr::Binary {
            op: BinaryOp::Ne,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr_plan(hasher, left, group_fields, aggregates);
            write_tag(hasher, 0x04);
            hash_group_having_value_expr_plan(hasher, right, group_fields, aggregates);
        }
        Expr::Binary {
            op: BinaryOp::Lt,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr_plan(hasher, left, group_fields, aggregates);
            write_tag(hasher, 0x05);
            hash_group_having_value_expr_plan(hasher, right, group_fields, aggregates);
        }
        Expr::Binary {
            op: BinaryOp::Lte,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr_plan(hasher, left, group_fields, aggregates);
            write_tag(hasher, 0x06);
            hash_group_having_value_expr_plan(hasher, right, group_fields, aggregates);
        }
        Expr::Binary {
            op: BinaryOp::Gt,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr_plan(hasher, left, group_fields, aggregates);
            write_tag(hasher, 0x07);
            hash_group_having_value_expr_plan(hasher, right, group_fields, aggregates);
        }
        Expr::Binary {
            op: BinaryOp::Gte,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr_plan(hasher, left, group_fields, aggregates);
            write_tag(hasher, 0x08);
            hash_group_having_value_expr_plan(hasher, right, group_fields, aggregates);
        }
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_AND_TAG);
            write_u32(hasher, 2);
            hash_group_having_expr_plan(hasher, left, group_fields, aggregates);
            hash_group_having_expr_plan(hasher, right, group_fields, aggregates);
        }
        _ => {
            write_tag(hasher, GROUP_HAVING_VALUE_EXPR_TAG);
            hash_group_having_value_expr_plan(hasher, expr, group_fields, aggregates);
        }
    }
}

fn hash_group_having_expr_explain(
    hasher: &mut Sha256,
    expr: &Expr,
    group_fields: &[ExplainGroupField],
    aggregates: &[ExplainGroupAggregate],
) {
    match expr {
        Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr_explain(hasher, left, group_fields, aggregates);
            write_tag(hasher, 0x03);
            hash_group_having_value_expr_explain(hasher, right, group_fields, aggregates);
        }
        Expr::Binary {
            op: BinaryOp::Ne,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr_explain(hasher, left, group_fields, aggregates);
            write_tag(hasher, 0x04);
            hash_group_having_value_expr_explain(hasher, right, group_fields, aggregates);
        }
        Expr::Binary {
            op: BinaryOp::Lt,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr_explain(hasher, left, group_fields, aggregates);
            write_tag(hasher, 0x05);
            hash_group_having_value_expr_explain(hasher, right, group_fields, aggregates);
        }
        Expr::Binary {
            op: BinaryOp::Lte,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr_explain(hasher, left, group_fields, aggregates);
            write_tag(hasher, 0x06);
            hash_group_having_value_expr_explain(hasher, right, group_fields, aggregates);
        }
        Expr::Binary {
            op: BinaryOp::Gt,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr_explain(hasher, left, group_fields, aggregates);
            write_tag(hasher, 0x07);
            hash_group_having_value_expr_explain(hasher, right, group_fields, aggregates);
        }
        Expr::Binary {
            op: BinaryOp::Gte,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr_explain(hasher, left, group_fields, aggregates);
            write_tag(hasher, 0x08);
            hash_group_having_value_expr_explain(hasher, right, group_fields, aggregates);
        }
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_AND_TAG);
            write_u32(hasher, 2);
            hash_group_having_expr_explain(hasher, left, group_fields, aggregates);
            hash_group_having_expr_explain(hasher, right, group_fields, aggregates);
        }
        _ => {
            write_tag(hasher, GROUP_HAVING_VALUE_EXPR_TAG);
            hash_group_having_value_expr_explain(hasher, expr, group_fields, aggregates);
        }
    }
}

fn hash_group_having_value_expr_plan(
    hasher: &mut Sha256,
    expr: &Expr,
    group_fields: &[FieldSlot],
    aggregates: &[GroupAggregateSpec],
) {
    match expr {
        Expr::Field(field_id) => {
            let field_slot = group_fields
                .iter()
                .find(|field| field.field() == field_id.as_str())
                .expect("grouped HAVING fingerprint requires grouped field identity");
            write_tag(hasher, GROUP_HAVING_VALUE_GROUP_FIELD_TAG);
            write_u32(hasher, field_slot.index() as u32);
            write_str(hasher, field_slot.field());
        }
        Expr::Aggregate(aggregate_expr) => {
            let identity = AggregateIdentity::from_aggregate_expr(aggregate_expr);
            let index = aggregates
                .iter()
                .position(|aggregate| {
                    aggregate.identity() == identity
                        && aggregate.target_field() == aggregate_expr.target_field()
                        && aggregate.filter_expr() == aggregate_expr.filter_expr()
                })
                .expect("grouped HAVING fingerprint requires grouped aggregate identity");
            write_tag(hasher, GROUP_HAVING_VALUE_AGGREGATE_INDEX_TAG);
            write_u32(hasher, index as u32);
        }
        Expr::Literal(value) => {
            write_tag(hasher, GROUP_HAVING_VALUE_LITERAL_TAG);
            write_value(hasher, value);
        }
        Expr::FunctionCall { function, args } => {
            write_tag(hasher, GROUP_HAVING_VALUE_FUNCTION_TAG);
            write_str(hasher, function.canonical_label());
            write_u32(hasher, args.len() as u32);
            for arg in args {
                hash_group_having_value_expr_plan(hasher, arg, group_fields, aggregates);
            }
        }
        Expr::Unary { op, expr } => {
            write_tag(hasher, GROUP_HAVING_VALUE_UNARY_TAG);
            write_tag(hasher, grouped_having_unary_op_tag(*op));
            hash_group_having_value_expr_plan(hasher, expr, group_fields, aggregates);
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            write_tag(hasher, GROUP_HAVING_VALUE_CASE_TAG);
            write_u32(hasher, when_then_arms.len() as u32);
            for arm in when_then_arms {
                hash_group_having_case_arm_plan(hasher, arm, group_fields, aggregates);
            }
            hash_group_having_value_expr_plan(hasher, else_expr, group_fields, aggregates);
        }
        Expr::Binary { op, left, right } => {
            write_tag(hasher, GROUP_HAVING_VALUE_BINARY_TAG);
            write_tag(hasher, grouped_having_binary_op_tag(*op));
            hash_group_having_value_expr_plan(hasher, left, group_fields, aggregates);
            hash_group_having_value_expr_plan(hasher, right, group_fields, aggregates);
        }
        #[cfg(test)]
        Expr::Alias { expr, .. } => {
            hash_group_having_value_expr_plan(hasher, expr, group_fields, aggregates);
        }
    }
}

fn hash_group_having_value_expr_explain(
    hasher: &mut Sha256,
    expr: &Expr,
    group_fields: &[ExplainGroupField],
    aggregates: &[ExplainGroupAggregate],
) {
    match expr {
        Expr::Field(field_id) => {
            let field_slot = group_fields
                .iter()
                .find(|field| field.field() == field_id.as_str())
                .expect("grouped HAVING explain fingerprint requires grouped field identity");
            write_tag(hasher, GROUP_HAVING_VALUE_GROUP_FIELD_TAG);
            write_u32(hasher, field_slot.slot_index() as u32);
            write_str(hasher, field_slot.field());
        }
        Expr::Aggregate(aggregate_expr) => {
            let semantic_distinct =
                AggregateIdentity::from_aggregate_expr(aggregate_expr).distinct();
            let input_expr = aggregate_expr
                .input_expr()
                .map(render_scalar_projection_expr_plan_label);
            let filter_expr = aggregate_expr
                .filter_expr()
                .map(render_scalar_projection_expr_plan_label);
            let index = aggregates
                .iter()
                .position(|aggregate| {
                    let input_matches = aggregate.input_expr() == input_expr.as_deref();
                    let filter_matches = aggregate.filter_expr() == filter_expr.as_deref();

                    aggregate.kind() == aggregate_expr.kind()
                        && aggregate.target_field() == aggregate_expr.target_field()
                        && input_matches
                        && filter_matches
                        && aggregate.distinct() == semantic_distinct
                })
                .expect("grouped HAVING explain fingerprint requires grouped aggregate identity");
            write_tag(hasher, GROUP_HAVING_VALUE_AGGREGATE_INDEX_TAG);
            write_u32(hasher, index as u32);
        }
        Expr::Literal(value) => {
            write_tag(hasher, GROUP_HAVING_VALUE_LITERAL_TAG);
            write_value(hasher, value);
        }
        Expr::FunctionCall { function, args } => {
            write_tag(hasher, GROUP_HAVING_VALUE_FUNCTION_TAG);
            write_str(hasher, function.canonical_label());
            write_u32(hasher, args.len() as u32);
            for arg in args {
                hash_group_having_value_expr_explain(hasher, arg, group_fields, aggregates);
            }
        }
        Expr::Unary { op, expr } => {
            write_tag(hasher, GROUP_HAVING_VALUE_UNARY_TAG);
            write_tag(hasher, grouped_having_unary_op_tag(*op));
            hash_group_having_value_expr_explain(hasher, expr, group_fields, aggregates);
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            write_tag(hasher, GROUP_HAVING_VALUE_CASE_TAG);
            write_u32(hasher, when_then_arms.len() as u32);
            for arm in when_then_arms {
                hash_group_having_case_arm_explain(hasher, arm, group_fields, aggregates);
            }
            hash_group_having_value_expr_explain(hasher, else_expr, group_fields, aggregates);
        }
        Expr::Binary { op, left, right } => {
            write_tag(hasher, GROUP_HAVING_VALUE_BINARY_TAG);
            write_tag(hasher, grouped_having_binary_op_tag(*op));
            hash_group_having_value_expr_explain(hasher, left, group_fields, aggregates);
            hash_group_having_value_expr_explain(hasher, right, group_fields, aggregates);
        }
        #[cfg(test)]
        Expr::Alias { expr, .. } => {
            hash_group_having_value_expr_explain(hasher, expr, group_fields, aggregates);
        }
    }
}

fn hash_group_having_case_arm_plan(
    hasher: &mut Sha256,
    expr: &CaseWhenArm,
    group_fields: &[FieldSlot],
    aggregates: &[GroupAggregateSpec],
) {
    write_tag(hasher, GROUP_HAVING_VALUE_CASE_ARM_TAG);
    hash_group_having_value_expr_plan(hasher, expr.condition(), group_fields, aggregates);
    hash_group_having_value_expr_plan(hasher, expr.result(), group_fields, aggregates);
}

fn hash_group_having_case_arm_explain(
    hasher: &mut Sha256,
    expr: &CaseWhenArm,
    group_fields: &[ExplainGroupField],
    aggregates: &[ExplainGroupAggregate],
) {
    write_tag(hasher, GROUP_HAVING_VALUE_CASE_ARM_TAG);
    hash_group_having_value_expr_explain(hasher, expr.condition(), group_fields, aggregates);
    hash_group_having_value_expr_explain(hasher, expr.result(), group_fields, aggregates);
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
