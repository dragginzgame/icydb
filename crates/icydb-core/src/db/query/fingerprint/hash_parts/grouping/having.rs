use crate::db::query::{
    explain::{GroupHavingCaseArm, GroupHavingExpr, GroupHavingValueExpr},
    fingerprint::hash_parts::{
        GROUP_HAVING_ABSENT_TAG, GROUP_HAVING_AND_TAG, GROUP_HAVING_COMPARE_TAG,
        GROUP_HAVING_PRESENT_TAG, GROUP_HAVING_VALUE_AGGREGATE_INDEX_TAG,
        GROUP_HAVING_VALUE_BINARY_TAG, GROUP_HAVING_VALUE_CASE_ARM_TAG,
        GROUP_HAVING_VALUE_CASE_TAG, GROUP_HAVING_VALUE_EXPR_TAG, GROUP_HAVING_VALUE_FUNCTION_TAG,
        GROUP_HAVING_VALUE_GROUP_FIELD_TAG, GROUP_HAVING_VALUE_LITERAL_TAG,
        GROUP_HAVING_VALUE_UNARY_TAG, write_str, write_tag, write_u32, write_value,
    },
    plan::{FieldSlot, GroupAggregateSpec, expr::Expr},
};
use sha2::Sha256;

/// Canonical grouped HAVING expression source shared by plan and explain hashing.
pub(super) enum GroupHavingFingerprintSource<'a> {
    Explain(&'a GroupHavingExpr),
    Plan {
        expr: &'a Expr,
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
        GroupHavingFingerprintSource::Explain(expr) => hash_group_having_expr(hasher, expr),
        GroupHavingFingerprintSource::Plan {
            expr,
            group_fields,
            aggregates,
        } => {
            let projected = GroupHavingExpr::from_plan(expr, group_fields, aggregates);
            hash_group_having_expr(hasher, &projected);
        }
    }
}

fn hash_group_having_value_expr(hasher: &mut Sha256, expr: &GroupHavingValueExpr) {
    match expr {
        GroupHavingValueExpr::GroupField { slot_index, field } => {
            write_tag(hasher, GROUP_HAVING_VALUE_GROUP_FIELD_TAG);
            write_u32(hasher, *slot_index as u32);
            write_str(hasher, field);
        }
        GroupHavingValueExpr::AggregateIndex { index } => {
            write_tag(hasher, GROUP_HAVING_VALUE_AGGREGATE_INDEX_TAG);
            write_u32(hasher, *index as u32);
        }
        GroupHavingValueExpr::Literal(value) => {
            write_tag(hasher, GROUP_HAVING_VALUE_LITERAL_TAG);
            write_value(hasher, value);
        }
        GroupHavingValueExpr::FunctionCall { function, args } => {
            write_tag(hasher, GROUP_HAVING_VALUE_FUNCTION_TAG);
            write_str(hasher, function);
            write_u32(hasher, args.len() as u32);
            for arg in args {
                hash_group_having_value_expr(hasher, arg);
            }
        }
        GroupHavingValueExpr::Unary { op, expr } => {
            write_tag(hasher, GROUP_HAVING_VALUE_UNARY_TAG);
            write_tag(hasher, grouped_having_unary_op_tag(op));
            hash_group_having_value_expr(hasher, expr);
        }
        GroupHavingValueExpr::Case {
            when_then_arms,
            else_expr,
        } => {
            write_tag(hasher, GROUP_HAVING_VALUE_CASE_TAG);
            write_u32(hasher, when_then_arms.len() as u32);
            for arm in when_then_arms {
                hash_group_having_case_arm_expr(hasher, arm);
            }
            hash_group_having_value_expr(hasher, else_expr);
        }
        GroupHavingValueExpr::Binary { op, left, right } => {
            write_tag(hasher, GROUP_HAVING_VALUE_BINARY_TAG);
            write_tag(hasher, grouped_having_binary_op_tag(op));
            hash_group_having_value_expr(hasher, left);
            hash_group_having_value_expr(hasher, right);
        }
    }
}

fn hash_group_having_case_arm_expr(hasher: &mut Sha256, expr: &GroupHavingCaseArm) {
    write_tag(hasher, GROUP_HAVING_VALUE_CASE_ARM_TAG);
    hash_group_having_value_expr(hasher, &expr.condition);
    hash_group_having_value_expr(hasher, &expr.result);
}

fn hash_group_having_expr(hasher: &mut Sha256, expr: &GroupHavingExpr) {
    match expr {
        GroupHavingExpr::Compare { left, op, right } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr(hasher, left);
            write_tag(hasher, op.tag());
            hash_group_having_value_expr(hasher, right);
        }
        GroupHavingExpr::And(children) => {
            write_tag(hasher, GROUP_HAVING_AND_TAG);
            write_u32(hasher, children.len() as u32);
            for child in children {
                hash_group_having_expr(hasher, child);
            }
        }
        GroupHavingExpr::Value(expr) => {
            write_tag(hasher, GROUP_HAVING_VALUE_EXPR_TAG);
            hash_group_having_value_expr(hasher, expr);
        }
    }
}

fn grouped_having_unary_op_tag(op: &str) -> u8 {
    match op {
        "NOT" => 0x01,
        other => panic!("unexpected grouped HAVING unary operator label: {other}"),
    }
}

fn grouped_having_binary_op_tag(op: &str) -> u8 {
    match op {
        "OR" => 0x01,
        "AND" => 0x02,
        "=" => 0x03,
        "!=" => 0x04,
        "<" => 0x05,
        "<=" => 0x06,
        ">" => 0x07,
        ">=" => 0x08,
        "+" => 0x09,
        "-" => 0x0A,
        "*" => 0x0B,
        "/" => 0x0C,
        other => panic!("unexpected grouped HAVING binary operator label: {other}"),
    }
}
