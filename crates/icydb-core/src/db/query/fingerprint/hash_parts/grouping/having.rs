use crate::{
    db::query::{
        explain::{ExplainGroupHavingExpr, ExplainGroupHavingValueExpr},
        fingerprint::hash_parts::{
            GROUP_HAVING_ABSENT_TAG, GROUP_HAVING_AND_TAG, GROUP_HAVING_COMPARE_TAG,
            GROUP_HAVING_PRESENT_TAG, GROUP_HAVING_VALUE_AGGREGATE_INDEX_TAG,
            GROUP_HAVING_VALUE_BINARY_TAG, GROUP_HAVING_VALUE_FUNCTION_TAG,
            GROUP_HAVING_VALUE_GROUP_FIELD_TAG, GROUP_HAVING_VALUE_LITERAL_TAG, write_str,
            write_tag, write_u32, write_value,
        },
        plan::{GroupHavingExpr, GroupHavingValueExpr, expr::BinaryOp},
    },
    value::Value,
};
use sha2::Sha256;

/// Canonical grouped HAVING expression source shared by plan and explain hashing.
pub(super) enum GroupHavingFingerprintSource<'a> {
    Explain(&'a ExplainGroupHavingExpr),
    Plan(&'a GroupHavingExpr),
}

/// Canonical grouped HAVING value projection shared by plan and explain hashing.
enum ProjectedGroupHavingValueExpr<'a> {
    GroupField {
        slot_index: u32,
        field: &'a str,
    },
    AggregateIndex {
        index: u32,
    },
    Literal(&'a Value),
    FunctionCall {
        function: &'a str,
        args: Vec<Self>,
    },
    Binary {
        op_tag: u8,
        left: Box<Self>,
        right: Box<Self>,
    },
}

/// Canonical grouped HAVING expression projection shared by plan and explain hashing.
enum ProjectedGroupHavingExpr<'a> {
    Compare {
        left: ProjectedGroupHavingValueExpr<'a>,
        op_tag: u8,
        right: ProjectedGroupHavingValueExpr<'a>,
    },
    And(Vec<Self>),
}

impl<'a> ProjectedGroupHavingValueExpr<'a> {
    fn from_explain(expr: &'a ExplainGroupHavingValueExpr) -> Self {
        match expr {
            ExplainGroupHavingValueExpr::GroupField { slot_index, field } => Self::GroupField {
                slot_index: *slot_index as u32,
                field,
            },
            ExplainGroupHavingValueExpr::AggregateIndex { index } => Self::AggregateIndex {
                index: *index as u32,
            },
            ExplainGroupHavingValueExpr::Literal(value) => Self::Literal(value),
            ExplainGroupHavingValueExpr::FunctionCall { function, args } => Self::FunctionCall {
                function,
                args: args.iter().map(Self::from_explain).collect(),
            },
            ExplainGroupHavingValueExpr::Binary { op, left, right } => Self::Binary {
                op_tag: grouped_having_binary_op_tag_from_explain(op),
                left: Box::new(Self::from_explain(left)),
                right: Box::new(Self::from_explain(right)),
            },
        }
    }

    fn from_plan(expr: &'a GroupHavingValueExpr) -> Self {
        match expr {
            GroupHavingValueExpr::GroupField(field_slot) => Self::GroupField {
                slot_index: field_slot.index() as u32,
                field: field_slot.field(),
            },
            GroupHavingValueExpr::AggregateIndex(index) => Self::AggregateIndex {
                index: *index as u32,
            },
            GroupHavingValueExpr::Literal(value) => Self::Literal(value),
            GroupHavingValueExpr::FunctionCall { function, args } => Self::FunctionCall {
                function: function.sql_label(),
                args: args.iter().map(Self::from_plan).collect(),
            },
            GroupHavingValueExpr::Binary { op, left, right } => Self::Binary {
                op_tag: grouped_having_binary_op_tag(*op),
                left: Box::new(Self::from_plan(left)),
                right: Box::new(Self::from_plan(right)),
            },
        }
    }
}

impl<'a> ProjectedGroupHavingExpr<'a> {
    fn from_source(source: &'a GroupHavingFingerprintSource<'a>) -> Self {
        match source {
            GroupHavingFingerprintSource::Explain(expr) => Self::from_explain(expr),
            GroupHavingFingerprintSource::Plan(expr) => Self::from_plan(expr),
        }
    }

    fn from_explain(expr: &'a ExplainGroupHavingExpr) -> Self {
        match expr {
            ExplainGroupHavingExpr::Compare { left, op, right } => Self::Compare {
                left: ProjectedGroupHavingValueExpr::from_explain(left),
                op_tag: op.tag(),
                right: ProjectedGroupHavingValueExpr::from_explain(right),
            },
            ExplainGroupHavingExpr::And(children) => {
                Self::And(children.iter().map(Self::from_explain).collect())
            }
        }
    }

    fn from_plan(expr: &'a GroupHavingExpr) -> Self {
        match expr {
            GroupHavingExpr::Compare { left, op, right } => Self::Compare {
                left: ProjectedGroupHavingValueExpr::from_plan(left),
                op_tag: op.tag(),
                right: ProjectedGroupHavingValueExpr::from_plan(right),
            },
            GroupHavingExpr::And(children) => {
                Self::And(children.iter().map(Self::from_plan).collect())
            }
        }
    }
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
    let projected = ProjectedGroupHavingExpr::from_source(expr);

    hash_projected_group_having_expr(hasher, &projected);
}

fn hash_projected_group_having_value_expr(
    hasher: &mut Sha256,
    expr: &ProjectedGroupHavingValueExpr<'_>,
) {
    match expr {
        ProjectedGroupHavingValueExpr::GroupField { slot_index, field } => {
            write_tag(hasher, GROUP_HAVING_VALUE_GROUP_FIELD_TAG);
            write_u32(hasher, *slot_index);
            write_str(hasher, field);
        }
        ProjectedGroupHavingValueExpr::AggregateIndex { index } => {
            write_tag(hasher, GROUP_HAVING_VALUE_AGGREGATE_INDEX_TAG);
            write_u32(hasher, *index);
        }
        ProjectedGroupHavingValueExpr::Literal(value) => {
            write_tag(hasher, GROUP_HAVING_VALUE_LITERAL_TAG);
            write_value(hasher, value);
        }
        ProjectedGroupHavingValueExpr::FunctionCall { function, args } => {
            write_tag(hasher, GROUP_HAVING_VALUE_FUNCTION_TAG);
            write_str(hasher, function);
            write_u32(hasher, args.len() as u32);
            for arg in args {
                hash_projected_group_having_value_expr(hasher, arg);
            }
        }
        ProjectedGroupHavingValueExpr::Binary {
            op_tag,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_VALUE_BINARY_TAG);
            write_tag(hasher, *op_tag);
            hash_projected_group_having_value_expr(hasher, left);
            hash_projected_group_having_value_expr(hasher, right);
        }
    }
}

fn hash_projected_group_having_expr(hasher: &mut Sha256, expr: &ProjectedGroupHavingExpr<'_>) {
    match expr {
        ProjectedGroupHavingExpr::Compare {
            left,
            op_tag,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_projected_group_having_value_expr(hasher, left);
            write_tag(hasher, *op_tag);
            hash_projected_group_having_value_expr(hasher, right);
        }
        ProjectedGroupHavingExpr::And(children) => {
            write_tag(hasher, GROUP_HAVING_AND_TAG);
            write_u32(hasher, children.len() as u32);
            for child in children {
                hash_projected_group_having_expr(hasher, child);
            }
        }
    }
}

const fn grouped_having_binary_op_tag(op: BinaryOp) -> u8 {
    match op {
        BinaryOp::Add => 0x01,
        BinaryOp::Sub => 0x02,
        BinaryOp::Mul => 0x03,
        BinaryOp::Div => 0x04,
        #[cfg(test)]
        BinaryOp::And => 0x05,
        #[cfg(test)]
        BinaryOp::Eq => 0x06,
    }
}

fn grouped_having_binary_op_tag_from_explain(op: &str) -> u8 {
    match op {
        "+" => 0x01,
        "-" => 0x02,
        "*" => 0x03,
        "/" => 0x04,
        "and" => 0x05,
        "=" => 0x06,
        other => panic!("unsupported explain grouped HAVING binary op: {other}"),
    }
}
