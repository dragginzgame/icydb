use crate::{
    db::query::{
        explain::{ExplainGroupHavingExpr, ExplainGroupHavingValueExpr},
        fingerprint::hash_parts::{
            GROUP_HAVING_ABSENT_TAG, GROUP_HAVING_AND_TAG, GROUP_HAVING_COMPARE_TAG,
            GROUP_HAVING_PRESENT_TAG, GROUP_HAVING_VALUE_AGGREGATE_INDEX_TAG,
            GROUP_HAVING_VALUE_BINARY_TAG, GROUP_HAVING_VALUE_CASE_ARM_TAG,
            GROUP_HAVING_VALUE_CASE_TAG, GROUP_HAVING_VALUE_EXPR_TAG,
            GROUP_HAVING_VALUE_FUNCTION_TAG, GROUP_HAVING_VALUE_GROUP_FIELD_TAG,
            GROUP_HAVING_VALUE_LITERAL_TAG, GROUP_HAVING_VALUE_UNARY_TAG, write_str, write_tag,
            write_u32, write_value,
        },
        plan::{
            FieldSlot, GroupAggregateSpec,
            expr::{BinaryOp, Expr, UnaryOp},
        },
    },
    value::Value,
};
use sha2::Sha256;

/// Canonical grouped HAVING expression source shared by plan and explain hashing.
pub(super) enum GroupHavingFingerprintSource<'a> {
    Explain(&'a ExplainGroupHavingExpr),
    Plan {
        expr: &'a Expr,
        group_fields: &'a [FieldSlot],
        aggregates: &'a [GroupAggregateSpec],
    },
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
    Unary {
        op_tag: u8,
        expr: Box<Self>,
    },
    Case {
        when_then_arms: Vec<ProjectedGroupHavingCaseArmExpr<'a>>,
        else_expr: Box<Self>,
    },
    Binary {
        op_tag: u8,
        left: Box<Self>,
        right: Box<Self>,
    },
}

/// Canonical grouped HAVING searched-CASE arm projection shared by plan and explain hashing.
enum ProjectedGroupHavingCaseArmExpr<'a> {
    Arm {
        condition: ProjectedGroupHavingValueExpr<'a>,
        result: ProjectedGroupHavingValueExpr<'a>,
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
    Value(ProjectedGroupHavingValueExpr<'a>),
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
            ExplainGroupHavingValueExpr::Unary { op, expr } => Self::Unary {
                op_tag: grouped_having_unary_op_tag_from_explain(op),
                expr: Box::new(Self::from_explain(expr)),
            },
            ExplainGroupHavingValueExpr::Case {
                when_then_arms,
                else_expr,
            } => Self::Case {
                when_then_arms: when_then_arms
                    .iter()
                    .map(ProjectedGroupHavingCaseArmExpr::from_explain)
                    .collect(),
                else_expr: Box::new(Self::from_explain(else_expr)),
            },
            ExplainGroupHavingValueExpr::Binary { op, left, right } => Self::Binary {
                op_tag: grouped_having_binary_op_tag_from_explain(op),
                left: Box::new(Self::from_explain(left)),
                right: Box::new(Self::from_explain(right)),
            },
        }
    }

    fn from_plan(
        expr: &'a Expr,
        group_fields: &'a [FieldSlot],
        aggregates: &'a [GroupAggregateSpec],
    ) -> Self {
        match expr {
            Expr::Field(field_id) => {
                let field_name = field_id.as_str();
                let field_slot = group_fields
                    .iter()
                    .find(|field| field.field() == field_name)
                    .expect("grouped HAVING fingerprint requires grouped key fields");

                Self::GroupField {
                    slot_index: field_slot.index() as u32,
                    field: field_slot.field(),
                }
            }
            Expr::Aggregate(aggregate_expr) => {
                let index = aggregates
                    .iter()
                    .position(|aggregate| {
                        let distinct_matches = aggregate.distinct() == aggregate_expr.is_distinct();

                        aggregate.kind() == aggregate_expr.kind()
                            && aggregate.target_field() == aggregate_expr.target_field()
                            && aggregate.input_expr() == aggregate_expr.input_expr()
                            && distinct_matches
                    })
                    .expect("grouped HAVING fingerprint requires declared grouped aggregates");

                Self::AggregateIndex {
                    index: index as u32,
                }
            }
            Expr::Literal(value) => Self::Literal(value),
            Expr::FunctionCall { function, args } => Self::FunctionCall {
                function: function.sql_label(),
                args: args
                    .iter()
                    .map(|arg| Self::from_plan(arg, group_fields, aggregates))
                    .collect(),
            },
            Expr::Unary { op, expr } => Self::Unary {
                op_tag: grouped_having_unary_op_tag(*op),
                expr: Box::new(Self::from_plan(expr, group_fields, aggregates)),
            },
            Expr::Case {
                when_then_arms,
                else_expr,
            } => Self::Case {
                when_then_arms: when_then_arms
                    .iter()
                    .map(|arm| {
                        ProjectedGroupHavingCaseArmExpr::from_plan(
                            arm.condition(),
                            arm.result(),
                            group_fields,
                            aggregates,
                        )
                    })
                    .collect(),
                else_expr: Box::new(Self::from_plan(else_expr, group_fields, aggregates)),
            },
            Expr::Binary { op, left, right } => Self::Binary {
                op_tag: grouped_having_binary_op_tag(*op),
                left: Box::new(Self::from_plan(left, group_fields, aggregates)),
                right: Box::new(Self::from_plan(right, group_fields, aggregates)),
            },
            #[cfg(test)]
            Expr::Alias { expr, name: _ } => Self::from_plan(expr, group_fields, aggregates),
        }
    }
}

impl<'a> ProjectedGroupHavingCaseArmExpr<'a> {
    fn from_explain(arm: &'a crate::db::query::explain::ExplainGroupHavingCaseArm) -> Self {
        Self::Arm {
            condition: ProjectedGroupHavingValueExpr::from_explain(&arm.condition),
            result: ProjectedGroupHavingValueExpr::from_explain(&arm.result),
        }
    }

    fn from_plan(
        condition: &'a Expr,
        result: &'a Expr,
        group_fields: &'a [FieldSlot],
        aggregates: &'a [GroupAggregateSpec],
    ) -> Self {
        Self::Arm {
            condition: ProjectedGroupHavingValueExpr::from_plan(
                condition,
                group_fields,
                aggregates,
            ),
            result: ProjectedGroupHavingValueExpr::from_plan(result, group_fields, aggregates),
        }
    }
}

impl<'a> ProjectedGroupHavingExpr<'a> {
    fn from_source(source: &'a GroupHavingFingerprintSource<'a>) -> Self {
        match source {
            GroupHavingFingerprintSource::Explain(expr) => Self::from_explain(expr),
            GroupHavingFingerprintSource::Plan {
                expr,
                group_fields,
                aggregates,
            } => Self::from_plan(expr, group_fields, aggregates),
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
            ExplainGroupHavingExpr::Value(expr) => {
                Self::Value(ProjectedGroupHavingValueExpr::from_explain(expr))
            }
        }
    }

    fn from_plan(
        expr: &'a Expr,
        group_fields: &'a [FieldSlot],
        aggregates: &'a [GroupAggregateSpec],
    ) -> Self {
        match expr {
            Expr::Binary { op, left, right } => match op {
                BinaryOp::Eq => Self::Compare {
                    left: ProjectedGroupHavingValueExpr::from_plan(left, group_fields, aggregates),
                    op_tag: crate::db::predicate::CompareOp::Eq.tag(),
                    right: ProjectedGroupHavingValueExpr::from_plan(
                        right,
                        group_fields,
                        aggregates,
                    ),
                },
                BinaryOp::Ne => Self::Compare {
                    left: ProjectedGroupHavingValueExpr::from_plan(left, group_fields, aggregates),
                    op_tag: crate::db::predicate::CompareOp::Ne.tag(),
                    right: ProjectedGroupHavingValueExpr::from_plan(
                        right,
                        group_fields,
                        aggregates,
                    ),
                },
                BinaryOp::Lt => Self::Compare {
                    left: ProjectedGroupHavingValueExpr::from_plan(left, group_fields, aggregates),
                    op_tag: crate::db::predicate::CompareOp::Lt.tag(),
                    right: ProjectedGroupHavingValueExpr::from_plan(
                        right,
                        group_fields,
                        aggregates,
                    ),
                },
                BinaryOp::Lte => Self::Compare {
                    left: ProjectedGroupHavingValueExpr::from_plan(left, group_fields, aggregates),
                    op_tag: crate::db::predicate::CompareOp::Lte.tag(),
                    right: ProjectedGroupHavingValueExpr::from_plan(
                        right,
                        group_fields,
                        aggregates,
                    ),
                },
                BinaryOp::Gt => Self::Compare {
                    left: ProjectedGroupHavingValueExpr::from_plan(left, group_fields, aggregates),
                    op_tag: crate::db::predicate::CompareOp::Gt.tag(),
                    right: ProjectedGroupHavingValueExpr::from_plan(
                        right,
                        group_fields,
                        aggregates,
                    ),
                },
                BinaryOp::Gte => Self::Compare {
                    left: ProjectedGroupHavingValueExpr::from_plan(left, group_fields, aggregates),
                    op_tag: crate::db::predicate::CompareOp::Gte.tag(),
                    right: ProjectedGroupHavingValueExpr::from_plan(
                        right,
                        group_fields,
                        aggregates,
                    ),
                },
                BinaryOp::And => Self::And(vec![
                    Self::from_plan(left, group_fields, aggregates),
                    Self::from_plan(right, group_fields, aggregates),
                ]),
                BinaryOp::Or | BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
                    Self::Value(ProjectedGroupHavingValueExpr::from_plan(
                        expr,
                        group_fields,
                        aggregates,
                    ))
                }
            },
            _ => Self::Value(ProjectedGroupHavingValueExpr::from_plan(
                expr,
                group_fields,
                aggregates,
            )),
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
        ProjectedGroupHavingValueExpr::Unary { op_tag, expr } => {
            write_tag(hasher, GROUP_HAVING_VALUE_UNARY_TAG);
            write_tag(hasher, *op_tag);
            hash_projected_group_having_value_expr(hasher, expr);
        }
        ProjectedGroupHavingValueExpr::Case {
            when_then_arms,
            else_expr,
        } => {
            write_tag(hasher, GROUP_HAVING_VALUE_CASE_TAG);
            write_u32(hasher, when_then_arms.len() as u32);
            for arm in when_then_arms {
                hash_projected_group_having_case_arm_expr(hasher, arm);
            }
            hash_projected_group_having_value_expr(hasher, else_expr);
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

fn hash_projected_group_having_case_arm_expr(
    hasher: &mut Sha256,
    expr: &ProjectedGroupHavingCaseArmExpr<'_>,
) {
    match expr {
        ProjectedGroupHavingCaseArmExpr::Arm { condition, result } => {
            write_tag(hasher, GROUP_HAVING_VALUE_CASE_ARM_TAG);
            hash_projected_group_having_value_expr(hasher, condition);
            hash_projected_group_having_value_expr(hasher, result);
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
        ProjectedGroupHavingExpr::Value(expr) => {
            write_tag(hasher, GROUP_HAVING_VALUE_EXPR_TAG);
            hash_projected_group_having_value_expr(hasher, expr);
        }
    }
}

const fn grouped_having_unary_op_tag(op: UnaryOp) -> u8 {
    match op {
        UnaryOp::Not => 0x01,
    }
}

fn grouped_having_unary_op_tag_from_explain(op: &str) -> u8 {
    match op {
        "NOT" => 0x01,
        other => panic!("unexpected grouped HAVING unary operator label: {other}"),
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

fn grouped_having_binary_op_tag_from_explain(op: &str) -> u8 {
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
