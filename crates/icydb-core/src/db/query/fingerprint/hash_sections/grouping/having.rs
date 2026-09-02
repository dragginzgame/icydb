use crate::db::query::{
    builder::{AggregateExpr, scalar_projection::render_scalar_projection_expr_plan_label},
    explain::{ExplainGroupAggregate, ExplainGroupField},
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
        AggregateIdentity, AggregateSemanticKey, GroupAggregateSpec, GroupFieldSet,
        expr::{BinaryOp, CaseWhenArm, Expr, UnaryOp},
    },
};
use sha2::Sha256;

const GROUP_HAVING_MISSING_SLOT_SENTINEL: u32 = u32::MAX;

/// Canonical grouped HAVING expression source shared by plan and explain hashing.
pub(super) enum GroupHavingFingerprintSource<'a> {
    Explain {
        expr: &'a Expr,
        group_fields: &'a [ExplainGroupField],
        aggregates: &'a [ExplainGroupAggregate],
    },
    PlanBorrowed {
        expr: &'a Expr,
        group_fields: &'a GroupFieldSet,
        aggregates: &'a [GroupAggregateSpec],
    },
    PlanOwned {
        expr: Expr,
        group_fields: &'a GroupFieldSet,
        aggregates: &'a [GroupAggregateSpec],
    },
}

enum GroupHavingFingerprintContext<'a> {
    Explain {
        group_fields: &'a [ExplainGroupField],
        aggregates: &'a [ExplainGroupAggregate],
    },
    Plan {
        group_fields: &'a GroupFieldSet,
        aggregates: &'a [GroupAggregateSpec],
    },
}

impl GroupHavingFingerprintContext<'_> {
    fn group_field<'a>(&'a self, expr: &Expr) -> Option<(u32, &'a str)> {
        match self {
            Self::Explain { group_fields, .. } => {
                let Expr::Field(field_id) = expr else {
                    return None;
                };
                group_fields
                    .iter()
                    .find(|field| field.field() == field_id.as_str())
                    .map(|field| (field.slot_index() as u32, field.field()))
            }
            Self::Plan { group_fields, .. } => group_fields
                .iter()
                .find(|field| field.matches_expr(expr))
                .map(|field| (field.root_slot() as u32, field.field())),
        }
    }

    fn aggregate_index(&self, aggregate_expr: &AggregateExpr) -> Option<usize> {
        match self {
            Self::Explain { aggregates, .. } => {
                let semantic_distinct =
                    AggregateIdentity::from_aggregate_expr(aggregate_expr).distinct();
                let input_expr = aggregate_expr
                    .input_expr()
                    .map(render_scalar_projection_expr_plan_label);
                let filter_expr = aggregate_expr
                    .filter_expr()
                    .map(render_scalar_projection_expr_plan_label);

                aggregates.iter().position(|aggregate| {
                    let input_matches = aggregate.input_expr() == input_expr.as_deref();
                    let filter_matches = aggregate.filter_expr() == filter_expr.as_deref();

                    aggregate.kind() == aggregate_expr.kind()
                        && aggregate.target_field() == aggregate_expr.target_field()
                        && input_matches
                        && filter_matches
                        && aggregate.distinct() == semantic_distinct
                })
            }
            Self::Plan { aggregates, .. } => {
                let semantic_key = AggregateSemanticKey::from_aggregate_expr(aggregate_expr);
                aggregates
                    .iter()
                    .position(|aggregate| aggregate.semantic_key() == semantic_key)
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
    match expr {
        GroupHavingFingerprintSource::Explain {
            expr,
            group_fields,
            aggregates,
        } => hash_group_having_expr(
            hasher,
            expr,
            &GroupHavingFingerprintContext::Explain {
                group_fields,
                aggregates,
            },
        ),
        GroupHavingFingerprintSource::PlanBorrowed {
            expr,
            group_fields,
            aggregates,
        } => hash_group_having_expr(
            hasher,
            expr,
            &GroupHavingFingerprintContext::Plan {
                group_fields,
                aggregates,
            },
        ),
        GroupHavingFingerprintSource::PlanOwned {
            expr,
            group_fields,
            aggregates,
        } => hash_group_having_expr(
            hasher,
            expr,
            &GroupHavingFingerprintContext::Plan {
                group_fields,
                aggregates,
            },
        ),
    }
}

fn hash_group_having_expr(
    hasher: &mut Sha256,
    expr: &Expr,
    context: &GroupHavingFingerprintContext<'_>,
) {
    match expr {
        Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr(hasher, left, context);
            write_tag(hasher, 0x03);
            hash_group_having_value_expr(hasher, right, context);
        }
        Expr::Binary {
            op: BinaryOp::Ne,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr(hasher, left, context);
            write_tag(hasher, 0x04);
            hash_group_having_value_expr(hasher, right, context);
        }
        Expr::Binary {
            op: BinaryOp::Lt,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr(hasher, left, context);
            write_tag(hasher, 0x05);
            hash_group_having_value_expr(hasher, right, context);
        }
        Expr::Binary {
            op: BinaryOp::Lte,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr(hasher, left, context);
            write_tag(hasher, 0x06);
            hash_group_having_value_expr(hasher, right, context);
        }
        Expr::Binary {
            op: BinaryOp::Gt,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr(hasher, left, context);
            write_tag(hasher, 0x07);
            hash_group_having_value_expr(hasher, right, context);
        }
        Expr::Binary {
            op: BinaryOp::Gte,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_COMPARE_TAG);
            hash_group_having_value_expr(hasher, left, context);
            write_tag(hasher, 0x08);
            hash_group_having_value_expr(hasher, right, context);
        }
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            write_tag(hasher, GROUP_HAVING_AND_TAG);
            write_u32(hasher, 2);
            hash_group_having_expr(hasher, left, context);
            hash_group_having_expr(hasher, right, context);
        }
        _ => {
            write_tag(hasher, GROUP_HAVING_VALUE_EXPR_TAG);
            hash_group_having_value_expr(hasher, expr, context);
        }
    }
}

fn hash_group_having_value_expr(
    hasher: &mut Sha256,
    expr: &Expr,
    context: &GroupHavingFingerprintContext<'_>,
) {
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
            write_tag(hasher, GROUP_HAVING_VALUE_AGGREGATE_INDEX_TAG);
            if let Some(index) = context.aggregate_index(aggregate_expr) {
                write_u32(hasher, index as u32);
            } else {
                write_u32(hasher, GROUP_HAVING_MISSING_SLOT_SENTINEL);
                hash_missing_group_having_aggregate_expr(hasher, aggregate_expr);
            }
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
                hash_group_having_value_expr(hasher, arg, context);
            }
        }
        Expr::Unary { op, expr } => {
            write_tag(hasher, GROUP_HAVING_VALUE_UNARY_TAG);
            write_tag(hasher, grouped_having_unary_op_tag(*op));
            hash_group_having_value_expr(hasher, expr, context);
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            write_tag(hasher, GROUP_HAVING_VALUE_CASE_TAG);
            write_u32(hasher, when_then_arms.len() as u32);
            for arm in when_then_arms {
                hash_group_having_case_arm(hasher, arm, context);
            }
            hash_group_having_value_expr(hasher, else_expr, context);
        }
        Expr::Binary { op, left, right } => {
            write_tag(hasher, GROUP_HAVING_VALUE_BINARY_TAG);
            write_tag(hasher, grouped_having_binary_op_tag(*op));
            hash_group_having_value_expr(hasher, left, context);
            hash_group_having_value_expr(hasher, right, context);
        }
        #[cfg(test)]
        Expr::Alias { expr, .. } => {
            hash_group_having_value_expr(hasher, expr, context);
        }
    }
}

fn hash_missing_group_having_aggregate_expr(hasher: &mut Sha256, aggregate_expr: &AggregateExpr) {
    let identity = AggregateIdentity::from_aggregate_expr(aggregate_expr);
    let input_expr = aggregate_expr
        .input_expr()
        .map(render_scalar_projection_expr_plan_label);
    let filter_expr = aggregate_expr
        .filter_expr()
        .map(render_scalar_projection_expr_plan_label);

    write_tag(hasher, aggregate_expr.kind().fingerprint_tag());
    write_optional_str(hasher, aggregate_expr.target_field());
    write_optional_str(hasher, input_expr.as_deref());
    write_optional_str(hasher, filter_expr.as_deref());
    write_bool(hasher, identity.distinct());
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
    context: &GroupHavingFingerprintContext<'_>,
) {
    write_tag(hasher, GROUP_HAVING_VALUE_CASE_ARM_TAG);
    hash_group_having_value_expr(hasher, expr.condition(), context);
    hash_group_having_value_expr(hasher, expr.result(), context);
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
