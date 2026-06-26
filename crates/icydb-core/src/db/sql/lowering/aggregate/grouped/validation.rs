use crate::{
    db::{
        query::builder::AggregateExpr,
        schema::SchemaInfo,
        sql::{
            lowering::{
                SqlLoweringError,
                aggregate::lowering::{
                    LoweredSqlAggregateShape, validate_analyzed_model_bound_scalar_expr,
                },
            },
            parser::{SqlAggregateCall, SqlExpr, SqlSelectItem},
        },
    },
    model::entity::EntityModel,
    value::{Value, hash_value},
};
use std::{
    collections::{HashMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
};

///
/// SqlAggregateCallInterner
///
/// Retains first-seen aggregate-call order while avoiding a full retained-call
/// scan for every distinct aggregate leaf collected from grouped SQL.
///
pub(in crate::db::sql::lowering) struct SqlAggregateCallInterner {
    indices_by_fingerprint: HashMap<u64, Vec<usize>>,
}

impl SqlAggregateCallInterner {
    pub(in crate::db::sql::lowering) fn new() -> Self {
        Self {
            indices_by_fingerprint: HashMap::new(),
        }
    }

    pub(in crate::db::sql::lowering) fn from_existing(
        aggregate_calls: &[SqlAggregateCall],
    ) -> Self {
        let mut interner = Self::new();
        for (index, aggregate) in aggregate_calls.iter().enumerate() {
            interner
                .indices_by_fingerprint
                .entry(sql_aggregate_call_fingerprint(aggregate))
                .or_default()
                .push(index);
        }

        interner
    }

    pub(in crate::db::sql::lowering) fn extend_expr(
        &mut self,
        aggregate_calls: &mut Vec<SqlAggregateCall>,
        expr: &SqlExpr,
    ) {
        expr.for_each_tree_aggregate(&mut |aggregate| {
            self.push_unique(aggregate_calls, aggregate.clone());
        });
    }

    pub(in crate::db::sql::lowering) fn extend_select_item(
        &mut self,
        aggregate_calls: &mut Vec<SqlAggregateCall>,
        item: &SqlSelectItem,
    ) {
        match item {
            SqlSelectItem::Field(_) => {}
            SqlSelectItem::Aggregate(aggregate) => {
                self.push_unique(aggregate_calls, aggregate.clone());
            }
            SqlSelectItem::Expr(expr) => {
                self.extend_expr(aggregate_calls, expr);
            }
        }
    }

    fn push_unique(
        &mut self,
        aggregate_calls: &mut Vec<SqlAggregateCall>,
        aggregate: SqlAggregateCall,
    ) {
        let fingerprint = sql_aggregate_call_fingerprint(&aggregate);
        let indices = self.indices_by_fingerprint.entry(fingerprint).or_default();
        if indices
            .iter()
            .any(|index| aggregate_calls.get(*index) == Some(&aggregate))
        {
            return;
        }

        indices.push(aggregate_calls.len());
        aggregate_calls.push(aggregate);
    }
}

pub(in crate::db::sql::lowering) fn resolve_having_aggregate_expr_index(
    target: &AggregateExpr,
    grouped_aggregates: &[AggregateExpr],
) -> Result<usize, SqlLoweringError> {
    let mut matched = grouped_aggregates
        .iter()
        .enumerate()
        .filter_map(|(index, aggregate)| (aggregate == target).then_some(index));
    let Some(index) = matched.next() else {
        return Err(SqlLoweringError::unsupported_select_having());
    };
    if matched.next().is_some() {
        return Err(SqlLoweringError::unsupported_select_having());
    }

    Ok(index)
}

// Keep grouped aggregate scalar-subexpression validation on one lowering seam
// so alias leakage inside FILTER or aggregate inputs fails as a user-facing
// SQL error before grouped execution reaches its scalar compiler invariant.
pub(in crate::db::sql::lowering::aggregate) fn validate_grouped_aggregate_scalar_subexpressions(
    model: &'static EntityModel,
    schema: &SchemaInfo,
    aggregate: &LoweredSqlAggregateShape,
) -> Result<(), SqlLoweringError> {
    if let Some(input_expr) = aggregate.input_expr() {
        validate_analyzed_model_bound_scalar_expr(
            model,
            schema,
            input_expr,
            SqlLoweringError::unsupported_aggregate_input_expressions,
        )?;
    }
    if let Some(filter_expr) = aggregate.filter_expr() {
        validate_analyzed_model_bound_scalar_expr(
            model,
            schema,
            filter_expr,
            SqlLoweringError::unsupported_where_expression,
        )?;
    }

    Ok(())
}

fn sql_aggregate_call_fingerprint(aggregate: &SqlAggregateCall) -> u64 {
    let mut hasher = DefaultHasher::new();
    "aggregate".hash(&mut hasher);
    sql_aggregate_kind_tag(aggregate.kind).hash(&mut hasher);
    aggregate.distinct.hash(&mut hasher);
    sql_expr_option_fingerprint(aggregate.input.as_deref()).hash(&mut hasher);
    sql_expr_option_fingerprint(aggregate.filter_expr.as_deref()).hash(&mut hasher);
    hasher.finish()
}

fn sql_expr_option_fingerprint(expr: Option<&SqlExpr>) -> u64 {
    let mut hasher = DefaultHasher::new();
    expr.is_some().hash(&mut hasher);
    if let Some(expr) = expr {
        sql_expr_fingerprint(expr).hash(&mut hasher);
    }
    hasher.finish()
}

fn sql_expr_fingerprint(expr: &SqlExpr) -> u64 {
    let mut hasher = DefaultHasher::new();
    match expr {
        SqlExpr::Field(field) => {
            0_u8.hash(&mut hasher);
            field.hash(&mut hasher);
        }
        SqlExpr::FieldPath { root, segments } => {
            1_u8.hash(&mut hasher);
            root.hash(&mut hasher);
            segments.hash(&mut hasher);
        }
        SqlExpr::Aggregate(aggregate) => {
            2_u8.hash(&mut hasher);
            sql_aggregate_call_fingerprint(aggregate).hash(&mut hasher);
        }
        SqlExpr::Literal(value) => {
            3_u8.hash(&mut hasher);
            value_fingerprint(value).hash(&mut hasher);
        }
        SqlExpr::Param { index } => {
            4_u8.hash(&mut hasher);
            index.hash(&mut hasher);
        }
        SqlExpr::Membership {
            expr,
            values,
            negated,
        } => {
            5_u8.hash(&mut hasher);
            sql_expr_fingerprint(expr).hash(&mut hasher);
            negated.hash(&mut hasher);
            values.len().hash(&mut hasher);
            for value in values {
                value_fingerprint(value).hash(&mut hasher);
            }
        }
        SqlExpr::NullTest { expr, negated } => {
            6_u8.hash(&mut hasher);
            sql_expr_fingerprint(expr).hash(&mut hasher);
            negated.hash(&mut hasher);
        }
        SqlExpr::Like {
            expr,
            pattern,
            negated,
            casefold,
        } => {
            7_u8.hash(&mut hasher);
            sql_expr_fingerprint(expr).hash(&mut hasher);
            pattern.hash(&mut hasher);
            negated.hash(&mut hasher);
            casefold.hash(&mut hasher);
        }
        SqlExpr::FunctionCall { function, args } => {
            8_u8.hash(&mut hasher);
            sql_scalar_function_tag(*function).hash(&mut hasher);
            args.len().hash(&mut hasher);
            for arg in args {
                sql_expr_fingerprint(arg).hash(&mut hasher);
            }
        }
        SqlExpr::Unary { op, expr } => {
            9_u8.hash(&mut hasher);
            sql_unary_op_tag(*op).hash(&mut hasher);
            sql_expr_fingerprint(expr).hash(&mut hasher);
        }
        SqlExpr::Binary { op, left, right } => {
            10_u8.hash(&mut hasher);
            sql_binary_op_tag(*op).hash(&mut hasher);
            sql_expr_fingerprint(left).hash(&mut hasher);
            sql_expr_fingerprint(right).hash(&mut hasher);
        }
        SqlExpr::Case { arms, else_expr } => {
            11_u8.hash(&mut hasher);
            arms.len().hash(&mut hasher);
            for arm in arms {
                sql_expr_fingerprint(&arm.condition).hash(&mut hasher);
                sql_expr_fingerprint(&arm.result).hash(&mut hasher);
            }
            sql_expr_option_fingerprint(else_expr.as_deref()).hash(&mut hasher);
        }
    }

    hasher.finish()
}

fn value_fingerprint(value: &Value) -> [u8; 16] {
    hash_value(value).unwrap_or_else(|_| {
        let mut hasher = DefaultHasher::new();
        value.canonical_tag().to_u8().hash(&mut hasher);
        let digest = hasher.finish().to_be_bytes();
        let mut out = [0_u8; 16];
        out[..8].copy_from_slice(&digest);
        out[8..].copy_from_slice(&digest);
        out
    })
}

const fn sql_aggregate_kind_tag(kind: crate::db::sql::parser::SqlAggregateKind) -> u8 {
    match kind {
        crate::db::sql::parser::SqlAggregateKind::Count => 0,
        crate::db::sql::parser::SqlAggregateKind::Sum => 1,
        crate::db::sql::parser::SqlAggregateKind::Avg => 2,
        crate::db::sql::parser::SqlAggregateKind::Min => 3,
        crate::db::sql::parser::SqlAggregateKind::Max => 4,
    }
}

const fn sql_unary_op_tag(op: crate::db::sql::parser::SqlExprUnaryOp) -> u8 {
    match op {
        crate::db::sql::parser::SqlExprUnaryOp::Not => 0,
    }
}

const fn sql_binary_op_tag(op: crate::db::sql::parser::SqlExprBinaryOp) -> u8 {
    match op {
        crate::db::sql::parser::SqlExprBinaryOp::Or => 0,
        crate::db::sql::parser::SqlExprBinaryOp::And => 1,
        crate::db::sql::parser::SqlExprBinaryOp::Eq => 2,
        crate::db::sql::parser::SqlExprBinaryOp::Ne => 3,
        crate::db::sql::parser::SqlExprBinaryOp::Lt => 4,
        crate::db::sql::parser::SqlExprBinaryOp::Lte => 5,
        crate::db::sql::parser::SqlExprBinaryOp::Gt => 6,
        crate::db::sql::parser::SqlExprBinaryOp::Gte => 7,
        crate::db::sql::parser::SqlExprBinaryOp::Add => 8,
        crate::db::sql::parser::SqlExprBinaryOp::Sub => 9,
        crate::db::sql::parser::SqlExprBinaryOp::Mul => 10,
        crate::db::sql::parser::SqlExprBinaryOp::Div => 11,
    }
}

const fn sql_scalar_function_tag(function: crate::db::sql::parser::SqlScalarFunction) -> u8 {
    use crate::db::sql::parser::SqlScalarFunction;

    match function {
        SqlScalarFunction::Abs => 0,
        SqlScalarFunction::Cbrt => 1,
        SqlScalarFunction::Ceiling => 2,
        SqlScalarFunction::Coalesce => 3,
        SqlScalarFunction::Contains => 4,
        SqlScalarFunction::EndsWith => 5,
        SqlScalarFunction::Exp => 6,
        SqlScalarFunction::Floor => 7,
        #[cfg(test)]
        SqlScalarFunction::IsEmpty => 8,
        #[cfg(test)]
        SqlScalarFunction::IsMissing => 9,
        #[cfg(test)]
        SqlScalarFunction::IsNotEmpty => 10,
        #[cfg(test)]
        SqlScalarFunction::IsNotNull => 11,
        #[cfg(test)]
        SqlScalarFunction::IsNull => 12,
        SqlScalarFunction::Left => 13,
        SqlScalarFunction::Length => 14,
        SqlScalarFunction::Ln => 15,
        SqlScalarFunction::Log => 16,
        SqlScalarFunction::Log2 => 17,
        SqlScalarFunction::Log10 => 18,
        SqlScalarFunction::Lower => 19,
        SqlScalarFunction::Ltrim => 20,
        SqlScalarFunction::Mod => 21,
        SqlScalarFunction::NullIf => 22,
        SqlScalarFunction::OctetLength => 23,
        SqlScalarFunction::Position => 24,
        SqlScalarFunction::Power => 25,
        SqlScalarFunction::Replace => 26,
        SqlScalarFunction::Right => 27,
        SqlScalarFunction::Round => 28,
        SqlScalarFunction::Rtrim => 29,
        SqlScalarFunction::Sign => 30,
        SqlScalarFunction::Sqrt => 31,
        SqlScalarFunction::StartsWith => 32,
        SqlScalarFunction::Substring => 33,
        SqlScalarFunction::Trim => 34,
        SqlScalarFunction::Trunc => 35,
        SqlScalarFunction::Upper => 36,
    }
}
