use crate::db::sql::lowering::{SqlLoweringError, aggregate::lower_aggregate_call};
use crate::{
    db::{
        QueryError,
        query::{
            builder::{NumericProjectionExpr, RoundProjectionExpr, TextProjectionExpr},
            plan::expr::{BinaryOp, CaseWhenArm, Expr, FieldId, Function, UnaryOp},
        },
        sql::parser::{
            SqlExpr, SqlExprBinaryOp, SqlExprUnaryOp, SqlRoundProjectionCall,
            SqlRoundProjectionInput, SqlTextFunction, SqlTextFunctionCall,
        },
    },
    value::Value,
};

///
/// SqlExprPhase
///
/// Lowering-time SQL expression phase boundary.
/// Clause owners pass this to the shared SQL-expression lowering seam so
/// aggregate admission stays explicit instead of leaking through wrappers.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::sql::lowering) enum SqlExprPhase {
    Scalar,
    PreAggregate,
    PostAggregate,
}

// Lower one SQL expression tree into the canonical planner expression tree
// while enforcing the aggregate-admission rule for the owning clause phase.
pub(in crate::db::sql::lowering) fn lower_sql_expr(
    expr: &SqlExpr,
    phase: SqlExprPhase,
) -> Result<Expr, SqlLoweringError> {
    match expr {
        SqlExpr::Field(field) => Ok(Expr::Field(FieldId::new(field.clone()))),
        SqlExpr::Aggregate(aggregate) => {
            if !phase_allows_aggregate(phase) {
                return Err(phase_aggregate_error(phase));
            }

            Ok(Expr::Aggregate(lower_aggregate_call(aggregate.clone())?))
        }
        SqlExpr::Literal(literal) => Ok(Expr::Literal(literal.clone())),
        SqlExpr::TextFunction(call) => lower_text_function_expr(call),
        SqlExpr::Round(call) => lower_round_projection_expr(call, phase),
        SqlExpr::Unary { op, expr } => Ok(Expr::Unary {
            op: lower_sql_unary_op(*op),
            expr: Box::new(lower_sql_expr(expr.as_ref(), phase)?),
        }),
        SqlExpr::Binary { op, left, right } => {
            lower_sql_binary_expr(*op, left.as_ref(), right.as_ref(), phase)
        }
        SqlExpr::Case { arms, else_expr } => Ok(Expr::Case {
            when_then_arms: arms
                .iter()
                .map(|arm| {
                    Ok(CaseWhenArm::new(
                        lower_sql_expr(&arm.condition, phase)?,
                        lower_sql_expr(&arm.result, phase)?,
                    ))
                })
                .collect::<Result<Vec<_>, SqlLoweringError>>()?,
            else_expr: Box::new(match else_expr.as_ref() {
                Some(else_expr) => lower_sql_expr(else_expr.as_ref(), phase)?,
                None => Expr::Literal(Value::Null),
            }),
        }),
    }
}

fn lower_sql_binary_expr(
    op: SqlExprBinaryOp,
    left: &SqlExpr,
    right: &SqlExpr,
    phase: SqlExprPhase,
) -> Result<Expr, SqlLoweringError> {
    if let (SqlExpr::Field(field), SqlExpr::Literal(literal)) = (left, right)
        && let Some(expr) = lower_field_literal_numeric_expr(op, field.as_str(), literal)?
    {
        return Ok(expr);
    }

    Ok(Expr::Binary {
        op: lower_sql_binary_op(op),
        left: Box::new(lower_sql_expr(left, phase)?),
        right: Box::new(lower_sql_expr(right, phase)?),
    })
}

fn lower_field_literal_numeric_expr(
    op: SqlExprBinaryOp,
    field: &str,
    literal: &Value,
) -> Result<Option<Expr>, SqlLoweringError> {
    let builder = match op {
        SqlExprBinaryOp::Add => Some(NumericProjectionExpr::add_value(
            field.to_string(),
            literal.clone(),
        )),
        SqlExprBinaryOp::Sub => Some(NumericProjectionExpr::sub_value(
            field.to_string(),
            literal.clone(),
        )),
        SqlExprBinaryOp::Mul => Some(NumericProjectionExpr::mul_value(
            field.to_string(),
            literal.clone(),
        )),
        SqlExprBinaryOp::Div => Some(NumericProjectionExpr::div_value(
            field.to_string(),
            literal.clone(),
        )),
        SqlExprBinaryOp::Or
        | SqlExprBinaryOp::And
        | SqlExprBinaryOp::Eq
        | SqlExprBinaryOp::Ne
        | SqlExprBinaryOp::Lt
        | SqlExprBinaryOp::Lte
        | SqlExprBinaryOp::Gt
        | SqlExprBinaryOp::Gte => None,
    };

    builder
        .transpose()
        .map(|projection| projection.map(|projection| projection.expr().clone()))
        .map_err(SqlLoweringError::from)
}

// Return true when the SQL expression tree contains any aggregate leaf.
pub(in crate::db::sql::lowering) fn sql_expr_contains_aggregate(expr: &SqlExpr) -> bool {
    expr.contains_aggregate()
}

const fn phase_allows_aggregate(phase: SqlExprPhase) -> bool {
    matches!(phase, SqlExprPhase::PostAggregate)
}

fn phase_aggregate_error(phase: SqlExprPhase) -> SqlLoweringError {
    match phase {
        SqlExprPhase::Scalar => SqlLoweringError::unsupported_select_projection(),
        SqlExprPhase::PreAggregate => SqlLoweringError::unsupported_aggregate_input_expressions(),
        SqlExprPhase::PostAggregate => {
            unreachable!("post-aggregate lowering allows aggregate leaves")
        }
    }
}

const fn lower_sql_unary_op(op: SqlExprUnaryOp) -> UnaryOp {
    match op {
        SqlExprUnaryOp::Not => UnaryOp::Not,
    }
}

const fn lower_sql_binary_op(op: SqlExprBinaryOp) -> BinaryOp {
    match op {
        SqlExprBinaryOp::Or => BinaryOp::Or,
        SqlExprBinaryOp::And => BinaryOp::And,
        SqlExprBinaryOp::Eq => BinaryOp::Eq,
        SqlExprBinaryOp::Ne => BinaryOp::Ne,
        SqlExprBinaryOp::Lt => BinaryOp::Lt,
        SqlExprBinaryOp::Lte => BinaryOp::Lte,
        SqlExprBinaryOp::Gt => BinaryOp::Gt,
        SqlExprBinaryOp::Gte => BinaryOp::Gte,
        SqlExprBinaryOp::Add => BinaryOp::Add,
        SqlExprBinaryOp::Sub => BinaryOp::Sub,
        SqlExprBinaryOp::Mul => BinaryOp::Mul,
        SqlExprBinaryOp::Div => BinaryOp::Div,
    }
}

// Lower one admitted SQL text function through the existing builder-backed
// text projection seam so runtime/explain contracts stay unchanged.
fn lower_text_function_expr(call: &SqlTextFunctionCall) -> Result<Expr, SqlLoweringError> {
    text_function_spec(call.function).lower_expr(call)
}

fn lower_round_projection_expr(
    call: &SqlRoundProjectionCall,
    phase: SqlExprPhase,
) -> Result<Expr, SqlLoweringError> {
    let scale = validate_round_projection_scale(call.scale.clone())?;

    match &call.input {
        SqlRoundProjectionInput::Operand(crate::db::sql::parser::SqlProjectionOperand::Field(
            field,
        )) => RoundProjectionExpr::field(field.clone(), scale)
            .map(|projection| projection.expr().clone())
            .map_err(SqlLoweringError::from),
        SqlRoundProjectionInput::Operand(operand) => Ok(round_projection_expr(
            lower_sql_expr(
                &crate::db::sql::parser::SqlExpr::from_projection_operand(operand),
                phase,
            )?,
            scale,
        )),
        SqlRoundProjectionInput::Arithmetic(arithmetic) => Ok(round_projection_expr(
            lower_sql_expr(
                &crate::db::sql::parser::SqlExpr::Binary {
                    op: match arithmetic.op {
                        crate::db::sql::parser::SqlArithmeticProjectionOp::Add => {
                            SqlExprBinaryOp::Add
                        }
                        crate::db::sql::parser::SqlArithmeticProjectionOp::Sub => {
                            SqlExprBinaryOp::Sub
                        }
                        crate::db::sql::parser::SqlArithmeticProjectionOp::Mul => {
                            SqlExprBinaryOp::Mul
                        }
                        crate::db::sql::parser::SqlArithmeticProjectionOp::Div => {
                            SqlExprBinaryOp::Div
                        }
                    },
                    left: Box::new(crate::db::sql::parser::SqlExpr::from_projection_operand(
                        &arithmetic.left,
                    )),
                    right: Box::new(crate::db::sql::parser::SqlExpr::from_projection_operand(
                        &arithmetic.right,
                    )),
                },
                phase,
            )?,
            scale,
        )),
    }
}

#[derive(Clone, Copy)]
struct TextFnSpec {
    sql_function: SqlTextFunction,
    function: Function,
    builder: TextFnBuilder,
    contract: TextFnLiteralContract,
}

#[derive(Clone, Copy)]
enum TextFnBuilder {
    Unary,
    WithLiteral,
    Position,
    WithTwoLiterals,
    Substring,
}

#[derive(Clone, Copy)]
enum TextFnLiteralContract {
    None,
    OptionalPrimaryText,
    RequiredPrimaryNumeric,
    RequiredPrimaryTextRequiredSecondText,
    RequiredPrimaryNumericOptionalSecondNumeric,
}

const TEXT_FN_SPECS: [TextFnSpec; 14] = [
    TextFnSpec::new(
        SqlTextFunction::Trim,
        Function::Trim,
        TextFnBuilder::Unary,
        TextFnLiteralContract::None,
    ),
    TextFnSpec::new(
        SqlTextFunction::Ltrim,
        Function::Ltrim,
        TextFnBuilder::Unary,
        TextFnLiteralContract::None,
    ),
    TextFnSpec::new(
        SqlTextFunction::Rtrim,
        Function::Rtrim,
        TextFnBuilder::Unary,
        TextFnLiteralContract::None,
    ),
    TextFnSpec::new(
        SqlTextFunction::Lower,
        Function::Lower,
        TextFnBuilder::Unary,
        TextFnLiteralContract::None,
    ),
    TextFnSpec::new(
        SqlTextFunction::Upper,
        Function::Upper,
        TextFnBuilder::Unary,
        TextFnLiteralContract::None,
    ),
    TextFnSpec::new(
        SqlTextFunction::Length,
        Function::Length,
        TextFnBuilder::Unary,
        TextFnLiteralContract::None,
    ),
    TextFnSpec::new(
        SqlTextFunction::Left,
        Function::Left,
        TextFnBuilder::WithLiteral,
        TextFnLiteralContract::RequiredPrimaryNumeric,
    ),
    TextFnSpec::new(
        SqlTextFunction::Right,
        Function::Right,
        TextFnBuilder::WithLiteral,
        TextFnLiteralContract::RequiredPrimaryNumeric,
    ),
    TextFnSpec::new(
        SqlTextFunction::StartsWith,
        Function::StartsWith,
        TextFnBuilder::WithLiteral,
        TextFnLiteralContract::OptionalPrimaryText,
    ),
    TextFnSpec::new(
        SqlTextFunction::EndsWith,
        Function::EndsWith,
        TextFnBuilder::WithLiteral,
        TextFnLiteralContract::OptionalPrimaryText,
    ),
    TextFnSpec::new(
        SqlTextFunction::Contains,
        Function::Contains,
        TextFnBuilder::WithLiteral,
        TextFnLiteralContract::OptionalPrimaryText,
    ),
    TextFnSpec::new(
        SqlTextFunction::Position,
        Function::Position,
        TextFnBuilder::Position,
        TextFnLiteralContract::OptionalPrimaryText,
    ),
    TextFnSpec::new(
        SqlTextFunction::Replace,
        Function::Replace,
        TextFnBuilder::WithTwoLiterals,
        TextFnLiteralContract::RequiredPrimaryTextRequiredSecondText,
    ),
    TextFnSpec::new(
        SqlTextFunction::Substring,
        Function::Substring,
        TextFnBuilder::Substring,
        TextFnLiteralContract::RequiredPrimaryNumericOptionalSecondNumeric,
    ),
];

impl TextFnSpec {
    const fn new(
        sql_function: SqlTextFunction,
        function: Function,
        builder: TextFnBuilder,
        contract: TextFnLiteralContract,
    ) -> Self {
        Self {
            sql_function,
            function,
            builder,
            contract,
        }
    }

    fn lower_expr(self, call: &SqlTextFunctionCall) -> Result<Expr, SqlLoweringError> {
        self.validate(call)?;

        Ok(self.build_projection(call).expr().clone())
    }

    fn validate(self, call: &SqlTextFunctionCall) -> Result<(), SqlLoweringError> {
        let function_name = self.function.sql_label();
        let field = call.field.as_str();

        match self.contract {
            TextFnLiteralContract::None | TextFnLiteralContract::OptionalPrimaryText => {
                ensure_text_or_null_literal(
                    function_name,
                    field,
                    "literal",
                    call.literal.as_ref(),
                )?;
                ensure_literal_absent(
                    call.literal2.as_ref(),
                    "only REPLACE and SUBSTRING should carry a second projection literal",
                )?;
                ensure_literal_absent(
                    call.literal3.as_ref(),
                    "only numeric text projection helpers should carry extra literal arguments",
                )?;
            }
            TextFnLiteralContract::RequiredPrimaryNumeric => {
                validate_numeric_projection_literal(
                    function_name,
                    field,
                    "length",
                    call.literal.as_ref(),
                    true,
                )?;
                if call.literal2.is_some() || call.literal3.is_some() {
                    return Err(QueryError::invariant(format!(
                        "{function_name} projection item carried unexpected extra literal arguments",
                    ))
                    .into());
                }
            }
            TextFnLiteralContract::RequiredPrimaryTextRequiredSecondText => {
                ensure_text_or_null_literal(
                    function_name,
                    field,
                    "literal",
                    call.literal.as_ref(),
                )?;
                match call.literal2.as_ref() {
                    Some(Value::Null | Value::Text(_)) => {}
                    Some(other) => {
                        return Err(QueryError::unsupported_query(format!(
                            "REPLACE({field}, ..., ...) requires text or NULL replacement literal, found {other:?}",
                        ))
                        .into());
                    }
                    None => {
                        return Err(QueryError::invariant(
                            "REPLACE projection item was missing its replacement literal",
                        )
                        .into());
                    }
                }
                ensure_literal_absent(
                    call.literal3.as_ref(),
                    "only numeric text projection helpers should carry extra literal arguments",
                )?;
            }
            TextFnLiteralContract::RequiredPrimaryNumericOptionalSecondNumeric => {
                validate_numeric_projection_literal(
                    function_name,
                    field,
                    "start",
                    call.literal.as_ref(),
                    true,
                )?;
                validate_numeric_projection_literal(
                    function_name,
                    field,
                    "length",
                    call.literal2.as_ref(),
                    false,
                )?;
                if call.literal3.is_some() {
                    return Err(QueryError::invariant(
                        "SUBSTRING projection item carried an unexpected extra literal",
                    )
                    .into());
                }
            }
        }

        Ok(())
    }

    fn build_projection(self, call: &SqlTextFunctionCall) -> TextProjectionExpr {
        let field = call.field.clone();

        match self.builder {
            TextFnBuilder::Unary => TextProjectionExpr::unary(field, self.function),
            TextFnBuilder::WithLiteral => TextProjectionExpr::with_literal(
                field,
                self.function,
                call.literal.clone().unwrap_or(Value::Null),
            ),
            TextFnBuilder::Position => {
                TextProjectionExpr::position(field, call.literal.clone().unwrap_or(Value::Null))
            }
            TextFnBuilder::WithTwoLiterals => TextProjectionExpr::with_two_literals(
                field,
                self.function,
                call.literal.clone().unwrap_or(Value::Null),
                call.literal2.clone().unwrap_or(Value::Null),
            ),
            TextFnBuilder::Substring => match call.literal2.clone() {
                Some(length) => TextProjectionExpr::with_two_literals(
                    field,
                    self.function,
                    call.literal.clone().unwrap_or(Value::Null),
                    length,
                ),
                None => TextProjectionExpr::with_literal(
                    field,
                    self.function,
                    call.literal.clone().unwrap_or(Value::Null),
                ),
            },
        }
    }
}

fn text_function_spec(function: SqlTextFunction) -> TextFnSpec {
    TEXT_FN_SPECS
        .iter()
        .copied()
        .find(|spec| spec.sql_function == function)
        .expect("every admitted SQL text function should have one lowering spec")
}

fn ensure_text_or_null_literal(
    function_name: &str,
    field: &str,
    label: &str,
    literal: Option<&Value>,
) -> Result<(), SqlLoweringError> {
    match literal {
        None | Some(Value::Null | Value::Text(_)) => Ok(()),
        Some(other) => Err(QueryError::unsupported_query(format!(
            "{function_name}({field}, ...) requires text or NULL {label} argument, found {other:?}",
        ))
        .into()),
    }
}

fn ensure_literal_absent(
    literal: Option<&Value>,
    message: &'static str,
) -> Result<(), SqlLoweringError> {
    if literal.is_some() {
        return Err(QueryError::invariant(message).into());
    }

    Ok(())
}

fn validate_numeric_projection_literal(
    function_name: &str,
    field: &str,
    label: &str,
    value: Option<&Value>,
    required: bool,
) -> Result<(), SqlLoweringError> {
    match value {
        Some(Value::Null | Value::Int(_) | Value::Uint(_)) => Ok(()),
        Some(other) => Err(QueryError::unsupported_query(format!(
            "{function_name}({field}, ...) requires integer or NULL {label}, found {other:?}",
        ))
        .into()),
        None if required => Err(QueryError::invariant(format!(
            "{function_name} projection item was missing its {label} literal",
        ))
        .into()),
        None => Ok(()),
    }
}

fn validate_round_projection_scale(scale: Value) -> Result<u32, SqlLoweringError> {
    match scale {
        Value::Int(value) => u32::try_from(value).map_err(|_| {
            QueryError::unsupported_query(format!(
                "ROUND(...) requires non-negative integer scale, found {value}",
            ))
            .into()
        }),
        Value::Uint(value) => u32::try_from(value).map_err(|_| {
            QueryError::unsupported_query(format!(
                "ROUND(...) scale exceeds supported integer range, found {value}",
            ))
            .into()
        }),
        other => Err(QueryError::unsupported_query(format!(
            "ROUND(...) requires integer scale, found {other:?}",
        ))
        .into()),
    }
}

fn round_projection_expr(input: Expr, scale: u32) -> Expr {
    Expr::FunctionCall {
        function: Function::Round,
        args: vec![input, Expr::Literal(Value::Uint(u64::from(scale)))],
    }
}
