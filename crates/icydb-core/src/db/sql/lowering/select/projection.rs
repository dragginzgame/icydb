use crate::db::sql::lowering::{SqlLoweringError, aggregate::lower_aggregate_call};
use crate::{
    db::{
        QueryError,
        query::{
            builder::{NumericProjectionExpr, RoundProjectionExpr, TextProjectionExpr},
            plan::expr::{
                Alias, Expr, FieldId, Function, ProjectionField, ProjectionSelection,
                expr_references_only_fields,
            },
        },
        sql::parser::{
            SqlArithmeticProjectionCall, SqlArithmeticProjectionOp, SqlProjection,
            SqlProjectionOperand, SqlRoundProjectionCall, SqlRoundProjectionInput, SqlSelectItem,
            SqlTextFunction, SqlTextFunctionCall,
        },
    },
    model::entity::{EntityModel, resolve_field_slot},
    value::Value,
};

// One bounded lowering spec for one admitted SQL text function.
#[derive(Clone, Copy)]
struct TextFnSpec {
    sql_function: SqlTextFunction,
    function: Function,
    builder: TextFnBuilder,
    contract: TextFnLiteralContract,
}

// Build shape for one admitted SQL text function.
#[derive(Clone, Copy)]
enum TextFnBuilder {
    Unary,
    WithLiteral,
    Position,
    WithTwoLiterals,
    Substring,
}

// Literal contract for one admitted SQL text function.
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

pub(super) fn lower_scalar_projection_selection(
    projection: SqlProjection,
    projection_aliases: &[Option<String>],
    distinct: bool,
) -> Result<ProjectionSelection, SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        let _ = distinct;
        return Ok(ProjectionSelection::All);
    };

    let has_aggregate = items
        .iter()
        .any(|item| matches!(item, SqlSelectItem::Aggregate(_)));
    if has_aggregate {
        return Err(SqlLoweringError::unsupported_select_projection());
    }

    if let Some(field_ids) = direct_scalar_field_selection(items.as_slice(), projection_aliases) {
        return Ok(ProjectionSelection::Fields(field_ids));
    }

    let fields = items
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            lower_projection_field(
                item,
                projection_aliases.get(index).and_then(Option::as_deref),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    if distinct && fields.is_empty() {
        return Ok(ProjectionSelection::Exprs(fields));
    }

    Ok(ProjectionSelection::Exprs(fields))
}

pub(super) fn lower_grouped_projection_selection(
    projection: SqlProjection,
    projection_aliases: &[Option<String>],
    group_by: &[String],
    allow_identity_fast_path: bool,
    model: &'static EntityModel,
) -> Result<ProjectionSelection, SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        return Err(SqlLoweringError::unsupported_select_group_by());
    };
    let grouped_field_names = group_by.iter().map(String::as_str).collect::<Vec<_>>();

    let mut seen_aggregate = false;
    let mut fields = Vec::with_capacity(items.len());

    for (index, item) in items.into_iter().enumerate() {
        let expr = lower_select_item_expr(&item)?;
        let contains_aggregate = expr_contains_aggregate(&expr);
        if seen_aggregate && !contains_aggregate {
            return Err(SqlLoweringError::unsupported_select_group_by());
        }
        validate_grouped_projection_expr(model, &expr, grouped_field_names.as_slice())?;
        seen_aggregate |= contains_aggregate;

        fields.push(ProjectionField::Scalar {
            expr,
            alias: projection_aliases
                .get(index)
                .and_then(Option::as_deref)
                .map(Alias::new),
        });
    }

    if !seen_aggregate {
        return Err(SqlLoweringError::unsupported_select_group_by());
    }

    if allow_identity_fast_path
        && projection_aliases.iter().all(Option::is_none)
        && grouped_projection_is_canonical_identity(fields.as_slice(), group_by)
    {
        return Ok(ProjectionSelection::All);
    }

    Ok(ProjectionSelection::Exprs(fields))
}

// Validate one grouped projection expression against grouped-key authority
// while preserving specific unknown-field diagnostics.
fn validate_grouped_projection_expr(
    model: &EntityModel,
    expr: &Expr,
    grouped_field_names: &[&str],
) -> Result<(), SqlLoweringError> {
    if let Some(field) = first_unknown_field_in_expr(expr, model) {
        return Err(SqlLoweringError::unknown_field(field));
    }
    if !expr_references_only_fields(expr, grouped_field_names) {
        return Err(SqlLoweringError::unsupported_select_group_by());
    }

    Ok(())
}

// Preserve the older grouped `ProjectionSelection::All` fast path only for
// the canonical identity shape where projected grouped fields match `GROUP BY`
// exactly and aggregate terminals follow in declaration order.
fn grouped_projection_is_canonical_identity(
    fields: &[ProjectionField],
    group_by: &[String],
) -> bool {
    if fields.len() < group_by.len() {
        return false;
    }

    let Some((group_fields, aggregate_fields)) = fields.split_at_checked(group_by.len()) else {
        return false;
    };

    group_fields
        .iter()
        .zip(group_by.iter())
        .all(|(field, group_by)| {
            matches!(
                field,
                ProjectionField::Scalar {
                    expr: Expr::Field(field_id),
                    alias: None,
                } if field_id.as_str() == group_by
            )
        })
        && aggregate_fields.iter().all(|field| {
            matches!(
                field,
                ProjectionField::Scalar {
                    expr: Expr::Aggregate(_),
                    alias: None,
                }
            )
        })
}

// Return the first unknown field referenced anywhere inside one projection
// expression so grouped SQL lowering can keep field-resolution errors specific.
fn first_unknown_field_in_expr(expr: &Expr, model: &EntityModel) -> Option<String> {
    match expr {
        Expr::Field(field) => (resolve_field_slot(model, field.as_str()).is_none())
            .then(|| field.as_str().to_string()),
        Expr::Literal(_) | Expr::Aggregate(_) => None,
        Expr::FunctionCall { args, .. } => args
            .iter()
            .find_map(|arg| first_unknown_field_in_expr(arg, model)),
        Expr::Binary { left, right, .. } => first_unknown_field_in_expr(left, model)
            .or_else(|| first_unknown_field_in_expr(right, model)),
        #[cfg(test)]
        Expr::Unary { expr, .. } | Expr::Alias { expr, .. } => {
            first_unknown_field_in_expr(expr, model)
        }
    }
}

// Keep grouped non-aggregate projection widening narrow: grouped key-side
// expressions may depend on grouped fields, but they may not carry aggregate
// leaves because aggregate projection remains explicit in the grouped runtime
// handoff.
fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate(_) => true,
        Expr::Field(_) | Expr::Literal(_) => false,
        Expr::FunctionCall { args, .. } => args.iter().any(expr_contains_aggregate),
        Expr::Binary { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        #[cfg(test)]
        Expr::Unary { expr, .. } | Expr::Alias { expr, .. } => expr_contains_aggregate(expr),
    }
}

pub(super) fn direct_scalar_field_selection(
    items: &[SqlSelectItem],
    projection_aliases: &[Option<String>],
) -> Option<Vec<FieldId>> {
    if !projection_aliases.iter().all(Option::is_none) {
        return None;
    }

    items
        .iter()
        .map(|item| match item {
            SqlSelectItem::Field(field) => Some(FieldId::new(field.clone())),
            SqlSelectItem::Aggregate(_)
            | SqlSelectItem::TextFunction(_)
            | SqlSelectItem::Arithmetic(_)
            | SqlSelectItem::Round(_) => None,
        })
        .collect()
}

fn lower_projection_field(
    item: SqlSelectItem,
    alias: Option<&str>,
) -> Result<ProjectionField, SqlLoweringError> {
    Ok(ProjectionField::Scalar {
        expr: lower_select_item_expr(&item)?,
        alias: alias.map(Alias::new),
    })
}

pub(in crate::db::sql::lowering) fn lower_select_item_expr(
    item: &SqlSelectItem,
) -> Result<Expr, SqlLoweringError> {
    match item {
        SqlSelectItem::Field(field) => Ok(Expr::Field(FieldId::new(field.clone()))),
        SqlSelectItem::Aggregate(aggregate) => {
            Ok(Expr::Aggregate(lower_aggregate_call(aggregate.clone())?))
        }
        SqlSelectItem::TextFunction(call) => lower_text_function_expr(call),
        SqlSelectItem::Arithmetic(call) => lower_arithmetic_projection_expr(call),
        SqlSelectItem::Round(call) => lower_round_projection_expr(call),
    }
}

fn lower_text_function_expr(call: &SqlTextFunctionCall) -> Result<Expr, SqlLoweringError> {
    text_function_spec(call.function).lower_expr(call)
}

fn lower_arithmetic_projection_expr(
    call: &SqlArithmeticProjectionCall,
) -> Result<Expr, SqlLoweringError> {
    // Keep arithmetic projection bounded to one admitted binary expression
    // while still preserving the old field-plus-literal fast path for scalar
    // projection lowering.
    if let (SqlProjectionOperand::Field(field), SqlProjectionOperand::Literal(literal)) =
        (&call.left, &call.right)
    {
        return match call.op {
            SqlArithmeticProjectionOp::Add => {
                NumericProjectionExpr::add_value(field.clone(), literal.clone())
                    .map(|projection| projection.expr().clone())
                    .map_err(SqlLoweringError::from)
            }
            SqlArithmeticProjectionOp::Sub => {
                NumericProjectionExpr::sub_value(field.clone(), literal.clone())
                    .map(|projection| projection.expr().clone())
                    .map_err(SqlLoweringError::from)
            }
            SqlArithmeticProjectionOp::Mul => {
                NumericProjectionExpr::mul_value(field.clone(), literal.clone())
                    .map(|projection| projection.expr().clone())
                    .map_err(SqlLoweringError::from)
            }
            SqlArithmeticProjectionOp::Div => {
                NumericProjectionExpr::div_value(field.clone(), literal.clone())
                    .map(|projection| projection.expr().clone())
                    .map_err(SqlLoweringError::from)
            }
        };
    }

    let left = lower_projection_operand_expr(&call.left)?;
    let right = lower_projection_operand_expr(&call.right)?;

    match call.op {
        SqlArithmeticProjectionOp::Add => Ok(Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Add,
            left: Box::new(left),
            right: Box::new(right),
        }),
        SqlArithmeticProjectionOp::Sub => Ok(Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Sub,
            left: Box::new(left),
            right: Box::new(right),
        }),
        SqlArithmeticProjectionOp::Mul => Ok(Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Mul,
            left: Box::new(left),
            right: Box::new(right),
        }),
        SqlArithmeticProjectionOp::Div => Ok(Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Div,
            left: Box::new(left),
            right: Box::new(right),
        }),
    }
}

fn lower_round_projection_expr(call: &SqlRoundProjectionCall) -> Result<Expr, SqlLoweringError> {
    let scale = validate_round_projection_scale(call.scale.clone())?;

    match &call.input {
        SqlRoundProjectionInput::Operand(SqlProjectionOperand::Field(field)) => {
            RoundProjectionExpr::field(field.clone(), scale)
                .map(|projection| projection.expr().clone())
                .map_err(SqlLoweringError::from)
        }
        SqlRoundProjectionInput::Operand(operand) => Ok(Expr::FunctionCall {
            function: crate::db::query::plan::expr::Function::Round,
            args: vec![
                lower_projection_operand_expr(operand)?,
                Expr::Literal(Value::Uint(u64::from(scale))),
            ],
        }),
        SqlRoundProjectionInput::Arithmetic(arithmetic) => Ok(Expr::FunctionCall {
            function: crate::db::query::plan::expr::Function::Round,
            args: vec![
                lower_arithmetic_projection_expr(arithmetic)?,
                Expr::Literal(Value::Uint(u64::from(scale))),
            ],
        }),
    }
}

fn lower_projection_operand_expr(operand: &SqlProjectionOperand) -> Result<Expr, SqlLoweringError> {
    match operand {
        SqlProjectionOperand::Field(field) => Ok(Expr::Field(FieldId::new(field.clone()))),
        SqlProjectionOperand::Aggregate(aggregate) => {
            Ok(Expr::Aggregate(lower_aggregate_call(aggregate.clone())?))
        }
        SqlProjectionOperand::Literal(literal) => Ok(Expr::Literal(literal.clone())),
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
