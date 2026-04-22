//! Module: query::plan::expr::type_inference
//! Responsibility: infer deterministic planner expression type classes from schema and AST.
//! Does not own: runtime projection evaluation or expression execution behavior.
//! Boundary: returns planner-domain type information and typed plan errors.

use crate::db::query::plan::expr::ast::UnaryOp;
use crate::value::Value;
use crate::{
    db::{
        query::{
            builder::aggregate::AggregateExpr,
            plan::{
                AggregateKind, PlanError,
                expr::{
                    FieldKindCategory, FieldKindNumericClass, FieldKindScalarClass,
                    ast::{BinaryOp, CaseWhenArm, Expr, FieldId, Function},
                    classify_field_kind,
                },
                validate::ExprPlanError,
            },
        },
        schema::SchemaInfo,
    },
    model::field::FieldKind,
};

///
/// ExprType
///
/// Minimal deterministic expression type classification for planner inference.
/// This intentionally remains coarse in the bootstrap phase.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ExprType {
    Bool,
    Numeric(NumericSubtype),
    Text,
    #[cfg(test)]
    Null,
    Collection,
    Structured,
    Opaque,
    Unknown,
}

///
/// ExprCoarseTypeFamily
///
/// Coarse planner-owned expression family projection used by boundaries that
/// intentionally validate against `Bool` / `Numeric` / `Text` contracts
/// without becoming a second independent type lattice.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExprCoarseTypeFamily {
    #[cfg(test)]
    Bool,
    Numeric,
    Text,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NumericSubtype {
    Integer,
    Float,
    Decimal,
    Unknown,
}

impl ExprType {
    // Eligibility answers "can this participate in numeric-only operators?".
    // Subtype answers "which numeric family?" and may remain unresolved.
    const fn is_numeric_eligible(&self) -> bool {
        matches!(self, Self::Numeric(_))
    }

    const fn numeric_subtype(&self) -> Option<NumericSubtype> {
        match self {
            Self::Numeric(subtype) => Some(*subtype),
            _ => None,
        }
    }
}

/// Infer expression type deterministically from canonical expression shape.
pub(crate) fn infer_expr_type(expr: &Expr, schema: &SchemaInfo) -> Result<ExprType, PlanError> {
    match expr {
        Expr::Field(field) => infer_field_expr_type(field, schema),
        Expr::Literal(value) => Ok(infer_literal_type(value)),
        Expr::FunctionCall { function, args } => {
            infer_function_expr_type(*function, args.as_slice(), schema)
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => infer_case_expr_type(when_then_arms.as_slice(), else_expr.as_ref(), schema),
        Expr::Aggregate(aggregate) => infer_aggregate_expr_type(aggregate, schema),
        #[cfg(test)]
        Expr::Alias { expr, .. } => infer_expr_type(expr.as_ref(), schema),
        Expr::Unary { op, expr } => {
            let inner = infer_expr_type(expr.as_ref(), schema)?;

            match op {
                UnaryOp::Not => {
                    if !matches!(inner, ExprType::Bool) {
                        return Err(PlanError::from(ExprPlanError::invalid_unary_operand(
                            "not",
                            format!("{inner:?}"),
                        )));
                    }

                    Ok(ExprType::Bool)
                }
            }
        }
        Expr::Binary { op, left, right } => {
            infer_binary_expr_type(*op, left.as_ref(), right.as_ref(), schema)
        }
    }
}

/// Project one inferred planner expression type into one coarse boundary-local
/// family without reinterpreting the underlying typing semantics.
#[must_use]
#[cfg(test)]
pub(crate) const fn coarse_family_for_expr_type(
    expr_type: &ExprType,
) -> Option<ExprCoarseTypeFamily> {
    match expr_type {
        ExprType::Bool => Some(ExprCoarseTypeFamily::Bool),
        ExprType::Numeric(_) => Some(ExprCoarseTypeFamily::Numeric),
        ExprType::Text => Some(ExprCoarseTypeFamily::Text),
        #[cfg(test)]
        ExprType::Null => None,
        ExprType::Collection | ExprType::Structured | ExprType::Opaque | ExprType::Unknown => None,
    }
}

/// Infer one planner-owned coarse family directly from one expression subtree.
#[cfg(test)]
pub(crate) fn infer_expr_coarse_family(
    expr: &Expr,
    schema: &SchemaInfo,
) -> Result<Option<ExprCoarseTypeFamily>, PlanError> {
    let inferred = infer_expr_type(expr, schema)?;

    Ok(coarse_family_for_expr_type(&inferred))
}

/// Infer one planner-owned coarse family from the lowerable searched `CASE`
/// result branches that are already visible at a caller boundary.
#[cfg(test)]
pub(crate) fn infer_case_result_exprs_coarse_family<'a>(
    result_exprs: impl IntoIterator<Item = &'a Expr>,
    schema: &SchemaInfo,
) -> Result<Option<ExprCoarseTypeFamily>, PlanError> {
    infer_folded_exprs_coarse_family(result_exprs, schema, |current, current_expr, next, expr| {
        unify_case_branch_types((&next, expr), (&current, current_expr))
    })
}

/// Infer one planner-owned coarse family from the lowerable arguments of a
/// dynamic-result scalar function whose result family depends on shared
/// argument unification instead of a fixed signature table.
#[cfg(test)]
pub(crate) fn infer_dynamic_function_result_exprs_coarse_family(
    function: Function,
    args: &[Expr],
    schema: &SchemaInfo,
) -> Result<Option<ExprCoarseTypeFamily>, PlanError> {
    match function {
        Function::Coalesce | Function::NullIf => infer_folded_exprs_coarse_family(
            args.iter(),
            schema,
            |current, _current_expr, next, _next_expr| unify_coalesce_expr_types(current, next),
        ),
        _ => Err(PlanError::from(ExprPlanError::invalid_function_argument(
            function.sql_label(),
            args.len(),
            "function is outside the dynamic partial-inference surface".to_string(),
        ))),
    }
}

// Fold one visible expression list through planner-owned type inference and one
// caller-supplied unification rule, then project the final planner type onto a
// coarse family for boundary consumers such as prepared fallback typing.
#[cfg(test)]
fn infer_folded_exprs_coarse_family<'a, F>(
    exprs: impl IntoIterator<Item = &'a Expr>,
    schema: &SchemaInfo,
    mut fold: F,
) -> Result<Option<ExprCoarseTypeFamily>, PlanError>
where
    F: FnMut(ExprType, &'a Expr, ExprType, &'a Expr) -> Result<ExprType, PlanError>,
{
    let mut resolved: Option<(ExprType, &'a Expr)> = None;

    for expr in exprs {
        let next = infer_expr_type(expr, schema)?;
        resolved = Some(match resolved {
            None => (next, expr),
            Some((current, current_expr)) => (fold(current, current_expr, next, expr)?, expr),
        });
    }

    Ok(resolved
        .as_ref()
        .and_then(|(expr_type, _)| coarse_family_for_expr_type(expr_type)))
}

///
/// FunctionTypeInferenceShape
///
/// Shared planner-owned scalar function signature shape for this module.
/// `function_arg_coarse_family`, `function_result_coarse_family`, and
/// `infer_function_expr_type` all consume this one classification so the
/// function-family grouping stays owned by the typing seam instead of being
/// repeated across parallel match ladders.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FunctionTypeInferenceShape {
    UnaryBoolPredicate,
    CollectionContains,
    TextResult {
        text_positions: &'static [usize],
        numeric_positions: &'static [usize],
    },
    NumericResult {
        text_positions: &'static [usize],
        numeric_positions: &'static [usize],
        subtype: NumericSubtype,
    },
    BoolResult {
        text_positions: &'static [usize],
    },
    RoundNumericResult,
    DynamicCoalesce,
    DynamicNullIf,
}

impl FunctionTypeInferenceShape {
    #[cfg(test)]
    fn arg_coarse_family(self, index: usize) -> Option<ExprCoarseTypeFamily> {
        match self {
            Self::UnaryBoolPredicate
            | Self::CollectionContains
            | Self::DynamicCoalesce
            | Self::DynamicNullIf => None,
            Self::TextResult {
                text_positions,
                numeric_positions,
            }
            | Self::NumericResult {
                text_positions,
                numeric_positions,
                ..
            } => {
                if text_positions.contains(&index) {
                    Some(ExprCoarseTypeFamily::Text)
                } else if numeric_positions.contains(&index) {
                    Some(ExprCoarseTypeFamily::Numeric)
                } else {
                    None
                }
            }
            Self::BoolResult { text_positions } => {
                if text_positions.contains(&index) {
                    Some(ExprCoarseTypeFamily::Text)
                } else {
                    None
                }
            }
            Self::RoundNumericResult => {
                if matches!(index, 0 | 1) {
                    Some(ExprCoarseTypeFamily::Numeric)
                } else {
                    None
                }
            }
        }
    }

    #[cfg(test)]
    const fn result_coarse_family(self) -> Option<ExprCoarseTypeFamily> {
        match self {
            Self::UnaryBoolPredicate | Self::CollectionContains | Self::BoolResult { .. } => {
                Some(ExprCoarseTypeFamily::Bool)
            }
            Self::TextResult { .. } => Some(ExprCoarseTypeFamily::Text),
            Self::NumericResult { .. } | Self::RoundNumericResult => {
                Some(ExprCoarseTypeFamily::Numeric)
            }
            Self::DynamicCoalesce | Self::DynamicNullIf => None,
        }
    }
}

const fn function_type_inference_shape(function: Function) -> FunctionTypeInferenceShape {
    match function {
        Function::IsNull
        | Function::IsNotNull
        | Function::IsMissing
        | Function::IsEmpty
        | Function::IsNotEmpty => FunctionTypeInferenceShape::UnaryBoolPredicate,
        Function::CollectionContains => FunctionTypeInferenceShape::CollectionContains,
        Function::Trim | Function::Ltrim | Function::Rtrim | Function::Lower | Function::Upper => {
            FunctionTypeInferenceShape::TextResult {
                text_positions: &[0],
                numeric_positions: &[],
            }
        }
        Function::Left | Function::Right => FunctionTypeInferenceShape::TextResult {
            text_positions: &[0],
            numeric_positions: &[1],
        },
        Function::Replace => FunctionTypeInferenceShape::TextResult {
            text_positions: &[0, 1, 2],
            numeric_positions: &[],
        },
        Function::Substring => FunctionTypeInferenceShape::TextResult {
            text_positions: &[0],
            numeric_positions: &[1, 2],
        },
        Function::Length => FunctionTypeInferenceShape::NumericResult {
            text_positions: &[0],
            numeric_positions: &[],
            subtype: NumericSubtype::Integer,
        },
        Function::Position => FunctionTypeInferenceShape::NumericResult {
            text_positions: &[0, 1],
            numeric_positions: &[],
            subtype: NumericSubtype::Integer,
        },
        Function::Abs | Function::Ceil | Function::Ceiling | Function::Floor => {
            FunctionTypeInferenceShape::NumericResult {
                text_positions: &[],
                numeric_positions: &[0],
                subtype: NumericSubtype::Decimal,
            }
        }
        Function::StartsWith | Function::EndsWith | Function::Contains => {
            FunctionTypeInferenceShape::BoolResult {
                text_positions: &[0, 1],
            }
        }
        Function::Round => FunctionTypeInferenceShape::RoundNumericResult,
        Function::Coalesce => FunctionTypeInferenceShape::DynamicCoalesce,
        Function::NullIf => FunctionTypeInferenceShape::DynamicNullIf,
    }
}

/// Return the shared expected coarse family for one fixed-arity scalar
/// function argument when planner typing defines that contract explicitly.
#[must_use]
#[cfg(test)]
pub(crate) fn function_arg_coarse_family(
    function: Function,
    index: usize,
) -> Option<ExprCoarseTypeFamily> {
    function_type_inference_shape(function).arg_coarse_family(index)
}

/// Return the shared coarse result family for one scalar function when planner
/// typing fixes that family independently of argument-specific unification.
#[must_use]
#[cfg(test)]
pub(crate) const fn function_result_coarse_family(
    function: Function,
) -> Option<ExprCoarseTypeFamily> {
    function_type_inference_shape(function).result_coarse_family()
}

/// Report whether planner typing classifies one scalar function as part of the
/// text/numeric compare-operand family consumed by canonicalization.
#[must_use]
pub(crate) const fn function_is_compare_operand_coarse_family(function: Function) -> bool {
    matches!(
        function_type_inference_shape(function),
        FunctionTypeInferenceShape::TextResult { .. }
            | FunctionTypeInferenceShape::NumericResult { .. }
            | FunctionTypeInferenceShape::RoundNumericResult
            | FunctionTypeInferenceShape::DynamicCoalesce
            | FunctionTypeInferenceShape::DynamicNullIf
    )
}

/// Return the shared argument family for dynamic-result scalar functions once
/// planner typing has already resolved their result family.
#[must_use]
#[cfg(test)]
pub(crate) const fn dynamic_function_arg_coarse_family(
    function: Function,
    result_family: ExprCoarseTypeFamily,
) -> Option<ExprCoarseTypeFamily> {
    match function {
        Function::Coalesce | Function::NullIf => Some(result_family),
        _ => None,
    }
}

fn infer_function_expr_type(
    function: Function,
    args: &[Expr],
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let arg_types = args
        .iter()
        .map(|arg| infer_expr_type(arg, schema))
        .collect::<Result<Vec<_>, _>>()?;

    match function_type_inference_shape(function) {
        FunctionTypeInferenceShape::UnaryBoolPredicate => {
            validate_exact_function_arg_count(function, args.len(), 1)?;

            Ok(ExprType::Bool)
        }
        FunctionTypeInferenceShape::CollectionContains => {
            validate_exact_function_arg_count(function, args.len(), 2)?;

            Ok(ExprType::Bool)
        }
        FunctionTypeInferenceShape::TextResult {
            text_positions,
            numeric_positions,
        } => {
            validate_function_arg_families(
                function,
                arg_types.as_slice(),
                text_positions,
                numeric_positions,
            )?;

            Ok(ExprType::Text)
        }
        FunctionTypeInferenceShape::NumericResult {
            text_positions,
            numeric_positions,
            subtype,
        } => {
            validate_function_arg_families(
                function,
                arg_types.as_slice(),
                text_positions,
                numeric_positions,
            )?;

            Ok(ExprType::Numeric(subtype))
        }
        FunctionTypeInferenceShape::BoolResult { text_positions } => {
            validate_function_arg_families(function, arg_types.as_slice(), text_positions, &[])?;

            Ok(ExprType::Bool)
        }
        FunctionTypeInferenceShape::RoundNumericResult => {
            validate_numeric_round_function_args(arg_types.as_slice())?;

            Ok(ExprType::Numeric(NumericSubtype::Decimal))
        }
        FunctionTypeInferenceShape::DynamicCoalesce => {
            infer_coalesce_function_type(arg_types.as_slice())
        }
        FunctionTypeInferenceShape::DynamicNullIf => {
            infer_nullif_function_type(arg_types.as_slice())
        }
    }
}

fn validate_exact_function_arg_count(
    function: Function,
    actual: usize,
    expected: usize,
) -> Result<(), PlanError> {
    if actual != expected {
        return Err(PlanError::from(ExprPlanError::invalid_function_argument(
            function.sql_label(),
            actual,
            format!("expected exactly {expected} args, found {actual}"),
        )));
    }

    Ok(())
}

const fn expr_type_accepts_required_coarse_family(
    expr_type: &ExprType,
    family: ExprCoarseTypeFamily,
) -> bool {
    (match family {
        #[cfg(test)]
        ExprCoarseTypeFamily::Bool => matches!(expr_type, ExprType::Bool),
        ExprCoarseTypeFamily::Numeric => matches!(expr_type, ExprType::Numeric(_)),
        ExprCoarseTypeFamily::Text => matches!(expr_type, ExprType::Text),
    }) || {
        #[cfg(test)]
        {
            matches!(expr_type, ExprType::Null)
        }
        #[cfg(not(test))]
        {
            false
        }
    }
}

fn validate_function_arg_families(
    function: Function,
    args: &[ExprType],
    text_positions: &[usize],
    numeric_positions: &[usize],
) -> Result<(), PlanError> {
    for (index, arg) in args.iter().enumerate() {
        if text_positions.contains(&index)
            && !expr_type_accepts_required_coarse_family(arg, ExprCoarseTypeFamily::Text)
        {
            return Err(PlanError::from(ExprPlanError::invalid_function_argument(
                function.sql_label(),
                index,
                format!("{arg:?}"),
            )));
        }
        if numeric_positions.contains(&index)
            && !expr_type_accepts_required_coarse_family(arg, ExprCoarseTypeFamily::Numeric)
        {
            return Err(PlanError::from(ExprPlanError::invalid_function_argument(
                function.sql_label(),
                index,
                format!("{arg:?}"),
            )));
        }
    }

    Ok(())
}

fn validate_numeric_round_function_args(args: &[ExprType]) -> Result<(), PlanError> {
    if args.len() != 2 {
        return Err(PlanError::from(ExprPlanError::invalid_function_argument(
            "ROUND",
            args.len(),
            format!("expected exactly 2 args, found {}", args.len()),
        )));
    }

    if !matches!(args[0], ExprType::Numeric(_)) {
        return Err(PlanError::from(ExprPlanError::invalid_function_argument(
            "ROUND",
            0,
            format!("{:?}", args[0]),
        )));
    }

    let scale_compatible = matches!(args[1], ExprType::Numeric(NumericSubtype::Integer)) || {
        #[cfg(test)]
        {
            matches!(args[1], ExprType::Null)
        }
        #[cfg(not(test))]
        {
            false
        }
    };

    if !scale_compatible {
        return Err(PlanError::from(ExprPlanError::invalid_function_argument(
            "ROUND",
            1,
            format!("{:?}", args[1]),
        )));
    }

    Ok(())
}

fn infer_coalesce_function_type(args: &[ExprType]) -> Result<ExprType, PlanError> {
    if args.len() < 2 {
        return Err(PlanError::from(ExprPlanError::invalid_function_argument(
            "COALESCE",
            args.len(),
            format!("expected at least 2 args, found {}", args.len()),
        )));
    }

    let mut common = None;
    for arg in args {
        #[cfg(test)]
        if matches!(arg, ExprType::Null) {
            continue;
        }

        common = Some(match common {
            None => arg.clone(),
            Some(current) => unify_coalesce_expr_types(current, arg.clone())?,
        });
    }

    Ok(common.unwrap_or(ExprType::Unknown))
}

fn infer_nullif_function_type(args: &[ExprType]) -> Result<ExprType, PlanError> {
    if args.len() != 2 {
        return Err(PlanError::from(ExprPlanError::invalid_function_argument(
            "NULLIF",
            args.len(),
            format!("expected exactly 2 args, found {}", args.len()),
        )));
    }

    #[cfg(test)]
    if matches!(args[0], ExprType::Null) || matches!(args[1], ExprType::Null) {
        return Ok(args[0].clone());
    }

    let _ = unify_coalesce_expr_types(args[0].clone(), args[1].clone())?;

    Ok(args[0].clone())
}

fn unify_coalesce_expr_types(current: ExprType, next: ExprType) -> Result<ExprType, PlanError> {
    match (current, next) {
        (ExprType::Numeric(left), ExprType::Numeric(right)) => {
            Ok(ExprType::Numeric(unify_numeric_subtypes(left, right)))
        }
        (ExprType::Text, ExprType::Text) => Ok(ExprType::Text),
        (ExprType::Bool, ExprType::Bool) => Ok(ExprType::Bool),
        (ExprType::Collection, ExprType::Collection) => Ok(ExprType::Collection),
        (ExprType::Structured, ExprType::Structured) => Ok(ExprType::Structured),
        (ExprType::Opaque, ExprType::Opaque) => Ok(ExprType::Opaque),
        (ExprType::Unknown, other) | (other, ExprType::Unknown) => Ok(other),
        #[cfg(test)]
        (ExprType::Null, other) | (other, ExprType::Null) => Ok(other),
        (left, right) => Err(PlanError::from(ExprPlanError::invalid_function_argument(
            "COALESCE",
            0,
            format!("incompatible argument types {left:?} and {right:?}"),
        ))),
    }
}

const fn unify_numeric_subtypes(left: NumericSubtype, right: NumericSubtype) -> NumericSubtype {
    match (left, right) {
        (NumericSubtype::Decimal, _) | (_, NumericSubtype::Decimal) => NumericSubtype::Decimal,
        (NumericSubtype::Float, _) | (_, NumericSubtype::Float) => NumericSubtype::Float,
        (NumericSubtype::Unknown, other) | (other, NumericSubtype::Unknown) => other,
        (NumericSubtype::Integer, NumericSubtype::Integer) => NumericSubtype::Integer,
    }
}

fn resolve_expr_field_kind<'a>(
    field_name: &str,
    schema: &'a SchemaInfo,
) -> Result<&'a FieldKind, PlanError> {
    schema
        .field_kind(field_name)
        .ok_or_else(|| PlanError::from(ExprPlanError::unknown_expr_field(field_name)))
}

fn infer_field_expr_type(field: &FieldId, schema: &SchemaInfo) -> Result<ExprType, PlanError> {
    let field_name = field.as_str();
    let field_kind = resolve_expr_field_kind(field_name, schema)?;

    Ok(expr_type_from_field_kind(field_kind))
}

fn infer_aggregate_expr_type(
    aggregate: &AggregateExpr,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let kind = aggregate.kind();
    let input_expr = aggregate.input_expr();

    match kind {
        AggregateKind::Count => Ok(ExprType::Numeric(NumericSubtype::Integer)),
        AggregateKind::Exists => Ok(ExprType::Bool),
        AggregateKind::Sum => infer_sum_aggregate_type(input_expr, schema, "sum"),
        AggregateKind::Avg => infer_sum_aggregate_type(input_expr, schema, "avg"),
        AggregateKind::Min | AggregateKind::Max | AggregateKind::First | AggregateKind::Last => {
            infer_target_field_aggregate_type(input_expr, schema)
        }
    }
}

fn infer_case_expr_type(
    when_then_arms: &[CaseWhenArm],
    else_expr: &Expr,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let mut result_type = infer_expr_type(else_expr, schema)?;

    for arm in when_then_arms {
        let condition_type = infer_expr_type(arm.condition(), schema)?;
        if !matches!(condition_type, ExprType::Bool) {
            return Err(PlanError::from(ExprPlanError::invalid_case_condition_type(
                format!("{condition_type:?}"),
            )));
        }

        let branch_type = infer_expr_type(arm.result(), schema)?;
        result_type =
            unify_case_branch_types((&branch_type, arm.result()), (&result_type, else_expr))?;
    }

    Ok(result_type)
}

fn infer_sum_aggregate_type(
    input_expr: Option<&Expr>,
    schema: &SchemaInfo,
    aggregate_name: &str,
) -> Result<ExprType, PlanError> {
    let Some(input_expr) = input_expr else {
        return Err(PlanError::from(ExprPlanError::aggregate_target_required(
            aggregate_name,
        )));
    };

    let inferred = infer_expr_type(input_expr, schema)?;

    match input_expr {
        Expr::Field(field) => {
            let field_kind = resolve_expr_field_kind(field.as_str(), schema)?;
            if !classify_field_kind(field_kind).supports_expr_numeric() {
                return Err(PlanError::from(
                    ExprPlanError::non_numeric_aggregate_target(aggregate_name, field.as_str()),
                ));
            }
        }
        _ if !matches!(inferred, ExprType::Numeric(_)) => {
            return Err(PlanError::from(
                ExprPlanError::non_numeric_aggregate_target(
                    aggregate_name,
                    render_aggregate_input_expr_label(input_expr).as_str(),
                ),
            ));
        }
        _ => {}
    }

    Ok(inferred)
}

fn infer_target_field_aggregate_type(
    input_expr: Option<&Expr>,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let Some(input_expr) = input_expr else {
        // Bootstrap behavior: target-less extrema/value terminals stay unresolved.
        return Ok(ExprType::Unknown);
    };

    infer_expr_type(input_expr, schema)
}

fn render_aggregate_input_expr_label(expr: &Expr) -> String {
    match expr {
        Expr::Field(field) => field.as_str().to_string(),
        Expr::Literal(value) => format!("{value:?}"),
        Expr::FunctionCall { function, args } => {
            let rendered_args = args
                .iter()
                .map(render_aggregate_input_expr_label)
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}({rendered_args})", function.sql_label())
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            let mut rendered = String::from("CASE");
            for arm in when_then_arms {
                rendered.push_str(" WHEN ");
                rendered.push_str(render_aggregate_input_expr_label(arm.condition()).as_str());
                rendered.push_str(" THEN ");
                rendered.push_str(render_aggregate_input_expr_label(arm.result()).as_str());
            }
            rendered.push_str(" ELSE ");
            rendered.push_str(render_aggregate_input_expr_label(else_expr).as_str());
            rendered.push_str(" END");
            rendered
        }
        Expr::Binary { op, left, right } => {
            let left = render_aggregate_input_expr_label(left);
            let right = render_aggregate_input_expr_label(right);
            let op = match op {
                BinaryOp::Or => "OR",
                BinaryOp::And => "AND",
                BinaryOp::Eq => "=",
                BinaryOp::Ne => "!=",
                BinaryOp::Lt => "<",
                BinaryOp::Lte => "<=",
                BinaryOp::Gt => ">",
                BinaryOp::Gte => ">=",
                BinaryOp::Add => "+",
                BinaryOp::Sub => "-",
                BinaryOp::Mul => "*",
                BinaryOp::Div => "/",
            };

            format!("{left} {op} {right}")
        }
        Expr::Aggregate(_) => "aggregate".to_string(),
        #[cfg(test)]
        Expr::Alias { expr, .. } => render_aggregate_input_expr_label(expr),
        Expr::Unary { expr, .. } => render_aggregate_input_expr_label(expr),
    }
}

fn infer_binary_expr_type(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let left_ty = infer_expr_type(left, schema)?;
    let right_ty = infer_expr_type(right, schema)?;

    match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
            if !binary_numeric_compatible(&left_ty, &right_ty) {
                return Err(invalid_binary_operands(op, &left_ty, &right_ty));
            }

            Ok(ExprType::Numeric(infer_numeric_result_subtype(
                op, &left_ty, &right_ty,
            )))
        }
        BinaryOp::Or | BinaryOp::And => {
            if !matches!(left_ty, ExprType::Bool) || !matches!(right_ty, ExprType::Bool) {
                return Err(invalid_binary_operands(op, &left_ty, &right_ty));
            }

            Ok(ExprType::Bool)
        }
        BinaryOp::Eq | BinaryOp::Ne => {
            if !binary_equality_comparable(&left_ty, &right_ty) {
                return Err(invalid_binary_operands(op, &left_ty, &right_ty));
            }

            Ok(ExprType::Bool)
        }
        BinaryOp::Lt | BinaryOp::Lte | BinaryOp::Gt | BinaryOp::Gte => {
            if !binary_order_comparable(&left_ty, &right_ty) {
                return Err(invalid_binary_operands(op, &left_ty, &right_ty));
            }

            Ok(ExprType::Bool)
        }
    }
}

// Binary type inference keeps one shared planner-facing operand mismatch error
// so arithmetic, boolean, and equality lanes cannot drift in diagnostics.
fn invalid_binary_operands(op: BinaryOp, left: &ExprType, right: &ExprType) -> PlanError {
    PlanError::from(ExprPlanError::invalid_binary_operands(
        binary_op_name(op),
        format!("{left:?}"),
        format!("{right:?}"),
    ))
}

const fn binary_numeric_compatible(left: &ExprType, right: &ExprType) -> bool {
    left.is_numeric_eligible() && right.is_numeric_eligible()
}

const fn binary_equality_comparable(left: &ExprType, right: &ExprType) -> bool {
    if left.is_numeric_eligible() && right.is_numeric_eligible() {
        return true;
    }

    #[cfg(test)]
    if matches!((left, right), (ExprType::Null, ExprType::Null)) {
        return true;
    }

    matches!(
        (left, right),
        (ExprType::Bool, ExprType::Bool)
            | (ExprType::Text, ExprType::Text)
            | (ExprType::Collection, ExprType::Collection)
            | (ExprType::Structured, ExprType::Structured)
            | (ExprType::Opaque, ExprType::Opaque)
    )
}

const fn binary_order_comparable(left: &ExprType, right: &ExprType) -> bool {
    if left.is_numeric_eligible() && right.is_numeric_eligible() {
        return true;
    }

    matches!((left, right), (ExprType::Text, ExprType::Text))
}

fn unify_case_branch_types(
    left: (&ExprType, &Expr),
    right: (&ExprType, &Expr),
) -> Result<ExprType, PlanError> {
    let (left_type, left_expr) = left;
    let (right_type, right_expr) = right;

    if left_type == right_type {
        return Ok(left_type.clone());
    }

    if case_branch_is_null_only(left_type, left_expr) {
        return Ok(right_type.clone());
    }
    if case_branch_is_null_only(right_type, right_expr) {
        return Ok(left_type.clone());
    }

    if left_type.is_numeric_eligible() && right_type.is_numeric_eligible() {
        return Ok(ExprType::Numeric(infer_numeric_result_subtype(
            BinaryOp::Add,
            left_type,
            right_type,
        )));
    }

    Err(PlanError::from(
        ExprPlanError::incompatible_case_branch_types(
            format!("{left_type:?}"),
            format!("{right_type:?}"),
        ),
    ))
}

#[cfg(test)]
const fn case_branch_is_null_only(branch_type: &ExprType, expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(Value::Null)) || matches!(branch_type, ExprType::Null)
}

#[cfg(not(test))]
const fn case_branch_is_null_only(_branch_type: &ExprType, expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(Value::Null))
}

const fn infer_numeric_result_subtype(
    op: BinaryOp,
    left: &ExprType,
    right: &ExprType,
) -> NumericSubtype {
    if matches!(op, BinaryOp::Div) {
        return NumericSubtype::Decimal;
    }

    let left_subtype = left.numeric_subtype();
    let right_subtype = right.numeric_subtype();
    let (Some(left_subtype), Some(right_subtype)) = (left_subtype, right_subtype) else {
        return if let Some(left_subtype) = left_subtype {
            left_subtype
        } else if let Some(right_subtype) = right_subtype {
            right_subtype
        } else {
            NumericSubtype::Integer
        };
    };

    match (left_subtype, right_subtype) {
        (NumericSubtype::Integer, NumericSubtype::Integer) => NumericSubtype::Integer,
        (NumericSubtype::Float, NumericSubtype::Float) => NumericSubtype::Float,
        (NumericSubtype::Decimal, NumericSubtype::Decimal) => NumericSubtype::Decimal,
        _ => NumericSubtype::Unknown,
    }
}

const fn infer_literal_type(value: &Value) -> ExprType {
    match value {
        Value::Bool(_) => ExprType::Bool,
        Value::Text(_) | Value::Enum(_) => ExprType::Text,
        Value::Int(_)
        | Value::Int128(_)
        | Value::IntBig(_)
        | Value::Uint(_)
        | Value::Uint128(_)
        | Value::UintBig(_)
        | Value::Duration(_)
        | Value::Timestamp(_) => ExprType::Numeric(NumericSubtype::Integer),
        Value::Float32(_) | Value::Float64(_) => ExprType::Numeric(NumericSubtype::Float),
        Value::Decimal(_) => ExprType::Numeric(NumericSubtype::Decimal),
        Value::List(_) | Value::Map(_) => ExprType::Collection,
        Value::Null => {
            #[cfg(test)]
            {
                ExprType::Null
            }
            #[cfg(not(test))]
            {
                ExprType::Unknown
            }
        }
        Value::Account(_)
        | Value::Blob(_)
        | Value::Date(_)
        | Value::Principal(_)
        | Value::Subaccount(_)
        | Value::Ulid(_)
        | Value::Unit => ExprType::Opaque,
    }
}

fn expr_type_from_field_kind(kind: &FieldKind) -> ExprType {
    match classify_field_kind(kind).category() {
        FieldKindCategory::Scalar(FieldKindScalarClass::Boolean)
        | FieldKindCategory::Relation(FieldKindScalarClass::Boolean) => ExprType::Bool,
        FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
            FieldKindNumericClass::Signed64
            | FieldKindNumericClass::Unsigned64
            | FieldKindNumericClass::SignedWide
            | FieldKindNumericClass::UnsignedWide
            | FieldKindNumericClass::DurationLike
            | FieldKindNumericClass::TimestampLike,
        ))
        | FieldKindCategory::Relation(FieldKindScalarClass::Numeric(
            FieldKindNumericClass::Signed64
            | FieldKindNumericClass::Unsigned64
            | FieldKindNumericClass::SignedWide
            | FieldKindNumericClass::UnsignedWide
            | FieldKindNumericClass::DurationLike
            | FieldKindNumericClass::TimestampLike,
        )) => ExprType::Numeric(NumericSubtype::Integer),
        FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
            FieldKindNumericClass::FloatLike,
        ))
        | FieldKindCategory::Relation(FieldKindScalarClass::Numeric(
            FieldKindNumericClass::FloatLike,
        )) => ExprType::Numeric(NumericSubtype::Float),
        FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
            FieldKindNumericClass::DecimalLike,
        ))
        | FieldKindCategory::Relation(FieldKindScalarClass::Numeric(
            FieldKindNumericClass::DecimalLike,
        )) => ExprType::Numeric(NumericSubtype::Decimal),
        FieldKindCategory::Scalar(FieldKindScalarClass::Text)
        | FieldKindCategory::Relation(FieldKindScalarClass::Text) => ExprType::Text,
        FieldKindCategory::Collection => ExprType::Collection,
        FieldKindCategory::Structured { .. } => ExprType::Structured,
        FieldKindCategory::Scalar(
            FieldKindScalarClass::OrderedOpaque | FieldKindScalarClass::Opaque,
        )
        | FieldKindCategory::Relation(
            FieldKindScalarClass::OrderedOpaque | FieldKindScalarClass::Opaque,
        ) => ExprType::Opaque,
    }
}

const fn binary_op_name(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Or => "or",
        BinaryOp::And => "and",
        BinaryOp::Eq => "eq",
        BinaryOp::Ne => "ne",
        BinaryOp::Lt => "lt",
        BinaryOp::Lte => "lte",
        BinaryOp::Gt => "gt",
        BinaryOp::Gte => "gte",
        BinaryOp::Add => "add",
        BinaryOp::Sub => "sub",
        BinaryOp::Mul => "mul",
        BinaryOp::Div => "div",
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
