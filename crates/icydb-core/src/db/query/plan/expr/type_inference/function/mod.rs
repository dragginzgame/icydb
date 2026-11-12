use crate::db::{
    QueryError,
    query::plan::{
        PlanError,
        expr::{
            Expr, Function, FunctionTypeInferenceShape, NumericSubtype,
            type_inference::{
                ExprType, FunctionArgumentFamily, infer_expr_type, unify::unify_coalesce_expr_types,
            },
        },
        validate::ExprPlanError,
    },
    query::preparation::PreparationWork,
    schema::SchemaInfo,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

impl FunctionTypeInferenceShape {
    fn argument_family(self, index: usize) -> Option<FunctionArgumentFamily> {
        match self {
            Self::ByteLengthResult
            | Self::UnaryBoolPredicate
            | Self::CollectionContains
            | Self::DynamicCoalesce
            | Self::DynamicNullIf
            | Self::Membership => None,
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
                    Some(FunctionArgumentFamily::Text)
                } else if numeric_positions.contains(&index) {
                    Some(FunctionArgumentFamily::Numeric)
                } else {
                    None
                }
            }
            Self::BoolResult { text_positions } => {
                if text_positions.contains(&index) {
                    Some(FunctionArgumentFamily::Text)
                } else {
                    None
                }
            }
            Self::NumericScaleResult => {
                matches!(index, 0 | 1).then_some(FunctionArgumentFamily::Numeric)
            }
        }
    }

    fn infer_function_result_type(
        self,
        function: Function,
        args: &[ExprType],
    ) -> Result<ExprType, PlanError> {
        match self {
            Self::ByteLengthResult => {
                validate_byte_length_function_args(function, args)?;

                Ok(ExprType::Numeric(NumericSubtype::Integer))
            }
            Self::UnaryBoolPredicate => {
                validate_exact_function_arg_count(function, args.len(), 1)?;

                Ok(ExprType::Bool)
            }
            Self::CollectionContains => {
                validate_exact_function_arg_count(function, args.len(), 2)?;

                Ok(ExprType::Bool)
            }
            Self::Membership => {
                validate_membership_function_args(function, args)?;

                Ok(ExprType::Bool)
            }
            Self::TextResult { .. } => {
                validate_function_arg_families(function, args, self)?;

                Ok(ExprType::Text)
            }
            Self::NumericResult { subtype, .. } => {
                if matches!(function, Function::Mod)
                    && matches!(args, [ExprType::U256, ExprType::U256])
                {
                    return Ok(ExprType::U256);
                }
                validate_function_arg_families(function, args, self)?;

                Ok(ExprType::Numeric(subtype))
            }
            Self::BoolResult { .. } => {
                validate_function_arg_families(function, args, self)?;

                Ok(ExprType::Bool)
            }
            Self::NumericScaleResult => {
                validate_numeric_scale_function_args(function, args)?;

                Ok(ExprType::Numeric(NumericSubtype::Decimal))
            }
            Self::DynamicCoalesce => infer_coalesce_function_type(function, args),
            Self::DynamicNullIf => infer_nullif_function_type(function, args),
        }
    }
}

fn validate_membership_function_args(
    function: Function,
    args: &[ExprType],
) -> Result<(), PlanError> {
    validate_exact_function_arg_count(function, args.len(), 2)?;

    if !matches!(args[1], ExprType::Collection) {
        return Err(invalid_function_argument(function, 1, &args[1]));
    }

    Ok(())
}

/// Report whether planner typing classifies one scalar function as part of the
/// text/numeric compare-operand family consumed by canonicalization.
#[must_use]
pub(in crate::db::query::plan::expr) const fn function_is_compare_operand_coarse_family(
    function: Function,
) -> bool {
    function.is_compare_operand_coarse_family()
}

pub(super) fn infer_function_expr_type(
    function: Function,
    args: &[Expr],
    schema: &SchemaInfo,
    work: &PreparationWork<'_>,
) -> Result<ExprType, QueryError> {
    // Preserve child inference before signature validation, including invalid
    // arity. The shared buffer helper admits its backing before construction.
    let arg_types = work.copy_slice(args, |arg| infer_expr_type(arg, schema, work))?;
    work.charge(Resource::PredicateExpressionSteps, arg_types.len() as u64)?;

    function
        .type_inference_shape()
        .infer_function_result_type(function, arg_types.as_slice())
        .map_err(QueryError::from)
}

fn validate_exact_function_arg_count(
    function: Function,
    actual: usize,
    expected: usize,
) -> Result<(), PlanError> {
    if actual != expected {
        return Err(PlanError::from(ExprPlanError::invalid_function_arity(
            function, expected, actual,
        )));
    }

    Ok(())
}

fn validate_byte_length_function_args(
    function: Function,
    args: &[ExprType],
) -> Result<(), PlanError> {
    validate_exact_function_arg_count(function, args.len(), 1)?;

    let input_compatible = matches!(args[0], ExprType::Text | ExprType::Blob | ExprType::Null);

    if !input_compatible {
        return Err(invalid_function_argument(function, 0, &args[0]));
    }

    Ok(())
}

const fn expr_type_accepts_required_family(
    expr_type: &ExprType,
    family: FunctionArgumentFamily,
) -> bool {
    (match family {
        FunctionArgumentFamily::Numeric => matches!(expr_type, ExprType::Numeric(_)),
        FunctionArgumentFamily::Text => matches!(expr_type, ExprType::Text),
    }) || matches!(expr_type, ExprType::Null)
}

fn validate_function_arg_families(
    function: Function,
    args: &[ExprType],
    shape: FunctionTypeInferenceShape,
) -> Result<(), PlanError> {
    for (index, arg) in args.iter().enumerate() {
        let Some(family) = shape.argument_family(index) else {
            continue;
        };

        if !expr_type_accepts_required_family(arg, family) {
            return Err(invalid_function_argument(function, index, arg));
        }
    }

    Ok(())
}

fn validate_numeric_scale_function_args(
    function: Function,
    args: &[ExprType],
) -> Result<(), PlanError> {
    if args.len() != 2 {
        return Err(PlanError::from(ExprPlanError::invalid_function_arity(
            function,
            2,
            args.len(),
        )));
    }

    if !matches!(args[0], ExprType::Numeric(_) | ExprType::Null) {
        return Err(invalid_function_argument(function, 0, &args[0]));
    }

    let scale_compatible = matches!(
        args[1],
        ExprType::Numeric(NumericSubtype::Integer) | ExprType::Null
    );

    if !scale_compatible {
        return Err(invalid_function_argument(function, 1, &args[1]));
    }

    Ok(())
}

fn infer_coalesce_function_type(
    function: Function,
    args: &[ExprType],
) -> Result<ExprType, PlanError> {
    if args.len() < 2 {
        return Err(PlanError::from(ExprPlanError::invalid_function_arity(
            function,
            2,
            args.len(),
        )));
    }

    let mut common = None;
    for (index, arg) in args.iter().enumerate() {
        if matches!(arg, ExprType::Null) {
            continue;
        }

        common = Some(match common {
            None => (index, arg.clone()),
            Some((current_index, current)) => (
                current_index,
                unify_coalesce_expr_types(current, arg.clone(), |left, right| {
                    PlanError::from(ExprPlanError::incompatible_function_arguments(
                        function,
                        current_index,
                        index,
                        &left,
                        &right,
                    ))
                })?,
            ),
        });
    }

    // Only an all-NULL input vector leaves no common type; it is not unresolved.
    Ok(common.map_or(ExprType::Null, |(_, expr_type)| expr_type))
}

fn infer_nullif_function_type(
    function: Function,
    args: &[ExprType],
) -> Result<ExprType, PlanError> {
    if args.len() != 2 {
        return Err(PlanError::from(ExprPlanError::invalid_function_arity(
            function,
            2,
            args.len(),
        )));
    }

    if matches!(args[0], ExprType::Null) || matches!(args[1], ExprType::Null) {
        return Ok(args[0].clone());
    }

    let _ = unify_coalesce_expr_types(args[0].clone(), args[1].clone(), |left, right| {
        PlanError::from(ExprPlanError::incompatible_function_arguments(
            function, 0, 1, &left, &right,
        ))
    })?;

    Ok(args[0].clone())
}

fn invalid_function_argument(
    function: Function,
    argument_index: usize,
    found: &ExprType,
) -> PlanError {
    PlanError::from(ExprPlanError::invalid_function_argument(
        function,
        argument_index,
        found,
    ))
}

#[cfg(test)]
mod tests;
