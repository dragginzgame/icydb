use crate::db::{
    query::plan::{
        PlanError,
        expr::{
            Expr, ExprCoarseTypeFamily, Function, FunctionTypeInferenceShape, NumericSubtype,
            type_inference::{ExprType, infer_expr_type, unify::unify_coalesce_expr_types},
        },
        validate::ExprPlanError,
    },
    schema::SchemaInfo,
};

impl FunctionTypeInferenceShape {
    pub(super) fn arg_coarse_family(self, index: usize) -> Option<ExprCoarseTypeFamily> {
        match self {
            Self::ByteLengthResult
            | Self::UnaryBoolPredicate
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
            Self::NumericScaleResult => {
                matches!(index, 0 | 1).then_some(ExprCoarseTypeFamily::Numeric)
            }
        }
    }

    #[cfg(test)]
    pub(super) const fn result_coarse_family(self) -> Option<ExprCoarseTypeFamily> {
        match self {
            Self::ByteLengthResult | Self::NumericResult { .. } | Self::NumericScaleResult => {
                Some(ExprCoarseTypeFamily::Numeric)
            }
            Self::UnaryBoolPredicate | Self::CollectionContains | Self::BoolResult { .. } => {
                Some(ExprCoarseTypeFamily::Bool)
            }
            Self::TextResult { .. } => Some(ExprCoarseTypeFamily::Text),
            Self::DynamicCoalesce | Self::DynamicNullIf => None,
        }
    }

    #[must_use]
    #[cfg(test)]
    pub(super) const fn dynamic_arg_coarse_family(
        self,
        result_family: ExprCoarseTypeFamily,
    ) -> Option<ExprCoarseTypeFamily> {
        match self {
            Self::DynamicCoalesce | Self::DynamicNullIf => Some(result_family),
            _ => None,
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
            Self::TextResult { .. } => {
                validate_function_arg_families(function, args, self)?;

                Ok(ExprType::Text)
            }
            Self::NumericResult { subtype, .. } => {
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
            Self::DynamicCoalesce => infer_coalesce_function_type(args),
            Self::DynamicNullIf => infer_nullif_function_type(args),
        }
    }
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
) -> Result<ExprType, PlanError> {
    let arg_types = args
        .iter()
        .map(|arg| infer_expr_type(arg, schema))
        .collect::<Result<Vec<_>, _>>()?;

    function
        .type_inference_shape()
        .infer_function_result_type(function, arg_types.as_slice())
}

fn validate_exact_function_arg_count(
    function: Function,
    actual: usize,
    expected: usize,
) -> Result<(), PlanError> {
    if actual != expected {
        return Err(PlanError::from(ExprPlanError::invalid_function_argument(
            function.canonical_label(),
            actual,
            format!("expected exactly {expected} args, found {actual}"),
        )));
    }

    Ok(())
}

fn validate_byte_length_function_args(
    function: Function,
    args: &[ExprType],
) -> Result<(), PlanError> {
    validate_exact_function_arg_count(function, args.len(), 1)?;

    let input_compatible = matches!(args[0], ExprType::Text | ExprType::Blob) || {
        #[cfg(test)]
        {
            matches!(args[0], ExprType::Null)
        }
        #[cfg(not(test))]
        {
            false
        }
    };

    if !input_compatible {
        return Err(PlanError::from(ExprPlanError::invalid_function_argument(
            function.canonical_label(),
            0,
            format!("{:?}", args[0]),
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
    shape: FunctionTypeInferenceShape,
) -> Result<(), PlanError> {
    for (index, arg) in args.iter().enumerate() {
        let Some(family) = shape.arg_coarse_family(index) else {
            continue;
        };

        if !expr_type_accepts_required_coarse_family(arg, family) {
            return Err(PlanError::from(ExprPlanError::invalid_function_argument(
                function.canonical_label(),
                index,
                format!("{arg:?}"),
            )));
        }
    }

    Ok(())
}

fn validate_numeric_scale_function_args(
    function: Function,
    args: &[ExprType],
) -> Result<(), PlanError> {
    if args.len() != 2 {
        return Err(PlanError::from(ExprPlanError::invalid_function_argument(
            function.canonical_label(),
            args.len(),
            format!("expected exactly 2 args, found {}", args.len()),
        )));
    }

    if !matches!(args[0], ExprType::Numeric(_)) {
        return Err(PlanError::from(ExprPlanError::invalid_function_argument(
            function.canonical_label(),
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
            function.canonical_label(),
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
