//! Conservative backing admission for the shared predicate encoder.
//! Does not define canonical bytes, normalization, or a new resource policy.

use crate::{
    db::{
        predicate::{CoercionId, CoercionSpec, CompareOp, Predicate},
        query::construction::{ConstructionBudget, ENCODING_NODE_BYTES, encoded_value_capacity},
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

/// Visit admitted input without encoding/coercing it; bound output and admit
/// coercion/map scratch before the shared encoder can allocate either.
pub(in crate::db::predicate) fn normalized_predicate_key_capacity(
    predicate: &Predicate,
    budget: &dyn ConstructionBudget,
) -> Result<usize, InternalError> {
    predicate_key_capacity(predicate, budget, true)
}

/// Raw encoding also admits temporary membership views before sorting/coercion.
/// This covers construction, not all nested payload-comparison work.
pub(in crate::db::predicate) fn raw_predicate_key_capacity(
    predicate: &Predicate,
    budget: &dyn ConstructionBudget,
) -> Result<usize, InternalError> {
    predicate_key_capacity(predicate, budget, false)
}

fn predicate_key_capacity(
    predicate: &Predicate,
    budget: &dyn ConstructionBudget,
    canonical_lists: bool,
) -> Result<usize, InternalError> {
    let bytes = predicate_capacity(predicate, budget, canonical_lists)?;
    // Covers filling and hashing the buffer, including framing backpatches.
    budget.charge(Resource::PredicateExpressionSteps, bytes.saturating_mul(2))?;
    usize::try_from(bytes).map_err(|_| InternalError::query_executor_invariant())
}

fn predicate_capacity(
    predicate: &Predicate,
    budget: &dyn ConstructionBudget,
    canonical_lists: bool,
) -> Result<u64, InternalError> {
    budget.charge(Resource::PredicateExpressionSteps, 1)?;
    let mut bytes = ENCODING_NODE_BYTES;
    match predicate {
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                bytes = bytes.saturating_add(predicate_capacity(child, budget, canonical_lists)?);
            }
        }
        Predicate::Not(child) => {
            bytes = bytes.saturating_add(predicate_capacity(child, budget, canonical_lists)?);
        }
        Predicate::Compare(compare) => {
            bytes = bytes.saturating_add(compare.field().len() as u64);
            bytes = bytes.saturating_add(coercion_capacity(compare.coercion(), budget)?);
            // Only direct membership items are coerced. Nested containers keep
            // strict encoding. Raw encoding may need a temporary ordered view;
            // allow it without repeating the encoder's canonicality scan.
            let operand = if matches!(compare.op(), CompareOp::In | CompareOp::NotIn)
                && let Value::List(items) = compare.value()
            {
                if !canonical_lists && !items.is_empty() {
                    budget.charge(
                        Resource::TemporaryBytes,
                        (items.len().max(4) as u64)
                            .saturating_mul(size_of::<std::borrow::Cow<'_, Value>>() as u64),
                    )?;
                }
                budget.charge(Resource::NestedValueSteps, 1)?;
                let mut bytes = ENCODING_NODE_BYTES;
                for item in items {
                    bytes = bytes.saturating_add(encoded_value_capacity(
                        item,
                        compare.coercion().id(),
                        budget,
                    )?);
                }
                bytes
            } else {
                encoded_value_capacity(compare.value(), compare.coercion().id(), budget)?
            };
            bytes = bytes.saturating_add(operand);
        }
        Predicate::CompareFields(compare) => {
            bytes = bytes
                .saturating_add(compare.left_field.len() as u64)
                .saturating_add(compare.right_field.len() as u64)
                .saturating_add(coercion_capacity(&compare.coercion, budget)?);
        }
        Predicate::TextContains { field, value } | Predicate::TextContainsCi { field, value } => {
            bytes = bytes
                .saturating_add(field.len() as u64)
                .saturating_add(encoded_value_capacity(value, CoercionId::Strict, budget)?);
        }
        Predicate::IsNull { field }
        | Predicate::IsNotNull { field }
        | Predicate::IsMissing { field }
        | Predicate::IsEmpty { field }
        | Predicate::IsNotEmpty { field } => bytes = bytes.saturating_add(field.len() as u64),
        Predicate::True | Predicate::False => {}
    }
    Ok(bytes)
}

fn coercion_capacity(
    spec: &CoercionSpec,
    budget: &dyn ConstructionBudget,
) -> Result<u64, InternalError> {
    budget.charge(
        Resource::PredicateExpressionSteps,
        spec.params().len() as u64,
    )?;
    Ok(spec.params().iter().fold(0u64, |bytes, (key, value)| {
        bytes
            .saturating_add(16)
            .saturating_add(key.len() as u64)
            .saturating_add(value.len() as u64)
    }))
}
