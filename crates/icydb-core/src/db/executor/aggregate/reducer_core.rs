//! Module: executor::aggregate::reducer_core
//! Responsibility: shared value aggregate reducer semantics.
//! Does not own: row access, DISTINCT admission, grouped keys, or execution routing.
//! Boundary: allocation-free COUNT(value), SUM, AVG, MIN, and MAX state transitions.

// Single source of truth for value aggregate semantics:
// COUNT(value), SUM, AVG, MIN, MAX.
//
// All execution paths (scalar, grouped, global) must delegate here.
// Does NOT handle DISTINCT, grouping, or key-stream semantics.

use crate::{
    db::numeric::{
        NumericEvalError, add_decimal_terms_checked, average_decimal_terms_checked,
        canonical_value_compare, coerce_numeric_decimal, compare_numeric_or_strict_order,
    },
    error::InternalError,
    types::Decimal,
    value::Value,
};

///
/// ValueReducerState
///
/// Shared value aggregate reducer state for scalar terminals and grouped
/// value-target terminals. The state owns only reducer payloads; callers remain
/// responsible for DISTINCT admission, row access, filters, and route-specific
/// control flow.
///

pub(in crate::db::executor::aggregate) enum ValueReducerState {
    Count { count: u64 },
    Sum { sum: Option<Decimal>, count: u64 },
    Avg { sum: Decimal, count: u64 },
    Min { selected: Option<Value> },
    Max { selected: Option<Value> },
}

impl ValueReducerState {
    #[must_use]
    pub(in crate::db::executor::aggregate) const fn count() -> Self {
        Self::Count { count: 0 }
    }

    #[must_use]
    pub(in crate::db::executor::aggregate) const fn sum() -> Self {
        Self::Sum {
            sum: None,
            count: 0,
        }
    }

    #[must_use]
    pub(in crate::db::executor::aggregate) const fn avg() -> Self {
        Self::Avg {
            sum: Decimal::ZERO,
            count: 0,
        }
    }

    #[must_use]
    pub(in crate::db::executor::aggregate) const fn min() -> Self {
        Self::Min { selected: None }
    }

    #[must_use]
    pub(in crate::db::executor::aggregate) const fn max() -> Self {
        Self::Max { selected: None }
    }

    /// Ingest one borrowed aggregate input value.
    ///
    /// COUNT ignores NULL, SUM/AVG coerce numeric values, and MIN/MAX compare
    /// with the same numeric-or-strict ordering used by scalar expression
    /// aggregates. Values are cloned only when they become the selected extrema.
    pub(in crate::db::executor::aggregate) fn ingest(
        &mut self,
        value: &Value,
    ) -> Result<(), InternalError> {
        if matches!(value, Value::Null) {
            return Ok(());
        }

        match self {
            Self::Count { .. } => self.increment_count(),
            Self::Sum { .. } | Self::Avg { .. } => {
                let decimal = coerce_numeric_decimal(value).ok_or_else(|| {
                    InternalError::query_executor_invariant(format!(
                        "value aggregate numeric reducer encountered non-numeric value: {value:?}",
                    ))
                })?;

                self.ingest_decimal(decimal)
            }
            Self::Min { selected } => {
                if selected_value_should_replace_min(selected.as_ref(), value)? {
                    *selected = Some(value.clone());
                }

                Ok(())
            }
            Self::Max { selected } => {
                if selected_value_should_replace_max(selected.as_ref(), value)? {
                    *selected = Some(value.clone());
                }

                Ok(())
            }
        }
    }

    /// Ingest one owned aggregate input value without cloning selected extrema.
    pub(in crate::db::executor::aggregate) fn ingest_owned(
        &mut self,
        value: Value,
    ) -> Result<(), InternalError> {
        if matches!(value, Value::Null) {
            return Ok(());
        }

        match self {
            Self::Count { .. } => self.increment_count(),
            Self::Sum { .. } | Self::Avg { .. } => {
                let decimal = coerce_numeric_decimal(&value).ok_or_else(|| {
                    InternalError::query_executor_invariant(format!(
                        "value aggregate numeric reducer encountered non-numeric value: {value:?}",
                    ))
                })?;

                self.ingest_decimal(decimal)
            }
            Self::Min { selected } => {
                if selected_value_should_replace_min(selected.as_ref(), &value)? {
                    *selected = Some(value);
                }

                Ok(())
            }
            Self::Max { selected } => {
                if selected_value_should_replace_max(selected.as_ref(), &value)? {
                    *selected = Some(value);
                }

                Ok(())
            }
        }
    }

    pub(in crate::db::executor::aggregate) fn increment_count(
        &mut self,
    ) -> Result<(), InternalError> {
        match self {
            Self::Count { count } => {
                *count = count.saturating_add(1);
                Ok(())
            }
            Self::Sum { .. } | Self::Avg { .. } | Self::Min { .. } | Self::Max { .. } => {
                Err(reducer_state_mismatch("COUNT"))
            }
        }
    }

    pub(in crate::db::executor::aggregate) fn ingest_decimal(
        &mut self,
        value: Decimal,
    ) -> Result<(), InternalError> {
        match self {
            Self::Sum { sum, count } => {
                *sum = Some(match sum {
                    Some(current) => add_decimal_terms_checked(*current, value)
                        .map_err(NumericEvalError::into_internal_error)?,
                    None => value,
                });
                *count = count.saturating_add(1);
                Ok(())
            }
            Self::Avg { sum, count } => {
                *sum = add_decimal_terms_checked(*sum, value)
                    .map_err(NumericEvalError::into_internal_error)?;
                *count = count.saturating_add(1);
                Ok(())
            }
            Self::Count { .. } | Self::Min { .. } | Self::Max { .. } => {
                Err(reducer_state_mismatch("SUM/AVG"))
            }
        }
    }

    pub(in crate::db::executor::aggregate) fn ingest_canonical_ordered_owned(
        &mut self,
        value: Value,
    ) -> Result<(), InternalError> {
        if matches!(value, Value::Null) {
            return Ok(());
        }

        match self {
            Self::Min { selected } => {
                let replace = selected
                    .as_ref()
                    .is_none_or(|current| canonical_value_compare(&value, current).is_lt());
                if replace {
                    *selected = Some(value);
                }

                Ok(())
            }
            Self::Max { selected } => {
                let replace = selected
                    .as_ref()
                    .is_none_or(|current| canonical_value_compare(&value, current).is_gt());
                if replace {
                    *selected = Some(value);
                }

                Ok(())
            }
            Self::Count { .. } | Self::Sum { .. } | Self::Avg { .. } => {
                Err(reducer_state_mismatch("MIN/MAX"))
            }
        }
    }

    #[must_use]
    pub(in crate::db::executor::aggregate) const fn selected(&self) -> Option<&Value> {
        match self {
            Self::Min { selected } | Self::Max { selected } => selected.as_ref(),
            Self::Count { .. } | Self::Sum { .. } | Self::Avg { .. } => None,
        }
    }

    pub(in crate::db::executor::aggregate) fn replace_selected(
        &mut self,
        value: Value,
    ) -> Result<(), InternalError> {
        match self {
            Self::Min { selected } | Self::Max { selected } => {
                *selected = Some(value);
                Ok(())
            }
            Self::Count { .. } | Self::Sum { .. } | Self::Avg { .. } => {
                Err(reducer_state_mismatch("MIN/MAX"))
            }
        }
    }

    /// Finalize this reducer into the canonical structural aggregate value.
    pub(in crate::db::executor::aggregate) fn finalize(&self) -> Result<Value, InternalError> {
        match self {
            Self::Count { count } => Ok(finalize_count(*count)),
            Self::Sum { sum, .. } => Ok(sum.map_or(Value::Null, Value::Decimal)),
            Self::Avg { sum, count } => {
                if *count == 0 {
                    return Ok(Value::Null);
                }

                average_decimal_terms_checked(*sum, *count)
                    .map(Value::Decimal)
                    .map_err(NumericEvalError::into_internal_error)
            }
            Self::Min { selected } | Self::Max { selected } => {
                Ok(selected.clone().unwrap_or(Value::Null))
            }
        }
    }

    /// Consume this reducer into the canonical structural aggregate value.
    /// This avoids cloning selected extrema when the caller no longer needs
    /// the reducer state after finalization.
    pub(in crate::db::executor::aggregate) fn into_final_value(
        self,
    ) -> Result<Value, InternalError> {
        match self {
            Self::Count { count } => Ok(finalize_count(count)),
            Self::Sum { sum, .. } => Ok(sum.map_or(Value::Null, Value::Decimal)),
            Self::Avg { sum, count } => {
                if count == 0 {
                    return Ok(Value::Null);
                }

                average_decimal_terms_checked(sum, count)
                    .map(Value::Decimal)
                    .map_err(NumericEvalError::into_internal_error)
            }
            Self::Min { selected } | Self::Max { selected } => Ok(selected.unwrap_or(Value::Null)),
        }
    }
}

/// Finalize one COUNT reducer payload through the shared aggregate policy.
#[must_use]
pub(in crate::db::executor::aggregate) const fn finalize_count(count: u64) -> Value {
    Value::Uint(count)
}

fn selected_value_should_replace_min(
    selected: Option<&Value>,
    candidate: &Value,
) -> Result<bool, InternalError> {
    selected_value_should_replace(selected, candidate, true)
}

fn selected_value_should_replace_max(
    selected: Option<&Value>,
    candidate: &Value,
) -> Result<bool, InternalError> {
    selected_value_should_replace(selected, candidate, false)
}

fn selected_value_should_replace(
    selected: Option<&Value>,
    candidate: &Value,
    select_min: bool,
) -> Result<bool, InternalError> {
    let Some(current) = selected else {
        return Ok(true);
    };
    let ordering = compare_numeric_or_strict_order(candidate, current).ok_or_else(|| {
        InternalError::query_executor_invariant(format!(
            "value aggregate extrema reducer encountered incomparable values: left={candidate:?} right={current:?}",
        ))
    })?;

    Ok(if select_min {
        ordering.is_lt()
    } else {
        ordering.is_gt()
    })
}

fn reducer_state_mismatch(kind: &'static str) -> InternalError {
    InternalError::query_executor_invariant(format!(
        "value aggregate reducer {kind} state mismatch"
    ))
}
