//! Module: executor::aggregate::value_reducer
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
        NumericEvalError, add_decimal_terms_checked, add_u256_terms_checked,
        average_decimal_terms_checked, coerce_numeric_decimal,
    },
    types::{Decimal, U256},
};
use crate::{
    db::numeric::{canonical_value_compare, compare_numeric_or_strict_order},
    error::InternalError,
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
    Sum { sum: Option<SumAccumulator> },
    Avg { sum: Decimal, count: u64 },
    Min { selected: Option<Value> },
    Max { selected: Option<Value> },
}

pub(in crate::db::executor::aggregate) enum SumAccumulator {
    Decimal(Decimal),
    U256(U256),
}

impl SumAccumulator {
    fn from_value(value: &Value) -> Result<Self, InternalError> {
        match value {
            Value::U256(value) => Ok(Self::U256(*value)),
            value => coerce_aggregate_decimal(value).map(Self::Decimal),
        }
    }

    fn checked_add(self, rhs: Self) -> Result<Self, NumericEvalError> {
        match (self, rhs) {
            (Self::Decimal(left), Self::Decimal(right)) => {
                add_decimal_terms_checked(left, right).map(Self::Decimal)
            }
            (Self::U256(left), Self::U256(right)) => {
                add_u256_terms_checked(left, right).map(Self::U256)
            }
            (Self::Decimal(_), Self::U256(_)) | (Self::U256(_), Self::Decimal(_)) => {
                Err(NumericEvalError::NotRepresentable)
            }
        }
    }

    const fn into_value(self) -> Value {
        match self {
            Self::Decimal(value) => Value::Decimal(value),
            Self::U256(value) => Value::U256(value),
        }
    }
}

impl ValueReducerState {
    #[must_use]
    pub(in crate::db::executor::aggregate) const fn count() -> Self {
        Self::Count { count: 0 }
    }

    #[must_use]
    pub(in crate::db::executor::aggregate) const fn sum() -> Self {
        Self::Sum { sum: None }
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
            Self::Sum { .. } => self.ingest_sum_value(value),
            Self::Avg { .. } => {
                let decimal = coerce_aggregate_decimal(value)?;

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
            Self::Sum { .. } => self.ingest_sum_value(&value),
            Self::Avg { .. } => {
                let decimal = coerce_aggregate_decimal(&value)?;

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

    fn ingest_decimal(&mut self, value: Decimal) -> Result<(), InternalError> {
        match self {
            Self::Sum { .. } => self.ingest_sum_accumulator(SumAccumulator::Decimal(value)),
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

    /// Ingest one SUM value without widening fixed-width domains.
    pub(in crate::db::executor::aggregate) fn ingest_sum_value(
        &mut self,
        value: &Value,
    ) -> Result<(), InternalError> {
        let value = SumAccumulator::from_value(value)?;
        self.ingest_sum_accumulator(value)
    }

    fn ingest_sum_accumulator(&mut self, value: SumAccumulator) -> Result<(), InternalError> {
        let Self::Sum { sum } = self else {
            return Err(reducer_state_mismatch("SUM"));
        };
        *sum = Some(match sum.take() {
            Some(current) => current
                .checked_add(value)
                .map_err(NumericEvalError::into_internal_error)?,
            None => value,
        });

        Ok(())
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

    /// Consume this reducer into the canonical structural aggregate value.
    /// This avoids cloning selected extrema when the caller no longer needs
    /// the reducer state after finalization.
    pub(in crate::db::executor::aggregate) fn into_final_value(
        self,
    ) -> Result<Value, InternalError> {
        match self {
            Self::Count { count } => Ok(finalize_count(count)),
            Self::Sum { sum } => Ok(sum.map_or(Value::Null, SumAccumulator::into_value)),
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
    Value::Nat64(count)
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
    let ordering = compare_numeric_or_strict_order(candidate, current)
        .ok_or_else(InternalError::query_executor_invariant)?;

    Ok(if select_min {
        ordering.is_lt()
    } else {
        ordering.is_gt()
    })
}

// Admission owns the numeric family; values may exceed Decimal's range, and
// big integers deliberately do not participate in its coercion policy at any
// magnitude. Keep those domain failures distinct from invalid reducer inputs.
fn coerce_aggregate_decimal(value: &Value) -> Result<Decimal, InternalError> {
    coerce_numeric_decimal(value).ok_or_else(|| {
        if value.supports_numeric_coercion() || matches!(value, Value::NatBig(_) | Value::IntBig(_))
        {
            InternalError::query_numeric_not_representable()
        } else {
            InternalError::query_executor_invariant()
        }
    })
}

fn reducer_state_mismatch(_kind: &'static str) -> InternalError {
    InternalError::query_executor_invariant()
}

#[cfg(test)]
mod tests {
    use super::ValueReducerState;
    use crate::{types::U256, value::Value};
    use icydb_diagnostic_code::DiagnosticCode;

    #[test]
    fn aggregate_coercion_distinguishes_numeric_domain_from_invalid_input() {
        for (value, expected) in [
            (
                Value::Nat128(u128::MAX),
                DiagnosticCode::QueryNumericNotRepresentable,
            ),
            (
                Value::Text("not numeric".into()),
                DiagnosticCode::RuntimeInvariantViolation,
            ),
        ] {
            for owned in [false, true] {
                for mut reducer in [ValueReducerState::sum(), ValueReducerState::avg()] {
                    let error = if owned {
                        reducer.ingest_owned(value.clone())
                    } else {
                        reducer.ingest(&value)
                    }
                    .expect_err("invalid reducer operand");
                    assert_eq!(error.diagnostic().code(), expected);
                }
            }
        }
        let error = ValueReducerState::avg()
            .ingest(&Value::U256(U256::ONE))
            .expect_err("AVG does not admit U256");
        assert_eq!(
            error.diagnostic().code(),
            DiagnosticCode::RuntimeInvariantViolation
        );
    }

    #[test]
    fn aggregate_decimal_bounds_and_nulls_preserve_current_results() {
        use crate::types::Decimal;

        for owned in [false, true] {
            for mut reducer in [ValueReducerState::sum(), ValueReducerState::avg()] {
                reducer.ingest(&Value::Null).unwrap();
                let maximum = Value::Int128(i128::MAX);
                if owned {
                    reducer.ingest_owned(maximum).unwrap();
                } else {
                    reducer.ingest(&maximum).unwrap();
                }
                let error = reducer
                    .ingest(&Value::Int128(1))
                    .expect_err("bounded running sum");
                assert_eq!(
                    error.diagnostic().code(),
                    DiagnosticCode::QueryNumericOverflow
                );
            }
        }
        for mut reducer in [ValueReducerState::sum(), ValueReducerState::avg()] {
            reducer.ingest(&Value::Null).unwrap();
            assert_eq!(reducer.into_final_value().unwrap(), Value::Null);
        }
        let mut average = ValueReducerState::avg();
        average.ingest(&Value::Nat64(1)).unwrap();
        average.ingest_owned(Value::Nat64(2)).unwrap();
        assert_eq!(
            average.into_final_value().unwrap(),
            Value::Decimal(Decimal::new(15, 1))
        );
    }

    #[test]
    fn u256_sum_stays_inline_and_returns_u256() {
        let mut reducer = ValueReducerState::sum();

        reducer
            .ingest_sum_value(&Value::U256(U256::from(2_u64)))
            .expect("first U256 SUM value should ingest");
        reducer
            .ingest_sum_value(&Value::U256(U256::from(3_u64)))
            .expect("second U256 SUM value should ingest");

        assert_eq!(
            reducer
                .into_final_value()
                .expect("U256 SUM should finalize"),
            Value::U256(U256::from(5_u64)),
        );
        assert_eq!(std::mem::size_of::<ValueReducerState>(), 80);
        assert_eq!(std::mem::align_of::<ValueReducerState>(), 16);
    }

    #[test]
    fn u256_sum_reports_typed_overflow() {
        let mut reducer = ValueReducerState::sum();
        reducer
            .ingest_sum_value(&Value::U256(U256::MAX))
            .expect("first U256 SUM value should ingest");
        let err = reducer
            .ingest_sum_value(&Value::U256(U256::ONE))
            .expect_err("U256 SUM should reject overflow");

        assert_eq!(
            err.diagnostic().code(),
            DiagnosticCode::QueryNumericOverflow
        );
    }
}
