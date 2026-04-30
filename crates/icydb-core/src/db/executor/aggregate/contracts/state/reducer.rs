use crate::{
    db::executor::aggregate::{
        contracts::spec::{AggregateKind, ScalarAggregateOutput, ScalarTerminalKind},
        reducer_core::{ValueReducerState, finalize_count},
    },
    db::numeric::{NumericEvalError, add_decimal_terms_checked, average_decimal_terms_checked},
    error::InternalError,
    types::Decimal,
    value::{StorageKey, Value, storage_key_as_runtime_value},
};

///
/// ScalarAggregateReducerState
///
/// Shared scalar aggregate terminal reducer state used by streaming and
/// fast-path aggregate execution so scalar terminal update semantics stay
/// centralized.
///

pub(in crate::db::executor) enum ScalarAggregateReducerState {
    Count(u32),
    Exists(bool),
    Min(Option<StorageKey>),
    Max(Option<StorageKey>),
    First(Option<StorageKey>),
    Last(Option<StorageKey>),
}

impl ScalarAggregateReducerState {
    // Build the canonical scalar reducer-state mismatch for one aggregate kind.
    fn state_mismatch(kind: &'static str) -> InternalError {
        InternalError::query_executor_invariant(format!("aggregate reducer {kind} state mismatch"))
    }

    /// Build the initial scalar reducer state for one supported scalar terminal.
    #[must_use]
    pub(in crate::db::executor::aggregate::contracts::state) const fn for_terminal_kind(
        kind: ScalarTerminalKind,
    ) -> Self {
        match kind {
            ScalarTerminalKind::Count => Self::Count(0),
            ScalarTerminalKind::Exists => Self::Exists(false),
            ScalarTerminalKind::Min => Self::Min(None),
            ScalarTerminalKind::Max => Self::Max(None),
            ScalarTerminalKind::First => Self::First(None),
            ScalarTerminalKind::Last => Self::Last(None),
        }
    }

    // Apply one COUNT reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn increment_count(
        &mut self,
    ) -> Result<(), InternalError> {
        match self {
            Self::Count(count) => {
                *count = count.saturating_add(1);
                Ok(())
            }
            _ => Err(Self::state_mismatch("COUNT")),
        }
    }

    // Apply one EXISTS reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn set_exists_true(
        &mut self,
    ) -> Result<(), InternalError> {
        match self {
            Self::Exists(exists) => {
                *exists = true;
                Ok(())
            }
            _ => Err(Self::state_mismatch("EXISTS")),
        }
    }

    // Apply one MIN reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn update_min_value(
        &mut self,
        key: StorageKey,
    ) -> Result<(), InternalError> {
        match self {
            Self::Min(min_key) => {
                let replace = match min_key.as_ref() {
                    Some(current) => key < *current,
                    None => true,
                };
                if replace {
                    *min_key = Some(key);
                }

                Ok(())
            }
            _ => Err(Self::state_mismatch("MIN")),
        }
    }

    // Apply one MAX reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn update_max_value(
        &mut self,
        key: StorageKey,
    ) -> Result<(), InternalError> {
        match self {
            Self::Max(max_key) => {
                let replace = match max_key.as_ref() {
                    Some(current) => key > *current,
                    None => true,
                };
                if replace {
                    *max_key = Some(key);
                }

                Ok(())
            }
            _ => Err(Self::state_mismatch("MAX")),
        }
    }

    // Apply one FIRST reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn set_first(
        &mut self,
        key: StorageKey,
    ) -> Result<(), InternalError> {
        match self {
            Self::First(first_key) => {
                *first_key = Some(key);
                Ok(())
            }
            _ => Err(Self::state_mismatch("FIRST")),
        }
    }

    // Apply one LAST reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn set_last(
        &mut self,
        key: StorageKey,
    ) -> Result<(), InternalError> {
        match self {
            Self::Last(last_key) => {
                *last_key = Some(key);
                Ok(())
            }
            _ => Err(Self::state_mismatch("LAST")),
        }
    }

    /// Convert reducer state into the structural scalar aggregate terminal output payload.
    #[must_use]
    pub(in crate::db::executor::aggregate::contracts::state) fn into_output(
        self,
    ) -> ScalarAggregateOutput {
        match self {
            Self::Count(value) => {
                let Value::Uint(count) = finalize_count(u64::from(value)) else {
                    unreachable!("COUNT finalization must produce Uint")
                };

                ScalarAggregateOutput::Count(u32::try_from(count).unwrap_or(u32::MAX))
            }
            Self::Exists(value) => ScalarAggregateOutput::Exists(value),
            Self::Min(value) => ScalarAggregateOutput::Min(value),
            Self::Max(value) => ScalarAggregateOutput::Max(value),
            Self::First(value) => ScalarAggregateOutput::First(value),
            Self::Last(value) => ScalarAggregateOutput::Last(value),
        }
    }
}

///
/// GroupedAggregateReducerState
///
/// GroupedAggregateReducerState stores grouped terminal reducer payloads as
/// structural values so grouped execution can return either row identities or
/// resolved field-target extrema without reopening typed decode.
///

pub(in crate::db::executor::aggregate::contracts::state) enum GroupedAggregateReducerState {
    Count(u64),
    Sum { sum: Option<Decimal>, count: u64 },
    Avg { sum: Decimal, count: u64 },
    Exists(bool),
    Min(ValueReducerState),
    Max(ValueReducerState),
    First(Option<Value>),
    Last(Option<Value>),
}

impl GroupedAggregateReducerState {
    // Build the canonical grouped reducer-state mismatch for one aggregate kind.
    fn state_mismatch(kind: &'static str) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped aggregate reducer {kind} state mismatch"
        ))
    }

    /// Build the initial grouped reducer state for one aggregate terminal.
    #[must_use]
    pub(in crate::db::executor::aggregate::contracts::state) const fn for_kind(
        kind: AggregateKind,
    ) -> Self {
        match kind {
            AggregateKind::Count => Self::Count(0),
            AggregateKind::Sum => Self::Sum {
                sum: None,
                count: 0,
            },
            AggregateKind::Avg => Self::Avg {
                sum: Decimal::ZERO,
                count: 0,
            },
            AggregateKind::Exists => Self::Exists(false),
            AggregateKind::Min => Self::Min(ValueReducerState::min()),
            AggregateKind::Max => Self::Max(ValueReducerState::max()),
            AggregateKind::First => Self::First(None),
            AggregateKind::Last => Self::Last(None),
        }
    }

    // Apply one COUNT reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn increment_count(
        &mut self,
    ) -> Result<(), InternalError> {
        match self {
            Self::Count(count) => {
                *count = count.saturating_add(1);
                Ok(())
            }
            _ => Err(Self::state_mismatch("COUNT")),
        }
    }

    // Apply one SUM reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn add_sum_value(
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
            _ => Err(Self::state_mismatch("SUM")),
        }
    }

    // Apply one AVG reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn add_average_value(
        &mut self,
        value: Decimal,
    ) -> Result<(), InternalError> {
        match self {
            Self::Avg { sum, count } => {
                *sum = add_decimal_terms_checked(*sum, value)
                    .map_err(NumericEvalError::into_internal_error)?;
                *count = count.saturating_add(1);

                Ok(())
            }
            _ => Err(Self::state_mismatch("AVG")),
        }
    }

    // Apply one EXISTS reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn set_exists_true(
        &mut self,
    ) -> Result<(), InternalError> {
        match self {
            Self::Exists(exists) => {
                *exists = true;
                Ok(())
            }
            _ => Err(Self::state_mismatch("EXISTS")),
        }
    }

    // Apply one MIN reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn update_min_value(
        &mut self,
        value: Value,
    ) -> Result<(), InternalError> {
        match self {
            Self::Min(reducer) => reducer.ingest_canonical_ordered_owned(value),
            _ => Err(Self::state_mismatch("MIN")),
        }
    }

    // Apply one MAX reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn update_max_value(
        &mut self,
        value: Value,
    ) -> Result<(), InternalError> {
        match self {
            Self::Max(reducer) => reducer.ingest_canonical_ordered_owned(value),
            _ => Err(Self::state_mismatch("MAX")),
        }
    }

    // Apply one expression MIN reducer update using expression-value ordering.
    pub(in crate::db::executor::aggregate::contracts::state) fn ingest_min_value(
        &mut self,
        value: Value,
    ) -> Result<(), InternalError> {
        match self {
            Self::Min(reducer) => reducer.ingest_owned(value),
            _ => Err(Self::state_mismatch("MIN")),
        }
    }

    // Apply one expression MAX reducer update using expression-value ordering.
    pub(in crate::db::executor::aggregate::contracts::state) fn ingest_max_value(
        &mut self,
        value: Value,
    ) -> Result<(), InternalError> {
        match self {
            Self::Max(reducer) => reducer.ingest_owned(value),
            _ => Err(Self::state_mismatch("MAX")),
        }
    }

    // Replace a field-target MIN reducer selection after field-kind-aware comparison.
    pub(in crate::db::executor::aggregate::contracts::state) fn replace_min_value(
        &mut self,
        value: Value,
    ) -> Result<(), InternalError> {
        match self {
            Self::Min(reducer) => reducer.replace_selected(value),
            _ => Err(Self::state_mismatch("MIN")),
        }
    }

    // Replace a field-target MAX reducer selection after field-kind-aware comparison.
    pub(in crate::db::executor::aggregate::contracts::state) fn replace_max_value(
        &mut self,
        value: Value,
    ) -> Result<(), InternalError> {
        match self {
            Self::Max(reducer) => reducer.replace_selected(value),
            _ => Err(Self::state_mismatch("MAX")),
        }
    }

    // Borrow the current field-target MIN selection for field-kind-aware comparison.
    pub(in crate::db::executor::aggregate::contracts::state) fn min_value(
        &self,
    ) -> Result<Option<&Value>, InternalError> {
        match self {
            Self::Min(reducer) => Ok(reducer.selected()),
            _ => Err(Self::state_mismatch("MIN")),
        }
    }

    // Borrow the current field-target MAX selection for field-kind-aware comparison.
    pub(in crate::db::executor::aggregate::contracts::state) fn max_value(
        &self,
    ) -> Result<Option<&Value>, InternalError> {
        match self {
            Self::Max(reducer) => Ok(reducer.selected()),
            _ => Err(Self::state_mismatch("MAX")),
        }
    }

    // Apply one FIRST reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn set_first(
        &mut self,
        key: StorageKey,
    ) -> Result<(), InternalError> {
        match self {
            Self::First(first_key) => {
                *first_key = Some(storage_key_as_runtime_value(&key));
                Ok(())
            }
            _ => Err(Self::state_mismatch("FIRST")),
        }
    }

    // Apply one LAST reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn set_last(
        &mut self,
        key: StorageKey,
    ) -> Result<(), InternalError> {
        match self {
            Self::Last(last_key) => {
                *last_key = Some(storage_key_as_runtime_value(&key));
                Ok(())
            }
            _ => Err(Self::state_mismatch("LAST")),
        }
    }

    /// Convert reducer state into the grouped aggregate terminal output value.
    pub(in crate::db::executor::aggregate::contracts::state) fn into_value(
        self,
    ) -> Result<Value, InternalError> {
        match self {
            Self::Count(value) => Ok(finalize_count(value)),
            Self::Sum { sum, .. } => Ok(sum.map_or(Value::Null, Value::Decimal)),
            Self::Avg { sum, count } => {
                if count == 0 {
                    return Ok(Value::Null);
                }

                average_decimal_terms_checked(sum, count)
                    .map(Value::Decimal)
                    .map_err(NumericEvalError::into_internal_error)
            }
            Self::Min(reducer) | Self::Max(reducer) => reducer.finalize(),
            Self::Exists(value) => Ok(Value::Bool(value)),
            Self::First(value) | Self::Last(value) => Ok(value.unwrap_or(Value::Null)),
        }
    }
}
