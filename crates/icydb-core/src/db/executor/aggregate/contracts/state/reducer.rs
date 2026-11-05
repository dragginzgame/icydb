//! Module: executor::aggregate::contracts::state::reducer
//! Responsibility: scalar and grouped aggregate reducer payload storage.
//! Does not own: route planning, row evaluation, or distinct-key admission.
//! Boundary: stores and finalizes aggregate terminal values.

use crate::{
    db::executor::aggregate::{contracts::AggregateKind, value_reducer::ValueReducerState},
    error::InternalError,
    value::Value,
};
use std::borrow::Cow;

///
/// GroupedAggregateReducerState
///
/// GroupedAggregateReducerState stores grouped terminal reducer payloads as
/// structural values so grouped execution can return either row identities or
/// resolved field-target extrema without reopening typed decode.
///

pub(in crate::db::executor::aggregate::contracts::state) enum GroupedAggregateReducerState {
    Count(ValueReducerState),
    Sum(ValueReducerState),
    Avg(ValueReducerState),
    Exists(bool),
    Min(ValueReducerState),
    Max(ValueReducerState),
    First(ValueReducerState),
    Last(ValueReducerState),
}

impl GroupedAggregateReducerState {
    /// Build the initial grouped reducer state for one aggregate terminal.
    #[must_use]
    pub(in crate::db::executor::aggregate::contracts::state) const fn for_kind(
        kind: AggregateKind,
    ) -> Self {
        match kind {
            AggregateKind::Count => Self::Count(ValueReducerState::count()),
            AggregateKind::Sum => Self::Sum(ValueReducerState::sum()),
            AggregateKind::Avg => Self::Avg(ValueReducerState::avg()),
            AggregateKind::Exists => Self::Exists(false),
            AggregateKind::Min => Self::Min(ValueReducerState::min()),
            AggregateKind::Max => Self::Max(ValueReducerState::max()),
            AggregateKind::First => Self::First(ValueReducerState::min()),
            AggregateKind::Last => Self::Last(ValueReducerState::max()),
        }
    }

    // Apply one COUNT reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn increment_count(
        &mut self,
    ) -> Result<(), InternalError> {
        match self {
            Self::Count(reducer) => reducer.increment_count(),
            _ => Err(InternalError::query_executor_invariant()),
        }
    }

    // Apply one SUM reducer update without widening fixed-width domains.
    pub(in crate::db::executor::aggregate::contracts::state) fn add_sum_value(
        &mut self,
        value: &Value,
    ) -> Result<(), InternalError> {
        match self {
            Self::Sum(reducer) => reducer.ingest_sum_value(value),
            _ => Err(InternalError::query_executor_invariant()),
        }
    }

    // Keep AVG input coercion and domain failures at the shared value reducer.
    pub(in crate::db::executor::aggregate::contracts::state) fn add_average_value(
        &mut self,
        value: &Value,
    ) -> Result<(), InternalError> {
        match self {
            Self::Avg(reducer) => reducer.ingest(value),
            _ => Err(InternalError::query_executor_invariant()),
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
            _ => Err(InternalError::query_executor_invariant()),
        }
    }

    // Apply one MIN reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn update_min_value(
        &mut self,
        value: Value,
    ) -> Result<(), InternalError> {
        match self {
            Self::Min(reducer) | Self::First(reducer) => {
                reducer.ingest_canonical_ordered_owned(value)
            }
            _ => Err(InternalError::query_executor_invariant()),
        }
    }

    // Apply one MAX reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn update_max_value(
        &mut self,
        value: Value,
    ) -> Result<(), InternalError> {
        match self {
            Self::Max(reducer) | Self::Last(reducer) => {
                reducer.ingest_canonical_ordered_owned(value)
            }
            _ => Err(InternalError::query_executor_invariant()),
        }
    }

    // Apply one expression MIN reducer update using expression-value ordering.
    pub(in crate::db::executor::aggregate::contracts::state) fn ingest_min_value(
        &mut self,
        value: Cow<'_, Value>,
    ) -> Result<(), InternalError> {
        match self {
            Self::Min(reducer) => reducer.ingest_cow(value),
            _ => Err(InternalError::query_executor_invariant()),
        }
    }

    // Apply one expression MAX reducer update using expression-value ordering.
    pub(in crate::db::executor::aggregate::contracts::state) fn ingest_max_value(
        &mut self,
        value: Cow<'_, Value>,
    ) -> Result<(), InternalError> {
        match self {
            Self::Max(reducer) => reducer.ingest_cow(value),
            _ => Err(InternalError::query_executor_invariant()),
        }
    }

    // Replace a field-target MIN reducer selection after field-kind-aware comparison.
    pub(in crate::db::executor::aggregate::contracts::state) fn replace_min_value(
        &mut self,
        value: Value,
    ) -> Result<(), InternalError> {
        match self {
            Self::Min(reducer) => reducer.replace_selected(value),
            _ => Err(InternalError::query_executor_invariant()),
        }
    }

    // Replace a field-target MAX reducer selection after field-kind-aware comparison.
    pub(in crate::db::executor::aggregate::contracts::state) fn replace_max_value(
        &mut self,
        value: Value,
    ) -> Result<(), InternalError> {
        match self {
            Self::Max(reducer) => reducer.replace_selected(value),
            _ => Err(InternalError::query_executor_invariant()),
        }
    }

    // Borrow the current field-target MIN selection for field-kind-aware comparison.
    pub(in crate::db::executor::aggregate::contracts::state) fn min_value(
        &self,
    ) -> Result<Option<&Value>, InternalError> {
        match self {
            Self::Min(reducer) => Ok(reducer.selected()),
            _ => Err(InternalError::query_executor_invariant()),
        }
    }

    // Borrow the current field-target MAX selection for field-kind-aware comparison.
    pub(in crate::db::executor::aggregate::contracts::state) fn max_value(
        &self,
    ) -> Result<Option<&Value>, InternalError> {
        match self {
            Self::Max(reducer) => Ok(reducer.selected()),
            _ => Err(InternalError::query_executor_invariant()),
        }
    }

    /// Convert reducer state into the grouped aggregate terminal output value.
    pub(in crate::db::executor::aggregate::contracts::state) fn into_value(
        self,
    ) -> Result<Value, InternalError> {
        match self {
            Self::Count(reducer)
            | Self::Sum(reducer)
            | Self::Avg(reducer)
            | Self::Min(reducer)
            | Self::Max(reducer)
            | Self::First(reducer)
            | Self::Last(reducer) => reducer.into_final_value(),
            Self::Exists(value) => Ok(Value::Bool(value)),
        }
    }
}

#[cfg(test)]
mod tests;
