//! Module: executor::aggregate::contracts::state::reducer
//! Responsibility: scalar and grouped aggregate reducer payload storage.
//! Does not own: route planning, row evaluation, or distinct-key admission.
//! Boundary: stores and finalizes aggregate terminal values.

use crate::{
    db::{
        executor::aggregate::{contracts::AggregateKind, value_reducer::ValueReducerState},
        key_taxonomy::PrimaryKeyValue,
    },
    error::InternalError,
    types::Decimal,
    value::Value,
};

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
    First(Option<Value>),
    Last(Option<Value>),
}

impl GroupedAggregateReducerState {
    // Build the canonical grouped reducer-state mismatch for one aggregate kind.
    fn state_mismatch(_kind: &'static str) -> InternalError {
        InternalError::query_executor_invariant()
    }

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
            AggregateKind::First => Self::First(None),
            AggregateKind::Last => Self::Last(None),
        }
    }

    // Apply one COUNT reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn increment_count(
        &mut self,
    ) -> Result<(), InternalError> {
        match self {
            Self::Count(reducer) => reducer.increment_count(),
            _ => Err(Self::state_mismatch("COUNT")),
        }
    }

    // Apply one SUM reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn add_sum_value(
        &mut self,
        value: Decimal,
    ) -> Result<(), InternalError> {
        match self {
            Self::Sum(reducer) => reducer.ingest_decimal(value),
            _ => Err(Self::state_mismatch("SUM")),
        }
    }

    // Apply one AVG reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn add_average_value(
        &mut self,
        value: Decimal,
    ) -> Result<(), InternalError> {
        match self {
            Self::Avg(reducer) => reducer.ingest_decimal(value),
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
        key: &PrimaryKeyValue,
    ) -> Result<(), InternalError> {
        match self {
            Self::First(first_key) => {
                *first_key = Some(key.as_runtime_value());
                Ok(())
            }
            _ => Err(Self::state_mismatch("FIRST")),
        }
    }

    // Apply one LAST reducer update.
    pub(in crate::db::executor::aggregate::contracts::state) fn set_last(
        &mut self,
        key: &PrimaryKeyValue,
    ) -> Result<(), InternalError> {
        match self {
            Self::Last(last_key) => {
                *last_key = Some(key.as_runtime_value());
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
            Self::Count(reducer)
            | Self::Sum(reducer)
            | Self::Avg(reducer)
            | Self::Min(reducer)
            | Self::Max(reducer) => reducer.into_final_value(),
            Self::Exists(value) => Ok(Value::Bool(value)),
            Self::First(value) | Self::Last(value) => Ok(value.unwrap_or(Value::Null)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        db::executor::aggregate::{AggregateKind, contracts::state::GroupedAggregateReducerState},
        types::Decimal,
        value::Value,
    };

    #[test]
    fn grouped_count_sum_and_avg_delegate_to_shared_value_reducers() {
        let one = Decimal::from_i64(1).expect("decimal one");
        let three = Decimal::from_i64(3).expect("decimal three");

        let mut count = GroupedAggregateReducerState::for_kind(AggregateKind::Count);
        count.increment_count().expect("count ingest");
        count.increment_count().expect("count ingest");
        assert_eq!(count.into_value().expect("count finalize"), Value::Nat64(2));

        let mut sum = GroupedAggregateReducerState::for_kind(AggregateKind::Sum);
        sum.add_sum_value(one).expect("sum ingest");
        sum.add_sum_value(three).expect("sum ingest");
        assert_eq!(
            sum.into_value().expect("sum finalize"),
            Value::Decimal(Decimal::from_i64(4).expect("decimal four")),
        );

        let mut avg = GroupedAggregateReducerState::for_kind(AggregateKind::Avg);
        avg.add_average_value(one).expect("avg ingest");
        avg.add_average_value(three).expect("avg ingest");
        assert_eq!(
            avg.into_value().expect("avg finalize"),
            Value::Decimal(Decimal::from_i64(2).expect("decimal two")),
        );
    }
}
