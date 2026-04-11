//! Module: executor::aggregate::contracts::state
//! Responsibility: scalar aggregate reducer state machines and grouped structural terminal reducers.
//! Does not own: grouped budget/accounting policy.
//! Boundary: state/fold mechanics used by aggregate execution kernels.

use crate::{
    db::{
        contracts::canonical_value_compare,
        data::DataKey,
        direction::Direction,
        executor::{
            aggregate::{
                contracts::{
                    error::GroupError,
                    grouped::ExecutionContext,
                    spec::{AggregateKind, ScalarAggregateOutput},
                },
                field::{
                    FieldSlot as AggregateFieldSlot, compare_orderable_field_values_with_slot,
                },
            },
            group::{CanonicalKey, GroupKey, GroupKeySet, KeyCanonicalError},
            pipeline::contracts::RowView,
        },
        numeric::{add_decimal_terms, average_decimal_terms, coerce_numeric_decimal},
        query::plan::FieldSlot,
    },
    error::InternalError,
    types::Decimal,
    value::{StorageKey, Value},
};

///
/// FoldControl
///

#[derive(Clone, Copy, Debug)]
pub(in crate::db::executor) enum FoldControl {
    Continue,
    Break,
}

///
/// AggregateReducerClass
///
/// Owner-local grouped classification for aggregate reducer state and terminal
/// update dispatch. This keeps `AggregateKind` shock radius out of the reducer
/// implementations themselves.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AggregateReducerClass {
    Count,
    SumLike,
    Exists,
    Min,
    Max,
    First,
    Last,
}

impl AggregateKind {
    // Classify one aggregate kind for reducer-state initialization and terminal dispatch.
    const fn reducer_class(self) -> AggregateReducerClass {
        match self {
            Self::Count => AggregateReducerClass::Count,
            Self::Sum | Self::Avg => AggregateReducerClass::SumLike,
            Self::Exists => AggregateReducerClass::Exists,
            Self::Min => AggregateReducerClass::Min,
            Self::Max => AggregateReducerClass::Max,
            Self::First => AggregateReducerClass::First,
            Self::Last => AggregateReducerClass::Last,
        }
    }
}

///
/// ScalarAggregateReducerState
///
/// Shared scalar aggregate terminal reducer state used by streaming and
/// fast-path aggregate execution so scalar terminal update semantics stay
/// centralized.
///

pub(in crate::db::executor) enum ScalarAggregateReducerState {
    Count(u32),
    Sum(Option<Decimal>),
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

    /// Build the initial scalar reducer state for one aggregate terminal.
    #[must_use]
    pub(in crate::db::executor) const fn for_kind(kind: AggregateKind) -> Self {
        match kind.reducer_class() {
            AggregateReducerClass::Count => Self::Count(0),
            AggregateReducerClass::SumLike => Self::Sum(None),
            AggregateReducerClass::Exists => Self::Exists(false),
            AggregateReducerClass::Min => Self::Min(None),
            AggregateReducerClass::Max => Self::Max(None),
            AggregateReducerClass::First => Self::First(None),
            AggregateReducerClass::Last => Self::Last(None),
        }
    }

    // Apply one COUNT reducer update.
    fn increment_count(&mut self) -> Result<(), InternalError> {
        match self {
            Self::Count(count) => {
                *count = count.saturating_add(1);
                Ok(())
            }
            _ => Err(Self::state_mismatch("COUNT")),
        }
    }

    // Apply one EXISTS reducer update.
    fn set_exists_true(&mut self) -> Result<(), InternalError> {
        match self {
            Self::Exists(exists) => {
                *exists = true;
                Ok(())
            }
            _ => Err(Self::state_mismatch("EXISTS")),
        }
    }

    // Apply one MIN reducer update.
    fn update_min_value(&mut self, key: StorageKey) -> Result<(), InternalError> {
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
    fn update_max_value(&mut self, key: StorageKey) -> Result<(), InternalError> {
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
    fn set_first(&mut self, key: StorageKey) -> Result<(), InternalError> {
        match self {
            Self::First(first_key) => {
                *first_key = Some(key);
                Ok(())
            }
            _ => Err(Self::state_mismatch("FIRST")),
        }
    }

    // Apply one LAST reducer update.
    fn set_last(&mut self, key: StorageKey) -> Result<(), InternalError> {
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
    pub(in crate::db::executor) const fn into_output(self) -> ScalarAggregateOutput {
        match self {
            Self::Count(value) => ScalarAggregateOutput::Count(value),
            Self::Sum(value) => ScalarAggregateOutput::Sum(value),
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

enum GroupedAggregateReducerState {
    Count(u32),
    Sum(Option<Decimal>),
    Avg { sum: Decimal, row_count: u64 },
    Exists(bool),
    Min(Option<Value>),
    Max(Option<Value>),
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
    const fn for_kind(kind: AggregateKind) -> Self {
        match kind {
            AggregateKind::Count => Self::Count(0),
            AggregateKind::Sum => Self::Sum(None),
            AggregateKind::Avg => Self::Avg {
                sum: Decimal::ZERO,
                row_count: 0,
            },
            AggregateKind::Exists => Self::Exists(false),
            AggregateKind::Min => Self::Min(None),
            AggregateKind::Max => Self::Max(None),
            AggregateKind::First => Self::First(None),
            AggregateKind::Last => Self::Last(None),
        }
    }

    // Apply one COUNT reducer update.
    fn increment_count(&mut self) -> Result<(), InternalError> {
        match self {
            Self::Count(count) => {
                *count = count.saturating_add(1);
                Ok(())
            }
            _ => Err(Self::state_mismatch("COUNT")),
        }
    }

    // Apply one SUM reducer update.
    fn add_sum_value(&mut self, value: Decimal) -> Result<(), InternalError> {
        match self {
            Self::Sum(sum) => {
                *sum = Some(sum.map_or(value, |current| add_decimal_terms(current, value)));
                Ok(())
            }
            _ => Err(Self::state_mismatch("SUM")),
        }
    }

    // Apply one AVG reducer update.
    fn add_average_value(&mut self, value: Decimal) -> Result<(), InternalError> {
        match self {
            Self::Avg { sum, row_count } => {
                *sum = add_decimal_terms(*sum, value);
                *row_count = row_count.saturating_add(1);
                Ok(())
            }
            _ => Err(Self::state_mismatch("AVG")),
        }
    }

    // Apply one EXISTS reducer update.
    fn set_exists_true(&mut self) -> Result<(), InternalError> {
        match self {
            Self::Exists(exists) => {
                *exists = true;
                Ok(())
            }
            _ => Err(Self::state_mismatch("EXISTS")),
        }
    }

    // Apply one MIN reducer update.
    fn update_min_value(&mut self, value: Value) -> Result<(), InternalError> {
        match self {
            Self::Min(min_value) => {
                let replace = match min_value.as_ref() {
                    Some(current) => canonical_value_compare(&value, current).is_lt(),
                    None => true,
                };
                if replace {
                    *min_value = Some(value);
                }

                Ok(())
            }
            _ => Err(Self::state_mismatch("MIN")),
        }
    }

    // Apply one MAX reducer update.
    fn update_max_value(&mut self, value: Value) -> Result<(), InternalError> {
        match self {
            Self::Max(max_value) => {
                let replace = match max_value.as_ref() {
                    Some(current) => canonical_value_compare(&value, current).is_gt(),
                    None => true,
                };
                if replace {
                    *max_value = Some(value);
                }

                Ok(())
            }
            _ => Err(Self::state_mismatch("MAX")),
        }
    }

    // Apply one FIRST reducer update.
    fn set_first(&mut self, key: StorageKey) -> Result<(), InternalError> {
        match self {
            Self::First(first_key) => {
                *first_key = Some(key.as_value());
                Ok(())
            }
            _ => Err(Self::state_mismatch("FIRST")),
        }
    }

    // Apply one LAST reducer update.
    fn set_last(&mut self, key: StorageKey) -> Result<(), InternalError> {
        match self {
            Self::Last(last_key) => {
                *last_key = Some(key.as_value());
                Ok(())
            }
            _ => Err(Self::state_mismatch("LAST")),
        }
    }

    /// Convert reducer state into the grouped aggregate terminal output value.
    #[must_use]
    fn into_value(self) -> Value {
        match self {
            Self::Count(value) => Value::Uint(u64::from(value)),
            Self::Sum(value) => value.map_or(Value::Null, Value::Decimal),
            Self::Avg { sum, row_count } => {
                average_decimal_terms(sum, row_count).map_or(Value::Null, Value::Decimal)
            }
            Self::Exists(value) => Value::Bool(value),
            Self::Min(value) | Self::Max(value) | Self::First(value) | Self::Last(value) => {
                value.unwrap_or(Value::Null)
            }
        }
    }
}

///
/// ScalarAggregateState
///
/// Canonical scalar aggregate state-machine contract consumed by kernel
/// reducer orchestration. Implementations must keep transitions deterministic
/// and emit scalar terminal outputs using the shared aggregate output taxonomy.
///

pub(in crate::db::executor) trait ScalarAggregateState {
    /// Apply one candidate data key to this aggregate state machine.
    fn apply(&mut self, key: &DataKey) -> Result<FoldControl, InternalError>;

    /// Finalize this aggregate state into one terminal output payload.
    fn finalize(self) -> ScalarAggregateOutput;
}

///
/// ScalarTerminalAggregateState
///
/// ScalarTerminalAggregateState binds one scalar aggregate kind + direction to one
/// reducer state machine so key-stream execution can use a single canonical
/// update pipeline across COUNT/EXISTS/MIN/MAX/FIRST/LAST terminals.
///

pub(in crate::db::executor) struct ScalarTerminalAggregateState {
    reducer_class: AggregateReducerClass,
    direction: Direction,
    distinct: bool,
    distinct_keys: Option<GroupKeySet>,
    requires_storage_key: bool,
    reducer: ScalarAggregateReducerState,
}

impl ScalarAggregateState for ScalarTerminalAggregateState {
    fn apply(&mut self, key: &DataKey) -> Result<FoldControl, InternalError> {
        if self.distinct && !record_distinct_key(self.distinct_keys.as_mut(), key)? {
            return Ok(FoldControl::Continue);
        }

        self.apply_terminal_update(key)
    }

    fn finalize(self) -> ScalarAggregateOutput {
        self.reducer.into_output()
    }
}

///
/// GroupedTerminalAggregateState
///
/// GroupedTerminalAggregateState binds one grouped aggregate kind + direction
/// to one structural reducer state machine so grouped execution no longer
/// depends on entity-typed terminal identity state.
///

pub(in crate::db::executor) struct GroupedTerminalAggregateState {
    kind: AggregateKind,
    reducer_class: AggregateReducerClass,
    direction: Direction,
    distinct: bool,
    max_distinct_values_per_group: u64,
    distinct_keys: Option<GroupKeySet>,
    target_field: Option<FieldSlot>,
    requires_storage_key: bool,
    reducer: GroupedAggregateReducerState,
}

impl GroupedTerminalAggregateState {
    // Build the canonical grouped terminal invariant for field-target-only kinds.
    fn field_target_execution_required(kind: &'static str) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped aggregate reducer {kind} requires field-target execution path"
        ))
    }

    // Build the canonical grouped terminal invariant for storage-key-required updates.
    fn storage_key_required(kind: &'static str) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped aggregate reducer {kind} update requires storage key"
        ))
    }

    // Build the canonical grouped terminal invariant for one non-numeric
    // SUM(field) payload that planner semantics should already have rejected.
    fn sum_field_requires_numeric_value(field: &str, value: &Value) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped aggregate reducer SUM(field) requires numeric field '{field}', found value {value:?}"
        ))
    }

    /// Apply one grouped candidate data key with grouped DISTINCT budget enforcement.
    #[cfg(test)]
    #[expect(
        dead_code,
        reason = "grouped contract tests still exercise the compatibility apply boundary"
    )]
    pub(in crate::db::executor) fn apply(
        &mut self,
        key: &DataKey,
        execution_context: &mut ExecutionContext,
    ) -> Result<FoldControl, GroupError> {
        self.apply_with_row_view(key, None, execution_context)
    }

    /// Apply one grouped candidate data key plus one structural row view when
    /// grouped field-target semantics need slot access.
    pub(in crate::db::executor) fn apply_with_row_view(
        &mut self,
        key: &DataKey,
        row_view: Option<&RowView>,
        execution_context: &mut ExecutionContext,
    ) -> Result<FoldControl, GroupError> {
        if self.distinct {
            let admitted = if self.target_field.is_some()
                && matches!(
                    self.kind,
                    AggregateKind::Count | AggregateKind::Sum | AggregateKind::Avg
                ) {
                self.record_grouped_distinct_target_field_value(row_view, execution_context)?
            } else {
                record_grouped_distinct_key(
                    self.distinct_keys.as_mut(),
                    key,
                    execution_context,
                    self.max_distinct_values_per_group,
                )?
            };
            if !admitted {
                return Ok(FoldControl::Continue);
            }
        }

        self.apply_terminal_update(key, row_view)
            .map_err(GroupError::from)
    }

    /// Finalize this grouped aggregate state into one structural output value.
    #[must_use]
    pub(in crate::db::executor) fn finalize(self) -> Value {
        self.reducer.into_value()
    }

    // Dispatch one grouped terminal aggregate update by kind at one canonical boundary.
    fn apply_terminal_update(
        &mut self,
        key: &DataKey,
        row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        let storage_key = self.requires_storage_key.then_some(key.storage_key());
        match self.reducer_class {
            AggregateReducerClass::Count => self.apply_count(storage_key, row_view),
            AggregateReducerClass::SumLike => self.apply_sum_like(storage_key, row_view),
            AggregateReducerClass::Exists => self.apply_exists(storage_key, row_view),
            AggregateReducerClass::Min => self.apply_min(storage_key, row_view),
            AggregateReducerClass::Max => self.apply_max(storage_key, row_view),
            AggregateReducerClass::First => self.apply_first(storage_key, row_view),
            AggregateReducerClass::Last => self.apply_last(storage_key, row_view),
        }
    }

    // Admit one grouped DISTINCT field-target value before grouped COUNT/SUM/AVG
    // reducers consume it so grouped DISTINCT deduplicates on the projected
    // field value instead of row identity.
    fn record_grouped_distinct_target_field_value(
        &mut self,
        row_view: Option<&RowView>,
        execution_context: &mut ExecutionContext,
    ) -> Result<bool, GroupError> {
        let Some(target_field) = self.target_field.as_ref() else {
            return Err(GroupError::from(Self::field_target_execution_required(
                "COUNT/SUM/AVG(DISTINCT field)",
            )));
        };
        let Some(row_view) = row_view else {
            return Err(GroupError::from(Self::field_target_execution_required(
                "COUNT/SUM/AVG(DISTINCT field)",
            )));
        };
        let value = row_view.require_slot_ref(target_field.index())?;
        if matches!(value, Value::Null) {
            return Ok(false);
        }
        let canonical_key = value
            .canonical_key()
            .map_err(KeyCanonicalError::into_internal_error)
            .map_err(GroupError::from)?;

        let Some(distinct_keys) = self.distinct_keys.as_mut() else {
            return Ok(true);
        };

        execution_context.admit_distinct_key(
            distinct_keys,
            self.max_distinct_values_per_group,
            canonical_key,
        )
    }

    // Apply one COUNT grouped terminal update.
    fn apply_count(
        &mut self,
        _key: Option<StorageKey>,
        row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        if let Some(target_field) = self.target_field.as_ref() {
            let Some(row_view) = row_view else {
                return Err(Self::field_target_execution_required("COUNT(field)"));
            };
            let value = row_view.require_slot_ref(target_field.index())?;
            if matches!(value, Value::Null) {
                return Ok(FoldControl::Continue);
            }
        }
        self.reducer.increment_count()?;

        Ok(FoldControl::Continue)
    }

    // Apply one EXISTS grouped terminal update.
    fn apply_exists(
        &mut self,
        _key: Option<StorageKey>,
        _row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        self.reducer.set_exists_true()?;

        Ok(FoldControl::Break)
    }

    // Apply grouped SUM/AVG field-target reducers through one shared numeric
    // row-view boundary.
    fn apply_sum_like(
        &mut self,
        _key: Option<StorageKey>,
        row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        let kind_label = match self.kind {
            AggregateKind::Sum => "SUM(field)",
            AggregateKind::Avg => "AVG(field)",
            _ => return Err(Self::field_target_execution_required("SUM/AVG(field)")),
        };

        let Some(target_field) = self.target_field.as_ref() else {
            return Err(Self::field_target_execution_required(kind_label));
        };
        let Some(row_view) = row_view else {
            return Err(Self::field_target_execution_required(kind_label));
        };
        let value = row_view.require_slot_ref(target_field.index())?;
        if matches!(value, Value::Null) {
            return Ok(FoldControl::Continue);
        }
        let Some(decimal) = coerce_numeric_decimal(value) else {
            return Err(Self::sum_field_requires_numeric_value(
                target_field.field(),
                value,
            ));
        };
        match self.kind {
            AggregateKind::Sum => self.reducer.add_sum_value(decimal)?,
            AggregateKind::Avg => self.reducer.add_average_value(decimal)?,
            _ => return Err(Self::field_target_execution_required("SUM/AVG(field)")),
        }

        Ok(FoldControl::Continue)
    }

    // Apply one MAX grouped terminal update.
    fn apply_max(
        &mut self,
        key: Option<StorageKey>,
        row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        if let Some(target_field) = self.target_field.as_ref() {
            let Some(target_kind) = target_field.kind() else {
                return Err(Self::field_target_execution_required("MAX(field)"));
            };
            let Some(row_view) = row_view else {
                return Err(Self::field_target_execution_required("MAX(field)"));
            };
            let value = row_view.require_slot_ref(target_field.index())?;
            if matches!(value, Value::Null) {
                return Ok(FoldControl::Continue);
            }
            let aggregate_field_slot = AggregateFieldSlot {
                index: target_field.index(),
                kind: target_kind,
            };
            let replace = match &self.reducer {
                GroupedAggregateReducerState::Max(Some(current)) => {
                    compare_orderable_field_values_with_slot(
                        target_field.field(),
                        aggregate_field_slot,
                        value,
                        current,
                    )
                    .map_err(super::super::field::AggregateFieldValueError::into_internal_error)?
                    .is_gt()
                }
                GroupedAggregateReducerState::Max(None) => true,
                _ => return Err(GroupedAggregateReducerState::state_mismatch("MAX")),
            };
            if replace {
                self.reducer.update_max_value(value.clone())?;
            }
        } else {
            let Some(key) = key else {
                return Err(Self::storage_key_required("MAX"));
            };
            self.reducer.update_max_value(key.as_value())?;
        }

        Ok(if self.direction == Direction::Desc {
            FoldControl::Break
        } else {
            FoldControl::Continue
        })
    }

    // Apply one FIRST grouped terminal update.
    fn apply_first(
        &mut self,
        key: Option<StorageKey>,
        _row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(Self::storage_key_required("FIRST"));
        };
        self.reducer.set_first(key)?;

        Ok(FoldControl::Break)
    }

    // Apply one LAST grouped terminal update.
    fn apply_last(
        &mut self,
        key: Option<StorageKey>,
        _row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(Self::storage_key_required("LAST"));
        };
        self.reducer.set_last(key)?;

        Ok(FoldControl::Continue)
    }

    // Apply one MIN grouped terminal update.
    fn apply_min(
        &mut self,
        key: Option<StorageKey>,
        row_view: Option<&RowView>,
    ) -> Result<FoldControl, InternalError> {
        if let Some(target_field) = self.target_field.as_ref() {
            let Some(target_kind) = target_field.kind() else {
                return Err(Self::field_target_execution_required("MIN(field)"));
            };
            let Some(row_view) = row_view else {
                return Err(Self::field_target_execution_required("MIN(field)"));
            };
            let value = row_view.require_slot_ref(target_field.index())?;
            if matches!(value, Value::Null) {
                return Ok(FoldControl::Continue);
            }
            let aggregate_field_slot = AggregateFieldSlot {
                index: target_field.index(),
                kind: target_kind,
            };
            let replace = match &self.reducer {
                GroupedAggregateReducerState::Min(Some(current)) => {
                    compare_orderable_field_values_with_slot(
                        target_field.field(),
                        aggregate_field_slot,
                        value,
                        current,
                    )
                    .map_err(super::super::field::AggregateFieldValueError::into_internal_error)?
                    .is_lt()
                }
                GroupedAggregateReducerState::Min(None) => true,
                _ => return Err(GroupedAggregateReducerState::state_mismatch("MIN")),
            };
            if replace {
                self.reducer.update_min_value(value.clone())?;
            }
        } else {
            let Some(key) = key else {
                return Err(Self::storage_key_required("MIN"));
            };
            self.reducer.update_min_value(key.as_value())?;
        }

        Ok(if self.direction == Direction::Asc {
            FoldControl::Break
        } else {
            FoldControl::Continue
        })
    }
}

///
/// AggregateStateFactory
///
/// AggregateStateFactory builds canonical scalar and grouped terminal state
/// machines from route-owned kind/direction decisions.
/// This keeps state initialization centralized at one boundary.
///

pub(in crate::db::executor) struct AggregateStateFactory;

impl AggregateStateFactory {
    /// Build one scalar terminal aggregate state machine for kernel reducers.
    #[must_use]
    pub(in crate::db::executor) fn create_scalar_terminal(
        kind: AggregateKind,
        direction: Direction,
        distinct: bool,
    ) -> ScalarTerminalAggregateState {
        ScalarTerminalAggregateState {
            reducer_class: kind.reducer_class(),
            direction,
            distinct,
            distinct_keys: if distinct {
                Some(GroupKeySet::new())
            } else {
                None
            },
            requires_storage_key: kind.requires_decoded_id(),
            reducer: ScalarAggregateReducerState::for_kind(kind),
        }
    }

    /// Build one grouped terminal aggregate state machine for grouped reducers.
    #[must_use]
    pub(in crate::db::executor) fn create_grouped_terminal(
        kind: AggregateKind,
        direction: Direction,
        distinct: bool,
        target_field: Option<FieldSlot>,
        max_distinct_values_per_group: u64,
    ) -> GroupedTerminalAggregateState {
        GroupedTerminalAggregateState {
            kind,
            reducer_class: kind.reducer_class(),
            direction,
            distinct,
            max_distinct_values_per_group,
            distinct_keys: if distinct {
                Some(GroupKeySet::new())
            } else {
                None
            },
            target_field,
            requires_storage_key: kind.requires_decoded_id(),
            reducer: GroupedAggregateReducerState::for_kind(kind),
        }
    }
}

impl ScalarTerminalAggregateState {
    // Build the canonical scalar terminal invariant for field-target-only kinds.
    fn field_target_execution_required(kind: &'static str) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "aggregate reducer {kind} requires field-target execution path"
        ))
    }

    // Build the canonical scalar terminal invariant for storage-key-required updates.
    fn storage_key_required(kind: &'static str) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "aggregate reducer {kind} update requires storage key"
        ))
    }

    // Dispatch one scalar terminal aggregate update by kind at one canonical boundary.
    fn apply_terminal_update(&mut self, key: &DataKey) -> Result<FoldControl, InternalError> {
        let storage_key = self.requires_storage_key.then_some(key.storage_key());
        match self.reducer_class {
            AggregateReducerClass::Count => self.apply_count(storage_key),
            AggregateReducerClass::SumLike => Self::apply_sum_like_unsupported(storage_key),
            AggregateReducerClass::Exists => self.apply_exists(storage_key),
            AggregateReducerClass::Min => self.apply_min(storage_key),
            AggregateReducerClass::Max => self.apply_max(storage_key),
            AggregateReducerClass::First => self.apply_first(storage_key),
            AggregateReducerClass::Last => self.apply_last(storage_key),
        }
    }

    // Apply one COUNT scalar terminal update.
    fn apply_count(&mut self, _key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        self.reducer.increment_count()?;

        Ok(FoldControl::Continue)
    }

    // Apply one EXISTS scalar terminal update.
    fn apply_exists(&mut self, _key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        self.reducer.set_exists_true()?;

        Ok(FoldControl::Break)
    }

    // Reject SUM/AVG through scalar key-based reducer paths.
    fn apply_sum_like_unsupported(_key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        Err(Self::field_target_execution_required("SUM/AVG"))
    }

    // Apply one MAX scalar terminal update.
    fn apply_max(&mut self, key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(Self::storage_key_required("MAX"));
        };
        self.reducer.update_max_value(key)?;

        Ok(if self.direction == Direction::Desc {
            FoldControl::Break
        } else {
            FoldControl::Continue
        })
    }

    // Apply one FIRST scalar terminal update.
    fn apply_first(&mut self, key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(Self::storage_key_required("FIRST"));
        };
        self.reducer.set_first(key)?;

        Ok(FoldControl::Break)
    }

    // Apply one LAST scalar terminal update.
    fn apply_last(&mut self, key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(Self::storage_key_required("LAST"));
        };
        self.reducer.set_last(key)?;

        Ok(FoldControl::Continue)
    }

    // Apply one MIN scalar terminal update.
    fn apply_min(&mut self, key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(Self::storage_key_required("MIN"));
        };
        self.reducer.update_min_value(key)?;

        Ok(if self.direction == Direction::Asc {
            FoldControl::Break
        } else {
            FoldControl::Continue
        })
    }
}

// Record one distinct data-key marker for one aggregate state.
fn record_distinct_key(
    distinct_keys: Option<&mut GroupKeySet>,
    key: &DataKey,
) -> Result<bool, InternalError> {
    let Some(distinct_keys) = distinct_keys else {
        return Ok(true);
    };
    let canonical_key = canonical_key_from_data_key(key)?;

    Ok(distinct_keys.insert_key(canonical_key))
}

// Record one grouped distinct data-key marker and enforce grouped distinct budgets.
fn record_grouped_distinct_key(
    distinct_keys: Option<&mut GroupKeySet>,
    key: &DataKey,
    execution_context: &mut ExecutionContext,
    max_distinct_values_per_group: u64,
) -> Result<bool, GroupError> {
    let Some(distinct_keys) = distinct_keys else {
        return Ok(true);
    };
    let canonical_key = canonical_key_from_data_key(key).map_err(GroupError::from)?;

    execution_context.admit_distinct_key(
        distinct_keys,
        max_distinct_values_per_group,
        canonical_key,
    )
}

// Convert one data key into the canonical grouped DISTINCT key surface.
fn canonical_key_from_data_key(key: &DataKey) -> Result<GroupKey, InternalError> {
    key.storage_key()
        .as_value()
        .canonical_key()
        .map_err(KeyCanonicalError::into_internal_error)
}

///
/// AggregateFoldMode
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum AggregateFoldMode {
    ExistingRows,
    KeysOnly,
}
