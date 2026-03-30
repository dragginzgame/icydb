//! Module: executor::aggregate::contracts::state
//! Responsibility: scalar aggregate reducer state machines and grouped structural terminal reducers.
//! Does not own: grouped budget/accounting policy.
//! Boundary: state/fold mechanics used by aggregate execution kernels.

use crate::{
    db::{
        data::DataKey,
        direction::Direction,
        executor::{
            aggregate::contracts::{
                error::GroupError,
                grouped::ExecutionContext,
                spec::{AggregateKind, ScalarAggregateOutput},
            },
            group::{CanonicalKey, GroupKey, GroupKeySet, KeyCanonicalError},
        },
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
/// ScalarTerminalUpdateDispatch
///
/// Pre-resolved scalar terminal update dispatch selected once from aggregate
/// kind so scalar reducer loops do not branch on aggregate kind per row.
///

type ScalarTerminalUpdateDispatch =
    fn(&mut ScalarTerminalAggregateState, Option<StorageKey>) -> Result<FoldControl, InternalError>;

///
/// GroupedTerminalUpdateDispatch
///
/// Pre-resolved grouped terminal update dispatch selected once from aggregate
/// kind so grouped reducer loops stay structural and avoid per-row kind checks.
///

type GroupedTerminalUpdateDispatch = fn(
    &mut GroupedTerminalAggregateState,
    Option<StorageKey>,
) -> Result<FoldControl, InternalError>;

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
/// GroupedAggregateReducerState stores grouped terminal reducer payloads using
/// structural `StorageKey` values so grouped execution no longer depends on an
/// entity-typed identity wrapper.
///

enum GroupedAggregateReducerState {
    Count(u32),
    Sum(Option<Decimal>),
    Exists(bool),
    Min(Option<StorageKey>),
    Max(Option<StorageKey>),
    First(Option<StorageKey>),
    Last(Option<StorageKey>),
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

    /// Convert reducer state into the grouped aggregate terminal output value.
    #[must_use]
    fn into_value(self) -> Value {
        match self {
            Self::Count(value) => Value::Uint(u64::from(value)),
            Self::Sum(value) => value.map_or(Value::Null, Value::Decimal),
            Self::Exists(value) => Value::Bool(value),
            Self::Min(value) | Self::Max(value) | Self::First(value) | Self::Last(value) => {
                value.map_or(Value::Null, |key| key.as_value())
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
    direction: Direction,
    distinct: bool,
    distinct_keys: Option<GroupKeySet>,
    requires_storage_key: bool,
    terminal_update_dispatch: ScalarTerminalUpdateDispatch,
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
    direction: Direction,
    distinct: bool,
    max_distinct_values_per_group: u64,
    distinct_keys: Option<GroupKeySet>,
    requires_storage_key: bool,
    terminal_update_dispatch: GroupedTerminalUpdateDispatch,
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

    /// Apply one grouped candidate data key with grouped DISTINCT budget enforcement.
    pub(in crate::db::executor) fn apply(
        &mut self,
        key: &DataKey,
        execution_context: &mut ExecutionContext,
    ) -> Result<FoldControl, GroupError> {
        if self.distinct
            && !record_grouped_distinct_key(
                self.distinct_keys.as_mut(),
                key,
                execution_context,
                self.max_distinct_values_per_group,
            )?
        {
            return Ok(FoldControl::Continue);
        }

        self.apply_terminal_update(key).map_err(GroupError::from)
    }

    /// Finalize this grouped aggregate state into one structural output value.
    #[must_use]
    pub(in crate::db::executor) fn finalize(self) -> Value {
        self.reducer.into_value()
    }

    // Resolve one grouped terminal update dispatch function from one aggregate kind.
    const fn terminal_update_dispatch_for_kind(
        kind: AggregateKind,
    ) -> GroupedTerminalUpdateDispatch {
        match kind.reducer_class() {
            AggregateReducerClass::Count => Self::apply_count,
            AggregateReducerClass::SumLike => Self::apply_sum_like_unsupported,
            AggregateReducerClass::Exists => Self::apply_exists,
            AggregateReducerClass::Min => Self::apply_min,
            AggregateReducerClass::Max => Self::apply_max,
            AggregateReducerClass::First => Self::apply_first,
            AggregateReducerClass::Last => Self::apply_last,
        }
    }

    // Dispatch one grouped terminal aggregate update by kind at one canonical boundary.
    fn apply_terminal_update(&mut self, key: &DataKey) -> Result<FoldControl, InternalError> {
        let storage_key = self.requires_storage_key.then_some(key.storage_key());

        (self.terminal_update_dispatch)(self, storage_key)
    }

    // Apply one COUNT grouped terminal update.
    fn apply_count(&mut self, _key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        self.reducer.increment_count()?;

        Ok(FoldControl::Continue)
    }

    // Apply one EXISTS grouped terminal update.
    fn apply_exists(&mut self, _key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        self.reducer.set_exists_true()?;

        Ok(FoldControl::Break)
    }

    // Reject SUM/AVG through grouped key-based reducer paths.
    fn apply_sum_like_unsupported(
        _state: &mut Self,
        _key: Option<StorageKey>,
    ) -> Result<FoldControl, InternalError> {
        Err(Self::field_target_execution_required("SUM/AVG"))
    }

    // Apply one MAX grouped terminal update.
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

    // Apply one FIRST grouped terminal update.
    fn apply_first(&mut self, key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(Self::storage_key_required("FIRST"));
        };
        self.reducer.set_first(key)?;

        Ok(FoldControl::Break)
    }

    // Apply one LAST grouped terminal update.
    fn apply_last(&mut self, key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(Self::storage_key_required("LAST"));
        };
        self.reducer.set_last(key)?;

        Ok(FoldControl::Continue)
    }

    // Apply one MIN grouped terminal update.
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
    pub(in crate::db::executor) const fn create_scalar_terminal(
        kind: AggregateKind,
        direction: Direction,
        distinct: bool,
    ) -> ScalarTerminalAggregateState {
        ScalarTerminalAggregateState {
            direction,
            distinct,
            distinct_keys: if distinct {
                Some(GroupKeySet::new())
            } else {
                None
            },
            requires_storage_key: kind.requires_decoded_id(),
            terminal_update_dispatch:
                ScalarTerminalAggregateState::terminal_update_dispatch_for_kind(kind),
            reducer: ScalarAggregateReducerState::for_kind(kind),
        }
    }

    /// Build one grouped terminal aggregate state machine for grouped reducers.
    #[must_use]
    pub(in crate::db::executor) const fn create_grouped_terminal(
        kind: AggregateKind,
        direction: Direction,
        distinct: bool,
        max_distinct_values_per_group: u64,
    ) -> GroupedTerminalAggregateState {
        GroupedTerminalAggregateState {
            direction,
            distinct,
            max_distinct_values_per_group,
            distinct_keys: if distinct {
                Some(GroupKeySet::new())
            } else {
                None
            },
            requires_storage_key: kind.requires_decoded_id(),
            terminal_update_dispatch:
                GroupedTerminalAggregateState::terminal_update_dispatch_for_kind(kind),
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

    // Resolve one scalar terminal update dispatch function from one aggregate kind.
    const fn terminal_update_dispatch_for_kind(
        kind: AggregateKind,
    ) -> ScalarTerminalUpdateDispatch {
        match kind.reducer_class() {
            AggregateReducerClass::Count => Self::apply_count,
            AggregateReducerClass::SumLike => Self::apply_sum_like_unsupported,
            AggregateReducerClass::Exists => Self::apply_exists,
            AggregateReducerClass::Min => Self::apply_min,
            AggregateReducerClass::Max => Self::apply_max,
            AggregateReducerClass::First => Self::apply_first,
            AggregateReducerClass::Last => Self::apply_last,
        }
    }

    // Dispatch one scalar terminal aggregate update by kind at one canonical boundary.
    fn apply_terminal_update(&mut self, key: &DataKey) -> Result<FoldControl, InternalError> {
        let storage_key = self.requires_storage_key.then_some(key.storage_key());

        (self.terminal_update_dispatch)(self, storage_key)
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
    fn apply_sum_like_unsupported(
        _state: &mut Self,
        _key: Option<StorageKey>,
    ) -> Result<FoldControl, InternalError> {
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
