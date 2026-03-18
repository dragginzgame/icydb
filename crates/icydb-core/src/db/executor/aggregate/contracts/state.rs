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
                spec::{AggregateKind, AggregateOutput},
            },
            group::{CanonicalKey, GroupKey, GroupKeySet, KeyCanonicalError},
        },
    },
    error::InternalError,
    traits::EntityKind,
    types::{Decimal, Id},
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
/// TerminalUpdateDispatch
///
/// Pre-resolved scalar terminal update dispatch selected once from aggregate
/// kind so scalar reducer loops do not branch on aggregate kind per row.
///

type TerminalUpdateDispatch<E> =
    fn(&mut TerminalAggregateState<E>, Option<Id<E>>) -> Result<FoldControl, InternalError>;

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
/// AggregateReducerState
///
/// Shared scalar aggregate terminal reducer state used by streaming and
/// fast-path aggregate execution so scalar terminal update semantics stay
/// centralized.
///

pub(in crate::db::executor) enum AggregateReducerState<E: EntityKind> {
    Count(u32),
    Sum(Option<Decimal>),
    Exists(bool),
    Min(Option<Id<E>>),
    Max(Option<Id<E>>),
    First(Option<Id<E>>),
    Last(Option<Id<E>>),
}

impl<E: EntityKind> AggregateReducerState<E> {
    /// Build the initial scalar reducer state for one aggregate terminal.
    #[must_use]
    pub(in crate::db::executor) const fn for_kind(kind: AggregateKind) -> Self {
        match kind {
            AggregateKind::Count => Self::Count(0),
            AggregateKind::Sum | AggregateKind::Avg => Self::Sum(None),
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
            _ => Err(crate::db::error::query_executor_invariant(
                "aggregate reducer COUNT state mismatch",
            )),
        }
    }

    // Apply one EXISTS reducer update.
    fn set_exists_true(&mut self) -> Result<(), InternalError> {
        match self {
            Self::Exists(exists) => {
                *exists = true;
                Ok(())
            }
            _ => Err(crate::db::error::query_executor_invariant(
                "aggregate reducer EXISTS state mismatch",
            )),
        }
    }

    // Apply one MIN reducer update.
    fn update_min_value(&mut self, id: Id<E>) -> Result<(), InternalError> {
        match self {
            Self::Min(min_id) => {
                let replace = match min_id.as_ref() {
                    Some(current) => id < *current,
                    None => true,
                };
                if replace {
                    *min_id = Some(id);
                }

                Ok(())
            }
            _ => Err(crate::db::error::query_executor_invariant(
                "aggregate reducer MIN state mismatch",
            )),
        }
    }

    // Apply one MAX reducer update.
    fn update_max_value(&mut self, id: Id<E>) -> Result<(), InternalError> {
        match self {
            Self::Max(max_id) => {
                let replace = match max_id.as_ref() {
                    Some(current) => id > *current,
                    None => true,
                };
                if replace {
                    *max_id = Some(id);
                }

                Ok(())
            }
            _ => Err(crate::db::error::query_executor_invariant(
                "aggregate reducer MAX state mismatch",
            )),
        }
    }

    // Apply one FIRST reducer update.
    fn set_first(&mut self, id: Id<E>) -> Result<(), InternalError> {
        match self {
            Self::First(first_id) => {
                *first_id = Some(id);
                Ok(())
            }
            _ => Err(crate::db::error::query_executor_invariant(
                "aggregate reducer FIRST state mismatch",
            )),
        }
    }

    // Apply one LAST reducer update.
    fn set_last(&mut self, id: Id<E>) -> Result<(), InternalError> {
        match self {
            Self::Last(last_id) => {
                *last_id = Some(id);
                Ok(())
            }
            _ => Err(crate::db::error::query_executor_invariant(
                "aggregate reducer LAST state mismatch",
            )),
        }
    }

    /// Convert reducer state into the scalar aggregate terminal output payload.
    #[must_use]
    pub(in crate::db::executor) const fn into_output(self) -> AggregateOutput<E> {
        match self {
            Self::Count(value) => AggregateOutput::Count(value),
            Self::Sum(value) => AggregateOutput::Sum(value),
            Self::Exists(value) => AggregateOutput::Exists(value),
            Self::Min(value) => AggregateOutput::Min(value),
            Self::Max(value) => AggregateOutput::Max(value),
            Self::First(value) => AggregateOutput::First(value),
            Self::Last(value) => AggregateOutput::Last(value),
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
    /// Build the initial grouped reducer state for one aggregate terminal.
    #[must_use]
    const fn for_kind(kind: AggregateKind) -> Self {
        match kind {
            AggregateKind::Count => Self::Count(0),
            AggregateKind::Sum | AggregateKind::Avg => Self::Sum(None),
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
            _ => Err(crate::db::error::query_executor_invariant(
                "grouped aggregate reducer COUNT state mismatch",
            )),
        }
    }

    // Apply one EXISTS reducer update.
    fn set_exists_true(&mut self) -> Result<(), InternalError> {
        match self {
            Self::Exists(exists) => {
                *exists = true;
                Ok(())
            }
            _ => Err(crate::db::error::query_executor_invariant(
                "grouped aggregate reducer EXISTS state mismatch",
            )),
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
            _ => Err(crate::db::error::query_executor_invariant(
                "grouped aggregate reducer MIN state mismatch",
            )),
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
            _ => Err(crate::db::error::query_executor_invariant(
                "grouped aggregate reducer MAX state mismatch",
            )),
        }
    }

    // Apply one FIRST reducer update.
    fn set_first(&mut self, key: StorageKey) -> Result<(), InternalError> {
        match self {
            Self::First(first_key) => {
                *first_key = Some(key);
                Ok(())
            }
            _ => Err(crate::db::error::query_executor_invariant(
                "grouped aggregate reducer FIRST state mismatch",
            )),
        }
    }

    // Apply one LAST reducer update.
    fn set_last(&mut self, key: StorageKey) -> Result<(), InternalError> {
        match self {
            Self::Last(last_key) => {
                *last_key = Some(key);
                Ok(())
            }
            _ => Err(crate::db::error::query_executor_invariant(
                "grouped aggregate reducer LAST state mismatch",
            )),
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
/// AggregateState
///
/// Canonical scalar aggregate state-machine contract consumed by kernel
/// reducer orchestration. Implementations must keep transitions deterministic
/// and emit scalar terminal outputs using the shared aggregate output taxonomy.
///

pub(in crate::db::executor) trait AggregateState<E: EntityKind> {
    /// Apply one candidate data key to this aggregate state machine.
    fn apply(&mut self, key: &DataKey) -> Result<FoldControl, InternalError>;

    /// Finalize this aggregate state into one terminal output payload.
    fn finalize(self) -> AggregateOutput<E>;
}

///
/// TerminalAggregateState
///
/// TerminalAggregateState binds one scalar aggregate kind + direction to one
/// reducer state machine so key-stream execution can use a single canonical
/// update pipeline across COUNT/EXISTS/MIN/MAX/FIRST/LAST terminals.
///

pub(in crate::db::executor) struct TerminalAggregateState<E: EntityKind> {
    direction: Direction,
    distinct: bool,
    distinct_keys: Option<GroupKeySet>,
    requires_decoded_id: bool,
    terminal_update_dispatch: TerminalUpdateDispatch<E>,
    reducer: AggregateReducerState<E>,
}

impl<E: EntityKind> AggregateState<E> for TerminalAggregateState<E> {
    fn apply(&mut self, key: &DataKey) -> Result<FoldControl, InternalError> {
        if self.distinct && !record_distinct_key(self.distinct_keys.as_mut(), key)? {
            return Ok(FoldControl::Continue);
        }

        self.apply_terminal_update(key)
    }

    fn finalize(self) -> AggregateOutput<E> {
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
        match kind {
            AggregateKind::Count => Self::apply_count,
            AggregateKind::Sum => Self::apply_sum_unsupported,
            AggregateKind::Exists => Self::apply_exists,
            AggregateKind::Min => Self::apply_min,
            AggregateKind::Max => Self::apply_max,
            AggregateKind::First => Self::apply_first,
            AggregateKind::Last => Self::apply_last,
            AggregateKind::Avg => Self::apply_avg_unsupported,
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

    // Reject SUM through grouped key-based reducer paths.
    fn apply_sum_unsupported(
        _state: &mut Self,
        _key: Option<StorageKey>,
    ) -> Result<FoldControl, InternalError> {
        Err(crate::db::error::query_executor_invariant(
            "grouped aggregate reducer SUM requires field-target execution path",
        ))
    }

    // Apply one MIN grouped terminal update.
    fn apply_min(&mut self, key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(crate::db::error::query_executor_invariant(
                "grouped aggregate reducer MIN update requires storage key",
            ));
        };
        self.reducer.update_min_value(key)?;

        Ok(if self.direction == Direction::Asc {
            FoldControl::Break
        } else {
            FoldControl::Continue
        })
    }

    // Apply one MAX grouped terminal update.
    fn apply_max(&mut self, key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(crate::db::error::query_executor_invariant(
                "grouped aggregate reducer MAX update requires storage key",
            ));
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
            return Err(crate::db::error::query_executor_invariant(
                "grouped aggregate reducer FIRST update requires storage key",
            ));
        };
        self.reducer.set_first(key)?;

        Ok(FoldControl::Break)
    }

    // Apply one LAST grouped terminal update.
    fn apply_last(&mut self, key: Option<StorageKey>) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(crate::db::error::query_executor_invariant(
                "grouped aggregate reducer LAST update requires storage key",
            ));
        };
        self.reducer.set_last(key)?;

        Ok(FoldControl::Continue)
    }

    // Reject AVG through grouped key-based reducer paths.
    fn apply_avg_unsupported(
        _state: &mut Self,
        _key: Option<StorageKey>,
    ) -> Result<FoldControl, InternalError> {
        Err(crate::db::error::query_executor_invariant(
            "grouped aggregate reducer AVG requires field-target execution path",
        ))
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
    pub(in crate::db::executor) const fn create_terminal<E: EntityKind>(
        kind: AggregateKind,
        direction: Direction,
        distinct: bool,
    ) -> TerminalAggregateState<E> {
        TerminalAggregateState {
            direction,
            distinct,
            distinct_keys: if distinct {
                Some(GroupKeySet::new())
            } else {
                None
            },
            requires_decoded_id: kind.requires_decoded_id(),
            terminal_update_dispatch:
                TerminalAggregateState::<E>::terminal_update_dispatch_for_kind(kind),
            reducer: AggregateReducerState::for_kind(kind),
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

impl<E: EntityKind> TerminalAggregateState<E> {
    // Resolve one scalar terminal update dispatch function from one aggregate kind.
    const fn terminal_update_dispatch_for_kind(kind: AggregateKind) -> TerminalUpdateDispatch<E> {
        match kind {
            AggregateKind::Count => Self::apply_count,
            AggregateKind::Sum => Self::apply_sum_unsupported,
            AggregateKind::Exists => Self::apply_exists,
            AggregateKind::Min => Self::apply_min,
            AggregateKind::Max => Self::apply_max,
            AggregateKind::First => Self::apply_first,
            AggregateKind::Last => Self::apply_last,
            AggregateKind::Avg => Self::apply_avg_unsupported,
        }
    }

    // Dispatch one scalar terminal aggregate update by kind at one canonical boundary.
    fn apply_terminal_update(&mut self, key: &DataKey) -> Result<FoldControl, InternalError> {
        let id = if self.requires_decoded_id {
            Some(Id::from_key(key.try_key::<E>()?))
        } else {
            None
        };

        (self.terminal_update_dispatch)(self, id)
    }

    // Apply one COUNT scalar terminal update.
    fn apply_count(&mut self, _id: Option<Id<E>>) -> Result<FoldControl, InternalError> {
        self.reducer.increment_count()?;

        Ok(FoldControl::Continue)
    }

    // Apply one EXISTS scalar terminal update.
    fn apply_exists(&mut self, _id: Option<Id<E>>) -> Result<FoldControl, InternalError> {
        self.reducer.set_exists_true()?;

        Ok(FoldControl::Break)
    }

    // Reject SUM through scalar key-based reducer paths.
    fn apply_sum_unsupported(
        _state: &mut Self,
        _id: Option<Id<E>>,
    ) -> Result<FoldControl, InternalError> {
        Err(crate::db::error::query_executor_invariant(
            "aggregate reducer SUM requires field-target execution path",
        ))
    }

    // Apply one MIN scalar terminal update.
    fn apply_min(&mut self, id: Option<Id<E>>) -> Result<FoldControl, InternalError> {
        let Some(id) = id else {
            return Err(crate::db::error::query_executor_invariant(
                "aggregate reducer MIN update requires decoded id",
            ));
        };
        self.reducer.update_min_value(id)?;

        Ok(if self.direction == Direction::Asc {
            FoldControl::Break
        } else {
            FoldControl::Continue
        })
    }

    // Apply one MAX scalar terminal update.
    fn apply_max(&mut self, id: Option<Id<E>>) -> Result<FoldControl, InternalError> {
        let Some(id) = id else {
            return Err(crate::db::error::query_executor_invariant(
                "aggregate reducer MAX update requires decoded id",
            ));
        };
        self.reducer.update_max_value(id)?;

        Ok(if self.direction == Direction::Desc {
            FoldControl::Break
        } else {
            FoldControl::Continue
        })
    }

    // Apply one FIRST scalar terminal update.
    fn apply_first(&mut self, id: Option<Id<E>>) -> Result<FoldControl, InternalError> {
        let Some(id) = id else {
            return Err(crate::db::error::query_executor_invariant(
                "aggregate reducer FIRST update requires decoded id",
            ));
        };
        self.reducer.set_first(id)?;

        Ok(FoldControl::Break)
    }

    // Apply one LAST scalar terminal update.
    fn apply_last(&mut self, id: Option<Id<E>>) -> Result<FoldControl, InternalError> {
        let Some(id) = id else {
            return Err(crate::db::error::query_executor_invariant(
                "aggregate reducer LAST update requires decoded id",
            ));
        };
        self.reducer.set_last(id)?;

        Ok(FoldControl::Continue)
    }

    // Reject AVG through scalar key-based reducer paths.
    fn apply_avg_unsupported(
        _state: &mut Self,
        _id: Option<Id<E>>,
    ) -> Result<FoldControl, InternalError> {
        Err(crate::db::error::query_executor_invariant(
            "aggregate reducer AVG requires field-target execution path",
        ))
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
