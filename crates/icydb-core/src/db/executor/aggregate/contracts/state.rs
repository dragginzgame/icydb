//! Module: executor::aggregate::contracts::state
//! Responsibility: scalar aggregate reducer state machines and fold control contracts.
//! Does not own: grouped budget/accounting policy.
//! Boundary: state/fold mechanics used by aggregate execution kernels.

use crate::{
    db::{
        data::DataKey,
        direction::Direction,
        executor::group::{CanonicalKey, GroupKeySet, KeyCanonicalError},
    },
    error::InternalError,
    traits::EntityKind,
    types::{Decimal, Id},
};

use crate::db::executor::aggregate::contracts::{
    error::GroupError,
    grouped::ExecutionContext,
    spec::{AggregateKind, AggregateOutput},
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
/// AggregateTerminalMode
///
/// Runtime lane mode for terminal aggregate reducer updates.
/// Scalar and grouped execution both dispatch through this mode boundary.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AggregateTerminalMode {
    Scalar,
    Grouped,
}

///
/// TerminalUpdateDispatch
///
/// Pre-resolved terminal update dispatch selected once from aggregate kind.
/// This removes per-row aggregate-kind matching in hot reducer loops.
///

type TerminalUpdateDispatch<E> = fn(
    &mut TerminalAggregateState<E>,
    AggregateTerminalMode,
    Option<Id<E>>,
) -> Result<FoldControl, InternalError>;

///
/// AggregateReducerState
///
/// Shared aggregate terminal reducer state used by streaming and fast-path
/// aggregate execution so terminal update semantics stay centralized.
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
    /// Build the initial reducer state for one aggregate terminal.
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

    /// Convert reducer state into the aggregate terminal output payload.
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
/// AggregateState
///
/// Canonical aggregate state-machine contract consumed by kernel reducer
/// orchestration. Implementations must keep transitions deterministic and
/// emit terminal outputs using the shared aggregate output taxonomy.
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
/// TerminalAggregateState binds one aggregate kind + direction to one reducer
/// state machine so key-stream execution can use a single canonical update
/// pipeline across COUNT/EXISTS/MIN/MAX/FIRST/LAST terminals.
///

pub(in crate::db::executor) struct TerminalAggregateState<E: EntityKind> {
    direction: Direction,
    distinct: bool,
    max_distinct_values_per_group: u64,
    distinct_keys: Option<GroupKeySet>,
    requires_decoded_id: bool,
    terminal_update_dispatch: TerminalUpdateDispatch<E>,
    reducer: AggregateReducerState<E>,
}

impl<E: EntityKind> AggregateState<E> for TerminalAggregateState<E> {
    fn apply(&mut self, key: &DataKey) -> Result<FoldControl, InternalError> {
        if self.distinct && !self.record_distinct_key(key)? {
            return Ok(FoldControl::Continue);
        }

        self.apply_terminal_update_for_mode(key, AggregateTerminalMode::Scalar)
    }

    fn finalize(self) -> AggregateOutput<E> {
        self.reducer.into_output()
    }
}

///
/// AggregateStateFactory
///
/// AggregateStateFactory builds canonical terminal aggregate state machines
/// from route-owned kind/direction decisions.
/// This keeps state initialization centralized at one boundary.
///

pub(in crate::db::executor) struct AggregateStateFactory;

impl AggregateStateFactory {
    /// Build one terminal aggregate state machine for kernel reducers.
    #[must_use]
    pub(in crate::db::executor) const fn create_terminal<E: EntityKind>(
        kind: AggregateKind,
        direction: Direction,
        distinct: bool,
        max_distinct_values_per_group: u64,
    ) -> TerminalAggregateState<E> {
        TerminalAggregateState {
            direction,
            distinct,
            max_distinct_values_per_group,
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
}

impl<E: EntityKind> TerminalAggregateState<E> {
    // Resolve one terminal update dispatch function from one aggregate kind.
    const fn terminal_update_dispatch_for_kind(kind: AggregateKind) -> TerminalUpdateDispatch<E> {
        match kind {
            AggregateKind::Count => Self::apply_count_for_mode,
            AggregateKind::Sum => Self::apply_sum_unsupported_for_mode,
            AggregateKind::Exists => Self::apply_exists_for_mode,
            AggregateKind::Min => Self::apply_min_for_mode,
            AggregateKind::Max => Self::apply_max_for_mode,
            AggregateKind::First => Self::apply_first_for_mode,
            AggregateKind::Last => Self::apply_last_for_mode,
            AggregateKind::Avg => Self::apply_avg_unsupported_for_mode,
        }
    }

    // Dispatch one terminal aggregate update by kind at one canonical boundary.
    fn apply_terminal_update_for_mode(
        &mut self,
        key: &DataKey,
        mode: AggregateTerminalMode,
    ) -> Result<FoldControl, InternalError> {
        let id = if self.requires_decoded_id {
            Some(Id::from_key(key.try_key::<E>()?))
        } else {
            None
        };

        (self.terminal_update_dispatch)(self, mode, id)
    }

    // Apply one COUNT terminal update for one execution mode.
    fn apply_count_for_mode(
        &mut self,
        mode: AggregateTerminalMode,
        _id: Option<Id<E>>,
    ) -> Result<FoldControl, InternalError> {
        self.reducer.increment_count()?;

        Ok(match mode {
            AggregateTerminalMode::Scalar | AggregateTerminalMode::Grouped => FoldControl::Continue,
        })
    }

    // Apply one EXISTS terminal update for one execution mode.
    fn apply_exists_for_mode(
        &mut self,
        mode: AggregateTerminalMode,
        _id: Option<Id<E>>,
    ) -> Result<FoldControl, InternalError> {
        self.reducer.set_exists_true()?;

        Ok(match mode {
            AggregateTerminalMode::Scalar | AggregateTerminalMode::Grouped => FoldControl::Break,
        })
    }

    // Reject SUM through key-based reducer paths.
    fn apply_sum_unsupported_for_mode(
        _state: &mut Self,
        _mode: AggregateTerminalMode,
        _id: Option<Id<E>>,
    ) -> Result<FoldControl, InternalError> {
        Err(crate::db::error::query_executor_invariant(
            "aggregate reducer SUM requires field-target execution path",
        ))
    }

    // Apply one MIN terminal update for one execution mode.
    fn apply_min_for_mode(
        &mut self,
        mode: AggregateTerminalMode,
        id: Option<Id<E>>,
    ) -> Result<FoldControl, InternalError> {
        let Some(id) = id else {
            return Err(crate::db::error::query_executor_invariant(
                "aggregate reducer MIN update requires decoded id",
            ));
        };
        self.reducer.update_min_value(id)?;

        Ok(match mode {
            AggregateTerminalMode::Scalar | AggregateTerminalMode::Grouped => {
                if self.direction == Direction::Asc {
                    FoldControl::Break
                } else {
                    FoldControl::Continue
                }
            }
        })
    }

    // Apply one MAX terminal update for one execution mode.
    fn apply_max_for_mode(
        &mut self,
        mode: AggregateTerminalMode,
        id: Option<Id<E>>,
    ) -> Result<FoldControl, InternalError> {
        let Some(id) = id else {
            return Err(crate::db::error::query_executor_invariant(
                "aggregate reducer MAX update requires decoded id",
            ));
        };
        self.reducer.update_max_value(id)?;

        Ok(match mode {
            AggregateTerminalMode::Scalar | AggregateTerminalMode::Grouped => {
                if self.direction == Direction::Desc {
                    FoldControl::Break
                } else {
                    FoldControl::Continue
                }
            }
        })
    }

    // Apply one FIRST terminal update for one execution mode.
    fn apply_first_for_mode(
        &mut self,
        mode: AggregateTerminalMode,
        id: Option<Id<E>>,
    ) -> Result<FoldControl, InternalError> {
        let Some(id) = id else {
            return Err(crate::db::error::query_executor_invariant(
                "aggregate reducer FIRST update requires decoded id",
            ));
        };
        self.reducer.set_first(id)?;

        Ok(match mode {
            AggregateTerminalMode::Scalar | AggregateTerminalMode::Grouped => FoldControl::Break,
        })
    }

    // Apply one LAST terminal update for one execution mode.
    fn apply_last_for_mode(
        &mut self,
        mode: AggregateTerminalMode,
        id: Option<Id<E>>,
    ) -> Result<FoldControl, InternalError> {
        let Some(id) = id else {
            return Err(crate::db::error::query_executor_invariant(
                "aggregate reducer LAST update requires decoded id",
            ));
        };
        self.reducer.set_last(id)?;

        Ok(match mode {
            AggregateTerminalMode::Scalar | AggregateTerminalMode::Grouped => FoldControl::Continue,
        })
    }

    // Reject AVG through key-based reducer paths.
    fn apply_avg_unsupported_for_mode(
        _state: &mut Self,
        _mode: AggregateTerminalMode,
        _id: Option<Id<E>>,
    ) -> Result<FoldControl, InternalError> {
        Err(crate::db::error::query_executor_invariant(
            "aggregate reducer AVG requires field-target execution path",
        ))
    }

    /// Apply one grouped candidate data key with grouped DISTINCT budget enforcement.
    pub(in crate::db::executor) fn apply_grouped(
        &mut self,
        key: &DataKey,
        execution_context: &mut ExecutionContext,
    ) -> Result<FoldControl, GroupError> {
        if self.distinct && !self.record_distinct_key_grouped(key, execution_context)? {
            return Ok(FoldControl::Continue);
        }

        self.apply_terminal_update_for_mode(key, AggregateTerminalMode::Grouped)
            .map_err(GroupError::from)
    }

    // Record one distinct data-key marker for this aggregate state.
    //
    // DISTINCT v1 for grouped id terminals deduplicates by canonical primary-key value
    // before reducer update so fold output is deterministic under duplicate-key inputs.
    fn record_distinct_key(&mut self, key: &DataKey) -> Result<bool, InternalError> {
        let Some(distinct_keys) = self.distinct_keys.as_mut() else {
            return Ok(true);
        };
        let key_value = key.storage_key().as_value();
        let canonical_key = key_value
            .canonical_key()
            .map_err(KeyCanonicalError::into_internal_error)?;

        Ok(distinct_keys.insert_key(canonical_key))
    }

    // Record one grouped distinct data-key marker and enforce grouped distinct budgets.
    fn record_distinct_key_grouped(
        &mut self,
        key: &DataKey,
        execution_context: &mut ExecutionContext,
    ) -> Result<bool, GroupError> {
        let Some(distinct_keys) = self.distinct_keys.as_mut() else {
            return Ok(true);
        };
        let key_value = key.storage_key().as_value();
        let canonical_key = key_value
            .canonical_key()
            .map_err(KeyCanonicalError::into_internal_error)
            .map_err(GroupError::from)?;
        execution_context.admit_distinct_key(
            distinct_keys,
            self.max_distinct_values_per_group,
            canonical_key,
        )
    }
}

///
/// AggregateFoldMode
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum AggregateFoldMode {
    ExistingRows,
    KeysOnly,
}
