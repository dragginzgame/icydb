//! Module: executor::aggregate::contracts::state
//! Responsibility: scalar aggregate reducer state machines and fold control contracts.
//! Does not own: grouped budget/accounting policy.
//! Boundary: state/fold mechanics used by aggregate execution kernels.

use crate::{
    db::{data::DataKey, direction::Direction},
    error::InternalError,
    traits::EntityKind,
    types::Id,
};

use crate::db::executor::aggregate::contracts::spec::{AggregateKind, AggregateOutput};

///
/// FoldControl
///

#[derive(Clone, Copy, Debug)]
pub(in crate::db::executor) enum FoldControl {
    Continue,
    Break,
}

///
/// AggregateReducerState
///
/// Shared aggregate terminal reducer state used by streaming and fast-path
/// aggregate execution so terminal update semantics stay centralized.
///

pub(in crate::db::executor) enum AggregateReducerState<E: EntityKind> {
    Count(u32),
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
            AggregateKind::Exists => Self::Exists(false),
            AggregateKind::Min => Self::Min(None),
            AggregateKind::Max => Self::Max(None),
            AggregateKind::First => Self::First(None),
            AggregateKind::Last => Self::Last(None),
        }
    }

    /// Apply one candidate data key to the reducer and return fold control.
    pub(in crate::db::executor) fn update_from_data_key(
        &mut self,
        kind: AggregateKind,
        direction: Direction,
        key: &DataKey,
    ) -> Result<FoldControl, InternalError> {
        let id = if kind.requires_decoded_id() {
            Some(Id::from_key(key.try_key::<E>()?))
        } else {
            None
        };

        self.update_with_optional_id(kind, direction, id)
    }

    /// Apply one reducer update using an optional decoded id payload.
    pub(in crate::db::executor) fn update_with_optional_id(
        &mut self,
        kind: AggregateKind,
        direction: Direction,
        id: Option<Id<E>>,
    ) -> Result<FoldControl, InternalError> {
        match (kind, self) {
            (AggregateKind::Count, Self::Count(count)) => {
                *count = count.saturating_add(1);
                Ok(FoldControl::Continue)
            }
            (AggregateKind::Exists, Self::Exists(exists)) => {
                *exists = true;
                Ok(FoldControl::Break)
            }
            (AggregateKind::Min, Self::Min(min_id)) => {
                let Some(id) = id else {
                    return Err(InternalError::query_executor_invariant(
                        "aggregate reducer MIN update requires decoded id",
                    ));
                };
                let replace = match min_id.as_ref() {
                    Some(current) => id < *current,
                    None => true,
                };
                if replace {
                    *min_id = Some(id);
                }
                if direction == Direction::Asc {
                    return Ok(FoldControl::Break);
                }

                Ok(FoldControl::Continue)
            }
            (AggregateKind::Max, Self::Max(max_id)) => {
                let Some(id) = id else {
                    return Err(InternalError::query_executor_invariant(
                        "aggregate reducer MAX update requires decoded id",
                    ));
                };
                let replace = match max_id.as_ref() {
                    Some(current) => id > *current,
                    None => true,
                };
                if replace {
                    *max_id = Some(id);
                }
                if direction == Direction::Desc {
                    return Ok(FoldControl::Break);
                }

                Ok(FoldControl::Continue)
            }
            (AggregateKind::First, Self::First(first_id)) => {
                let Some(id) = id else {
                    return Err(InternalError::query_executor_invariant(
                        "aggregate reducer FIRST update requires decoded id",
                    ));
                };
                *first_id = Some(id);
                Ok(FoldControl::Break)
            }
            (AggregateKind::Last, Self::Last(last_id)) => {
                let Some(id) = id else {
                    return Err(InternalError::query_executor_invariant(
                        "aggregate reducer LAST update requires decoded id",
                    ));
                };
                *last_id = Some(id);
                Ok(FoldControl::Continue)
            }
            _ => Err(InternalError::query_executor_invariant(
                "aggregate reducer state/kind mismatch",
            )),
        }
    }

    /// Convert reducer state into the aggregate terminal output payload.
    #[must_use]
    pub(in crate::db::executor) const fn into_output(self) -> AggregateOutput<E> {
        match self {
            Self::Count(value) => AggregateOutput::Count(value),
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
    kind: AggregateKind,
    direction: Direction,
    reducer: AggregateReducerState<E>,
}

impl<E: EntityKind> AggregateState<E> for TerminalAggregateState<E> {
    fn apply(&mut self, key: &DataKey) -> Result<FoldControl, InternalError> {
        self.reducer
            .update_from_data_key(self.kind, self.direction, key)
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
    ) -> TerminalAggregateState<E> {
        TerminalAggregateState {
            kind,
            direction,
            reducer: AggregateReducerState::for_kind(kind),
        }
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
