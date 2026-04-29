use crate::{
    db::{
        data::DataKey,
        direction::Direction,
        executor::{
            aggregate::contracts::{
                spec::{ScalarAggregateOutput, ScalarTerminalKind},
                state::{
                    ExtremumKind, FoldControl, ScalarAggregateReducerState, record_distinct_key,
                },
            },
            group::GroupKeySet,
        },
    },
    error::InternalError,
    value::StorageKey,
};

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
/// ScalarTerminalAggregateState binds one scalar aggregate kind + direction to
/// one reducer state machine so key-stream execution can use a single canonical
/// update pipeline across COUNT/EXISTS/MIN/MAX/FIRST/LAST terminals.
///

pub(in crate::db::executor) struct ScalarTerminalAggregateState {
    pub(in crate::db::executor::aggregate::contracts::state) kind: ScalarTerminalKind,
    pub(in crate::db::executor::aggregate::contracts::state) direction: Direction,
    pub(in crate::db::executor::aggregate::contracts::state) distinct: bool,
    pub(in crate::db::executor::aggregate::contracts::state) distinct_keys: Option<GroupKeySet>,
    pub(in crate::db::executor::aggregate::contracts::state) requires_storage_key: bool,
    pub(in crate::db::executor::aggregate::contracts::state) reducer: ScalarAggregateReducerState,
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

impl ScalarTerminalAggregateState {
    // Build the canonical scalar terminal invariant for storage-key-required updates.
    fn storage_key_required(kind: &'static str) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "aggregate reducer {kind} update requires storage key"
        ))
    }

    // Dispatch one scalar terminal aggregate update by kind at one canonical boundary.
    fn apply_terminal_update(&mut self, key: &DataKey) -> Result<FoldControl, InternalError> {
        let storage_key = self.requires_storage_key.then_some(key.storage_key());
        match self.kind {
            ScalarTerminalKind::Count => self.apply_count(storage_key),
            ScalarTerminalKind::Exists => self.apply_exists(storage_key),
            ScalarTerminalKind::Min => self.apply_extremum(ExtremumKind::Min, storage_key),
            ScalarTerminalKind::Max => self.apply_extremum(ExtremumKind::Max, storage_key),
            ScalarTerminalKind::First => self.apply_first(storage_key),
            ScalarTerminalKind::Last => self.apply_last(storage_key),
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

    // Apply one MIN/MAX scalar terminal update through the shared extrema
    // storage-key path.
    fn apply_extremum(
        &mut self,
        kind: ExtremumKind,
        key: Option<StorageKey>,
    ) -> Result<FoldControl, InternalError> {
        let Some(key) = key else {
            return Err(Self::storage_key_required(kind.storage_key_label()));
        };
        match kind {
            ExtremumKind::Min => self.reducer.update_min_value(key)?,
            ExtremumKind::Max => self.reducer.update_max_value(key)?,
        }

        Ok(kind.fold_control_for_direction(self.direction))
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
}
