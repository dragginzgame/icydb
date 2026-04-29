use crate::{
    db::{
        data::DataKey,
        executor::group::{CanonicalKey, GroupKey, GroupKeySet, KeyCanonicalError},
    },
    error::InternalError,
    value::storage_key_as_runtime_value,
};

///
/// GroupedDistinctExecutionMode
///
/// GroupedDistinctExecutionMode carries the planner-prepared grouped DISTINCT
/// facts into reducer state.
/// It prevents reducer execution from reinterpreting aggregate kind while still
/// keeping key-based and value-based DISTINCT admission explicit.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct GroupedDistinctExecutionMode {
    enabled: bool,
    uses_value_dedup: bool,
}

impl GroupedDistinctExecutionMode {
    /// Build one prepared grouped DISTINCT execution mode.
    #[must_use]
    pub(in crate::db::executor) const fn new(enabled: bool, uses_value_dedup: bool) -> Self {
        Self {
            enabled,
            uses_value_dedup,
        }
    }

    /// Return whether grouped DISTINCT admission is enabled.
    #[must_use]
    pub(in crate::db::executor::aggregate::contracts::state) const fn enabled(self) -> bool {
        self.enabled
    }

    /// Return whether grouped DISTINCT admission deduplicates by input value.
    #[must_use]
    pub(in crate::db::executor::aggregate::contracts::state) const fn uses_value_dedup(
        self,
    ) -> bool {
        self.uses_value_dedup
    }
}

// Record one distinct data-key marker for one aggregate state.
pub(in crate::db::executor::aggregate::contracts::state) fn record_distinct_key(
    distinct_keys: Option<&mut GroupKeySet>,
    key: &DataKey,
) -> Result<bool, InternalError> {
    let Some(distinct_keys) = distinct_keys else {
        return Ok(true);
    };
    let canonical_key = canonical_key_from_data_key(key)?;

    Ok(distinct_keys.insert_key(canonical_key))
}

// Convert one data key into the canonical grouped DISTINCT key surface.
pub(in crate::db::executor::aggregate::contracts::state) fn canonical_key_from_data_key(
    key: &DataKey,
) -> Result<GroupKey, InternalError> {
    storage_key_as_runtime_value(&key.storage_key())
        .canonical_key()
        .map_err(KeyCanonicalError::into_internal_error)
}
