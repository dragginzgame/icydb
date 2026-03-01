//! Module: executor::aggregate::materialized_distinct
//! Responsibility: canonical DISTINCT-key admission for non-grouped materialized helpers.
//! Does not own: grouped Class B DISTINCT budget accounting.
//! Boundary: shared helper for materialized effective-window DISTINCT terminals.
//!
//! These helpers are used by scalar materialized terminals
//! (`count_distinct_by`, `distinct_values_by`) and intentionally do not route
//! through grouped `ExecutionContext` budget counters.

use crate::{
    db::executor::group::{GroupKeySet, KeyCanonicalError},
    error::InternalError,
    value::Value,
};

// Admit one materialized DISTINCT candidate value through canonical GroupKey
// equality and return whether it was inserted for the first time.
pub(in crate::db::executor::aggregate) fn insert_materialized_distinct_value(
    distinct_values: &mut GroupKeySet,
    value: &Value,
) -> Result<bool, InternalError> {
    distinct_values
        .insert_value(value)
        .map_err(KeyCanonicalError::into_internal_error)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_materialized_distinct_value_dedups_repeated_values() {
        let mut distinct_values = GroupKeySet::new();

        assert!(
            insert_materialized_distinct_value(&mut distinct_values, &Value::Uint(7))
                .expect("first distinct insertion should succeed"),
            "first value should be inserted",
        );
        assert!(
            !insert_materialized_distinct_value(&mut distinct_values, &Value::Uint(7))
                .expect("duplicate distinct insertion should succeed"),
            "duplicate value should not be inserted twice",
        );
    }
}
