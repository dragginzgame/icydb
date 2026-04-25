//! Module: executor::aggregate::runtime::grouped_row
//! Responsibility: grouped runtime row carrier for executor-owned aggregate output.
//! Does not own: public grouped response materialization or grouped route planning.
//! Boundary: keeps runtime `Value` rows inside executor until the session boundary
//! converts them into public grouped response DTOs.

use crate::value::Value;

///
/// RuntimeGroupedRow
///
/// Internal grouped runtime row carrier with ordered key and aggregate values.
/// Executor grouped paths use this DTO while values are still runtime `Value`s,
/// then the session boundary converts it into the public grouped row shape.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct RuntimeGroupedRow {
    group_key: Vec<Value>,
    aggregate_values: Vec<Value>,
}

impl RuntimeGroupedRow {
    /// Construct one grouped runtime row payload.
    #[must_use]
    pub(in crate::db) const fn new(group_key: Vec<Value>, aggregate_values: Vec<Value>) -> Self {
        Self {
            group_key,
            aggregate_values,
        }
    }

    /// Borrow grouped runtime key values.
    #[must_use]
    pub(in crate::db) const fn group_key(&self) -> &[Value] {
        self.group_key.as_slice()
    }

    /// Borrow grouped runtime aggregate values.
    #[must_use]
    pub(in crate::db) const fn aggregate_values(&self) -> &[Value] {
        self.aggregate_values.as_slice()
    }

    /// Consume this runtime row into its grouped key and aggregate values.
    #[must_use]
    pub(in crate::db) fn into_parts(self) -> (Vec<Value>, Vec<Value>) {
        (self.group_key, self.aggregate_values)
    }
}
