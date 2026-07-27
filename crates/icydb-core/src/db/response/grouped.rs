//! Module: response::grouped
//! Responsibility: grouped paged response payload contracts.
//! Does not own: grouped execution evaluation, route policy, or cursor token protocol.
//! Boundary: grouped DTOs returned by session/query execution APIs.

use crate::value::OutputValue;

///
/// GroupedRow
///
/// One grouped public output row: ordered grouping key values plus ordered
/// aggregate outputs. Group and aggregate vectors preserve query declaration
/// order at the outward API boundary.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupedRow {
    group_key: Vec<OutputValue>,
    aggregate_values: Vec<OutputValue>,
}

impl GroupedRow {
    /// Construct one grouped output row payload.
    #[must_use]
    pub fn new<I, J, K, L>(group_key: I, aggregate_values: J) -> Self
    where
        I: IntoIterator<Item = K>,
        J: IntoIterator<Item = L>,
        K: Into<OutputValue>,
        L: Into<OutputValue>,
    {
        Self {
            group_key: group_key.into_iter().map(Into::into).collect(),
            aggregate_values: aggregate_values.into_iter().map(Into::into).collect(),
        }
    }

    /// Borrow grouped key values.
    #[must_use]
    pub const fn group_key(&self) -> &[OutputValue] {
        self.group_key.as_slice()
    }

    /// Borrow aggregate output values.
    #[must_use]
    pub const fn aggregate_values(&self) -> &[OutputValue] {
        self.aggregate_values.as_slice()
    }
}
