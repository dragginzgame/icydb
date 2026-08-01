//! Module: response::grouped
//! Responsibility: grouped paged response payload contracts.
//! Does not own: grouped execution evaluation, route policy, or cursor token protocol.
//! Boundary: grouped DTOs returned by session/query execution APIs.

use crate::value::OutputValue;
use candid::CandidType;
use serde::Deserialize;

///
/// GroupedRow
///
/// One grouped public output row: ordered grouping key values plus ordered
/// aggregate outputs. Group and aggregate vectors preserve query declaration
/// order at the outward API boundary.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
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

/// One bounded grouped-query page produced by the engine-neutral query lane.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct GroupedQueryOutput {
    /// Accepted entity name used for the read.
    pub entity: String,
    /// Ordered grouped rows. Key and aggregate value order follows declaration order.
    pub rows: Vec<GroupedRow>,
    /// Number of rows returned in this page.
    pub row_count: u32,
    /// Opaque continuation cursor for the next page, when one exists.
    pub next_cursor: Option<String>,
}

#[cfg(test)]
mod tests {
    use candid::CandidType;

    use super::{GroupedQueryOutput, GroupedRow};
    use crate::value::OutputValue;

    #[derive(CandidType)]
    struct FrozenGroupedRowWire {
        group_key: Vec<OutputValue>,
        aggregate_values: Vec<OutputValue>,
    }

    #[derive(CandidType)]
    struct FrozenGroupedQueryOutputWire {
        entity: String,
        rows: Vec<FrozenGroupedRowWire>,
        row_count: u32,
        next_cursor: Option<String>,
    }

    #[test]
    fn grouped_query_output_preserves_its_initial_candid_record_shape() {
        let current = GroupedQueryOutput {
            entity: "Example".to_string(),
            rows: vec![GroupedRow::new(
                [OutputValue::Nat64(7)],
                [OutputValue::Nat64(1)],
            )],
            row_count: 1,
            next_cursor: Some("abcd".to_string()),
        };
        let frozen = FrozenGroupedQueryOutputWire {
            entity: current.entity.clone(),
            rows: vec![FrozenGroupedRowWire {
                group_key: vec![OutputValue::Nat64(7)],
                aggregate_values: vec![OutputValue::Nat64(1)],
            }],
            row_count: current.row_count,
            next_cursor: current.next_cursor.clone(),
        };

        assert_eq!(
            candid::encode_one(&current).expect("current grouped output should encode"),
            candid::encode_one(&frozen).expect("frozen grouped output should encode"),
        );
    }
}
