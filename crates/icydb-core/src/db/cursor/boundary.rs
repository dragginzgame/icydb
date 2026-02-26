use crate::value::Value;
use serde::{Deserialize, Serialize};

///
/// CursorBoundarySlot
/// Slot value used for deterministic cursor boundaries.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum CursorBoundarySlot {
    Missing,
    Present(Value),
}

///
/// CursorBoundary
/// Ordered boundary tuple for continuation pagination.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct CursorBoundary {
    pub(crate) slots: Vec<CursorBoundarySlot>,
}
