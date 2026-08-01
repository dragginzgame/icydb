//! Module: response
//! Responsibility: public structural query response rows.
//! Does not own: execution routing, planning policy, or cursor protocols.
//! Boundary: exposes logical grouped rows without typed persistence carriers.

mod grouped;
#[cfg(any(test, feature = "query"))]
mod rows;

pub use grouped::GroupedRow;
#[cfg(any(test, feature = "query"))]
pub use rows::RowProjectionOutput;
