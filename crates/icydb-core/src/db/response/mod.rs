//! Module: response
//! Responsibility: public structural query response rows.
//! Does not own: execution routing, planning policy, or cursor protocols.
//! Boundary: exposes logical grouped rows without typed persistence carriers.

mod exact_key;
mod grouped;
mod rows;

#[doc(hidden)]
pub use exact_key::ExactKeyBatchProjectionOutput;
pub use grouped::{GroupedQueryOutput, GroupedRow};
pub use rows::RowProjectionOutput;
