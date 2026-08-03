//! Module: response
//! Responsibility: public structural query response rows.
//! Does not own: execution routing, planning policy, or cursor protocols.
//! Boundary: exposes logical grouped rows without typed persistence carriers.

mod grouped;
mod rows;

pub use grouped::{GroupedQueryOutput, GroupedRow};
pub use rows::RowProjectionOutput;
