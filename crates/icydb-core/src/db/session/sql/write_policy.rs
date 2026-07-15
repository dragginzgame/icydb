//! Module: db::session::sql::write_policy
//! Responsibility: shared SQL write-shape proofs used by policy classifiers.
//! Does not own: statement-family admission or mutation execution.
//! Boundary: proves primary-key `WHERE`, canonical order, execution bounds,
//! and `RETURNING` shapes consistently for UPDATE and DELETE policy gates.

mod bounds;
mod model;
mod shape;

pub(in crate::db::session::sql) use bounds::combined_optional_row_bound;
pub(in crate::db::session::sql) use model::{
    DEFAULT_PUBLIC_BOUNDED_WRITE_LIMIT, DEFAULT_PUBLIC_WRITE_RETURNING_RESPONSE_BYTES,
    SqlWriteBoundedPolicyRejection, SqlWriteExposureClass, SqlWritePlanCore, SqlWritePolicyBounds,
    SqlWriteShapePolicyRejection,
};
pub(in crate::db) use model::{
    SqlWriteExecutionBounds, SqlWriteReturningBounds, SqlWriteStatementShape,
};
#[cfg(test)]
pub(in crate::db) use model::{SqlWriteReturningShape, SqlWriteWhereProof};
pub(in crate::db::session::sql) use shape::{
    SqlWriteStatementShapeInput, classify_write_statement_shape, contains_field,
    current_table_field_name,
};
