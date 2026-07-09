//! Module: db::session::sql::delete_policy
//! Responsibility: SQL `DELETE` exposure policy facade.
//! Does not own: delete execution, row materialization, or commit semantics.
//! Boundary: keeps public DELETE DTO exports separate from model and planning owners.

mod model;
mod planning;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(in crate::db) use model::{
    DEFAULT_PUBLIC_BOUNDED_DELETE_LIMIT, DEFAULT_PUBLIC_DELETE_RETURNING_RESPONSE_BYTES,
};
pub use model::{
    SqlAdminBulkDeletePlan, SqlDeleteExposurePolicy, SqlDeletePolicyContext,
    SqlDeletePolicyRejection, SqlDeletePolicyReport, SqlDeleteStatementClassification,
    SqlPublicBoundedDeletePlan, SqlPublicPrimaryKeyDeletePlan, SqlSessionCurrentDeletePlan,
    SqlValidatedDeletePlan,
};
pub use planning::classify_sql_delete_policy;
