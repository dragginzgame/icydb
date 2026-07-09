//! Module: db::session::sql::update_policy
//! Responsibility: SQL `UPDATE` exposure policy facade.
//! Does not own: row mutation execution, field validation, or persistence.
//! Boundary: keeps public UPDATE DTO exports separate from model and planning owners.

mod model;
mod planning;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(in crate::db) use model::{
    DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT, DEFAULT_PUBLIC_UPDATE_RETURNING_RESPONSE_BYTES,
};
pub use model::{
    SqlAdminBulkUpdatePlan, SqlPublicBoundedUpdatePlan, SqlPublicPrimaryKeyUpdatePlan,
    SqlSessionCurrentUpdatePlan, SqlUpdateAssignmentPolicy, SqlUpdateExposurePolicy,
    SqlUpdatePolicyContext, SqlUpdatePolicyRejection, SqlUpdatePolicyReport,
    SqlUpdateStatementClassification, SqlValidatedUpdatePlan,
};
pub use planning::classify_sql_update_policy;
