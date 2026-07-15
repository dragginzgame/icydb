//! Module: db::session::sql::update_policy
//! Responsibility: SQL `UPDATE` exposure policy facade.
//! Does not own: row mutation execution, field validation, or persistence.
//! Boundary: exposes current public-write proofs only inside the database runtime.

mod model;
mod planning;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(in crate::db) use model::{
    DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT, DEFAULT_PUBLIC_UPDATE_RETURNING_RESPONSE_BYTES,
};
pub(in crate::db) use model::{
    SqlPublicBoundedUpdatePlan, SqlPublicPrimaryKeyUpdatePlan, SqlUpdateExposurePolicy,
    SqlUpdatePolicyContext, SqlValidatedUpdatePlan,
};
#[cfg(test)]
pub(in crate::db) use model::{
    SqlUpdateAssignmentPolicy, SqlUpdatePolicyRejection, SqlUpdatePolicyReport,
};
pub(in crate::db) use planning::classify_sql_update_policy;
