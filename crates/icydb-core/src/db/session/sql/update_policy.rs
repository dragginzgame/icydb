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
    SqlExactUpdatePolicy, SqlExactUpdatePolicyRejection, SqlPublicBoundedUpdatePlan,
    SqlPublicPrimaryKeyUpdatePlan, SqlResumableUpdatePolicyReport, SqlTrustedExactUpdatePlan,
    SqlTrustedResumableUpdatePlan, SqlUpdateExposurePolicy, SqlUpdatePolicyRejection,
    SqlUpdatePolicyReport, SqlValidatedUpdatePlan,
};
#[cfg(test)]
pub(in crate::db) use model::{SqlUpdateAssignmentPolicy, SqlUpdatePolicyContext};
#[cfg(test)]
pub(in crate::db) use planning::classify_sql_update_policy;
pub(in crate::db) use planning::{
    classify_sql_resumable_update_policy, classify_sql_update_policy_for_entity,
    with_accepted_sql_update_policy_context,
};
