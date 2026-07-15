//! Module: db::session::sql::delete_policy
//! Responsibility: SQL `DELETE` exposure policy facade.
//! Does not own: delete execution, row materialization, or commit semantics.
//! Boundary: exposes current public-write proofs only inside the database runtime.

mod model;
mod planning;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(in crate::db) use model::{
    DEFAULT_PUBLIC_BOUNDED_DELETE_LIMIT, DEFAULT_PUBLIC_DELETE_RETURNING_RESPONSE_BYTES,
};
pub(in crate::db) use model::{
    SqlDeleteExposurePolicy, SqlDeletePolicyContext, SqlPublicBoundedDeletePlan,
    SqlPublicPrimaryKeyDeletePlan, SqlValidatedDeletePlan,
};
#[cfg(test)]
pub(in crate::db) use model::{SqlDeletePolicyRejection, SqlDeletePolicyReport};
pub(in crate::db) use planning::classify_sql_delete_policy;
