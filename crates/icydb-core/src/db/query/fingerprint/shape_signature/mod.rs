//! Module: query::fingerprint::shape_signature
//! Responsibility: deterministic query-shape signature derivation from planned
//! query contracts.
//! Does not own: continuation token decoding/validation.
//! Boundary: shared query-shape hashing surface used by execution identity and
//! cursor token checks.

use crate::db::resource::construction::ConstructionBudget;

use crate::db::{
    cursor::ContinuationSignature,
    query::fingerprint::{
        finalize_sha256_digest, hash_sections, new_continuation_signature_hasher,
    },
    query::plan::AccessPlannedQuery,
};

use crate::error::InternalError;

impl AccessPlannedQuery {
    /// Compute a continuation signature bound to the entity path.
    ///
    /// This is used to validate that a continuation token belongs to the
    /// same canonical query shape.
    pub(in crate::db) fn continuation_signature(
        &self,
        entity_path: &str,
        budget: &dyn ConstructionBudget,
    ) -> Result<ContinuationSignature, InternalError> {
        let projection = self.projection_spec_for_identity();

        continuation_signature_for_plan_with_projection(self, entity_path, &projection, budget)
    }
}

fn continuation_signature_for_plan_with_projection(
    plan: &AccessPlannedQuery,
    entity_path: &str,
    projection: &crate::db::query::plan::expr::ProjectionSpec,
    budget: &dyn ConstructionBudget,
) -> Result<ContinuationSignature, InternalError> {
    let mut hasher = new_continuation_signature_hasher();
    hash_sections::hash_continuation_with_projection(
        &mut hasher,
        plan,
        entity_path,
        projection,
        budget,
    )?;
    Ok(ContinuationSignature::from_bytes(finalize_sha256_digest(
        hasher,
    )))
}

#[cfg(test)]
mod tests;
