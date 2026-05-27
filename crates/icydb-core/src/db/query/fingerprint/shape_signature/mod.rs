//! Module: query::fingerprint::shape_signature
//! Responsibility: deterministic query-shape signature derivation from planned
//! and explained query contracts.
//! Does not own: continuation token decoding/validation.
//! Boundary: shared query-shape hashing surface used by execution identity and
//! cursor token checks.

#[cfg(test)]
mod tests;

use crate::db::{
    cursor::ContinuationSignature,
    query::plan::AccessPlannedQuery,
    query::{
        explain::ExplainPlan,
        fingerprint::{finalize_sha256_digest, hash_sections, new_continuation_signature_hasher},
    },
};

impl AccessPlannedQuery {
    /// Compute a continuation signature bound to the entity path.
    ///
    /// This is used to validate that a continuation token belongs to the
    /// same canonical query shape.
    #[must_use]
    pub(in crate::db) fn continuation_signature(
        &self,
        entity_path: &'static str,
    ) -> ContinuationSignature {
        let projection = self.projection_spec_for_identity();

        continuation_signature_for_plan_with_projection(self, entity_path, &projection)
    }
}

fn continuation_signature_for_plan_with_projection(
    plan: &AccessPlannedQuery,
    entity_path: &'static str,
    projection: &crate::db::query::plan::expr::ProjectionSpec,
) -> ContinuationSignature {
    let mut hasher = new_continuation_signature_hasher();
    hash_sections::hash_planned_query_profile_with_projection(
        &mut hasher,
        plan,
        hash_sections::ExplainHashProfile::Continuation { entity_path },
        projection,
    );
    ContinuationSignature::from_bytes(finalize_sha256_digest(hasher))
}

impl ExplainPlan {
    /// Compute the continuation signature for this explain plan.
    ///
    /// Included fields:
    /// - entity path
    /// - mode (load/delete)
    /// - access path
    /// - canonical scalar semantic filter (`filter_expr` when present, otherwise predicate)
    /// - canonical order-by (including implicit PK tie-break)
    /// - distinct flag
    /// - grouped shape (group keys, aggregate terminals, grouped limits)
    /// - projection identity section (semantic projection hash when available)
    ///
    /// Excluded fields:
    /// - pagination window (`limit`, `offset`)
    /// - delete limits
    /// - cursor boundary/token state
    #[must_use]
    pub fn continuation_signature(&self, entity_path: &'static str) -> ContinuationSignature {
        let mut hasher = new_continuation_signature_hasher();
        hash_sections::hash_explain_plan_profile(
            &mut hasher,
            self,
            hash_sections::ExplainHashProfile::Continuation { entity_path },
        );
        ContinuationSignature::from_bytes(finalize_sha256_digest(hasher))
    }
}
