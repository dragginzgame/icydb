//! Module: query::fingerprint::continuation_signature
//! Responsibility: deterministic continuation-signature derivation from planner contracts.
//! Does not own: continuation token decoding/validation.
//! Boundary: query-plan shape signature surface used by cursor token checks.

#[cfg(test)]
mod tests;

use crate::db::{
    cursor::ContinuationSignature,
    query::plan::AccessPlannedQuery,
    query::{
        explain::ExplainPlan,
        fingerprint::{finalize_sha256_digest, hash_parts, new_continuation_signature_hasher_v1},
    },
};

impl AccessPlannedQuery {
    /// Compute a continuation signature bound to the entity path.
    ///
    /// This is used to validate that a continuation token belongs to the
    /// same canonical query shape.
    #[must_use]
    pub(crate) fn continuation_signature(
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
    let mut hasher = new_continuation_signature_hasher_v1();
    hash_parts::hash_planned_query_profile_with_projection(
        &mut hasher,
        plan,
        hash_parts::ExplainHashProfile::ContinuationV1 { entity_path },
        projection,
    );
    ContinuationSignature::from_bytes(finalize_sha256_digest(hasher))
}

#[cfg(test)]
fn continuation_signature_with_projection(
    explain: &ExplainPlan,
    entity_path: &'static str,
    projection: &crate::db::query::plan::expr::ProjectionSpec,
) -> ContinuationSignature {
    let mut hasher = new_continuation_signature_hasher_v1();
    hash_parts::hash_explain_plan_profile_with_projection(
        &mut hasher,
        explain,
        hash_parts::ExplainHashProfile::ContinuationV1 { entity_path },
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
    /// - normalized predicate
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
        let mut hasher = new_continuation_signature_hasher_v1();
        hash_parts::hash_explain_plan_profile(
            &mut hasher,
            self,
            hash_parts::ExplainHashProfile::ContinuationV1 { entity_path },
        );
        ContinuationSignature::from_bytes(finalize_sha256_digest(hasher))
    }
}
