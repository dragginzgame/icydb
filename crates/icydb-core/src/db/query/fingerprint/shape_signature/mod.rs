//! Module: query::fingerprint::continuation_signature
//! Responsibility: deterministic continuation-signature derivation from explain plans.
//! Does not own: continuation token decoding/validation.
//! Boundary: query-plan shape signature surface used by cursor token checks.

#[cfg(test)]
mod tests;

use crate::{
    db::{
        cursor::ContinuationSignature,
        query::plan::AccessPlannedQuery,
        query::{explain::ExplainPlan, fingerprint::hash_parts},
    },
    traits::FieldValue,
};
use sha2::{Digest, Sha256};

impl<K> AccessPlannedQuery<K>
where
    K: FieldValue,
{
    /// Compute a continuation signature bound to the entity path.
    ///
    /// This is used to validate that a continuation token belongs to the
    /// same canonical query shape.
    #[must_use]
    pub(crate) fn continuation_signature(
        &self,
        entity_path: &'static str,
    ) -> ContinuationSignature {
        let explain = self.explain();
        let projection = self.projection_spec_for_identity();

        continuation_signature_with_projection(&explain, entity_path, &projection)
    }
}

fn continuation_signature_with_projection(
    explain: &ExplainPlan,
    entity_path: &'static str,
    projection: &crate::db::query::plan::expr::ProjectionSpec,
) -> ContinuationSignature {
    let mut hasher = Sha256::new();
    hasher.update(b"contsig:v1");
    hash_parts::hash_explain_plan_profile_with_projection(
        &mut hasher,
        explain,
        hash_parts::ExplainHashProfile::ContinuationV1 { entity_path },
        projection,
    );

    let digest = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    ContinuationSignature::from_bytes(out)
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
        let mut hasher = Sha256::new();
        hasher.update(b"contsig:v1");
        hash_parts::hash_explain_plan_profile(
            &mut hasher,
            self,
            hash_parts::ExplainHashProfile::ContinuationV1 { entity_path },
        );

        let digest = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&digest);
        ContinuationSignature::from_bytes(out)
    }
}
