use crate::db::{
    cursor::ContinuationSignature,
    query::{
        explain::ExplainPlan,
        fingerprint::{finalize_sha256_digest, hash_parts, new_continuation_signature_hasher_v1},
        plan::expr::ProjectionSpec,
    },
};
use sha2::Sha256;

/// Hash an `ExplainPlan` with one explicit semantic projection section for tests.
pub(in crate::db::query) fn hash_explain_plan_profile_with_projection(
    hasher: &mut Sha256,
    plan: &ExplainPlan,
    profile: hash_parts::ExplainHashProfile<'_>,
    projection: &ProjectionSpec,
) {
    hash_parts::hash_explain_plan_profile_internal(hasher, plan, profile, Some(projection));
}

/// Compute one continuation signature with one explicit semantic projection section for tests.
pub(in crate::db::query::fingerprint) fn continuation_signature_with_projection(
    explain: &ExplainPlan,
    entity_path: &'static str,
    projection: &ProjectionSpec,
) -> ContinuationSignature {
    let mut hasher = new_continuation_signature_hasher_v1();
    hash_explain_plan_profile_with_projection(
        &mut hasher,
        explain,
        hash_parts::ExplainHashProfile::ContinuationV1 { entity_path },
        projection,
    );
    ContinuationSignature::from_bytes(finalize_sha256_digest(hasher))
}
