//! Module: query::fingerprint::fingerprint
//! Responsibility: deterministic plan fingerprint derivation from planner contracts.
//! Does not own: explain projection assembly or execution-plan compilation.
//! Boundary: stable plan identity hash surface for diagnostics/caching.

use crate::db::{
    codec::cursor::encode_cursor,
    query::plan::AccessPlannedQuery,
    query::{
        explain::ExplainPlan,
        fingerprint::{finalize_sha256_digest, hash_parts, new_plan_fingerprint_hasher},
    },
};

///
/// PlanFingerprint
///
/// Stable, deterministic fingerprint for logical plans.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PlanFingerprint([u8; 32]);

impl PlanFingerprint {
    #[must_use]
    pub fn as_hex(&self) -> String {
        encode_cursor(&self.0)
    }
}

impl std::fmt::Display for PlanFingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_hex())
    }
}

impl AccessPlannedQuery {
    /// Compute a stable fingerprint for this logical plan.
    #[must_use]
    pub(in crate::db) fn fingerprint(&self) -> PlanFingerprint {
        let projection = self.projection_spec_for_identity();
        let mut hasher = new_plan_fingerprint_hasher();
        hash_parts::hash_planned_query_profile_with_projection(
            &mut hasher,
            self,
            hash_parts::ExplainHashProfile::Fingerprint,
            &projection,
        );

        PlanFingerprint(finalize_sha256_digest(hasher))
    }
}

impl ExplainPlan {
    /// Compute a stable fingerprint for this explain plan.
    #[must_use]
    pub fn fingerprint(&self) -> PlanFingerprint {
        // Phase 1: hash canonical explain fields under the current fingerprint profile.
        let mut hasher = new_plan_fingerprint_hasher();
        hash_parts::hash_explain_plan_profile(
            &mut hasher,
            self,
            hash_parts::ExplainHashProfile::Fingerprint,
        );

        // Phase 2: finalize into the fixed-width fingerprint payload.
        PlanFingerprint(finalize_sha256_digest(hasher))
    }
}
