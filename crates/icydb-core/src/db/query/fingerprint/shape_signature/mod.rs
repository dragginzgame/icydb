//! Module: query::fingerprint::shape_signature
//! Responsibility: deterministic query-shape signature derivation from planned
//! and explained query contracts.
//! Does not own: continuation token decoding/validation.
//! Boundary: shared query-shape hashing surface used by execution identity and
//! cursor token checks.

use crate::db::{
    cursor::ContinuationSignature,
    query::fingerprint::{
        finalize_sha256_digest, hash_sections, new_continuation_signature_hasher,
    },
    query::plan::AccessPlannedQuery,
};

impl AccessPlannedQuery {
    /// Compute a continuation signature bound to the entity path.
    ///
    /// This is used to validate that a continuation token belongs to the
    /// same canonical query shape.
    #[must_use]
    pub(in crate::db) fn continuation_signature(&self, entity_path: &str) -> ContinuationSignature {
        let projection = self.projection_spec_for_identity();

        continuation_signature_for_plan_with_projection(self, entity_path, &projection)
    }
}

fn continuation_signature_for_plan_with_projection(
    plan: &AccessPlannedQuery,
    entity_path: &str,
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

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            Predicate,
            access::AccessPath,
            predicate::MissingRowPolicy,
            query::plan::{AccessPlannedQuery, LogicalPlan},
        },
        value::Value,
    };

    fn plan_with_bound_value(value: &str) -> AccessPlannedQuery {
        let mut plan =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
        let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
            panic!("test plan should remain scalar");
        };
        scalar.predicate = Some(Predicate::eq(
            "label".to_string(),
            Value::Text(value.to_string()),
        ));

        plan
    }

    #[test]
    fn continuation_signature_binds_current_parameter_values() {
        let first = plan_with_bound_value("first");
        let second = plan_with_bound_value("second");

        assert_ne!(
            first.continuation_signature("tests::Entity"),
            second.continuation_signature("tests::Entity"),
            "one template must not admit a cursor issued for different bound values",
        );
    }
}
