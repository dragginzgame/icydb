//! Module: query::fingerprint::shape_signature
//! Responsibility: deterministic query-shape signature derivation from planned
//! query contracts.
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

use crate::error::InternalError;

impl AccessPlannedQuery {
    /// Compute a continuation signature bound to the entity path.
    ///
    /// This is used to validate that a continuation token belongs to the
    /// same canonical query shape.
    pub(in crate::db) fn continuation_signature(
        &self,
        entity_path: &str,
    ) -> Result<ContinuationSignature, InternalError> {
        let projection = self.projection_spec_for_identity();

        continuation_signature_for_plan_with_projection(self, entity_path, &projection)
    }
}

fn continuation_signature_for_plan_with_projection(
    plan: &AccessPlannedQuery,
    entity_path: &str,
    projection: &crate::db::query::plan::expr::ProjectionSpec,
) -> Result<ContinuationSignature, InternalError> {
    let mut hasher = new_continuation_signature_hasher();
    hash_sections::hash_continuation_with_projection(&mut hasher, plan, entity_path, projection)?;
    Ok(ContinuationSignature::from_bytes(finalize_sha256_digest(
        hasher,
    )))
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
            first.continuation_signature("tests::Entity").unwrap(),
            second.continuation_signature("tests::Entity").unwrap(),
            "one template must not admit a cursor issued for different bound values",
        );
    }

    #[test]
    fn continuation_construction_preserves_hash_failure_and_allows_retry() {
        use crate::value::{test_hash_budget_error, with_test_hash_override};
        let mut plan = plan_with_bound_value("account");
        let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
            unreachable!()
        };
        scalar.filter_expr = Some(crate::db::query::plan::expr::Expr::Literal(Value::Nat64(7)));
        let expected = plan.continuation_signature("tests::Entity").unwrap();
        for _ in 0..2 {
            with_test_hash_override(Err(test_hash_budget_error), || {
                let error = plan
                    .planned_continuation_contract_with_accepted_identity("tests::Entity", None)
                    .expect_err("failed hash must not return a continuation contract");
                assert_eq!(error.diagnostic(), test_hash_budget_error().diagnostic());
                assert_eq!(
                    error.diagnostic_facts(),
                    test_hash_budget_error().diagnostic_facts()
                );
            });
        }
        assert_eq!(
            plan.continuation_signature("tests::Entity").unwrap(),
            expected
        );
    }
}
