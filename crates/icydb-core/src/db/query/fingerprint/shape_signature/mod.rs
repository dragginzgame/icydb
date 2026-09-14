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
    query::{construction::ConstructionBudget, plan::AccessPlannedQuery},
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
mod tests {
    use crate::db::query::preparation::with_preparation_work;
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
            with_preparation_work(|work| first.continuation_signature("tests::Entity", work))
                .unwrap(),
            with_preparation_work(|work| second.continuation_signature("tests::Entity", work))
                .unwrap(),
            "one template must not admit a cursor issued for different bound values",
        );
    }

    #[test]
    fn predicate_continuation_admission_is_cumulative_and_retryable() {
        use crate::db::{
            QueryError, RequestExecutionRoot,
            executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
            query::preparation::PreparationWork,
        };
        use icydb_diagnostic_code::{
            DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
            DiagnosticFactTag,
        };
        let request = |resource, limit| {
            RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    32_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(resource, limit),
            )
        };
        let plan = plan_with_bound_value("account");
        let snapshot = plan.clone();
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            let build = |root: &RequestExecutionRoot| {
                PreparationWork::run(&root.scope(), lane, |work| {
                    plan.planned_continuation_contract_with_accepted_identity(
                        "tests::Entity",
                        None,
                        work,
                    )
                    .map_err(QueryError::execute)
                })
            };
            let measured = request(Resource::TemporaryBytes, 32_000_000);
            let expected = build(&measured).unwrap().unwrap().continuation_signature();
            for resource in [
                Resource::TemporaryBytes,
                Resource::PredicateExpressionSteps,
                Resource::NestedValueSteps,
            ] {
                let cost = measured.observed(resource);
                assert!(cost > 0);
                let repeated = request(resource, 2 * cost);
                for _ in 0..2 {
                    assert_eq!(
                        build(&repeated).unwrap().unwrap().continuation_signature(),
                        expected
                    );
                }
                let error = build(&repeated).unwrap_err();
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
                );
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::ExecutionLane, lane.raw(),))
                );
                assert_eq!(repeated.observed(Resource::RowsVisited), 0);
                let fresh = request(resource, cost);
                assert_eq!(
                    build(&fresh).unwrap().unwrap().continuation_signature(),
                    expected
                );
                assert_eq!(plan, snapshot);
            }
        }
    }

    #[test]
    fn expression_owned_and_absent_filters_skip_predicate_copy_admission() {
        use crate::db::query::{construction::ConstructionBudget, plan::expr::Expr};
        struct RejectConstruction;
        impl ConstructionBudget for RejectConstruction {
            fn charge(
                &self,
                _: icydb_diagnostic_code::DiagnosticExecutionBudgetResource,
                _: u64,
            ) -> Result<(), crate::error::InternalError> {
                panic!("this path must not construct predicate scratch");
            }
        }
        let mut plan = plan_with_bound_value("account");
        let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
            unreachable!()
        };
        scalar.filter_expr = Some(Expr::Literal(Value::Bool(true)));
        plan.continuation_signature("tests::Entity", &RejectConstruction)
            .unwrap();
        let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
            unreachable!()
        };
        scalar.filter_expr = None;
        scalar.predicate = None;
        plan.continuation_signature("tests::Entity", &RejectConstruction)
            .unwrap();
    }

    #[test]
    fn continuation_construction_preserves_hash_failure_and_allows_retry() {
        use crate::value::{test_hash_budget_error, with_test_hash_override};
        let mut plan = plan_with_bound_value("account");
        let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
            unreachable!()
        };
        scalar.filter_expr = Some(crate::db::query::plan::expr::Expr::Literal(Value::Nat64(7)));
        let expected =
            with_preparation_work(|work| plan.continuation_signature("tests::Entity", work))
                .unwrap();
        for _ in 0..2 {
            with_test_hash_override(Err(test_hash_budget_error), || {
                let error = with_preparation_work(|work| {
                    plan.planned_continuation_contract_with_accepted_identity(
                        "tests::Entity",
                        None,
                        work,
                    )
                })
                .expect_err("failed hash must not return a continuation contract");
                assert_eq!(error.diagnostic(), test_hash_budget_error().diagnostic());
                assert_eq!(
                    error.diagnostic_facts(),
                    test_hash_budget_error().diagnostic_facts()
                );
            });
        }
        assert_eq!(
            with_preparation_work(|work| plan.continuation_signature("tests::Entity", work))
                .unwrap(),
            expected
        );
    }
}
