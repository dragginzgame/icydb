//! Candidate-list construction shares the request; warm template reuse does not
//! allocate another candidate list or publish an incomplete cold artifact.

use super::*;
use crate::db::{
    MissingRowPolicy, RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    predicate::IndexCompileTarget,
    query::{
        intent::StructuralQuery,
        plan::{OrderSpec, ResolvedOrderField, VisibleIndexes},
        preparation::PreparationWork,
    },
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane,
};

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

#[test]
fn template_candidate_construction_rejects_before_publication_and_shares_warm_authority() {
    let setup = initialize();
    seed_singleton(&setup);
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let schema = catalog.accepted_schema_info();
    let visible = VisibleIndexes::accepted_schema_visible(schema);
    let count = visible.accepted_semantic_index_contracts().len();
    assert!(count > 0, "exercise nonempty accepted index authority");
    let bytes =
        (size_of_val(visible.accepted_semantic_index_contracts()) + 2 * size_of::<usize>()) as u64;
    let queries = ["singleton", "missing", "singleton"].map(|label| indexed_query(&setup, label));
    let (projection_bytes, projection_steps) = projection_cost(&setup, &queries[0]);
    let metadata_bytes = projection_bytes
        + (size_of::<ResolvedOrderField>()
            + 5 * size_of::<usize>()
            + size_of::<IndexCompileTarget>()) as u64;
    let metadata_steps = projection_steps + 4 + "label".len() as u64 + "id".len() as u64;
    for lane in [
        DiagnosticExecutionLane::PublicRead,
        DiagnosticExecutionLane::TrustedRead,
    ] {
        for (resource, exact, rebound) in [
            (
                Resource::TemporaryBytes,
                bytes + metadata_bytes,
                metadata_bytes,
            ),
            (
                Resource::PredicateExpressionSteps,
                count as u64 + metadata_steps,
                metadata_steps,
            ),
        ] {
            setup.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
            let rejected = request(resource, exact - 1);
            let session = new_request_session_with_root(&rejected);
            let mut previous = 0;
            for _ in 0..2 {
                let error = session
                    .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
                        catalog.accepted_entity_authority().clone(),
                        &catalog,
                        &queries[0],
                        lane,
                    )
                    .unwrap_err();
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),)),
                    "construction should reject {resource:?}: {error:?}, {:?}",
                    error.diagnostic_facts(),
                );
                // A retry can exhaust at an earlier construction owner because
                // the same request has already consumed its budget.
                let observed = rejected.observed(resource);
                assert!(observed >= exact && observed > previous);
                previous = observed;
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::ExecutionLane, lane.raw()))
                );
                assert_eq!(rejected.observed(Resource::RowsVisited), 0);
                assert_eq!(rejected.observed(Resource::QueryExecutions), 0);
                assert_eq!(setup.shared_query_cache_usage_for_tests(), (0, 0));
            }
            let admitted = request(resource, exact);
            let session = new_request_session_with_root(&admitted);
            let (_, reuse) = session
                .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
                    catalog.accepted_entity_authority().clone(),
                    &catalog,
                    &queries[0],
                    lane,
                )
                .unwrap();
            assert!(!reuse.is_hit());
            assert_eq!(admitted.observed(resource), exact);
            assert_eq!(setup.shared_query_cache_usage_for_tests().0, 1);
            // A memo hit skips construction. A/B/A rebinding rebuilds static
            // metadata but retains the candidate array instead of copying it.
            let warm = request(resource, 2 * rebound);
            let session = new_request_session_with_root(&warm);
            for (position, query) in queries.iter().enumerate() {
                let (_, reuse) = session
                    .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
                        catalog.accepted_entity_authority().clone(),
                        &catalog,
                        query,
                        lane,
                    )
                    .unwrap();
                assert!(reuse.is_hit());
                assert_eq!(warm.observed(resource), position as u64 * rebound);
                assert_eq!(warm.observed(Resource::PlanCompilations), 0);
                assert_eq!(setup.shared_query_cache_usage_for_tests().0, 1);
            }
        }
    }
}

fn indexed_query(setup: &DbSession<TestCanister>, label: &str) -> StructuralQuery {
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    PreparationWork::run(
        setup.db.request_execution_scope(),
        DiagnosticExecutionLane::TrustedRead,
        |work| {
            StructuralQuery::new(MissingRowPolicy::Ignore).filter_for_schema(
                catalog.accepted_schema_info(),
                &FieldRef::new("label").eq(label),
                work,
            )
        },
    )
    .unwrap()
    .order_spec(OrderSpec {
        fields: vec![asc("id").lower()],
    })
    .limit(1)
}

// Include the projection owner's independently tested construction charges
// without duplicating its schema traversal in these template-cache checks.
fn projection_cost(setup: &DbSession<TestCanister>, query: &StructuralQuery) -> (u64, u64) {
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let (plan, _) = setup
        .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
            catalog.accepted_entity_authority(),
            &catalog,
            query,
            DiagnosticExecutionLane::PublicRead,
        )
        .unwrap();
    let root = request(Resource::TemporaryBytes, 16_000_000);
    PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        let projection = plan
            .logical_plan()
            .projection_spec_with_schema(catalog.accepted_schema_info());
        crate::db::query::plan::lower_direct_projection_layouts_with_schema(
            catalog.accepted_schema_info(),
            &plan.logical_plan().logical,
            &projection,
            work,
        )?;
        projection.referenced_slots_for_schema(catalog.accepted_schema_info(), work)?;
        projection.is_schema_identity_for(catalog.accepted_schema_info(), work)
    })
    .unwrap();
    (
        root.observed(Resource::TemporaryBytes),
        root.observed(Resource::PredicateExpressionSteps),
    )
}
