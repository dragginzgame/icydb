//! Static index metadata uses one accepted-key pass and current request authority.

use super::*;
use crate::db::{
    RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    predicate::{IndexCompileTarget, IndexCompileTargetKind, Predicate},
    query::{plan::ResolvedOrderField, preparation::PreparationWork},
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource as Resource, DiagnosticFactTag};

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn indexed_query() -> StructuralQuery {
    StructuralQuery::new(MissingRowPolicy::Ignore)
        .filter_normalized_predicate(Predicate::eq(
            "rare".to_string(),
            crate::value::Value::Text("group-a".into()),
        ))
        .order_spec(OrderSpec {
            fields: vec![asc("id").lower()],
        })
        .limit(10)
}

#[test]
fn static_index_metadata_preserves_layout_at_exact_construction_limits() {
    let session = initialize();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let (prepared, _) = session
        .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
            catalog.accepted_entity_authority(),
            &catalog,
            &indexed_query(),
            DiagnosticExecutionLane::PublicRead,
        )
        .unwrap();
    let plan = prepared.logical_plan();
    let expected_slot = catalog
        .accepted_schema_info()
        .field_slot_index("rare")
        .unwrap();
    assert_eq!(plan.slot_map().unwrap(), &[expected_slot]);
    let expected_target = IndexCompileTarget {
        component_index: 0,
        field_slot: expected_slot,
        kind: IndexCompileTargetKind::Field,
    };
    assert_eq!(plan.index_compile_targets().unwrap(), &[expected_target]);
    let (projection_bytes, projection_steps) =
        projection_metadata::cost(plan, catalog.accepted_schema_info());
    let bytes = projection_bytes
        + (size_of::<ResolvedOrderField>()
            + 5 * size_of::<usize>()
            + size_of::<IndexCompileTarget>()) as u64;
    let steps = projection_steps + 4 + "rare".len() as u64 + "id".len() as u64;

    for lane in [
        DiagnosticExecutionLane::PublicRead,
        DiagnosticExecutionLane::TrustedRead,
    ] {
        for (resource, exact) in [
            (Resource::TemporaryBytes, bytes),
            (Resource::PredicateExpressionSteps, steps),
        ] {
            for limit in [exact - 1, exact] {
                let root = request(resource, limit);
                let mut candidate = plan.clone();
                let old_slots = candidate.slot_map().unwrap().as_ptr();
                let result = PreparationWork::run(&root.scope(), lane, |work| {
                    let projection =
                        candidate.prepare_projection(catalog.accepted_schema_info(), work)?;
                    candidate.finalize_static_execution_planning_contract_with_schema(
                        catalog.accepted_schema_info(),
                        projection,
                        work,
                    )
                });
                if limit == exact {
                    result.unwrap();
                    assert_eq!(candidate.slot_map().unwrap(), &[expected_slot]);
                    assert_eq!(
                        candidate.index_compile_targets().unwrap(),
                        &[expected_target]
                    );
                } else {
                    let error = result.unwrap_err();
                    assert!(
                        error
                            .diagnostic_facts()
                            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                    );
                    assert_eq!(candidate.slot_map().unwrap().as_ptr(), old_slots);
                }
                assert_eq!(root.observed(resource), exact);
                assert_eq!(root.observed(Resource::RowsVisited), 0);
            }
        }
    }
}

#[test]
fn exhausted_index_metadata_does_not_publish_a_plan() {
    let setup = initialize();
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let query = indexed_query();
    for lane in [
        DiagnosticExecutionLane::PublicRead,
        DiagnosticExecutionLane::TrustedRead,
    ] {
        setup.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
        let root = request(Resource::TemporaryBytes, 0);
        let session = new_request_session(&root);
        for _ in 0..2 {
            let error = session
                .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
                    catalog.accepted_entity_authority(),
                    &catalog,
                    &query,
                    lane,
                )
                .unwrap_err();
            assert!(
                error.diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::TemporaryBytes.raw()
                )),
                "unexpected rejection: {error:?}"
            );
            assert_eq!(setup.shared_query_cache_usage_for_tests(), (0, 0));
        }
        assert_eq!(root.observed(Resource::RowsVisited), 0);
        assert_eq!(root.observed(Resource::QueryExecutions), 0);
        setup
            .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
                catalog.accepted_entity_authority(),
                &catalog,
                &query,
                lane,
            )
            .unwrap();
        assert!(
            session
                .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
                    catalog.accepted_entity_authority(),
                    &catalog,
                    &query,
                    lane,
                )
                .unwrap()
                .1
                .is_hit()
        );
    }
}
