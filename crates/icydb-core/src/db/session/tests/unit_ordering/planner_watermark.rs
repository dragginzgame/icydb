//! Planner preparation ownership and completion boundaries.
//! Host budget tests inject charged instruction units: the native IC counter is zero.
//! They prove publication/error boundaries, not measured Wasm instruction cost.

use super::*;
use crate::db::{
    MissingRowPolicy, QueryPlanCacheReuse, RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom},
    index::IndexId,
    predicate::Predicate,
    query::{
        intent::StructuralQuery,
        plan::{CardinalityTiebreakFamily, CardinalityTiebreakRoutePin, OrderSpec},
        preparation::PreparationWork,
    },
    session::AcceptedSchemaCatalogContext,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionBudgetScope,
    DiagnosticExecutionLane,
};

fn request(exhausted: bool, lane: DiagnosticExecutionLane) -> RequestExecutionRoot {
    let root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::InstructionUnits, 0),
    );
    if exhausted {
        root.scope()
            .charge(
                HardExecutionContext::new(DiagnosticExecutionBudgetScope::Request, lane, 0),
                Resource::InstructionUnits,
                1,
            )
            .unwrap_err();
    }
    root
}

fn query() -> StructuralQuery {
    StructuralQuery::new(MissingRowPolicy::Ignore)
        .order_spec(OrderSpec {
            fields: vec![asc("id").lower()],
        })
        .limit(1)
}

fn plan(
    session: &DbSession<TestCanister>,
    catalog: &AcceptedSchemaCatalogContext,
    query: &StructuralQuery,
    lane: DiagnosticExecutionLane,
) -> Result<QueryPlanCacheReuse, QueryError> {
    session
        .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
            catalog.accepted_entity_authority(),
            catalog,
            query,
            lane,
        )
        .map(|(_, reuse)| reuse)
}

fn assert_exhausted(error: QueryError, root: &RequestExecutionRoot, lane: DiagnosticExecutionLane) {
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::InstructionUnits.raw()
    )));
    assert!(
        error
            .diagnostic_facts()
            .contains(&(DiagnosticFactTag::ExecutionLane, lane.raw()))
    );
    assert_eq!(root.observed(Resource::RowsVisited), 0);
    assert_eq!(root.observed(Resource::QueryExecutions), 0);
}

#[test]
fn construction_exhaustion_keeps_cold_cache_empty_and_memoized_keys_skip_copies() {
    let setup = initialize();
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    for lane in [
        DiagnosticExecutionLane::PublicRead,
        DiagnosticExecutionLane::TrustedRead,
        DiagnosticExecutionLane::Diagnostic,
    ] {
        for query in [
            query(),
            query().filter_normalized_predicate(Predicate::False),
        ] {
            setup.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
            let root = RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    16_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(Resource::TemporaryBytes, 0),
            );
            let session = new_request_session_with_root(&root);
            let error = plan(&session, &catalog, &query, lane).unwrap_err();
            assert!(error.diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::TemporaryBytes.raw(),
            )));
            assert_eq!(setup.shared_query_cache_usage_for_tests(), (0, 0));
            let fresh = request(false, lane);
            assert!(
                !plan(
                    &new_request_session_with_root(&fresh),
                    &catalog,
                    &query,
                    lane
                )
                .unwrap()
                .is_hit()
            );
            let bytes = root.observed(Resource::TemporaryBytes);
            let retained = setup.shared_query_cache_usage_for_tests();
            if query.has_scalar_filter() {
                let error = plan(&session, &catalog, &query, lane).unwrap_err();
                assert!(error.diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::TemporaryBytes.raw(),
                )));
                assert!(root.observed(Resource::TemporaryBytes) > bytes);
            } else {
                assert!(plan(&session, &catalog, &query, lane).unwrap().is_hit());
                assert_eq!(root.observed(Resource::TemporaryBytes), bytes);
            }
            assert_eq!(setup.shared_query_cache_usage_for_tests(), retained);
            assert_eq!(root.observed(Resource::RowsVisited), 0);
            assert_eq!(root.observed(Resource::QueryExecutions), 0);
        }
    }
}

#[test]
fn cache_lookup_watermark_precedes_compilation_and_warm_reuse() {
    let setup = initialize();
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    for lane in [
        DiagnosticExecutionLane::PublicRead,
        DiagnosticExecutionLane::TrustedRead,
        DiagnosticExecutionLane::Diagnostic,
    ] {
        let query = query();
        setup.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
        let root = request(true, lane);
        let session = new_request_session_with_root(&root);
        for _ in 0..2 {
            assert_exhausted(
                plan(&session, &catalog, &query, lane).unwrap_err(),
                &root,
                lane,
            );
            assert_eq!(setup.shared_query_cache_usage_for_tests(), (0, 0));
        }
        assert_eq!(root.observed(Resource::PlanCompilations), 0);
        let fresh = request(false, lane);
        assert!(
            !plan(
                &new_request_session_with_root(&fresh),
                &catalog,
                &query,
                lane
            )
            .unwrap()
            .is_hit()
        );
        let retained = setup.shared_query_cache_usage_for_tests();
        // Warm lookup still hashes/compares the key. Exhaustion cannot return
        // a plan, replace its cache entry, or start compilation on a changed key.
        for current in [&query, &query.clone().limit(2), &query] {
            assert_exhausted(
                plan(&session, &catalog, current, lane).unwrap_err(),
                &root,
                lane,
            );
            assert_eq!(setup.shared_query_cache_usage_for_tests(), retained);
        }
        assert!(plan(&setup, &catalog, &query, lane).unwrap().is_hit());
        assert_eq!(root.observed(Resource::PlanCompilations), 0);
    }
}

#[test]
fn filtered_preparation_watermark_keeps_the_previous_bound_memo() {
    let setup = initialize();
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let bind = |label: &str| {
        PreparationWork::run(
            setup.db.request_execution_scope(),
            DiagnosticExecutionLane::TrustedRead,
            |work| {
                query().filter_for_schema(
                    catalog.accepted_schema_info(),
                    &FieldRef::new("label").eq(label),
                    work,
                )
            },
        )
        .unwrap()
    };
    let first = bind("first");
    let second = bind("second");
    for lane in [
        DiagnosticExecutionLane::PublicRead,
        DiagnosticExecutionLane::TrustedRead,
        DiagnosticExecutionLane::Diagnostic,
    ] {
        setup.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
        let root = request(true, lane);
        let session = new_request_session_with_root(&root);
        assert_exhausted(
            plan(&session, &catalog, &first, lane).unwrap_err(),
            &root,
            lane,
        );
        assert_eq!(setup.shared_query_cache_usage_for_tests(), (0, 0));
        assert!(!plan(&setup, &catalog, &first, lane).unwrap().is_hit());
        let before = setup.shared_query_cache_usage_for_tests();
        for _ in 0..2 {
            assert_exhausted(
                plan(&session, &catalog, &second, lane).unwrap_err(),
                &root,
                lane,
            );
            assert_eq!(setup.shared_query_cache_usage_for_tests(), before);
            assert_exhausted(
                plan(&session, &catalog, &first, lane).unwrap_err(),
                &root,
                lane,
            );
            assert!(plan(&setup, &catalog, &first, lane).unwrap().is_hit());
        }
        assert_eq!(root.observed(Resource::PlanCompilations), 0);
        assert!(plan(&setup, &catalog, &second, lane).unwrap().is_hit());
    }
}

#[test]
fn filtered_nonparameterized_hits_finish_preparation_before_cache_reuse() {
    let setup = initialize();
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    for lane in [
        DiagnosticExecutionLane::PublicRead,
        DiagnosticExecutionLane::TrustedRead,
        DiagnosticExecutionLane::Diagnostic,
    ] {
        for query in [
            query().filter_normalized_predicate(Predicate::False),
            query().filter_normalized_predicate(Predicate::True),
            query()
                .filter_normalized_predicate(Predicate::False)
                .limit(0),
        ] {
            setup.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
            let root = request(true, lane);
            let session = new_request_session_with_root(&root);
            assert_exhausted(
                plan(&session, &catalog, &query, lane).unwrap_err(),
                &root,
                lane,
            );
            assert_eq!(setup.shared_query_cache_usage_for_tests(), (0, 0));
            assert_eq!(root.observed(Resource::PlanCompilations), 0);
            assert!(!plan(&setup, &catalog, &query, lane).unwrap().is_hit());
            let retained = setup.shared_query_cache_usage_for_tests();
            for _ in 0..2 {
                assert_exhausted(
                    plan(&session, &catalog, &query, lane).unwrap_err(),
                    &root,
                    lane,
                );
                assert_eq!(setup.shared_query_cache_usage_for_tests(), retained);
            }
            assert!(plan(&setup, &catalog, &query, lane).unwrap().is_hit());
            assert_eq!(root.observed(Resource::PlanCompilations), 0);
        }
    }
}

#[test]
fn pinned_route_unavailability_does_not_hide_watermark_exhaustion() {
    let setup = initialize();
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let pin = CardinalityTiebreakRoutePin::new(
        IndexId::new(ENTITY_TAG, 63),
        CardinalityTiebreakFamily::Prefix,
        1,
    )
    .unwrap();
    for lane in [
        DiagnosticExecutionLane::PublicRead,
        DiagnosticExecutionLane::TrustedRead,
    ] {
        let root = request(true, lane);
        let session = new_request_session_with_root(&root);
        let error = session
            .structural_projection_prepared_plan_for_accepted_authority_with_route_pin(
                &query(),
                catalog.accepted_entity_authority(),
                catalog.snapshot(),
                lane,
                pin,
            )
            .unwrap_err();
        assert_exhausted(error, &root, lane);
        assert!(
            setup
                .structural_projection_prepared_plan_for_accepted_authority_with_route_pin(
                    &query(),
                    catalog.accepted_entity_authority(),
                    catalog.snapshot(),
                    lane,
                    pin,
                )
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn executor_handoff_requires_and_preserves_finalized_planner_metadata() {
    use crate::{db::executor::SharedPreparedExecutionPlan, error::ErrorClass};

    let setup = initialize();
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    for lane in [
        DiagnosticExecutionLane::PublicRead,
        DiagnosticExecutionLane::TrustedRead,
    ] {
        for query in [
            query(),
            query().filter_normalized_predicate(Predicate::False),
        ] {
            setup.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
            let (prepared, _) = setup
                .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
                    catalog.accepted_entity_authority(),
                    &catalog,
                    &query,
                    lane,
                )
                .unwrap();
            let finalized = prepared.logical_plan();
            assert!(finalized.has_static_execution_planning_contract());
            let expected_profile = finalized.planner_route_profile().clone();
            let expected_signature = prepared.continuation_signature_for_runtime().unwrap();
            let denied = RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    16_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(Resource::PredicateExpressionSteps, 0),
            );
            let before = setup.shared_query_cache_usage_for_tests();
            let error = PreparationWork::run(&denied.scope(), lane, |work| {
                SharedPreparedExecutionPlan::from_plan(
                    catalog.accepted_entity_authority(),
                    finalized.clone(),
                    catalog.fingerprint(),
                    work,
                )
                .map_err(QueryError::execute)
            })
            .unwrap_err();
            assert!(error.diagnostic_facts().contains(&(
                icydb_diagnostic_code::DiagnosticFactTag::BudgetResource,
                Resource::PredicateExpressionSteps.raw(),
            )));
            assert_eq!(denied.observed(Resource::PredicateExpressionSteps), 1);
            assert_eq!(denied.observed(Resource::RowsVisited), 0);
            assert_eq!(setup.shared_query_cache_usage_for_tests(), before);
            let rebound = crate::db::query::preparation::with_preparation_work(|work| {
                SharedPreparedExecutionPlan::from_plan(
                    catalog.accepted_entity_authority(),
                    finalized.clone(),
                    catalog.fingerprint(),
                    work,
                )
            })
            .unwrap();
            assert_eq!(
                rebound.logical_plan().planner_route_profile(),
                &expected_profile
            );
            assert_eq!(
                rebound.continuation_signature_for_runtime().unwrap(),
                expected_signature
            );

            let mut incomplete = finalized.clone();
            incomplete.static_execution_planning_contract = None;
            let before = setup.shared_query_cache_usage_for_tests();
            let error = crate::db::query::preparation::with_preparation_work(|work| {
                SharedPreparedExecutionPlan::from_plan(
                    catalog.accepted_entity_authority(),
                    incomplete,
                    catalog.fingerprint(),
                    work,
                )
            })
            .unwrap_err();
            assert_eq!(error.class(), ErrorClass::InvariantViolation);
            assert_eq!(error.origin(), ErrorOrigin::Query);
            assert_eq!(setup.shared_query_cache_usage_for_tests(), before);
            assert!(plan(&setup, &catalog, &query, lane).unwrap().is_hit());
        }
    }
}

#[test]
fn grouped_rebinding_allocation_exhaustion_precedes_cache_publication() {
    let setup = initialize();
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let grouped = PreparationWork::run(
        setup.db.request_execution_scope(),
        DiagnosticExecutionLane::PublicRead,
        |work| {
            StructuralQuery::new(MissingRowPolicy::Ignore)
                .group_fields_with_schema(
                    &["label".to_string()],
                    catalog.accepted_schema_info(),
                    work,
                )
                .map(|query| {
                    query
                        .group_aggregates(vec![
                            crate::db::query::plan::GroupAggregateSpec::from_aggregate_expr(
                                crate::db::query::builder::count(),
                            ),
                        ])
                        .grouped_limits(100, 64 * 1024)
                })
        },
    )
    .unwrap();
    for lane in [
        DiagnosticExecutionLane::PublicRead,
        DiagnosticExecutionLane::TrustedRead,
    ] {
        setup.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
        let root = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::TemporaryBytes, 0),
        );
        let session = new_request_session_with_root(&root);
        for _ in 0..2 {
            let error = plan(&session, &catalog, &grouped, lane).unwrap_err();
            assert!(error.diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::TemporaryBytes.raw()
            )));
            assert_eq!(setup.shared_query_cache_usage_for_tests(), (0, 0));
        }
        assert_eq!(root.observed(Resource::RowsVisited), 0);
        assert_eq!(root.observed(Resource::QueryExecutions), 0);
        assert!(!plan(&setup, &catalog, &grouped, lane).unwrap().is_hit());
        assert!(plan(&session, &catalog, &grouped, lane).unwrap().is_hit());
    }
}
