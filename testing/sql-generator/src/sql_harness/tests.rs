//! Module: sql_harness::tests
//!
//! Responsibility: boundary tests for the shared SQL evidence contract.
//! Does not own: production SQL behavior or runner-specific fixture execution.
//! Boundary: proves typed taxonomies, selection, normalization, and verdict invariants.

mod typed_core;

use super::*;

#[test]
fn route_taxonomies_cover_the_current_slice_contract() {
    let route_families = [
        RouteFamily::EqualityPrefixOrderedSuffix,
        RouteFamily::GroupedAggregate,
        RouteFamily::IncompatibleFilterFirstOrder,
        RouteFamily::MaterializedOrder,
        RouteFamily::NotContractual,
        RouteFamily::NotOrderedOrNotPaginated,
        RouteFamily::PrimaryOrder,
        RouteFamily::ResidualFilterOrderedScan,
        RouteFamily::SecondaryOrder,
        RouteFamily::UnsupportedAccessKind,
    ];
    assert!(
        route_families
            .iter()
            .all(|family| !family.code().is_empty())
    );
    let route_outcomes = [
        RouteOutcome::EligibleButNotPushed,
        RouteOutcome::Materialized,
        RouteOutcome::MissingTieBreaker,
        RouteOutcome::Pushed,
        RouteOutcome::ResidualUnbounded,
        RouteOutcome::UnchangedOrNotApplicable,
        RouteOutcome::Unsupported,
    ];
    assert!(
        route_outcomes
            .iter()
            .all(|outcome| !outcome.code().is_empty())
    );
    let route_reasons = [
        RouteReason::EqualityPrefixOrderedSuffixCandidate,
        RouteReason::EqualityPrefixOrderedSuffixLimitStopProven,
        RouteReason::FilterOrderMismatch,
        RouteReason::GroupedAggregateOwnsExecution,
        RouteReason::IndexOrderSuffixGap,
        RouteReason::NoOrderBy,
        RouteReason::NotAPaginatedSelect,
        RouteReason::OrderExpressionNotClassified,
        RouteReason::PrimaryOrderCandidate,
        RouteReason::PrimaryOrderLimitStopProven,
        RouteReason::RequiresMaterializedSort,
        RouteReason::ResidualFilterRequiresCandidateScan,
        RouteReason::SecondaryOrderCandidate,
        RouteReason::SecondaryOrderLimitStopProven,
        RouteReason::StorageMirrorHasPrimaryIndexOnly,
        RouteReason::StorageMirrorPrimaryOrderCandidate,
    ];
    assert!(route_reasons.iter().all(|reason| !reason.code().is_empty()));

    let index_route = RouteExpectation::IndexOrder {
        family: RouteFamily::SecondaryOrder,
        candidate_reason: RouteReason::SecondaryOrderCandidate,
        pushed_reason: RouteReason::SecondaryOrderLimitStopProven,
    };
    assert_eq!(
        index_route
            .classify(
                WindowSpec::ordered(1, 0, "age ASC, id ASC"),
                RouteObservation {
                    data_store_get_calls: 1,
                    index_store_entry_reads: 1,
                    ..RouteObservation::default()
                },
            )
            .outcome,
        RouteOutcome::Pushed
    );
}

#[test]
fn failure_taxonomies_cover_the_current_slice_contract() {
    let _ = [
        HarnessFailureKind::Artifact,
        HarnessFailureKind::Fixture,
        HarnessFailureKind::Infrastructure,
        HarnessFailureKind::Rendering,
        HarnessFailureKind::Timeout,
    ];
    let _ = ObservedOutcome::HarnessFailure(HarnessFailureKind::Rendering);
    let failure_owners = [
        FailureOwner::HarnessRendering,
        FailureOwner::Infrastructure,
        FailureOwner::Product,
        FailureOwner::ReferenceAdapter,
        FailureOwner::Unresolved,
    ];
    assert!(failure_owners.iter().all(|owner| !owner.code().is_empty()));
    let mismatch_categories = [
        MismatchCategory::Acceptance,
        MismatchCategory::Harness,
        MismatchCategory::MissingProvider,
        MismatchCategory::Ordering,
        MismatchCategory::Route,
        MismatchCategory::RowShape,
        MismatchCategory::TypedError,
        MismatchCategory::Value,
    ];
    assert!(
        mismatch_categories
            .iter()
            .all(|category| !category.code().is_empty())
    );
}
