//! Module: sql_correctness_support::typed_core
//! Responsibility: focused boundary tests for shared typed SQL harness behavior.
//! Does not own: harness implementation, production SQL semantics, or performance reports.
//! Boundary: proves route, normalization, verdict, and stratified-selection contracts.

use std::collections::BTreeSet;

use crate::sql_harness::{
    CorrectnessObservation, CorrectnessScenario, CorrectnessVerdict, DiagnosticFact,
    EligibleProvider, EvidenceStrength, ExpectedAcceptance, FailureOwner, HarnessFailureKind,
    MeasurementStatus, MismatchCategory, MutationKind, NormalizedCell, NormalizedResult,
    NullabilityClass, ObservedOutcome, PerformanceFailure, PerformanceVerdict, PredicateFamily,
    QueryShape, RouteExpectation, RouteFact, RouteFamily, RouteObservation, RouteOutcome,
    RouteReason, RowOrder, ScenarioMetadata, ScenarioStratum, SelectionError, StatementFamily,
    ValueTypeFamily, WindowSpec, compare_normalized_results, correctness_verdict,
    performance_verdict, select_stratified,
};

const PRIMARY_PUSHED: RouteFact = RouteFact::new(
    RouteFamily::PrimaryOrder,
    RouteOutcome::Pushed,
    RouteReason::PrimaryOrderLimitStopProven,
);

const fn metadata(
    provider: EligibleProvider,
    evidence_strength: EvidenceStrength,
    route: RouteExpectation,
) -> ScenarioMetadata {
    ScenarioMetadata {
        contract_features: &["select.scalar_rows"],
        provider_id: "test.provider",
        provider,
        evidence_strength,
        statement: StatementFamily::Select,
        shape: QueryShape::Scalar,
        value_type: ValueTypeFamily::Numeric,
        nullability: NullabilityClass::NonNullable,
        predicate: PredicateFamily::PrimaryKey,
        window: WindowSpec::ordered(1, 0, "id ASC"),
        mutation: MutationKind::None,
        row_order: RowOrder::Ordered,
        route,
        required_route: Some(PRIMARY_PUSHED),
        expected: ExpectedAcceptance::Accepted,
    }
}

fn scenario(
    key: &str,
    provider: EligibleProvider,
    evidence_strength: EvidenceStrength,
    route: RouteExpectation,
) -> CorrectnessScenario<()> {
    CorrectnessScenario {
        key: key.to_string(),
        surface: (),
        family: "select.primary".to_string(),
        sql: "SELECT id FROM T ORDER BY id ASC LIMIT 1".to_string(),
        metadata: metadata(provider, evidence_strength, route),
    }
}

fn one_row(value: i128, row_order: RowOrder) -> NormalizedResult {
    NormalizedResult {
        columns: vec!["id".to_string()],
        rows: vec![vec![NormalizedCell::Int(value)]],
        row_order,
    }
}

fn passing_observation() -> CorrectnessObservation {
    CorrectnessObservation {
        subject: ObservedOutcome::Accepted(one_row(1, RowOrder::Ordered)),
        provider: Some(ObservedOutcome::Accepted(one_row(1, RowOrder::Ordered))),
        route: Some(PRIMARY_PUSHED),
    }
}

#[test]
fn typed_route_classification_uses_declared_facts_not_sql_text() {
    let primary = RouteExpectation::PrimaryOrder {
        candidate_reason: RouteReason::PrimaryOrderCandidate,
        residual_filter: false,
    };
    let pushed = primary.classify(
        WindowSpec::ordered(2, 1, "id ASC"),
        RouteObservation {
            data_store_get_calls: 4,
            ..RouteObservation::default()
        },
    );
    assert_eq!(pushed, PRIMARY_PUSHED);

    let residual = RouteExpectation::PrimaryOrder {
        candidate_reason: RouteReason::PrimaryOrderCandidate,
        residual_filter: true,
    }
    .classify(
        WindowSpec::ordered(2, 0, "id ASC"),
        RouteObservation::default(),
    );
    assert_eq!(
        residual,
        RouteFact::new(
            RouteFamily::ResidualFilterOrderedScan,
            RouteOutcome::ResidualUnbounded,
            RouteReason::ResidualFilterRequiresCandidateScan,
        )
    );
}

#[test]
fn typed_normalization_preserves_order_null_bytes_and_duplicate_multiplicity() {
    let unordered_left = NormalizedResult {
        columns: vec!["value".to_string()],
        rows: vec![
            vec![NormalizedCell::Null],
            vec![NormalizedCell::Bytes(vec![0, 1])],
            vec![NormalizedCell::Null],
        ],
        row_order: RowOrder::Unordered,
    };
    let unordered_right = NormalizedResult {
        columns: vec!["value".to_string()],
        rows: vec![
            vec![NormalizedCell::Null],
            vec![NormalizedCell::Null],
            vec![NormalizedCell::Bytes(vec![0, 1])],
        ],
        row_order: RowOrder::Unordered,
    };
    assert_eq!(
        compare_normalized_results(&unordered_left, &unordered_right),
        Ok(())
    );

    let missing_duplicate = NormalizedResult {
        rows: unordered_right.rows[1..].to_vec(),
        ..unordered_right
    };
    assert!(compare_normalized_results(&unordered_left, &missing_duplicate).is_err());

    let ordered_right = NormalizedResult {
        row_order: RowOrder::Ordered,
        ..unordered_left.clone()
    };
    assert!(compare_normalized_results(&unordered_left, &ordered_right).is_err());
}

#[test]
fn correctness_and_performance_verdicts_fail_closed_independently() {
    let scenario = scenario(
        "admitted.failure",
        EligibleProvider::SqliteReference,
        EvidenceStrength::ReferenceOracle,
        RouteExpectation::Fixed(PRIMARY_PUSHED),
    );
    assert_eq!(
        correctness_verdict(&scenario, &passing_observation()),
        CorrectnessVerdict::Passed
    );

    let rejected = CorrectnessObservation {
        subject: ObservedOutcome::Rejected(DiagnosticFact {
            error_code: 1,
            diagnostic_code: 2,
        }),
        provider: None,
        route: None,
    };
    let failed = correctness_verdict(&scenario, &rejected);
    let CorrectnessVerdict::Failed(failure) = &failed else {
        panic!("an admitted rejection must fail correctness")
    };
    assert_eq!(failure.signature.owner, FailureOwner::Product);
    assert_eq!(failure.signature.category, MismatchCategory::Acceptance);
    assert!(matches!(
        performance_verdict(&failed, MeasurementStatus::Comparable),
        PerformanceVerdict::Failed(PerformanceFailure::CorrectnessFailed(_))
    ));
    assert_eq!(
        performance_verdict(&CorrectnessVerdict::Passed, MeasurementStatus::Missing),
        PerformanceVerdict::Failed(PerformanceFailure::MissingMeasurement)
    );

    let harness_failure = CorrectnessObservation {
        subject: ObservedOutcome::HarnessFailure(HarnessFailureKind::Rendering),
        provider: None,
        route: None,
    };
    assert!(matches!(
        correctness_verdict(&scenario, &harness_failure),
        CorrectnessVerdict::Failed(_)
    ));

    let mut rejected_scenario = scenario;
    rejected_scenario.metadata.expected = ExpectedAcceptance::Rejected {
        error_code: 7,
        diagnostic_code: 11,
    };
    let expected_rejection = CorrectnessObservation {
        subject: ObservedOutcome::Rejected(DiagnosticFact {
            error_code: 7,
            diagnostic_code: 11,
        }),
        provider: None,
        route: None,
    };
    assert_eq!(
        correctness_verdict(&rejected_scenario, &expected_rejection),
        CorrectnessVerdict::Passed
    );
    let wrong_typed_cause = CorrectnessObservation {
        subject: ObservedOutcome::Rejected(DiagnosticFact {
            error_code: 8,
            diagnostic_code: 11,
        }),
        ..expected_rejection
    };
    let CorrectnessVerdict::Failed(failure) =
        correctness_verdict(&rejected_scenario, &wrong_typed_cause)
    else {
        panic!("a different typed error cause must fail correctness")
    };
    assert_eq!(failure.signature.category, MismatchCategory::TypedError);
    assert_eq!(failure.signature.expected_error_code, Some(7));
    assert_eq!(failure.signature.observed_error_code, Some(8));
}

#[test]
fn stratified_selection_is_order_invariant_and_preserves_every_declared_stratum() {
    let scenarios = vec![
        scenario(
            "z.reference",
            EligibleProvider::SqliteReference,
            EvidenceStrength::ReferenceOracle,
            RouteExpectation::Fixed(PRIMARY_PUSHED),
        ),
        scenario(
            "a.contract",
            EligibleProvider::IcyDbContractOnly,
            EvidenceStrength::ContractAssertion,
            RouteExpectation::Fixed(RouteFact::new(
                RouteFamily::GroupedAggregate,
                RouteOutcome::UnchangedOrNotApplicable,
                RouteReason::GroupedAggregateOwnsExecution,
            )),
        ),
        scenario(
            "m.boundary",
            EligibleProvider::RejectionInvariant,
            EvidenceStrength::BoundaryAssertion,
            RouteExpectation::Fixed(RouteFact::new(
                RouteFamily::UnsupportedAccessKind,
                RouteOutcome::Unsupported,
                RouteReason::OrderExpressionNotClassified,
            )),
        ),
    ];
    let selected = select_stratified(&scenarios, 3).expect("three cases cover all strata");
    let selected_ids = selected
        .iter()
        .map(|scenario| scenario.key.as_str())
        .collect::<Vec<_>>();
    let mut reversed_scenarios = scenarios.clone();
    reversed_scenarios.reverse();
    let reversed = select_stratified(&reversed_scenarios, 3).expect("input order must not matter");
    assert_eq!(
        selected_ids,
        reversed
            .iter()
            .map(|scenario| scenario.key.as_str())
            .collect::<Vec<_>>()
    );

    let selected_strata = selected
        .iter()
        .flat_map(|scenario| {
            [
                ScenarioStratum::Provider(scenario.metadata.provider),
                ScenarioStratum::EvidenceStrength(scenario.metadata.evidence_strength),
                ScenarioStratum::Route(scenario.metadata.route.family()),
            ]
        })
        .collect::<BTreeSet<_>>();
    assert!(selected_strata.contains(&ScenarioStratum::Provider(
        EligibleProvider::SqliteReference
    )));
    assert!(selected_strata.contains(&ScenarioStratum::Provider(
        EligibleProvider::IcyDbContractOnly
    )));
    assert!(selected_strata.contains(&ScenarioStratum::Provider(
        EligibleProvider::RejectionInvariant
    )));

    assert!(matches!(
        select_stratified(&scenarios, 1),
        Err(SelectionError::InsufficientBudget { .. })
    ));
}
