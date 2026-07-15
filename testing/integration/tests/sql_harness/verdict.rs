//! Module: sql_harness::verdict
//! Responsibility: fail-closed correctness and performance verdict attribution.
//! Does not own: scenario intent, SQL execution, result normalization, or route observation.
//! Boundary: turns typed expectations and observations into stable pass or failure identities.

use crate::sql_harness::{
    CorrectnessScenario, EligibleProvider, ExpectedAcceptance, NormalizationMismatchKind,
    NormalizedResult, RouteFact, compare_normalized_results,
};

///
/// DiagnosticFact
///
/// Typed public error and diagnostic identity observed for a rejected scenario.
/// Owned by the verdict layer and populated by correctness-aware runners.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DiagnosticFact {
    /// Stable public error code.
    pub(crate) error_code: u16,

    /// Stable diagnostic cause code.
    pub(crate) diagnostic_code: u16,
}

///
/// HarnessFailureKind
///
/// Failure class produced by test infrastructure rather than the product under test.
/// Owned by the verdict layer and reported independently from product failures.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HarnessFailureKind {
    Artifact,
    Fixture,
    Infrastructure,
    Rendering,
    Timeout,
}

///
/// ObservedOutcome
///
/// Typed execution, rejection, or harness outcome observed for one scenario endpoint.
/// Owned by the verdict layer and constructed by correctness-aware runners.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ObservedOutcome {
    Accepted(NormalizedResult),
    Rejected(DiagnosticFact),
    HarnessFailure(HarnessFailureKind),
}

///
/// CorrectnessObservation
///
/// Subject, reference-provider, and route facts observed for one scenario.
/// Owned by the verdict layer and populated by correctness-aware runners.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CorrectnessObservation {
    /// Outcome produced by IcyDB under test.
    pub(crate) subject: ObservedOutcome,

    /// Outcome produced by the declared reference provider, when required.
    pub(crate) provider: Option<ObservedOutcome>,

    /// Route fact observed for the IcyDB execution, when available.
    pub(crate) route: Option<RouteFact>,
}

///
/// FailureOwner
///
/// Layer responsible for a correctness mismatch or harness failure.
/// Owned by the verdict layer and emitted as a stable report identity.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FailureOwner {
    HarnessRendering,
    Infrastructure,
    Product,
    ReferenceAdapter,
    Unresolved,
}

impl FailureOwner {
    /// Return the stable report code for this failure owner.
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::HarnessRendering => "harness_rendering_failure",
            Self::Infrastructure => "infrastructure_failure",
            Self::Product => "product_failure",
            Self::ReferenceAdapter => "reference_adapter_failure",
            Self::Unresolved => "unresolved_failure",
        }
    }
}

///
/// MismatchCategory
///
/// Semantic category of a failed correctness comparison.
/// Owned by the verdict layer and used in stable mismatch signatures.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MismatchCategory {
    Acceptance,
    Harness,
    MissingProvider,
    Ordering,
    Route,
    RowShape,
    TypedError,
    Value,
}

impl MismatchCategory {
    /// Return the stable report code for this mismatch category.
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::Acceptance => "acceptance",
            Self::Harness => "harness",
            Self::MissingProvider => "missing_provider",
            Self::Ordering => "ordering",
            Self::Route => "route",
            Self::RowShape => "row_shape",
            Self::TypedError => "typed_error",
            Self::Value => "value",
        }
    }
}

///
/// MismatchSignature
///
/// Reproducible typed identity for one failed correctness comparison.
/// Owned by the verdict layer and serialized by correctness and performance reports.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MismatchSignature {
    /// Stable scenario identity.
    pub(crate) scenario_id: String,

    /// Stable identity of the declared reference or invariant provider.
    pub(crate) provider_id: &'static str,

    /// Layer attributed as the mismatch owner.
    pub(crate) owner: FailureOwner,

    /// Semantic mismatch category.
    pub(crate) category: MismatchCategory,

    /// Expected public error code, when rejection was expected.
    pub(crate) expected_error_code: Option<u16>,

    /// Observed public error code, when the subject rejected the scenario.
    pub(crate) observed_error_code: Option<u16>,

    /// Expected diagnostic cause code, when rejection was expected.
    pub(crate) expected_diagnostic: Option<u16>,

    /// Observed diagnostic cause code, when the subject rejected the scenario.
    pub(crate) observed_diagnostic: Option<u16>,

    /// Route required by the scenario contract, when applicable.
    pub(crate) expected_route: Option<RouteFact>,

    /// Route observed during execution, when available.
    pub(crate) observed_route: Option<RouteFact>,
}

///
/// CorrectnessFailure
///
/// Failed correctness verdict carrying its reproducible mismatch signature.
/// Owned by the verdict layer and embedded in correctness and performance failures.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CorrectnessFailure {
    /// Stable typed identity of the failure.
    pub(crate) signature: MismatchSignature,
}

///
/// CorrectnessVerdict
///
/// Fail-closed semantic verdict for one SQL correctness scenario.
/// Owned by the verdict layer and consumed before performance evidence is accepted.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CorrectnessVerdict {
    Passed,
    Failed(CorrectnessFailure),
}

/// Build a typed failed verdict with the scenario's stable expected facts.
fn failure<S>(
    scenario: &CorrectnessScenario<S>,
    owner: FailureOwner,
    category: MismatchCategory,
) -> CorrectnessVerdict {
    CorrectnessVerdict::Failed(CorrectnessFailure {
        signature: MismatchSignature {
            scenario_id: scenario.key.clone(),
            provider_id: scenario.metadata.provider_id,
            owner,
            category,
            expected_error_code: None,
            observed_error_code: None,
            expected_diagnostic: None,
            observed_diagnostic: None,
            expected_route: scenario.metadata.required_route,
            observed_route: None,
        },
    })
}

/// Return whether this provider class requires a distinct reference observation.
const fn provider_is_required(provider: EligibleProvider) -> bool {
    matches!(
        provider,
        EligibleProvider::SqliteReference | EligibleProvider::StateModelReference
    )
}

/// Return a product-owned route failure when required and observed routes differ.
fn route_failure<S>(
    scenario: &CorrectnessScenario<S>,
    observation: &CorrectnessObservation,
) -> Option<CorrectnessVerdict> {
    let expected_route = scenario.metadata.required_route?;
    if observation.route == Some(expected_route) {
        return None;
    }

    let mut verdict = failure(scenario, FailureOwner::Product, MismatchCategory::Route);
    if let CorrectnessVerdict::Failed(failure) = &mut verdict {
        failure.signature.observed_route = observation.route;
    }
    Some(verdict)
}

/// Compare required reference-provider output and attribute any adapter or product failure.
fn provider_failure<S>(
    scenario: &CorrectnessScenario<S>,
    subject_result: &NormalizedResult,
    provider: Option<&ObservedOutcome>,
) -> Option<CorrectnessVerdict> {
    if !provider_is_required(scenario.metadata.provider) {
        return None;
    }
    let Some(provider_outcome) = provider else {
        return Some(failure(
            scenario,
            FailureOwner::ReferenceAdapter,
            MismatchCategory::MissingProvider,
        ));
    };
    let ObservedOutcome::Accepted(provider_result) = provider_outcome else {
        return Some(failure(
            scenario,
            FailureOwner::ReferenceAdapter,
            MismatchCategory::Acceptance,
        ));
    };
    let mismatch = compare_normalized_results(subject_result, provider_result).err()?;
    let category = match mismatch.kind {
        NormalizationMismatchKind::OrderingContract => MismatchCategory::Ordering,
        NormalizationMismatchKind::ColumnShape
        | NormalizationMismatchKind::RowCount
        | NormalizationMismatchKind::RowShape => MismatchCategory::RowShape,
        NormalizationMismatchKind::Value => MismatchCategory::Value,
    };
    Some(failure(scenario, FailureOwner::Product, category))
}

/// Derive a fail-closed correctness verdict from typed scenario and observation facts.
pub(crate) fn correctness_verdict<S>(
    scenario: &CorrectnessScenario<S>,
    observation: &CorrectnessObservation,
) -> CorrectnessVerdict {
    let subject_result = match (&scenario.metadata.expected, &observation.subject) {
        (ExpectedAcceptance::Accepted, ObservedOutcome::Accepted(result)) => result,
        (ExpectedAcceptance::Accepted, ObservedOutcome::Rejected(diagnostic)) => {
            let mut verdict = failure(
                scenario,
                FailureOwner::Product,
                MismatchCategory::Acceptance,
            );
            if let CorrectnessVerdict::Failed(failure) = &mut verdict {
                failure.signature.observed_error_code = Some(diagnostic.error_code);
                failure.signature.observed_diagnostic = Some(diagnostic.diagnostic_code);
            }
            return verdict;
        }
        (
            ExpectedAcceptance::Rejected {
                error_code,
                diagnostic_code,
            },
            ObservedOutcome::Rejected(observed),
        ) if *error_code == observed.error_code && *diagnostic_code == observed.diagnostic_code => {
            return CorrectnessVerdict::Passed;
        }
        (
            ExpectedAcceptance::Rejected {
                error_code,
                diagnostic_code,
            },
            ObservedOutcome::Rejected(observed),
        ) => {
            let mut verdict = failure(
                scenario,
                FailureOwner::Product,
                MismatchCategory::TypedError,
            );
            if let CorrectnessVerdict::Failed(failure) = &mut verdict {
                failure.signature.expected_error_code = Some(*error_code);
                failure.signature.observed_error_code = Some(observed.error_code);
                failure.signature.expected_diagnostic = Some(*diagnostic_code);
                failure.signature.observed_diagnostic = Some(observed.diagnostic_code);
            }
            return verdict;
        }
        (ExpectedAcceptance::Rejected { .. }, ObservedOutcome::Accepted(_)) => {
            return failure(
                scenario,
                FailureOwner::Product,
                MismatchCategory::Acceptance,
            );
        }
        (_, ObservedOutcome::HarnessFailure(kind)) => {
            let owner = match kind {
                HarnessFailureKind::Rendering => FailureOwner::HarnessRendering,
                HarnessFailureKind::Artifact
                | HarnessFailureKind::Fixture
                | HarnessFailureKind::Infrastructure
                | HarnessFailureKind::Timeout => FailureOwner::Infrastructure,
            };
            return failure(scenario, owner, MismatchCategory::Harness);
        }
    };

    if let Some(verdict) = route_failure(scenario, observation) {
        return verdict;
    }
    if let Some(verdict) = provider_failure(scenario, subject_result, observation.provider.as_ref())
    {
        return verdict;
    }

    CorrectnessVerdict::Passed
}

///
/// MeasurementStatus
///
/// Availability and comparison status of one performance measurement.
/// Owned by the verdict layer and supplied by performance report generation.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MeasurementStatus {
    Comparable,
    Incomparable,
    Missing,
    Regression,
}

///
/// PerformanceFailure
///
/// Fail-closed reason that performance evidence cannot pass.
/// Owned by the verdict layer and derived only after correctness is evaluated.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PerformanceFailure {
    CorrectnessFailed(CorrectnessFailure),

    IncomparableMeasurement,

    MissingMeasurement,

    Regression,
}

///
/// PerformanceVerdict
///
/// Performance acceptance verdict gated by correctness and measurement status.
/// Owned by the verdict layer and consumed by performance report generation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PerformanceVerdict {
    Passed,
    Failed(PerformanceFailure),
}

/// Derive a fail-closed performance verdict after correctness has been established.
pub(crate) fn performance_verdict(
    correctness: &CorrectnessVerdict,
    measurement: MeasurementStatus,
) -> PerformanceVerdict {
    if let CorrectnessVerdict::Failed(failure) = correctness {
        return PerformanceVerdict::Failed(PerformanceFailure::CorrectnessFailed(failure.clone()));
    }

    match measurement {
        MeasurementStatus::Comparable => PerformanceVerdict::Passed,
        MeasurementStatus::Incomparable => {
            PerformanceVerdict::Failed(PerformanceFailure::IncomparableMeasurement)
        }
        MeasurementStatus::Missing => {
            PerformanceVerdict::Failed(PerformanceFailure::MissingMeasurement)
        }
        MeasurementStatus::Regression => PerformanceVerdict::Failed(PerformanceFailure::Regression),
    }
}
