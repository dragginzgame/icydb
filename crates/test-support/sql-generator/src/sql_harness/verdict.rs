//! Module: sql_harness::verdict
//!
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
pub struct DiagnosticFact {
    /// Stable public error code.
    pub error_code: u16,

    /// Stable diagnostic cause code.
    pub diagnostic_code: u16,
}

///
/// HarnessFailureKind
///
/// Failure class produced by test infrastructure rather than the product under test.
/// Owned by the verdict layer and reported independently from product failures.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HarnessFailureKind {
    /// A required test artifact is missing or invalid.
    Artifact,
    /// The test fixture could not be prepared or admitted.
    Fixture,
    /// The test infrastructure failed independently of the product result.
    Infrastructure,
    /// The declared scenario could not be rendered for execution.
    Rendering,
    /// The harness reached its execution timeout.
    Timeout,
}

///
/// ObservedOutcome
///
/// Typed execution, rejection, or harness outcome observed for one scenario endpoint.
/// Owned by the verdict layer and constructed by correctness-aware runners.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservedOutcome {
    /// The provider accepted the statement and returned a normalized result.
    Accepted(NormalizedResult),
    /// The provider rejected the statement with a typed diagnostic.
    Rejected(DiagnosticFact),
    /// The harness failed before producing a comparable product result.
    HarnessFailure(HarnessFailureKind),
}

///
/// CorrectnessObservation
///
/// Subject, reference-provider, and route facts observed for one scenario.
/// Owned by the verdict layer and populated by correctness-aware runners.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrectnessObservation {
    /// Outcome produced by IcyDB under test.
    pub subject: ObservedOutcome,

    /// Outcome produced by the declared reference provider, when required.
    pub provider: Option<ObservedOutcome>,

    /// Route fact observed for the IcyDB execution, when available.
    pub route: Option<RouteFact>,
}

///
/// FailureOwner
///
/// Layer responsible for a correctness mismatch or harness failure.
/// Owned by the verdict layer and emitted as a stable report identity.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureOwner {
    /// The failure is attributed to scenario rendering.
    HarnessRendering,
    /// The test infrastructure failed independently of the product result.
    Infrastructure,
    /// The failure is attributed to the product under test.
    Product,
    /// The failure is attributed to the reference-provider adapter.
    ReferenceAdapter,
    /// The available evidence does not establish the failure's owner.
    Unresolved,
}

impl FailureOwner {
    /// Return the stable report code for this failure owner.
    #[must_use]
    pub const fn code(self) -> &'static str {
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
pub enum MismatchCategory {
    /// Observed acceptance differs from the scenario's declared outcome.
    Acceptance,
    /// A harness failure prevents comparison.
    Harness,
    /// A required reference-provider observation is absent.
    MissingProvider,
    /// The observed ordering differs from the declared contract.
    Ordering,
    /// The observed execution route differs from the declared route.
    Route,
    /// The compared rows contain different column counts.
    RowShape,
    /// The observed typed rejection differs from the expected rejection.
    TypedError,
    /// The compared values differ.
    Value,
}

impl MismatchCategory {
    /// Return the stable report code for this mismatch category.
    #[must_use]
    pub const fn code(self) -> &'static str {
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
pub struct MismatchSignature {
    /// Stable scenario identity.
    pub scenario_id: String,

    /// Stable identity of the declared reference or invariant provider.
    pub provider_id: &'static str,

    /// Layer attributed as the mismatch owner.
    pub owner: FailureOwner,

    /// Semantic mismatch category.
    pub category: MismatchCategory,

    /// Expected public error code, when rejection was expected.
    pub expected_error_code: Option<u16>,

    /// Observed public error code, when the subject rejected the scenario.
    pub observed_error_code: Option<u16>,

    /// Expected diagnostic cause code, when rejection was expected.
    pub expected_diagnostic: Option<u16>,

    /// Observed diagnostic cause code, when the subject rejected the scenario.
    pub observed_diagnostic: Option<u16>,

    /// Route required by the scenario contract, when applicable.
    pub expected_route: Option<RouteFact>,

    /// Route observed during execution, when available.
    pub observed_route: Option<RouteFact>,
}

///
/// CorrectnessFailure
///
/// Failed correctness verdict carrying its reproducible mismatch signature.
/// Owned by the verdict layer and embedded in correctness and performance failures.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrectnessFailure {
    /// Stable typed identity of the failure.
    pub signature: MismatchSignature,
}

///
/// CorrectnessVerdict
///
/// Fail-closed semantic verdict for one SQL correctness scenario.
/// Owned by the verdict layer and consumed before performance evidence is accepted.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CorrectnessVerdict {
    /// All applicable correctness comparisons passed.
    Passed,
    /// A typed correctness failure identifies the failed comparison.
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
pub fn correctness_verdict<S>(
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
