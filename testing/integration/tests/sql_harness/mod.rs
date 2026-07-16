//! Module: sql_harness
//! Responsibility: shared typed scenario, normalization, selection, and verdict contracts.
//! Does not own: product SQL semantics, production planning, or runner-specific execution.
//! Boundary: lets SQL evidence runners share facts without deriving authority from SQL text.

mod model;
mod normalization;
mod selection;
mod verdict;

#[cfg(test)]
mod tests;

pub(crate) use icydb_testing_sql_generator::{
    EligibleProvider, EvidenceStrength, MutationKind, NullabilityClass, PredicateFamily,
    QueryShape, RouteFamily, StatementFamily, ValueTypeFamily, WindowBehavior,
};
pub(crate) use model::EvidenceClass;
pub(crate) use model::{
    CorrectnessScenario, ExpectedAcceptance, RouteExpectation, RouteFact, RouteObservation,
    RouteOutcome, RouteReason, RowOrder, ScenarioMetadata, WindowSpec,
};
pub(crate) use normalization::{
    NormalizationMismatchKind, NormalizedCell, NormalizedResult, compare_normalized_results,
};
#[allow(
    unused_imports,
    reason = "shared harness targets consume different scenario-selection subsets"
)]
pub(crate) use selection::select_stratified;
pub(crate) use selection::{ScenarioStratum, SelectionError};
pub(crate) use verdict::{
    CorrectnessObservation, CorrectnessVerdict, DiagnosticFact, ObservedOutcome,
    correctness_verdict,
};
pub(crate) use verdict::{
    FailureOwner, HarnessFailureKind, MeasurementStatus, MismatchCategory, PerformanceFailure,
    PerformanceVerdict, performance_verdict,
};
