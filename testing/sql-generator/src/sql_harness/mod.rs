//! Module: sql_harness
//!
//! Responsibility: shared typed scenario, normalization, selection, and verdict contracts.
//! Does not own: product SQL semantics, production planning, or runner-specific execution.
//! Boundary: lets SQL evidence runners share facts without deriving authority from SQL text.

mod model;
mod normalization;
mod selection;
mod verdict;

#[cfg(test)]
mod tests;

pub use crate::{
    EligibleProvider, EvidenceStrength, MutationKind, NullabilityClass, PredicateFamily,
    QueryShape, RouteFamily, StatementFamily, ValueTypeFamily, WindowBehavior,
};
pub use model::{
    CorrectnessScenario, ExpectedAcceptance, RouteExpectation, RouteFact, RouteObservation,
    RouteOutcome, RouteReason, RowOrder, ScenarioMetadata, WindowSpec,
};
pub use normalization::{
    NormalizationMismatchKind, NormalizedCell, NormalizedResult, compare_normalized_results,
};
pub use selection::select_stratified;
pub use selection::{ScenarioStratum, SelectionError};
pub use verdict::{
    CorrectnessObservation, CorrectnessVerdict, DiagnosticFact, ObservedOutcome,
    correctness_verdict,
};
pub use verdict::{FailureOwner, HarnessFailureKind, MismatchCategory};
