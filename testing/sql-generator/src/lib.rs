//! Module: icydb_testing_sql_generator
//! Responsibility: typed deterministic SQL generation, replay, and shrinking.
//! Does not own: IcyDB parsing, planning, execution, or reference-engine semantics.
//! Boundary: produces bounded test inputs from accepted-snapshot facts before SQL rendering.

mod corpus;
mod corpus_inventory;
mod coverage;
mod error;
mod failure;
mod fixture;
mod generator;
mod model;
mod mutation;
mod replay;
mod rng;
mod scheduled;
mod shard;
mod shrink;

#[cfg(test)]
mod tests;

pub use corpus::{
    REGRESSION_CORPUS_FORMAT_VERSION, REGRESSION_CORPUS_MAX_ENTRY_BYTES, RegressionCorpusCase,
    RegressionCorpusEntry,
};
pub use corpus_inventory::{RegressionCorpusInventoryError, checked_in_regression_corpus};
pub use coverage::{
    EligibleProvider, EvidenceStrength, GeneratedExpressionDepth, MutationKind, NullabilityClass,
    PredicateFamily, QueryShape, RouteFamily, StatementFamily, TIER_C_DISTRIBUTION_FORMAT_VERSION,
    TierCCoverageDistributionReport, TierCCoverageLabels, TierCDistributionError,
    TierCExpectedAcceptance, TierCScenarioDeclaration, ValueTypeFamily, WindowBehavior,
    generated_mutation_tier_c_declaration, generated_select_tier_c_declaration,
};
pub use error::{SqlGeneratorError, SqlGeneratorErrorKind};
pub use failure::{
    TIER_C_FAILURE_ARTIFACT_FORMAT_VERSION, TierCFailureArtifact, TierCFailureArtifactError,
    TierCFailureReplay, is_valid_tier_c_failure_artifact_id,
};
pub use fixture::{GeneratedFixture, GeneratedFixtureRow, GeneratedValue};
pub use generator::{
    TIER_A_INVALID_CASES_PER_VIOLATION, TIER_A_ROOT_SEEDS, TIER_A_VALID_CASES_PER_FAMILY,
    TIER_C_INVALID_CASES_PER_VIOLATION, TIER_C_ROOT_SEEDS, TIER_C_VALID_CASES_PER_FAMILY,
    generate_invalid_select_case, generate_valid_select_case,
};
pub use model::{
    ALL_SELECT_GENERATOR_FAMILIES, ALL_SELECT_VIOLATIONS, GeneratedSelectCase,
    GeneratedSelectIdentity, SelectBudgets, SelectExpectedOutcome, SelectExpectedRejection,
    SelectFeature, SelectField, SelectFieldKind, SelectGeneratorFamily, SelectIndex,
    SelectProvider, SelectQuery, SelectQueryShape, SelectResultOrder, SelectSnapshot,
    SelectValueKind, SelectViolation, TIER_A_SELECT_BUDGETS, TIER_C_SELECT_BUDGETS,
};
pub use mutation::{
    GeneratedMutationIdentity, GeneratedMutationSequence, GeneratedMutationStep,
    MUTATION_GENERATOR_VERSION, MUTATION_REPLAY_FORMAT_VERSION, MutationAssignment,
    MutationBudgets, MutationExecutionPhase, MutationExpectedRejection, MutationFeature,
    MutationField, MutationFieldKind, MutationFieldRole, MutationInsertQueryKeySource,
    MutationMismatchCategory, MutationMismatchSignature, MutationObservedOutcome,
    MutationOperation, MutationOrder, MutationPredicate, MutationReplayRecord, MutationRow,
    MutationShrinkReport, MutationSnapshot, MutationSqliteEligibility, MutationSqliteExclusion,
    MutationStatement, MutationStepOutcome, MutationWindow, TIER_A_MUTATION_BUDGETS,
    TIER_A_MUTATION_CASES_PER_ROOT, TIER_C_MUTATION_BUDGETS, TIER_C_MUTATION_CASES_PER_ROOT,
    generate_mutation_sequence, shrink_mutation_failure,
};
pub use replay::{
    SELECT_REPLAY_FORMAT_VERSION, SelectComparisonProvider, SelectExecutionPhase,
    SelectMismatchCategory, SelectMismatchSignature, SelectObservedOutcome, SelectReplayRecord,
};
pub use rng::SELECT_GENERATOR_VERSION;
pub use scheduled::{
    TIER_C_EVIDENCE_FORMAT_VERSION, TIER_C_EVIDENCE_MAX_ARTIFACT_BYTES,
    TIER_C_SQL_COVERAGE_MANIFEST_REVISION, TierCEvidenceError, TierCMergedReport,
    TierCScenarioObservation, TierCScenarioOutcome, TierCShardReport,
};
pub use shard::{SQL_SCHEDULED_SHARD_COUNT, ScenarioShardError, scheduled_sql_scenario_shard};
pub use shrink::{SelectShrinkReport, shrink_select_failure};
