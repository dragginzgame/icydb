//! Module: icydb_testing_sql_generator
//! Responsibility: typed deterministic SQL generation, replay, and shrinking.
//! Does not own: IcyDB parsing, planning, execution, or reference-engine semantics.
//! Boundary: produces bounded test inputs from accepted-snapshot facts before SQL rendering.

mod error;
mod fixture;
mod generator;
mod model;
mod mutation;
mod replay;
mod rng;
mod shrink;

#[cfg(test)]
mod tests;

pub use error::{SqlGeneratorError, SqlGeneratorErrorKind};
pub use fixture::{GeneratedFixture, GeneratedFixtureRow, GeneratedValue};
pub use generator::{
    TIER_A_INVALID_CASES_PER_VIOLATION, TIER_A_ROOT_SEEDS, TIER_A_VALID_CASES_PER_FAMILY,
    generate_invalid_select_case, generate_valid_select_case,
};
pub use model::{
    ALL_SELECT_GENERATOR_FAMILIES, ALL_SELECT_VIOLATIONS, GeneratedSelectCase,
    GeneratedSelectIdentity, SelectBudgets, SelectExpectedOutcome, SelectExpectedRejection,
    SelectFeature, SelectField, SelectFieldKind, SelectGeneratorFamily, SelectIndex,
    SelectProvider, SelectQuery, SelectQueryShape, SelectResultOrder, SelectSnapshot,
    SelectValueKind, SelectViolation, TIER_A_SELECT_BUDGETS,
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
    TIER_A_MUTATION_CASES_PER_ROOT, TIER_A_MUTATION_ROOT_SEEDS, generate_mutation_sequence,
    shrink_mutation_failure,
};
pub use replay::{
    SELECT_REPLAY_FORMAT_VERSION, SelectExecutionPhase, SelectMismatchCategory,
    SelectMismatchSignature, SelectObservedOutcome, SelectReplayRecord,
};
pub use rng::SELECT_GENERATOR_VERSION;
pub use shrink::{SelectShrinkReport, shrink_select_failure};
