//! Module: sql_generator::mutation
//! Responsibility: typed deterministic DML sequences, independent state transitions, replay, and shrinking.
//! Does not own: IcyDB mutation execution or SQLite reference semantics.
//! Boundary: derives bounded current-contract SQL from accepted-snapshot facts and models every step atomically.

mod generator;
mod model;
mod replay;
mod shrink;

#[cfg(test)]
mod tests;

pub use generator::{
    MUTATION_GENERATOR_VERSION, TIER_A_MUTATION_REPETITIONS, TIER_C_MUTATION_REPETITIONS,
    generate_scheduled_mutation_sequence,
};
pub use model::{
    GeneratedMutationIdentity, GeneratedMutationSequence, GeneratedMutationStep,
    MutationAssignment, MutationBudgets, MutationDefaultValue, MutationExpectedRejection,
    MutationField, MutationFieldKind, MutationFieldRole, MutationIndexEntry, MutationIngress,
    MutationInsertQueryKeySource, MutationInsertRow, MutationIntentClass, MutationIntentKind,
    MutationOperation, MutationOrder, MutationPredicate, MutationProjectedField,
    MutationProjectedRow, MutationReturning, MutationRow, MutationRowPayload,
    MutationSchemaProfile, MutationSnapshot, MutationSqliteEligibility, MutationSqliteExclusion,
    MutationStatement, MutationStepOutcome, MutationUpdateIntent, MutationValue, MutationWindow,
    MutationWriteIntent, TIER_A_MUTATION_BUDGETS, TIER_C_MUTATION_BUDGETS,
};
pub use replay::{
    MUTATION_REPLAY_FORMAT_VERSION, MutationExecutionPhase, MutationFeature,
    MutationMismatchCategory, MutationMismatchSignature, MutationObservedOutcome,
    MutationReplayRecord,
};
pub use shrink::{MutationShrinkReport, shrink_mutation_failure};
