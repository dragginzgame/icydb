//! Module: db::session::tests::mutation_reference
//! Responsibility: required native IcyDB mutation-state evidence against the independent model and eligible SQLite overlap.
//! Does not own: mutation generation, accepted schema authority, product execution, or SQLite eligibility.
//! Boundary: executes each sequence end to end and compares typed response, rejection, state, and atomicity after every step.

use super::*;
use crate::db::schema::{AcceptedFieldKind, AcceptedSchemaSnapshot};
use icydb_testing_sql_generator::{
    GeneratedMutationSequence, GeneratedMutationStep, MutationExecutionPhase,
    MutationExpectedRejection, MutationFeature, MutationField, MutationFieldKind,
    MutationFieldRole, MutationMismatchCategory, MutationMismatchSignature,
    MutationObservedOutcome, MutationOperation, MutationRow, MutationSnapshot,
    MutationSqliteEligibility, MutationStepOutcome, TIER_A_MUTATION_BUDGETS,
    TIER_A_MUTATION_CASES_PER_ROOT, TIER_A_ROOT_SEEDS, generate_mutation_sequence,
};
use icydb_testing_sqlite_reference::{MutationSqliteEvidence, execute_generated_mutation_sequence};
use std::collections::BTreeSet;

#[test]
fn tier_a_generated_mutation_sequences_match_native_state_and_eligible_sqlite() {
    reset_session_sql_store();
    let session = sql_session();
    let snapshot = generated_mutation_snapshot_from_accepted_authority(&session)
        .expect("accepted write snapshot should map into mutation generator facts");
    let mut execution_facts = MutationSequenceExecutionFacts::default();

    for root_seed in TIER_A_ROOT_SEEDS {
        for case_index in 0..TIER_A_MUTATION_CASES_PER_ROOT {
            reset_session_sql_store();
            let session = sql_session();
            let sequence = generate_mutation_sequence(
                &snapshot,
                *root_seed,
                case_index,
                TIER_A_MUTATION_BUDGETS,
            )
            .expect("Tier A mutation sequence should generate from accepted facts");
            execution_facts.merge(
                observe_generated_native_mutation_sequence(&session, &sequence)
                    .unwrap_or_else(|failure| panic!("generated mutation mismatch: {failure:?}")),
            );
        }
    }

    assert_eq!(execution_facts.sequences, 8);
    assert!(execution_facts.inserts > 0);
    assert!(execution_facts.insert_queries > 0);
    assert!(execution_facts.updates > 0);
    assert!(execution_facts.deletes > 0);
    assert!(execution_facts.returning > 0);
    assert_eq!(execution_facts.rejections, execution_facts.sequences);
    assert!(execution_facts.sqlite_compared > 0);
    assert!(execution_facts.sqlite_excluded > 0);
}

#[test]
fn generated_mutation_adapter_mismatch_is_typed_and_fingerprint_backed() {
    reset_session_sql_store();
    let session = sql_session();
    let snapshot = generated_mutation_snapshot_from_accepted_authority(&session)
        .expect("accepted write snapshot should map into mutation generator facts");
    let sequence =
        generate_mutation_sequence(&snapshot, TIER_A_ROOT_SEEDS[0], 0, TIER_A_MUTATION_BUDGETS)
            .expect("typed mutation adapter mismatch sequence should generate");
    let step = sequence
        .steps()
        .iter()
        .find(|step| matches!(step.expected(), MutationStepOutcome::Accepted { .. }))
        .expect("fixed mutation sequence should contain an accepted step");
    let MutationStepOutcome::Accepted {
        affected_rows,
        returned_rows,
        state_after,
    } = step.expected()
    else {
        unreachable!("accepted step was selected above");
    };
    let subject = MutationStepOutcome::Accepted {
        affected_rows: affected_rows.saturating_add(1),
        returned_rows: returned_rows.clone(),
        state_after: state_after.clone(),
    };
    let mismatch = GeneratedMutationMismatch::from_outcomes(
        step,
        &subject,
        step.expected(),
        "independent-model",
        "injected-adapter-mismatch",
    );
    let (subject_outcome, comparison_outcome) = mismatch.outcomes();

    assert_eq!(
        mismatch.signature().category(),
        MutationMismatchCategory::AffectedRows
    );
    assert_eq!(
        mismatch.signature().comparison_provider_id(),
        "independent-model"
    );
    assert_ne!(subject_outcome, comparison_outcome);

    let infrastructure = GeneratedMutationMismatch::from_infrastructure(
        &sequence,
        "sqlite-reference",
        "sqlite.connection",
        "sqlite-reference-execution",
        MutationObservedOutcome::infrastructure_failure(
            "icydb.not_executed",
            MutationExecutionPhase::Reference,
        ),
        MutationObservedOutcome::infrastructure_failure(
            "sqlite.connection",
            MutationExecutionPhase::Reference,
        ),
    );
    assert_eq!(
        infrastructure.signature().category(),
        MutationMismatchCategory::InternalInvariant
    );
}

///
/// MutationSequenceExecutionFacts
///
/// Test-owned typed counts proving one generated sequence exercised its declared
/// operations and independent provider overlap. They are not product metrics.
///

#[derive(Debug, Default)]
struct MutationSequenceExecutionFacts {
    deletes: u32,
    insert_queries: u32,
    inserts: u32,
    rejections: u32,
    returning: u32,
    sequences: u32,
    sqlite_compared: u32,
    sqlite_excluded: u32,
    updates: u32,
}

impl MutationSequenceExecutionFacts {
    const fn merge(&mut self, other: Self) {
        self.deletes = self.deletes.saturating_add(other.deletes);
        self.insert_queries = self.insert_queries.saturating_add(other.insert_queries);
        self.inserts = self.inserts.saturating_add(other.inserts);
        self.rejections = self.rejections.saturating_add(other.rejections);
        self.returning = self.returning.saturating_add(other.returning);
        self.sequences = self.sequences.saturating_add(other.sequences);
        self.sqlite_compared = self.sqlite_compared.saturating_add(other.sqlite_compared);
        self.sqlite_excluded = self.sqlite_excluded.saturating_add(other.sqlite_excluded);
        self.updates = self.updates.saturating_add(other.updates);
    }
}

///
/// GeneratedMutationMismatch
///
/// Typed generated mutation failure spanning provider outcomes, typed
/// rejections, setup, row state, and execution invariants. Scheduled execution
/// shrinks this exact signature before constructing replay evidence.
///

#[derive(Clone, Debug)]
pub(super) struct GeneratedMutationMismatch {
    comparison_outcome: MutationObservedOutcome,
    signature: MutationMismatchSignature,
    subject_outcome: MutationObservedOutcome,
}

impl GeneratedMutationMismatch {
    /// Borrow the exact structured mismatch identity preserved by shrinking.
    pub(super) const fn signature(&self) -> &MutationMismatchSignature {
        &self.signature
    }

    /// Clone compact subject and comparison outcomes for replay construction.
    pub(super) fn outcomes(&self) -> (MutationObservedOutcome, MutationObservedOutcome) {
        (
            self.subject_outcome.clone(),
            self.comparison_outcome.clone(),
        )
    }

    fn from_fixture_error(sequence: &GeneratedMutationSequence, error: &InternalError) -> Self {
        let error_class_id = internal_error_id(error);
        Self::from_infrastructure(
            sequence,
            "independent-model",
            error_class_id.as_str(),
            "native-fixture-setup",
            MutationObservedOutcome::infrastructure_failure(
                error_class_id.clone(),
                MutationExecutionPhase::Reference,
            ),
            MutationObservedOutcome::infrastructure_failure(
                "model.not_executed",
                MutationExecutionPhase::Reference,
            ),
        )
    }

    fn from_reference_error(
        sequence: &GeneratedMutationSequence,
        error: &icydb_testing_sqlite_reference::SqliteAdapterError,
    ) -> Self {
        let error_class_id = error.kind().id();
        Self::from_infrastructure(
            sequence,
            "sqlite-reference",
            error_class_id,
            "sqlite-reference-execution",
            MutationObservedOutcome::infrastructure_failure(
                "icydb.not_executed",
                MutationExecutionPhase::Reference,
            ),
            MutationObservedOutcome::infrastructure_failure(
                error_class_id,
                MutationExecutionPhase::Reference,
            ),
        )
    }

    fn from_infrastructure(
        sequence: &GeneratedMutationSequence,
        comparison_provider_id: &str,
        error_class_id: &str,
        invariant_class_id: &str,
        subject_outcome: MutationObservedOutcome,
        comparison_outcome: MutationObservedOutcome,
    ) -> Self {
        let features = sequence
            .steps()
            .first()
            .map(mutation_step_features)
            .unwrap_or_default();
        let signature = MutationMismatchSignature::try_new(
            features,
            MutationExecutionPhase::Reference,
            "icydb-native",
            comparison_provider_id,
            Some(error_class_id.to_string()),
            MutationMismatchCategory::InternalInvariant,
            Some(invariant_class_id.to_string()),
        )
        .expect("static generated mutation infrastructure identity should validate");

        Self {
            comparison_outcome,
            signature,
            subject_outcome,
        }
    }

    fn from_subject_failure(
        step: &GeneratedMutationStep,
        error_class_id: &str,
        category: MutationMismatchCategory,
        invariant_class_id: &str,
        subject_outcome: MutationObservedOutcome,
    ) -> Self {
        let signature = MutationMismatchSignature::try_new(
            mutation_step_features(step),
            MutationExecutionPhase::Execution,
            "icydb-native",
            "independent-model",
            Some(error_class_id.to_string()),
            category,
            Some(invariant_class_id.to_string()),
        )
        .expect("static generated mutation subject-failure identity should validate");

        Self {
            comparison_outcome: MutationObservedOutcome::try_from_step_outcome(step.expected())
                .expect("modeled comparison mutation outcome should fingerprint"),
            signature,
            subject_outcome,
        }
    }

    fn from_outcomes(
        step: &GeneratedMutationStep,
        subject: &MutationStepOutcome,
        comparison: &MutationStepOutcome,
        comparison_provider_id: &str,
        invariant_class_id: &str,
    ) -> Self {
        let signature = MutationMismatchSignature::try_new(
            mutation_step_features(step),
            MutationExecutionPhase::Comparison,
            "icydb-native",
            comparison_provider_id,
            mutation_error_class(subject, comparison),
            classify_mutation_mismatch(subject, comparison),
            Some(invariant_class_id.to_string()),
        )
        .expect("static generated mutation mismatch identity should validate");

        Self {
            comparison_outcome: MutationObservedOutcome::try_from_step_outcome(comparison)
                .expect("typed comparison mutation outcome should fingerprint"),
            signature,
            subject_outcome: MutationObservedOutcome::try_from_step_outcome(subject)
                .expect("typed subject mutation outcome should fingerprint"),
        }
    }
}

fn mutation_step_features(step: &GeneratedMutationStep) -> BTreeSet<MutationFeature> {
    let mut features = BTreeSet::new();
    match step.statement().operation() {
        MutationOperation::Delete { window, .. } => {
            features.insert(MutationFeature::Delete);
            if window.is_some() {
                features.insert(MutationFeature::Window);
            }
        }
        MutationOperation::Insert { rows } => {
            features.insert(MutationFeature::Insert);
            if rows.len() > 1 {
                features.insert(MutationFeature::MultiRowInsert);
            }
        }
        MutationOperation::InsertFromQuery { .. } => {
            features.insert(MutationFeature::InsertFromQuery);
        }
        MutationOperation::Update { window, .. } => {
            features.insert(MutationFeature::Update);
            if window.is_some() {
                features.insert(MutationFeature::Window);
            }
        }
    }
    if step.statement().returning() {
        features.insert(MutationFeature::Returning);
    }
    if matches!(step.expected(), MutationStepOutcome::Rejected { .. }) {
        features.insert(MutationFeature::Rejection);
    }
    features
}

fn internal_error_id(error: &InternalError) -> String {
    format!(
        "diagnostic.{:04x}",
        error.diagnostic_code().error_code().raw()
    )
}

fn mutation_error_class(
    subject: &MutationStepOutcome,
    comparison: &MutationStepOutcome,
) -> Option<String> {
    match (subject.rejection(), comparison.rejection()) {
        (Some(subject), Some(comparison)) if subject != comparison => {
            Some("mutation.rejection_class".to_string())
        }
        _ => None,
    }
}

fn classify_mutation_mismatch(
    subject: &MutationStepOutcome,
    comparison: &MutationStepOutcome,
) -> MutationMismatchCategory {
    match (subject, comparison) {
        (MutationStepOutcome::Accepted { .. }, MutationStepOutcome::Rejected { .. })
        | (MutationStepOutcome::Rejected { .. }, MutationStepOutcome::Accepted { .. }) => {
            MutationMismatchCategory::Acceptance
        }
        (
            MutationStepOutcome::Rejected {
                rejection: subject_rejection,
                ..
            },
            MutationStepOutcome::Rejected {
                rejection: comparison_rejection,
                ..
            },
        ) if subject_rejection != comparison_rejection => MutationMismatchCategory::TypedError,
        (
            MutationStepOutcome::Accepted {
                affected_rows: subject_rows,
                ..
            },
            MutationStepOutcome::Accepted {
                affected_rows: comparison_rows,
                ..
            },
        ) if subject_rows != comparison_rows => MutationMismatchCategory::AffectedRows,
        (
            MutationStepOutcome::Accepted {
                returned_rows: subject_rows,
                ..
            },
            MutationStepOutcome::Accepted {
                returned_rows: comparison_rows,
                ..
            },
        ) if subject_rows != comparison_rows => MutationMismatchCategory::ReturnedRows,
        (MutationStepOutcome::Rejected { .. }, MutationStepOutcome::Rejected { .. }) => {
            MutationMismatchCategory::Atomicity
        }
        _ => MutationMismatchCategory::PostState,
    }
}

/// Compare one generated sequence without converting a semantic mismatch into
/// a panic, so scheduled evidence can shrink it first.
pub(super) fn compare_generated_native_mutation_sequence(
    session: &DbSession<SessionSqlCanister>,
    sequence: &GeneratedMutationSequence,
) -> Result<(), Box<GeneratedMutationMismatch>> {
    observe_generated_native_mutation_sequence(session, sequence).map(|_| ())
}

fn observe_generated_native_mutation_sequence(
    session: &DbSession<SessionSqlCanister>,
    sequence: &GeneratedMutationSequence,
) -> Result<MutationSequenceExecutionFacts, Box<GeneratedMutationMismatch>> {
    let mut facts = MutationSequenceExecutionFacts {
        sequences: 1,
        ..MutationSequenceExecutionFacts::default()
    };
    seed_mutation_fixture(session, sequence).map_err(|error| {
        Box::new(GeneratedMutationMismatch::from_fixture_error(
            sequence, &error,
        ))
    })?;
    let sqlite_evidence = execute_generated_mutation_sequence(sequence).map_err(|error| {
        Box::new(GeneratedMutationMismatch::from_reference_error(
            sequence, &error,
        ))
    })?;
    assert_eq!(sqlite_evidence.len(), sequence.steps().len());

    for (step, sqlite) in sequence.steps().iter().zip(sqlite_evidence) {
        let actual = execute_native_mutation_step(session, step)?;
        if &actual != step.expected() {
            return Err(Box::new(GeneratedMutationMismatch::from_outcomes(
                step,
                &actual,
                step.expected(),
                "independent-model",
                "native-model-step",
            )));
        }
        match sqlite {
            MutationSqliteEvidence::Compared(outcome) => {
                if actual != outcome {
                    return Err(Box::new(GeneratedMutationMismatch::from_outcomes(
                        step,
                        &actual,
                        &outcome,
                        "sqlite-reference",
                        "native-sqlite-step",
                    )));
                }
                facts.sqlite_compared = facts.sqlite_compared.saturating_add(1);
            }
            MutationSqliteEvidence::Excluded(reason) => {
                assert_eq!(
                    step.sqlite_eligibility(),
                    MutationSqliteEligibility::Excluded(reason),
                );
                facts.sqlite_excluded = facts.sqlite_excluded.saturating_add(1);
            }
        }
        match step.statement().operation() {
            MutationOperation::Delete { .. } => {
                facts.deletes = facts.deletes.saturating_add(1);
            }
            MutationOperation::Insert { .. } => {
                facts.inserts = facts.inserts.saturating_add(1);
            }
            MutationOperation::InsertFromQuery { .. } => {
                facts.insert_queries = facts.insert_queries.saturating_add(1);
            }
            MutationOperation::Update { .. } => {
                facts.updates = facts.updates.saturating_add(1);
            }
        }
        facts.returning = facts
            .returning
            .saturating_add(u32::from(step.statement().returning()));
        facts.rejections = facts.rejections.saturating_add(u32::from(matches!(
            actual,
            MutationStepOutcome::Rejected { .. }
        )));
    }
    Ok(facts)
}

/// Project the accepted runtime catalog into the bounded mutation generator contract.
pub(super) fn generated_mutation_snapshot_from_accepted_authority(
    session: &DbSession<SessionSqlCanister>,
) -> Result<MutationSnapshot, String> {
    let context = session
        .accepted_schema_catalog_context_for_query::<SessionSqlWriteEntity>()
        .map_err(|error| error.to_string())?;
    mutation_snapshot_from_accepted(context.snapshot())
}

fn mutation_snapshot_from_accepted(
    accepted: &AcceptedSchemaSnapshot,
) -> Result<MutationSnapshot, String> {
    let persisted = accepted.persisted_snapshot();
    let fields = persisted
        .fields()
        .iter()
        .map(|field| {
            let primary_key = persisted.primary_key_field_ids().contains(&field.id());
            let (kind, role) = match (primary_key, field.kind()) {
                (true, AcceptedFieldKind::Nat64) => (
                    MutationFieldKind::UnsignedInteger,
                    MutationFieldRole::Key,
                ),
                (false, AcceptedFieldKind::Nat64) => (
                    MutationFieldKind::UnsignedInteger,
                    MutationFieldRole::Number,
                ),
                (false, AcceptedFieldKind::Text { .. }) => {
                    (MutationFieldKind::Text, MutationFieldRole::Text)
                }
                (_, unsupported) => {
                    return Err(format!(
                        "accepted mutation field {:?} has unsupported role/kind {primary_key}/{unsupported:?}",
                        field.name(),
                    ));
                }
            };
            Ok(MutationField::new(
                field.id().get(),
                field.name(),
                kind,
                role,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;

    MutationSnapshot::try_new(
        "session-write-accepted-snapshot-v1",
        accepted.entity_path(),
        accepted.entity_name(),
        persisted.version().get(),
        fields,
    )
    .map_err(|error| error.to_string())
}

fn seed_mutation_fixture(
    session: &DbSession<SessionSqlCanister>,
    sequence: &GeneratedMutationSequence,
) -> Result<(), InternalError> {
    for row in sequence.initial_rows() {
        session.insert(SessionSqlWriteEntity {
            id: row.key(),
            name: row.text().to_string(),
            age: row.number(),
        })?;
    }

    Ok(())
}

fn execute_native_mutation_step(
    session: &DbSession<SessionSqlCanister>,
    step: &icydb_testing_sql_generator::GeneratedMutationStep,
) -> Result<MutationStepOutcome, Box<GeneratedMutationMismatch>> {
    match step.expected() {
        MutationStepOutcome::Accepted { .. } => execute_model_accepted_mutation(session, step),
        MutationStepOutcome::Rejected { rejection, .. } => {
            execute_model_rejected_mutation(session, step, *rejection)
        }
    }
}

fn execute_model_accepted_mutation(
    session: &DbSession<SessionSqlCanister>,
    step: &GeneratedMutationStep,
) -> Result<MutationStepOutcome, Box<GeneratedMutationMismatch>> {
    let result = match execute_generated_native_mutation_statement(session, step) {
        Ok(result) => result,
        Err(error) => {
            return Err(Box::new(native_query_failure(
                session,
                step,
                &error,
                MutationMismatchCategory::Acceptance,
                "accepted-mutation-rejected",
            )));
        }
    };
    let (affected_rows, returned_rows) =
        normalize_native_mutation_result(result, step.statement().returning(), step.rendered_sql())
            .map_err(|_| mutation_shape_failure(step, MutationMismatchCategory::ReturnedRows))?;
    let state_after = read_native_mutation_state(session).map_err(|_| {
        mutation_state_read_failure(
            step,
            MutationMismatchCategory::PostState,
            "accepted-mutation-state-read",
        )
    })?;
    Ok(MutationStepOutcome::Accepted {
        affected_rows,
        returned_rows,
        state_after,
    })
}

fn execute_model_rejected_mutation(
    session: &DbSession<SessionSqlCanister>,
    step: &GeneratedMutationStep,
    rejection: MutationExpectedRejection,
) -> Result<MutationStepOutcome, Box<GeneratedMutationMismatch>> {
    match execute_generated_native_mutation_statement(session, step) {
        Err(error) if native_mutation_rejection_matches(&error, rejection) => {
            let state_after = read_native_mutation_state(session).map_err(|_| {
                mutation_state_read_failure(
                    step,
                    MutationMismatchCategory::Atomicity,
                    "rejected-mutation-state-read",
                )
            })?;
            Ok(MutationStepOutcome::Rejected {
                rejection,
                state_after,
            })
        }
        Err(error) => Err(Box::new(native_query_failure(
            session,
            step,
            &error,
            MutationMismatchCategory::TypedError,
            "rejected-mutation-error-class",
        ))),
        Ok(result) => unexpected_mutation_acceptance(session, step, result),
    }
}

// Generated UPDATE statements must declare the same exact or ordered-prefix
// execution intent required by the maintained session boundary.
fn execute_generated_native_mutation_statement(
    session: &DbSession<SessionSqlCanister>,
    step: &GeneratedMutationStep,
) -> Result<SqlStatementResult, QueryError> {
    match step.statement().operation() {
        MutationOperation::Update {
            window: Some(_), ..
        } => execute_prefix_sql_update_for_tests::<SessionSqlWriteEntity>(
            session,
            step.rendered_sql(),
        ),
        MutationOperation::Update { window: None, .. } => execute_exact_sql_update_for_tests::<
            SessionSqlWriteEntity,
        >(session, step.rendered_sql()),
        MutationOperation::Delete { .. }
        | MutationOperation::Insert { .. }
        | MutationOperation::InsertFromQuery { .. } => {
            execute_sql_statement_for_tests::<SessionSqlWriteEntity>(session, step.rendered_sql())
        }
    }
}

fn unexpected_mutation_acceptance(
    session: &DbSession<SessionSqlCanister>,
    step: &GeneratedMutationStep,
    result: SqlStatementResult,
) -> Result<MutationStepOutcome, Box<GeneratedMutationMismatch>> {
    let (affected_rows, returned_rows) =
        normalize_native_mutation_result(result, step.statement().returning(), step.rendered_sql())
            .map_err(|_| mutation_shape_failure(step, MutationMismatchCategory::Acceptance))?;
    let state_after = read_native_mutation_state(session).map_err(|_| {
        mutation_state_read_failure(
            step,
            MutationMismatchCategory::Acceptance,
            "rejected-mutation-accepted-state-read",
        )
    })?;
    let subject_outcome = MutationObservedOutcome::try_accepted_with_rows(
        affected_rows,
        returned_rows.as_slice(),
        state_after.as_slice(),
    )
    .expect("normalized unexpected mutation acceptance should fingerprint");
    Err(Box::new(GeneratedMutationMismatch::from_subject_failure(
        step,
        "mutation.unexpected_acceptance",
        MutationMismatchCategory::Acceptance,
        "rejected-mutation-accepted",
        subject_outcome,
    )))
}

fn mutation_shape_failure(
    step: &GeneratedMutationStep,
    category: MutationMismatchCategory,
) -> Box<GeneratedMutationMismatch> {
    Box::new(GeneratedMutationMismatch::from_subject_failure(
        step,
        "icydb.mutation_result_shape",
        category,
        "mutation-result-shape",
        MutationObservedOutcome::infrastructure_failure(
            "icydb.mutation_result_shape",
            MutationExecutionPhase::Execution,
        ),
    ))
}

fn mutation_state_read_failure(
    step: &GeneratedMutationStep,
    category: MutationMismatchCategory,
    invariant_class_id: &str,
) -> Box<GeneratedMutationMismatch> {
    Box::new(GeneratedMutationMismatch::from_subject_failure(
        step,
        "icydb.mutation_state_read",
        category,
        invariant_class_id,
        MutationObservedOutcome::infrastructure_failure(
            "icydb.mutation_state_read",
            MutationExecutionPhase::Execution,
        ),
    ))
}

fn native_query_failure(
    session: &DbSession<SessionSqlCanister>,
    step: &GeneratedMutationStep,
    error: &QueryError,
    category: MutationMismatchCategory,
    invariant_class_id: &str,
) -> GeneratedMutationMismatch {
    let error_class_id = query_error_id(error);
    let observed_state = read_native_mutation_state(session);
    let category = if observed_state.as_ref().is_ok_and(|state_after| {
        matches!(step.expected(), MutationStepOutcome::Rejected { .. })
            && state_after.as_slice() != step.expected().state_after()
    }) {
        MutationMismatchCategory::Atomicity
    } else {
        category
    };
    let subject_outcome = observed_state.map_or_else(
        |_| {
            MutationObservedOutcome::infrastructure_failure(
                error_class_id.clone(),
                MutationExecutionPhase::Execution,
            )
        },
        |state_after| {
            MutationObservedOutcome::try_rejected_with_state(
                error_class_id.clone(),
                state_after.as_slice(),
            )
            .expect("normalized rejected mutation state should fingerprint")
        },
    );
    GeneratedMutationMismatch::from_subject_failure(
        step,
        error_class_id.as_str(),
        category,
        invariant_class_id,
        subject_outcome,
    )
}

fn query_error_id(error: &QueryError) -> String {
    format!(
        "diagnostic.{:04x}",
        error.diagnostic_code().error_code().raw()
    )
}

fn normalize_native_mutation_result(
    result: SqlStatementResult,
    returning: bool,
    sql: &str,
) -> Result<(u32, Vec<MutationRow>), String> {
    match (returning, result) {
        (false, SqlStatementResult::Count { row_count }) => Ok((row_count, Vec::new())),
        (
            true,
            SqlStatementResult::Projection {
                rows, row_count, ..
            },
        ) => {
            let row_count_usize = usize::try_from(row_count)
                .map_err(|_| "mutation RETURNING row count does not fit usize".to_string())?;
            if row_count_usize != rows.len() {
                return Err(format!(
                    "mutation RETURNING row count drifted for SQL {sql:?}",
                ));
            }
            let values = rows
                .into_iter()
                .map(|row| row.into_iter().map(runtime_output).collect())
                .collect();
            Ok((row_count, normalize_mutation_rows(values, "RETURNING")?))
        }
        (_, other) => Err(format!(
            "mutation SQL {sql:?} returned unexpected payload {other:?}",
        )),
    }
}

fn read_native_mutation_state(
    session: &DbSession<SessionSqlCanister>,
) -> Result<Vec<MutationRow>, String> {
    let rows = statement_projection_rows::<SessionSqlWriteEntity>(
        session,
        "SELECT id, name, age FROM SessionSqlWriteEntity ORDER BY id ASC",
    )
    .map_err(|error| error.to_string())?;
    normalize_mutation_rows(rows, "post-state")
}

fn normalize_mutation_rows(
    rows: Vec<Vec<Value>>,
    context: &str,
) -> Result<Vec<MutationRow>, String> {
    let mut normalized = rows
        .into_iter()
        .map(|row| match row.as_slice() {
            [Value::Nat64(key), Value::Text(text), Value::Nat64(number)] => {
                Ok(MutationRow::new(*key, text.clone(), *number))
            }
            other => Err(format!(
                "mutation {context} row should be [Nat64, Text, Nat64], got {other:?}",
            )),
        })
        .collect::<Result<Vec<_>, String>>()?;
    normalized.sort_by_key(MutationRow::key);
    Ok(normalized)
}

fn native_mutation_rejection_matches(
    error: &QueryError,
    expected: MutationExpectedRejection,
) -> bool {
    match expected {
        MutationExpectedRejection::DuplicateKey => {
            let QueryError::Execute(execution) = error else {
                return false;
            };
            execution.as_internal().class() == ErrorClass::Conflict
        }
    }
}
