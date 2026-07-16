//! Module: db::session::tests::mutation_reference
//! Responsibility: required native IcyDB mutation-state evidence against the independent model and eligible SQLite overlap.
//! Does not own: mutation generation, accepted schema authority, product execution, or SQLite eligibility.
//! Boundary: executes each sequence end to end and compares typed response, rejection, state, and atomicity after every step.

use super::*;
use crate::db::schema::{AcceptedFieldKind, AcceptedSchemaSnapshot};
use icydb_testing_sql_generator::{
    GeneratedMutationSequence, MutationExpectedRejection, MutationField, MutationFieldKind,
    MutationFieldRole, MutationOperation, MutationRow, MutationSnapshot, MutationSqliteEligibility,
    MutationStepOutcome, TIER_A_MUTATION_BUDGETS, TIER_A_MUTATION_CASES_PER_ROOT,
    TIER_A_MUTATION_ROOT_SEEDS, generate_mutation_sequence,
};
use icydb_testing_sqlite_reference::{MutationSqliteEvidence, execute_generated_mutation_sequence};

#[test]
fn tier_a_generated_mutation_sequences_match_native_state_and_eligible_sqlite() {
    reset_session_sql_store();
    let session = sql_session();
    let snapshot = generated_mutation_snapshot_from_accepted_authority(&session)
        .expect("accepted write snapshot should map into mutation generator facts");
    let mut sequences = 0_u32;
    let mut inserts = 0_u32;
    let mut insert_queries = 0_u32;
    let mut updates = 0_u32;
    let mut deletes = 0_u32;
    let mut returning = 0_u32;
    let mut rejections = 0_u32;
    let mut sqlite_compared = 0_u32;
    let mut sqlite_excluded = 0_u32;

    for root_seed in TIER_A_MUTATION_ROOT_SEEDS {
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
            seed_mutation_fixture(&session, &sequence);
            let sqlite_evidence = execute_generated_mutation_sequence(&sequence)
                .expect("eligible mutation steps should execute in bundled SQLite");
            assert_eq!(sqlite_evidence.len(), sequence.steps().len());

            for (step, sqlite) in sequence.steps().iter().zip(sqlite_evidence) {
                let actual = execute_native_mutation_step(&session, step);
                assert_eq!(
                    &actual,
                    step.expected(),
                    "native mutation outcome drifted for sequence {:?} SQL {:?}",
                    sequence.identity().id(),
                    step.rendered_sql(),
                );
                match sqlite {
                    MutationSqliteEvidence::Compared(outcome) => {
                        assert_eq!(
                            actual,
                            outcome,
                            "native IcyDB and bundled SQLite drifted for sequence {:?} SQL {:?}",
                            sequence.identity().id(),
                            step.rendered_sql(),
                        );
                        sqlite_compared = sqlite_compared.saturating_add(1);
                    }
                    MutationSqliteEvidence::Excluded(reason) => {
                        assert_eq!(
                            step.sqlite_eligibility(),
                            MutationSqliteEligibility::Excluded(reason),
                        );
                        sqlite_excluded = sqlite_excluded.saturating_add(1);
                    }
                }
                match step.statement().operation() {
                    MutationOperation::Delete { .. } => deletes = deletes.saturating_add(1),
                    MutationOperation::Insert { .. } => inserts = inserts.saturating_add(1),
                    MutationOperation::InsertFromQuery { .. } => {
                        insert_queries = insert_queries.saturating_add(1);
                    }
                    MutationOperation::Update { .. } => updates = updates.saturating_add(1),
                }
                returning = returning.saturating_add(u32::from(step.statement().returning()));
                rejections = rejections.saturating_add(u32::from(matches!(
                    actual,
                    MutationStepOutcome::Rejected { .. }
                )));
            }
            assert_eq!(
                read_native_mutation_state(&session),
                sequence.final_state(),
                "native final state drifted for sequence {:?}",
                sequence.identity().id(),
            );
            sequences = sequences.saturating_add(1);
        }
    }

    assert_eq!(sequences, 8);
    assert!(inserts > 0);
    assert!(insert_queries > 0);
    assert!(updates > 0);
    assert!(deletes > 0);
    assert!(returning > 0);
    assert_eq!(rejections, sequences);
    assert!(sqlite_compared > 0);
    assert!(sqlite_excluded > 0);
}

fn generated_mutation_snapshot_from_accepted_authority(
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
) {
    for row in sequence.initial_rows() {
        session
            .insert(SessionSqlWriteEntity {
                id: row.key(),
                name: row.text().to_string(),
                age: row.number(),
            })
            .unwrap_or_else(|error| {
                panic!(
                    "mutation fixture insert should succeed for sequence {:?}: {error}",
                    sequence.identity().id(),
                )
            });
    }
}

fn execute_native_mutation_step(
    session: &DbSession<SessionSqlCanister>,
    step: &icydb_testing_sql_generator::GeneratedMutationStep,
) -> MutationStepOutcome {
    match step.expected() {
        MutationStepOutcome::Accepted { .. } => {
            let result = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
                session,
                step.rendered_sql(),
            )
            .unwrap_or_else(|error| {
                panic!(
                    "model-accepted mutation SQL {:?} should execute: {error}",
                    step.rendered_sql(),
                )
            });
            let (affected_rows, returned_rows) = normalize_native_mutation_result(
                result,
                step.statement().returning(),
                step.rendered_sql(),
            );
            MutationStepOutcome::Accepted {
                affected_rows,
                returned_rows,
                state_after: read_native_mutation_state(session),
            }
        }
        MutationStepOutcome::Rejected { rejection, .. } => {
            let error = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
                session,
                step.rendered_sql(),
            )
            .expect_err("model-rejected mutation SQL must fail closed");
            assert_native_mutation_rejection(error, *rejection, step.rendered_sql());
            MutationStepOutcome::Rejected {
                rejection: *rejection,
                state_after: read_native_mutation_state(session),
            }
        }
    }
}

fn normalize_native_mutation_result(
    result: SqlStatementResult,
    returning: bool,
    sql: &str,
) -> (u32, Vec<MutationRow>) {
    match (returning, result) {
        (false, SqlStatementResult::Count { row_count }) => (row_count, Vec::new()),
        (
            true,
            SqlStatementResult::Projection {
                rows, row_count, ..
            },
        ) => {
            assert_eq!(
                usize::try_from(row_count).expect("bounded mutation row count should fit usize"),
                rows.len(),
                "mutation RETURNING row count drifted for SQL {sql:?}",
            );
            let values = rows
                .into_iter()
                .map(|row| row.into_iter().map(runtime_output).collect())
                .collect();
            (row_count, normalize_mutation_rows(values, "RETURNING"))
        }
        (_, other) => panic!("mutation SQL {sql:?} returned unexpected payload {other:?}"),
    }
}

fn read_native_mutation_state(session: &DbSession<SessionSqlCanister>) -> Vec<MutationRow> {
    let rows = statement_projection_rows::<SessionSqlWriteEntity>(
        session,
        "SELECT id, name, age FROM SessionSqlWriteEntity ORDER BY id ASC",
    )
    .expect("mutation state read should remain valid after every statement");
    normalize_mutation_rows(rows, "post-state")
}

fn normalize_mutation_rows(rows: Vec<Vec<Value>>, context: &str) -> Vec<MutationRow> {
    let mut normalized = rows
        .into_iter()
        .map(|row| match row.as_slice() {
            [Value::Nat64(key), Value::Text(text), Value::Nat64(number)] => {
                MutationRow::new(*key, text.clone(), *number)
            }
            other => panic!("mutation {context} row should be [Nat64, Text, Nat64], got {other:?}"),
        })
        .collect::<Vec<_>>();
    normalized.sort_by_key(MutationRow::key);
    normalized
}

fn assert_native_mutation_rejection(
    error: QueryError,
    expected: MutationExpectedRejection,
    sql: &str,
) {
    match expected {
        MutationExpectedRejection::DuplicateKey => {
            let QueryError::Execute(execution) = error else {
                panic!("duplicate-key mutation should preserve an execution error: {error}");
            };
            let internal = execution.as_internal();
            assert_eq!(
                internal.class(),
                ErrorClass::Conflict,
                "duplicate-key mutation SQL {sql:?} drifted: origin={:?} diagnostic={:?} detail={:?}",
                internal.origin(),
                internal.diagnostic_code(),
                internal.detail(),
            );
        }
    }
}
