//! Module: sqlite_reference::mutation
//! Responsibility: bundled SQLite evidence for the explicitly eligible mutation overlap.
//! Does not own: IcyDB mutation semantics, generation, model transitions, or eligibility policy.
//! Boundary: executes each eligible step from its canonical pre-state and reports typed outcomes.

use crate::{SqliteAdapterError, SqliteAdapterErrorKind, adapter::open_checked_connection};
use icydb_testing_sql_generator::{
    GeneratedMutationSequence, GeneratedMutationStep, MutationExpectedRejection, MutationField,
    MutationFieldRole, MutationRow, MutationSnapshot, MutationSqliteEligibility,
    MutationSqliteExclusion, MutationStepOutcome,
};
use rusqlite::{ErrorCode, Transaction, params};

///
/// MutationSqliteEvidence
///
/// Typed secondary-provider evidence aligned one-for-one with generated steps.
/// An exclusion is a predeclared contract fact, never an adapter error recovery path.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MutationSqliteEvidence {
    /// Bundled SQLite executed the step and produced this normalized outcome.
    Compared(MutationStepOutcome),

    /// The step is outside the enumerated SQLite overlap.
    Excluded(MutationSqliteExclusion),
}

/// Execute every eligible step in one generated sequence against checked bundled SQLite.
///
/// One fresh connection and transaction execute the complete sequence. Rejected
/// statements run inside a savepoint, while explicitly excluded windowed steps
/// advance only the modeled fixture before the next eligible comparison. The
/// returned vector remains aligned with generated step order.
///
/// # Errors
///
/// Returns a typed adapter error for invalid generated facts, SQLite environment
/// drift, setup or execution failure, unexpected acceptance/rejection, or row-state mapping failure.
pub fn execute_generated_mutation_sequence(
    sequence: &GeneratedMutationSequence,
) -> Result<Vec<MutationSqliteEvidence>, SqliteAdapterError> {
    sequence.validate().map_err(|error| {
        SqliteAdapterError::new(
            SqliteAdapterErrorKind::GeneratedCase,
            format!("generated mutation sequence failed validation: {error}"),
        )
    })?;
    let mut connection = open_checked_connection()?;
    let transaction = connection.transaction().map_err(|source| {
        SqliteAdapterError::with_source(
            SqliteAdapterErrorKind::Transaction,
            format!(
                "failed to begin generated mutation transaction {:?}",
                sequence.identity().id(),
            ),
            source,
        )
    })?;
    create_and_seed_mutation_fixture(&transaction, sequence, sequence.initial_rows())?;
    let evidence = execute_sequence_steps(&transaction, sequence)?;
    transaction.commit().map_err(|source| {
        SqliteAdapterError::with_source(
            SqliteAdapterErrorKind::Transaction,
            format!(
                "failed to commit generated mutation transaction {:?}",
                sequence.identity().id(),
            ),
            source,
        )
    })?;

    Ok(evidence)
}

fn execute_sequence_steps(
    transaction: &Transaction<'_>,
    sequence: &GeneratedMutationSequence,
) -> Result<Vec<MutationSqliteEvidence>, SqliteAdapterError> {
    let mut evidence = Vec::with_capacity(sequence.steps().len());
    for step in sequence.steps() {
        let observed_before = read_complete_state(transaction, sequence)?;
        if observed_before != step.state_before() {
            return Err(SqliteAdapterError::new(
                SqliteAdapterErrorKind::Result,
                format!(
                    "generated mutation sequence {:?} reached a stale SQLite pre-state",
                    sequence.identity().id(),
                ),
            ));
        }
        match step.sqlite_eligibility() {
            MutationSqliteEligibility::Eligible => {
                let outcome = execute_eligible_step(transaction, sequence, step)?;
                evidence.push(MutationSqliteEvidence::Compared(outcome));
            }
            MutationSqliteEligibility::Excluded(reason) => {
                replace_mutation_fixture_state(
                    transaction,
                    sequence,
                    step.expected().state_after(),
                )?;
                evidence.push(MutationSqliteEvidence::Excluded(reason));
            }
        }
    }

    Ok(evidence)
}

fn execute_eligible_step(
    transaction: &Transaction<'_>,
    sequence: &GeneratedMutationSequence,
    step: &GeneratedMutationStep,
) -> Result<MutationStepOutcome, SqliteAdapterError> {
    let outcome = match step.expected() {
        MutationStepOutcome::Accepted { .. } => {
            execute_expected_accepted_step(transaction, sequence, step)?
        }
        MutationStepOutcome::Rejected { rejection, .. } => {
            execute_rejected_step_in_savepoint(transaction, sequence, step, *rejection)?
        }
    };

    Ok(outcome)
}

fn create_and_seed_mutation_fixture(
    transaction: &Transaction<'_>,
    sequence: &GeneratedMutationSequence,
    rows: &[MutationRow],
) -> Result<(), SqliteAdapterError> {
    let snapshot = sequence.snapshot();
    let key = required_field_name(snapshot, MutationFieldRole::Key)?;
    let text = required_field_name(snapshot, MutationFieldRole::Text)?;
    let number = required_field_name(snapshot, MutationFieldRole::Number)?;
    let entity = snapshot.entity_name();
    transaction
        .execute_batch(&format!(
            "CREATE TABLE {entity} (\
             {key} INTEGER PRIMARY KEY NOT NULL, \
             {text} TEXT NOT NULL, \
             {number} INTEGER NOT NULL\
             ) STRICT;"
        ))
        .map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Fixture,
                format!(
                    "failed to create generated mutation fixture {:?}",
                    sequence.identity().id(),
                ),
                source,
            )
        })?;
    seed_mutation_rows(transaction, sequence, rows)
}

fn seed_mutation_rows(
    transaction: &Transaction<'_>,
    sequence: &GeneratedMutationSequence,
    rows: &[MutationRow],
) -> Result<(), SqliteAdapterError> {
    let snapshot = sequence.snapshot();
    let key = required_field_name(snapshot, MutationFieldRole::Key)?;
    let text = required_field_name(snapshot, MutationFieldRole::Text)?;
    let number = required_field_name(snapshot, MutationFieldRole::Number)?;
    let insert_sql = format!(
        "INSERT INTO {} ({key}, {text}, {number}) VALUES (?1, ?2, ?3)",
        snapshot.entity_name(),
    );
    for row in rows {
        transaction
            .execute(
                insert_sql.as_str(),
                params![
                    sqlite_integer(row.key(), "fixture key")?,
                    row.text(),
                    sqlite_integer(row.number(), "fixture number")?,
                ],
            )
            .map_err(|source| {
                SqliteAdapterError::with_source(
                    SqliteAdapterErrorKind::Fixture,
                    format!(
                        "failed to seed generated mutation fixture {:?}",
                        sequence.identity().id(),
                    ),
                    source,
                )
            })?;
    }

    Ok(())
}

// Windowed IcyDB mutations are outside the declared SQLite overlap. Replacing
// only their modeled post-state keeps later eligible steps in the same sequence
// comparable without claiming SQLite evidence for the excluded transition.
fn replace_mutation_fixture_state(
    transaction: &Transaction<'_>,
    sequence: &GeneratedMutationSequence,
    rows: &[MutationRow],
) -> Result<(), SqliteAdapterError> {
    transaction
        .execute(
            &format!("DELETE FROM {}", sequence.snapshot().entity_name()),
            [],
        )
        .map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Fixture,
                format!(
                    "failed to reset excluded mutation state {:?}",
                    sequence.identity().id(),
                ),
                source,
            )
        })?;
    seed_mutation_rows(transaction, sequence, rows)
}

fn execute_rejected_step_in_savepoint(
    transaction: &Transaction<'_>,
    sequence: &GeneratedMutationSequence,
    step: &GeneratedMutationStep,
    rejection: MutationExpectedRejection,
) -> Result<MutationStepOutcome, SqliteAdapterError> {
    transaction
        .execute_batch("SAVEPOINT icydb_generated_mutation_step;")
        .map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Transaction,
                "failed to begin generated mutation rejection savepoint",
                source,
            )
        })?;
    let outcome = execute_expected_rejected_step(transaction, sequence, step, rejection);
    let cleanup = transaction
        .execute_batch(
            "ROLLBACK TO icydb_generated_mutation_step; \
             RELEASE icydb_generated_mutation_step;",
        )
        .map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Transaction,
                "failed to roll back generated mutation rejection savepoint",
                source,
            )
        });

    cleanup?;
    outcome
}

fn execute_expected_accepted_step(
    transaction: &Transaction<'_>,
    sequence: &GeneratedMutationSequence,
    step: &GeneratedMutationStep,
) -> Result<MutationStepOutcome, SqliteAdapterError> {
    let (affected_rows, mut returned_rows) = if step.statement().returning() {
        let rows = execute_returning_rows(transaction, sequence, step.rendered_sql())?;
        let count = sqlite_row_count(rows.len())?;
        (count, rows)
    } else {
        let count = transaction
            .execute(step.rendered_sql(), [])
            .map_err(|source| {
                SqliteAdapterError::with_source(
                    SqliteAdapterErrorKind::Mutation,
                    format!(
                        "accepted generated mutation {:?} rejected in SQLite",
                        sequence.identity().id(),
                    ),
                    source,
                )
            })?;
        (sqlite_row_count(count)?, Vec::new())
    };
    returned_rows.sort_by_key(MutationRow::key);
    let state_after = read_complete_state(transaction, sequence)?;

    Ok(MutationStepOutcome::Accepted {
        affected_rows,
        returned_rows,
        state_after,
    })
}

fn execute_expected_rejected_step(
    transaction: &Transaction<'_>,
    sequence: &GeneratedMutationSequence,
    step: &GeneratedMutationStep,
    rejection: MutationExpectedRejection,
) -> Result<MutationStepOutcome, SqliteAdapterError> {
    let result = if step.statement().returning() {
        execute_rejected_returning_statement(transaction, step.rendered_sql())
    } else {
        transaction.execute(step.rendered_sql(), []).map(|_| ())
    };
    let source = match result {
        Ok(()) => {
            return Err(SqliteAdapterError::new(
                SqliteAdapterErrorKind::Mutation,
                format!(
                    "rejected generated mutation {:?} unexpectedly succeeded in SQLite",
                    sequence.identity().id(),
                ),
            ));
        }
        Err(source) => source,
    };
    match rejection {
        MutationExpectedRejection::DuplicateKey if is_constraint_violation(&source) => {}
        MutationExpectedRejection::DuplicateKey => {
            return Err(SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Mutation,
                format!(
                    "generated mutation {:?} rejected with a non-constraint SQLite error",
                    sequence.identity().id(),
                ),
                source,
            ));
        }
    }
    let state_after = read_complete_state(transaction, sequence)?;

    Ok(MutationStepOutcome::Rejected {
        rejection,
        state_after,
    })
}

fn execute_rejected_returning_statement(
    transaction: &Transaction<'_>,
    sql: &str,
) -> Result<(), rusqlite::Error> {
    let mut statement = transaction.prepare(sql)?;
    let mut rows = statement.query([])?;
    while rows.next()?.is_some() {}

    Ok(())
}

fn execute_returning_rows(
    transaction: &Transaction<'_>,
    sequence: &GeneratedMutationSequence,
    sql: &str,
) -> Result<Vec<MutationRow>, SqliteAdapterError> {
    let mut statement = transaction.prepare(sql).map_err(|source| {
        SqliteAdapterError::with_source(
            SqliteAdapterErrorKind::Mutation,
            format!(
                "failed to prepare generated mutation RETURNING {:?}",
                sequence.identity().id(),
            ),
            source,
        )
    })?;
    let mapped = statement
        .query_map([], |row| {
            let key = row.get::<_, i64>(0)?;
            let text = row.get::<_, String>(1)?;
            let number = row.get::<_, i64>(2)?;
            Ok((key, text, number))
        })
        .map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Mutation,
                format!(
                    "failed to execute generated mutation RETURNING {:?}",
                    sequence.identity().id(),
                ),
                source,
            )
        })?;
    mapped
        .map(|row| {
            let (key, text, number) = row.map_err(|source| {
                SqliteAdapterError::with_source(
                    SqliteAdapterErrorKind::Result,
                    "failed to decode generated mutation RETURNING row",
                    source,
                )
            })?;
            Ok(MutationRow::new(
                unsigned_integer(key, "RETURNING key")?,
                text,
                unsigned_integer(number, "RETURNING number")?,
            ))
        })
        .collect()
}

fn read_complete_state(
    transaction: &Transaction<'_>,
    sequence: &GeneratedMutationSequence,
) -> Result<Vec<MutationRow>, SqliteAdapterError> {
    let snapshot = sequence.snapshot();
    let key = required_field_name(snapshot, MutationFieldRole::Key)?;
    let text = required_field_name(snapshot, MutationFieldRole::Text)?;
    let number = required_field_name(snapshot, MutationFieldRole::Number)?;
    execute_returning_rows(
        transaction,
        sequence,
        format!(
            "SELECT {key}, {text}, {number} FROM {} ORDER BY {key} ASC",
            snapshot.entity_name(),
        )
        .as_str(),
    )
}

fn required_field_name(
    snapshot: &MutationSnapshot,
    role: MutationFieldRole,
) -> Result<&str, SqliteAdapterError> {
    snapshot
        .field(role)
        .map(MutationField::name)
        .ok_or_else(|| {
            SqliteAdapterError::new(
                SqliteAdapterErrorKind::GeneratedCase,
                "validated mutation snapshot is missing a required field role",
            )
        })
}

fn sqlite_integer(value: u64, context: &str) -> Result<i64, SqliteAdapterError> {
    i64::try_from(value).map_err(|_| {
        SqliteAdapterError::new(
            SqliteAdapterErrorKind::GeneratedCase,
            format!("generated mutation {context} exceeds SQLite INTEGER"),
        )
    })
}

fn unsigned_integer(value: i64, context: &str) -> Result<u64, SqliteAdapterError> {
    u64::try_from(value).map_err(|_| {
        SqliteAdapterError::new(
            SqliteAdapterErrorKind::Result,
            format!("generated mutation {context} is negative"),
        )
    })
}

fn sqlite_row_count(count: usize) -> Result<u32, SqliteAdapterError> {
    u32::try_from(count).map_err(|_| {
        SqliteAdapterError::new(
            SqliteAdapterErrorKind::Result,
            "generated mutation SQLite row count exceeds u32",
        )
    })
}

fn is_constraint_violation(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == ErrorCode::ConstraintViolation
    )
}
