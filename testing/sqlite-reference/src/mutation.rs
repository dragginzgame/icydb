//! Module: sqlite_reference::mutation
//! Responsibility: bundled SQLite evidence for the explicitly eligible mutation overlap.
//! Does not own: IcyDB mutation semantics, generation, model transitions, or eligibility policy.
//! Boundary: executes each eligible step from its canonical pre-state and reports typed outcomes.

use crate::{SqliteAdapterError, SqliteAdapterErrorKind, adapter::open_checked_connection};
use icydb_testing_sql_generator::{
    GeneratedMutationSequence, GeneratedMutationStep, MutationDefaultValue,
    MutationExpectedRejection, MutationField, MutationFieldRole, MutationIndexEntry,
    MutationInsertRow, MutationOperation, MutationProjectedField, MutationProjectedRow,
    MutationRow, MutationRowPayload, MutationSchemaProfile, MutationSnapshot,
    MutationSqliteEligibility, MutationSqliteExclusion, MutationStepOutcome, MutationUpdateIntent,
    MutationValue, MutationWriteIntent,
};
use rusqlite::{ErrorCode, Row, Transaction};

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
/// statements run inside a savepoint, while explicitly excluded steps advance
/// only the modeled fixture before the next eligible comparison.
///
/// # Errors
///
/// Returns a typed adapter error for invalid generated facts, SQLite environment
/// drift, setup or execution failure, unexpected acceptance/rejection, or row-state mapping failure.
pub fn execute_generated_mutation_sequence(
    sequence: &GeneratedMutationSequence,
) -> Result<Vec<MutationSqliteEvidence>, SqliteAdapterError> {
    sequence.validate().map_err(generated_case_error)?;
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
        let observed_before = read_complete_state(transaction, sequence.snapshot())?;
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
                    sequence.snapshot(),
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
    match step.expected() {
        MutationStepOutcome::Accepted { .. } => {
            execute_expected_accepted_step(transaction, sequence, step)
        }
        MutationStepOutcome::Rejected { rejection, .. } => {
            execute_rejected_step_in_savepoint(transaction, sequence, step, *rejection)
        }
    }
}

fn create_and_seed_mutation_fixture(
    transaction: &Transaction<'_>,
    sequence: &GeneratedMutationSequence,
    rows: &[MutationRow],
) -> Result<(), SqliteAdapterError> {
    let snapshot = sequence.snapshot();
    let entity = snapshot.entity_name();
    let create_sql = match snapshot.profile() {
        MutationSchemaProfile::AuthoredScalar => format!(
            "CREATE TABLE {entity} (\
             {} INTEGER PRIMARY KEY NOT NULL, \
             {} TEXT NOT NULL, \
             {} INTEGER NOT NULL\
             ) STRICT;",
            required_field_name(snapshot, MutationFieldRole::Key)?,
            required_field_name(snapshot, MutationFieldRole::Text)?,
            required_field_name(snapshot, MutationFieldRole::Number)?,
        ),
        MutationSchemaProfile::AcceptedDefault => format!(
            "CREATE TABLE {entity} (\
             {} INTEGER PRIMARY KEY NOT NULL, \
             {} TEXT NOT NULL, \
             {} TEXT NOT NULL DEFAULT 'bronze', \
             {} INTEGER NOT NULL DEFAULT 7, \
             {} TEXT DEFAULT NULL\
             ) STRICT;\
             CREATE INDEX {entity}_tier_idx ON {entity} ({});",
            required_field_name(snapshot, MutationFieldRole::Key)?,
            required_field_name(snapshot, MutationFieldRole::Name)?,
            required_field_name(snapshot, MutationFieldRole::Tier)?,
            required_field_name(snapshot, MutationFieldRole::Score)?,
            required_field_name(snapshot, MutationFieldRole::Note)?,
            required_field_name(snapshot, MutationFieldRole::Tier)?,
        ),
    };
    transaction.execute_batch(&create_sql).map_err(|source| {
        SqliteAdapterError::with_source(
            SqliteAdapterErrorKind::Fixture,
            format!(
                "failed to create generated mutation fixture {:?}",
                sequence.identity().id(),
            ),
            source,
        )
    })?;
    seed_mutation_rows(transaction, snapshot, rows)
}

fn seed_mutation_rows(
    transaction: &Transaction<'_>,
    snapshot: &MutationSnapshot,
    rows: &[MutationRow],
) -> Result<(), SqliteAdapterError> {
    for row in rows {
        let sql = complete_row_insert_sql(snapshot, row);
        transaction.execute(&sql, []).map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Fixture,
                "failed to seed generated mutation fixture",
                source,
            )
        })?;
    }
    Ok(())
}

fn complete_row_insert_sql(snapshot: &MutationSnapshot, row: &MutationRow) -> String {
    match row.payload() {
        MutationRowPayload::AuthoredScalar { text, number } => format!(
            "INSERT INTO {} (id, name, age) VALUES ({}, '{}', {number})",
            snapshot.entity_name(),
            row.key(),
            quote_text(text),
        ),
        MutationRowPayload::AcceptedDefault {
            name,
            tier,
            score,
            note,
        } => format!(
            "INSERT INTO {} (id, name, tier, score, note) VALUES ({}, '{}', '{}', {score}, {})",
            snapshot.entity_name(),
            row.key(),
            quote_text(name),
            quote_text(tier),
            nullable_text_sql(note.as_deref()),
        ),
    }
}

// Excluded IcyDB transitions are outside declared SQLite overlap. Replacing
// only their modeled post-state keeps later eligible steps comparable without
// claiming SQLite evidence for the excluded transition.
fn replace_mutation_fixture_state(
    transaction: &Transaction<'_>,
    snapshot: &MutationSnapshot,
    rows: &[MutationRow],
) -> Result<(), SqliteAdapterError> {
    transaction
        .execute(&format!("DELETE FROM {}", snapshot.entity_name()), [])
        .map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Fixture,
                "failed to reset excluded mutation state",
                source,
            )
        })?;
    seed_mutation_rows(transaction, snapshot, rows)
}

fn execute_expected_accepted_step(
    transaction: &Transaction<'_>,
    sequence: &GeneratedMutationSequence,
    step: &GeneratedMutationStep,
) -> Result<MutationStepOutcome, SqliteAdapterError> {
    let (affected_rows, mut returned_rows) =
        execute_step_sql(transaction, sequence.snapshot(), step).map_err(|source| {
            SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Mutation,
                format!(
                    "accepted generated mutation {:?} rejected in SQLite",
                    sequence.identity().id(),
                ),
                source,
            )
        })?;
    returned_rows.sort();
    let state_after = read_complete_state(transaction, sequence.snapshot())?;
    let index_after = read_secondary_index_state(transaction, sequence.snapshot())?;
    Ok(MutationStepOutcome::Accepted {
        affected_rows,
        returned_rows,
        state_after,
        index_after,
    })
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
    let result = execute_step_sql(transaction, sequence.snapshot(), step);
    transaction
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
        })?;

    match (rejection, result) {
        (
            MutationExpectedRejection::DuplicatePrimaryKey,
            Err(rusqlite::Error::SqliteFailure(failure, _)),
        ) if failure.code == ErrorCode::ConstraintViolation => {}
        (MutationExpectedRejection::DuplicatePrimaryKey, Ok(_)) => {
            return Err(SqliteAdapterError::new(
                SqliteAdapterErrorKind::Mutation,
                format!(
                    "rejected generated mutation {:?} unexpectedly succeeded in SQLite",
                    sequence.identity().id(),
                ),
            ));
        }
        (MutationExpectedRejection::DuplicatePrimaryKey, Err(source)) => {
            return Err(SqliteAdapterError::with_source(
                SqliteAdapterErrorKind::Mutation,
                "duplicate mutation rejected with a non-constraint SQLite error",
                source,
            ));
        }
        (
            MutationExpectedRejection::DefaultUnavailable
            | MutationExpectedRejection::MissingRequiredField,
            _,
        ) => {
            return Err(SqliteAdapterError::new(
                SqliteAdapterErrorKind::GeneratedCase,
                "typed policy rejection reached the SQLite-eligible adapter path",
            ));
        }
    }
    let state_after = read_complete_state(transaction, sequence.snapshot())?;
    let index_after = read_secondary_index_state(transaction, sequence.snapshot())?;
    Ok(MutationStepOutcome::Rejected {
        rejection,
        state_after,
        index_after,
    })
}

fn execute_step_sql(
    transaction: &Transaction<'_>,
    snapshot: &MutationSnapshot,
    step: &GeneratedMutationStep,
) -> rusqlite::Result<(u32, Vec<MutationProjectedRow>)> {
    match step.statement().operation() {
        MutationOperation::Insert { rows }
            if snapshot.profile() == MutationSchemaProfile::AcceptedDefault =>
        {
            execute_default_insert_rows(transaction, snapshot, step, rows)
        }
        _ => {
            let sql = sqlite_compatible_statement(snapshot, step);
            execute_one_sql(transaction, snapshot, &sql, step.statement().returning())
        }
    }
}

fn execute_default_insert_rows(
    transaction: &Transaction<'_>,
    snapshot: &MutationSnapshot,
    step: &GeneratedMutationStep,
    rows: &[MutationInsertRow],
) -> rusqlite::Result<(u32, Vec<MutationProjectedRow>)> {
    let mut affected = 0_u32;
    let mut returned = Vec::new();
    for row in rows {
        let sql = default_insert_sql(snapshot, row, step.statement().returning())?;
        let (row_count, mut row_returning) =
            execute_one_sql(transaction, snapshot, &sql, step.statement().returning())?;
        affected = affected.saturating_add(row_count);
        returned.append(&mut row_returning);
    }
    Ok((affected, returned))
}

fn default_insert_sql(
    snapshot: &MutationSnapshot,
    row: &MutationInsertRow,
    returning: &icydb_testing_sql_generator::MutationReturning,
) -> rusqlite::Result<String> {
    let MutationInsertRow::AcceptedDefault {
        key,
        name,
        tier,
        score,
        note,
    } = row
    else {
        return Err(rusqlite::Error::InvalidQuery);
    };
    let mut columns = Vec::new();
    let mut values = Vec::new();
    push_unsigned_write(&mut columns, &mut values, "id", key);
    push_text_write(&mut columns, &mut values, "name", name);
    push_text_write(&mut columns, &mut values, "tier", tier);
    push_unsigned_write(&mut columns, &mut values, "score", score);
    push_nullable_text_write(&mut columns, &mut values, "note", note);
    let mut sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        snapshot.entity_name(),
        columns.join(", "),
        values.join(", "),
    );
    append_returning(snapshot, returning, &mut sql)?;
    Ok(sql)
}

fn push_unsigned_write(
    columns: &mut Vec<&'static str>,
    values: &mut Vec<String>,
    column: &'static str,
    intent: &MutationWriteIntent<u64>,
) {
    if let MutationWriteIntent::Authored(value) = intent {
        columns.push(column);
        values.push(value.to_string());
    }
}

fn push_text_write(
    columns: &mut Vec<&'static str>,
    values: &mut Vec<String>,
    column: &'static str,
    intent: &MutationWriteIntent<String>,
) {
    if let MutationWriteIntent::Authored(value) = intent {
        columns.push(column);
        values.push(format!("'{}'", quote_text(value)));
    }
}

fn push_nullable_text_write(
    columns: &mut Vec<&'static str>,
    values: &mut Vec<String>,
    column: &'static str,
    intent: &MutationWriteIntent<Option<String>>,
) {
    if let MutationWriteIntent::Authored(value) = intent {
        columns.push(column);
        values.push(nullable_text_sql(value.as_deref()));
    }
}

fn sqlite_compatible_statement(
    snapshot: &MutationSnapshot,
    step: &GeneratedMutationStep,
) -> String {
    let mut sql = step.rendered_sql().to_string();
    if let MutationOperation::Update {
        assignment:
            icydb_testing_sql_generator::MutationAssignment::AcceptedDefault {
                name,
                tier,
                score,
                note,
            },
        ..
    } = step.statement().operation()
    {
        replace_update_default(snapshot, MutationFieldRole::Name, name, &mut sql);
        replace_update_default(snapshot, MutationFieldRole::Tier, tier, &mut sql);
        replace_update_default(snapshot, MutationFieldRole::Score, score, &mut sql);
        replace_update_default(snapshot, MutationFieldRole::Note, note, &mut sql);
    }
    sql
}

fn replace_update_default<T>(
    snapshot: &MutationSnapshot,
    role: MutationFieldRole,
    intent: &MutationUpdateIntent<T>,
    sql: &mut String,
) {
    if !matches!(intent, MutationUpdateIntent::Default) {
        return;
    }
    let Some(field) = snapshot.field(role) else {
        return;
    };
    let replacement = match field.default() {
        Some(MutationDefaultValue::NullText) => "NULL".to_string(),
        Some(MutationDefaultValue::Text(value)) => format!("'{}'", quote_text(value)),
        Some(MutationDefaultValue::UnsignedInteger(value)) => value.to_string(),
        None => return,
    };
    *sql = sql.replace(
        &format!("{} = DEFAULT", field.name()),
        &format!("{} = {replacement}", field.name()),
    );
}

fn execute_one_sql(
    transaction: &Transaction<'_>,
    snapshot: &MutationSnapshot,
    sql: &str,
    returning: &icydb_testing_sql_generator::MutationReturning,
) -> rusqlite::Result<(u32, Vec<MutationProjectedRow>)> {
    let roles = returning
        .field_roles(snapshot)
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    if roles.is_empty() {
        let affected = transaction.execute(sql, [])?;
        return Ok((
            u32::try_from(affected).map_err(|_| rusqlite::Error::InvalidQuery)?,
            Vec::new(),
        ));
    }
    let mut statement = transaction.prepare(sql)?;
    let rows = statement.query_map([], |row| projected_row(row, &roles))?;
    let returned = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok((
        u32::try_from(returned.len()).map_err(|_| rusqlite::Error::InvalidQuery)?,
        returned,
    ))
}

fn projected_row(
    row: &Row<'_>,
    roles: &[MutationFieldRole],
) -> rusqlite::Result<MutationProjectedRow> {
    let fields = roles
        .iter()
        .enumerate()
        .map(|(index, role)| {
            sqlite_value(row, index, *role).map(|value| MutationProjectedField::new(*role, value))
        })
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(MutationProjectedRow::new(fields))
}

fn sqlite_value(
    row: &Row<'_>,
    index: usize,
    role: MutationFieldRole,
) -> rusqlite::Result<MutationValue> {
    match role {
        MutationFieldRole::Key | MutationFieldRole::Number | MutationFieldRole::Score => {
            let value = row.get::<_, i64>(index)?;
            let unsigned = u64::try_from(value)
                .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(index, value))?;
            Ok(MutationValue::UnsignedInteger(unsigned))
        }
        MutationFieldRole::Note => Ok(row
            .get::<_, Option<String>>(index)?
            .map_or(MutationValue::Null, MutationValue::Text)),
        MutationFieldRole::Text | MutationFieldRole::Name | MutationFieldRole::Tier => {
            Ok(MutationValue::Text(row.get(index)?))
        }
    }
}

fn append_returning(
    snapshot: &MutationSnapshot,
    returning: &icydb_testing_sql_generator::MutationReturning,
    sql: &mut String,
) -> rusqlite::Result<()> {
    let roles = returning
        .field_roles(snapshot)
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    if !roles.is_empty() {
        let names = roles
            .iter()
            .map(|role| {
                snapshot
                    .field(*role)
                    .map(MutationField::name)
                    .ok_or(rusqlite::Error::InvalidQuery)
            })
            .collect::<rusqlite::Result<Vec<_>>>()?;
        sql.push_str(" RETURNING ");
        sql.push_str(&names.join(", "));
    }
    Ok(())
}

fn read_complete_state(
    transaction: &Transaction<'_>,
    snapshot: &MutationSnapshot,
) -> Result<Vec<MutationRow>, SqliteAdapterError> {
    let names = snapshot
        .fields()
        .iter()
        .map(MutationField::name)
        .collect::<Vec<_>>()
        .join(", ");
    let mut statement = transaction
        .prepare(&format!(
            "SELECT {names} FROM {} ORDER BY {} ASC",
            snapshot.entity_name(),
            required_field_name(snapshot, MutationFieldRole::Key)?,
        ))
        .map_err(result_error)?;
    let rows = statement
        .query_map([], |row| complete_row(row, snapshot.profile()))
        .map_err(result_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(result_error)?;
    Ok(rows)
}

fn read_secondary_index_state(
    transaction: &Transaction<'_>,
    snapshot: &MutationSnapshot,
) -> Result<Vec<MutationIndexEntry>, SqliteAdapterError> {
    if snapshot.profile() == MutationSchemaProfile::AuthoredScalar {
        return Ok(Vec::new());
    }
    let entity = snapshot.entity_name();
    let tier = required_field_name(snapshot, MutationFieldRole::Tier)?;
    let key = required_field_name(snapshot, MutationFieldRole::Key)?;
    let mut statement = transaction
        .prepare(&format!(
            "SELECT {tier}, {key} FROM {entity} INDEXED BY {entity}_tier_idx \
             ORDER BY {tier} ASC, {key} ASC",
        ))
        .map_err(result_error)?;
    statement
        .query_map([], |row| {
            Ok(MutationIndexEntry::new(
                MutationValue::Text(row.get::<_, String>(0)?),
                sqlite_unsigned(row, 1)?,
            ))
        })
        .map_err(result_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(result_error)
}

fn complete_row(row: &Row<'_>, profile: MutationSchemaProfile) -> rusqlite::Result<MutationRow> {
    let key = sqlite_unsigned(row, 0)?;
    match profile {
        MutationSchemaProfile::AuthoredScalar => Ok(MutationRow::authored_scalar(
            key,
            row.get::<_, String>(1)?,
            sqlite_unsigned(row, 2)?,
        )),
        MutationSchemaProfile::AcceptedDefault => Ok(MutationRow::accepted_default(
            key,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            sqlite_unsigned(row, 3)?,
            row.get::<_, Option<String>>(4)?,
        )),
    }
}

fn sqlite_unsigned(row: &Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let value = row.get::<_, i64>(index)?;
    u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(index, value))
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

fn quote_text(value: &str) -> String {
    value.replace('\'', "''")
}

fn nullable_text_sql(value: Option<&str>) -> String {
    value.map_or_else(
        || "NULL".to_string(),
        |value| format!("'{}'", quote_text(value)),
    )
}

fn generated_case_error(
    error: icydb_testing_sql_generator::SqlGeneratorError,
) -> SqliteAdapterError {
    SqliteAdapterError::new(
        SqliteAdapterErrorKind::GeneratedCase,
        format!("generated mutation sequence failed validation: {error}"),
    )
}

fn result_error(source: rusqlite::Error) -> SqliteAdapterError {
    SqliteAdapterError::with_source(
        SqliteAdapterErrorKind::Result,
        "failed to read generated mutation state",
        source,
    )
}
