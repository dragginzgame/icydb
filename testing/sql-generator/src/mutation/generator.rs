//! Module: sql_generator::mutation::generator
//! Responsibility: fixed-budget deterministic mutation sequence generation.
//! Does not own: expected state semantics, SQL execution, or provider comparison.
//! Boundary: selects typed operations from versioned seed identity and delegates all transitions to the independent model.

use crate::{
    GeneratedMutationIdentity, GeneratedMutationSequence, MutationAssignment, MutationBudgets,
    MutationInsertQueryKeySource, MutationOperation, MutationOrder, MutationPredicate, MutationRow,
    MutationSnapshot, MutationStatement, MutationWindow, SqlGeneratorError, SqlGeneratorErrorKind,
    rng::derive_sql_sub_seed,
};

/// Closed structural sequence variants repeated with independently seeded values.
const MUTATION_STRUCTURAL_VARIANT_COUNT: u32 = 4;

/// Stable independently seeded family identity for the current mutation sequence.
const MUTATION_SEQUENCE_FAMILY_ID: &str = "mutation.sequence";

/// Current hard-cut deterministic mutation generator version.
pub const MUTATION_GENERATOR_VERSION: u32 = 1;

/// Required root-local mutation cases.
pub const TIER_A_MUTATION_CASES_PER_ROOT: u64 = 4;

/// Required scheduled root-local mutation cases.
pub const TIER_C_MUTATION_CASES_PER_ROOT: u64 = 16;

/// Generate one deterministic accepted-snapshot-aware mutation sequence.
///
/// # Errors
///
/// Returns a typed generator error when the case index, snapshot, statement,
/// fixture, or deterministic budget is invalid.
pub fn generate_mutation_sequence(
    snapshot: &MutationSnapshot,
    root_seed: u64,
    case_index: u64,
    budgets: MutationBudgets,
) -> Result<GeneratedMutationSequence, SqlGeneratorError> {
    if case_index >= TIER_C_MUTATION_CASES_PER_ROOT {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            format!(
                "mutation case index {case_index} exceeds the current {TIER_C_MUTATION_CASES_PER_ROOT}-case root profile"
            ),
        ));
    }
    let sub_seed = derive_sql_sub_seed(
        MUTATION_GENERATOR_VERSION,
        root_seed,
        MUTATION_SEQUENCE_FAMILY_ID,
        case_index,
    )?;
    let identity = GeneratedMutationIdentity::new(
        mutation_identity_id(root_seed, case_index, sub_seed),
        MUTATION_GENERATOR_VERSION,
        MUTATION_SEQUENCE_FAMILY_ID.to_string(),
        root_seed,
        sub_seed,
        case_index,
    );
    let variant = u32::try_from(case_index).map_err(|_| {
        SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "mutation case index does not fit its bounded variant",
        )
    })?;
    let initial_rows = vec![
        MutationRow::new(1, "alpha", 20),
        MutationRow::new(2, "beta", 30),
        MutationRow::new(3, "beta", 40),
        MutationRow::new(4, "gamma", 40),
    ];
    let statements = sequence_statements(variant, sub_seed)?;

    GeneratedMutationSequence::try_from_statements(
        identity,
        snapshot.clone(),
        initial_rows,
        statements,
        budgets,
    )
}

/// Re-derive every authored mutation identity fact under the current generator.
///
/// # Errors
///
/// Returns a typed case error for a stale version, family, case range, sub-seed,
/// or stable ID. Replay and corpus decoding call this through sequence validation.
pub(crate) fn validate_generated_mutation_identity(
    identity: &GeneratedMutationIdentity,
) -> Result<(), SqlGeneratorError> {
    if identity.generator_version() != MUTATION_GENERATOR_VERSION {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            format!(
                "mutation identity uses generator version {}, expected {MUTATION_GENERATOR_VERSION}",
                identity.generator_version(),
            ),
        ));
    }
    if identity.case_index() >= TIER_C_MUTATION_CASES_PER_ROOT {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "mutation identity case index is outside the current fixed profile",
        ));
    }
    if identity.family_id() != MUTATION_SEQUENCE_FAMILY_ID {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "mutation identity family does not match the current sequence family",
        ));
    }
    let expected_sub_seed = derive_sql_sub_seed(
        MUTATION_GENERATOR_VERSION,
        identity.root_seed(),
        identity.family_id(),
        identity.case_index(),
    )?;
    if identity.sub_seed() != expected_sub_seed {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "mutation identity sub-seed does not match its current BLAKE3 identity",
        ));
    }
    let expected_id = mutation_identity_id(
        identity.root_seed(),
        identity.case_index(),
        expected_sub_seed,
    );
    if identity.id() != expected_id {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "mutation identity ID does not match its current canonical facts",
        ));
    }

    Ok(())
}

fn sequence_statements(
    variant: u32,
    sub_seed: u64,
) -> Result<Vec<MutationStatement>, SqlGeneratorError> {
    let structural_variant = variant % MUTATION_STRUCTURAL_VARIANT_COUNT;
    Ok(vec![
        single_insert_statement(sub_seed),
        multi_insert_statement(),
        exact_update_statement(sub_seed),
        compound_update_statement(variant, sub_seed),
        bounded_update_statement(variant, sub_seed)?,
        no_op_or_insert_query_statement(structural_variant, sub_seed),
        delete_statement(structural_variant, sub_seed)?,
        rejected_insert_statement(structural_variant, sub_seed),
    ])
}

fn single_insert_statement(sub_seed: u64) -> MutationStatement {
    MutationStatement::new(
        MutationOperation::Insert {
            rows: vec![MutationRow::new(
                single_insert_key(sub_seed),
                format!("insert-single-{sub_seed:016x}"),
                500 + sub_seed % 100,
            )],
        },
        sub_seed.is_multiple_of(2),
    )
}

fn multi_insert_statement() -> MutationStatement {
    MutationStatement::new(
        MutationOperation::Insert {
            rows: vec![
                MutationRow::new(20, "insert-multi", 80),
                MutationRow::new(21, "insert-multi", 81),
            ],
        },
        true,
    )
}

const fn exact_update_statement(sub_seed: u64) -> MutationStatement {
    MutationStatement::new(
        MutationOperation::Update {
            predicate: MutationPredicate::KeyEqual { value: 1 },
            assignment: MutationAssignment::Number {
                value: 21 + sub_seed % 5,
            },
            window: None,
        },
        true,
    )
}

fn compound_update_statement(variant: u32, sub_seed: u64) -> MutationStatement {
    MutationStatement::new(
        MutationOperation::Update {
            predicate: MutationPredicate::And {
                left: Box::new(MutationPredicate::TextEqual {
                    value: "beta".to_string(),
                }),
                right: Box::new(MutationPredicate::NumberRange {
                    min_inclusive: 30,
                    max_exclusive: 41,
                }),
            },
            assignment: MutationAssignment::TextAndNumber {
                text: format!("compound-{variant}-{sub_seed:016x}"),
                number: 50 + u64::from(variant),
            },
            window: None,
        },
        true,
    )
}

fn bounded_update_statement(
    variant: u32,
    sub_seed: u64,
) -> Result<MutationStatement, SqlGeneratorError> {
    let order = if variant.is_multiple_of(2) {
        MutationOrder::KeyDescending
    } else {
        MutationOrder::KeyAscending
    };
    Ok(MutationStatement::new(
        MutationOperation::Update {
            predicate: MutationPredicate::NumberRange {
                min_inclusive: 0,
                max_exclusive: 100,
            },
            assignment: MutationAssignment::Number {
                value: 900 + sub_seed % 100,
            },
            window: Some(MutationWindow::try_new(order, 2, 1)?),
        },
        true,
    ))
}

fn no_op_or_insert_query_statement(variant: u32, sub_seed: u64) -> MutationStatement {
    match variant {
        0 => MutationStatement::new(
            MutationOperation::InsertFromQuery {
                predicate: MutationPredicate::KeyEqual {
                    value: 1_000 + sub_seed % 100,
                },
                key_source: MutationInsertQueryKeySource::Key,
            },
            true,
        ),
        1 => MutationStatement::new(
            MutationOperation::InsertFromQuery {
                predicate: MutationPredicate::KeyEqual { value: 4 },
                key_source: MutationInsertQueryKeySource::Number,
            },
            true,
        ),
        _ => MutationStatement::new(
            MutationOperation::Update {
                predicate: MutationPredicate::TextEqual {
                    value: format!("absent-update-{sub_seed:016x}"),
                },
                assignment: MutationAssignment::Text {
                    value: format!("unreachable-{sub_seed:016x}"),
                },
                window: None,
            },
            false,
        ),
    }
}

fn delete_statement(variant: u32, sub_seed: u64) -> Result<MutationStatement, SqlGeneratorError> {
    let operation = match variant {
        0 => MutationStatement::new(
            MutationOperation::Delete {
                predicate: MutationPredicate::KeyEqual { value: 1 },
                window: None,
            },
            true,
        ),
        1 => MutationStatement::new(
            MutationOperation::Delete {
                predicate: MutationPredicate::NumberRange {
                    min_inclusive: 0,
                    max_exclusive: 100,
                },
                window: Some(MutationWindow::try_new(MutationOrder::KeyDescending, 1, 1)?),
            },
            true,
        ),
        2 => MutationStatement::new(
            MutationOperation::Delete {
                predicate: MutationPredicate::TextEqual {
                    value: format!("absent-delete-{sub_seed:016x}"),
                },
                window: None,
            },
            true,
        ),
        3 => MutationStatement::new(
            MutationOperation::Delete {
                predicate: MutationPredicate::All,
                window: None,
            },
            true,
        ),
        _ => {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "mutation sequence variant is outside the current profile",
            ));
        }
    };

    Ok(operation)
}

// Every invalid sequence writes one unique row before encountering either an
// existing key or a same-batch duplicate, proving statement-wide rollback.
fn rejected_insert_statement(variant: u32, sub_seed: u64) -> MutationStatement {
    let duplicate_rows = if matches!(variant, 2 | 3) {
        let duplicate_key = 90 + sub_seed % 8;
        vec![
            MutationRow::new(duplicate_key, "must-not-commit", 999),
            MutationRow::new(duplicate_key, "same-batch-duplicate", 998),
        ]
    } else {
        vec![
            MutationRow::new(100 + sub_seed % 8, "must-not-commit", 999),
            MutationRow::new(single_insert_key(sub_seed), "existing-key-duplicate", 500),
        ]
    };
    MutationStatement::new(
        MutationOperation::Insert {
            rows: duplicate_rows,
        },
        true,
    )
}

const fn single_insert_key(sub_seed: u64) -> u64 {
    10 + sub_seed % 8
}

fn mutation_identity_id(root_seed: u64, case_index: u64, sub_seed: u64) -> String {
    format!(
        "sql-mutation/v{MUTATION_GENERATOR_VERSION}/{MUTATION_SEQUENCE_FAMILY_ID}/{root_seed:016x}/{case_index:016x}/{sub_seed:016x}"
    )
}
