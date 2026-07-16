//! Module: sql_generator::mutation::generator
//! Responsibility: fixed-budget deterministic mutation sequence generation.
//! Does not own: expected state semantics, SQL execution, or provider comparison.
//! Boundary: selects typed operations from versioned seed identity and delegates all transitions to the independent model.

use crate::{
    GeneratedMutationIdentity, GeneratedMutationSequence, MutationAssignment, MutationBudgets,
    MutationInsertQueryKeySource, MutationOperation, MutationOrder, MutationPredicate, MutationRow,
    MutationSnapshot, MutationStatement, MutationWindow, SqlGeneratorError, SqlGeneratorErrorKind,
};

/// Current hard-cut deterministic mutation generator version.
pub const MUTATION_GENERATOR_VERSION: u32 = 1;

/// Required native Tier A mutation roots.
pub const TIER_A_MUTATION_ROOT_SEEDS: &[u64] = &[0x1cdb_0204_0000_0003, 0x1cdb_0204_0000_0004];

/// Required root-local mutation cases.
pub const TIER_A_MUTATION_CASES_PER_ROOT: u64 = 4;

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
    if case_index >= TIER_A_MUTATION_CASES_PER_ROOT {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            format!(
                "mutation case index {case_index} exceeds the current {TIER_A_MUTATION_CASES_PER_ROOT}-case root profile"
            ),
        ));
    }
    let sub_seed = derive_sub_seed(root_seed, case_index);
    let identity = GeneratedMutationIdentity::new(
        format!(
            "sql-mutation/v{MUTATION_GENERATOR_VERSION}/{root_seed:016x}/{case_index:016x}/{sub_seed:016x}"
        ),
        MUTATION_GENERATOR_VERSION,
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

fn sequence_statements(
    variant: u32,
    sub_seed: u64,
) -> Result<Vec<MutationStatement>, SqlGeneratorError> {
    Ok(vec![
        single_insert_statement(sub_seed),
        multi_insert_statement(),
        exact_update_statement(sub_seed),
        compound_update_statement(variant, sub_seed),
        bounded_update_statement(variant, sub_seed)?,
        no_op_or_insert_query_statement(variant, sub_seed),
        delete_statement(variant, sub_seed)?,
        rejected_insert_statement(variant, sub_seed),
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

fn derive_sub_seed(root_seed: u64, case_index: u64) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"icydb/sql-mutation");
    hasher.update(&MUTATION_GENERATOR_VERSION.to_le_bytes());
    hasher.update(&root_seed.to_le_bytes());
    hasher.update(&case_index.to_le_bytes());
    let digest = hasher.finalize();
    let bytes = digest.as_bytes();
    u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}
