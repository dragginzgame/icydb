//! Module: sql_generator::mutation::generator
//! Responsibility: catalog-bound mutation witness composition and deterministic identity.
//! Does not own: expected state semantics, product execution, or provider comparison.
//! Boundary: typed recipes own structure while the independent model derives every outcome.

use crate::{
    GeneratedMutationIdentity, GeneratedMutationSequence, MutationAssignment, MutationBudgets,
    MutationIngress, MutationInsertQueryKeySource, MutationInsertRow, MutationIntentClass,
    MutationOperation, MutationOrder, MutationPredicate, MutationReturning, MutationRow,
    MutationSchemaProfile, MutationStatement, MutationUpdateIntent, MutationWindow,
    MutationWriteIntent, ScheduledMutationWitness, SqlGeneratorError, SqlGeneratorErrorKind,
    rng::derive_mutation_witness_sub_seed, scheduled_mutation_witnesses,
    structural_derivation::derive_mutation_signature,
};

/// Current hard-cut deterministic mutation generator version.
pub const MUTATION_GENERATOR_VERSION: u32 = 1;

/// Required pull-request repetitions per frozen mutation witness and root.
pub const TIER_A_MUTATION_REPETITIONS: u64 = 1;

/// Required scheduled repetitions per frozen mutation witness and root.
pub const TIER_C_MUTATION_REPETITIONS: u64 = 2;

/// Derive the frozen structural requirement for one reviewed mutation witness.
///
/// This construction derives from the typed recipe independently of the
/// code-owned witness declaration, freezing the expected signature before any
/// scheduled observation runs.
///
/// # Errors
///
/// Returns a typed generator error for an unknown witness or invalid canonical
/// typed construction.
pub fn structural_signature_for_scheduled_mutation_witness(
    witness_id: &str,
) -> Result<crate::StructuralSignature, SqlGeneratorError> {
    let recipe = MutationRecipe::from_witness_id(witness_id)?;
    let snapshot = crate::MutationSnapshot::for_profile(recipe.profile())?;
    let (_, statements) = recipe.material(derive_mutation_witness_sub_seed(
        MUTATION_GENERATOR_VERSION,
        crate::TIER_A_ROOT_SEEDS[0],
        witness_id,
        0,
    )?)?;
    derive_mutation_signature(
        &snapshot,
        recipe.ingress(),
        recipe.intent_class(),
        &statements,
        recipe.violation_code(),
    )
}

/// Generate one catalog-bound mutation witness.
///
/// # Errors
///
/// Returns a typed error when the witness, profile, signature, deterministic
/// identity, statement composition, or independent model disagrees.
pub fn generate_scheduled_mutation_sequence(
    witness: &ScheduledMutationWitness,
    root_seed: u64,
    repetition: u64,
    budgets: MutationBudgets,
) -> Result<GeneratedMutationSequence, SqlGeneratorError> {
    if repetition >= TIER_C_MUTATION_REPETITIONS {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "mutation repetition exceeds the current scheduled profile",
        ));
    }
    let recipe = MutationRecipe::from_witness_id(witness.witness_id())?;
    let sub_seed = derive_mutation_witness_sub_seed(
        MUTATION_GENERATOR_VERSION,
        root_seed,
        witness.witness_id(),
        repetition,
    )?;
    let identity = GeneratedMutationIdentity::new(
        mutation_identity_id(witness.witness_id(), root_seed, repetition),
        MUTATION_GENERATOR_VERSION,
        witness.witness_id().to_string(),
        root_seed,
        sub_seed,
        repetition,
    );
    let snapshot = crate::MutationSnapshot::for_profile(recipe.profile())?;
    let (initial_rows, statements) = recipe.material(sub_seed)?;
    let sequence = GeneratedMutationSequence::try_from_statements(
        identity,
        recipe.ingress(),
        recipe.intent_class(),
        snapshot,
        initial_rows,
        statements,
        budgets,
    )?;
    validate_generated_mutation_witness(&sequence)?;
    let observed_signature = sequence.structural_signature()?;
    if observed_signature != *witness.signature() {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            format!(
                "generated mutation witness {:?} observed signature {} but requires {}",
                witness.witness_id(),
                observed_signature.digest()?,
                witness.signature().digest()?,
            ),
        ));
    }
    Ok(sequence)
}

/// Re-derive every current mutation identity fact.
///
/// # Errors
///
/// Returns a typed error for a stale version, unknown witness, repetition,
/// sub-seed, or stable scenario ID.
pub(crate) fn validate_generated_mutation_identity(
    identity: &GeneratedMutationIdentity,
) -> Result<(), SqlGeneratorError> {
    if identity.generator_version() != MUTATION_GENERATOR_VERSION
        || identity.repetition() >= TIER_C_MUTATION_REPETITIONS
    {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "mutation identity does not use the current generator/repetition contract",
        ));
    }
    MutationRecipe::from_witness_id(identity.witness_id())?;
    let expected_sub_seed = derive_mutation_witness_sub_seed(
        MUTATION_GENERATOR_VERSION,
        identity.root_seed(),
        identity.witness_id(),
        identity.repetition(),
    )?;
    if identity.sub_seed() != expected_sub_seed
        || identity.id()
            != mutation_identity_id(
                identity.witness_id(),
                identity.root_seed(),
                identity.repetition(),
            )
    {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "mutation identity disagrees with its current witness sub-seed or stable ID",
        ));
    }
    Ok(())
}

/// Re-derive one complete scheduled witness without trusting embedded replay material.
///
/// # Errors
///
/// Returns a typed error when signature, profile, ingress, intent, fixture, or
/// statements drift from the frozen typed recipe.
pub(crate) fn validate_generated_mutation_witness(
    sequence: &GeneratedMutationSequence,
) -> Result<(), SqlGeneratorError> {
    let witness = scheduled_mutation_witnesses()?
        .into_iter()
        .find(|witness| witness.witness_id() == sequence.identity().witness_id())
        .ok_or_else(|| {
            SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "generated mutation sequence has no frozen scheduled witness",
            )
        })?;
    let recipe = MutationRecipe::from_witness_id(witness.witness_id())?;
    let (expected_rows, expected_statements) = recipe.material(sequence.identity().sub_seed())?;
    if sequence.snapshot().profile() != recipe.profile()
        || sequence.ingress() != recipe.ingress()
        || sequence.intent_class() != recipe.intent_class()
        || sequence.initial_rows() != expected_rows
        || sequence.statements() != expected_statements
    {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "generated mutation sequence drifted from its frozen typed witness recipe",
        ));
    }
    sequence.structural_signature()?;
    Ok(())
}

#[derive(Clone, Copy)]
enum MutationRecipe {
    AuthoredInsert,
    AuthoredInsertFromQuery,
    AuthoredWindowed,
    DefaultDeleteReturning,
    DefaultInsertAuthored,
    DefaultInsertExplicit,
    DefaultInsertMixedBatch,
    DefaultInsertOmitted,
    DefaultNoMatch,
    DefaultRejectDuplicate,
    DefaultRejectPkDefault,
    DefaultRejectRequired,
    DefaultUpdateAuthored,
    DefaultUpdateDefault,
    DefaultUpdatePreserve,
}

impl MutationRecipe {
    fn from_witness_id(witness_id: &str) -> Result<Self, SqlGeneratorError> {
        match witness_id {
            "tier_c.mutation.authored_insert" => Ok(Self::AuthoredInsert),
            "tier_c.mutation.authored_insert_from_query" => Ok(Self::AuthoredInsertFromQuery),
            "tier_c.mutation.authored_windowed" => Ok(Self::AuthoredWindowed),
            "tier_c.mutation.default_delete_returning" => Ok(Self::DefaultDeleteReturning),
            "tier_c.mutation.default_insert_authored" => Ok(Self::DefaultInsertAuthored),
            "tier_c.mutation.default_insert_explicit" => Ok(Self::DefaultInsertExplicit),
            "tier_c.mutation.default_insert_mixed_batch" => Ok(Self::DefaultInsertMixedBatch),
            "tier_c.mutation.default_insert_omitted" => Ok(Self::DefaultInsertOmitted),
            "tier_c.mutation.default_no_match" => Ok(Self::DefaultNoMatch),
            "tier_c.mutation.default_reject_duplicate" => Ok(Self::DefaultRejectDuplicate),
            "tier_c.mutation.default_reject_pk_default" => Ok(Self::DefaultRejectPkDefault),
            "tier_c.mutation.default_reject_required" => Ok(Self::DefaultRejectRequired),
            "tier_c.mutation.default_update_authored" => Ok(Self::DefaultUpdateAuthored),
            "tier_c.mutation.default_update_default" => Ok(Self::DefaultUpdateDefault),
            "tier_c.mutation.default_update_preserve" => Ok(Self::DefaultUpdatePreserve),
            _ => Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                format!("unsupported scheduled mutation witness {witness_id:?}"),
            )),
        }
    }

    const fn profile(self) -> MutationSchemaProfile {
        match self {
            Self::AuthoredInsert | Self::AuthoredInsertFromQuery | Self::AuthoredWindowed => {
                MutationSchemaProfile::AuthoredScalar
            }
            Self::DefaultDeleteReturning
            | Self::DefaultInsertAuthored
            | Self::DefaultInsertExplicit
            | Self::DefaultInsertMixedBatch
            | Self::DefaultInsertOmitted
            | Self::DefaultNoMatch
            | Self::DefaultRejectDuplicate
            | Self::DefaultRejectPkDefault
            | Self::DefaultRejectRequired
            | Self::DefaultUpdateAuthored
            | Self::DefaultUpdateDefault
            | Self::DefaultUpdatePreserve => MutationSchemaProfile::AcceptedDefault,
        }
    }

    const fn violation_code(self) -> Option<&'static str> {
        match self {
            Self::DefaultRejectDuplicate => Some("unique_violation"),
            Self::DefaultRejectPkDefault => Some("primary_key_default_forbidden"),
            Self::DefaultRejectRequired => Some("required_field_missing"),
            _ => None,
        }
    }

    const fn ingress(self) -> MutationIngress {
        match self {
            Self::AuthoredInsertFromQuery
            | Self::DefaultInsertExplicit
            | Self::DefaultInsertMixedBatch
            | Self::DefaultRejectPkDefault
            | Self::DefaultRejectRequired
            | Self::DefaultUpdateDefault => MutationIngress::Sql,
            Self::AuthoredInsert
            | Self::AuthoredWindowed
            | Self::DefaultDeleteReturning
            | Self::DefaultInsertAuthored
            | Self::DefaultInsertOmitted
            | Self::DefaultNoMatch
            | Self::DefaultRejectDuplicate
            | Self::DefaultUpdateAuthored
            | Self::DefaultUpdatePreserve => MutationIngress::SqlAndTyped,
        }
    }

    const fn intent_class(self) -> MutationIntentClass {
        match self {
            Self::DefaultInsertExplicit
            | Self::DefaultRejectPkDefault
            | Self::DefaultUpdateDefault => MutationIntentClass::ExplicitDefault,
            Self::DefaultInsertMixedBatch | Self::DefaultRejectDuplicate => {
                MutationIntentClass::MixedBatch
            }
            Self::DefaultInsertOmitted | Self::DefaultRejectRequired => {
                MutationIntentClass::Omitted
            }
            Self::DefaultUpdatePreserve => MutationIntentClass::Preserve,
            Self::AuthoredInsert
            | Self::AuthoredInsertFromQuery
            | Self::AuthoredWindowed
            | Self::DefaultDeleteReturning
            | Self::DefaultInsertAuthored
            | Self::DefaultNoMatch
            | Self::DefaultUpdateAuthored => MutationIntentClass::Authored,
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one exhaustive typed match freezes all 15 scheduled mutation recipes"
    )]
    fn material(
        self,
        sub_seed: u64,
    ) -> Result<(Vec<MutationRow>, Vec<MutationStatement>), SqlGeneratorError> {
        let material = match self {
            Self::AuthoredInsert => (
                Vec::new(),
                vec![
                    MutationStatement::new(
                        MutationOperation::Insert {
                            rows: vec![MutationInsertRow::authored_scalar(
                                1,
                                format!("single-{sub_seed:016x}"),
                                10 + sub_seed % 5,
                            )],
                        },
                        MutationReturning::AllFields,
                    ),
                    MutationStatement::new(
                        MutationOperation::Insert {
                            rows: vec![
                                MutationInsertRow::authored_scalar(2, "multi", 20),
                                MutationInsertRow::authored_scalar(3, "multi", 21),
                            ],
                        },
                        MutationReturning::None,
                    ),
                ],
            ),
            Self::AuthoredInsertFromQuery => (
                authored_rows(),
                vec![MutationStatement::new(
                    MutationOperation::InsertFromQuery {
                        predicate: MutationPredicate::NumberRange {
                            min_inclusive: 20,
                            max_exclusive: 40,
                        },
                        key_source: MutationInsertQueryKeySource::Number,
                    },
                    MutationReturning::None,
                )],
            ),
            Self::AuthoredWindowed => (
                authored_rows(),
                vec![
                    MutationStatement::new(
                        MutationOperation::Update {
                            predicate: MutationPredicate::KeyEqual { value: 1 },
                            assignment: MutationAssignment::AuthoredNumber { value: 25 },
                            window: None,
                        },
                        MutationReturning::AllFields,
                    ),
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
                            assignment: MutationAssignment::AuthoredTextAndNumber {
                                text: format!("compound-{sub_seed:016x}"),
                                number: 50,
                            },
                            window: None,
                        },
                        MutationReturning::None,
                    ),
                    MutationStatement::new(
                        MutationOperation::Update {
                            predicate: MutationPredicate::NumberRange {
                                min_inclusive: 0,
                                max_exclusive: 100,
                            },
                            assignment: MutationAssignment::AuthoredNumber { value: 90 },
                            window: Some(MutationWindow::try_new(
                                MutationOrder::KeyAscending,
                                2,
                                0,
                            )?),
                        },
                        MutationReturning::AllFields,
                    ),
                    MutationStatement::new(
                        MutationOperation::Delete {
                            predicate: MutationPredicate::NumberRange {
                                min_inclusive: 0,
                                max_exclusive: 100,
                            },
                            window: Some(MutationWindow::try_new(
                                MutationOrder::KeyAscending,
                                1,
                                1,
                            )?),
                        },
                        MutationReturning::AllFields,
                    ),
                ],
            ),
            Self::DefaultDeleteReturning => (
                vec![default_nondefault_row(1)],
                vec![MutationStatement::new(
                    MutationOperation::Delete {
                        predicate: MutationPredicate::KeyEqual { value: 1 },
                        window: None,
                    },
                    MutationReturning::AllFields,
                )],
            ),
            Self::DefaultInsertAuthored => (
                Vec::new(),
                vec![MutationStatement::new(
                    MutationOperation::Insert {
                        rows: vec![default_authored_insert(1, "authored")],
                    },
                    MutationReturning::AllFields,
                )],
            ),
            Self::DefaultInsertExplicit => (
                Vec::new(),
                vec![MutationStatement::new(
                    MutationOperation::Insert {
                        rows: vec![default_explicit_insert(1, "explicit")],
                    },
                    MutationReturning::AllFields,
                )],
            ),
            Self::DefaultInsertMixedBatch => (
                Vec::new(),
                vec![MutationStatement::new(
                    MutationOperation::Insert {
                        rows: vec![
                            default_authored_insert(1, "authored"),
                            default_omitted_insert(2, "omitted"),
                            default_explicit_insert(3, "explicit"),
                        ],
                    },
                    MutationReturning::AllFields,
                )],
            ),
            Self::DefaultInsertOmitted => (
                Vec::new(),
                vec![MutationStatement::new(
                    MutationOperation::Insert {
                        rows: vec![default_omitted_insert(1, "omitted")],
                    },
                    MutationReturning::AllFields,
                )],
            ),
            Self::DefaultNoMatch => (
                vec![default_nondefault_row(1)],
                vec![
                    MutationStatement::new(
                        MutationOperation::Update {
                            predicate: MutationPredicate::KeyEqual { value: 99 },
                            assignment: default_name_update("unreachable"),
                            window: None,
                        },
                        MutationReturning::AllFields,
                    ),
                    MutationStatement::new(
                        MutationOperation::Delete {
                            predicate: MutationPredicate::KeyEqual { value: 99 },
                            window: None,
                        },
                        MutationReturning::AllFields,
                    ),
                ],
            ),
            Self::DefaultRejectDuplicate => (
                vec![default_nondefault_row(1)],
                vec![MutationStatement::new(
                    MutationOperation::Insert {
                        rows: vec![
                            default_omitted_insert(2, "must-not-commit"),
                            default_explicit_insert(1, "duplicate"),
                        ],
                    },
                    MutationReturning::AllFields,
                )],
            ),
            Self::DefaultRejectPkDefault => (
                vec![default_nondefault_row(1)],
                vec![MutationStatement::new(
                    MutationOperation::Insert {
                        rows: vec![MutationInsertRow::accepted_default(
                            MutationWriteIntent::Default,
                            MutationWriteIntent::Authored("pk-default".to_string()),
                            MutationWriteIntent::Omitted,
                            MutationWriteIntent::Omitted,
                            MutationWriteIntent::Omitted,
                        )],
                    },
                    MutationReturning::AllFields,
                )],
            ),
            Self::DefaultRejectRequired => (
                vec![default_nondefault_row(1)],
                vec![MutationStatement::new(
                    MutationOperation::Insert {
                        rows: vec![MutationInsertRow::accepted_default(
                            MutationWriteIntent::Authored(2),
                            MutationWriteIntent::Omitted,
                            MutationWriteIntent::Omitted,
                            MutationWriteIntent::Omitted,
                            MutationWriteIntent::Omitted,
                        )],
                    },
                    MutationReturning::AllFields,
                )],
            ),
            Self::DefaultUpdateAuthored => (
                vec![default_nondefault_row(1)],
                vec![MutationStatement::new(
                    MutationOperation::Update {
                        predicate: MutationPredicate::KeyEqual { value: 1 },
                        assignment: MutationAssignment::AcceptedDefault {
                            name: MutationUpdateIntent::Preserve,
                            tier: MutationUpdateIntent::Authored("gold".to_string()),
                            score: MutationUpdateIntent::Authored(99),
                            note: MutationUpdateIntent::Authored(Some("authored".to_string())),
                        },
                        window: None,
                    },
                    MutationReturning::AllFields,
                )],
            ),
            Self::DefaultUpdateDefault => (
                vec![default_nondefault_row(1)],
                vec![MutationStatement::new(
                    MutationOperation::Update {
                        predicate: MutationPredicate::KeyEqual { value: 1 },
                        assignment: MutationAssignment::AcceptedDefault {
                            name: MutationUpdateIntent::Preserve,
                            tier: MutationUpdateIntent::Default,
                            score: MutationUpdateIntent::Default,
                            note: MutationUpdateIntent::Default,
                        },
                        window: None,
                    },
                    MutationReturning::Fields(vec![
                        crate::MutationFieldRole::Tier,
                        crate::MutationFieldRole::Score,
                        crate::MutationFieldRole::Note,
                    ]),
                )],
            ),
            Self::DefaultUpdatePreserve => (
                vec![default_nondefault_row(1)],
                vec![MutationStatement::new(
                    MutationOperation::Update {
                        predicate: MutationPredicate::KeyEqual { value: 1 },
                        assignment: default_name_update("renamed"),
                        window: None,
                    },
                    MutationReturning::AllFields,
                )],
            ),
        };
        Ok(material)
    }
}

fn authored_rows() -> Vec<MutationRow> {
    vec![
        MutationRow::authored_scalar(1, "alpha", 20),
        MutationRow::authored_scalar(2, "beta", 30),
        MutationRow::authored_scalar(3, "beta", 40),
        MutationRow::authored_scalar(4, "gamma", 40),
    ]
}

fn default_nondefault_row(key: u64) -> MutationRow {
    MutationRow::accepted_default(
        key,
        format!("name-{key}"),
        "silver",
        42,
        Some("existing".to_string()),
    )
}

fn default_authored_insert(key: u64, name: &str) -> MutationInsertRow {
    MutationInsertRow::accepted_default(
        MutationWriteIntent::Authored(key),
        MutationWriteIntent::Authored(name.to_string()),
        MutationWriteIntent::Authored("silver".to_string()),
        MutationWriteIntent::Authored(42),
        MutationWriteIntent::Authored(Some("authored".to_string())),
    )
}

fn default_omitted_insert(key: u64, name: &str) -> MutationInsertRow {
    MutationInsertRow::accepted_default(
        MutationWriteIntent::Authored(key),
        MutationWriteIntent::Authored(name.to_string()),
        MutationWriteIntent::Omitted,
        MutationWriteIntent::Omitted,
        MutationWriteIntent::Omitted,
    )
}

fn default_explicit_insert(key: u64, name: &str) -> MutationInsertRow {
    MutationInsertRow::accepted_default(
        MutationWriteIntent::Authored(key),
        MutationWriteIntent::Authored(name.to_string()),
        MutationWriteIntent::Default,
        MutationWriteIntent::Default,
        MutationWriteIntent::Default,
    )
}

fn default_name_update(name: &str) -> MutationAssignment {
    MutationAssignment::AcceptedDefault {
        name: MutationUpdateIntent::Authored(name.to_string()),
        tier: MutationUpdateIntent::Preserve,
        score: MutationUpdateIntent::Preserve,
        note: MutationUpdateIntent::Preserve,
    }
}

fn mutation_identity_id(witness_id: &str, root_seed: u64, repetition: u64) -> String {
    format!(
        "sql-mutation/v{MUTATION_GENERATOR_VERSION}/{witness_id}/{root_seed:016x}/{repetition:016x}"
    )
}
