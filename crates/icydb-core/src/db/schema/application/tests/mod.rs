#[cfg(feature = "sql")]
mod sql_activations;

mod row_presence;
mod topology;

// Forced pending fixtures retain the same source proof as real admission.
fn bind_pending_lineage(
    proposal: &SchemaProposal,
    current: &AcceptedSchemaRevisionBundle,
    record: SchemaApplicationRecord,
) -> SchemaApplicationRecord {
    let before = super::load_entity_source_lineage_catalog()
        .unwrap()
        .unwrap_or_default();
    let (_, pending) = super::prepare_ordinary_source_lineage(
        before,
        proposal,
        &[Some(current.clone())],
        proposal.expected_head(),
        record.activations().first(),
    )
    .unwrap();
    record.with_pending_source_lineage(pending).unwrap()
}

mod entity_creation;

mod collection_relations;

#[cfg(feature = "migration")]
mod identity_field_removal;

#[cfg(feature = "migration")]
mod populated_field_removal;

#[cfg(feature = "migration")]
mod entity_rename;

#[cfg(all(feature = "migration", feature = "sql"))]
mod migration_staging;

#[cfg(feature = "migration")]
mod nested_migration;

#[cfg(feature = "migration")]
mod migration_page_limits;

#[cfg(feature = "migration")]
mod record_member_rename;

use super::{
    AcceptedSchemaPublication, AcceptedStoreHead, DirectGeneratedRowLocalProof,
    PendingGeneratedRowLocalConstraint, abort_schema_application,
    aborted_generated_row_local_candidate, accepted_head_after_candidates, application_authorities,
    apply_schema, continue_schema_application, derive_accepted_head, derive_schema_change_job_id,
    final_candidates_for_pending_row_local_constraint, generated_database_identity,
    include_identity_state_count, lower_existing_schema_proposal, lower_initial_schema_proposal,
    publish_accepted_schema_candidates_with_application_record, schema_application_target,
};
use crate::{
    db::{
        DatabaseStartupState, Db, GeneratedStartupDriverStep,
        commit::{
            RecoveryProgress, continue_recovery, database_incarnation_id,
            forget_recovered_domain_for_tests,
        },
        data::DataStore,
        drive_generated_startup_recovery_page,
        index::IndexStore,
        journal::JournalTailStore,
        observe_generated_startup_state,
        registry::{
            StoreAllocationIdentities, StoreAllocationIdentity, StoreHandle, StoreRegistry,
            StoreRuntimeStorageCapabilities,
        },
        schema::{
            AcceptedConstraintKind, AcceptedRuleOperation, AcceptedSchemaRevisionBundle,
            CandidateSchemaRevision, ConstraintOrigin, ConstraintValidationJob,
            ExistingProposalStore, ProposalStoreTarget, SchemaApplicationRecord,
            SchemaApplicationRecordOp, SchemaChangeActivation, SchemaChangeJob,
            SchemaChangeOutcome, SchemaChangeProgressStatus, SchemaStore,
            cardinality_build::{
                CardinalityBuildAuthority, CardinalityGenerationPageOutcome,
                drive_cardinality_generation_page,
            },
        },
    },
    error::{ErrorClass, ErrorOrigin},
    testing::test_memory,
    traits::{CanisterKind, Path},
};
use icydb_schema::{
    ConstraintFragment, ConstraintSourceKey, DeclaredEntityVersion, EntityFragment,
    EntitySourceKey, EntityStoreAssignment, ExpectedAcceptedHead, ExpectedSchemaFingerprint,
    FieldFragment, FieldInsertPolicy, FieldSourceKey, FieldType, NamedTypeFragment, RuleSourceKey,
    ScalarLiteral, ScalarType, SchemaCapability, SchemaFragment, SchemaName, SchemaProposal,
    SchemaSubmissionKey, SourceCheckExpr, SourceCheckInstruction, SourceRuleOperation,
    TargetDatabaseIdentity, TargetStoreIdentity, TargetedRuleFragment, TypeSourceKey,
};
use std::cell::RefCell;

fn drive_startup_recovery_to_completion<C: CanisterKind>(db: &Db<C>) {
    for _ in 0..1_024 {
        match continue_recovery(db).expect("test startup recovery page should succeed") {
            RecoveryProgress::Complete => return,
            RecoveryProgress::Pending => {}
        }
    }
    panic!("test startup recovery should complete within 1,024 bounded pages");
}

// Reset only derived live projections; replay retains the original durable data.
#[cfg(feature = "migration")]
fn restart_journaled_store<C: CanisterKind>(db: &Db<C>, store_path: &str) {
    let store = db.store_handle(store_path).unwrap();
    let generation = store.with_data_mut(|data| {
        data.reset_journaled_live_projection().unwrap();
        data.generation()
    });
    let watermark = store
        .journal_tail_store()
        .unwrap()
        .with_borrow(JournalTailStore::fold_watermark)
        .unwrap();
    store
        .with_index_mut(|index| {
            index.reset_journaled_live_projection(
                generation,
                watermark,
                &std::collections::BTreeSet::new(),
            )
        })
        .unwrap();
    store
        .with_schema_mut(SchemaStore::reset_journaled_live_projection)
        .unwrap();
    forget_recovered_domain_for_tests(db).unwrap();
    drive_startup_recovery_to_completion(db);
}

fn drive_cardinality_to_ready(store: StoreHandle) {
    let journal = store
        .journal_tail_store()
        .expect("cardinality fixture store should be journaled");
    for _ in 0..8 {
        let outcome = store
            .with_data(|data| {
                store.with_index(|index| {
                    store.with_schema_mut(|schema| {
                        drive_cardinality_generation_page(data, index, schema, |schema| {
                            CardinalityBuildAuthority::derive(
                                schema,
                                database_incarnation_id()?,
                                store.allocation_identities(),
                                journal.with_borrow(JournalTailStore::fold_watermark)?,
                            )
                        })
                    })
                })
            })
            .expect("bounded cardinality generation should advance");
        if outcome == CardinalityGenerationPageOutcome::Quiescent {
            return;
        }
    }
    panic!("cardinality generation should become Ready within eight bounded pages");
}

#[cfg(feature = "migration")]
use crate::db::schema::SchemaChangeReceipt;
use crate::{
    db::{DbSession, DynamicMutation, DynamicStructuralPatch, DynamicWriteCell},
    value::InputValue,
};
#[cfg(feature = "migration")]
use icydb_schema::{
    EntityMigration, IndexFragment, IndexKeyFragment, RelationDeleteAction, RelationFragment,
    SchemaMigrationPlan, SchemaMigrationTransform, SchemaProposalDigest,
};

fn version_one() -> DeclaredEntityVersion {
    DeclaredEntityVersion::try_new(1).expect("fixture version should admit")
}

const ABORT_STORE_PATH: &str = "schema_application_tests::AbortStore";
const EVOLUTION_STORE_PATH: &str = "schema_application_tests::EvolutionStore";
#[cfg(feature = "migration")]
const MIGRATION_STORE_PATH: &str = "schema_application_tests::MigrationStore";
#[cfg(feature = "migration")]
const MIGRATION_EXECUTION_STORE_PATH: &str = "schema_application_tests::MigrationExecutionStore";
#[cfg(feature = "migration")]
const MIGRATION_FINDING_STORE_PATH: &str = "schema_application_tests::MigrationFindingStore";

#[test]
fn database_identity_state_capacity_combines_store_inventories_exactly() {
    let below = include_identity_state_count(0, 65_535)
        .expect("the first store inventory should remain below the database cap");
    let exact = include_identity_state_count(below, 1)
        .expect("the combined database boundary should admit");
    assert_eq!(exact, 65_536);

    let error = include_identity_state_count(exact, 1)
        .expect_err("the next owner in another store must reject");
    assert_eq!(error.class(), ErrorClass::Unsupported);
    assert_eq!(error.origin(), ErrorOrigin::Identity);
}

#[test]
fn generated_database_identity_cache_is_bound_to_the_incarnation() {
    let first_incarnation = crate::db::DatabaseIncarnationId::for_tests(0x41);
    let second_incarnation = crate::db::DatabaseIncarnationId::for_tests(0x42);
    let first = generated_database_identity(first_incarnation);
    assert_eq!(generated_database_identity(first_incarnation), first,);
    let second = generated_database_identity(second_incarnation);
    assert_ne!(second, first);
    assert_eq!(generated_database_identity(first_incarnation), first,);
}

thread_local! {
    static ABORT_DATA_MEMORY: crate::testing::TestMemoryOwner = crate::testing::test_memory_owner(180);
    static ABORT_INDEX_MEMORY: crate::testing::TestMemoryOwner = crate::testing::test_memory_owner(181);
    static ABORT_SCHEMA_MEMORY: crate::testing::TestMemoryOwner = crate::testing::test_memory_owner(182);
    static ABORT_JOURNAL_MEMORY: crate::testing::TestMemoryOwner = crate::testing::test_memory_owner(183);
    static ABORT_DATA: RefCell<DataStore> =
        ABORT_DATA_MEMORY.with(|memory| {
            RefCell::new(DataStore::init_journaled(memory.open()).expect("fixture storage should initialize"))
        });
    static ABORT_INDEX: RefCell<IndexStore> =
        ABORT_INDEX_MEMORY.with(|memory| {
            RefCell::new(IndexStore::init_journaled(memory.open()).expect("fixture storage should initialize"))
        });
    static ABORT_SCHEMA: RefCell<SchemaStore> =
        ABORT_SCHEMA_MEMORY.with(|memory| {
            RefCell::new(SchemaStore::init_journaled(memory.open()).expect("fixture storage should initialize"))
        });
    static ABORT_JOURNAL: RefCell<JournalTailStore> =
        ABORT_JOURNAL_MEMORY.with(|memory| {
            RefCell::new(JournalTailStore::init(memory.open()).expect("fixture storage should initialize"))
        });
    static ABORT_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_journaled_store(
            ABORT_STORE_PATH,
            &ABORT_DATA,
            &ABORT_INDEX,
            &ABORT_SCHEMA,
            &ABORT_JOURNAL,
            StoreAllocationIdentities::new_journaled(
                StoreAllocationIdentity::new(180, "icydb.test.application_abort.data.v1"),
                StoreAllocationIdentity::new(181, "icydb.test.application_abort.index.v1"),
                StoreAllocationIdentity::new(182, "icydb.test.application_abort.schema.v1"),
                StoreAllocationIdentity::new(183, "icydb.test.application_abort.journal.v1"),
            ),
            StoreRuntimeStorageCapabilities::journaled(),
        ).expect("abort journaled store should register");
        registry
    };
}

#[cfg(feature = "migration")]
thread_local! {
    static MIGRATION_EXECUTION_DATA: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(210)).expect("fixture storage should initialize"));
    static MIGRATION_EXECUTION_INDEX: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(211)).expect("fixture storage should initialize"));
    static MIGRATION_EXECUTION_SCHEMA: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(212)).expect("fixture storage should initialize"));
    static MIGRATION_EXECUTION_JOURNAL: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(213)).expect("fixture storage should initialize"));
    static MIGRATION_EXECUTION_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_journaled_store(
            MIGRATION_EXECUTION_STORE_PATH,
            &MIGRATION_EXECUTION_DATA,
            &MIGRATION_EXECUTION_INDEX,
            &MIGRATION_EXECUTION_SCHEMA,
            &MIGRATION_EXECUTION_JOURNAL,
            StoreAllocationIdentities::new_journaled(
                StoreAllocationIdentity::new(210, "icydb.test.migration_execution.data.v1"),
                StoreAllocationIdentity::new(211, "icydb.test.migration_execution.index.v1"),
                StoreAllocationIdentity::new(212, "icydb.test.migration_execution.schema.v1"),
                StoreAllocationIdentity::new(213, "icydb.test.migration_execution.journal.v1"),
            ),
            StoreRuntimeStorageCapabilities::journaled(),
        ).expect("migration execution store should register");
        registry
    };
}

#[cfg(feature = "migration")]
thread_local! {
    static MIGRATION_DATA: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(200)).expect("fixture storage should initialize"));
    static MIGRATION_INDEX: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(201)).expect("fixture storage should initialize"));
    static MIGRATION_SCHEMA: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(202)).expect("fixture storage should initialize"));
    static MIGRATION_JOURNAL: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(203)).expect("fixture storage should initialize"));
    static MIGRATION_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_journaled_store(
            MIGRATION_STORE_PATH,
            &MIGRATION_DATA,
            &MIGRATION_INDEX,
            &MIGRATION_SCHEMA,
            &MIGRATION_JOURNAL,
            StoreAllocationIdentities::new_journaled(
                StoreAllocationIdentity::new(200, "icydb.test.migration_validation.data.v1"),
                StoreAllocationIdentity::new(201, "icydb.test.migration_validation.index.v1"),
                StoreAllocationIdentity::new(202, "icydb.test.migration_validation.schema.v1"),
                StoreAllocationIdentity::new(203, "icydb.test.migration_validation.journal.v1"),
            ),
            StoreRuntimeStorageCapabilities::journaled(),
        ).expect("migration validation store should register");
        registry
    };
}

#[cfg(feature = "migration")]
thread_local! {
    static MIGRATION_FINDING_DATA: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(206)).expect("fixture storage should initialize"));
    static MIGRATION_FINDING_INDEX: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(207)).expect("fixture storage should initialize"));
    static MIGRATION_FINDING_SCHEMA: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(208)).expect("fixture storage should initialize"));
    static MIGRATION_FINDING_JOURNAL: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(209)).expect("fixture storage should initialize"));
    static MIGRATION_FINDING_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_journaled_store(
            MIGRATION_FINDING_STORE_PATH,
            &MIGRATION_FINDING_DATA,
            &MIGRATION_FINDING_INDEX,
            &MIGRATION_FINDING_SCHEMA,
            &MIGRATION_FINDING_JOURNAL,
            StoreAllocationIdentities::new_journaled(
                StoreAllocationIdentity::new(206, "icydb.test.migration_finding.data.v1"),
                StoreAllocationIdentity::new(207, "icydb.test.migration_finding.index.v1"),
                StoreAllocationIdentity::new(208, "icydb.test.migration_finding.schema.v1"),
                StoreAllocationIdentity::new(209, "icydb.test.migration_finding.journal.v1"),
            ),
            StoreRuntimeStorageCapabilities::journaled(),
        ).expect("migration finding store should register");
        registry
    };
}

thread_local! {
    static EVOLUTION_DATA: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(192)).expect("fixture storage should initialize"));
    static EVOLUTION_INDEX: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(193)).expect("fixture storage should initialize"));
    static EVOLUTION_SCHEMA: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(194)).expect("fixture storage should initialize"));
    static EVOLUTION_JOURNAL: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(195)).expect("fixture storage should initialize"));
    static EVOLUTION_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_journaled_store(
            EVOLUTION_STORE_PATH,
            &EVOLUTION_DATA,
            &EVOLUTION_INDEX,
            &EVOLUTION_SCHEMA,
            &EVOLUTION_JOURNAL,
            StoreAllocationIdentities::new_journaled(
                StoreAllocationIdentity::new(192, "icydb.test.rule_evolution.data.v1"),
                StoreAllocationIdentity::new(193, "icydb.test.rule_evolution.index.v1"),
                StoreAllocationIdentity::new(194, "icydb.test.rule_evolution.schema.v1"),
                StoreAllocationIdentity::new(195, "icydb.test.rule_evolution.journal.v1"),
            ),
            StoreRuntimeStorageCapabilities::journaled(),
        ).expect("rule-evolution journaled store should register");
        registry
    };
}

struct AbortCanister;

impl Path for AbortCanister {
    const PATH: &'static str = "schema_application_tests::AbortCanister";
}

impl CanisterKind for AbortCanister {
    fn commit_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(184)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.test.application_abort.commit.v1";
    fn startup_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(186)
    }
    const STARTUP_STABLE_KEY: &'static str = "icydb.test.application_abort.startup.control.v1";
    fn integrity_progress_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(185)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str = "icydb.test.application_abort.integrity.v1";
}

struct EvolutionCanister;

impl Path for EvolutionCanister {
    const PATH: &'static str = "schema_application_tests::EvolutionCanister";
}

impl CanisterKind for EvolutionCanister {
    fn commit_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(196)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.test.rule_evolution.commit.v1";
    fn startup_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(198)
    }
    const STARTUP_STABLE_KEY: &'static str = "icydb.test.rule_evolution.startup.control.v1";
    fn integrity_progress_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(197)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str = "icydb.test.rule_evolution.integrity.v1";
}

#[cfg(feature = "migration")]
struct MigrationCanister;

#[cfg(feature = "migration")]
impl Path for MigrationCanister {
    const PATH: &'static str = "schema_application_tests::MigrationCanister";
}

#[cfg(feature = "migration")]
impl CanisterKind for MigrationCanister {
    fn commit_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(204)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.test.migration_validation.commit.v1";
    fn startup_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(206)
    }
    const STARTUP_STABLE_KEY: &'static str = "icydb.test.migration_validation.startup.control.v1";
    fn integrity_progress_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(205)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
        "icydb.test.migration_validation.integrity.v1";
}

#[cfg(feature = "migration")]
struct MigrationExecutionCanister;

#[cfg(feature = "migration")]
impl Path for MigrationExecutionCanister {
    const PATH: &'static str = "schema_application_tests::MigrationExecutionCanister";
}

#[cfg(feature = "migration")]
impl CanisterKind for MigrationExecutionCanister {
    fn commit_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(214)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.test.migration_execution.commit.v1";
    fn startup_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(216)
    }
    const STARTUP_STABLE_KEY: &'static str = "icydb.test.migration_execution.startup.control.v1";
    fn integrity_progress_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(215)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
        "icydb.test.migration_execution.integrity.v1";
}

#[cfg(feature = "migration")]
struct MigrationFindingCanister;

#[cfg(feature = "migration")]
impl Path for MigrationFindingCanister {
    const PATH: &'static str = "schema_application_tests::MigrationFindingCanister";
}

#[cfg(feature = "migration")]
impl CanisterKind for MigrationFindingCanister {
    fn commit_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(210)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.test.migration_finding.commit.v1";
    fn startup_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(212)
    }
    const STARTUP_STABLE_KEY: &'static str = "icydb.test.migration_finding.startup.control.v1";
    fn integrity_progress_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(211)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str = "icydb.test.migration_finding.integrity.v1";
}

fn name(value: &str) -> SchemaName {
    SchemaName::try_new(value).expect("test schema name should admit")
}

fn generated_check_proposal(
    expected_head: ExpectedAcceptedHead,
    submission_key: &str,
    include_check: bool,
    database: TargetDatabaseIdentity,
    store: TargetStoreIdentity,
) -> (SchemaProposal, EntitySourceKey, ConstraintSourceKey) {
    let entity_source = EntitySourceKey::try_new("Item").expect("entity source should admit");
    let id_source = FieldSourceKey::try_new("id").expect("id source should admit");
    let score_source = FieldSourceKey::try_new("score").expect("score source should admit");
    let check_source =
        ConstraintSourceKey::try_new("score_non_negative").expect("check source should admit");
    let check = SourceCheckExpr::try_new(vec![
        SourceCheckInstruction::Field(score_source),
        SourceCheckInstruction::Literal(ScalarLiteral::Int(0)),
        SourceCheckInstruction::GreaterThanOrEqual,
    ])
    .expect("check expression should admit");
    let constraints = include_check
        .then(|| ConstraintFragment::check(name("score_non_negative"), check))
        .into_iter()
        .collect();
    let entity = EntityFragment::try_new(
        name("Item"),
        version_one(),
        vec![
            FieldFragment::new(
                name("id"),
                FieldType::Scalar(ScalarType::Nat64),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
            FieldFragment::new(
                name("score"),
                FieldType::Scalar(ScalarType::Int64),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
        ],
        vec![id_source],
        Vec::new(),
        Vec::new(),
        constraints,
    )
    .expect("entity should admit");
    let proposal = SchemaProposal::try_compose(
        vec![SchemaCapability::ACCEPTED_CHECKS],
        database,
        SchemaSubmissionKey::try_new(submission_key).expect("submission key should admit"),
        expected_head,
        vec![
            SchemaFragment::try_new(vec![entity], Vec::new())
                .expect("schema fragment should admit"),
        ],
        vec![EntityStoreAssignment::new(entity_source.clone(), store)],
        Vec::new(),
        None,
    )
    .expect("schema proposal should compose");
    (proposal, entity_source, check_source)
}

#[cfg(feature = "migration")]
#[derive(Clone, Copy, Eq, PartialEq)]
enum ValidationMigrationShape {
    Clean,
    AllFindingFamilies,
}

#[cfg(feature = "migration")]
fn validation_migration_proposal(
    shape: ValidationMigrationShape,
    current: bool,
    expected_head: ExpectedAcceptedHead,
    database: TargetDatabaseIdentity,
    store: TargetStoreIdentity,
) -> SchemaProposal {
    let entity_source =
        EntitySourceKey::try_new("MigratingItem").expect("migration entity source should admit");
    let old_value =
        FieldSourceKey::try_new("old_value").expect("predecessor field source should admit");
    let current_value =
        FieldSourceKey::try_new("value").expect("candidate field source should admit");
    let target_entity =
        EntitySourceKey::try_new("MigrationTarget").expect("migration target source should admit");
    let target_id = FieldSourceKey::try_new("id").expect("target id source should admit");
    let constraint = SourceCheckExpr::try_new(vec![
        SourceCheckInstruction::Field(current_value.clone()),
        SourceCheckInstruction::Literal(ScalarLiteral::Nat(8)),
        SourceCheckInstruction::LessThanOrEqual,
    ])
    .expect("candidate check should admit");
    let findings = shape == ValidationMigrationShape::AllFindingFamilies;
    let entity = EntityFragment::try_new(
        name("MigratingItem"),
        DeclaredEntityVersion::try_new(if current { 2 } else { 1 })
            .expect("migration version should admit"),
        vec![
            FieldFragment::new(
                name("id"),
                FieldType::Scalar(ScalarType::Nat64),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
            FieldFragment::new(
                name(if current { "value" } else { "old_value" }),
                FieldType::Scalar(if current {
                    ScalarType::Nat8
                } else {
                    ScalarType::Int64
                }),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
        ],
        vec![FieldSourceKey::try_new("id").expect("id source should admit")],
        current
            .then(|| {
                IndexFragment::try_new(
                    name("value_unique"),
                    vec![IndexKeyFragment::Field(current_value.clone())],
                    true,
                    None,
                )
                .expect("candidate index should admit")
            })
            .into_iter()
            .collect(),
        (current && findings)
            .then(|| {
                RelationFragment::try_new(
                    name("value_target"),
                    icydb_schema::RelationSourceFragment::direct(vec![current_value.clone()]),
                    target_entity.clone(),
                    vec![target_id.clone()],
                    RelationDeleteAction::Restrict,
                )
                .expect("candidate relation should admit")
            })
            .into_iter()
            .collect(),
        (current && findings)
            .then(|| ConstraintFragment::check(name("value_at_most_eight"), constraint))
            .into_iter()
            .collect(),
    )
    .expect("migration entity should admit");
    let target = EntityFragment::try_new(
        name("MigrationTarget"),
        version_one(),
        vec![FieldFragment::new(
            name("id"),
            FieldType::Scalar(ScalarType::Nat8),
            false,
            FieldInsertPolicy::Required,
            None,
        )],
        vec![target_id],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .expect("migration relation target should admit");
    let migration = current.then(|| {
        SchemaMigrationPlan::try_new(vec![
            EntityMigration::try_new(
                entity_source.clone(),
                DeclaredEntityVersion::try_new(1).expect("predecessor should admit"),
                None,
                Vec::new(),
                vec![SchemaMigrationTransform::CheckedCast {
                    from: old_value.clone(),
                    to: current_value,
                    target: ScalarType::Nat8,
                }],
            )
            .expect("migration transition should admit"),
        ])
        .expect("migration plan should admit")
    });
    let mut capabilities = Vec::new();
    if current && findings {
        capabilities.extend([
            SchemaCapability::ACCEPTED_CHECKS,
            SchemaCapability::SECONDARY_INDEXES,
            SchemaCapability::RESTRICTIVE_RELATIONS,
        ]);
    }
    if migration.is_some() {
        capabilities.push(SchemaCapability::VERSIONED_MIGRATIONS);
    }
    let mut entities = vec![entity];
    let mut assignments = vec![EntityStoreAssignment::new(entity_source.clone(), store)];
    if findings {
        entities.push(target);
        assignments.push(EntityStoreAssignment::new(target_entity, store));
    }
    SchemaProposal::try_compose(
        capabilities,
        database,
        SchemaSubmissionKey::try_new(if current {
            "migration-validation-v2"
        } else {
            "migration-validation-v1"
        })
        .expect("submission should admit"),
        expected_head,
        vec![
            SchemaFragment::try_new(entities, Vec::new()).expect("migration fragment should admit"),
        ],
        assignments,
        current
            .then_some(icydb_schema::SchemaRemoval::Field {
                entity: entity_source,
                field: old_value,
            })
            .into_iter()
            .collect(),
        migration,
    )
    .expect("migration proposal should compose")
}

fn targeted_rule_proposal(
    expected_head: ExpectedAcceptedHead,
    submission_key: &str,
    operation: SourceRuleOperation,
    database: TargetDatabaseIdentity,
    store: TargetStoreIdentity,
) -> (SchemaProposal, EntitySourceKey, ConstraintSourceKey) {
    let entity_source = EntitySourceKey::try_new("Measured").expect("entity source should admit");
    let id_source = FieldSourceKey::try_new("id").expect("id source should admit");
    let value_source = FieldSourceKey::try_new("value").expect("value source should admit");
    let value_type = TypeSourceKey::try_new("Measure").expect("type source should admit");
    let rule_source = RuleSourceKey::try_new("limit").expect("rule source should admit");
    let constraint_source =
        ConstraintSourceKey::for_targeted_field_rule(&value_source, &value_type, &rule_source);
    let entity = EntityFragment::try_new(
        name("Measured"),
        version_one(),
        vec![
            FieldFragment::new(
                name("id"),
                FieldType::Scalar(ScalarType::Nat64),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
            FieldFragment::new(
                name("value"),
                FieldType::Named(value_type.clone()),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
        ],
        vec![id_source],
        Vec::new(),
        Vec::new(),
        vec![ConstraintFragment::targeted_rule(
            TargetedRuleFragment::new(value_source, value_type, name("limit"), operation),
        )],
    )
    .expect("targeted entity should admit");
    let proposal = SchemaProposal::try_compose(
        vec![SchemaCapability::ACCEPTED_CHECKS],
        database,
        SchemaSubmissionKey::try_new(submission_key).expect("submission key should admit"),
        expected_head,
        vec![
            SchemaFragment::try_new(
                vec![entity],
                vec![NamedTypeFragment::newtype(
                    name("Measure"),
                    FieldType::Scalar(ScalarType::Nat8),
                )],
            )
            .expect("schema fragment should admit"),
        ],
        vec![EntityStoreAssignment::new(entity_source.clone(), store)],
        Vec::new(),
        None,
    )
    .expect("schema proposal should compose");
    (proposal, entity_source, constraint_source)
}

#[test]
fn database_head_is_empty_only_when_every_store_root_is_absent() {
    assert_eq!(
        derive_accepted_head(&[("test::A", None), ("test::B", None)]),
        ExpectedAcceptedHead::Empty,
    );
}

#[test]
fn database_head_covers_store_path_revision_fingerprint_and_absence() {
    let first = derive_accepted_head(&[
        (
            "test::A",
            Some(AcceptedStoreHead {
                revision: 3,
                fingerprint: [0x11; 32],
            }),
        ),
        ("test::B", None),
    ]);
    let changed_fingerprint = derive_accepted_head(&[
        (
            "test::A",
            Some(AcceptedStoreHead {
                revision: 3,
                fingerprint: [0x12; 32],
            }),
        ),
        ("test::B", None),
    ]);
    let changed_absence = derive_accepted_head(&[
        (
            "test::A",
            Some(AcceptedStoreHead {
                revision: 3,
                fingerprint: [0x11; 32],
            }),
        ),
        (
            "test::B",
            Some(AcceptedStoreHead {
                revision: 1,
                fingerprint: [0x22; 32],
            }),
        ),
    ]);

    assert_ne!(first, changed_fingerprint);
    assert_ne!(first, changed_absence);
    assert!(matches!(
        first,
        ExpectedAcceptedHead::Exact { revision: 3, .. }
    ));
}

#[test]
fn generated_check_abort_retires_source_identity_and_allows_fresh_reproposal() {
    let database = TargetDatabaseIdentity::from_bytes([0x71; 32]);
    let store = TargetStoreIdentity::from_bytes([0x72; 32]);
    let (initial, entity_source, _) = generated_check_proposal(
        ExpectedAcceptedHead::Empty,
        "abort-initial",
        false,
        database,
        store,
    );
    let initial_candidate = lower_initial_schema_proposal(
        &initial,
        &[ProposalStoreTarget {
            path: "abort::Store",
            identity: store,
        }],
    )
    .expect("initial proposal should lower")
    .pop()
    .expect("initial proposal should produce one candidate");
    let (with_check, _, check_source) = generated_check_proposal(
        ExpectedAcceptedHead::Exact {
            revision: 1,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x73; 32]),
        },
        "abort-add-check",
        true,
        database,
        store,
    );
    let pending_candidate = lower_existing_schema_proposal(
        &with_check,
        &[ExistingProposalStore {
            path: "abort::Store",
            identity: store,
            bundle: initial_candidate.bundle().catalog(),
        }],
    )
    .expect("generated check should lower")
    .pop()
    .expect("generated check should produce one candidate");
    let entity_tag = pending_candidate
        .bundle()
        .source_bindings_for_tests()
        .entity(&entity_source)
        .expect("entity source should remain bound");
    let constraint_id = pending_candidate
        .bundle()
        .source_bindings_for_tests()
        .constraint(entity_tag, &check_source)
        .expect("generated check source should bind");
    let pending_snapshot = pending_candidate
        .bundle()
        .entity_snapshots()
        .get(&entity_tag)
        .expect("pending entity should exist");
    let activation = pending_snapshot
        .constraint_catalog()
        .activation(constraint_id)
        .expect("generated check should remain an activation");
    assert_eq!(activation.origin(), ConstraintOrigin::Generated);

    let aborted = aborted_generated_row_local_candidate(
        pending_candidate.bundle(),
        entity_tag,
        constraint_id,
    )
    .expect("generated check abort should build one catalog-native candidate");
    let aborted_snapshot = aborted
        .bundle()
        .entity_snapshots()
        .get(&entity_tag)
        .expect("aborted entity should remain");
    assert!(
        aborted_snapshot
            .constraint_catalog()
            .activation(constraint_id)
            .is_none(),
    );
    assert_eq!(aborted_snapshot.row_layout(), pending_snapshot.row_layout());
    assert!(
        aborted
            .bundle()
            .source_bindings_for_tests()
            .constraint(entity_tag, &check_source)
            .is_none(),
    );

    let reproposed = lower_existing_schema_proposal(
        &with_check,
        &[ExistingProposalStore {
            path: "abort::Store",
            identity: store,
            bundle: aborted.bundle().catalog(),
        }],
    )
    .expect("aborted generated check should be independently reproposable")
    .pop()
    .expect("reproposal should produce one candidate");
    let replacement_id = reproposed
        .bundle()
        .source_bindings_for_tests()
        .constraint(entity_tag, &check_source)
        .expect("reproposal should bind a fresh constraint identity");
    assert!(
        replacement_id > constraint_id,
        "aborted accepted IDs must remain retired",
    );
}

#[test]
fn targeted_rule_edit_abort_keeps_prior_accepted_semantics_and_source_identity() {
    let database = TargetDatabaseIdentity::from_bytes([0x81; 32]);
    let store = TargetStoreIdentity::from_bytes([0x82; 32]);
    let (initial, entity_source, constraint_source) = targeted_rule_proposal(
        ExpectedAcceptedHead::Empty,
        "targeted-abort-initial",
        SourceRuleOperation::NumericRangeInclusive {
            min: ScalarLiteral::Nat(0),
            max: ScalarLiteral::Nat(10),
        },
        database,
        store,
    );
    let initial_candidate = lower_initial_schema_proposal(
        &initial,
        &[ProposalStoreTarget {
            path: "abort::TargetedStore",
            identity: store,
        }],
    )
    .expect("initial targeted proposal should lower")
    .pop()
    .expect("initial targeted proposal should produce one candidate");
    let initial_bundle = initial_candidate.bundle();
    let entity_tag = initial_bundle
        .source_bindings_for_tests()
        .entity(&entity_source)
        .expect("entity source should bind");
    let constraint_id = initial_bundle
        .source_bindings_for_tests()
        .constraint(entity_tag, &constraint_source)
        .expect("targeted source should bind");
    let high_water = initial_bundle.entity_snapshots()[&entity_tag]
        .constraint_id_allocator()
        .high_water();
    let (edited, _, _) = targeted_rule_proposal(
        ExpectedAcceptedHead::Exact {
            revision: initial_bundle.revision().get(),
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x83; 32]),
        },
        "targeted-abort-edit",
        SourceRuleOperation::NumericMaximumInclusive {
            value: ScalarLiteral::Nat(8),
        },
        database,
        store,
    );
    let staged = lower_existing_schema_proposal(
        &edited,
        &[ExistingProposalStore {
            path: "abort::TargetedStore",
            identity: store,
            bundle: initial_bundle.catalog(),
        }],
    )
    .expect("targeted semantic edit should stage")
    .pop()
    .expect("targeted semantic edit should produce one candidate");
    let aborted = aborted_generated_row_local_candidate(staged.bundle(), entity_tag, constraint_id)
        .expect("targeted semantic edit should abort through catalog authority");
    let snapshot = &aborted.bundle().entity_snapshots()[&entity_tag];

    assert!(
        snapshot
            .constraint_catalog()
            .activation(constraint_id)
            .is_none()
    );
    assert_eq!(snapshot.constraint_id_allocator().high_water(), high_water);
    assert_eq!(
        aborted
            .bundle()
            .source_bindings_for_tests()
            .constraint(entity_tag, &constraint_source),
        Some(constraint_id),
    );
    assert!(snapshot.constraints().iter().any(|constraint| {
        constraint.id() == constraint_id
            && matches!(
                constraint.kind(),
                AcceptedConstraintKind::TargetedRule { operation, .. }
                    if matches!(
                        operation.as_ref(),
                        AcceptedRuleOperation::NumericRangeInclusive { .. }
                    )
            )
    }));
}

#[test]
fn generated_startup_driver_resumes_pending_activation_and_promotes_without_source_model() {
    let db = Db::<EvolutionCanister>::new(
        &EVOLUTION_REGISTRY,
        crate::db::RequestExecutionRoot::__new_runtime_root().scope(),
    );
    drive_startup_recovery_to_completion(&db);
    let empty_target = schema_application_target(&db).expect("empty evolution target should issue");
    let store_identity = empty_target
        .stores()
        .first()
        .expect("evolution store should register")
        .identity();
    let (initial, entity_source, constraint_source) = targeted_rule_proposal(
        empty_target.accepted_head().clone(),
        "targeted-recovery-initial",
        SourceRuleOperation::NumericRangeInclusive {
            min: ScalarLiteral::Nat(0),
            max: ScalarLiteral::Nat(10),
        },
        empty_target.database_identity(),
        store_identity,
    );
    assert!(matches!(
        apply_schema(&db, &initial)
            .expect("initial targeted proposal should publish")
            .outcome(),
        SchemaChangeOutcome::Applied { .. },
    ));
    drive_startup_recovery_to_completion(&db);
    drive_cardinality_to_ready(
        db.store_handle(EVOLUTION_STORE_PATH)
            .expect("evolution store should resolve"),
    );

    let direct_target =
        schema_application_target(&db).expect("direct evolution target should issue");
    let (direct_edit, _, _) = targeted_rule_proposal(
        direct_target.accepted_head().clone(),
        "targeted-direct-edit",
        SourceRuleOperation::NumericMaximumInclusive {
            value: ScalarLiteral::Nat(8),
        },
        direct_target.database_identity(),
        store_identity,
    );
    assert!(matches!(
        apply_schema(&db, &direct_edit)
            .expect("empty-domain semantic edit should publish directly")
            .outcome(),
        SchemaChangeOutcome::Applied { .. },
    ));
    let store = db
        .store_handle(EVOLUTION_STORE_PATH)
        .expect("evolution store should resolve");
    let direct = store
        .with_schema(SchemaStore::current_accepted_schema_bundle)
        .expect("directly edited bundle should remain readable")
        .expect("directly edited bundle should exist");
    let entity_tag = direct
        .source_bindings_for_tests()
        .entity(&entity_source)
        .expect("entity source should remain bound");
    let constraint_id = direct
        .source_bindings_for_tests()
        .constraint(entity_tag, &constraint_source)
        .expect("direct edit should preserve constraint identity");
    assert!(
        direct.entity_snapshots()[&entity_tag]
            .constraint_catalog()
            .activation(constraint_id)
            .is_none()
    );
    assert!(
        direct.entity_snapshots()[&entity_tag]
            .constraints()
            .iter()
            .any(|constraint| {
                constraint.id() == constraint_id
                    && matches!(
                        constraint.kind(),
                        AcceptedConstraintKind::TargetedRule { operation, .. }
                            if matches!(
                                operation.as_ref(),
                                AcceptedRuleOperation::NumericMaximumInclusive { .. }
                            )
                    )
            })
    );

    let target = schema_application_target(&db).expect("staged evolution target should issue");
    let (edited, _, _) = targeted_rule_proposal(
        target.accepted_head().clone(),
        "targeted-recovery-edit",
        SourceRuleOperation::MultipleOf {
            divisor: ScalarLiteral::Nat(2),
        },
        target.database_identity(),
        store_identity,
    );
    let current = store
        .with_schema(SchemaStore::current_accepted_schema_bundle)
        .expect("accepted evolution bundle should remain readable")
        .expect("directly edited evolution bundle should exist");
    let staged = lower_existing_schema_proposal(
        &edited,
        &[ExistingProposalStore {
            path: EVOLUTION_STORE_PATH,
            identity: store_identity,
            bundle: current.catalog(),
        }],
    )
    .expect("targeted edit should stage")
    .pop()
    .expect("targeted edit should produce one candidate");
    assert_eq!(
        staged
            .bundle()
            .source_bindings_for_tests()
            .constraint(entity_tag, &constraint_source),
        Some(constraint_id),
    );
    let proof = DirectGeneratedRowLocalProof {
        candidate_index: 0,
        store,
        store_path: EVOLUTION_STORE_PATH,
        entity_tag,
        entity_path: staged.bundle().entity_snapshots()[&entity_tag]
            .entity_path()
            .to_string(),
        constraint_id,
        has_rows: false,
    };
    let final_candidates = final_candidates_for_pending_row_local_constraint(
        std::slice::from_ref(&staged),
        &PendingGeneratedRowLocalConstraint { proof },
    )
    .expect("final semantic replacement should derive without source input");
    let authorities = application_authorities(&db);
    let candidate_head = accepted_head_after_candidates(authorities.as_slice(), &final_candidates)
        .expect("final candidate head should derive");
    let digest = edited.digest().expect("proposal digest should derive");
    let job_id = derive_schema_change_job_id(
        target.database_identity(),
        edited.submission_key(),
        digest,
        target.accepted_head(),
    )
    .expect("job identity should derive");
    let receipt = crate::db::schema::SchemaChangeReceipt::new(
        target.database_identity(),
        edited.submission_key().clone(),
        digest,
        target.accepted_head().clone(),
        SchemaChangeOutcome::Pending {
            job: SchemaChangeJob::new(job_id),
            candidate_head,
        },
    )
    .expect("pending replacement receipt should admit");
    let record = SchemaApplicationRecord::new(
        receipt,
        vec![
            SchemaChangeActivation::new(store_identity, entity_tag.value(), constraint_id.get())
                .expect("replacement activation should admit"),
        ],
    )
    .expect("pending replacement record should admit");
    let record = bind_pending_lineage(&edited, &current, record);
    let operation =
        SchemaApplicationRecordOp::insert(&record).expect("pending insert should prepare");
    publish_accepted_schema_candidates_with_application_record(
        vec![AcceptedSchemaPublication::new(
            EVOLUTION_STORE_PATH,
            store,
            current.revision(),
            &staged,
        )],
        operation,
    )
    .expect("staged replacement and record should publish atomically");

    forget_recovered_domain_for_tests(&db).expect("upgrade should reset recovery ownership");
    drive_startup_recovery_to_completion(&db);

    let recovered = store
        .with_schema(SchemaStore::current_accepted_schema_bundle)
        .expect("recovered staged bundle should decode")
        .expect("recovered staged bundle should exist");
    let recovered_snapshot = recovered.entity_snapshots()[&entity_tag].clone();
    let validating_catalog = recovered_snapshot
        .constraint_catalog()
        .clone()
        .with_validation_started(constraint_id)
        .expect("recovered replacement should enter validation");
    let mut validating_snapshots = recovered.entity_snapshots().clone();
    validating_snapshots.insert(
        entity_tag,
        recovered_snapshot.with_constraint_catalog(validating_catalog),
    );
    let validating_bundle = AcceptedSchemaRevisionBundle::new_with_source_bindings(
        recovered
            .revision()
            .checked_next()
            .expect("validation revision should remain available"),
        recovered.store_path(),
        recovered.enum_catalog().clone(),
        recovered.composite_catalog().clone(),
        recovered.source_bindings_for_tests().clone(),
        validating_snapshots,
    )
    .expect("validating replacement bundle should close");
    let validating_candidate = CandidateSchemaRevision::new(validating_bundle)
        .expect("validating replacement candidate should encode");
    let validating_activation = validating_candidate.bundle().entity_snapshots()[&entity_tag]
        .constraint_catalog()
        .activation(constraint_id)
        .expect("validating replacement activation should remain present");
    let validation_job = ConstraintValidationJob::start(
        entity_tag,
        validating_candidate.bundle().entity_snapshots()[&entity_tag]
            .entity_path()
            .to_string(),
        validating_activation,
        None,
    )
    .expect("validating replacement job should derive from accepted state");
    store
        .with_schema(|schema| {
            schema.validate_live_activation_transition(validating_candidate.bundle())?;
            schema.validate_constraint_validation_job_closure_with_change(
                validating_candidate.bundle(),
                Some(&validation_job),
                None,
            )
        })
        .expect("validating replacement transition and job should close");
    let startup_root = crate::db::RequestExecutionRoot::__new_runtime_root();
    let startup_session =
        crate::db::DbSession::<EvolutionCanister>::new(&EVOLUTION_REGISTRY, &startup_root);

    assert_eq!(
        drive_generated_startup_recovery_page(
            &startup_session,
            &EVOLUTION_REGISTRY,
            edited.submission_key().as_str(),
        )
        .expect("generated startup should begin pending validation"),
        GeneratedStartupDriverStep::Recovering,
    );
    let mut terminal = false;
    for _ in 0..8 {
        match drive_generated_startup_recovery_page(
            &startup_session,
            &EVOLUTION_REGISTRY,
            edited.submission_key().as_str(),
        )
        .expect("generated startup should advance pending validation")
        {
            GeneratedStartupDriverStep::Recovering => {}
            GeneratedStartupDriverStep::Terminal => {
                terminal = true;
                break;
            }
            GeneratedStartupDriverStep::ApplyGeneratedSchema => {
                panic!("an exact pending receipt must resume instead of being resubmitted")
            }
        }
    }
    assert!(
        terminal,
        "empty historical domain should promote within bounded startup steps"
    );
    assert_eq!(
        observe_generated_startup_state::<EvolutionCanister>(
            &EVOLUTION_REGISTRY,
            edited.submission_key().as_str(),
        ),
        Ok(DatabaseStartupState::Ready),
    );
    let applied = super::exact_schema_application_receipt(
        &edited,
        edited
            .digest()
            .expect("proposal digest should remain stable"),
    )
    .expect("terminal generated receipt should remain readable")
    .expect("terminal generated receipt should remain present");
    assert!(matches!(
        applied.outcome(),
        SchemaChangeOutcome::Applied { .. }
    ));
    let promoted = store
        .with_schema(SchemaStore::current_accepted_schema_bundle)
        .expect("promoted bundle should remain readable")
        .expect("promoted bundle should exist");
    let snapshot = &promoted.entity_snapshots()[&entity_tag];
    assert!(
        snapshot
            .constraint_catalog()
            .activation(constraint_id)
            .is_none()
    );
    assert_eq!(
        promoted
            .source_bindings_for_tests()
            .constraint(entity_tag, &constraint_source),
        Some(constraint_id),
    );
    assert!(snapshot.constraints().iter().any(|constraint| {
        constraint.id() == constraint_id
            && matches!(
                constraint.kind(),
                AcceptedConstraintKind::TargetedRule { operation, .. }
                    if matches!(
                        operation.as_ref(),
                        AcceptedRuleOperation::MultipleOf { .. }
                    )
            )
    }));
}

#[test]
fn generated_startup_driver_persists_e223_for_a_retained_historical_finding() {
    let db = Db::<AbortCanister>::new(
        &ABORT_REGISTRY,
        crate::db::RequestExecutionRoot::__new_runtime_root().scope(),
    );
    drive_startup_recovery_to_completion(&db);
    let empty_target =
        schema_application_target(&db).expect("empty application target should issue");
    let store_identity = empty_target
        .stores()
        .first()
        .expect("abort store should be registered")
        .identity();
    let (initial, _, _) = generated_check_proposal(
        empty_target.accepted_head().clone(),
        "startup-finding-initial",
        false,
        empty_target.database_identity(),
        store_identity,
    );
    apply_schema(&db, &initial).expect("initial generated schema should publish");

    let root = crate::db::RequestExecutionRoot::__new_runtime_root();
    let session = DbSession::<AbortCanister>::new(&ABORT_REGISTRY, &root);
    let rows = (1..=257)
        .map(|id| DynamicMutation::Insert {
            entity: "Item".to_string(),
            patch: DynamicStructuralPatch::new(vec![
                (
                    "id".to_string(),
                    DynamicWriteCell::Value(InputValue::nat64(id)),
                ),
                (
                    "score".to_string(),
                    DynamicWriteCell::Value(InputValue::int64(if id == 257 { -1 } else { 1 })),
                ),
            ]),
        })
        .collect();
    session
        .execute_trusted_dynamic_mutation_batch(rows)
        .expect("historical finding fixture rows should commit as one legal batch");
    drive_startup_recovery_to_completion(&db);
    drive_cardinality_to_ready(
        db.store_handle(ABORT_STORE_PATH)
            .expect("abort store should resolve"),
    );

    let target = schema_application_target(&db).expect("existing application target should issue");
    let (with_check, _, _) = generated_check_proposal(
        target.accepted_head().clone(),
        "startup-finding-pending",
        true,
        target.database_identity(),
        store_identity,
    );
    let pending = apply_schema(&db, &with_check)
        .expect("the first clean page should admit durable continuation");
    let SchemaChangeOutcome::Pending { job, .. } = pending.outcome() else {
        panic!("a 257-row domain must exceed the 256-row direct proof page")
    };

    let mut terminal = false;
    for _ in 0..8 {
        match drive_generated_startup_recovery_page(
            &session,
            &ABORT_REGISTRY,
            with_check.submission_key().as_str(),
        )
        .expect("generated startup should retain a typed finding failure")
        {
            GeneratedStartupDriverStep::Recovering => {}
            GeneratedStartupDriverStep::Terminal => {
                terminal = true;
                break;
            }
            GeneratedStartupDriverStep::ApplyGeneratedSchema => {
                panic!("an exact pending receipt must not be resubmitted")
            }
        }
    }
    assert!(terminal, "the retained finding should become terminal");
    let failure = observe_generated_startup_state::<AbortCanister>(
        &ABORT_REGISTRY,
        with_check.submission_key().as_str(),
    )
    .expect_err("the retained finding must remain durably observable");
    assert_eq!(
        failure.kind(),
        crate::db::StartupFailureKind::SchemaReconciliation,
    );
    assert_eq!(
        failure.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION,
    );
    assert_eq!(
        ABORT_DATA.with(|store| store.borrow().len()),
        257,
        "terminal startup publication must not change historical rows",
    );
    assert!(matches!(
        continue_schema_application(&db, job.id(), None)
            .expect("the retained finding page should replay exactly")
            .status(),
        SchemaChangeProgressStatus::Findings { findings, .. } if !findings.is_empty(),
    ));
}

#[test]
fn pending_generated_check_abort_is_atomic_terminal_and_replayable() {
    let db = Db::<AbortCanister>::new(
        &ABORT_REGISTRY,
        crate::db::RequestExecutionRoot::__new_runtime_root().scope(),
    );
    drive_startup_recovery_to_completion(&db);
    let empty_target =
        schema_application_target(&db).expect("empty application target should issue");
    let store_identity = empty_target
        .stores()
        .first()
        .expect("abort store should be registered")
        .identity();
    let (initial, entity_source, _) = generated_check_proposal(
        empty_target.accepted_head().clone(),
        "abort-runtime-initial",
        false,
        empty_target.database_identity(),
        store_identity,
    );
    assert!(matches!(
        apply_schema(&db, &initial)
            .expect("initial application should publish")
            .outcome(),
        SchemaChangeOutcome::Applied { .. },
    ));

    let target = schema_application_target(&db).expect("existing application target should issue");
    let (with_check, _, check_source) = generated_check_proposal(
        target.accepted_head().clone(),
        "abort-runtime-pending",
        true,
        target.database_identity(),
        store_identity,
    );
    let store = db
        .store_handle(ABORT_STORE_PATH)
        .expect("abort store should resolve");
    let current = store
        .with_schema(SchemaStore::current_accepted_schema_bundle)
        .expect("accepted bundle should remain readable")
        .expect("initial accepted bundle should exist");
    let pending_candidate = lower_existing_schema_proposal(
        &with_check,
        &[ExistingProposalStore {
            path: ABORT_STORE_PATH,
            identity: store_identity,
            bundle: current.catalog(),
        }],
    )
    .expect("pending generated check should lower")
    .pop()
    .expect("pending generated check should produce one candidate");
    let entity_tag = pending_candidate
        .bundle()
        .source_bindings_for_tests()
        .entity(&entity_source)
        .expect("entity source should bind");
    let constraint_id = pending_candidate
        .bundle()
        .source_bindings_for_tests()
        .constraint(entity_tag, &check_source)
        .expect("generated check source should bind");
    let digest = with_check.digest().expect("proposal digest should derive");
    let job_id = derive_schema_change_job_id(
        target.database_identity(),
        with_check.submission_key(),
        digest,
        target.accepted_head(),
    )
    .expect("job identity should derive");
    let receipt = crate::db::schema::SchemaChangeReceipt::new(
        target.database_identity(),
        with_check.submission_key().clone(),
        digest,
        target.accepted_head().clone(),
        SchemaChangeOutcome::Pending {
            job: SchemaChangeJob::new(job_id),
            candidate_head: ExpectedAcceptedHead::Exact {
                revision: pending_candidate.revision().get().saturating_add(2),
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x76; 32]),
            },
        },
    )
    .expect("pending receipt should admit");
    let record = SchemaApplicationRecord::new(
        receipt,
        vec![
            SchemaChangeActivation::new(store_identity, entity_tag.value(), constraint_id.get())
                .expect("application activation should admit"),
        ],
    )
    .expect("pending application record should admit");
    let record = bind_pending_lineage(&with_check, &current, record);
    let operation =
        SchemaApplicationRecordOp::insert(&record).expect("pending insert should prepare");
    publish_accepted_schema_candidates_with_application_record(
        vec![AcceptedSchemaPublication::new(
            ABORT_STORE_PATH,
            store,
            current.revision(),
            &pending_candidate,
        )],
        operation,
    )
    .expect("pending candidate and record should publish atomically");

    let started = continue_schema_application(&db, job_id, None)
        .expect("first continuation should durably start validation");
    assert_eq!(started.status(), &SchemaChangeProgressStatus::Started);
    let progress =
        abort_schema_application(&db, job_id, None).expect("pending application should abort");
    assert_eq!(progress.status(), &SchemaChangeProgressStatus::Aborted);
    assert!(matches!(
        progress.receipt().outcome(),
        SchemaChangeOutcome::Aborted { .. },
    ));
    let replay = abort_schema_application(&db, job_id, None).expect("terminal abort should replay");
    assert_eq!(replay, progress);
    assert_eq!(
        continue_schema_application(&db, job_id, None)
            .expect("continuation after abort should replay terminal state"),
        progress,
    );

    let aborted = store
        .with_schema(SchemaStore::current_accepted_schema_bundle)
        .expect("accepted bundle should remain readable")
        .expect("aborted accepted bundle should exist");
    assert!(
        aborted
            .entity_snapshots()
            .get(&entity_tag)
            .expect("entity should remain after abort")
            .constraint_catalog()
            .activation(constraint_id)
            .is_none(),
    );
    assert!(
        aborted
            .source_bindings_for_tests()
            .constraint(entity_tag, &check_source)
            .is_none(),
    );
    assert!(
        store
            .with_schema(|schema| { schema.constraint_validation_job(entity_tag, constraint_id) })
            .expect("validation-job storage should remain readable")
            .is_none(),
    );

    ABORT_DATA.with(|store| {
        // Drop the crashed collection before reopening its exclusive lease.
        drop(store.replace(DataStore::init_heap()));
        ABORT_DATA_MEMORY.with(|memory| {
            drop(
                store.replace(
                    DataStore::init_journaled(memory.open())
                        .expect("fixture storage should initialize"),
                ),
            );
        });
    });
    ABORT_INDEX.with(|store| {
        // Drop the crashed collection before reopening its exclusive lease.
        drop(store.replace(IndexStore::init_heap()));
        ABORT_INDEX_MEMORY.with(|memory| {
            drop(
                store.replace(
                    IndexStore::init_journaled(memory.open())
                        .expect("fixture storage should initialize"),
                ),
            );
        });
    });
    ABORT_SCHEMA.with(|store| {
        // Drop the crashed collection before reopening its exclusive lease.
        drop(store.replace(SchemaStore::init_heap()));
        ABORT_SCHEMA_MEMORY.with(|memory| {
            drop(
                store.replace(
                    SchemaStore::init_journaled(memory.open())
                        .expect("fixture storage should initialize"),
                ),
            );
        });
    });
    ABORT_JOURNAL.with(|store| {
        // Drop the crashed collection before reopening its exclusive lease.
        drop(store.replace(
            JournalTailStore::init(test_memory(254)).expect("fixture storage should initialize"),
        ));
        ABORT_JOURNAL_MEMORY.with(|memory| {
            drop(store.replace(
                JournalTailStore::init(memory.open()).expect("fixture storage should initialize"),
            ));
        });
    });
    forget_recovered_domain_for_tests(&db).expect("upgrade should reset recovery ownership");
    drive_startup_recovery_to_completion(&db);
    assert_eq!(
        abort_schema_application(&db, job_id, None)
            .expect("recovered terminal abort should replay"),
        progress,
    );
    assert!(
        store
            .with_schema(|schema| { schema.constraint_validation_job(entity_tag, constraint_id) })
            .expect("recovered validation-job storage should remain readable")
            .is_none(),
    );
    let startup_root = crate::db::RequestExecutionRoot::__new_runtime_root();
    let startup_session =
        crate::db::DbSession::<AbortCanister>::new(&ABORT_REGISTRY, &startup_root);
    ABORT_JOURNAL.with(|journal| {
        let journal = journal.borrow();
        assert!(
            journal
                .validate_current_tail_authority()
                .expect("recovered abort tail control should remain readable")
                .is_empty(),
            "recovered abort tail control should close exactly",
        );
    });
    assert_eq!(
        drive_generated_startup_recovery_page(
            &startup_session,
            &ABORT_REGISTRY,
            with_check.submission_key().as_str(),
        )
        .expect("an exact aborted generated submission should publish terminal startup state"),
        GeneratedStartupDriverStep::Terminal,
    );
    let failure = observe_generated_startup_state::<AbortCanister>(
        &ABORT_REGISTRY,
        with_check.submission_key().as_str(),
    )
    .expect_err("an aborted generated submission must not remain retryable forever");
    assert_eq!(
        failure.kind(),
        crate::db::StartupFailureKind::SchemaReconciliation,
    );
    assert_eq!(
        failure.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_CONFLICT,
    );
}

#[cfg(feature = "migration")]
#[test]
fn migration_planning_failures_retain_typed_public_classification() {
    use super::schema_migration_planning_error;
    use crate::db::schema::migration_planner::SchemaMigrationPlanningError;
    use icydb_diagnostic_code::{DiagnosticDetail, SchemaMigrationCode};

    for (error, reason) in [
        (
            SchemaMigrationPlanningError::Unadopted,
            SchemaMigrationCode::Unadopted,
        ),
        (
            SchemaMigrationPlanningError::MissingMigration,
            SchemaMigrationCode::MissingMigration,
        ),
        (
            SchemaMigrationPlanningError::VersionGap,
            SchemaMigrationCode::VersionGap,
        ),
        (
            SchemaMigrationPlanningError::Downgrade,
            SchemaMigrationCode::Downgrade,
        ),
        (
            SchemaMigrationPlanningError::EmptyEntityVersionBump,
            SchemaMigrationCode::EmptyEntityVersionBump,
        ),
        (
            SchemaMigrationPlanningError::UnknownFromObject,
            SchemaMigrationCode::UnknownFromObject,
        ),
        (
            SchemaMigrationPlanningError::UnknownToObject,
            SchemaMigrationCode::UnknownToObject,
        ),
        (
            SchemaMigrationPlanningError::KindMismatch,
            SchemaMigrationCode::KindMismatch,
        ),
        (
            SchemaMigrationPlanningError::IdentityConflict,
            SchemaMigrationCode::IdentityConflict,
        ),
        (
            SchemaMigrationPlanningError::UnexplainedSchemaDifference,
            SchemaMigrationCode::UnexplainedSchemaDifference,
        ),
        (
            SchemaMigrationPlanningError::UnsupportedTransform,
            SchemaMigrationCode::UnsupportedTransform,
        ),
        (
            SchemaMigrationPlanningError::RekeyedCatalogInvalid,
            SchemaMigrationCode::CandidateMismatch,
        ),
        (
            SchemaMigrationPlanningError::CandidateMismatch,
            SchemaMigrationCode::CandidateMismatch,
        ),
        (
            SchemaMigrationPlanningError::CorruptLineage,
            SchemaMigrationCode::ProgressCorrupt,
        ),
    ] {
        let error = schema_migration_planning_error(error);
        let diagnostic = error.diagnostic();
        assert_eq!(
            diagnostic.detail(),
            Some(&DiagnosticDetail::SchemaMigration { reason }),
        );
        assert_eq!(diagnostic.code(), reason.diagnostic_code());
        let failure = crate::db::startup::classify_terminal_failure(
            crate::db::StartupFailureKind::SchemaReconciliation,
            &error,
        )
        .expect("every deterministic planner rejection must stop generated startup");
        assert_eq!(*failure.diagnostic(), diagnostic);
    }
}

#[cfg(feature = "migration")]
#[test]
fn migration_preparation_failure_retains_original_budget_diagnostic() {
    use crate::db::{
        resource::budget::MaintenanceConstructionBudget,
        resource::construction::ConstructionBudget,
        schema::migration_planner::SchemaMigrationPlanningError,
    };
    use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

    let budget = MaintenanceConstructionBudget::with_limit_for_tests(Resource::TemporaryBytes, 0);
    let error = budget.charge(Resource::TemporaryBytes, 1).unwrap_err();
    let expected = error.diagnostic();
    let result =
        super::schema_migration_planning_error(SchemaMigrationPlanningError::Preparation(error));
    assert_eq!(result.diagnostic(), expected);
    assert!(
        crate::db::startup::classify_terminal_failure(
            crate::db::StartupFailureKind::SchemaReconciliation,
            &result,
        )
        .is_none(),
        "transient preparation budgets must not become terminal migration failures",
    );
}

#[cfg(feature = "migration")]
#[test]
fn physical_migration_validation_is_bounded_staged_and_does_not_rewrite_rows() {
    qualify_migration_abort_domains(0);
}

#[cfg(feature = "migration")]
#[test]
fn physical_migration_abort_resumes_across_candidate_namespaces() {
    qualify_migration_abort_domains(513);
}

#[cfg(feature = "migration")]
fn qualify_migration_abort_domains(extra_entries: u64) {
    use crate::error::InternalError;

    use super::{ensure_generated_schema_application_admitted, migrate_schema};
    use crate::db::{
        data::StoreVisit,
        index::{IndexEntryValue, IndexId, IndexKey, IndexKeyKind},
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        schema::{SchemaMigrationCommand, SchemaMigrationPhase},
    };
    use crate::types::EntityTag;

    let db = Db::<MigrationCanister>::new(
        &MIGRATION_REGISTRY,
        crate::db::RequestExecutionRoot::__new_runtime_root().scope(),
    );
    drive_startup_recovery_to_completion(&db);
    let initial_target = schema_application_target(&db).expect("initial target should issue");
    let store_identity = initial_target
        .stores()
        .first()
        .expect("migration store should exist")
        .identity();
    let initial = validation_migration_proposal(
        ValidationMigrationShape::Clean,
        false,
        initial_target.accepted_head().clone(),
        initial_target.database_identity(),
        store_identity,
    );
    apply_schema(&db, &initial).expect("initial schema should publish");

    let session = DbSession::<MigrationCanister>::new(
        &MIGRATION_REGISTRY,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    );
    for (id, value) in [(1, 7), (2, 8)] {
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                entity: "MigratingItem".to_string(),
                patch: DynamicStructuralPatch::new(vec![
                    (
                        "id".to_string(),
                        DynamicWriteCell::Value(InputValue::nat64(id)),
                    ),
                    (
                        "old_value".to_string(),
                        DynamicWriteCell::Value(InputValue::int64(value)),
                    ),
                ]),
            })
            .expect("predecessor row should insert");
    }
    let store = db
        .store_handle(MIGRATION_STORE_PATH)
        .expect("migration store should resolve");
    let row_bytes = || {
        store.with_data(|data| {
            let mut rows = Vec::new();
            let result: Result<(), InternalError> = data.visit_entries(|key, row| {
                rows.push((key.as_bytes().to_vec(), row.as_bytes().to_vec()));
                Ok(StoreVisit::Continue)
            });
            result.expect("infallible row visit should complete");
            rows
        })
    };
    let before_rows = row_bytes();

    let target = schema_application_target(&db).expect("migration target should issue");
    let proposal = validation_migration_proposal(
        ValidationMigrationShape::Clean,
        true,
        target.accepted_head().clone(),
        target.database_identity(),
        store_identity,
    );
    let plan = proposal
        .migration()
        .expect("migration plan should exist")
        .digest();
    let pending = apply_schema(&db, &proposal)
        .expect_err("valid adjacent migration requires explicit advancement");
    assert_eq!(
        pending.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::SCHEMA_MIGRATION_IN_PROGRESS,
    );
    assert_eq!(
        schema_application_target(&db).unwrap().accepted_head(),
        target.accepted_head(),
        "ordinary startup must not publish the pending successor",
    );
    let command = || SchemaMigrationCommand::Advance {
        expected_database: target.database_identity(),
        expected_head: target.accepted_head().clone(),
        expected_plan: plan,
    };
    assert_eq!(
        migrate_schema(&db, &proposal, command())
            .unwrap_or_else(|error| {
                panic!(
                    "physical migration should prepare: {:?}",
                    error.diagnostic()
                )
            })
            .phase(),
        SchemaMigrationPhase::Prepared,
    );
    let pending = ensure_generated_schema_application_admitted(&db, &proposal)
        .expect_err("prepared migration still owns generated schema publication");
    assert_eq!(
        pending.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::SCHEMA_MIGRATION_IN_PROGRESS,
    );
    let mismatched = ensure_generated_schema_application_admitted(&db, &initial)
        .expect_err("pending must not hide a mismatched deployed proposal");
    assert_eq!(
        mismatched.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::SCHEMA_MIGRATION_PLAN_CHANGED,
    );
    assert_eq!(
        migrate_schema(&db, &proposal, command())
            .expect("physical migration should enter validation")
            .phase(),
        SchemaMigrationPhase::Validating,
    );
    let record = super::load_schema_migration_record()
        .expect("migration record should remain readable")
        .expect("validating migration record should exist");
    let planned = super::recompile_active_physical_migration(&db, &proposal, &record)
        .expect("the exact active plan should recompile");
    assert_eq!(
        super::migration_derived_domain_count(&db, &planned).unwrap(),
        1
    );
    for _ in 0..2 {
        assert_eq!(
            super::migration_derived_domain_count(&db, &planned).unwrap(),
            1
        );
        let page =
            super::validate_migration_page(&db, &planned, record.progress(), record.plan_digest())
                .expect("the same validation page should remain replayable");
        let (progress, staged, exhausted) = page.into_parts();
        assert!(progress.findings().is_empty());
        assert!(exhausted);
        super::stage_migration_index_entries(staged)
            .expect("staging before a cursor marker should be idempotent");
    }
    assert_eq!(
        (store.with_index(IndexStore::len)).expect("storage operation should succeed"),
        2,
        "replaying an uncheckpointed page must retain one exact staged key per row",
    );
    let ready =
        migrate_schema(&db, &proposal, command()).expect("bounded validation should complete");
    assert_eq!(ready.phase(), SchemaMigrationPhase::ReadyToRewrite);
    assert_eq!(ready.rows_validated(), 2);
    assert!(ready.findings().is_empty());
    assert_eq!(row_bytes(), before_rows, "validation must not rewrite rows");
    assert_eq!(
        (store.with_index(IndexStore::len)).expect("storage operation should succeed"),
        2,
        "the isolated candidate unique generation should be durably staged",
    );
    let candidate_id = store.with_index(|index| {
        let mut candidate = None;
        index
            .visit_raw_entries_in_range(
                (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
                crate::db::direction::Direction::Asc,
                |raw, _| {
                    candidate = Some(*IndexKey::try_from_raw(raw).unwrap().index_id());
                    Ok::<_, InternalError>(true)
                },
            )
            .unwrap();
        candidate.unwrap()
    });
    store.with_index_mut(|index| {
        // Candidate system keys follow user keys in raw order, exercising
        // resume across namespaces with the unchanged persisted cursor.
        for ordinal in 0..extra_entries {
            let key = IndexKey::new_from_components_with_primary_key_value(
                &candidate_id,
                IndexKeyKind::System,
                &[ordinal.to_be_bytes()],
                &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(ordinal)),
            )
            .unwrap()
            .to_raw()
            .unwrap();
            index
                .apply_canonical_entry(key, Some(&IndexEntryValue::presence()))
                .unwrap();
        }
        for ordinal in 0..513_u64 {
            let component = ordinal.to_be_bytes();
            let key = IndexKey::new_from_components_with_primary_key_value(
                &IndexId::new(EntityTag::new(2), 0),
                IndexKeyKind::User,
                &[component],
                &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(ordinal)),
            )
            .expect("unrelated abort-scan key should build")
            .to_raw()
            .expect("unrelated abort-scan key should encode");
            index.insert(key, IndexEntryValue::presence());
        }
    });
    let abort = || SchemaMigrationCommand::Abort {
        expected_database: target.database_identity(),
        expected_head: target.accepted_head().clone(),
        expected_plan: plan,
    };
    if extra_entries != 0 {
        let cleaning = migrate_schema(&db, &proposal, abort()).unwrap();
        assert_eq!(cleaning.phase(), SchemaMigrationPhase::ReadyToRewrite);
        assert_eq!(
            (store.with_index(IndexStore::len)).expect("storage operation should succeed"),
            516
        );
    }
    let aborted =
        migrate_schema(&db, &proposal, abort()).expect("pre-rewrite migration should abort");
    assert_eq!(aborted.phase(), SchemaMigrationPhase::Aborted);
    assert_eq!(
        (store.with_index(IndexStore::len)).expect("storage operation should succeed"),
        513,
        "abort must remove only planner-invisible candidate generations",
    );
    assert_eq!(
        row_bytes(),
        before_rows,
        "abort must retain predecessor rows"
    );
    ensure_generated_schema_application_admitted(&db, &proposal)
        .expect("terminal aborted record must not block generated startup");
}

#[cfg(feature = "migration")]
#[test]
fn physical_migration_rewrite_recovers_and_publishes_one_complete_candidate() {
    use super::{
        ensure_generated_schema_application_admitted, migrate_schema,
        schema_migration_status_for_target,
    };
    use crate::db::{
        data::{CanonicalSlotReader, DecodedDataStoreKey, StoreVisit, StructuralSlotReader},
        schema::{
            MigrationRewriteInterruption, SchemaMigrationCommand, SchemaMigrationPhase,
            ensure_schema_migration_ready_for_ordinary_operations,
            interrupt_next_migration_rewrite_at,
        },
    };
    use crate::error::InternalError;

    let db = Db::<MigrationExecutionCanister>::new(
        &MIGRATION_EXECUTION_REGISTRY,
        crate::db::RequestExecutionRoot::__new_runtime_root().scope(),
    );
    drive_startup_recovery_to_completion(&db);
    let initial_target = schema_application_target(&db).expect("initial target should issue");
    let store_identity = initial_target
        .stores()
        .first()
        .expect("migration execution store should exist")
        .identity();
    let initial = validation_migration_proposal(
        ValidationMigrationShape::Clean,
        false,
        initial_target.accepted_head().clone(),
        initial_target.database_identity(),
        store_identity,
    );
    apply_schema(&db, &initial).expect("initial schema should publish");
    let session = DbSession::<MigrationExecutionCanister>::new(
        &MIGRATION_EXECUTION_REGISTRY,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    );
    for (id, value) in [(1, 7), (2, 8), (3, 9)] {
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                entity: "MigratingItem".to_string(),
                patch: DynamicStructuralPatch::new(vec![
                    (
                        "id".to_string(),
                        DynamicWriteCell::Value(InputValue::nat64(id)),
                    ),
                    (
                        "old_value".to_string(),
                        DynamicWriteCell::Value(InputValue::int64(value)),
                    ),
                ]),
            })
            .expect("predecessor row should insert");
    }
    let target = schema_application_target(&db).expect("migration target should issue");
    let proposal = validation_migration_proposal(
        ValidationMigrationShape::Clean,
        true,
        target.accepted_head().clone(),
        target.database_identity(),
        store_identity,
    );
    let plan = proposal
        .migration()
        .expect("migration plan should exist")
        .digest();
    let command = || SchemaMigrationCommand::Advance {
        expected_database: target.database_identity(),
        expected_head: target.accepted_head().clone(),
        expected_plan: plan,
    };
    for expected in [
        SchemaMigrationPhase::Prepared,
        SchemaMigrationPhase::Validating,
        SchemaMigrationPhase::ReadyToRewrite,
        SchemaMigrationPhase::RewritingRows,
    ] {
        assert_eq!(
            migrate_schema(&db, &proposal, command())
                .expect("migration phase should advance")
                .phase(),
            expected,
        );
    }

    for interruption in [
        MigrationRewriteInterruption::MarkerPersisted,
        MigrationRewriteInterruption::JournalPublished,
        MigrationRewriteInterruption::PhysicalApplied,
    ] {
        interrupt_next_migration_rewrite_at(interruption);
        migrate_schema(&db, &proposal, command())
            .expect_err("injected interruption should retain the rewrite marker");

        forget_recovered_domain_for_tests(&db).expect("upgrade should reset recovery ownership");
        drive_startup_recovery_to_completion(&db);
    }

    let final_validation = schema_migration_status_for_target(
        &db,
        &proposal,
        &schema_application_target(&db).expect("recovered target should issue"),
    )
    .expect("recovered status should remain readable");
    assert_eq!(
        final_validation.phase(),
        SchemaMigrationPhase::FinalValidation
    );
    assert_eq!(final_validation.rows_rewritten(), 3);
    assert_eq!(final_validation.indexes_rebuilt(), 1);
    assert_eq!(
        migrate_schema(&db, &proposal, command())
            .expect("final validation should complete")
            .phase(),
        SchemaMigrationPhase::Publishing,
    );
    let applied = migrate_schema(&db, &proposal, command())
        .expect("candidate publication should complete atomically");
    assert_eq!(applied.phase(), SchemaMigrationPhase::Applied);
    assert_eq!(applied.rows_rewritten(), 3);
    assert_eq!(applied.indexes_rebuilt(), 1);
    assert_ne!(applied.accepted_head(), target.accepted_head());
    let terminal_target = schema_application_target(&db).expect("terminal target should issue");
    let terminal_proposal = validation_migration_proposal(
        ValidationMigrationShape::Clean,
        true,
        terminal_target.accepted_head().clone(),
        terminal_target.database_identity(),
        store_identity,
    );
    ensure_generated_schema_application_admitted(&db, &terminal_proposal)
        .expect("terminal record must not block generated startup");

    let store = db
        .store_handle(MIGRATION_EXECUTION_STORE_PATH)
        .expect("migration execution store should resolve");
    let runtime = db
        .accepted_runtime_entity_for_path("MigratingItem")
        .expect("published candidate entity should resolve");
    let selection = store
        .with_schema(|schema| {
            schema.current_accepted_catalog_selection(
                runtime.entity_tag(),
                runtime.entity_path(),
                runtime.store_path(),
            )
        })
        .expect("candidate selection should remain readable")
        .expect("candidate selection should exist");
    let contract = crate::db::data::AcceptedStructuralRowAuthority::from_catalog_selection(
        runtime.entity_path(),
        &selection,
    )
    .expect("candidate row authority should compile")
    .into_row_contract();
    let mut values = Vec::new();
    store
        .with_data(|data| {
            data.visit_entries(|key, row| {
                let decoded =
                    DecodedDataStoreKey::try_from_raw(key).expect("rewritten key should decode");
                let reader = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                    row, &contract,
                )
                .expect("rewritten row should use the candidate layout");
                reader
                    .validate_primary_key(&decoded)
                    .expect("rewritten row and key should remain bound");
                values.push(
                    reader
                        .required_value_by_contract(1)
                        .expect("candidate value slot should decode"),
                );
                Ok::<StoreVisit, InternalError>(StoreVisit::Continue)
            })
        })
        .expect("rewritten row scan should complete");
    assert_eq!(
        values,
        vec![
            crate::value::Value::Nat64(7),
            crate::value::Value::Nat64(8),
            crate::value::Value::Nat64(9),
        ],
    );
    assert_eq!(
        (store.with_index(IndexStore::len)).expect("storage operation should succeed"),
        3
    );
    let accepted = store
        .with_schema(SchemaStore::current_accepted_schema_bundle)
        .expect("published candidate bundle should remain readable")
        .expect("published candidate bundle should exist");
    let entity_source =
        EntitySourceKey::try_new("MigratingItem").expect("migration entity source should admit");
    let entity_tag = accepted
        .source_bindings_for_tests()
        .entity(&entity_source)
        .expect("candidate entity source should remain bound");
    let old_value = FieldSourceKey::try_new("old_value").expect("predecessor source should admit");
    let current_value = FieldSourceKey::try_new("value").expect("candidate source should admit");
    assert_eq!(
        accepted
            .source_bindings_for_tests()
            .field(entity_tag, &old_value),
        None,
    );
    assert!(
        accepted
            .source_bindings_for_tests()
            .field(entity_tag, &current_value)
            .is_some(),
    );
    ensure_schema_migration_ready_for_ordinary_operations()
        .expect("terminal publication must clear the database-wide gate");
}

#[cfg(feature = "migration")]
#[test]
fn physical_migration_validation_reports_every_typed_finding_family_without_writes() {
    use crate::error::InternalError;

    use super::migrate_schema;
    use crate::db::{
        data::StoreVisit,
        schema::{
            SchemaMigrationCommand, SchemaMigrationFindingKind, SchemaMigrationPhase,
            SchemaMigrationStatusPage, SchemaMigrationTransformReason,
        },
    };

    let db = Db::<MigrationFindingCanister>::new(
        &MIGRATION_FINDING_REGISTRY,
        crate::db::RequestExecutionRoot::__new_runtime_root().scope(),
    );
    drive_startup_recovery_to_completion(&db);
    let initial_target = schema_application_target(&db).expect("initial target should issue");
    let store_identity = initial_target
        .stores()
        .first()
        .expect("migration finding store should exist")
        .identity();
    let initial = validation_migration_proposal(
        ValidationMigrationShape::AllFindingFamilies,
        false,
        initial_target.accepted_head().clone(),
        initial_target.database_identity(),
        store_identity,
    );
    apply_schema(&db, &initial).expect("initial finding schema should publish");

    let session = DbSession::<MigrationFindingCanister>::new(
        &MIGRATION_FINDING_REGISTRY,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    );
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: "MigrationTarget".to_string(),
            patch: DynamicStructuralPatch::new(vec![(
                "id".to_string(),
                DynamicWriteCell::Value(InputValue::nat64(7)),
            )]),
        })
        .expect("relation target should insert");
    for (id, value) in [(1, 9), (2, 8), (3, 7), (4, 7), (5, 300)] {
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                entity: "MigratingItem".to_string(),
                patch: DynamicStructuralPatch::new(vec![
                    (
                        "id".to_string(),
                        DynamicWriteCell::Value(InputValue::nat64(id)),
                    ),
                    (
                        "old_value".to_string(),
                        DynamicWriteCell::Value(InputValue::int64(value)),
                    ),
                ]),
            })
            .expect("predecessor finding row should insert");
    }
    let store = db
        .store_handle(MIGRATION_FINDING_STORE_PATH)
        .expect("migration finding store should resolve");
    let row_bytes = || {
        store.with_data(|data| {
            let mut rows = Vec::new();
            let result: Result<(), InternalError> = data.visit_entries(|key, row| {
                rows.push((key.as_bytes().to_vec(), row.as_bytes().to_vec()));
                Ok(StoreVisit::Continue)
            });
            result.expect("infallible row visit should complete");
            rows
        })
    };
    let before_rows = row_bytes();

    let target = schema_application_target(&db).expect("migration target should issue");
    let proposal = validation_migration_proposal(
        ValidationMigrationShape::AllFindingFamilies,
        true,
        target.accepted_head().clone(),
        target.database_identity(),
        store_identity,
    );
    let plan = proposal
        .migration()
        .expect("migration plan should exist")
        .digest();
    let command = || SchemaMigrationCommand::Advance {
        expected_database: target.database_identity(),
        expected_head: target.accepted_head().clone(),
        expected_plan: plan,
    };
    assert_eq!(
        migrate_schema(&db, &proposal, command())
            .expect("finding migration should prepare")
            .phase(),
        SchemaMigrationPhase::Prepared,
    );
    assert_eq!(
        migrate_schema(&db, &proposal, command())
            .expect("finding migration should enter validation")
            .phase(),
        SchemaMigrationPhase::Validating,
    );
    let rejected =
        migrate_schema(&db, &proposal, command()).expect("validation should report findings");
    assert_eq!(rejected.phase(), SchemaMigrationPhase::Rejected);
    assert_eq!(rejected.rows_validated(), 5);
    assert_eq!(
        rejected
            .findings()
            .iter()
            .map(crate::db::schema::SchemaMigrationFinding::kind)
            .collect::<Vec<_>>(),
        vec![
            SchemaMigrationFindingKind::Constraint,
            SchemaMigrationFindingKind::Relation,
            SchemaMigrationFindingKind::UniqueIndex,
            SchemaMigrationFindingKind::Transform,
        ],
    );
    for finding in rejected.findings() {
        if finding.kind() == SchemaMigrationFindingKind::Transform {
            assert!(finding.source_field().is_some());
            assert_eq!(finding.target_field(), finding.source_field());
            assert_eq!(
                finding.transform_reason(),
                Some(SchemaMigrationTransformReason::Overflow)
            );
        } else {
            assert_eq!(finding.source_field(), None);
            assert_eq!(finding.target_field(), None);
            assert_eq!(finding.transform_reason(), None);
        }
    }
    let bytes = icydb_schema::wire::encode_one(&rejected).expect("finding status should encode");
    let decoded: SchemaMigrationStatusPage =
        icydb_schema::wire::decode_one(&bytes).expect("finding status should decode");
    assert_eq!(decoded, rejected);
    assert_eq!(
        row_bytes(),
        before_rows,
        "rejected validation must not rewrite accepted rows"
    );
    assert_eq!(
        (store.with_index(IndexStore::len)).expect("storage operation should succeed"),
        0,
        "a rejected page must not publish any staged generation"
    );
}

#[cfg(feature = "migration")]
#[test]
fn exact_migration_retry_binds_the_terminal_head_not_the_predecessor_head() {
    use super::exact_migration_replay_target;

    let db = Db::<EvolutionCanister>::new(
        &EVOLUTION_REGISTRY,
        crate::db::RequestExecutionRoot::__new_runtime_root().scope(),
    );
    drive_startup_recovery_to_completion(&db);
    let initial_target = schema_application_target(&db).expect("initial target should issue");
    let (proposal, _, _) = generated_check_proposal(
        initial_target.accepted_head().clone(),
        "migration-retry-initial",
        false,
        initial_target.database_identity(),
        initial_target
            .stores()
            .first()
            .expect("test store should exist")
            .identity(),
    );
    apply_schema(&db, &proposal).expect("initial schema should publish");
    let current_target = schema_application_target(&db).expect("current target should issue");
    assert_ne!(
        current_target.accepted_head(),
        initial_target.accepted_head(),
    );

    let receipt = SchemaChangeReceipt::new(
        current_target.database_identity(),
        SchemaSubmissionKey::try_new("migration/retry").expect("migration submission should admit"),
        SchemaProposalDigest::from_bytes([0x77; 32]),
        initial_target.accepted_head().clone(),
        SchemaChangeOutcome::Applied {
            accepted_head: current_target.accepted_head().clone(),
        },
    )
    .expect("terminal migration receipt should admit");
    let record = SchemaApplicationRecord::new(receipt, Vec::new())
        .expect("terminal migration record should admit");

    assert_eq!(
        exact_migration_replay_target(&db, current_target.database_identity(), &record,)
            .expect("exact retry should resolve the terminal target"),
        current_target,
    );
}
