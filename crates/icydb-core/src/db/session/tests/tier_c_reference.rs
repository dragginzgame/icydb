//! Native IcyDB execution owner for exact Tier C SQL structural evidence.

use crate::{
    db::{
        DynamicStructuralPatch, DynamicWriteCell,
        data::{DataStore, encode_input_value_for_candidate_field_contract},
        index::IndexStore,
        registry::{StoreAllocationIdentities, StoreRegistry, StoreRuntimeStorageCapabilities},
        schema::{
            AcceptedCompositeCatalog, AcceptedFieldDecodeContract, AcceptedFieldKind,
            AcceptedSchemaRevision, FieldId, FieldInsertGeneration, FieldStorageDecode, LeafCodec,
            PersistedFieldSnapshot, PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot,
            PersistedIndexSnapshot, PersistedSchemaSnapshot, SchemaFieldSlot,
            SchemaFieldWritePolicy, SchemaIndexId, SchemaInsertDefault, SchemaRowLayout,
            SchemaStore, SchemaVersion, accepted_schema_candidate_with_field_bindings_for_tests,
            empty_accepted_enum_catalog_for_tests, enum_catalog::ValueAdmissionBudget,
        },
        session::{DbSession, SqlStatementResult},
    },
    traits::{CanisterKind, Path},
    types::EntityTag,
    value::{InputValue, OutputValue},
};
use icydb_schema::FieldSourceKey;
use icydb_testing_sql_generator::{
    ExecutionAccess, ExecutionCovering, GeneratedMutationSequence, GeneratedSelectCase,
    GeneratedValue, MutationDefaultValue, MutationFieldKind, MutationOperation, MutationRow,
    MutationRowPayload, MutationSchemaProfile, MutationStepOutcome, ObservedExecutionFacts,
    RequiredExecutionFacts, SQL_SCHEDULED_SHARD_COUNT, SelectFieldKind, SelectResultOrder,
    SelectSnapshot, TIER_C_MUTATION_BUDGETS, TIER_C_MUTATION_REPETITIONS, TIER_C_ROOT_SEEDS,
    TIER_C_SELECT_BUDGETS, TIER_C_SELECT_REPETITIONS, TierCMergedReport, TierCScenarioObservation,
    TierCScenarioOutcome, TierCShardReport, generate_scheduled_mutation_sequence,
    generate_scheduled_select_case, scheduled_mutation_witnesses, scheduled_select_witnesses,
    scheduled_sql_scenario_shard,
};
use icydb_testing_sqlite_reference::{
    SqliteReferenceResult, SqliteReferenceRowOrder, SqliteReferenceValue,
    execute_generated_select_case,
};
use std::{
    cell::RefCell,
    collections::BTreeMap,
    env, fs,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path as FsPath, PathBuf},
};

const STORE_PATH: &str = "db::session::tests::tier_c_reference::Store";
const SELECT_REFERENCE_TAG: EntityTag = EntityTag::new(201);
const SELECT_INDEXED_TAG: EntityTag = EntityTag::new(202);
const MUTATION_AUTHORED_TAG: EntityTag = EntityTag::new(203);
const MUTATION_DEFAULT_TAG: EntityTag = EntityTag::new(204);
const SHARD_FILE_PREFIX: &str = "tier-c-shard-";
const MERGED_FILE: &str = "tier-c-merged.json";

struct TestCanister;

impl Path for TestCanister {
    const PATH: &'static str = "db::session::tests::tier_c_reference::Canister";
}

impl CanisterKind for TestCanister {
    const COMMIT_MEMORY_ID: u8 = 210;
    const COMMIT_STABLE_KEY: &'static str = "icydb.test.tier_c.commit.v1";
    const STARTUP_MEMORY_ID: u8 = 212;
    const STARTUP_STABLE_KEY: &'static str = "icydb.test.tier_c.startup.control.v1";
    const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 211;
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str = "icydb.test.tier_c.integrity.progress.v1";
}

thread_local! {
    static DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
    static INDEX_STORE: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
    static SCHEMA_STORE: RefCell<SchemaStore> =
        const { RefCell::new(SchemaStore::init_heap()) };
    static STORE_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_store(
            STORE_PATH,
            &DATA_STORE,
            &INDEX_STORE,
            &SCHEMA_STORE,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
        ).expect("Tier C store should register");
        registry
    };
}

#[test]
#[ignore = "the Make shard target owns native Tier C execution"]
fn tier_c_native_shard_emits_exact_receipt() {
    let shard_index = required_shard_index();
    let artifact_dir = required_artifact_dir();
    let declared = declared_scenarios();
    let session = initialize();
    let mut observations = Vec::new();

    for witness in scheduled_select_witnesses().expect("reviewed SELECT schedule should decode") {
        for root_seed in TIER_C_ROOT_SEEDS {
            for repetition in 0..TIER_C_SELECT_REPETITIONS {
                let case = generate_scheduled_select_case(
                    &witness,
                    *root_seed,
                    repetition,
                    TIER_C_SELECT_BUDGETS,
                )
                .expect("scheduled SELECT should generate");
                if scheduled_sql_scenario_shard(case.identity().id())
                    .expect("scheduled SELECT should shard")
                    != shard_index
                {
                    continue;
                }
                observations.push(execute_select_case(
                    &session,
                    &case,
                    witness.required_execution_facts(),
                ));
            }
        }
    }

    for witness in scheduled_mutation_witnesses().expect("reviewed mutation schedule should decode")
    {
        for root_seed in TIER_C_ROOT_SEEDS {
            for repetition in 0..TIER_C_MUTATION_REPETITIONS {
                let sequence = generate_scheduled_mutation_sequence(
                    &witness,
                    *root_seed,
                    repetition,
                    TIER_C_MUTATION_BUDGETS,
                )
                .expect("scheduled mutation should generate");
                if scheduled_sql_scenario_shard(sequence.identity().id())
                    .expect("scheduled mutation should shard")
                    != shard_index
                {
                    continue;
                }
                observations.push(execute_mutation_sequence(&session, &sequence));
            }
        }
    }

    let declared_refs = declared.iter().map(String::as_str).collect::<Vec<_>>();
    let report = TierCShardReport::try_new(shard_index, &declared_refs, observations)
        .expect("native Tier C shard must be exact and complete");
    assert!(
        report.observed_scenario_count() > 0,
        "a selected Tier C shard must execute at least one IcyDB scenario",
    );
    write_artifact(
        shard_path(&artifact_dir, shard_index),
        report
            .to_canonical_json(&declared_refs)
            .expect("Tier C shard should encode canonically"),
    );
}

#[test]
#[ignore = "the Make merge target owns exact eight-shard evidence merge"]
fn tier_c_native_receipts_merge_exactly_and_require_clean_evidence() {
    let artifact_dir = required_artifact_dir();
    let declared = declared_scenarios();
    let declared_refs = declared.iter().map(String::as_str).collect::<Vec<_>>();
    let reports = (0..SQL_SCHEDULED_SHARD_COUNT)
        .map(|shard_index| {
            let bytes = fs::read(shard_path(&artifact_dir, shard_index))
                .unwrap_or_else(|error| panic!("missing Tier C shard {shard_index}: {error}"));
            TierCShardReport::from_canonical_json(&bytes, &declared_refs)
                .unwrap_or_else(|error| panic!("invalid Tier C shard {shard_index}: {error}"))
        })
        .collect::<Vec<_>>();
    let merged = TierCMergedReport::try_merge(&declared_refs, reports)
        .expect("all eight native Tier C shards must merge exactly");
    merged
        .require_clean()
        .expect("native Tier C evidence must contain no failed scenario");
    assert_eq!(
        merged.observed_scenario_count(),
        u32::try_from(declared.len()).expect("Tier C scenario count should fit u32"),
    );
    write_artifact(
        artifact_dir.join(MERGED_FILE),
        merged
            .to_canonical_json(&declared_refs)
            .expect("merged Tier C receipt should encode canonically"),
    );
}

#[test]
#[ignore = "the Make replay target supplies one current failure artifact"]
fn tier_c_failure_artifact_replays_exact_minimized_failure() {
    let path = env::var("ICYDB_SQL_TIER_C_FAILURE_ARTIFACT")
        .map(PathBuf::from)
        .expect("ICYDB_SQL_TIER_C_FAILURE_ARTIFACT must be set");
    let bytes = fs::read(&path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    let artifact = icydb_testing_sql_generator::TierCFailureArtifact::from_canonical_json(&bytes)
        .expect("failure artifact must use the sole current canonical format");
    let session = initialize();
    match artifact.replay() {
        icydb_testing_sql_generator::TierCFailureReplay::Select(replay) => {
            let case = replay.minimized_case();
            let required = scheduled_select_witnesses()
                .expect("reviewed SELECT schedule should decode")
                .into_iter()
                .find(|witness| witness.witness_id() == case.identity().witness_id())
                .map_or_else(not_applicable_required_facts, |witness| {
                    witness.required_execution_facts()
                });
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    execute_select_case(&session, case, required)
                }))
                .is_err(),
                "replayed minimized SELECT no longer reproduces its recorded failure",
            );
        }
        icydb_testing_sql_generator::TierCFailureReplay::Mutation(replay) => {
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    execute_mutation_sequence(&session, replay.minimized_sequence())
                }))
                .is_err(),
                "replayed minimized mutation no longer reproduces its recorded failure",
            );
        }
    }
}

fn execute_select_case(
    session: &DbSession<TestCanister>,
    case: &GeneratedSelectCase,
    required: RequiredExecutionFacts,
) -> TierCScenarioObservation {
    replace_select_fixture(session, case);
    if case.violation().is_some() {
        assert!(
            session
                .execute_trusted_sql_query_with_attribution(case.rendered_sql())
                .is_err(),
            "Tier C invalid SELECT was accepted: scenario={} sql={:?}",
            case.identity().id(),
            case.rendered_sql(),
        );
        return observation(
            case.identity().id(),
            case.structural_signature()
                .expect("rejection structural signature should derive"),
            ObservedExecutionFacts::new(
                ExecutionAccess::NotApplicable,
                ExecutionCovering::NotApplicable,
            ),
            TierCScenarioOutcome::ExpectedRejection,
        );
    }

    let expected = execute_generated_select_case(case)
        .expect("accepted scheduled SELECT should execute in the SQLite reference");
    let observed_facts = planned_select_execution_facts(session, case.rendered_sql());
    let executed = session.execute_trusted_sql_query_with_attribution(case.rendered_sql());
    let (result, _) = match executed {
        Ok(executed) => executed,
        Err(error) => panic!(
            "Tier C accepted SELECT was rejected: scenario={} sql={:?} error={error:?}",
            case.identity().id(),
            case.rendered_sql(),
        ),
    };
    let actual = sqlite_result_from_icydb(case, result);
    let result_matches = actual.as_ref() == Ok(&expected);
    let facts_match = observed_facts == required.into();
    assert!(
        result_matches && facts_match,
        "Tier C SELECT mismatch: scenario={} result_matches={} facts_match={} required={required:?} observed={observed_facts:?} expected={expected:?} actual={actual:?}",
        case.identity().id(),
        result_matches,
        facts_match,
    );
    observation(
        case.identity().id(),
        case.structural_signature()
            .expect("SELECT structural signature should derive"),
        observed_facts,
        TierCScenarioOutcome::Passed,
    )
}

fn execute_mutation_sequence(
    session: &DbSession<TestCanister>,
    sequence: &GeneratedMutationSequence,
) -> TierCScenarioObservation {
    replace_mutation_fixture(session, sequence);
    let mut mismatch = false;
    for step in sequence.steps() {
        let executed = match step.statement().operation() {
            MutationOperation::Update {
                window: Some(_), ..
            } => session.execute_trusted_sql_prefix_update(step.rendered_sql()),
            MutationOperation::Update { window: None, .. } => {
                session.execute_trusted_sql_exact_update(step.rendered_sql(), 4_096)
            }
            MutationOperation::Delete { .. }
            | MutationOperation::Insert { .. }
            | MutationOperation::InsertFromQuery { .. } => {
                session.execute_trusted_sql_mutation(step.rendered_sql())
            }
        };
        let result_mismatch = match step.expected() {
            MutationStepOutcome::Accepted { affected_rows, .. } => {
                executed.as_ref().map(sql_result_row_count).ok().flatten() != Some(*affected_rows)
            }
            MutationStepOutcome::Rejected { .. } => executed.is_ok(),
        };
        let state = mutation_rows(session, sequence.snapshot().profile());
        let state_mismatch = state != step.expected().state_after();
        if result_mismatch || state_mismatch {
            eprintln!(
                "Tier C mutation mismatch: scenario={} sql={:?} result_mismatch={} state_mismatch={} expected={:?} actual_result={executed:?} actual_state={state:?}",
                sequence.identity().id(),
                step.rendered_sql(),
                result_mismatch,
                state_mismatch,
                step.expected(),
            );
        }
        mismatch |= result_mismatch || state_mismatch;
    }
    assert!(
        !mismatch,
        "Tier C mutation outcome disagreed with the independent state model: scenario={}",
        sequence.identity().id(),
    );
    let outcome = if sequence
        .steps()
        .iter()
        .any(|step| step.expected().rejection().is_some())
    {
        TierCScenarioOutcome::ExpectedRejection
    } else {
        TierCScenarioOutcome::Passed
    };
    observation(
        sequence.identity().id(),
        sequence
            .structural_signature()
            .expect("mutation structural signature should derive"),
        ObservedExecutionFacts::new(
            ExecutionAccess::MutationSelection,
            ExecutionCovering::NotApplicable,
        ),
        outcome,
    )
}

fn initialize() -> DbSession<TestCanister> {
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    let select_witnesses =
        scheduled_select_witnesses().expect("reviewed SELECT schedule should decode");
    let reference_case = generate_scheduled_select_case(
        select_witnesses
            .iter()
            .find(|witness| witness.provider_id().ends_with("reference_scalar"))
            .expect("reference SELECT profile should exist"),
        TIER_C_ROOT_SEEDS[0],
        0,
        TIER_C_SELECT_BUDGETS,
    )
    .expect("reference SELECT schema should generate");
    let indexed_case = generate_scheduled_select_case(
        select_witnesses
            .iter()
            .find(|witness| {
                witness
                    .provider_id()
                    .ends_with("indexed_nullable_reference")
            })
            .expect("indexed SELECT profile should exist"),
        TIER_C_ROOT_SEEDS[0],
        0,
        TIER_C_SELECT_BUDGETS,
    )
    .expect("indexed SELECT schema should generate");
    let mutation_witnesses =
        scheduled_mutation_witnesses().expect("reviewed mutation schedule should decode");
    let authored = generate_scheduled_mutation_sequence(
        mutation_witnesses
            .iter()
            .find(|witness| witness.provider_id().ends_with("authored_scalar"))
            .expect("authored mutation profile should exist"),
        TIER_C_ROOT_SEEDS[0],
        0,
        TIER_C_MUTATION_BUDGETS,
    )
    .expect("authored mutation schema should generate");
    let defaults = generate_scheduled_mutation_sequence(
        mutation_witnesses
            .iter()
            .find(|witness| witness.provider_id().ends_with("accepted_default"))
            .expect("default mutation profile should exist"),
        TIER_C_ROOT_SEEDS[0],
        0,
        TIER_C_MUTATION_BUDGETS,
    )
    .expect("default mutation schema should generate");

    let snapshots = BTreeMap::from([
        (
            SELECT_REFERENCE_TAG,
            select_schema(reference_case.snapshot()),
        ),
        (SELECT_INDEXED_TAG, select_schema(indexed_case.snapshot())),
        (MUTATION_AUTHORED_TAG, mutation_schema(authored.snapshot())),
        (MUTATION_DEFAULT_TAG, mutation_schema(defaults.snapshot())),
    ]);
    let fields = source_bindings(
        &[
            (SELECT_REFERENCE_TAG, reference_case.snapshot()),
            (SELECT_INDEXED_TAG, indexed_case.snapshot()),
        ],
        &[
            (MUTATION_AUTHORED_TAG, authored.snapshot()),
            (MUTATION_DEFAULT_TAG, defaults.snapshot()),
        ],
    );
    let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        snapshots,
        fields,
    );
    let session = DbSession::<TestCanister>::new(
        &STORE_REGISTRY,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    );
    session
        .db
        .drive_startup_recovery_page()
        .expect("Tier C database should initialize");
    let store = session
        .db
        .store_handle(STORE_PATH)
        .expect("Tier C store should resolve");
    crate::db::commit::publish_accepted_schema_candidate(
        STORE_PATH,
        store,
        AcceptedSchemaRevision::NONE,
        &candidate,
    )
    .expect("Tier C accepted schema should publish");
    session
}

fn select_schema(snapshot: &SelectSnapshot) -> PersistedSchemaSnapshot {
    let fields = snapshot
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            let kind = select_field_kind(field.kind());
            let slot = SchemaFieldSlot::new(u16::try_from(ordinal).expect("field slot should fit"));
            if field.generated() {
                PersistedFieldSnapshot::new_initial_with_write_policy(
                    FieldId::new(field.id()),
                    field.name().to_string(),
                    slot,
                    kind.clone(),
                    Vec::new(),
                    field.nullable(),
                    SchemaInsertDefault::None,
                    SchemaFieldWritePolicy::from_model_policies(
                        Some(FieldInsertGeneration::Ulid),
                        None,
                    ),
                    FieldStorageDecode::ByKind,
                    kind.leaf_codec_for_storage(FieldStorageDecode::ByKind),
                )
            } else {
                PersistedFieldSnapshot::new_initial(
                    FieldId::new(field.id()),
                    field.name().to_string(),
                    slot,
                    kind.clone(),
                    Vec::new(),
                    field.nullable(),
                    SchemaInsertDefault::None,
                    FieldStorageDecode::ByKind,
                    kind.leaf_codec_for_storage(FieldStorageDecode::ByKind),
                )
            }
        })
        .collect::<Vec<_>>();
    let indexes = snapshot
        .indexes()
        .iter()
        .enumerate()
        .map(|(ordinal, index)| {
            PersistedIndexSnapshot::new(
                SchemaIndexId::new(u32::from(index.id()))
                    .expect("SELECT index identity should be non-zero"),
                u16::try_from(ordinal + 1).expect("SELECT index ordinal should fit"),
                index.name().to_string(),
                STORE_PATH.to_string(),
                false,
                PersistedIndexKeySnapshot::FieldPath(
                    index
                        .field_ids()
                        .iter()
                        .map(|field_id| {
                            let field = snapshot
                                .fields()
                                .iter()
                                .enumerate()
                                .find(|(_, field)| field.id() == *field_id)
                                .expect("SELECT index field should exist");
                            PersistedIndexFieldPathSnapshot::new(
                                FieldId::new(*field_id),
                                SchemaFieldSlot::new(
                                    u16::try_from(field.0).expect("field slot should fit"),
                                ),
                                vec![field.1.name().to_string()],
                                select_field_kind(field.1.kind()),
                                field.1.nullable(),
                            )
                        })
                        .collect(),
                ),
                None,
            )
        })
        .collect();
    PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        snapshot.entity_path().to_string(),
        snapshot.entity_name().to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
        indexes,
    )
}

fn mutation_schema(
    snapshot: &icydb_testing_sql_generator::MutationSnapshot,
) -> PersistedSchemaSnapshot {
    let enum_catalog = empty_accepted_enum_catalog_for_tests();
    let composite_catalog = AcceptedCompositeCatalog::empty();
    let fields = snapshot
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            let kind = mutation_field_kind(field.kind());
            let storage_decode = FieldStorageDecode::ByKind;
            let leaf_codec = kind.leaf_codec_for_storage(storage_decode);
            let default = mutation_default(
                field.name(),
                field.default(),
                &kind,
                field.nullable(),
                leaf_codec,
                &enum_catalog,
                &composite_catalog,
            );
            PersistedFieldSnapshot::new_initial(
                FieldId::new(field.id()),
                field.name().to_string(),
                SchemaFieldSlot::new(u16::try_from(ordinal).expect("field slot should fit")),
                kind,
                Vec::new(),
                field.nullable(),
                default,
                storage_decode,
                leaf_codec,
            )
        })
        .collect::<Vec<_>>();
    let indexes = snapshot
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| field.indexed())
        .map(|(ordinal, field)| {
            PersistedIndexSnapshot::new(
                SchemaIndexId::new(1).expect("mutation index identity should be non-zero"),
                1,
                format!("by_{}", field.name()),
                STORE_PATH.to_string(),
                false,
                PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                    FieldId::new(field.id()),
                    SchemaFieldSlot::new(u16::try_from(ordinal).expect("field slot should fit")),
                    vec![field.name().to_string()],
                    mutation_field_kind(field.kind()),
                    field.nullable(),
                )]),
                None,
            )
        })
        .collect();
    PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        snapshot.entity_path().to_string(),
        snapshot.entity_name().to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
        indexes,
    )
}

fn mutation_default(
    field_name: &str,
    default: Option<&MutationDefaultValue>,
    kind: &AcceptedFieldKind,
    nullable: bool,
    leaf_codec: LeafCodec,
    enum_catalog: &crate::db::schema::AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> SchemaInsertDefault {
    let Some(default) = default else {
        return SchemaInsertDefault::None;
    };
    let input = match default {
        MutationDefaultValue::NullText => return SchemaInsertDefault::None,
        MutationDefaultValue::Text(value) => InputValue::text(value.clone()),
        MutationDefaultValue::UnsignedInteger(value) => InputValue::nat64(*value),
    };
    let payload = encode_input_value_for_candidate_field_contract(
        enum_catalog,
        composite_catalog,
        AcceptedFieldDecodeContract::new(
            field_name,
            kind,
            nullable,
            FieldStorageDecode::ByKind,
            leaf_codec,
        ),
        input,
        &mut ValueAdmissionBudget::standard(),
    )
    .expect("mutation default should encode through accepted scalar authority");
    SchemaInsertDefault::SlotPayload(payload)
}

fn source_bindings(
    select: &[(EntityTag, &SelectSnapshot)],
    mutation: &[(EntityTag, &icydb_testing_sql_generator::MutationSnapshot)],
) -> BTreeMap<(EntityTag, FieldSourceKey), FieldId> {
    select
        .iter()
        .flat_map(|(tag, snapshot)| {
            snapshot.fields().iter().map(move |field| {
                (
                    (
                        *tag,
                        FieldSourceKey::try_new(format!(
                            "{}::{}",
                            snapshot.entity_path(),
                            field.name()
                        ))
                        .expect("SELECT field source should admit"),
                    ),
                    FieldId::new(field.id()),
                )
            })
        })
        .chain(mutation.iter().flat_map(|(tag, snapshot)| {
            snapshot.fields().iter().map(move |field| {
                (
                    (
                        *tag,
                        FieldSourceKey::try_new(format!(
                            "{}::{}",
                            snapshot.entity_path(),
                            field.name()
                        ))
                        .expect("mutation field source should admit"),
                    ),
                    FieldId::new(field.id()),
                )
            })
        }))
        .collect()
}

fn replace_select_fixture(session: &DbSession<TestCanister>, case: &GeneratedSelectCase) {
    clear_entity(session, case.snapshot().entity_name());
    let patches = case
        .fixture()
        .rows()
        .iter()
        .map(|row| {
            DynamicStructuralPatch::new(
                case.snapshot()
                    .fields()
                    .iter()
                    .filter(|field| !field.generated())
                    .map(|field| {
                        let cell = match row
                            .value_by_field_id(field.id())
                            .expect("fixture should populate every authored scalar field")
                        {
                            GeneratedValue::Boolean(value) => {
                                DynamicWriteCell::Value(InputValue::boolean(*value))
                            }
                            GeneratedValue::Integer(value) => {
                                DynamicWriteCell::Value(InputValue::int64(*value))
                            }
                            GeneratedValue::Text(value) => {
                                DynamicWriteCell::Value(InputValue::text(value.clone()))
                            }
                            GeneratedValue::Null(_) => DynamicWriteCell::Null,
                        };
                        (field.name().to_string(), cell)
                    })
                    .collect(),
            )
        })
        .collect::<Vec<_>>();
    if !patches.is_empty() {
        session
            .execute_trusted_dynamic_insert_batch(case.snapshot().entity_name(), patches)
            .expect("SELECT fixture should seed through the ordinary structural write path");
    }
}

fn replace_mutation_fixture(
    session: &DbSession<TestCanister>,
    sequence: &GeneratedMutationSequence,
) {
    clear_entity(session, sequence.snapshot().entity_name());
    let patches = sequence
        .initial_rows()
        .iter()
        .map(mutation_patch)
        .collect::<Vec<_>>();
    if !patches.is_empty() {
        session
            .execute_trusted_dynamic_insert_batch(sequence.snapshot().entity_name(), patches)
            .expect("mutation fixture should seed through the ordinary structural write path");
    }
}

fn mutation_patch(row: &MutationRow) -> DynamicStructuralPatch {
    let mut fields = vec![(
        "id".to_string(),
        DynamicWriteCell::Value(InputValue::nat64(row.key())),
    )];
    match row.payload() {
        MutationRowPayload::AuthoredScalar { text, number } => {
            fields.push((
                "name".to_string(),
                DynamicWriteCell::Value(InputValue::text(text.clone())),
            ));
            fields.push((
                "age".to_string(),
                DynamicWriteCell::Value(InputValue::nat64(*number)),
            ));
        }
        MutationRowPayload::AcceptedDefault {
            name,
            tier,
            score,
            note,
        } => {
            fields.extend([
                (
                    "name".to_string(),
                    DynamicWriteCell::Value(InputValue::text(name.clone())),
                ),
                (
                    "tier".to_string(),
                    DynamicWriteCell::Value(InputValue::text(tier.clone())),
                ),
                (
                    "score".to_string(),
                    DynamicWriteCell::Value(InputValue::nat64(*score)),
                ),
                (
                    "note".to_string(),
                    note.as_ref().map_or(DynamicWriteCell::Null, |note| {
                        DynamicWriteCell::Value(InputValue::text(note.clone()))
                    }),
                ),
            ]);
        }
    }
    DynamicStructuralPatch::new(fields)
}

fn clear_entity(session: &DbSession<TestCanister>, entity_name: &str) {
    session
        .execute_trusted_sql_mutation(&format!("DELETE FROM {entity_name} WHERE 1 = 1"))
        .expect("Tier C fixture reset should use the ordinary SQL mutation path");
}

fn mutation_rows(
    session: &DbSession<TestCanister>,
    profile: MutationSchemaProfile,
) -> Vec<MutationRow> {
    let (entity, columns) = match profile {
        MutationSchemaProfile::AuthoredScalar => ("GeneratedAuthoredMutation", "id, name, age"),
        MutationSchemaProfile::AcceptedDefault => {
            ("GeneratedDefaultMutation", "id, name, tier, score, note")
        }
    };
    let SqlStatementResult::Projection { rows, .. } = session
        .execute_trusted_sql_query(&format!("SELECT {columns} FROM {entity} ORDER BY id ASC"))
        .expect("mutation state should remain queryable")
    else {
        panic!("mutation state query should return a projection");
    };
    rows.into_iter()
        .map(|row| match (profile, row.as_slice()) {
            (MutationSchemaProfile::AuthoredScalar, [key, text, number]) => {
                let (
                    crate::value::PublicValue::Nat64(key),
                    crate::value::PublicValue::Text(text),
                    crate::value::PublicValue::Nat64(number),
                ) = (key.as_public(), text.as_public(), number.as_public())
                else {
                    panic!("authored scalar row should retain its accepted shape");
                };
                MutationRow::authored_scalar(*key, text.clone(), *number)
            }
            (MutationSchemaProfile::AcceptedDefault, [key, name, tier, score, note]) => {
                let (
                    crate::value::PublicValue::Nat64(key),
                    crate::value::PublicValue::Text(name),
                    crate::value::PublicValue::Text(tier),
                    crate::value::PublicValue::Nat64(score),
                ) = (
                    key.as_public(),
                    name.as_public(),
                    tier.as_public(),
                    score.as_public(),
                )
                else {
                    panic!("accepted-default row should retain its accepted shape");
                };
                let note = match note.as_public() {
                    crate::value::PublicValue::Null => None,
                    crate::value::PublicValue::Text(note) => Some(note.clone()),
                    _ => panic!("default mutation note should be text or null"),
                };
                MutationRow::accepted_default(*key, name.clone(), tier.clone(), *score, note)
            }
            _ => panic!("mutation state row disagreed with its accepted profile"),
        })
        .collect()
}

fn sqlite_result_from_icydb(
    case: &GeneratedSelectCase,
    result: SqlStatementResult,
) -> Result<SqliteReferenceResult, ()> {
    let (columns, rows) = match result {
        SqlStatementResult::Projection { columns, rows, .. } => (columns, rows),
        SqlStatementResult::Grouped { columns, rows, .. } => (
            columns,
            rows.into_iter()
                .map(|row| {
                    row.group_key()
                        .iter()
                        .chain(row.aggregate_values())
                        .cloned()
                        .collect()
                })
                .collect(),
        ),
        _ => return Err(()),
    };
    let rows = rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(sqlite_value_from_output)
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    SqliteReferenceResult::try_new(
        columns,
        rows,
        match case.query().result_order() {
            SelectResultOrder::Ordered => SqliteReferenceRowOrder::Ordered,
            SelectResultOrder::Unordered => SqliteReferenceRowOrder::Unordered,
        },
    )
    .map_err(|_| ())
}

fn sqlite_value_from_output(value: OutputValue) -> Result<SqliteReferenceValue, ()> {
    match value.into_public() {
        crate::value::PublicValue::Bool(value) => Ok(SqliteReferenceValue::Boolean(value)),
        crate::value::PublicValue::Decimal(value) => Ok(SqliteReferenceValue::Decimal {
            mantissa: value.mantissa(),
            scale: value.scale(),
        }),
        crate::value::PublicValue::Int64(value) => Ok(SqliteReferenceValue::Integer(value)),
        crate::value::PublicValue::Nat64(value) => i64::try_from(value)
            .map(SqliteReferenceValue::Integer)
            .map_err(|_| ()),
        crate::value::PublicValue::Null => Ok(SqliteReferenceValue::Null),
        crate::value::PublicValue::Text(value) => Ok(SqliteReferenceValue::Text(value)),
        _ => Err(()),
    }
}

const fn sql_result_row_count(result: &SqlStatementResult) -> Option<u32> {
    match result {
        SqlStatementResult::Count { row_count }
        | SqlStatementResult::Projection { row_count, .. }
        | SqlStatementResult::Grouped { row_count, .. } => Some(*row_count),
        _ => None,
    }
}

fn planned_select_execution_facts(
    session: &DbSession<TestCanister>,
    sql: &str,
) -> ObservedExecutionFacts {
    let (context, _, _, _) = session
        .compile_sql_query_with_execution_context(sql)
        .unwrap_or_else(|error| {
            panic!("accepted scheduled SELECT should compile: sql={sql:?} error={error:?}")
        });
    let query = match context.command() {
        crate::db::session::sql::CompiledSqlCommand::Select { query, .. } => query.as_ref(),
        crate::db::session::sql::CompiledSqlCommand::GlobalAggregate { command, .. } => {
            command.query()
        }
        _ => panic!("scheduled SELECT should compile to a query command"),
    };
    let authority = context
        .accepted_authority()
        .cloned()
        .or_else(|| Some(context.accepted_catalog().accepted_entity_authority()))
        .expect("scheduled SELECT should resolve accepted authority");
    let plan = session
        .sql_select_prepared_plan_for_tests(query, authority, context.accepted_schema())
        .expect("scheduled SELECT should produce one prepared plan");
    let access_shape = plan.logical_plan().access_shape_facts();
    let access = if access_shape
        .single_path_index_prefix_details()
        .is_some_and(|details| details.key_arity() > 1)
    {
        ExecutionAccess::CompositePrefix
    } else if access_shape.has_selected_index_access_path() {
        ExecutionAccess::SecondaryRange
    } else {
        ExecutionAccess::FullScan
    };
    let covering = if plan.has_projection_covering_read_plan_for_tests() {
        ExecutionCovering::Pure
    } else if plan.has_hybrid_covering_read_plan_for_tests() {
        ExecutionCovering::Hybrid
    } else {
        ExecutionCovering::NonCovering
    };
    ObservedExecutionFacts::new(access, covering)
}

const fn select_field_kind(kind: SelectFieldKind) -> AcceptedFieldKind {
    match kind {
        SelectFieldKind::Blob => AcceptedFieldKind::Blob { max_len: None },
        SelectFieldKind::Boolean => AcceptedFieldKind::Bool,
        SelectFieldKind::Integer => AcceptedFieldKind::Int64,
        SelectFieldKind::Text => AcceptedFieldKind::Text { max_len: None },
        SelectFieldKind::Ulid => AcceptedFieldKind::Ulid,
    }
}

const fn mutation_field_kind(kind: MutationFieldKind) -> AcceptedFieldKind {
    match kind {
        MutationFieldKind::Text => AcceptedFieldKind::Text { max_len: None },
        MutationFieldKind::UnsignedInteger => AcceptedFieldKind::Nat64,
    }
}

fn declared_scenarios() -> Vec<String> {
    let mut declared = Vec::new();
    for witness in scheduled_select_witnesses().expect("reviewed SELECT schedule should decode") {
        for root_seed in TIER_C_ROOT_SEEDS {
            for repetition in 0..TIER_C_SELECT_REPETITIONS {
                declared.push(
                    generate_scheduled_select_case(
                        &witness,
                        *root_seed,
                        repetition,
                        TIER_C_SELECT_BUDGETS,
                    )
                    .expect("scheduled SELECT should generate")
                    .identity()
                    .id()
                    .to_string(),
                );
            }
        }
    }
    for witness in scheduled_mutation_witnesses().expect("reviewed mutation schedule should decode")
    {
        for root_seed in TIER_C_ROOT_SEEDS {
            for repetition in 0..TIER_C_MUTATION_REPETITIONS {
                declared.push(
                    generate_scheduled_mutation_sequence(
                        &witness,
                        *root_seed,
                        repetition,
                        TIER_C_MUTATION_BUDGETS,
                    )
                    .expect("scheduled mutation should generate")
                    .identity()
                    .id()
                    .to_string(),
                );
            }
        }
    }
    declared.sort();
    declared
}

fn observation(
    scenario_id: &str,
    signature: icydb_testing_sql_generator::StructuralSignature,
    execution_facts: ObservedExecutionFacts,
    outcome: TierCScenarioOutcome,
) -> TierCScenarioObservation {
    TierCScenarioObservation::try_new(scenario_id, signature, execution_facts, outcome)
        .expect("native Tier C observation should validate")
}

const fn not_applicable_required_facts() -> RequiredExecutionFacts {
    RequiredExecutionFacts::new(
        ExecutionAccess::NotApplicable,
        ExecutionCovering::NotApplicable,
    )
}

fn required_shard_index() -> u8 {
    let raw =
        env::var("ICYDB_SQL_TIER_C_SHARD_INDEX").expect("ICYDB_SQL_TIER_C_SHARD_INDEX must be set");
    let shard = raw
        .parse::<u8>()
        .expect("Tier C shard index must be an unsigned integer");
    assert!(
        shard < SQL_SCHEDULED_SHARD_COUNT,
        "Tier C shard index must be from 0 through 7",
    );
    shard
}

fn required_artifact_dir() -> PathBuf {
    env::var("ICYDB_SQL_TIER_C_ARTIFACT_DIR")
        .map(PathBuf::from)
        .expect("ICYDB_SQL_TIER_C_ARTIFACT_DIR must be set")
}

fn shard_path(artifact_dir: &FsPath, shard_index: u8) -> PathBuf {
    artifact_dir.join(format!("{SHARD_FILE_PREFIX}{shard_index}.json"))
}

fn write_artifact(path: PathBuf, bytes: Vec<u8>) {
    let parent = path.parent().expect("Tier C artifact should have a parent");
    fs::create_dir_all(parent)
        .unwrap_or_else(|error| panic!("failed to create {}: {error}", parent.display()));
    fs::write(&path, bytes)
        .unwrap_or_else(|error| panic!("failed to write {}: {error}", path.display()));
}
