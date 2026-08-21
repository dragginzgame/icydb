//! End-to-end proof for the bounded exact-cardinality planner tie-break.

use crate::{
    db::{
        DbSession, DynamicQuery, DynamicStructuralPatch, DynamicWriteCell, FieldRef, FilterExpr,
        SqlStatementResult, asc,
        commit::database_incarnation_id,
        data::DataStore,
        index::IndexStore,
        journal::JournalTailStore,
        registry::{
            StoreAllocationIdentities, StoreAllocationIdentity, StoreRegistry,
            StoreRuntimeStorageCapabilities, exact_prefix_evidence_call_counts_for_tests,
            reset_exact_prefix_evidence_call_counts_for_tests,
        },
        schema::{
            AcceptedFieldKind, AcceptedSchemaRevision, CandidateSchemaRevision, FieldId,
            FieldStorageDecode, PersistedFieldSnapshot, PersistedIndexFieldPathSnapshot,
            PersistedIndexKeySnapshot, PersistedIndexSnapshot, PersistedSchemaSnapshot,
            SchemaFieldSlot, SchemaIndexId, SchemaInsertDefault, SchemaRowLayout, SchemaStore,
            SchemaVersion, accepted_schema_candidate_with_field_bindings_for_tests,
            cardinality_build::{
                CardinalityBuildAuthority, CardinalityGenerationPageOutcome,
                drive_cardinality_generation_page,
            },
        },
    },
    testing::test_memory,
    traits::{CanisterKind, Path},
    types::EntityTag,
    value::{InputValue, OutputValue},
};
use icydb_schema::FieldSourceKey;
use std::{cell::RefCell, collections::BTreeMap};

const STORE_PATH: &str = "db::session::tests::cardinality_tiebreak::Store";
const ENTITY_SOURCE: &str = "db::session::tests::cardinality_tiebreak::PlannerRow";
const ENTITY_NAME: &str = "PlannerRow";
const ENTITY_TAG: EntityTag = EntityTag::new(219);
const JOURNALED_STORE_PATH: &str = "db::session::tests::cardinality_tiebreak::JournaledStore";

struct TestCanister;

impl Path for TestCanister {
    const PATH: &'static str = "db::session::tests::cardinality_tiebreak::Canister";
}

impl CanisterKind for TestCanister {
    const COMMIT_MEMORY_ID: u8 = 162;
    const COMMIT_STABLE_KEY: &'static str = "icydb.test.cardinality-tiebreak.commit.v1";
    const STARTUP_MEMORY_ID: u8 = 163;
    const STARTUP_STABLE_KEY: &'static str = "icydb.test.cardinality-tiebreak.startup.v1";
    const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 164;
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
        "icydb.test.cardinality-tiebreak.integrity.v1";
}

struct JournaledTestCanister;

impl Path for JournaledTestCanister {
    const PATH: &'static str = "db::session::tests::cardinality_tiebreak::JournaledCanister";
}

impl CanisterKind for JournaledTestCanister {
    const COMMIT_MEMORY_ID: u8 = 159;
    const COMMIT_STABLE_KEY: &'static str = "icydb.cardinality_tie.commit.v1";
    const STARTUP_MEMORY_ID: u8 = 161;
    const STARTUP_STABLE_KEY: &'static str = "icydb.cardinality_tie.startup.v1";
    const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 160;
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str = "icydb.cardinality_tie.integrity.v1";
}

thread_local! {
    static DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
    static INDEX_STORE: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
    static SCHEMA_STORE: RefCell<SchemaStore> = const { RefCell::new(SchemaStore::init_heap()) };
    static STORE_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_store(
            STORE_PATH,
            &DATA_STORE,
            &INDEX_STORE,
            &SCHEMA_STORE,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
        ).expect("cardinality tie-break store should register");
        registry
    };
    static JOURNALED_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(155)));
    static JOURNALED_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(156)));
    static JOURNALED_SCHEMA_STORE: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(157)));
    static JOURNALED_TAIL_STORE: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(158)));
    static JOURNALED_STORE_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_journaled_store(
            JOURNALED_STORE_PATH,
            &JOURNALED_DATA_STORE,
            &JOURNALED_INDEX_STORE,
            &JOURNALED_SCHEMA_STORE,
            &JOURNALED_TAIL_STORE,
            StoreAllocationIdentities::new_journaled(
                StoreAllocationIdentity::new(155, "icydb.cardinality_tie.data.v1"),
                StoreAllocationIdentity::new(156, "icydb.cardinality_tie.index.v1"),
                StoreAllocationIdentity::new(157, "icydb.cardinality_tie.schema.v1"),
                StoreAllocationIdentity::new(158, "icydb.cardinality_tie.journal.v1"),
            ),
            StoreRuntimeStorageCapabilities::journaled(),
        ).expect("journaled cardinality tie-break store should register");
        registry
    };
}

#[test]
fn exact_cardinality_resolves_only_final_ties_and_cached_selection_is_advisory() {
    let session = initialize();
    seed_rows(&session);

    reset_exact_prefix_evidence_call_counts_for_tests();
    projection_rows(
        &session,
        "SELECT id FROM PlannerRow WHERE rare = 'group-a' ORDER BY id LIMIT 20",
    );
    assert_eq!(
        exact_prefix_evidence_call_counts_for_tests(),
        (0, 0),
        "a query without a final tied set must not touch cardinality evidence",
    );

    reset_exact_prefix_evidence_call_counts_for_tests();
    let cold = explain(&session, "common = 'everyone' AND rare = 'group-a'");
    assert!(cold.contains("z_rare_idx"), "{cold}");
    assert!(cold.contains("exact_cardinality_tiebreak"), "{cold}");
    assert!(
        cold.contains("cardinality_evidence: exact_at_selection"),
        "{cold}"
    );
    assert!(cold.contains("exact_prefix_entries: 12"), "{cold}");
    assert!(cold.contains("exact_prefix_entries: 6"), "{cold}");
    let (cold_probes, cold_lifecycle_reads) = exact_prefix_evidence_call_counts_for_tests();
    assert!(cold_probes > 0);
    assert_eq!(cold_lifecycle_reads, 0);

    reset_exact_prefix_evidence_call_counts_for_tests();
    let structured = explain_json(&session, "common = 'everyone' AND rare = 'group-a'");
    assert!(
        structured.contains("\"reason\":\"exact_cardinality_tiebreak\""),
        "{structured}"
    );
    assert!(
        structured.contains("\"cardinality_evidence_state\":\"exact_at_selection\""),
        "{structured}"
    );
    assert_eq!(exact_prefix_evidence_call_counts_for_tests(), (0, 0));

    reset_exact_prefix_evidence_call_counts_for_tests();
    let rows = projection_rows(
        &session,
        "SELECT id FROM PlannerRow \
         WHERE common = 'everyone' AND rare = 'group-a' ORDER BY id LIMIT 20",
    );
    assert_eq!(rows.len(), 6);
    assert_eq!(exact_prefix_evidence_call_counts_for_tests(), (0, 0));

    for id in 100..110 {
        insert_row(&session, id, "other", "group-a");
    }
    reset_exact_prefix_evidence_call_counts_for_tests();
    let stale = explain(&session, "common = 'everyone' AND rare = 'group-a'");
    assert!(stale.contains("IndexPrefix(z_rare_idx)"), "{stale}");
    assert_eq!(
        exact_prefix_evidence_call_counts_for_tests(),
        (0, 0),
        "ordinary writes must not refresh a now-cardinality-stale cached plan",
    );

    reset_exact_prefix_evidence_call_counts_for_tests();
    let rebound = explain(&session, "common = 'other' AND rare = 'group-a'");
    assert!(rebound.contains("IndexPrefix(a_common_idx)"), "{rebound}");
    assert_eq!(exact_prefix_evidence_call_counts_for_tests(), (1, 0));
}

#[test]
fn exact_cardinality_supports_multi_lookup_and_branch_set_but_excludes_range_and_grouped() {
    let session = initialize();
    seed_rows(&session);

    reset_exact_prefix_evidence_call_counts_for_tests();
    let multi = explain(&session, "common IN ('everyone', 'absent')");
    assert!(multi.contains("IndexMultiLookup(a_common_idx)"), "{multi}");
    assert!(
        multi.contains("cardinality_evidence: exact_at_selection"),
        "{multi}"
    );
    assert_eq!(exact_prefix_evidence_call_counts_for_tests(), (1, 0));

    reset_exact_prefix_evidence_call_counts_for_tests();
    let branch = explain(
        &session,
        "wide_fixed = 'all' AND wide_branch IN ('x', 'y') \
         AND selective_fixed = 'target' AND selective_branch IN ('x', 'y')",
    );
    assert!(
        branch.contains("IndexBranchSet(y_selective_branch_idx)"),
        "{branch}"
    );
    assert!(branch.contains("exact_cardinality_tiebreak"), "{branch}");
    assert_eq!(exact_prefix_evidence_call_counts_for_tests(), (1, 0));

    reset_exact_prefix_evidence_call_counts_for_tests();
    projection_rows(
        &session,
        "SELECT id FROM PlannerRow WHERE common >= 'everyone' ORDER BY id LIMIT 20",
    );
    assert_eq!(exact_prefix_evidence_call_counts_for_tests(), (0, 0));

    reset_exact_prefix_evidence_call_counts_for_tests();
    session
        .execute_trusted_sql_query(
            "SELECT common, COUNT(*) FROM PlannerRow \
             WHERE common = 'everyone' AND rare = 'group-a' GROUP BY common LIMIT 20",
        )
        .expect("grouped exclusion fixture should execute");
    assert_eq!(exact_prefix_evidence_call_counts_for_tests(), (0, 0));
}

#[test]
fn exact_cardinality_cursor_pin_resumes_without_fresh_evidence() {
    let session = initialize();
    seed_rows(&session);

    let query = selective_dynamic_query();
    let first = session
        .execute_trusted_live_page(&query, None)
        .expect("cardinality-selected dynamic page should execute");
    let mut continuation = first.continuation;
    assert!(
        continuation.is_some(),
        "six matching rows should produce a bounded test continuation"
    );
    reset_exact_prefix_evidence_call_counts_for_tests();
    let mut row_count = first.row_count;
    for _ in 0..8 {
        let Some(cursor) = continuation.take() else {
            break;
        };
        let page = session
            .execute_trusted_live_page(&query, Some(cursor.as_str()))
            .expect("authenticated pinned continuation should resume");
        row_count += page.row_count;
        continuation = page.continuation;
    }
    assert_eq!(row_count, 3);
    assert!(continuation.is_none());
    assert_eq!(
        exact_prefix_evidence_call_counts_for_tests(),
        (0, 0),
        "a pinned continuation must not consult current cardinality",
    );
}

#[test]
fn unavailable_fallback_refreshes_only_on_lifecycle_change_and_keeps_cursor_route() {
    let session = initialize_journaled();
    seed_rows(&session);

    reset_exact_prefix_evidence_call_counts_for_tests();
    let unavailable = explain(&session, "common = 'everyone' AND rare = 'group-a'");
    assert!(
        unavailable.contains("IndexPrefix(a_common_idx)"),
        "{unavailable}"
    );
    assert!(
        unavailable.contains("cardinality_evidence: unavailable"),
        "{unavailable}"
    );
    assert_eq!(exact_prefix_evidence_call_counts_for_tests(), (1, 1));

    reset_exact_prefix_evidence_call_counts_for_tests();
    projection_rows(
        &session,
        "SELECT id FROM PlannerRow WHERE common = 'everyone' \
         AND rare = 'group-a' ORDER BY id LIMIT 20",
    );
    assert_eq!(exact_prefix_evidence_call_counts_for_tests(), (0, 1));

    let query = selective_dynamic_query();
    let first = session
        .execute_trusted_live_page(&query, None)
        .expect("unavailable fallback cursor should start");
    let cursor = first
        .continuation
        .expect("bounded unavailable fallback should continue");
    drive_journaled_cardinality_to_ready(&session);

    reset_exact_prefix_evidence_call_counts_for_tests();
    session
        .execute_trusted_live_page(&query, Some(cursor.as_str()))
        .expect("issued fallback cursor should retain its pinned route");
    assert_eq!(exact_prefix_evidence_call_counts_for_tests(), (0, 0));

    reset_exact_prefix_evidence_call_counts_for_tests();
    projection_rows(
        &session,
        "SELECT id FROM PlannerRow WHERE common = 'everyone' \
         AND rare = 'group-a' ORDER BY id LIMIT 20",
    );
    assert_eq!(
        exact_prefix_evidence_call_counts_for_tests(),
        (1, 1),
        "a compiled SQL fallback must re-enter the shared cache after Ready publication",
    );

    reset_exact_prefix_evidence_call_counts_for_tests();
    let ready = explain(&session, "common = 'everyone' AND rare = 'group-a'");
    assert!(ready.contains("IndexPrefix(z_rare_idx)"), "{ready}");
    assert!(
        ready.contains("cardinality_evidence: exact_at_selection"),
        "{ready}"
    );
    assert_eq!(exact_prefix_evidence_call_counts_for_tests(), (0, 0));
}

fn selective_dynamic_query() -> DynamicQuery {
    DynamicQuery::new(ENTITY_NAME)
        .filter(FilterExpr::and(vec![
            FieldRef::new("common").eq(InputValue::Text("everyone".to_string())),
            FieldRef::new("rare").eq(InputValue::Text("group-a".to_string())),
        ]))
        .select(["id"])
        .order_by(asc("id"))
        .limit(3)
}

fn initialize() -> DbSession<TestCanister> {
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    let session = DbSession::new(
        &STORE_REGISTRY,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    );
    let candidate = schema_candidate(STORE_PATH);
    session
        .db
        .drive_startup_recovery_page()
        .expect("cardinality tie-break database should initialize");
    let store = session
        .db
        .store_handle(STORE_PATH)
        .expect("cardinality tie-break store should resolve");
    crate::db::commit::publish_accepted_schema_candidate(
        STORE_PATH,
        store,
        AcceptedSchemaRevision::NONE,
        &candidate,
    )
    .expect("cardinality tie-break schema should publish");

    session
}

fn initialize_journaled() -> DbSession<JournaledTestCanister> {
    let session = DbSession::new(
        &JOURNALED_STORE_REGISTRY,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    );
    let recovered = (0..8).any(|_| {
        session
            .db
            .drive_startup_recovery_page()
            .expect("journaled cardinality recovery should advance")
    });
    assert!(
        recovered,
        "journaled cardinality recovery should complete within its test bound"
    );
    let store = session
        .db
        .store_handle(JOURNALED_STORE_PATH)
        .expect("journaled cardinality store should resolve");
    crate::db::commit::publish_accepted_schema_candidate(
        JOURNALED_STORE_PATH,
        store,
        AcceptedSchemaRevision::NONE,
        &schema_candidate(JOURNALED_STORE_PATH),
    )
    .expect("journaled cardinality schema should publish");

    session
}

fn drive_journaled_cardinality_to_ready(session: &DbSession<JournaledTestCanister>) {
    let store = session
        .db
        .store_handle(JOURNALED_STORE_PATH)
        .expect("journaled cardinality store should resolve");
    for _ in 0..16 {
        let outcome = store
            .with_data(|data| {
                store.with_index(|index| {
                    store.with_schema_mut(|schema| {
                        drive_cardinality_generation_page(data, index, schema, |schema| {
                            let watermark =
                                JOURNALED_TAIL_STORE.with(|tail| tail.borrow().fold_watermark())?;
                            CardinalityBuildAuthority::derive(
                                schema,
                                database_incarnation_id()?,
                                store.allocation_identities(),
                                watermark,
                            )
                        })
                    })
                })
            })
            .expect("journaled cardinality generation should advance");
        if outcome == CardinalityGenerationPageOutcome::Quiescent {
            return;
        }
    }
    panic!("journaled cardinality generation should become Ready");
}

fn schema_candidate(store_path: &str) -> CandidateSchemaRevision {
    let fields = vec![
        field(1, "id", 0, AcceptedFieldKind::Nat64),
        field(2, "common", 1, AcceptedFieldKind::Text { max_len: None }),
        field(3, "rare", 2, AcceptedFieldKind::Text { max_len: None }),
        field(
            4,
            "wide_fixed",
            3,
            AcceptedFieldKind::Text { max_len: None },
        ),
        field(
            5,
            "wide_branch",
            4,
            AcceptedFieldKind::Text { max_len: None },
        ),
        field(
            6,
            "selective_fixed",
            5,
            AcceptedFieldKind::Text { max_len: None },
        ),
        field(
            7,
            "selective_branch",
            6,
            AcceptedFieldKind::Text { max_len: None },
        ),
    ];
    let indexes = vec![
        index(store_path, 1, 1, "a_common_idx", 2, 1, "common"),
        index(store_path, 2, 2, "z_rare_idx", 3, 2, "rare"),
        index(store_path, 3, 3, "m_common_dup_idx", 2, 1, "common"),
        composite_index(
            store_path,
            4,
            4,
            "b_wide_branch_idx",
            [(4, 3, "wide_fixed"), (5, 4, "wide_branch")],
        ),
        composite_index(
            store_path,
            5,
            5,
            "y_selective_branch_idx",
            [(6, 5, "selective_fixed"), (7, 6, "selective_branch")],
        ),
    ];
    let snapshot = PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        ENTITY_SOURCE.to_string(),
        ENTITY_NAME.to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
        indexes,
    );
    let bindings = BTreeMap::from([
        ((ENTITY_TAG, field_source("id")), FieldId::new(1)),
        ((ENTITY_TAG, field_source("common")), FieldId::new(2)),
        ((ENTITY_TAG, field_source("rare")), FieldId::new(3)),
        ((ENTITY_TAG, field_source("wide_fixed")), FieldId::new(4)),
        ((ENTITY_TAG, field_source("wide_branch")), FieldId::new(5)),
        (
            (ENTITY_TAG, field_source("selective_fixed")),
            FieldId::new(6),
        ),
        (
            (ENTITY_TAG, field_source("selective_branch")),
            FieldId::new(7),
        ),
    ]);
    accepted_schema_candidate_with_field_bindings_for_tests(
        store_path,
        AcceptedSchemaRevision::INITIAL,
        BTreeMap::from([(ENTITY_TAG, snapshot)]),
        bindings,
    )
}

fn seed_rows<C: CanisterKind>(session: &DbSession<C>) {
    let rows: Vec<_> = (0..12)
        .map(|id| {
            let rare = if id < 6 { "group-a" } else { "group-b" };
            row(id, "everyone", rare)
        })
        .collect();
    for chunk in rows.chunks(4) {
        session
            .execute_trusted_dynamic_insert_batch(ENTITY_NAME, chunk.to_vec())
            .expect("cardinality fixture rows should insert");
    }
}

fn insert_row<C: CanisterKind>(session: &DbSession<C>, id: u64, common: &str, rare: &str) {
    session
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![row(id, common, rare)])
        .expect("cardinality fixture row should insert");
}

fn row(id: u64, common: &str, rare: &str) -> DynamicStructuralPatch {
    let branch = if id.is_multiple_of(2) { "x" } else { "y" };
    let selective_fixed = if rare == "group-a" { "target" } else { "other" };
    DynamicStructuralPatch::new(vec![
        (
            "id".to_string(),
            DynamicWriteCell::Value(InputValue::Nat64(id)),
        ),
        (
            "common".to_string(),
            DynamicWriteCell::Value(InputValue::Text(common.to_string())),
        ),
        (
            "rare".to_string(),
            DynamicWriteCell::Value(InputValue::Text(rare.to_string())),
        ),
        (
            "wide_fixed".to_string(),
            DynamicWriteCell::Value(InputValue::Text("all".to_string())),
        ),
        (
            "wide_branch".to_string(),
            DynamicWriteCell::Value(InputValue::Text(branch.to_string())),
        ),
        (
            "selective_fixed".to_string(),
            DynamicWriteCell::Value(InputValue::Text(selective_fixed.to_string())),
        ),
        (
            "selective_branch".to_string(),
            DynamicWriteCell::Value(InputValue::Text(branch.to_string())),
        ),
    ])
}

fn explain<C: CanisterKind>(session: &DbSession<C>, predicate: &str) -> String {
    let sql = format!(
        "EXPLAIN EXECUTION VERBOSE SELECT id FROM PlannerRow \
         WHERE {predicate} ORDER BY id LIMIT 20"
    );
    let SqlStatementResult::Explain(explain) = session
        .execute_trusted_sql_query(sql.as_str())
        .expect("cardinality fixture should explain")
    else {
        panic!("EXPLAIN EXECUTION VERBOSE should return an explain payload");
    };

    explain
}

fn explain_json<C: CanisterKind>(session: &DbSession<C>, predicate: &str) -> String {
    let sql =
        format!("EXPLAIN JSON SELECT id FROM PlannerRow WHERE {predicate} ORDER BY id LIMIT 20");
    let SqlStatementResult::Explain(explain) = session
        .execute_trusted_sql_query(sql.as_str())
        .expect("cardinality fixture should explain as JSON")
    else {
        panic!("EXPLAIN JSON should return an explain payload");
    };

    explain
}

fn projection_rows<C: CanisterKind>(session: &DbSession<C>, sql: &str) -> Vec<Vec<OutputValue>> {
    let SqlStatementResult::Projection { rows, .. } = session
        .execute_trusted_sql_query(sql)
        .expect("cardinality fixture query should execute")
    else {
        panic!("cardinality fixture query should return rows");
    };
    rows
}

fn field(id: u32, name: &str, slot: u16, kind: AcceptedFieldKind) -> PersistedFieldSnapshot {
    let storage_decode = FieldStorageDecode::ByKind;
    let leaf_codec = kind.leaf_codec_for_storage(storage_decode);
    PersistedFieldSnapshot::new_initial(
        FieldId::new(id),
        name.to_string(),
        SchemaFieldSlot::new(slot),
        kind,
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        storage_decode,
        leaf_codec,
    )
}

fn index(
    store_path: &str,
    id: u32,
    ordinal: u16,
    name: &str,
    field_id: u32,
    slot: u16,
    field_name: &str,
) -> PersistedIndexSnapshot {
    PersistedIndexSnapshot::new(
        SchemaIndexId::new(id).expect("test index identity should be nonzero"),
        ordinal,
        name.to_string(),
        store_path.to_string(),
        false,
        PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
            FieldId::new(field_id),
            SchemaFieldSlot::new(slot),
            vec![field_name.to_string()],
            AcceptedFieldKind::Text { max_len: None },
            false,
        )]),
        None,
    )
}

fn composite_index(
    store_path: &str,
    id: u32,
    ordinal: u16,
    name: &str,
    fields: [(u32, u16, &str); 2],
) -> PersistedIndexSnapshot {
    let paths = fields
        .into_iter()
        .map(|(field_id, slot, field_name)| {
            PersistedIndexFieldPathSnapshot::new(
                FieldId::new(field_id),
                SchemaFieldSlot::new(slot),
                vec![field_name.to_string()],
                AcceptedFieldKind::Text { max_len: None },
                false,
            )
        })
        .collect();
    PersistedIndexSnapshot::new(
        SchemaIndexId::new(id).expect("test index identity should be nonzero"),
        ordinal,
        name.to_string(),
        store_path.to_string(),
        false,
        PersistedIndexKeySnapshot::FieldPath(paths),
        None,
    )
}

fn field_source(field: &str) -> FieldSourceKey {
    FieldSourceKey::try_new(format!("{ENTITY_SOURCE}::{field}"))
        .expect("cardinality fixture field source should admit")
}
