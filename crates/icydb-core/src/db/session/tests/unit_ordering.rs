//! End-to-end accepted-schema coverage for the trivial total order of `Unit`.

use crate::{
    db::{
        DbSession, DynamicQuery, DynamicStructuralPatch, DynamicTypedEntityBinding,
        DynamicTypedFieldBindingRequest, DynamicTypedFieldType, DynamicWriteCell, FieldRef,
        FilterExpr, QueryError, QueryExecutionError, SqlStatementResult, asc,
        data::{DataStore, DecodedDataStoreKey},
        index::IndexStore,
        registry::{StoreAllocationIdentities, StoreRegistry, StoreRuntimeStorageCapabilities},
        schema::{
            AcceptedFieldKind, AcceptedSchemaRevision, AcceptedSchemaStoreObservationCounts,
            FieldId, FieldStorageDecode, PersistedFieldSnapshot, PersistedIndexFieldPathSnapshot,
            PersistedIndexKeySnapshot, PersistedIndexSnapshot, PersistedSchemaSnapshot,
            SchemaFieldSlot, SchemaIndexId, SchemaInsertDefault, SchemaRowLayout, SchemaStore,
            SchemaVersion, accepted_schema_candidate_for_tests,
            accepted_schema_candidate_with_field_bindings_for_tests,
            accepted_schema_snapshot_fingerprint_builds_for_tests,
            accepted_schema_store_observation_counts_for_tests,
            reset_accepted_schema_snapshot_fingerprint_builds_for_tests,
            reset_accepted_schema_store_observation_counts_for_tests,
        },
        session::{
            AcceptedSchemaRuntimeBuildCounts, accepted_schema_runtime_build_counts_for_tests,
            query::{
                shared_query_plan_cache_len_for_tests,
                shared_query_template_cache_entry_upper_bound_for_tests,
                shared_query_template_cache_len_for_tests,
            },
            reset_accepted_schema_runtime_build_counts_for_tests,
        },
    },
    error::ErrorOrigin,
    traits::{CanisterKind, Path},
    types::EntityTag,
    value::{InputValue, OutputValue, Value},
};
use icydb_diagnostic_code::{DiagnosticFactTag, QueryFieldRole};
use icydb_schema::{FieldSourceKey, ScalarType};
use std::{cell::RefCell, collections::BTreeMap};

const STORE_PATH: &str = "db::session::tests::unit_ordering::Store";
const ENTITY_SOURCE: &str = "db::session::tests::unit_ordering::Singleton";
const ENTITY_NAME: &str = "Singleton";
const ID_SOURCE: &str = "db::session::tests::unit_ordering::Singleton::id";
const LABEL_SOURCE: &str = "db::session::tests::unit_ordering::Singleton::label";
const ENTITY_TAG: EntityTag = EntityTag::new(220);
const MULTI_TARGET_STORE_PATH: &str = "db::session::tests::unit_ordering::TargetStore";
const MULTI_UNRELATED_STORE_PATH: &str = "db::session::tests::unit_ordering::UnrelatedStore";
const MULTI_TARGET_ENTITY_SOURCE: &str = "db::session::tests::unit_ordering::TargetEntity";
const MULTI_UNRELATED_ENTITY_SOURCE: &str = "db::session::tests::unit_ordering::UnrelatedEntity";
const MULTI_TARGET_ENTITY_NAME: &str = "TargetEntity";
const MULTI_UNRELATED_ENTITY_NAME: &str = "UnrelatedEntity";
const MULTI_TARGET_ENTITY_TAG: EntityTag = EntityTag::new(221);
const MULTI_UNRELATED_ENTITY_TAG: EntityTag = EntityTag::new(222);

struct TestCanister;

impl Path for TestCanister {
    const PATH: &'static str = "db::session::tests::unit_ordering::Canister";
}

impl CanisterKind for TestCanister {
    const COMMIT_MEMORY_ID: u8 = 220;
    const COMMIT_STABLE_KEY: &'static str = "icydb.test.unit_ordering.commit.v1";
    const STARTUP_MEMORY_ID: u8 = 222;
    const STARTUP_STABLE_KEY: &'static str = "icydb.test.unit_ordering.startup.control.v1";
    const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 221;
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
        "icydb.test.unit_ordering.integrity.progress.v1";
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
        ).expect("Unit ordering test store should register");
        registry
    };
    static MULTI_TARGET_DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
    static MULTI_TARGET_INDEX_STORE: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
    static MULTI_TARGET_SCHEMA_STORE: RefCell<SchemaStore> = const { RefCell::new(SchemaStore::init_heap()) };
    static MULTI_UNRELATED_DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
    static MULTI_UNRELATED_INDEX_STORE: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
    static MULTI_UNRELATED_SCHEMA_STORE: RefCell<SchemaStore> = const { RefCell::new(SchemaStore::init_heap()) };
    static MULTI_STORE_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_store(
            MULTI_TARGET_STORE_PATH,
            &MULTI_TARGET_DATA_STORE,
            &MULTI_TARGET_INDEX_STORE,
            &MULTI_TARGET_SCHEMA_STORE,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
        ).expect("target test store should register");
        registry.register_store(
            MULTI_UNRELATED_STORE_PATH,
            &MULTI_UNRELATED_DATA_STORE,
            &MULTI_UNRELATED_INDEX_STORE,
            &MULTI_UNRELATED_SCHEMA_STORE,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
        ).expect("unrelated test store should register");
        registry
    };
}

#[test]
fn rejected_sql_fields_keep_exact_role() {
    let session = initialize();

    let sql_cases = [
        (
            "SELECT missing FROM Singleton ORDER BY id LIMIT 1",
            QueryFieldRole::Projection,
            "missing",
        ),
        (
            "SELECT Singleton.missing FROM Singleton ORDER BY id LIMIT 1",
            QueryFieldRole::Projection,
            "missing",
        ),
        (
            "SELECT Other.missing FROM Singleton ORDER BY id LIMIT 1",
            QueryFieldRole::Projection,
            "Other.missing",
        ),
        (
            "SELECT label.missing FROM Singleton ORDER BY id LIMIT 1",
            QueryFieldRole::Projection,
            "label.missing",
        ),
        (
            "SELECT missing AS selected FROM Singleton ORDER BY selected LIMIT 1",
            QueryFieldRole::Projection,
            "missing",
        ),
        (
            "SELECT id FROM Singleton WHERE missing = 'x' ORDER BY id LIMIT 1",
            QueryFieldRole::Predicate,
            "missing",
        ),
        (
            "SELECT id FROM Singleton ORDER BY missing LIMIT 1",
            QueryFieldRole::OrderBy,
            "missing",
        ),
        (
            "SELECT label AS shown FROM Singleton ORDER BY missing LIMIT 1",
            QueryFieldRole::OrderBy,
            "missing",
        ),
        (
            "SELECT COUNT(*) FROM Singleton GROUP BY missing",
            QueryFieldRole::GroupBy,
            "missing",
        ),
        (
            "SELECT SUM(ABS(missing)) FROM Singleton",
            QueryFieldRole::AggregateTarget,
            "missing",
        ),
    ];
    for (sql, role, field) in sql_cases {
        let error = session
            .execute_trusted_sql_query(sql)
            .expect_err("unknown SQL field should fail before execution");
        assert_eq!(
            error.diagnostic_code(),
            icydb_diagnostic_code::DiagnosticCode::QueryPlan,
            "unexpected diagnostic code for {sql}",
        );
        assert_query_field(&error, role, field);
    }

    for sql in [
        "SELECT label AS missing FROM Singleton ORDER BY missing LIMIT 1",
        "SELECT label AS id FROM Singleton ORDER BY id LIMIT 1",
        "SELECT id AS duplicate, label AS duplicate FROM Singleton ORDER BY duplicate LIMIT 1",
    ] {
        session
            .execute_trusted_sql_query(sql)
            .expect("resolved aliases should not become rejected-field context");
    }

    let quoted_error = session
        .execute_trusted_sql_query("SELECT \"missing\" FROM Singleton ORDER BY id LIMIT 1")
        .expect_err("quoted identifiers are outside the maintained resolver context");
    assert_eq!(quoted_error.query_field_context(), None);
}

#[test]
fn direct_aggregate_and_having_fields_keep_exact_role() {
    let session = initialize();

    let cases = [
        "SELECT COUNT(missing) FROM Singleton",
        "SELECT SUM(missing) FROM Singleton",
        "SELECT AVG(missing) FROM Singleton",
        "SELECT MIN(missing) FROM Singleton",
        "SELECT MAX(missing) FROM Singleton",
    ];
    for sql in cases {
        let error = session
            .execute_trusted_sql_query(sql)
            .expect_err("unknown direct aggregate field should fail before execution");
        assert_query_field(&error, QueryFieldRole::AggregateTarget, "missing");
    }

    let error = session
        .execute_trusted_sql_query(
            "SELECT COUNT(*) FROM Singleton GROUP BY label HAVING missing = 'x'",
        )
        .expect_err("unknown HAVING field should fail before execution");
    assert_query_field(&error, QueryFieldRole::Having, "missing");
}

#[test]
fn rejected_dynamic_fields_keep_exact_role() {
    let session = initialize();

    let dynamic_predicate = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("missing").eq(InputValue::Text("x".to_string())))
        .select(["id"])
        .order_by(asc("id"))
        .limit(1);
    let predicate_error = session
        .execute_trusted_live_page(&dynamic_predicate, None)
        .expect_err("dynamic unknown predicate field should fail planning");
    assert_query_field(&predicate_error, QueryFieldRole::Predicate, "missing");

    let dynamic_order = DynamicQuery::new(ENTITY_NAME)
        .select(["id"])
        .order_by(asc("missing"))
        .limit(1);
    let order_error = session
        .execute_trusted_live_page(&dynamic_order, None)
        .expect_err("dynamic unknown order field should fail planning");
    assert_query_field(&order_error, QueryFieldRole::OrderBy, "missing");
    assert_eq!(
        order_error.diagnostic_facts(),
        vec![(DiagnosticFactTag::TermIndex, 0)]
    );
}

fn assert_query_field(error: &QueryError, role: QueryFieldRole, field: &str) {
    assert_eq!(
        error.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::QueryPlan
    );
    assert_eq!(error.query_field_context(), Some((role, field)));
}

#[test]
fn unit_primary_key_ordering_is_consistent_across_query_surfaces() {
    let session = initialize();
    seed_singleton(&session);

    assert_eq!(
        sql_rows(
            &session,
            "SELECT id, label FROM Singleton ORDER BY id LIMIT 100 OFFSET 0",
        ),
        vec![singleton_row()],
    );
    assert!(
        sql_rows(
            &session,
            "SELECT id, label FROM Singleton ORDER BY id LIMIT 100 OFFSET 1",
        )
        .is_empty(),
    );
    assert_eq!(
        sql_rows(
            &session,
            "SELECT id, label FROM Singleton ORDER BY label, id LIMIT 100",
        ),
        vec![singleton_row()],
    );

    // Ordering by the authored field relies on the planner appending the Unit
    // primary key as its deterministic tie-breaker.
    let tie_break_query = DynamicQuery::new(ENTITY_NAME)
        .select(["id", "label"])
        .order_by(asc("label"))
        .limit(100);
    let dynamic = session
        .execute_trusted_live_page(&tie_break_query, None)
        .expect("dynamic Unit primary-key tie-break should plan and execute");
    assert_eq!(dynamic.rows, vec![singleton_row()]);

    let primary_key_query = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("id").eq(InputValue::Unit))
        .select(["id", "label"])
        .order_by(asc("id"))
        .limit(100);
    let binding = session
        .issue_typed_entity_binding(
            ENTITY_SOURCE,
            &[
                DynamicTypedFieldBindingRequest::new(
                    ID_SOURCE.to_string(),
                    DynamicTypedFieldType::Scalar(ScalarType::Unit),
                    false,
                ),
                DynamicTypedFieldBindingRequest::new(
                    LABEL_SOURCE.to_string(),
                    DynamicTypedFieldType::Scalar(ScalarType::Text { max_len: None }),
                    false,
                ),
            ],
        )
        .expect("generated-style typed binding should resolve accepted Unit authority");
    let typed = session
        .execute_public_live_page_for_typed_binding(&binding, &primary_key_query, None)
        .expect("typed Unit primary-key tie-break should execute")
        .expect("typed binding should remain current");
    assert_eq!(typed.rows, vec![singleton_row()]);

    assert_unit_exact_key_batch(&session, &binding);

    for (filter, expected_rows) in [
        (FieldRef::new("id").eq(InputValue::Unit), 1),
        (FieldRef::new("id").lt(InputValue::Unit), 0),
        (FieldRef::new("id").lte(InputValue::Unit), 1),
        (FieldRef::new("id").gt(InputValue::Unit), 0),
        (FieldRef::new("id").gte(InputValue::Unit), 1),
    ] {
        let query = DynamicQuery::new(ENTITY_NAME)
            .filter(filter)
            .select(["id"])
            .order_by(asc("id"))
            .limit(100);
        let output = session
            .execute_trusted_live_page(&query, None)
            .expect("Unit comparison should plan and execute");

        assert_eq!(output.row_count, expected_rows);
    }
}

#[test]
fn accepted_index_missing_row_is_typed_store_corruption() {
    let session = initialize();
    seed_singleton(&session);
    let raw_key = DecodedDataStoreKey::try_from_structural_key(ENTITY_TAG, &Value::Unit)
        .expect("Unit data key should decode")
        .to_raw()
        .expect("Unit data key should encode");
    let store = session
        .db
        .store_handle(STORE_PATH)
        .expect("Unit ordering store should resolve");
    assert!(
        store.with_data_mut(|data| data.remove(&raw_key)).is_some(),
        "corruption fixture must remove the authoritative row only",
    );

    let primary_lookup = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("id").eq(InputValue::Unit))
        .select(["id"]);
    assert!(
        session
            .execute_trusted_live_page(&primary_lookup, None)
            .expect("an ordinary missing primary lookup keeps Ignore semantics")
            .rows
            .is_empty(),
    );

    let mixed_union = DynamicQuery::new(ENTITY_NAME)
        .filter(FilterExpr::or(vec![
            FieldRef::new("id").eq(InputValue::Unit),
            FieldRef::new("label").eq(InputValue::Text("other".to_string())),
        ]))
        .select(["id"]);
    assert!(
        session
            .execute_trusted_live_page(&mixed_union, None)
            .expect("a missing exact-key OR branch remains an ordinary absent row")
            .rows
            .is_empty(),
    );

    let query = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("label").eq(InputValue::Text("singleton".to_string())))
        .select(["id", "label"])
        .order_by(asc("label"));
    let error = session
        .execute_trusted_live_page(&query, None)
        .expect_err("an accepted index must not hide its missing row");
    assert_store_corruption(error);

    let error = session
        .execute_trusted_sql_query(
            "SELECT id FROM Singleton WHERE label = 'singleton' ORDER BY label LIMIT 100",
        )
        .expect_err("a covering accepted index must still prove row presence");
    assert_store_corruption(error);
}

fn assert_store_corruption(error: QueryError) {
    let QueryError::Execute(QueryExecutionError::Corruption(error)) = error else {
        panic!("missing accepted-index row should retain corruption taxonomy");
    };
    assert_eq!(error.origin(), ErrorOrigin::Store);
}

#[test]
fn accepted_entity_display_name_lookup_is_case_insensitive() {
    let session = initialize();
    seed_singleton(&session);

    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some("sInGlEtOn"))
        .expect("mixed-case display name should resolve accepted authority");
    assert_eq!(catalog.snapshot().entity_name(), ENTITY_NAME);
    assert_eq!(
        sql_rows(
            &session,
            "SELECT id, label FROM singleton ORDER BY id LIMIT 100",
        ),
        vec![singleton_row()],
    );

    let SqlStatementResult::Describe(crate::db::SqlDescribeOutput::Verbose { description }) =
        session
            .execute_trusted_sql_query("DESCRIBE public.singleton VERBOSE")
            .expect("lowercase DESCRIBE should resolve accepted authority")
    else {
        panic!("DESCRIBE should return accepted schema metadata");
    };
    assert_eq!(description.entity_name(), ENTITY_NAME);

    assert!(
        session
            .find_accepted_schema_catalog_context_for_entity_source_key(
                &ENTITY_SOURCE.to_ascii_lowercase(),
            )
            .expect("source-key lookup should remain valid")
            .is_none(),
        "immutable source-key lookup must remain exact",
    );
}

#[test]
fn missing_describe_entity_reports_accepted_schema_not_found() {
    let session = initialize();

    let error = session
        .execute_trusted_sql_query("DESCRIBE Card")
        .expect_err("a missing DESCRIBE target should fail");

    assert_eq!(
        error.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeNotFound,
    );
    assert_eq!(
        error.diagnostic().detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::SqlQueryEntityNotFound,
        },),
    );
}

fn assert_unit_exact_key_batch(
    session: &DbSession<TestCanister>,
    binding: &DynamicTypedEntityBinding,
) {
    let gets_before = DataStore::current_get_call_count();
    let cached_plans_before = shared_query_plan_cache_len_for_tests(session.db.cache_scope_id());
    let exact = session
        .execute_public_exact_key_batch_for_typed_binding(binding, &[(), ()])
        .expect("typed Unit exact-key batch should execute")
        .expect("typed binding should remain current");
    assert_eq!(exact.positions, vec![0, 0]);
    assert_eq!(exact.distinct_rows, vec![Some(singleton_row())]);
    assert_eq!(
        DataStore::current_get_call_count().saturating_sub(gets_before),
        1,
        "duplicate input positions must share one physical row read",
    );
    assert_eq!(
        shared_query_plan_cache_len_for_tests(session.db.cache_scope_id()),
        cached_plans_before,
        "exact-key reads must not populate the general query-plan cache",
    );

    let too_many = vec![(); crate::db::MAX_TYPED_EXACT_KEY_BATCH_ITEMS + 1];
    let over_bound = session
        .execute_public_exact_key_batch_for_typed_binding(binding, too_many.as_slice())
        .expect_err("exact-key count cap plus one should reject before store access");
    assert!(matches!(
        over_bound.diagnostic().detail(),
        Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::ExactKeyBatchTooManyItems,
        }),
    ));
    assert_eq!(
        over_bound.diagnostic_facts(),
        vec![
            (
                icydb_diagnostic_code::DiagnosticFactTag::ActualCount,
                (crate::db::MAX_TYPED_EXACT_KEY_BATCH_ITEMS + 1) as u64,
            ),
            (
                icydb_diagnostic_code::DiagnosticFactTag::Limit,
                crate::db::MAX_TYPED_EXACT_KEY_BATCH_ITEMS as u64,
            ),
        ],
    );
}

#[test]
fn accepted_runtime_root_is_reused_across_one_thousand_queries() {
    let session = initialize();
    seed_singleton(&session);
    let query = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("id").eq(InputValue::Unit))
        .select(["id"])
        .limit(1);

    session
        .execute_trusted_live_page(&query, None)
        .expect("warm query should build the accepted runtime root");
    reset_accepted_schema_runtime_build_counts_for_tests();
    reset_accepted_schema_snapshot_fingerprint_builds_for_tests();
    reset_accepted_schema_store_observation_counts_for_tests();

    for _ in 0..1_000 {
        let request_session = new_request_session();
        let output = request_session
            .execute_trusted_live_page(&query, None)
            .expect("warm query should reuse accepted runtime state");
        assert_eq!(output.row_count, 1);
    }

    assert_eq!(
        accepted_schema_runtime_build_counts_for_tests(),
        AcceptedSchemaRuntimeBuildCounts::default(),
    );
    assert_eq!(
        accepted_schema_store_observation_counts_for_tests(),
        AcceptedSchemaStoreObservationCounts::default(),
    );
    assert_eq!(accepted_schema_snapshot_fingerprint_builds_for_tests(), 0);
}

#[test]
fn accepted_runtime_root_cold_and_warm_observation_counts_are_bounded() {
    let session = initialize();
    reset_accepted_schema_runtime_build_counts_for_tests();
    reset_accepted_schema_store_observation_counts_for_tests();

    session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .expect("cold accepted runtime root should compile");

    assert_eq!(
        accepted_schema_store_observation_counts_for_tests(),
        AcceptedSchemaStoreObservationCounts {
            root_selections: 2,
            authority_selections: 1,
            identity_closure_validations: 1,
            entity_selection_projections: 1,
        },
    );
    assert_eq!(
        accepted_schema_runtime_build_counts_for_tests(),
        AcceptedSchemaRuntimeBuildCounts {
            root_identity_builds: 1,
            root_publications: 1,
            entity_compilations: 1,
        },
    );

    reset_accepted_schema_runtime_build_counts_for_tests();
    reset_accepted_schema_store_observation_counts_for_tests();
    new_request_session()
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .expect("warm accepted runtime root should be reused");

    assert_eq!(
        accepted_schema_store_observation_counts_for_tests(),
        AcceptedSchemaStoreObservationCounts::default(),
    );
    assert_eq!(
        accepted_schema_runtime_build_counts_for_tests(),
        AcceptedSchemaRuntimeBuildCounts::default(),
    );
}

#[test]
fn accepted_runtime_root_with_absent_store_authority_uses_full_capture() {
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    let session = new_request_session();
    session
        .db
        .drive_startup_recovery_page()
        .expect("empty database should initialize");
    reset_accepted_schema_runtime_build_counts_for_tests();
    reset_accepted_schema_store_observation_counts_for_tests();

    session
        .accepted_schema_catalog_context_for_entity_name(None)
        .expect_err("empty accepted authority should not resolve an entity");
    assert_eq!(
        accepted_schema_store_observation_counts_for_tests(),
        AcceptedSchemaStoreObservationCounts {
            root_selections: 2,
            authority_selections: 1,
            ..AcceptedSchemaStoreObservationCounts::default()
        },
    );
    assert_eq!(
        accepted_schema_runtime_build_counts_for_tests(),
        AcceptedSchemaRuntimeBuildCounts {
            root_identity_builds: 1,
            root_publications: 1,
            entity_compilations: 0,
        },
    );

    reset_accepted_schema_runtime_build_counts_for_tests();
    reset_accepted_schema_store_observation_counts_for_tests();
    new_request_session()
        .accepted_schema_catalog_context_for_entity_name(None)
        .expect_err("empty accepted authority should remain empty");
    assert_eq!(
        accepted_schema_store_observation_counts_for_tests(),
        AcceptedSchemaStoreObservationCounts {
            root_selections: 1,
            ..AcceptedSchemaStoreObservationCounts::default()
        },
    );
    assert_eq!(
        accepted_schema_runtime_build_counts_for_tests(),
        AcceptedSchemaRuntimeBuildCounts::default(),
    );
}

#[test]
fn parameterized_plan_cache_binds_current_values_across_dynamic_and_sql_surfaces() {
    let session = initialize();
    seed_singleton(&session);
    let matching = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("label").eq(InputValue::Text("singleton".to_string())))
        .select(["id", "label"])
        .order_by(asc("id"));

    let first = session
        .execute_trusted_live_page(&matching, None)
        .expect("first dynamic equality should compile its parameterized template");
    assert_eq!(first.rows, vec![singleton_row()]);
    let cached_after_first = shared_query_template_cache_len_for_tests(session.db.cache_scope_id());

    let (second, attribution) = session
        .execute_trusted_sql_query_with_attribution(
            "SELECT id, label FROM Singleton WHERE label = 'missing' ORDER BY id LIMIT 2",
        )
        .expect("different SQL literal should bind through the shared dynamic template");
    let SqlStatementResult::Projection { rows, .. } = second else {
        panic!("parameterized SQL lookup should return a projection");
    };
    assert!(
        rows.is_empty(),
        "the first literal's index bound must not leak"
    );
    assert_eq!(attribution.cache.shared_query_plan_hits, 1);
    assert_eq!(attribution.cache.shared_query_plan_misses, 0);
    assert_eq!(
        shared_query_template_cache_len_for_tests(session.db.cache_scope_id()),
        cached_after_first,
        "different literal values should reuse one shared template",
    );
}

#[test]
fn request_diagnostics_share_one_root_and_expose_repeated_point_lookups() {
    let bootstrap = initialize();
    seed_singleton(&bootstrap);
    let binding = bootstrap
        .issue_typed_entity_binding(
            ENTITY_SOURCE,
            &[
                DynamicTypedFieldBindingRequest::new(
                    ID_SOURCE.to_string(),
                    DynamicTypedFieldType::Scalar(ScalarType::Unit),
                    false,
                ),
                DynamicTypedFieldBindingRequest::new(
                    LABEL_SOURCE.to_string(),
                    DynamicTypedFieldType::Scalar(ScalarType::Text { max_len: None }),
                    false,
                ),
            ],
        )
        .expect("request diagnostics binding should resolve");
    let root = crate::db::RequestExecutionRoot::__new_runtime_root();
    let first = DbSession::<TestCanister>::new(&STORE_REGISTRY, &root);
    let second = DbSession::<TestCanister>::new(&STORE_REGISTRY, &root);
    assert!(first.enable_request_diagnostics());
    assert!(
        !second.enable_request_diagnostics(),
        "a derived session must not reset the request summary",
    );

    let query = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("id").eq(InputValue::Unit))
        .select(["id"])
        .limit(1);
    for session in [
        &first, &first, &first, &first, &second, &second, &second, &second,
    ] {
        session
            .execute_trusted_live_page(&query, None)
            .expect("repeated point lookup should execute");
    }
    second
        .execute_public_exact_key_batch_for_typed_binding(&binding, &[(), ()])
        .expect("direct exact-key batch should execute")
        .expect("binding should remain current");

    let diagnostics = first
        .request_diagnostics()
        .expect("enabled request diagnostics should snapshot");
    let dynamic = diagnostics
        .shapes
        .iter()
        .find(|shape| {
            shape.entity == ENTITY_SOURCE
                && shape.access_path == crate::db::RequestDiagnosticAccessPath::ByKey
        })
        .expect("dynamic point shape should be retained");
    assert_eq!(dynamic.executions, 8);
    assert_eq!(
        dynamic
            .plan_cache_hits
            .saturating_add(dynamic.plan_cache_misses),
        8,
    );
    assert_eq!(dynamic.hottest_key_lookups, 8);
    assert_eq!(dynamic.rows_returned, 8);
    assert!(diagnostics.warnings.iter().any(|warning| {
        warning.normalized_shape_fingerprint_prefix == dynamic.normalized_shape_fingerprint_prefix
            && warning.message.contains("get_many")
    }));

    let direct = diagnostics
        .shapes
        .iter()
        .find(|shape| shape.access_path == crate::db::RequestDiagnosticAccessPath::ByKeys)
        .expect("planner-free exact-key shape should be retained");
    assert_eq!(direct.executions, 1);
    assert_eq!(direct.exact_key_lookups, 2);
    assert_eq!(direct.hottest_key_lookups, 2);
    assert_eq!(second.request_diagnostics(), Some(diagnostics));
}

#[test]
fn parameterized_in_list_cache_identity_is_independent_of_nonempty_arity() {
    let session = initialize();
    seed_singleton(&session);
    let one = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("label").in_list([InputValue::Text("missing".to_string())]))
        .select(["id", "label"]);
    let two = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("label").in_list([
            InputValue::Text("missing".to_string()),
            InputValue::Text("singleton".to_string()),
        ]))
        .select(["id", "label"]);

    let first = session
        .execute_trusted_live_page(&one, None)
        .expect("one-item IN should compile its list-slot template");
    assert!(first.rows.is_empty());
    let cached_after_first = shared_query_template_cache_len_for_tests(session.db.cache_scope_id());
    let second = session
        .execute_trusted_live_page(&two, None)
        .expect("two-item IN should bind to the same list-slot template");

    assert_eq!(second.rows, vec![singleton_row()]);
    assert_eq!(
        shared_query_template_cache_len_for_tests(session.db.cache_scope_id()),
        cached_after_first,
        "IN list arity must not create one template per length",
    );
}

#[test]
fn parameterized_range_rebinds_bounds_and_rejects_wrong_types_before_reuse() {
    let session = initialize();
    seed_singleton(&session);
    let above = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("label").gte(InputValue::Text("z".to_string())))
        .select(["id", "label"]);
    let below = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("label").gte(InputValue::Text("a".to_string())))
        .select(["id", "label"]);

    assert!(
        session
            .execute_trusted_live_page(&above, None)
            .expect("first range should compile its parameterized template")
            .rows
            .is_empty(),
    );
    let cached_after_first = shared_query_template_cache_len_for_tests(session.db.cache_scope_id());
    assert_eq!(
        session
            .execute_trusted_live_page(&below, None)
            .expect("second range should bind a fresh lower bound")
            .rows,
        vec![singleton_row()],
    );
    assert_eq!(
        shared_query_template_cache_len_for_tests(session.db.cache_scope_id()),
        cached_after_first,
    );

    let wrong_type = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("label").gte(InputValue::Bool(true)))
        .select(["id", "label"]);
    assert!(
        session
            .execute_trusted_live_page(&wrong_type, None)
            .is_err(),
        "schema validation must reject a wrong-typed binding before cache reuse",
    );
    assert_eq!(
        shared_query_template_cache_len_for_tests(session.db.cache_scope_id()),
        cached_after_first,
    );
}

#[test]
fn parameterized_cache_keeps_projection_and_order_topology_distinct() {
    let session = initialize();
    seed_singleton(&session);
    let base = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("label").eq(InputValue::Text("singleton".to_string())))
        .select(["id"]);
    session
        .execute_trusted_live_page(&base, None)
        .expect("base parameterized shape should execute");
    let after_base = shared_query_template_cache_len_for_tests(session.db.cache_scope_id());

    let projection = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("label").eq(InputValue::Text("singleton".to_string())))
        .select(["id", "label"]);
    session
        .execute_trusted_live_page(&projection, None)
        .expect("different projection shape should execute");
    let after_projection = shared_query_template_cache_len_for_tests(session.db.cache_scope_id());
    assert_eq!(after_projection, after_base.saturating_add(1));

    let ordered = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("label").eq(InputValue::Text("singleton".to_string())))
        .select(["id", "label"])
        .order_by(asc("label"));
    session
        .execute_trusted_live_page(&ordered, None)
        .expect("different ordering topology should execute");
    assert_eq!(
        shared_query_template_cache_len_for_tests(session.db.cache_scope_id()),
        after_projection.saturating_add(1),
    );
}

#[test]
fn parameterized_template_cache_evicts_deterministically_at_its_capacity_bound() {
    let session = initialize();
    seed_singleton(&session);
    let entry_upper_bound = shared_query_template_cache_entry_upper_bound_for_tests();

    let mut last_attribution = None;
    for limit in 1..=entry_upper_bound.saturating_add(1) {
        let sql = format!(
            "SELECT id, label FROM Singleton WHERE label = 'singleton' ORDER BY id LIMIT {limit}"
        );
        let (_, attribution) = session
            .execute_trusted_sql_query_with_attribution(&sql)
            .expect("each bounded parameterized shape should execute");
        assert_eq!(attribution.cache.shared_query_plan_misses, 1);
        last_attribution = Some(attribution);
    }

    assert_eq!(
        last_attribution
            .expect("at least one cache insertion should execute")
            .cache
            .shared_query_plan_evictions,
        1,
    );

    let cache_scope_id = session.db.cache_scope_id();
    assert!(shared_query_template_cache_len_for_tests(cache_scope_id) <= entry_upper_bound,);

    let (_, newest) = session
        .execute_trusted_sql_query_with_attribution(&format!(
            "SELECT id, label FROM Singleton WHERE label = 'other' ORDER BY id LIMIT {}",
            entry_upper_bound.saturating_add(1),
        ))
        .expect("newest retained shape should execute");
    assert_eq!(newest.cache.shared_query_plan_hits, 1);

    let (_, oldest) = session
        .execute_trusted_sql_query_with_attribution(
            "SELECT id, label FROM Singleton WHERE label = 'other' ORDER BY id LIMIT 1",
        )
        .expect("evicted oldest shape should recompile");
    assert_eq!(oldest.cache.shared_query_plan_misses, 1);
}

#[test]
fn accepted_runtime_root_publication_is_atomic_across_schema_revisions() {
    let session = initialize();
    let binding = session
        .issue_typed_entity_binding(
            ENTITY_SOURCE,
            &[
                DynamicTypedFieldBindingRequest::new(
                    ID_SOURCE.to_string(),
                    DynamicTypedFieldType::Scalar(ScalarType::Unit),
                    false,
                ),
                DynamicTypedFieldBindingRequest::new(
                    LABEL_SOURCE.to_string(),
                    DynamicTypedFieldType::Scalar(ScalarType::Text { max_len: None }),
                    false,
                ),
            ],
        )
        .expect("initial exact-key binding should issue");
    let first_context = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .expect("initial accepted runtime root should resolve");
    let first_root = first_context.runtime_root_identity();

    publish_schema(
        &session,
        AcceptedSchemaRevision::INITIAL,
        AcceptedSchemaRevision::new(2),
    );
    reset_accepted_schema_runtime_build_counts_for_tests();
    reset_accepted_schema_store_observation_counts_for_tests();
    let second_context = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .expect("replacement accepted runtime root should resolve");

    assert_ne!(first_root, second_context.runtime_root_identity());
    assert!(
        session
            .execute_public_exact_key_batch_for_typed_binding(&binding, &[()])
            .expect("stale exact-key binding should fail closed")
            .is_none(),
    );
    assert_eq!(first_context.runtime_root_identity(), first_root);
    assert_eq!(
        accepted_schema_runtime_build_counts_for_tests(),
        AcceptedSchemaRuntimeBuildCounts {
            root_identity_builds: 1,
            root_publications: 1,
            entity_compilations: 1,
        },
    );
    assert_eq!(
        accepted_schema_store_observation_counts_for_tests(),
        AcceptedSchemaStoreObservationCounts {
            root_selections: 2,
            authority_selections: 1,
            identity_closure_validations: 1,
            entity_selection_projections: 1,
        },
    );
}

#[test]
fn unrelated_store_publication_rebuilds_one_complete_runtime_root() {
    let session = initialize_multi_store();
    let first_context = session
        .accepted_schema_catalog_context_for_entity_name(Some(MULTI_TARGET_ENTITY_NAME))
        .expect("initial target runtime context should resolve");
    let first_root = first_context.runtime_root_identity();

    publish_simple_schema(
        &session,
        MULTI_UNRELATED_STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        AcceptedSchemaRevision::new(2),
        MULTI_UNRELATED_ENTITY_TAG,
        MULTI_UNRELATED_ENTITY_SOURCE,
        MULTI_UNRELATED_ENTITY_NAME,
    );
    reset_accepted_schema_runtime_build_counts_for_tests();
    reset_accepted_schema_store_observation_counts_for_tests();
    let second_context = new_multi_store_request_session()
        .accepted_schema_catalog_context_for_entity_name(Some(MULTI_TARGET_ENTITY_NAME))
        .expect("target context should observe unrelated publication atomically");

    assert_ne!(first_root, second_context.runtime_root_identity());
    assert_eq!(first_context.runtime_root_identity(), first_root);
    assert_eq!(
        accepted_schema_store_observation_counts_for_tests(),
        AcceptedSchemaStoreObservationCounts {
            root_selections: 4,
            authority_selections: 2,
            identity_closure_validations: 2,
            entity_selection_projections: 2,
        },
    );
    assert_eq!(
        accepted_schema_runtime_build_counts_for_tests(),
        AcceptedSchemaRuntimeBuildCounts {
            root_identity_builds: 1,
            root_publications: 1,
            entity_compilations: 2,
        },
    );

    reset_accepted_schema_runtime_build_counts_for_tests();
    reset_accepted_schema_store_observation_counts_for_tests();
    new_multi_store_request_session()
        .accepted_schema_catalog_context_for_entity_name(Some(MULTI_TARGET_ENTITY_NAME))
        .expect("warm target context should reuse the complete replacement root");
    assert_eq!(
        accepted_schema_store_observation_counts_for_tests(),
        AcceptedSchemaStoreObservationCounts::default(),
    );
    assert_eq!(
        accepted_schema_runtime_build_counts_for_tests(),
        AcceptedSchemaRuntimeBuildCounts::default(),
    );
}

fn initialize() -> DbSession<TestCanister> {
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());

    let session = new_request_session();
    publish_schema(
        &session,
        AcceptedSchemaRevision::NONE,
        AcceptedSchemaRevision::INITIAL,
    );

    session
}

fn new_request_session() -> DbSession<TestCanister> {
    DbSession::new(
        &STORE_REGISTRY,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    )
}

fn initialize_multi_store() -> DbSession<TestCanister> {
    MULTI_TARGET_DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    MULTI_TARGET_INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    MULTI_TARGET_SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    MULTI_UNRELATED_DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    MULTI_UNRELATED_INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    MULTI_UNRELATED_SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());

    let session = new_multi_store_request_session();
    publish_simple_schema(
        &session,
        MULTI_TARGET_STORE_PATH,
        AcceptedSchemaRevision::NONE,
        AcceptedSchemaRevision::INITIAL,
        MULTI_TARGET_ENTITY_TAG,
        MULTI_TARGET_ENTITY_SOURCE,
        MULTI_TARGET_ENTITY_NAME,
    );
    publish_simple_schema(
        &session,
        MULTI_UNRELATED_STORE_PATH,
        AcceptedSchemaRevision::NONE,
        AcceptedSchemaRevision::INITIAL,
        MULTI_UNRELATED_ENTITY_TAG,
        MULTI_UNRELATED_ENTITY_SOURCE,
        MULTI_UNRELATED_ENTITY_NAME,
    );
    session
}

fn new_multi_store_request_session() -> DbSession<TestCanister> {
    DbSession::new(
        &MULTI_STORE_REGISTRY,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    )
}

fn publish_simple_schema(
    session: &DbSession<TestCanister>,
    store_path: &'static str,
    expected: AcceptedSchemaRevision,
    revision: AcceptedSchemaRevision,
    entity_tag: EntityTag,
    entity_source: &str,
    entity_name: &str,
) {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        entity_source.to_string(),
        entity_name.to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![field(1, "id", 0, AcceptedFieldKind::Unit)],
    );
    let candidate = accepted_schema_candidate_for_tests(
        store_path,
        revision,
        BTreeMap::from([(entity_tag, snapshot)]),
    );
    session
        .db
        .drive_startup_recovery_page()
        .expect("multi-store database should initialize");
    let store = session
        .db
        .store_handle(store_path)
        .expect("multi-store schema store should resolve");
    crate::db::commit::publish_accepted_schema_candidate(store_path, store, expected, &candidate)
        .expect("multi-store accepted schema should publish");
}

fn publish_schema(
    session: &DbSession<TestCanister>,
    expected: AcceptedSchemaRevision,
    revision: AcceptedSchemaRevision,
) {
    let fields = vec![
        field(1, "id", 0, AcceptedFieldKind::Unit),
        field(2, "label", 1, AcceptedFieldKind::Text { max_len: None }),
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
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).expect("test index identity should be non-zero"),
            1,
            "singleton_label_idx".to_string(),
            STORE_PATH.to_string(),
            false,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["label".to_string()],
                AcceptedFieldKind::Text { max_len: None },
                false,
            )]),
            None,
        )],
    );
    let field_bindings = BTreeMap::from([
        ((ENTITY_TAG, field_source(ID_SOURCE)), FieldId::new(1)),
        ((ENTITY_TAG, field_source(LABEL_SOURCE)), FieldId::new(2)),
    ]);
    let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        STORE_PATH,
        revision,
        BTreeMap::from([(ENTITY_TAG, snapshot)]),
        field_bindings,
    );
    session
        .db
        .drive_startup_recovery_page()
        .expect("Unit ordering database should initialize");
    let store = session
        .db
        .store_handle(STORE_PATH)
        .expect("Unit ordering store should resolve");
    crate::db::commit::publish_accepted_schema_candidate(STORE_PATH, store, expected, &candidate)
        .expect("Unit ordering accepted schema should publish");
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

fn field_source(source: &str) -> FieldSourceKey {
    FieldSourceKey::try_new(source).expect("Unit ordering field source should admit")
}

fn seed_singleton(session: &DbSession<TestCanister>) {
    session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            vec![DynamicStructuralPatch::new(vec![
                ("id".to_string(), DynamicWriteCell::Value(InputValue::Unit)),
                (
                    "label".to_string(),
                    DynamicWriteCell::Value(InputValue::Text("singleton".to_string())),
                ),
            ])],
        )
        .expect("singleton row should insert through accepted write authority");
}

fn sql_rows(session: &DbSession<TestCanister>, sql: &str) -> Vec<Vec<OutputValue>> {
    let SqlStatementResult::Projection { rows, .. } = session
        .execute_trusted_sql_query(sql)
        .expect("Unit ORDER BY query should execute")
    else {
        panic!("Unit ORDER BY query should return a projection");
    };
    rows
}

fn singleton_row() -> Vec<OutputValue> {
    vec![
        OutputValue::Unit,
        OutputValue::Text("singleton".to_string()),
    ]
}
