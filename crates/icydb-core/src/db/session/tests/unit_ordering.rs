//! End-to-end accepted-schema coverage for the trivial total order of `Unit`.

use crate::{
    db::{
        DbSession, DynamicQuery, DynamicStructuralPatch, DynamicTypedEntityBinding,
        DynamicWriteCell, FieldRef, FilterExpr, PrimaryKeyComponent, PrimaryKeyValue, QueryError,
        QueryExecutionError, SqlStatementResult, TypedEntityDescriptor, TypedFieldDescriptor,
        TypedFieldType, asc,
        data::{DataStore, DecodedDataStoreKey},
        index::{IndexEntryValue, IndexStore, IndexStoreVisit, RawIndexStoreKey},
        registry::{StoreAllocationIdentities, StoreRegistry, StoreRuntimeStorageCapabilities},
        schema::{
            AcceptedFieldKind, AcceptedSchemaRevision, FieldId, FieldStorageDecode,
            PersistedFieldSnapshot, PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot,
            PersistedIndexSnapshot, PersistedSchemaSnapshot, SchemaFieldSlot, SchemaIndexId,
            SchemaInsertDefault, SchemaRowLayout, SchemaStore, SchemaVersion,
            accepted_schema_candidate_with_field_bindings_for_tests,
        },
        session::sql::{
            SqlCompiledSchemaFingerprint, SqlGlobalAggregateCachedPlan,
            SqlGlobalAggregatePlanCacheEntry,
        },
        sql_statement_dispatch, sum,
    },
    error::ErrorOrigin,
    traits::{CanisterKind, Path},
    types::{EntityTag, U256},
    value::{InputValue, OutputValue, Value},
};
use ic_stable_structures::Storable;
use icydb_diagnostic_code::{DiagnosticFactTag, QueryFieldRole};
use icydb_schema::{FieldSourceKey, ScalarType};
use std::{borrow::Cow, cell::RefCell, collections::BTreeMap, rc::Rc};

const STORE_PATH: &str = "db::session::tests::unit_ordering::Store";
const ENTITY_SOURCE: &str = "db::session::tests::unit_ordering::Singleton";
const ENTITY_NAME: &str = "Singleton";
const ID_SOURCE: &str = "db::session::tests::unit_ordering::Singleton::id";
const LABEL_SOURCE: &str = "db::session::tests::unit_ordering::Singleton::label";
const AMOUNT_SOURCE: &str = "db::session::tests::unit_ordering::Singleton::amount";
const ENTITY_TAG: EntityTag = EntityTag::new(220);
const TYPED_DESCRIPTOR: TypedEntityDescriptor = TypedEntityDescriptor::new(
    ENTITY_SOURCE,
    &[ID_SOURCE],
    &[
        TypedFieldDescriptor::new(ID_SOURCE, TypedFieldType::Scalar(ScalarType::Unit), false),
        TypedFieldDescriptor::new(
            LABEL_SOURCE,
            TypedFieldType::Scalar(ScalarType::Text { max_len: None }),
            false,
        ),
    ],
);
const UNIT_PRIMARY_KEY: PrimaryKeyValue = PrimaryKeyValue::Scalar(PrimaryKeyComponent::Unit);

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
fn admitted_generated_dispatch_preserves_entity_routing() {
    let session = initialize();

    let dispatch = sql_statement_dispatch(
        "SELECT id, label FROM Singleton WHERE label = 'dispatch' ORDER BY id LIMIT 1",
    )
    .expect("generated endpoint dispatch should parse the query");
    assert!(!dispatch.requires_introspection());
    assert_eq!(dispatch.entity_name(), Some(ENTITY_NAME));
    let (_, entity) = session
        .execute_trusted_sql_query_with_entity_name(&dispatch)
        .expect("admitted generated dispatch should execute");

    assert_eq!(entity, ENTITY_NAME);
}

#[test]
fn trusted_sql_response_and_mutation_surface_routing_remain_distinct() {
    let session = initialize();

    let dispatch = sql_statement_dispatch("SHOW STORES")
        .expect("entity-less introspection dispatch should parse");
    assert_eq!(dispatch.entity_name(), None);
    let (_, entity) = session
        .execute_trusted_sql_query_with_entity_name(&dispatch)
        .expect("entity-less introspection should execute");
    assert!(entity.is_empty());

    let mutation_dispatch =
        sql_statement_dispatch("DELETE FROM Singleton WHERE id = '00000000000000000000000000'")
            .expect("mutation dispatch should parse");
    session
        .compile_sql_mutation_with_execution_context(&mutation_dispatch)
        .expect("mutation SQL should compile through its own surface");

    let error = session
        .execute_trusted_sql_query("DELETE FROM Singleton")
        .expect_err("query ingress should reject mutation SQL");
    assert_eq!(
        error.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::QuerySqlSurfaceMismatch,
    );
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
fn global_aggregate_command_cache_retains_one_fingerprint_bound_preparation() {
    let session = initialize();
    seed_singleton(&session);

    let (exact_context, _) = session
        .compile_sql_query_for_tests("SELECT COUNT(*) FROM Singleton")
        .expect("exact count should compile");
    let exact_fingerprint = exact_context.compiled_schema_fingerprint();
    assert!(
        exact_context
            .command()
            .cached_global_aggregate_plan(exact_fingerprint)
            .is_none()
    );
    session
        .execute_compiled_sql_query_context(&exact_context)
        .expect("exact count should execute");
    let exact_entry = exact_context
        .command()
        .cached_global_aggregate_plan(exact_fingerprint)
        .expect("exact count should retain its selected preparation");
    assert!(exact_entry.exact_cardinality_target().is_some());
    assert!(exact_entry.prepared_plan().is_none());

    let mismatch = SqlCompiledSchemaFingerprint::new(u8::MAX, [0xa5; 16]);
    assert!(
        exact_context
            .command()
            .cached_global_aggregate_plan(mismatch)
            .is_none(),
        "a populated command entry must not establish freshness for another fingerprint",
    );
    exact_context
        .command()
        .set_cached_global_aggregate_plan(Rc::new(SqlGlobalAggregatePlanCacheEntry::new(
            mismatch,
            SqlGlobalAggregateCachedPlan::exact_entity_cardinality(),
        )));
    let retained_entry = exact_context
        .command()
        .cached_global_aggregate_plan(exact_fingerprint)
        .expect("a populated command slot must remain bound to its original fingerprint");
    assert!(Rc::ptr_eq(&exact_entry, &retained_entry));

    let (prepared_context, _) = session
        .compile_sql_query_for_tests("SELECT COUNT(DISTINCT label) FROM Singleton")
        .expect("ordinary aggregate should compile");
    let prepared_fingerprint = prepared_context.compiled_schema_fingerprint();
    session
        .execute_compiled_sql_query_context(&prepared_context)
        .expect("ordinary aggregate should execute");
    let prepared_entry = prepared_context
        .command()
        .cached_global_aggregate_plan(prepared_fingerprint)
        .expect("ordinary aggregate should retain its selected preparation");
    assert!(prepared_entry.exact_cardinality_target().is_none());
    assert!(prepared_entry.prepared_plan().is_some());
}

#[test]
fn u256_arithmetic_and_sum_converge_across_sql_prepared_and_fluent_paths() {
    let session = initialize();
    seed_singleton(&session);
    let arithmetic_sql = "SELECT amount + U256 '3', amount - U256 '1', \
        amount * U256 '4', amount / U256 '2', MOD(amount, U256 '3') FROM Singleton";
    let expected_arithmetic = vec![vec![
        OutputValue::u256(U256::from(5_u64)),
        OutputValue::u256(U256::ONE),
        OutputValue::u256(U256::from(8_u64)),
        OutputValue::u256(U256::ONE),
        OutputValue::u256(U256::from(2_u64)),
    ]];

    let direct = session
        .execute_trusted_sql_query(arithmetic_sql)
        .expect("direct U256 arithmetic SQL should execute");
    let SqlStatementResult::Projection { rows, .. } = &direct else {
        panic!("U256 arithmetic SQL should return a projection");
    };
    assert_eq!(rows, &expected_arithmetic);

    let (prepared, _) = session
        .compile_sql_query_for_tests(arithmetic_sql)
        .expect("U256 arithmetic SQL should compile once");
    let prepared_result = session
        .execute_compiled_sql_query_context(&prepared)
        .expect("prepared U256 arithmetic SQL should execute");
    let SqlStatementResult::Projection {
        rows: prepared_rows,
        ..
    } = prepared_result
    else {
        panic!("prepared U256 arithmetic SQL should return a projection");
    };
    assert_eq!(prepared_rows, expected_arithmetic);

    assert_eq!(
        sql_rows(
            &session,
            "SELECT SUM(amount), SUM(amount + U256 '3') FROM Singleton",
        ),
        vec![vec![
            OutputValue::u256(U256::from(2_u64)),
            OutputValue::u256(U256::from(5_u64)),
        ]],
    );

    let fluent = session
        .execute_trusted_dynamic_grouped_query(
            &DynamicQuery::new(ENTITY_NAME)
                .group_by("label")
                .aggregate(sum("amount"))
                .grouped_limits(1, 4 * 1024)
                .limit(1),
        )
        .expect("fluent U256 SUM should execute through the shared reducer");
    assert_eq!(fluent.row_count, 1);
    assert_eq!(
        fluent.rows[0].aggregate_values(),
        &[OutputValue::u256(U256::from(2_u64))],
    );

    for rejected in [
        "SELECT amount + 1 FROM Singleton",
        "SELECT AVG(amount) FROM Singleton",
    ] {
        session
            .execute_trusted_sql_query(rejected)
            .expect_err("mixed U256 arithmetic and AVG(U256) should remain rejected");
    }

    let overflow_sql = format!("SELECT amount + U256 '{}' FROM Singleton", U256::MAX);
    let (prepared_overflow, _) = session
        .compile_sql_query_for_tests(&overflow_sql)
        .expect("U256 overflow expression should compile before execution");
    let direct_error = session
        .execute_trusted_sql_query(&overflow_sql)
        .expect_err("direct U256 overflow should fail");
    let prepared_error = session
        .execute_compiled_sql_query_context(&prepared_overflow)
        .expect_err("prepared U256 overflow should fail");
    assert_eq!(direct_error.diagnostic(), prepared_error.diagnostic());
}

#[test]
fn rejected_dynamic_fields_keep_exact_role() {
    let session = initialize();

    let dynamic_predicate = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("missing").eq(InputValue::text("x".to_string())))
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
        .filter(FieldRef::new("id").eq(InputValue::unit()))
        .select(["id", "label"])
        .order_by(asc("id"))
        .limit(100);
    let binding = session
        .issue_typed_entity_binding(&TYPED_DESCRIPTOR)
        .expect("generated-style typed binding should resolve accepted Unit authority");
    let typed = session
        .execute_public_live_page_for_typed_binding(&binding, &primary_key_query, None)
        .expect("typed Unit primary-key tie-break should execute")
        .expect("typed binding should remain current");
    assert_eq!(typed.rows, vec![singleton_row()]);

    assert_unit_exact_key_batch(&session, &binding);

    for (filter, expected_rows) in [
        (FieldRef::new("id").eq(InputValue::unit()), 1),
        (FieldRef::new("id").lt(InputValue::unit()), 0),
        (FieldRef::new("id").lte(InputValue::unit()), 1),
        (FieldRef::new("id").gt(InputValue::unit()), 0),
        (FieldRef::new("id").gte(InputValue::unit()), 1),
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
    for direction in ["ASC", "DESC"] {
        assert_eq!(
            sql_rows(
                &session,
                &format!("SELECT DISTINCT label FROM Singleton ORDER BY label {direction} LIMIT 1"),
            ),
            vec![vec![OutputValue::text("singleton".to_string())]],
        );
    }
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
        .filter(FieldRef::new("id").eq(InputValue::unit()))
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
            FieldRef::new("id").eq(InputValue::unit()),
            FieldRef::new("label").eq(InputValue::text("other".to_string())),
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
        .filter(FieldRef::new("label").eq(InputValue::text("singleton".to_string())))
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

    assert_group_seek_corruption(
        &session,
        ErrorOrigin::Store,
        "ordered DISTINCT group seek must fail closed on a missing representative",
    );

    let raw_index_key = store.with_index(|index| {
        let mut raw_index_key = None;
        let result: Result<(), std::convert::Infallible> = index.visit_entries(|key, _value| {
            raw_index_key = Some(key.clone());
            Ok(IndexStoreVisit::Stop)
        });
        result.expect("corruption fixture index traversal should be infallible");
        raw_index_key.expect("corruption fixture should retain one accepted index key")
    });
    store.with_index_mut(|index| {
        index.insert(
            raw_index_key.clone(),
            <IndexEntryValue as Storable>::from_bytes(Cow::Owned(vec![0xff])),
        );
    });
    assert_group_seek_corruption(
        &session,
        ErrorOrigin::Index,
        "ordered DISTINCT group seek must fail closed on an invalid witness",
    );

    let mut malformed_bytes = raw_index_key.as_bytes().to_vec();
    malformed_bytes.push(0xff);
    let malformed_key = <RawIndexStoreKey as Storable>::from_bytes(Cow::Owned(malformed_bytes));
    store.with_index_mut(|index| {
        assert!(index.remove(&raw_index_key).is_some());
        index.insert(malformed_key, IndexEntryValue::presence());
    });
    assert_group_seek_corruption(
        &session,
        ErrorOrigin::Index,
        "ordered DISTINCT group seek must fail closed on a malformed raw key",
    );
}

fn assert_group_seek_corruption(
    session: &DbSession<TestCanister>,
    expected_origin: ErrorOrigin,
    expectation: &str,
) {
    for direction in ["ASC", "DESC"] {
        let sql =
            format!("SELECT DISTINCT label FROM Singleton ORDER BY label {direction} LIMIT 1");
        let error = session
            .execute_trusted_sql_query(&sql)
            .expect_err(expectation);
        assert_corruption_origin(error, expected_origin);
    }
}

fn assert_store_corruption(error: QueryError) {
    assert_corruption_origin(error, ErrorOrigin::Store);
}

fn assert_corruption_origin(error: QueryError, expected_origin: ErrorOrigin) {
    let QueryError::Execute(QueryExecutionError::Corruption(error)) = error else {
        panic!("accepted-index corruption should retain corruption taxonomy");
    };
    assert_eq!(error.origin(), expected_origin);
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
    let exact = session
        .execute_public_exact_key_batch_for_typed_binding(
            binding,
            &[UNIT_PRIMARY_KEY, UNIT_PRIMARY_KEY],
        )
        .expect("typed Unit exact-key batch should execute")
        .expect("typed binding should remain current");
    assert_eq!(exact.positions, vec![0, 0]);
    assert_eq!(exact.distinct_rows, vec![Some(singleton_stored_row())]);
    assert_eq!(
        DataStore::current_get_call_count().saturating_sub(gets_before),
        1,
        "duplicate input positions must share one physical row read",
    );
    let too_many = vec![UNIT_PRIMARY_KEY; crate::db::MAX_TYPED_EXACT_KEY_BATCH_ITEMS + 1];
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
fn parameterized_plan_cache_binds_current_values_across_dynamic_and_sql_surfaces() {
    let session = initialize();
    seed_singleton(&session);
    let matching = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("label").eq(InputValue::text("singleton".to_string())))
        .select(["id", "label"])
        .order_by(asc("id"));

    let first = session
        .execute_trusted_live_page(&matching, None)
        .expect("first dynamic equality should compile its parameterized template");
    assert_eq!(first.rows, vec![singleton_row()]);
    let second = session
        .execute_trusted_sql_query(
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
}

#[test]
fn parameterized_in_list_cache_identity_is_independent_of_nonempty_arity() {
    let session = initialize();
    seed_singleton(&session);
    let one = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("label").in_list([InputValue::text("missing".to_string())]))
        .select(["id", "label"]);
    let two = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("label").in_list([
            InputValue::text("missing".to_string()),
            InputValue::text("singleton".to_string()),
        ]))
        .select(["id", "label"]);

    let first = session
        .execute_trusted_live_page(&one, None)
        .expect("one-item IN should compile its list-slot template");
    assert!(first.rows.is_empty());
    let second = session
        .execute_trusted_live_page(&two, None)
        .expect("two-item IN should bind to the same list-slot template");

    assert_eq!(second.rows, vec![singleton_row()]);
}

#[test]
fn parameterized_range_rebinds_bounds_and_rejects_wrong_types_before_reuse() {
    let session = initialize();
    seed_singleton(&session);
    let above = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("label").gte(InputValue::text("z".to_string())))
        .select(["id", "label"]);
    let below = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("label").gte(InputValue::text("a".to_string())))
        .select(["id", "label"]);

    assert!(
        session
            .execute_trusted_live_page(&above, None)
            .expect("first range should compile its parameterized template")
            .rows
            .is_empty(),
    );
    assert_eq!(
        session
            .execute_trusted_live_page(&below, None)
            .expect("second range should bind a fresh lower bound")
            .rows,
        vec![singleton_row()],
    );

    let wrong_type = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("label").gte(InputValue::boolean(true)))
        .select(["id", "label"]);
    assert!(
        session
            .execute_trusted_live_page(&wrong_type, None)
            .is_err(),
        "schema validation must reject a wrong-typed binding before cache reuse",
    );
}

#[test]
fn accepted_runtime_root_publication_is_atomic_across_schema_revisions() {
    let session = initialize();
    let binding = session
        .issue_typed_entity_binding(&TYPED_DESCRIPTOR)
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
    let second_context = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .expect("replacement accepted runtime root should resolve");

    assert_ne!(first_root, second_context.runtime_root_identity());
    assert!(
        session
            .execute_public_exact_key_batch_for_typed_binding(&binding, &[UNIT_PRIMARY_KEY])
            .expect("stale exact-key binding should fail closed")
            .is_none(),
    );
    assert_eq!(first_context.runtime_root_identity(), first_root);
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

fn publish_schema(
    session: &DbSession<TestCanister>,
    expected: AcceptedSchemaRevision,
    revision: AcceptedSchemaRevision,
) {
    let fields = vec![
        field(1, "id", 0, AcceptedFieldKind::Unit),
        field(2, "label", 1, AcceptedFieldKind::Text { max_len: None }),
        field(3, "amount", 2, AcceptedFieldKind::U256),
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
        ((ENTITY_TAG, field_source(AMOUNT_SOURCE)), FieldId::new(3)),
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
                (
                    "id".to_string(),
                    DynamicWriteCell::Value(InputValue::unit()),
                ),
                (
                    "label".to_string(),
                    DynamicWriteCell::Value(InputValue::text("singleton".to_string())),
                ),
                (
                    "amount".to_string(),
                    DynamicWriteCell::Value(InputValue::u256(U256::from(2_u64))),
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
        OutputValue::unit(),
        OutputValue::text("singleton".to_string()),
    ]
}

fn singleton_stored_row() -> Vec<OutputValue> {
    let mut row = singleton_row();
    row.push(OutputValue::u256(U256::from(2_u64)));
    row
}
