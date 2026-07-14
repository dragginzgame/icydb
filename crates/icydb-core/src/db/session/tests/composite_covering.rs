use super::*;
use crate::db::session::sql::with_sql_projection_materialization_metrics;

#[test]
fn session_explain_execution_order_only_composite_covering_matrix_uses_index_range_access() {
    let cases = [
        (
            "ascending composite order-only covering SQL query",
            vec![
                (9_221_u128, "alpha", 2),
                (9_222, "alpha", 1),
                (9_223, "beta", 1),
            ],
            "SELECT id, code, serial FROM CompositeIndexedSessionSqlEntity ORDER BY code ASC, serial ASC, id ASC LIMIT 2",
        ),
        (
            "descending composite order-only covering SQL query",
            vec![
                (9_231_u128, "alpha", 2),
                (9_232, "alpha", 1),
                (9_233, "beta", 1),
            ],
            "SELECT id, code, serial FROM CompositeIndexedSessionSqlEntity ORDER BY code DESC, serial DESC, id DESC LIMIT 2",
        ),
    ];

    for (context, seed_rows, sql) in cases {
        reset_indexed_session_sql_store();
        let session = indexed_sql_session();

        // Phase 1: seed one deterministic composite-index dataset so the SQL lane
        // proves planner-selected order-only access on the live `(code, serial)` index.
        seed_composite_indexed_session_sql_entities(&session, seed_rows.as_slice());

        // Phase 2: require EXPLAIN EXECUTION to surface the shared order-only
        // composite index-range root and covering-read route.
        let descriptor =
            lower_select_query_for_tests::<CompositeIndexedSessionSqlEntity>(&session, sql)
                .unwrap_or_else(|err| panic!("{context} should lower: {err}"))
                .explain_execution()
                .unwrap_or_else(|err| panic!("{context} should explain_execution: {err}"));

        assert_eq!(
            descriptor.node_type(),
            ExplainExecutionNodeType::IndexRangeScan,
            "{context} should stay on the shared index-range root",
        );
        assert_eq!(
            descriptor.covering_scan(),
            Some(true),
            "{context} should keep the explicit covering-read route",
        );
        let projection_node =
            explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::CoveringRead)
                .unwrap_or_else(|| panic!("{context} should emit a covering-read node"));
        assert_eq!(
            projection_node.node_properties().get("existing_row_mode"),
            Some(&Value::Text("planner_proven".to_string())),
            "{context} should inherit the planner-proven covering mode",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::SecondaryOrderPushdown
            )
            .is_some(),
            "{context} should report secondary order pushdown",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::OrderByAccessSatisfied
            )
            .is_some(),
            "{context} should report access-satisfied ordering",
        );
    }
}

#[test]
fn execute_sql_projection_index_coverable_multi_component_matches_entity_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic composite-index dataset so the SQL
    // projection lane must decode both indexed components from one secondary
    // `(code, serial)` access path.
    seed_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_201_u128, "alpha", 2),
            (9_202, "alpha", 1),
            (9_203, "beta", 1),
        ],
    );

    // Phase 2: verify the projection lane returns the same `(id, code,
    // serial)` rows as the entity lane for a direct composite covering query.
    let sql = "SELECT id, code, serial FROM CompositeIndexedSessionSqlEntity ORDER BY code ASC, serial ASC, id ASC LIMIT 2";
    let projected_rows =
        statement_projection_rows::<CompositeIndexedSessionSqlEntity>(&session, sql)
            .expect("multi-component covering projection query should execute");
    let entity_rows =
        execute_scalar_select_for_tests::<CompositeIndexedSessionSqlEntity>(&session, sql)
            .expect("multi-component covering entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.entity_ref().id),
                Value::Text(row.entity_ref().code.clone()),
                Value::Nat64(row.entity_ref().serial),
            ]
        })
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_rows, projected_rows);
}

#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy)]
struct ExplicitPkSuffixExpectedRow {
    id: u128,
    bucket: Option<u64>,
}

#[cfg(feature = "diagnostics")]
impl ExplicitPkSuffixExpectedRow {
    const fn id_only(id: u128) -> Self {
        Self { id, bucket: None }
    }

    const fn id_bucket(id: u128, bucket: u64) -> Self {
        Self {
            id,
            bucket: Some(bucket),
        }
    }

    fn into_values(self) -> Vec<Value> {
        let mut row = vec![Value::Ulid(Ulid::from_u128(self.id))];
        if let Some(bucket) = self.bucket {
            row.push(Value::Nat64(bucket));
        }

        row
    }
}

#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy)]
struct ExplicitPkSuffixQueryCase {
    context: &'static str,
    sql: &'static str,
    expected_root: ExplainExecutionNodeType,
    expected_rows: &'static [ExplicitPkSuffixExpectedRow],
}

#[cfg(feature = "diagnostics")]
const EXPLICIT_PK_SUFFIX_EXPECTED_WHOLE_ASC: [ExplicitPkSuffixExpectedRow; 3] = [
    ExplicitPkSuffixExpectedRow::id_only(9_405),
    ExplicitPkSuffixExpectedRow::id_only(9_420),
    ExplicitPkSuffixExpectedRow::id_only(9_430),
];
#[cfg(feature = "diagnostics")]
const EXPLICIT_PK_SUFFIX_EXPECTED_WHOLE_ASC_WITH_BUCKET: [ExplicitPkSuffixExpectedRow; 3] = [
    ExplicitPkSuffixExpectedRow::id_bucket(9_405, 10),
    ExplicitPkSuffixExpectedRow::id_bucket(9_420, 10),
    ExplicitPkSuffixExpectedRow::id_bucket(9_430, 20),
];
#[cfg(feature = "diagnostics")]
const EXPLICIT_PK_SUFFIX_EXPECTED_EQUALITY_ASC: [ExplicitPkSuffixExpectedRow; 2] = [
    ExplicitPkSuffixExpectedRow::id_only(9_405),
    ExplicitPkSuffixExpectedRow::id_only(9_420),
];
#[cfg(feature = "diagnostics")]
const EXPLICIT_PK_SUFFIX_EXPECTED_MULTI_LOOKUP_ASC: [ExplicitPkSuffixExpectedRow; 3] = [
    ExplicitPkSuffixExpectedRow::id_only(9_405),
    ExplicitPkSuffixExpectedRow::id_only(9_420),
    ExplicitPkSuffixExpectedRow::id_only(9_430),
];
#[cfg(feature = "diagnostics")]
const EXPLICIT_PK_SUFFIX_EXPECTED_MULTI_LOOKUP_BUCKET_ASC: [ExplicitPkSuffixExpectedRow; 3] = [
    ExplicitPkSuffixExpectedRow::id_only(9_420),
    ExplicitPkSuffixExpectedRow::id_only(9_430),
    ExplicitPkSuffixExpectedRow::id_only(9_405),
];
#[cfg(feature = "diagnostics")]
const EXPLICIT_PK_SUFFIX_EXPECTED_WHOLE_DESC: [ExplicitPkSuffixExpectedRow; 3] = [
    ExplicitPkSuffixExpectedRow::id_only(9_410),
    ExplicitPkSuffixExpectedRow::id_only(9_430),
    ExplicitPkSuffixExpectedRow::id_only(9_420),
];
#[cfg(feature = "diagnostics")]
const EXPLICIT_PK_SUFFIX_EXPECTED_RANGE_DESC: [ExplicitPkSuffixExpectedRow; 3] = [
    ExplicitPkSuffixExpectedRow::id_only(9_430),
    ExplicitPkSuffixExpectedRow::id_only(9_420),
    ExplicitPkSuffixExpectedRow::id_only(9_405),
];
#[cfg(feature = "diagnostics")]
const EXPLICIT_PK_SUFFIX_QUERY_CASES: [ExplicitPkSuffixQueryCase; 6] = [
    ExplicitPkSuffixQueryCase {
        context: "whole secondary order key-only",
        sql: "SELECT id FROM ExplicitPkSuffixIndexedSessionSqlEntity ORDER BY bucket ASC, id ASC LIMIT 3",
        expected_root: ExplainExecutionNodeType::IndexRangeScan,
        expected_rows: &EXPLICIT_PK_SUFFIX_EXPECTED_WHOLE_ASC,
    },
    ExplicitPkSuffixQueryCase {
        context: "whole secondary order pk plus index component",
        sql: "SELECT id, bucket FROM ExplicitPkSuffixIndexedSessionSqlEntity ORDER BY bucket ASC, id ASC LIMIT 3",
        expected_root: ExplainExecutionNodeType::IndexRangeScan,
        expected_rows: &EXPLICIT_PK_SUFFIX_EXPECTED_WHOLE_ASC_WITH_BUCKET,
    },
    ExplicitPkSuffixQueryCase {
        context: "equality bucket key-only",
        sql: "SELECT id FROM ExplicitPkSuffixIndexedSessionSqlEntity WHERE bucket = 10 ORDER BY bucket ASC, id ASC LIMIT 3",
        expected_root: ExplainExecutionNodeType::IndexPrefixScan,
        expected_rows: &EXPLICIT_PK_SUFFIX_EXPECTED_EQUALITY_ASC,
    },
    ExplicitPkSuffixQueryCase {
        context: "bounded bucket range key-only",
        sql: "SELECT id FROM ExplicitPkSuffixIndexedSessionSqlEntity WHERE bucket >= 10 AND bucket < 30 ORDER BY bucket ASC, id ASC LIMIT 3",
        expected_root: ExplainExecutionNodeType::IndexRangeScan,
        expected_rows: &EXPLICIT_PK_SUFFIX_EXPECTED_WHOLE_ASC,
    },
    ExplicitPkSuffixQueryCase {
        context: "descending whole secondary order key-only",
        sql: "SELECT id FROM ExplicitPkSuffixIndexedSessionSqlEntity ORDER BY bucket DESC, id DESC LIMIT 3",
        expected_root: ExplainExecutionNodeType::IndexRangeScan,
        expected_rows: &EXPLICIT_PK_SUFFIX_EXPECTED_WHOLE_DESC,
    },
    ExplicitPkSuffixQueryCase {
        context: "descending bounded bucket range key-only",
        sql: "SELECT id FROM ExplicitPkSuffixIndexedSessionSqlEntity WHERE bucket >= 10 AND bucket < 30 ORDER BY bucket DESC, id DESC LIMIT 3",
        expected_root: ExplainExecutionNodeType::IndexRangeScan,
        expected_rows: &EXPLICIT_PK_SUFFIX_EXPECTED_RANGE_DESC,
    },
];

#[cfg(feature = "diagnostics")]
fn expected_explicit_pk_suffix_rows(rows: &[ExplicitPkSuffixExpectedRow]) -> Vec<Vec<Value>> {
    rows.iter()
        .copied()
        .map(ExplicitPkSuffixExpectedRow::into_values)
        .collect()
}

#[cfg(feature = "diagnostics")]
fn expected_explicit_pk_suffix_output_rows(
    rows: &[ExplicitPkSuffixExpectedRow],
) -> Vec<Vec<crate::value::OutputValue>> {
    rows.iter()
        .copied()
        .map(|row| outputs(row.into_values()))
        .collect()
}

#[cfg(feature = "diagnostics")]
fn assert_explicit_pk_suffix_query_avoids_store_gets(
    session: &DbSession<SessionSqlCanister>,
    case: ExplicitPkSuffixQueryCase,
) {
    let descriptor =
        lower_select_query_for_tests::<ExplicitPkSuffixIndexedSessionSqlEntity>(session, case.sql)
            .unwrap_or_else(|err| panic!("{} should lower: {err}", case.context))
            .explain_execution()
            .unwrap_or_else(|err| panic!("{} should explain_execution: {err}", case.context));
    assert_eq!(
        descriptor.node_type(),
        case.expected_root,
        "{} should stay on the explicit primary-key suffix secondary index",
        case.context,
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "{} should use the covering-read route",
        case.context,
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "{} should report access-satisfied ordering",
        case.context,
    );

    let (_result, attribution) = session
        .execute_trusted_sql_query_with_attribution::<ExplicitPkSuffixIndexedSessionSqlEntity>(
            case.sql,
        )
        .unwrap_or_else(|err| {
            panic!(
                "{} should execute as a covering query: {err:?}",
                case.context
            )
        });
    let projected_rows =
        statement_projection_rows::<ExplicitPkSuffixIndexedSessionSqlEntity>(session, case.sql)
            .unwrap_or_else(|err| panic!("{} should return projected rows: {err:?}", case.context));

    assert_eq!(
        projected_rows,
        expected_explicit_pk_suffix_rows(case.expected_rows),
        "{} row order drifted",
        case.context,
    );
    assert_eq!(
        attribution.store_get_calls, 0,
        "{} should avoid row-store get() calls",
        case.context,
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn execute_sql_projection_explicit_primary_key_suffix_index_queries_avoid_store_gets() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed a deterministic audit-like dataset where the selected
    // secondary index stores `(bucket, id)`, with `id` also serving as the
    // primary-key tie-break.
    seed_explicit_pk_suffix_indexed_session_sql_entities(
        &session,
        &[
            (9_430_u128, 20, "charlie"),
            (9_410, 30, "delta"),
            (9_420, 10, "bravo"),
            (9_405, 10, "alpha"),
        ],
    );

    for case in EXPLICIT_PK_SUFFIX_QUERY_CASES {
        assert_explicit_pk_suffix_query_avoids_store_gets(&session, case);
    }
}

#[cfg(feature = "diagnostics")]
#[test]
fn execute_sql_projection_explicit_primary_key_suffix_in_order_uses_lazy_multi_lookup() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_explicit_pk_suffix_indexed_session_sql_entities(
        &session,
        &[
            (9_430_u128, 20, "charlie"),
            (9_410, 30, "delta"),
            (9_420, 10, "bravo"),
            (9_405, 10, "alpha"),
        ],
    );
    let sql = "SELECT id FROM ExplicitPkSuffixIndexedSessionSqlEntity \
               WHERE bucket IN (10, 20, 99) \
               ORDER BY id ASC \
               LIMIT 3";
    let descriptor =
        lower_select_query_for_tests::<ExplicitPkSuffixIndexedSessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("pk-suffix IN query should lower: {err}"))
            .explain_execution()
            .unwrap_or_else(|err| panic!("pk-suffix IN query should explain_execution: {err}"));

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexMultiLookup,
        "pk-suffix IN query should use multi-lookup access",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "pk-suffix IN query should prove primary-key order from the index suffix",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_none(),
        "pk-suffix IN query should not materialize-sort the lookup branches",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "key-only pk-suffix IN query should stay covering",
    );

    let (result, attribution) = session
        .execute_trusted_sql_query_with_attribution::<ExplicitPkSuffixIndexedSessionSqlEntity>(sql)
        .unwrap_or_else(|err| panic!("pk-suffix IN query should execute: {err:?}"));
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("pk-suffix IN query should return projection rows");
    };

    assert_eq!(
        rows,
        expected_explicit_pk_suffix_output_rows(&EXPLICIT_PK_SUFFIX_EXPECTED_MULTI_LOOKUP_ASC),
        "pk-suffix IN query should merge lookup branches by primary key",
    );
    assert_eq!(
        attribution.store_get_calls, 0,
        "key-only pk-suffix IN query should avoid row-store get() calls",
    );
    assert!(
        attribution.index_store_entry_reads <= 6,
        "pk-suffix IN query should keep index reads bounded by active lookup branches, got {attribution:?}",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn execute_sql_projection_explicit_primary_key_suffix_in_secondary_order_preserves_branch_order() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_explicit_pk_suffix_indexed_session_sql_entities(
        &session,
        &[
            (9_430_u128, 10, "charlie"),
            (9_410, 30, "delta"),
            (9_420, 10, "bravo"),
            (9_405, 20, "alpha"),
        ],
    );
    let sql = "SELECT id FROM ExplicitPkSuffixIndexedSessionSqlEntity \
               WHERE bucket IN (10, 20, 99) \
               ORDER BY bucket ASC, id ASC \
               LIMIT 3";
    let descriptor =
        lower_select_query_for_tests::<ExplicitPkSuffixIndexedSessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("pk-suffix secondary-order IN query should lower: {err}"))
            .explain_execution()
            .unwrap_or_else(|err| {
                panic!("pk-suffix secondary-order IN query should explain_execution: {err}")
            });

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexMultiLookup,
        "pk-suffix secondary-order IN query should use multi-lookup access",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "pk-suffix secondary-order IN query should prove branch-ordered secondary access",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_none(),
        "pk-suffix secondary-order IN query should not materialize-sort lookup branches",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "key-only pk-suffix secondary-order IN query should stay covering",
    );
    let covering_node =
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::CoveringRead)
            .expect("pk-suffix secondary-order IN query should emit covering read");
    assert_eq!(
        covering_node.node_properties().get("covering_order"),
        Some(&Value::Text("index_asc".to_string())),
        "pk-suffix secondary-order IN query should keep an index-order covering contract",
    );

    let projected_rows =
        statement_projection_rows::<ExplicitPkSuffixIndexedSessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| {
                panic!("pk-suffix secondary-order IN query should return projected rows: {err:?}")
            });

    assert_eq!(
        projected_rows,
        expected_explicit_pk_suffix_rows(&EXPLICIT_PK_SUFFIX_EXPECTED_MULTI_LOOKUP_BUCKET_ASC),
        "pk-suffix secondary-order IN query should concatenate lookup branches by secondary prefix",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn execute_sql_projection_computed_multi_lookup_primary_order_sorts_after_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_explicit_pk_suffix_indexed_session_sql_entities(
        &session,
        &[
            (9_430_u128, 10, "charlie"),
            (9_410, 30, "delta"),
            (9_420, 10, "bravo"),
            (9_405, 20, "alpha"),
        ],
    );

    let sql = "SELECT id, bucket + 1 AS bucket_plus_one \
               FROM ExplicitPkSuffixIndexedSessionSqlEntity \
               WHERE bucket IN (10, 20, 99) \
               ORDER BY id ASC \
               LIMIT 3";
    let projected_rows =
        statement_projection_rows::<ExplicitPkSuffixIndexedSessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| {
                panic!("computed primary-order IN query should return projected rows: {err:?}")
            });

    assert_eq!(
        projected_rows,
        vec![
            vec![
                Value::Ulid(Ulid::from_u128(9_405)),
                Value::Decimal(crate::types::Decimal::from(21_u64)),
            ],
            vec![
                Value::Ulid(Ulid::from_u128(9_420)),
                Value::Decimal(crate::types::Decimal::from(11_u64)),
            ],
            vec![
                Value::Ulid(Ulid::from_u128(9_430)),
                Value::Decimal(crate::types::Decimal::from(11_u64)),
            ],
        ],
        "computed retained-slot projection should apply primary-key order after secondary IN access",
    );
}

#[test]
fn execute_sql_projection_hybrid_covering_projection_mixes_covering_and_row_fields() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic composite-index dataset so the SQL
    // projection lane can read `code` and `serial` from the covering index
    // while sparse-decoding `note` from row storage.
    seed_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_301_u128, "alpha", 2),
            (9_302, "alpha", 1),
            (9_303, "beta", 1),
        ],
    );

    // Phase 2: require the SQL projection lane to preserve row parity while
    // taking the dedicated hybrid covering path instead of the generic
    // structural row materialization path.
    let sql = "SELECT id, code, serial, note FROM CompositeIndexedSessionSqlEntity ORDER BY code ASC, serial ASC, id ASC LIMIT 2";
    let (projected_rows, metrics) = with_sql_projection_materialization_metrics(|| {
        statement_projection_rows::<CompositeIndexedSessionSqlEntity>(&session, sql)
            .expect("hybrid composite covering projection query should execute")
    });
    let entity_rows =
        execute_scalar_select_for_tests::<CompositeIndexedSessionSqlEntity>(&session, sql)
            .expect("hybrid composite covering entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.entity_ref().id),
                Value::Text(row.entity_ref().code.clone()),
                Value::Nat64(row.entity_ref().serial),
                Value::Text(row.entity_ref().note.clone()),
            ]
        })
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_rows, projected_rows);
    assert_eq!(
        metrics.hybrid_covering_path_hits, 1,
        "hybrid composite covering projection should use the SQL-side mixed covering path",
    );
    assert_eq!(
        metrics.hybrid_covering_row_field_accesses, 2,
        "hybrid composite covering projection should cap sparse row-backed field reads to the final SQL page window when index order already satisfies the query order",
    );
    assert_eq!(
        metrics.data_rows_path_hits, 0,
        "hybrid composite covering projection should bypass the generic data-row path",
    );
    assert_eq!(
        metrics.slot_rows_path_hits, 0,
        "hybrid composite covering projection should bypass retained slot rows",
    );
}

#[test]
fn execute_sql_projection_hybrid_covering_residual_predicate_filters_before_row_reads() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed early `alpha` rows that the fully indexable residual
    // predicate must reject before sparse row-backed projection reads.
    seed_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_331_u128, "alpha", 1),
            (9_332, "alpha", 2),
            (9_333, "beta", 1),
            (9_334, "beta", 2),
            (9_335, "gamma", 1),
        ],
    );

    let sql = "SELECT id, code, note \
               FROM CompositeIndexedSessionSqlEntity \
               WHERE code != 'alpha' \
               ORDER BY code ASC, serial ASC, id ASC \
               LIMIT 2";
    let (projected_rows, metrics) = with_sql_projection_materialization_metrics(|| {
        statement_projection_rows::<CompositeIndexedSessionSqlEntity>(&session, sql)
            .expect("hybrid residual covering projection query should execute")
    });

    assert_eq!(
        projected_rows,
        vec![
            vec![
                Value::Ulid(Ulid::from_u128(9_333)),
                Value::Text("beta".to_string()),
                Value::Text("note-beta-1".to_string()),
            ],
            vec![
                Value::Ulid(Ulid::from_u128(9_334)),
                Value::Text("beta".to_string()),
                Value::Text("note-beta-2".to_string()),
            ],
        ],
        "hybrid covering must apply fully indexable residual predicates before LIMIT",
    );
    assert_eq!(
        metrics.hybrid_covering_path_hits, 1,
        "fully indexable residual predicates should stay on the hybrid covering path",
    );
    assert_eq!(
        metrics.hybrid_covering_row_field_accesses, 2,
        "hybrid covering should sparse-read row-backed fields only for accepted page rows",
    );
    assert_eq!(
        metrics.data_rows_path_hits, 0,
        "hybrid residual covering should bypass the generic data-row path",
    );
}

#[test]
fn execute_sql_projection_hybrid_covering_projection_skips_offset_before_index_projection() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic composite-index dataset where the query
    // has to skip the first index-ordered row before emitting the LIMIT window.
    seed_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_321_u128, "alpha", 2),
            (9_322, "alpha", 1),
            (9_323, "beta", 1),
        ],
    );

    // Phase 2: require row parity with the entity path while proving the
    // hybrid covering projector does not decode/project index components for
    // rows discarded by OFFSET after row-presence filtering.
    let sql = "SELECT id, code, serial, note FROM CompositeIndexedSessionSqlEntity ORDER BY code ASC, serial ASC, id ASC LIMIT 1 OFFSET 1";
    let (projected_rows, metrics) = with_sql_projection_materialization_metrics(|| {
        statement_projection_rows::<CompositeIndexedSessionSqlEntity>(&session, sql)
            .expect("offset hybrid composite covering projection query should execute")
    });
    let entity_rows =
        execute_scalar_select_for_tests::<CompositeIndexedSessionSqlEntity>(&session, sql)
            .expect("offset hybrid composite covering entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.entity_ref().id),
                Value::Text(row.entity_ref().code.clone()),
                Value::Nat64(row.entity_ref().serial),
                Value::Text(row.entity_ref().note.clone()),
            ]
        })
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_rows, projected_rows);
    assert_eq!(
        metrics.hybrid_covering_path_hits, 1,
        "offset hybrid covering projection should use the SQL-side mixed covering path",
    );
    assert_eq!(
        metrics.hybrid_covering_row_field_accesses, 1,
        "offset hybrid covering projection should materialize row-backed projected field values only for retained output rows",
    );
    assert_eq!(
        metrics.hybrid_covering_index_field_accesses, 2,
        "offset hybrid covering projection should decode projected index fields only for retained output rows",
    );
}

#[test]
fn execute_sql_projection_hybrid_covering_projection_admits_pk_plus_row_field_only() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic composite-index dataset so the SQL
    // projection lane can satisfy ordering from the `(code, serial)` index
    // while sparse-decoding only the uncovered `note` field from row storage.
    seed_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_311_u128, "alpha", 2),
            (9_312, "alpha", 1),
            (9_313, "beta", 1),
        ],
    );

    // Phase 2: prove the SQL projection lane admits the sparse row-backed
    // path even when no projected index component is returned to the caller.
    let sql = "SELECT id, note FROM CompositeIndexedSessionSqlEntity ORDER BY code ASC, serial ASC, id ASC LIMIT 2";
    let (projected_rows, metrics) = with_sql_projection_materialization_metrics(|| {
        statement_projection_rows::<CompositeIndexedSessionSqlEntity>(&session, sql)
            .expect("pk-plus-row-field covering projection query should execute")
    });
    let entity_rows =
        execute_scalar_select_for_tests::<CompositeIndexedSessionSqlEntity>(&session, sql)
            .expect("pk-plus-row-field entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.entity_ref().id),
                Value::Text(row.entity_ref().note.clone()),
            ]
        })
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_rows, projected_rows);
    assert_eq!(
        metrics.hybrid_covering_path_hits, 1,
        "pk-plus-row-field covering projection should use the SQL-side sparse index-backed path",
    );
    assert_eq!(
        metrics.hybrid_covering_index_field_accesses, 0,
        "pk-plus-row-field covering projection should not materialize projected index component values",
    );
    assert_eq!(
        metrics.hybrid_covering_row_field_accesses, 2,
        "pk-plus-row-field covering projection should sparse-read one uncovered field per emitted row",
    );
    assert_eq!(
        metrics.data_rows_path_hits, 0,
        "pk-plus-row-field covering projection should bypass the generic data-row path",
    );
    assert_eq!(
        metrics.slot_rows_path_hits, 0,
        "pk-plus-row-field covering projection should bypass retained slot rows",
    );
}
