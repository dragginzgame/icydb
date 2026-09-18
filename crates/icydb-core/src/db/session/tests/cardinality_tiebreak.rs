//! End-to-end proof for the bounded exact-cardinality planner tie-break.

mod access_projection;
mod index_metadata;
mod order_metadata;
mod prefix_accounting;
mod prepared_explain;
mod projection_metadata;
mod residual_bounds;
mod secondary_order;
mod sparse_indexes;
mod typed_explain;

use crate::{
    db::{
        DbSession, DynamicQuery, DynamicStructuralPatch, DynamicWriteCell, FieldRef, FilterExpr,
        MissingRowPolicy, SqlStatementResult, asc,
        commit::database_incarnation_id,
        data::DataStore,
        index::{IndexId, IndexStore},
        journal::JournalTailStore,
        query::{
            intent::StructuralQuery,
            plan::{CardinalityTiebreakFamily, CardinalityTiebreakRoutePin, OrderSpec},
        },
        registry::{
            StoreAllocationIdentities, StoreAllocationIdentity, StoreRegistry,
            StoreRuntimeStorageCapabilities,
        },
        schema::{
            AcceptedFieldKind, AcceptedSchemaRevision, CandidateSchemaRevision, FieldId,
            FieldStorageDecode, PersistedFieldSnapshot, PersistedIndexExpressionOp,
            PersistedIndexExpressionSnapshot, PersistedIndexFieldPathSnapshot,
            PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
            PersistedSchemaSnapshot, SchemaFieldSlot, SchemaIndexId, SchemaInsertDefault,
            SchemaRowLayout, SchemaStore, SchemaVersion,
            accepted_schema_candidate_with_field_bindings_for_tests,
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
use icydb_diagnostic_code::DiagnosticExecutionLane;
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
    fn commit_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(162)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.test.cardinality-tiebreak.commit.v1";
    fn startup_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(163)
    }
    const STARTUP_STABLE_KEY: &'static str = "icydb.test.cardinality-tiebreak.startup.v1";
    fn integrity_progress_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(164)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
        "icydb.test.cardinality-tiebreak.integrity.v1";
}

struct JournaledTestCanister;

impl Path for JournaledTestCanister {
    const PATH: &'static str = "db::session::tests::cardinality_tiebreak::JournaledCanister";
}

impl CanisterKind for JournaledTestCanister {
    fn commit_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(159)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.cardinality_tie.commit.v1";
    fn startup_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(161)
    }
    const STARTUP_STABLE_KEY: &'static str = "icydb.cardinality_tie.startup.v1";
    fn integrity_progress_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(160)
    }
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

    projection_rows(
        &session,
        "SELECT id FROM PlannerRow WHERE rare = 'group-a' ORDER BY id LIMIT 20",
    );
    let cold = explain(&session, "common = 'everyone' AND rare = 'group-a'");
    assert!(cold.contains("z_rare_idx"), "{cold}");
    assert!(cold.contains("exact_cardinality_tiebreak"), "{cold}");
    assert!(
        cold.contains("cardinality_evidence: exact_at_selection"),
        "{cold}"
    );
    assert!(cold.contains("exact_prefix_entries: 12"), "{cold}");
    assert!(cold.contains("exact_prefix_entries: 6"), "{cold}");
    let structured = explain_json(&session, "common = 'everyone' AND rare = 'group-a'");
    assert!(
        structured.contains("\"reason\":\"exact_cardinality_tiebreak\""),
        "{structured}"
    );
    assert!(
        structured.contains("\"cardinality_evidence_state\":\"exact_at_selection\""),
        "{structured}"
    );
    let rows = projection_rows(
        &session,
        "SELECT id FROM PlannerRow \
         WHERE common = 'everyone' AND rare = 'group-a' ORDER BY id LIMIT 20",
    );
    assert_eq!(rows.len(), 6);

    for id in 100..110 {
        insert_row(&session, id, "other", "group-a");
    }
    let stale = explain(&session, "common = 'everyone' AND rare = 'group-a'");
    assert!(stale.contains("IndexPrefix(z_rare_idx)"), "{stale}");

    let rebound = explain(&session, "common = 'other' AND rare = 'group-a'");
    assert!(rebound.contains("IndexPrefix(a_common_idx)"), "{rebound}");
}

#[test]
fn verbose_explain_preserves_internal_plan_cache_reuse_fact() {
    let session = initialize();
    seed_rows(&session);
    let predicate = "common = 'everyone' AND rare = 'group-a'";

    let cold = explain(&session, predicate);
    let reused = explain(&session, predicate);

    // Cache-hit metadata may differ; the rendered planner snapshot must not.
    let planner_lines = |text: &str| {
        text.lines()
            .filter(|line| !line.starts_with("diag.s."))
            .map(str::to_owned)
            .collect::<Vec<_>>()
    };
    assert_eq!(planner_lines(&cold), planner_lines(&reused));

    assert!(
        reused.contains("diag.s.semantic_reuse_artifact=shared_prepared_query_plan"),
        "{reused}"
    );
    assert!(reused.contains("diag.s.semantic_reuse=hit"), "{reused}");
}

#[test]
fn grouped_pages_preserve_filtered_counts_with_and_without_retention() {
    use crate::db::count;

    let session = initialize();
    seed_rows(&session);
    let query = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("id").gte(InputValue::nat64(2)))
        .group_by("rare")
        .aggregate(count())
        .order_by(asc("rare"))
        .grouped_limits(4, 16 * 1024)
        .limit(1);
    for capacity in [0, 4 * 1024 * 1024] {
        session.clear_shared_query_cache_for_tests(capacity);
        for _ in 0..2 {
            let first = session
                .execute_trusted_dynamic_grouped_query(&query)
                .unwrap();
            assert_eq!(first.row_count, 1);
            assert_eq!(
                first.rows[0].group_key(),
                &[OutputValue::text("group-a".into())]
            );
            assert_eq!(first.rows[0].aggregate_values(), &[OutputValue::nat64(4)]);
            let resumed = query
                .clone()
                .cursor(first.next_cursor.expect("second group remains"));
            let second = session
                .execute_trusted_dynamic_grouped_query(&resumed)
                .unwrap();
            assert_eq!(second.row_count, 1);
            assert_eq!(
                second.rows[0].group_key(),
                &[OutputValue::text("group-b".into())]
            );
            assert_eq!(second.rows[0].aggregate_values(), &[OutputValue::nat64(6)]);
            assert!(second.next_cursor.is_none());
        }
    }
}

#[test]
fn grouped_decimal_cursor_resumes_canonical_scalar_and_collection_keys() {
    use crate::{db::count, types::Decimal, value::PublicValue};

    for collection in [false, true] {
        std::thread::spawn(move || {
            let session = initialize();
            let decimal = AcceptedFieldKind::Decimal { scale: 2 };
            let kind = if collection {
                AcceptedFieldKind::List(Box::new(decimal))
            } else {
                decimal
            };
            let fields = vec![
                field(1, "id", 0, AcceptedFieldKind::Nat64),
                field(2, "amount", 1, kind),
            ];
            let snapshot = PersistedSchemaSnapshot::new(
                SchemaVersion::initial(),
                ENTITY_SOURCE.into(),
                ENTITY_NAME.into(),
                FieldId::new(1),
                SchemaRowLayout::initial(
                    fields
                        .iter()
                        .map(|field| (field.id(), field.slot()))
                        .collect(),
                ),
                fields,
            );
            let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
                STORE_PATH,
                AcceptedSchemaRevision::new(2),
                BTreeMap::from([(ENTITY_TAG, snapshot)]),
                BTreeMap::from([
                    ((ENTITY_TAG, field_source("id")), FieldId::new(1)),
                    ((ENTITY_TAG, field_source("amount")), FieldId::new(2)),
                ]),
            );
            crate::db::commit::publish_accepted_schema_candidate(
                STORE_PATH,
                session.db.store_handle(STORE_PATH).unwrap(),
                AcceptedSchemaRevision::INITIAL,
                &candidate,
            )
            .unwrap();
            for (id, amount) in [(1, 100), (2, 250), (3, 300)] {
                let value = InputValue::decimal(Decimal::new(amount, 2));
                let value = if collection {
                    InputValue::list(vec![value])
                } else {
                    value
                };
                session
                    .execute_trusted_dynamic_insert_batch(
                        ENTITY_NAME,
                        vec![DynamicStructuralPatch::new(vec![
                            ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                            ("amount".into(), DynamicWriteCell::Value(value)),
                        ])],
                    )
                    .unwrap();
            }
            let query = DynamicQuery::new(ENTITY_NAME)
                .group_by("amount")
                .aggregate(count())
                .grouped_limits(4, 16 * 1024)
                .limit(1);
            for capacity in [0, 4 * 1024 * 1024] {
                session.clear_shared_query_cache_for_tests(capacity);
                let mut cursor = None;
                for (index, expected) in
                    [Decimal::new(1, 0), Decimal::new(25, 1), Decimal::new(3, 0)]
                        .into_iter()
                        .enumerate()
                {
                    let request = cursor.as_ref().map_or_else(
                        || query.clone(),
                        |cursor: &String| query.clone().cursor(cursor),
                    );
                    let page = session
                        .execute_trusted_dynamic_grouped_query(&request)
                        .expect("unchanged issued decimal cursor must resume");
                    let value = OutputValue::decimal(expected);
                    let value = if collection {
                        OutputValue::list(vec![PublicValue::Decimal(expected)])
                    } else {
                        value
                    };
                    assert_eq!(page.rows.len(), 1);
                    assert_eq!(page.rows[0].group_key(), &[value]);
                    assert_eq!(page.rows[0].aggregate_values(), &[OutputValue::nat64(1)]);
                    cursor = page.next_cursor;
                    assert_eq!(cursor.is_some(), index < 2);
                }
            }
        })
        .join()
        .unwrap();
    }
}

#[test]
fn grouped_cursor_integrity_and_boundary_shape_hold_on_cold_and_warm_plans() {
    use crate::{
        db::{
            commit::cursor_authentication_key,
            count,
            cursor::{GroupedContinuationToken, decode_optional_cursor_token, encode_cursor},
            desc,
        },
        value::Value,
    };
    use icydb_diagnostic_code::{DiagnosticDecodeReason, DiagnosticFactTag, ErrorCode};

    let session = initialize();
    seed_rows(&session);
    for (order, expected) in [(asc("rare"), "group-b"), (desc("rare"), "group-a")] {
        let query = DynamicQuery::new(ENTITY_NAME)
            .group_by("rare")
            .aggregate(count())
            .order_by(order)
            .grouped_limits(4, 16 * 1024)
            .limit(1);
        let first = session
            .execute_trusted_dynamic_grouped_query(&query)
            .unwrap();
        let cursor = first.next_cursor.unwrap();
        let bytes = decode_optional_cursor_token(Some(&cursor))
            .unwrap()
            .unwrap();
        let key = cursor_authentication_key().unwrap();
        let token = GroupedContinuationToken::decode(&bytes, &key).unwrap();
        let (signature, _, direction, offset) = token.into_components();
        let mut tampered = bytes;
        let last_payload_byte = tampered.len() - 33;
        tampered[last_payload_byte] ^= 1;
        let mut invalid = vec![encode_cursor(&tampered)];
        // A valid MAC is necessary but does not replace accepted boundary checks.
        for values in [
            vec![],
            vec![Value::Nat64(9)],
            vec![Value::Null],
            vec![Value::Text("group-a".into()), Value::Text("extra".into())],
        ] {
            let malformed =
                GroupedContinuationToken::new_with_direction(signature, values, direction, offset)
                    .encode(&key)
                    .unwrap();
            invalid.push(encode_cursor(&malformed));
        }
        for invalid_cursor in invalid {
            session.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
            for _ in 0..2 {
                let error = session
                    .execute_trusted_dynamic_grouped_query(&query.clone().cursor(&invalid_cursor))
                    .expect_err("malformed cursor must not change the resume boundary");
                assert_eq!(
                    error.diagnostic().error_code(),
                    ErrorCode::QUERY_INVALID_CONTINUATION_CURSOR
                );
                assert_eq!(
                    error.diagnostic_facts(),
                    vec![(
                        DiagnosticFactTag::DecodeReason,
                        DiagnosticDecodeReason::CursorTokenDecode.raw(),
                    )]
                );
            }
        }
        let resumed = session
            .execute_trusted_dynamic_grouped_query(&query.cursor(&cursor))
            .expect("unchanged cursor must still resume");
        assert_eq!(
            resumed.rows[0].group_key(),
            &[OutputValue::text(expected.into())]
        );
        assert!(resumed.next_cursor.is_none());
    }
}

#[test]
fn grouped_cursor_policy_rejections_are_request_errors_on_cold_and_warm_plans() {
    use crate::db::{count, count_by};
    use icydb_diagnostic_code::{
        DiagnosticDecodeReason, DiagnosticFactTag, ErrorCode, ErrorOrigin,
    };

    let session = initialize();
    seed_rows(&session);
    let grouped = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("id").gte(InputValue::nat64(2)))
        .group_by("rare")
        .aggregate(count())
        .order_by(asc("rare"))
        .grouped_limits(4, 16 * 1024);
    let first = session
        .execute_trusted_dynamic_grouped_query(&grouped.clone().limit(1))
        .expect("first grouped page");
    let cursor = first.next_cursor.expect("second group remains");
    let resumed = session
        .execute_trusted_dynamic_grouped_query(&grouped.clone().limit(1).cursor(&cursor))
        .expect("control cursor must resume successfully");
    assert_eq!(
        resumed.rows[0].group_key(),
        &[OutputValue::text("group-b".into())]
    );

    let global_distinct = DynamicQuery::new(ENTITY_NAME)
        .aggregate(count_by("rare").distinct())
        .grouped_limits(4, 16 * 1024)
        .limit(1);
    for (query, reason, expected_rows) in [
        (
            grouped,
            DiagnosticDecodeReason::CursorGroupedContinuationRequiresLimit,
            2,
        ),
        (
            global_distinct,
            DiagnosticDecodeReason::CursorGlobalDistinctContinuationUnsupported,
            1,
        ),
    ] {
        session.clear_shared_query_cache_for_tests(4 * 1024 * 1024);
        for _ in 0..2 {
            let error = session
                .execute_trusted_dynamic_grouped_query(&query.clone().cursor(&cursor))
                .expect_err("unsupported cursor use is a request rejection");
            assert_eq!(
                error.diagnostic().error_code(),
                ErrorCode::QUERY_INVALID_CONTINUATION_CURSOR
            );
            assert_eq!(error.diagnostic().origin(), ErrorOrigin::Cursor);
            assert_eq!(
                error.diagnostic_facts(),
                vec![(DiagnosticFactTag::DecodeReason, reason.raw())]
            );
        }
        let control = session
            .execute_trusted_dynamic_grouped_query(&query)
            .expect("the same query without a cursor is supported");
        assert_eq!(control.row_count, expected_rows);
        assert!(control.next_cursor.is_none());
    }
}

#[test]
fn byte_length_projection_preserves_mixed_outputs_and_order_inputs() {
    let session = initialize();
    for (id, text) in [(0, "é"), (1, "abc"), (2, ""), (3, "éé")] {
        insert_row(&session, id, "everyone", text);
    }
    for capacity in [0, 4 * 1024 * 1024] {
        session.clear_shared_query_cache_for_tests(capacity);
        for _ in 0..2 {
            assert_eq!(
                projection_rows(
                    &session,
                    "SELECT OCTET_LENGTH(rare) FROM PlannerRow ORDER BY id"
                ),
                [2, 3, 0, 4].map(|length| vec![OutputValue::nat64(length)]),
            );
            let expected =
                [(0, "é", 2), (1, "abc", 3), (2, "", 0), (3, "éé", 4)].map(|(id, text, length)| {
                    vec![
                        OutputValue::nat64(id),
                        OutputValue::nat64(length),
                        OutputValue::nat64(length),
                        OutputValue::text(text.into()),
                    ]
                });
            assert_eq!(
                projection_rows(
                    &session,
                    "SELECT id, OCTET_LENGTH(rare), OCTET_LENGTH(rare), rare FROM PlannerRow ORDER BY id"
                ),
                expected,
            );
            assert_eq!(
                projection_rows(
                    &session,
                    "SELECT id, OCTET_LENGTH(rare) FROM PlannerRow ORDER BY rare, id"
                ),
                [(2, 0), (1, 3), (0, 2), (3, 4)]
                    .map(|(id, length)| vec![OutputValue::nat64(id), OutputValue::nat64(length)]),
            );
            // The same text ordering must remain usable for continuation anchors.
            let query = DynamicQuery::new(ENTITY_NAME)
                .select(["id", "rare"])
                .order_by(asc("rare"))
                .order_by(asc("id"));
            let mut continuation = None;
            let mut actual = Vec::new();
            for _ in 0..8 {
                let page = session
                    .execute_trusted_live_page(&query, continuation.as_deref())
                    .unwrap();
                actual.extend(page.rows);
                continuation = page.continuation;
                if continuation.is_none() {
                    break;
                }
            }
            assert_eq!(
                actual,
                [(2, ""), (1, "abc"), (0, "é"), (3, "éé")]
                    .map(|(id, text)| vec![OutputValue::nat64(id), OutputValue::text(text.into())])
            );
            assert!(continuation.is_none());
        }
    }
}

#[test]
fn global_distinct_preserves_implicit_group_when_no_rows_match() {
    use crate::db::count_by;

    let session = initialize();
    seed_rows(&session);
    for capacity in [0, 4 * 1024 * 1024] {
        session.clear_shared_query_cache_for_tests(capacity);
        for _ in 0..2 {
            for (minimum_id, expected) in [(99, 0), (0, 2)] {
                let query = DynamicQuery::new(ENTITY_NAME)
                    .filter(FieldRef::new("id").gte(InputValue::nat64(minimum_id)))
                    .aggregate(count_by("rare").distinct())
                    .grouped_limits(1, 16 * 1024);
                let page = session
                    .execute_trusted_dynamic_grouped_query(&query)
                    .unwrap();
                assert_eq!(page.row_count, 1);
                assert_eq!(page.rows.len(), 1);
                assert!(page.rows[0].group_key().is_empty());
                assert_eq!(
                    page.rows[0].aggregate_values(),
                    &[OutputValue::nat64(expected)]
                );
                assert!(page.next_cursor.is_none());
            }
        }
    }
}

#[test]
fn grouped_input_and_filter_expressions_preserve_independent_results_on_warm_calls() {
    let session = initialize();
    seed_rows(&session);
    let sql = "SELECT rare, COUNT(id + 1) FILTER (WHERE id < 9), COUNT(*) \
               FROM PlannerRow GROUP BY rare ORDER BY rare LIMIT 2";
    for _ in 0..2 {
        let SqlStatementResult::Grouped {
            rows, row_count, ..
        } = session.execute_trusted_sql_query(sql).unwrap()
        else {
            panic!("grouped expression query must return grouped rows");
        };
        assert_eq!(row_count, 2);
        assert_eq!(rows.len(), 2);
        for (row, group, filtered) in [(&rows[0], "group-a", 6), (&rows[1], "group-b", 3)] {
            assert_eq!(row.group_key(), &[OutputValue::text(group.into())]);
            assert_eq!(
                row.aggregate_values(),
                &[OutputValue::nat64(filtered), OutputValue::nat64(6)]
            );
        }
    }
}

#[test]
fn exact_cardinality_supports_multi_lookup_and_branch_set_but_excludes_range_and_grouped() {
    let session = initialize();
    seed_rows(&session);

    let multi = explain(&session, "common IN ('everyone', 'absent')");
    assert!(multi.contains("IndexMultiLookup(a_common_idx)"), "{multi}");
    assert!(
        multi.contains("cardinality_evidence: exact_at_selection"),
        "{multi}"
    );
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
    projection_rows(
        &session,
        "SELECT id FROM PlannerRow WHERE common >= 'everyone' ORDER BY id LIMIT 20",
    );
    session
        .execute_trusted_sql_query(
            "SELECT common, COUNT(*) FROM PlannerRow \
             WHERE common = 'everyone' AND rare = 'group-a' GROUP BY common LIMIT 20",
        )
        .expect("grouped exclusion fixture should execute");
}

#[test]
fn chained_filters_form_one_compound_index_predicate_for_public_admission() {
    let session = initialize();
    seed_rows(&session);

    let query = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("wide_fixed").eq(InputValue::text("all".to_string())))
        .filter(FieldRef::new("wide_branch").eq(InputValue::text("y".to_string())))
        .select(["id"])
        .limit(1);
    let page = session
        .execute_public_live_page(&query, None)
        .expect("chained exact filters should select their accepted compound index");

    assert_eq!(page.rows, vec![vec![OutputValue::nat64(1)]]);

    let full_scan = DynamicQuery::new(ENTITY_NAME)
        .filter(FieldRef::new("wide_branch").eq(InputValue::text("y".to_string())))
        .select(["id"])
        .limit(1);
    let diagnostic = session
        .execute_public_live_page(&full_scan, None)
        .expect_err("LIMIT 1 must not admit a genuine full scan")
        .diagnostic();
    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QueryReadAdmission,
    );
    assert_eq!(
        diagnostic.detail(),
        Some(
            &icydb_diagnostic_code::DiagnosticDetail::QueryReadAdmission {
                reason: icydb_diagnostic_code::QueryReadAdmissionCode::UnboundedFullScanRejected,
            },
        ),
    );
}

#[test]
fn structural_and_residual_ranking_remain_strictly_ahead_of_cardinality() {
    let session = initialize();
    seed_rows(&session);

    let structurally_stronger = explain(
        &session,
        "wide_fixed = 'all' AND wide_branch IN ('x', 'y') AND rare = 'absent'",
    );
    assert!(
        structurally_stronger.contains("IndexBranchSet(b_wide_branch_idx)"),
        "{structurally_stronger}",
    );
    assert!(
        structurally_stronger.contains("cardinality_evidence: not_applicable"),
        "{structurally_stronger}",
    );
    let lower_residual = explain(&session, "LOWER(common) = 'everyone' AND rare = 'absent'");
    assert!(
        lower_residual.contains("IndexPrefix(zz_lower_common_idx)"),
        "{lower_residual}",
    );
    assert!(
        lower_residual.contains("reason=residual_burden_preferred"),
        "{lower_residual}",
    );
    assert!(
        lower_residual.contains("cardinality_evidence: not_applicable"),
        "{lower_residual}",
    );
}

#[test]
fn expression_lookup_selection_preserves_equality_membership_and_warm_results() {
    let session = initialize();
    seed_rows(&session);
    let expected = projection_rows(
        &session,
        "SELECT id FROM PlannerRow WHERE common = 'everyone' ORDER BY id LIMIT 20",
    );
    assert!(!expected.is_empty());
    for (predicate, route) in [
        (
            "LOWER(common) = 'EVERYONE'",
            "IndexPrefix(zz_lower_common_idx)",
        ),
        (
            "LOWER(common) IN ('ABSENT', 'EVERYONE', 'everyone')",
            "IndexMultiLookup(zz_lower_common_idx)",
        ),
    ] {
        let sql = format!("SELECT id FROM PlannerRow WHERE {predicate} ORDER BY id LIMIT 20");
        for _ in 0..3 {
            let plan = explain(&session, predicate);
            assert!(plan.contains(route), "{plan}");
            assert_eq!(projection_rows(&session, &sql), expected);
        }
    }
}

#[test]
fn scalar_preparation_shares_complete_expression_index_authority() {
    let session = initialize();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let authority = catalog.accepted_entity_authority();
    let schema = authority.accepted_schema_info_handle();
    assert!(std::ptr::eq(
        schema.as_ref(),
        catalog.accepted_schema_info()
    ));
    assert!(!schema.expression_indexes().is_empty());
    assert!(std::ptr::eq(
        authority.accepted_value_catalog_handle().enum_catalog(),
        schema.enum_catalog(),
    ));
    let query = StructuralQuery::new(MissingRowPolicy::Ignore).limit(20);
    let prepared = crate::db::query::preparation::with_preparation_work(|work| {
        query.prepare_scalar_planning_state_with_schema_info(schema.clone(), work)
    })
    .unwrap();
    assert!(std::ptr::eq(prepared.schema_info(), schema.as_ref()));
    assert_eq!(
        prepared.schema_info().expression_indexes().len(),
        catalog.accepted_schema_info().expression_indexes().len(),
    );
}

#[test]
fn expression_ordered_ranges_preserve_bounds_and_warm_results() {
    let session = initialize();
    seed_rows(&session);
    for op in [">", ">=", "<", "<="] {
        for bound in ["EVERYONE", "ABSENT"] {
            let predicate = format!("LOWER(common) {op} '{bound}'");
            let sql = format!("SELECT id FROM PlannerRow WHERE {predicate} ORDER BY id LIMIT 20");
            let expected = projection_rows(
                &session,
                &format!(
                    "SELECT id FROM PlannerRow WHERE common {op} '{}' ORDER BY id LIMIT 20",
                    bound.to_lowercase()
                ),
            );
            for _ in 0..3 {
                let plan = explain(&session, &predicate);
                assert!(plan.contains("IndexRange(zz_lower_common_idx)"), "{plan}");
                assert_eq!(projection_rows(&session, &sql), expected);
            }
        }
    }
}

#[test]
fn starts_with_ranges_preserve_longer_matches_and_warm_results() {
    let session = initialize();
    // Distinct lengths exercise the expression index's lower-only envelope;
    // residual filtering must remove later nonmatching values inside it.
    for (id, text) in [
        (1, "ev"),
        (2, "every"),
        (3, "everyone"),
        (4, "EVERMORE"),
        (5, "zzz"),
    ] {
        insert_row(&session, id, text, "group-a");
    }
    for (predicate, route, ids) in [
        (
            "STARTS_WITH(common, 'ev')",
            "IndexRange(a_common_idx)",
            vec![1, 2, 3],
        ),
        (
            "STARTS_WITH(LOWER(common), 'EV')",
            "IndexRange(zz_lower_common_idx)",
            vec![1, 2, 3, 4],
        ),
    ] {
        let expected: Vec<_> = ids
            .into_iter()
            .map(|id| vec![OutputValue::nat64(id)])
            .collect();
        let sql = format!("SELECT id FROM PlannerRow WHERE {predicate} ORDER BY id LIMIT 20");
        for _ in 0..3 {
            let plan = explain(&session, predicate);
            assert!(plan.contains(route), "{plan}");
            assert_eq!(projection_rows(&session, &sql), expected);
        }
    }
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
    insert_row(&session, 100, "everyone", "group-a");
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
}

#[test]
fn pinned_route_requires_one_current_eligible_index_identity() {
    let session = initialize();
    seed_rows(&session);
    let request = selective_dynamic_query();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .expect("accepted cursor fixture should resolve");
    let query = crate::db::query::preparation::PreparationWork::run(
        session.db.request_execution_scope(),
        DiagnosticExecutionLane::PublicRead,
        |work| {
            StructuralQuery::new(MissingRowPolicy::Ignore).filter_for_schema(
                catalog.accepted_schema_info(),
                request
                    .filter_expr()
                    .expect("cursor fixture should retain its filter"),
                work,
            )
        },
    )
    .expect("fixture lowering fits request budget")
    .order_spec(OrderSpec {
        fields: vec![asc("id").lower()],
    })
    .select_fields(["id"])
    .limit(3);
    let (prepared, _) = session
        .structural_projection_prepared_plan_for_accepted_authority(
            &query,
            catalog.accepted_entity_authority(),
            catalog.snapshot(),
            DiagnosticExecutionLane::TrustedRead,
        )
        .expect("cursor fixture should prepare");
    let route_pin = prepared
        .logical_plan()
        .cardinality_tiebreak_route_pin()
        .expect("tied fixture should select one route");
    let exact = session
        .structural_projection_prepared_plan_for_accepted_authority_with_route_pin(
            &query,
            catalog.accepted_entity_authority(),
            DiagnosticExecutionLane::TrustedRead,
            route_pin,
        )
        .expect("current route pin should be checked");
    assert!(exact.is_some(), "one eligible route must be selectable");

    let foreign_index = CardinalityTiebreakRoutePin::new(
        IndexId::new_with_generation(ENTITY_TAG, 63, route_pin.index_id().generation()),
        route_pin.family(),
        usize::from(route_pin.consumed_prefix_arity()),
    )
    .expect("foreign index route shape should be structurally valid");
    let foreign_entity = CardinalityTiebreakRoutePin::new(
        IndexId::new_with_generation(
            EntityTag::new(ENTITY_TAG.value() + 1),
            route_pin.index_id().ordinal(),
            route_pin.index_id().generation(),
        ),
        CardinalityTiebreakFamily::Prefix,
        1,
    )
    .expect("foreign entity route shape should be structurally valid");
    for rejected in [foreign_index, foreign_entity] {
        let planned = session
            .structural_projection_prepared_plan_for_accepted_authority_with_route_pin(
                &query,
                catalog.accepted_entity_authority(),
                DiagnosticExecutionLane::TrustedRead,
                rejected,
            )
            .expect("foreign route pin should fail closed without execution");
        assert!(planned.is_none());
    }
}

#[test]
fn pinned_cursor_rejects_a_foreign_accepted_root() {
    let session = initialize();
    seed_rows(&session);
    let query = selective_dynamic_query();
    let first = session
        .execute_trusted_live_page(&query, None)
        .expect("current accepted root should issue a cursor");
    let continuation = first
        .continuation
        .expect("bounded accepted-root fixture should continue");
    let store = session
        .db
        .store_handle(STORE_PATH)
        .expect("accepted-root fixture store should resolve");
    crate::db::commit::publish_accepted_schema_candidate(
        STORE_PATH,
        store,
        AcceptedSchemaRevision::INITIAL,
        &schema_candidate_at(STORE_PATH, AcceptedSchemaRevision::new(2)),
    )
    .expect("replacement accepted root should publish");

    assert!(
        session
            .execute_trusted_live_page(&query, Some(continuation.as_str()))
            .is_err(),
        "a cursor from another accepted root must fail before route execution",
    );
}

#[test]
fn unavailable_fallback_refreshes_only_on_lifecycle_change_and_keeps_cursor_route() {
    let session = initialize_journaled();
    seed_rows(&session);

    let unavailable = explain(&session, "common = 'everyone' AND rare = 'group-a'");
    assert!(
        unavailable.contains("IndexPrefix(a_common_idx)"),
        "{unavailable}"
    );
    assert!(
        unavailable.contains("cardinality_evidence: unavailable"),
        "{unavailable}"
    );
    projection_rows(
        &session,
        "SELECT id FROM PlannerRow WHERE common = 'everyone' \
         AND rare = 'group-a' ORDER BY id LIMIT 20",
    );
    let query = selective_dynamic_query();
    let first = session
        .execute_trusted_live_page(&query, None)
        .expect("unavailable fallback cursor should start");
    let cursor = first
        .continuation
        .expect("bounded unavailable fallback should continue");
    drive_journaled_cardinality_to_ready(&session);

    session
        .execute_trusted_live_page(&query, Some(cursor.as_str()))
        .expect("issued fallback cursor should retain its pinned route");

    projection_rows(
        &session,
        "SELECT id FROM PlannerRow WHERE common = 'everyone' \
         AND rare = 'group-a' ORDER BY id LIMIT 20",
    );
    let ready = explain(&session, "common = 'everyone' AND rare = 'group-a'");
    assert!(ready.contains("IndexPrefix(z_rare_idx)"), "{ready}");
    assert!(
        ready.contains("cardinality_evidence: exact_at_selection"),
        "{ready}"
    );
}

// Reuse accepted session authority for direct evidence-owner qualification.
pub(in crate::db::session) fn ranking_candidates_for_tests() -> (
    DbSession<impl CanisterKind>,
    crate::db::executor::EntityAuthority,
    Vec<crate::db::query::plan::CardinalityTiebreakCandidate<'static>>,
) {
    use crate::{db::predicate::Predicate, value::Value};

    candidate_fixture_for_tests(Predicate::And(vec![
        Predicate::eq("common".into(), Value::Text("everyone".into())),
        Predicate::eq("rare".into(), Value::Text("group-a".into())),
    ]))
}

pub(in crate::db::session) fn probe_candidates_for_tests(
    value_count: usize,
) -> (
    DbSession<impl CanisterKind>,
    crate::db::executor::EntityAuthority,
    Vec<crate::db::query::plan::CardinalityTiebreakCandidate<'static>>,
) {
    candidate_fixture_for_tests(crate::db::predicate::Predicate::in_(
        "common".into(),
        (0..value_count)
            .map(|value| crate::value::Value::Text(format!("probe-{value:04}")))
            .collect(),
    ))
}

fn candidate_fixture_for_tests(
    predicate: crate::db::predicate::Predicate,
) -> (
    DbSession<TestCanister>,
    crate::db::executor::EntityAuthority,
    Vec<crate::db::query::plan::CardinalityTiebreakCandidate<'static>>,
) {
    let session = initialize();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let query = StructuralQuery::new(MissingRowPolicy::Ignore)
        .filter_normalized_predicate(predicate)
        .order_spec(OrderSpec {
            fields: vec![asc("id").lower()],
        })
        .limit(3);
    let (prepared, _) = session
        .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
            catalog.accepted_entity_authority(),
            &catalog,
            &query,
            DiagnosticExecutionLane::PublicRead,
        )
        .unwrap();
    let indexes = session
        .visible_indexes_for_store_accepted_schema(STORE_PATH, catalog.accepted_schema_info())
        .unwrap();
    let candidates = crate::db::query::preparation::with_preparation_work(|work| {
        crate::db::query::plan::exact_cardinality_tiebreak_candidates(
            indexes.accepted_semantic_index_contracts(),
            catalog.accepted_schema_info(),
            prepared.logical_plan(),
            work,
        )
        .map_err(crate::db::QueryError::execute)
    })
    .unwrap()
    .unwrap();

    let candidates = candidates
        .into_iter()
        .map(crate::db::query::plan::CardinalityTiebreakCandidate::into_owned)
        .collect();
    (session, catalog.accepted_entity_authority(), candidates)
}

fn selective_dynamic_query() -> DynamicQuery {
    DynamicQuery::new(ENTITY_NAME)
        .filter(FilterExpr::and(vec![
            FieldRef::new("common").eq(InputValue::text("everyone".to_string())),
            FieldRef::new("rare").eq(InputValue::text("group-a".to_string())),
        ]))
        .select(["id"])
        .order_by(asc("id"))
        .limit(3)
}

// Share one registry reference site so request fixtures retain the same
// recovery/cache identity while supplying independent request budgets.
fn new_request_session(root: &crate::db::RequestExecutionRoot) -> DbSession<TestCanister> {
    DbSession::new(&STORE_REGISTRY, root)
}

fn initialize() -> DbSession<TestCanister> {
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    let session = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
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
    schema_candidate_at(store_path, AcceptedSchemaRevision::INITIAL)
}

fn schema_candidate_at(
    store_path: &str,
    accepted_revision: AcceptedSchemaRevision,
) -> CandidateSchemaRevision {
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
        lower_expression_index(store_path, 6, 6, "zz_lower_common_idx", 2, 1, "common"),
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
        accepted_revision,
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
            DynamicWriteCell::Value(InputValue::nat64(id)),
        ),
        (
            "common".to_string(),
            DynamicWriteCell::Value(InputValue::text(common.to_string())),
        ),
        (
            "rare".to_string(),
            DynamicWriteCell::Value(InputValue::text(rare.to_string())),
        ),
        (
            "wide_fixed".to_string(),
            DynamicWriteCell::Value(InputValue::text("all".to_string())),
        ),
        (
            "wide_branch".to_string(),
            DynamicWriteCell::Value(InputValue::text(branch.to_string())),
        ),
        (
            "selective_fixed".to_string(),
            DynamicWriteCell::Value(InputValue::text(selective_fixed.to_string())),
        ),
        (
            "selective_branch".to_string(),
            DynamicWriteCell::Value(InputValue::text(branch.to_string())),
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

fn lower_expression_index(
    store_path: &str,
    id: u32,
    ordinal: u16,
    name: &str,
    field_id: u32,
    slot: u16,
    field_name: &str,
) -> PersistedIndexSnapshot {
    let source = PersistedIndexFieldPathSnapshot::new(
        FieldId::new(field_id),
        SchemaFieldSlot::new(slot),
        vec![field_name.to_string()],
        AcceptedFieldKind::Text { max_len: None },
        false,
    );
    PersistedIndexSnapshot::new(
        SchemaIndexId::new(id).expect("test index identity should be nonzero"),
        ordinal,
        name.to_string(),
        store_path.to_string(),
        false,
        PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::Expression(
            Box::new(PersistedIndexExpressionSnapshot::new(
                PersistedIndexExpressionOp::Lower,
                source,
                AcceptedFieldKind::Text { max_len: None },
                AcceptedFieldKind::Text { max_len: None },
                format!("expr:v1:LOWER({field_name})"),
            )),
        )]),
        None,
    )
}

fn field_source(field: &str) -> FieldSourceKey {
    FieldSourceKey::try_new(format!("{ENTITY_SOURCE}::{field}"))
        .expect("cardinality fixture field source should admit")
}
