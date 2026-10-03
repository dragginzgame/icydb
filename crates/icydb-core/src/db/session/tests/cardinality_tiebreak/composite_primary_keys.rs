//! Accepted wide composite identities remain readable through secondary indexes.

use super::*;
use crate::{
    db::{OrderTerm, RequestExecutionRoot, desc},
    types::{Account, Principal, Subaccount},
};

fn initialize_composite_store(key_fields: &[&str], kind: &AcceptedFieldKind) {
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
    session.db.drive_startup_recovery_page().unwrap();
    let mut fields = key_fields
        .iter()
        .enumerate()
        .map(|(slot, name)| {
            field(
                u32::try_from(slot + 1).unwrap(),
                name,
                u16::try_from(slot).unwrap(),
                kind.clone(),
            )
        })
        .collect::<Vec<_>>();
    let first_index_slot = u16::try_from(fields.len()).unwrap();
    fields.push(field(
        u32::from(first_index_slot) + 1,
        "rank",
        first_index_slot,
        AcceptedFieldKind::Nat64,
    ));
    fields.push(field(
        u32::from(first_index_slot) + 2,
        "band",
        first_index_slot + 1,
        AcceptedFieldKind::Nat64,
    ));
    let indexes = [("rank", true), ("band", false)]
        .into_iter()
        .enumerate()
        .map(|(offset, (name, unique))| {
            let slot = first_index_slot + u16::try_from(offset).unwrap();
            PersistedIndexSnapshot::new(
                SchemaIndexId::new(u32::try_from(offset + 1).unwrap()).unwrap(),
                u16::try_from(offset + 1).unwrap(),
                format!("{name}_idx"),
                STORE_PATH.into(),
                unique,
                PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                    FieldId::new(u32::from(slot) + 1),
                    SchemaFieldSlot::new(slot),
                    vec![name.into()],
                    AcceptedFieldKind::Nat64,
                    false,
                )]),
                None,
            )
        })
        .collect();
    let bindings = fields
        .iter()
        .map(|field| ((ENTITY_TAG, field_source(field.name())), field.id()))
        .collect();
    let snapshot = PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        ENTITY_SOURCE.into(),
        ENTITY_NAME.into(),
        (1..=key_fields.len())
            .map(|id| FieldId::new(u32::try_from(id).unwrap()))
            .collect::<Vec<_>>(),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
        indexes,
    );
    let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        BTreeMap::from([(ENTITY_TAG, snapshot)]),
        bindings,
    );
    crate::db::commit::publish_accepted_schema_candidate(
        STORE_PATH,
        session.db.store_handle(STORE_PATH).unwrap(),
        AcceptedSchemaRevision::NONE,
        &candidate,
    )
    .unwrap();
}

const fn key_values(seed: u8, account: bool) -> (InputValue, OutputValue) {
    let owner = Principal::from_slice(&[seed; 29]);
    if account {
        let value =
            Account::from_owner_and_subaccount(owner, Some(Subaccount::from_array([seed; 32])));
        (InputValue::account(value), OutputValue::account(value))
    } else {
        (InputValue::principal(owner), OutputValue::principal(owner))
    }
}

fn collect_pages(
    query: &DynamicQuery,
    mut cursor: Option<String>,
) -> (Vec<Vec<OutputValue>>, Vec<(String, usize)>) {
    let mut rows = Vec::new();
    let mut tokens = Vec::new();
    for _ in 0..32 {
        let page = new_request_session(&RequestExecutionRoot::__new_runtime_root())
            .execute_trusted_live_page(query, cursor.as_deref())
            .unwrap();
        rows.extend(page.rows);
        let Some(next) = page.continuation else {
            return (rows, tokens);
        };
        assert_ne!(Some(&next), cursor.as_ref());
        assert!(!tokens.iter().any(|(token, _)| token == &next));
        tokens.push((next.clone(), rows.len()));
        cursor = Some(next);
    }
    panic!("composite index reads must exhaust within the bounded page count");
}

fn assert_paged_rows(query: &DynamicQuery, expected: &[Vec<OutputValue>]) {
    // Prove these accepted queries consume index suffixes rather than passing
    // through a full row scan that would mask a strict index-decoder defect.
    let root = RequestExecutionRoot::__new_runtime_root();
    let session = new_request_session(&root);
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let structural = crate::db::query::preparation::with_preparation_work(|work| {
        StructuralQuery::new(MissingRowPolicy::Ignore).filter_for_schema(
            catalog.accepted_schema_info(),
            query.filter_expr().unwrap(),
            work,
        )
    })
    .unwrap()
    .order_spec(OrderSpec {
        fields: query
            .order_terms()
            .iter()
            .cloned()
            .map(OrderTerm::lower)
            .collect(),
    });
    let (prepared, _) = session
        .cached_shared_query_plan_for_accepted_authority_with_catalog_and_reuse(
            catalog.accepted_entity_authority(),
            &catalog,
            &structural,
            DiagnosticExecutionLane::TrustedRead,
        )
        .unwrap();
    let access = &prepared.logical_plan().access;
    assert!(
        access.as_index_range_path().is_some() || access.as_index_prefix_contract_path().is_some()
    );
    // The maintained secondary order must resume a complete wide PK suffix
    // before consuming the next page's row budget.
    let mut cursor = None;
    let mut bounded_rows = Vec::new();
    for _ in 0..32 {
        let root = super::secondary_order::bounded_secondary_request(3);
        let page = new_request_session(&root)
            .execute_trusted_live_page(query, cursor.as_deref())
            .unwrap();
        bounded_rows.extend(page.rows);
        cursor = page.continuation;
        if cursor.is_none() {
            break;
        }
    }
    assert!(cursor.is_none());
    assert_eq!(bounded_rows, expected);
    let (actual, tokens) = collect_pages(query, None);
    assert_eq!(actual, expected);
    assert!(!tokens.is_empty());
    for (token, offset) in tokens {
        assert_eq!(collect_pages(query, Some(token)).0, expected[offset..]);
    }
    let limited = new_request_session(&RequestExecutionRoot::__new_runtime_root())
        .execute_trusted_live_page(&query.clone().limit(1), None)
        .unwrap();
    assert_eq!(limited.rows, expected[..1]);
    assert!(limited.continuation.is_none());
}

fn assert_composite_index_reads(account: bool) {
    let key_fields: &[&str] = if account {
        &["a", "b", "c", "d"]
    } else {
        &["owner", "spender"]
    };
    let kind = if account {
        AcceptedFieldKind::Account
    } else {
        AcceptedFieldKind::Principal
    };
    initialize_composite_store(key_fields, &kind);
    let mut expected = Vec::new();
    for seed in 1_u8..=13 {
        let (input, output) = key_values(seed, account);
        let rank = 14 - u64::from(seed);
        let mut cells = key_fields
            .iter()
            .map(|name| ((*name).into(), DynamicWriteCell::Value(input.clone())))
            .collect::<Vec<_>>();
        cells.extend([
            (
                "rank".into(),
                DynamicWriteCell::Value(InputValue::nat64(rank)),
            ),
            ("band".into(), DynamicWriteCell::Value(InputValue::nat64(5))),
        ]);
        new_request_session(&RequestExecutionRoot::__new_runtime_root())
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(cells)],
            )
            .expect("accepted wide primary key must insert with secondary indexes");
        let mut row = vec![output; key_fields.len()];
        row.extend([OutputValue::nat64(rank), OutputValue::nat64(5)]);
        expected.push(row);
    }
    // A different complete primary identity must still conflict on the unique index.
    let (input, _) = key_values(20, account);
    let mut cells = key_fields
        .iter()
        .map(|name| ((*name).into(), DynamicWriteCell::Value(input.clone())))
        .collect::<Vec<_>>();
    cells.extend([
        ("rank".into(), DynamicWriteCell::Value(InputValue::nat64(7))),
        ("band".into(), DynamicWriteCell::Value(InputValue::nat64(5))),
    ]);
    let conflict = new_request_session(&RequestExecutionRoot::__new_runtime_root())
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![DynamicStructuralPatch::new(cells)])
        .expect_err("unique index must detect a retained wide identity");
    assert_eq!(
        conflict.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION
    );
    for rank in 1..=13 {
        let query = DynamicQuery::new(ENTITY_NAME)
            .filter(FieldRef::new("rank").eq(InputValue::nat64(rank)));
        for _ in 0..2 {
            assert_eq!(
                collect_pages(&query, None).0,
                [expected[usize::try_from(13 - rank).unwrap()].clone()]
            );
        }
    }
    for descending in [false, true] {
        let query = DynamicQuery::new(ENTITY_NAME)
            .filter(FieldRef::new("band").eq(InputValue::nat64(5)))
            .order_by(if descending {
                desc("band")
            } else {
                asc("band")
            });
        let mut ordered = expected.clone();
        if descending {
            ordered.reverse();
        }
        assert_paged_rows(&query, &ordered);
        let query = DynamicQuery::new(ENTITY_NAME)
            .filter(FilterExpr::and(vec![
                FieldRef::new("rank").gte(InputValue::nat64(3)),
                FieldRef::new("rank").lt(InputValue::nat64(10)),
            ]))
            .order_by(if descending {
                desc("rank")
            } else {
                asc("rank")
            });
        let mut ordered = expected[4..11].to_vec();
        if !descending {
            ordered.reverse();
        }
        assert_paged_rows(&query, &ordered);
    }
}

#[test]
fn maximum_composite_account_keys_support_indexed_writes_and_resumed_reads() {
    assert_composite_index_reads(true);
}

#[test]
fn full_width_composite_principal_keys_support_indexed_writes_and_resumed_reads() {
    assert_composite_index_reads(false);
}
