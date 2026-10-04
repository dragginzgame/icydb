//! Mixed-length Principal indexes retain semantic ordering and complete pages.

use super::*;
use crate::{
    db::{RequestExecutionRoot, desc},
    types::Principal,
};

fn principals() -> Vec<Principal> {
    [
        vec![],
        vec![255],
        vec![0],
        vec![4],
        vec![1, 0],
        vec![0, 255],
        vec![255; 10],
        vec![2; 29],
        vec![0; 29],
        vec![255; 29],
    ]
    .iter()
    .map(|bytes| Principal::from_slice(bytes))
    .collect()
}

fn principal_field(id: u32, name: &str, slot: u16, nullable: bool) -> PersistedFieldSnapshot {
    let kind = AcceptedFieldKind::Principal;
    PersistedFieldSnapshot::new_initial(
        FieldId::new(id),
        name.into(),
        SchemaFieldSlot::new(slot),
        kind.clone(),
        Vec::new(),
        nullable,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        kind.leaf_codec_for_storage(FieldStorageDecode::ByKind),
    )
}

fn index(
    unique: bool,
    items: &[(u32, u16, &str, AcceptedFieldKind, bool)],
) -> PersistedIndexSnapshot {
    PersistedIndexSnapshot::new(
        SchemaIndexId::new(1).unwrap(),
        1,
        "principal_idx".into(),
        STORE_PATH.into(),
        unique,
        PersistedIndexKeySnapshot::FieldPath(
            items
                .iter()
                .map(|(id, slot, name, kind, nullable)| {
                    PersistedIndexFieldPathSnapshot::new(
                        FieldId::new(*id),
                        SchemaFieldSlot::new(*slot),
                        vec![(*name).into()],
                        kind.clone(),
                        *nullable,
                    )
                })
                .collect(),
        ),
        None,
    )
}

fn seed_principals(unique: bool) -> Vec<(Principal, u64)> {
    scalar_page_limits::initialize_payload_schema(
        vec![
            field(1, "id", 0, AcceptedFieldKind::Nat64),
            principal_field(2, "owner", 1, !unique),
            principal_field(3, "mirror", 2, !unique),
        ],
        vec![index(
            unique,
            &[(2, 1, "owner", AcceptedFieldKind::Principal, !unique)],
        )],
    );
    let mut values = principals();
    if !unique {
        values.extend([Principal::from_slice(&[4]); 6]);
    }
    let mut rows = Vec::new();
    for (offset, owner) in values.into_iter().enumerate() {
        let id = 100 - u64::try_from(offset).unwrap();
        new_request_session(&RequestExecutionRoot::__new_runtime_root())
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                    (
                        "owner".into(),
                        DynamicWriteCell::Value(InputValue::principal(owner)),
                    ),
                    (
                        "mirror".into(),
                        DynamicWriteCell::Value(InputValue::principal(owner)),
                    ),
                ])],
            )
            .unwrap();
        rows.push((owner, id));
    }
    if !unique {
        new_request_session(&RequestExecutionRoot::__new_runtime_root())
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(101))),
                    ("owner".into(), DynamicWriteCell::Value(InputValue::null())),
                    ("mirror".into(), DynamicWriteCell::Value(InputValue::null())),
                ])],
            )
            .unwrap();
    }
    rows.sort_unstable();
    rows
}

fn collect_pages(
    query: &DynamicQuery,
    public: bool,
    mut cursor: Option<String>,
) -> (Vec<Vec<OutputValue>>, Vec<(String, usize)>) {
    let mut rows = Vec::new();
    let mut tokens = Vec::new();
    for _ in 0..32 {
        let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
        let page = if public {
            session.execute_public_live_page(query, cursor.as_deref())
        } else {
            session.execute_trusted_live_page(query, cursor.as_deref())
        }
        .unwrap();
        rows.extend(page.rows);
        let Some(next) = page.continuation else {
            return (rows, tokens);
        };
        assert_ne!(Some(&next), cursor.as_ref());
        tokens.push((next.clone(), rows.len()));
        cursor = Some(next);
    }
    panic!("Principal pages must exhaust within the bounded page count");
}

fn assert_index_access(query: &DynamicQuery, range: bool) {
    let session = new_request_session(&RequestExecutionRoot::__new_runtime_root());
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
            .map(crate::db::OrderTerm::lower)
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
    if range {
        assert!(access.as_index_range_path().is_some());
    } else {
        assert!(
            access.as_index_prefix_contract_path().is_some()
                || access.as_index_multi_lookup_contract_path().is_some()
        );
    }
}

fn assert_principal_ranges(unique: bool) {
    let rows = seed_principals(unique);
    for cache_bytes in [0, 4 * 1024 * 1024] {
        new_request_session(&RequestExecutionRoot::__new_runtime_root())
            .clear_shared_query_cache_for_tests(cache_bytes);
        for bound in principals() {
            for op in 0..6 {
                let matches = |value: Principal| match op {
                    0 => value == bound,
                    1 => value != bound,
                    2 => value < bound,
                    3 => value <= bound,
                    4 => value > bound,
                    _ => value >= bound,
                };
                let filter = |name: &'static str| {
                    let field = FieldRef::new(name);
                    let value = InputValue::principal(bound);
                    match op {
                        0 => field.eq(value),
                        1 => field.ne(value),
                        2 => field.lt(value),
                        3 => field.lte(value),
                        4 => field.gt(value),
                        _ => field.gte(value),
                    }
                };
                for descending in [false, true] {
                    let order = if descending {
                        desc("owner")
                    } else {
                        asc("owner")
                    };
                    let query = DynamicQuery::new(ENTITY_NAME)
                        .filter(filter("owner"))
                        .select(["id", "owner"])
                        .order_by(order.clone());
                    let control = DynamicQuery::new(ENTITY_NAME)
                        .filter(filter("mirror"))
                        .select(["id", "owner"])
                        .order_by(order);
                    if op != 1 {
                        assert_index_access(&query, op >= 2);
                    }
                    let mut expected = rows
                        .iter()
                        .filter(|(owner, _)| matches(*owner))
                        .map(|(owner, id)| {
                            vec![OutputValue::nat64(*id), OutputValue::principal(*owner)]
                        })
                        .collect::<Vec<_>>();
                    if descending {
                        expected.reverse();
                    }
                    assert_eq!(collect_pages(&control, false, None).0, expected);
                    // NE cannot prove selective access on its own; preserve the
                    // public full-scan rejection rather than changing admission.
                    for public in [false, true]
                        .into_iter()
                        .filter(|public| !public || op != 1)
                    {
                        for _ in 0..2 {
                            let (actual, tokens) = collect_pages(&query, public, None);
                            assert_eq!(
                                actual, expected,
                                "unique={unique} op={op} bound={bound:?} descending={descending}"
                            );
                            for (token, offset) in tokens {
                                assert_eq!(
                                    collect_pages(&query, public, Some(token)).0,
                                    expected[offset..]
                                );
                            }
                        }
                        assert_eq!(
                            collect_pages(&query.clone().limit(1), public, None).0,
                            expected[..expected.len().min(1)]
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn nonunique_principal_ranges_match_row_scans_and_every_resume_suffix() {
    assert_principal_ranges(false);
}

#[test]
fn unique_principal_ranges_match_row_scans_and_every_resume_suffix() {
    assert_principal_ranges(true);
}

#[test]
fn principal_primary_key_suffix_merges_preserve_complete_order() {
    scalar_page_limits::initialize_payload_schema(
        vec![
            field(1, "id", 0, AcceptedFieldKind::Principal),
            field(2, "bucket", 1, AcceptedFieldKind::Nat64),
        ],
        vec![index(
            false,
            &[
                (2, 1, "bucket", AcceptedFieldKind::Nat64, false),
                (1, 0, "id", AcceptedFieldKind::Principal, false),
            ],
        )],
    );
    let mut rows = principals();
    rows.sort_unstable();
    for (offset, &id) in rows.iter().enumerate() {
        let bucket = u64::try_from(offset % 2).unwrap();
        new_request_session(&RequestExecutionRoot::__new_runtime_root())
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    (
                        "id".into(),
                        DynamicWriteCell::Value(InputValue::principal(id)),
                    ),
                    (
                        "bucket".into(),
                        DynamicWriteCell::Value(InputValue::nat64(bucket)),
                    ),
                ])],
            )
            .unwrap();
    }
    for descending in [false, true] {
        let query = DynamicQuery::new(ENTITY_NAME)
            .filter(FieldRef::new("bucket").in_list([1_u64, 0, 1]))
            .select(["id"])
            .order_by(if descending { desc("id") } else { asc("id") });
        let mut expected = rows
            .iter()
            .map(|id| vec![OutputValue::principal(*id)])
            .collect::<Vec<_>>();
        if descending {
            expected.reverse();
        }
        for public in [false, true] {
            let (actual, tokens) = collect_pages(&query, public, None);
            assert_eq!(actual, expected);
            for (token, offset) in tokens {
                assert_eq!(
                    collect_pages(&query, public, Some(token)).0,
                    expected[offset..]
                );
            }
        }
    }
}
