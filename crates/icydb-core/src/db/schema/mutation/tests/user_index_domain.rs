use super::*;

const ENTITY_PATH: &str = "test::MutationEntity";
const STORE_PATH: &str = "test::mutation::entity";

#[test]
fn complete_domain_stage_builds_field_and_expression_projection_without_writes() {
    let field_index = domain_field_index(1, "by_name", false);
    let expression_index = domain_expression_index(2, "by_lower_name", false, None);
    let before = snapshot_with_indexes(&base_snapshot(), vec![field_index.clone()]);
    let after = snapshot_with_indexes(&before, vec![field_index.clone(), expression_index]);
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let rows = [domain_row(2, &second), domain_row(1, &first)];
    let mut store = IndexStore::init_heap();
    insert_projection_entries(
        &mut store,
        &[field_index],
        &[(2, &second), (1, &first)],
        EntityTag::new(7),
    );
    let system_key = unrelated_key(EntityTag::new(7), IndexKeyKind::System, 9, 91);
    let other_entity_key = unrelated_key(EntityTag::new(8), IndexKeyKind::User, 1, 92);
    store.insert(system_key.clone(), IndexEntryValue::presence());
    store.insert(other_entity_key.clone(), IndexEntryValue::presence());
    let physical_before = index_store_entries(&store);

    let staged = super::StagedUserIndexDomainReplacement::stage(
        accepted_identity(&before),
        &before,
        &after,
        None,
        rows,
        &store,
    )
    .unwrap_or_else(|_| panic!("complete domain should stage without mutation"));

    assert_eq!(staged.store_path(), STORE_PATH);
    assert_eq!(staged.entity_tag(), EntityTag::new(7));
    assert_eq!(staged.deletion_keys().len(), 2);
    assert_eq!(staged.final_entries().len(), 4);
    assert_eq!(staged.usage().source_rows(), 2);
    assert!(staged.usage().staged_raw_bytes() > 0);
    assert!(
        staged
            .final_entries()
            .windows(2)
            .all(|pair| pair[0].key() < pair[1].key()),
        "accepted-after entries must be deterministically raw-key sorted",
    );
    let ordinals = staged
        .final_entries()
        .iter()
        .map(|entry| {
            assert_eq!(entry.value(), &IndexEntryValue::presence());
            IndexKey::try_from_raw(entry.key())
                .expect("staged key should decode")
                .index_id()
                .ordinal()
        })
        .collect::<Vec<_>>();
    assert_eq!(ordinals, vec![1, 1, 2, 2]);
    assert_eq!(index_store_entries(&store), physical_before);
    assert_eq!(store.get(&system_key), Some(IndexEntryValue::presence()));
    assert_eq!(
        store.get(&other_entity_key),
        Some(IndexEntryValue::presence()),
    );
    assert_eq!(store.state(), IndexState::Ready);
}

#[test]
fn complete_domain_stage_rejects_physical_before_projection_mismatch() {
    let field_index = domain_field_index(1, "by_name", false);
    let before = snapshot_with_indexes(&base_snapshot(), vec![field_index.clone()]);
    let after = snapshot_with_indexes(&before, vec![field_index]);
    let row = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let store = IndexStore::init_heap();

    let result = super::StagedUserIndexDomainReplacement::stage(
        accepted_identity(&before),
        &before,
        &after,
        None,
        [domain_row(1, &row)],
        &store,
    );

    assert!(matches!(
        result,
        Err(super::StagedUserIndexDomainError::CurrentDomainMismatch),
    ));
    assert!(store.is_empty());
    assert_eq!(store.state(), IndexState::Ready);
}

#[test]
fn complete_domain_stage_rejects_index_owned_by_another_store() {
    let before = base_snapshot();
    let foreign_index = PersistedIndexSnapshot::new(
        1,
        "by_name".to_string(),
        "test::mutation::foreign".to_string(),
        false,
        PersistedIndexKeySnapshot::FieldPath(vec![name_key_path()]),
        None,
    );
    let after = snapshot_with_indexes(&before, vec![foreign_index]);
    let row = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let store = IndexStore::init_heap();

    let result = super::StagedUserIndexDomainReplacement::stage(
        accepted_identity(&before),
        &before,
        &after,
        None,
        [domain_row(1, &row)],
        &store,
    );

    assert!(matches!(
        result,
        Err(super::StagedUserIndexDomainError::AcceptedIndexStoreMismatch),
    ));
    assert!(store.is_empty());
    assert_eq!(store.state(), IndexState::Ready);
}

#[test]
fn complete_domain_stage_rejects_duplicate_unique_expression_components() {
    let before = base_snapshot();
    let after = snapshot_with_indexes(
        &before,
        vec![domain_expression_index(1, "unique_lower_name", true, None)],
    );
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let duplicate = RebuildSlotReader {
        values: vec![None, Some(Value::Text("ada".to_string()))],
    };
    let store = IndexStore::init_heap();

    let result = super::StagedUserIndexDomainReplacement::stage(
        accepted_identity(&before),
        &before,
        &after,
        None,
        [domain_row(1, &first), domain_row(2, &duplicate)],
        &store,
    );

    assert!(matches!(
        result,
        Err(super::StagedUserIndexDomainError::DuplicateUniqueKey),
    ));
    assert!(store.is_empty());
    assert_eq!(store.state(), IndexState::Ready);
}

#[test]
fn complete_domain_stage_preserves_mixed_key_component_order() {
    let before = base_snapshot();
    let after = snapshot_with_indexes(&before, vec![domain_mixed_index(1, "by_name_and_lower")]);
    let row = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let store = IndexStore::init_heap();

    let staged = super::StagedUserIndexDomainReplacement::stage(
        accepted_identity(&before),
        &before,
        &after,
        None,
        [domain_row(1, &row)],
        &store,
    )
    .unwrap_or_else(|_| panic!("mixed accepted index should stage"));

    assert_eq!(staged.final_entries().len(), 1);
    let key = IndexKey::try_from_raw(staged.final_entries()[0].key())
        .expect("mixed staged key should decode");
    assert_eq!(key.component_count(), 2);
    assert_ne!(key.component(0), key.component(1));
    assert!(store.is_empty());
}

#[test]
fn complete_domain_stage_requires_row_contract_for_filtered_indexes() {
    let before = base_snapshot();
    let after = snapshot_with_indexes(
        &before,
        vec![domain_expression_index(
            1,
            "filtered_lower_name",
            false,
            Some("name IS NOT NULL".to_string()),
        )],
    );
    let store = IndexStore::init_heap();

    let result = super::StagedUserIndexDomainReplacement::stage(
        accepted_identity(&before),
        &before,
        &after,
        None,
        [],
        &store,
    );

    assert!(matches!(
        result,
        Err(super::StagedUserIndexDomainError::MissingPredicateRowContract),
    ));
    assert!(store.is_empty());
}

#[test]
fn complete_domain_stage_derives_drop_ordinal_compaction_as_final_projection() {
    let first_index = domain_field_index(1, "by_name", false);
    let dropped_index = domain_expression_index(2, "by_lower_name", false, None);
    let last_index = domain_field_index(3, "by_name_copy", false);
    let compacted_last_index = domain_field_index(2, "by_name_copy", false);
    let before = snapshot_with_indexes(
        &base_snapshot(),
        vec![
            first_index.clone(),
            dropped_index.clone(),
            last_index.clone(),
        ],
    );
    let after = snapshot_with_indexes(&before, vec![first_index.clone(), compacted_last_index]);
    let first = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Ada".to_string()))],
    };
    let second = RebuildSlotReader {
        values: vec![None, Some(Value::Text("Grace".to_string()))],
    };
    let mut store = IndexStore::init_heap();
    insert_projection_entries(
        &mut store,
        &[first_index, dropped_index, last_index],
        &[(1, &first), (2, &second)],
        EntityTag::new(7),
    );
    let physical_before = index_store_entries(&store);

    let staged = super::StagedUserIndexDomainReplacement::stage(
        accepted_identity(&before),
        &before,
        &after,
        None,
        [domain_row(1, &first), domain_row(2, &second)],
        &store,
    )
    .unwrap_or_else(|_| panic!("drop projection should stage without ordinal remap writes"));

    assert_eq!(staged.deletion_keys().len(), 6);
    assert_eq!(staged.final_entries().len(), 4);
    let final_ordinals = staged
        .final_entries()
        .iter()
        .map(|entry| {
            IndexKey::try_from_raw(entry.key())
                .expect("staged key should decode")
                .index_id()
                .ordinal()
        })
        .collect::<Vec<_>>();
    assert_eq!(final_ordinals, vec![1, 1, 2, 2]);
    assert_eq!(index_store_entries(&store), physical_before);
}

fn accepted_identity(
    snapshot: &PersistedSchemaSnapshot,
) -> crate::db::schema::AcceptedCatalogIdentity {
    let fingerprint =
        crate::db::schema::accepted_schema_cache_fingerprint_for_persisted_snapshot(snapshot)
            .expect("test snapshot fingerprint should derive");
    crate::db::schema::AcceptedCatalogIdentity::new(
        EntityTag::new(7),
        ENTITY_PATH,
        STORE_PATH,
        crate::db::schema::AcceptedSchemaRevision::INITIAL,
        snapshot.version(),
        fingerprint,
    )
}

fn domain_row(primary_key: u64, slots: &RebuildSlotReader) -> super::SchemaUserIndexDomainRow<'_> {
    super::SchemaUserIndexDomainRow::new(PrimaryKeyComponent::Nat64(primary_key), slots, 32)
}

fn domain_field_index(ordinal: u16, name: &str, unique: bool) -> PersistedIndexSnapshot {
    PersistedIndexSnapshot::new(
        ordinal,
        name.to_string(),
        STORE_PATH.to_string(),
        unique,
        PersistedIndexKeySnapshot::FieldPath(vec![name_key_path()]),
        None,
    )
}

fn domain_expression_index(
    ordinal: u16,
    name: &str,
    unique: bool,
    predicate: Option<String>,
) -> PersistedIndexSnapshot {
    PersistedIndexSnapshot::new(
        ordinal,
        name.to_string(),
        STORE_PATH.to_string(),
        unique,
        PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::Expression(
            Box::new(PersistedIndexExpressionSnapshot::new(
                PersistedIndexExpressionOp::Lower,
                name_key_path(),
                AcceptedFieldKind::Text { max_len: None },
                AcceptedFieldKind::Text { max_len: None },
                "expr:v1:LOWER(name)".to_string(),
            )),
        )]),
        predicate,
    )
}

fn domain_mixed_index(ordinal: u16, name: &str) -> PersistedIndexSnapshot {
    PersistedIndexSnapshot::new(
        ordinal,
        name.to_string(),
        STORE_PATH.to_string(),
        false,
        PersistedIndexKeySnapshot::Items(vec![
            PersistedIndexKeyItemSnapshot::FieldPath(name_key_path()),
            PersistedIndexKeyItemSnapshot::Expression(Box::new(
                PersistedIndexExpressionSnapshot::new(
                    PersistedIndexExpressionOp::Lower,
                    name_key_path(),
                    AcceptedFieldKind::Text { max_len: None },
                    AcceptedFieldKind::Text { max_len: None },
                    "expr:v1:LOWER(name)".to_string(),
                ),
            )),
        ]),
        None,
    )
}

fn insert_projection_entries(
    store: &mut IndexStore,
    indexes: &[PersistedIndexSnapshot],
    rows: &[(u64, &RebuildSlotReader)],
    entity_tag: EntityTag,
) {
    for index in indexes {
        let request = if index.key().is_field_path_only() {
            SchemaMutationRequest::from_accepted_field_path_index(index)
        } else {
            SchemaMutationRequest::from_accepted_expression_index(index)
        }
        .expect("accepted test index should lower");
        for (primary_key, slots) in rows {
            let key = match &request {
                SchemaMutationRequest::AddFieldPathIndex { target } => {
                    IndexKey::new_from_slots_with_field_path_rebuild_target(
                        entity_tag,
                        PrimaryKeyComponent::Nat64(*primary_key),
                        target,
                        *slots,
                    )
                }
                SchemaMutationRequest::AddExpressionIndex { target } => {
                    IndexKey::new_from_slots_with_expression_rebuild_target(
                        entity_tag,
                        PrimaryKeyComponent::Nat64(*primary_key),
                        target,
                        *slots,
                    )
                }
                SchemaMutationRequest::ExactMatch | SchemaMutationRequest::AppendOnlyFields(_) => {
                    panic!("test index should lower to an index target")
                }
            }
            .expect("test key should derive")
            .expect("test value should be indexable")
            .to_raw()
            .expect("test key should encode");
            assert_eq!(store.insert(key, IndexEntryValue::presence()), None);
        }
    }
}

fn unrelated_key(
    entity_tag: EntityTag,
    kind: IndexKeyKind,
    ordinal: u16,
    primary_key: u64,
) -> RawIndexStoreKey {
    IndexKey::new_from_components_with_primary_key_value(
        &IndexId::new(entity_tag, ordinal),
        kind,
        &[b"unrelated"],
        &PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(primary_key)),
    )
    .expect("unrelated test key should build")
    .to_raw()
    .expect("unrelated test key should encode")
}

fn index_store_entries(store: &IndexStore) -> Vec<(RawIndexStoreKey, IndexEntryValue)> {
    let mut entries = Vec::new();
    let result: Result<(), std::convert::Infallible> = store.visit_entries(|key, value| {
        entries.push((key.clone(), value.clone()));
        Ok(crate::db::index::IndexStoreVisit::Continue)
    });
    result.unwrap_or_else(|never| match never {});
    entries
}
