//! Journaled nested-relation admission and interruption recovery contracts.
//! Reuses the session boundary's accepted-catalog and stable-store fixtures.

use super::*;
use crate::db::{
    key_taxonomy::{EncodedPrimaryKey, PrimaryKeyComponent, PrimaryKeyValue, RawIndexStoreKey},
    schema::PersistedRelationPathStepSnapshot,
};

fn nested_source_snapshot() -> PersistedSchemaSnapshot {
    use PersistedRelationPathStepSnapshot::{ListItems, MapValues, SetItems};

    let base = identity_snapshot_for_entity(
        JOURNALED_STORE_PATH,
        false,
        false,
        false,
        SECOND_ENTITY_SOURCE,
        SECOND_ENTITY_NAME,
        None,
    );
    let list = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64));
    let mut fields = base.fields().to_vec();
    let mut relations = Vec::new();
    for (id, name, kind, steps) in [
        (3, "list", list.clone(), vec![ListItems]),
        (
            4,
            "set",
            AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Nat64)),
            vec![SetItems],
        ),
        (
            5,
            "map",
            AcceptedFieldKind::Map {
                key: Box::new(AcceptedFieldKind::Nat64),
                value: Box::new(list),
            },
            vec![MapValues, ListItems],
        ),
    ] {
        fields.push(PersistedFieldSnapshot::new_initial(
            FieldId::new(id),
            name.to_string(),
            SchemaFieldSlot::new(u16::try_from(id - 1).expect("fixture slot should fit")),
            kind,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Structural,
        ));
        relations.push(PersistedRelationEdgeSnapshot::new_nested(
            RelationId::new(id).expect("fixture relation ID should admit"),
            name.to_string(),
            ENTITY_SOURCE.to_string(),
            FieldId::new(id),
            steps,
        ));
    }
    let snapshot = PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        SECOND_ENTITY_SOURCE.to_string(),
        SECOND_ENTITY_NAME.to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
        base.indexes().to_vec(),
    )
    .with_relations(relations);
    let constraints = AcceptedConstraintCatalog::initial(
        snapshot.fields(),
        snapshot.indexes(),
        snapshot.relations(),
    )
    .expect("nested constraint catalog should admit");
    snapshot.with_constraint_catalog(constraints)
}

fn initialize_nested() -> DbSession<JournaledTestCanister> {
    let root = crate::db::RequestExecutionRoot::__new_runtime_root();
    let session = DbSession::<JournaledTestCanister>::new(&JOURNALED_STORE_REGISTRY, &root);
    drive_journaled_recovery_to_completion(&session);
    let mut bindings = BTreeMap::from([
        ((ENTITY_TAG, source_key(ID_SOURCE)), FieldId::new(1)),
        ((ENTITY_TAG, source_key(PAYLOAD_SOURCE)), FieldId::new(2)),
        (
            (SECOND_ENTITY_TAG, source_key(SECOND_ID_SOURCE)),
            FieldId::new(1),
        ),
        (
            (SECOND_ENTITY_TAG, source_key(SECOND_PAYLOAD_SOURCE)),
            FieldId::new(2),
        ),
    ]);
    for (id, name) in [(3, "list"), (4, "set"), (5, "map")] {
        bindings.insert(
            (
                SECOND_ENTITY_TAG,
                source_key(&format!("{SECOND_ENTITY_SOURCE}::{name}")),
            ),
            FieldId::new(id),
        );
    }
    let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        JOURNALED_STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        BTreeMap::from([
            (ENTITY_TAG, identity_snapshot(JOURNALED_STORE_PATH, false)),
            (SECOND_ENTITY_TAG, nested_source_snapshot()),
        ]),
        bindings,
    );
    crate::db::commit::publish_accepted_schema_candidate(
        JOURNALED_STORE_PATH,
        session
            .db
            .store_handle(JOURNALED_STORE_PATH)
            .expect("journaled store should resolve"),
        AcceptedSchemaRevision::NONE,
        &candidate,
    )
    .expect("nested accepted catalog should publish");
    drive_journaled_recovery_to_completion(&session);
    session
}

fn nested_patch(targets: &[u64]) -> DynamicStructuralPatch {
    let values = || InputValue::list(targets.iter().copied().map(InputValue::nat64).collect());
    let mut distinct = targets.to_vec();
    distinct.sort_unstable();
    distinct.dedup();
    DynamicStructuralPatch::new(vec![
        (
            "payload".to_string(),
            DynamicWriteCell::Value(InputValue::nat64(100)),
        ),
        ("list".to_string(), DynamicWriteCell::Value(values())),
        (
            "set".to_string(),
            DynamicWriteCell::Value(InputValue::list(
                distinct.into_iter().map(InputValue::nat64).collect(),
            )),
        ),
        (
            "map".to_string(),
            DynamicWriteCell::Value(InputValue::map(vec![(InputValue::nat64(0), values())])),
        ),
    ])
}

// Inspect physical reverse entries as well as live queries: recovery must not
// retain obsolete edges or multiply deduplicated occurrences.
fn reverse_entries() -> Vec<Vec<u8>> {
    JOURNALED_INDEX_STORE.with(|store| {
        let mut entries = Vec::new();
        store
            .borrow()
            .visit_entries(|raw_key, _| {
                let key = IndexKey::try_from_raw(raw_key).expect("reverse key should decode");
                if key.key_kind() == IndexKeyKind::System {
                    entries.push(raw_key.as_bytes().to_vec());
                }
                Ok::<_, InternalError>(IndexStoreVisit::Continue)
            })
            .expect("reverse entries should remain readable");
        entries
    })
}

fn source_rows(session: &DbSession<JournaledTestCanister>) -> Vec<Vec<OutputValue>> {
    session
        .execute_trusted_live_page(
            &DynamicQuery::new(SECOND_ENTITY_NAME)
                .select(["id", "payload", "list", "set", "map"])
                .order_by(crate::db::asc("id"))
                .limit(64),
            None,
        )
        .expect("nested source should remain queryable")
        .rows
}

fn assert_nested_state(session: &DbSession<JournaledTestCanister>, targets: &[u64]) {
    let mut expected_row = vec![OutputValue::nat64(1)];
    for (_, cell) in nested_patch(targets).fields() {
        let DynamicWriteCell::Value(value) = cell else {
            panic!("nested fixture authors concrete values");
        };
        expected_row.push(OutputValue::from_public(value.clone().into_public()));
    }
    assert_eq!(source_rows(session), vec![expected_row]);
    let mut actual = Vec::new();
    for bytes in reverse_entries() {
        let raw = RawIndexStoreKey::from_persisted_bytes(bytes);
        let key = IndexKey::try_from_raw(&raw).expect("reverse key should decode");
        let decoded = raw.decode().expect("source identity should decode");
        assert_eq!(
            decoded
                .primary_key()
                .decode()
                .expect("source key should decode"),
            PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(1))
        );
        assert_eq!(key.index_id().entity_tag(), SECOND_ENTITY_TAG);
        actual.push((
            key.component(0)
                .expect("relation domain should exist")
                .to_vec(),
            key.component(1)
                .expect("target identity should exist")
                .to_vec(),
        ));
    }
    let mut expected = Vec::new();
    for relation in 3_u32..=5 {
        for target in targets {
            let encoded = EncodedPrimaryKey::encode(PrimaryKeyValue::Scalar(
                PrimaryKeyComponent::Nat64(*target),
            ))
            .expect("target key should encode");
            expected.push((relation.to_be_bytes().to_vec(), encoded.as_bytes().to_vec()));
        }
    }
    actual.sort();
    expected.sort();
    expected.dedup();
    assert_eq!(
        actual, expected,
        "each accepted relation must have exactly its current targets"
    );
}

fn delete_target(
    session: &DbSession<JournaledTestCanister>,
    id: u64,
) -> Result<crate::db::DynamicMutationResult, InternalError> {
    session.execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
        entity: ENTITY_NAME.to_string(),
        key: InputValue::nat64(id),
    })
}

fn interrupt_and_recover(
    session: &DbSession<JournaledTestCanister>,
    interruption: MutationCommitInterruption,
    mutation: DynamicMutation,
) {
    // Prefix publication requires more than one changed row. A target update
    // also proves recovery converges the same final batch overlay as its source.
    let target_payload = match &mutation {
        DynamicMutation::Insert { .. } => 31,
        DynamicMutation::Replace { .. } => 32,
        DynamicMutation::Delete { .. } => 33,
        DynamicMutation::Update { .. } => {
            panic!("recovery fixture uses insert, replacement, and deletion");
        }
    };
    interrupt_next_mutation_commit_for_tests(interruption);
    let error = session
        .execute_trusted_dynamic_mutation_batch(vec![
            mutation,
            DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(3),
                patch: dynamic_payload_patch(target_payload),
            },
        ])
        .expect_err("selected publication boundary should interrupt");
    assert_eq!(error.class(), ErrorClass::InvariantViolation);
    forget_recovered_domain_for_tests(&session.db).expect("volatile recovery should reset");
    let pending = session
        .db
        .ensure_recovered_state()
        .expect_err("admission must wait for recovery");
    assert_eq!(
        pending.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING
    );
    drive_journaled_recovery_to_completion(session);
    let target = session
        .execute_trusted_live_page(
            &DynamicQuery::new(ENTITY_NAME)
                .filter(crate::db::FieldRef::new("id").eq(3_u64))
                .select(["payload"])
                .limit(1),
            None,
        )
        .expect("companion target should recover");
    assert_eq!(target.rows, vec![vec![OutputValue::nat64(target_payload)]]);
    let rows = source_rows(session);
    let edges = reverse_entries();
    forget_recovered_domain_for_tests(&session.db).expect("second recovery should reset");
    drive_journaled_recovery_to_completion(session);
    assert_eq!(source_rows(session), rows, "row replay must be idempotent");
    assert_eq!(
        reverse_entries(),
        edges,
        "reverse replay must be idempotent"
    );
}

fn assert_nested_recovery(interruption: MutationCommitInterruption) {
    let session = initialize_nested();
    session
        .execute_trusted_dynamic_mutation_batch(
            (1..=3)
                .map(|id| DynamicMutation::Insert {
                    entity: ENTITY_NAME.to_string(),
                    patch: dynamic_payload_patch(id),
                })
                .collect(),
        )
        .expect("relation targets should seed");
    drive_journaled_recovery_to_completion(&session);
    interrupt_and_recover(
        &session,
        interruption,
        DynamicMutation::Insert {
            entity: SECOND_ENTITY_NAME.to_string(),
            patch: nested_patch(&[1, 1, 2]),
        },
    );
    assert_nested_state(&session, &[1, 1, 2]);
    let inserted = reverse_entries();
    assert_eq!(
        inserted.len(),
        6,
        "three relation domains each have two distinct targets"
    );
    for id in [1, 2] {
        let error =
            delete_target(&session, id).expect_err("inserted reverse edges must restrict deletion");
        assert!(error.diagnostic_facts().contains(&(
            icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
            icydb_diagnostic_code::DiagnosticConstraintKind::Relation.raw(),
        )));
    }
    interrupt_and_recover(
        &session,
        interruption,
        DynamicMutation::Replace {
            entity: SECOND_ENTITY_NAME.to_string(),
            key: InputValue::nat64(1),
            patch: nested_patch(&[2, 3, 3]),
        },
    );
    let replaced = reverse_entries();
    assert_nested_state(&session, &[2, 3, 3]);
    assert_eq!(replaced.len(), 6);
    assert_eq!(
        inserted
            .iter()
            .filter(|edge| replaced.contains(edge))
            .count(),
        3,
        "replacement must retain exactly the shared target across three domains"
    );
    delete_target(&session, 1).expect("replacement must remove every obsolete target edge");
    for id in [2, 3] {
        let error =
            delete_target(&session, id).expect_err("replacement edges must restrict deletion");
        assert!(error.diagnostic_facts().contains(&(
            icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
            icydb_diagnostic_code::DiagnosticConstraintKind::Relation.raw(),
        )));
    }
    drive_journaled_recovery_to_completion(&session);
    interrupt_and_recover(
        &session,
        interruption,
        DynamicMutation::Delete {
            entity: SECOND_ENTITY_NAME.to_string(),
            key: InputValue::nat64(1),
        },
    );
    assert!(source_rows(&session).is_empty());
    assert!(reverse_entries().is_empty());
    for id in [2, 3] {
        delete_target(&session, id).expect("source deletion must release all nested targets");
    }
}

#[test]
fn nested_relation_recovery_after_marker_persistence() {
    assert_nested_recovery(MutationCommitInterruption::MarkerPersisted);
}

#[test]
fn nested_relation_recovery_after_journal_publication() {
    assert_nested_recovery(MutationCommitInterruption::JournalPublished);
}

#[test]
fn nested_relation_recovery_after_row_prefix_publication() {
    assert_nested_recovery(MutationCommitInterruption::RowPrefixPublished);
}

#[test]
fn nested_relation_recovery_after_all_rows_publish() {
    assert_nested_recovery(MutationCommitInterruption::RowsPublished);
}

#[test]
fn nested_relation_recovery_after_state_materialization() {
    assert_nested_recovery(MutationCommitInterruption::StateMaterialized);
}

#[test]
fn nested_relation_missing_lookup_budget_rejects_before_publication() {
    let session = initialize_nested();
    let before = JOURNALED_TAIL_STORE
        .with(|tail| tail.borrow().current_tail_control())
        .expect("tail control should decode");
    let patch = DynamicStructuralPatch::new(vec![
        (
            "payload".to_string(),
            DynamicWriteCell::Value(InputValue::nat64(100)),
        ),
        (
            "list".to_string(),
            DynamicWriteCell::Value(InputValue::list(
                (10_000..13_277).map(InputValue::nat64).collect(),
            )),
        ),
        (
            "set".to_string(),
            DynamicWriteCell::Value(InputValue::list(Vec::new())),
        ),
        (
            "map".to_string(),
            DynamicWriteCell::Value(InputValue::map(Vec::new())),
        ),
    ]);
    let error = session
        .execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Insert {
                entity: ENTITY_NAME.to_string(),
                patch: dynamic_payload_patch(1),
            },
            DynamicMutation::Insert {
                entity: SECOND_ENTITY_NAME.to_string(),
                patch,
            },
        ])
        .expect_err("distinct missing targets must exhaust the shared lookup ceiling");
    assert_eq!(
        error.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_EXECUTION_BUDGET_EXCEEDED
    );
    let expected = InternalError::relation_budget_exceeded(
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::RowsVisited,
        3_276,
        3_277,
    );
    assert!(
        expected
            .diagnostic_facts()
            .iter()
            .all(|fact| error.diagnostic_facts().contains(fact))
    );
    assert_eq!(
        JOURNALED_TAIL_STORE
            .with(|tail| tail.borrow().current_tail_control())
            .expect("tail control should still decode"),
        before
    );
    assert!(source_rows(&session).is_empty());
    let targets = session
        .execute_trusted_live_page(
            &DynamicQuery::new(ENTITY_NAME)
                .select(["id"])
                .order_by(crate::db::asc("id"))
                .limit(64),
            None,
        )
        .expect("target rows should remain readable");
    assert!(
        targets.rows.is_empty(),
        "earlier staged inserts must not publish"
    );
    assert!(reverse_entries().is_empty());
    assert!(
        crate::db::commit::retained_commit_marker_measurement_for_tests()
            .expect("commit marker should remain readable")
            .is_none()
    );
}
