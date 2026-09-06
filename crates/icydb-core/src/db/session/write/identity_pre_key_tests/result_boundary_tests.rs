//! End-to-end result admission for the maintained capped batch surfaces.

use super::*;
use crate::db::{
    DynamicMutationResult, commit::database_control_proof_identity, data::DecodedDataStoreKey,
};

const BLOB_DESCRIPTOR: TypedEntityDescriptor = TypedEntityDescriptor::new(
    ENTITY_SOURCE,
    &[ID_SOURCE],
    &[
        TypedFieldDescriptor::new(ID_SOURCE, TypedFieldType::Scalar(ScalarType::Nat64), false),
        TypedFieldDescriptor::new(
            PAYLOAD_SOURCE,
            TypedFieldType::Scalar(ScalarType::Blob { max_len: None }),
            false,
        ),
    ],
);

#[derive(Clone, Copy)]
enum BatchSurface {
    Structural,
    Typed,
    SameEntityTyped,
}

impl BatchSurface {
    // Use the actual response envelope of each maintained entry point. Their
    // metadata overhead differs, so payload length alone is not the boundary.
    fn encode_results(self, results: Vec<DynamicMutationResult>) -> Vec<u8> {
        match self {
            Self::Structural | Self::Typed => candid::encode_one(&results),
            Self::SameEntityTyped => {
                let rows = results.into_iter().flat_map(|result| result.rows).collect();
                candid::encode_one(DynamicMutationResult {
                    entity: ENTITY_NAME.to_string(),
                    columns: vec!["id".to_string(), "payload".to_string()],
                    rows,
                    affected_rows: 2,
                })
            }
        }
        .expect("maintained batch response should encode")
    }

    fn execute(
        self,
        session: &DbSession<TestCanister>,
        binding: &DynamicTypedEntityBinding,
        payload_len: usize,
    ) -> Result<Vec<u8>, InternalError> {
        let results = match self {
            Self::Structural => session.execute_trusted_dynamic_mutation_batch(vec![
                DynamicMutation::Update {
                    entity: ENTITY_NAME.to_string(),
                    key: InputValue::nat64(1),
                    patch: blob_patch(vec![1]),
                },
                DynamicMutation::Insert {
                    entity: ENTITY_NAME.to_string(),
                    patch: blob_patch(vec![2; payload_len]),
                },
            ])?,
            Self::Typed | Self::SameEntityTyped => {
                let patch = |value| {
                    binding
                        .bind_write_ordinals(vec![(
                            1,
                            DynamicWriteCell::Value(InputValue::blob(value)),
                        )])
                        .expect("blob field should bind without authoring Identity")
                };
                let mutations = vec![
                    DynamicTypedMutation::Update {
                        key: InputValue::nat64(1),
                        patch: patch(vec![1]),
                    },
                    DynamicTypedMutation::Insert {
                        patch: patch(vec![2; payload_len]),
                    },
                ];
                if matches!(self, Self::Typed) {
                    session
                        .execute_trusted_typed_mutation_batch(
                            mutations
                                .into_iter()
                                .map(|item| (binding.clone(), item))
                                .collect(),
                        )?
                        .expect("accepted typed binding should stay current")
                } else {
                    let result = session
                        .execute_trusted_same_entity_typed_mutation_batch(binding, mutations)?
                        .expect("accepted typed binding should stay current");
                    return Ok(candid::encode_one(result).expect("actual response should encode"));
                }
            }
        };
        Ok(self.encode_results(results))
    }
}

fn blob_patch(value: Vec<u8>) -> DynamicStructuralPatch {
    DynamicStructuralPatch::new(vec![(
        "payload".to_string(),
        DynamicWriteCell::Value(InputValue::blob(value)),
    )])
}

fn initialize_blob_session() -> DbSession<TestCanister> {
    let id = identity_snapshot(STORE_PATH, false).fields()[0].clone();
    let payload = PersistedFieldSnapshot::new_initial(
        FieldId::new(2),
        "payload".to_string(),
        SchemaFieldSlot::new(1),
        AcceptedFieldKind::Blob { max_len: None },
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Blob),
    );
    initialize_with_snapshot(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        ENTITY_SOURCE.to_string(),
        ENTITY_NAME.to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![id, payload],
    ))
}

fn expected_results(payload_len: usize) -> Vec<DynamicMutationResult> {
    [(1, vec![1]), (2, vec![2; payload_len])]
        .into_iter()
        .map(|(id, payload)| DynamicMutationResult {
            entity: ENTITY_NAME.to_string(),
            columns: vec!["id".to_string(), "payload".to_string()],
            rows: vec![vec![OutputValue::nat64(id), OutputValue::blob(payload)]],
            affected_rows: 1,
        })
        .collect()
}

fn stored_row(id: u64) -> Option<Vec<u8>> {
    let key = DecodedDataStoreKey::try_from_structural_key(ENTITY_TAG, &Value::Nat64(id))
        .expect("fixture key should encode")
        .to_raw()
        .expect("fixture key should encode");
    DATA_STORE.with(|store| store.borrow().get(&key).map(|row| row.as_bytes().to_vec()))
}

fn assert_result_boundary(surface: BatchSurface) {
    let session = initialize_blob_session();
    let binding = session
        .issue_typed_entity_binding(&BLOB_DESCRIPTOR)
        .expect("blob binding");
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: ENTITY_NAME.to_string(),
            patch: blob_patch(vec![0]),
        })
        .expect("seed row should commit");
    let before = stored_row(1).expect("seed row should exist");
    let control_before = database_control_proof_identity().expect("seed commit control");

    // Start near the cap so Candid's variable-length blob prefix has the same
    // width at the exact and cap-plus-one boundaries; verify both encodings.
    let limit = MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES;
    let overhead = surface.encode_results(expected_results(limit)).len() - limit;
    let payload_len = limit.checked_sub(overhead).expect("response metadata fits");
    let expected = surface.encode_results(expected_results(payload_len));
    assert_eq!(expected.len(), limit);
    assert_eq!(
        surface
            .encode_results(expected_results(payload_len + 1))
            .len(),
        limit + 1
    );

    let error = surface
        .execute(&session, &binding, payload_len + 1)
        .expect_err("one byte over the response cap must reject the entire batch");
    assert!(matches!(
        error.diagnostic().detail(),
        Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::MutationBatchResultBytesExceeded,
            ..
        })
    ));
    assert_eq!(
        error.diagnostic_facts(),
        vec![
            (
                icydb_diagnostic_code::DiagnosticFactTag::ActualLength,
                (limit + 1) as u64
            ),
            (
                icydb_diagnostic_code::DiagnosticFactTag::Limit,
                limit as u64
            ),
        ],
    );
    assert_eq!(
        stored_row(1).as_ref(),
        Some(&before),
        "earlier update must not commit"
    );
    assert_eq!(stored_row(2), None, "later insert must not commit");
    assert_eq!(DATA_STORE.with(|store| store.borrow().len()), 1);
    assert_eq!(
        database_control_proof_identity().expect("rejected batch commit control"),
        control_before,
        "result rejection must not publish a commit marker or consume its sequence",
    );
    SCHEMA_STORE.with(|store| {
        let cursor = store
            .borrow()
            .identity_statement_cursor(
                database_incarnation_id().expect("incarnation"),
                ENTITY_TAG,
                FieldId::new(1),
                &AcceptedFieldKind::Nat64,
            )
            .expect("Identity state should stay readable");
        assert_eq!(cursor.expected_high_water(), 1);
        assert!(!cursor.has_allocations());
    });

    let actual = surface
        .execute(&session, &binding, payload_len)
        .expect("exactly the response cap should commit");
    assert_eq!(
        actual, expected,
        "real response must match exact envelope and unconsumed ID 2"
    );
    assert_ne!(stored_row(1).as_ref(), Some(&before));
    assert!(stored_row(2).is_some());
    assert_eq!(DATA_STORE.with(|store| store.borrow().len()), 2);
}

#[test]
fn structural_batch_result_boundary_preserves_rows_and_identity() {
    assert_result_boundary(BatchSurface::Structural);
}

#[test]
fn typed_batch_result_boundary_preserves_rows_and_identity() {
    assert_result_boundary(BatchSurface::Typed);
}

#[test]
fn same_entity_typed_batch_result_boundary_preserves_rows_and_identity() {
    assert_result_boundary(BatchSurface::SameEntityTyped);
}
