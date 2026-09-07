//! Owned batch inputs retain exact authored payloads until accepted resolution.

use super::*;
use crate::{
    db::{
        data::AcceptedMutationFieldWriteIntent,
        dynamic_write::{DynamicMutation, DynamicStructuralPatch, DynamicTypedStructuralPatch},
        schema::AcceptedRowLayoutRuntimeContract,
        session::write::{
            AcceptedStructuralMutation, lower_dynamic_mutation_intent, lower_typed_mutation_intent,
        },
    },
    value::{OutputValue, PublicValue},
};

const HEAP_DESCRIPTOR: TypedEntityDescriptor = TypedEntityDescriptor::new(
    ENTITY_SOURCE,
    &[ID_SOURCE],
    &[
        TypedFieldDescriptor::new(ID_SOURCE, TypedFieldType::Scalar(ScalarType::Nat64), false),
        TypedFieldDescriptor::new(
            VALUE_SOURCE,
            TypedFieldType::List(&TypedFieldType::Scalar(ScalarType::Blob {
                max_len: Some(4096),
            })),
            false,
        ),
    ],
);

fn heap_session() -> (DbSession<TestCanister>, DynamicTypedEntityBinding) {
    let session = initialize_typed_session();
    let tag = EntityTag::new(91);
    let payload = PersistedFieldSnapshot::new_initial(
        FieldId::new(2),
        "value".into(),
        SchemaFieldSlot::new(1),
        AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Blob {
            max_len: Some(4096),
        })),
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::CatalogValue,
        LeafCodec::Structural,
    );
    publish(
        &session,
        AcceptedSchemaRevision::INITIAL,
        AcceptedSchemaRevision::new(2),
        BTreeMap::from([(
            tag,
            snapshot(
                ENTITY_SOURCE,
                "Entity",
                vec![nat64_field(1, "id", 0), payload],
            ),
        )]),
        BTreeMap::from([
            ((tag, field_source(ID_SOURCE)), FieldId::new(1)),
            ((tag, field_source(VALUE_SOURCE)), FieldId::new(2)),
        ]),
    );
    let binding = session
        .issue_typed_entity_binding(&HEAP_DESCRIPTOR)
        .unwrap();
    (session, binding)
}

fn payload(count: usize) -> InputValue {
    InputValue::list(
        (0..count)
            .map(|_| InputValue::blob(vec![7; 1024]))
            .collect(),
    )
}

fn backing(value: &InputValue) -> Vec<*const u8> {
    let PublicValue::List(values) = value.as_public() else {
        panic!("list fixture")
    };
    let mut pointers = vec![values.as_ptr().cast()];
    for value in values {
        let PublicValue::Blob(bytes) = value else {
            panic!("blob fixture")
        };
        pointers.push(bytes.as_ptr());
    }
    pointers
}

#[test]
fn owned_dynamic_and_typed_lowering_move_nested_backing_and_keep_caller_order() {
    let (session, binding) = heap_session();
    let catalog = session
        .current_typed_entity_binding_catalog(&binding)
        .unwrap()
        .unwrap();
    let descriptor =
        AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot()).unwrap();
    for typed in [false, true] {
        let input = payload(8);
        let before = backing(&input);
        let mut cells = vec![
            (1, DynamicWriteCell::Value(input)),
            (0, DynamicWriteCell::Value(InputValue::nat64(7))),
        ];
        let lowered = if typed {
            // Typed patches admit ascending descriptor ordinals; dynamic names
            // retain arbitrary caller order. Lowering must preserve both.
            cells.reverse();
            lower_typed_mutation_intent(
                catalog.identity().entity_tag(),
                &descriptor,
                &binding,
                DynamicTypedMutation::Insert {
                    patch: binding.bind_write_ordinals(cells).unwrap(),
                },
                0,
            )
            .unwrap()
            .unwrap()
        } else {
            lower_dynamic_mutation_intent(
                catalog.identity().entity_tag(),
                &descriptor,
                DynamicMutation::Insert {
                    entity: "Entity".into(),
                    patch: DynamicStructuralPatch::new(
                        cells
                            .into_iter()
                            .map(|(ordinal, cell)| {
                                (if ordinal == 0 { "id" } else { "value" }.into(), cell)
                            })
                            .collect(),
                    ),
                },
                0,
            )
            .unwrap()
        };
        let AcceptedStructuralMutation::Save { patch, .. } = lowered else {
            panic!("save fixture")
        };
        let entries = patch.entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[0].slot(),
            crate::db::data::FieldSlot::from_validated_index(usize::from(!typed))
        );
        assert_eq!(
            entries[1].slot(),
            crate::db::data::FieldSlot::from_validated_index(usize::from(typed))
        );
        let AcceptedMutationFieldWriteIntent::Authored(input) =
            entries[usize::from(typed)].intent()
        else {
            panic!("authored fixture")
        };
        assert_eq!(input, &payload(8));
        assert_eq!(backing(input), before);
    }
}

#[test]
fn owned_heap_batches_return_equal_values_and_reject_atomically() {
    let (session, binding) = heap_session();
    let typed_insert = |id, value| DynamicTypedMutation::Insert {
        patch: binding
            .bind_write_ordinals(vec![
                (0, DynamicWriteCell::Value(InputValue::nat64(id))),
                (1, DynamicWriteCell::Value(value)),
            ])
            .unwrap(),
    };
    let dynamic_insert = |id, value| DynamicMutation::Insert {
        entity: "Entity".into(),
        patch: DynamicStructuralPatch::new(vec![
            ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
            ("value".into(), DynamicWriteCell::Value(value)),
        ]),
    };
    let dynamic = session
        .execute_trusted_dynamic_mutation_batch(vec![dynamic_insert(1, payload(8))])
        .unwrap();
    let typed = session
        .execute_trusted_typed_mutation_batch(vec![(binding.clone(), typed_insert(2, payload(8)))])
        .unwrap()
        .unwrap();
    assert_eq!(dynamic[0].rows[0][1], typed[0].rows[0][1]);
    assert_eq!(
        dynamic[0].rows[0][1],
        OutputValue::from_public(payload(8).into_public())
    );

    // Invalid later input must not commit the earlier, already-lowered heap payload.
    assert!(
        session
            .execute_trusted_dynamic_mutation_batch(vec![
                dynamic_insert(3, payload(8)),
                dynamic_insert(4, InputValue::boolean(true)),
            ])
            .is_err()
    );
    assert!(
        session
            .execute_trusted_same_entity_typed_mutation_batch(
                &binding,
                vec![
                    typed_insert(5, payload(8)),
                    typed_insert(6, InputValue::boolean(true)),
                ]
            )
            .is_err()
    );
    let result = session
        .execute_trusted_same_entity_typed_mutation_batch(
            &binding,
            vec![
                typed_insert(3, payload(1)),
                typed_insert(4, payload(1)),
                typed_insert(5, payload(1)),
                typed_insert(6, payload(1)),
            ],
        )
        .unwrap()
        .unwrap();
    assert_eq!(result.affected_rows, 4);
}

#[test]
fn owned_typed_lowering_checks_keys_before_patch_binding() {
    let (session, binding) = heap_session();
    let catalog = session
        .current_typed_entity_binding_catalog(&binding)
        .unwrap()
        .unwrap();
    let descriptor =
        AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot()).unwrap();
    let lower = |request| {
        lower_typed_mutation_intent(
            catalog.identity().entity_tag(),
            &descriptor,
            &binding,
            request,
            7,
        )
    };
    assert!(
        lower(DynamicTypedMutation::Insert {
            patch: DynamicTypedStructuralPatch::default()
        })
        .unwrap()
        .is_none()
    );
    assert!(
        lower(DynamicTypedMutation::Update {
            key: InputValue::nat64(1),
            patch: DynamicTypedStructuralPatch::default(),
        })
        .unwrap()
        .is_none()
    );
    let invalid_key = InputValue::text("not a supported key".into());
    let expected = lower(DynamicTypedMutation::Delete {
        key: invalid_key.clone(),
    })
    .err()
    .expect("unsupported key must reject");
    for request in [
        DynamicTypedMutation::Update {
            key: invalid_key.clone(),
            patch: DynamicTypedStructuralPatch::default(),
        },
        DynamicTypedMutation::Replace {
            key: invalid_key,
            patch: DynamicTypedStructuralPatch::default(),
        },
    ] {
        let error = lower(request)
            .err()
            .expect("key failure precedes stale patch");
        assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
        assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
    }
}
