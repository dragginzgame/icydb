//! Nullable collection payloads retain NULL independently of relation-key projection.

use super::*;
use crate::{
    db::{
        data::{
            decode_accepted_relation_target_primary_key_components_bytes,
            decode_runtime_value_from_accepted_field_contract,
            persisted_row::contract::validate_non_scalar_accepted_slot_value,
        },
        schema::{
            AcceptedFieldKind, AcceptedSchemaRevision, AcceptedValueCatalogHandle,
            empty_accepted_enum_catalog_for_tests,
        },
    },
    types::{EntityTag, Ulid},
};

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep accepted encode/decode and reverse-projection controls paired for each collection and key kind"
)]
fn nullable_relation_collections_preserve_null_empty_and_populated_slots() {
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    for (key_kind, targets) in [
        (
            AcceptedFieldKind::Nat64,
            vec![Value::Nat64(7), Value::Nat64(9)],
        ),
        (
            AcceptedFieldKind::Int128,
            vec![Value::Int128(-7), Value::Int128(9)],
        ),
        (
            AcceptedFieldKind::Ulid,
            vec![
                Value::Ulid(Ulid::from_u128(7)),
                Value::Ulid(Ulid::from_u128(9)),
            ],
        ),
    ] {
        let relation = AcceptedFieldKind::Relation {
            target_path: "tests::Target".into(),
            target_entity_name: "Target".into(),
            target_entity_tag: EntityTag::new(7),
            target_store_path: "tests::TargetStore".into(),
            key_kind: Box::new(key_kind),
        };
        for kind in [
            AcceptedFieldKind::List(Box::new(relation.clone())),
            AcceptedFieldKind::Set(Box::new(relation)),
        ] {
            for nullable in [true, false] {
                let field = AcceptedFieldDecodeContract::new(
                    "refs",
                    &kind,
                    nullable,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Structural,
                );
                let encoding =
                    AcceptedFieldPersistenceContract::new_for_tests(&catalog, field).unwrap();
                for value in [
                    Value::Null,
                    Value::List(vec![]),
                    Value::List(targets.clone()),
                ] {
                    if !nullable && matches!(value, Value::Null) {
                        assert!(
                            encode_canonical_value_for_accepted_field_contract(
                                encoding.clone(),
                                &value
                            )
                            .is_err()
                        );
                        assert!(
                            decode_runtime_value_from_accepted_field_contract(field, &[0]).is_err()
                        );
                        assert!(validate_non_scalar_accepted_slot_value(&[0], field).is_err());
                        continue;
                    }
                    let bytes = encode_canonical_value_for_accepted_field_contract(
                        encoding.clone(),
                        &value,
                    )
                    .unwrap();
                    assert_eq!(bytes == [0], matches!(value, Value::Null));
                    for _ in 0..2 {
                        let decoded =
                            decode_runtime_value_from_accepted_field_contract(field, &bytes)
                                .unwrap();
                        assert_eq!(decoded, value);
                        validate_non_scalar_accepted_slot_value(&bytes, field).unwrap();
                        assert_eq!(
                            encode_canonical_value_for_accepted_field_contract(
                                encoding.clone(),
                                &decoded
                            )
                            .unwrap(),
                            bytes
                        );
                        let keys = decode_accepted_relation_target_primary_key_components_bytes(
                            &bytes, &kind,
                        )
                        .unwrap();
                        assert_eq!(
                            keys.iter()
                                .map(|key| key.as_runtime_value())
                                .collect::<Vec<_>>(),
                            match &value {
                                Value::List(items) => items.clone(),
                                _ => vec![],
                            }
                        );
                    }
                }
                for malformed in [&[][..], &[0, 0], &[0xff]] {
                    let decoded =
                        decode_runtime_value_from_accepted_field_contract(field, malformed)
                            .unwrap_err();
                    let validated =
                        validate_non_scalar_accepted_slot_value(malformed, field).unwrap_err();
                    assert_eq!(decoded.class(), crate::error::ErrorClass::Corruption);
                    assert_eq!(decoded.diagnostic_code(), validated.diagnostic_code());
                }
            }
        }
    }
}
