//! Accepted canonical value slot encoding and strict decoding.
//!
//! This boundary consumes catalog-proven values and accepted field contracts.
//! It does not reconstruct generated models or lower through runtime `Value`.

#[cfg(test)]
use crate::db::schema::AcceptedEnumCatalogHandle;
use crate::{
    db::{
        data::{
            accepted_kind_supports_primary_key_component_binary,
            encode_structural_field_by_accepted_kind_bytes,
            encode_structural_value_storage_null_bytes,
            persisted_row::codec::{
                ScalarSlotValueRef, ScalarValueRef, decode_scalar_slot_value,
                encode_scalar_slot_value,
            },
            structural_field::{
                decode_canonical_value_storage_bytes, encode_canonical_value_storage_bytes,
            },
        },
        schema::{
            AcceptedFieldDecodeContract, AcceptedFieldPersistenceContract,
            enum_catalog::validate_decoded_persisted_field_value_in_catalog,
            enum_catalog::{
                AcceptedEnumCatalog, AcceptedValueRef, AdmittedOwnedValue, CanonicalValue,
                ValueAdmissionBudget,
            },
        },
    },
    error::InternalError,
    model::field::{FieldStorageDecode, LeafCodec, ScalarCodec},
    value::{InputValue, Value},
};

/// Normalize and encode one authored input through an accepted field contract.
pub(in crate::db) fn encode_input_value_for_accepted_field_contract(
    encoding: AcceptedFieldPersistenceContract<'_>,
    input: InputValue,
    budget: &mut ValueAdmissionBudget,
) -> Result<Vec<u8>, InternalError> {
    let field = encoding.field();
    encoding
        .admission_contract()
        .with_normalized(input, budget, |accepted| {
            encode_accepted_value_ref_for_accepted_field_contract(field, &accepted)
        })
        .map_err(|_| InternalError::persisted_row_field_encode_internal(field.field_name()))?
}

/// Strictly validate and encode one canonical value through an accepted field contract.
pub(in crate::db) fn encode_canonical_value_for_accepted_field_contract(
    encoding: AcceptedFieldPersistenceContract<'_>,
    value: &CanonicalValue,
) -> Result<Vec<u8>, InternalError> {
    let field = encoding.field();
    let mut budget = ValueAdmissionBudget::standard();
    encoding
        .admission_contract()
        .with_validated(value, &mut budget, |accepted| {
            encode_accepted_value_ref_for_accepted_field_contract(field, &accepted)
        })
        .map_err(|_| InternalError::persisted_row_field_encode_internal(field.field_name()))?
}

fn encode_accepted_value_ref_for_accepted_field_contract(
    field: AcceptedFieldDecodeContract<'_>,
    accepted: &AcceptedValueRef<'_>,
) -> Result<Vec<u8>, InternalError> {
    let value = accepted.value();
    if field.uses_canonical_value_wire() {
        return encode_canonical_value_storage_bytes(value);
    }

    if matches!(value, Value::Null) {
        return encode_accepted_null_slot_value(field);
    }

    match field.storage_decode() {
        FieldStorageDecode::Value => Err(InternalError::persisted_row_field_encode_internal(
            field.field_name(),
        )),
        FieldStorageDecode::ByKind => match field.leaf_codec() {
            LeafCodec::Scalar(codec) => {
                let scalar =
                    scalar_slot_value_ref_from_accepted_value(value, codec).ok_or_else(|| {
                        InternalError::persisted_row_field_encode_internal(field.field_name())
                    })?;

                Ok(encode_scalar_slot_value(scalar))
            }
            LeafCodec::StructuralFallback => encode_structural_field_by_accepted_kind_bytes(
                field.kind(),
                value,
                field.field_name(),
            ),
        },
    }
}

// Encode an admitted nullable `NULL` through the accepted field's storage lane.
fn encode_accepted_null_slot_value(
    field: AcceptedFieldDecodeContract<'_>,
) -> Result<Vec<u8>, InternalError> {
    if !field.nullable() {
        return Err(InternalError::persisted_row_field_encode_internal(
            field.field_name(),
        ));
    }

    match field.storage_decode() {
        FieldStorageDecode::Value => Err(InternalError::persisted_row_field_encode_internal(
            field.field_name(),
        )),
        FieldStorageDecode::ByKind => match field.leaf_codec() {
            LeafCodec::Scalar(_) => Ok(encode_scalar_slot_value(ScalarSlotValueRef::Null)),
            LeafCodec::StructuralFallback
                if accepted_kind_supports_primary_key_component_binary(field.kind()) =>
            {
                encode_structural_field_by_accepted_kind_bytes(
                    field.kind(),
                    &Value::Null,
                    field.field_name(),
                )
            }
            LeafCodec::StructuralFallback => Ok(encode_structural_value_storage_null_bytes()),
        },
    }
}

// Convert one accepted scalar into the borrowed scalar-slot view used by the codec.
const fn scalar_slot_value_ref_from_accepted_value(
    value: &Value,
    codec: ScalarCodec,
) -> Option<ScalarSlotValueRef<'_>> {
    let scalar = match (codec, value) {
        (ScalarCodec::Blob, Value::Blob(value)) => ScalarValueRef::Blob(value.as_slice()),
        (ScalarCodec::Bool, Value::Bool(value)) => ScalarValueRef::Bool(*value),
        (ScalarCodec::Date, Value::Date(value)) => ScalarValueRef::Date(*value),
        (ScalarCodec::Duration, Value::Duration(value)) => ScalarValueRef::Duration(*value),
        (ScalarCodec::Float32, Value::Float32(value)) => ScalarValueRef::Float32(*value),
        (ScalarCodec::Float64, Value::Float64(value)) => ScalarValueRef::Float64(*value),
        (ScalarCodec::Int64, Value::Int64(value)) => ScalarValueRef::Int(*value),
        (ScalarCodec::Principal, Value::Principal(value)) => ScalarValueRef::Principal(*value),
        (ScalarCodec::Subaccount, Value::Subaccount(value)) => ScalarValueRef::Subaccount(*value),
        (ScalarCodec::Text, Value::Text(value)) => ScalarValueRef::Text(value.as_str()),
        (ScalarCodec::Timestamp, Value::Timestamp(value)) => ScalarValueRef::Timestamp(*value),
        (ScalarCodec::Nat64, Value::Nat64(value)) => ScalarValueRef::Nat(*value),
        (ScalarCodec::Ulid, Value::Ulid(value)) => ScalarValueRef::Ulid(*value),
        (ScalarCodec::Unit, Value::Unit) => ScalarValueRef::Unit,
        _ => return None,
    };

    Some(ScalarSlotValueRef::Value(scalar))
}

/// Decode one slot through the current canonical value format and strict
/// accepted schema validation.
pub(in crate::db) fn decode_admitted_value_from_accepted_field_contract(
    persistence: AcceptedFieldPersistenceContract<'_>,
    raw_value: &[u8],
) -> Result<AdmittedOwnedValue, InternalError> {
    let field = persistence.field();
    let value = decode_canonical_value_from_accepted_field_contract(field, raw_value)?;
    let mut budget = ValueAdmissionBudget::standard();
    persistence
        .admission_contract()
        .admit_canonical(value, &mut budget)
        .map_err(|_| InternalError::persisted_row_decode_corruption())
}

/// Validate one persisted default before it becomes accepted schema content.
pub(in crate::db) fn validate_default_payload_for_accepted_field_contract(
    catalog: &AcceptedEnumCatalog,
    field: AcceptedFieldDecodeContract<'_>,
    raw_value: &[u8],
) -> Result<(), InternalError> {
    if !field.uses_canonical_value_wire() {
        let value =
            super::contract::decode_runtime_value_from_accepted_field_contract(field, raw_value)?;
        return (!matches!(value, crate::value::Value::Null))
            .then_some(())
            .ok_or_else(InternalError::store_invariant);
    }

    let value = decode_canonical_value_from_accepted_field_contract(field, raw_value)?;
    if matches!(value, CanonicalValue::Null) {
        return Err(InternalError::store_invariant());
    }
    let mut budget = ValueAdmissionBudget::standard();
    validate_decoded_persisted_field_value_in_catalog(
        catalog,
        field.kind(),
        field.storage_decode(),
        field.nullable(),
        &value,
        &mut budget,
    )
    .map_err(|_| InternalError::store_invariant())
}

fn decode_canonical_value_from_accepted_field_contract(
    field: AcceptedFieldDecodeContract<'_>,
    raw_value: &[u8],
) -> Result<CanonicalValue, InternalError> {
    let value = if field.uses_canonical_value_wire() {
        decode_canonical_value_storage_bytes(raw_value)
            .map_err(|_| InternalError::persisted_row_decode_corruption())?
    } else {
        match field.storage_decode() {
            FieldStorageDecode::Value => decode_canonical_value_storage_bytes(raw_value)
                .map_err(|_| InternalError::persisted_row_decode_corruption())?,
            FieldStorageDecode::ByKind => match field.leaf_codec() {
                LeafCodec::Scalar(codec) => {
                    let value = decode_scalar_slot_value(raw_value, codec, field.field_name())?;
                    canonical_value_from_scalar_slot(value)
                }
                LeafCodec::StructuralFallback => {
                    return Err(InternalError::persisted_row_decode_corruption());
                }
            },
        }
    };
    Ok(value)
}

fn canonical_value_from_scalar_slot(value: ScalarSlotValueRef<'_>) -> CanonicalValue {
    match value {
        ScalarSlotValueRef::Null => CanonicalValue::Null,
        ScalarSlotValueRef::Value(ScalarValueRef::Blob(value)) => {
            CanonicalValue::Blob(value.to_vec())
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Bool(value)) => CanonicalValue::Bool(value),
        ScalarSlotValueRef::Value(ScalarValueRef::Date(value)) => CanonicalValue::Date(value),
        ScalarSlotValueRef::Value(ScalarValueRef::Duration(value)) => {
            CanonicalValue::Duration(value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Float32(value)) => CanonicalValue::Float32(value),
        ScalarSlotValueRef::Value(ScalarValueRef::Float64(value)) => CanonicalValue::Float64(value),
        ScalarSlotValueRef::Value(ScalarValueRef::Int(value)) => CanonicalValue::Int64(value),
        ScalarSlotValueRef::Value(ScalarValueRef::Principal(value)) => {
            CanonicalValue::Principal(value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Subaccount(value)) => {
            CanonicalValue::Subaccount(value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Text(value)) => {
            CanonicalValue::Text(value.to_owned())
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Timestamp(value)) => {
            CanonicalValue::Timestamp(value)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Nat(value)) => CanonicalValue::Nat64(value),
        ScalarSlotValueRef::Value(ScalarValueRef::Ulid(value)) => CanonicalValue::Ulid(value),
        ScalarSlotValueRef::Value(ScalarValueRef::Unit) => CanonicalValue::Unit,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::schema::{
            AcceptedFieldKind,
            enum_catalog::{
                AcceptedSchemaRevision, build_initial_accepted_enum_catalog_from_kinds_for_tests,
            },
        },
        model::field::{EnumVariantModel, FieldKind, ScalarCodec},
        value::{InputValue, InputValueEnum},
    };

    static STATUS_VARIANTS: [EnumVariantModel; 1] = [EnumVariantModel::new(
        "Ready",
        None,
        FieldStorageDecode::ByKind,
    )];
    static STATUS_KIND: FieldKind = FieldKind::Enum {
        path: "tests::CanonicalStatus",
        variants: &STATUS_VARIANTS,
    };

    fn accepted_enum_fixture() -> (AcceptedEnumCatalogHandle, AcceptedFieldKind) {
        let catalog = build_initial_accepted_enum_catalog_from_kinds_for_tests(&[STATUS_KIND])
            .expect("accepted enum catalog should build");
        let catalog =
            AcceptedEnumCatalogHandle::new_for_tests(catalog, AcceptedSchemaRevision::INITIAL);
        let kind = AcceptedFieldKind::from_model_kind(STATUS_KIND);

        (catalog, kind)
    }

    fn test_encoding<'a>(
        catalog: &'a AcceptedEnumCatalogHandle,
        field: AcceptedFieldDecodeContract<'a>,
    ) -> AcceptedFieldPersistenceContract<'a> {
        AcceptedFieldPersistenceContract::new_for_tests(catalog, field)
            .expect("accepted test field should match its catalog")
    }

    #[test]
    fn accepted_input_slot_round_trips_id_backed_enum() {
        let (catalog, kind) = accepted_enum_fixture();
        let field = AcceptedFieldDecodeContract::new(
            "status",
            &kind,
            false,
            FieldStorageDecode::Value,
            LeafCodec::StructuralFallback,
        );
        let mut budget = ValueAdmissionBudget::standard();
        let encoded = encode_input_value_for_accepted_field_contract(
            test_encoding(&catalog, field),
            InputValue::Enum(InputValueEnum::loose("Ready")),
            &mut budget,
        )
        .expect("accepted enum slot should encode");
        assert_eq!(encoded.first(), Some(&0x84));

        let decoded = decode_admitted_value_from_accepted_field_contract(
            test_encoding(&catalog, field),
            &encoded,
        )
        .expect("canonical enum slot should decode strictly");
        let CanonicalValue::Enum(value) = decoded.value() else {
            panic!("accepted enum slot should decode to canonical IDs");
        };
        assert_eq!(
            Some(value.type_id()),
            catalog.catalog().type_id("tests::CanonicalStatus"),
        );
    }

    #[test]
    fn accepted_input_slot_rejects_wrong_contract() {
        let (catalog, _) = accepted_enum_fixture();
        let blob_kind = AcceptedFieldKind::Blob { max_len: Some(8) };
        let blob_field = AcceptedFieldDecodeContract::new(
            "payload",
            &blob_kind,
            false,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Blob),
        );
        let mut budget = ValueAdmissionBudget::standard();

        assert!(
            encode_input_value_for_accepted_field_contract(
                test_encoding(&catalog, blob_field),
                InputValue::Text("ready".to_string()),
                &mut budget,
            )
            .is_err(),
            "authored input must match the selected accepted contract",
        );
    }

    #[test]
    fn accepted_input_slot_enforces_nullability() {
        let (catalog, _) = accepted_enum_fixture();
        let kind = AcceptedFieldKind::Text { max_len: Some(8) };
        let nullable_field = AcceptedFieldDecodeContract::new(
            "label",
            &kind,
            true,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Text),
        );
        let mut nullable_budget = ValueAdmissionBudget::standard();
        let encoded = encode_input_value_for_accepted_field_contract(
            test_encoding(&catalog, nullable_field),
            InputValue::Null,
            &mut nullable_budget,
        )
        .expect("nullable proof should encode null");
        let decoded = decode_admitted_value_from_accepted_field_contract(
            test_encoding(&catalog, nullable_field),
            &encoded,
        )
        .expect("nullable null should decode through the accepted contract");
        assert_eq!(decoded.value(), &CanonicalValue::Null);

        let required_field = AcceptedFieldDecodeContract::new(
            "label",
            &kind,
            false,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Text),
        );
        let mut required_budget = ValueAdmissionBudget::standard();
        assert!(
            encode_input_value_for_accepted_field_contract(
                test_encoding(&catalog, required_field),
                InputValue::Null,
                &mut required_budget,
            )
            .is_err(),
            "required fields must reject null before persistence",
        );
    }
}
