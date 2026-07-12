//! Accepted canonical value slot encoding and strict decoding.
//!
//! This boundary consumes catalog-proven values and accepted field contracts.
//! It does not reconstruct generated models or lower through runtime `Value`.

use crate::{
    db::{
        data::{
            persisted_row::codec::{ScalarSlotValueRef, ScalarValueRef, decode_scalar_slot_value},
            structural_field::{
                decode_canonical_value_storage_bytes, encode_canonical_value_storage_bytes,
            },
        },
        schema::{
            AcceptedFieldDecodeContract,
            enum_catalog::validate_decoded_persisted_field_value_in_catalog,
            enum_catalog::{
                AcceptedEnumCatalog, AcceptedEnumCatalogHandle, AcceptedValueContract,
                AcceptedValueRef, AdmittedOwnedValue, CanonicalValue, ValueAdmissionBudget,
                admit_decoded_persisted_field_value, validate_nullable_canonical_value,
            },
        },
    },
    error::InternalError,
    model::field::{FieldStorageDecode, LeafCodec},
};

/// Encode one admitted canonical value through its accepted field contract.
pub(in crate::db) fn encode_admitted_value_for_accepted_field_contract(
    catalog: &AcceptedEnumCatalogHandle,
    field: AcceptedFieldDecodeContract<'_>,
    admitted: &AdmittedOwnedValue,
) -> Result<Vec<u8>, InternalError> {
    if admitted.authority() != catalog.authority() {
        return Err(InternalError::persisted_row_field_encode_internal(
            field.field_name(),
        ));
    }
    let contract = AcceptedValueContract::from_accepted_field(
        catalog.catalog(),
        field.kind(),
        field.storage_decode(),
    )
    .map_err(|_| InternalError::persisted_row_field_encode_internal(field.field_name()))?;
    let mut budget = ValueAdmissionBudget::standard();
    let accepted = validate_nullable_canonical_value(
        catalog,
        &contract,
        field.nullable(),
        admitted.value(),
        &mut budget,
    )
    .map_err(|_| InternalError::persisted_row_field_encode_internal(field.field_name()))?;

    encode_accepted_value_ref_for_accepted_field_contract(field, &accepted)
}

fn encode_accepted_value_ref_for_accepted_field_contract(
    field: AcceptedFieldDecodeContract<'_>,
    accepted: &AcceptedValueRef<'_>,
) -> Result<Vec<u8>, InternalError> {
    if accepted.nullable() != field.nullable() {
        return Err(InternalError::persisted_row_field_encode_internal(
            field.field_name(),
        ));
    }
    let value = accepted.value();
    if field.uses_canonical_value_wire() {
        return encode_canonical_value_storage_bytes(value);
    }

    super::contract::encode_runtime_value_for_accepted_field_contract(field, value)
}

/// Decode one slot through the current canonical value format and strict
/// accepted schema validation.
pub(in crate::db) fn decode_admitted_value_from_accepted_field_contract(
    catalog: &AcceptedEnumCatalogHandle,
    field: AcceptedFieldDecodeContract<'_>,
    raw_value: &[u8],
) -> Result<AdmittedOwnedValue, InternalError> {
    let value = decode_canonical_value_from_accepted_field_contract(field, raw_value)?;
    let mut budget = ValueAdmissionBudget::standard();
    admit_decoded_persisted_field_value(
        catalog,
        field.kind(),
        field.storage_decode(),
        field.nullable(),
        value,
        &mut budget,
    )
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
                normalize_and_admit_persisted_field_value,
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

    fn accepted_enum_fixture() -> (
        AcceptedEnumCatalogHandle,
        AcceptedFieldKind,
        AdmittedOwnedValue,
    ) {
        let catalog = build_initial_accepted_enum_catalog_from_kinds_for_tests(&[STATUS_KIND])
            .expect("accepted enum catalog should build");
        let catalog =
            AcceptedEnumCatalogHandle::new_for_tests(catalog, AcceptedSchemaRevision::INITIAL);
        let kind = AcceptedFieldKind::from_model_kind(STATUS_KIND);
        let mut budget = ValueAdmissionBudget::standard();
        let admitted = normalize_and_admit_persisted_field_value(
            &catalog,
            &kind,
            FieldStorageDecode::Value,
            false,
            InputValue::Enum(InputValueEnum::loose("Ready")),
            &mut budget,
        )
        .expect("accepted enum input should admit");

        (catalog, kind, admitted)
    }

    #[test]
    fn accepted_canonical_slot_round_trips_id_backed_enum_with_authority() {
        let (catalog, kind, admitted) = accepted_enum_fixture();
        let field = AcceptedFieldDecodeContract::new(
            "status",
            &kind,
            false,
            FieldStorageDecode::Value,
            LeafCodec::StructuralFallback,
        );
        let encoded = encode_admitted_value_for_accepted_field_contract(&catalog, field, &admitted)
            .expect("admitted enum slot should encode");
        assert_eq!(encoded.first(), Some(&0x84));

        let decoded = decode_admitted_value_from_accepted_field_contract(&catalog, field, &encoded)
            .expect("canonical enum slot should decode strictly");
        assert_eq!(decoded, admitted);
    }

    #[test]
    fn accepted_canonical_slot_rejects_foreign_authority() {
        let (catalog, kind, admitted) = accepted_enum_fixture();
        let field = AcceptedFieldDecodeContract::new(
            "status",
            &kind,
            false,
            FieldStorageDecode::Value,
            LeafCodec::StructuralFallback,
        );
        let foreign_catalog = AcceptedEnumCatalogHandle::new_for_tests(
            catalog.catalog().clone(),
            AcceptedSchemaRevision::INITIAL,
        );
        assert!(
            encode_admitted_value_for_accepted_field_contract(&foreign_catalog, field, &admitted,)
                .is_err()
        );
    }

    #[test]
    fn accepted_canonical_slot_requires_nullable_proof_for_null() {
        let (catalog, _, _) = accepted_enum_fixture();
        let kind = AcceptedFieldKind::Text { max_len: Some(8) };
        let mut budget = ValueAdmissionBudget::standard();
        let admitted = normalize_and_admit_persisted_field_value(
            &catalog,
            &kind,
            FieldStorageDecode::ByKind,
            true,
            InputValue::Null,
            &mut budget,
        )
        .expect("nullable field should admit null");
        let nullable_field = AcceptedFieldDecodeContract::new(
            "label",
            &kind,
            true,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Text),
        );
        let encoded =
            encode_admitted_value_for_accepted_field_contract(&catalog, nullable_field, &admitted)
                .expect("nullable proof should encode null");
        let decoded =
            decode_admitted_value_from_accepted_field_contract(&catalog, nullable_field, &encoded)
                .expect("nullable null should decode through the accepted contract");
        assert_eq!(decoded, admitted);

        let required_field = AcceptedFieldDecodeContract::new(
            "label",
            &kind,
            false,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Text),
        );
        assert!(
            encode_admitted_value_for_accepted_field_contract(&catalog, required_field, &admitted,)
                .is_err(),
            "nullable admission must not authorize required-field persistence",
        );
    }
}
