//! Canonical equality-key capability and bytes for admitted enum values.
use super::{
    AcceptedEnumCatalog, AcceptedEnumVariantBody, AcceptedFieldKind, EnumTypeId,
    EnumValueResolutionError,
    admission::{AcceptedValueRef, CanonicalValue},
};
use crate::value::{CanonicalEnumBody, ValueTag};

const ENUM_EQUALITY_KEY_VERSION: u8 = 1;
const UNIT_ENUM_EQUALITY_KEY_BYTES: usize = 11;
const UNIT_ENUM_BODY_TAG: u8 = 0;

/// Equality operations supported by one accepted enum definition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum EqualityCapability {
    PairwiseOnly,
    CanonicalStableKey,
}

/// Typed rejection from unit-enum equality-key construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum EnumEqualityKeyError {
    ContractMismatch,
    UnknownEnumType,
    UnknownEnumVariant,
    PayloadEnumUnsupported,
    ValueBodyMismatch,
}

pub(in crate::db) fn enum_equality_capability(
    catalog: &AcceptedEnumCatalog,
    type_id: EnumTypeId,
) -> Result<EqualityCapability, EnumEqualityKeyError> {
    let definition = catalog
        .enum_type(type_id)
        .ok_or(EnumEqualityKeyError::UnknownEnumType)?;
    let all_unit = !definition.variants_by_id.is_empty()
        && definition
            .variants_by_id
            .values()
            .all(|variant| matches!(variant.body, AcceptedEnumVariantBody::Unit));

    Ok(if all_unit {
        EqualityCapability::CanonicalStableKey
    } else {
        EqualityCapability::PairwiseOnly
    })
}

pub(in crate::db) fn encode_unit_enum_equality_key(
    value: &AcceptedValueRef<'_>,
) -> Result<[u8; UNIT_ENUM_EQUALITY_KEY_BYTES], EnumEqualityKeyError> {
    let AcceptedFieldKind::Enum { type_id } = value.contract().kind() else {
        return Err(EnumEqualityKeyError::ContractMismatch);
    };
    let CanonicalValue::Enum(enum_value) = value.value() else {
        return Err(EnumEqualityKeyError::ContractMismatch);
    };
    if enum_value.type_id() != *type_id {
        return Err(EnumEqualityKeyError::ContractMismatch);
    }
    let selection = value
        .catalog()
        .resolve_value(enum_value.canonical())
        .map_err(|error| match error {
            EnumValueResolutionError::UnknownType => EnumEqualityKeyError::UnknownEnumType,
            EnumValueResolutionError::UnknownVariant => EnumEqualityKeyError::UnknownEnumVariant,
        })?;
    if enum_equality_capability(value.catalog(), *type_id)?
        != EqualityCapability::CanonicalStableKey
    {
        return Err(EnumEqualityKeyError::PayloadEnumUnsupported);
    }
    if !matches!(selection.value_body(), CanonicalEnumBody::Unit) {
        return Err(EnumEqualityKeyError::ValueBodyMismatch);
    }

    let mut encoded = [0_u8; UNIT_ENUM_EQUALITY_KEY_BYTES];
    encoded[0] = ValueTag::Enum.to_u8();
    encoded[1] = ENUM_EQUALITY_KEY_VERSION;
    encoded[2..6].copy_from_slice(&selection.type_id().get().to_be_bytes());
    encoded[6..10].copy_from_slice(&selection.variant_id().get().to_be_bytes());
    encoded[10] = UNIT_ENUM_BODY_TAG;
    Ok(encoded)
}
