use crate::{
    db::{
        data::{
            StructuralRowContract, StructuralRowFieldBytes,
            decode_runtime_value_from_accepted_field_contract,
            persisted_row::{
                codec::{ScalarSlotValueRef, ScalarValueRef, decode_scalar_slot_value},
                reader::metrics::StructuralReadProbe,
            },
        },
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        schema::{AcceptedFieldDecodeContract, AcceptedFieldKind},
    },
    error::InternalError,
    model::field::LeafCodec,
    value::Value,
};

// Convert one scalar slot fast-path value into its decoded primary-key value
// when the field kind is primary-key compatible.
const fn primary_key_component_from_scalar_ref(
    value: ScalarValueRef<'_>,
) -> Option<PrimaryKeyComponent> {
    match value {
        ScalarValueRef::Int(value) => Some(PrimaryKeyComponent::Int64(value)),
        ScalarValueRef::Principal(value) => Some(PrimaryKeyComponent::Principal(value)),
        ScalarValueRef::Subaccount(value) => Some(PrimaryKeyComponent::Subaccount(value)),
        ScalarValueRef::Timestamp(value) => Some(PrimaryKeyComponent::Timestamp(value)),
        ScalarValueRef::Nat(value) => Some(PrimaryKeyComponent::Nat64(value)),
        ScalarValueRef::Ulid(value) => Some(PrimaryKeyComponent::Ulid(value)),
        ScalarValueRef::Unit => Some(PrimaryKeyComponent::Unit),
        _ => None,
    }
}

const fn primary_key_component_from_runtime_value(value: &Value) -> Option<PrimaryKeyComponent> {
    PrimaryKeyComponent::from_runtime_value(value)
}

// Validate the persisted primary-key payload against one authoritative
// primary-key value directly from structural field bytes.
pub(super) fn validate_primary_key_value_from_field_bytes(
    contract: &StructuralRowContract,
    field_bytes: &StructuralRowFieldBytes<'_>,
    expected_key: &PrimaryKeyValue,
) -> Result<(), InternalError> {
    match *expected_key {
        PrimaryKeyValue::Scalar(component) => {
            let primary_key_slot = contract.primary_key_slot();
            let primary_key_name = contract.field_name(primary_key_slot)?;
            let raw_value = field_bytes.field(primary_key_slot).ok_or_else(|| {
                InternalError::persisted_row_declared_field_missing(primary_key_name)
            })?;

            validate_primary_key_component_from_slot_bytes_with_contract(
                contract,
                primary_key_slot,
                raw_value,
                component,
            )
        }
        PrimaryKeyValue::Composite(composite) => {
            let slots = contract.primary_key_slot_indices();
            if slots.len() != composite.len() {
                return Err(InternalError::persisted_row_decode_corruption());
            }

            for (&slot, &component) in slots.iter().zip(composite.components()) {
                let field_name = contract.field_name(slot)?;
                let raw_value = field_bytes.field(slot).ok_or_else(|| {
                    InternalError::persisted_row_declared_field_missing(field_name)
                })?;
                validate_primary_key_component_from_slot_bytes_with_contract(
                    contract, slot, raw_value, component,
                )?;
            }

            Ok(())
        }
    }
}

// Validate one primary-key component payload through the row contract owner.
// Composite primary-key validation calls this for every component slot in
// accepted key order.
pub(super) fn validate_primary_key_component_from_slot_bytes_with_contract(
    contract: &StructuralRowContract,
    slot: usize,
    raw_value: &[u8],
    expected_key: PrimaryKeyComponent,
) -> Result<(), InternalError> {
    let primary_key_field = contract.required_accepted_field_decode_contract(slot)?;

    validate_primary_key_value_from_slot_bytes_with_accepted_field(
        raw_value,
        primary_key_field,
        expected_key,
    )
}

// Validate one primary-key payload through accepted persisted schema metadata.
fn validate_primary_key_value_from_slot_bytes_with_accepted_field(
    raw_value: &[u8],
    field: AcceptedFieldDecodeContract<'_>,
    expected_key: PrimaryKeyComponent,
) -> Result<(), InternalError> {
    let decoded_key = match field.leaf_codec() {
        LeafCodec::Scalar(codec) => {
            match decode_scalar_slot_value(raw_value, codec, field.field_name())? {
                ScalarSlotValueRef::Null => {
                    return Err(InternalError::persisted_row_primary_key_slot_missing(
                        expected_key,
                    ));
                }
                ScalarSlotValueRef::Value(value) => primary_key_component_from_scalar_ref(value)
                    .ok_or_else(|| {
                        InternalError::persisted_row_primary_key_not_primary_key_encodable(
                            expected_key,
                            "",
                        )
                    })?,
            }
        }
        LeafCodec::StructuralFallback => {
            let value = decode_runtime_value_from_accepted_field_contract(field, raw_value)
                .map_err(|err| {
                    InternalError::persisted_row_primary_key_not_primary_key_encodable(
                        expected_key,
                        err,
                    )
                })?;

            if matches!(value, Value::Null) {
                return Err(InternalError::persisted_row_primary_key_slot_missing(
                    expected_key,
                ));
            }

            primary_key_component_from_runtime_value(&value).ok_or_else(|| {
                InternalError::persisted_row_primary_key_not_primary_key_encodable(expected_key, "")
            })?
        }
    };

    if decoded_key != expected_key {
        return Err(InternalError::persisted_row_key_mismatch());
    }

    Ok(())
}

// Materialize one accepted-schema primary-key slot from the authoritative row
// key. This mirrors generated primary-key materialization while keeping
// relation-key recursion on accepted persisted kind metadata.
pub(super) fn materialize_primary_key_slot_value_from_expected_component_with_accepted_field(
    field: AcceptedFieldDecodeContract<'_>,
    expected_key: PrimaryKeyComponent,
    probe: &StructuralReadProbe,
) -> Result<Value, InternalError> {
    probe.record_validated_slot();
    if matches!(field.leaf_codec(), LeafCodec::StructuralFallback) {
        probe.record_validated_non_scalar();
        probe.record_materialized_non_scalar();
    }

    materialize_primary_key_value_from_persisted_kind(field.kind(), expected_key)
}

// Rebuild one primary-key runtime value through the accepted persisted kind.
// Only primary-key-compatible shapes are accepted; relation keys recurse to
// their declared target-key kind.
fn materialize_primary_key_value_from_persisted_kind(
    kind: &AcceptedFieldKind,
    component: PrimaryKeyComponent,
) -> Result<Value, InternalError> {
    match (kind, component) {
        (AcceptedFieldKind::Account, PrimaryKeyComponent::Account(value)) => {
            Ok(Value::Account(value))
        }
        (AcceptedFieldKind::Int64, PrimaryKeyComponent::Int64(value)) => Ok(Value::Int64(value)),
        (AcceptedFieldKind::Int8, PrimaryKeyComponent::Int64(value))
            if i8::try_from(value).is_ok() =>
        {
            Ok(Value::Int64(value))
        }
        (AcceptedFieldKind::Int16, PrimaryKeyComponent::Int64(value))
            if i16::try_from(value).is_ok() =>
        {
            Ok(Value::Int64(value))
        }
        (AcceptedFieldKind::Int32, PrimaryKeyComponent::Int64(value))
            if i32::try_from(value).is_ok() =>
        {
            Ok(Value::Int64(value))
        }
        (AcceptedFieldKind::Int128, PrimaryKeyComponent::Int128(value)) => Ok(Value::Int128(value)),
        (AcceptedFieldKind::Principal, PrimaryKeyComponent::Principal(value)) => {
            Ok(Value::Principal(value))
        }
        (AcceptedFieldKind::Relation { key_kind, .. }, component) => {
            materialize_primary_key_value_from_persisted_kind(key_kind, component)
        }
        (AcceptedFieldKind::Subaccount, PrimaryKeyComponent::Subaccount(value)) => {
            Ok(Value::Subaccount(value))
        }
        (AcceptedFieldKind::Timestamp, PrimaryKeyComponent::Timestamp(value)) => {
            Ok(Value::Timestamp(value))
        }
        (AcceptedFieldKind::Nat64, PrimaryKeyComponent::Nat64(value)) => Ok(Value::Nat64(value)),
        (AcceptedFieldKind::Nat8, PrimaryKeyComponent::Nat64(value))
            if u8::try_from(value).is_ok() =>
        {
            Ok(Value::Nat64(value))
        }
        (AcceptedFieldKind::Nat16, PrimaryKeyComponent::Nat64(value))
            if u16::try_from(value).is_ok() =>
        {
            Ok(Value::Nat64(value))
        }
        (AcceptedFieldKind::Nat32, PrimaryKeyComponent::Nat64(value))
            if u32::try_from(value).is_ok() =>
        {
            Ok(Value::Nat64(value))
        }
        (AcceptedFieldKind::Nat128, PrimaryKeyComponent::Nat128(value)) => Ok(Value::Nat128(value)),
        (AcceptedFieldKind::Ulid, PrimaryKeyComponent::Ulid(value)) => Ok(Value::Ulid(value)),
        (AcceptedFieldKind::Unit, PrimaryKeyComponent::Unit) => Ok(Value::Unit),
        (_, _) => Err(InternalError::persisted_row_decode_corruption()),
    }
}
