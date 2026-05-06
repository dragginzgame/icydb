use crate::{
    db::{
        data::{
            RawRow, SparseRequiredRowFieldBytes, StructuralFieldDecodeContract,
            StructuralRowContract, StructuralRowDecodeError, StructuralRowFieldBytes,
            persisted_row::{
                codec::{ScalarSlotValueRef, decode_scalar_slot_value},
                contract::{
                    decode_runtime_value_from_accepted_field_contract,
                    decode_runtime_value_from_field_contract, validate_non_scalar_slot_value,
                },
                reader::{
                    metrics::{StructuralReadProbe, finish_direct_probe},
                    primary_key::{
                        materialize_primary_key_slot_value_from_expected_key,
                        materialize_primary_key_slot_value_from_expected_key_with_accepted_field,
                        validate_storage_key_from_field_bytes,
                        validate_storage_key_from_primary_key_bytes_with_accepted_field,
                        validate_storage_key_from_primary_key_bytes_with_field,
                    },
                },
            },
        },
        schema::AcceptedFieldDecodeContract,
    },
    error::InternalError,
    model::field::LeafCodec,
    value::{StorageKey, Value},
};

///
/// DirectStructuralRowFields
///
/// DirectStructuralRowFields owns the opened structural field spans for direct
/// dense and sparse row decode helpers.
/// It exists so those helpers share one reader-owned row-open and primary-key
/// validation boundary before decoding their caller-selected slot layout.
///

struct DirectStructuralRowFields<'a> {
    contract: StructuralRowContract,
    expected_key: StorageKey,
    field_bytes: StructuralRowFieldBytes<'a>,
}

impl<'a> DirectStructuralRowFields<'a> {
    // Open one raw row into structural field spans and validate the persisted
    // primary-key payload against the authoritative row key before any selected
    // slot decode can run.
    fn open(
        raw_row: &'a RawRow,
        contract: StructuralRowContract,
        expected_key: StorageKey,
    ) -> Result<Self, InternalError> {
        let field_bytes =
            StructuralRowFieldBytes::from_raw_row_with_contract(raw_row, contract.clone())
                .map_err(StructuralRowDecodeError::into_internal_error)?;
        validate_storage_key_from_field_bytes(contract.clone(), &field_bytes, expected_key)?;

        Ok(Self {
            contract,
            expected_key,
            field_bytes,
        })
    }

    // Decode one caller-selected slot from the already opened and key-validated
    // field spans.
    fn decode_slot(
        &self,
        slot: usize,
        probe: &StructuralReadProbe,
    ) -> Result<Value, InternalError> {
        decode_selected_slot_value(
            self.contract.clone(),
            &self.field_bytes,
            slot,
            self.expected_key,
            probe,
        )
    }
}

///
/// DirectSparseRequiredRowField
///
/// DirectSparseRequiredRowField owns the narrow one-slot sparse row decode
/// state.
/// It keeps the selected-slot span path separate from full-span direct decode
/// while still centralizing primary-key validation before value materialization.
///

struct DirectSparseRequiredRowField<'a> {
    contract: StructuralRowContract,
    expected_key: StorageKey,
    required_slot: usize,
    required_field: Option<StructuralFieldDecodeContract>,
    field_bytes: SparseRequiredRowFieldBytes<'a>,
}

impl<'a> DirectSparseRequiredRowField<'a> {
    // Open one raw row through the compact two-span scanner and validate the
    // primary-key span before the requested slot is decoded.
    fn open(
        raw_row: &'a RawRow,
        contract: StructuralRowContract,
        expected_key: StorageKey,
        required_slot: usize,
        required_field: Option<StructuralFieldDecodeContract>,
        primary_key_field: Option<StructuralFieldDecodeContract>,
    ) -> Result<Self, InternalError> {
        let field_bytes = SparseRequiredRowFieldBytes::from_raw_row_with_contract(
            raw_row,
            contract.clone(),
            required_slot,
        )
        .map_err(StructuralRowDecodeError::into_internal_error)?;
        if let Some(accepted_field) =
            contract.accepted_field_decode_contract(contract.primary_key_slot())
        {
            validate_storage_key_from_primary_key_bytes_with_accepted_field(
                field_bytes.primary_key_field(),
                accepted_field,
                expected_key,
            )?;
        } else {
            let primary_key_field = primary_key_field.map_or_else(
                || contract.field_decode_contract(contract.primary_key_slot()),
                Ok,
            )?;
            validate_storage_key_from_primary_key_bytes_with_field(
                field_bytes.primary_key_field(),
                primary_key_field,
                expected_key,
            )?;
        }

        Ok(Self {
            contract,
            expected_key,
            required_slot,
            required_field,
            field_bytes,
        })
    }

    // Decode the selected slot after sparse row-open and primary-key
    // validation have already succeeded.
    fn decode(&self, probe: &StructuralReadProbe) -> Result<Value, InternalError> {
        if let Some(required_field) = self.required_field {
            return decode_slot_with_contract_and_field(
                &self.contract,
                self.required_slot,
                required_field,
                self.field_bytes.required_field(),
                self.expected_key,
                self.required_slot == self.contract.primary_key_slot(),
                probe,
            );
        }

        let Some(raw_value) = self.field_bytes.required_field() else {
            return self.contract.missing_slot_value(self.required_slot);
        };

        decode_slot_with_contract(
            &self.contract,
            self.required_slot,
            raw_value,
            self.expected_key,
            self.required_slot == self.contract.primary_key_slot(),
            probe,
        )
    }
}

// Decode one full structural row image directly from persisted field bytes
// without constructing the lazy per-slot cache used by sparse readers.
pub(in crate::db) fn decode_dense_raw_row_with_contract(
    raw_row: &RawRow,
    contract: StructuralRowContract,
    expected_key: StorageKey,
) -> Result<Vec<Option<Value>>, InternalError> {
    // Phase 1: open and key-validate the row once through the direct reader
    // boundary shared by dense and sparse decode helpers.
    let fields = DirectStructuralRowFields::open(raw_row, contract.clone(), expected_key)?;

    // Phase 2: decode every declared slot in one straight-line loop.
    let mut values = Vec::with_capacity(contract.field_count());
    // Dense full-row decode intentionally stays off the sparse-reader metrics
    // surface. We still reuse the shared slot helper so PK slots can avoid a
    // second decode, but we do not flush this probe into the aggregate.
    let probe = StructuralReadProbe::begin(contract.field_count());
    for slot in 0..contract.field_count() {
        values.push(Some(fields.decode_slot(slot, &probe)?));
    }

    Ok(values)
}

/// Decode one sparse slot subset directly from persisted field bytes without
/// constructing the per-slot lazy cache used by general `StructuralSlotReader`
/// callers.
///
/// Executor sparse row paths usually touch each requested slot exactly once,
/// so they can skip the reader-owned cache initialization loop and decode the
/// selected slots directly after the shared row-envelope and primary-key
/// validation steps.
pub(in crate::db) fn decode_sparse_raw_row_with_contract(
    raw_row: &RawRow,
    contract: StructuralRowContract,
    expected_key: StorageKey,
    required_slots: &[usize],
) -> Result<Vec<Option<Value>>, InternalError> {
    // Phase 1: open and key-validate the row once through the direct reader
    // boundary shared by dense and sparse decode helpers.
    let fields = DirectStructuralRowFields::open(raw_row, contract.clone(), expected_key)?;

    // Phase 2: decode only the requested slots without building the general
    // lazy cache shape that sparse executor reads never reuse.
    let mut values = vec![None; contract.field_count()];
    let probe = StructuralReadProbe::begin(contract.field_count());

    for &slot in required_slots {
        values[slot] = Some(fields.decode_slot(slot, &probe)?);
    }

    finish_direct_probe(&probe);

    Ok(values)
}

/// Decode one compact sparse slot buffer directly from persisted field bytes
/// without constructing the surrounding `StructuralSlotReader`.
pub(in crate::db) fn decode_sparse_indexed_raw_row_with_contract(
    raw_row: &RawRow,
    contract: StructuralRowContract,
    expected_key: StorageKey,
    required_slots: &[usize],
) -> Result<Vec<Option<Value>>, InternalError> {
    // Phase 1: open and key-validate the row once through the direct reader
    // boundary shared by dense and sparse decode helpers.
    let fields = DirectStructuralRowFields::open(raw_row, contract.clone(), expected_key)?;

    // Phase 2: decode only the requested retained slots into compact layout
    // order, matching the executor-owned retained-slot layout contract.
    let mut values = Vec::with_capacity(required_slots.len());
    let probe = StructuralReadProbe::begin(contract.field_count());

    for &slot in required_slots {
        values.push(Some(fields.decode_slot(slot, &probe)?));
    }

    finish_direct_probe(&probe);

    Ok(values)
}

/// Decode one selected slot value directly from persisted field bytes without
/// constructing the surrounding row-reader cache.
pub(in crate::db) fn decode_sparse_required_slot_with_contract(
    raw_row: &RawRow,
    contract: StructuralRowContract,
    expected_key: StorageKey,
    required_slot: usize,
) -> Result<Option<Value>, InternalError> {
    decode_sparse_required_slot_with_optional_fields(
        raw_row,
        contract,
        expected_key,
        required_slot,
        None,
        None,
    )
}

/// Decode one selected slot value directly from persisted field bytes using
/// caller-frozen field metadata instead of rediscovering the selected and
/// primary-key field contracts from the structural row contract.
pub(in crate::db) fn decode_sparse_required_slot_with_contract_and_fields(
    raw_row: &RawRow,
    contract: StructuralRowContract,
    expected_key: StorageKey,
    required_slot: usize,
    required_field: StructuralFieldDecodeContract,
    primary_key_field: StructuralFieldDecodeContract,
) -> Result<Option<Value>, InternalError> {
    decode_sparse_required_slot_with_optional_fields(
        raw_row,
        contract,
        expected_key,
        required_slot,
        Some(required_field),
        Some(primary_key_field),
    )
}

// Decode one selected slot directly from persisted bytes, optionally using
// caller-frozen generated-compatible contracts for bridges that intentionally
// still avoid fallback contract lookup in generated-only layouts.
fn decode_sparse_required_slot_with_optional_fields(
    raw_row: &RawRow,
    contract: StructuralRowContract,
    expected_key: StorageKey,
    required_slot: usize,
    required_field: Option<StructuralFieldDecodeContract>,
    primary_key_field: Option<StructuralFieldDecodeContract>,
) -> Result<Option<Value>, InternalError> {
    // Phase 1: scan and key-validate the row through the compact two-span
    // reader owner used only by narrow one-slot decode paths.
    let field = DirectSparseRequiredRowField::open(
        raw_row,
        contract.clone(),
        expected_key,
        required_slot,
        required_field,
        primary_key_field,
    )?;

    // Phase 2: decode exactly one caller-selected slot and report it through
    // the same sparse-read metrics surface used by the reader-backed path.
    let probe = StructuralReadProbe::begin(contract.field_count());
    let value = field.decode(&probe)?;
    finish_direct_probe(&probe);

    Ok(Some(value))
}

// Decode one caller-selected slot directly from the structural field-byte
// spans after row-envelope and primary-key validation have already succeeded.
fn decode_selected_slot_value(
    contract: StructuralRowContract,
    field_bytes: &StructuralRowFieldBytes<'_>,
    slot: usize,
    expected_key: StorageKey,
    probe: &StructuralReadProbe,
) -> Result<Value, InternalError> {
    let Some(raw_value) = field_bytes.field(slot) else {
        return contract.missing_slot_value(slot);
    };

    decode_slot_with_contract(
        &contract,
        slot,
        raw_value,
        expected_key,
        slot == contract.primary_key_slot(),
        probe,
    )
}

// Decode one caller-selected slot through accepted field contracts when the
// row contract carries them, falling back to the generated-compatible contract
// for generated-only row layouts and primary-key materialization.
fn decode_slot_with_contract(
    contract: &StructuralRowContract,
    slot: usize,
    raw_value: &[u8],
    expected_key: StorageKey,
    is_primary: bool,
    probe: &StructuralReadProbe,
) -> Result<Value, InternalError> {
    if is_primary {
        if let Some(accepted_field) = contract.accepted_field_decode_contract(slot) {
            return materialize_primary_key_slot_value_from_expected_key_with_accepted_field(
                accepted_field,
                expected_key,
                probe,
            );
        }
    } else if let Some(accepted_field) = contract.accepted_field_decode_contract(slot) {
        return decode_slot_with_accepted_field(accepted_field, raw_value, probe);
    }

    let field = contract.field_decode_contract(slot)?;

    decode_slot_with_field(field, raw_value, expected_key, is_primary, probe)
}

// Decode one caller-selected slot when the generated-compatible field contract
// has already been resolved by the caller. Accepted row-layout contracts still
// take priority for non-primary-key slots so sparse and dense direct decoders
// share the same persisted-schema authority boundary.
fn decode_slot_with_contract_and_field(
    contract: &StructuralRowContract,
    slot: usize,
    field: StructuralFieldDecodeContract,
    raw_value: Option<&[u8]>,
    expected_key: StorageKey,
    is_primary: bool,
    probe: &StructuralReadProbe,
) -> Result<Value, InternalError> {
    let Some(raw_value) = raw_value else {
        return contract.missing_slot_value(slot);
    };

    if is_primary {
        if let Some(accepted_field) = contract.accepted_field_decode_contract(slot) {
            return materialize_primary_key_slot_value_from_expected_key_with_accepted_field(
                accepted_field,
                expected_key,
                probe,
            );
        }

        return decode_slot_with_field(field, raw_value, expected_key, true, probe);
    }

    if let Some(accepted_field) = contract.accepted_field_decode_contract(slot) {
        return decode_slot_with_accepted_field(accepted_field, raw_value, probe);
    }

    decode_slot_with_field(field, raw_value, expected_key, false, probe)
}

// Decode one caller-selected slot from raw bytes using accepted row-layout
// metadata only. Dense direct readers enter here directly; sparse bridge
// readers use the same helper after their generated-compatibility field has
// already been supplied for the fallback case.
fn decode_slot_with_accepted_field(
    field: AcceptedFieldDecodeContract<'_>,
    raw_value: &[u8],
    probe: &StructuralReadProbe,
) -> Result<Value, InternalError> {
    match field.leaf_codec() {
        LeafCodec::Scalar(codec) => {
            probe.record_validated_slot();

            match decode_scalar_slot_value(raw_value, codec, field.field_name())? {
                ScalarSlotValueRef::Null => Ok(Value::Null),
                ScalarSlotValueRef::Value(value) => Ok(value.into_value()),
            }
        }
        LeafCodec::StructuralFallback => {
            probe.record_validated_slot();
            probe.record_validated_non_scalar();
            probe.record_materialized_non_scalar();

            decode_runtime_value_from_accepted_field_contract(field, raw_value)
        }
    }
}

// Decode one caller-selected slot from raw field bytes once the caller has
// already resolved the field contract and primary-key role for that slot.
fn decode_slot_with_field(
    field: StructuralFieldDecodeContract,
    raw_value: &[u8],
    expected_key: StorageKey,
    is_primary: bool,
    probe: &StructuralReadProbe,
) -> Result<Value, InternalError> {
    // The direct row-decode helpers already validated the primary-key slot
    // against `expected_key` before they enter the per-slot decode loop.
    // Reconstruct the semantic PK value from that authoritative key instead of
    // decoding the same slot bytes again when the caller also projects `id`.
    if is_primary {
        return materialize_primary_key_slot_value_from_expected_key(field, expected_key, probe);
    }
    match field.leaf_codec() {
        LeafCodec::Scalar(codec) => {
            probe.record_validated_slot();

            match decode_scalar_slot_value(raw_value, codec, field.name())? {
                ScalarSlotValueRef::Null => Ok(Value::Null),
                ScalarSlotValueRef::Value(value) => Ok(value.into_value()),
            }
        }
        LeafCodec::StructuralFallback => {
            probe.record_validated_slot();
            probe.record_validated_non_scalar();
            probe.record_materialized_non_scalar();
            validate_non_scalar_slot_value(raw_value, field)?;

            decode_runtime_value_from_field_contract(field, raw_value)
        }
    }
}
