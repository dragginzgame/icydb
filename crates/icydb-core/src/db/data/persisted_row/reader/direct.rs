use crate::{
    db::{
        data::{
            RawRow, SparseRequiredRowFieldBytes, StructuralRowContract, StructuralRowDecodeError,
            StructuralRowFieldBytes,
            persisted_row::{
                codec::{ScalarSlotValueRef, decode_scalar_slot_value},
                contract::{
                    decode_runtime_value_from_accepted_field_contract,
                    decode_runtime_value_from_row_contract,
                },
                reader::{
                    metrics::{StructuralReadProbe, finish_direct_probe},
                    primary_key::{
                        materialize_primary_key_slot_value_from_expected_component_with_accepted_field,
                        validate_primary_key_component_from_slot_bytes_with_contract,
                        validate_primary_key_value_from_field_bytes,
                    },
                },
            },
        },
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        schema::AcceptedFieldDecodeContract,
    },
    error::InternalError,
    model::field::LeafCodec,
    value::Value,
};

///
/// DirectStructuralRowFields
///
/// DirectStructuralRowFields owns the opened structural field spans for direct
/// dense and sparse row decode helpers.
/// It exists so those helpers share one reader-owned row-open and primary-key
/// validation boundary before decoding their caller-selected slot layout.
///

struct DirectStructuralRowFields<'row, 'contract> {
    contract: &'contract StructuralRowContract,
    expected_key: PrimaryKeyValue,
    field_bytes: StructuralRowFieldBytes<'row>,
}

impl<'row, 'contract> DirectStructuralRowFields<'row, 'contract> {
    // Open one raw row into structural field spans and validate the persisted
    // primary-key payload against the authoritative row key before any selected
    // slot decode can run.
    fn open(
        raw_row: &'row RawRow,
        contract: &'contract StructuralRowContract,
        expected_key: &PrimaryKeyValue,
    ) -> Result<Self, InternalError> {
        let field_bytes = StructuralRowFieldBytes::from_raw_row_with_contract(raw_row, contract)
            .map_err(StructuralRowDecodeError::into_internal_error)?;
        validate_primary_key_value_from_field_bytes(contract, &field_bytes, expected_key)?;

        Ok(Self {
            contract,
            expected_key: *expected_key,
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
            self.contract,
            &self.field_bytes,
            slot,
            &self.expected_key,
            probe,
        )
    }

    const fn field_count(&self) -> usize {
        self.contract.field_count()
    }

    fn has_active_field_slot(&self, slot: usize) -> bool {
        self.contract.has_active_field_slot(slot)
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

struct DirectSparseRequiredRowField<'row, 'contract> {
    contract: &'contract StructuralRowContract,
    expected_key: PrimaryKeyComponent,
    required_slot: usize,
    field_bytes: SparseRequiredRowFieldBytes<'row>,
}

impl<'row, 'contract> DirectSparseRequiredRowField<'row, 'contract> {
    // Open one raw row through the compact two-span scanner and validate the
    // primary-key span before the requested slot is decoded.
    fn open(
        raw_row: &'row RawRow,
        contract: &'contract StructuralRowContract,
        expected_key: PrimaryKeyComponent,
        required_slot: usize,
    ) -> Result<Self, InternalError> {
        let field_bytes = SparseRequiredRowFieldBytes::from_raw_row_with_contract(
            raw_row,
            contract,
            required_slot,
        )
        .map_err(StructuralRowDecodeError::into_internal_error)?;
        validate_primary_key_component_from_slot_bytes_with_contract(
            contract,
            contract.primary_key_slot(),
            field_bytes.primary_key_field(),
            expected_key,
        )?;

        Ok(Self {
            contract,
            expected_key,
            required_slot,
            field_bytes,
        })
    }

    // Decode the selected slot after sparse row-open and primary-key
    // validation have already succeeded.
    fn decode(&self, probe: &StructuralReadProbe) -> Result<Value, InternalError> {
        if self.required_slot == self.contract.primary_key_slot() {
            return decode_slot_with_contract(
                self.contract,
                self.required_slot,
                &[],
                Some(self.expected_key),
                probe,
            );
        }

        let Some(raw_value) = self.field_bytes.required_field() else {
            return self.contract.missing_slot_value(self.required_slot);
        };

        decode_slot_with_contract(
            self.contract,
            self.required_slot,
            raw_value,
            (self.required_slot == self.contract.primary_key_slot()).then_some(self.expected_key),
            probe,
        )
    }

    const fn field_count(&self) -> usize {
        self.contract.field_count()
    }
}

// Decode one full structural row image directly from persisted field bytes
// without constructing the lazy per-slot cache used by sparse readers.
pub(in crate::db) fn decode_dense_raw_row_with_contract(
    raw_row: &RawRow,
    contract: &StructuralRowContract,
    expected_key: &PrimaryKeyValue,
) -> Result<Vec<Option<Value>>, InternalError> {
    // Phase 1: open and key-validate the row once through the direct reader
    // boundary shared by dense and sparse decode helpers.
    let fields = DirectStructuralRowFields::open(raw_row, contract, expected_key)?;

    // Phase 2: decode every declared slot in one straight-line loop.
    let field_count = fields.field_count();
    let mut values = Vec::with_capacity(field_count);
    // Dense full-row decode intentionally stays off the sparse-reader metrics
    // surface. We still reuse the shared slot helper so PK slots can avoid a
    // second decode, but we do not flush this probe into the aggregate.
    let probe = StructuralReadProbe::begin(field_count);
    for slot in 0..field_count {
        if fields.has_active_field_slot(slot) {
            values.push(Some(fields.decode_slot(slot, &probe)?));
        } else {
            values.push(None);
        }
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
    contract: &StructuralRowContract,
    expected_key: &PrimaryKeyValue,
    required_slots: &[usize],
) -> Result<Vec<Option<Value>>, InternalError> {
    // Phase 1: open and key-validate the row once through the direct reader
    // boundary shared by dense and sparse decode helpers.
    let fields = DirectStructuralRowFields::open(raw_row, contract, expected_key)?;

    // Phase 2: decode only the requested slots without building the general
    // lazy cache shape that sparse executor reads never reuse.
    let field_count = fields.field_count();
    let mut values = vec![None; field_count];
    let probe = StructuralReadProbe::begin(field_count);

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
    contract: &StructuralRowContract,
    expected_key: &PrimaryKeyValue,
    required_slots: &[usize],
) -> Result<Vec<Option<Value>>, InternalError> {
    // Phase 1: open and key-validate the row once through the direct reader
    // boundary shared by dense and sparse decode helpers.
    let fields = DirectStructuralRowFields::open(raw_row, contract, expected_key)?;

    // Phase 2: decode only the requested retained slots into compact layout
    // order, matching the executor-owned retained-slot layout contract.
    let mut values = Vec::with_capacity(required_slots.len());
    let probe = StructuralReadProbe::begin(fields.field_count());

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
    contract: &StructuralRowContract,
    expected_key: &PrimaryKeyValue,
    required_slot: usize,
) -> Result<Option<Value>, InternalError> {
    decode_sparse_required_slot(raw_row, contract, expected_key, required_slot)
}

// Decode one selected slot directly from persisted bytes through the owning
// accepted row contract for all sparse required-slot readers.
fn decode_sparse_required_slot(
    raw_row: &RawRow,
    contract: &StructuralRowContract,
    expected_key: &PrimaryKeyValue,
    required_slot: usize,
) -> Result<Option<Value>, InternalError> {
    if matches!(expected_key, PrimaryKeyValue::Composite(_)) {
        let fields = DirectStructuralRowFields::open(raw_row, contract, expected_key)?;
        let probe = StructuralReadProbe::begin(fields.field_count());
        let value = fields.decode_slot(required_slot, &probe)?;
        finish_direct_probe(&probe);

        return Ok(Some(value));
    }

    let PrimaryKeyValue::Scalar(expected_key) = *expected_key else {
        unreachable!("persisted row invariant");
    };

    // Phase 1: scan and key-validate the row through the compact two-span
    // reader owner used only by narrow one-slot decode paths.
    let field = DirectSparseRequiredRowField::open(raw_row, contract, expected_key, required_slot)?;

    // Phase 2: decode exactly one caller-selected slot and report it through
    // the same sparse-read metrics surface used by the reader-backed path.
    let probe = StructuralReadProbe::begin(field.field_count());
    let value = field.decode(&probe)?;
    finish_direct_probe(&probe);

    Ok(Some(value))
}

// Decode one caller-selected slot directly from the structural field-byte
// spans after row-envelope and primary-key validation have already succeeded.
fn decode_selected_slot_value(
    contract: &StructuralRowContract,
    field_bytes: &StructuralRowFieldBytes<'_>,
    slot: usize,
    expected_key: &PrimaryKeyValue,
    probe: &StructuralReadProbe,
) -> Result<Value, InternalError> {
    let expected_primary_key_component =
        expected_primary_key_component_for_slot(contract, expected_key, slot)?;
    if expected_primary_key_component.is_some() {
        return decode_slot_with_contract(
            contract,
            slot,
            &[],
            expected_primary_key_component,
            probe,
        );
    }

    let Some(raw_value) = field_bytes.field(slot) else {
        return contract.missing_slot_value(slot);
    };

    decode_slot_with_contract(
        contract,
        slot,
        raw_value,
        expected_primary_key_component,
        probe,
    )
}

// Decode one caller-selected slot through accepted field authority.
fn decode_slot_with_contract(
    contract: &StructuralRowContract,
    slot: usize,
    raw_value: &[u8],
    expected_primary_key_component: Option<PrimaryKeyComponent>,
    probe: &StructuralReadProbe,
) -> Result<Value, InternalError> {
    let accepted_field = contract.required_accepted_field_decode_contract(slot)?;

    if let Some(expected_key) = expected_primary_key_component {
        return materialize_primary_key_slot_value_from_expected_component_with_accepted_field(
            accepted_field,
            expected_key,
            probe,
        );
    }
    if accepted_field.uses_canonical_value_wire() {
        probe.record_validated_slot();
        probe.record_validated_non_scalar();
        probe.record_materialized_non_scalar();
        return decode_runtime_value_from_row_contract(contract, slot, raw_value);
    }

    decode_slot_with_accepted_field(accepted_field, raw_value, probe)
}

// Decode one caller-selected slot from raw bytes using accepted row-layout
// metadata only. Direct readers enter here after the row contract has already
// selected the accepted branch for the slot.
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

fn expected_primary_key_component_for_slot(
    contract: &StructuralRowContract,
    expected_key: &PrimaryKeyValue,
    slot: usize,
) -> Result<Option<PrimaryKeyComponent>, InternalError> {
    match *expected_key {
        PrimaryKeyValue::Scalar(component) => {
            Ok((slot == contract.primary_key_slot()).then_some(component))
        }
        PrimaryKeyValue::Composite(composite) => {
            let slots = contract.primary_key_slot_indices();
            if slots.len() != composite.len() {
                return Err(InternalError::persisted_row_decode_corruption());
            }

            Ok(slots
                .iter()
                .position(|primary_key_slot| *primary_key_slot == slot)
                .map(|component_index| composite.components()[component_index]))
        }
    }
}
