use crate::{
    db::data::{
        DataKey, RawRow, StructuralRowContract, StructuralRowDecodeError, StructuralRowFieldBytes,
    },
    error::InternalError,
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel, LeafCodec},
    },
    value::{StorageKey, Value},
};
#[cfg(any(test, feature = "structural-read-metrics"))]
use std::cell::{Cell, RefCell};
use std::{borrow::Cow, cell::OnceCell};

use crate::db::data::persisted_row::{
    codec::{ScalarSlotValueRef, decode_scalar_slot_value},
    contract::{
        decode_slot_value_for_field, storage_key_from_scalar_ref, validate_non_scalar_slot_value,
    },
    types::{CanonicalSlotReader, SlotReader},
};

///
/// StructuralSlotReader
///
/// StructuralSlotReader adapts the current persisted-row bytes into the
/// canonical slot-reader seam.
/// It validates the persisted row envelope eagerly, then validates and
/// materializes individual slots only when a caller actually touches them.
/// That keeps row-backed selective reads from paying an O(field_count) decode
/// loop before the first real slot access.
///

pub(in crate::db) struct StructuralSlotReader<'a> {
    model: Option<&'static EntityModel>,
    contract: StructuralRowContract,
    field_bytes: StructuralRowFieldBytes<'a>,
    pub(in crate::db::data::persisted_row) cached_values: Vec<CachedSlotValue>,
    #[cfg(any(test, feature = "structural-read-metrics"))]
    metrics: StructuralReadProbe,
}

impl<'a> StructuralSlotReader<'a> {
    /// Build one slot reader over one persisted row using the current structural row scanner.
    pub(in crate::db) fn from_raw_row(
        raw_row: &'a RawRow,
        model: &'static EntityModel,
    ) -> Result<Self, InternalError> {
        let reader = Self::from_raw_row_with_model(raw_row, model)?;
        reader.validate_all_declared_slots()?;

        Ok(reader)
    }

    /// Build one slot reader over one persisted row while preserving lazy
    /// slot validation for caller-owned selective-read experiments and tests.
    #[cfg(test)]
    pub(in crate::db) fn from_raw_row_lazy(
        raw_row: &'a RawRow,
        model: &'static EntityModel,
    ) -> Result<Self, InternalError> {
        Self::from_raw_row_with_model(raw_row, model)
    }

    /// Build one slot reader over one persisted row using one static
    /// structural row contract without retaining the full entity model.
    pub(in crate::db) fn from_raw_row_with_contract(
        raw_row: &'a RawRow,
        contract: StructuralRowContract,
    ) -> Result<Self, InternalError> {
        let field_bytes = StructuralRowFieldBytes::from_raw_row_with_contract(raw_row, contract)
            .map_err(StructuralRowDecodeError::into_internal_error)?;
        let reader = Self {
            model: None,
            contract,
            field_bytes,
            cached_values: build_initial_slot_cache(contract),
            #[cfg(any(test, feature = "structural-read-metrics"))]
            metrics: StructuralReadProbe::begin(contract.field_count()),
        };

        Ok(reader)
    }

    // Build one slot reader over one persisted row while retaining the full
    // entity model for typed slot-reader seams that still require it.
    fn from_raw_row_with_model(
        raw_row: &'a RawRow,
        model: &'static EntityModel,
    ) -> Result<Self, InternalError> {
        let contract = StructuralRowContract::from_model(model);
        let field_bytes = StructuralRowFieldBytes::from_raw_row_with_contract(raw_row, contract)
            .map_err(StructuralRowDecodeError::into_internal_error)?;
        let reader = Self {
            model: Some(model),
            contract,
            field_bytes,
            cached_values: build_initial_slot_cache(contract),
            #[cfg(any(test, feature = "structural-read-metrics"))]
            metrics: StructuralReadProbe::begin(contract.field_count()),
        };

        Ok(reader)
    }

    /// Return the owning structural model.
    #[must_use]
    pub(in crate::db) const fn model(&self) -> &'static EntityModel {
        self.model
            .expect("model-backed structural slot reader required by typed slot-reader seam")
    }

    /// Validate the decoded primary-key slot against the authoritative row key.
    pub(in crate::db) fn validate_storage_key(
        &self,
        data_key: &DataKey,
    ) -> Result<(), InternalError> {
        self.validate_storage_key_value(data_key.storage_key())
    }

    // Validate the decoded primary-key slot against one authoritative storage
    // key without rebuilding a full `DataKey` wrapper at the call site.
    pub(in crate::db) fn validate_storage_key_value(
        &self,
        expected_key: StorageKey,
    ) -> Result<(), InternalError> {
        let Some(model) = self.model else {
            return self.validate_storage_key_value_with_contract(
                expected_key,
                self.contract.primary_key_slot(),
            );
        };
        let primary_key_slot = model.primary_key_slot();
        let field = self.field_model(primary_key_slot)?;
        let decoded_key = match self.get_scalar(primary_key_slot)? {
            Some(ScalarSlotValueRef::Null) => None,
            Some(ScalarSlotValueRef::Value(value)) => storage_key_from_scalar_ref(value),
            None => Some(
                crate::db::data::decode_storage_key_field_bytes(
                    self.required_field_bytes(primary_key_slot, field.name())?,
                    field.kind,
                )
                .map_err(|err| {
                    InternalError::persisted_row_primary_key_not_storage_encodable(
                        expected_key,
                        err,
                    )
                })?,
            ),
        };
        let Some(decoded_key) = decoded_key else {
            return Err(InternalError::persisted_row_primary_key_slot_missing(
                expected_key,
            ));
        };

        if decoded_key != expected_key {
            return Err(InternalError::persisted_row_key_mismatch(
                expected_key,
                decoded_key,
            ));
        }

        Ok(())
    }

    // Validate the decoded primary-key slot through one model-free structural
    // row contract used by executor-owned decode/runtime paths.
    fn validate_storage_key_value_with_contract(
        &self,
        expected_key: StorageKey,
        primary_key_slot: usize,
    ) -> Result<(), InternalError> {
        let field = self.field_model(primary_key_slot)?;
        let decoded_key = match self.get_scalar(primary_key_slot)? {
            Some(ScalarSlotValueRef::Null) => None,
            Some(ScalarSlotValueRef::Value(value)) => storage_key_from_scalar_ref(value),
            None => Some(
                crate::db::data::decode_storage_key_field_bytes(
                    self.required_field_bytes(primary_key_slot, field.name())?,
                    field.kind,
                )
                .map_err(|err| {
                    InternalError::persisted_row_primary_key_not_storage_encodable(
                        expected_key,
                        err,
                    )
                })?,
            ),
        };
        let Some(decoded_key) = decoded_key else {
            return Err(InternalError::persisted_row_primary_key_slot_missing(
                expected_key,
            ));
        };

        if decoded_key != expected_key {
            return Err(InternalError::persisted_row_key_mismatch(
                expected_key,
                decoded_key,
            ));
        }

        Ok(())
    }

    // Resolve one field model entry by stable slot index.
    fn field_model(&self, slot: usize) -> Result<&FieldModel, InternalError> {
        self.contract.fields().get(slot).ok_or_else(|| {
            InternalError::persisted_row_slot_lookup_out_of_bounds(
                self.contract.entity_path(),
                slot,
            )
        })
    }

    // Validate one scalar slot at most once and freeze the compact validated
    // scalar cache state shared by both scalar reads and semantic materialization.
    fn required_validated_scalar_slot_value(
        &self,
        slot: usize,
        validated: &OnceCell<ValidatedScalarSlotValue>,
    ) -> Result<ValidatedScalarSlotValue, InternalError> {
        if let Some(validated) = validated.get() {
            return Ok(*validated);
        }

        let field = self.field_model(slot)?;
        let raw_value = self.required_field_bytes(slot, field.name())?;
        let LeafCodec::Scalar(codec) = field.leaf_codec() else {
            return Err(InternalError::persisted_row_decode_failed(format!(
                "validated scalar cache routed through non-scalar field contract: slot={slot}",
            )));
        };
        #[cfg(any(test, feature = "structural-read-metrics"))]
        self.metrics.record_validated_slot();
        let validated_value =
            validated_scalar_slot_value(decode_scalar_slot_value(raw_value, codec, field.name())?);
        let _ = validated.set(validated_value);

        Ok(validated_value)
    }

    // Borrow one declared slot value from the validated structural cache,
    // materializing the semantic `Value` lazily when the caller first touches
    // that slot.
    pub(in crate::db::data::persisted_row) fn required_cached_value(
        &self,
        slot: usize,
    ) -> Result<&Value, InternalError> {
        let cached = self.cached_values.get(slot).ok_or_else(|| {
            InternalError::persisted_row_slot_cache_lookup_out_of_bounds(
                self.contract.entity_path(),
                slot,
            )
        })?;

        match cached {
            CachedSlotValue::Scalar {
                validated,
                materialized,
            } => {
                let validated = self.required_validated_scalar_slot_value(slot, validated)?;
                if materialized.get().is_none() {
                    let value = materialize_validated_scalar_slot_value(
                        validated,
                        self.contract,
                        &self.field_bytes,
                        slot,
                    )?;
                    let _ = materialized.set(value);
                }

                materialized.get().ok_or_else(|| {
                    InternalError::persisted_row_decode_failed(format!(
                        "structural scalar cache failed to materialize deferred value: slot={slot}",
                    ))
                })
            }
            CachedSlotValue::Deferred { materialized } => {
                let field = self.field_model(slot)?;
                let raw_value = self.required_field_bytes(slot, field.name())?;
                if materialized.get().is_none() {
                    #[cfg(any(test, feature = "structural-read-metrics"))]
                    {
                        self.metrics.record_validated_slot();
                        self.metrics.record_validated_non_scalar();
                        self.metrics.record_materialized_non_scalar();
                    }
                    validate_non_scalar_slot_value(raw_value, field)?;
                    let value = decode_slot_value_for_field(field, raw_value)?;
                    let _ = materialized.set(value);
                }

                materialized.get().ok_or_else(|| {
                    InternalError::persisted_row_decode_failed(format!(
                        "structural slot cache failed to materialize deferred value: slot={slot}",
                    ))
                })
            }
        }
    }

    // Borrow one declared slot payload, treating absence as a persisted-row
    // invariant violation instead of a normal structural branch.
    pub(in crate::db) fn required_field_bytes(
        &self,
        slot: usize,
        field_name: &str,
    ) -> Result<&[u8], InternalError> {
        self.field_bytes
            .field(slot)
            .ok_or_else(|| InternalError::persisted_row_declared_field_missing(field_name))
    }

    // Validate every declared slot once at the model-backed structural row
    // boundary so fail-closed callers reject malformed unused fields before
    // projection, relation, or commit logic runs.
    fn validate_all_declared_slots(&self) -> Result<(), InternalError> {
        for (slot, field) in self.contract.fields().iter().enumerate() {
            let raw_value = self.required_field_bytes(slot, field.name())?;

            match field.leaf_codec() {
                LeafCodec::Scalar(codec) => {
                    #[cfg(any(test, feature = "structural-read-metrics"))]
                    self.metrics.record_validated_slot();
                    decode_scalar_slot_value(raw_value, codec, field.name())?;
                }
                LeafCodec::StructuralFallback => {
                    #[cfg(any(test, feature = "structural-read-metrics"))]
                    {
                        self.metrics.record_validated_slot();
                        self.metrics.record_validated_non_scalar();
                    }
                    validate_non_scalar_slot_value(raw_value, field)?;
                }
            }
        }

        Ok(())
    }
}

// Decode one full structural row image directly from persisted field bytes
// without constructing the lazy per-slot cache used by sparse readers.
pub(in crate::db) fn decode_dense_raw_row_with_contract(
    raw_row: &RawRow,
    contract: StructuralRowContract,
    expected_key: StorageKey,
) -> Result<Vec<Option<Value>>, InternalError> {
    // Phase 1: open the canonical field-byte spans once through the shared
    // structural row envelope scanner.
    let field_bytes = StructuralRowFieldBytes::from_raw_row_with_contract(raw_row, contract)
        .map_err(StructuralRowDecodeError::into_internal_error)?;

    // Phase 2: validate the persisted primary-key payload directly against the
    // authoritative storage key before decoding the remaining fields.
    validate_storage_key_value_from_field_bytes(contract, &field_bytes, expected_key)?;

    // Phase 3: decode every declared slot in one straight-line loop.
    let mut values = Vec::with_capacity(contract.field_count());
    // Dense full-row decode intentionally stays off the sparse-reader metrics
    // surface. We still reuse the shared slot helper so PK slots can avoid a
    // second decode, but we do not flush this probe into the aggregate.
    let probe = StructuralReadProbe::begin(contract.field_count());
    for slot in 0..contract.field_count() {
        values.push(Some(decode_selected_slot_value(
            contract,
            &field_bytes,
            slot,
            expected_key,
            &probe,
        )?));
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
    // Phase 1: open the canonical field-byte spans once through the shared
    // structural row envelope scanner.
    let field_bytes = StructuralRowFieldBytes::from_raw_row_with_contract(raw_row, contract)
        .map_err(StructuralRowDecodeError::into_internal_error)?;

    // Phase 2: validate the persisted primary-key payload directly against the
    // authoritative storage key before decoding caller-selected fields.
    validate_storage_key_value_from_field_bytes(contract, &field_bytes, expected_key)?;

    // Phase 3: decode only the requested slots without building the general
    // lazy cache shape that sparse executor reads never reuse.
    let mut values = vec![None; contract.field_count()];
    let probe = StructuralReadProbe::begin(contract.field_count());

    for &slot in required_slots {
        values[slot] = Some(decode_selected_slot_value(
            contract,
            &field_bytes,
            slot,
            expected_key,
            &probe,
        )?);
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
    // Phase 1: open the canonical field-byte spans once through the shared
    // structural row envelope scanner.
    let field_bytes = StructuralRowFieldBytes::from_raw_row_with_contract(raw_row, contract)
        .map_err(StructuralRowDecodeError::into_internal_error)?;

    // Phase 2: validate the persisted primary-key payload directly against the
    // authoritative storage key before decoding caller-selected fields.
    validate_storage_key_value_from_field_bytes(contract, &field_bytes, expected_key)?;

    // Phase 3: decode only the requested retained slots into compact layout
    // order, matching the executor-owned retained-slot layout contract.
    let mut values = Vec::with_capacity(required_slots.len());
    let probe = StructuralReadProbe::begin(contract.field_count());

    for &slot in required_slots {
        values.push(Some(decode_selected_slot_value(
            contract,
            &field_bytes,
            slot,
            expected_key,
            &probe,
        )?));
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
    let required_field = contract.fields().get(required_slot).ok_or_else(|| {
        InternalError::persisted_row_slot_lookup_out_of_bounds(
            contract.entity_path(),
            required_slot,
        )
    })?;
    let primary_key_field = contract
        .fields()
        .get(contract.primary_key_slot())
        .ok_or_else(|| {
            InternalError::persisted_row_slot_lookup_out_of_bounds(
                contract.entity_path(),
                contract.primary_key_slot(),
            )
        })?;

    decode_sparse_required_slot_with_contract_and_fields(
        raw_row,
        contract,
        expected_key,
        required_slot,
        required_field,
        primary_key_field,
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
    required_field: &FieldModel,
    primary_key_field: &FieldModel,
) -> Result<Option<Value>, InternalError> {
    // Phase 1: open the canonical field-byte spans once through the shared
    // structural row envelope scanner.
    let field_bytes = StructuralRowFieldBytes::from_raw_row_with_contract(raw_row, contract)
        .map_err(StructuralRowDecodeError::into_internal_error)?;

    // Phase 2: validate the persisted primary-key payload directly against the
    // authoritative storage key before decoding the requested field.
    validate_storage_key_value_from_field_bytes_with_field(
        contract,
        &field_bytes,
        expected_key,
        primary_key_field,
    )?;

    // Phase 3: decode exactly one caller-selected slot and report it through
    // the same sparse-read metrics surface used by the reader-backed path.
    let probe = StructuralReadProbe::begin(contract.field_count());
    let value = decode_selected_slot_value_with_field(
        contract,
        &field_bytes,
        required_slot,
        required_field,
        expected_key,
        &probe,
    )?;
    finish_direct_probe(&probe);

    Ok(Some(value))
}

// Validate the persisted primary-key payload against one authoritative storage
// key directly from structural field bytes.
fn validate_storage_key_value_from_field_bytes(
    contract: StructuralRowContract,
    field_bytes: &StructuralRowFieldBytes<'_>,
    expected_key: StorageKey,
) -> Result<(), InternalError> {
    let primary_key_field = contract
        .fields()
        .get(contract.primary_key_slot())
        .ok_or_else(|| {
            InternalError::persisted_row_slot_lookup_out_of_bounds(
                contract.entity_path(),
                contract.primary_key_slot(),
            )
        })?;

    validate_storage_key_value_from_field_bytes_with_field(
        contract,
        field_bytes,
        expected_key,
        primary_key_field,
    )
}

// Validate the persisted primary-key payload against one authoritative storage
// key using caller-frozen primary-key field metadata.
fn validate_storage_key_value_from_field_bytes_with_field(
    contract: StructuralRowContract,
    field_bytes: &StructuralRowFieldBytes<'_>,
    expected_key: StorageKey,
    primary_key_field: &FieldModel,
) -> Result<(), InternalError> {
    let primary_key_slot = contract.primary_key_slot();
    let raw_value = field_bytes.field(primary_key_slot).ok_or_else(|| {
        InternalError::persisted_row_declared_field_missing(primary_key_field.name())
    })?;
    let decoded_key = match primary_key_field.leaf_codec() {
        LeafCodec::Scalar(codec) => {
            match decode_scalar_slot_value(raw_value, codec, primary_key_field.name())? {
                ScalarSlotValueRef::Null => {
                    return Err(InternalError::persisted_row_primary_key_slot_missing(
                        expected_key,
                    ));
                }
                ScalarSlotValueRef::Value(value) => {
                    storage_key_from_scalar_ref(value).ok_or_else(|| {
                        InternalError::persisted_row_primary_key_not_storage_encodable(
                            expected_key,
                            format!(
                                "scalar primary-key field '{}' is not storage-key compatible",
                                primary_key_field.name()
                            ),
                        )
                    })?
                }
            }
        }
        LeafCodec::StructuralFallback => {
            crate::db::data::decode_storage_key_field_bytes(raw_value, primary_key_field.kind())
                .map_err(|err| {
                    InternalError::persisted_row_primary_key_not_storage_encodable(
                        expected_key,
                        err,
                    )
                })?
        }
    };

    if decoded_key != expected_key {
        return Err(InternalError::persisted_row_key_mismatch(
            expected_key,
            decoded_key,
        ));
    }

    Ok(())
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
    let field = contract.fields().get(slot).ok_or_else(|| {
        InternalError::persisted_row_slot_lookup_out_of_bounds(contract.entity_path(), slot)
    })?;

    decode_selected_slot_value_with_field(contract, field_bytes, slot, field, expected_key, probe)
}

// Decode one caller-selected slot using caller-frozen field metadata so hot
// single-slot decode paths do not rediscover the same field contract per row.
fn decode_selected_slot_value_with_field(
    contract: StructuralRowContract,
    field_bytes: &StructuralRowFieldBytes<'_>,
    slot: usize,
    field: &FieldModel,
    expected_key: StorageKey,
    probe: &StructuralReadProbe,
) -> Result<Value, InternalError> {
    // The direct row-decode helpers already validated the primary-key slot
    // against `expected_key` before they enter the per-slot decode loop.
    // Reconstruct the semantic PK value from that authoritative key instead of
    // decoding the same slot bytes again when the caller also projects `id`.
    if slot == contract.primary_key_slot() {
        return materialize_primary_key_slot_value_from_expected_key(field, expected_key, probe);
    }

    let raw_value = field_bytes
        .field(slot)
        .ok_or_else(|| InternalError::persisted_row_declared_field_missing(field.name()))?;

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

            decode_slot_value_for_field(field, raw_value)
        }
    }
}

// Materialize the already-validated primary-key slot directly from the
// authoritative storage key carried by the row boundary.
fn materialize_primary_key_slot_value_from_expected_key(
    field: &FieldModel,
    expected_key: StorageKey,
    probe: &StructuralReadProbe,
) -> Result<Value, InternalError> {
    probe.record_validated_slot();
    if matches!(field.leaf_codec(), LeafCodec::StructuralFallback) {
        probe.record_validated_non_scalar();
        probe.record_materialized_non_scalar();
    }

    match (field.kind(), expected_key) {
        (FieldKind::Account, StorageKey::Account(value)) => Ok(Value::Account(value)),
        (FieldKind::Int, StorageKey::Int(value)) => Ok(Value::Int(value)),
        (FieldKind::Principal, StorageKey::Principal(value)) => Ok(Value::Principal(value)),
        (FieldKind::Subaccount, StorageKey::Subaccount(value)) => Ok(Value::Subaccount(value)),
        (FieldKind::Timestamp, StorageKey::Timestamp(value)) => Ok(Value::Timestamp(value)),
        (FieldKind::Uint, StorageKey::Uint(value)) => Ok(Value::Uint(value)),
        (FieldKind::Ulid, StorageKey::Ulid(value)) => Ok(Value::Ulid(value)),
        (FieldKind::Unit, StorageKey::Unit) => Ok(Value::Unit),
        (kind, storage_key) => Err(InternalError::persisted_row_decode_failed(format!(
            "validated primary-key storage key does not match field kind: field='{}' kind={kind:?} storage_key={storage_key:?}",
            field.name(),
        ))),
    }
}

// Build the initial per-slot cache shape from the static field contract only.
// This avoids a row-open decode loop while still letting access-time readers
// branch cheaply by leaf codec.
fn build_initial_slot_cache(contract: StructuralRowContract) -> Vec<CachedSlotValue> {
    contract
        .fields()
        .iter()
        .map(|field| match field.leaf_codec() {
            LeafCodec::Scalar(_) => CachedSlotValue::Scalar {
                validated: OnceCell::new(),
                materialized: OnceCell::new(),
            },
            LeafCodec::StructuralFallback => CachedSlotValue::Deferred {
                materialized: OnceCell::new(),
            },
        })
        .collect()
}

// Flush one direct sparse-read probe into the thread-local structural metrics
// aggregator so executor sparse decode paths preserve the same observability
// contract as reader-backed lazy decode paths.
#[cfg(any(test, feature = "structural-read-metrics"))]
fn finish_direct_probe(probe: &StructuralReadProbe) {
    if !probe.collect {
        return;
    }

    let validated_non_scalar_slots = probe.validated_non_scalar_slots.get();
    let materialized_non_scalar_slots = probe.materialized_non_scalar_slots.get();
    let declared_slots_validated = probe.declared_slots_validated.get();

    STRUCTURAL_READ_METRICS.with(|metrics| {
        if let Some(aggregate) = metrics.borrow_mut().as_mut() {
            aggregate.rows_opened = aggregate.rows_opened.saturating_add(1);
            aggregate.declared_slots_validated = aggregate
                .declared_slots_validated
                .saturating_add(declared_slots_validated);
            aggregate.validated_non_scalar_slots = aggregate
                .validated_non_scalar_slots
                .saturating_add(validated_non_scalar_slots);
            aggregate.materialized_non_scalar_slots = aggregate
                .materialized_non_scalar_slots
                .saturating_add(materialized_non_scalar_slots);
            if materialized_non_scalar_slots == 0 {
                aggregate.rows_without_lazy_non_scalar_materializations = aggregate
                    .rows_without_lazy_non_scalar_materializations
                    .saturating_add(1);
            }
        }
    });
}

#[cfg(not(any(test, feature = "structural-read-metrics")))]
const fn finish_direct_probe(_probe: &StructuralReadProbe) {}

// Freeze one validated scalar slot into a compact cache state that preserves
// fixed-width scalar payloads by value and defers payload-backed scalar
// materialization until a caller actually asks for a runtime `Value`.
const fn validated_scalar_slot_value(value: ScalarSlotValueRef<'_>) -> ValidatedScalarSlotValue {
    match value {
        ScalarSlotValueRef::Null => ValidatedScalarSlotValue::Null,
        ScalarSlotValueRef::Value(value) => match value {
            crate::db::data::ScalarValueRef::Blob(_) => ValidatedScalarSlotValue::Blob,
            crate::db::data::ScalarValueRef::Bool(value) => ValidatedScalarSlotValue::Bool(value),
            crate::db::data::ScalarValueRef::Date(value) => ValidatedScalarSlotValue::Date(value),
            crate::db::data::ScalarValueRef::Duration(value) => {
                ValidatedScalarSlotValue::Duration(value)
            }
            crate::db::data::ScalarValueRef::Float32(value) => {
                ValidatedScalarSlotValue::Float32(value)
            }
            crate::db::data::ScalarValueRef::Float64(value) => {
                ValidatedScalarSlotValue::Float64(value)
            }
            crate::db::data::ScalarValueRef::Int(value) => ValidatedScalarSlotValue::Int(value),
            crate::db::data::ScalarValueRef::Principal(value) => {
                ValidatedScalarSlotValue::Principal(value)
            }
            crate::db::data::ScalarValueRef::Subaccount(value) => {
                ValidatedScalarSlotValue::Subaccount(value)
            }
            crate::db::data::ScalarValueRef::Text(_) => ValidatedScalarSlotValue::Text,
            crate::db::data::ScalarValueRef::Timestamp(value) => {
                ValidatedScalarSlotValue::Timestamp(value)
            }
            crate::db::data::ScalarValueRef::Uint(value) => ValidatedScalarSlotValue::Uint(value),
            crate::db::data::ScalarValueRef::Ulid(value) => ValidatedScalarSlotValue::Ulid(value),
            crate::db::data::ScalarValueRef::Unit => ValidatedScalarSlotValue::Unit,
        },
    }
}

// Borrow one scalar slot view from the validated cache without rebuilding
// fixed-width scalar values from persisted bytes.
fn scalar_slot_value_ref_from_validated<'a>(
    validated: ValidatedScalarSlotValue,
    contract: StructuralRowContract,
    field_bytes: &'a StructuralRowFieldBytes<'a>,
    slot: usize,
) -> Result<ScalarSlotValueRef<'a>, InternalError> {
    match validated {
        ValidatedScalarSlotValue::Null => Ok(ScalarSlotValueRef::Null),
        ValidatedScalarSlotValue::Blob | ValidatedScalarSlotValue::Text => {
            let field = contract.fields().get(slot).ok_or_else(|| {
                InternalError::persisted_row_slot_lookup_out_of_bounds(contract.entity_path(), slot)
            })?;
            let raw_value = field_bytes
                .field(slot)
                .ok_or_else(|| InternalError::persisted_row_declared_field_missing(field.name()))?;
            let LeafCodec::Scalar(codec) = field.leaf_codec() else {
                return Err(InternalError::persisted_row_decode_failed(format!(
                    "validated scalar cache routed through non-scalar field contract: slot={slot}",
                )));
            };

            decode_scalar_slot_value(raw_value, codec, field.name())
        }
        ValidatedScalarSlotValue::Bool(value) => Ok(ScalarSlotValueRef::Value(
            crate::db::data::ScalarValueRef::Bool(value),
        )),
        ValidatedScalarSlotValue::Date(value) => Ok(ScalarSlotValueRef::Value(
            crate::db::data::ScalarValueRef::Date(value),
        )),
        ValidatedScalarSlotValue::Duration(value) => Ok(ScalarSlotValueRef::Value(
            crate::db::data::ScalarValueRef::Duration(value),
        )),
        ValidatedScalarSlotValue::Float32(value) => Ok(ScalarSlotValueRef::Value(
            crate::db::data::ScalarValueRef::Float32(value),
        )),
        ValidatedScalarSlotValue::Float64(value) => Ok(ScalarSlotValueRef::Value(
            crate::db::data::ScalarValueRef::Float64(value),
        )),
        ValidatedScalarSlotValue::Int(value) => Ok(ScalarSlotValueRef::Value(
            crate::db::data::ScalarValueRef::Int(value),
        )),
        ValidatedScalarSlotValue::Principal(value) => Ok(ScalarSlotValueRef::Value(
            crate::db::data::ScalarValueRef::Principal(value),
        )),
        ValidatedScalarSlotValue::Subaccount(value) => Ok(ScalarSlotValueRef::Value(
            crate::db::data::ScalarValueRef::Subaccount(value),
        )),
        ValidatedScalarSlotValue::Timestamp(value) => Ok(ScalarSlotValueRef::Value(
            crate::db::data::ScalarValueRef::Timestamp(value),
        )),
        ValidatedScalarSlotValue::Uint(value) => Ok(ScalarSlotValueRef::Value(
            crate::db::data::ScalarValueRef::Uint(value),
        )),
        ValidatedScalarSlotValue::Ulid(value) => Ok(ScalarSlotValueRef::Value(
            crate::db::data::ScalarValueRef::Ulid(value),
        )),
        ValidatedScalarSlotValue::Unit => Ok(ScalarSlotValueRef::Value(
            crate::db::data::ScalarValueRef::Unit,
        )),
    }
}

// Materialize one validated scalar slot into the runtime `Value` enum.
fn materialize_validated_scalar_slot_value(
    validated: ValidatedScalarSlotValue,
    contract: StructuralRowContract,
    field_bytes: &StructuralRowFieldBytes<'_>,
    slot: usize,
) -> Result<Value, InternalError> {
    match scalar_slot_value_ref_from_validated(validated, contract, field_bytes, slot)? {
        ScalarSlotValueRef::Null => Ok(Value::Null),
        ScalarSlotValueRef::Value(value) => Ok(value.into_value()),
    }
}

impl SlotReader for StructuralSlotReader<'_> {
    fn model(&self) -> &'static EntityModel {
        self.model()
    }

    fn has(&self, slot: usize) -> bool {
        self.field_bytes.field(slot).is_some()
    }

    fn get_bytes(&self, slot: usize) -> Option<&[u8]> {
        self.field_bytes.field(slot)
    }

    fn get_scalar(&self, slot: usize) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
        let field = self.field_model(slot)?;

        match field.leaf_codec() {
            LeafCodec::Scalar(_codec) => match self.cached_values.get(slot) {
                Some(CachedSlotValue::Scalar { validated, .. }) => {
                    let validated = self.required_validated_scalar_slot_value(slot, validated)?;

                    scalar_slot_value_ref_from_validated(
                        validated,
                        self.contract,
                        &self.field_bytes,
                        slot,
                    )
                    .map(Some)
                }
                Some(CachedSlotValue::Deferred { .. }) => {
                    Err(InternalError::persisted_row_decode_failed(format!(
                        "structural scalar slot routed through non-scalar cache variant: slot={slot}",
                    )))
                }
                None => Err(
                    InternalError::persisted_row_slot_cache_lookup_out_of_bounds(
                        self.contract.entity_path(),
                        slot,
                    ),
                ),
            },
            LeafCodec::StructuralFallback => Ok(None),
        }
    }

    fn get_value(&mut self, slot: usize) -> Result<Option<Value>, InternalError> {
        Ok(Some(self.required_cached_value(slot)?.clone()))
    }
}

impl CanonicalSlotReader for StructuralSlotReader<'_> {
    fn required_bytes(&self, slot: usize) -> Result<&[u8], InternalError> {
        let field = self.field_model(slot)?;

        self.get_bytes(slot)
            .ok_or_else(|| InternalError::persisted_row_declared_field_missing(field.name()))
    }

    fn required_scalar(&self, slot: usize) -> Result<ScalarSlotValueRef<'_>, InternalError> {
        let field = self.field_model(slot)?;
        debug_assert!(matches!(field.leaf_codec(), LeafCodec::Scalar(_)));

        self.get_scalar(slot)?
            .ok_or_else(|| InternalError::persisted_row_declared_field_missing(field.name()))
    }

    fn required_value_by_contract(&self, slot: usize) -> Result<Value, InternalError> {
        Ok(self.required_cached_value(slot)?.clone())
    }

    fn required_value_by_contract_cow(&self, slot: usize) -> Result<Cow<'_, Value>, InternalError> {
        Ok(Cow::Borrowed(self.required_cached_value(slot)?))
    }
}

///
/// CachedSlotValue
///
/// CachedSlotValue tracks whether one slot has already been validated, and
/// whether its semantic runtime `Value` has been materialized yet, during the
/// current structural row access pass.
///

#[derive(Clone, Copy, Debug)]
pub(in crate::db::data::persisted_row) enum ValidatedScalarSlotValue {
    Null,
    Blob,
    Bool(bool),
    Date(crate::types::Date),
    Duration(crate::types::Duration),
    Float32(crate::types::Float32),
    Float64(crate::types::Float64),
    Int(i64),
    Principal(crate::types::Principal),
    Subaccount(crate::types::Subaccount),
    Text,
    Timestamp(crate::types::Timestamp),
    Uint(u64),
    Ulid(crate::types::Ulid),
    Unit,
}

#[derive(Debug)]
pub(in crate::db::data::persisted_row) enum CachedSlotValue {
    Scalar {
        validated: OnceCell<ValidatedScalarSlotValue>,
        materialized: OnceCell<Value>,
    },
    Deferred {
        materialized: OnceCell<Value>,
    },
}

///
/// StructuralReadMetrics
///
/// StructuralReadMetrics aggregates one test-scoped view of structural row
/// validation and lazy non-scalar materialization activity.
/// It lets row-backed benchmarks prove the new boundary validates all declared
/// slots while only materializing the non-scalar slots a caller actually
/// touches.
///

#[cfg(any(test, feature = "structural-read-metrics"))]
#[cfg_attr(
    all(test, not(feature = "structural-read-metrics")),
    allow(unreachable_pub)
)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StructuralReadMetrics {
    pub rows_opened: u64,
    pub declared_slots_validated: u64,
    pub validated_non_scalar_slots: u64,
    pub materialized_non_scalar_slots: u64,
    pub rows_without_lazy_non_scalar_materializations: u64,
}

#[cfg(any(test, feature = "structural-read-metrics"))]
std::thread_local! {
    static STRUCTURAL_READ_METRICS: RefCell<Option<StructuralReadMetrics>> = const {
        RefCell::new(None)
    };
}

///
/// StructuralReadProbe
///
/// StructuralReadProbe tracks one reader instance's structural validation and
/// deferred non-scalar materialization counts while a test-scoped metrics
/// capture is active.
///

#[cfg(any(test, feature = "structural-read-metrics"))]
#[derive(Debug)]
struct StructuralReadProbe {
    collect: bool,
    declared_slots_validated: Cell<u64>,
    validated_non_scalar_slots: Cell<u64>,
    materialized_non_scalar_slots: Cell<u64>,
}

#[cfg(not(any(test, feature = "structural-read-metrics")))]
#[derive(Debug)]
struct StructuralReadProbe;

#[cfg(any(test, feature = "structural-read-metrics"))]
impl StructuralReadProbe {
    // Begin one optional per-reader metrics probe when a test-scoped capture
    // is active on the current thread.
    fn begin(_field_count: usize) -> Self {
        let collect = STRUCTURAL_READ_METRICS.with(|metrics| metrics.borrow().is_some());

        Self {
            collect,
            declared_slots_validated: Cell::new(0),
            validated_non_scalar_slots: Cell::new(0),
            materialized_non_scalar_slots: Cell::new(0),
        }
    }

    // Record one distinct slot validated on first access.
    fn record_validated_slot(&self) {
        if !self.collect {
            return;
        }

        self.declared_slots_validated
            .set(self.declared_slots_validated.get().saturating_add(1));
    }

    // Record one non-scalar slot validated at row-open.
    fn record_validated_non_scalar(&self) {
        if !self.collect {
            return;
        }

        self.validated_non_scalar_slots
            .set(self.validated_non_scalar_slots.get().saturating_add(1));
    }

    // Record one distinct non-scalar slot materialized after row-open.
    fn record_materialized_non_scalar(&self) {
        if !self.collect {
            return;
        }

        self.materialized_non_scalar_slots
            .set(self.materialized_non_scalar_slots.get().saturating_add(1));
    }
}

#[cfg(not(any(test, feature = "structural-read-metrics")))]
impl StructuralReadProbe {
    // Build one no-op probe when structural read metrics are not compiled in.
    const fn begin(_field_count: usize) -> Self {
        Self
    }

    // Record one distinct slot validated on first access.
    const fn record_validated_slot(&self) {
        let _ = self;
    }

    // Record one non-scalar slot validated at row-open.
    const fn record_validated_non_scalar(&self) {
        let _ = self;
    }

    // Record one distinct non-scalar slot materialized after row-open.
    const fn record_materialized_non_scalar(&self) {
        let _ = self;
    }
}

#[cfg(any(test, feature = "structural-read-metrics"))]
impl Drop for StructuralSlotReader<'_> {
    fn drop(&mut self) {
        if !self.metrics.collect {
            return;
        }

        let validated_non_scalar_slots = self.metrics.validated_non_scalar_slots.get();
        let materialized_non_scalar_slots = self.metrics.materialized_non_scalar_slots.get();
        let declared_slots_validated = self.metrics.declared_slots_validated.get();

        STRUCTURAL_READ_METRICS.with(|metrics| {
            if let Some(aggregate) = metrics.borrow_mut().as_mut() {
                aggregate.rows_opened = aggregate.rows_opened.saturating_add(1);
                aggregate.declared_slots_validated = aggregate
                    .declared_slots_validated
                    .saturating_add(declared_slots_validated);
                aggregate.validated_non_scalar_slots = aggregate
                    .validated_non_scalar_slots
                    .saturating_add(validated_non_scalar_slots);
                aggregate.materialized_non_scalar_slots = aggregate
                    .materialized_non_scalar_slots
                    .saturating_add(materialized_non_scalar_slots);
                if materialized_non_scalar_slots == 0 {
                    aggregate.rows_without_lazy_non_scalar_materializations = aggregate
                        .rows_without_lazy_non_scalar_materializations
                        .saturating_add(1);
                }
            }
        });
    }
}

///
/// with_structural_read_metrics
///
/// Run one closure while collecting structural-read metrics on the current
/// thread, then return the closure result plus the aggregated snapshot.
///

#[cfg(any(test, feature = "structural-read-metrics"))]
#[cfg_attr(
    all(test, not(feature = "structural-read-metrics")),
    allow(unreachable_pub)
)]
pub fn with_structural_read_metrics<T>(f: impl FnOnce() -> T) -> (T, StructuralReadMetrics) {
    STRUCTURAL_READ_METRICS.with(|metrics| {
        debug_assert!(
            metrics.borrow().is_none(),
            "structural read metrics captures should not nest"
        );
        *metrics.borrow_mut() = Some(StructuralReadMetrics::default());
    });

    let result = f();
    let metrics =
        STRUCTURAL_READ_METRICS.with(|metrics| metrics.borrow_mut().take().unwrap_or_default());

    (result, metrics)
}
