use crate::{
    db::data::{
        DataKey, RawRow, StructuralRowContract, StructuralRowDecodeError, StructuralRowFieldBytes,
    },
    error::InternalError,
    model::{
        entity::{EntityModel, resolve_primary_key_slot},
        field::{FieldModel, LeafCodec},
    },
    value::{StorageKey, Value},
};
#[cfg(any(test, feature = "structural-read-metrics"))]
use std::cell::{Cell, RefCell};
use std::{borrow::Cow, cell::OnceCell};

use crate::db::data::persisted_row::{
    codec::{ScalarSlotValueRef, ScalarValueRef, decode_scalar_slot_value},
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
/// It validates row shape and every declared field contract before any
/// consumer can observe the row, then materializes semantic `Value`s lazily so
/// hot readers do not pay full allocation cost for untouched fields.
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
        let cached_values = std::iter::repeat_with(|| CachedSlotValue::Pending)
            .take(contract.field_count())
            .collect();
        let mut reader = Self {
            model: None,
            contract,
            field_bytes,
            cached_values,
            #[cfg(any(test, feature = "structural-read-metrics"))]
            metrics: StructuralReadProbe::begin(contract.field_count()),
        };

        // Phase 1: validate every declared slot through the field contract
        // once so malformed persisted bytes cannot stay latent behind later
        // hot-path reads.
        reader.validate_all_declared_slots()?;

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
        let cached_values = std::iter::repeat_with(|| CachedSlotValue::Pending)
            .take(contract.field_count())
            .collect();
        let mut reader = Self {
            model: Some(model),
            contract,
            field_bytes,
            cached_values,
            #[cfg(any(test, feature = "structural-read-metrics"))]
            metrics: StructuralReadProbe::begin(contract.field_count()),
        };

        // Phase 1: validate every declared slot through the field contract
        // once so malformed persisted bytes cannot stay latent behind later
        // hot-path reads.
        reader.validate_all_declared_slots()?;

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
        let primary_key_slot = self.contract.primary_key_slot();
        let Some(model) = self.model else {
            return self.validate_storage_key_value_with_contract(expected_key, primary_key_slot);
        };
        let Some(primary_key_slot) = resolve_primary_key_slot(model) else {
            return Err(InternalError::persisted_row_primary_key_field_missing(
                self.contract.entity_path(),
            ));
        };
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

    // Validate every declared slot exactly once at the structural row boundary
    // so later consumers inherit one globally enforced canonical-row contract
    // before any caller can observe the row.
    fn validate_all_declared_slots(&mut self) -> Result<(), InternalError> {
        for slot in 0..self.contract.field_count() {
            self.validate_slot_into_cache(slot)?;
        }

        Ok(())
    }

    // Validate one declared slot directly into the owned cache without
    // eagerly building the final runtime `Value` unless the slot is already a
    // cheap scalar fast-path.
    fn validate_slot_into_cache(&mut self, slot: usize) -> Result<(), InternalError> {
        if !matches!(self.cached_values.get(slot), Some(CachedSlotValue::Pending)) {
            return Ok(());
        }

        let field = self.field_model(slot)?;
        let raw_value = self
            .field_bytes
            .field(slot)
            .ok_or_else(|| InternalError::persisted_row_declared_field_missing(field.name()))?;
        let cached = match field.leaf_codec() {
            LeafCodec::Scalar(codec) => CachedSlotValue::Scalar(materialize_scalar_slot_value(
                decode_scalar_slot_value(raw_value, codec, field.name())?,
            )),
            LeafCodec::CborFallback => {
                #[cfg(any(test, feature = "structural-read-metrics"))]
                self.metrics.record_validated_non_scalar();
                validate_non_scalar_slot_value(raw_value, field)?;
                CachedSlotValue::Deferred {
                    materialized: OnceCell::new(),
                }
            }
        };
        self.cached_values[slot] = cached;

        Ok(())
    }

    // Consume the structural slot reader into one slot-indexed decoded-value
    // vector once the canonical row boundary has already validated every slot.
    // This lets hot row-decode callers pay semantic materialization exactly
    // once while preserving the strict row-open fail-closed contract.
    pub(in crate::db) fn into_decoded_values(
        mut self,
    ) -> Result<Vec<Option<Value>>, InternalError> {
        let contract = self.contract;
        let cached_values = std::mem::take(&mut self.cached_values);
        let mut values = Vec::with_capacity(cached_values.len());

        for (slot, cached) in cached_values.into_iter().enumerate() {
            match cached {
                CachedSlotValue::Scalar(value) => values.push(Some(value)),
                CachedSlotValue::Deferred { materialized } => {
                    let field = contract.fields().get(slot).ok_or_else(|| {
                        InternalError::persisted_row_slot_lookup_out_of_bounds(
                            contract.entity_path(),
                            slot,
                        )
                    })?;
                    let value = if let Some(value) = materialized.into_inner() {
                        value
                    } else {
                        #[cfg(any(test, feature = "structural-read-metrics"))]
                        self.metrics.record_materialized_non_scalar();
                        let raw_value = self.field_bytes.field(slot).ok_or_else(|| {
                            InternalError::persisted_row_declared_field_missing(field.name())
                        })?;
                        decode_slot_value_for_field(field, raw_value)?
                    };
                    values.push(Some(value));
                }
                CachedSlotValue::Pending => {
                    return Err(InternalError::persisted_row_decode_failed(format!(
                        "structural slot cache was not fully validated before consumption: slot={slot}",
                    )));
                }
            }
        }

        Ok(values)
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
            CachedSlotValue::Scalar(value) => Ok(value),
            CachedSlotValue::Deferred { materialized } => {
                let field = self.field_model(slot)?;
                let raw_value = self.required_field_bytes(slot, field.name())?;
                if materialized.get().is_none() {
                    #[cfg(any(test, feature = "structural-read-metrics"))]
                    self.metrics.record_materialized_non_scalar();
                    let value = decode_slot_value_for_field(field, raw_value)?;
                    let _ = materialized.set(value);
                }

                materialized.get().ok_or_else(|| {
                    InternalError::persisted_row_decode_failed(format!(
                        "structural slot cache failed to materialize deferred value: slot={slot}",
                    ))
                })
            }
            CachedSlotValue::Pending => Err(InternalError::persisted_row_decode_failed(format!(
                "structural slot cache missing validated value after row-open validation: slot={slot}",
            ))),
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
}

// Borrow one scalar-slot view directly from one already-decoded runtime value.
fn scalar_slot_value_ref_from_cached_value(
    value: &Value,
) -> Result<ScalarSlotValueRef<'_>, InternalError> {
    let scalar = match value {
        Value::Null => return Ok(ScalarSlotValueRef::Null),
        Value::Blob(value) => ScalarValueRef::Blob(value.as_slice()),
        Value::Bool(value) => ScalarValueRef::Bool(*value),
        Value::Date(value) => ScalarValueRef::Date(*value),
        Value::Duration(value) => ScalarValueRef::Duration(*value),
        Value::Float32(value) => ScalarValueRef::Float32(*value),
        Value::Float64(value) => ScalarValueRef::Float64(*value),
        Value::Int(value) => ScalarValueRef::Int(*value),
        Value::Principal(value) => ScalarValueRef::Principal(*value),
        Value::Subaccount(value) => ScalarValueRef::Subaccount(*value),
        Value::Text(value) => ScalarValueRef::Text(value.as_str()),
        Value::Timestamp(value) => ScalarValueRef::Timestamp(*value),
        Value::Uint(value) => ScalarValueRef::Uint(*value),
        Value::Ulid(value) => ScalarValueRef::Ulid(*value),
        Value::Unit => ScalarValueRef::Unit,
        _ => {
            return Err(InternalError::persisted_row_decode_failed(format!(
                "cached structural scalar slot cannot borrow non-scalar value variant: {value:?}",
            )));
        }
    };

    Ok(ScalarSlotValueRef::Value(scalar))
}

// Materialize one validated scalar slot view into the runtime `Value` enum.
fn materialize_scalar_slot_value(value: ScalarSlotValueRef<'_>) -> Value {
    match value {
        ScalarSlotValueRef::Null => Value::Null,
        ScalarSlotValueRef::Value(value) => value.into_value(),
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
                Some(CachedSlotValue::Scalar(value)) => {
                    scalar_slot_value_ref_from_cached_value(value).map(Some)
                }
                Some(CachedSlotValue::Pending) => {
                    Err(InternalError::persisted_row_decode_failed(format!(
                        "structural scalar slot cache missing validated value after row-open validation: slot={slot}",
                    )))
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
            LeafCodec::CborFallback => Ok(None),
        }
    }

    fn get_value(&mut self, slot: usize) -> Result<Option<Value>, InternalError> {
        self.validate_slot_into_cache(slot)?;
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

#[derive(Debug)]
pub(in crate::db::data::persisted_row) enum CachedSlotValue {
    Pending,
    Scalar(Value),
    Deferred { materialized: OnceCell<Value> },
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
    declared_slots_validated: u64,
    validated_non_scalar_slots: Cell<u64>,
    materialized_non_scalar_slots: Cell<u64>,
}

#[cfg(any(test, feature = "structural-read-metrics"))]
impl StructuralReadProbe {
    // Begin one optional per-reader metrics probe when a test-scoped capture
    // is active on the current thread.
    fn begin(field_count: usize) -> Self {
        let collect = STRUCTURAL_READ_METRICS.with(|metrics| metrics.borrow().is_some());

        Self {
            collect,
            declared_slots_validated: field_count as u64,
            validated_non_scalar_slots: Cell::new(0),
            materialized_non_scalar_slots: Cell::new(0),
        }
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

#[cfg(any(test, feature = "structural-read-metrics"))]
impl Drop for StructuralSlotReader<'_> {
    fn drop(&mut self) {
        if !self.metrics.collect {
            return;
        }

        let validated_non_scalar_slots = self.metrics.validated_non_scalar_slots.get();
        let materialized_non_scalar_slots = self.metrics.materialized_non_scalar_slots.get();

        STRUCTURAL_READ_METRICS.with(|metrics| {
            if let Some(aggregate) = metrics.borrow_mut().as_mut() {
                aggregate.rows_opened = aggregate.rows_opened.saturating_add(1);
                aggregate.declared_slots_validated = aggregate
                    .declared_slots_validated
                    .saturating_add(self.metrics.declared_slots_validated);
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
