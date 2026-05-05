#[cfg(any(test, feature = "diagnostics"))]
use crate::db::data::persisted_row::reader::metrics;
use crate::{
    db::data::{
        DataKey, RawRow, StructuralRowContract, StructuralRowDecodeError, StructuralRowFieldBytes,
        ValueStorageView,
        persisted_row::{
            codec::{ScalarSlotValueRef, ScalarValueRef, decode_scalar_slot_value},
            contract::{decode_field_slot_into_runtime_value, validate_non_scalar_slot_value},
            reader::{
                cache::{
                    CachedSlotValue, ValidatedScalarSlotValue, build_initial_slot_cache,
                    materialize_validated_scalar_slot_value, scalar_slot_value_ref_from_validated,
                    validated_scalar_slot_value,
                },
                primary_key::validate_storage_key_from_primary_key_bytes_with_field,
            },
            types::{CanonicalSlotReader, SlotReader},
        },
    },
    error::InternalError,
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel, FieldStorageDecode, LeafCodec},
    },
    value::{StorageKey, Value},
};
use std::{borrow::Cow, cell::OnceCell};

// Materialize one borrowed scalar slot view when a caller reaches a boundary
// that still requires owned runtime `Value` cells.
fn scalar_slot_value_ref_into_value(value: ScalarSlotValueRef<'_>) -> Value {
    match value {
        ScalarSlotValueRef::Null => Value::Null,
        ScalarSlotValueRef::Value(value) => value.into_value(),
    }
}

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
    contract: StructuralRowContract,
    field_bytes: StructuralRowFieldBytes<'a>,
    pub(in crate::db::data::persisted_row) cached_values: Vec<CachedSlotValue>,
    #[cfg(any(test, feature = "diagnostics"))]
    pub(in crate::db::data::persisted_row::reader) metrics: metrics::StructuralReadProbe,
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

    /// Build one slot reader over one persisted row using one frozen
    /// structural row contract without retaining the full entity model.
    pub(in crate::db) fn from_raw_row_with_contract(
        raw_row: &'a RawRow,
        contract: StructuralRowContract,
    ) -> Result<Self, InternalError> {
        let field_bytes = StructuralRowFieldBytes::from_raw_row_with_contract(raw_row, contract)
            .map_err(StructuralRowDecodeError::into_internal_error)?;
        let reader = Self {
            contract,
            field_bytes,
            cached_values: build_initial_slot_cache(contract),
            #[cfg(any(test, feature = "diagnostics"))]
            metrics: metrics::StructuralReadProbe::begin(contract.field_count()),
        };

        Ok(reader)
    }

    /// Build one slot reader over one persisted row using a frozen structural
    /// row contract, then validate every declared slot eagerly.
    pub(in crate::db) fn from_raw_row_with_validated_contract(
        raw_row: &'a RawRow,
        contract: StructuralRowContract,
    ) -> Result<Self, InternalError> {
        let reader = Self::from_raw_row_with_contract(raw_row, contract)?;
        reader.validate_all_declared_slots()?;

        Ok(reader)
    }

    // Build one slot reader over one persisted row from a generated model by
    // immediately projecting that model into the structural row contract.
    pub(in crate::db) fn from_raw_row_with_model(
        raw_row: &'a RawRow,
        model: &'static EntityModel,
    ) -> Result<Self, InternalError> {
        Self::from_raw_row_with_contract(raw_row, StructuralRowContract::from_model(model))
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
    fn validate_storage_key_value(&self, expected_key: StorageKey) -> Result<(), InternalError> {
        let primary_key_slot = self.contract.primary_key_slot();
        let field = self.field_contract(primary_key_slot)?;

        // Preserve the reader's scalar validation/cache side effect before the
        // shared raw-bytes validator performs the authoritative key check.
        if matches!(field.leaf_codec(), LeafCodec::Scalar(_))
            && let Some(CachedSlotValue::Scalar { validated, .. }) =
                self.cached_values.get(primary_key_slot)
        {
            let _ = self.required_validated_scalar_slot_value(primary_key_slot, validated)?;
        }

        let raw_value = self.required_field_bytes(primary_key_slot, field.name())?;

        validate_storage_key_from_primary_key_bytes_with_field(raw_value, field, expected_key)
    }

    // Resolve one field contract entry by stable slot index.
    fn field_contract(&self, slot: usize) -> Result<&FieldModel, InternalError> {
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

        let field = self.field_contract(slot)?;
        let raw_value = self.required_field_bytes(slot, field.name())?;
        let LeafCodec::Scalar(codec) = field.leaf_codec() else {
            return Err(InternalError::persisted_row_decode_failed(format!(
                "validated scalar cache routed through non-scalar field contract: slot={slot}",
            )));
        };
        #[cfg(any(test, feature = "diagnostics"))]
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
                let field = self.field_contract(slot)?;
                let raw_value = self.required_field_bytes(slot, field.name())?;
                if materialized.get().is_none() {
                    #[cfg(any(test, feature = "diagnostics"))]
                    {
                        self.metrics.record_validated_slot();
                        self.metrics.record_validated_non_scalar();
                        self.metrics.record_materialized_non_scalar();
                    }
                    validate_non_scalar_slot_value(raw_value, field)?;
                    let value = decode_field_slot_into_runtime_value(field, raw_value)?;
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

    /// Materialize one slot for a direct projection with a scalar value-storage fast path.
    pub(in crate::db) fn required_direct_projection_value(
        &self,
        slot: usize,
    ) -> Result<Value, InternalError> {
        let field = self.field_contract(slot)?;

        // Phase 1: value-storage scalar fields can project directly from the
        // validated byte view when the persisted tag matches the declared
        // scalar kind. Mismatches fall through to preserve the existing
        // permissive `FieldStorageDecode::Value` materialization behavior.
        if matches!(field.storage_decode(), FieldStorageDecode::Value)
            && let Some(value) = self.required_value_storage_scalar(slot)?
        {
            #[cfg(any(test, feature = "diagnostics"))]
            {
                self.metrics.record_materialized_non_scalar();
            }

            return Ok(scalar_slot_value_ref_into_value(value));
        }

        self.required_value_by_contract(slot)
    }

    // Decode a non-null value-storage scalar only when its tag already proves
    // it is the scalar family expected by the field. Otherwise the caller falls
    // back to the canonical materializing path.
    fn try_value_storage_non_null_scalar_slot_value<'view>(
        field: &FieldModel,
        view: &ValueStorageView<'view>,
    ) -> Result<Option<ScalarSlotValueRef<'view>>, InternalError> {
        let value = match field.kind() {
            FieldKind::Bool if view.is_bool() => {
                ScalarValueRef::Bool(view.as_bool().map_err(|err| {
                    InternalError::persisted_row_field_kind_decode_failed(
                        field.name(),
                        field.kind(),
                        err,
                    )
                })?)
            }
            FieldKind::Int if view.is_i64() => {
                ScalarValueRef::Int(view.as_i64().map_err(|err| {
                    InternalError::persisted_row_field_kind_decode_failed(
                        field.name(),
                        field.kind(),
                        err,
                    )
                })?)
            }
            FieldKind::Text { .. } if view.is_text() => {
                ScalarValueRef::Text(view.as_text().map_err(|err| {
                    InternalError::persisted_row_field_kind_decode_failed(
                        field.name(),
                        field.kind(),
                        err,
                    )
                })?)
            }
            FieldKind::Uint if view.is_u64() => {
                ScalarValueRef::Uint(view.as_u64().map_err(|err| {
                    InternalError::persisted_row_field_kind_decode_failed(
                        field.name(),
                        field.kind(),
                        err,
                    )
                })?)
            }
            _ => return Ok(None),
        };

        Ok(Some(ScalarSlotValueRef::Value(value)))
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
                    #[cfg(any(test, feature = "diagnostics"))]
                    self.metrics.record_validated_slot();
                    decode_scalar_slot_value(raw_value, codec, field.name())?;
                }
                LeafCodec::StructuralFallback => {
                    #[cfg(any(test, feature = "diagnostics"))]
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

impl SlotReader for StructuralSlotReader<'_> {
    fn field_contract(&self, slot: usize) -> Result<&FieldModel, InternalError> {
        StructuralSlotReader::field_contract(self, slot)
    }

    fn has(&self, slot: usize) -> bool {
        self.field_bytes.field(slot).is_some()
    }

    fn get_bytes(&self, slot: usize) -> Option<&[u8]> {
        self.field_bytes.field(slot)
    }

    fn get_scalar(&self, slot: usize) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
        let field = self.field_contract(slot)?;

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
        let field = self.field_contract(slot)?;

        self.get_bytes(slot)
            .ok_or_else(|| InternalError::persisted_row_declared_field_missing(field.name()))
    }

    fn required_scalar(&self, slot: usize) -> Result<ScalarSlotValueRef<'_>, InternalError> {
        let field = self.field_contract(slot)?;
        debug_assert!(matches!(field.leaf_codec(), LeafCodec::Scalar(_)));

        self.get_scalar(slot)?
            .ok_or_else(|| InternalError::persisted_row_declared_field_missing(field.name()))
    }

    fn required_value_storage_scalar(
        &self,
        slot: usize,
    ) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
        let field = self.field_contract(slot)?;
        if !matches!(field.storage_decode(), FieldStorageDecode::Value) {
            return Ok(None);
        }

        let raw_value = self.required_field_bytes(slot, field.name())?;
        let view = ValueStorageView::from_raw_validated(raw_value).map_err(|err| {
            InternalError::persisted_row_field_kind_decode_failed(field.name(), field.kind(), err)
        })?;

        let value = if view.is_null() {
            Some(ScalarSlotValueRef::Null)
        } else {
            Self::try_value_storage_non_null_scalar_slot_value(field, &view)?
        };

        if value.is_some() {
            #[cfg(any(test, feature = "diagnostics"))]
            {
                self.metrics.record_validated_slot();
                self.metrics.record_validated_non_scalar();
            }
        }

        Ok(value)
    }

    fn required_value_by_contract(&self, slot: usize) -> Result<Value, InternalError> {
        Ok(self.required_cached_value(slot)?.clone())
    }

    fn required_value_by_contract_cow(&self, slot: usize) -> Result<Cow<'_, Value>, InternalError> {
        Ok(Cow::Borrowed(self.required_cached_value(slot)?))
    }
}
