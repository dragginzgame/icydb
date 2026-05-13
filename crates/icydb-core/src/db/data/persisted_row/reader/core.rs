#[cfg(any(test, feature = "diagnostics"))]
use crate::db::data::persisted_row::reader::metrics;
#[cfg(test)]
use crate::model::entity::EntityModel;
use crate::{
    db::{
        data::{
            DataKey, RawRow, StructuralFieldDecodeContract, StructuralRowContract,
            StructuralRowDecodeError, StructuralRowFieldBytes, ValueStorageView,
            persisted_row::{
                codec::{ScalarSlotValueRef, ScalarValueRef},
                contract::{
                    decode_runtime_value_from_row_contract,
                    decode_scalar_slot_value_from_row_contract,
                    validate_non_scalar_slot_value_with_row_contract,
                },
                reader::{
                    cache::{
                        CachedSlotValue, ValidatedScalarSlotValue, build_initial_slot_cache,
                        materialize_validated_scalar_slot_value,
                        scalar_slot_value_ref_from_validated, validated_scalar_slot_value,
                    },
                    primary_key::validate_storage_key_from_primary_key_bytes_with_contract,
                },
                types::{CanonicalSlotReader, SlotReader},
            },
        },
        schema::{AcceptedFieldDecodeContract, PersistedFieldKind},
    },
    error::InternalError,
    model::field::{FieldKind, FieldModel, FieldStorageDecode, LeafCodec},
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
    #[cfg(test)]
    pub(in crate::db) fn from_raw_row_with_generated_model_for_test(
        raw_row: &'a RawRow,
        model: &'static EntityModel,
    ) -> Result<Self, InternalError> {
        let reader = Self::from_raw_row_with_unvalidated_generated_model_for_test(raw_row, model)?;
        reader.validate_all_declared_slots()?;

        Ok(reader)
    }

    /// Build one slot reader over one persisted row using one frozen
    /// structural row contract without retaining the full entity model.
    pub(in crate::db) fn from_raw_row_with_contract(
        raw_row: &'a RawRow,
        contract: StructuralRowContract,
    ) -> Result<Self, InternalError> {
        let field_bytes =
            StructuralRowFieldBytes::from_raw_row_with_contract(raw_row, contract.clone())
                .map_err(StructuralRowDecodeError::into_internal_error)?;
        let reader = Self {
            contract: contract.clone(),
            field_bytes,
            cached_values: build_initial_slot_cache(&contract),
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
    #[cfg(test)]
    pub(in crate::db) fn from_raw_row_with_unvalidated_generated_model_for_test(
        raw_row: &'a RawRow,
        model: &'static EntityModel,
    ) -> Result<Self, InternalError> {
        Self::from_raw_row_with_contract(
            raw_row,
            StructuralRowContract::from_generated_model_for_test(model),
        )
    }

    fn required_accepted_field_decode_contract(
        &self,
        slot: usize,
    ) -> Result<AcceptedFieldDecodeContract<'_>, InternalError> {
        self.contract.required_accepted_field_decode_contract(slot)
    }

    /// Return the declared structural field count for this reader contract.
    #[must_use]
    pub(in crate::db) const fn field_count(&self) -> usize {
        self.contract.field_count()
    }

    /// Borrow the structural row contract selected for this reader.
    #[must_use]
    pub(in crate::db) const fn contract(&self) -> &StructuralRowContract {
        &self.contract
    }

    /// Return whether this reader is governed by accepted persisted schema.
    #[must_use]
    pub(in crate::db) const fn has_accepted_decode_contract(&self) -> bool {
        self.contract.has_accepted_decode_contract()
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

        // Preserve the reader's scalar validation/cache side effect before the
        // shared row-contract validator performs the authoritative key check.
        if matches!(
            self.contract.field_leaf_codec(primary_key_slot)?,
            LeafCodec::Scalar(_)
        ) && let Some(CachedSlotValue::Scalar { validated, .. }) =
            self.cached_values.get(primary_key_slot)
        {
            let _ =
                self.required_validated_scalar_slot_value_for_slot(primary_key_slot, validated)?;
        }

        let field_name = self.contract.field_name(primary_key_slot)?;
        let raw_value = self.required_field_bytes(primary_key_slot, field_name)?;

        validate_storage_key_from_primary_key_bytes_with_contract(
            &self.contract,
            raw_value,
            expected_key,
        )
    }

    // Resolve one generated-compatible field model by stable slot index for
    // typed materialization compatibility surfaces.
    fn generated_compatible_field_model_for_slot(
        &self,
        slot: usize,
    ) -> Result<&FieldModel, InternalError> {
        self.contract.generated_compatible_field_model(slot)
    }

    // Validate one scalar slot through the row contract, then freeze its
    // compact cache state for repeated scalar reads and later materialization.
    fn required_validated_scalar_slot_value_for_slot(
        &self,
        slot: usize,
        validated: &OnceCell<ValidatedScalarSlotValue>,
    ) -> Result<ValidatedScalarSlotValue, InternalError> {
        if let Some(validated) = validated.get() {
            return Ok(*validated);
        }

        let field_name = self.contract.field_name(slot)?;
        let raw_value = self.required_field_bytes(slot, field_name)?;
        #[cfg(any(test, feature = "diagnostics"))]
        self.metrics.record_validated_slot();
        let validated_value =
            validated_scalar_slot_value(decode_scalar_slot_value_from_row_contract(
                &self.contract,
                slot,
                raw_value,
                "accepted scalar cache routed through non-scalar field contract",
                "validated scalar cache routed through non-scalar field contract",
            )?);
        let _ = validated.set(validated_value);

        Ok(validated_value)
    }

    // Borrow one declared slot value from the validated structural cache,
    // materializing the semantic `Value` lazily when the caller first touches
    // that slot.
    pub(in crate::db) fn required_cached_value(
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
                if self.field_bytes.field(slot).is_none() {
                    if materialized.get().is_none() {
                        let _ = materialized.set(self.contract.missing_slot_value(slot)?);
                    }

                    return materialized.get().ok_or_else(|| {
                        InternalError::persisted_row_decode_failed(format!(
                            "structural missing scalar slot failed to materialize deferred value: slot={slot}",
                        ))
                    });
                }

                let validated =
                    self.required_validated_scalar_slot_value_for_slot(slot, validated)?;
                if materialized.get().is_none() {
                    let value = materialize_validated_scalar_slot_value(
                        validated,
                        self.contract.clone(),
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
                if self.field_bytes.field(slot).is_none() {
                    if materialized.get().is_none() {
                        let _ = materialized.set(self.contract.missing_slot_value(slot)?);
                    }

                    return materialized.get().ok_or_else(|| {
                        InternalError::persisted_row_decode_failed(format!(
                            "structural missing deferred slot failed to materialize value: slot={slot}",
                        ))
                    });
                }

                let field_name = self.contract.field_name(slot)?;
                let raw_value = self.required_field_bytes(slot, field_name)?;
                if materialized.get().is_none() {
                    #[cfg(any(test, feature = "diagnostics"))]
                    {
                        self.metrics.record_validated_slot();
                        self.metrics.record_validated_non_scalar();
                        self.metrics.record_materialized_non_scalar();
                    }
                    self.validate_non_scalar_slot_for_contract(slot, raw_value)?;
                    let value =
                        decode_runtime_value_from_row_contract(&self.contract, slot, raw_value)?;
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
        if self.contract.has_accepted_decode_contract() {
            return self.required_direct_projection_value_with_accepted_contract(slot);
        }

        self.required_direct_projection_value_with_generated_contract(slot)
    }

    // Project one direct value through accepted row metadata. The scalar
    // value-storage shortcut is attempted only from accepted field contracts;
    // cache materialization remains the fallback for mismatched value-storage
    // tags.
    fn required_direct_projection_value_with_accepted_contract(
        &self,
        slot: usize,
    ) -> Result<Value, InternalError> {
        // Phase 1: value-storage scalar fields can project directly from the
        // validated byte view when the persisted tag matches the declared
        // scalar kind. Mismatches fall through to preserve the existing
        // permissive `FieldStorageDecode::Value` materialization behavior.
        let accepted_field = self.required_accepted_field_decode_contract(slot)?;
        if let Some(value) = self.required_accepted_value_storage_scalar(slot, accepted_field)? {
            #[cfg(any(test, feature = "diagnostics"))]
            {
                self.metrics.record_materialized_non_scalar();
            }

            return Ok(scalar_slot_value_ref_into_value(value));
        }

        self.required_value_by_contract(slot)
    }

    // Project one direct value through generated-compatible field metadata.
    // Accepted rows use the helper above so this compatibility lane no longer
    // branches on accepted schema contracts inside the projection path.
    fn required_direct_projection_value_with_generated_contract(
        &self,
        slot: usize,
    ) -> Result<Value, InternalError> {
        // Phase 1: value-storage scalar fields can project directly from the
        // validated byte view when the persisted tag matches the declared
        // scalar kind. Mismatches fall through to preserve the existing
        // permissive `FieldStorageDecode::Value` materialization behavior.
        let field = self.contract.field_decode_contract(slot)?;
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

    // Decode one scalar slot for eager all-slot validation through accepted
    // metadata when available. This mirrors the lazy scalar-cache validation
    // helper so both paths agree on the field name and scalar codec authority.
    fn decode_scalar_slot_value_for_slot<'raw>(
        &self,
        slot: usize,
        raw_value: &'raw [u8],
    ) -> Result<ScalarSlotValueRef<'raw>, InternalError> {
        decode_scalar_slot_value_from_row_contract(
            &self.contract,
            slot,
            raw_value,
            "accepted all-slot validation routed scalar cache through non-scalar field contract",
            "all-slot validation routed scalar cache through non-scalar field contract",
        )
    }

    // Validate one structural-fallback slot through accepted metadata when
    // available. Generated contracts remain the fallback for generated-only
    // readers.
    fn validate_non_scalar_slot_for_contract(
        &self,
        slot: usize,
        raw_value: &[u8],
    ) -> Result<(), InternalError> {
        validate_non_scalar_slot_value_with_row_contract(&self.contract, slot, raw_value)
    }

    // Decode a non-null value-storage scalar only when its tag already proves
    // it is the scalar family expected by the field. Otherwise the caller falls
    // back to the canonical materializing path.
    fn try_value_storage_non_null_scalar_slot_value<'view>(
        field: StructuralFieldDecodeContract,
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
            FieldKind::Nat if view.is_u64() => {
                ScalarValueRef::Nat(view.as_u64().map_err(|err| {
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

    // Accepted-schema counterpart to the generated-field value-storage scalar
    // fast path. Exact-match layouts make both contracts equivalent today, but
    // this keeps the fast path from reopening generated kind metadata once a
    // saved row-layout contract is present.
    fn try_value_storage_non_null_accepted_scalar_slot_value<'view>(
        field: AcceptedFieldDecodeContract<'_>,
        view: &ValueStorageView<'view>,
    ) -> Result<Option<ScalarSlotValueRef<'view>>, InternalError> {
        let value = match field.kind() {
            PersistedFieldKind::Bool if view.is_bool() => {
                ScalarValueRef::Bool(view.as_bool().map_err(|err| {
                    InternalError::persisted_row_field_kind_decode_failed(
                        field.field_name(),
                        field.kind(),
                        err,
                    )
                })?)
            }
            PersistedFieldKind::Int if view.is_i64() => {
                ScalarValueRef::Int(view.as_i64().map_err(|err| {
                    InternalError::persisted_row_field_kind_decode_failed(
                        field.field_name(),
                        field.kind(),
                        err,
                    )
                })?)
            }
            PersistedFieldKind::Text { .. } if view.is_text() => {
                ScalarValueRef::Text(view.as_text().map_err(|err| {
                    InternalError::persisted_row_field_kind_decode_failed(
                        field.field_name(),
                        field.kind(),
                        err,
                    )
                })?)
            }
            PersistedFieldKind::Nat if view.is_u64() => {
                ScalarValueRef::Nat(view.as_u64().map_err(|err| {
                    InternalError::persisted_row_field_kind_decode_failed(
                        field.field_name(),
                        field.kind(),
                        err,
                    )
                })?)
            }
            _ => return Ok(None),
        };

        Ok(Some(ScalarSlotValueRef::Value(value)))
    }

    // Borrow a scalar directly from a value-storage payload using the accepted
    // field contract. The generated-field branch remains only for readers that
    // do not yet carry accepted row-layout contracts.
    fn required_accepted_value_storage_scalar(
        &self,
        slot: usize,
        field: AcceptedFieldDecodeContract<'_>,
    ) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
        if !matches!(field.storage_decode(), FieldStorageDecode::Value) {
            return Ok(None);
        }
        if self.field_bytes.field(slot).is_none() {
            return match self.contract.missing_slot_value(slot)? {
                Value::Null => Ok(Some(ScalarSlotValueRef::Null)),
                value => Err(InternalError::persisted_row_decode_failed(format!(
                    "missing value-storage scalar slot materialized non-null value: slot={slot} value={value:?}",
                ))),
            };
        }

        let raw_value = self.required_field_bytes(slot, field.field_name())?;
        let view = ValueStorageView::from_raw_validated(raw_value).map_err(|err| {
            InternalError::persisted_row_field_kind_decode_failed(
                field.field_name(),
                field.kind(),
                err,
            )
        })?;

        let value = if view.is_null() {
            Some(ScalarSlotValueRef::Null)
        } else {
            Self::try_value_storage_non_null_accepted_scalar_slot_value(field, &view)?
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

    // Validate every declared slot once at the structural row contract
    // boundary so fail-closed callers reject malformed unused fields before
    // projection, relation, or commit logic runs.
    fn validate_all_declared_slots(&self) -> Result<(), InternalError> {
        for slot in 0..self.contract.field_count() {
            let Some(raw_value) = self.field_bytes.field(slot) else {
                let _ = self.contract.missing_slot_value(slot)?;
                continue;
            };

            match self.contract.field_leaf_codec(slot)? {
                LeafCodec::Scalar(_) => {
                    #[cfg(any(test, feature = "diagnostics"))]
                    self.metrics.record_validated_slot();
                    self.decode_scalar_slot_value_for_slot(slot, raw_value)?;
                }
                LeafCodec::StructuralFallback => {
                    #[cfg(any(test, feature = "diagnostics"))]
                    {
                        self.metrics.record_validated_slot();
                        self.metrics.record_validated_non_scalar();
                    }
                    self.validate_non_scalar_slot_for_contract(slot, raw_value)?;
                }
            }
        }

        Ok(())
    }

    // Read a direct value-storage scalar through accepted field metadata only.
    fn required_value_storage_scalar_with_accepted_contract(
        &self,
        slot: usize,
    ) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
        let accepted_field = self.required_accepted_field_decode_contract(slot)?;

        self.required_accepted_value_storage_scalar(slot, accepted_field)
    }

    // Read a direct value-storage scalar through generated-compatible field
    // metadata only.
    fn required_value_storage_scalar_with_generated_contract(
        &self,
        slot: usize,
    ) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
        let field = self.contract.field_decode_contract(slot)?;
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
}

impl SlotReader for StructuralSlotReader<'_> {
    fn generated_compatible_field_model(&self, slot: usize) -> Result<&FieldModel, InternalError> {
        self.generated_compatible_field_model_for_slot(slot)
    }

    fn has(&self, slot: usize) -> bool {
        self.field_bytes.field(slot).is_some()
    }

    fn get_bytes(&self, slot: usize) -> Option<&[u8]> {
        self.field_bytes.field(slot)
    }

    fn get_scalar(&self, slot: usize) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
        match self.contract.field_leaf_codec(slot)? {
            LeafCodec::Scalar(_) => match self.cached_values.get(slot) {
                Some(CachedSlotValue::Scalar { validated, .. }) => {
                    if self.field_bytes.field(slot).is_none() {
                        return match self.contract.missing_slot_value(slot)? {
                            Value::Null => Ok(Some(ScalarSlotValueRef::Null)),
                            value => Err(InternalError::persisted_row_decode_failed(format!(
                                "missing scalar slot materialized non-null value: slot={slot} value={value:?}",
                            ))),
                        };
                    }

                    let validated =
                        self.required_validated_scalar_slot_value_for_slot(slot, validated)?;

                    scalar_slot_value_ref_from_validated(
                        validated,
                        self.contract.clone(),
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
    fn field_decode_contract(
        &self,
        slot: usize,
    ) -> Result<StructuralFieldDecodeContract, InternalError> {
        self.contract.field_decode_contract(slot)
    }

    fn field_leaf_codec(&self, slot: usize) -> Result<LeafCodec, InternalError> {
        self.contract.field_leaf_codec(slot)
    }

    fn required_bytes(&self, slot: usize) -> Result<&[u8], InternalError> {
        let field_name = self.contract.field_name(slot)?;

        self.get_bytes(slot)
            .ok_or_else(|| InternalError::persisted_row_declared_field_missing(field_name))
    }

    fn required_scalar(&self, slot: usize) -> Result<ScalarSlotValueRef<'_>, InternalError> {
        let field_name = self.contract.field_name(slot)?;
        debug_assert!(matches!(
            self.contract.field_leaf_codec(slot)?,
            LeafCodec::Scalar(_)
        ));

        self.get_scalar(slot)?
            .ok_or_else(|| InternalError::persisted_row_declared_field_missing(field_name))
    }

    fn required_value_storage_scalar(
        &self,
        slot: usize,
    ) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
        if self.contract.has_accepted_decode_contract() {
            return self.required_value_storage_scalar_with_accepted_contract(slot);
        }

        self.required_value_storage_scalar_with_generated_contract(slot)
    }

    fn required_value_by_contract(&self, slot: usize) -> Result<Value, InternalError> {
        Ok(self.required_cached_value(slot)?.clone())
    }

    fn required_value_by_contract_cow(&self, slot: usize) -> Result<Cow<'_, Value>, InternalError> {
        Ok(Cow::Borrowed(self.required_cached_value(slot)?))
    }
}
