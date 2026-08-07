//! Persisted-row structural slot reader.
//!
//! This module owns the concrete `StructuralSlotReader` adapter over raw
//! persisted row bytes. It is the slot-reader boundary, not a generic row
//! reader core.

use crate::{
    db::schema::{FieldStorageDecode, LeafCodec},
    db::{
        data::{
            DecodedDataStoreKey, RawRow, StructuralRowContract, StructuralRowFieldBytes,
            ValueStorageView,
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
                    primary_key::validate_primary_key_component_from_slot_bytes_with_contract,
                },
                types::{CanonicalSlotReader, SlotReader},
            },
        },
        key_taxonomy::PrimaryKeyValue,
        schema::{AcceptedFieldDecodeContract, AcceptedFieldKind},
    },
    error::InternalError,
    value::Value,
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
    contract: Cow<'a, StructuralRowContract>,
    field_bytes: StructuralRowFieldBytes<'a>,
    pub(in crate::db::data::persisted_row) cached_values: Vec<CachedSlotValue>,
}

impl<'a> StructuralSlotReader<'a> {
    /// Build one slot reader over one persisted row using one frozen
    /// structural row contract without retaining the full entity model.
    pub(in crate::db) fn from_raw_row_with_contract(
        raw_row: &'a RawRow,
        contract: StructuralRowContract,
    ) -> Result<Self, InternalError> {
        Self::from_raw_row_with_contract_cow(raw_row, Cow::Owned(contract))
    }

    /// Build one slot reader over one persisted row by borrowing an existing
    /// frozen structural row contract.
    pub(in crate::db) fn from_raw_row_with_borrowed_contract(
        raw_row: &'a RawRow,
        contract: &'a StructuralRowContract,
    ) -> Result<Self, InternalError> {
        Self::from_raw_row_with_contract_cow(raw_row, Cow::Borrowed(contract))
    }

    fn from_raw_row_with_contract_cow(
        raw_row: &'a RawRow,
        contract: Cow<'a, StructuralRowContract>,
    ) -> Result<Self, InternalError> {
        let field_bytes =
            StructuralRowFieldBytes::from_raw_row_with_contract(raw_row, contract.as_ref())?;
        let cached_values = build_initial_slot_cache(contract.as_ref())?;
        let reader = Self {
            contract,
            field_bytes,
            cached_values,
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

    /// Build one borrowed-contract slot reader, then validate every declared
    /// slot eagerly.
    pub(in crate::db) fn from_raw_row_with_validated_borrowed_contract(
        raw_row: &'a RawRow,
        contract: &'a StructuralRowContract,
    ) -> Result<Self, InternalError> {
        let reader = Self::from_raw_row_with_borrowed_contract(raw_row, contract)?;
        reader.validate_all_declared_slots()?;

        Ok(reader)
    }

    fn required_accepted_field_decode_contract(
        &self,
        slot: usize,
    ) -> Result<AcceptedFieldDecodeContract<'_>, InternalError> {
        self.contract.required_accepted_field_decode_contract(slot)
    }

    /// Return the declared structural field count for this reader contract.
    #[must_use]
    pub(in crate::db) fn field_count(&self) -> usize {
        self.contract.field_count()
    }

    /// Decode selected accepted slots into their full-layout positions.
    ///
    /// The data boundary owns lazy slot decoding; semantic consumers supply
    /// only the precompiled slot set and never reopen field-name projection.
    pub(in crate::db) fn decode_selected_slot_values(
        &self,
        required_slots: &[usize],
    ) -> Result<Vec<Option<Value>>, InternalError> {
        let mut values = vec![None; self.contract.field_count()];
        for &slot in required_slots {
            let value = self.required_cached_value(slot)?.clone();
            let target = values.get_mut(slot).ok_or_else(|| {
                InternalError::persisted_row_slot_cache_lookup_out_of_bounds(
                    self.contract.entity_path(),
                    slot,
                )
            })?;
            *target = Some(value);
        }

        Ok(values)
    }

    /// Borrow the structural row contract selected for this reader.
    #[must_use]
    pub(in crate::db) fn contract(&self) -> &StructuralRowContract {
        self.contract.as_ref()
    }

    /// Validate the decoded primary-key slot against the authoritative row identity.
    pub(in crate::db) fn validate_primary_key(
        &self,
        data_key: &DecodedDataStoreKey,
    ) -> Result<(), InternalError> {
        self.validate_primary_key_value(&data_key.primary_key_value())
    }

    // Validate the decoded primary-key slot against one authoritative
    // primary-key value without rebuilding a full `DecodedDataStoreKey`
    // wrapper at the call site.
    pub(in crate::db) fn validate_primary_key_value(
        &self,
        expected_key: &PrimaryKeyValue,
    ) -> Result<(), InternalError> {
        match *expected_key {
            PrimaryKeyValue::Scalar(component) => {
                let primary_key_slot = self.contract.primary_key_slot();

                // Preserve the reader's scalar validation/cache side effect before the
                // shared row-contract validator performs the authoritative key check.
                if matches!(
                    self.contract.field_leaf_codec(primary_key_slot)?,
                    LeafCodec::Scalar(_)
                ) && let Some(CachedSlotValue::Scalar { validated, .. }) =
                    self.cached_values.get(primary_key_slot)
                {
                    let _ = self.required_validated_scalar_slot_value_for_slot(
                        primary_key_slot,
                        validated,
                    )?;
                }

                let field_name = self.contract.field_name(primary_key_slot)?;
                let raw_value = self.required_field_bytes(primary_key_slot, field_name)?;

                validate_primary_key_component_from_slot_bytes_with_contract(
                    &self.contract,
                    primary_key_slot,
                    raw_value,
                    component,
                )
            }
            PrimaryKeyValue::Composite(composite) => {
                let slots = self.contract.primary_key_slot_indices();
                if slots.len() != composite.len() {
                    return Err(InternalError::persisted_row_decode_corruption());
                }

                for (&slot, &component) in slots.iter().zip(composite.components()) {
                    if matches!(self.contract.field_leaf_codec(slot)?, LeafCodec::Scalar(_))
                        && let Some(CachedSlotValue::Scalar { validated, .. }) =
                            self.cached_values.get(slot)
                    {
                        let _ =
                            self.required_validated_scalar_slot_value_for_slot(slot, validated)?;
                    }

                    let field_name = self.contract.field_name(slot)?;
                    let raw_value = self.required_field_bytes(slot, field_name)?;
                    validate_primary_key_component_from_slot_bytes_with_contract(
                        &self.contract,
                        slot,
                        raw_value,
                        component,
                    )?;
                }

                Ok(())
            }
        }
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
        let validated_value = validated_scalar_slot_value(
            decode_scalar_slot_value_from_row_contract(&self.contract, slot, raw_value)?,
        );
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
                        let _ = materialized.set(
                            self.contract
                                .historical_slot_value(slot, self.field_bytes.layout_version())?,
                        );
                    }

                    return materialized
                        .get()
                        .ok_or_else(InternalError::persisted_row_decode_corruption);
                }

                let validated =
                    self.required_validated_scalar_slot_value_for_slot(slot, validated)?;
                if materialized.get().is_none() {
                    let value = materialize_validated_scalar_slot_value(
                        validated,
                        &self.contract,
                        &self.field_bytes,
                        slot,
                    )?;
                    let _ = materialized.set(value);
                }

                materialized
                    .get()
                    .ok_or_else(InternalError::persisted_row_decode_corruption)
            }
            CachedSlotValue::Deferred { materialized } => {
                if self.field_bytes.field(slot).is_none() {
                    if materialized.get().is_none() {
                        let _ = materialized.set(
                            self.contract
                                .historical_slot_value(slot, self.field_bytes.layout_version())?,
                        );
                    }

                    return materialized
                        .get()
                        .ok_or_else(InternalError::persisted_row_decode_corruption);
                }

                let field_name = self.contract.field_name(slot)?;
                let raw_value = self.required_field_bytes(slot, field_name)?;
                if materialized.get().is_none() {
                    self.validate_non_scalar_slot_for_contract(slot, raw_value)?;
                    let value =
                        decode_runtime_value_from_row_contract(&self.contract, slot, raw_value)?;
                    let _ = materialized.set(value);
                }

                materialized
                    .get()
                    .ok_or_else(InternalError::persisted_row_decode_corruption)
            }
        }
    }

    /// Materialize one slot for a direct projection with a scalar value-storage fast path.
    pub(in crate::db) fn required_direct_projection_value(
        &self,
        slot: usize,
    ) -> Result<Value, InternalError> {
        // Phase 1: value-storage scalar fields can project directly from the
        // validated byte view when the persisted tag matches the declared
        // scalar kind. Mismatches fall through to preserve the existing
        // full catalog-backed materialization and validation behavior.
        let accepted_field = self.required_accepted_field_decode_contract(slot)?;
        if let Some(value) = self.required_accepted_value_storage_scalar(slot, accepted_field)? {
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
        decode_scalar_slot_value_from_row_contract(&self.contract, slot, raw_value)
    }

    // Validate one structural slot through accepted metadata.
    fn validate_non_scalar_slot_for_contract(
        &self,
        slot: usize,
        raw_value: &[u8],
    ) -> Result<(), InternalError> {
        validate_non_scalar_slot_value_with_row_contract(&self.contract, slot, raw_value)
    }

    // Read a value-storage scalar without reopening generated kind metadata.
    fn try_value_storage_non_null_accepted_scalar_slot_value<'view>(
        field: AcceptedFieldDecodeContract<'_>,
        view: &ValueStorageView<'view>,
    ) -> Result<Option<ScalarSlotValueRef<'view>>, InternalError> {
        let value = match field.kind() {
            AcceptedFieldKind::Bool if view.is_bool() => {
                ScalarValueRef::Bool(view.as_bool().map_err(|err| {
                    InternalError::persisted_row_field_kind_decode_failed(
                        field.field_name(),
                        field.kind(),
                        err,
                    )
                })?)
            }
            AcceptedFieldKind::Blob { .. } if view.is_blob() => {
                ScalarValueRef::Blob(view.as_blob().map_err(|err| {
                    InternalError::persisted_row_field_kind_decode_failed(
                        field.field_name(),
                        field.kind(),
                        err,
                    )
                })?)
            }
            AcceptedFieldKind::Int64 if view.is_i64() => {
                ScalarValueRef::Int(view.as_i64().map_err(|err| {
                    InternalError::persisted_row_field_kind_decode_failed(
                        field.field_name(),
                        field.kind(),
                        err,
                    )
                })?)
            }
            AcceptedFieldKind::Text { .. } if view.is_text() => {
                ScalarValueRef::Text(view.as_text().map_err(|err| {
                    InternalError::persisted_row_field_kind_decode_failed(
                        field.field_name(),
                        field.kind(),
                        err,
                    )
                })?)
            }
            AcceptedFieldKind::Nat64 if view.is_u64() => {
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
    // field contract.
    fn required_accepted_value_storage_scalar(
        &self,
        slot: usize,
        field: AcceptedFieldDecodeContract<'_>,
    ) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
        if !matches!(field.storage_decode(), FieldStorageDecode::CatalogValue) {
            return Ok(None);
        }
        if self.field_bytes.field(slot).is_none() {
            return match self
                .contract
                .historical_slot_value(slot, self.field_bytes.layout_version())?
            {
                Value::Null => Ok(Some(ScalarSlotValueRef::Null)),
                _ => Err(InternalError::persisted_row_decode_corruption()),
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
            if !self.contract.has_active_field_slot(slot) {
                continue;
            }

            let Some(raw_value) = self.field_bytes.field(slot) else {
                let _ = self
                    .contract
                    .historical_slot_value(slot, self.field_bytes.layout_version())?;
                continue;
            };

            match self.contract.field_leaf_codec(slot)? {
                LeafCodec::Scalar(_) => {
                    self.decode_scalar_slot_value_for_slot(slot, raw_value)?;
                }
                LeafCodec::Structural => {
                    self.validate_non_scalar_slot_for_contract(slot, raw_value)?;
                }
            }
        }

        Ok(())
    }

    // Read a direct value-storage scalar through accepted field metadata only.
    fn required_value_storage_scalar(
        &self,
        slot: usize,
    ) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
        let accepted_field = self.required_accepted_field_decode_contract(slot)?;

        self.required_accepted_value_storage_scalar(slot, accepted_field)
    }
}

impl SlotReader for StructuralSlotReader<'_> {
    fn get_bytes(&self, slot: usize) -> Option<&[u8]> {
        self.field_bytes.field(slot)
    }

    fn get_scalar(&self, slot: usize) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
        match self.contract.field_leaf_codec(slot)? {
            LeafCodec::Scalar(_) => match self.cached_values.get(slot) {
                Some(CachedSlotValue::Scalar { validated, .. }) => {
                    if self.field_bytes.field(slot).is_none() {
                        return match self
                            .contract
                            .historical_slot_value(slot, self.field_bytes.layout_version())?
                        {
                            Value::Null => Ok(Some(ScalarSlotValueRef::Null)),
                            _ => Err(InternalError::persisted_row_decode_corruption()),
                        };
                    }

                    let validated =
                        self.required_validated_scalar_slot_value_for_slot(slot, validated)?;

                    scalar_slot_value_ref_from_validated(
                        validated,
                        &self.contract,
                        &self.field_bytes,
                        slot,
                    )
                    .map(Some)
                }
                Some(CachedSlotValue::Deferred { .. }) => {
                    Err(InternalError::persisted_row_decode_corruption())
                }
                None => Err(
                    InternalError::persisted_row_slot_cache_lookup_out_of_bounds(
                        self.contract.entity_path(),
                        slot,
                    ),
                ),
            },
            LeafCodec::Structural => Ok(None),
        }
    }

    fn get_value(&mut self, slot: usize) -> Result<Option<Value>, InternalError> {
        Ok(Some(self.required_cached_value(slot)?.clone()))
    }
}

impl CanonicalSlotReader for StructuralSlotReader<'_> {
    fn field_name(&self, slot: usize) -> Result<&str, InternalError> {
        self.contract.field_name(slot)
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
        StructuralSlotReader::required_value_storage_scalar(self, slot)
    }

    fn required_value_by_contract(&self, slot: usize) -> Result<Value, InternalError> {
        Ok(self.required_cached_value(slot)?.clone())
    }

    fn required_value_by_contract_cow(&self, slot: usize) -> Result<Cow<'_, Value>, InternalError> {
        Ok(Cow::Borrowed(self.required_cached_value(slot)?))
    }
}

#[cfg(test)]
mod tests {
    use super::{CachedSlotValue, StructuralSlotReader};
    use crate::{
        db::{
            data::{
                StructuralRowContract,
                canonical_row_from_runtime_value_source_with_accepted_contract,
            },
            schema::{
                AcceptedCompositeCatalog, AcceptedFieldKind, AcceptedRowLayoutRuntimeContract,
                AcceptedSchemaRevision, AcceptedSchemaSnapshot, AcceptedValueCatalogHandle,
                FieldId, FieldStorageDecode, LeafCodec, PersistedFieldSnapshot,
                PersistedSchemaSnapshot, ScalarCodec, SchemaFieldSlot, SchemaInsertDefault,
                SchemaRowLayout, SchemaVersion, empty_accepted_enum_catalog_for_tests,
            },
        },
        value::Value,
    };
    use std::borrow::Cow;

    fn selective_payload_contract() -> StructuralRowContract {
        let fields = vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "matches".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Bool,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Bool),
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(3),
                "snapshot".to_string(),
                SchemaFieldSlot::new(2),
                AcceptedFieldKind::Blob {
                    max_len: Some(512 * 1_024),
                },
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Blob),
            ),
        ];
        let accepted = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "tests::SelectivePayload".to_string(),
            "SelectivePayload".to_string(),
            FieldId::new(1),
            SchemaRowLayout::initial(
                fields
                    .iter()
                    .map(|field| (field.id(), field.slot()))
                    .collect(),
            ),
            fields,
        ));
        let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)
            .expect("large-payload accepted row layout should build");
        let value_catalog = AcceptedValueCatalogHandle::new_for_tests(
            empty_accepted_enum_catalog_for_tests(),
            AcceptedCompositeCatalog::empty(),
            AcceptedSchemaRevision::INITIAL,
        );

        StructuralRowContract::from_accepted_decode_contract(
            accepted.entity_path(),
            descriptor.row_decode_contract(value_catalog),
        )
    }

    fn scalar_slot_is_untouched(value: &CachedSlotValue) -> bool {
        matches!(
            value,
            CachedSlotValue::Scalar {
                validated,
                materialized,
            } if validated.get().is_none() && materialized.get().is_none()
        )
    }

    #[test]
    fn selective_small_field_read_does_not_decode_large_unrelated_payload() {
        let contract = selective_payload_contract();
        let values = [
            Value::Nat64(7),
            Value::Bool(false),
            Value::Blob(vec![0xA5; 300 * 1_024]),
        ];
        let row =
            canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
                Ok(Cow::Borrowed(&values[slot]))
            })
            .expect("large-payload fixture row should encode")
            .into_raw_row();
        let reader = StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract)
            .expect("large-payload fixture row should open lazily");

        assert!(scalar_slot_is_untouched(&reader.cached_values[2]));
        assert_eq!(
            reader
                .required_cached_value(1)
                .expect("small residual-like metadata slot should decode"),
            &Value::Bool(false),
        );
        assert!(
            scalar_slot_is_untouched(&reader.cached_values[2]),
            "rejecting on small metadata must not validate or materialize the unrelated snapshot",
        );
    }
}
