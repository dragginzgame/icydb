//! Module: data::structural_row
//! Responsibility: canonical structural persisted-row decode helpers.
//! Does not own: typed entity reconstruction, slot layout planning, or query semantics.
//! Boundary: runtime paths use this module when they need persisted-row structure without `E`.

use crate::{
    db::{
        codec::{DecodedRowPayload, decode_row_payload_bytes},
        data::{
            RawRow, decode_runtime_value_from_row_contract,
            encode_canonical_value_for_accepted_field_contract,
        },
        schema::{
            AcceptedCatalogSnapshotSelection, AcceptedFieldDecodeContract,
            AcceptedFieldPersistenceContract, AcceptedInsertOmissionPolicy,
            AcceptedRowDecodeContract, AcceptedRowLayoutRuntimeContract, AcceptedSchemaSnapshot,
            OwnedAcceptedFieldDecodeContract, OwnedAcceptedRelationEdgeContract, RowLayoutVersion,
            SchemaHistoricalFill,
        },
    },
    error::InternalError,
    model::{entity::EntityModel, field::LeafCodec},
    value::Value,
};
use std::{borrow::Cow, rc::Rc};

type SlotSpan = Option<(usize, usize)>;
type SlotSpans = Vec<SlotSpan>;
type RowFieldSpans<'a> = (Cow<'a, [u8]>, SlotSpans);
type RowSlotTableSections<'a> = (usize, usize, &'a [u8], &'a [u8]);

enum FieldMaterialization<'a> {
    Null,
    DefaultPayload(&'a [u8]),
}

/// Accepted snapshot and structural row contract selected from one catalog root.
#[derive(Clone, Debug)]
pub(in crate::db) struct AcceptedStructuralRowAuthority {
    accepted_schema: AcceptedSchemaSnapshot,
    row_contract: StructuralRowContract,
}

impl AcceptedStructuralRowAuthority {
    /// Build accepted-only row authority without separating snapshot and catalog.
    pub(in crate::db) fn from_catalog_selection(
        entity_path: &'static str,
        selection: &AcceptedCatalogSnapshotSelection,
    ) -> Result<Self, InternalError> {
        let accepted_schema = selection.decode_verified()?;
        let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted_schema)?;
        let row_contract = Self::catalog_backed_row_contract(entity_path, &descriptor, selection);

        Ok(Self {
            accepted_schema,
            row_contract,
        })
    }

    /// Build generated-compatible row authority from one catalog selection.
    pub(in crate::db) fn from_generated_compatible_catalog_selection(
        entity_path: &'static str,
        model: &'static EntityModel,
        selection: &AcceptedCatalogSnapshotSelection,
    ) -> Result<Self, InternalError> {
        let accepted_schema = selection.decode_verified()?;
        let (descriptor, _row_proof) =
            AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(
                &accepted_schema,
                model,
                selection.value_catalog_handle().enum_catalog(),
                selection.value_catalog_handle().composite_catalog(),
            )?;
        let row_contract = Self::catalog_backed_row_contract(entity_path, &descriptor, selection);

        Ok(Self {
            accepted_schema,
            row_contract,
        })
    }

    /// Build candidate row authority from an accepted snapshot and the exact
    /// value catalogs that will be published with it.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn from_candidate_snapshot(
        entity_path: &'static str,
        accepted_schema: AcceptedSchemaSnapshot,
        value_catalog: crate::db::schema::AcceptedValueCatalogHandle,
    ) -> Result<Self, InternalError> {
        let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted_schema)?;
        let row_decode_contract = descriptor.row_decode_contract(value_catalog);
        let row_contract =
            StructuralRowContract::from_accepted_decode_contract(entity_path, row_decode_contract);

        Ok(Self {
            accepted_schema,
            row_contract,
        })
    }

    fn catalog_backed_row_contract(
        entity_path: &'static str,
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        selection: &AcceptedCatalogSnapshotSelection,
    ) -> StructuralRowContract {
        let identity = selection.identity();
        let row_decode_contract =
            descriptor.row_decode_contract(selection.value_catalog_handle().clone());
        debug_assert_eq!(
            row_decode_contract.accepted_schema_revision(),
            identity.accepted_schema_revision()
        );
        debug_assert!(std::ptr::eq(
            row_decode_contract.enum_catalog(),
            selection.value_catalog_handle().enum_catalog(),
        ));
        StructuralRowContract::from_accepted_decode_contract(entity_path, row_decode_contract)
    }

    /// Borrow the accepted snapshot selected with this row contract.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db) const fn accepted_schema(&self) -> &AcceptedSchemaSnapshot {
        &self.accepted_schema
    }

    /// Consume this authority into its still-paired accepted artifacts.
    #[must_use]
    pub(in crate::db) fn into_parts(self) -> (AcceptedSchemaSnapshot, StructuralRowContract) {
        (self.accepted_schema, self.row_contract)
    }

    /// Consume this authority when the caller only needs structural row decode.
    #[must_use]
    pub(in crate::db) fn into_row_contract(self) -> StructuralRowContract {
        self.row_contract
    }
}

///
/// StructuralRowContract
///
/// StructuralRowContract is the compact static row-shape authority used by
/// structural row readers that do not need the full semantic `EntityModel`.
/// It keeps the entity path and accepted row-decode contract required to open
/// canonical persisted rows through the data-layer decode boundary.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct StructuralRowContract {
    entity_path: &'static str,
    field_count: usize,
    primary_key_slot: usize,
    accepted_decode_contract: Rc<AcceptedRowDecodeContract>,
}

impl StructuralRowContract {
    /// Build an accepted structural row contract from one model proposal for tests.
    #[cfg(test)]
    pub(in crate::db) fn from_model_proposal_for_test(
        model: &'static crate::model::entity::EntityModel,
    ) -> Self {
        Self::from_accepted_decode_contract(
            model.path(),
            AcceptedRowDecodeContract::from_model_proposal_for_test(model),
        )
    }

    /// Build one structural row contract from accepted persisted schema only.
    #[must_use]
    pub(in crate::db) fn from_accepted_decode_contract(
        entity_path: &'static str,
        accepted_decode_contract: AcceptedRowDecodeContract,
    ) -> Self {
        Self {
            entity_path,
            field_count: accepted_decode_contract.required_slot_count(),
            primary_key_slot: accepted_decode_contract.first_primary_key_slot_index(),
            accepted_decode_contract: Rc::new(accepted_decode_contract),
        }
    }

    /// Borrow the owning entity path for diagnostics.
    #[must_use]
    pub(in crate::db) const fn entity_path(&self) -> &'static str {
        self.entity_path
    }

    /// Return the declared structural field count.
    #[must_use]
    pub(in crate::db) const fn field_count(&self) -> usize {
        self.field_count
    }

    /// Return the layout identity stamped by every current canonical writer.
    #[must_use]
    pub(in crate::db) fn current_layout_version(&self) -> RowLayoutVersion {
        self.accepted_decode_contract.current_layout_version()
    }

    /// Return the authoritative primary-key slot.
    #[must_use]
    pub(in crate::db) const fn primary_key_slot(&self) -> usize {
        self.primary_key_slot
    }

    /// Borrow ordered primary-key physical slots in key order.
    #[must_use]
    pub(in crate::db) fn primary_key_slot_indices(&self) -> &[usize] {
        self.accepted_decode_contract.primary_key_slot_indices()
    }

    pub(in crate::db) fn required_accepted_field_decode_contract(
        &self,
        slot: usize,
    ) -> Result<AcceptedFieldDecodeContract<'_>, InternalError> {
        Ok(self
            .accepted_decode_contract
            .required_field_for_slot(self.entity_path(), slot)?
            .decode_contract())
    }

    /// Borrow one complete accepted field contract by stable physical slot.
    pub(in crate::db) fn required_accepted_field_contract(
        &self,
        slot: usize,
    ) -> Result<&OwnedAcceptedFieldDecodeContract, InternalError> {
        self.accepted_decode_contract
            .required_field_for_slot(self.entity_path(), slot)
    }

    /// Borrow one accepted field with the catalog authority that admitted it.
    pub(in crate::db) fn required_accepted_field_persistence_contract(
        &self,
        slot: usize,
    ) -> Result<AcceptedFieldPersistenceContract<'_>, InternalError> {
        self.accepted_decode_contract
            .required_field_persistence_contract(self.entity_path(), slot)
    }

    /// Borrow the catalog authority carried by this accepted row contract.
    #[must_use]
    pub(in crate::db) fn accepted_value_catalog_handle(
        &self,
    ) -> &crate::db::schema::AcceptedValueCatalogHandle {
        self.accepted_decode_contract.value_catalog_handle()
    }

    /// Borrow accepted relation-edge metadata declared on this source row.
    #[must_use]
    pub(in crate::db) fn accepted_relation_edges(&self) -> &[OwnedAcceptedRelationEdgeContract] {
        self.accepted_decode_contract.relation_edges()
    }

    /// Return whether a physical slot is active in this row contract.
    ///
    /// Accepted row layouts may retain retired physical slots as allocation
    /// history. Those slots can still be present in old rows, but they are no
    /// longer active fields and must be skipped by dense validation/emission.
    #[must_use]
    pub(in crate::db) fn has_active_field_slot(&self, slot: usize) -> bool {
        self.accepted_decode_contract.field_for_slot(slot).is_some()
    }

    /// Return the leaf codec for one structural slot.
    ///
    pub(in crate::db) fn field_leaf_codec(&self, slot: usize) -> Result<LeafCodec, InternalError> {
        self.required_accepted_field_decode_contract(slot)
            .map(|field| field.leaf_codec())
    }

    /// Return the persisted field name for diagnostics at one row slot.
    pub(in crate::db) fn field_name(&self, slot: usize) -> Result<&str, InternalError> {
        self.required_accepted_field_decode_contract(slot)
            .map(|field| field.field_name())
    }

    /// Return one field's physical row slot by persisted field name.
    ///
    pub(in crate::db) fn field_slot_index_by_name(
        &self,
        field_name: &str,
    ) -> Result<usize, InternalError> {
        for slot in 0..self.field_count() {
            let Some(field) = self.accepted_decode_contract.field_for_slot(slot) else {
                continue;
            };
            if field.field_name() == field_name {
                return Ok(slot);
            }
        }

        Err(InternalError::persisted_row_declared_field_missing(
            field_name,
        ))
    }

    /// Resolve an omitted field for a future logical insert after-image.
    pub(in crate::db) fn insert_omission_value(&self, slot: usize) -> Result<Value, InternalError> {
        match self.insert_omission_materialization(slot)? {
            FieldMaterialization::Null => Ok(Value::Null),
            FieldMaterialization::DefaultPayload(payload) => {
                decode_runtime_value_from_row_contract(self, slot, payload)
            }
        }
    }

    /// Resolve an omitted field into a current canonical insertion payload.
    pub(in crate::db) fn insert_omission_payload(
        &self,
        slot: usize,
    ) -> Result<Vec<u8>, InternalError> {
        match self.insert_omission_materialization(slot)? {
            FieldMaterialization::Null => {
                let encoding = self.required_accepted_field_persistence_contract(slot)?;
                encode_canonical_value_for_accepted_field_contract(encoding, &Value::Null)
            }
            FieldMaterialization::DefaultPayload(payload) => Ok(payload.to_vec()),
        }
    }

    fn insert_omission_materialization(
        &self,
        slot: usize,
    ) -> Result<FieldMaterialization<'_>, InternalError> {
        let field = self
            .accepted_decode_contract
            .required_field_for_slot(self.entity_path(), slot)?;
        match field.insert_omission_policy() {
            AcceptedInsertOmissionPolicy::NullIfMissing => Ok(FieldMaterialization::Null),
            AcceptedInsertOmissionPolicy::DefaultIfMissing => field
                .insert_default()
                .slot_payload()
                .map(FieldMaterialization::DefaultPayload)
                .ok_or_else(|| {
                    InternalError::persisted_row_declared_field_missing(field.field_name())
                }),
            AcceptedInsertOmissionPolicy::Required => Err(
                InternalError::persisted_row_declared_field_missing(field.field_name()),
            ),
        }
    }

    /// Materialize a logically present value from legitimate historical absence.
    pub(in crate::db) fn historical_slot_value(
        &self,
        slot: usize,
        row_layout_version: RowLayoutVersion,
    ) -> Result<Value, InternalError> {
        let field = self
            .accepted_decode_contract
            .required_field_for_slot(self.entity_path(), slot)?;
        if field.introduced_in_layout() <= row_layout_version {
            return Err(InternalError::persisted_row_decode_corruption());
        }

        match field.historical_fill() {
            SchemaHistoricalFill::Reject => Err(InternalError::persisted_row_decode_corruption()),
            SchemaHistoricalFill::Null => Ok(Value::Null),
            SchemaHistoricalFill::SlotPayload(payload) => {
                decode_runtime_value_from_row_contract(self, slot, payload)
            }
        }
    }

    // Require the stamped layout to be admitted and its physical slot count to
    // match exactly before any full or sparse reader traverses the slot table.
    fn validate_physical_slot_count(
        &self,
        layout_version: RowLayoutVersion,
        physical_count: usize,
    ) -> Result<(), InternalError> {
        let expected = self
            .accepted_decode_contract
            .expected_slot_count(layout_version)?;
        if physical_count == expected {
            Ok(())
        } else {
            Err(InternalError::persisted_row_slot_count_mismatch())
        }
    }
}

///
/// StructuralRowFieldBytes
///
/// StructuralRowFieldBytes is the top-level persisted-row field scanner for
/// slot-driven proof paths.
/// It keeps the original encoded field payload bytes and records one byte span
/// per model slot so callers can decode only the fields they actually need.
///

#[derive(Clone, Debug)]
pub(in crate::db::data) struct StructuralRowFieldBytes<'a> {
    layout_version: RowLayoutVersion,
    payload: Cow<'a, [u8]>,
    spans: SlotSpans,
}

impl<'a> StructuralRowFieldBytes<'a> {
    /// Decode one raw row payload into contract slot-aligned encoded field spans.
    fn from_row_bytes_with_contract(
        row_bytes: &'a [u8],
        contract: &StructuralRowContract,
    ) -> Result<Self, InternalError> {
        let decoded = decode_structural_row_payload_bytes(row_bytes)?;
        let layout_version = decoded.layout_version();
        let (payload, spans) =
            decode_row_field_spans(decoded.into_payload(), layout_version, contract)?;

        Ok(Self {
            layout_version,
            payload,
            spans,
        })
    }

    /// Decode one raw row into contract slot-aligned encoded field payload spans.
    pub(in crate::db::data) fn from_raw_row_with_contract(
        raw_row: &'a RawRow,
        contract: &StructuralRowContract,
    ) -> Result<Self, InternalError> {
        Self::from_row_bytes_with_contract(raw_row.as_bytes(), contract)
    }

    /// Borrow one encoded persisted field payload by stable slot index.
    #[must_use]
    pub(in crate::db::data) fn field(&self, slot: usize) -> Option<&[u8]> {
        let (start, end) = self.spans.get(slot).copied().flatten()?;

        Some(&self.payload[start..end])
    }

    /// Return the stamped physical row-layout identity.
    #[must_use]
    pub(in crate::db::data) const fn layout_version(&self) -> RowLayoutVersion {
        self.layout_version
    }

    /// Return the exact physical slot count admitted for the stamped layout.
    #[must_use]
    pub(in crate::db::data) fn physical_slot_count(&self) -> usize {
        self.spans.iter().filter(|span| span.is_some()).count()
    }
}

///
/// SparseRequiredRowFieldBytes
///
/// SparseRequiredRowFieldBytes carries the shared payload plus just the two
/// slot spans needed by the narrow sparse required-slot decode path.
/// Executor one-slot reads use this to preserve full row-table validation
/// without allocating one field-count-sized span vector on every row.
///

#[derive(Clone, Debug)]
pub(in crate::db::data) struct SparseRequiredRowFieldBytes<'a> {
    layout_version: RowLayoutVersion,
    payload: Cow<'a, [u8]>,
    required_span: Option<(usize, usize)>,
    primary_key_span: (usize, usize),
}

impl<'a> SparseRequiredRowFieldBytes<'a> {
    /// Decode one raw row into the selected and primary-key field spans needed
    /// by sparse direct slot reads.
    pub(in crate::db::data) fn from_raw_row_with_contract(
        raw_row: &'a RawRow,
        contract: &StructuralRowContract,
        required_slot: usize,
    ) -> Result<Self, InternalError> {
        let decoded = decode_structural_row_payload_bytes(raw_row.as_bytes())?;
        let layout_version = decoded.layout_version();
        let (payload, required_span, primary_key_span) = decode_sparse_required_row_field_spans(
            decoded.into_payload(),
            layout_version,
            contract,
            required_slot,
        )?;

        Ok(Self {
            layout_version,
            payload,
            required_span,
            primary_key_span,
        })
    }

    /// Borrow the selected required field payload bytes.
    #[must_use]
    pub(in crate::db::data) fn required_field(&self) -> Option<&[u8]> {
        let (start, end) = self.required_span?;

        Some(&self.payload[start..end])
    }

    /// Borrow the primary-key field payload bytes.
    #[must_use]
    pub(in crate::db::data) fn primary_key_field(&self) -> &[u8] {
        &self.payload[self.primary_key_span.0..self.primary_key_span.1]
    }

    /// Return the stamped physical row-layout identity.
    #[must_use]
    pub(in crate::db::data) const fn layout_version(&self) -> RowLayoutVersion {
        self.layout_version
    }
}

/// Decode one persisted row through the structural row-envelope validation path.
///
/// The only supported persisted row shape is the slot-framed payload envelope,
/// so this helper returns the validated enclosed payload bytes directly.
pub(in crate::db) fn decode_structural_row_payload(
    raw_row: &RawRow,
) -> Result<DecodedRowPayload<'_>, InternalError> {
    decode_structural_row_payload_bytes(raw_row.as_bytes())
}

// Decode one persisted row envelope into the enclosed slot payload bytes.
fn decode_structural_row_payload_bytes(
    bytes: &[u8],
) -> Result<DecodedRowPayload<'_>, InternalError> {
    decode_row_payload_bytes(bytes)
}

// Decode the canonical slot-container header into slot-aligned payload spans.
fn decode_row_field_spans<'payload>(
    payload: Cow<'payload, [u8]>,
    layout_version: RowLayoutVersion,
    contract: &StructuralRowContract,
) -> Result<RowFieldSpans<'payload>, InternalError> {
    let bytes = payload.as_ref();
    let (data_start, physical_count, table, data_section) =
        decode_slot_table_sections(bytes, layout_version, contract)?;
    let mut spans: SlotSpans = vec![None; contract.field_count()];

    for (slot, span) in spans.iter_mut().take(physical_count).enumerate() {
        let entry_start = slot
            .checked_mul(8)
            .ok_or_else(InternalError::persisted_row_decode_corruption)?;
        let entry = table
            .get(entry_start..entry_start + 8)
            .ok_or_else(InternalError::persisted_row_decode_corruption)?;
        let start = usize::try_from(u32::from_be_bytes([entry[0], entry[1], entry[2], entry[3]]))
            .map_err(|_| InternalError::persisted_row_decode_corruption())?;
        let len = usize::try_from(u32::from_be_bytes([entry[4], entry[5], entry[6], entry[7]]))
            .map_err(|_| InternalError::persisted_row_decode_corruption())?;
        if len == 0 {
            return Err(InternalError::persisted_row_decode_corruption());
        }
        let end = start
            .checked_add(len)
            .ok_or_else(InternalError::persisted_row_decode_corruption)?;
        if end > data_section.len() {
            return Err(InternalError::persisted_row_decode_corruption());
        }
        *span = Some((start, end));
    }

    let payload = match payload {
        Cow::Borrowed(bytes) => Cow::Borrowed(&bytes[data_start..]),
        Cow::Owned(bytes) => Cow::Owned(bytes[data_start..].to_vec()),
    };

    Ok((payload, spans))
}

type SparseRequiredRowFieldSpans<'a> =
    Result<(Cow<'a, [u8]>, Option<(usize, usize)>, (usize, usize)), InternalError>;

// Decode the canonical slot-container header while retaining only one required
// slot span plus the primary-key span for sparse direct slot reads.
fn decode_sparse_required_row_field_spans<'payload>(
    payload: Cow<'payload, [u8]>,
    layout_version: RowLayoutVersion,
    contract: &StructuralRowContract,
    required_slot: usize,
) -> SparseRequiredRowFieldSpans<'payload> {
    let bytes = payload.as_ref();
    let (data_start, physical_count, table, data_section) =
        decode_slot_table_sections(bytes, layout_version, contract)?;
    let primary_key_slot = contract.primary_key_slot();
    let mut required_span = None;
    let mut primary_key_span = None;

    for slot in 0..physical_count {
        let entry_start = slot
            .checked_mul(8)
            .ok_or_else(InternalError::persisted_row_decode_corruption)?;
        let entry = table
            .get(entry_start..entry_start + 8)
            .ok_or_else(InternalError::persisted_row_decode_corruption)?;
        let start = usize::try_from(u32::from_be_bytes([entry[0], entry[1], entry[2], entry[3]]))
            .map_err(|_| InternalError::persisted_row_decode_corruption())?;
        let len = usize::try_from(u32::from_be_bytes([entry[4], entry[5], entry[6], entry[7]]))
            .map_err(|_| InternalError::persisted_row_decode_corruption())?;
        if len == 0 {
            return Err(InternalError::persisted_row_decode_corruption());
        }
        let end = start
            .checked_add(len)
            .ok_or_else(InternalError::persisted_row_decode_corruption)?;
        if end > data_section.len() {
            return Err(InternalError::persisted_row_decode_corruption());
        }
        if slot == required_slot {
            required_span = Some((start, end));
        }
        if slot == primary_key_slot {
            primary_key_span = Some((start, end));
        }
    }

    let primary_key_span =
        primary_key_span.ok_or_else(InternalError::persisted_row_decode_corruption)?;
    let payload = match payload {
        Cow::Borrowed(bytes) => Cow::Borrowed(&bytes[data_start..]),
        Cow::Owned(bytes) => Cow::Owned(bytes[data_start..].to_vec()),
    };

    Ok((payload, required_span, primary_key_span))
}

// Decode the shared slot-table header and validate that the physical row slot
// count matches the structural contract before any full or sparse slot scanner
// walks the table. This keeps accepted raw-row shape authority in one place.
fn decode_slot_table_sections<'bytes>(
    bytes: &'bytes [u8],
    layout_version: RowLayoutVersion,
    contract: &StructuralRowContract,
) -> Result<RowSlotTableSections<'bytes>, InternalError> {
    let field_count_bytes = bytes
        .get(..2)
        .ok_or_else(InternalError::persisted_row_decode_corruption)?;
    let field_count = usize::from(u16::from_be_bytes([
        field_count_bytes[0],
        field_count_bytes[1],
    ]));
    contract.validate_physical_slot_count(layout_version, field_count)?;
    let table_len = field_count
        .checked_mul(8)
        .ok_or_else(InternalError::persisted_row_decode_corruption)?;
    let data_start = 2usize
        .checked_add(table_len)
        .ok_or_else(InternalError::persisted_row_decode_corruption)?;
    let table = bytes
        .get(2..data_start)
        .ok_or_else(InternalError::persisted_row_decode_corruption)?;
    let data_section = bytes
        .get(data_start..)
        .ok_or_else(InternalError::persisted_row_decode_corruption)?;

    Ok((data_start, field_count, table, data_section))
}
