//! Module: db::schema::runtime
//! Responsibility: accepted-schema runtime row-layout descriptors.
//! Does not own: raw row decoding, write execution, or transition policy.
//! Boundary: turns accepted metadata into explicit decode/write layout facts.

use crate::{
    db::schema::{
        AcceptedEnumCatalog, AcceptedFieldKind, AcceptedSchemaRevision, AcceptedSchemaSnapshot,
        AcceptedValueAdmissionContract, AcceptedValueCatalogHandle, AcceptedValueContract, FieldId,
        PersistedNestedLeafSnapshot, PersistedRelationEdgeSnapshot, RowLayoutVersion,
        SchemaFieldSlot, SchemaFieldWritePolicy, SchemaHistoricalFill, SchemaInsertDefault,
        enum_catalog::EnumCatalogBuildError,
    },
    error::InternalError,
    model::field::{FieldStorageDecode, LeafCodec},
};

///
/// AcceptedInsertOmissionPolicy
///
/// Accepted insertion policy for an omitted ordinary field. Historical
/// physical absence is owned separately by `SchemaHistoricalFill`.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedInsertOmissionPolicy {
    NullIfMissing,
    DefaultIfMissing,
    Required,
}

/// Return whether an insert may omit one accepted field.
///
/// Accepted null/default policy and database-owned generation are the only
/// omission authorities. Rust `Default` and generated construction values do
/// not participate.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) const fn accepted_insert_field_is_omittable(
    omission_policy: AcceptedInsertOmissionPolicy,
    write_policy: SchemaFieldWritePolicy,
) -> bool {
    !matches!(omission_policy, AcceptedInsertOmissionPolicy::Required)
        || write_policy.insert_generation().is_some()
        || write_policy.write_management().is_some()
}

///
/// AcceptedRowLayoutRuntimeField
///
/// AcceptedRowLayoutRuntimeField is the per-field fact bundle consumed by
/// runtime decode/write boundaries. It borrows persisted schema metadata while
/// freezing the physical slot from `SchemaRowLayout`, which is the accepted
/// row-layout authority.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedRowLayoutRuntimeField<'a> {
    field_id: FieldId,
    name: &'a str,
    slot: SchemaFieldSlot,
    kind: &'a AcceptedFieldKind,
    nested_leaves: &'a [PersistedNestedLeafSnapshot],
    nullable: bool,
    introduced_in_layout: RowLayoutVersion,
    insert_default: &'a SchemaInsertDefault,
    historical_fill: &'a SchemaHistoricalFill,
    write_policy: SchemaFieldWritePolicy,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
    insert_omission_policy: AcceptedInsertOmissionPolicy,
    generated: bool,
}

impl<'a> AcceptedRowLayoutRuntimeField<'a> {
    /// Return the durable accepted field identity.
    #[must_use]
    pub(in crate::db) const fn field_id(&self) -> FieldId {
        self.field_id
    }

    /// Borrow the accepted persisted field name.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &'a str {
        self.name
    }

    /// Return the accepted physical row slot for this field.
    #[must_use]
    pub(in crate::db) const fn slot(&self) -> SchemaFieldSlot {
        self.slot
    }

    /// Borrow the accepted persisted field kind.
    #[cfg(any(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> &'a AcceptedFieldKind {
        self.kind
    }

    /// Return the physical layout that first contained this field.
    #[must_use]
    pub(in crate::db) const fn introduced_in_layout(&self) -> RowLayoutVersion {
        self.introduced_in_layout
    }

    /// Return the accepted future insertion-default contract.
    #[must_use]
    pub(in crate::db) const fn insert_default(&self) -> &'a SchemaInsertDefault {
        self.insert_default
    }

    /// Return the accepted frozen historical-absence contract.
    #[must_use]
    pub(in crate::db) const fn historical_fill(&self) -> &'a SchemaHistoricalFill {
        self.historical_fill
    }

    /// Return the accepted database-level write policy for this field.
    #[must_use]
    pub(in crate::db) const fn write_policy(&self) -> SchemaFieldWritePolicy {
        self.write_policy
    }

    /// Return the accepted insertion-omission policy for this field.
    #[must_use]
    pub(in crate::db) const fn insert_omission_policy(&self) -> AcceptedInsertOmissionPolicy {
        self.insert_omission_policy
    }

    /// Return whether this accepted field is generated-schema owned.
    #[must_use]
    pub(in crate::db) const fn generated(&self) -> bool {
        self.generated
    }

    /// Return the accepted field-level payload decode contract.
    #[must_use]
    pub(in crate::db) const fn decode_contract(&self) -> AcceptedFieldDecodeContract<'a> {
        AcceptedFieldDecodeContract::new(
            self.name,
            self.kind,
            self.nullable,
            self.storage_decode,
            self.leaf_codec,
        )
    }
}

///
/// AcceptedFieldDecodeContract
///
/// AcceptedFieldDecodeContract is the field-level decode shape accepted schema
/// exposes to generated-compatible row-layout checks. It exists so the bridge
/// compares one named contract instead of reopening individual field facts in
/// executor or data decode code.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedFieldDecodeContract<'a> {
    field_name: &'a str,
    kind: &'a AcceptedFieldKind,
    nullable: bool,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
}

impl<'a> AcceptedFieldDecodeContract<'a> {
    /// Build one accepted field-level decode contract from persisted schema
    /// facts selected by the owning schema module.
    #[must_use]
    pub(in crate::db) const fn new(
        field_name: &'a str,
        kind: &'a AcceptedFieldKind,
        nullable: bool,
        storage_decode: FieldStorageDecode,
        leaf_codec: LeafCodec,
    ) -> Self {
        Self {
            field_name,
            kind,
            nullable,
            storage_decode,
            leaf_codec,
        }
    }

    /// Borrow the accepted field name that owns this decode contract.
    #[must_use]
    pub(in crate::db) const fn field_name(&self) -> &'a str {
        self.field_name
    }

    /// Borrow the accepted persisted field kind for decode.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> &'a AcceptedFieldKind {
        self.kind
    }

    /// Return whether this accepted field permits explicit persisted `NULL`.
    #[must_use]
    pub(in crate::db) const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Return the accepted storage decode lane.
    #[must_use]
    pub(in crate::db) const fn storage_decode(&self) -> FieldStorageDecode {
        self.storage_decode
    }

    /// Return the accepted scalar/structural leaf codec.
    #[must_use]
    pub(in crate::db) const fn leaf_codec(&self) -> LeafCodec {
        self.leaf_codec
    }

    /// Return whether this field uses the canonical recursive value wire.
    ///
    /// Schema-native enums and exact composite values use the canonical
    /// recursive wire even when proposal metadata classifies their dispatch
    /// lane as `ByKind`.
    #[must_use]
    pub(in crate::db) fn uses_canonical_value_wire(&self) -> bool {
        self.kind.requires_canonical_value_wire()
            || matches!(self.storage_decode, FieldStorageDecode::CatalogValue)
    }
}

/// Complete accepted field authority for value admission and persistence.
///
/// Schema owns construction and validates the recursive value contract so
/// persistence cannot combine field facts with another store or revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedFieldPersistenceContract<'a> {
    field: AcceptedFieldDecodeContract<'a>,
    admission_contract: AcceptedValueAdmissionContract<'a>,
}

impl<'a> AcceptedFieldPersistenceContract<'a> {
    /// Pair one schema-owned field contract with its accepted catalog.
    pub(in crate::db) fn new(
        value_catalog: &'a AcceptedValueCatalogHandle,
        field: AcceptedFieldDecodeContract<'a>,
    ) -> Result<Self, EnumCatalogBuildError> {
        let value_contract = AcceptedValueContract::from_accepted_field(
            value_catalog,
            field.kind(),
            field.storage_decode(),
        )?;
        Ok(Self {
            field,
            admission_contract: AcceptedValueAdmissionContract::owned(
                value_catalog,
                value_contract,
                field.nullable(),
            ),
        })
    }

    /// Build an explicit paired contract for focused data-layer tests.
    #[cfg(test)]
    pub(in crate::db) fn new_for_tests(
        value_catalog: &'a AcceptedValueCatalogHandle,
        field: AcceptedFieldDecodeContract<'a>,
    ) -> Result<Self, EnumCatalogBuildError> {
        Self::new(value_catalog, field)
    }

    /// Return the field codec facts admitted with this catalog.
    #[must_use]
    pub(in crate::db) const fn field(&self) -> AcceptedFieldDecodeContract<'a> {
        self.field
    }

    /// Borrow the semantic admission authority wrapped by this persistence contract.
    #[must_use]
    pub(in crate::db) const fn admission_contract(&self) -> &AcceptedValueAdmissionContract<'a> {
        &self.admission_contract
    }
}

///
/// OwnedAcceptedFieldDecodeContract
///
/// OwnedAcceptedFieldDecodeContract is the owned form of one accepted
/// field-level decode contract.
/// It exists so runtime row-layout artifacts can carry accepted field
/// contracts beyond the borrow of the schema descriptor that produced them.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct OwnedAcceptedFieldDecodeContract {
    field_name: String,
    kind: AcceptedFieldKind,
    nullable: bool,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
    write_policy: SchemaFieldWritePolicy,
    insert_omission_policy: AcceptedInsertOmissionPolicy,
    introduced_in_layout: RowLayoutVersion,
    insert_default: SchemaInsertDefault,
    historical_fill: SchemaHistoricalFill,
    generated: bool,
}

impl OwnedAcceptedFieldDecodeContract {
    /// Build one owned field contract from a full runtime field descriptor.
    #[must_use]
    fn from_runtime_field(field: &AcceptedRowLayoutRuntimeField<'_>) -> Self {
        let contract = field.decode_contract();

        Self {
            field_name: contract.field_name().to_string(),
            kind: contract.kind().clone(),
            nullable: contract.nullable(),
            storage_decode: contract.storage_decode(),
            leaf_codec: contract.leaf_codec(),
            write_policy: field.write_policy(),
            insert_omission_policy: field.insert_omission_policy(),
            introduced_in_layout: field.introduced_in_layout(),
            insert_default: field.insert_default().clone(),
            historical_fill: field.historical_fill().clone(),
            generated: field.generated(),
        }
    }

    /// Borrow this owned field contract as the accepted decode contract shape.
    #[must_use]
    pub(in crate::db) const fn decode_contract(&self) -> AcceptedFieldDecodeContract<'_> {
        AcceptedFieldDecodeContract::new(
            self.field_name.as_str(),
            &self.kind,
            self.nullable,
            self.storage_decode,
            self.leaf_codec,
        )
    }

    /// Return the accepted insertion-omission behavior for this field.
    #[must_use]
    pub(in crate::db) const fn insert_omission_policy(&self) -> AcceptedInsertOmissionPolicy {
        self.insert_omission_policy
    }

    /// Return the accepted database write policy for this field.
    #[must_use]
    pub(in crate::db) const fn write_policy(&self) -> SchemaFieldWritePolicy {
        self.write_policy
    }

    /// Return the physical layout that first contained this field.
    #[must_use]
    pub(in crate::db) const fn introduced_in_layout(&self) -> RowLayoutVersion {
        self.introduced_in_layout
    }

    /// Borrow the accepted future insertion-default payload contract.
    #[must_use]
    pub(in crate::db) const fn insert_default(&self) -> &SchemaInsertDefault {
        &self.insert_default
    }

    /// Borrow the frozen historical-absence contract.
    #[must_use]
    pub(in crate::db) const fn historical_fill(&self) -> &SchemaHistoricalFill {
        &self.historical_fill
    }

    /// Borrow the accepted persisted field name.
    #[must_use]
    pub(in crate::db) const fn field_name(&self) -> &str {
        self.field_name.as_str()
    }

    /// Borrow the owned accepted persisted field kind.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> &AcceptedFieldKind {
        &self.kind
    }
}

///
/// OwnedAcceptedRelationEdgeContract
///
/// Owned accepted relation-edge metadata carried by row decode contracts.
/// It gives relation runtime paths source-local relation declarations from
/// persisted schema authority instead of rediscovering them by scanning fields.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct OwnedAcceptedRelationEdgeContract {
    name: String,
    physical_generation: u64,
    target_path: String,
    local_field_slots: Vec<usize>,
}

impl OwnedAcceptedRelationEdgeContract {
    fn from_runtime_relation_edge(
        relation: &PersistedRelationEdgeSnapshot,
        fields: &[AcceptedRowLayoutRuntimeField<'_>],
    ) -> Result<Self, InternalError> {
        let mut local_field_slots = Vec::with_capacity(relation.local_field_ids().len());
        for field_id in relation.local_field_ids() {
            let Some(field) = fields.iter().find(|field| field.field_id() == *field_id) else {
                return Err(InternalError::store_invariant());
            };
            local_field_slots.push(usize::from(field.slot().get()));
        }

        Ok(Self {
            name: relation.name().to_string(),
            physical_generation: relation.physical_generation(),
            target_path: relation.target_path().to_string(),
            local_field_slots,
        })
    }

    /// Borrow the accepted relation-edge name.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Return the accepted reverse-index generation for this relation edge.
    #[must_use]
    pub(in crate::db) const fn physical_generation(&self) -> u64 {
        self.physical_generation
    }

    /// Borrow the accepted target entity path.
    #[must_use]
    pub(in crate::db) const fn target_path(&self) -> &str {
        self.target_path.as_str()
    }

    /// Borrow ordered accepted local physical slots for this relation edge.
    #[must_use]
    pub(in crate::db) const fn local_field_slots(&self) -> &[usize] {
        self.local_field_slots.as_slice()
    }
}

///
/// AcceptedRowDecodeContract
///
/// AcceptedRowDecodeContract is the owned, slot-indexed row decode contract
/// projected from accepted schema metadata.
/// It is the handoff object consumed by `RowLayout`: schema owns construction,
/// while data/executor code can read accepted slot contracts without reopening
/// generated `FieldModel` metadata.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedRowDecodeContract {
    current_layout_version: RowLayoutVersion,
    history_floor: RowLayoutVersion,
    required_slot_count: usize,
    primary_key_slot_index: usize,
    primary_key_slot_indices: Vec<usize>,
    fields_by_slot: Vec<Option<OwnedAcceptedFieldDecodeContract>>,
    relation_edges: Vec<OwnedAcceptedRelationEdgeContract>,
    value_catalog: AcceptedValueCatalogHandle,
}

impl AcceptedRowDecodeContract {
    /// Build one accepted row decode contract from runtime contract field facts
    /// and the immutable catalog authority that admitted them.
    fn from_runtime_contract(
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        value_catalog: AcceptedValueCatalogHandle,
    ) -> Self {
        let mut fields_by_slot = vec![None; descriptor.required_slot_count()];

        for field in descriptor.fields() {
            fields_by_slot[usize::from(field.slot().get())] =
                Some(OwnedAcceptedFieldDecodeContract::from_runtime_field(field));
        }

        Self {
            current_layout_version: descriptor.current_layout_version(),
            history_floor: descriptor.history_floor(),
            required_slot_count: descriptor.required_slot_count(),
            primary_key_slot_index: descriptor.first_primary_key_slot_index(),
            primary_key_slot_indices: descriptor.primary_key_slot_indices().to_vec(),
            fields_by_slot,
            relation_edges: descriptor.relation_edges().to_vec(),
            value_catalog,
        }
    }

    /// Return the accepted physical slot count required by this row contract.
    #[must_use]
    pub(in crate::db) const fn required_slot_count(&self) -> usize {
        self.required_slot_count
    }

    /// Return the physical layout stamped by every current canonical writer.
    #[must_use]
    pub(in crate::db) const fn current_layout_version(&self) -> RowLayoutVersion {
        self.current_layout_version
    }

    /// Derive the exact physical slot count for one admitted layout identity.
    pub(in crate::db) fn expected_slot_count(
        &self,
        version: RowLayoutVersion,
    ) -> Result<usize, InternalError> {
        if version < self.history_floor || version > self.current_layout_version {
            return Err(InternalError::persisted_row_layout_outside_accepted_window());
        }

        Ok(self
            .fields_by_slot
            .iter()
            .filter_map(Option::as_ref)
            .filter(|field| field.introduced_in_layout() <= version)
            .count())
    }

    /// Return the accepted primary-key physical slot index.
    #[must_use]
    pub(in crate::db) const fn first_primary_key_slot_index(&self) -> usize {
        self.primary_key_slot_index
    }

    /// Borrow accepted primary-key physical slot indices in key order.
    #[must_use]
    pub(in crate::db) const fn primary_key_slot_indices(&self) -> &[usize] {
        self.primary_key_slot_indices.as_slice()
    }

    /// Borrow accepted relation-edge contracts declared on this source entity.
    #[must_use]
    pub(in crate::db) const fn relation_edges(&self) -> &[OwnedAcceptedRelationEdgeContract] {
        self.relation_edges.as_slice()
    }

    /// Borrow the immutable enum catalog within this row contract's accepted
    /// value catalog.
    #[must_use]
    pub(in crate::db) fn enum_catalog(&self) -> &AcceptedEnumCatalog {
        self.value_catalog.enum_catalog()
    }

    /// Borrow the immutable catalog handle and its store/revision authority.
    #[must_use]
    pub(in crate::db) const fn value_catalog_handle(&self) -> &AcceptedValueCatalogHandle {
        &self.value_catalog
    }

    /// Return the accepted revision that admitted this row contract's catalog.
    #[must_use]
    pub(in crate::db) const fn accepted_schema_revision(&self) -> AcceptedSchemaRevision {
        self.value_catalog.revision()
    }

    /// Borrow one accepted field decode contract by physical row slot.
    #[must_use]
    pub(in crate::db) fn field_for_slot(
        &self,
        slot: usize,
    ) -> Option<&OwnedAcceptedFieldDecodeContract> {
        self.fields_by_slot.get(slot)?.as_ref()
    }

    /// Borrow one accepted field decode contract by physical row slot,
    /// erroring when the selected accepted row contract does not own that slot.
    pub(in crate::db) fn required_field_for_slot(
        &self,
        entity_path: &str,
        slot: usize,
    ) -> Result<&OwnedAcceptedFieldDecodeContract, InternalError> {
        self.field_for_slot(slot).ok_or_else(|| {
            InternalError::persisted_row_slot_lookup_out_of_bounds(entity_path, slot)
        })
    }

    /// Borrow one required field with its immutable catalog authority.
    pub(in crate::db) fn required_field_persistence_contract(
        &self,
        entity_path: &str,
        slot: usize,
    ) -> Result<AcceptedFieldPersistenceContract<'_>, InternalError> {
        let field = self.required_field_for_slot(entity_path, slot)?;
        AcceptedFieldPersistenceContract::new(self.value_catalog_handle(), field.decode_contract())
            .map_err(|_| InternalError::persisted_row_field_encode_internal(field.field_name()))
    }
}

///
/// AcceptedRowLayoutRuntimeContract
///
/// AcceptedRowLayoutRuntimeContract is the schema-owned runtime contract for
/// one accepted row layout. It is intentionally read-only and closed: decode
/// and write code can consume its field facts, but cannot reinterpret raw
/// persisted snapshots or generated model fields to decide slot behavior.
///

#[derive(Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedRowLayoutRuntimeContract<'a> {
    current_layout_version: RowLayoutVersion,
    history_floor: RowLayoutVersion,
    required_slot_count: usize,
    primary_key_names: Vec<&'a str>,
    primary_key_slot_indices: Vec<usize>,
    fields: Vec<AcceptedRowLayoutRuntimeField<'a>>,
    relation_edges: Vec<OwnedAcceptedRelationEdgeContract>,
}

impl<'a> AcceptedRowLayoutRuntimeContract<'a> {
    /// Build one runtime contract from an already accepted schema snapshot.
    ///
    /// The constructor still validates local row-layout completeness because
    /// this contract is a trust boundary for decode/write code. A
    /// missing row-layout slot is reported as an internal invariant violation
    /// rather than hidden behind a partial contract.
    pub(in crate::db) fn from_accepted_schema(
        accepted: &'a AcceptedSchemaSnapshot,
    ) -> Result<Self, InternalError> {
        let snapshot = accepted.persisted_snapshot();
        let row_layout = snapshot.row_layout();
        let mut required_slot_count = 0usize;
        let mut fields = Vec::with_capacity(snapshot.fields().len());

        // Phase 1: project accepted field metadata through the schema-owned
        // row-layout mapping so duplicated field-slot payloads never become
        // the runtime slot authority.
        for field in snapshot.fields() {
            let Some(slot) = row_layout.slot_for_field(field.id()) else {
                return Err(InternalError::store_invariant());
            };
            let slot_end = usize::from(slot.get()).saturating_add(1);
            required_slot_count = required_slot_count.max(slot_end);

            fields.push(AcceptedRowLayoutRuntimeField {
                field_id: field.id(),
                name: field.name(),
                slot,
                kind: field.kind(),
                nested_leaves: field.nested_leaves(),
                nullable: field.nullable(),
                introduced_in_layout: field.introduced_in_layout(),
                insert_default: field.insert_default(),
                historical_fill: field.historical_fill(),
                write_policy: field.write_policy(),
                storage_decode: field.storage_decode(),
                leaf_codec: field.leaf_codec(),
                insert_omission_policy: accepted_insert_omission_policy(
                    field.nullable(),
                    field.insert_default(),
                ),
                generated: field.generated(),
            });
        }
        let mut primary_key_names = Vec::with_capacity(snapshot.primary_key_field_ids().len());
        let mut primary_key_slot_indices =
            Vec::with_capacity(snapshot.primary_key_field_ids().len());
        for primary_key_field_id in snapshot.primary_key_field_ids() {
            let Some(primary_key_field) = fields
                .iter()
                .find(|field| field.field_id() == *primary_key_field_id)
            else {
                return Err(InternalError::store_invariant());
            };
            primary_key_names.push(primary_key_field.name());
            primary_key_slot_indices.push(usize::from(primary_key_field.slot().get()));
        }
        let relation_edges = snapshot
            .relations()
            .iter()
            .map(|relation| {
                OwnedAcceptedRelationEdgeContract::from_runtime_relation_edge(relation, &fields)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            current_layout_version: row_layout.current_version(),
            history_floor: row_layout.history_floor(),
            required_slot_count,
            primary_key_names,
            primary_key_slot_indices,
            fields,
            relation_edges,
        })
    }

    /// Return the current accepted physical row-layout identity.
    #[must_use]
    pub(in crate::db) const fn current_layout_version(&self) -> RowLayoutVersion {
        self.current_layout_version
    }

    /// Return the oldest accepted physical row-layout identity.
    #[must_use]
    pub(in crate::db) const fn history_floor(&self) -> RowLayoutVersion {
        self.history_floor
    }

    /// Return the minimum physical slot count required by this layout.
    #[must_use]
    pub(in crate::db) const fn required_slot_count(&self) -> usize {
        self.required_slot_count
    }

    /// Borrow accepted primary-key field names in key order.
    #[must_use]
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) const fn primary_key_names(&self) -> &[&'a str] {
        self.primary_key_names.as_slice()
    }

    /// Return whether one accepted field name belongs to the primary key.
    #[must_use]
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) fn is_primary_key_field_name(&self, field_name: &str) -> bool {
        self.primary_key_names.contains(&field_name)
    }

    /// Return the first accepted primary-key physical slot index.
    ///
    /// This first-component helper remains for row-decode contracts that still
    /// expose one key slot. Composite-aware code must read
    /// `primary_key_slot_indices`.
    #[must_use]
    pub(in crate::db) fn first_primary_key_slot_index(&self) -> usize {
        self.primary_key_slot_indices[0]
    }

    /// Borrow accepted primary-key physical slot indices in key order.
    #[must_use]
    pub(in crate::db) const fn primary_key_slot_indices(&self) -> &[usize] {
        self.primary_key_slot_indices.as_slice()
    }

    /// Borrow accepted relation-edge contracts for this source entity.
    #[must_use]
    pub(in crate::db) const fn relation_edges(&self) -> &[OwnedAcceptedRelationEdgeContract] {
        self.relation_edges.as_slice()
    }

    /// Borrow runtime field facts in accepted snapshot field order.
    #[must_use]
    pub(in crate::db) const fn fields(&self) -> &[AcceptedRowLayoutRuntimeField<'a>] {
        self.fields.as_slice()
    }

    /// Borrow one runtime field by accepted physical row slot index.
    #[must_use]
    pub(in crate::db) fn field_for_slot_index(
        &self,
        slot: usize,
    ) -> Option<&AcceptedRowLayoutRuntimeField<'a>> {
        self.fields
            .iter()
            .find(|field| usize::from(field.slot().get()) == slot)
    }

    /// Borrow one runtime field by accepted persisted field name.
    #[must_use]
    pub(in crate::db) fn field_by_name(
        &self,
        name: &str,
    ) -> Option<&AcceptedRowLayoutRuntimeField<'a>> {
        self.fields.iter().find(|field| field.name() == name)
    }

    /// Return one runtime field's accepted physical slot index by name.
    #[must_use]
    pub(in crate::db) fn field_slot_index_by_name(&self, name: &str) -> Option<usize> {
        self.field_by_name(name)
            .map(|field| usize::from(field.slot().get()))
    }

    /// Build the owned row-decode contract with immutable catalog authority.
    #[must_use]
    pub(in crate::db) fn row_decode_contract(
        &self,
        value_catalog: AcceptedValueCatalogHandle,
    ) -> AcceptedRowDecodeContract {
        AcceptedRowDecodeContract::from_runtime_contract(self, value_catalog)
    }
}

// Decide the missing-slot behavior from accepted database metadata only. Rust
// struct defaults are deliberately absent from this calculation.
const fn accepted_insert_omission_policy(
    nullable: bool,
    default: &SchemaInsertDefault,
) -> AcceptedInsertOmissionPolicy {
    match (nullable, default) {
        (true, SchemaInsertDefault::None) => AcceptedInsertOmissionPolicy::NullIfMissing,
        (false, SchemaInsertDefault::None) => AcceptedInsertOmissionPolicy::Required,
        (_, SchemaInsertDefault::SlotPayload(_)) => AcceptedInsertOmissionPolicy::DefaultIfMissing,
    }
}
