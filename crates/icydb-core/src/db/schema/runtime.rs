//! Module: db::schema::runtime
//! Responsibility: accepted-schema runtime row-layout descriptors.
//! Does not own: raw row decoding, write execution, or transition policy.
//! Boundary: turns accepted metadata into explicit decode/write layout facts.

use crate::{
    db::schema::{
        AcceptedSchemaSnapshot, FieldId, PersistedFieldKind, PersistedNestedLeafSnapshot,
        PersistedRelationEdgeSnapshot, SchemaFieldDefault, SchemaFieldSlot, SchemaFieldWritePolicy,
        SchemaVersion,
    },
    error::InternalError,
    model::{
        entity::EntityModel,
        field::{FieldModel, FieldStorageDecode, LeafCodec},
    },
};
#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
thread_local! {
    static GENERATED_COMPATIBLE_ROW_LAYOUT_PROOFS: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
pub(in crate::db) fn reset_generated_compatible_row_layout_proof_count_for_tests() {
    GENERATED_COMPATIBLE_ROW_LAYOUT_PROOFS.with(|proofs| proofs.set(0));
}

#[cfg(test)]
pub(in crate::db) fn generated_compatible_row_layout_proof_count_for_tests() -> u64 {
    GENERATED_COMPATIBLE_ROW_LAYOUT_PROOFS.with(Cell::get)
}

///
/// AcceptedFieldAbsencePolicy
///
/// AcceptedFieldAbsencePolicy describes how runtime row materialization should
/// treat a missing physical payload slot for one accepted field. It exists so
/// additive-field support has an explicit schema-owned contract instead of
/// asking row decode code to infer missing-field behavior from generated
/// nullable flags or Rust defaults.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedFieldAbsencePolicy {
    NullIfMissing,
    DefaultIfMissing,
    Required,
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
    kind: &'a PersistedFieldKind,
    nested_leaves: &'a [PersistedNestedLeafSnapshot],
    nullable: bool,
    default: &'a SchemaFieldDefault,
    write_policy: SchemaFieldWritePolicy,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
    absence_policy: AcceptedFieldAbsencePolicy,
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
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> &'a PersistedFieldKind {
        self.kind
    }

    /// Borrow accepted nested leaf metadata rooted at this field.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn nested_leaves(&self) -> &'a [PersistedNestedLeafSnapshot] {
        self.nested_leaves
    }

    /// Return whether this field permits explicit persisted `NULL`.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Return the accepted database-level default contract.
    #[must_use]
    pub(in crate::db) const fn default(&self) -> &'a SchemaFieldDefault {
        self.default
    }

    /// Return the accepted database-level write policy for this field.
    #[must_use]
    pub(in crate::db) const fn write_policy(&self) -> SchemaFieldWritePolicy {
        self.write_policy
    }

    /// Return the accepted missing-slot policy for this field.
    #[must_use]
    pub(in crate::db) const fn absence_policy(&self) -> AcceptedFieldAbsencePolicy {
        self.absence_policy
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
    kind: &'a PersistedFieldKind,
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
        kind: &'a PersistedFieldKind,
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
    pub(in crate::db) const fn kind(&self) -> &'a PersistedFieldKind {
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
    kind: PersistedFieldKind,
    nullable: bool,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
    write_policy: SchemaFieldWritePolicy,
    absence_policy: AcceptedFieldAbsencePolicy,
    default: SchemaFieldDefault,
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
            absence_policy: field.absence_policy(),
            default: field.default().clone(),
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

    /// Return the accepted missing-slot behavior for this field.
    #[must_use]
    pub(in crate::db) const fn absence_policy(&self) -> AcceptedFieldAbsencePolicy {
        self.absence_policy
    }

    /// Return the accepted database write policy for this field.
    #[must_use]
    pub(in crate::db) const fn write_policy(&self) -> SchemaFieldWritePolicy {
        self.write_policy
    }

    /// Borrow the accepted database default payload contract.
    #[must_use]
    pub(in crate::db) const fn default(&self) -> &SchemaFieldDefault {
        &self.default
    }

    /// Borrow the accepted persisted field name.
    #[must_use]
    pub(in crate::db) const fn field_name(&self) -> &str {
        self.field_name.as_str()
    }

    /// Borrow the owned accepted persisted field kind.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> &PersistedFieldKind {
        &self.kind
    }

    /// Return whether this accepted field is generated-schema owned.
    #[must_use]
    pub(in crate::db) const fn generated(&self) -> bool {
        self.generated
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
            target_path: relation.target_path().to_string(),
            local_field_slots,
        })
    }

    /// Borrow the accepted relation-edge name.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &str {
        self.name.as_str()
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
    required_slot_count: usize,
    max_physical_slot_count: usize,
    primary_key_slot_index: usize,
    primary_key_slot_indices: Vec<usize>,
    fields_by_slot: Vec<Option<OwnedAcceptedFieldDecodeContract>>,
    relation_edges: Vec<OwnedAcceptedRelationEdgeContract>,
}

impl AcceptedRowDecodeContract {
    /// Build one accepted row decode contract from runtime contract field facts.
    fn from_runtime_contract(descriptor: &AcceptedRowLayoutRuntimeContract<'_>) -> Self {
        let mut fields_by_slot = vec![None; descriptor.required_slot_count()];

        for field in descriptor.fields() {
            fields_by_slot[usize::from(field.slot().get())] =
                Some(OwnedAcceptedFieldDecodeContract::from_runtime_field(field));
        }

        Self {
            required_slot_count: descriptor.required_slot_count(),
            max_physical_slot_count: descriptor.max_physical_slot_count(),
            primary_key_slot_index: descriptor.first_primary_key_slot_index(),
            primary_key_slot_indices: descriptor.primary_key_slot_indices().to_vec(),
            fields_by_slot,
            relation_edges: descriptor.relation_edges().to_vec(),
        }
    }

    /// Build a generated-compatible accepted row contract for executor tests.
    ///
    /// Production code must source this contract from the accepted schema store.
    /// This helper exists only so low-level executor tests can keep exercising
    /// save mechanics without bootstrapping a session/schema store around every
    /// fixture.
    #[cfg(test)]
    pub(in crate::db) fn from_generated_model_for_tests(model: &'static EntityModel) -> Self {
        let proposal = crate::db::schema::compiled_schema_proposal_for_model(model);
        let accepted =
            AcceptedSchemaSnapshot::try_new(proposal.initial_persisted_schema_snapshot())
                .expect("generated model proposal should produce an accepted test schema");
        let (descriptor, _) =
            AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(&accepted, model)
                .expect("generated model accepted test schema should be generated-compatible");

        descriptor.row_decode_contract()
    }

    /// Return the accepted physical slot count required by this row contract.
    #[must_use]
    pub(in crate::db) const fn required_slot_count(&self) -> usize {
        self.required_slot_count
    }

    /// Return the maximum physical row slot count accepted for older rows.
    #[must_use]
    pub(in crate::db) const fn max_physical_slot_count(&self) -> usize {
        self.max_physical_slot_count
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
}

///
/// AcceptedGeneratedRowCompatibilityProof
///
/// AcceptedGeneratedRowCompatibilityProof is the schema-runtime proof that one
/// accepted row layout can still be decoded by generated field codecs.
/// Row decode consumes this small proof instead of recombining descriptor
/// fields after compatibility validation has already succeeded.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedGeneratedRowCompatibilityProof {
    required_slot_count: usize,
    primary_key_slot_index: usize,
}

impl AcceptedGeneratedRowCompatibilityProof {
    /// Return the accepted physical slot count proven generated-compatible.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn required_slot_count(self) -> usize {
        self.required_slot_count
    }

    /// Return the accepted primary-key physical slot proven generated-compatible.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn first_primary_key_slot_index(self) -> usize {
        self.primary_key_slot_index
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
    version: SchemaVersion,
    required_slot_count: usize,
    max_physical_slot_count: usize,
    primary_key_names: Vec<&'a str>,
    primary_key_kinds: Vec<&'a PersistedFieldKind>,
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
                default: field.default(),
                write_policy: field.write_policy(),
                storage_decode: field.storage_decode(),
                leaf_codec: field.leaf_codec(),
                absence_policy: accepted_field_absence_policy(field.nullable(), field.default()),
                generated: field.generated(),
            });
        }
        let mut primary_key_names = Vec::with_capacity(snapshot.primary_key_field_ids().len());
        let mut primary_key_kinds = Vec::with_capacity(snapshot.primary_key_field_ids().len());
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
            primary_key_kinds.push(primary_key_field.kind());
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
            version: row_layout.version(),
            required_slot_count,
            max_physical_slot_count: row_layout.allocated_slot_count().max(required_slot_count),
            primary_key_names,
            primary_key_kinds,
            primary_key_slot_indices,
            fields,
            relation_edges,
        })
    }

    /// Build one descriptor and prove it remains generated-compatible.
    ///
    /// This is the schema-runtime owner for the common accepted-schema handoff
    /// used by write, commit, relation, and row-layout code. Callers receive
    /// both the accepted contract and the proof object, so they do not repeat
    /// contract construction or forget the generated-compatible guard.
    pub(in crate::db) fn from_generated_compatible_schema(
        accepted: &'a AcceptedSchemaSnapshot,
        model: &'static EntityModel,
    ) -> Result<(Self, AcceptedGeneratedRowCompatibilityProof), InternalError> {
        #[cfg(test)]
        GENERATED_COMPATIBLE_ROW_LAYOUT_PROOFS
            .with(|proofs| proofs.set(proofs.get().saturating_add(1)));

        let descriptor = Self::from_accepted_schema(accepted)?;
        let row_proof = descriptor.generated_row_compatibility_proof_for_model(model)?;

        Ok((descriptor, row_proof))
    }

    /// Return the accepted schema version backing this runtime layout.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn version(&self) -> SchemaVersion {
        self.version
    }

    /// Return the minimum physical slot count required by this layout.
    #[must_use]
    pub(in crate::db) const fn required_slot_count(&self) -> usize {
        self.required_slot_count
    }

    /// Return the maximum physical row slot count tolerated for older rows.
    #[must_use]
    pub(in crate::db) const fn max_physical_slot_count(&self) -> usize {
        self.max_physical_slot_count
    }

    /// Borrow accepted primary-key field names in key order.
    #[must_use]
    pub(in crate::db) const fn primary_key_names(&self) -> &[&'a str] {
        self.primary_key_names.as_slice()
    }

    /// Return whether one accepted field name belongs to the primary key.
    #[must_use]
    pub(in crate::db) fn is_primary_key_field_name(&self, field_name: &str) -> bool {
        self.primary_key_names.contains(&field_name)
    }

    /// Borrow the first accepted primary-key persisted field kind.
    ///
    /// This first-component helper remains for scalar-only SQL literal
    /// coercion paths. Composite-aware code must read `primary_key_kinds`.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) fn first_primary_key_kind(&self) -> &'a PersistedFieldKind {
        self.primary_key_kinds[0]
    }

    /// Borrow accepted primary-key persisted field kinds in key order.
    #[cfg(any(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db) const fn primary_key_kinds(&self) -> &[&'a PersistedFieldKind] {
        self.primary_key_kinds.as_slice()
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

    /// Borrow one runtime field by accepted physical row slot.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn field_for_slot(
        &self,
        slot: SchemaFieldSlot,
    ) -> Option<&AcceptedRowLayoutRuntimeField<'a>> {
        self.fields.iter().find(|field| field.slot() == slot)
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

    /// Borrow one runtime field by durable accepted field identity.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn field_for_id(
        &self,
        field_id: FieldId,
    ) -> Option<&AcceptedRowLayoutRuntimeField<'a>> {
        self.fields
            .iter()
            .find(|field| field.field_id() == field_id)
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

    /// Borrow one runtime field's accepted persisted kind by name.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) fn field_kind_by_name(&self, name: &str) -> Option<&PersistedFieldKind> {
        self.field_by_name(name)
            .map(AcceptedRowLayoutRuntimeField::kind)
    }

    /// Build the owned accepted row-decode contract for this contract.
    #[must_use]
    pub(in crate::db) fn row_decode_contract(&self) -> AcceptedRowDecodeContract {
        AcceptedRowDecodeContract::from_runtime_contract(self)
    }

    /// Return the proof that this accepted layout can still use generated field codecs.
    ///
    /// Accepted-field decoders now own runtime payload interpretation, but
    /// typed materialization still needs proof that the accepted layout can be
    /// bridged back to generated field codecs. Keeping this compatibility
    /// proof in the contract owner makes generated compatibility a
    /// schema-runtime contract instead of an executor side calculation.
    pub(in crate::db) fn generated_row_compatibility_proof_for_model(
        &self,
        model: &'static EntityModel,
    ) -> Result<AcceptedGeneratedRowCompatibilityProof, InternalError> {
        // Phase 1: require primary-key identity and the accepted row layout to
        // match the generated decoder contract.
        let generated_primary_key_names = model
            .primary_key_model()
            .fields()
            .iter()
            .map(FieldModel::name)
            .collect::<Vec<_>>();
        if self.primary_key_names() != generated_primary_key_names.as_slice() {
            return Err(InternalError::store_invariant());
        }

        // Phase 2: require the accepted row layout to cover every generated
        // slot. Extra trailing DDL-owned slots may exist after SQL ADD COLUMN;
        // they remain accepted-runtime fields and are not exposed through the
        // generated typed materializer.
        if self.required_slot_count() < model.fields().len() {
            return Err(InternalError::store_invariant());
        }

        // Phase 3: compare every generated field against the accepted
        // contract fact used by runtime decode before executor code can
        // consume the descriptor.
        for (generated_slot, field) in model.fields().iter().enumerate() {
            let Some(accepted_field) = self.field_by_name(field.name()) else {
                return Err(InternalError::store_invariant());
            };
            let accepted_slot = usize::from(accepted_field.slot().get());
            if accepted_slot != generated_slot {
                return Err(InternalError::store_invariant());
            }

            ensure_generated_field_decode_contract_compatible(accepted_field, field)?;
        }

        for slot in model.fields().len()..self.required_slot_count() {
            let Some(extra_field) = self.field_for_slot_index(slot) else {
                continue;
            };
            if extra_field.generated() {
                return Err(InternalError::store_invariant());
            }
            if matches!(
                extra_field.absence_policy(),
                AcceptedFieldAbsencePolicy::Required
            ) {
                return Err(InternalError::store_invariant());
            }
        }

        Ok(AcceptedGeneratedRowCompatibilityProof {
            required_slot_count: self.required_slot_count(),
            primary_key_slot_index: self.first_primary_key_slot_index(),
        })
    }
}

// Prove that one accepted field still has the exact decode contract expected by
// its generated field codec. This is the field-level bridge that lets typed
// materialization keep using generated decoders after accepted runtime decode
// has already proven the persisted field contract.
fn ensure_generated_field_decode_contract_compatible(
    accepted_field: &AcceptedRowLayoutRuntimeField<'_>,
    generated_field: &FieldModel,
) -> Result<(), InternalError> {
    let accepted_contract = accepted_field.decode_contract();
    let generated_kind = PersistedFieldKind::from_model_kind(generated_field.kind());
    if accepted_contract.kind() != &generated_kind {
        return Err(InternalError::store_invariant());
    }

    if accepted_contract.nullable() != generated_field.nullable() {
        return Err(InternalError::store_invariant());
    }

    if accepted_contract.storage_decode() != generated_field.storage_decode() {
        return Err(InternalError::store_invariant());
    }

    if accepted_contract.leaf_codec() != generated_field.leaf_codec() {
        return Err(InternalError::store_invariant());
    }

    Ok(())
}

// Decide the missing-slot behavior from accepted database metadata only. Rust
// struct defaults are deliberately absent from this calculation.
const fn accepted_field_absence_policy(
    nullable: bool,
    default: &SchemaFieldDefault,
) -> AcceptedFieldAbsencePolicy {
    match (nullable, default) {
        (true, SchemaFieldDefault::None) => AcceptedFieldAbsencePolicy::NullIfMissing,
        (false, SchemaFieldDefault::None) => AcceptedFieldAbsencePolicy::Required,
        (_, SchemaFieldDefault::SlotPayload(_)) => AcceptedFieldAbsencePolicy::DefaultIfMissing,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
