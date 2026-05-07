//! Module: db::schema::runtime
//! Responsibility: accepted-schema runtime row-layout descriptors.
//! Does not own: raw row decoding, write execution, or transition policy.
//! Boundary: turns accepted metadata into explicit decode/write layout facts.

use crate::{
    db::schema::{
        AcceptedSchemaSnapshot, FieldId, PersistedFieldKind, PersistedNestedLeafSnapshot,
        SchemaFieldDefault, SchemaFieldSlot, SchemaFieldWritePolicy, SchemaVersion,
    },
    error::InternalError,
    model::{
        entity::EntityModel,
        field::{FieldModel, FieldStorageDecode, LeafCodec},
    },
};

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
    #[allow(
        dead_code,
        reason = "nested leaf facts are part of the accepted runtime boundary before nested-path row decode consumes them directly"
    )]
    #[must_use]
    pub(in crate::db) const fn nested_leaves(&self) -> &'a [PersistedNestedLeafSnapshot] {
        self.nested_leaves
    }

    /// Return whether this field permits explicit persisted `NULL`.
    #[allow(
        dead_code,
        reason = "missing-slot nullability is part of the accepted runtime boundary before additive decode support"
    )]
    #[must_use]
    pub(in crate::db) const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Return the accepted database-level default contract.
    #[allow(
        dead_code,
        reason = "database defaults are part of the accepted runtime boundary before additive write support"
    )]
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
    primary_key_slot_index: usize,
    fields_by_slot: Vec<Option<OwnedAcceptedFieldDecodeContract>>,
}

impl AcceptedRowDecodeContract {
    /// Build one accepted row decode contract from descriptor field facts.
    fn from_descriptor(descriptor: &AcceptedRowLayoutRuntimeDescriptor<'_>) -> Self {
        let mut fields_by_slot = vec![None; descriptor.required_slot_count()];

        for field in descriptor.fields() {
            fields_by_slot[usize::from(field.slot().get())] =
                Some(OwnedAcceptedFieldDecodeContract::from_runtime_field(field));
        }

        Self {
            required_slot_count: descriptor.required_slot_count(),
            primary_key_slot_index: descriptor.primary_key_slot_index(),
            fields_by_slot,
        }
    }

    /// Return the accepted physical slot count required by this row contract.
    #[must_use]
    pub(in crate::db) const fn required_slot_count(&self) -> usize {
        self.required_slot_count
    }

    /// Return the accepted primary-key physical slot index.
    #[must_use]
    pub(in crate::db) const fn primary_key_slot_index(&self) -> usize {
        self.primary_key_slot_index
    }

    /// Borrow one accepted field decode contract by physical row slot.
    #[must_use]
    pub(in crate::db) fn field_for_slot(
        &self,
        slot: usize,
    ) -> Option<&OwnedAcceptedFieldDecodeContract> {
        self.fields_by_slot.get(slot)?.as_ref()
    }
}

///
/// AcceptedGeneratedCompatibleRowShape
///
/// AcceptedGeneratedCompatibleRowShape is the schema-runtime proof that one
/// accepted row layout can still be decoded by generated field codecs.
/// Row decode consumes this small shape instead of recombining descriptor
/// fields after compatibility validation has already succeeded.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedGeneratedCompatibleRowShape {
    required_slot_count: usize,
    primary_key_slot_index: usize,
}

impl AcceptedGeneratedCompatibleRowShape {
    /// Return the accepted physical slot count proven generated-compatible.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn required_slot_count(self) -> usize {
        self.required_slot_count
    }

    /// Return the accepted primary-key physical slot proven generated-compatible.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn primary_key_slot_index(self) -> usize {
        self.primary_key_slot_index
    }
}

///
/// AcceptedRowLayoutRuntimeDescriptor
///
/// AcceptedRowLayoutRuntimeDescriptor is the schema-owned runtime contract for
/// one accepted row layout. It is intentionally read-only and closed: decode
/// and write code can consume its field facts, but cannot reinterpret raw
/// persisted snapshots or generated model fields to decide slot behavior.
///

#[derive(Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedRowLayoutRuntimeDescriptor<'a> {
    version: SchemaVersion,
    required_slot_count: usize,
    primary_key_name: &'a str,
    primary_key_kind: &'a PersistedFieldKind,
    primary_key_slot_index: usize,
    fields: Vec<AcceptedRowLayoutRuntimeField<'a>>,
}

impl<'a> AcceptedRowLayoutRuntimeDescriptor<'a> {
    /// Build one runtime descriptor from an already accepted schema snapshot.
    ///
    /// The constructor still validates local row-layout completeness because
    /// this descriptor is a trust boundary for decode/write code. A
    /// missing row-layout slot is reported as an internal invariant violation
    /// rather than hidden behind a partial descriptor.
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
                return Err(InternalError::store_invariant(format!(
                    "accepted row layout runtime descriptor missing slot for field_id={}",
                    field.id().get(),
                )));
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
            });
        }
        let Some(primary_key_field) = fields
            .iter()
            .find(|field| field.field_id() == snapshot.primary_key_field_id())
        else {
            return Err(InternalError::store_invariant(format!(
                "accepted row layout runtime descriptor missing primary-key field_id={}",
                snapshot.primary_key_field_id().get(),
            )));
        };
        let primary_key_name = primary_key_field.name();
        let primary_key_kind = primary_key_field.kind();
        let primary_key_slot_index = usize::from(primary_key_field.slot().get());

        Ok(Self {
            version: row_layout.version(),
            required_slot_count,
            primary_key_name,
            primary_key_kind,
            primary_key_slot_index,
            fields,
        })
    }

    /// Build one descriptor and prove it remains generated-compatible.
    ///
    /// This is the schema-runtime owner for the common accepted-schema handoff
    /// used by write, commit, relation, and row-layout code. Callers receive
    /// both the accepted descriptor and the proof object, so they do not repeat
    /// descriptor construction or forget the generated-compatible guard.
    pub(in crate::db) fn from_generated_compatible_schema(
        accepted: &'a AcceptedSchemaSnapshot,
        model: &'static EntityModel,
    ) -> Result<(Self, AcceptedGeneratedCompatibleRowShape), InternalError> {
        let descriptor = Self::from_accepted_schema(accepted)?;
        let row_shape = descriptor.generated_compatible_row_shape_for_model(model)?;

        Ok((descriptor, row_shape))
    }

    /// Return the accepted schema version backing this runtime layout.
    #[allow(
        dead_code,
        reason = "schema-version reads are reserved for accepted transition plans beyond exact-match"
    )]
    #[must_use]
    pub(in crate::db) const fn version(&self) -> SchemaVersion {
        self.version
    }

    /// Return the minimum physical slot count required by this layout.
    #[must_use]
    pub(in crate::db) const fn required_slot_count(&self) -> usize {
        self.required_slot_count
    }

    /// Borrow the accepted primary-key field name carried by this layout.
    #[must_use]
    pub(in crate::db) const fn primary_key_name(&self) -> &'a str {
        self.primary_key_name
    }

    /// Borrow the accepted primary-key persisted field kind.
    #[must_use]
    pub(in crate::db) const fn primary_key_kind(&self) -> &'a PersistedFieldKind {
        self.primary_key_kind
    }

    /// Return the accepted primary-key physical slot index.
    #[must_use]
    pub(in crate::db) const fn primary_key_slot_index(&self) -> usize {
        self.primary_key_slot_index
    }

    /// Borrow runtime field facts in accepted snapshot field order.
    #[must_use]
    pub(in crate::db) const fn fields(&self) -> &[AcceptedRowLayoutRuntimeField<'a>] {
        self.fields.as_slice()
    }

    /// Borrow one runtime field by accepted physical row slot.
    #[allow(
        dead_code,
        reason = "slot-indexed accepted field lookup becomes live when decode consumes accepted field contracts directly"
    )]
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
    #[allow(
        dead_code,
        reason = "field-id accepted lookup becomes live when migration plans remap durable field identities"
    )]
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
    pub(in crate::db) fn field_kind_by_name(&self, name: &str) -> Option<&PersistedFieldKind> {
        self.field_by_name(name)
            .map(AcceptedRowLayoutRuntimeField::kind)
    }

    /// Build the owned accepted row-decode contract for this descriptor.
    #[must_use]
    pub(in crate::db) fn row_decode_contract(&self) -> AcceptedRowDecodeContract {
        AcceptedRowDecodeContract::from_descriptor(self)
    }

    /// Return the row shape when this accepted layout can still use generated field codecs.
    ///
    /// Accepted-field decoders now own runtime payload interpretation, but
    /// typed materialization still needs proof that the accepted layout can be
    /// bridged back to generated field codecs. Keeping this check and shape
    /// projection in the descriptor owner makes generated compatibility a
    /// schema-runtime contract instead of an executor side calculation.
    pub(in crate::db) fn generated_compatible_row_shape_for_model(
        &self,
        model: &'static EntityModel,
    ) -> Result<AcceptedGeneratedCompatibleRowShape, InternalError> {
        // Phase 1: require primary-key identity and the accepted row shape to
        // match the generated decoder contract.
        if self.primary_key_name() != model.primary_key.name {
            return Err(InternalError::store_invariant(format!(
                "accepted row layout primary key is not generated-compatible: accepted_primary_key='{}' generated_primary_key='{}'",
                self.primary_key_name(),
                model.primary_key.name,
            )));
        }

        // Phase 2: require the accepted row shape to have the same dense slot
        // count the generated decoder expects.
        if self.required_slot_count() != model.fields().len() {
            return Err(InternalError::store_invariant(format!(
                "accepted row layout field count is not generated-compatible: accepted={} generated={}",
                self.required_slot_count(),
                model.fields().len(),
            )));
        }

        // Phase 3: compare every generated field against the accepted
        // descriptor fact used by runtime decode before executor code can
        // consume the descriptor.
        for (generated_slot, field) in model.fields().iter().enumerate() {
            let Some(accepted_field) = self.field_by_name(field.name()) else {
                return Err(InternalError::store_invariant(format!(
                    "accepted row layout missing generated field '{}'",
                    field.name(),
                )));
            };
            let accepted_slot = usize::from(accepted_field.slot().get());
            if accepted_slot != generated_slot {
                return Err(InternalError::store_invariant(format!(
                    "accepted row layout slot is not generated-compatible: field='{}' accepted_slot={} generated_slot={}",
                    field.name(),
                    accepted_slot,
                    generated_slot,
                )));
            }

            ensure_generated_field_decode_contract_compatible(accepted_field, field)?;
        }

        Ok(AcceptedGeneratedCompatibleRowShape {
            required_slot_count: self.required_slot_count(),
            primary_key_slot_index: self.primary_key_slot_index(),
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
        return Err(InternalError::store_invariant(format!(
            "accepted row layout kind is not generated-compatible: field='{}' accepted_kind={:?} generated_kind={:?}",
            accepted_contract.field_name(),
            accepted_contract.kind(),
            generated_kind,
        )));
    }

    if accepted_contract.nullable() != generated_field.nullable() {
        return Err(InternalError::store_invariant(format!(
            "accepted row layout nullability is not generated-compatible: field='{}' accepted_nullable={} generated_nullable={}",
            accepted_contract.field_name(),
            accepted_contract.nullable(),
            generated_field.nullable(),
        )));
    }

    if accepted_contract.storage_decode() != generated_field.storage_decode() {
        return Err(InternalError::store_invariant(format!(
            "accepted row layout storage decode is not generated-compatible: field='{}' accepted_storage_decode={:?} generated_storage_decode={:?}",
            accepted_contract.field_name(),
            accepted_contract.storage_decode(),
            generated_field.storage_decode(),
        )));
    }

    if accepted_contract.leaf_codec() != generated_field.leaf_codec() {
        return Err(InternalError::store_invariant(format!(
            "accepted row layout leaf codec is not generated-compatible: field='{}' accepted_leaf_codec={:?} generated_leaf_codec={:?}",
            accepted_contract.field_name(),
            accepted_contract.leaf_codec(),
            generated_field.leaf_codec(),
        )));
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
mod tests {
    use crate::{
        db::{
            data::{
                decode_runtime_value_from_accepted_field_contract,
                encode_persisted_scalar_slot_payload,
            },
            schema::{
                AcceptedSchemaSnapshot, FieldId, PersistedFieldKind, PersistedFieldSnapshot,
                PersistedSchemaSnapshot, SchemaFieldDefault, SchemaFieldSlot,
                SchemaFieldWritePolicy, SchemaRowLayout, SchemaVersion,
                runtime::{
                    AcceptedFieldAbsencePolicy, AcceptedRowDecodeContract,
                    AcceptedRowLayoutRuntimeDescriptor, AcceptedRowLayoutRuntimeField,
                },
            },
        },
        model::{
            entity::EntityModel,
            field::{
                FieldInsertGeneration, FieldKind, FieldModel, FieldStorageDecode,
                FieldWriteManagement, LeafCodec, ScalarCodec,
            },
            index::IndexModel,
        },
        testing::entity_model_from_static,
        value::Value,
    };

    static RUNTIME_ENTITY_FIELDS: [FieldModel; 2] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated("nickname", FieldKind::Text { max_len: Some(32) }),
    ];
    static RUNTIME_ENTITY_INDEXES: [&IndexModel; 0] = [];
    static RUNTIME_ENTITY_MODEL: EntityModel = entity_model_from_static(
        "schema::tests::RuntimeEntity",
        "RuntimeEntity",
        &RUNTIME_ENTITY_FIELDS[0],
        0,
        &RUNTIME_ENTITY_FIELDS,
        &RUNTIME_ENTITY_INDEXES,
    );

    static WRITE_POLICY_ENTITY_FIELDS: [FieldModel; 3] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated_with_storage_decode_nullability_and_write_policies(
            "token",
            FieldKind::Ulid,
            FieldStorageDecode::ByKind,
            false,
            Some(FieldInsertGeneration::Ulid),
            None,
        ),
        FieldModel::generated_with_storage_decode_nullability_and_write_policies(
            "updated_at",
            FieldKind::Timestamp,
            FieldStorageDecode::ByKind,
            false,
            None,
            Some(FieldWriteManagement::UpdatedAt),
        ),
    ];
    static WRITE_POLICY_ENTITY_INDEXES: [&IndexModel; 0] = [];
    static WRITE_POLICY_ENTITY_MODEL: EntityModel = entity_model_from_static(
        "schema::tests::WritePolicyEntity",
        "WritePolicyEntity",
        &WRITE_POLICY_ENTITY_FIELDS[0],
        0,
        &WRITE_POLICY_ENTITY_FIELDS,
        &WRITE_POLICY_ENTITY_INDEXES,
    );

    fn accepted_schema_fixture() -> AcceptedSchemaSnapshot {
        AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "schema::tests::RuntimeEntity".to_string(),
            "RuntimeEntity".to_string(),
            FieldId::new(1),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(9)),
                ],
            ),
            vec![
                PersistedFieldSnapshot::new(
                    FieldId::new(1),
                    "id".to_string(),
                    SchemaFieldSlot::new(0),
                    PersistedFieldKind::Ulid,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Ulid),
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "nickname".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Text { max_len: Some(32) },
                    Vec::new(),
                    true,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Text),
                ),
            ],
        ))
    }

    fn generated_compatible_accepted_schema_fixture() -> AcceptedSchemaSnapshot {
        AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "schema::tests::RuntimeEntity".to_string(),
            "RuntimeEntity".to_string(),
            FieldId::new(1),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                ],
            ),
            vec![
                PersistedFieldSnapshot::new(
                    FieldId::new(1),
                    "id".to_string(),
                    SchemaFieldSlot::new(0),
                    PersistedFieldKind::Ulid,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Ulid),
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "nickname".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Text { max_len: Some(32) },
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Text),
                ),
            ],
        ))
    }

    fn generated_slot_compatible_accepted_schema_with_nickname_decode(
        nullable: bool,
        storage_decode: FieldStorageDecode,
        leaf_codec: LeafCodec,
    ) -> AcceptedSchemaSnapshot {
        AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "schema::tests::RuntimeEntity".to_string(),
            "RuntimeEntity".to_string(),
            FieldId::new(1),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                ],
            ),
            vec![
                PersistedFieldSnapshot::new(
                    FieldId::new(1),
                    "id".to_string(),
                    SchemaFieldSlot::new(0),
                    PersistedFieldKind::Ulid,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Ulid),
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "nickname".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Text { max_len: Some(32) },
                    Vec::new(),
                    nullable,
                    SchemaFieldDefault::None,
                    storage_decode,
                    leaf_codec,
                ),
            ],
        ))
    }

    fn write_policy_accepted_schema_fixture() -> AcceptedSchemaSnapshot {
        AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "schema::tests::WritePolicyEntity".to_string(),
            "WritePolicyEntity".to_string(),
            FieldId::new(1),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                    (FieldId::new(3), SchemaFieldSlot::new(2)),
                ],
            ),
            vec![
                PersistedFieldSnapshot::new(
                    FieldId::new(1),
                    "id".to_string(),
                    SchemaFieldSlot::new(0),
                    PersistedFieldKind::Ulid,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Ulid),
                ),
                PersistedFieldSnapshot::new_with_write_policy(
                    FieldId::new(2),
                    "token".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Ulid,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    SchemaFieldWritePolicy::from_model_policies(
                        Some(FieldInsertGeneration::Ulid),
                        None,
                    ),
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Ulid),
                ),
                PersistedFieldSnapshot::new_with_write_policy(
                    FieldId::new(3),
                    "updated_at".to_string(),
                    SchemaFieldSlot::new(2),
                    PersistedFieldKind::Timestamp,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    SchemaFieldWritePolicy::from_model_policies(
                        None,
                        Some(FieldWriteManagement::UpdatedAt),
                    ),
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Timestamp),
                ),
            ],
        ))
    }

    #[test]
    fn accepted_row_layout_runtime_descriptor_uses_row_layout_slot_authority() {
        let accepted = accepted_schema_fixture();
        let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
            .expect("accepted runtime descriptor should build");

        assert_eq!(descriptor.version(), SchemaVersion::initial());
        assert_eq!(descriptor.required_slot_count(), 10);
        assert_eq!(descriptor.primary_key_name(), "id");
        assert_eq!(descriptor.primary_key_slot_index(), 0);
        assert_eq!(descriptor.fields().len(), 2);

        let nickname = descriptor
            .fields()
            .iter()
            .find(|field| field.name() == "nickname")
            .expect("nickname field should be present");
        assert_eq!(nickname.field_id(), FieldId::new(2));
        assert_eq!(nickname.slot(), SchemaFieldSlot::new(9));
        assert_eq!(
            nickname.absence_policy(),
            AcceptedFieldAbsencePolicy::NullIfMissing
        );
        assert_eq!(nickname.default(), &SchemaFieldDefault::None);
        let nickname_decode_contract = nickname.decode_contract();
        assert!(nickname_decode_contract.nullable());
        assert_eq!(
            nickname_decode_contract.storage_decode(),
            FieldStorageDecode::ByKind,
        );
        assert_eq!(
            nickname_decode_contract.leaf_codec(),
            LeafCodec::Scalar(ScalarCodec::Text),
        );
        assert!(matches!(
            nickname.kind(),
            PersistedFieldKind::Text { max_len: Some(32) },
        ));
        assert_eq!(
            descriptor
                .field_for_slot(SchemaFieldSlot::new(9))
                .expect("nickname should be indexed by accepted slot")
                .name(),
            "nickname",
        );
        assert_eq!(
            descriptor
                .field_for_id(FieldId::new(2))
                .expect("nickname should be indexed by durable field ID")
                .slot(),
            SchemaFieldSlot::new(9),
        );
        assert_eq!(
            descriptor
                .field_by_name("nickname")
                .expect("nickname should be indexed by persisted field name")
                .field_id(),
            FieldId::new(2),
        );
        assert_eq!(descriptor.field_slot_index_by_name("nickname"), Some(9));
        assert!(matches!(
            descriptor.field_kind_by_name("nickname"),
            Some(PersistedFieldKind::Text { max_len: Some(32) }),
        ));
        assert!(nickname.nested_leaves().is_empty());
        assert!(nickname.nullable());
    }

    #[test]
    fn accepted_row_decode_contract_owns_slot_indexed_field_contracts() {
        let accepted = accepted_schema_fixture();
        let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
            .expect("accepted runtime descriptor should build");
        let contract = descriptor.row_decode_contract();
        let nickname = contract
            .field_for_slot(9)
            .expect("nickname field should be available by accepted row slot");

        assert_eq!(contract.required_slot_count(), 10);
        assert_eq!(contract.primary_key_slot_index(), 0);
        assert_eq!(nickname.field_name(), "nickname");
        assert!(
            contract.field_for_slot(1).is_none(),
            "accepted row decode contract should preserve row-layout gaps"
        );
        assert!(matches!(
            nickname.kind(),
            PersistedFieldKind::Text { max_len: Some(32) },
        ));
    }

    #[test]
    fn accepted_row_decode_contract_survives_descriptor_borrow_scope() {
        let contract: AcceptedRowDecodeContract = {
            let accepted = generated_compatible_accepted_schema_fixture();
            let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
                .expect("accepted runtime descriptor should build");

            descriptor.row_decode_contract()
        };
        let nickname_field = contract
            .field_for_slot(1)
            .expect("nickname field should survive as owned accepted contract");
        let raw_value = encode_persisted_scalar_slot_payload(&"Ada".to_string(), "nickname")
            .expect("owned accepted scalar fixture should encode");

        let value = decode_runtime_value_from_accepted_field_contract(
            nickname_field.decode_contract(),
            raw_value.as_slice(),
        )
        .expect("owned accepted field contract should decode outside descriptor borrow scope");

        assert_eq!(value, Value::Text("Ada".to_string()));
    }

    #[test]
    fn accepted_row_layout_runtime_descriptor_projects_generated_compatible_shape() {
        let accepted = generated_compatible_accepted_schema_fixture();
        let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
            .expect("generated-compatible schema should build descriptor");

        let shape = descriptor
            .generated_compatible_row_shape_for_model(&RUNTIME_ENTITY_MODEL)
            .expect("matching generated model should produce row shape proof");

        assert_eq!(shape.required_slot_count(), 2);
        assert_eq!(shape.primary_key_slot_index(), 0);
    }

    #[test]
    fn accepted_row_layout_runtime_descriptor_builds_descriptor_and_row_shape_proof() {
        let accepted = generated_compatible_accepted_schema_fixture();
        let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
            .expect("accepted schema should build descriptor");
        let shape = descriptor
            .generated_compatible_row_shape_for_model(&RUNTIME_ENTITY_MODEL)
            .expect("generated-compatible schema should build row shape proof");

        assert_eq!(descriptor.required_slot_count(), 2);
        assert_eq!(descriptor.primary_key_slot_index(), 0);
        assert_eq!(descriptor.primary_key_name(), "id");
        assert_eq!(descriptor.primary_key_kind(), &PersistedFieldKind::Ulid);
        assert_eq!(shape.required_slot_count(), 2);
        assert_eq!(shape.primary_key_slot_index(), 0);
        assert_eq!(
            descriptor.field_slot_index_by_name("nickname"),
            Some(1),
            "checked descriptor should retain accepted field lookup facts",
        );
        assert_eq!(
            descriptor
                .field_for_slot_index(1)
                .map(AcceptedRowLayoutRuntimeField::name),
            Some("nickname"),
            "checked descriptor should resolve accepted physical slots by index",
        );
        let nickname_field = descriptor
            .field_by_name("nickname")
            .expect("nickname should resolve accepted descriptor field");
        assert_eq!(
            nickname_field.write_policy().insert_generation(),
            None,
            "generated-compatible descriptor should project accepted fields to write-policy facts",
        );
    }

    #[test]
    fn accepted_field_decode_contract_reports_persisted_scalar_field_name() {
        let accepted = generated_compatible_accepted_schema_fixture();
        let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
            .expect("accepted schema should build descriptor");
        let nickname_field = descriptor
            .field_by_name("nickname")
            .expect("nickname should resolve accepted descriptor field");

        // Invalid UTF-8 inside a scalar text envelope should be attributed to
        // the accepted persisted field name, not to a generated placeholder.
        let invalid_text_scalar_payload = [0xFF, 0x01, 0xFF];
        let err = decode_runtime_value_from_accepted_field_contract(
            nickname_field.decode_contract(),
            invalid_text_scalar_payload.as_slice(),
        )
        .expect_err("invalid accepted scalar payload should fail closed");

        assert!(
            err.message.contains("field 'nickname'"),
            "accepted scalar decode should retain field ownership in diagnostics: {}",
            err.message,
        );
        assert!(
            !err.message.contains("accepted field"),
            "accepted scalar decode should not use the old placeholder field name: {}",
            err.message,
        );
    }

    #[test]
    fn accepted_row_layout_runtime_descriptor_projects_persisted_write_policy() {
        let accepted = write_policy_accepted_schema_fixture();
        let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
            .expect("write-policy accepted schema should build descriptor");
        descriptor
            .generated_compatible_row_shape_for_model(&WRITE_POLICY_ENTITY_MODEL)
            .expect("write-policy schema should remain generated-compatible");

        let token_field = descriptor
            .field_by_name("token")
            .expect("token should resolve accepted descriptor field");
        let token_policy = token_field.write_policy();
        assert_eq!(
            token_policy.insert_generation(),
            Some(FieldInsertGeneration::Ulid)
        );
        assert_eq!(token_policy.write_management(), None);

        let updated_at_field = descriptor
            .field_by_name("updated_at")
            .expect("updated_at should resolve accepted descriptor field");
        let updated_at_policy_from_field = updated_at_field.write_policy();
        assert_eq!(
            updated_at_policy_from_field.write_management(),
            Some(FieldWriteManagement::UpdatedAt),
            "descriptor-owned field projection should avoid name re-resolution",
        );
    }

    #[test]
    fn accepted_row_layout_runtime_descriptor_rejects_non_generated_compatible_shape() {
        let accepted = accepted_schema_fixture();
        let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
            .expect("slot-expanded accepted schema should build descriptor");

        let err = descriptor
            .generated_compatible_row_shape_for_model(&RUNTIME_ENTITY_MODEL)
            .expect_err("slot-expanded schema must not produce generated-compatible shape proof");

        assert!(
            err.message
                .contains("accepted row layout field count is not generated-compatible"),
            "unexpected generated-compatible shape error: {}",
            err.message,
        );
    }

    #[test]
    fn accepted_row_layout_runtime_descriptor_rejects_storage_decode_drift() {
        let accepted = generated_slot_compatible_accepted_schema_with_nickname_decode(
            false,
            FieldStorageDecode::Value,
            LeafCodec::Scalar(ScalarCodec::Text),
        );
        let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
            .expect("slot-compatible accepted schema should build descriptor");

        let err = descriptor
            .generated_compatible_row_shape_for_model(&RUNTIME_ENTITY_MODEL)
            .expect_err("storage decode drift must reject generated decoder bridge");

        assert!(
            err.message
                .contains("accepted row layout storage decode is not generated-compatible"),
            "unexpected generated-compatible storage decode error: {}",
            err.message,
        );
    }

    #[test]
    fn accepted_row_layout_runtime_descriptor_rejects_leaf_codec_drift() {
        let accepted = generated_slot_compatible_accepted_schema_with_nickname_decode(
            false,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Blob),
        );
        let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
            .expect("slot-compatible accepted schema should build descriptor");

        let err = descriptor
            .generated_compatible_row_shape_for_model(&RUNTIME_ENTITY_MODEL)
            .expect_err("leaf codec drift must reject generated decoder bridge");

        assert!(
            err.message
                .contains("accepted row layout leaf codec is not generated-compatible"),
            "unexpected generated-compatible leaf codec error: {}",
            err.message,
        );
    }

    #[test]
    fn accepted_row_layout_runtime_descriptor_rejects_nullability_drift() {
        let accepted = generated_slot_compatible_accepted_schema_with_nickname_decode(
            true,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Text),
        );
        let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
            .expect("slot-compatible accepted schema should build descriptor");

        let err = descriptor
            .generated_compatible_row_shape_for_model(&RUNTIME_ENTITY_MODEL)
            .expect_err("nullability drift must reject generated decoder bridge");

        assert!(
            err.message
                .contains("accepted row layout nullability is not generated-compatible"),
            "unexpected generated-compatible nullability error: {}",
            err.message,
        );
    }

    #[test]
    fn accepted_row_layout_runtime_descriptor_rejects_missing_layout_slot() {
        let accepted = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "schema::tests::BrokenEntity".to_string(),
            "BrokenEntity".to_string(),
            FieldId::new(1),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![(FieldId::new(1), SchemaFieldSlot::new(0))],
            ),
            vec![
                PersistedFieldSnapshot::new(
                    FieldId::new(1),
                    "id".to_string(),
                    SchemaFieldSlot::new(0),
                    PersistedFieldKind::Ulid,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Ulid),
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "nickname".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Text { max_len: None },
                    Vec::new(),
                    true,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Text),
                ),
            ],
        ));

        let err = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
            .expect_err("missing row-layout slot should fail closed");
        assert!(
            err.to_string().contains("missing slot for field_id=2"),
            "unexpected descriptor error: {err}",
        );
    }
}
