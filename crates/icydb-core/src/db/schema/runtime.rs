//! Module: db::schema::runtime
//! Responsibility: accepted-schema runtime row-layout descriptors.
//! Does not own: raw row decoding, write execution, or transition policy.
//! Boundary: turns accepted metadata into explicit decode/write layout facts.

use crate::{
    db::schema::{
        AcceptedSchemaSnapshot, FieldId, PersistedFieldKind, PersistedNestedLeafSnapshot,
        SchemaFieldDefault, SchemaFieldSlot, SchemaVersion,
    },
    error::InternalError,
    model::{
        entity::EntityModel,
        field::{FieldStorageDecode, LeafCodec},
    },
};

///
/// AcceptedFieldAbsencePolicy
///
/// AcceptedFieldAbsencePolicy describes how runtime row materialization should
/// treat a missing physical payload slot for one accepted field. It exists so
/// future additive-field support has an explicit schema-owned contract instead
/// of asking row decode code to infer missing-field behavior from nullable
/// flags or Rust defaults.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedFieldAbsencePolicy {
    NullIfMissing,
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
    default: SchemaFieldDefault,
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
        reason = "nested leaf facts are part of the accepted runtime boundary before row decode consumes them directly"
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
    pub(in crate::db) const fn default(&self) -> SchemaFieldDefault {
        self.default
    }

    /// Return the accepted payload decode contract.
    #[must_use]
    pub(in crate::db) const fn storage_decode(&self) -> FieldStorageDecode {
        self.storage_decode
    }

    /// Return the accepted leaf codec contract.
    #[must_use]
    pub(in crate::db) const fn leaf_codec(&self) -> LeafCodec {
        self.leaf_codec
    }

    /// Return the accepted missing-slot policy for this field.
    #[must_use]
    pub(in crate::db) const fn absence_policy(&self) -> AcceptedFieldAbsencePolicy {
        self.absence_policy
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
    pub(in crate::db) const fn required_slot_count(self) -> usize {
        self.required_slot_count
    }

    /// Return the accepted primary-key physical slot proven generated-compatible.
    #[must_use]
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
    primary_key_slot_index: usize,
    fields: Vec<AcceptedRowLayoutRuntimeField<'a>>,
}

impl<'a> AcceptedRowLayoutRuntimeDescriptor<'a> {
    /// Build one runtime descriptor from an already accepted schema snapshot.
    ///
    /// The constructor still validates local row-layout completeness because
    /// this descriptor will become a trust boundary for decode/write code. A
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
        let primary_key_slot_index = usize::from(primary_key_field.slot().get());

        Ok(Self {
            version: row_layout.version(),
            required_slot_count,
            primary_key_name,
            primary_key_slot_index,
            fields,
        })
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

    /// Return the row shape when this accepted layout can still use generated field codecs.
    ///
    /// The row decoder remains generated-codec backed until accepted-field
    /// decoders exist. Keeping this bridge check and shape projection in the
    /// descriptor owner makes generated compatibility a schema-runtime contract
    /// instead of an executor side calculation.
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
            let accepted_slot = self
                .field_slot_index_by_name(field.name())
                .expect("accepted field must have a descriptor-owned slot");
            if accepted_slot != generated_slot {
                return Err(InternalError::store_invariant(format!(
                    "accepted row layout slot is not generated-compatible: field='{}' accepted_slot={} generated_slot={}",
                    field.name(),
                    accepted_slot,
                    generated_slot,
                )));
            }

            let generated_kind = PersistedFieldKind::from_model_kind(field.kind());
            if accepted_field.kind() != &generated_kind {
                return Err(InternalError::store_invariant(format!(
                    "accepted row layout kind is not generated-compatible: field='{}' accepted_kind={:?} generated_kind={:?}",
                    field.name(),
                    accepted_field.kind(),
                    generated_kind,
                )));
            }

            if accepted_field.storage_decode() != field.storage_decode() {
                return Err(InternalError::store_invariant(format!(
                    "accepted row layout storage decode is not generated-compatible: field='{}' accepted_storage_decode={:?} generated_storage_decode={:?}",
                    field.name(),
                    accepted_field.storage_decode(),
                    field.storage_decode(),
                )));
            }

            if accepted_field.leaf_codec() != field.leaf_codec() {
                return Err(InternalError::store_invariant(format!(
                    "accepted row layout leaf codec is not generated-compatible: field='{}' accepted_leaf_codec={:?} generated_leaf_codec={:?}",
                    field.name(),
                    accepted_field.leaf_codec(),
                    field.leaf_codec(),
                )));
            }
        }

        Ok(AcceptedGeneratedCompatibleRowShape {
            required_slot_count: self.required_slot_count(),
            primary_key_slot_index: self.primary_key_slot_index(),
        })
    }
}

// Decide the missing-slot behavior from accepted database metadata only. Rust
// struct defaults are deliberately absent from this calculation.
const fn accepted_field_absence_policy(
    nullable: bool,
    default: SchemaFieldDefault,
) -> AcceptedFieldAbsencePolicy {
    match (nullable, default) {
        (true, SchemaFieldDefault::None) => AcceptedFieldAbsencePolicy::NullIfMissing,
        (false, SchemaFieldDefault::None) => AcceptedFieldAbsencePolicy::Required,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::schema::{
            AcceptedSchemaSnapshot, FieldId, PersistedFieldKind, PersistedFieldSnapshot,
            PersistedSchemaSnapshot, SchemaFieldDefault, SchemaFieldSlot, SchemaRowLayout,
            SchemaVersion,
            runtime::{AcceptedFieldAbsencePolicy, AcceptedRowLayoutRuntimeDescriptor},
        },
        model::{
            entity::EntityModel,
            field::{FieldKind, FieldModel, FieldStorageDecode, LeafCodec, ScalarCodec},
            index::IndexModel,
        },
        testing::entity_model_from_static,
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
        assert_eq!(nickname.default(), SchemaFieldDefault::None);
        assert_eq!(nickname.storage_decode(), FieldStorageDecode::ByKind);
        assert_eq!(nickname.leaf_codec(), LeafCodec::Scalar(ScalarCodec::Text));
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
