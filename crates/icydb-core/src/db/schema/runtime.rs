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
    model::field::{FieldStorageDecode, LeafCodec},
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

#[allow(
    dead_code,
    reason = "0.147 introduces the accepted layout runtime boundary before row decode consumes it"
)]
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

#[allow(
    dead_code,
    reason = "0.147 introduces the accepted layout runtime boundary before row decode consumes it"
)]
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

#[allow(
    dead_code,
    reason = "0.147 introduces the accepted layout runtime boundary before row decode consumes it"
)]
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
    #[must_use]
    pub(in crate::db) const fn nested_leaves(&self) -> &'a [PersistedNestedLeafSnapshot] {
        self.nested_leaves
    }

    /// Return whether this field permits explicit persisted `NULL`.
    #[must_use]
    pub(in crate::db) const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Return the accepted database-level default contract.
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
/// AcceptedRowLayoutRuntimeDescriptor
///
/// AcceptedRowLayoutRuntimeDescriptor is the schema-owned runtime contract for
/// one accepted row layout. It is intentionally read-only and closed: decode
/// and write code can consume its field facts, but cannot reinterpret raw
/// persisted snapshots or generated model fields to decide slot behavior.
///

#[allow(
    dead_code,
    reason = "0.147 introduces the accepted layout runtime boundary before row decode consumes it"
)]
#[derive(Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedRowLayoutRuntimeDescriptor<'a> {
    version: SchemaVersion,
    required_slot_count: usize,
    fields: Vec<AcceptedRowLayoutRuntimeField<'a>>,
}

#[allow(
    dead_code,
    reason = "0.147 introduces the accepted layout runtime boundary before row decode consumes it"
)]
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

        Ok(Self {
            version: row_layout.version(),
            required_slot_count,
            fields,
        })
    }

    /// Return the accepted schema version backing this runtime layout.
    #[must_use]
    pub(in crate::db) const fn version(&self) -> SchemaVersion {
        self.version
    }

    /// Return the minimum physical slot count required by this layout.
    #[must_use]
    pub(in crate::db) const fn required_slot_count(&self) -> usize {
        self.required_slot_count
    }

    /// Borrow runtime field facts in accepted snapshot field order.
    #[must_use]
    pub(in crate::db) const fn fields(&self) -> &[AcceptedRowLayoutRuntimeField<'a>] {
        self.fields.as_slice()
    }

    /// Borrow one runtime field by accepted physical row slot.
    #[must_use]
    pub(in crate::db) fn field_for_slot(
        &self,
        slot: SchemaFieldSlot,
    ) -> Option<&AcceptedRowLayoutRuntimeField<'a>> {
        self.fields.iter().find(|field| field.slot() == slot)
    }

    /// Borrow one runtime field by durable accepted field identity.
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
}

// Decide the missing-slot behavior from accepted database metadata only. Rust
// struct defaults are deliberately absent from this calculation.
#[allow(
    dead_code,
    reason = "0.147 introduces the accepted layout runtime boundary before row decode consumes it"
)]
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
        model::field::{FieldStorageDecode, LeafCodec, ScalarCodec},
    };

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

    #[test]
    fn accepted_row_layout_runtime_descriptor_uses_row_layout_slot_authority() {
        let accepted = accepted_schema_fixture();
        let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)
            .expect("accepted runtime descriptor should build");

        assert_eq!(descriptor.version(), SchemaVersion::initial());
        assert_eq!(descriptor.required_slot_count(), 10);
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
        assert!(nickname.nested_leaves().is_empty());
        assert!(nickname.nullable());
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
