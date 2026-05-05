//! Module: db::schema::proposal
//! Responsibility: compiled schema proposal projection from generated entity metadata.
//! Does not own: live schema persistence or compatibility reconciliation.
//! Boundary: turns trusted `EntityModel` data into a typed schema proposal for 0.146.

use crate::{
    db::schema::{
        FieldId, PersistedFieldKind, PersistedFieldSnapshot, PersistedNestedLeafSnapshot,
        PersistedSchemaSnapshot, SchemaFieldDefault, SchemaFieldSlot, SchemaFieldWritePolicy,
        SchemaRowLayout, SchemaVersion, sql_capabilities,
    },
    model::{
        entity::EntityModel,
        field::{FieldDatabaseDefault, FieldKind, FieldModel, FieldStorageDecode, LeafCodec},
    },
};

///
/// CompiledSchemaProposal
///
/// Runtime projection of generated entity metadata into schema-identity terms.
/// This is not the live schema authority; it is the compiled proposal that
/// startup reconciliation will compare with the persisted schema snapshot.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct CompiledSchemaProposal {
    entity_path: &'static str,
    entity_name: &'static str,
    primary_key_name: &'static str,
    primary_key_field_id: FieldId,
    fields: Vec<CompiledFieldProposal>,
}

impl CompiledSchemaProposal {
    /// Return the generated entity path for diagnostics and reconciliation keys.
    #[must_use]
    pub(in crate::db) const fn entity_path(&self) -> &'static str {
        self.entity_path
    }

    /// Return the generated external entity name.
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &'static str {
        self.entity_name
    }

    /// Return the generated primary-key field name.
    #[must_use]
    pub(in crate::db) const fn primary_key_name(&self) -> &'static str {
        self.primary_key_name
    }

    /// Return the schema field ID assigned to the generated primary key.
    #[must_use]
    pub(in crate::db) const fn primary_key_field_id(&self) -> FieldId {
        self.primary_key_field_id
    }

    /// Return generated field proposals in generated slot order.
    #[must_use]
    pub(in crate::db) const fn fields(&self) -> &[CompiledFieldProposal] {
        self.fields.as_slice()
    }

    /// Build the initial row layout implied by this compiled proposal.
    ///
    /// This uses proposal-assigned IDs only for first initialization. Once a
    /// persisted schema exists, reconciliation must build the row layout from
    /// stored field IDs and slots instead.
    #[must_use]
    pub(in crate::db) fn initial_row_layout(&self) -> SchemaRowLayout {
        let field_to_slot = self
            .fields()
            .iter()
            .map(|field| (field.id(), field.slot()))
            .collect::<Vec<_>>();

        SchemaRowLayout::new(SchemaVersion::initial(), field_to_slot)
    }

    /// Build the initial persisted-schema snapshot implied by this proposal.
    ///
    /// This is only valid for first initialization when no stored schema exists.
    /// Reconciliation must preserve stored field IDs, retired slots, and defaults
    /// once a live persisted schema has been written.
    #[must_use]
    pub(in crate::db) fn initial_persisted_schema_snapshot(&self) -> PersistedSchemaSnapshot {
        let fields = self
            .fields()
            .iter()
            .map(CompiledFieldProposal::initial_persisted_field_snapshot)
            .collect::<Vec<_>>();

        PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            self.entity_path().to_string(),
            self.entity_name().to_string(),
            self.primary_key_field_id(),
            self.initial_row_layout(),
            fields,
        )
    }
}

///
/// CompiledFieldProposal
///
/// One generated field projected into the schema-identity proposal surface.
/// It carries both durable identity and current generated slot metadata so
/// reconciliation can separate logical field identity from row layout.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct CompiledFieldProposal {
    id: FieldId,
    name: &'static str,
    slot: SchemaFieldSlot,
    kind: FieldKind,
    nested_leaves: Vec<PersistedNestedLeafSnapshot>,
    nullable: bool,
    database_default: FieldDatabaseDefault,
    write_policy: SchemaFieldWritePolicy,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
}

impl CompiledFieldProposal {
    /// Return the proposed durable identity for this field.
    #[must_use]
    pub(in crate::db) const fn id(&self) -> FieldId {
        self.id
    }

    /// Return the generated field name.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &'static str {
        self.name
    }

    /// Return the generated row slot for this field.
    #[must_use]
    pub(in crate::db) const fn slot(&self) -> SchemaFieldSlot {
        self.slot
    }

    /// Return the generated runtime field kind.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> FieldKind {
        self.kind
    }

    /// Borrow the nested leaf snapshots generated under this top-level field.
    #[must_use]
    pub(in crate::db) const fn nested_leaves(&self) -> &[PersistedNestedLeafSnapshot] {
        self.nested_leaves.as_slice()
    }

    /// Return whether the generated contract permits explicit `NULL`.
    #[must_use]
    pub(in crate::db) const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Return the generated database-level default contract.
    #[must_use]
    pub(in crate::db) const fn database_default(&self) -> FieldDatabaseDefault {
        self.database_default
    }

    /// Return the generated database-level write policy.
    #[must_use]
    pub(in crate::db) const fn write_policy(&self) -> SchemaFieldWritePolicy {
        self.write_policy
    }

    /// Return the generated persisted decode contract.
    #[must_use]
    pub(in crate::db) const fn storage_decode(&self) -> FieldStorageDecode {
        self.storage_decode
    }

    /// Return the generated leaf codec contract.
    #[must_use]
    pub(in crate::db) const fn leaf_codec(&self) -> LeafCodec {
        self.leaf_codec
    }

    /// Build the initial persisted field snapshot implied by this proposal.
    ///
    /// Database defaults intentionally start as `None`; generated Rust defaults
    /// remain construction behavior and are not imported into live schema
    /// authority by this projection.
    #[must_use]
    pub(in crate::db) fn initial_persisted_field_snapshot(&self) -> PersistedFieldSnapshot {
        PersistedFieldSnapshot::new_with_write_policy(
            self.id(),
            self.name().to_string(),
            self.slot(),
            PersistedFieldKind::from_model_kind(self.kind()),
            self.nested_leaves().to_vec(),
            self.nullable(),
            SchemaFieldDefault::from_model_default(self.database_default()),
            self.write_policy(),
            self.storage_decode(),
            self.leaf_codec(),
        )
    }
}

/// Build the compiled schema proposal for one trusted generated entity model.
#[must_use]
pub(in crate::db) fn compiled_schema_proposal_for_model(
    model: &EntityModel,
) -> CompiledSchemaProposal {
    let fields = model
        .fields()
        .iter()
        .enumerate()
        .map(compiled_field_proposal_from_model_field)
        .collect::<Vec<_>>();

    let proposal = CompiledSchemaProposal {
        entity_path: model.path(),
        entity_name: model.name(),
        primary_key_name: model.primary_key().name(),
        primary_key_field_id: FieldId::from_initial_slot(model.primary_key_slot()),
        fields,
    };

    debug_assert_compiled_schema_proposal_invariants(model, &proposal);

    proposal
}

// Check the initial proposal projection remains a pure slot-order projection.
// Startup reconciliation will replace this deterministic first-snapshot ID
// assignment with stored IDs once a live persisted schema exists.
fn debug_assert_compiled_schema_proposal_invariants(
    model: &EntityModel,
    proposal: &CompiledSchemaProposal,
) {
    debug_assert_eq!(
        proposal.primary_key_field_id(),
        FieldId::from_initial_slot(model.primary_key_slot())
    );

    let layout = proposal.initial_row_layout();
    let snapshot = proposal.initial_persisted_schema_snapshot();
    debug_assert_eq!(layout.version(), SchemaVersion::initial());
    debug_assert_eq!(layout.version().get(), SchemaVersion::initial().get());
    debug_assert_eq!(layout.field_to_slot().len(), proposal.fields().len());
    debug_assert_eq!(snapshot.version(), SchemaVersion::initial());
    debug_assert_eq!(snapshot.entity_path(), proposal.entity_path());
    debug_assert_eq!(snapshot.entity_name(), proposal.entity_name());
    debug_assert_eq!(
        snapshot.primary_key_field_id(),
        proposal.primary_key_field_id()
    );
    debug_assert_eq!(snapshot.row_layout(), &layout);
    debug_assert_eq!(snapshot.fields().len(), proposal.fields().len());

    for field in snapshot.fields() {
        let _ = (
            field.id(),
            field.name(),
            field.slot(),
            field.kind(),
            field.nested_leaves(),
            field.nullable(),
            field.default(),
            field.storage_decode(),
            field.leaf_codec(),
        );

        let capabilities = sql_capabilities(field.kind());
        let aggregate = capabilities.aggregate_input();
        let _ = (
            capabilities.selectable(),
            capabilities.comparable(),
            capabilities.orderable(),
            capabilities.groupable(),
            aggregate.count(),
            aggregate.numeric(),
            aggregate.extrema(),
        );
    }

    for (expected_slot, field) in proposal.fields().iter().enumerate() {
        debug_assert_eq!(field.id(), FieldId::from_initial_slot(expected_slot));
        debug_assert_eq!(
            field.slot(),
            SchemaFieldSlot::from_generated_index(expected_slot)
        );

        let _ = (
            field.name(),
            field.kind(),
            field.nullable(),
            field.database_default(),
            field.write_policy(),
            field.storage_decode(),
            field.leaf_codec(),
            field.nested_leaves(),
            field.initial_persisted_field_snapshot(),
        );
    }
}

// Project one generated field and its generated slot into the compiled schema
// proposal. This remains a pure projection until live-schema reconciliation
// starts substituting stored field IDs.
fn compiled_field_proposal_from_model_field(
    (slot, field): (usize, &FieldModel),
) -> CompiledFieldProposal {
    let slot = SchemaFieldSlot::from_generated_index(slot);

    CompiledFieldProposal {
        id: FieldId::from_initial_slot(usize::from(slot.get())),
        name: field.name(),
        slot,
        kind: field.kind(),
        nested_leaves: persisted_nested_leaf_snapshots_from_model_fields(field.nested_fields()),
        nullable: field.nullable(),
        database_default: field.database_default(),
        write_policy: SchemaFieldWritePolicy::from_model_policies(
            field.insert_generation(),
            field.write_management(),
        ),
        storage_decode: field.storage_decode(),
        leaf_codec: field.leaf_codec(),
    }
}

// Flatten generated nested field metadata into path-addressed persisted leaf
// descriptors rooted at one top-level field. The top-level field owns the
// physical slot; nested entries only carry planning metadata for field paths.
fn persisted_nested_leaf_snapshots_from_model_fields(
    fields: &[FieldModel],
) -> Vec<PersistedNestedLeafSnapshot> {
    let mut leaves = Vec::new();

    for field in fields {
        push_persisted_nested_leaf_snapshots(field, Vec::new(), &mut leaves);
    }

    leaves
}

// Record one nested field itself, then recurse through its children so every
// queryable path segment chain has an accepted-schema descriptor.
fn push_persisted_nested_leaf_snapshots(
    field: &FieldModel,
    mut path: Vec<String>,
    leaves: &mut Vec<PersistedNestedLeafSnapshot>,
) {
    path.push(field.name().to_string());
    leaves.push(PersistedNestedLeafSnapshot::new(
        path.clone(),
        PersistedFieldKind::from_model_kind(field.kind()),
        field.nullable(),
        field.storage_decode(),
        field.leaf_codec(),
    ));

    for nested in field.nested_fields() {
        push_persisted_nested_leaf_snapshots(nested, path.clone(), leaves);
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::schema::{
            FieldId, PersistedFieldKind, SchemaFieldDefault, SchemaFieldSlot, SchemaVersion,
            compiled_schema_proposal_for_model,
        },
        model::{
            entity::EntityModel,
            field::{
                FieldDatabaseDefault, FieldKind, FieldModel, FieldStorageDecode, LeafCodec,
                ScalarCodec,
            },
            index::IndexModel,
        },
        testing::entity_model_from_static,
    };

    static PROFILE_FIELDS: [FieldModel; 2] = [
        FieldModel::generated("nickname", FieldKind::Text { max_len: None }),
        FieldModel::generated("score", FieldKind::Uint),
    ];
    static FIELDS: [FieldModel; 4] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated_with_storage_decode_and_nullability(
            "name",
            FieldKind::Text { max_len: None },
            FieldStorageDecode::ByKind,
            true,
        ),
        FieldModel::generated("rank", FieldKind::Uint),
        FieldModel::generated_with_storage_decode_nullability_write_policies_and_nested_fields(
            "profile",
            FieldKind::Structured { queryable: true },
            FieldStorageDecode::Value,
            false,
            None,
            None,
            &PROFILE_FIELDS,
        ),
    ];
    static INDEXES: [&IndexModel; 0] = [];
    static MODEL: EntityModel = entity_model_from_static(
        "schema::proposal::tests::Entity",
        "Entity",
        &FIELDS[0],
        0,
        &FIELDS,
        &INDEXES,
    );

    #[test]
    fn compiled_schema_proposal_assigns_initial_field_ids_from_slots() {
        let proposal = compiled_schema_proposal_for_model(&MODEL);

        assert_eq!(proposal.entity_path(), "schema::proposal::tests::Entity");
        assert_eq!(proposal.entity_name(), "Entity");
        assert_eq!(proposal.primary_key_field_id(), FieldId::new(1));
        assert_eq!(proposal.fields().len(), 4);

        let ids = proposal
            .fields()
            .iter()
            .map(super::CompiledFieldProposal::id)
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![
                FieldId::new(1),
                FieldId::new(2),
                FieldId::new(3),
                FieldId::new(4),
            ],
        );
    }

    #[test]
    fn compiled_schema_proposal_preserves_generated_storage_contracts() {
        let proposal = compiled_schema_proposal_for_model(&MODEL);
        let name = &proposal.fields()[1];

        assert_eq!(name.name(), "name");
        assert_eq!(name.slot(), SchemaFieldSlot::from_generated_index(1));
        assert!(matches!(name.kind(), FieldKind::Text { max_len: None }));
        assert!(name.nullable());
        assert_eq!(name.database_default(), FieldDatabaseDefault::None);
        assert_eq!(name.storage_decode(), FieldStorageDecode::ByKind);
        assert_eq!(name.leaf_codec(), LeafCodec::Scalar(ScalarCodec::Text));
    }

    #[test]
    fn compiled_schema_proposal_builds_initial_row_layout() {
        let proposal = compiled_schema_proposal_for_model(&MODEL);
        let layout = proposal.initial_row_layout();

        assert_eq!(layout.version(), SchemaVersion::initial());
        assert_eq!(
            layout.field_to_slot(),
            &[
                (FieldId::new(1), SchemaFieldSlot::from_generated_index(0)),
                (FieldId::new(2), SchemaFieldSlot::from_generated_index(1)),
                (FieldId::new(3), SchemaFieldSlot::from_generated_index(2)),
                (FieldId::new(4), SchemaFieldSlot::from_generated_index(3)),
            ]
        );
    }

    #[test]
    fn compiled_schema_proposal_builds_initial_persisted_snapshot() {
        let proposal = compiled_schema_proposal_for_model(&MODEL);
        let snapshot = proposal.initial_persisted_schema_snapshot();

        assert_eq!(snapshot.version(), SchemaVersion::initial());
        assert_eq!(snapshot.entity_path(), "schema::proposal::tests::Entity");
        assert_eq!(snapshot.entity_name(), "Entity");
        assert_eq!(snapshot.primary_key_field_id(), FieldId::new(1));
        assert_eq!(snapshot.fields().len(), 4);

        let name = &snapshot.fields()[1];
        assert_eq!(name.id(), FieldId::new(2));
        assert_eq!(name.name(), "name");
        assert_eq!(name.slot(), SchemaFieldSlot::from_generated_index(1));
        assert!(matches!(
            name.kind(),
            PersistedFieldKind::Text { max_len: None }
        ));
        assert!(name.nullable());
        assert_eq!(name.default(), SchemaFieldDefault::None);
        assert_eq!(name.storage_decode(), FieldStorageDecode::ByKind);
        assert_eq!(name.leaf_codec(), LeafCodec::Scalar(ScalarCodec::Text));

        let profile = &snapshot.fields()[3];
        assert_eq!(profile.name(), "profile");
        assert_eq!(profile.nested_leaves().len(), 2);
        assert_eq!(profile.nested_leaves()[0].path(), &["nickname".to_string()],);
        assert!(matches!(
            profile.nested_leaves()[0].kind(),
            PersistedFieldKind::Text { max_len: None }
        ));
        assert_eq!(profile.nested_leaves()[1].path(), &["score".to_string()]);
        assert!(matches!(
            profile.nested_leaves()[1].kind(),
            PersistedFieldKind::Uint
        ));
    }
}
