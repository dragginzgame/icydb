//! Test-only constructors and adapters for the parent module.

use super::*;

impl AcceptedSchemaSnapshot {
    /// Wrap one persisted snapshot for owner-local test fixtures.
    ///
    /// This unchecked constructor exists only for tests that intentionally
    /// construct inconsistent accepted metadata to prove accessor authority.
    /// Runtime acceptance must use `try_new`.
    #[must_use]
    pub(in crate::db) const fn new(snapshot: PersistedSchemaSnapshot) -> Self {
        Self { snapshot }
    }

    /// Borrow one accepted field kind by persisted field name.
    #[must_use]
    pub(in crate::db) fn field_kind_by_name(&self, name: &str) -> Option<&AcceptedFieldKind> {
        self.field_by_name(name).map(PersistedFieldSnapshot::kind)
    }
}

impl PersistedSchemaSnapshot {
    /// Build one persisted schema snapshot from already-validated pieces.
    #[must_use]
    pub(in crate::db) fn new(
        version: SchemaVersion,
        entity_path: String,
        entity_name: String,
        primary_key_field_ids: impl IntoPrimaryKeyFieldIds,
        row_layout: SchemaRowLayout,
        fields: Vec<PersistedFieldSnapshot>,
    ) -> Self {
        let snapshot = Self::new_with_primary_key_fields_and_indexes(
            version,
            entity_path,
            entity_name,
            primary_key_field_ids.into_primary_key_field_ids(),
            row_layout,
            fields,
            Vec::new(),
        );
        let catalog = AcceptedConstraintCatalog::initial(
            snapshot.fields(),
            snapshot.indexes(),
            snapshot.relations(),
        )
        .expect("test structural constraint catalog should build");
        snapshot.with_constraint_catalog(catalog)
    }

    /// Build one persisted schema snapshot with accepted index contracts.
    #[must_use]
    pub(in crate::db) fn new_with_indexes(
        version: SchemaVersion,
        entity_path: String,
        entity_name: String,
        primary_key_field_ids: impl IntoPrimaryKeyFieldIds,
        row_layout: SchemaRowLayout,
        fields: Vec<PersistedFieldSnapshot>,
        indexes: Vec<PersistedIndexSnapshot>,
    ) -> Self {
        let snapshot = Self::new_with_primary_key_fields_and_indexes(
            version,
            entity_path,
            entity_name,
            primary_key_field_ids.into_primary_key_field_ids(),
            row_layout,
            fields,
            indexes,
        );
        let catalog = AcceptedConstraintCatalog::initial(
            snapshot.fields(),
            snapshot.indexes(),
            snapshot.relations(),
        )
        .expect("test structural constraint catalog should build");
        snapshot.with_constraint_catalog(catalog)
    }
}

impl PersistedIndexSnapshot {
    pub(in crate::db) fn clone_with_predicate_for_tests(
        &self,
        predicate: Option<crate::db::schema::AcceptedIndexPredicate>,
    ) -> Self {
        let mut index = self.clone();
        index.predicate = predicate;
        index
    }
}

impl PersistedIndexKeySnapshot {
    /// Lower a field-only key through the same item representation.
    #[must_use]
    pub(in crate::db) fn from_field_paths(paths: Vec<PersistedIndexFieldPathSnapshot>) -> Self {
        Self::new(
            paths
                .into_iter()
                .map(PersistedIndexKeyItemSnapshot::FieldPath)
                .collect(),
        )
    }
}

impl PersistedFieldSnapshot {
    /// Build an ordinary initial field fixture without defaults or write policies.
    ///
    /// Tests of alternate codecs, historical layouts and generated values retain
    /// the explicit constructors so those contracts remain visible at each case.
    pub(in crate::db) fn new_initial_test_field(
        id: u32,
        name: &str,
        slot: u16,
        kind: AcceptedFieldKind,
        nullable: bool,
    ) -> Self {
        let storage_decode = FieldStorageDecode::ByKind;
        let leaf_codec = kind.leaf_codec_for_storage(storage_decode);
        Self::new_initial(
            FieldId::new(id),
            name.into(),
            SchemaFieldSlot::new(slot),
            kind,
            Vec::new(),
            nullable,
            SchemaInsertDefault::None,
            storage_decode,
            leaf_codec,
        )
    }

    /// Build one persisted field snapshot from already-validated pieces.
    #[expect(
        clippy::too_many_arguments,
        reason = "schema snapshot construction keeps every persisted field contract explicit"
    )]
    #[must_use]
    pub(in crate::db) const fn new_initial(
        id: FieldId,
        name: String,
        slot: SchemaFieldSlot,
        kind: AcceptedFieldKind,
        nested_leaves: Vec<PersistedNestedLeafSnapshot>,
        nullable: bool,
        insert_default: SchemaInsertDefault,
        storage_decode: FieldStorageDecode,
        leaf_codec: LeafCodec,
    ) -> Self {
        Self::new_with_write_policy_and_origin(
            id,
            name,
            slot,
            kind,
            nested_leaves,
            nullable,
            RowLayoutVersion::INITIAL,
            insert_default,
            SchemaHistoricalFill::Reject,
            SchemaFieldWritePolicy::none(),
            PersistedFieldOrigin::Generated,
            storage_decode,
            leaf_codec,
        )
    }

    /// Build one initial generated field with explicit database write policy.
    #[expect(
        clippy::too_many_arguments,
        reason = "schema snapshot construction keeps every persisted field contract explicit"
    )]
    #[must_use]
    pub(in crate::db) const fn new_initial_with_write_policy(
        id: FieldId,
        name: String,
        slot: SchemaFieldSlot,
        kind: AcceptedFieldKind,
        nested_leaves: Vec<PersistedNestedLeafSnapshot>,
        nullable: bool,
        insert_default: SchemaInsertDefault,
        write_policy: SchemaFieldWritePolicy,
        storage_decode: FieldStorageDecode,
        leaf_codec: LeafCodec,
    ) -> Self {
        Self::new_with_write_policy_and_origin(
            id,
            name,
            slot,
            kind,
            nested_leaves,
            nullable,
            RowLayoutVersion::INITIAL,
            insert_default,
            SchemaHistoricalFill::Reject,
            write_policy,
            PersistedFieldOrigin::Generated,
            storage_decode,
            leaf_codec,
        )
    }

    /// Build one initial field with explicit write policy and durable origin.
    #[expect(
        clippy::too_many_arguments,
        reason = "schema snapshot construction keeps every persisted field contract explicit"
    )]
    #[must_use]
    pub(in crate::db) const fn new_initial_with_write_policy_and_origin(
        id: FieldId,
        name: String,
        slot: SchemaFieldSlot,
        kind: AcceptedFieldKind,
        nested_leaves: Vec<PersistedNestedLeafSnapshot>,
        nullable: bool,
        insert_default: SchemaInsertDefault,
        write_policy: SchemaFieldWritePolicy,
        origin: PersistedFieldOrigin,
        storage_decode: FieldStorageDecode,
        leaf_codec: LeafCodec,
    ) -> Self {
        Self::new_with_write_policy_and_origin(
            id,
            name,
            slot,
            kind,
            nested_leaves,
            nullable,
            RowLayoutVersion::INITIAL,
            insert_default,
            SchemaHistoricalFill::Reject,
            write_policy,
            origin,
            storage_decode,
            leaf_codec,
        )
    }
}

impl SchemaFieldWritePolicy {
    /// Build one empty write-policy contract.
    #[must_use]
    pub(in crate::db) const fn none() -> Self {
        Self {
            insert_generation: None,
            write_management: None,
        }
    }
}
