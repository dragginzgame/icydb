//! Accepted schema fixture shared by planner tests with and without SQL.

use crate::db::schema::{
    AcceptedCompositeCatalog, AcceptedFieldKind, AcceptedSchemaRevision, AcceptedSchemaSnapshot,
    AcceptedValueCatalogHandle, FieldId, FieldStorageDecode, LeafCodec, PersistedFieldSnapshot,
    PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
    PersistedSchemaSnapshot, ScalarCodec, SchemaFieldSlot, SchemaIndexId, SchemaInfo,
    SchemaInsertDefault, SchemaRowLayout, SchemaVersion, empty_accepted_enum_catalog_for_tests,
};

// Build accepted authority directly; no generated-model fallback or SQL setup.
pub(in crate::db) fn exact_metadata_schema(
    indexes: &[(&str, &[&str])],
    nullable: &[&str],
) -> SchemaInfo {
    let fields = ["id", "age", "rank", "maybe"]
        .into_iter()
        .enumerate()
        .map(|(offset, name)| {
            let id = u32::try_from(offset + 1).expect("test field identity should fit");
            let slot = u16::try_from(offset).expect("test field slot should fit");
            PersistedFieldSnapshot::new_initial(
                FieldId::new(id),
                name.to_string(),
                SchemaFieldSlot::new(slot),
                AcceptedFieldKind::Int32,
                Vec::new(),
                nullable.contains(&name),
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Int32),
            )
        })
        .collect::<Vec<_>>();
    let row_layout = SchemaRowLayout::initial(
        fields
            .iter()
            .map(|field| (field.id(), field.slot()))
            .collect(),
    );
    let indexes = indexes
        .iter()
        .enumerate()
        .map(|(offset, (name, key_fields))| {
            let ordinal = u16::try_from(offset + 1).expect("test index ordinal should fit");
            let key = key_fields
                .iter()
                .map(|key_field| {
                    let field = fields
                        .iter()
                        .find(|field| field.name() == *key_field)
                        .expect("test index field should exist");
                    PersistedIndexFieldPathSnapshot::new(
                        field.id(),
                        field.slot(),
                        vec![field.name().to_string()],
                        field.kind().clone(),
                        field.nullable(),
                    )
                })
                .collect();
            PersistedIndexSnapshot::new(
                SchemaIndexId::new(u32::from(ordinal))
                    .expect("test index identity should be non-zero"),
                ordinal,
                (*name).to_string(),
                format!("pipeline_tests::{name}"),
                false,
                PersistedIndexKeySnapshot::FieldPath(key),
                None,
            )
        })
        .collect();
    let snapshot = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        "query::plan::pipeline::tests::Entity".to_string(),
        "Entity".to_string(),
        FieldId::new(1),
        row_layout,
        fields,
        indexes,
    ));
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );

    SchemaInfo::from_accepted_snapshot_and_catalog(&snapshot, catalog)
}
