//! Module: db::schema::info
//! Responsibility: schema model/index integrity checks used during schema info construction.
//! Does not own: query planning policy or runtime predicate evaluation.
//! Boundary: validates entity/index model consistency for predicate schema metadata.

use crate::{
    db::schema::{
        AcceptedSchemaSnapshot, FieldType, field_type_from_model_kind,
        field_type_from_persisted_kind,
    },
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel},
    },
};
use std::sync::{Mutex, OnceLock};

type SchemaFieldEntry = (&'static str, SchemaFieldInfo);
type CachedSchemaEntries = Vec<(&'static str, &'static SchemaInfo)>;

fn schema_field_info<'a>(
    fields: &'a [SchemaFieldEntry],
    name: &str,
) -> Option<&'a SchemaFieldInfo> {
    fields
        .binary_search_by_key(&name, |(field_name, _)| *field_name)
        .ok()
        .map(|index| &fields[index].1)
}

///
/// SchemaInfo
///
/// Lightweight, runtime-usable field-type map for one entity.
/// This is the *only* schema surface the predicate validator depends on.
///

///
/// SchemaFieldInfo
///
/// Compact per-field schema entry used by `SchemaInfo`.
/// Keeps reduced predicate type metadata and the full field-kind authority in
/// one table so schema construction does not duplicate field-name maps.
///

#[derive(Clone, Debug)]
struct SchemaFieldInfo {
    ty: FieldType,
    kind: FieldKind,
    nested_fields: &'static [FieldModel],
}

#[derive(Clone, Debug)]
pub(crate) struct SchemaInfo {
    fields: Vec<SchemaFieldEntry>,
}

impl SchemaInfo {
    // Build one compact field table from trusted generated field metadata.
    fn from_trusted_field_models(fields: &[FieldModel]) -> Self {
        let mut fields = fields
            .iter()
            .map(|field| {
                (
                    field.name(),
                    SchemaFieldInfo {
                        ty: field_type_from_model_kind(&field.kind()),
                        kind: field.kind(),
                        nested_fields: field.nested_fields(),
                    },
                )
            })
            .collect::<Vec<_>>();

        fields.sort_unstable_by_key(|(field_name, _)| *field_name);

        Self { fields }
    }

    // Build one compact field table from trusted generated entity metadata.
    fn from_trusted_entity_model(model: &EntityModel) -> Self {
        Self::from_trusted_field_models(model.fields())
    }

    #[must_use]
    pub(crate) fn field(&self, name: &str) -> Option<&FieldType> {
        schema_field_info(self.fields.as_slice(), name).map(|field| &field.ty)
    }

    #[must_use]
    pub(crate) fn field_kind(&self, name: &str) -> Option<&FieldKind> {
        schema_field_info(self.fields.as_slice(), name).map(|field| &field.kind)
    }

    #[must_use]
    pub(crate) fn field_nested_fields(&self, name: &str) -> Option<&'static [FieldModel]> {
        schema_field_info(self.fields.as_slice(), name).map(|field| field.nested_fields)
    }

    /// Build one owned schema view from trusted generated field metadata.
    #[must_use]
    pub(crate) fn from_field_models(fields: &[FieldModel]) -> Self {
        Self::from_trusted_field_models(fields)
    }

    /// Build one owned schema view from an accepted persisted snapshot.
    ///
    /// This is the live-schema counterpart to the generated metadata cache.
    /// It intentionally keeps generated nested-field metadata until persisted
    /// snapshots carry nested leaf descriptions, but top-level SQL/query type
    /// checks now read the accepted persisted field kind.
    #[must_use]
    pub(in crate::db) fn from_accepted_snapshot_for_model(
        model: &EntityModel,
        schema: &AcceptedSchemaSnapshot,
    ) -> Self {
        let mut fields = model
            .fields()
            .iter()
            .map(|field| {
                let ty = schema.field_by_name(field.name()).map_or_else(
                    || field_type_from_model_kind(&field.kind()),
                    |persisted| field_type_from_persisted_kind(persisted.kind()),
                );

                (
                    field.name(),
                    SchemaFieldInfo {
                        ty,
                        kind: field.kind(),
                        nested_fields: field.nested_fields(),
                    },
                )
            })
            .collect::<Vec<_>>();

        fields.sort_unstable_by_key(|(field_name, _)| *field_name);

        Self { fields }
    }

    /// Return one cached schema view for a trusted generated entity model.
    pub(crate) fn cached_for_entity_model(model: &EntityModel) -> &'static Self {
        static CACHE: OnceLock<Mutex<CachedSchemaEntries>> = OnceLock::new();

        let cache = CACHE.get_or_init(|| Mutex::new(CachedSchemaEntries::new()));
        let mut guard = cache.lock().expect("schema info cache mutex poisoned");
        if let Some(cached) = guard
            .iter()
            .find(|(entity_path, _)| *entity_path == model.path())
            .map(|(_, schema)| *schema)
        {
            return cached;
        }

        let schema = Box::leak(Box::new(Self::from_trusted_entity_model(model)));
        guard.push((model.path(), schema));
        schema
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
            PersistedSchemaSnapshot, SchemaFieldDefault, SchemaFieldSlot, SchemaInfo,
            SchemaRowLayout, SchemaVersion, literal_matches_type,
        },
        model::{
            entity::EntityModel,
            field::{FieldKind, FieldModel, FieldStorageDecode, LeafCodec},
            index::IndexModel,
        },
        testing::entity_model_from_static,
        value::Value,
    };

    static FIELDS: [FieldModel; 2] = [
        FieldModel::generated("name", FieldKind::Text { max_len: None }),
        FieldModel::generated("id", FieldKind::Ulid),
    ];
    static INDEXES: [&IndexModel; 0] = [];
    static MODEL: EntityModel = entity_model_from_static(
        "schema::info::tests::Entity",
        "Entity",
        &FIELDS[1],
        1,
        &FIELDS,
        &INDEXES,
    );

    #[test]
    fn cached_for_entity_model_reuses_one_schema_instance() {
        let first = SchemaInfo::cached_for_entity_model(&MODEL);
        let second = SchemaInfo::cached_for_entity_model(&MODEL);

        assert!(std::ptr::eq(first, second));
        assert!(first.field("id").is_some());
        assert!(first.field("name").is_some());
    }

    #[test]
    fn accepted_snapshot_schema_info_uses_persisted_top_level_field_type() {
        let snapshot = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "schema::info::tests::Entity".to_string(),
            "Entity".to_string(),
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
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::StructuralFallback,
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "name".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Blob,
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::StructuralFallback,
                ),
            ],
        ));

        let schema = SchemaInfo::from_accepted_snapshot_for_model(&MODEL, &snapshot);
        let name_type = schema.field("name").expect("accepted field should exist");

        assert!(literal_matches_type(&Value::Blob(vec![1, 2, 3]), name_type));
        assert!(!literal_matches_type(
            &Value::Text("name".into()),
            name_type
        ));
    }
}
