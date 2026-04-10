//! Module: db::schema::info
//! Responsibility: schema model/index integrity checks used during schema info construction.
//! Does not own: query planning policy or runtime predicate evaluation.
//! Boundary: validates entity/index model consistency for predicate schema metadata.

use crate::{
    db::schema::{FieldType, field_type_from_model_kind},
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

    /// Build runtime predicate schema information from one trusted entity model.
    ///
    /// Tests still use this compatibility shim when they want one owned schema
    /// value without going through the global cache.
    #[expect(dead_code)]
    pub(crate) fn from_entity_model(model: &EntityModel) -> Self {
        Self::from_trusted_entity_model(model)
    }

    /// Build one owned schema view from trusted generated field metadata.
    #[must_use]
    pub(crate) fn from_field_models(fields: &[FieldModel]) -> Self {
        Self::from_trusted_field_models(fields)
    }

    /// Return one cached schema view for a trusted generated entity model.
    pub(crate) fn cached_for_entity_model(model: &EntityModel) -> &'static Self {
        static CACHE: OnceLock<Mutex<CachedSchemaEntries>> = OnceLock::new();

        let cache = CACHE.get_or_init(|| Mutex::new(CachedSchemaEntries::new()));
        if let Some(cached) = cache
            .lock()
            .expect("schema info cache mutex poisoned")
            .iter()
            .find(|(entity_path, _)| *entity_path == model.path())
            .map(|(_, schema)| *schema)
        {
            return cached;
        }

        let schema = Box::leak(Box::new(Self::from_trusted_entity_model(model)));
        let mut guard = cache.lock().expect("schema info cache mutex poisoned");
        if let Some((_, cached)) = guard
            .iter()
            .find(|(entity_path, _)| *entity_path == model.path())
        {
            return cached;
        }

        guard.push((model.path(), schema));
        schema
    }

    /// Preserve legacy test call sites that still read like schema validation.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn expect(self, _message: &str) -> Self {
        self
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::schema::SchemaInfo,
        model::{
            entity::EntityModel,
            field::{FieldKind, FieldModel},
            index::IndexModel,
        },
        testing::entity_model_from_static,
    };

    static FIELDS: [FieldModel; 2] = [
        FieldModel::generated("name", FieldKind::Text),
        FieldModel::generated("id", FieldKind::Ulid),
    ];
    static INDEXES: [&IndexModel; 0] = [];
    static MODEL: EntityModel = entity_model_from_static(
        "schema::info::tests::Entity",
        "Entity",
        &FIELDS[1],
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
}
