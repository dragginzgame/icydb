//! Module: db::schema::info
//! Responsibility: schema model/index integrity checks used during schema info construction.
//! Does not own: query planning policy or runtime predicate evaluation.
//! Boundary: validates entity/index model consistency for predicate schema metadata.

use crate::{
    db::schema::{
        AcceptedSchemaSnapshot, FieldType, PersistedFieldKind, PersistedNestedLeafSnapshot,
        SqlCapabilities, canonicalize_strict_sql_literal_for_persisted_kind,
        field_type_from_model_kind, field_type_from_persisted_kind, sql_capabilities,
    },
    model::{
        canonicalize_strict_sql_literal_for_kind,
        entity::EntityModel,
        field::{FieldKind, FieldModel},
    },
    value::Value,
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
    slot: usize,
    ty: FieldType,
    kind: FieldKind,
    sql_capabilities: SqlCapabilities,
    persisted_kind: Option<PersistedFieldKind>,
    nested_leaves: Option<Vec<PersistedNestedLeafSnapshot>>,
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
            .enumerate()
            .map(|(slot, field)| {
                (
                    field.name(),
                    SchemaFieldInfo {
                        slot,
                        ty: field_type_from_model_kind(&field.kind()),
                        kind: field.kind(),
                        sql_capabilities: sql_capabilities(&PersistedFieldKind::from_model_kind(
                            field.kind(),
                        )),
                        persisted_kind: None,
                        nested_leaves: None,
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

    /// Return the top-level physical row slot for one field.
    ///
    /// Accepted schema views source this from `SchemaRowLayout`; generated
    /// schema views keep using generated field-table position. The method gives
    /// planning validation one schema-owned slot surface instead of requiring
    /// direct `EntityModel` field-table checks.
    #[must_use]
    pub(in crate::db) fn field_slot_index(&self, name: &str) -> Option<usize> {
        schema_field_info(self.fields.as_slice(), name).map(|field| field.slot)
    }

    /// Return SQL operation capabilities for one top-level field.
    ///
    /// Accepted live schema views derive this from persisted field kinds so SQL
    /// admission follows reconciled schema authority. Generated schema views
    /// use generated model metadata for compile-time-only callers.
    ///
    #[must_use]
    pub(in crate::db) fn sql_capabilities(&self, name: &str) -> Option<SqlCapabilities> {
        schema_field_info(self.fields.as_slice(), name).map(|field| field.sql_capabilities)
    }

    /// Return SQL operation capabilities for one nested field path.
    ///
    /// Accepted schema views resolve nested paths from persisted nested leaf
    /// metadata. Generated schema views derive the same facts from generated
    /// nested `FieldModel` metadata until live row-layout authority exists.
    #[must_use]
    pub(in crate::db) fn nested_sql_capabilities(
        &self,
        name: &str,
        segments: &[String],
    ) -> Option<SqlCapabilities> {
        let field = schema_field_info(self.fields.as_slice(), name)?;

        if let Some(nested_leaves) = field.nested_leaves.as_ref() {
            return nested_leaves
                .iter()
                .find(|leaf| leaf.path() == segments)
                .map(|leaf| sql_capabilities(leaf.kind()));
        }

        resolve_nested_field_path_kind(field.nested_fields, segments)
            .map(|kind| sql_capabilities(&PersistedFieldKind::from_model_kind(kind)))
    }

    /// Return the first top-level field that SQL cannot project directly.
    #[must_use]
    pub(in crate::db) fn first_non_sql_selectable_field(&self) -> Option<&'static str> {
        self.fields
            .iter()
            .find(|(_, field)| !field.sql_capabilities.selectable())
            .map(|(field_name, _)| *field_name)
    }

    /// Return the type for one nested field path rooted at a top-level field.
    ///
    /// Accepted schema views resolve nested paths from persisted nested leaf
    /// metadata. Generated schema views retain generated nested `FieldModel`
    /// traversal for compile-time-only callers.
    #[must_use]
    pub(crate) fn nested_field_type(&self, name: &str, segments: &[String]) -> Option<FieldType> {
        let field = schema_field_info(self.fields.as_slice(), name)?;

        if let Some(nested_leaves) = field.nested_leaves.as_ref() {
            return nested_leaves
                .iter()
                .find(|leaf| leaf.path() == segments)
                .map(|leaf| field_type_from_persisted_kind(leaf.kind()));
        }

        resolve_nested_field_path_kind(field.nested_fields, segments)
            .map(|kind| field_type_from_model_kind(&kind))
    }

    /// Return whether one top-level field exposes any nested path metadata.
    #[must_use]
    pub(crate) fn field_has_nested_paths(&self, name: &str) -> bool {
        schema_field_info(self.fields.as_slice(), name).is_some_and(|field| {
            field.nested_leaves.as_ref().map_or_else(
                || !field.nested_fields.is_empty(),
                |leaves| !leaves.is_empty(),
            )
        })
    }

    /// Canonicalize one strict SQL literal against this schema's field authority.
    ///
    /// Accepted live schemas use persisted field kinds so SQL read predicates
    /// follow the same top-level type boundary as SQL writes and planning.
    /// Generated schema views retain the old generated-kind fallback for
    /// direct lowering tests and compile-time-only callers.
    ///
    #[must_use]
    pub(in crate::db) fn canonicalize_strict_sql_literal(
        &self,
        field_name: &str,
        value: &Value,
    ) -> Option<Value> {
        let field = schema_field_info(self.fields.as_slice(), field_name)?;

        field.persisted_kind.as_ref().map_or_else(
            || canonicalize_strict_sql_literal_for_kind(&field.kind, value),
            |kind| canonicalize_strict_sql_literal_for_persisted_kind(kind, value),
        )
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
            .enumerate()
            .map(|(generated_slot, field)| {
                let accepted_facts = schema.field_facts_by_name(field.name());
                let accepted_kind = accepted_facts.map(|(kind, _, _)| kind);
                let slot = accepted_facts.map_or_else(
                    || generated_slot,
                    |(_, accepted_slot, _)| usize::from(accepted_slot.get()),
                );
                let ty = accepted_kind.map_or_else(
                    || field_type_from_model_kind(&field.kind()),
                    field_type_from_persisted_kind,
                );
                let sql_capabilities = accepted_kind.map_or_else(
                    || sql_capabilities(&PersistedFieldKind::from_model_kind(field.kind())),
                    sql_capabilities,
                );

                (
                    field.name(),
                    SchemaFieldInfo {
                        slot,
                        ty,
                        kind: field.kind(),
                        sql_capabilities,
                        persisted_kind: accepted_kind.cloned(),
                        nested_leaves: accepted_facts
                            .map(|(_, _, nested_leaves)| nested_leaves.to_vec()),
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

// Resolve generated nested metadata for compile-time-only schema views. Accepted
// schema views use persisted nested leaf descriptors before this fallback is
// considered.
fn resolve_nested_field_path_kind(fields: &[FieldModel], segments: &[String]) -> Option<FieldKind> {
    let (segment, rest) = segments.split_first()?;
    let field = fields
        .iter()
        .find(|field| field.name() == segment.as_str())?;

    if rest.is_empty() {
        return Some(field.kind());
    }

    resolve_nested_field_path_kind(field.nested_fields(), rest)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::schema::{
            AcceptedSchemaSnapshot, FieldId, PersistedFieldKind, PersistedFieldSnapshot,
            PersistedNestedLeafSnapshot, PersistedSchemaSnapshot, SchemaFieldDefault,
            SchemaFieldSlot, SchemaInfo, SchemaRowLayout, SchemaVersion, literal_matches_type,
        },
        model::{
            entity::EntityModel,
            field::{FieldKind, FieldModel, FieldStorageDecode, LeafCodec, ScalarCodec},
            index::IndexModel,
        },
        testing::entity_model_from_static,
        value::Value,
    };

    static FIELDS: [FieldModel; 2] = [
        FieldModel::generated("name", FieldKind::Text { max_len: None }),
        FieldModel::generated("id", FieldKind::Ulid),
    ];
    static PROFILE_NESTED_FIELDS: [FieldModel; 1] =
        [FieldModel::generated("rank", FieldKind::Uint)];
    static PROFILE_FIELDS: [FieldModel; 2] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated_with_storage_decode_nullability_write_policies_and_nested_fields(
            "profile",
            FieldKind::Structured { queryable: true },
            FieldStorageDecode::Value,
            false,
            None,
            None,
            &PROFILE_NESTED_FIELDS,
        ),
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
    static PROFILE_MODEL: EntityModel = entity_model_from_static(
        "schema::info::tests::ProfileEntity",
        "ProfileEntity",
        &PROFILE_FIELDS[0],
        0,
        &PROFILE_FIELDS,
        &INDEXES,
    );

    // Build one accepted schema whose second field deliberately differs from
    // generated metadata so tests can prove `SchemaInfo` follows the persisted
    // top-level authority.
    fn accepted_schema_with_name_kind(kind: PersistedFieldKind) -> AcceptedSchemaSnapshot {
        accepted_schema_with_name_kind_and_slots(
            kind,
            SchemaFieldSlot::new(1),
            SchemaFieldSlot::new(1),
        )
    }

    // Build one accepted schema fixture with independently selected layout and
    // field-snapshot slots. Owner-local tests use this to prove `SchemaInfo`
    // reads slot facts from accepted row layout, not duplicated field data.
    fn accepted_schema_with_name_kind_and_slots(
        kind: PersistedFieldKind,
        layout_slot: SchemaFieldSlot,
        field_slot: SchemaFieldSlot,
    ) -> AcceptedSchemaSnapshot {
        AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "schema::info::tests::Entity".to_string(),
            "Entity".to_string(),
            FieldId::new(1),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), layout_slot),
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
                    LeafCodec::StructuralFallback,
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "name".to_string(),
                    field_slot,
                    kind,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::StructuralFallback,
                ),
            ],
        ))
    }

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
        let snapshot = accepted_schema_with_name_kind(PersistedFieldKind::Blob);

        let schema = SchemaInfo::from_accepted_snapshot_for_model(&MODEL, &snapshot);
        let name_type = schema.field("name").expect("accepted field should exist");

        assert!(literal_matches_type(&Value::Blob(vec![1, 2, 3]), name_type));
        assert!(!literal_matches_type(
            &Value::Text("name".into()),
            name_type
        ));
    }

    #[test]
    fn accepted_snapshot_schema_info_canonicalizes_sql_literals_from_persisted_kind() {
        let generated = SchemaInfo::cached_for_entity_model(&MODEL);
        let snapshot = accepted_schema_with_name_kind(PersistedFieldKind::Uint);
        let accepted = SchemaInfo::from_accepted_snapshot_for_model(&MODEL, &snapshot);

        assert_eq!(
            generated.canonicalize_strict_sql_literal("name", &Value::Int(7)),
            None
        );
        assert_eq!(
            accepted.canonicalize_strict_sql_literal("name", &Value::Int(7)),
            Some(Value::Uint(7))
        );
    }

    #[test]
    fn accepted_snapshot_schema_info_uses_persisted_sql_capabilities() {
        let generated = SchemaInfo::cached_for_entity_model(&MODEL);
        let snapshot = accepted_schema_with_name_kind(PersistedFieldKind::Blob);
        let accepted = SchemaInfo::from_accepted_snapshot_for_model(&MODEL, &snapshot);

        let generated_name = generated
            .sql_capabilities("name")
            .expect("generated field capability should exist");
        let accepted_name = accepted
            .sql_capabilities("name")
            .expect("accepted field capability should exist");

        assert!(generated_name.orderable());
        assert!(accepted_name.selectable());
        assert!(accepted_name.comparable());
        assert!(!accepted_name.orderable());
    }

    #[test]
    fn accepted_snapshot_schema_info_uses_row_layout_slot_authority() {
        let generated = SchemaInfo::cached_for_entity_model(&MODEL);
        let snapshot = accepted_schema_with_name_kind_and_slots(
            PersistedFieldKind::Text { max_len: None },
            SchemaFieldSlot::new(9),
            SchemaFieldSlot::new(1),
        );
        let accepted = SchemaInfo::from_accepted_snapshot_for_model(&MODEL, &snapshot);

        assert_eq!(generated.field_slot_index("name"), Some(0));
        assert_eq!(accepted.field_slot_index("name"), Some(9));
    }

    #[test]
    fn accepted_snapshot_schema_info_uses_persisted_nested_leaf_type() {
        let accepted = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "schema::info::tests::ProfileEntity".to_string(),
            "ProfileEntity".to_string(),
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
                    LeafCodec::StructuralFallback,
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "profile".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Structured { queryable: true },
                    vec![PersistedNestedLeafSnapshot::new(
                        vec!["rank".to_string()],
                        PersistedFieldKind::Blob,
                        false,
                        FieldStorageDecode::ByKind,
                        LeafCodec::Scalar(ScalarCodec::Blob),
                    )],
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::Value,
                    LeafCodec::StructuralFallback,
                ),
            ],
        ));
        let schema = SchemaInfo::from_accepted_snapshot_for_model(&PROFILE_MODEL, &accepted);
        let path = vec!["rank".to_string()];
        let nested_type = schema
            .nested_field_type("profile", path.as_slice())
            .expect("accepted nested leaf should resolve");

        assert!(literal_matches_type(&Value::Blob(vec![1]), &nested_type));
        assert!(!literal_matches_type(&Value::Uint(1), &nested_type));
    }
}
