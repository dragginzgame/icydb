//! Module: db::schema::info
//! Responsibility: schema model/index integrity checks used during schema info construction.
//! Does not own: query planning policy or runtime predicate evaluation.
//! Boundary: validates entity/index model consistency for predicate schema metadata.

use crate::{
    db::schema::{
        AcceptedSchemaSnapshot, FieldType, PersistedFieldKind, PersistedNestedLeafSnapshot,
        PersistedRelationStrength, SchemaFieldSlot, SqlCapabilities,
        canonicalize_strict_sql_literal_for_persisted_kind, field_type_from_model_kind,
        field_type_from_persisted_kind, sql_capabilities,
    },
    model::{
        canonicalize_strict_sql_literal_for_kind,
        entity::EntityModel,
        field::{FieldKind, FieldModel},
    },
    value::Value,
};
use std::sync::{Mutex, OnceLock};

type SchemaFieldEntry = (String, SchemaFieldInfo);
type CachedSchemaEntries = Vec<(&'static str, &'static SchemaInfo)>;
const EMPTY_GENERATED_NESTED_FIELDS: &[FieldModel] = &[];

fn schema_field_info<'a>(
    fields: &'a [SchemaFieldEntry],
    name: &str,
) -> Option<&'a SchemaFieldInfo> {
    fields
        .binary_search_by(|(field_name, _)| field_name.as_str().cmp(name))
        .ok()
        .map(|index| &fields[index].1)
}

fn generated_field_by_name<'a>(
    model: &'a EntityModel,
    field_name: &str,
) -> Option<(usize, &'a FieldModel)> {
    model
        .fields()
        .iter()
        .enumerate()
        .find(|(_, field)| field.name() == field_name)
}

// Attach generated index-membership facts to `SchemaInfo` while accepted
// snapshots do not yet persist their own index definitions.
fn generated_field_is_indexed(model: &EntityModel, field_name: &str) -> bool {
    model
        .indexes()
        .iter()
        .any(|index| index.fields().contains(&field_name))
}

// Convert a schema-owned row-layout slot into the usize slot surface consumed
// by planner and executor DTOs.
fn accepted_slot_index(slot: SchemaFieldSlot) -> usize {
    usize::from(slot.get())
}

fn persisted_kind_has_strong_relation(kind: &PersistedFieldKind) -> bool {
    match kind {
        PersistedFieldKind::Relation { strength, .. } => {
            *strength == PersistedRelationStrength::Strong
        }
        PersistedFieldKind::List(inner) | PersistedFieldKind::Set(inner) => {
            persisted_kind_has_strong_relation(inner)
        }
        _ => false,
    }
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
/// Keeps reduced predicate type metadata and the temporary generated field-kind
/// bridge in one table while accepted persisted facts become the field-list
/// authority for SQL/session paths.
///

#[derive(Clone, Debug)]
struct SchemaFieldInfo {
    slot: usize,
    ty: FieldType,
    kind: Option<FieldKind>,
    sql_capabilities: SqlCapabilities,
    persisted_kind: Option<PersistedFieldKind>,
    indexed: bool,
    nested_leaves: Option<Vec<PersistedNestedLeafSnapshot>>,
    nested_fields: &'static [FieldModel],
}

#[derive(Clone, Debug)]
pub(crate) struct SchemaInfo {
    fields: Vec<SchemaFieldEntry>,
    entity_name: Option<String>,
    primary_key_name: Option<String>,
    has_any_strong_relations: bool,
}

impl SchemaInfo {
    // Build one compact field table from trusted generated field metadata.
    fn from_trusted_field_models(fields: &[FieldModel]) -> Self {
        let mut fields = fields
            .iter()
            .enumerate()
            .map(|(slot, field)| {
                (
                    field.name().to_string(),
                    SchemaFieldInfo {
                        slot,
                        ty: field_type_from_model_kind(&field.kind()),
                        kind: Some(field.kind()),
                        sql_capabilities: sql_capabilities(&PersistedFieldKind::from_model_kind(
                            field.kind(),
                        )),
                        persisted_kind: None,
                        indexed: false,
                        nested_leaves: None,
                        nested_fields: field.nested_fields(),
                    },
                )
            })
            .collect::<Vec<_>>();

        fields.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));

        Self {
            fields,
            entity_name: None,
            primary_key_name: None,
            has_any_strong_relations: false,
        }
    }

    // Build one compact field table from trusted generated entity metadata.
    fn from_trusted_entity_model(model: &EntityModel) -> Self {
        let mut schema = Self::from_trusted_field_models(model.fields());
        schema.entity_name = Some(model.name().to_string());
        schema.primary_key_name = Some(model.primary_key().name().to_string());
        schema.has_any_strong_relations = model.has_any_strong_relations();

        for (field_name, field) in &mut schema.fields {
            field.indexed = generated_field_is_indexed(model, field_name.as_str());
        }

        schema
    }

    #[must_use]
    pub(crate) fn field(&self, name: &str) -> Option<&FieldType> {
        schema_field_info(self.fields.as_slice(), name).map(|field| &field.ty)
    }

    #[must_use]
    pub(crate) fn field_kind(&self, name: &str) -> Option<&FieldKind> {
        schema_field_info(self.fields.as_slice(), name).and_then(|field| field.kind.as_ref())
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

    /// Borrow the schema-owned entity name when this schema view was built
    /// from an entity model or accepted persisted snapshot.
    #[must_use]
    pub(in crate::db) fn entity_name(&self) -> Option<&str> {
        self.entity_name.as_deref()
    }

    /// Borrow the schema-owned primary-key field name when this schema view
    /// was built from an entity model or accepted persisted snapshot.
    #[must_use]
    pub(in crate::db) fn primary_key_name(&self) -> Option<&str> {
        self.primary_key_name.as_deref()
    }

    /// Return whether this entity has any strong relation checks.
    ///
    /// Relation metadata is still generated-model authority, but save
    /// orchestration reads the reduced boolean from `SchemaInfo` so it does not
    /// reopen `E::MODEL` at every write entrypoint.
    #[must_use]
    pub(in crate::db) const fn has_any_strong_relations(&self) -> bool {
        self.has_any_strong_relations
    }

    /// Return whether one top-level field participates in any generated index.
    ///
    /// Accepted schema snapshots do not yet persist index definitions, so the
    /// field-index flag remains a generated compatibility fact attached to the
    /// schema info boundary instead of being rediscovered by write validators.
    #[must_use]
    pub(in crate::db) fn field_is_indexed(&self, name: &str) -> bool {
        schema_field_info(self.fields.as_slice(), name).is_some_and(|field| field.indexed)
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
    pub(in crate::db) fn first_non_sql_selectable_field(&self) -> Option<&str> {
        self.fields
            .iter()
            .find(|(_, field)| !field.sql_capabilities.selectable())
            .map(|(field_name, _)| field_name.as_str())
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

        if let Some(kind) = field.persisted_kind.as_ref() {
            return canonicalize_strict_sql_literal_for_persisted_kind(kind, value);
        }

        field
            .kind
            .as_ref()
            .and_then(|kind| canonicalize_strict_sql_literal_for_kind(kind, value))
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
        let snapshot = schema.persisted_snapshot();
        let mut fields = snapshot
            .fields()
            .iter()
            .map(|field| {
                let generated_field = generated_field_by_name(model, field.name());
                let slot = snapshot
                    .row_layout()
                    .slot_for_field(field.id())
                    .map_or_else(|| usize::from(field.slot().get()), accepted_slot_index);
                let generated_kind = generated_field.map(|(_, field)| field.kind());
                let generated_nested_fields = generated_field
                    .map_or(EMPTY_GENERATED_NESTED_FIELDS, |(_, field)| {
                        field.nested_fields()
                    });

                (
                    field.name().to_string(),
                    SchemaFieldInfo {
                        slot,
                        ty: field_type_from_persisted_kind(field.kind()),
                        kind: generated_kind,
                        sql_capabilities: sql_capabilities(field.kind()),
                        persisted_kind: Some(field.kind().clone()),
                        indexed: generated_field_is_indexed(model, field.name()),
                        nested_leaves: Some(field.nested_leaves().to_vec()),
                        nested_fields: generated_nested_fields,
                    },
                )
            })
            .collect::<Vec<_>>();

        fields.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));

        let primary_key_name = snapshot
            .fields()
            .iter()
            .find(|field| field.id() == snapshot.primary_key_field_id())
            .map(|field| field.name().to_string());

        Self {
            fields,
            entity_name: Some(schema.entity_name().to_string()),
            primary_key_name,
            has_any_strong_relations: snapshot
                .fields()
                .iter()
                .any(|field| persisted_kind_has_strong_relation(field.kind())),
        }
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
            PersistedNestedLeafSnapshot, PersistedRelationStrength, PersistedSchemaSnapshot,
            SchemaFieldDefault, SchemaFieldSlot, SchemaInfo, SchemaRowLayout, SchemaVersion,
            literal_matches_type,
        },
        model::{
            entity::EntityModel,
            field::{FieldKind, FieldModel, FieldStorageDecode, LeafCodec, ScalarCodec},
            index::IndexModel,
        },
        testing::entity_model_from_static,
        types::EntityTag,
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
    static NAME_INDEX_FIELDS: [&str; 1] = ["name"];
    static NAME_INDEX: IndexModel = IndexModel::generated(
        "schema_info_name",
        "schema::info::tests::name",
        &NAME_INDEX_FIELDS,
        false,
    );
    static INDEXED_INDEXES: [&IndexModel; 1] = [&NAME_INDEX];
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
    static INDEXED_MODEL: EntityModel = entity_model_from_static(
        "schema::info::tests::IndexedEntity",
        "IndexedEntity",
        &FIELDS[1],
        1,
        &FIELDS,
        &INDEXED_INDEXES,
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
        let snapshot = accepted_schema_with_name_kind(PersistedFieldKind::Blob { max_len: None });

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
        let snapshot = accepted_schema_with_name_kind(PersistedFieldKind::Blob { max_len: None });
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
        assert_eq!(generated.entity_name(), Some("Entity"));
        assert_eq!(accepted.entity_name(), Some("Entity"));
        assert_eq!(generated.primary_key_name(), Some("id"));
        assert_eq!(accepted.primary_key_name(), Some("id"));
    }

    #[test]
    fn schema_info_keeps_index_membership_at_schema_boundary() {
        let generated = SchemaInfo::cached_for_entity_model(&INDEXED_MODEL);
        let snapshot = accepted_schema_with_name_kind(PersistedFieldKind::Text { max_len: None });
        let accepted = SchemaInfo::from_accepted_snapshot_for_model(&INDEXED_MODEL, &snapshot);

        assert!(generated.field_is_indexed("name"));
        assert!(!generated.field_is_indexed("id"));
        assert!(accepted.field_is_indexed("name"));
        assert!(!accepted.field_is_indexed("id"));
    }

    #[test]
    fn accepted_snapshot_schema_info_uses_persisted_strong_relation_authority() {
        let generated = SchemaInfo::cached_for_entity_model(&MODEL);
        let accepted_relation = accepted_schema_with_name_kind(PersistedFieldKind::Relation {
            target_path: "schema::info::tests::Target".to_string(),
            target_entity_name: "Target".to_string(),
            target_entity_tag: EntityTag::new(7),
            target_store_path: "schema::info::tests::target_store".to_string(),
            key_kind: Box::new(PersistedFieldKind::Ulid),
            strength: PersistedRelationStrength::Strong,
        });
        let accepted = SchemaInfo::from_accepted_snapshot_for_model(&MODEL, &accepted_relation);

        assert!(!generated.has_any_strong_relations());
        assert!(accepted.has_any_strong_relations());
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
                        PersistedFieldKind::Blob { max_len: None },
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
