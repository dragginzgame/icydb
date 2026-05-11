//! Module: db::schema::info
//! Responsibility: schema model/index integrity checks used during schema info construction.
//! Does not own: query planning policy or runtime predicate evaluation.
//! Boundary: validates entity/index model consistency for predicate schema metadata.

use crate::{
    db::schema::{
        AcceptedSchemaSnapshot, FieldId, FieldType, PersistedFieldKind, PersistedFieldSnapshot,
        PersistedIndexExpressionOp, PersistedIndexFieldPathSnapshot, PersistedIndexKeyItemSnapshot,
        PersistedIndexKeySnapshot, PersistedIndexSnapshot, PersistedNestedLeafSnapshot,
        PersistedRelationStrength, PersistedSchemaSnapshot, SchemaFieldSlot, SqlCapabilities,
        canonicalize_strict_sql_literal_for_persisted_kind, field_type_from_model_kind,
        field_type_from_persisted_kind, sql_capabilities,
    },
    model::{
        canonicalize_strict_sql_literal_for_kind,
        entity::EntityModel,
        field::{FieldKind, FieldModel, LeafCodec},
        index::{IndexKeyItem, IndexKeyItemsRef, IndexModel},
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

// Attach generated index-membership facts to generated `SchemaInfo` views.
fn generated_field_is_indexed(model: &EntityModel, field_name: &str) -> bool {
    model
        .indexes()
        .iter()
        .any(|index| index.fields().contains(&field_name))
}

// Resolve top-level index membership from accepted persisted index contracts
// once per schema view. Runtime accepted schema views must not reopen generated
// `EntityModel` indexes after schema acceptance.
fn accepted_indexed_field_ids(snapshot: &PersistedSchemaSnapshot) -> Vec<FieldId> {
    let mut field_ids = Vec::new();

    for index in snapshot.indexes() {
        for field in snapshot.fields() {
            if index.key().references_field(field.id()) && !field_ids.contains(&field.id()) {
                field_ids.push(field.id());
            }
        }
    }

    field_ids
}

fn accepted_field_name(snapshot: &PersistedSchemaSnapshot, field_id: FieldId) -> Option<&str> {
    snapshot
        .fields()
        .iter()
        .find(|field| field.id() == field_id)
        .map(PersistedFieldSnapshot::name)
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
    nullable: bool,
    leaf_codec: LeafCodec,
    sql_capabilities: SqlCapabilities,
    persisted_kind: Option<PersistedFieldKind>,
    indexed: bool,
    nested_leaves: Option<Vec<PersistedNestedLeafSnapshot>>,
    nested_fields: &'static [FieldModel],
}

///
/// SchemaIndexInfo
///
/// Compact field-path index contract exposed by `SchemaInfo`.
/// Accepted schema views source this from persisted index snapshots; generated
/// schema views source it from generated field-only index metadata for
/// proposal/model-only compatibility callers.
///
#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "0.150 staged accepted-index authority surface; planner/explain routing consumes this DTO in the next runtime slice"
)]
pub(in crate::db) struct SchemaIndexInfo {
    ordinal: u16,
    name: String,
    store: String,
    unique: bool,
    fields: Vec<SchemaIndexFieldPathInfo>,
    predicate_sql: Option<String>,
}

#[allow(
    dead_code,
    reason = "0.150 staged accepted-index authority surface; planner/explain routing consumes this DTO in the next runtime slice"
)]
impl SchemaIndexInfo {
    /// Return the accepted or generated stable per-entity index ordinal.
    #[must_use]
    pub(in crate::db) const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    /// Borrow the stable index name.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Borrow the backing index store path.
    #[must_use]
    pub(in crate::db) const fn store(&self) -> &str {
        self.store.as_str()
    }

    /// Return whether this index enforces value uniqueness.
    #[must_use]
    pub(in crate::db) const fn unique(&self) -> bool {
        self.unique
    }

    /// Borrow accepted field-path key item metadata for this index.
    #[must_use]
    pub(in crate::db) const fn fields(&self) -> &[SchemaIndexFieldPathInfo] {
        self.fields.as_slice()
    }

    /// Borrow optional predicate SQL display metadata.
    #[must_use]
    pub(in crate::db) const fn predicate_sql(&self) -> Option<&str> {
        match &self.predicate_sql {
            Some(sql) => Some(sql.as_str()),
            None => None,
        }
    }
}

///
/// SchemaExpressionIndexInfo
///
/// Compact accepted expression-index contract exposed by `SchemaInfo`.
/// Accepted schema views source this from persisted index snapshots so
/// expression-index runtime routing can stop reopening generated `IndexModel`.
///
#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "0.151 stages accepted expression-index authority for the next planner/write routing slice"
)]
pub(in crate::db) struct SchemaExpressionIndexInfo {
    ordinal: u16,
    name: String,
    store: String,
    unique: bool,
    key_items: Vec<SchemaExpressionIndexKeyItemInfo>,
    predicate_sql: Option<String>,
}

#[allow(
    dead_code,
    reason = "0.151 stages accepted expression-index authority for the next planner/write routing slice"
)]
impl SchemaExpressionIndexInfo {
    /// Return the accepted stable per-entity index ordinal.
    #[must_use]
    pub(in crate::db) const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    /// Borrow the accepted stable index name.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Borrow the accepted backing index store path.
    #[must_use]
    pub(in crate::db) const fn store(&self) -> &str {
        self.store.as_str()
    }

    /// Return whether this accepted expression index enforces uniqueness.
    #[must_use]
    pub(in crate::db) const fn unique(&self) -> bool {
        self.unique
    }

    /// Borrow accepted key-item contracts in index key order.
    #[must_use]
    pub(in crate::db) const fn key_items(&self) -> &[SchemaExpressionIndexKeyItemInfo] {
        self.key_items.as_slice()
    }

    /// Borrow optional accepted index-membership predicate SQL metadata.
    #[must_use]
    pub(in crate::db) const fn predicate_sql(&self) -> Option<&str> {
        match &self.predicate_sql {
            Some(sql) => Some(sql.as_str()),
            None => None,
        }
    }
}

///
/// SchemaExpressionIndexKeyItemInfo
///
/// Accepted expression-index key item surfaced through `SchemaInfo`.
///
#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "0.151 stages accepted expression-index authority for the next planner/write routing slice"
)]
pub(in crate::db) enum SchemaExpressionIndexKeyItemInfo {
    FieldPath(SchemaIndexFieldPathInfo),
    Expression(Box<SchemaIndexExpressionInfo>),
}

#[allow(
    dead_code,
    reason = "0.151 stages accepted expression-index authority for the next planner/write routing slice"
)]
impl SchemaExpressionIndexKeyItemInfo {
    /// Borrow this key item as a field-path component, when applicable.
    #[must_use]
    pub(in crate::db) const fn field_path(&self) -> Option<&SchemaIndexFieldPathInfo> {
        match self {
            Self::FieldPath(field_path) => Some(field_path),
            Self::Expression(_) => None,
        }
    }

    /// Borrow this key item as an expression component, when applicable.
    #[must_use]
    pub(in crate::db) fn expression(&self) -> Option<&SchemaIndexExpressionInfo> {
        match self {
            Self::FieldPath(_) => None,
            Self::Expression(expression) => Some(expression.as_ref()),
        }
    }
}

///
/// SchemaIndexExpressionInfo
///
/// Compact accepted expression key contract for one expression-index key item.
///
#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "0.151 stages accepted expression-index authority for the next planner/write routing slice"
)]
pub(in crate::db) struct SchemaIndexExpressionInfo {
    op: PersistedIndexExpressionOp,
    source: SchemaIndexFieldPathInfo,
    input_kind: PersistedFieldKind,
    output_kind: PersistedFieldKind,
    canonical_text: String,
}

#[allow(
    dead_code,
    reason = "0.151 stages accepted expression-index authority for the next planner/write routing slice"
)]
impl SchemaIndexExpressionInfo {
    /// Return the accepted expression operation.
    #[must_use]
    pub(in crate::db) const fn op(&self) -> PersistedIndexExpressionOp {
        self.op
    }

    /// Borrow the accepted source field-path contract.
    #[must_use]
    pub(in crate::db) const fn source(&self) -> &SchemaIndexFieldPathInfo {
        &self.source
    }

    /// Borrow the accepted expression input kind.
    #[must_use]
    pub(in crate::db) const fn input_kind(&self) -> &PersistedFieldKind {
        &self.input_kind
    }

    /// Borrow the accepted expression output kind.
    #[must_use]
    pub(in crate::db) const fn output_kind(&self) -> &PersistedFieldKind {
        &self.output_kind
    }

    /// Borrow the accepted canonical expression text.
    #[must_use]
    pub(in crate::db) const fn canonical_text(&self) -> &str {
        self.canonical_text.as_str()
    }
}

///
/// SchemaIndexFieldPathInfo
///
/// Compact key-item contract for one field-path index component.
/// Accepted schema views carry durable field IDs and persisted kinds; generated
/// compatibility views omit field IDs until generated metadata is reconciled.
///
#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "0.150 staged accepted-index authority surface; planner/explain routing consumes this DTO in the next runtime slice"
)]
pub(in crate::db) struct SchemaIndexFieldPathInfo {
    field_id: Option<FieldId>,
    field_name: String,
    slot: usize,
    path: Vec<String>,
    ty: FieldType,
    persisted_kind: Option<PersistedFieldKind>,
    nullable: bool,
}

#[allow(
    dead_code,
    reason = "0.150 staged accepted-index authority surface; planner/explain routing consumes this DTO in the next runtime slice"
)]
impl SchemaIndexFieldPathInfo {
    /// Return the accepted durable top-level field ID, when this came from a
    /// persisted schema snapshot.
    #[must_use]
    pub(in crate::db) const fn field_id(&self) -> Option<FieldId> {
        self.field_id
    }

    /// Borrow the top-level field name for this key item.
    #[must_use]
    pub(in crate::db) const fn field_name(&self) -> &str {
        self.field_name.as_str()
    }

    /// Return the schema-owned top-level row slot for this key item.
    #[must_use]
    pub(in crate::db) const fn slot(&self) -> usize {
        self.slot
    }

    /// Borrow the accepted field path for this key item.
    #[must_use]
    pub(in crate::db) const fn path(&self) -> &[String] {
        self.path.as_slice()
    }

    /// Borrow reduced predicate/query type metadata for this key item.
    #[must_use]
    pub(in crate::db) const fn ty(&self) -> &FieldType {
        &self.ty
    }

    /// Borrow the persisted field kind, when this key item came from accepted
    /// schema authority.
    #[must_use]
    pub(in crate::db) const fn persisted_kind(&self) -> Option<&PersistedFieldKind> {
        match &self.persisted_kind {
            Some(kind) => Some(kind),
            None => None,
        }
    }

    /// Return whether this key item permits explicit persisted `NULL`.
    #[must_use]
    pub(in crate::db) const fn nullable(&self) -> bool {
        self.nullable
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SchemaInfo {
    fields: Vec<SchemaFieldEntry>,
    indexes: Vec<SchemaIndexInfo>,
    expression_indexes: Vec<SchemaExpressionIndexInfo>,
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
                        nullable: field.nullable(),
                        leaf_codec: field.leaf_codec(),
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
            indexes: Vec::new(),
            expression_indexes: Vec::new(),
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
        schema.indexes = model
            .indexes()
            .iter()
            .filter_map(|index| schema_index_info_from_generated_index(index, &schema.fields))
            .collect();
        schema.expression_indexes = Vec::new();

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

    /// Return whether one top-level field permits explicit persisted `NULL`.
    ///
    /// Accepted schema views source this from persisted field snapshots, while
    /// generated schema views retain generated field metadata for test-only
    /// compatibility callers.
    #[must_use]
    pub(in crate::db) fn field_nullable(&self, name: &str) -> Option<bool> {
        schema_field_info(self.fields.as_slice(), name).map(|field| field.nullable)
    }

    /// Return whether one top-level row slot is backed by a scalar leaf codec.
    ///
    /// Accepted schema views source this from persisted field snapshots, giving
    /// predicate fast-path classification schema authority instead of generated
    /// model field tables.
    #[must_use]
    pub(in crate::db) fn field_slot_has_scalar_leaf(&self, slot: usize) -> bool {
        self.fields
            .iter()
            .find(|(_, field)| field.slot == slot)
            .is_some_and(|(_, field)| matches!(field.leaf_codec, LeafCodec::Scalar(_)))
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

    /// Return whether one top-level field participates in any index.
    ///
    /// Accepted schema views source this from persisted index contracts.
    /// Generated schema views source it from generated index metadata for
    /// proposal/model-only callers.
    #[must_use]
    pub(in crate::db) fn field_is_indexed(&self, name: &str) -> bool {
        schema_field_info(self.fields.as_slice(), name).is_some_and(|field| field.indexed)
    }

    /// Borrow field-path index contracts visible through this schema view.
    ///
    /// Accepted schema views source this from persisted index contracts.
    /// Generated schema views source it from generated field-only indexes for
    /// proposal/model-only compatibility.
    #[must_use]
    #[allow(
        dead_code,
        reason = "0.150 staged accepted-index authority surface; planner/explain routing consumes this accessor in the next runtime slice"
    )]
    pub(in crate::db) const fn field_path_indexes(&self) -> &[SchemaIndexInfo] {
        self.indexes.as_slice()
    }

    /// Borrow accepted expression-index contracts visible through this schema view.
    ///
    /// Accepted schema views source this from persisted expression index
    /// contracts. Generated schema views leave this empty until generated
    /// expression indexes have been reconciled into accepted metadata.
    #[must_use]
    #[allow(
        dead_code,
        reason = "0.151 stages accepted expression-index authority for the next planner/write routing slice"
    )]
    pub(in crate::db) const fn expression_indexes(&self) -> &[SchemaExpressionIndexInfo] {
        self.expression_indexes.as_slice()
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
        Self::from_accepted_snapshot_for_model_with_expression_indexes(model, schema, false)
    }

    /// Build one owned schema view from an accepted persisted snapshot and
    /// optionally project accepted expression-index metadata.
    ///
    /// Expression-index metadata is intentionally opt-in until planner/write
    /// routing consumes it. The ordinary accepted `SchemaInfo` path is hot
    /// during query loops and should not pay for staged DTO projection.
    #[must_use]
    pub(in crate::db) fn from_accepted_snapshot_for_model_with_expression_indexes(
        model: &EntityModel,
        schema: &AcceptedSchemaSnapshot,
        include_expression_indexes: bool,
    ) -> Self {
        let snapshot = schema.persisted_snapshot();
        let indexed_field_ids = accepted_indexed_field_ids(snapshot);
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
                        nullable: field.nullable(),
                        leaf_codec: field.leaf_codec(),
                        sql_capabilities: sql_capabilities(field.kind()),
                        persisted_kind: Some(field.kind().clone()),
                        indexed: indexed_field_ids.contains(&field.id()),
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
            indexes: snapshot
                .indexes()
                .iter()
                .filter_map(|index| schema_index_info_from_accepted_index(index, snapshot))
                .collect(),
            expression_indexes: snapshot
                .indexes()
                .iter()
                .filter_map(|index| {
                    include_expression_indexes
                        .then(|| schema_expression_index_info_from_accepted_index(index, snapshot))
                        .flatten()
                })
                .collect(),
            entity_name: Some(schema.entity_name().to_string()),
            primary_key_name,
            has_any_strong_relations: snapshot
                .fields()
                .iter()
                .any(|field| persisted_kind_has_strong_relation(field.kind())),
        }
    }

    /// Build one accepted schema view with expression-index metadata projected.
    ///
    /// This constructor exists for the 0.151 expression-index routing slice and
    /// tests that need to inspect accepted expression contracts without adding
    /// allocation work to every existing accepted schema view.
    #[must_use]
    #[cfg(test)]
    fn from_accepted_snapshot_for_model_including_expression_indexes(
        model: &EntityModel,
        schema: &AcceptedSchemaSnapshot,
    ) -> Self {
        Self::from_accepted_snapshot_for_model_with_expression_indexes(model, schema, true)
    }

    /// Return one cached schema view for a trusted generated entity model.
    pub(crate) fn cached_for_generated_entity_model(model: &EntityModel) -> &'static Self {
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

fn schema_index_info_from_generated_index(
    index: &IndexModel,
    fields: &[SchemaFieldEntry],
) -> Option<SchemaIndexInfo> {
    let key_fields = generated_index_field_names(index)?
        .into_iter()
        .map(|field_name| {
            let field = schema_field_info(fields, field_name)?;
            Some(SchemaIndexFieldPathInfo {
                field_id: None,
                field_name: field_name.to_string(),
                slot: field.slot,
                path: vec![field_name.to_string()],
                ty: field.ty.clone(),
                persisted_kind: None,
                nullable: field.nullable,
            })
        })
        .collect::<Option<Vec<_>>>()?;

    Some(SchemaIndexInfo {
        ordinal: index.ordinal(),
        name: index.name().to_string(),
        store: index.store().to_string(),
        unique: index.is_unique(),
        fields: key_fields,
        predicate_sql: index.predicate().map(str::to_string),
    })
}

fn generated_index_field_names(index: &IndexModel) -> Option<Vec<&'static str>> {
    match index.key_items() {
        IndexKeyItemsRef::Fields(fields) => Some(fields.to_vec()),
        IndexKeyItemsRef::Items(items) => items
            .iter()
            .map(|item| match item {
                IndexKeyItem::Field(field) => Some(*field),
                IndexKeyItem::Expression(_) => None,
            })
            .collect(),
    }
}

fn schema_index_info_from_accepted_index(
    index: &PersistedIndexSnapshot,
    snapshot: &PersistedSchemaSnapshot,
) -> Option<SchemaIndexInfo> {
    if !index.key().is_field_path_only() {
        return None;
    }

    Some(SchemaIndexInfo {
        ordinal: index.ordinal(),
        name: index.name().to_string(),
        store: index.store().to_string(),
        unique: index.unique(),
        fields: index
            .key()
            .field_paths()
            .iter()
            .map(|path| schema_index_field_path_info_from_accepted(path, snapshot))
            .collect(),
        predicate_sql: index.predicate_sql().map(str::to_string),
    })
}

fn schema_expression_index_info_from_accepted_index(
    index: &PersistedIndexSnapshot,
    snapshot: &PersistedSchemaSnapshot,
) -> Option<SchemaExpressionIndexInfo> {
    let PersistedIndexKeySnapshot::Items(items) = index.key() else {
        return None;
    };

    if !items
        .iter()
        .any(|item| matches!(item, PersistedIndexKeyItemSnapshot::Expression(_)))
    {
        return None;
    }

    Some(SchemaExpressionIndexInfo {
        ordinal: index.ordinal(),
        name: index.name().to_string(),
        store: index.store().to_string(),
        unique: index.unique(),
        key_items: items
            .iter()
            .map(|item| schema_expression_index_key_item_info(item, snapshot))
            .collect(),
        predicate_sql: index.predicate_sql().map(str::to_string),
    })
}

fn schema_expression_index_key_item_info(
    item: &PersistedIndexKeyItemSnapshot,
    snapshot: &PersistedSchemaSnapshot,
) -> SchemaExpressionIndexKeyItemInfo {
    match item {
        PersistedIndexKeyItemSnapshot::FieldPath(path) => {
            SchemaExpressionIndexKeyItemInfo::FieldPath(schema_index_field_path_info_from_accepted(
                path, snapshot,
            ))
        }
        PersistedIndexKeyItemSnapshot::Expression(expression) => {
            SchemaExpressionIndexKeyItemInfo::Expression(Box::new(SchemaIndexExpressionInfo {
                op: expression.op(),
                source: schema_index_field_path_info_from_accepted(expression.source(), snapshot),
                input_kind: expression.input_kind().clone(),
                output_kind: expression.output_kind().clone(),
                canonical_text: expression.canonical_text().to_string(),
            }))
        }
    }
}

fn schema_index_field_path_info_from_accepted(
    path: &PersistedIndexFieldPathSnapshot,
    snapshot: &PersistedSchemaSnapshot,
) -> SchemaIndexFieldPathInfo {
    let field_name = accepted_field_name(snapshot, path.field_id())
        .or_else(|| path.path().first().map(String::as_str))
        .unwrap_or_default()
        .to_string();

    SchemaIndexFieldPathInfo {
        field_id: Some(path.field_id()),
        field_name,
        slot: accepted_slot_index(path.slot()),
        path: path.path().to_vec(),
        ty: field_type_from_persisted_kind(path.kind()),
        persisted_kind: Some(path.kind().clone()),
        nullable: path.nullable(),
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
            PersistedIndexExpressionOp, PersistedIndexExpressionSnapshot,
            PersistedIndexFieldPathSnapshot, PersistedIndexKeyItemSnapshot,
            PersistedIndexKeySnapshot, PersistedIndexSnapshot, PersistedNestedLeafSnapshot,
            PersistedRelationStrength, PersistedSchemaSnapshot, SchemaFieldDefault,
            SchemaFieldSlot, SchemaInfo, SchemaRowLayout, SchemaVersion, literal_matches_type,
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

    fn accepted_schema_with_name_index() -> AcceptedSchemaSnapshot {
        AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new_with_indexes(
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
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::StructuralFallback,
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "name".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Text { max_len: None },
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::StructuralFallback,
                ),
            ],
            vec![PersistedIndexSnapshot::new(
                1,
                "schema_info_name".to_string(),
                "schema::info::tests::name".to_string(),
                false,
                PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                    FieldId::new(2),
                    SchemaFieldSlot::new(1),
                    vec!["name".to_string()],
                    PersistedFieldKind::Text { max_len: None },
                    false,
                )]),
                None,
            )],
        ))
    }

    fn accepted_schema_with_lower_name_index() -> AcceptedSchemaSnapshot {
        let source = PersistedIndexFieldPathSnapshot::new(
            FieldId::new(2),
            SchemaFieldSlot::new(1),
            vec!["name".to_string()],
            PersistedFieldKind::Text { max_len: None },
            false,
        );

        AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new_with_indexes(
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
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::StructuralFallback,
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "name".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Text { max_len: None },
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::StructuralFallback,
                ),
            ],
            vec![PersistedIndexSnapshot::new(
                2,
                "schema_info_lower_name".to_string(),
                "schema::info::tests::lower_name".to_string(),
                true,
                PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::Expression(
                    Box::new(PersistedIndexExpressionSnapshot::new(
                        PersistedIndexExpressionOp::Lower,
                        source,
                        PersistedFieldKind::Text { max_len: None },
                        PersistedFieldKind::Text { max_len: None },
                        "expr:v1:LOWER(name)".to_string(),
                    )),
                )]),
                Some("name IS NOT NULL".to_string()),
            )],
        ))
    }

    #[test]
    fn cached_for_generated_entity_model_reuses_one_schema_instance() {
        let first = SchemaInfo::cached_for_generated_entity_model(&MODEL);
        let second = SchemaInfo::cached_for_generated_entity_model(&MODEL);

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
        let generated = SchemaInfo::cached_for_generated_entity_model(&MODEL);
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
        let generated = SchemaInfo::cached_for_generated_entity_model(&MODEL);
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
        let generated = SchemaInfo::cached_for_generated_entity_model(&MODEL);
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
    fn accepted_snapshot_schema_info_uses_persisted_index_membership() {
        let generated = SchemaInfo::cached_for_generated_entity_model(&INDEXED_MODEL);
        let unindexed_snapshot =
            accepted_schema_with_name_kind(PersistedFieldKind::Text { max_len: None });
        let indexed_snapshot = accepted_schema_with_name_index();
        let accepted_unindexed =
            SchemaInfo::from_accepted_snapshot_for_model(&INDEXED_MODEL, &unindexed_snapshot);
        let accepted_indexed =
            SchemaInfo::from_accepted_snapshot_for_model(&MODEL, &indexed_snapshot);

        assert!(generated.field_is_indexed("name"));
        assert!(!generated.field_is_indexed("id"));
        assert!(
            !accepted_unindexed.field_is_indexed("name"),
            "accepted SchemaInfo must not inherit generated index membership when the accepted snapshot has no index contract",
        );
        assert!(accepted_indexed.field_is_indexed("name"));
        assert!(!accepted_indexed.field_is_indexed("id"));
        assert!(accepted_unindexed.field_path_indexes().is_empty());
    }

    #[test]
    fn accepted_snapshot_schema_info_exposes_persisted_field_path_indexes() {
        let snapshot = accepted_schema_with_name_index();
        let accepted = SchemaInfo::from_accepted_snapshot_for_model(&MODEL, &snapshot);
        let indexes = accepted.field_path_indexes();

        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].ordinal(), 1);
        assert_eq!(indexes[0].name(), "schema_info_name");
        assert_eq!(indexes[0].store(), "schema::info::tests::name");
        assert!(!indexes[0].unique());
        assert_eq!(indexes[0].predicate_sql(), None);

        let fields = indexes[0].fields();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].field_id(), Some(FieldId::new(2)));
        assert_eq!(fields[0].field_name(), "name");
        assert_eq!(fields[0].slot(), 1);
        assert_eq!(fields[0].path(), &["name".to_string()]);
        assert_eq!(
            fields[0].persisted_kind(),
            Some(&PersistedFieldKind::Text { max_len: None })
        );
        assert!(fields[0].ty().is_text());
        assert!(!fields[0].nullable());
    }

    #[test]
    fn accepted_snapshot_schema_info_exposes_persisted_expression_indexes() {
        let snapshot = accepted_schema_with_lower_name_index();
        let accepted = SchemaInfo::from_accepted_snapshot_for_model_including_expression_indexes(
            &MODEL, &snapshot,
        );

        assert!(
            accepted.field_path_indexes().is_empty(),
            "field-path visibility should stay field-path-only until expression planner routing moves over",
        );
        assert!(
            accepted.field_is_indexed("name"),
            "accepted expression indexes should still count as index membership for their source field",
        );

        let indexes = accepted.expression_indexes();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].ordinal(), 2);
        assert_eq!(indexes[0].name(), "schema_info_lower_name");
        assert_eq!(indexes[0].store(), "schema::info::tests::lower_name");
        assert!(indexes[0].unique());
        assert_eq!(indexes[0].predicate_sql(), Some("name IS NOT NULL"));

        let key_items = indexes[0].key_items();
        assert_eq!(key_items.len(), 1);
        let Some(expression) = key_items[0].expression() else {
            panic!("accepted expression index should expose an expression key item");
        };
        assert_eq!(expression.op(), PersistedIndexExpressionOp::Lower);
        assert_eq!(expression.canonical_text(), "expr:v1:LOWER(name)");
        assert_eq!(
            expression.input_kind(),
            &PersistedFieldKind::Text { max_len: None }
        );
        assert_eq!(
            expression.output_kind(),
            &PersistedFieldKind::Text { max_len: None }
        );

        let source = expression.source();
        assert_eq!(source.field_id(), Some(FieldId::new(2)));
        assert_eq!(source.field_name(), "name");
        assert_eq!(source.slot(), 1);
        assert_eq!(source.path(), &["name".to_string()]);

        assert!(matches!(
            &key_items[0],
            super::SchemaExpressionIndexKeyItemInfo::Expression(_)
        ));
    }

    #[test]
    fn accepted_snapshot_schema_info_uses_persisted_strong_relation_authority() {
        let generated = SchemaInfo::cached_for_generated_entity_model(&MODEL);
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
