//! Module: db::schema::info
//! Responsibility: schema model/index integrity checks used during schema info construction.
//! Does not own: query planning policy or runtime predicate evaluation.
//! Boundary: validates entity/index model consistency for predicate schema metadata.

#[cfg(feature = "sql")]
use crate::db::schema::{
    SqlCapabilities, sql_capabilities, sql_capabilities_for_model_kind,
    sql_capabilities_with_enum_catalog,
};
use crate::{
    db::schema::{
        AcceptedEnumCatalog, AcceptedEnumCatalogHandle, AcceptedFieldKind, AcceptedSchemaSnapshot,
        AcceptedValueAdmissionContract, FieldId, FieldType, PersistedFieldSnapshot,
        PersistedIndexExpressionOp, PersistedIndexFieldPathSnapshot, PersistedIndexKeyItemSnapshot,
        PersistedIndexKeySnapshot, PersistedIndexSnapshot, PersistedNestedLeafSnapshot,
        PersistedSchemaSnapshot, SchemaFieldSlot, enum_catalog::AcceptedValueContract,
        field_type_from_model_kind, field_type_from_persisted_kind,
    },
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel, FieldStorageDecode, LeafCodec},
        index::{IndexKeyItem, IndexKeyItemsRef, IndexModel},
    },
};
#[cfg(feature = "sql")]
use crate::{
    db::schema::{
        canonicalize_strict_sql_literal_for_persisted_kind, enum_catalog::ValueAdmissionBudget,
    },
    model::canonicalize_strict_sql_literal_for_kind,
    value::Value,
};
#[cfg(test)]
use std::cell::Cell;
use std::sync::{Mutex, OnceLock};

type SchemaFieldEntry = (String, SchemaFieldInfo);
type CachedSchemaEntries = Vec<(&'static str, &'static SchemaInfo)>;
const EMPTY_GENERATED_NESTED_FIELDS: &[FieldModel] = &[];

#[cfg(feature = "sql")]
fn accepted_sql_capabilities(
    kind: &AcceptedFieldKind,
    enum_catalog: Option<&AcceptedEnumCatalogHandle>,
) -> SqlCapabilities {
    enum_catalog.map_or_else(
        || sql_capabilities(kind),
        |catalog| sql_capabilities_with_enum_catalog(kind, catalog.catalog()),
    )
}

#[cfg(test)]
thread_local! {
    static ACCEPTED_SCHEMA_INFO_PROJECTIONS: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
pub(in crate::db) fn reset_accepted_schema_info_projection_count_for_tests() {
    ACCEPTED_SCHEMA_INFO_PROJECTIONS.with(|projections| projections.set(0));
}

#[cfg(test)]
pub(in crate::db) fn accepted_schema_info_projection_count_for_tests() -> u64 {
    ACCEPTED_SCHEMA_INFO_PROJECTIONS.with(Cell::get)
}

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

fn persisted_kind_has_relation(kind: &AcceptedFieldKind) -> bool {
    match kind {
        AcceptedFieldKind::Relation { .. } => true,
        AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => {
            persisted_kind_has_relation(inner)
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
/// Generated field kinds and nested models exist only on model-only views;
/// accepted views carry persisted contracts and an accepted enum catalog.
///

#[derive(Clone, Debug)]
struct SchemaFieldInfo {
    slot: usize,
    ty: FieldType,
    kind: Option<FieldKind>,
    nullable: bool,
    leaf_codec: LeafCodec,
    #[cfg(feature = "sql")]
    sql_capabilities: SqlCapabilities,
    #[cfg(feature = "sql")]
    persisted_kind: Option<AcceptedFieldKind>,
    accepted_value_contract: Option<AcceptedValueContract>,
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
/// proposal and model-only callers.
///
#[derive(Clone, Debug)]
pub(in crate::db) struct SchemaIndexInfo {
    ordinal: u16,
    name: String,
    store: String,
    unique: bool,
    generated: bool,
    fields: Vec<SchemaIndexFieldPathInfo>,
    predicate_sql: Option<String>,
    enum_catalog: Option<AcceptedEnumCatalogHandle>,
}

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

    /// Return whether this index is declared by the generated entity model.
    #[must_use]
    pub(in crate::db) const fn generated(&self) -> bool {
        self.generated
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

    /// Bind one owned field-path component to this index's catalog authority.
    #[must_use]
    pub(in crate::db) fn accepted_field_contract<'a>(
        &'a self,
        field: &'a SchemaIndexFieldPathInfo,
    ) -> Option<AcceptedValueAdmissionContract<'a>> {
        if !self
            .fields
            .iter()
            .any(|candidate| std::ptr::eq(candidate, field))
        {
            return None;
        }
        field.accepted_value_contract(self.enum_catalog.as_ref())
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
pub(in crate::db) struct SchemaExpressionIndexInfo {
    ordinal: u16,
    name: String,
    store: String,
    unique: bool,
    generated: bool,
    key_items: Vec<SchemaExpressionIndexKeyItemInfo>,
    predicate_sql: Option<String>,
    enum_catalog: Option<AcceptedEnumCatalogHandle>,
}

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

    /// Return whether this expression index came from generated entity metadata.
    #[must_use]
    pub(in crate::db) const fn generated(&self) -> bool {
        self.generated
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

    /// Bind one owned field-path component to this index's catalog authority.
    #[must_use]
    pub(in crate::db) fn accepted_field_contract<'a>(
        &'a self,
        field: &'a SchemaIndexFieldPathInfo,
    ) -> Option<AcceptedValueAdmissionContract<'a>> {
        if !self.key_items.iter().any(|item| match item {
            SchemaExpressionIndexKeyItemInfo::FieldPath(candidate) => {
                std::ptr::eq(candidate, field)
            }
            SchemaExpressionIndexKeyItemInfo::Expression(expression) => {
                std::ptr::eq(expression.source(), field)
            }
        }) {
            return None;
        }
        field.accepted_value_contract(self.enum_catalog.as_ref())
    }
}

///
/// SchemaExpressionIndexKeyItemInfo
///
/// Accepted expression-index key item surfaced through `SchemaInfo`.
///
#[derive(Clone, Debug)]
pub(in crate::db) enum SchemaExpressionIndexKeyItemInfo {
    FieldPath(SchemaIndexFieldPathInfo),
    Expression(Box<SchemaIndexExpressionInfo>),
}

impl SchemaExpressionIndexKeyItemInfo {
    /// Borrow this key item as an expression component, when applicable.
    #[cfg(test)]
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
pub(in crate::db) struct SchemaIndexExpressionInfo {
    op: PersistedIndexExpressionOp,
    source: SchemaIndexFieldPathInfo,
    #[cfg(test)]
    input_kind: AcceptedFieldKind,
    #[cfg(test)]
    output_kind: AcceptedFieldKind,
    canonical_text: String,
}

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
    #[cfg(test)]
    pub(in crate::db) const fn input_kind(&self) -> &AcceptedFieldKind {
        &self.input_kind
    }

    /// Borrow the accepted expression output kind.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn output_kind(&self) -> &AcceptedFieldKind {
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
/// proposal views omit field IDs until generated metadata is reconciled.
///
#[derive(Clone, Debug)]
pub(in crate::db) struct SchemaIndexFieldPathInfo {
    #[cfg(test)]
    field_id: Option<FieldId>,
    field_name: String,
    slot: usize,
    path: Vec<String>,
    #[cfg(test)]
    ty: FieldType,
    persisted_kind: Option<AcceptedFieldKind>,
    accepted_value_contract: Option<Box<AcceptedValueContract>>,
    nullable: bool,
}

impl SchemaIndexFieldPathInfo {
    /// Return the accepted durable top-level field ID, when this came from a
    /// persisted schema snapshot.
    #[must_use]
    #[cfg(test)]
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
    #[cfg(test)]
    pub(in crate::db) const fn ty(&self) -> &FieldType {
        &self.ty
    }

    /// Borrow the persisted field kind, when this key item came from accepted
    /// schema authority.
    #[must_use]
    pub(in crate::db) fn persisted_kind(&self) -> Option<&AcceptedFieldKind> {
        self.accepted_value_contract
            .as_deref()
            .map(AcceptedValueContract::kind)
            .or(self.persisted_kind.as_ref())
    }

    fn accepted_value_contract<'a>(
        &'a self,
        enum_catalog: Option<&'a AcceptedEnumCatalogHandle>,
    ) -> Option<AcceptedValueAdmissionContract<'a>> {
        Some(AcceptedValueAdmissionContract::borrowed(
            enum_catalog?,
            self.accepted_value_contract.as_deref()?,
            self.nullable,
        ))
    }

    /// Return whether this key item permits explicit persisted `NULL`.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn nullable(&self) -> bool {
        self.nullable
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SchemaInfo {
    fields: Vec<SchemaFieldEntry>,
    indexes: Vec<SchemaIndexInfo>,
    expression_indexes: Vec<SchemaExpressionIndexInfo>,
    enum_catalog: Option<AcceptedEnumCatalogHandle>,
    entity_path: Option<String>,
    entity_name: Option<String>,
    primary_key_names: Vec<String>,
    has_any_relations: bool,
}

impl SchemaInfo {
    /// Return whether this view is pinned to accepted catalog authority.
    #[must_use]
    pub(in crate::db) const fn has_accepted_authority(&self) -> bool {
        self.enum_catalog.is_some()
    }

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
                        #[cfg(feature = "sql")]
                        sql_capabilities: sql_capabilities_for_model_kind(&field.kind()),
                        #[cfg(feature = "sql")]
                        persisted_kind: None,
                        accepted_value_contract: None,
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
            enum_catalog: None,
            entity_path: None,
            entity_name: None,
            primary_key_names: Vec::new(),
            has_any_relations: false,
        }
    }

    // Build one compact field table from trusted generated entity metadata.
    fn from_trusted_entity_model(model: &EntityModel) -> Self {
        let mut schema = Self::from_trusted_field_models(model.fields());
        schema.entity_path = Some(model.path().to_string());
        schema.entity_name = Some(model.name().to_string());
        schema.primary_key_names = model
            .primary_key_model()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect();
        schema.has_any_relations = model.has_any_relations();

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

    /// Borrow the complete accepted value contract for one live field.
    ///
    /// Generated-only schema views return `None`; runtime admission must use a
    /// schema view pinned to an accepted catalog revision.
    #[must_use]
    pub(in crate::db) fn accepted_field_contract(
        &self,
        name: &str,
    ) -> Option<AcceptedValueAdmissionContract<'_>> {
        let field = schema_field_info(self.fields.as_slice(), name)?;
        Some(AcceptedValueAdmissionContract::borrowed(
            self.enum_catalog.as_ref()?,
            field.accepted_value_contract.as_ref()?,
            field.nullable,
        ))
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
    /// model queries.
    #[must_use]
    #[cfg(feature = "sql")]
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
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) fn entity_name(&self) -> Option<&str> {
        self.entity_name.as_deref()
    }

    /// Borrow the schema-owned primary-key field name for scalar primary-key
    /// entities. Composite entities return `None` so scalar access-planning
    /// helpers cannot silently treat the first component as a complete key.
    #[must_use]
    pub(in crate::db) fn scalar_primary_key_name(&self) -> Option<&str> {
        (self.primary_key_names.len() == 1).then(|| self.primary_key_names[0].as_str())
    }

    /// Borrow schema-owned primary-key field names in accepted key order.
    ///
    /// Callers that need deterministic ordering or composite identity must use
    /// the full ordered slice.
    #[must_use]
    pub(in crate::db) const fn primary_key_names(&self) -> &[String] {
        self.primary_key_names.as_slice()
    }

    /// Return whether this entity has any relation checks.
    ///
    /// Accepted schema views source this from persisted relation field
    /// contracts. Generated schema views source it from generated model
    /// metadata only for proposal/model-only callers.
    #[must_use]
    pub(in crate::db) const fn has_any_relations(&self) -> bool {
        self.has_any_relations
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

    /// Borrow accepted enum authority when this is a live accepted schema view.
    #[must_use]
    pub(in crate::db) fn enum_catalog(&self) -> Option<&AcceptedEnumCatalog> {
        self.enum_catalog
            .as_ref()
            .map(AcceptedEnumCatalogHandle::catalog)
    }

    /// Borrow accepted enum authority when this is a live accepted schema view.
    #[must_use]
    #[cfg_attr(
        target_arch = "wasm32",
        allow(
            dead_code,
            reason = "schema DDL binding is host-owned even when SQL query support is built for wasm"
        )
    )]
    pub(in crate::db) const fn enum_catalog_handle(&self) -> Option<&AcceptedEnumCatalogHandle> {
        self.enum_catalog.as_ref()
    }

    /// Borrow field-path index contracts visible through this schema view.
    ///
    /// Accepted schema views source this from persisted index contracts.
    /// Generated schema views source it from generated field-only indexes for
    /// proposal and model-only use.
    #[must_use]
    pub(in crate::db) const fn field_path_indexes(&self) -> &[SchemaIndexInfo] {
        self.indexes.as_slice()
    }

    /// Borrow accepted expression-index contracts visible through this schema view.
    ///
    /// Accepted schema views source this from persisted expression index
    /// contracts. Generated schema views leave this empty until generated
    /// expression indexes have been reconciled into accepted metadata.
    #[must_use]
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
    #[cfg(feature = "sql")]
    pub(in crate::db) fn sql_capabilities(&self, name: &str) -> Option<SqlCapabilities> {
        schema_field_info(self.fields.as_slice(), name).map(|field| field.sql_capabilities)
    }

    /// Return whether one top-level field stores an exact composite value.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) fn field_is_composite_value(&self, name: &str) -> bool {
        schema_field_info(self.fields.as_slice(), name)
            .is_some_and(|field| matches!(field.ty, FieldType::Composite))
    }

    /// Return SQL operation capabilities for one nested field path.
    ///
    /// Accepted schema views resolve nested paths from persisted nested leaf
    /// metadata. Generated schema views derive the same facts from generated
    /// nested `FieldModel` metadata until live row-layout authority exists.
    #[must_use]
    #[cfg(feature = "sql")]
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
                .map(|leaf| accepted_sql_capabilities(leaf.kind(), self.enum_catalog.as_ref()));
        }

        resolve_nested_field_path_kind(field.nested_fields, segments)
            .map(|kind| sql_capabilities_for_model_kind(&kind))
    }

    /// Return the first top-level field that SQL cannot project directly.
    #[must_use]
    #[cfg(feature = "sql")]
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
    /// Generated schema views use generated kinds only for direct lowering
    /// tests and compile-time-only callers.
    ///
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db) fn canonicalize_strict_sql_literal(
        &self,
        field_name: &str,
        value: &Value,
    ) -> Option<Value> {
        let field = schema_field_info(self.fields.as_slice(), field_name)?;

        if let Some(kind) = field.persisted_kind.as_ref() {
            if matches!(kind, AcceptedFieldKind::Enum { .. }) {
                let Value::Text(variant) = value else {
                    return None;
                };
                let contract = self.accepted_field_contract(field_name)?;
                let input = crate::value::InputValue::Enum(crate::value::InputValueEnum::loose(
                    variant.clone(),
                ));
                return contract
                    .normalize_input_to_runtime(input, &mut ValueAdmissionBudget::standard())
                    .ok();
            }
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

    /// Build one snapshot-shaped model bridge for focused tests.
    ///
    /// This intentionally lacks accepted catalog authority. Production runtime
    /// paths must use `from_accepted_snapshot_and_catalog_for_model`.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn from_snapshot_with_generated_model_for_test(
        model: &EntityModel,
        schema: &AcceptedSchemaSnapshot,
    ) -> Self {
        Self::from_snapshot_for_model(model, schema, None, false)
    }

    /// Build one accepted schema view retaining its immutable enum catalog.
    #[must_use]
    pub(in crate::db) fn from_accepted_snapshot_and_catalog_for_model(
        model: &EntityModel,
        schema: &AcceptedSchemaSnapshot,
        enum_catalog: AcceptedEnumCatalogHandle,
        include_expression_indexes: bool,
    ) -> Self {
        Self::from_snapshot_for_model(
            model,
            schema,
            Some(enum_catalog),
            include_expression_indexes,
        )
    }

    fn from_snapshot_for_model(
        model: &EntityModel,
        schema: &AcceptedSchemaSnapshot,
        enum_catalog: Option<AcceptedEnumCatalogHandle>,
        include_expression_indexes: bool,
    ) -> Self {
        #[cfg(test)]
        ACCEPTED_SCHEMA_INFO_PROJECTIONS
            .with(|projections| projections.set(projections.get().saturating_add(1)));

        let snapshot = schema.persisted_snapshot();
        let indexed_field_ids = accepted_indexed_field_ids(snapshot);
        let mut fields = snapshot
            .fields()
            .iter()
            .map(|field| {
                let generated_field = enum_catalog
                    .is_none()
                    .then(|| generated_field_by_name(model, field.name()))
                    .flatten();
                let slot = snapshot
                    .row_layout()
                    .slot_for_field(field.id())
                    .map_or_else(|| usize::from(field.slot().get()), accepted_slot_index);
                let generated_kind = generated_field.map(|(_, field)| field.kind());
                let generated_nested_fields = generated_field
                    .map_or(EMPTY_GENERATED_NESTED_FIELDS, |(_, field)| {
                        field.nested_fields()
                    });
                let accepted_value_contract = enum_catalog.as_ref().and_then(|catalog| {
                    AcceptedValueContract::from_accepted_field(
                        catalog,
                        field.kind(),
                        field.storage_decode(),
                    )
                    .ok()
                });
                debug_assert!(enum_catalog.is_none() || accepted_value_contract.is_some());

                (
                    field.name().to_string(),
                    SchemaFieldInfo {
                        slot,
                        ty: field_type_from_persisted_kind(field.kind()),
                        kind: generated_kind,
                        nullable: field.nullable(),
                        leaf_codec: field.leaf_codec(),
                        #[cfg(feature = "sql")]
                        sql_capabilities: accepted_sql_capabilities(
                            field.kind(),
                            enum_catalog.as_ref(),
                        ),
                        #[cfg(feature = "sql")]
                        persisted_kind: Some(field.kind().clone()),
                        accepted_value_contract,
                        indexed: indexed_field_ids.contains(&field.id()),
                        nested_leaves: Some(field.nested_leaves().to_vec()),
                        nested_fields: generated_nested_fields,
                    },
                )
            })
            .collect::<Vec<_>>();

        fields.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));

        let primary_key_names = snapshot
            .primary_key_field_ids()
            .iter()
            .filter_map(|field_id| {
                snapshot
                    .fields()
                    .iter()
                    .find(|field| field.id() == *field_id)
                    .map(|field| field.name().to_string())
            })
            .collect();

        Self {
            fields,
            indexes: snapshot
                .indexes()
                .iter()
                .filter_map(|index| {
                    schema_index_info_from_accepted_index(index, snapshot, enum_catalog.as_ref())
                })
                .collect(),
            expression_indexes: snapshot
                .indexes()
                .iter()
                .filter_map(|index| {
                    include_expression_indexes
                        .then(|| {
                            schema_expression_index_info_from_accepted_index(
                                index,
                                snapshot,
                                enum_catalog.as_ref(),
                            )
                        })
                        .flatten()
                })
                .collect(),
            enum_catalog,
            entity_path: Some(schema.entity_path().to_string()),
            entity_name: Some(schema.entity_name().to_string()),
            primary_key_names,
            has_any_relations: !snapshot.relations().is_empty()
                || snapshot
                    .fields()
                    .iter()
                    .any(|field| persisted_kind_has_relation(field.kind())),
        }
    }

    /// Build one accepted schema view with expression-index metadata projected.
    ///
    /// This constructor exists for expression-index routing and
    /// tests that need to inspect accepted expression contracts without adding
    /// allocation work to every existing accepted schema view.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn from_snapshot_with_generated_model_and_expression_indexes_for_test(
        model: &EntityModel,
        schema: &AcceptedSchemaSnapshot,
    ) -> Self {
        Self::from_snapshot_for_model(model, schema, None, true)
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
                #[cfg(test)]
                field_id: None,
                field_name: field_name.to_string(),
                slot: field.slot,
                path: vec![field_name.to_string()],
                #[cfg(test)]
                ty: field.ty.clone(),
                persisted_kind: None,
                accepted_value_contract: None,
                nullable: field.nullable,
            })
        })
        .collect::<Option<Vec<_>>>()?;

    Some(SchemaIndexInfo {
        ordinal: index.ordinal(),
        name: index.name().to_string(),
        store: index.store().to_string(),
        unique: index.is_unique(),
        generated: true,
        fields: key_fields,
        predicate_sql: index.predicate().map(str::to_string),
        enum_catalog: None,
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
    enum_catalog: Option<&AcceptedEnumCatalogHandle>,
) -> Option<SchemaIndexInfo> {
    if !index.key().is_field_path_only() {
        return None;
    }

    Some(SchemaIndexInfo {
        ordinal: index.ordinal(),
        name: index.name().to_string(),
        store: index.store().to_string(),
        unique: index.unique(),
        generated: index.generated(),
        fields: index
            .key()
            .field_paths()
            .iter()
            .map(|path| schema_index_field_path_info_from_accepted(path, snapshot, enum_catalog))
            .collect(),
        predicate_sql: index.predicate_sql().map(str::to_string),
        enum_catalog: enum_catalog.cloned(),
    })
}

fn schema_expression_index_info_from_accepted_index(
    index: &PersistedIndexSnapshot,
    snapshot: &PersistedSchemaSnapshot,
    enum_catalog: Option<&AcceptedEnumCatalogHandle>,
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
        generated: index.generated(),
        key_items: items
            .iter()
            .map(|item| schema_expression_index_key_item_info(item, snapshot, enum_catalog))
            .collect(),
        predicate_sql: index.predicate_sql().map(str::to_string),
        enum_catalog: enum_catalog.cloned(),
    })
}

fn schema_expression_index_key_item_info(
    item: &PersistedIndexKeyItemSnapshot,
    snapshot: &PersistedSchemaSnapshot,
    enum_catalog: Option<&AcceptedEnumCatalogHandle>,
) -> SchemaExpressionIndexKeyItemInfo {
    match item {
        PersistedIndexKeyItemSnapshot::FieldPath(path) => {
            SchemaExpressionIndexKeyItemInfo::FieldPath(schema_index_field_path_info_from_accepted(
                path,
                snapshot,
                enum_catalog,
            ))
        }
        PersistedIndexKeyItemSnapshot::Expression(expression) => {
            SchemaExpressionIndexKeyItemInfo::Expression(Box::new(SchemaIndexExpressionInfo {
                op: expression.op(),
                source: schema_index_field_path_info_from_accepted(
                    expression.source(),
                    snapshot,
                    enum_catalog,
                ),
                #[cfg(test)]
                input_kind: expression.input_kind().clone(),
                #[cfg(test)]
                output_kind: expression.output_kind().clone(),
                canonical_text: expression.canonical_text().to_string(),
            }))
        }
    }
}

fn schema_index_field_path_info_from_accepted(
    path: &PersistedIndexFieldPathSnapshot,
    snapshot: &PersistedSchemaSnapshot,
    enum_catalog: Option<&AcceptedEnumCatalogHandle>,
) -> SchemaIndexFieldPathInfo {
    let field_name = accepted_field_name(snapshot, path.field_id())
        .or_else(|| path.path().first().map(String::as_str))
        .unwrap_or_default()
        .to_string();
    let accepted_value_contract = enum_catalog.and_then(|catalog| {
        AcceptedValueContract::from_accepted_field(catalog, path.kind(), FieldStorageDecode::ByKind)
            .ok()
            .map(Box::new)
    });
    debug_assert!(enum_catalog.is_none() || accepted_value_contract.is_some());
    let persisted_kind = accepted_value_contract
        .is_none()
        .then(|| path.kind().clone());

    SchemaIndexFieldPathInfo {
        #[cfg(test)]
        field_id: Some(path.field_id()),
        field_name,
        slot: accepted_slot_index(path.slot()),
        path: path.path().to_vec(),
        #[cfg(test)]
        ty: field_type_from_persisted_kind(path.kind()),
        persisted_kind,
        accepted_value_contract,
        nullable: path.nullable(),
    }
}

// Resolve generated nested metadata for compile-time-only schema views.
// Accepted schema views use persisted nested leaf descriptors instead.
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
mod tests;
