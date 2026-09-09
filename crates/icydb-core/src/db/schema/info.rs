//! Module: db::schema::info
//! Responsibility: schema model/index integrity checks used during schema info construction.
//! Does not own: query planning policy or runtime predicate evaluation.
//! Boundary: validates entity/index model consistency for predicate schema metadata.

use std::{borrow::Cow, rc::Rc, sync::Arc};

#[cfg(feature = "sql")]
use crate::db::schema::canonicalize_strict_sql_literal_for_persisted_kind;
use crate::db::schema::{
    AcceptedConstraintIdentity, AcceptedEnumCatalog, AcceptedFieldKind, AcceptedSchemaSnapshot,
    AcceptedValueAdmissionContract, AcceptedValueCatalogHandle, FieldId, FieldStorageDecode,
    FieldType, LeafCodec, PersistedFieldSnapshot, PersistedIndexExpressionOp,
    PersistedIndexFieldPathSnapshot, PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot,
    PersistedIndexSnapshot, PersistedNestedLeafSnapshot, PersistedSchemaSnapshot, SchemaFieldSlot,
    enum_catalog::AcceptedValueContract, field_type_from_persisted_kind,
    query_field_kind_from_persisted_kind,
};
#[cfg(feature = "sql")]
use crate::db::schema::{SqlCapabilities, sql_capabilities_with_enum_catalog};
use crate::{
    db::schema::{
        canonicalize_filter_collection_element_for_persisted_kind,
        canonicalize_filter_literal_for_persisted_kind, enum_catalog::ValueAdmissionBudget,
    },
    db::{QueryError, query::preparation::PreparationWork},
    value::{InputValue, Value},
};
type SchemaFieldEntry = (Arc<str>, SchemaFieldInfo);

#[cfg(feature = "sql")]
fn accepted_sql_capabilities(
    kind: &AcceptedFieldKind,
    value_catalog: &AcceptedValueCatalogHandle,
) -> SqlCapabilities {
    sql_capabilities_with_enum_catalog(kind, value_catalog.enum_catalog())
}

fn schema_field_entry<'a>(
    fields: &'a [SchemaFieldEntry],
    name: &str,
) -> Option<&'a SchemaFieldEntry> {
    fields
        .binary_search_by(|(field_name, _)| field_name.as_ref().cmp(name))
        .ok()
        .map(|index| &fields[index])
}

// Resolve top-level index membership from accepted persisted index contracts
// once per schema view. Runtime accepted schema views must not reopen generated
// generated index declarations after schema acceptance.
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
/// Every entry is projected from one accepted snapshot and catalog.
///

#[derive(Clone, Debug)]
struct SchemaFieldInfo {
    slot: usize,
    ty: FieldType,
    nullable: bool,
    leaf_codec: LeafCodec,
    #[cfg(feature = "sql")]
    sql_capabilities: SqlCapabilities,
    query_kind: Arc<AcceptedFieldKind>,
    accepted_value_contract: Option<AcceptedValueContract>,
    indexed: bool,
    nested_leaves: Vec<PersistedNestedLeafSnapshot>,
}

///
/// SchemaIndexInfo
///
/// Compact field-path index contract exposed by `SchemaInfo`.
/// Entries source their identity and shape from accepted index snapshots.
///
#[derive(Clone, Debug)]
pub(in crate::db) struct SchemaIndexInfo {
    ordinal: u16,
    physical_generation: u64,
    name: String,
    store: String,
    unique: bool,
    unique_constraint: Option<AcceptedConstraintIdentity>,
    fields: Vec<SchemaIndexFieldPathInfo>,
    predicate_sql: Option<String>,
    value_catalog: AcceptedValueCatalogHandle,
}

impl SchemaIndexInfo {
    /// Return the accepted dense physical index ordinal.
    #[must_use]
    pub(in crate::db) const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    /// Return the isolated physical key generation.
    #[must_use]
    pub(in crate::db) const fn physical_generation(&self) -> u64 {
        self.physical_generation
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

    /// Borrow accepted unique-constraint identity for this index.
    ///
    /// A unique index without this identity is corrupt accepted authority and
    /// must fail closed before reporting a runtime collision.
    #[must_use]
    pub(in crate::db) const fn unique_constraint(&self) -> Option<&AcceptedConstraintIdentity> {
        self.unique_constraint.as_ref()
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
        field.accepted_value_contract(&self.value_catalog)
    }
}

///
/// SchemaExpressionIndexInfo
///
/// Compact accepted expression-index contract exposed by `SchemaInfo`.
/// Accepted schema views source this from persisted index snapshots so
/// expression-index runtime routing does not reopen generated index declarations.
///
#[derive(Clone, Debug)]
pub(in crate::db) struct SchemaExpressionIndexInfo {
    ordinal: u16,
    physical_generation: u64,
    name: String,
    store: String,
    unique: bool,
    unique_constraint: Option<AcceptedConstraintIdentity>,
    key_items: Vec<SchemaExpressionIndexKeyItemInfo>,
    predicate_sql: Option<String>,
    value_catalog: AcceptedValueCatalogHandle,
}

impl SchemaExpressionIndexInfo {
    /// Return the accepted dense physical index ordinal.
    #[must_use]
    pub(in crate::db) const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    /// Return the isolated physical key generation.
    #[must_use]
    pub(in crate::db) const fn physical_generation(&self) -> u64 {
        self.physical_generation
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

    /// Borrow accepted unique-constraint identity for this index.
    ///
    /// A unique index without this identity is corrupt accepted authority and
    /// must fail closed before reporting a runtime collision.
    #[must_use]
    pub(in crate::db) const fn unique_constraint(&self) -> Option<&AcceptedConstraintIdentity> {
        self.unique_constraint.as_ref()
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
        field.accepted_value_contract(&self.value_catalog)
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

///
/// SchemaIndexExpressionInfo
///
/// Compact accepted expression key contract for one expression-index key item.
///
#[derive(Clone, Debug)]
pub(in crate::db) struct SchemaIndexExpressionInfo {
    op: PersistedIndexExpressionOp,
    source: SchemaIndexFieldPathInfo,
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
/// Accepted schema supplies the durable field identity and kind.
///
#[derive(Clone, Debug)]
pub(in crate::db) struct SchemaIndexFieldPathInfo {
    field_name: String,
    slot: usize,
    path: Vec<String>,
    persisted_kind: AcceptedFieldKind,
    accepted_value_contract: Option<Box<AcceptedValueContract>>,
    nullable: bool,
}

impl SchemaIndexFieldPathInfo {
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

    /// Borrow the accepted persisted field kind.
    #[must_use]
    pub(in crate::db) fn persisted_kind(&self) -> Option<&AcceptedFieldKind> {
        self.accepted_value_contract
            .as_deref()
            .map(AcceptedValueContract::kind)
            .or(Some(&self.persisted_kind))
    }

    fn accepted_value_contract<'a>(
        &'a self,
        value_catalog: &'a AcceptedValueCatalogHandle,
    ) -> Option<AcceptedValueAdmissionContract<'a>> {
        Some(AcceptedValueAdmissionContract::borrowed(
            value_catalog,
            self.accepted_value_contract.as_deref()?,
            self.nullable,
        ))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SchemaInfo {
    fields: Vec<SchemaFieldEntry>,
    indexes: Vec<SchemaIndexInfo>,
    expression_indexes: Vec<SchemaExpressionIndexInfo>,
    value_catalog: AcceptedValueCatalogHandle,
    entity_name: Option<String>,
    primary_key_names: Rc<[String]>,
}

impl SchemaInfo {
    #[must_use]
    pub(crate) fn field(&self, name: &str) -> Option<&FieldType> {
        schema_field_entry(self.fields.as_slice(), name).map(|(_, field)| &field.ty)
    }

    /// Borrow the complete accepted value contract for one live field.
    ///
    #[must_use]
    pub(in crate::db) fn accepted_field_contract(
        &self,
        name: &str,
    ) -> Option<AcceptedValueAdmissionContract<'_>> {
        let (_, field) = schema_field_entry(self.fields.as_slice(), name)?;
        Some(AcceptedValueAdmissionContract::borrowed(
            &self.value_catalog,
            field.accepted_value_contract.as_ref()?,
            field.nullable,
        ))
    }

    /// Return accepted top-level field nullability without reopening proposal metadata.
    #[must_use]
    pub(in crate::db) fn accepted_field_is_nullable(&self, name: &str) -> Option<bool> {
        schema_field_entry(self.fields.as_slice(), name).map(|(_, field)| field.nullable)
    }

    /// Borrow the accepted field kind projected to its admitted query-value shape.
    #[must_use]
    pub(in crate::db) fn accepted_query_field_kind(
        &self,
        name: &str,
    ) -> Option<&AcceptedFieldKind> {
        schema_field_entry(self.fields.as_slice(), name).map(|(_, field)| field.query_kind.as_ref())
    }

    /// Resolve retained slot authority in one lookup. Share only this field's
    /// immutable label and query kind, not the complete schema or a session borrow.
    #[must_use]
    pub(in crate::db) fn retained_query_field_authority(
        &self,
        name: &str,
    ) -> Option<(usize, Arc<str>, Arc<AcceptedFieldKind>)> {
        let (label, field) = schema_field_entry(self.fields.as_slice(), name)?;
        Some((field.slot, Arc::clone(label), Arc::clone(&field.query_kind)))
    }

    /// Return the top-level physical row slot for one field.
    ///
    /// The accepted row layout is the only slot source.
    #[must_use]
    pub(in crate::db) fn field_slot_index(&self, name: &str) -> Option<usize> {
        schema_field_entry(self.fields.as_slice(), name).map(|(_, field)| field.slot)
    }

    /// Resolve one accepted direct field or nested query path to its query type.
    #[must_use]
    pub(in crate::db) fn accepted_query_field_type(
        &self,
        name: &str,
    ) -> Option<Cow<'_, FieldType>> {
        if let Some(field_type) = self.field(name) {
            return Some(Cow::Borrowed(field_type));
        }

        let (root, nested) = name.split_once('.')?;
        if root.is_empty() || nested.is_empty() {
            return None;
        }
        let (_, field) = schema_field_entry(self.fields.as_slice(), root)?;
        let leaf = field
            .nested_leaves
            .iter()
            .find(|leaf| leaf.path().iter().map(String::as_str).eq(nested.split('.')))?;
        let query_kind = query_field_kind_from_persisted_kind(
            leaf.kind(),
            self.value_catalog.composite_catalog(),
        );
        Some(Cow::Owned(field_type_from_persisted_kind(&query_kind)))
    }

    /// Return whether null at this field or any accepted path ancestor omits
    /// the field from a physical secondary index.
    #[must_use]
    pub(in crate::db) fn accepted_query_field_is_omittable(&self, name: &str) -> Option<bool> {
        if let Some((_, field)) = schema_field_entry(self.fields.as_slice(), name) {
            return Some(field.nullable);
        }

        let (root, nested) = name.split_once('.')?;
        if root.is_empty() || nested.is_empty() {
            return None;
        }
        let (_, field) = schema_field_entry(self.fields.as_slice(), root)?;
        let terminal = field
            .nested_leaves
            .iter()
            .find(|leaf| leaf.path().iter().map(String::as_str).eq(nested.split('.')))?;

        Some(
            field.nullable
                || terminal.nullable()
                || field.nested_leaves.iter().any(|candidate| {
                    candidate.nullable()
                        && candidate.path().len() < terminal.path().len()
                        && terminal.path().starts_with(candidate.path())
                }),
        )
    }

    /// Return the number of live top-level accepted fields without allocating.
    #[must_use]
    pub(in crate::db) const fn field_count(&self) -> usize {
        self.fields.len()
    }

    /// Return accepted field names in canonical physical-slot order.
    ///
    /// Structural identity projection uses this ordering instead of reopening
    /// generated declaration metadata. Accepted row layouts guarantee one
    /// unique top-level slot per live field.
    #[must_use]
    pub(in crate::db) fn field_names_in_slot_order(&self) -> Vec<&str> {
        let mut fields = self
            .fields
            .iter()
            .map(|(name, field)| (field.slot, name.as_ref()))
            .collect::<Vec<_>>();
        icydb_schema::compact_sort_unstable_by(&mut fields, |left, right| left.0.cmp(&right.0));

        fields.into_iter().map(|(_, name)| name).collect()
    }

    /// Return whether one top-level row slot is backed by a scalar leaf codec.
    ///
    /// Persisted accepted field snapshots select the codec class.
    #[must_use]
    pub(in crate::db) fn field_slot_has_scalar_leaf(&self, slot: usize) -> bool {
        self.fields
            .iter()
            .find(|(_, field)| field.slot == slot)
            .is_some_and(|(_, field)| matches!(field.leaf_codec, LeafCodec::Scalar(_)))
    }

    /// Borrow the accepted entity name.
    #[must_use]
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
    pub(in crate::db) fn primary_key_names(&self) -> &[String] {
        &self.primary_key_names
    }

    /// Retain the accepted key-name list without copying its backing or labels.
    /// A finalized plan shares this authority, never reconstructs it from models.
    #[must_use]
    pub(in crate::db) fn shared_primary_key_names(&self) -> Rc<[String]> {
        Rc::clone(&self.primary_key_names)
    }

    /// Return whether one top-level field participates in any index.
    ///
    /// Accepted persisted index contracts are the only source.
    #[must_use]
    pub(in crate::db) fn field_is_indexed(&self, name: &str) -> bool {
        schema_field_entry(self.fields.as_slice(), name).is_some_and(|(_, field)| field.indexed)
    }

    /// Borrow accepted enum authority.
    #[must_use]
    pub(in crate::db) fn enum_catalog(&self) -> &AcceptedEnumCatalog {
        self.value_catalog.enum_catalog()
    }

    /// Borrow accepted value-catalog authority.
    #[must_use]
    pub(in crate::db) const fn value_catalog_handle(&self) -> &AcceptedValueCatalogHandle {
        &self.value_catalog
    }

    /// Borrow field-path index contracts visible through this schema view.
    ///
    /// Accepted persisted index contracts are the only source.
    #[must_use]
    pub(in crate::db) const fn field_path_indexes(&self) -> &[SchemaIndexInfo] {
        self.indexes.as_slice()
    }

    /// Borrow accepted expression-index contracts visible through this schema view.
    ///
    /// Accepted persisted expression-index contracts are the only source.
    #[must_use]
    pub(in crate::db) const fn expression_indexes(&self) -> &[SchemaExpressionIndexInfo] {
        self.expression_indexes.as_slice()
    }

    /// Return SQL operation capabilities for one top-level field.
    ///
    /// SQL admission follows the reconciled accepted field kind.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) fn sql_capabilities(&self, name: &str) -> Option<SqlCapabilities> {
        schema_field_entry(self.fields.as_slice(), name).map(|(_, field)| field.sql_capabilities)
    }

    /// Return SQL operation capabilities for one nested field path.
    ///
    /// Nested paths resolve from persisted accepted leaf metadata.
    #[must_use]
    #[cfg(feature = "sql")]
    pub(in crate::db) fn nested_sql_capabilities(
        &self,
        name: &str,
        segments: &[String],
    ) -> Option<SqlCapabilities> {
        let (_, field) = schema_field_entry(self.fields.as_slice(), name)?;

        field
            .nested_leaves
            .iter()
            .find(|leaf| leaf.path() == segments)
            .map(|leaf| {
                let query_kind = query_field_kind_from_persisted_kind(
                    leaf.kind(),
                    self.value_catalog.composite_catalog(),
                );
                accepted_sql_capabilities(&query_kind, &self.value_catalog)
            })
    }

    /// Return the type for one nested field path rooted at a top-level field.
    ///
    /// Nested paths resolve from persisted accepted leaf metadata.
    #[must_use]
    pub(crate) fn nested_field_type(&self, name: &str, segments: &[String]) -> Option<FieldType> {
        let (_, field) = schema_field_entry(self.fields.as_slice(), name)?;

        field
            .nested_leaves
            .iter()
            .find(|leaf| leaf.path() == segments)
            .map(|leaf| {
                let query_kind = query_field_kind_from_persisted_kind(
                    leaf.kind(),
                    self.value_catalog.composite_catalog(),
                );
                field_type_from_persisted_kind(&query_kind)
            })
    }

    /// Borrow the accepted query kind for one nested scalar leaf.
    #[must_use]
    pub(in crate::db) fn accepted_nested_query_field_kind<'a>(
        &self,
        name: &str,
        segments: impl Iterator<Item = &'a str> + Clone,
    ) -> Option<&AcceptedFieldKind> {
        let (_, field) = schema_field_entry(self.fields.as_slice(), name)?;

        field
            .nested_leaves
            .iter()
            .find(|leaf| leaf.path().iter().map(String::as_str).eq(segments.clone()))
            .map(PersistedNestedLeafSnapshot::kind)
    }

    /// Return whether one top-level field exposes any nested path metadata.
    #[must_use]
    pub(crate) fn field_has_nested_paths(&self, name: &str) -> bool {
        schema_field_entry(self.fields.as_slice(), name)
            .is_some_and(|(_, field)| !field.nested_leaves.is_empty())
    }

    /// Canonicalize one strict SQL literal against this schema's field authority.
    ///
    /// SQL read predicates use the same accepted top-level kind as writes.
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db) fn canonicalize_strict_sql_literal(
        &self,
        field_name: &str,
        value: &Value,
    ) -> Option<Value> {
        let (_, field) = schema_field_entry(self.fields.as_slice(), field_name)?;

        let kind = field.query_kind.as_ref();
        if matches!(kind, AcceptedFieldKind::Enum { .. }) {
            let Value::Text(variant) = value else {
                return None;
            };
            let contract = self.accepted_field_contract(field_name)?;
            let input = InputValue::loose_enum(variant.clone());
            return contract
                .normalize_input_to_runtime(input, &mut ValueAdmissionBudget::standard())
                .ok();
        }
        canonicalize_strict_sql_literal_for_persisted_kind(kind, value)
    }

    /// Canonicalize one string-backed public filter literal against this
    /// schema's accepted field authority.
    pub(in crate::db) fn canonicalize_filter_literal<'a>(
        &self,
        field_name: &str,
        value: &'a Value,
        work: &PreparationWork<'_>,
    ) -> Result<Option<Cow<'a, Value>>, QueryError> {
        let Some((_, field)) = schema_field_entry(self.fields.as_slice(), field_name) else {
            return Ok(None);
        };

        let kind = field.query_kind.as_ref();
        if matches!(kind, AcceptedFieldKind::Enum { .. }) {
            let Some(input) = loose_enum_filter_input(kind, value, work)? else {
                return Ok(None);
            };
            let Some(contract) = self.accepted_field_contract(field_name) else {
                return Ok(None);
            };
            return Ok(contract
                .normalize_input_to_runtime(input, &mut ValueAdmissionBudget::standard())
                .ok()
                .map(Cow::Owned));
        }
        canonicalize_filter_literal_for_persisted_kind(kind, value, work)
    }

    /// Canonicalize one collection-containment literal against the accepted
    /// element contract of a list or set field.
    pub(in crate::db) fn canonicalize_filter_collection_element<'a>(
        &self,
        field_name: &str,
        value: &'a Value,
        work: &PreparationWork<'_>,
    ) -> Result<Option<Cow<'a, Value>>, QueryError> {
        let Some((_, field)) = schema_field_entry(self.fields.as_slice(), field_name) else {
            return Ok(None);
        };
        let element_kind = match field.query_kind.as_ref() {
            AcceptedFieldKind::List(element_kind) | AcceptedFieldKind::Set(element_kind) => {
                element_kind.as_ref()
            }
            _ => return Ok(None),
        };

        if element_kind.contains_enum() {
            let Some(input) = loose_enum_filter_input(element_kind, value, work)? else {
                return Ok(None);
            };
            let Some(contract) = self
                .accepted_field_contract(field_name)
                .and_then(|contract| contract.collection_element_contract())
            else {
                return Ok(None);
            };
            return Ok(contract
                .normalize_input_to_runtime(input, &mut ValueAdmissionBudget::standard())
                .ok()
                .map(Cow::Owned));
        }

        canonicalize_filter_collection_element_for_persisted_kind(&field.query_kind, value, work)
    }

    /// Build one accepted-only schema view retaining its immutable value catalog.
    ///
    /// Integrity and other catalog-native consumers must not require a
    /// generated entity model merely to project accepted index contracts.
    #[must_use]
    pub(in crate::db) fn from_accepted_snapshot_and_catalog(
        schema: &AcceptedSchemaSnapshot,
        value_catalog: AcceptedValueCatalogHandle,
        include_expression_indexes: bool,
    ) -> Self {
        Self::from_snapshot(schema, value_catalog, include_expression_indexes)
    }

    fn from_snapshot(
        schema: &AcceptedSchemaSnapshot,
        value_catalog: AcceptedValueCatalogHandle,
        include_expression_indexes: bool,
    ) -> Self {
        let snapshot = schema.persisted_snapshot();
        let indexed_field_ids = accepted_indexed_field_ids(snapshot);
        let mut fields = snapshot
            .fields()
            .iter()
            .map(|field| {
                let slot = snapshot
                    .row_layout()
                    .slot_for_field(field.id())
                    .map_or_else(|| usize::from(field.slot().get()), accepted_slot_index);
                let accepted_value_contract = AcceptedValueContract::from_accepted_field(
                    &value_catalog,
                    field.kind(),
                    field.storage_decode(),
                )
                .ok();
                debug_assert!(accepted_value_contract.is_some());
                let query_kind = query_field_kind_from_persisted_kind(
                    field.kind(),
                    value_catalog.composite_catalog(),
                );

                (
                    Arc::<str>::from(field.name()),
                    SchemaFieldInfo {
                        slot,
                        ty: field_type_from_persisted_kind(&query_kind),
                        nullable: field.nullable(),
                        leaf_codec: field.leaf_codec(),
                        #[cfg(feature = "sql")]
                        sql_capabilities: accepted_sql_capabilities(&query_kind, &value_catalog),
                        query_kind: Arc::new(query_kind),
                        accepted_value_contract,
                        indexed: indexed_field_ids.contains(&field.id()),
                        nested_leaves: field.nested_leaves().to_vec(),
                    },
                )
            })
            .collect::<Vec<_>>();

        icydb_schema::compact_sort_unstable_by(&mut fields, |left, right| left.0.cmp(&right.0));

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
                    schema_index_info_from_accepted_index(index, snapshot, &value_catalog)
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
                                &value_catalog,
                            )
                        })
                        .flatten()
                })
                .collect(),
            value_catalog,
            entity_name: Some(schema.entity_name().to_string()),
            primary_key_names,
        }
    }
}

fn loose_enum_filter_input(
    kind: &AcceptedFieldKind,
    value: &Value,
    work: &PreparationWork<'_>,
) -> Result<Option<InputValue>, QueryError> {
    work.charge(
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::NestedValueSteps,
        1,
    )?;
    Ok(match kind {
        AcceptedFieldKind::Enum { .. } => match value {
            Value::Text(variant) => {
                work.charge(
                    icydb_diagnostic_code::DiagnosticExecutionBudgetResource::TemporaryBytes,
                    variant.len() as u64,
                )?;
                Some(InputValue::loose_enum(variant.clone()))
            }
            _ => None,
        },
        AcceptedFieldKind::Relation { key_kind, .. } => {
            loose_enum_filter_input(key_kind, value, work)?
        }
        _ => None,
    })
}

pub(in crate::db) fn schema_index_info_from_accepted_index(
    index: &PersistedIndexSnapshot,
    snapshot: &PersistedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Option<SchemaIndexInfo> {
    if !index.key().is_field_path_only() {
        return None;
    }

    Some(SchemaIndexInfo {
        ordinal: index.ordinal(),
        physical_generation: index.physical_generation(),
        name: index.name().to_string(),
        store: index.store().to_string(),
        unique: index.unique(),
        unique_constraint: index
            .unique()
            .then(|| snapshot.unique_constraint_identity(index.schema_id()))
            .flatten(),
        fields: index
            .key()
            .field_paths()
            .iter()
            .map(|path| schema_index_field_path_info_from_accepted(path, snapshot, value_catalog))
            .collect(),
        predicate_sql: index.predicate_sql().map(str::to_string),
        value_catalog: value_catalog.clone(),
    })
}

pub(in crate::db) fn schema_expression_index_info_from_accepted_index(
    index: &PersistedIndexSnapshot,
    snapshot: &PersistedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
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
        physical_generation: index.physical_generation(),
        name: index.name().to_string(),
        store: index.store().to_string(),
        unique: index.unique(),
        unique_constraint: index
            .unique()
            .then(|| snapshot.unique_constraint_identity(index.schema_id()))
            .flatten(),
        key_items: items
            .iter()
            .map(|item| schema_expression_index_key_item_info(item, snapshot, value_catalog))
            .collect(),
        predicate_sql: index.predicate_sql().map(str::to_string),
        value_catalog: value_catalog.clone(),
    })
}

fn schema_expression_index_key_item_info(
    item: &PersistedIndexKeyItemSnapshot,
    snapshot: &PersistedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
) -> SchemaExpressionIndexKeyItemInfo {
    match item {
        PersistedIndexKeyItemSnapshot::FieldPath(path) => {
            SchemaExpressionIndexKeyItemInfo::FieldPath(schema_index_field_path_info_from_accepted(
                path,
                snapshot,
                value_catalog,
            ))
        }
        PersistedIndexKeyItemSnapshot::Expression(expression) => {
            SchemaExpressionIndexKeyItemInfo::Expression(Box::new(SchemaIndexExpressionInfo {
                op: expression.op(),
                source: schema_index_field_path_info_from_accepted(
                    expression.source(),
                    snapshot,
                    value_catalog,
                ),
                canonical_text: expression.canonical_text().to_string(),
            }))
        }
    }
}

fn schema_index_field_path_info_from_accepted(
    path: &PersistedIndexFieldPathSnapshot,
    snapshot: &PersistedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
) -> SchemaIndexFieldPathInfo {
    let field_name = accepted_field_name(snapshot, path.field_id())
        .or_else(|| path.path().first().map(String::as_str))
        .unwrap_or_default()
        .to_string();
    let accepted_value_contract = AcceptedValueContract::from_accepted_field(
        value_catalog,
        path.kind(),
        FieldStorageDecode::ByKind,
    )
    .ok()
    .map(Box::new);
    debug_assert!(accepted_value_contract.is_some());

    SchemaIndexFieldPathInfo {
        field_name,
        slot: accepted_slot_index(path.slot()),
        path: path.path().to_vec(),
        persisted_kind: path.kind().clone(),
        accepted_value_contract,
        nullable: path.nullable(),
    }
}

#[cfg(test)]
mod tests {
    mod primary_key_names;
    mod projection_identity;
    mod scalar_compilation;

    use icydb_schema::ScalarKind;
    use std::sync::Arc;

    use crate::{
        db::{
            predicate::normalize_enum_literals,
            predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
            query::{plan::FieldSlot, predicate::validate_predicate},
            schema::{
                AcceptedFieldKind, AcceptedSchemaRevision, AcceptedSchemaSnapshot,
                AcceptedValueCatalogHandle, FieldId, FieldStorageDecode, FieldType, LeafCodec,
                PersistedFieldSnapshot, PersistedNestedLeafSnapshot, PersistedSchemaSnapshot,
                ScalarCodec, SchemaFieldSlot, SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
                TestEnumDefinition, TestEnumVariant, ValidateError,
                build_accepted_enum_catalog_for_tests,
                build_record_newtype_composite_catalog_for_tests,
                empty_accepted_enum_catalog_for_tests, field_type_from_persisted_kind,
            },
        },
        value::Value,
    };

    use super::SchemaInfo;

    fn resolve_group_field(
        schema: &super::SchemaInfo,
        label: &str,
    ) -> Option<crate::db::query::plan::GroupField> {
        crate::db::query::preparation::with_preparation_work(|work| {
            crate::db::query::plan::GroupField::resolve_with_schema(schema, label, work)
        })
        .unwrap()
    }

    #[test]
    fn binding_preflight_query_operand_is_not_a_stored_field_value() {
        use crate::{db::schema::enum_catalog::ValueAdmissionBudget, value::InputValue};
        let schema = newtype_query_schema();
        let input = Value::Int64(-1);
        let predicate = Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::Gte,
            input,
            CoercionId::NumericWiden,
        ));
        let normalized = normalize_enum_literals(&schema, &predicate)
            .expect("query normalization admits the comparison operand");
        validate_predicate(&schema, &normalized)
            .expect("a negative comparison boundary is valid for a Nat64 field");
        assert_eq!(normalized, predicate);
        assert!(
            schema
                .accepted_field_contract("id")
                .expect("accepted Nat64 field")
                .normalize_input_to_runtime(
                    InputValue::int64(-1),
                    &mut ValueAdmissionBudget::standard()
                )
                .is_err()
        );
    }

    fn newtype_query_schema() -> SchemaInfo {
        let enums = empty_accepted_enum_catalog_for_tests();
        let (composites, record_type, name_type, _) =
            build_record_newtype_composite_catalog_for_tests(
                "tests::Profile".to_string(),
                "name".to_string(),
                "tests::Name".to_string(),
                AcceptedFieldKind::Text { max_len: Some(64) },
                &enums,
            )
            .expect("newtype query fixture catalog should build");
        let name_kind = AcceptedFieldKind::Composite { type_id: name_type };
        let fields = vec![
            scalar_field(1, 0, "id", AcceptedFieldKind::Nat64, ScalarCodec::Nat64),
            composite_field(2, 1, "name", name_kind.clone(), Vec::new()),
            composite_field(
                3,
                2,
                "profile",
                AcceptedFieldKind::Composite {
                    type_id: record_type,
                },
                vec![PersistedNestedLeafSnapshot::new(
                    vec!["name".to_string()],
                    name_kind.clone(),
                    false,
                )],
            ),
            composite_field(
                4,
                3,
                "aliases",
                AcceptedFieldKind::Set(Box::new(name_kind)),
                Vec::new(),
            ),
        ];
        let snapshot = PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "tests::Token".to_string(),
            "Token".to_string(),
            FieldId::new(1),
            SchemaRowLayout::initial(
                fields
                    .iter()
                    .map(|field| (field.id(), field.slot()))
                    .collect(),
            ),
            fields,
        );
        let accepted = AcceptedSchemaSnapshot::new(snapshot);
        let catalog = AcceptedValueCatalogHandle::new_for_tests(
            enums,
            composites,
            AcceptedSchemaRevision::INITIAL,
        );

        SchemaInfo::from_accepted_snapshot_and_catalog(&accepted, catalog, true)
    }

    fn enum_newtype_query_schema(collection: bool) -> SchemaInfo {
        let enums = build_accepted_enum_catalog_for_tests(&[TestEnumDefinition::new(
            "tests::Stage",
            vec![TestEnumVariant::unit("Active")],
        )])
        .expect("newtype enum fixture catalog should build");
        let enum_type = enums
            .type_id("tests::Stage")
            .expect("newtype enum fixture type should exist");
        let enum_kind = AcceptedFieldKind::Enum { type_id: enum_type };
        let newtype_kind = if collection {
            AcceptedFieldKind::Set(Box::new(enum_kind))
        } else {
            enum_kind
        };
        let (composites, _, stage_type, _) = build_record_newtype_composite_catalog_for_tests(
            "tests::StageRecord".to_string(),
            "stage".to_string(),
            "tests::StageNewtype".to_string(),
            newtype_kind,
            &enums,
        )
        .expect("newtype enum composite catalog should build");
        let fields = vec![
            scalar_field(1, 0, "id", AcceptedFieldKind::Nat64, ScalarCodec::Nat64),
            composite_field(
                2,
                1,
                "stage",
                AcceptedFieldKind::Composite {
                    type_id: stage_type,
                },
                Vec::new(),
            ),
        ];
        let snapshot = PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "tests::EnumToken".to_string(),
            "EnumToken".to_string(),
            FieldId::new(1),
            SchemaRowLayout::initial(
                fields
                    .iter()
                    .map(|field| (field.id(), field.slot()))
                    .collect(),
            ),
            fields,
        );
        let accepted = AcceptedSchemaSnapshot::new(snapshot);
        let catalog = AcceptedValueCatalogHandle::new_for_tests(
            enums,
            composites,
            AcceptedSchemaRevision::INITIAL,
        );

        SchemaInfo::from_accepted_snapshot_and_catalog(&accepted, catalog, true)
    }

    fn scalar_field(
        id: u32,
        slot: u16,
        name: &str,
        kind: AcceptedFieldKind,
        codec: ScalarCodec,
    ) -> PersistedFieldSnapshot {
        PersistedFieldSnapshot::new_initial(
            FieldId::new(id),
            name.to_string(),
            SchemaFieldSlot::new(slot),
            kind,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(codec),
        )
    }

    fn composite_field(
        id: u32,
        slot: u16,
        name: &str,
        kind: AcceptedFieldKind,
        nested_leaves: Vec<PersistedNestedLeafSnapshot>,
    ) -> PersistedFieldSnapshot {
        PersistedFieldSnapshot::new_initial(
            FieldId::new(id),
            name.to_string(),
            SchemaFieldSlot::new(slot),
            kind,
            nested_leaves,
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::CatalogValue,
            LeafCodec::Structural,
        )
    }

    #[test]
    fn accepted_newtype_fields_use_their_admitted_value_shape_for_query_planning() {
        let schema = newtype_query_schema();
        let text = Value::Text("Copper".to_string());
        let predicates = [
            Predicate::eq("name".to_string(), text.clone()),
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::StartsWith,
                Value::Text("Cop".to_string()),
                CoercionId::Strict,
            )),
            Predicate::TextContainsCi {
                field: "name".to_string(),
                value: Value::Text("opp".to_string()),
            },
        ];

        assert_eq!(
            schema.field("name"),
            Some(&FieldType::Scalar(ScalarKind::Text))
        );
        for predicate in predicates {
            validate_predicate(&schema, &predicate)
                .expect("text newtype should retain underlying predicate support");
        }
        assert_eq!(
            crate::db::query::preparation::with_preparation_work(|work| schema
                .canonicalize_filter_literal("name", &text, work)
                .expect("literal preparation")
                .map(std::borrow::Cow::into_owned)),
            Some(text)
        );
        #[cfg(feature = "sql")]
        assert!(schema.sql_capabilities("name").is_some_and(|capabilities| {
            capabilities.orderable()
                && capabilities.groupable()
                && capabilities.aggregate_input().count()
                && capabilities.aggregate_input().extrema()
        }));
        assert!(matches!(
            FieldSlot::resolve_with_schema(&schema, "name")
                .and_then(|slot| slot.accepted_kind().cloned()),
            Some(AcceptedFieldKind::Text { max_len: Some(64) })
        ));
    }

    #[test]
    fn borrowed_group_labels_match_retained_key_eligibility() {
        use crate::db::query::plan::GroupField;

        let schema = newtype_query_schema();
        for label in [
            "id",
            "name",
            "profile",
            "aliases",
            "profile.name",
            "profile.missing",
            "profile..name",
            ".name",
            "profile.",
            "missing",
            "",
            "名",
        ] {
            assert_eq!(
                GroupField::accepted_kind_for_label(&schema, label).is_some(),
                resolve_group_field(&schema, label).is_some(),
                "{label}"
            );
        }
    }

    #[test]
    fn retained_field_slots_share_detached_accepted_labels_and_query_kinds() {
        use crate::db::query::plan::FieldSlot;

        let schema = newtype_query_schema();
        let cloned_schema = schema.clone();
        let mut retained = Vec::new();
        for field in ["id", "name", "profile", "aliases"] {
            let slot = FieldSlot::resolve_with_schema(&schema, field).unwrap();
            let cloned_slot = slot.clone();
            let resolved_again = FieldSlot::resolve_with_schema(&cloned_schema, field).unwrap();
            let accepted_kind = schema.accepted_query_field_kind(field).unwrap();
            let (accepted_label, _) = super::schema_field_entry(&schema.fields, field).unwrap();
            // Sharing is the ownership contract: neither resolving a key nor
            // cloning its plan/view may recursively copy this metadata tree.
            for candidate in [&slot, &cloned_slot, &resolved_again] {
                assert!(std::ptr::eq(
                    candidate.accepted_kind().unwrap(),
                    accepted_kind
                ));
                assert_eq!(candidate.index(), schema.field_slot_index(field).unwrap());
                assert_eq!(candidate.field(), field);
                assert!(std::ptr::eq(candidate.field(), accepted_label.as_ref()));
            }
            retained.push(slot);
        }
        assert!(FieldSlot::resolve_with_schema(&schema, "missing").is_none());
        drop(cloned_schema);
        drop(schema);
        assert_eq!(retained[1].field(), "name");
        assert!(matches!(
            retained[0].accepted_kind(),
            Some(AcceptedFieldKind::Nat64)
        ));
        assert!(matches!(
            retained[1].accepted_kind(),
            Some(AcceptedFieldKind::Text { max_len: Some(64) })
        ));
        assert!(matches!(
            retained[2].accepted_kind(),
            Some(AcceptedFieldKind::Composite { .. })
        ));
        assert!(
            matches!(retained[3].accepted_kind(), Some(AcceptedFieldKind::Set(inner))
            if matches!(inner.as_ref(), AcceptedFieldKind::Text { max_len: Some(64) }))
        );
    }

    #[test]
    fn retained_group_rebinding_selects_current_metadata_without_mutating_old_slots() {
        use crate::db::query::plan::{FieldSlot, GroupFieldSet};

        let mut current = newtype_query_schema();
        let old_slot = FieldSlot::resolve_with_schema(&current, "name").unwrap();
        let retained = GroupFieldSet::Direct(vec![old_slot.clone()]);
        // A second schema view has the same field identity but a different kind.
        // Retained metadata is immutable; only explicit rebinding selects it.
        let (label, field) = current
            .fields
            .iter_mut()
            .find(|(name, _)| name.as_ref() == "name")
            .unwrap();
        *label = Arc::from("name");
        field.query_kind = Arc::new(AcceptedFieldKind::Bool);
        field.ty = field_type_from_persisted_kind(&field.query_kind);
        let rebound = crate::db::query::preparation::with_preparation_work(|work| {
            retained.resolve_with_schema(&current, work)
        })
        .unwrap()
        .unwrap();
        let rebound_slot = &rebound.as_direct().unwrap()[0];
        assert!(!std::ptr::eq(old_slot.field(), rebound_slot.field()));
        let (current_label, _) = super::schema_field_entry(&current.fields, "name").unwrap();
        assert!(std::ptr::eq(rebound_slot.field(), current_label.as_ref()));
        assert_eq!(
            old_slot, *rebound_slot,
            "field identity is independent of metadata ownership"
        );
        assert!(matches!(
            old_slot.accepted_kind(),
            Some(AcceptedFieldKind::Text { max_len: Some(64) })
        ));
        assert!(matches!(
            rebound_slot.accepted_kind(),
            Some(AcceptedFieldKind::Bool)
        ));
        assert!(std::ptr::eq(
            rebound_slot.accepted_kind().unwrap(),
            current.accepted_query_field_kind("name").unwrap()
        ));
    }

    #[test]
    fn retained_slot_accounting_includes_shared_label_and_kind_allocations() {
        use crate::{db::query::plan::FieldSlot, retained::RetainedBytes};

        let schema = newtype_query_schema();
        let slot = FieldSlot::resolve_with_schema(&schema, "aliases").unwrap();
        let kind_bytes = RetainedBytes::measure(slot.accepted_kind().unwrap(), usize::MAX).unwrap();
        let label_bytes =
            (2 * size_of::<usize>() + slot.field().len()).next_multiple_of(align_of::<usize>());
        let expected = size_of::<FieldSlot>() + label_bytes + 2 * size_of::<usize>() + kind_bytes;
        assert_eq!(RetainedBytes::measure(&slot, expected), Some(expected));
        assert_eq!(RetainedBytes::measure(&slot, expected - 1), None);
        // Independent residents remain conservatively charged for shared data.
        for resident in [slot.clone(), slot] {
            assert_eq!(
                RetainedBytes::measure(&resident, usize::MAX),
                Some(expected)
            );
        }
    }

    #[test]
    fn borrowed_group_validation_matches_current_resolution_without_rebinding() {
        use crate::db::query::plan::{FieldSlot, GroupField, GroupFieldRef};

        let mut schema = newtype_query_schema();
        let direct = resolve_group_field(&schema, "name").unwrap();
        let profile = schema
            .fields
            .iter()
            .position(|(name, _)| name.as_ref() == "profile")
            .unwrap();
        let path = |label: &str, root: &str, segments: &[&str], slot| {
            GroupField::scalar_path_for_test(
                label,
                root,
                segments
                    .iter()
                    .map(|segment| (*segment).to_string())
                    .collect(),
                slot,
                AcceptedFieldKind::Int32,
            )
        };
        let candidates = [
            direct.clone(),
            GroupField::Direct(FieldSlot::from_test_accepted_kind(
                999,
                "name",
                AcceptedFieldKind::Bool,
            )),
            GroupField::Direct(FieldSlot::from_test_accepted_kind(
                2,
                "profile.name",
                AcceptedFieldKind::Bool,
            )),
            path("profile.name", "profile", &["name"], 2),
            path("profile.name", "profile", &["name"], 999),
            path("profile.name", "missing", &["name"], 2),
            path("profile.name", "profile", &["missing"], 2),
            path("profile.name", "profile", &["na", "me"], 2),
            path("profile", "profile", &["name"], 2),
            path("profile..name", "profile", &["", "name"], 2),
            path("missing.name", "missing", &["name"], 2),
        ];
        // The maintained resolver is the reference: eligibility can change
        // with current authority, but this check must not refresh retained kinds.
        for kind in [
            AcceptedFieldKind::Int64,
            AcceptedFieldKind::Text { max_len: Some(9) },
            AcceptedFieldKind::Float64,
            AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Int64)),
        ] {
            schema.fields[profile].1.nested_leaves = vec![PersistedNestedLeafSnapshot::new(
                vec!["name".into()],
                kind,
                false,
            )];
            for candidate in &candidates {
                let borrowed = GroupFieldRef::PathAware(candidate);
                let expected =
                    resolve_group_field(&schema, candidate.field()).is_some_and(|resolved| {
                        borrowed.root_slot() == resolved.root_slot()
                            && borrowed.same_identity(GroupFieldRef::PathAware(&resolved))
                    });
                assert_eq!(
                    borrowed.matches_schema_identity(&schema),
                    expected,
                    "{candidate:?}"
                );
            }
        }
        let slot = direct.as_direct().unwrap();
        assert!(GroupFieldRef::Direct(slot).matches_schema_identity(&schema));
        assert!(GroupFieldRef::Direct(slot).same_identity(GroupFieldRef::PathAware(&direct)));

        schema.fields[profile].1.nested_leaves = vec![PersistedNestedLeafSnapshot::new(
            vec!["name".into()],
            AcceptedFieldKind::Int64,
            false,
        )];
        let retained = path("profile.name", "profile", &["name"], 2);
        assert!(GroupFieldRef::PathAware(&retained).matches_schema_identity(&schema));
        schema.fields[profile].1.slot = 9;
        assert!(!GroupFieldRef::PathAware(&retained).matches_schema_identity(&schema));
    }

    #[test]
    fn grouped_expression_resolution_borrows_current_direct_and_nested_authority() {
        use crate::db::query::plan::{
            GroupField,
            expr::{Expr, FieldPath},
        };

        let mut schema = newtype_query_schema();
        for field in ["id", "name", "aliases", "profile"] {
            let expr = Expr::Field(field.into());
            let kind = GroupField::accepted_kind_for_expr(&schema, &expr)
                .expect("direct resolution does not impose nested-key eligibility");
            assert!(std::ptr::eq(
                kind,
                schema.accepted_query_field_kind(field).unwrap()
            ));
            let retained = resolve_group_field(&schema, field).unwrap();
            assert_eq!(retained.as_direct().unwrap().accepted_kind(), Some(kind));
        }

        // Vary the accepted nested-leaf projection, not generated model metadata.
        // Reusing the expression must always observe the supplied schema view.
        let expr = Expr::FieldPath(FieldPath::new("profile", vec!["name".into()]));
        let profile = schema
            .fields
            .iter()
            .position(|(name, _)| name.as_ref() == "profile")
            .unwrap();
        for kind in [
            AcceptedFieldKind::Int64,
            AcceptedFieldKind::Text { max_len: Some(9) },
        ] {
            schema.fields[profile].1.nested_leaves = vec![PersistedNestedLeafSnapshot::new(
                vec!["name".into()],
                kind,
                false,
            )];
            let borrowed = GroupField::accepted_kind_for_expr(&schema, &expr).unwrap();
            assert!(std::ptr::eq(
                borrowed,
                schema.fields[profile].1.nested_leaves[0].kind()
            ));
            let retained = resolve_group_field(&schema, "profile.name").unwrap();
            assert_eq!(retained.projection_expr(), expr);
            assert_eq!(retained.root_slot(), 2);
            assert_eq!(
                GroupField::accepted_kind_for_expr(&schema, &retained.projection_expr()),
                Some(borrowed)
            );
            #[cfg(feature = "sql")]
            {
                let mut value = Value::Nat64(7);
                crate::db::query::preparation::with_preparation_work(|work| {
                    crate::db::query::plan::canonicalize_grouped_having_numeric_literal_for_expr(
                        &schema, &expr, &mut value, work,
                    )
                })
                .unwrap();
                assert_eq!(
                    value,
                    if matches!(borrowed, AcceptedFieldKind::Int64) {
                        Value::Int64(7)
                    } else {
                        Value::Nat64(7)
                    }
                );
            }
        }
    }

    #[test]
    fn grouped_expression_resolution_keeps_nested_eligibility_and_spelling() {
        use crate::db::query::plan::{
            GroupField,
            expr::{Expr, FieldPath},
        };

        let mut schema = newtype_query_schema();
        let profile = schema
            .fields
            .iter()
            .position(|(name, _)| name.as_ref() == "profile")
            .unwrap();
        for kind in [
            AcceptedFieldKind::Text { max_len: None },
            AcceptedFieldKind::Float64,
            AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Int64)),
            schema.fields[profile].1.nested_leaves[0].kind().clone(),
        ] {
            schema.fields[profile].1.nested_leaves = vec![PersistedNestedLeafSnapshot::new(
                vec!["name".into()],
                kind,
                false,
            )];
            for expr in [
                Expr::Field("profile.name".into()),
                Expr::FieldPath(FieldPath::new("profile", vec!["name".into()])),
                Expr::FieldPath(FieldPath::new("profile.name", vec![String::new()])),
                Expr::FieldPath(FieldPath::new("profile", vec![".name".into()])),
                Expr::FieldPath(FieldPath::new("profile", vec!["name.missing".into()])),
                Expr::FieldPath(FieldPath::new("missing", vec!["name".into()])),
            ] {
                let label = match &expr {
                    Expr::Field(field) => field.as_str().to_string(),
                    Expr::FieldPath(path) => path.path_spec().dotted_label(),
                    _ => unreachable!(),
                };
                assert_eq!(
                    GroupField::accepted_kind_for_expr(&schema, &expr).is_some(),
                    resolve_group_field(&schema, &label).is_some(),
                    "{label}",
                );
            }
            let semantics = crate::db::schema::classify_accepted_field_kind(
                schema.fields[profile].1.nested_leaves[0].kind(),
            );
            assert_eq!(
                GroupField::accepted_kind_for_expr(&schema, &Expr::Field("profile.name".into()))
                    .is_some(),
                semantics.is_scalar()
                    && semantics.is_sql_comparable()
                    && semantics.supports_stable_group_key(),
            );
        }
        assert!(GroupField::accepted_kind_for_expr(&schema, &Expr::Literal(Value::Null)).is_none());
        assert!(
            GroupField::accepted_kind_for_expr(&schema, &Expr::Field("missing".into())).is_none()
        );
    }

    #[test]
    fn query_projection_recurses_but_keeps_structural_composites_closed() {
        let schema = newtype_query_schema();

        assert_eq!(
            schema.field("aliases"),
            Some(&FieldType::Set(Box::new(FieldType::Scalar(
                ScalarKind::Text
            ))))
        );
        assert_eq!(
            schema.nested_field_type("profile", &["name".to_string()]),
            Some(FieldType::Scalar(ScalarKind::Text))
        );
        assert!(matches!(
            schema.field("profile"),
            Some(FieldType::Composite)
        ));
        assert!(matches!(
            validate_predicate(
                &schema,
                &Predicate::eq("profile".to_string(), Value::Text("Copper".to_string())),
            ),
            Err(ValidateError::NonQueryableFieldType { field }) if field == "profile"
        ));
    }

    #[test]
    fn filter_enum_label_copies_use_the_current_request_budget() {
        use crate::db::{
            RequestExecutionRoot,
            executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
            query::preparation::PreparationWork,
        };
        use icydb_diagnostic_code::{
            DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane,
            DiagnosticFactTag,
        };

        for collection in [false, true] {
            let schema = enum_newtype_query_schema(collection);
            let input = Value::Text("Active".into());
            for limit in [5, 6] {
                let root = RequestExecutionRoot::new_for_tests(
                    HardExecutionBudget::uniform_for_tests(
                        16_000_000,
                        HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                    )
                    .with_limit_for_tests(Resource::TemporaryBytes, limit),
                );
                let result = PreparationWork::run(
                    &root.scope(),
                    DiagnosticExecutionLane::PublicRead,
                    |work| {
                        if collection {
                            schema.canonicalize_filter_collection_element("stage", &input, work)
                        } else {
                            schema.canonicalize_filter_literal("stage", &input, work)
                        }
                    },
                );
                if limit == 6 {
                    assert!(matches!(
                        result.expect("label copy fits").as_deref(),
                        Some(Value::Enum(_))
                    ));
                } else {
                    let error = result.expect_err("reject before copying the enum label");
                    assert!(error.diagnostic_facts().contains(&(
                        DiagnosticFactTag::BudgetResource,
                        Resource::TemporaryBytes.raw(),
                    )));
                }
                assert_eq!(root.observed(Resource::TemporaryBytes), 6);
            }
        }
    }

    #[test]
    fn retained_group_path_construction_admits_exact_work_and_copy_limits() {
        use crate::db::{
            RequestExecutionRoot,
            executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
            query::{plan::GroupField, preparation::PreparationWork},
        };
        use icydb_diagnostic_code::{
            DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane,
            DiagnosticFactTag,
        };

        let mut schema = newtype_query_schema();
        let (_, profile) = schema
            .fields
            .iter_mut()
            .find(|(name, _)| name.as_ref() == "profile")
            .unwrap();
        profile.nested_leaves = vec![PersistedNestedLeafSnapshot::new(
            vec!["name".into()],
            AcceptedFieldKind::Int64,
            false,
        )];
        let path = "profile.name";
        let bytes = (size_of::<String>() + path.len() + "profile".len() + "name".len()) as u64;
        let steps = (4 + 2 * path.len() + "profile".len() + "name".len()) as u64;
        for lane in [
            DiagnosticExecutionLane::PublicRead,
            DiagnosticExecutionLane::TrustedRead,
        ] {
            for (resource, exact) in [
                (Resource::TemporaryBytes, bytes),
                (Resource::PredicateExpressionSteps, steps),
            ] {
                for limit in [exact - 1, exact] {
                    let root = RequestExecutionRoot::new_for_tests(
                        HardExecutionBudget::uniform_for_tests(
                            16_000_000,
                            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                        )
                        .with_limit_for_tests(resource, limit),
                    );
                    let result = PreparationWork::run(&root.scope(), lane, |work| {
                        GroupField::resolve_with_schema(&schema, path, work)
                    });
                    if limit == exact {
                        assert_eq!(result.unwrap().unwrap().field(), path);
                    } else {
                        let error = result.unwrap_err();
                        assert!(
                            error
                                .diagnostic_facts()
                                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                        );
                    }
                    assert_eq!(root.observed(resource), exact);
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                }
            }
        }
    }

    #[test]
    fn enum_newtype_literals_normalize_through_the_nominal_admission_contract() {
        let schema = enum_newtype_query_schema(false);
        let predicate = Predicate::eq("stage".to_string(), Value::Text("Active".to_string()));

        let normalized = normalize_enum_literals(&schema, &predicate)
            .expect("loose enum literal should normalize through its newtype contract");

        assert!(matches!(
            &normalized,
            Predicate::Compare(compare) if matches!(compare.value(), Value::Enum(_))
        ));
        validate_predicate(&schema, &normalized)
            .expect("normalized enum newtype predicate should validate");

        let collection_schema = enum_newtype_query_schema(true);
        let contains = Predicate::Compare(ComparePredicate::with_coercion(
            "stage",
            CompareOp::Contains,
            Value::Text("Active".to_string()),
            CoercionId::CollectionElement,
        ));
        let normalized_contains = normalize_enum_literals(&collection_schema, &contains)
            .expect("collection newtype enum literal should normalize");
        assert!(matches!(
            &normalized_contains,
            Predicate::Compare(compare) if matches!(compare.value(), Value::Enum(_))
        ));
        validate_predicate(&collection_schema, &normalized_contains)
            .expect("normalized collection newtype predicate should validate");
    }
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(SchemaExpressionIndexInfo {
Self{ordinal,physical_generation,name,store,unique,unique_constraint,key_items,predicate_sql,value_catalog} => [ordinal,physical_generation,name,store,unique,unique_constraint,key_items,predicate_sql,value_catalog],
});
crate::retained::retained_fields!(SchemaExpressionIndexKeyItemInfo {
Self::FieldPath(field_0) => [field_0],
Self::Expression(field_0) => [field_0],
});
crate::retained::retained_fields!(SchemaFieldInfo {
Self{slot,ty,nullable,leaf_codec,# [cfg (feature = "sql")] sql_capabilities,query_kind,accepted_value_contract,indexed,nested_leaves} => [slot,ty,nullable,leaf_codec,# [cfg (feature = "sql")] sql_capabilities,query_kind,accepted_value_contract,indexed,nested_leaves],
});
crate::retained::retained_fields!(SchemaIndexExpressionInfo {
Self{op,source,canonical_text} => [op,source,canonical_text],
});
crate::retained::retained_fields!(SchemaIndexFieldPathInfo {
Self{field_name,slot,path,persisted_kind,accepted_value_contract,nullable} => [field_name,slot,path,persisted_kind,accepted_value_contract,nullable],
});
crate::retained::retained_fields!(SchemaIndexInfo {
Self{ordinal,physical_generation,name,store,unique,unique_constraint,fields,predicate_sql,value_catalog} => [ordinal,physical_generation,name,store,unique,unique_constraint,fields,predicate_sql,value_catalog],
});
crate::retained::retained_fields!(SchemaInfo {
Self{fields,indexes,expression_indexes,value_catalog,entity_name,primary_key_names} => [fields,indexes,expression_indexes,value_catalog,entity_name,primary_key_names],
});
