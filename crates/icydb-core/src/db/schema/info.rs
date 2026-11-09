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
    PersistedIndexFieldPathSnapshot, PersistedIndexKeyItemSnapshot, PersistedIndexSnapshot,
    PersistedNestedLeafSnapshot, PersistedSchemaSnapshot, SchemaFieldSlot,
    enum_catalog::AcceptedValueContract, field_type_from_persisted_kind,
    query_field_kind_from_persisted_kind, query_field_type_from_persisted_kind,
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

// Keep dotted-label admission identical for type and membership lookup.
fn split_nested_query_field(name: &str) -> Option<(&str, &str)> {
    let (root, nested) = name.split_once('.')?;
    (!root.is_empty() && !nested.is_empty()).then_some((root, nested))
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

/// Borrowed nested metadata for an existing root, including roots with no leaves.
pub(in crate::db) struct SchemaNestedFields<'a> {
    leaves: &'a [PersistedNestedLeafSnapshot],
    value_catalog: &'a AcceptedValueCatalogHandle,
}

impl<'a> SchemaNestedFields<'a> {
    /// Return whether this root has no accepted nested path metadata.
    #[must_use]
    pub(in crate::db) const fn is_empty(&self) -> bool {
        self.leaves.is_empty()
    }

    // Match borrowed segments without copying paths or projecting leaf types.
    fn leaf<'path>(
        &self,
        segments: impl Iterator<Item = &'path str> + Clone,
    ) -> Option<&'a PersistedNestedLeafSnapshot> {
        self.leaves
            .iter()
            .find(|leaf| leaf.path().iter().map(String::as_str).eq(segments.clone()))
    }

    /// Project one accepted leaf through the current newtype query contract.
    #[must_use]
    pub(in crate::db) fn field_type<'path>(
        &self,
        segments: impl Iterator<Item = &'path str> + Clone,
    ) -> Option<FieldType> {
        let leaf = self.leaf(segments)?;
        Some(query_field_type_from_persisted_kind(
            leaf.kind(),
            self.value_catalog.composite_catalog(),
        ))
    }
}

///
/// AcceptedIndexInfo
///
/// Shared accepted index metadata with independently typed key contracts.
/// Entries source their identity and shape from accepted index snapshots.
///
#[derive(Clone, Debug)]
pub(in crate::db) struct AcceptedIndexInfo<K> {
    ordinal: u16,
    physical_generation: u64,
    name: String,
    store: String,
    unique: bool,
    unique_constraint: Option<AcceptedConstraintIdentity>,
    key_items: Vec<K>,
    predicate: Option<crate::db::schema::AcceptedIndexPredicate>,
    predicate_names: Vec<(FieldId, String)>,
    value_catalog: AcceptedValueCatalogHandle,
}

impl<K> AcceptedIndexInfo<K> {
    // Copy metadata only from the accepted snapshot, alongside its key contracts.
    fn from_accepted(
        index: &PersistedIndexSnapshot,
        snapshot: &PersistedSchemaSnapshot,
        value_catalog: &AcceptedValueCatalogHandle,
        key_items: Vec<K>,
    ) -> Self {
        Self {
            ordinal: index.ordinal(),
            physical_generation: index.physical_generation(),
            name: index.name().to_string(),
            store: index.store().to_string(),
            unique: index.unique(),
            unique_constraint: index
                .unique()
                .then(|| snapshot.unique_constraint_identity(index.schema_id()))
                .flatten(),
            key_items,
            predicate: index.predicate().cloned(),
            predicate_names: snapshot
                .fields()
                .iter()
                .filter(|field| {
                    index
                        .predicate()
                        .is_some_and(|p| p.references_field(field.id()))
                })
                .map(|field| (field.id(), field.name().to_string()))
                .collect(),
            value_catalog: value_catalog.clone(),
        }
    }

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

    /// Return whether the accepted index restricts membership.
    pub(in crate::db) const fn has_predicate(&self) -> bool {
        self.predicate.is_some()
    }

    /// Borrow optional predicate SQL display metadata.
    pub(in crate::db) fn predicate_sql(
        &self,
    ) -> Result<Option<String>, crate::error::InternalError> {
        self.predicate
            .as_ref()
            .map(|predicate| {
                predicate.render_sql_with_names(&self.predicate_names, &self.value_catalog)
            })
            .transpose()
    }

    /// Project the accepted bound tree without reparsing SQL or normalizing it.
    /// Consumers pass the same explicit authority to projection and normalization.
    pub(in crate::db) fn predicate(
        &self,
        work: Option<&dyn crate::db::resource::construction::ConstructionBudget>,
    ) -> Result<Option<crate::db::predicate::Predicate>, crate::error::InternalError> {
        self.predicate
            .as_ref()
            .map(|predicate| {
                predicate.to_predicate_with_names(&self.predicate_names, &self.value_catalog, work)
            })
            .transpose()
    }
}

/// Accepted field-path index contract.
pub(in crate::db) type SchemaIndexInfo = AcceptedIndexInfo<SchemaIndexFieldPathInfo>;

/// Accepted mixed expression/field-path index contract.
pub(in crate::db) type SchemaExpressionIndexInfo =
    AcceptedIndexInfo<SchemaExpressionIndexKeyItemInfo>;

impl SchemaIndexInfo {
    /// Borrow accepted field-path key item metadata for this index.
    #[must_use]
    pub(in crate::db) const fn fields(&self) -> &[SchemaIndexFieldPathInfo] {
        self.key_items.as_slice()
    }

    /// Bind one owned field-path component to this index's catalog authority.
    #[must_use]
    pub(in crate::db) fn accepted_field_contract<'a>(
        &'a self,
        field: &'a SchemaIndexFieldPathInfo,
    ) -> Option<AcceptedValueAdmissionContract<'a>> {
        if !self
            .key_items
            .iter()
            .any(|candidate| std::ptr::eq(candidate, field))
        {
            return None;
        }
        field.accepted_value_contract(&self.value_catalog)
    }
}

impl SchemaExpressionIndexInfo {
    /// Borrow accepted key-item contracts in index key order.
    #[must_use]
    pub(in crate::db) const fn key_items(&self) -> &[SchemaExpressionIndexKeyItemInfo] {
        self.key_items.as_slice()
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

        let (root, nested) = split_nested_query_field(name)?;
        self.nested_fields(root)?
            .field_type(nested.split('.'))
            .map(Cow::Owned)
    }

    /// Check direct or nested field membership without materializing a query type.
    #[must_use]
    pub(in crate::db) fn has_query_field(&self, name: &str) -> bool {
        if self.field(name).is_some() {
            return true;
        }
        split_nested_query_field(name).is_some_and(|(root, nested)| {
            self.nested_fields(root)
                .is_some_and(|fields| fields.leaf(nested.split('.')).is_some())
        })
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

    /// Admit the shared slot-order projection before its scratch allocation and
    /// sort. The compact heapsort uses two position vectors and at most two
    /// comparisons per level for each heap build/extraction root. Reserve a
    /// conservative bound; do not introduce a second ordering implementation.
    pub(in crate::db) fn field_names_in_slot_order_for_preparation(
        &self,
        work: &PreparationWork<'_>,
    ) -> Result<Vec<&str>, QueryError> {
        use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

        let len = self.field_count() as u64;
        let levels = u64::from(len.bit_width());
        let scratch_per_field = size_of::<(usize, &str)>()
            + size_of::<&str>()
            + if len > 1 { 2 * size_of::<usize>() } else { 0 };
        work.charge(
            Resource::TemporaryBytes,
            len.saturating_mul(scratch_per_field as u64),
        )?;
        work.charge(
            Resource::SortComparisons,
            len.saturating_mul(levels).saturating_mul(3),
        )?;
        work.charge(
            Resource::PredicateExpressionSteps,
            len.saturating_mul(3 * levels + 4),
        )?;

        Ok(self.field_names_in_slot_order())
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
        let fields = self.nested_fields(name)?;
        let leaf = fields.leaf(segments.iter().map(String::as_str))?;
        let query_kind = query_field_kind_from_persisted_kind(
            leaf.kind(),
            self.value_catalog.composite_catalog(),
        );
        Some(accepted_sql_capabilities(&query_kind, &self.value_catalog))
    }

    /// Borrow nested metadata in one root lookup; an empty view still has a root.
    #[must_use]
    pub(in crate::db) fn nested_fields(&self, name: &str) -> Option<SchemaNestedFields<'_>> {
        let (_, field) = schema_field_entry(self.fields.as_slice(), name)?;
        Some(SchemaNestedFields {
            leaves: &field.nested_leaves,
            value_catalog: &self.value_catalog,
        })
    }

    /// Borrow the accepted query kind for one nested scalar leaf.
    #[must_use]
    pub(in crate::db) fn accepted_nested_query_field_kind<'a>(
        &self,
        name: &str,
        segments: impl Iterator<Item = &'a str> + Clone,
    ) -> Option<&AcceptedFieldKind> {
        self.nested_fields(name)?
            .leaf(segments)
            .map(PersistedNestedLeafSnapshot::kind)
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
                    schema_expression_index_info_from_accepted_index(
                        index,
                        snapshot,
                        &value_catalog,
                    )
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

    Some(SchemaIndexInfo::from_accepted(
        index,
        snapshot,
        value_catalog,
        index
            .key()
            .field_paths()
            .map(|path| schema_index_field_path_info_from_accepted(path, snapshot, value_catalog))
            .collect(),
    ))
}

pub(in crate::db) fn schema_expression_index_info_from_accepted_index(
    index: &PersistedIndexSnapshot,
    snapshot: &PersistedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Option<SchemaExpressionIndexInfo> {
    let items = index.key().items();

    if !items
        .iter()
        .any(|item| matches!(item, PersistedIndexKeyItemSnapshot::Expression(_)))
    {
        return None;
    }

    Some(SchemaExpressionIndexInfo::from_accepted(
        index,
        snapshot,
        value_catalog,
        items
            .iter()
            .map(|item| schema_expression_index_key_item_info(item, snapshot, value_catalog))
            .collect(),
    ))
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
mod tests;

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(SchemaExpressionIndexInfo {
Self{ordinal,physical_generation,name,store,unique,unique_constraint,key_items,predicate,predicate_names,value_catalog} => [ordinal,physical_generation,name,store,unique,unique_constraint,key_items,predicate,predicate_names,value_catalog],
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
Self{ordinal,physical_generation,name,store,unique,unique_constraint,key_items,predicate,predicate_names,value_catalog} => [ordinal,physical_generation,name,store,unique,unique_constraint,key_items,predicate,predicate_names,value_catalog],
});
crate::retained::retained_fields!(SchemaInfo {
Self{fields,indexes,expression_indexes,value_catalog,entity_name,primary_key_names} => [fields,indexes,expression_indexes,value_catalog,entity_name,primary_key_names],
});
