//! Module: db::schema::snapshot
//! Responsibility: owned persisted-schema snapshot shapes.
//! Does not own: startup reconciliation, stable-memory storage, or generated model metadata.
//! Boundary: schema-owned DTOs that can become the `__icydb_schema` payload.

use crate::{
    db::{
        predicate::relabel_sql_predicate_field_root,
        schema::{
            FieldId, SchemaFieldSlot, SchemaRowLayout, SchemaVersion,
            schema_snapshot_index_integrity_detail, schema_snapshot_integrity_detail,
        },
    },
    error::InternalError,
    model::field::{
        FieldDatabaseDefault, FieldInsertGeneration, FieldKind, FieldStorageDecode,
        FieldWriteManagement, LeafCodec, RelationStrength,
    },
    types::EntityTag,
};

///
/// AcceptedSchemaSnapshot
///
/// Schema snapshot accepted by startup reconciliation.
/// This wrapper marks the boundary between a decoded persisted payload and a
/// schema authority that has been checked against the compiled proposal.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedSchemaSnapshot {
    snapshot: PersistedSchemaSnapshot,
}

///
/// AcceptedSchemaFootprint
///
/// Low-cardinality footprint summary for one accepted schema snapshot. Metrics
/// use this shape to report live schema-authority size without exposing field
/// names, nested paths, or persisted type details.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedSchemaFootprint {
    fields: u64,
    nested_leaf_facts: u64,
}

impl AcceptedSchemaFootprint {
    /// Build one accepted schema footprint from counted snapshot facts.
    #[must_use]
    const fn new(fields: u64, nested_leaf_facts: u64) -> Self {
        Self {
            fields,
            nested_leaf_facts,
        }
    }

    /// Return the number of top-level persisted field facts.
    #[must_use]
    pub(in crate::db) const fn fields(self) -> u64 {
        self.fields
    }

    /// Return the number of accepted nested leaf metadata facts.
    #[must_use]
    pub(in crate::db) const fn nested_leaf_facts(self) -> u64 {
        self.nested_leaf_facts
    }
}

impl AcceptedSchemaSnapshot {
    /// Wrap one persisted snapshot after reconciliation accepts it.
    ///
    /// Production callers use this fallible constructor so accepted schema
    /// metadata cannot bypass the same integrity rules enforced by the schema
    /// store and raw schema codec. Owner-local tests may use `new` to build
    /// deliberately inconsistent fixtures that exercise accessor authority.
    pub(in crate::db) fn try_new(snapshot: PersistedSchemaSnapshot) -> Result<Self, InternalError> {
        if let Some(detail) = schema_snapshot_integrity_detail(
            "accepted schema snapshot",
            snapshot.version(),
            snapshot.primary_key_field_ids(),
            snapshot.row_layout(),
            snapshot.fields(),
        ) {
            return Err(InternalError::store_invariant(detail));
        }

        if let Some(detail) = schema_snapshot_index_integrity_detail(
            "accepted schema snapshot",
            snapshot.row_layout(),
            snapshot.fields(),
            snapshot.indexes(),
        ) {
            return Err(InternalError::store_invariant(detail));
        }

        Ok(Self { snapshot })
    }

    /// Wrap one persisted snapshot for owner-local test fixtures.
    ///
    /// This unchecked constructor exists only for tests that intentionally
    /// construct inconsistent accepted metadata to prove accessor authority.
    /// Runtime acceptance must use `try_new`.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn new(snapshot: PersistedSchemaSnapshot) -> Self {
        Self { snapshot }
    }

    /// Borrow the accepted persisted snapshot payload.
    #[must_use]
    pub(in crate::db) const fn persisted_snapshot(&self) -> &PersistedSchemaSnapshot {
        &self.snapshot
    }

    /// Borrow the accepted entity path.
    #[must_use]
    pub(in crate::db) const fn entity_path(&self) -> &str {
        self.snapshot.entity_path()
    }

    /// Borrow the accepted entity name.
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.snapshot.entity_name()
    }

    /// Borrow accepted primary-key field snapshots in primary-key order.
    #[must_use]
    fn primary_key_fields(&self) -> Vec<&PersistedFieldSnapshot> {
        self.snapshot
            .primary_key_field_ids()
            .iter()
            .filter_map(|field_id| {
                self.snapshot
                    .fields()
                    .iter()
                    .find(|field| field.id() == *field_id)
            })
            .collect()
    }

    /// Return accepted primary-key field names in key order.
    #[must_use]
    pub(in crate::db) fn primary_key_field_names(&self) -> Vec<&str> {
        self.primary_key_fields()
            .iter()
            .map(|field| field.name())
            .collect()
    }

    /// Borrow one accepted field snapshot by its persisted field name.
    #[must_use]
    #[cfg(test)]
    fn field_by_name(&self, name: &str) -> Option<&PersistedFieldSnapshot> {
        self.snapshot
            .fields()
            .iter()
            .find(|field| field.name() == name)
    }

    /// Borrow one accepted field kind by persisted field name.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn field_kind_by_name(&self, name: &str) -> Option<&PersistedFieldKind> {
        self.field_by_name(name).map(PersistedFieldSnapshot::kind)
    }

    /// Return a low-cardinality footprint of accepted schema field facts.
    #[must_use]
    pub(in crate::db) fn footprint(&self) -> AcceptedSchemaFootprint {
        let fields = u64::try_from(self.snapshot.fields().len()).unwrap_or(u64::MAX);
        let nested_leaf_facts = self
            .snapshot
            .fields()
            .iter()
            .map(|field| u64::try_from(field.nested_leaves().len()).unwrap_or(u64::MAX))
            .fold(0u64, u64::saturating_add);

        AcceptedSchemaFootprint::new(fields, nested_leaf_facts)
    }
}

///
/// PersistedSchemaSnapshot
///
/// Owned schema snapshot for one live entity schema.
/// This is the shape intended for the future `__icydb_schema` payload; it is
/// separate from generated `EntityModel` so startup reconciliation can compare
/// stored authority with the compiled proposal.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PersistedSchemaSnapshot {
    version: SchemaVersion,
    entity_path: String,
    entity_name: String,
    primary_key_field_ids: Vec<FieldId>,
    row_layout: SchemaRowLayout,
    fields: Vec<PersistedFieldSnapshot>,
    indexes: Vec<PersistedIndexSnapshot>,
}

pub(in crate::db) trait IntoPrimaryKeyFieldIds {
    fn into_primary_key_field_ids(self) -> Vec<FieldId>;
}

impl IntoPrimaryKeyFieldIds for FieldId {
    fn into_primary_key_field_ids(self) -> Vec<FieldId> {
        vec![self]
    }
}

impl IntoPrimaryKeyFieldIds for Vec<FieldId> {
    fn into_primary_key_field_ids(self) -> Vec<FieldId> {
        self
    }
}

impl PersistedSchemaSnapshot {
    /// Build one persisted schema snapshot from already-validated pieces.
    #[must_use]
    #[allow(
        dead_code,
        reason = "owner-local tests build field-only snapshots while production now preserves accepted index contracts through new_with_indexes"
    )]
    pub(in crate::db) fn new(
        version: SchemaVersion,
        entity_path: String,
        entity_name: String,
        primary_key_field_ids: impl IntoPrimaryKeyFieldIds,
        row_layout: SchemaRowLayout,
        fields: Vec<PersistedFieldSnapshot>,
    ) -> Self {
        Self::new_with_primary_key_fields_and_indexes(
            version,
            entity_path,
            entity_name,
            primary_key_field_ids.into_primary_key_field_ids(),
            row_layout,
            fields,
            Vec::new(),
        )
    }

    /// Build one persisted schema snapshot with accepted index contracts.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn new_with_indexes(
        version: SchemaVersion,
        entity_path: String,
        entity_name: String,
        primary_key_field_ids: impl IntoPrimaryKeyFieldIds,
        row_layout: SchemaRowLayout,
        fields: Vec<PersistedFieldSnapshot>,
        indexes: Vec<PersistedIndexSnapshot>,
    ) -> Self {
        Self::new_with_primary_key_fields_and_indexes(
            version,
            entity_path,
            entity_name,
            primary_key_field_ids.into_primary_key_field_ids(),
            row_layout,
            fields,
            indexes,
        )
    }

    /// Build one persisted schema snapshot with ordered accepted primary-key
    /// field identities and accepted index contracts.
    #[must_use]
    pub(in crate::db) fn new_with_primary_key_fields_and_indexes(
        version: SchemaVersion,
        entity_path: String,
        entity_name: String,
        primary_key_field_ids: impl IntoPrimaryKeyFieldIds,
        row_layout: SchemaRowLayout,
        fields: Vec<PersistedFieldSnapshot>,
        indexes: Vec<PersistedIndexSnapshot>,
    ) -> Self {
        Self {
            version,
            entity_path,
            entity_name,
            primary_key_field_ids: primary_key_field_ids.into_primary_key_field_ids(),
            row_layout,
            fields,
            indexes,
        }
    }

    /// Return the schema version for this snapshot.
    #[must_use]
    pub(in crate::db) const fn version(&self) -> SchemaVersion {
        self.version
    }

    /// Borrow the stored entity path.
    #[must_use]
    pub(in crate::db) const fn entity_path(&self) -> &str {
        self.entity_path.as_str()
    }

    /// Borrow the stored entity name.
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Return the first stored primary-key field identity.
    ///
    /// This accessor exists for scalar-only execution paths that have not yet
    /// admitted composite row identity. Composite-aware code must use
    /// `primary_key_field_ids`.
    #[must_use]
    pub(in crate::db) fn primary_key_field_id(&self) -> FieldId {
        self.primary_key_field_ids[0]
    }

    /// Borrow ordered stored primary-key field identities.
    #[must_use]
    pub(in crate::db) const fn primary_key_field_ids(&self) -> &[FieldId] {
        self.primary_key_field_ids.as_slice()
    }

    /// Borrow the live row-layout mapping for this snapshot.
    #[must_use]
    pub(in crate::db) const fn row_layout(&self) -> &SchemaRowLayout {
        &self.row_layout
    }

    /// Borrow persisted field entries in row-layout order.
    #[must_use]
    pub(in crate::db) const fn fields(&self) -> &[PersistedFieldSnapshot] {
        self.fields.as_slice()
    }

    /// Borrow accepted field-path index contracts for this schema snapshot.
    #[must_use]
    pub(in crate::db) const fn indexes(&self) -> &[PersistedIndexSnapshot] {
        self.indexes.as_slice()
    }
}

///
/// PersistedIndexSnapshot
///
/// Accepted schema metadata for one index contract.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PersistedIndexSnapshot {
    ordinal: u16,
    name: String,
    store: String,
    unique: bool,
    origin: PersistedIndexOrigin,
    key: PersistedIndexKeySnapshot,
    predicate_sql: Option<String>,
}

impl PersistedIndexSnapshot {
    /// Build one generated accepted index snapshot.
    #[must_use]
    pub(in crate::db) const fn new(
        ordinal: u16,
        name: String,
        store: String,
        unique: bool,
        key: PersistedIndexKeySnapshot,
        predicate_sql: Option<String>,
    ) -> Self {
        Self::new_with_origin(
            ordinal,
            name,
            store,
            unique,
            PersistedIndexOrigin::Generated,
            key,
            predicate_sql,
        )
    }

    /// Build one SQL DDL-created accepted index snapshot.
    #[must_use]
    pub(in crate::db) const fn new_sql_ddl(
        ordinal: u16,
        name: String,
        store: String,
        unique: bool,
        key: PersistedIndexKeySnapshot,
        predicate_sql: Option<String>,
    ) -> Self {
        Self::new_with_origin(
            ordinal,
            name,
            store,
            unique,
            PersistedIndexOrigin::SqlDdl,
            key,
            predicate_sql,
        )
    }

    /// Build one accepted index snapshot from already-validated pieces.
    #[must_use]
    pub(in crate::db) const fn new_with_origin(
        ordinal: u16,
        name: String,
        store: String,
        unique: bool,
        origin: PersistedIndexOrigin,
        key: PersistedIndexKeySnapshot,
        predicate_sql: Option<String>,
    ) -> Self {
        Self {
            ordinal,
            name,
            store,
            unique,
            origin,
            key,
            predicate_sql,
        }
    }

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

    /// Return whether this accepted index enforces value uniqueness.
    #[must_use]
    pub(in crate::db) const fn unique(&self) -> bool {
        self.unique
    }

    /// Return whether this accepted index came from the generated entity model.
    #[must_use]
    pub(in crate::db) const fn generated(&self) -> bool {
        matches!(self.origin, PersistedIndexOrigin::Generated)
    }

    /// Return the persisted origin for this accepted index.
    #[must_use]
    pub(in crate::db) const fn origin(&self) -> PersistedIndexOrigin {
        self.origin
    }

    /// Borrow the accepted index key contract.
    #[must_use]
    pub(in crate::db) const fn key(&self) -> &PersistedIndexKeySnapshot {
        &self.key
    }

    /// Borrow optional schema-declared predicate SQL display metadata.
    #[must_use]
    pub(in crate::db) const fn predicate_sql(&self) -> Option<&str> {
        match &self.predicate_sql {
            Some(sql) => Some(sql.as_str()),
            None => None,
        }
    }

    /// Clone this accepted index with display metadata updated for a renamed
    /// accepted field. Physical index identity and key shape remain unchanged.
    #[must_use]
    pub(in crate::db) fn clone_with_renamed_field_path_root(
        &self,
        field_id: FieldId,
        old_name: &str,
        new_name: &str,
    ) -> Self {
        Self {
            ordinal: self.ordinal,
            name: self.name.clone(),
            store: self.store.clone(),
            unique: self.unique,
            origin: self.origin,
            key: self
                .key
                .clone_with_renamed_field_path_root(field_id, new_name),
            predicate_sql: self.predicate_sql.as_deref().map(|predicate_sql| {
                relabel_sql_predicate_field_root(predicate_sql, old_name, new_name)
                    .unwrap_or_else(|| predicate_sql.to_string())
            }),
        }
    }
}

///
/// PersistedIndexOrigin
///
/// Catalog-owned origin marker for one accepted secondary index.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum PersistedIndexOrigin {
    Generated,
    SqlDdl,
}

///
/// PersistedIndexKeySnapshot
///
/// Accepted index-key contract.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum PersistedIndexKeySnapshot {
    FieldPath(Vec<PersistedIndexFieldPathSnapshot>),
    Items(Vec<PersistedIndexKeyItemSnapshot>),
}

impl PersistedIndexKeySnapshot {
    /// Return whether this key carries only accepted field-path components.
    #[must_use]
    pub(in crate::db) const fn is_field_path_only(&self) -> bool {
        matches!(self, Self::FieldPath(_))
    }

    /// Borrow accepted field-path key items when this is a field-path index.
    #[must_use]
    pub(in crate::db) const fn field_paths(&self) -> &[PersistedIndexFieldPathSnapshot] {
        match self {
            Self::FieldPath(paths) => paths.as_slice(),
            Self::Items(_) => &[],
        }
    }

    /// Return whether any key component references the accepted field ID.
    #[must_use]
    pub(in crate::db) fn references_field(&self, field_id: FieldId) -> bool {
        match self {
            Self::FieldPath(paths) => paths.iter().any(|path| path.field_id() == field_id),
            Self::Items(items) => items.iter().any(|item| item.references_field(field_id)),
        }
    }

    /// Clone this key with direct field-path root labels renamed for the
    /// supplied durable field ID.
    #[must_use]
    pub(in crate::db) fn clone_with_renamed_field_path_root(
        &self,
        field_id: FieldId,
        new_name: &str,
    ) -> Self {
        match self {
            Self::FieldPath(paths) => Self::FieldPath(
                paths
                    .iter()
                    .map(|path| path.clone_with_renamed_root(field_id, new_name))
                    .collect(),
            ),
            Self::Items(items) => Self::Items(
                items
                    .iter()
                    .map(|item| item.clone_with_renamed_field_path_root(field_id, new_name))
                    .collect(),
            ),
        }
    }
}

///
/// PersistedIndexKeyItemSnapshot
///
/// Accepted key-item metadata for one explicit index key item. Mixed keys use
/// this shape so field-path and expression components preserve generated order
/// after schema acceptance.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum PersistedIndexKeyItemSnapshot {
    FieldPath(PersistedIndexFieldPathSnapshot),
    Expression(Box<PersistedIndexExpressionSnapshot>),
}

impl PersistedIndexKeyItemSnapshot {
    /// Return whether this key item references the accepted field ID.
    #[must_use]
    pub(in crate::db) fn references_field(&self, field_id: FieldId) -> bool {
        match self {
            Self::FieldPath(path) => path.field_id() == field_id,
            Self::Expression(expression) => expression.source().field_id() == field_id,
        }
    }

    /// Clone this item with direct field-path root labels renamed.
    #[must_use]
    pub(in crate::db) fn clone_with_renamed_field_path_root(
        &self,
        field_id: FieldId,
        new_name: &str,
    ) -> Self {
        match self {
            Self::FieldPath(path) => {
                Self::FieldPath(path.clone_with_renamed_root(field_id, new_name))
            }
            Self::Expression(expression) => Self::Expression(Box::new(
                expression.clone_with_renamed_source_root(field_id, new_name),
            )),
        }
    }
}

///
/// PersistedIndexFieldPathSnapshot
///
/// Accepted key-item metadata for one field-path index component. The top-level
/// field ID plus accepted row slot are persisted together so later runtime key
/// construction can stop reopening generated field order.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PersistedIndexFieldPathSnapshot {
    field_id: FieldId,
    slot: SchemaFieldSlot,
    path: Vec<String>,
    kind: PersistedFieldKind,
    nullable: bool,
}

impl PersistedIndexFieldPathSnapshot {
    /// Build one accepted field-path key-item snapshot.
    #[must_use]
    pub(in crate::db) const fn new(
        field_id: FieldId,
        slot: SchemaFieldSlot,
        path: Vec<String>,
        kind: PersistedFieldKind,
        nullable: bool,
    ) -> Self {
        Self {
            field_id,
            slot,
            path,
            kind,
            nullable,
        }
    }

    /// Return the accepted top-level field identity.
    #[must_use]
    pub(in crate::db) const fn field_id(&self) -> FieldId {
        self.field_id
    }

    /// Return the accepted top-level row slot.
    #[must_use]
    pub(in crate::db) const fn slot(&self) -> SchemaFieldSlot {
        self.slot
    }

    /// Borrow the full accepted field path for this key item.
    #[must_use]
    pub(in crate::db) const fn path(&self) -> &[String] {
        self.path.as_slice()
    }

    /// Borrow the accepted persisted field kind at this key item path.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> &PersistedFieldKind {
        &self.kind
    }

    /// Return whether this accepted field path permits explicit `NULL`.
    #[must_use]
    pub(in crate::db) const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Clone this key item with a renamed top-level accepted field label when
    /// it targets the supplied durable field ID.
    #[must_use]
    pub(in crate::db) fn clone_with_renamed_root(&self, field_id: FieldId, new_name: &str) -> Self {
        let mut path = self.path.clone();
        if self.field_id == field_id
            && let Some(root) = path.first_mut()
        {
            *root = new_name.to_string();
        }

        Self {
            field_id: self.field_id,
            slot: self.slot,
            path,
            kind: self.kind.clone(),
            nullable: self.nullable,
        }
    }
}

///
/// PersistedIndexExpressionSnapshot
///
/// Accepted key-item metadata for one deterministic expression index component.
/// The source remains an accepted field-path contract so expression keys can be
/// derived from persisted row-layout slots instead of generated model order.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PersistedIndexExpressionSnapshot {
    op: PersistedIndexExpressionOp,
    source: PersistedIndexFieldPathSnapshot,
    input_kind: PersistedFieldKind,
    output_kind: PersistedFieldKind,
    canonical_text: String,
}

impl PersistedIndexExpressionSnapshot {
    /// Build one accepted expression key-item snapshot.
    #[must_use]
    pub(in crate::db) const fn new(
        op: PersistedIndexExpressionOp,
        source: PersistedIndexFieldPathSnapshot,
        input_kind: PersistedFieldKind,
        output_kind: PersistedFieldKind,
        canonical_text: String,
    ) -> Self {
        Self {
            op,
            source,
            input_kind,
            output_kind,
            canonical_text,
        }
    }

    /// Return the accepted expression operation.
    #[must_use]
    pub(in crate::db) const fn op(&self) -> PersistedIndexExpressionOp {
        self.op
    }

    /// Borrow the accepted source field-path contract.
    #[must_use]
    pub(in crate::db) const fn source(&self) -> &PersistedIndexFieldPathSnapshot {
        &self.source
    }

    /// Borrow the accepted input field kind.
    #[must_use]
    pub(in crate::db) const fn input_kind(&self) -> &PersistedFieldKind {
        &self.input_kind
    }

    /// Borrow the accepted expression output field kind.
    #[must_use]
    pub(in crate::db) const fn output_kind(&self) -> &PersistedFieldKind {
        &self.output_kind
    }

    /// Borrow the accepted canonical expression text.
    #[must_use]
    pub(in crate::db) const fn canonical_text(&self) -> &str {
        self.canonical_text.as_str()
    }

    /// Clone this expression with its accepted source field-path root label
    /// renamed and canonical expression text regenerated from the new path.
    #[must_use]
    pub(in crate::db) fn clone_with_renamed_source_root(
        &self,
        field_id: FieldId,
        new_name: &str,
    ) -> Self {
        let source = self.source.clone_with_renamed_root(field_id, new_name);
        let canonical_text = canonical_expression_text_for_path(self.op, source.path());

        Self {
            op: self.op,
            source,
            input_kind: self.input_kind.clone(),
            output_kind: self.output_kind.clone(),
            canonical_text,
        }
    }
}

fn canonical_expression_text_for_path(op: PersistedIndexExpressionOp, path: &[String]) -> String {
    let path = path.join(".");
    match op {
        PersistedIndexExpressionOp::Lower => format!("expr:v1:LOWER({path})"),
        PersistedIndexExpressionOp::Upper => format!("expr:v1:UPPER({path})"),
        PersistedIndexExpressionOp::Trim => format!("expr:v1:TRIM({path})"),
        PersistedIndexExpressionOp::LowerTrim => format!("expr:v1:LOWER(TRIM({path}))"),
        PersistedIndexExpressionOp::Date => format!("expr:v1:DATE({path})"),
        PersistedIndexExpressionOp::Year => format!("expr:v1:YEAR({path})"),
        PersistedIndexExpressionOp::Month => format!("expr:v1:MONTH({path})"),
        PersistedIndexExpressionOp::Day => format!("expr:v1:DAY({path})"),
    }
}

///
/// PersistedIndexExpressionOp
///
/// Accepted deterministic expression-index operation. This mirrors the current
/// generated `IndexExpression` subset without retaining generated field names
/// inside the operation itself.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum PersistedIndexExpressionOp {
    Lower,
    Upper,
    Trim,
    LowerTrim,
    Date,
    Year,
    Month,
    Day,
}

///
/// PersistedFieldOrigin
///
/// Durable ownership marker for one accepted field.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum PersistedFieldOrigin {
    Generated,
    SqlDdl,
}

///
/// PersistedFieldSnapshot
///
/// Owned schema snapshot for one live field.
/// It carries durable identity, current slot placement, type/storage metadata,
/// and the database-level default contract used by future reconciliation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PersistedFieldSnapshot {
    id: FieldId,
    name: String,
    slot: SchemaFieldSlot,
    kind: PersistedFieldKind,
    nested_leaves: Vec<PersistedNestedLeafSnapshot>,
    nullable: bool,
    default: SchemaFieldDefault,
    write_policy: SchemaFieldWritePolicy,
    origin: PersistedFieldOrigin,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
}

impl PersistedFieldSnapshot {
    /// Build one persisted field snapshot from already-validated pieces.
    #[cfg(test)]
    #[expect(
        clippy::too_many_arguments,
        reason = "schema snapshot construction keeps every persisted field contract explicit"
    )]
    #[must_use]
    pub(in crate::db) const fn new(
        id: FieldId,
        name: String,
        slot: SchemaFieldSlot,
        kind: PersistedFieldKind,
        nested_leaves: Vec<PersistedNestedLeafSnapshot>,
        nullable: bool,
        default: SchemaFieldDefault,
        storage_decode: FieldStorageDecode,
        leaf_codec: LeafCodec,
    ) -> Self {
        Self::new_with_write_policy(
            id,
            name,
            slot,
            kind,
            nested_leaves,
            nullable,
            default,
            SchemaFieldWritePolicy::none(),
            storage_decode,
            leaf_codec,
        )
    }

    /// Build one persisted field snapshot with explicit database write policy.
    #[expect(
        clippy::too_many_arguments,
        reason = "schema snapshot construction keeps every persisted field contract explicit"
    )]
    #[must_use]
    pub(in crate::db) const fn new_with_write_policy(
        id: FieldId,
        name: String,
        slot: SchemaFieldSlot,
        kind: PersistedFieldKind,
        nested_leaves: Vec<PersistedNestedLeafSnapshot>,
        nullable: bool,
        default: SchemaFieldDefault,
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
            default,
            write_policy,
            PersistedFieldOrigin::Generated,
            storage_decode,
            leaf_codec,
        )
    }

    /// Build one persisted field snapshot with explicit write policy and origin.
    #[expect(
        clippy::too_many_arguments,
        reason = "schema snapshot construction keeps every persisted field contract explicit"
    )]
    #[must_use]
    pub(in crate::db) const fn new_with_write_policy_and_origin(
        id: FieldId,
        name: String,
        slot: SchemaFieldSlot,
        kind: PersistedFieldKind,
        nested_leaves: Vec<PersistedNestedLeafSnapshot>,
        nullable: bool,
        default: SchemaFieldDefault,
        write_policy: SchemaFieldWritePolicy,
        origin: PersistedFieldOrigin,
        storage_decode: FieldStorageDecode,
        leaf_codec: LeafCodec,
    ) -> Self {
        Self {
            id,
            name,
            slot,
            kind,
            nested_leaves,
            nullable,
            default,
            write_policy,
            origin,
            storage_decode,
            leaf_codec,
        }
    }

    /// Return the durable field identity.
    #[must_use]
    pub(in crate::db) const fn id(&self) -> FieldId {
        self.id
    }

    /// Borrow the stored field name.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Return the stored physical slot for this field.
    #[must_use]
    pub(in crate::db) const fn slot(&self) -> SchemaFieldSlot {
        self.slot
    }

    /// Borrow the owned persisted field kind.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> &PersistedFieldKind {
        &self.kind
    }

    /// Borrow persisted nested leaf descriptors rooted at this top-level field.
    #[must_use]
    pub(in crate::db) const fn nested_leaves(&self) -> &[PersistedNestedLeafSnapshot] {
        self.nested_leaves.as_slice()
    }

    /// Return whether this field permits explicit persisted `NULL`.
    #[must_use]
    pub(in crate::db) const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Return the database-level default contract for this field.
    #[must_use]
    pub(in crate::db) const fn default(&self) -> &SchemaFieldDefault {
        &self.default
    }

    /// Return a copy of this field with an updated database-level default.
    #[must_use]
    pub(in crate::db) fn clone_with_default(&self, default: SchemaFieldDefault) -> Self {
        Self {
            id: self.id,
            name: self.name.clone(),
            slot: self.slot,
            kind: self.kind.clone(),
            nested_leaves: self.nested_leaves.clone(),
            nullable: self.nullable,
            default,
            write_policy: self.write_policy,
            origin: self.origin,
            storage_decode: self.storage_decode,
            leaf_codec: self.leaf_codec,
        }
    }

    /// Return a copy of this field with an updated nullability contract.
    #[must_use]
    pub(in crate::db) fn clone_with_nullable(&self, nullable: bool) -> Self {
        Self {
            id: self.id,
            name: self.name.clone(),
            slot: self.slot,
            kind: self.kind.clone(),
            nested_leaves: self.nested_leaves.clone(),
            nullable,
            default: self.default.clone(),
            write_policy: self.write_policy,
            origin: self.origin,
            storage_decode: self.storage_decode,
            leaf_codec: self.leaf_codec,
        }
    }

    /// Return a copy of this field with an updated accepted SQL/catalog name.
    #[must_use]
    pub(in crate::db) fn clone_with_name(&self, name: String) -> Self {
        Self {
            id: self.id,
            name,
            slot: self.slot,
            kind: self.kind.clone(),
            nested_leaves: self.nested_leaves.clone(),
            nullable: self.nullable,
            default: self.default.clone(),
            write_policy: self.write_policy,
            origin: self.origin,
            storage_decode: self.storage_decode,
            leaf_codec: self.leaf_codec,
        }
    }

    /// Return the accepted database-level write policy for this field.
    #[must_use]
    pub(in crate::db) const fn write_policy(&self) -> SchemaFieldWritePolicy {
        self.write_policy
    }

    /// Return the durable field origin.
    #[must_use]
    pub(in crate::db) const fn origin(&self) -> PersistedFieldOrigin {
        self.origin
    }

    /// Return whether this accepted field came from generated schema metadata.
    #[must_use]
    pub(in crate::db) const fn generated(&self) -> bool {
        matches!(self.origin, PersistedFieldOrigin::Generated)
    }

    /// Return the stored payload decode contract.
    #[must_use]
    pub(in crate::db) const fn storage_decode(&self) -> FieldStorageDecode {
        self.storage_decode
    }

    /// Return the stored leaf codec contract.
    #[must_use]
    pub(in crate::db) const fn leaf_codec(&self) -> LeafCodec {
        self.leaf_codec
    }
}

///
/// PersistedNestedLeafSnapshot
///
/// Accepted schema metadata for one queryable nested leaf rooted at a
/// top-level field. The path is relative to the owning persisted field, so
/// nested leaves can describe field-path planning facts without claiming their
/// own physical row slots.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PersistedNestedLeafSnapshot {
    path: Vec<String>,
    kind: PersistedFieldKind,
    nullable: bool,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
}

impl PersistedNestedLeafSnapshot {
    /// Build one nested leaf snapshot from already-validated pieces.
    #[must_use]
    pub(in crate::db) const fn new(
        path: Vec<String>,
        kind: PersistedFieldKind,
        nullable: bool,
        storage_decode: FieldStorageDecode,
        leaf_codec: LeafCodec,
    ) -> Self {
        Self {
            path,
            kind,
            nullable,
            storage_decode,
            leaf_codec,
        }
    }

    /// Borrow the path relative to the owning top-level field.
    #[must_use]
    pub(in crate::db) const fn path(&self) -> &[String] {
        self.path.as_slice()
    }

    /// Borrow the persisted field kind for this nested leaf.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> &PersistedFieldKind {
        &self.kind
    }

    /// Return whether this nested leaf permits explicit persisted `NULL`.
    #[must_use]
    pub(in crate::db) const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Return the nested leaf payload decode contract.
    #[must_use]
    pub(in crate::db) const fn storage_decode(&self) -> FieldStorageDecode {
        self.storage_decode
    }

    /// Return the nested leaf payload codec contract.
    #[must_use]
    pub(in crate::db) const fn leaf_codec(&self) -> LeafCodec {
        self.leaf_codec
    }
}

///
/// SchemaFieldDefault
///
/// Database-level default contract for one persisted field.
/// Defaults are stored as already-validated slot payload bytes instead of as
/// runtime `Value` data so persisted schema metadata remains tied to field
/// codecs rather than to the query/runtime value union.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaFieldDefault {
    None,
    SlotPayload(Vec<u8>),
}

impl SchemaFieldDefault {
    /// Convert runtime model default metadata into persisted schema shape.
    #[must_use]
    pub(in crate::db) fn from_model_default(default: FieldDatabaseDefault) -> Self {
        match default {
            FieldDatabaseDefault::None => Self::None,
            FieldDatabaseDefault::EncodedSlotPayload(bytes) => Self::SlotPayload(Vec::from(bytes)),
        }
    }

    /// Return whether this field declares no database-level default.
    #[must_use]
    pub(in crate::db) const fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    /// Borrow the encoded slot payload for a persisted database default.
    #[must_use]
    pub(in crate::db) const fn slot_payload(&self) -> Option<&[u8]> {
        match self {
            Self::None => None,
            Self::SlotPayload(bytes) => Some(bytes.as_slice()),
        }
    }
}

///
/// SchemaFieldWritePolicy
///
/// SchemaFieldWritePolicy stores database-owned write synthesis and management
/// metadata for one persisted field. It is separate from generated field
/// metadata so SQL/session write admission can rely on accepted schema facts.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaFieldWritePolicy {
    insert_generation: Option<FieldInsertGeneration>,
    write_management: Option<FieldWriteManagement>,
}

impl SchemaFieldWritePolicy {
    /// Build one empty write-policy contract.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn none() -> Self {
        Self {
            insert_generation: None,
            write_management: None,
        }
    }

    /// Build one write-policy contract from generated schema metadata.
    #[must_use]
    pub(in crate::db) const fn from_model_policies(
        insert_generation: Option<FieldInsertGeneration>,
        write_management: Option<FieldWriteManagement>,
    ) -> Self {
        Self {
            insert_generation,
            write_management,
        }
    }

    /// Return the insert-time generated value contract, when present.
    #[must_use]
    pub(in crate::db) const fn insert_generation(self) -> Option<FieldInsertGeneration> {
        self.insert_generation
    }

    /// Return the write-managed field contract, when present.
    #[must_use]
    pub(in crate::db) const fn write_management(self) -> Option<FieldWriteManagement> {
        self.write_management
    }
}

///
/// PersistedFieldKind
///
/// Owned field-kind representation for persisted schema snapshots.
/// It mirrors the runtime `FieldKind` shape but owns strings and nested field
/// kinds so the live schema can outlive generated static metadata.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum PersistedFieldKind {
    Account,
    Blob {
        max_len: Option<u32>,
    },
    Bool,
    Date,
    Decimal {
        scale: u32,
    },
    Duration,
    Enum {
        path: String,
        variants: Vec<PersistedEnumVariant>,
    },
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    IntBig {
        max_bytes: u32,
    },
    Principal,
    Subaccount,
    Text {
        max_len: Option<u32>,
    },
    Timestamp,
    Nat8,
    Nat16,
    Nat32,
    Nat64,
    Nat128,
    NatBig {
        max_bytes: u32,
    },
    Ulid,
    Unit,
    Relation {
        target_path: String,
        target_entity_name: String,
        target_entity_tag: EntityTag,
        target_store_path: String,
        key_kind: Box<Self>,
        strength: PersistedRelationStrength,
    },
    List(Box<Self>),
    Set(Box<Self>),
    Map {
        key: Box<Self>,
        value: Box<Self>,
    },
    Structured {
        queryable: bool,
    },
}

impl PersistedFieldKind {
    /// Convert generated runtime field kind metadata into owned persisted shape.
    #[must_use]
    pub(in crate::db) fn from_model_kind(kind: FieldKind) -> Self {
        match kind {
            FieldKind::Account => Self::Account,
            FieldKind::Blob { max_len } => Self::Blob { max_len },
            FieldKind::Bool => Self::Bool,
            FieldKind::Date => Self::Date,
            FieldKind::Decimal { scale } => Self::Decimal { scale },
            FieldKind::Duration => Self::Duration,
            FieldKind::Enum { path, variants } => Self::Enum {
                path: path.to_string(),
                variants: variants
                    .iter()
                    .map(|variant| PersistedEnumVariant {
                        ident: variant.ident().to_string(),
                        payload_kind: variant
                            .payload_kind()
                            .map(|payload| Box::new(Self::from_model_kind(*payload))),
                        payload_storage_decode: variant.payload_storage_decode(),
                    })
                    .collect(),
            },
            FieldKind::Float32 => Self::Float32,
            FieldKind::Float64 => Self::Float64,
            FieldKind::Int8 => Self::Int8,
            FieldKind::Int16 => Self::Int16,
            FieldKind::Int32 => Self::Int32,
            FieldKind::Int64 => Self::Int64,
            FieldKind::Int128 => Self::Int128,
            FieldKind::IntBig { max_bytes } => Self::IntBig { max_bytes },
            FieldKind::Principal => Self::Principal,
            FieldKind::Subaccount => Self::Subaccount,
            FieldKind::Text { max_len } => Self::Text { max_len },
            FieldKind::Timestamp => Self::Timestamp,
            FieldKind::Nat8 => Self::Nat8,
            FieldKind::Nat16 => Self::Nat16,
            FieldKind::Nat32 => Self::Nat32,
            FieldKind::Nat64 => Self::Nat64,
            FieldKind::Nat128 => Self::Nat128,
            FieldKind::NatBig { max_bytes } => Self::NatBig { max_bytes },
            FieldKind::Ulid => Self::Ulid,
            FieldKind::Unit => Self::Unit,
            FieldKind::Relation {
                target_path,
                target_entity_name,
                target_entity_tag,
                target_store_path,
                key_kind,
                strength,
            } => Self::Relation {
                target_path: target_path.to_string(),
                target_entity_name: target_entity_name.to_string(),
                target_entity_tag,
                target_store_path: target_store_path.to_string(),
                key_kind: Box::new(Self::from_model_kind(*key_kind)),
                strength: PersistedRelationStrength::from_model_strength(strength),
            },
            FieldKind::List(inner) => Self::List(Box::new(Self::from_model_kind(*inner))),
            FieldKind::Set(inner) => Self::Set(Box::new(Self::from_model_kind(*inner))),
            FieldKind::Map { key, value } => Self::Map {
                key: Box::new(Self::from_model_kind(*key)),
                value: Box::new(Self::from_model_kind(*value)),
            },
            FieldKind::Structured { queryable } => Self::Structured { queryable },
        }
    }
}

///
/// PersistedEnumVariant
///
/// Owned persisted-schema representation of one enum variant.
/// The payload metadata is stored recursively so generated enum payload
/// metadata does not remain the live-schema authority after reconciliation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PersistedEnumVariant {
    ident: String,
    payload_kind: Option<Box<PersistedFieldKind>>,
    payload_storage_decode: FieldStorageDecode,
}

impl PersistedEnumVariant {
    /// Build one persisted enum variant from trusted schema metadata.
    #[must_use]
    pub(in crate::db) const fn new(
        ident: String,
        payload_kind: Option<Box<PersistedFieldKind>>,
        payload_storage_decode: FieldStorageDecode,
    ) -> Self {
        Self {
            ident,
            payload_kind,
            payload_storage_decode,
        }
    }

    /// Borrow the stable enum variant identifier.
    #[must_use]
    pub(in crate::db) const fn ident(&self) -> &str {
        self.ident.as_str()
    }

    /// Borrow the persisted payload kind, when this variant stores data.
    #[must_use]
    pub(in crate::db) fn payload_kind(&self) -> Option<&PersistedFieldKind> {
        match self.payload_kind.as_ref() {
            Some(kind) => Some(kind.as_ref()),
            None => None,
        }
    }

    /// Return the payload storage-decode contract.
    #[must_use]
    pub(in crate::db) const fn payload_storage_decode(&self) -> FieldStorageDecode {
        self.payload_storage_decode
    }
}

///
/// PersistedRelationStrength
///
/// Owned relation-strength representation for persisted schema snapshots.
/// It mirrors generated relation metadata without requiring live schema code to
/// depend on generated `RelationStrength` values after reconciliation.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum PersistedRelationStrength {
    Strong,
    Weak,
}

impl PersistedRelationStrength {
    /// Convert generated relation strength into persisted-schema shape.
    #[must_use]
    const fn from_model_strength(strength: RelationStrength) -> Self {
        match strength {
            RelationStrength::Strong => Self::Strong,
            RelationStrength::Weak => Self::Weak,
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::field::ScalarCodec;

    // Build a small accepted schema snapshot with deliberately non-generated
    // slot values so accessor tests prove they read persisted schema facts.
    fn accepted_schema_fixture() -> AcceptedSchemaSnapshot {
        accepted_schema_fixture_with_payload_slots(SchemaFieldSlot::new(7), SchemaFieldSlot::new(7))
    }

    // Build a deliberately inconsistent accepted wrapper for owner-local
    // boundary tests. Production reconciliation rejects this shape, but the
    // accessor must still prove which internal artifact owns slot answers.
    fn accepted_schema_fixture_with_payload_slots(
        layout_slot: SchemaFieldSlot,
        field_slot: SchemaFieldSlot,
    ) -> AcceptedSchemaSnapshot {
        AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "schema::snapshot::tests::Asset".to_string(),
            "Asset".to_string(),
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
                    LeafCodec::Scalar(ScalarCodec::Ulid),
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "payload".to_string(),
                    field_slot,
                    PersistedFieldKind::Blob { max_len: None },
                    vec![PersistedNestedLeafSnapshot::new(
                        vec!["thumbnail".to_string()],
                        PersistedFieldKind::Blob { max_len: None },
                        false,
                        FieldStorageDecode::ByKind,
                        LeafCodec::Scalar(ScalarCodec::Blob),
                    )],
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Blob),
                ),
            ],
        ))
    }

    #[test]
    fn accepted_schema_snapshot_exposes_schema_facts_without_raw_payload_access() {
        let snapshot = accepted_schema_fixture();

        assert_eq!(snapshot.entity_path(), "schema::snapshot::tests::Asset");
        assert_eq!(snapshot.entity_name(), "Asset");
        assert_eq!(snapshot.primary_key_field_names(), ["id"]);
        assert_eq!(
            snapshot.field_kind_by_name("id"),
            Some(&PersistedFieldKind::Ulid)
        );
        assert_eq!(
            snapshot.field_kind_by_name("payload"),
            Some(&PersistedFieldKind::Blob { max_len: None }),
        );
        assert_eq!(snapshot.field_kind_by_name("missing"), None);
    }

    #[test]
    fn accepted_schema_snapshot_exposes_ordered_primary_key_field_names() {
        let snapshot = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "schema::snapshot::tests::Placement".to_string(),
            "Placement".to_string(),
            vec![FieldId::new(2), FieldId::new(1)],
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
                    "entity_id".to_string(),
                    SchemaFieldSlot::new(0),
                    PersistedFieldKind::Ulid,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Ulid),
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "battle_id".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Nat64,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Nat64),
                ),
            ],
        ));

        assert_eq!(
            snapshot.primary_key_field_names(),
            ["battle_id", "entity_id"]
        );
    }

    #[test]
    fn accepted_schema_snapshot_footprint_counts_field_and_nested_leaf_facts() {
        let snapshot = accepted_schema_fixture();
        let footprint = snapshot.footprint();

        assert_eq!(footprint.fields(), 2);
        assert_eq!(footprint.nested_leaf_facts(), 1);
    }

    #[test]
    fn accepted_schema_snapshot_try_new_rejects_invalid_metadata() {
        let snapshot = PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "schema::snapshot::tests::Invalid".to_string(),
            "Invalid".to_string(),
            FieldId::new(99),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![(FieldId::new(1), SchemaFieldSlot::new(0))],
            ),
            vec![PersistedFieldSnapshot::new(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                PersistedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            )],
        );

        let err = AcceptedSchemaSnapshot::try_new(snapshot)
            .expect_err("accepted schema construction should reject invalid metadata");

        assert!(
            err.message()
                .contains("accepted schema snapshot primary key field missing from row layout"),
            "accepted schema construction should report the integrity failure"
        );
    }

    #[test]
    fn accepted_schema_snapshot_try_new_rejects_duplicate_primary_key_fields() {
        let snapshot = PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "schema::snapshot::tests::DuplicatePk".to_string(),
            "DuplicatePk".to_string(),
            vec![FieldId::new(1), FieldId::new(1)],
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![(FieldId::new(1), SchemaFieldSlot::new(0))],
            ),
            vec![PersistedFieldSnapshot::new(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                PersistedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaFieldDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            )],
        );

        let err = AcceptedSchemaSnapshot::try_new(snapshot)
            .expect_err("accepted schema construction should reject duplicate primary-key ids");

        assert!(
            err.message()
                .contains("accepted schema snapshot duplicate primary key field"),
            "accepted schema construction should report duplicate primary-key fields"
        );
    }

    #[test]
    fn accepted_schema_snapshot_try_new_rejects_invalid_index_contract() {
        let snapshot = PersistedSchemaSnapshot::new_with_indexes(
            SchemaVersion::initial(),
            "schema::snapshot::tests::Indexed".to_string(),
            "Indexed".to_string(),
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
                    LeafCodec::Scalar(ScalarCodec::Ulid),
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "email".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Text { max_len: None },
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Text),
                ),
            ],
            vec![PersistedIndexSnapshot::new(
                1,
                "idx_indexed__email".to_string(),
                "indexed::email".to_string(),
                false,
                PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                    FieldId::new(2),
                    SchemaFieldSlot::new(7),
                    vec!["email".to_string()],
                    PersistedFieldKind::Text { max_len: None },
                    false,
                )]),
                None,
            )],
        );

        let err = AcceptedSchemaSnapshot::try_new(snapshot)
            .expect_err("accepted schema construction should reject invalid index metadata");

        assert!(
            err.message()
                .contains("accepted schema snapshot index field slot mismatch"),
            "accepted schema construction should reject index slots that diverge from row layout"
        );
    }

    #[test]
    fn accepted_schema_snapshot_try_new_rejects_invalid_expression_index_contract() {
        let source = PersistedIndexFieldPathSnapshot::new(
            FieldId::new(2),
            SchemaFieldSlot::new(1),
            vec!["email".to_string()],
            PersistedFieldKind::Text { max_len: None },
            false,
        );
        let snapshot = PersistedSchemaSnapshot::new_with_indexes(
            SchemaVersion::initial(),
            "schema::snapshot::tests::ExpressionIndexed".to_string(),
            "ExpressionIndexed".to_string(),
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
                    LeafCodec::Scalar(ScalarCodec::Ulid),
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "email".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Text { max_len: None },
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Text),
                ),
            ],
            vec![PersistedIndexSnapshot::new(
                1,
                "idx_expression_indexed__lower_email".to_string(),
                "expression_indexed::lower_email".to_string(),
                false,
                PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::Expression(
                    Box::new(PersistedIndexExpressionSnapshot::new(
                        PersistedIndexExpressionOp::Lower,
                        source,
                        PersistedFieldKind::Text { max_len: None },
                        PersistedFieldKind::Date,
                        "expr:v1:LOWER(email)".to_string(),
                    )),
                )]),
                None,
            )],
        );

        let err = AcceptedSchemaSnapshot::try_new(snapshot)
            .expect_err("accepted schema construction should reject invalid expression metadata");

        assert!(
            err.message()
                .contains("accepted schema snapshot index expression output kind mismatch"),
            "accepted schema construction should reject expression output-kind drift"
        );
    }
}
