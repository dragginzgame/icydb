//! Module: db::schema::proposal
//! Responsibility: compiled schema proposal projection from generated entity metadata.
//! Does not own: live schema persistence or compatibility reconciliation.
//! Boundary: turns trusted `EntityModel` data into a typed schema proposal.

use crate::{
    db::schema::{
        AcceptedFieldKind, FieldId, PersistedFieldSnapshot, PersistedIndexExpressionOp,
        PersistedIndexExpressionSnapshot, PersistedIndexFieldPathSnapshot,
        PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
        PersistedNestedLeafSnapshot, PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot,
        SchemaFieldDefault, SchemaFieldSlot, SchemaFieldWritePolicy, SchemaRowLayout,
        SchemaVersion,
        enum_catalog::{
            AcceptedEnumCatalog, encode_unit_enum_default_in_catalog, resolve_model_field_kind,
        },
        sql_capabilities,
    },
    error::InternalError,
    model::{
        entity::{EntityModel, RelationEdgeModel},
        field::{FieldDatabaseDefault, FieldKind, FieldModel, FieldStorageDecode, LeafCodec},
        index::{IndexExpression, IndexKeyItem, IndexKeyItemsRef, IndexModel},
    },
};

///
/// CompiledSchemaProposal
///
/// Runtime projection of generated entity metadata into schema-identity terms.
/// This is not the live schema authority; it is the compiled proposal that
/// startup reconciliation will compare with the persisted schema snapshot.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct CompiledSchemaProposal {
    entity_path: &'static str,
    entity_name: &'static str,
    declared_schema_version: SchemaVersion,
    primary_key_field_id: FieldId,
    primary_key_field_ids: Vec<FieldId>,
    fields: Vec<CompiledFieldProposal>,
    indexes: Vec<CompiledIndexProposal>,
    relations: Vec<CompiledRelationEdgeProposal>,
}

impl CompiledSchemaProposal {
    /// Return the generated entity path for diagnostics and reconciliation keys.
    #[must_use]
    pub(in crate::db) const fn entity_path(&self) -> &'static str {
        self.entity_path
    }

    /// Return the generated external entity name.
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &'static str {
        self.entity_name
    }

    /// Return the generated source-declared schema version carried by this proposal.
    #[must_use]
    pub(in crate::db) const fn declared_schema_version(&self) -> SchemaVersion {
        self.declared_schema_version
    }

    /// Return the schema field ID assigned to the generated primary key.
    #[must_use]
    pub(in crate::db) const fn first_primary_key_field_id(&self) -> FieldId {
        self.primary_key_field_id
    }

    /// Return the ordered generated primary-key field IDs.
    #[must_use]
    pub(in crate::db) const fn primary_key_field_ids(&self) -> &[FieldId] {
        self.primary_key_field_ids.as_slice()
    }

    /// Return generated field proposals in generated slot order.
    #[must_use]
    pub(in crate::db) const fn fields(&self) -> &[CompiledFieldProposal] {
        self.fields.as_slice()
    }

    /// Return generated field-path index proposals that can already be
    /// represented as accepted schema contracts.
    #[must_use]
    pub(in crate::db) const fn indexes(&self) -> &[CompiledIndexProposal] {
        self.indexes.as_slice()
    }

    /// Return generated relation-edge proposals that can be represented as
    /// accepted source-schema contracts.
    #[must_use]
    pub(in crate::db) const fn relations(&self) -> &[CompiledRelationEdgeProposal] {
        self.relations.as_slice()
    }

    /// Build the initial row layout implied by this compiled proposal.
    ///
    /// This uses proposal-assigned IDs only for first initialization. Once a
    /// persisted schema exists, reconciliation must build the row layout from
    /// stored field IDs and slots instead.
    #[must_use]
    pub(in crate::db) fn initial_row_layout(&self) -> SchemaRowLayout {
        let field_to_slot = self
            .fields()
            .iter()
            .map(|field| (field.id(), field.slot()))
            .collect::<Vec<_>>();

        SchemaRowLayout::new(self.declared_schema_version(), field_to_slot)
    }

    /// Build the initial persisted-schema snapshot implied by this proposal.
    ///
    /// This is only valid for first initialization when no stored schema exists.
    /// Reconciliation must preserve current field IDs, slots, and defaults
    /// once a live persisted schema has been written.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn initial_persisted_schema_snapshot(&self) -> PersistedSchemaSnapshot {
        let kinds = self
            .fields()
            .iter()
            .flat_map(|field| {
                std::iter::once(field.kind()).chain(
                    field
                        .nested_leaves()
                        .iter()
                        .map(CompiledNestedLeafProposal::kind),
                )
            })
            .collect::<Vec<_>>();
        let catalog = crate::db::schema::enum_catalog::build_initial_accepted_enum_catalog_from_kinds_for_tests(
            kinds.as_slice(),
        )
        .expect("test proposal enum catalog should build");
        self.initial_persisted_schema_snapshot_with_enum_catalog(&catalog)
            .expect("test proposal should resolve through its enum catalog")
    }

    /// Build an initial persisted snapshot after catalog-native default admission.
    pub(in crate::db) fn initial_persisted_schema_snapshot_with_enum_catalog(
        &self,
        enum_catalog: &AcceptedEnumCatalog,
    ) -> Result<PersistedSchemaSnapshot, InternalError> {
        self.initial_persisted_schema_snapshot_with_catalog(enum_catalog)
    }

    fn initial_persisted_schema_snapshot_with_catalog(
        &self,
        enum_catalog: &AcceptedEnumCatalog,
    ) -> Result<PersistedSchemaSnapshot, InternalError> {
        let fields = self
            .fields()
            .iter()
            .map(|field| field.initial_persisted_field_snapshot(enum_catalog))
            .collect::<Result<Vec<_>, _>>()?;

        let indexes = self
            .indexes()
            .iter()
            .map(|index| index.initial_persisted_index_snapshot(enum_catalog))
            .collect::<Result<Vec<_>, _>>()?;
        let relations = self
            .relations()
            .iter()
            .map(CompiledRelationEdgeProposal::initial_persisted_relation_snapshot)
            .collect::<Vec<_>>();

        Ok(
            PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
                self.declared_schema_version(),
                self.entity_path().to_string(),
                self.entity_name().to_string(),
                self.primary_key_field_ids().to_vec(),
                self.initial_row_layout(),
                fields,
                indexes,
            )
            .with_relations(relations),
        )
    }
}

///
/// CompiledFieldProposal
///
/// One generated field projected into the schema-identity proposal surface.
/// It carries both durable identity and current generated slot metadata so
/// reconciliation can separate logical field identity from row layout.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct CompiledFieldProposal {
    id: FieldId,
    name: &'static str,
    slot: SchemaFieldSlot,
    kind: FieldKind,
    nested_leaves: Vec<CompiledNestedLeafProposal>,
    nullable: bool,
    database_default: FieldDatabaseDefault,
    write_policy: SchemaFieldWritePolicy,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
}

#[derive(Clone, Debug)]
pub(in crate::db) struct CompiledNestedLeafProposal {
    path: Vec<String>,
    kind: FieldKind,
    nullable: bool,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
}

impl CompiledNestedLeafProposal {
    #[must_use]
    pub(in crate::db) const fn path(&self) -> &[String] {
        self.path.as_slice()
    }

    #[must_use]
    pub(in crate::db) const fn kind(&self) -> FieldKind {
        self.kind
    }

    #[must_use]
    pub(in crate::db) const fn nullable(&self) -> bool {
        self.nullable
    }

    fn initial_persisted_nested_leaf_snapshot(
        &self,
        enum_catalog: &AcceptedEnumCatalog,
    ) -> Result<PersistedNestedLeafSnapshot, InternalError> {
        let kind = resolve_model_field_kind(enum_catalog, self.kind())
            .map_err(|_| InternalError::store_unsupported())?;
        Ok(PersistedNestedLeafSnapshot::new(
            self.path.clone(),
            kind,
            self.nullable,
            self.storage_decode,
            self.leaf_codec,
        ))
    }
}

impl CompiledFieldProposal {
    /// Return the proposed durable identity for this field.
    #[must_use]
    pub(in crate::db) const fn id(&self) -> FieldId {
        self.id
    }

    /// Return the generated field name.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &'static str {
        self.name
    }

    /// Return the generated row slot for this field.
    #[must_use]
    pub(in crate::db) const fn slot(&self) -> SchemaFieldSlot {
        self.slot
    }

    /// Return the generated runtime field kind.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> FieldKind {
        self.kind
    }

    /// Borrow the nested leaf snapshots generated under this top-level field.
    #[must_use]
    pub(in crate::db) const fn nested_leaves(&self) -> &[CompiledNestedLeafProposal] {
        self.nested_leaves.as_slice()
    }

    /// Return whether the generated contract permits explicit `NULL`.
    #[must_use]
    pub(in crate::db) const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Return the generated database-level default contract.
    #[must_use]
    pub(in crate::db) const fn database_default(&self) -> FieldDatabaseDefault {
        self.database_default
    }

    /// Return the generated database-level write policy.
    #[must_use]
    pub(in crate::db) const fn write_policy(&self) -> SchemaFieldWritePolicy {
        self.write_policy
    }

    /// Return the generated persisted decode contract.
    #[must_use]
    pub(in crate::db) const fn storage_decode(&self) -> FieldStorageDecode {
        self.storage_decode
    }

    /// Return the generated leaf codec contract.
    #[must_use]
    pub(in crate::db) const fn leaf_codec(&self) -> LeafCodec {
        self.leaf_codec
    }

    /// Build the initial persisted field snapshot implied by this proposal.
    ///
    /// Name-based enum defaults require the complete store-local catalog.
    fn initial_persisted_field_snapshot(
        &self,
        enum_catalog: &AcceptedEnumCatalog,
    ) -> Result<PersistedFieldSnapshot, InternalError> {
        let kind = resolve_model_field_kind(enum_catalog, self.kind())
            .map_err(|_| InternalError::store_unsupported())?;
        let default = self.persisted_database_default(enum_catalog, &kind)?;
        let nested_leaves = self
            .nested_leaves()
            .iter()
            .map(|leaf| leaf.initial_persisted_nested_leaf_snapshot(enum_catalog))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PersistedFieldSnapshot::new_with_write_policy(
            self.id(),
            self.name().to_string(),
            self.slot(),
            kind,
            nested_leaves,
            self.nullable(),
            default,
            self.write_policy(),
            self.storage_decode(),
            self.leaf_codec(),
        ))
    }

    fn persisted_database_default(
        &self,
        enum_catalog: &AcceptedEnumCatalog,
        kind: &AcceptedFieldKind,
    ) -> Result<SchemaFieldDefault, InternalError> {
        let (enum_path, variant) = match self.database_default() {
            FieldDatabaseDefault::None => return Ok(SchemaFieldDefault::None),
            FieldDatabaseDefault::EncodedSlotPayload(bytes) => {
                return Ok(SchemaFieldDefault::SlotPayload(Vec::from(bytes)));
            }
            FieldDatabaseDefault::AuthoredEnumUnit { enum_path, variant } => (enum_path, variant),
        };
        let Some(expected_type_id) = enum_catalog.type_id(enum_path) else {
            return Err(InternalError::store_unsupported());
        };
        if !matches!(kind, AcceptedFieldKind::Enum { type_id } if *type_id == expected_type_id) {
            return Err(InternalError::store_unsupported());
        }

        let payload = encode_unit_enum_default_in_catalog(enum_catalog, enum_path, variant)
            .map_err(|_| InternalError::store_unsupported())?;
        Ok(SchemaFieldDefault::SlotPayload(payload))
    }
}

///
/// CompiledIndexProposal
///
/// One generated index projected into accepted schema terms.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct CompiledIndexProposal {
    ordinal: u16,
    name: &'static str,
    store: &'static str,
    unique: bool,
    key: CompiledIndexKeyProposal,
    predicate_sql: Option<&'static str>,
}

impl CompiledIndexProposal {
    /// Return the generated stable index ordinal.
    #[must_use]
    pub(in crate::db) const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    /// Return the generated stable index name.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &'static str {
        self.name
    }

    /// Return the generated backing index store path.
    #[must_use]
    pub(in crate::db) const fn store(&self) -> &'static str {
        self.store
    }

    /// Return whether this index enforces value uniqueness.
    #[must_use]
    pub(in crate::db) const fn unique(&self) -> bool {
        self.unique
    }

    /// Borrow the accepted key proposal.
    #[must_use]
    pub(in crate::db) const fn key(&self) -> &CompiledIndexKeyProposal {
        &self.key
    }

    /// Borrow optional schema-declared predicate SQL display metadata.
    #[must_use]
    pub(in crate::db) const fn predicate_sql(&self) -> Option<&'static str> {
        self.predicate_sql
    }

    /// Build the initial persisted index snapshot implied by this proposal.
    pub(in crate::db) fn initial_persisted_index_snapshot(
        &self,
        enum_catalog: &AcceptedEnumCatalog,
    ) -> Result<PersistedIndexSnapshot, InternalError> {
        Ok(PersistedIndexSnapshot::new(
            self.ordinal(),
            self.name().to_string(),
            self.store().to_string(),
            self.unique(),
            self.key().initial_persisted_key_snapshot(enum_catalog)?,
            self.predicate_sql().map(str::to_string),
        ))
    }
}

///
/// CompiledRelationEdgeProposal
///
/// One generated relation edge projected into accepted source-schema terms.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct CompiledRelationEdgeProposal {
    name: &'static str,
    target_path: &'static str,
    local_field_ids: Vec<FieldId>,
}

impl CompiledRelationEdgeProposal {
    /// Return the generated relation-edge name.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &'static str {
        self.name
    }

    /// Return the generated target entity path.
    #[must_use]
    pub(in crate::db) const fn target_path(&self) -> &'static str {
        self.target_path
    }

    /// Borrow ordered accepted local field IDs.
    #[must_use]
    pub(in crate::db) const fn local_field_ids(&self) -> &[FieldId] {
        self.local_field_ids.as_slice()
    }

    /// Build the initial persisted relation-edge snapshot implied by this
    /// proposal.
    #[must_use]
    pub(in crate::db) fn initial_persisted_relation_snapshot(
        &self,
    ) -> PersistedRelationEdgeSnapshot {
        PersistedRelationEdgeSnapshot::new(
            self.name().to_string(),
            self.target_path().to_string(),
            self.local_field_ids().to_vec(),
        )
    }
}

///
/// CompiledIndexKeyProposal
///
/// Accepted-schema projection of one generated index key. Field-path-only keys
/// keep their compact shape; mixed or expression keys preserve explicit item
/// order through `Items`.
///

#[derive(Clone, Debug)]
pub(in crate::db) enum CompiledIndexKeyProposal {
    FieldPath(Vec<CompiledIndexFieldPathProposal>),
    Items(Vec<CompiledIndexKeyItemProposal>),
}

impl CompiledIndexKeyProposal {
    fn initial_persisted_key_snapshot(
        &self,
        enum_catalog: &AcceptedEnumCatalog,
    ) -> Result<PersistedIndexKeySnapshot, InternalError> {
        match self {
            Self::FieldPath(fields) => Ok(PersistedIndexKeySnapshot::FieldPath(
                fields
                    .iter()
                    .map(|field| field.initial_persisted_field_path_snapshot(enum_catalog))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            Self::Items(items) => Ok(PersistedIndexKeySnapshot::Items(
                items
                    .iter()
                    .map(|item| item.initial_persisted_key_item_snapshot(enum_catalog))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
        }
    }
}

///
/// CompiledIndexKeyItemProposal
///
/// One accepted-schema key-item proposal for explicit generated key metadata.
///

#[derive(Clone, Debug)]
pub(in crate::db) enum CompiledIndexKeyItemProposal {
    FieldPath(CompiledIndexFieldPathProposal),
    Expression(CompiledIndexExpressionProposal),
}

impl CompiledIndexKeyItemProposal {
    fn initial_persisted_key_item_snapshot(
        &self,
        enum_catalog: &AcceptedEnumCatalog,
    ) -> Result<PersistedIndexKeyItemSnapshot, InternalError> {
        match self {
            Self::FieldPath(field_path) => Ok(PersistedIndexKeyItemSnapshot::FieldPath(
                field_path.initial_persisted_field_path_snapshot(enum_catalog)?,
            )),
            Self::Expression(expression) => Ok(PersistedIndexKeyItemSnapshot::Expression(
                Box::new(expression.initial_persisted_expression_snapshot(enum_catalog)?),
            )),
        }
    }
}

///
/// CompiledIndexFieldPathProposal
///
/// One generated index field path resolved to accepted field identity and row
/// slot metadata.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct CompiledIndexFieldPathProposal {
    field_id: FieldId,
    slot: SchemaFieldSlot,
    path: Vec<String>,
    kind: FieldKind,
    nullable: bool,
}

impl CompiledIndexFieldPathProposal {
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

    /// Borrow the full accepted field path.
    #[must_use]
    pub(in crate::db) const fn path(&self) -> &[String] {
        self.path.as_slice()
    }

    /// Borrow the accepted persisted field kind at this path.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> FieldKind {
        self.kind
    }

    /// Return whether this field path permits explicit `NULL`.
    #[must_use]
    pub(in crate::db) const fn nullable(&self) -> bool {
        self.nullable
    }

    fn initial_persisted_field_path_snapshot(
        &self,
        enum_catalog: &AcceptedEnumCatalog,
    ) -> Result<PersistedIndexFieldPathSnapshot, InternalError> {
        let kind = resolve_model_field_kind(enum_catalog, self.kind())
            .map_err(|_| InternalError::store_unsupported())?;
        Ok(PersistedIndexFieldPathSnapshot::new(
            self.field_id(),
            self.slot(),
            self.path().to_vec(),
            kind,
            self.nullable(),
        ))
    }
}

///
/// CompiledIndexExpressionProposal
///
/// One generated expression key item resolved into accepted field identity,
/// accepted row slot, and canonical expression output metadata.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct CompiledIndexExpressionProposal {
    op: PersistedIndexExpressionOp,
    source: CompiledIndexFieldPathProposal,
    output_kind: FieldKind,
    canonical_text: String,
}

impl CompiledIndexExpressionProposal {
    fn initial_persisted_expression_snapshot(
        &self,
        enum_catalog: &AcceptedEnumCatalog,
    ) -> Result<PersistedIndexExpressionSnapshot, InternalError> {
        let input_kind = resolve_model_field_kind(enum_catalog, self.source.kind())
            .map_err(|_| InternalError::store_unsupported())?;
        let output_kind = resolve_model_field_kind(enum_catalog, self.output_kind)
            .map_err(|_| InternalError::store_unsupported())?;
        Ok(PersistedIndexExpressionSnapshot::new(
            self.op,
            self.source
                .initial_persisted_field_path_snapshot(enum_catalog)?,
            input_kind,
            output_kind,
            self.canonical_text.clone(),
        ))
    }
}

/// Build the compiled schema proposal for one trusted generated entity model.
#[must_use]
pub(in crate::db) fn compiled_schema_proposal_for_model(
    model: &EntityModel,
) -> CompiledSchemaProposal {
    let fields = model
        .fields()
        .iter()
        .enumerate()
        .map(compiled_field_proposal_from_model_field)
        .collect::<Vec<_>>();
    let indexes = model
        .indexes()
        .iter()
        .filter_map(|index| compiled_index_proposal_from_model_index(index, &fields))
        .collect::<Vec<_>>();
    let relations = model
        .relations()
        .iter()
        .filter_map(|relation| compiled_relation_proposal_from_model_relation(relation, model))
        .collect::<Vec<_>>();

    let proposal = CompiledSchemaProposal {
        entity_path: model.path(),
        entity_name: model.name(),
        declared_schema_version: SchemaVersion::new(model.declared_schema_version()),
        primary_key_field_id: FieldId::from_initial_slot(model.primary_key_slot()),
        primary_key_field_ids: compiled_primary_key_field_ids(model),
        fields,
        indexes,
        relations,
    };

    debug_assert_compiled_schema_proposal_invariants(model, &proposal);

    proposal
}

fn compiled_primary_key_field_ids(model: &EntityModel) -> Vec<FieldId> {
    model
        .primary_key_model()
        .fields()
        .iter()
        .map(|primary_key_field| {
            let slot = model
                .fields()
                .iter()
                .position(|field| std::ptr::eq(field, primary_key_field))
                .unwrap_or_else(|| {
                    panic!(
                        "primary-key field '{}' must be present in generated field table",
                        primary_key_field.name()
                    )
                });
            FieldId::from_initial_slot(slot)
        })
        .collect()
}

// Check the initial proposal projection remains a pure slot-order projection.
// Startup reconciliation preserves accepted stored IDs when a live schema
// already exists; this deterministic assignment belongs only to first contact.
fn debug_assert_compiled_schema_proposal_invariants(
    model: &EntityModel,
    proposal: &CompiledSchemaProposal,
) {
    debug_assert_eq!(
        proposal.first_primary_key_field_id(),
        FieldId::from_initial_slot(model.primary_key_slot())
    );
    debug_assert_eq!(
        proposal.primary_key_field_ids().first().copied(),
        Some(proposal.first_primary_key_field_id())
    );

    let layout = proposal.initial_row_layout();
    let Ok(catalog) =
        crate::db::schema::enum_catalog::build_initial_accepted_enum_catalog(&[model])
    else {
        debug_assert!(false, "generated enum catalog should build");
        return;
    };
    let Ok(snapshot) = proposal.initial_persisted_schema_snapshot_with_enum_catalog(&catalog)
    else {
        debug_assert!(false, "generated defaults should admit through the catalog");
        return;
    };
    debug_assert_eq!(layout.version(), proposal.declared_schema_version());
    debug_assert_eq!(
        layout.version().get(),
        proposal.declared_schema_version().get()
    );
    debug_assert_eq!(layout.field_to_slot().len(), proposal.fields().len());
    debug_assert_eq!(snapshot.version(), proposal.declared_schema_version());
    debug_assert_eq!(snapshot.entity_path(), proposal.entity_path());
    debug_assert_eq!(snapshot.entity_name(), proposal.entity_name());
    debug_assert_eq!(
        snapshot.first_primary_key_field_id(),
        proposal.first_primary_key_field_id()
    );
    debug_assert_eq!(snapshot.row_layout(), &layout);
    debug_assert_eq!(snapshot.fields().len(), proposal.fields().len());
    debug_assert_eq!(snapshot.indexes().len(), proposal.indexes().len());
    debug_assert_eq!(snapshot.relations().len(), proposal.relations().len());

    for field in snapshot.fields() {
        let _ = (
            field.id(),
            field.name(),
            field.slot(),
            field.kind(),
            field.nested_leaves(),
            field.nullable(),
            field.default(),
            field.storage_decode(),
            field.leaf_codec(),
        );

        let capabilities = sql_capabilities(field.kind());
        let aggregate = capabilities.aggregate_input();
        let _ = (
            capabilities.selectable(),
            capabilities.comparable(),
            capabilities.orderable(),
            capabilities.groupable(),
            aggregate.count(),
            aggregate.numeric(),
            aggregate.extrema(),
        );
    }

    for (expected_slot, field) in proposal.fields().iter().enumerate() {
        debug_assert_eq!(field.id(), FieldId::from_initial_slot(expected_slot));
        debug_assert_eq!(
            field.slot(),
            SchemaFieldSlot::from_generated_index(expected_slot)
        );

        let _ = (
            field.name(),
            field.kind(),
            field.nullable(),
            field.database_default(),
            field.write_policy(),
            field.storage_decode(),
            field.leaf_codec(),
            field.nested_leaves(),
            field.initial_persisted_field_snapshot(&catalog),
        );
    }

    for index in proposal.indexes() {
        let _ = (
            index.ordinal(),
            index.name(),
            index.store(),
            index.unique(),
            index.key(),
            index.predicate_sql(),
            index.initial_persisted_index_snapshot(&catalog),
        );
    }

    for relation in proposal.relations() {
        let _ = (
            relation.name(),
            relation.target_path(),
            relation.local_field_ids(),
            relation.initial_persisted_relation_snapshot(),
        );
    }
}

// Project one generated field and its generated slot into the compiled schema
// proposal. This remains a pure projection until live-schema reconciliation
// starts substituting stored field IDs.
fn compiled_field_proposal_from_model_field(
    (slot, field): (usize, &FieldModel),
) -> CompiledFieldProposal {
    let slot = SchemaFieldSlot::from_generated_index(slot);

    CompiledFieldProposal {
        id: FieldId::from_initial_slot(usize::from(slot.get())),
        name: field.name(),
        slot,
        kind: field.kind(),
        nested_leaves: persisted_nested_leaf_snapshots_from_model_fields(field.nested_fields()),
        nullable: field.nullable(),
        database_default: field.database_default(),
        write_policy: SchemaFieldWritePolicy::from_model_policies(
            field.insert_generation(),
            field.write_management(),
        ),
        storage_decode: field.storage_decode(),
        leaf_codec: field.leaf_codec(),
    }
}

// Project one generated index into accepted schema terms.
fn compiled_index_proposal_from_model_index(
    index: &IndexModel,
    fields: &[CompiledFieldProposal],
) -> Option<CompiledIndexProposal> {
    let key = match index.key_items() {
        IndexKeyItemsRef::Fields(field_names) => CompiledIndexKeyProposal::FieldPath(
            field_names
                .iter()
                .map(|field_name| compiled_index_field_path_proposal_from_name(field_name, fields))
                .collect::<Option<Vec<_>>>()?,
        ),
        IndexKeyItemsRef::Items(items) => items
            .iter()
            .map(|item| match item {
                IndexKeyItem::Field(field_name) => {
                    compiled_index_field_path_proposal_from_name(field_name, fields)
                        .map(CompiledIndexKeyItemProposal::FieldPath)
                }
                IndexKeyItem::Expression(expression) => {
                    compiled_index_expression_proposal_from_expression(*expression, fields)
                        .map(CompiledIndexKeyItemProposal::Expression)
                }
            })
            .collect::<Option<Vec<_>>>()
            .map(CompiledIndexKeyProposal::Items)?,
    };

    Some(CompiledIndexProposal {
        ordinal: index.ordinal(),
        name: index.name(),
        store: index.store(),
        unique: index.is_unique(),
        key,
        predicate_sql: index.predicate(),
    })
}

fn compiled_relation_proposal_from_model_relation(
    relation: &RelationEdgeModel,
    model: &EntityModel,
) -> Option<CompiledRelationEdgeProposal> {
    let local_field_ids = relation
        .local_fields()
        .iter()
        .map(|relation_field| {
            model
                .fields()
                .iter()
                .position(|field| std::ptr::eq(field, *relation_field))
                .map(FieldId::from_initial_slot)
        })
        .collect::<Option<Vec<_>>>()?;

    Some(CompiledRelationEdgeProposal {
        name: relation.name(),
        target_path: relation.target_path(),
        local_field_ids,
    })
}

fn compiled_index_expression_proposal_from_expression(
    expression: IndexExpression,
    fields: &[CompiledFieldProposal],
) -> Option<CompiledIndexExpressionProposal> {
    let source = compiled_index_field_path_proposal_from_name(expression.field(), fields)?;
    let op = persisted_expression_op_from_index_expression(expression);
    let output_kind = persisted_expression_output_kind(op, source.kind())?;
    let canonical_text = canonical_expression_text(op, source.path());

    Some(CompiledIndexExpressionProposal {
        op,
        source,
        output_kind,
        canonical_text,
    })
}

const fn persisted_expression_op_from_index_expression(
    expression: IndexExpression,
) -> PersistedIndexExpressionOp {
    match expression {
        IndexExpression::Lower(_) => PersistedIndexExpressionOp::Lower,
        IndexExpression::Upper(_) => PersistedIndexExpressionOp::Upper,
        IndexExpression::Trim(_) => PersistedIndexExpressionOp::Trim,
        IndexExpression::LowerTrim(_) => PersistedIndexExpressionOp::LowerTrim,
        IndexExpression::Date(_) => PersistedIndexExpressionOp::Date,
        IndexExpression::Year(_) => PersistedIndexExpressionOp::Year,
        IndexExpression::Month(_) => PersistedIndexExpressionOp::Month,
        IndexExpression::Day(_) => PersistedIndexExpressionOp::Day,
    }
}

const fn persisted_expression_output_kind(
    op: PersistedIndexExpressionOp,
    source_kind: FieldKind,
) -> Option<FieldKind> {
    match op {
        PersistedIndexExpressionOp::Lower
        | PersistedIndexExpressionOp::Upper
        | PersistedIndexExpressionOp::Trim
        | PersistedIndexExpressionOp::LowerTrim => {
            if matches!(source_kind, FieldKind::Text { .. }) {
                Some(source_kind)
            } else {
                None
            }
        }
        PersistedIndexExpressionOp::Date => {
            if matches!(source_kind, FieldKind::Date | FieldKind::Timestamp) {
                Some(FieldKind::Date)
            } else {
                None
            }
        }
        PersistedIndexExpressionOp::Year
        | PersistedIndexExpressionOp::Month
        | PersistedIndexExpressionOp::Day => {
            if matches!(source_kind, FieldKind::Date | FieldKind::Timestamp) {
                Some(FieldKind::Int64)
            } else {
                None
            }
        }
    }
}

fn canonical_expression_text(op: PersistedIndexExpressionOp, path: &[String]) -> String {
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

fn compiled_index_field_path_proposal_from_name(
    field_name: &str,
    fields: &[CompiledFieldProposal],
) -> Option<CompiledIndexFieldPathProposal> {
    let path = field_name
        .split('.')
        .map(str::to_string)
        .collect::<Vec<_>>();
    let (top_level, relative_path) = path.split_first()?;
    let field = fields.iter().find(|field| field.name() == top_level)?;

    if relative_path.is_empty() {
        return Some(CompiledIndexFieldPathProposal {
            field_id: field.id(),
            slot: field.slot(),
            path,
            kind: field.kind(),
            nullable: field.nullable(),
        });
    }

    let nested = field
        .nested_leaves()
        .iter()
        .find(|leaf| leaf.path() == relative_path)?;

    Some(CompiledIndexFieldPathProposal {
        field_id: field.id(),
        slot: field.slot(),
        path,
        kind: nested.kind(),
        nullable: nested.nullable(),
    })
}

// Flatten generated nested field metadata into path-addressed persisted leaf
// descriptors rooted at one top-level field. The top-level field owns the
// physical slot; nested entries only carry planning metadata for field paths.
fn persisted_nested_leaf_snapshots_from_model_fields(
    fields: &[FieldModel],
) -> Vec<CompiledNestedLeafProposal> {
    let mut leaves = Vec::new();

    for field in fields {
        push_persisted_nested_leaf_snapshots(field, Vec::new(), &mut leaves);
    }

    leaves
}

// Record one nested field itself, then recurse through its children so every
// queryable path segment chain has an accepted-schema descriptor.
fn push_persisted_nested_leaf_snapshots(
    field: &FieldModel,
    mut path: Vec<String>,
    leaves: &mut Vec<CompiledNestedLeafProposal>,
) {
    path.push(field.name().to_string());
    leaves.push(CompiledNestedLeafProposal {
        path: path.clone(),
        kind: field.kind(),
        nullable: field.nullable(),
        storage_decode: field.storage_decode(),
        leaf_codec: field.leaf_codec(),
    });

    for nested in field.nested_fields() {
        push_persisted_nested_leaf_snapshots(nested, path.clone(), leaves);
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
