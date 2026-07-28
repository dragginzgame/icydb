//! Module: relation::reverse_index
//! Responsibility: maintain reverse-index relation targets for relation consistency.
//! Does not own: planner query semantics or execution routing policies.
//! Boundary: applies relation reverse-index mutations during commit pathways.

mod target_keys;

use crate::{
    db::schema::{FieldStorageDecode, LeafCodec},
    db::{
        Db,
        commit::PreparedIndexMutation,
        data::{
            CanonicalSlotReader, DecodedDataStoreKey, RawDataStoreKey, RawRow, ScalarSlotValueRef,
            ScalarValueRef, SlotReader, StructuralRowContract, StructuralSlotReader,
            decode_accepted_relation_target_primary_key_components_bytes,
            decode_runtime_value_from_accepted_field_contract,
        },
        index::{
            IndexEntryValue, IndexId, IndexKey, IndexKeyKind, IndexRowIdentity, IndexState,
            IndexStore, IndexStoreVisit, RawIndexStoreKey, StructuralPrimaryRowReader,
            raw_keys_for_component_prefix_with_kind,
        },
        key_taxonomy::{EncodedPrimaryKey, PrimaryKeyComponent, PrimaryKeyValue},
        registry::{StoreHandle, StoreRelationSourceCapability, StoreRelationTargetCapability},
        relation::{
            AcceptedRelationCardinality, AcceptedRelationTargetAuthority,
            AcceptedRelationTargetContract, AcceptedRelationTupleEdgeLocalComponent,
            RelationTargetDecodeContext, RelationTargetMismatchPolicy,
            accepted_relation_target_metadata_from_kind, accepted_relation_tuple_edge_descriptor,
            accepted_scalar_relation_target_descriptor,
            validate_relation_primary_key_component_kind,
        },
        schema::AcceptedFieldKind,
        schema::{
            AcceptedConstraintIdentity, AcceptedFieldDecodeContract,
            MAX_SCHEMA_PROJECTION_WORK_UNITS, OwnedAcceptedRelationEdgeContract,
            PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot,
        },
    },
    error::{
        ConstraintDiagnostic, ConstraintDiagnosticKind, InternalError,
        SchemaTransitionBudgetResource,
    },
    traits::CanisterKind,
    types::EntityTag,
};
use std::{cell::RefCell, ops::Bound, rc::Rc, thread::LocalKey};

use target_keys::RelationTargetKeys;

///
/// ReverseRelationSourceInfo
///
/// Resolved authority used while preparing reverse-index mutations.
/// Carries only the source entity path and tag required for diagnostics and
/// reverse-index identity, so the heavy mutation loop does not need `S`.
///

#[derive(Clone, Debug)]
pub(crate) struct ReverseRelationSourceInfo {
    path: Rc<str>,
    entity_tag: EntityTag,
}

impl ReverseRelationSourceInfo {
    /// Build structural source authority from an accepted runtime entity identity.
    pub(in crate::db) fn new(path: impl Into<Rc<str>>, entity_tag: EntityTag) -> Self {
        Self {
            path: path.into(),
            entity_tag,
        }
    }

    /// Borrow the accepted source path used for diagnostics.
    #[must_use]
    pub(in crate::db::relation) fn path(&self) -> &str {
        &self.path
    }

    /// Return the structural source entity tag used for reverse-index identity.
    #[must_use]
    pub(in crate::db::relation) const fn entity_tag(&self) -> EntityTag {
        self.entity_tag
    }
}

#[derive(Clone, Debug)]
pub(in crate::db::relation) struct AcceptedRelationInfo {
    constraint: AcceptedConstraintIdentity,
    relation_name: String,
    relation_ordinal: usize,
    physical_generation: u64,
    local_components: AcceptedRelationLocalComponents,
    target: AcceptedRelationTargetIdentity,
    cardinality: AcceptedRelationCardinality,
}

/// Accepted-schema relation projection bound to one exact reverse generation.
///
/// The projection covers either active accepted state or an isolated
/// activation candidate. Callers own visibility, traversal, and publication.
#[derive(Clone, Debug)]
pub(in crate::db) struct RelationConstraintProjection {
    source: ReverseRelationSourceInfo,
    relation_id: crate::db::schema::RelationId,
    relation: AcceptedRelationInfo,
    target_store_path: &'static str,
    target_store: StoreHandle,
}

/// One candidate reverse entry and its registry-owned target store.
#[derive(Clone)]
pub(in crate::db) struct RelationConstraintIndexEntry {
    target_store_path: &'static str,
    target_store: StoreHandle,
    key: RawIndexStoreKey,
}

/// Candidate projection of one source row, including unresolved targets.
pub(in crate::db) struct RelationConstraintRowProjection {
    entries: Vec<RelationConstraintIndexEntry>,
    missing_targets: Vec<RawDataStoreKey>,
}

impl AcceptedRelationInfo {
    fn new(
        constraint: AcceptedConstraintIdentity,
        relation_name: impl Into<String>,
        relation_ordinal: usize,
        physical_generation: u64,
        local_components: AcceptedRelationLocalComponents,
        target_contract: AcceptedRelationTargetContract,
        cardinality: AcceptedRelationCardinality,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            constraint,
            relation_name: relation_name.into(),
            relation_ordinal,
            physical_generation,
            local_components,
            target: AcceptedRelationTargetIdentity::from_target_contract(target_contract)?,
            cardinality,
        })
    }

    #[must_use]
    pub(in crate::db::relation) const fn field_name(&self) -> &str {
        self.relation_name.as_str()
    }

    #[must_use]
    pub(in crate::db::relation) const fn field_index(&self) -> usize {
        self.relation_ordinal
    }

    #[must_use]
    pub(in crate::db::relation) const fn physical_generation(&self) -> u64 {
        self.physical_generation
    }

    #[must_use]
    fn scalar_relation_field_kind(&self) -> Option<&AcceptedFieldKind> {
        self.scalar_local_component()
            .map(AcceptedRelationLocalComponent::field_kind)
    }

    #[must_use]
    const fn local_components(&self) -> &AcceptedRelationLocalComponents {
        &self.local_components
    }

    #[must_use]
    pub(in crate::db::relation) const fn target(&self) -> &AcceptedRelationTargetIdentity {
        &self.target
    }

    const fn cardinality(&self) -> AcceptedRelationCardinality {
        self.cardinality
    }

    fn scalar_local_component(&self) -> Option<&AcceptedRelationLocalComponent> {
        self.local_components.scalar_component()
    }

    pub(in crate::db::relation) fn write_violation(
        &self,
        entity_path: &str,
        primary_key: Option<Vec<u8>>,
    ) -> InternalError {
        InternalError::mutation_constraint_violation(ConstraintDiagnostic::write_violation(
            self.constraint.id().get(),
            self.constraint.name().to_string(),
            ConstraintDiagnosticKind::Relation,
            entity_path.to_string(),
            primary_key,
            self.local_components
                .components()
                .iter()
                .map(|component| component.field_name().to_string())
                .collect(),
        ))
    }
}

impl RelationConstraintProjection {
    /// Bind one isolated activation candidate to row and target-store authority.
    pub(in crate::db) fn new<C: CanisterKind>(
        db: &Db<C>,
        source: ReverseRelationSourceInfo,
        snapshot: &crate::db::schema::PersistedSchemaSnapshot,
        row_contract: &StructuralRowContract,
        edge: &crate::db::schema::PersistedRelationEdgeSnapshot,
    ) -> Result<Self, InternalError> {
        if edge.physical_generation() == 0 {
            return Err(InternalError::store_corruption());
        }
        Self::bind(db, source, snapshot, row_contract, edge)
    }

    /// Bind one active accepted relation to row and target-store authority.
    ///
    /// Initial accepted relations legitimately use generation zero. Activated
    /// candidates use [`Self::new`] so their isolated generation remains
    /// nonzero.
    pub(in crate::db) fn new_active<C: CanisterKind>(
        db: &Db<C>,
        source: ReverseRelationSourceInfo,
        snapshot: &crate::db::schema::PersistedSchemaSnapshot,
        row_contract: &StructuralRowContract,
        edge: &crate::db::schema::PersistedRelationEdgeSnapshot,
    ) -> Result<Self, InternalError> {
        Self::bind(db, source, snapshot, row_contract, edge)
    }

    fn bind<C: CanisterKind>(
        db: &Db<C>,
        source: ReverseRelationSourceInfo,
        snapshot: &crate::db::schema::PersistedSchemaSnapshot,
        row_contract: &StructuralRowContract,
        edge: &crate::db::schema::PersistedRelationEdgeSnapshot,
    ) -> Result<Self, InternalError> {
        let relation =
            relation_info_from_snapshot_edge(db, source.path(), snapshot, row_contract, edge)?;
        let (target_store_path, target_store) =
            relation_target_store_binding(db, &source, &relation)?;
        Ok(Self {
            source,
            relation_id: edge.id(),
            relation,
            target_store_path,
            target_store,
        })
    }

    /// Return the stable accepted logical relation identity.
    #[must_use]
    pub(in crate::db) const fn relation_id(&self) -> crate::db::schema::RelationId {
        self.relation_id
    }

    /// Return the exact generation carried by every projected reverse key.
    #[must_use]
    pub(in crate::db) const fn physical_generation(&self) -> u64 {
        self.relation.physical_generation()
    }

    /// Borrow the target store path participating in projection verification.
    #[must_use]
    pub(in crate::db) const fn target_store_path(&self) -> &'static str {
        self.target_store_path
    }

    /// Return the target store participating in projection verification.
    #[must_use]
    pub(in crate::db) const fn target_store(&self) -> StoreHandle {
        self.target_store
    }

    /// Build canonical inclusive bounds for this active reverse generation.
    pub(in crate::db) fn raw_bounds(
        &self,
    ) -> Result<(Bound<RawIndexStoreKey>, Bound<RawIndexStoreKey>), InternalError> {
        let index_id = reverse_index_id_for_relation(&self.source, &self.relation)?;
        let (lower, upper) = raw_keys_for_component_prefix_with_kind::<Vec<u8>>(
            &index_id,
            IndexKeyKind::System,
            1,
            &[],
        )
        .map_err(|_| InternalError::store_corruption())?;

        Ok((Bound::Included(lower), Bound::Included(upper)))
    }

    /// Prove that one decoded key names this exact active reverse generation.
    #[must_use]
    pub(in crate::db) fn contains_decoded_key(&self, key: &IndexKey) -> bool {
        let Ok(expected) = reverse_index_id_for_relation(&self.source, &self.relation) else {
            return false;
        };
        key.key_kind() == IndexKeyKind::System && key.index_id() == &expected
    }

    /// Project one source row and classify target existence deterministically.
    pub(in crate::db) fn project_row(
        &self,
        source_primary_key: &PrimaryKeyValue,
        row: &StructuralSlotReader<'_>,
        validate_targets: bool,
    ) -> Result<RelationConstraintRowProjection, InternalError> {
        self.project_row_with_target_lookup(
            source_primary_key,
            row,
            validate_targets,
            |target, raw_target| {
                Ok(self
                    .target_store
                    .with_data(|data_store| data_store.get(raw_target).is_some())
                    && target.entity_tag() == self.relation.target().entity_tag())
            },
        )
    }

    fn project_row_with_target_reader(
        &self,
        source_primary_key: &PrimaryKeyValue,
        row: &StructuralSlotReader<'_>,
        validate_targets: bool,
        target_reader: &dyn StructuralPrimaryRowReader,
    ) -> Result<RelationConstraintRowProjection, InternalError> {
        self.project_row_with_target_lookup(
            source_primary_key,
            row,
            validate_targets,
            |target, _| Ok(target_reader.read_primary_row(target)?.is_some()),
        )
    }

    fn project_row_with_target_lookup(
        &self,
        source_primary_key: &PrimaryKeyValue,
        row: &StructuralSlotReader<'_>,
        validate_targets: bool,
        mut target_exists: impl FnMut(
            &DecodedDataStoreKey,
            &RawDataStoreKey,
        ) -> Result<bool, InternalError>,
    ) -> Result<RelationConstraintRowProjection, InternalError> {
        let target_keys =
            relation_target_raw_keys_for_source_slots(row, &self.source, &self.relation)?;
        let mut entries = Vec::with_capacity(target_keys.len());
        let mut missing_targets = Vec::new();
        for target_key in target_keys {
            let target = decode_relation_target_data_key(
                &self.source,
                &self.relation,
                &target_key,
                RelationTargetDecodeContext::ReverseIndexPrepare,
                RelationTargetMismatchPolicy::Reject,
            )?
            .ok_or_else(InternalError::store_invariant)?;
            if validate_targets && !target_exists(&target, &target_key)? {
                missing_targets.push(target_key);
                continue;
            }
            let Some(key) = reverse_index_key_for_target_and_source_primary_key_value(
                &self.source,
                &self.relation,
                &target.primary_key_value(),
                source_primary_key,
            )?
            else {
                continue;
            };
            entries.push(RelationConstraintIndexEntry {
                target_store_path: self.target_store_path,
                target_store: self.target_store,
                key,
            });
        }
        Ok(RelationConstraintRowProjection {
            entries,
            missing_targets,
        })
    }

    /// Build the typed missing-target failure at a live source-write boundary.
    pub(in crate::db) fn missing_target_error(
        &self,
        target_key: &RawDataStoreKey,
        source_primary_key: Option<Vec<u8>>,
    ) -> Result<InternalError, InternalError> {
        let _target = DecodedDataStoreKey::try_from_raw(target_key)
            .map_err(|_| InternalError::store_corruption())?;
        Ok(self
            .relation
            .write_violation(self.source.path(), source_primary_key))
    }

    /// Prepare reverse deltas for one live source-row transition against the
    /// mutation scheduler's authoritative target-row view.
    pub(in crate::db) fn prepare_source_transition(
        &self,
        target_reader: &dyn StructuralPrimaryRowReader,
        validate_targets: bool,
        source_primary_key: &PrimaryKeyValue,
        old_row: Option<&StructuralSlotReader<'_>>,
        new_row: Option<&StructuralSlotReader<'_>>,
    ) -> Result<Vec<PreparedIndexMutation>, InternalError> {
        let old_entries = old_row
            .map(|row| {
                self.project_row_with_target_reader(source_primary_key, row, false, target_reader)
            })
            .transpose()?
            .map(RelationConstraintRowProjection::into_entries)
            .unwrap_or_default();
        let new_projection = new_row
            .map(|row| {
                self.project_row_with_target_reader(
                    source_primary_key,
                    row,
                    validate_targets,
                    target_reader,
                )
            })
            .transpose()?;
        if let Some(missing) = new_projection
            .as_ref()
            .and_then(|projection| projection.missing_targets().first())
        {
            let source_key =
                DecodedDataStoreKey::new(self.source.entity_tag(), source_primary_key).to_raw()?;
            let source_primary_key = source_key
                .encoded_primary_key_bytes()
                .ok_or_else(InternalError::store_invariant)?
                .to_vec();
            return Err(self.missing_target_error(missing, Some(source_primary_key))?);
        }
        let new_entries = new_projection
            .map(RelationConstraintRowProjection::into_entries)
            .unwrap_or_default();

        Ok(merge_relation_entries(old_entries, new_entries))
    }
}

impl RelationConstraintIndexEntry {
    /// Borrow the deterministic target-store ordering identity.
    #[must_use]
    pub(in crate::db) const fn target_store_path(&self) -> &'static str {
        self.target_store_path
    }

    /// Return the target store that owns this candidate reverse entry.
    #[must_use]
    pub(in crate::db) const fn target_store(&self) -> StoreHandle {
        self.target_store
    }

    /// Borrow the fully encoded isolated reverse key.
    #[must_use]
    pub(in crate::db) const fn key(&self) -> &RawIndexStoreKey {
        &self.key
    }
}

fn merge_relation_entries(
    old_entries: Vec<RelationConstraintIndexEntry>,
    new_entries: Vec<RelationConstraintIndexEntry>,
) -> Vec<PreparedIndexMutation> {
    let mut effects = Vec::new();
    let mut old_index = 0usize;
    let mut new_index = 0usize;
    while old_index < old_entries.len() || new_index < new_entries.len() {
        let (entry, old_contains, new_contains) =
            match (old_entries.get(old_index), new_entries.get(new_index)) {
                (Some(old), Some(new)) => {
                    match relation_entry_identity(old).cmp(&relation_entry_identity(new)) {
                        std::cmp::Ordering::Less => {
                            old_index = old_index.saturating_add(1);
                            (old, true, false)
                        }
                        std::cmp::Ordering::Greater => {
                            new_index = new_index.saturating_add(1);
                            (new, false, true)
                        }
                        std::cmp::Ordering::Equal => {
                            old_index = old_index.saturating_add(1);
                            new_index = new_index.saturating_add(1);
                            (old, true, true)
                        }
                    }
                }
                (Some(old), None) => {
                    old_index = old_index.saturating_add(1);
                    (old, true, false)
                }
                (None, Some(new)) => {
                    new_index = new_index.saturating_add(1);
                    (new, false, true)
                }
                (None, None) => break,
            };
        if old_contains == new_contains {
            continue;
        }
        effects.push(PreparedIndexMutation::from_reverse_index_membership(
            entry.target_store.index_store(),
            entry.key.clone(),
            new_contains.then(IndexEntryValue::presence),
            old_contains,
            new_contains,
        ));
    }
    effects
}

const fn relation_entry_identity(
    entry: &RelationConstraintIndexEntry,
) -> (&'static str, &RawIndexStoreKey) {
    (entry.target_store_path, &entry.key)
}

impl RelationConstraintRowProjection {
    /// Borrow canonical reverse entries for this source row.
    #[must_use]
    pub(in crate::db) const fn entries(&self) -> &[RelationConstraintIndexEntry] {
        self.entries.as_slice()
    }

    /// Borrow target keys absent from authoritative target data.
    #[must_use]
    pub(in crate::db) const fn missing_targets(&self) -> &[RawDataStoreKey] {
        self.missing_targets.as_slice()
    }

    /// Consume the projection into candidate reverse entries.
    pub(in crate::db) fn into_entries(self) -> Vec<RelationConstraintIndexEntry> {
        self.entries
    }
}

#[derive(Clone, Debug)]
struct AcceptedRelationLocalComponents {
    components: Vec<AcceptedRelationLocalComponent>,
}

impl AcceptedRelationLocalComponents {
    fn scalar(
        field_index: usize,
        field: AcceptedFieldDecodeContract<'_>,
    ) -> Result<Self, InternalError> {
        Self::try_from_component_specs(&[AcceptedRelationLocalComponentSpec {
            index: field_index,
            field,
        }])
    }

    fn try_from_component_specs(
        components: &[AcceptedRelationLocalComponentSpec<'_>],
    ) -> Result<Self, InternalError> {
        if components.is_empty() {
            return Err(InternalError::relation_source_row_unsupported_key_kind(
                components,
            ));
        }

        Ok(Self {
            components: components
                .iter()
                .map(|component| AcceptedRelationLocalComponent {
                    index: component.index,
                    name: component.field.field_name().to_string(),
                    kind: component.field.kind().clone(),
                    nullable: component.field.nullable(),
                    storage_decode: component.field.storage_decode(),
                    leaf_codec: component.field.leaf_codec(),
                })
                .collect(),
        })
    }

    #[must_use]
    const fn component_count(&self) -> usize {
        self.components.len()
    }

    #[must_use]
    const fn components(&self) -> &[AcceptedRelationLocalComponent] {
        self.components.as_slice()
    }

    #[must_use]
    fn scalar_component(&self) -> Option<&AcceptedRelationLocalComponent> {
        let [component] = self.components.as_slice() else {
            return None;
        };

        Some(component)
    }
}

#[derive(Clone, Copy, Debug)]
struct AcceptedRelationLocalComponentSpec<'a> {
    index: usize,
    field: AcceptedFieldDecodeContract<'a>,
}

#[derive(Clone, Debug)]
struct AcceptedRelationLocalComponent {
    index: usize,
    name: String,
    kind: AcceptedFieldKind,
    nullable: bool,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
}

impl AcceptedRelationLocalComponent {
    #[must_use]
    const fn field_index(&self) -> usize {
        self.index
    }

    #[must_use]
    const fn field_name(&self) -> &str {
        self.name.as_str()
    }

    #[must_use]
    const fn field_kind(&self) -> &AcceptedFieldKind {
        &self.kind
    }

    #[must_use]
    const fn decode_contract(&self) -> AcceptedFieldDecodeContract<'_> {
        AcceptedFieldDecodeContract::new(
            self.name.as_str(),
            &self.kind,
            self.nullable,
            self.storage_decode,
            self.leaf_codec,
        )
    }
}

#[derive(Clone, Debug)]
pub(in crate::db::relation) struct AcceptedRelationTargetIdentity {
    authority: AcceptedRelationTargetAuthority,
    primary_key: AcceptedRelationTargetPrimaryKey,
}

impl AcceptedRelationTargetIdentity {
    #[cfg(test)]
    fn try_new(
        source_path: &str,
        field_name: &str,
        target_path: &str,
        target_entity_name: &str,
        target_entity_tag: EntityTag,
        target_store_path: &str,
        key_kinds: &[AcceptedFieldKind],
    ) -> Result<Self, InternalError> {
        Ok(Self {
            authority: AcceptedRelationTargetAuthority::try_new(
                source_path,
                field_name,
                target_path,
                target_entity_name,
                target_entity_tag,
                target_store_path,
            )?,
            primary_key: AcceptedRelationTargetPrimaryKey::try_from_component_kinds(key_kinds)?,
        })
    }

    fn from_target_contract(
        contract: AcceptedRelationTargetContract,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            primary_key: AcceptedRelationTargetPrimaryKey::try_from_component_kinds(
                contract.primary_key_kinds(),
            )?,
            authority: contract.into_target(),
        })
    }

    #[must_use]
    pub(in crate::db::relation) const fn path(&self) -> &str {
        self.authority.path()
    }

    #[must_use]
    const fn entity_name(&self) -> crate::db::identity::EntityName {
        self.authority.entity_name()
    }

    #[must_use]
    const fn entity_tag(&self) -> EntityTag {
        self.authority.entity_tag()
    }

    #[must_use]
    const fn store_path(&self) -> &str {
        self.authority.store_path()
    }

    #[must_use]
    const fn primary_key(&self) -> &AcceptedRelationTargetPrimaryKey {
        &self.primary_key
    }
}

#[derive(Clone, Debug)]
struct AcceptedRelationTargetPrimaryKey {
    component_kinds: Vec<AcceptedFieldKind>,
}

impl AcceptedRelationTargetPrimaryKey {
    fn try_from_component_kinds(
        component_kinds: &[AcceptedFieldKind],
    ) -> Result<Self, InternalError> {
        if component_kinds.is_empty() {
            return Err(InternalError::relation_source_row_unsupported_key_kind(
                component_kinds,
            ));
        }

        Ok(Self {
            component_kinds: component_kinds.to_vec(),
        })
    }

    #[must_use]
    const fn component_kinds(&self) -> &[AcceptedFieldKind] {
        self.component_kinds.as_slice()
    }

    #[must_use]
    fn single_component_kind(&self) -> Option<&AcceptedFieldKind> {
        let [key_kind] = self.component_kinds.as_slice() else {
            return None;
        };

        Some(key_kind)
    }
}

// Resolve the canonical relation-target decode context label used by
// corruption diagnostics.
const fn relation_target_key_decode_context_label(
    context: RelationTargetDecodeContext,
) -> &'static str {
    match context {
        RelationTargetDecodeContext::DeleteValidation => "delete relation target key decode failed",
        RelationTargetDecodeContext::ReverseIndexPrepare => {
            "relation target key decode failed while preparing reverse index"
        }
    }
}

// Resolve the canonical relation-target entity mismatch label used by
// corruption diagnostics.
const fn relation_target_entity_mismatch_context_label(
    context: RelationTargetDecodeContext,
) -> &'static str {
    match context {
        RelationTargetDecodeContext::DeleteValidation => {
            "relation target entity mismatch during delete validation"
        }
        RelationTargetDecodeContext::ReverseIndexPrepare => {
            "relation target entity mismatch while preparing reverse index"
        }
    }
}

pub(in crate::db::relation) fn accepted_relations_for_row_contract<C>(
    db: &Db<C>,
    source_path: &str,
    source_row_contract: &StructuralRowContract,
    target_path_filter: Option<&str>,
) -> Result<Vec<AcceptedRelationInfo>, InternalError>
where
    C: CanisterKind,
{
    accepted_relations_from_edges(db, source_path, source_row_contract, target_path_filter)
}

fn accepted_relations_from_edges<C>(
    db: &Db<C>,
    source_path: &str,
    source_row_contract: &StructuralRowContract,
    target_path_filter: Option<&str>,
) -> Result<Vec<AcceptedRelationInfo>, InternalError>
where
    C: CanisterKind,
{
    let mut relations = Vec::new();

    for edge in source_row_contract.accepted_relation_edges() {
        let Some(relation) =
            accepted_relation_from_edge(db, source_path, source_row_contract, edge)?
        else {
            continue;
        };

        if target_path_filter.is_some_and(|filter| filter != relation.target().path()) {
            continue;
        }

        relations.push(relation);
    }

    Ok(relations)
}

fn accepted_relation_from_edge<C>(
    db: &Db<C>,
    source_path: &str,
    source_row_contract: &StructuralRowContract,
    edge: &OwnedAcceptedRelationEdgeContract,
) -> Result<Option<AcceptedRelationInfo>, InternalError>
where
    C: CanisterKind,
{
    let local_fields = edge
        .local_field_slots()
        .iter()
        .map(|slot| source_row_contract.required_accepted_field_decode_contract(*slot))
        .collect::<Result<Vec<_>, _>>()?;

    if let [field] = local_fields.as_slice()
        && let Some(descriptor) = accepted_scalar_relation_target_descriptor(
            db,
            source_path,
            edge.name(),
            field.field_name(),
            field.kind(),
            Some(edge.target_path()),
        )?
    {
        let cardinality = descriptor.cardinality();
        return Ok(Some(AcceptedRelationInfo::new(
            edge.constraint().clone(),
            field.field_name(),
            edge.local_field_slots()[0],
            edge.physical_generation(),
            AcceptedRelationLocalComponents::scalar(edge.local_field_slots()[0], *field)?,
            descriptor.into_target_contract(),
            cardinality,
        )?));
    }

    let local_component_facts = local_fields
        .iter()
        .map(|field| AcceptedRelationTupleEdgeLocalComponent::new(field.field_name(), field.kind()))
        .collect::<Vec<_>>();
    let tuple_descriptor = accepted_relation_tuple_edge_descriptor(
        db,
        source_path,
        edge.name(),
        edge.target_path(),
        local_component_facts.as_slice(),
    )?;

    let component_specs = local_fields
        .iter()
        .enumerate()
        .map(|(offset, field)| AcceptedRelationLocalComponentSpec {
            index: edge.local_field_slots()[offset],
            field: *field,
        })
        .collect::<Vec<_>>();

    Ok(Some(AcceptedRelationInfo::new(
        edge.constraint().clone(),
        edge.name(),
        edge.local_field_slots()[0],
        edge.physical_generation(),
        AcceptedRelationLocalComponents::try_from_component_specs(component_specs.as_slice())?,
        tuple_descriptor.into_target_contract(),
        AcceptedRelationCardinality::Single,
    )?))
}

fn relation_info_from_snapshot_edge<C>(
    db: &Db<C>,
    source_path: &str,
    snapshot: &crate::db::schema::PersistedSchemaSnapshot,
    row_contract: &StructuralRowContract,
    edge: &crate::db::schema::PersistedRelationEdgeSnapshot,
) -> Result<AcceptedRelationInfo, InternalError>
where
    C: CanisterKind,
{
    let constraint = snapshot
        .relation_enforcement_identity(edge.id())
        .ok_or_else(InternalError::store_corruption)?;
    let local_fields = edge
        .local_field_ids()
        .iter()
        .map(|field_id| {
            let field = snapshot
                .fields()
                .iter()
                .find(|field| field.id() == *field_id)
                .ok_or_else(InternalError::store_corruption)?;
            let slot = usize::from(field.slot().get());
            row_contract
                .required_accepted_field_decode_contract(slot)
                .map(|contract| (slot, contract))
        })
        .collect::<Result<Vec<_>, _>>()?;

    if let [(slot, field)] = local_fields.as_slice()
        && let Some(descriptor) = accepted_scalar_relation_target_descriptor(
            db,
            source_path,
            edge.name(),
            field.field_name(),
            field.kind(),
            Some(edge.target_path()),
        )?
    {
        let cardinality = descriptor.cardinality();
        return AcceptedRelationInfo::new(
            constraint,
            edge.name(),
            *slot,
            edge.physical_generation(),
            AcceptedRelationLocalComponents::scalar(*slot, *field)?,
            descriptor.into_target_contract(),
            cardinality,
        );
    }

    let local_component_facts = local_fields
        .iter()
        .map(|(_, field)| {
            AcceptedRelationTupleEdgeLocalComponent::new(field.field_name(), field.kind())
        })
        .collect::<Vec<_>>();
    let tuple_descriptor = accepted_relation_tuple_edge_descriptor(
        db,
        source_path,
        edge.name(),
        edge.target_path(),
        local_component_facts.as_slice(),
    )?;
    let component_specs = local_fields
        .iter()
        .map(|(slot, field)| AcceptedRelationLocalComponentSpec {
            index: *slot,
            field: *field,
        })
        .collect::<Vec<_>>();

    let relation_ordinal = local_fields
        .first()
        .map(|(slot, _)| *slot)
        .ok_or_else(InternalError::store_corruption)?;
    AcceptedRelationInfo::new(
        constraint,
        edge.name(),
        relation_ordinal,
        edge.physical_generation(),
        AcceptedRelationLocalComponents::try_from_component_specs(component_specs.as_slice())?,
        tuple_descriptor.into_target_contract(),
        AcceptedRelationCardinality::Single,
    )
}

/// Build the canonical reverse-index id for a `(source entity, relation field)` pair.
fn reverse_index_id_for_relation(
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
) -> Result<IndexId, InternalError> {
    let ordinal = u16::try_from(relation.field_index()).map_err(|err| {
        InternalError::reverse_index_ordinal_overflow(
            source.path(),
            relation.field_name(),
            relation.target().path(),
            err,
        )
    })?;

    Ok(IndexId::new_with_generation(
        source.entity_tag,
        ordinal,
        relation.physical_generation(),
    ))
}

/// Prove that one removed relation owns no surviving reverse physical entry.
///
/// The observer derives the exact physical generation from accepted source
/// authority. It scans the target index store through the same canonical
/// merged view used by runtime reads and rejects malformed or oversized state.
pub(in crate::db) fn prove_empty_reverse_relation_domain(
    index_store: &IndexStore,
    source_entity: EntityTag,
    source_snapshot: &PersistedSchemaSnapshot,
    relation: &PersistedRelationEdgeSnapshot,
) -> Result<(), InternalError> {
    if index_store.state() != IndexState::Ready {
        return Err(InternalError::store_unsupported());
    }
    let local_field = relation
        .local_field_ids()
        .first()
        .and_then(|field_id| {
            source_snapshot
                .fields()
                .iter()
                .find(|field| field.id() == *field_id)
        })
        .ok_or_else(InternalError::store_corruption)?;
    let expected = IndexId::new_with_generation(
        source_entity,
        local_field.slot().get(),
        relation.physical_generation(),
    );
    let mut work_units = 0_usize;
    index_store.visit_entries(|raw_key, _| {
        work_units = work_units.checked_add(1).ok_or_else(|| {
            InternalError::schema_transition_budget_exceeded(
                SchemaTransitionBudgetResource::ProjectionWorkUnits,
            )
        })?;
        if work_units > MAX_SCHEMA_PROJECTION_WORK_UNITS {
            return Err(InternalError::schema_transition_budget_exceeded(
                SchemaTransitionBudgetResource::ProjectionWorkUnits,
            ));
        }
        let key = IndexKey::try_from_raw(raw_key).map_err(|_| InternalError::store_corruption())?;
        if key.key_kind() == IndexKeyKind::System && key.index_id() == &expected {
            return Err(InternalError::store_unsupported());
        }
        Ok(IndexStoreVisit::Continue)
    })
}

/// Build reverse-index prefix bounds for one complete target primary key.
pub(super) fn reverse_index_key_bounds_for_target_primary_key_value(
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
    target_key_value: &PrimaryKeyValue,
) -> Result<Option<(RawIndexStoreKey, RawIndexStoreKey)>, InternalError> {
    let encoded_value =
        encode_reverse_relation_target_identity_component(source, relation, target_key_value)?;

    let index_id = reverse_index_id_for_relation(source, relation)?;
    let (start, end) = raw_keys_for_component_prefix_with_kind(
        &index_id,
        IndexKeyKind::System,
        1,
        std::slice::from_ref(&encoded_value),
    )
    .map_err(|_| InternalError::query_executor_invariant())?;

    Ok(Some((start, end)))
}

/// Build the concrete reverse-index key for one target/source relation edge.
fn reverse_index_key_for_target_and_source_primary_key_value(
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
    target_key_value: &PrimaryKeyValue,
    source_key_value: &PrimaryKeyValue,
) -> Result<Option<RawIndexStoreKey>, InternalError> {
    let encoded_value =
        encode_reverse_relation_target_identity_component(source, relation, target_key_value)?;

    let index_id = reverse_index_id_for_relation(source, relation)?;
    let key = IndexKey::new_from_components_with_primary_key_value(
        &index_id,
        IndexKeyKind::System,
        std::slice::from_ref(&encoded_value),
        source_key_value,
    )?;

    Ok(Some(key.to_raw()?))
}

// Encode full relation target row identity as the reverse-index target
// component. This keeps scalar and composite targets on one key-owned path and
// prevents first-component projection from entering reverse-index storage.
fn encode_reverse_relation_target_identity_component(
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
    target_key_value: &PrimaryKeyValue,
) -> Result<Vec<u8>, InternalError> {
    EncodedPrimaryKey::encode(*target_key_value)
        .map(|encoded| encoded.as_bytes().to_vec())
        .map_err(|err| {
            InternalError::relation_source_row_decode_failed(
                source.path(),
                relation.field_name(),
                relation.target().path(),
                err,
            )
        })
}

// Read relation-target raw keys directly from one already-decoded structural
// source row so commit preflight can reuse slot readers it has already
// validated for forward-index planning.
fn relation_target_raw_keys_for_source_slots(
    row_fields: &StructuralSlotReader<'_>,
    source_info: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
) -> Result<Vec<RawDataStoreKey>, InternalError> {
    let keys = relation_target_keys_for_source_slots(row_fields, source_info, relation)?;

    relation_target_raw_keys_from_relation_target_keys(source_info, relation, keys)
}

/// Check whether one persisted source row still references one complete target
/// primary key for the declared relation.
pub(in crate::db::relation) fn source_row_references_relation_target_primary_key_value(
    raw_row: &RawRow,
    source_row_contract: StructuralRowContract,
    source_info: ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
    target_key: &PrimaryKeyValue,
) -> Result<bool, InternalError> {
    let row_fields =
        StructuralSlotReader::from_raw_row_with_validated_contract(raw_row, source_row_contract)?;

    source_slots_reference_relation_target(&row_fields, &source_info, relation, target_key)
}

// Check one already-decoded structural source row for membership of one target
// key without rebuilding the full canonical target-key vector.
fn source_slots_reference_relation_target(
    row_fields: &StructuralSlotReader<'_>,
    source_info: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
    target_key: &PrimaryKeyValue,
) -> Result<bool, InternalError> {
    let keys = relation_target_keys_for_source_slots(row_fields, source_info, relation)?;

    Ok(keys.contains(target_key))
}

// Canonicalize reverse-index target keys into deterministic sorted-unique order.
fn canonicalize_relation_target_keys(keys: &mut Vec<RawDataStoreKey>) {
    keys.sort_unstable();
    keys.dedup();
}

/// Decode a reverse-index entry into source-key membership for validation.
pub(super) fn decode_reverse_entry(
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
    index_key: &RawIndexStoreKey,
    raw_entry: &IndexEntryValue,
) -> Result<IndexRowIdentity, InternalError> {
    raw_entry.decode_row_identity(index_key).map_err(|err| {
        InternalError::reverse_index_entry_corrupted(
            source.path(),
            relation.field_name(),
            relation.target().path(),
            index_key,
            err,
        )
    })
}

/// Resolve target store handle for one relation edge.
pub(super) fn relation_target_store<C>(
    db: &Db<C>,
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
) -> Result<&'static LocalKey<RefCell<IndexStore>>, InternalError>
where
    C: CanisterKind,
{
    relation_target_store_binding(db, source, relation).map(|(_, store)| store.index_store())
}

// Resolve the registry-owned static path together with its store handle so a
// staged relation projection has deterministic cross-store ordering identity.
fn relation_target_store_binding<C>(
    db: &Db<C>,
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
) -> Result<(&'static str, StoreHandle), InternalError>
where
    C: CanisterKind,
{
    let target = relation.target();
    let (target_store_path, target_store) = db.with_store_registry(|registry| {
        registry
            .iter()
            .find(|(path, _)| *path == target.store_path())
            .ok_or_else(|| {
                InternalError::relation_target_store_missing(
                    source.path(),
                    relation.field_name(),
                    target.path(),
                    target.store_path(),
                    "accepted relation target store is not registered",
                )
            })
    })?;
    let source_runtime = db.accepted_runtime_entity_for_tag(source.entity_tag())?;
    let source_store = db.store_handle(source_runtime.store_path())?;
    if matches!(
        (
            source_store.storage_capabilities().relation_source(),
            target_store.storage_capabilities().relation_target(),
        ),
        (
            StoreRelationSourceCapability::DurableSource,
            StoreRelationTargetCapability::VolatileTarget,
        )
    ) {
        return Err(InternalError::executor_unsupported());
    }

    Ok((target_store_path, target_store))
}

/// Decode one raw relation target key and enforce reverse-index target invariants.
pub(in crate::db::relation) fn decode_relation_target_data_key(
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
    target_raw_key: &RawDataStoreKey,
    context: RelationTargetDecodeContext,
    mismatch_policy: RelationTargetMismatchPolicy,
) -> Result<Option<DecodedDataStoreKey>, InternalError> {
    let target_data_key = DecodedDataStoreKey::try_from_raw(target_raw_key).map_err(|err| {
        InternalError::relation_target_key_decode_failed(
            relation_target_key_decode_context_label(context),
            source.path(),
            relation.field_name(),
            relation.target().path(),
            err,
        )
    })?;

    let target = relation.target();
    if target_data_key.entity_tag() != target.entity_tag() {
        if matches!(mismatch_policy, RelationTargetMismatchPolicy::Skip) {
            return Ok(None);
        }

        return Err(InternalError::relation_target_entity_mismatch(
            relation_target_entity_mismatch_context_label(context),
            source.path(),
            relation.field_name(),
            target.path(),
            target.entity_name().as_str(),
            target.entity_tag().value(),
            target_data_key.entity_tag().value(),
        ));
    }

    Ok(Some(target_data_key))
}

// Convert decoded relation target keys into canonical sorted raw keys.
fn relation_target_raw_keys_from_relation_target_keys(
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
    keys: RelationTargetKeys,
) -> Result<Vec<RawDataStoreKey>, InternalError> {
    let mut keys = keys
        .into_values()
        .into_iter()
        .map(|value| raw_relation_target_key_from_primary_key_value(source, relation, &value))
        .collect::<Result<Vec<_>, _>>()?;
    canonicalize_relation_target_keys(&mut keys);

    Ok(keys)
}

// Decode one relation field into structural target keys through the shared
// scalar-fast-path or field-bytes path used by delete validation and
// reverse-index mutation preparation.
fn relation_target_keys_for_source_slots(
    row_fields: &StructuralSlotReader<'_>,
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
) -> Result<RelationTargetKeys, InternalError> {
    if relation
        .scalar_relation_field_kind()
        .and_then(accepted_relation_target_metadata_from_kind)
        .is_none()
    {
        return relation_target_keys_from_component_slots(row_fields, source, relation);
    }

    // Phase 1: keep single relation slots on the scalar fast path when the
    // persisted field already uses a primary-key-compatible leaf codec.
    if let Some(keys) = relation_target_keys_from_scalar_slot(row_fields, source, relation)? {
        return Ok(keys);
    }

    // Phase 2: decode the declared relation field payload directly into target
    // keys without rebuilding a runtime `Value` container.
    relation_target_keys_from_field_bytes(row_fields, source, relation)
}

fn relation_target_keys_from_component_slots(
    row_fields: &StructuralSlotReader<'_>,
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
) -> Result<RelationTargetKeys, InternalError> {
    let mut components = Vec::with_capacity(relation.local_components().component_count());
    let mut null_count = 0usize;

    for local_component in relation.local_components().components() {
        let bytes = row_fields
            .required_field_bytes(local_component.field_index(), local_component.field_name())?;
        let value = decode_runtime_value_from_accepted_field_contract(
            local_component.decode_contract(),
            bytes,
        )
        .map_err(|err| {
            InternalError::relation_source_row_decode_failed(
                source.path(),
                relation.field_name(),
                relation.target().path(),
                err,
            )
        })?;
        if matches!(value, crate::value::Value::Null) {
            null_count = null_count.saturating_add(1);
            continue;
        }
        let Some(component) = PrimaryKeyComponent::from_runtime_value(&value) else {
            return Err(InternalError::relation_source_row_decode_failed(
                source.path(),
                relation.field_name(),
                relation.target().path(),
                "unsupported composite relation target component",
            ));
        };
        components.push(component);
    }

    if null_count == relation.local_components().component_count() {
        return Ok(RelationTargetKeys::none());
    }
    if null_count != 0 {
        return Err(InternalError::relation_source_row_decode_failed(
            source.path(),
            relation.field_name(),
            relation.target().path(),
            "partial composite relation target tuple",
        ));
    }

    let key = relation_target_primary_key_value_from_components(components.as_slice())?;

    Ok(RelationTargetKeys::one(&key))
}

fn relation_target_primary_key_value_from_components(
    components: &[PrimaryKeyComponent],
) -> Result<PrimaryKeyValue, InternalError> {
    match components {
        [component] => Ok(PrimaryKeyValue::Scalar(*component)),
        _ => Ok(PrimaryKeyValue::Composite(
            crate::db::key_taxonomy::CompositePrimaryKeyValue::try_from_components(components)
                .map_err(InternalError::relation_source_row_unsupported_key_kind)?,
        )),
    }
}

// Decode the one relation field payload needed by structural delete
// validation directly into relation target keys from the encoded field bytes.
fn relation_target_keys_from_field_bytes(
    row_fields: &StructuralSlotReader<'_>,
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
) -> Result<RelationTargetKeys, InternalError> {
    validate_relation_field_kind(relation)?;

    let component = relation.scalar_local_component().ok_or_else(|| {
        InternalError::relation_source_row_unsupported_key_kind(
            relation.target().primary_key().component_kinds(),
        )
    })?;
    let bytes = row_fields.required_field_bytes(component.field_index(), component.field_name())?;
    let keys =
        decode_accepted_relation_target_primary_key_components_bytes(bytes, component.field_kind())
            .map_err(|err| {
                InternalError::relation_source_row_decode_failed(
                    source.path(),
                    relation.field_name(),
                    relation.target().path(),
                    err,
                )
            })?;

    Ok(RelationTargetKeys::from_scalar_components(keys))
}

// Decode one singular relation directly from the scalar slot codec when
// the relation key kind is already primary-key-compatible on the persisted row.
fn relation_target_keys_from_scalar_slot(
    row_fields: &StructuralSlotReader<'_>,
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
) -> Result<Option<RelationTargetKeys>, InternalError> {
    let Some(field_kind) = relation.scalar_relation_field_kind() else {
        return Ok(None);
    };
    if !matches!(field_kind, AcceptedFieldKind::Relation { .. }) {
        return Ok(None);
    }
    if !relation_scalar_slot_fast_path_key_kind_supported(field_kind) {
        return Ok(None);
    }
    if !matches!(
        row_fields.field_leaf_codec(relation.field_index())?,
        LeafCodec::Scalar(_)
    ) {
        return Ok(None);
    }

    // A candidate-logical row may supply a frozen historical scalar without
    // carrying physical bytes for the newly introduced slot. Consume that
    // semantic value through the same accepted row contract instead of
    // misclassifying legitimate historical absence as scalar corruption.
    if row_fields.get_bytes(relation.field_index()).is_none() {
        return match row_fields.required_value_by_contract(relation.field_index())? {
            crate::value::Value::Null => Ok(Some(RelationTargetKeys::none())),
            value => {
                let component =
                    PrimaryKeyComponent::from_runtime_value(&value).ok_or_else(|| {
                        InternalError::relation_source_row_unsupported_scalar_relation_key(
                            source.path(),
                            relation.field_name(),
                            relation.target().path(),
                        )
                    })?;
                let key = PrimaryKeyValue::Scalar(component);

                Ok(Some(RelationTargetKeys::one(&key)))
            }
        };
    }

    match row_fields.required_scalar(relation.field_index())? {
        ScalarSlotValueRef::Null => Ok(Some(RelationTargetKeys::none())),
        ScalarSlotValueRef::Value(value) => {
            let primary_key_value =
                primary_key_value_from_relation_scalar(value).ok_or_else(|| {
                    InternalError::relation_source_row_unsupported_scalar_relation_key(
                        source.path(),
                        relation.field_name(),
                        relation.target().path(),
                    )
                })?;

            let key = PrimaryKeyValue::Scalar(primary_key_value);

            Ok(Some(RelationTargetKeys::one(&key)))
        }
    }
}

fn relation_scalar_slot_fast_path_key_kind_supported(kind: &AcceptedFieldKind) -> bool {
    let AcceptedFieldKind::Relation { key_kind, .. } = kind else {
        return false;
    };

    matches!(
        key_kind.as_ref(),
        AcceptedFieldKind::Int8
            | AcceptedFieldKind::Int16
            | AcceptedFieldKind::Int32
            | AcceptedFieldKind::Int64
            | AcceptedFieldKind::Principal
            | AcceptedFieldKind::Subaccount
            | AcceptedFieldKind::Timestamp
            | AcceptedFieldKind::Nat8
            | AcceptedFieldKind::Nat16
            | AcceptedFieldKind::Nat32
            | AcceptedFieldKind::Nat64
            | AcceptedFieldKind::Ulid
            | AcceptedFieldKind::Unit
    )
}

// Convert one scalar relation payload into the decoded primary-key
// representation used by reverse-index and target-row identities.
const fn primary_key_value_from_relation_scalar(
    value: ScalarValueRef<'_>,
) -> Option<PrimaryKeyComponent> {
    match value {
        ScalarValueRef::Int(value) => Some(PrimaryKeyComponent::Int64(value)),
        ScalarValueRef::Principal(value) => Some(PrimaryKeyComponent::Principal(value)),
        ScalarValueRef::Subaccount(value) => Some(PrimaryKeyComponent::Subaccount(value)),
        ScalarValueRef::Timestamp(value) => Some(PrimaryKeyComponent::Timestamp(value)),
        ScalarValueRef::Nat(value) => Some(PrimaryKeyComponent::Nat64(value)),
        ScalarValueRef::Ulid(value) => Some(PrimaryKeyComponent::Ulid(value)),
        ScalarValueRef::Unit => Some(PrimaryKeyComponent::Unit),
        ScalarValueRef::Blob(_)
        | ScalarValueRef::Bool(_)
        | ScalarValueRef::Date(_)
        | ScalarValueRef::Duration(_)
        | ScalarValueRef::Float32(_)
        | ScalarValueRef::Float64(_)
        | ScalarValueRef::Text(_) => None,
    }
}

// Encode one decoded relation primary-key value directly into the target raw-key
// shape without materializing an intermediate runtime `Value`.
fn raw_relation_target_key_from_primary_key_value(
    source: &ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
    value: &PrimaryKeyValue,
) -> Result<RawDataStoreKey, InternalError> {
    DecodedDataStoreKey::new(relation.target().entity_tag(), value)
        .to_raw()
        .map_err(|err| {
            InternalError::relation_source_row_decode_failed(
                source.path(),
                relation.field_name(),
                relation.target().path(),
                err,
            )
        })
}

// Enforce the narrow relation-field shapes that relation structural
// decode is allowed to accept on this path.
fn validate_relation_field_kind(relation: &AcceptedRelationInfo) -> Result<(), InternalError> {
    match relation.cardinality() {
        AcceptedRelationCardinality::Single
        | AcceptedRelationCardinality::List
        | AcceptedRelationCardinality::Set => {
            validate_scalar_relation_target_primary_key_kind(relation)
        }
    }
}

// Scalar collection relation fields still use this single-component gate;
// tuple relation edges use accepted relation-edge metadata instead.
fn validate_scalar_relation_target_primary_key_kind(
    relation: &AcceptedRelationInfo,
) -> Result<(), InternalError> {
    if relation.local_components().component_count()
        != relation.target().primary_key().component_kinds().len()
    {
        return Err(InternalError::relation_source_row_unsupported_key_kind(
            relation.target().primary_key().component_kinds(),
        ));
    }

    let Some(key_kind) = relation.target().primary_key().single_component_kind() else {
        return Err(InternalError::relation_source_row_unsupported_key_kind(
            relation.target().primary_key().component_kinds(),
        ));
    };

    validate_relation_primary_key_component_kind(key_kind)
}

#[cfg(test)]
mod tests;
