//! Module: relation::reverse_index
//! Responsibility: maintain reverse-index relation targets for relation consistency.
//! Does not own: planner query semantics or execution routing policies.
//! Boundary: applies relation reverse-index mutations during commit pathways.

mod target_keys;

use crate::{
    db::{
        Db,
        commit::PreparedIndexMutation,
        data::{
            CanonicalSlotReader, DecodedDataStoreKey, RawDataStoreKey, RawRow, ScalarSlotValueRef,
            ScalarValueRef, StructuralRowContract, StructuralSlotReader,
            decode_accepted_relation_target_primary_key_components_bytes,
            decode_runtime_value_from_accepted_field_contract,
        },
        index::{
            IndexEntryValue, IndexId, IndexKey, IndexKeyKind, IndexRowIdentity, IndexStore,
            RawIndexStoreKey, raw_keys_for_component_prefix_with_kind,
        },
        key_taxonomy::{EncodedPrimaryKey, PrimaryKeyComponent, PrimaryKeyValue},
        relation::{
            AcceptedRelationCardinality, AcceptedRelationTargetAuthority,
            AcceptedRelationTargetContract, AcceptedRelationTupleEdgeLocalComponent,
            RelationTargetDecodeContext, RelationTargetMismatchPolicy,
            accepted_relation_target_metadata_from_kind, accepted_relation_tuple_edge_descriptor,
            accepted_scalar_relation_target_descriptor,
            validate_relation_primary_key_component_kind,
        },
        schema::AcceptedFieldKind,
        schema::{AcceptedFieldDecodeContract, OwnedAcceptedRelationEdgeContract},
    },
    entity::EntityKind,
    error::InternalError,
    model::field::{FieldStorageDecode, LeafCodec},
    traits::CanisterKind,
    types::EntityTag,
};
use std::{cell::RefCell, thread::LocalKey};

use target_keys::RelationTargetKeys;

///
/// ReverseRelationSourceInfo
///
/// Resolved authority used while preparing reverse-index mutations.
/// Carries only the source entity path and tag required for diagnostics and
/// reverse-index identity, so the heavy mutation loop does not need `S`.
///

#[derive(Clone, Copy)]
pub(crate) struct ReverseRelationSourceInfo {
    path: &'static str,
    entity_tag: EntityTag,
}

impl ReverseRelationSourceInfo {
    /// Build structural source authority from an accepted runtime entity identity.
    pub(in crate::db) const fn new(path: &'static str, entity_tag: EntityTag) -> Self {
        Self { path, entity_tag }
    }

    /// Lower one typed source entity into the resolved authority used by reverse-index prep.
    pub(crate) const fn for_type<S>() -> Self
    where
        S: EntityKind,
    {
        Self {
            path: S::PATH,
            entity_tag: S::ENTITY_TAG,
        }
    }

    /// Return the structural source entity tag used for reverse-index identity.
    #[must_use]
    pub(in crate::db::relation) const fn entity_tag(self) -> EntityTag {
        self.entity_tag
    }
}

///
/// ReverseRelationMutationTarget
///
/// Shared reverse-index mutation context for one touched target key.
/// This keeps the structural mutation helper narrow without dragging the
/// whole typed source shell through the per-target update path.
///

#[derive(Clone)]
struct ReverseRelationMutationTarget {
    target_store: &'static LocalKey<RefCell<IndexStore>>,
    reverse_key: RawIndexStoreKey,
    old_contains: bool,
    new_contains: bool,
}

///
/// ReverseRelationSourceTransition
///
/// Shared old/new source-row views used during reverse-index preparation.
/// Commit preflight supplies already-decoded structural slot readers; an
/// absent old or new side represents an insert or delete respectively.
///

struct ReverseRelationSourceTransition<'row, 'slots> {
    source_row_contract: StructuralRowContract,
    old_row_fields: Option<&'slots StructuralSlotReader<'row>>,
    new_row_fields: Option<&'slots StructuralSlotReader<'row>>,
}

#[derive(Clone, Debug)]
pub(in crate::db::relation) struct AcceptedRelationInfo {
    relation_name: String,
    relation_ordinal: usize,
    local_components: AcceptedRelationLocalComponents,
    target: AcceptedRelationTargetIdentity,
    cardinality: AcceptedRelationCardinality,
}

impl AcceptedRelationInfo {
    fn new(
        relation_name: impl Into<String>,
        relation_ordinal: usize,
        local_components: AcceptedRelationLocalComponents,
        target_contract: AcceptedRelationTargetContract,
        cardinality: AcceptedRelationCardinality,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            relation_name: relation_name.into(),
            relation_ordinal,
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
}

#[derive(Clone, Debug)]
struct AcceptedRelationLocalComponents {
    components: Vec<AcceptedRelationLocalComponent>,
}

impl AcceptedRelationLocalComponents {
    fn scalar(field_index: usize, field: AcceptedFieldDecodeContract<'_>) -> Self {
        Self::try_from_component_specs(&[AcceptedRelationLocalComponentSpec {
            index: field_index,
            field,
        }])
        .expect("relation invariant")
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
    let mut relations =
        accepted_relations_from_edges(db, source_path, source_row_contract, target_path_filter)?;
    for slot in 0..source_row_contract.field_count() {
        if !source_row_contract.has_active_field_slot(slot) {
            continue;
        }
        if source_row_contract
            .accepted_relation_edges()
            .iter()
            .any(|edge| edge.local_field_slots().contains(&slot))
        {
            continue;
        }
        let field = source_row_contract.required_accepted_field_decode_contract(slot)?;
        let Some(relation) =
            accepted_relation_from_field(db, source_path, slot, field, target_path_filter)?
        else {
            continue;
        };

        relations.push(relation);
    }

    Ok(relations)
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
            field.field_name(),
            edge.local_field_slots()[0],
            AcceptedRelationLocalComponents::scalar(edge.local_field_slots()[0], *field),
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
        edge.name(),
        edge.local_field_slots()[0],
        AcceptedRelationLocalComponents::try_from_component_specs(component_specs.as_slice())?,
        tuple_descriptor.into_target_contract(),
        AcceptedRelationCardinality::Single,
    )?))
}

fn accepted_relation_from_field<C>(
    db: &Db<C>,
    source_path: &str,
    field_index: usize,
    field: AcceptedFieldDecodeContract<'_>,
    target_path_filter: Option<&str>,
) -> Result<Option<AcceptedRelationInfo>, InternalError>
where
    C: CanisterKind,
{
    let Some(target) = accepted_relation_target_metadata_from_kind(field.kind()) else {
        return Ok(None);
    };
    if target_path_filter.is_some_and(|filter| filter != target.target_path) {
        return Ok(None);
    }

    let Some(descriptor) = accepted_scalar_relation_target_descriptor(
        db,
        source_path,
        field.field_name(),
        field.field_name(),
        field.kind(),
        None,
    )?
    else {
        return Ok(None);
    };
    let cardinality = descriptor.cardinality();

    Ok(Some(AcceptedRelationInfo::new(
        field.field_name(),
        field_index,
        AcceptedRelationLocalComponents::scalar(field_index, field),
        descriptor.into_target_contract(),
        cardinality,
    )?))
}

/// Build the canonical reverse-index id for a `(source entity, relation field)` pair.
fn reverse_index_id_for_relation(
    source: ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
) -> Result<IndexId, InternalError> {
    let ordinal = u16::try_from(relation.field_index()).map_err(|err| {
        InternalError::reverse_index_ordinal_overflow(
            source.path,
            relation.field_name(),
            relation.target().path(),
            err,
        )
    })?;

    Ok(IndexId::new(source.entity_tag, ordinal))
}

/// Build reverse-index prefix bounds for one complete target primary key.
pub(super) fn reverse_index_key_bounds_for_target_primary_key_value(
    source: ReverseRelationSourceInfo,
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
    source: ReverseRelationSourceInfo,
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
    source: ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
    target_key_value: &PrimaryKeyValue,
) -> Result<Vec<u8>, InternalError> {
    EncodedPrimaryKey::encode(*target_key_value)
        .map(|encoded| encoded.as_bytes().to_vec())
        .map_err(|err| {
            InternalError::relation_source_row_decode_failed(
                source.path,
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
    source_info: ReverseRelationSourceInfo,
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

    source_slots_reference_relation_target(&row_fields, source_info, relation, target_key)
}

// Check one already-decoded structural source row for membership of one target
// key without rebuilding the full canonical target-key vector.
fn source_slots_reference_relation_target(
    row_fields: &StructuralSlotReader<'_>,
    source_info: ReverseRelationSourceInfo,
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
    source: ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
    index_key: &RawIndexStoreKey,
    raw_entry: &IndexEntryValue,
) -> Result<IndexRowIdentity, InternalError> {
    raw_entry.decode_row_identity(index_key).map_err(|err| {
        InternalError::reverse_index_entry_corrupted(
            source.path,
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
    source: ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
) -> Result<&'static LocalKey<RefCell<IndexStore>>, InternalError>
where
    C: CanisterKind,
{
    // Accepted relation discovery validates runtime target identity before any
    // relation info can reach this store-resolution boundary.
    let target = relation.target();

    db.with_store_registry(|reg| reg.try_get_store(target.store_path()))
        .map(|store| store.index_store())
        .map_err(|err| {
            InternalError::relation_target_store_missing(
                source.path,
                relation.field_name(),
                target.path(),
                target.store_path(),
                err,
            )
        })
}

/// Decode one raw relation target key and enforce reverse-index target invariants.
pub(in crate::db::relation) fn decode_relation_target_data_key(
    source: ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
    target_raw_key: &RawDataStoreKey,
    context: RelationTargetDecodeContext,
    mismatch_policy: RelationTargetMismatchPolicy,
) -> Result<Option<DecodedDataStoreKey>, InternalError> {
    let target_data_key = DecodedDataStoreKey::try_from_raw(target_raw_key).map_err(|err| {
        InternalError::relation_target_key_decode_failed(
            relation_target_key_decode_context_label(context),
            source.path,
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
            source.path,
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
    source: ReverseRelationSourceInfo,
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
    source: ReverseRelationSourceInfo,
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
    source: ReverseRelationSourceInfo,
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
                source.path,
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
                source.path,
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
            source.path,
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
    source: ReverseRelationSourceInfo,
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
                    source.path,
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
    source: ReverseRelationSourceInfo,
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

    match row_fields.required_scalar(relation.field_index())? {
        ScalarSlotValueRef::Null => Ok(Some(RelationTargetKeys::none())),
        ScalarSlotValueRef::Value(value) => {
            let primary_key_value =
                primary_key_value_from_relation_scalar(value).ok_or_else(|| {
                    InternalError::relation_source_row_unsupported_scalar_relation_key(
                        source.path,
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
    source: ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
    value: &PrimaryKeyValue,
) -> Result<RawDataStoreKey, InternalError> {
    DecodedDataStoreKey::new(relation.target().entity_tag(), value)
        .to_raw()
        .map_err(|err| {
            InternalError::relation_source_row_decode_failed(
                source.path,
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

/// Build one reverse-index mutation for one touched target key.
fn prepare_reverse_relation_index_mutation_for_target(
    target: ReverseRelationMutationTarget,
) -> Option<PreparedIndexMutation> {
    if target.old_contains == target.new_contains {
        return None;
    }

    // Each reverse-index raw key now includes both target and source keys, so
    // the value is just the one-byte existence witness for that edge.
    let next_value = target.new_contains.then(IndexEntryValue::presence);

    Some(PreparedIndexMutation::from_reverse_index_membership(
        target.target_store,
        target.reverse_key,
        next_value,
        target.old_contains,
        target.new_contains,
    ))
}

/// Prepare reverse-index mutations for one source entity transition using
/// already-decoded structural slot readers from commit preflight.
pub(crate) fn prepare_reverse_relation_index_mutations_for_source_slot_readers<C>(
    db: &Db<C>,
    source: ReverseRelationSourceInfo,
    source_row_contract: StructuralRowContract,
    source_primary_key: &PrimaryKeyValue,
    old_row_fields: Option<&StructuralSlotReader<'_>>,
    new_row_fields: Option<&StructuralSlotReader<'_>>,
) -> Result<Vec<PreparedIndexMutation>, InternalError>
where
    C: CanisterKind,
{
    let source_rows = ReverseRelationSourceTransition {
        source_row_contract,
        old_row_fields,
        new_row_fields,
    };

    prepare_reverse_relation_index_mutations_for_source_rows_impl(
        db,
        source,
        source_primary_key,
        source_rows,
    )
}

// Keep the reverse-index mutation loop structural once the source entity has
// already been lowered onto accepted row contracts and source identity.
fn prepare_reverse_relation_index_mutations_for_source_rows_impl<C>(
    db: &Db<C>,
    source: ReverseRelationSourceInfo,
    source_primary_key: &PrimaryKeyValue,
    source_rows: ReverseRelationSourceTransition<'_, '_>,
) -> Result<Vec<PreparedIndexMutation>, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: reuse the already-validated commit marker key instead of
    // recomputing it through typed entity ids.
    let mut ops = Vec::new();

    let relations = accepted_relations_for_row_contract(
        db,
        source.path,
        &source_rows.source_row_contract,
        None,
    )?;
    if relations.is_empty() {
        return Ok(ops);
    }

    // Phase 2: evaluate each relation independently and derive index deltas
    // directly from persisted row payloads.
    for relation in relations {
        let old_targets = relation_target_keys_for_transition_side(
            source_rows.old_row_fields,
            source,
            &relation,
        )?;
        let new_targets = relation_target_keys_for_transition_side(
            source_rows.new_row_fields,
            source,
            &relation,
        )?;
        let target_store = relation_target_store(db, source, &relation)?;
        let mut old_index = 0usize;
        let mut new_index = 0usize;

        // Phase 3: walk the canonical union of old/new targets directly
        // instead of cloning, re-sorting, and then binary-searching both
        // source vectors again for each touched target.
        while old_index < old_targets.len() || new_index < new_targets.len() {
            let (target_raw_key, old_contains, new_contains) =
                match (old_targets.get(old_index), new_targets.get(new_index)) {
                    (Some(old_key), Some(new_key)) => match old_key.cmp(new_key) {
                        std::cmp::Ordering::Less => {
                            old_index += 1;
                            (old_key.clone(), true, false)
                        }
                        std::cmp::Ordering::Greater => {
                            new_index += 1;
                            (new_key.clone(), false, true)
                        }
                        std::cmp::Ordering::Equal => {
                            old_index += 1;
                            new_index += 1;
                            (old_key.clone(), true, true)
                        }
                    },
                    (Some(old_key), None) => {
                        old_index += 1;
                        (old_key.clone(), true, false)
                    }
                    (None, Some(new_key)) => {
                        new_index += 1;
                        (new_key.clone(), false, true)
                    }
                    (None, None) => break,
                };

            let Some(target_data_key) = decode_relation_target_data_key(
                source,
                &relation,
                &target_raw_key,
                RelationTargetDecodeContext::ReverseIndexPrepare,
                RelationTargetMismatchPolicy::Reject,
            )?
            else {
                return Err(
                    InternalError::reverse_index_relation_target_decode_invariant_violated(
                        source.path,
                        relation.field_name(),
                        relation.target().path(),
                    ),
                );
            };

            let Some(reverse_key) = reverse_index_key_for_target_and_source_primary_key_value(
                source,
                &relation,
                &target_data_key.primary_key_value(),
                source_primary_key,
            )?
            else {
                continue;
            };

            let target = ReverseRelationMutationTarget {
                target_store,
                reverse_key,
                old_contains,
                new_contains,
            };
            let Some(op) = prepare_reverse_relation_index_mutation_for_target(target) else {
                continue;
            };

            ops.push(op);
        }
    }

    Ok(ops)
}

// Resolve relation targets for one old/new source-row side from the decoded
// slot-reader view prepared by commit preflight.
fn relation_target_keys_for_transition_side(
    row_fields: Option<&StructuralSlotReader<'_>>,
    source: ReverseRelationSourceInfo,
    relation: &AcceptedRelationInfo,
) -> Result<Vec<RawDataStoreKey>, InternalError> {
    match row_fields {
        Some(row_fields) => relation_target_raw_keys_for_source_slots(row_fields, source, relation),
        None => Ok(Vec::new()),
    }
}

#[cfg(test)]
mod tests;
