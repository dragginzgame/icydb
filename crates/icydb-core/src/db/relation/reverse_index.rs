//! Module: relation::reverse_index
//! Responsibility: maintain reverse-index relation targets for strong relation consistency.
//! Does not own: planner query semantics or execution routing policies.
//! Boundary: applies relation reverse-index mutations during commit pathways.

use crate::{
    db::{
        Db,
        commit::PreparedIndexMutation,
        data::{
            CanonicalSlotReader, DecodedDataStoreKey, RawDataStoreKey, RawRow, ScalarSlotValueRef,
            ScalarValueRef, StructuralRowContract, StructuralSlotReader,
            decode_accepted_relation_target_primary_key_components_bytes,
        },
        index::{
            IndexEntryValue, IndexId, IndexKey, IndexKeyKind, IndexRowIdentity, IndexStore,
            RawIndexStoreKey, raw_keys_for_component_prefix_with_kind,
        },
        key_taxonomy::{EncodedPrimaryKey, PrimaryKeyComponent, PrimaryKeyValue},
        relation::{
            AcceptedRelationCardinality, AcceptedRelationTargetAuthority,
            RelationTargetDecodeContext, RelationTargetMismatchPolicy,
            accepted_relation_target_descriptor_from_kind,
            validate_relation_primary_key_component_kind,
        },
        schema::{PersistedFieldKind, PersistedRelationStrength},
    },
    error::InternalError,
    model::field::LeafCodec,
    traits::{CanisterKind, EntityKind},
    types::EntityTag,
};
use std::{cell::RefCell, thread::LocalKey};

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
    pub(crate) const fn entity_tag(self) -> EntityTag {
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
/// This lets commit preflight reuse already-decoded structural slot readers
/// while preserving the existing raw-row fallback for other call sites.
///

struct ReverseRelationSourceTransition<'row, 'slots> {
    source_row_contract: StructuralRowContract,
    old_row_fields: Option<&'slots StructuralSlotReader<'row>>,
    new_row_fields: Option<&'slots StructuralSlotReader<'row>>,
}

struct RelationTargetKeys {
    values: Vec<PrimaryKeyValue>,
}

impl RelationTargetKeys {
    const fn none() -> Self {
        Self { values: Vec::new() }
    }

    fn one(value: &PrimaryKeyValue) -> Self {
        Self {
            values: vec![*value],
        }
    }

    const fn from_values(values: Vec<PrimaryKeyValue>) -> Self {
        Self { values }
    }

    fn from_scalar_components(components: Vec<PrimaryKeyComponent>) -> Self {
        Self::from_values(
            components
                .into_iter()
                .map(PrimaryKeyValue::Scalar)
                .collect(),
        )
    }

    fn contains(&self, target_key: &PrimaryKeyValue) -> bool {
        self.values.iter().any(|key| key == target_key)
    }

    fn into_values(self) -> Vec<PrimaryKeyValue> {
        self.values
    }
}

#[derive(Clone, Debug)]
pub(in crate::db::relation) struct AcceptedStrongRelationInfo {
    local_components: AcceptedStrongRelationLocalComponents,
    target: AcceptedStrongRelationTargetIdentity,
    cardinality: AcceptedRelationCardinality,
}

impl AcceptedStrongRelationInfo {
    #[must_use]
    pub(in crate::db::relation) fn field_name(&self) -> &str {
        self.scalar_local_component().field_name()
    }

    #[must_use]
    pub(in crate::db::relation) fn field_index(&self) -> usize {
        self.scalar_local_component().field_index()
    }

    #[must_use]
    fn field_kind(&self) -> &PersistedFieldKind {
        self.scalar_local_component().field_kind()
    }

    #[must_use]
    pub(in crate::db::relation) const fn local_components(
        &self,
    ) -> &AcceptedStrongRelationLocalComponents {
        &self.local_components
    }

    #[must_use]
    pub(in crate::db::relation) const fn target(&self) -> &AcceptedStrongRelationTargetIdentity {
        &self.target
    }

    const fn cardinality(&self) -> AcceptedRelationCardinality {
        self.cardinality
    }

    fn scalar_local_component(&self) -> &AcceptedStrongRelationLocalComponent {
        self.local_components
            .scalar_component()
            .expect("scalar relation descriptor must carry exactly one local component")
    }
}

#[derive(Clone, Debug)]
pub(in crate::db::relation) struct AcceptedStrongRelationLocalComponents {
    components: Vec<AcceptedStrongRelationLocalComponent>,
}

impl AcceptedStrongRelationLocalComponents {
    fn scalar(field_index: usize, field_name: &str, field_kind: &PersistedFieldKind) -> Self {
        Self::try_from_component_specs(&[AcceptedStrongRelationLocalComponentSpec {
            index: field_index,
            name: field_name,
            kind: field_kind,
        }])
        .expect("scalar relation descriptor must carry one local component")
    }

    fn try_from_component_specs(
        components: &[AcceptedStrongRelationLocalComponentSpec<'_>],
    ) -> Result<Self, InternalError> {
        if components.is_empty() {
            return Err(InternalError::relation_source_row_unsupported_key_kind(
                components,
            ));
        }

        Ok(Self {
            components: components
                .iter()
                .map(|component| AcceptedStrongRelationLocalComponent {
                    index: component.index,
                    name: component.name.to_string(),
                    kind: component.kind.clone(),
                })
                .collect(),
        })
    }

    #[must_use]
    const fn component_count(&self) -> usize {
        self.components.len()
    }

    #[cfg(test)]
    #[must_use]
    const fn components(&self) -> &[AcceptedStrongRelationLocalComponent] {
        self.components.as_slice()
    }

    #[must_use]
    fn scalar_component(&self) -> Option<&AcceptedStrongRelationLocalComponent> {
        let [component] = self.components.as_slice() else {
            return None;
        };

        Some(component)
    }
}

#[derive(Clone, Copy, Debug)]
struct AcceptedStrongRelationLocalComponentSpec<'a> {
    index: usize,
    name: &'a str,
    kind: &'a PersistedFieldKind,
}

#[derive(Clone, Debug)]
pub(in crate::db::relation) struct AcceptedStrongRelationLocalComponent {
    index: usize,
    name: String,
    kind: PersistedFieldKind,
}

impl AcceptedStrongRelationLocalComponent {
    #[must_use]
    pub(in crate::db::relation) const fn field_index(&self) -> usize {
        self.index
    }

    #[must_use]
    pub(in crate::db::relation) const fn field_name(&self) -> &str {
        self.name.as_str()
    }

    #[must_use]
    pub(in crate::db::relation) const fn field_kind(&self) -> &PersistedFieldKind {
        &self.kind
    }
}

#[derive(Clone, Debug)]
pub(in crate::db::relation) struct AcceptedStrongRelationTargetIdentity {
    authority: AcceptedRelationTargetAuthority,
    primary_key: AcceptedStrongRelationTargetPrimaryKey,
}

impl AcceptedStrongRelationTargetIdentity {
    fn try_new(
        source_path: &str,
        field_name: &str,
        target_path: &str,
        target_entity_name: &str,
        target_entity_tag: EntityTag,
        target_store_path: &str,
        key_kinds: &[PersistedFieldKind],
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
            primary_key: AcceptedStrongRelationTargetPrimaryKey::try_from_component_kinds(
                key_kinds,
            )?,
        })
    }

    #[must_use]
    pub(in crate::db::relation) const fn path(&self) -> &str {
        self.authority.path()
    }

    #[must_use]
    pub(in crate::db::relation) const fn entity_name(&self) -> crate::db::identity::EntityName {
        self.authority.entity_name()
    }

    #[must_use]
    pub(in crate::db::relation) const fn entity_tag(&self) -> EntityTag {
        self.authority.entity_tag()
    }

    #[must_use]
    const fn store_path(&self) -> &str {
        self.authority.store_path()
    }

    #[must_use]
    const fn primary_key(&self) -> &AcceptedStrongRelationTargetPrimaryKey {
        &self.primary_key
    }

    fn validate_against_db<C>(
        &self,
        db: &Db<C>,
        source_path: &str,
        field_name: &str,
    ) -> Result<(), InternalError>
    where
        C: CanisterKind,
    {
        self.authority
            .validate_against_db(db, source_path, field_name)
            .map(|_| ())
    }
}

#[derive(Clone, Debug)]
pub(in crate::db::relation) struct AcceptedStrongRelationTargetPrimaryKey {
    component_kinds: Vec<PersistedFieldKind>,
}

impl AcceptedStrongRelationTargetPrimaryKey {
    fn try_from_component_kinds(
        component_kinds: &[PersistedFieldKind],
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
    pub(in crate::db::relation) const fn component_kinds(&self) -> &[PersistedFieldKind] {
        self.component_kinds.as_slice()
    }

    #[must_use]
    fn single_component_kind(&self) -> Option<&PersistedFieldKind> {
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

pub(in crate::db::relation) fn accepted_strong_relations_for_row_contract(
    source_path: &str,
    source_row_contract: &StructuralRowContract,
    target_path_filter: Option<&str>,
) -> Result<Vec<AcceptedStrongRelationInfo>, InternalError> {
    let mut relations = Vec::new();
    for slot in 0..source_row_contract.field_count() {
        if !source_row_contract.has_active_field_slot(slot) {
            continue;
        }
        let field = source_row_contract.required_accepted_field_decode_contract(slot)?;
        let Some(relation) = accepted_strong_relation_from_field(
            source_path,
            slot,
            field.field_name(),
            field.kind(),
            target_path_filter,
        )?
        else {
            continue;
        };

        relations.push(relation);
    }

    Ok(relations)
}

fn accepted_strong_relation_from_field(
    source_path: &str,
    field_index: usize,
    field_name: &str,
    kind: &PersistedFieldKind,
    target_path_filter: Option<&str>,
) -> Result<Option<AcceptedStrongRelationInfo>, InternalError> {
    let Some(target) = accepted_relation_target_descriptor_from_kind(kind) else {
        return Ok(None);
    };
    if target.strength != PersistedRelationStrength::Strong {
        return Ok(None);
    }
    if target_path_filter.is_some_and(|filter| filter != target.target_path) {
        return Ok(None);
    }

    Ok(Some(AcceptedStrongRelationInfo {
        local_components: AcceptedStrongRelationLocalComponents::scalar(
            field_index,
            field_name,
            kind,
        ),
        target: AcceptedStrongRelationTargetIdentity::try_new(
            source_path,
            field_name,
            target.target_path,
            target.target_entity_name,
            target.target_entity_tag,
            target.target_store_path,
            std::slice::from_ref(target.scalar_target_key_kind),
        )?,
        cardinality: target.cardinality,
    }))
}

/// Build the canonical reverse-index id for a `(source entity, relation field)` pair.
fn reverse_index_id_for_relation(
    source: ReverseRelationSourceInfo,
    relation: &AcceptedStrongRelationInfo,
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
    relation: &AcceptedStrongRelationInfo,
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
    );

    Ok(Some((start, end)))
}

/// Build the concrete reverse-index key for one target/source relation edge.
fn reverse_index_key_for_target_and_source_primary_key_value(
    source: ReverseRelationSourceInfo,
    relation: &AcceptedStrongRelationInfo,
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
    );

    Ok(Some(key.to_raw()))
}

// Encode full relation target row identity as the reverse-index target
// component. This keeps scalar and composite targets on one key-owned path and
// prevents first-component projection from entering reverse-index storage.
fn encode_reverse_relation_target_identity_component(
    source: ReverseRelationSourceInfo,
    relation: &AcceptedStrongRelationInfo,
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
    relation: &AcceptedStrongRelationInfo,
) -> Result<Vec<RawDataStoreKey>, InternalError> {
    let keys = relation_target_keys_for_source_slots(row_fields, source_info, relation)?;

    relation_target_raw_keys_from_relation_target_keys(source_info, relation, keys)
}

/// Check whether one persisted source row still references one complete target
/// primary key for the declared strong relation.
pub(in crate::db::relation) fn source_row_references_relation_target_primary_key_value(
    raw_row: &RawRow,
    source_row_contract: StructuralRowContract,
    source_info: ReverseRelationSourceInfo,
    relation: &AcceptedStrongRelationInfo,
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
    relation: &AcceptedStrongRelationInfo,
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
    relation: &AcceptedStrongRelationInfo,
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

/// Resolve target store handle for one relation descriptor.
pub(super) fn relation_target_store<C>(
    db: &Db<C>,
    source: ReverseRelationSourceInfo,
    relation: &AcceptedStrongRelationInfo,
) -> Result<&'static LocalKey<RefCell<IndexStore>>, InternalError>
where
    C: CanisterKind,
{
    relation
        .target()
        .validate_against_db(db, source.path, relation.field_name())?;
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
    relation: &AcceptedStrongRelationInfo,
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
    relation: &AcceptedStrongRelationInfo,
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
    relation: &AcceptedStrongRelationInfo,
) -> Result<RelationTargetKeys, InternalError> {
    // Phase 1: keep single relation slots on the scalar fast path when the
    // persisted field already uses a primary-key-compatible leaf codec.
    if let Some(keys) = relation_target_keys_from_scalar_slot(row_fields, source, relation)? {
        return Ok(keys);
    }

    // Phase 2: decode the declared relation field payload directly into target
    // keys without rebuilding a runtime `Value` container.
    relation_target_keys_from_field_bytes(row_fields, source, relation)
}

// Decode the one strong-relation field payload needed by structural delete
// validation directly into relation target keys from the encoded field bytes.
fn relation_target_keys_from_field_bytes(
    row_fields: &StructuralSlotReader<'_>,
    source: ReverseRelationSourceInfo,
    relation: &AcceptedStrongRelationInfo,
) -> Result<RelationTargetKeys, InternalError> {
    validate_relation_field_kind(relation)?;

    let bytes = row_fields.required_field_bytes(relation.field_index(), relation.field_name())?;
    let keys =
        decode_accepted_relation_target_primary_key_components_bytes(bytes, relation.field_kind())
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

// Decode one singular strong relation directly from the scalar slot codec when
// the relation key kind is already primary-key-compatible on the persisted row.
fn relation_target_keys_from_scalar_slot(
    row_fields: &StructuralSlotReader<'_>,
    source: ReverseRelationSourceInfo,
    relation: &AcceptedStrongRelationInfo,
) -> Result<Option<RelationTargetKeys>, InternalError> {
    let PersistedFieldKind::Relation { .. } = relation.field_kind() else {
        return Ok(None);
    };
    if !relation_scalar_slot_fast_path_key_kind_supported(relation.field_kind()) {
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

fn relation_scalar_slot_fast_path_key_kind_supported(kind: &PersistedFieldKind) -> bool {
    let PersistedFieldKind::Relation { key_kind, .. } = kind else {
        return false;
    };

    matches!(
        key_kind.as_ref(),
        PersistedFieldKind::Int8
            | PersistedFieldKind::Int16
            | PersistedFieldKind::Int32
            | PersistedFieldKind::Int64
            | PersistedFieldKind::Principal
            | PersistedFieldKind::Subaccount
            | PersistedFieldKind::Timestamp
            | PersistedFieldKind::Nat8
            | PersistedFieldKind::Nat16
            | PersistedFieldKind::Nat32
            | PersistedFieldKind::Nat64
            | PersistedFieldKind::Ulid
            | PersistedFieldKind::Unit
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
    relation: &AcceptedStrongRelationInfo,
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

// Enforce the narrow relation-field shapes that strong-relation structural
// decode is allowed to accept on this path.
fn validate_relation_field_kind(
    relation: &AcceptedStrongRelationInfo,
) -> Result<(), InternalError> {
    match relation.cardinality() {
        AcceptedRelationCardinality::Single
        | AcceptedRelationCardinality::List
        | AcceptedRelationCardinality::Set => {
            validate_scalar_relation_target_primary_key_kind(relation)
        }
    }
}

// Enforce the current scalar relation-field target shape supported by the
// raw relation target-key builder. Composite relation targets will need a
// relation-edge descriptor instead of this single-component gate.
fn validate_scalar_relation_target_primary_key_kind(
    relation: &AcceptedStrongRelationInfo,
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

    let relations = accepted_strong_relations_for_row_contract(
        source.path,
        &source_rows.source_row_contract,
        None,
    )?;
    if relations.is_empty() {
        return Ok(ops);
    }

    // Phase 2: evaluate each strong relation independently and derive index deltas
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
    relation: &AcceptedStrongRelationInfo,
) -> Result<Vec<RawDataStoreKey>, InternalError> {
    match row_fields {
        Some(row_fields) => relation_target_raw_keys_for_source_slots(row_fields, source, relation),
        None => Ok(Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AcceptedStrongRelationInfo, AcceptedStrongRelationLocalComponentSpec,
        AcceptedStrongRelationLocalComponents, AcceptedStrongRelationTargetIdentity,
        RelationTargetKeys, ReverseRelationSourceInfo,
        relation_scalar_slot_fast_path_key_kind_supported,
        reverse_index_key_bounds_for_target_primary_key_value,
        reverse_index_key_for_target_and_source_primary_key_value,
        validate_scalar_relation_target_primary_key_kind,
    };
    use crate::db::relation::AcceptedRelationCardinality;
    use crate::db::{
        index::IndexId,
        key_taxonomy::{
            CompositePrimaryKeyValue, EncodedIndexComponent, EncodedPrimaryKey, IndexStoreKeyKind,
            PrimaryKeyComponent, PrimaryKeyValue,
        },
        schema::{PersistedFieldKind, PersistedRelationStrength},
    };
    use crate::types::EntityTag;

    fn relation(field_index: usize, key_kind: PersistedFieldKind) -> AcceptedStrongRelationInfo {
        let field_kind = PersistedFieldKind::Relation {
            target_path: "Target".to_string(),
            target_entity_name: "Target".to_string(),
            target_entity_tag: EntityTag::new(77),
            target_store_path: "TargetStore".to_string(),
            key_kind: Box::new(key_kind.clone()),
            strength: PersistedRelationStrength::Strong,
        };

        AcceptedStrongRelationInfo {
            local_components: AcceptedStrongRelationLocalComponents::scalar(
                field_index,
                "target_id",
                &field_kind,
            ),
            target: AcceptedStrongRelationTargetIdentity::try_new(
                "Source",
                "target_id",
                "Target",
                "Target",
                EntityTag::new(77),
                "TargetStore",
                std::slice::from_ref(&key_kind),
            )
            .expect("target identity should build"),
            cardinality: AcceptedRelationCardinality::Single,
        }
    }

    #[test]
    fn accepted_relation_target_identity_carries_ordered_primary_key_metadata() {
        let relation = relation(3, PersistedFieldKind::Nat64);

        assert_eq!(
            relation.target().primary_key().component_kinds(),
            &[PersistedFieldKind::Nat64],
            "current scalar relation metadata is represented as a one-component target primary key",
        );
    }

    #[test]
    fn accepted_relation_target_identity_can_carry_ordered_composite_metadata() {
        let target = AcceptedStrongRelationTargetIdentity::try_new(
            "Source",
            "target_id",
            "Target",
            "Target",
            EntityTag::new(77),
            "TargetStore",
            &[PersistedFieldKind::Nat64, PersistedFieldKind::Ulid],
        )
        .expect("target identity should build");

        assert_eq!(
            target.primary_key().component_kinds(),
            &[PersistedFieldKind::Nat64, PersistedFieldKind::Ulid],
        );
    }

    #[test]
    fn accepted_relation_target_identity_rejects_empty_primary_key_metadata() {
        AcceptedStrongRelationTargetIdentity::try_new(
            "Source",
            "target_id",
            "Target",
            "Target",
            EntityTag::new(77),
            "TargetStore",
            &[],
        )
        .expect_err("relation target identity must fail closed without PK metadata");
    }

    #[test]
    fn relation_target_keys_make_none_one_and_many_explicit() {
        assert!(
            !RelationTargetKeys::none()
                .contains(&PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(1),))
        );

        let key = PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(7));
        let one = RelationTargetKeys::one(&key);
        assert!(one.contains(&PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(7))));
        assert_eq!(one.into_values().len(), 1);

        let many = RelationTargetKeys::from_scalar_components(vec![
            PrimaryKeyComponent::Nat64(7),
            PrimaryKeyComponent::Nat64(8),
        ]);
        assert!(many.contains(&PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(8))));
        assert_eq!(many.into_values().len(), 2);
    }

    #[test]
    fn accepted_relation_info_carries_ordered_local_component_metadata() {
        let relation = relation(3, PersistedFieldKind::Nat64);
        let [component] = relation.local_components().components() else {
            panic!("scalar relation descriptor should expose one local component");
        };

        assert_eq!(component.field_index(), 3);
        assert_eq!(component.field_name(), "target_id");
        assert!(matches!(
            component.field_kind(),
            PersistedFieldKind::Relation { .. }
        ));
    }

    #[test]
    fn accepted_relation_local_components_can_carry_ordered_tuple_metadata() {
        let tenant_kind = PersistedFieldKind::Nat64;
        let local_kind = PersistedFieldKind::Ulid;

        let components = AcceptedStrongRelationLocalComponents::try_from_component_specs(&[
            AcceptedStrongRelationLocalComponentSpec {
                index: 2,
                name: "tenant_id",
                kind: &tenant_kind,
            },
            AcceptedStrongRelationLocalComponentSpec {
                index: 4,
                name: "local_id",
                kind: &local_kind,
            },
        ])
        .expect("ordered local component tuple should build");

        let [tenant, local] = components.components() else {
            panic!("tuple relation descriptor should expose both local components");
        };
        assert_eq!(tenant.field_index(), 2);
        assert_eq!(tenant.field_name(), "tenant_id");
        assert_eq!(tenant.field_kind(), &PersistedFieldKind::Nat64);
        assert_eq!(local.field_index(), 4);
        assert_eq!(local.field_name(), "local_id");
        assert_eq!(local.field_kind(), &PersistedFieldKind::Ulid);
    }

    #[test]
    fn accepted_relation_local_components_reject_empty_metadata() {
        AcceptedStrongRelationLocalComponents::try_from_component_specs(&[])
            .expect_err("relation local component metadata must fail closed when empty");
    }

    #[test]
    fn relation_validation_rejects_local_target_component_arity_mismatch() {
        let field_kind = PersistedFieldKind::Relation {
            target_path: "Target".to_string(),
            target_entity_name: "Target".to_string(),
            target_entity_tag: EntityTag::new(77),
            target_store_path: "TargetStore".to_string(),
            key_kind: Box::new(PersistedFieldKind::Nat64),
            strength: PersistedRelationStrength::Strong,
        };
        let relation = AcceptedStrongRelationInfo {
            local_components: AcceptedStrongRelationLocalComponents::scalar(
                3,
                "target_id",
                &field_kind,
            ),
            target: AcceptedStrongRelationTargetIdentity::try_new(
                "Source",
                "target_id",
                "Target",
                "Target",
                EntityTag::new(77),
                "TargetStore",
                &[PersistedFieldKind::Nat64, PersistedFieldKind::Ulid],
            )
            .expect("target identity should build"),
            cardinality: AcceptedRelationCardinality::Single,
        };

        validate_scalar_relation_target_primary_key_kind(&relation)
            .expect_err("single local field must not validate against composite target metadata");
    }

    #[test]
    fn scalar_relation_target_key_kind_validation_accepts_128_bit_lanes() {
        for key_kind in [PersistedFieldKind::Int128, PersistedFieldKind::Nat128] {
            let relation = relation(3, key_kind);

            validate_scalar_relation_target_primary_key_kind(&relation)
                .expect("128-bit scalar relation target key kinds should validate");
        }
    }

    #[test]
    fn relation_scalar_slot_fast_path_excludes_structural_128_bit_lanes() {
        for key_kind in [
            PersistedFieldKind::Int64,
            PersistedFieldKind::Nat64,
            PersistedFieldKind::Ulid,
        ] {
            let relation = relation(3, key_kind);
            assert!(
                relation_scalar_slot_fast_path_key_kind_supported(relation.field_kind()),
                "scalar-slot relation key kinds should stay on the fast path",
            );
        }

        for key_kind in [PersistedFieldKind::Int128, PersistedFieldKind::Nat128] {
            let relation = relation(3, key_kind);
            assert!(
                !relation_scalar_slot_fast_path_key_kind_supported(relation.field_kind()),
                "128-bit relation key kinds use structural field-bytes decoding",
            );
        }
    }

    #[test]
    fn reverse_relation_keys_accept_128_bit_target_primary_key_components() {
        let source = ReverseRelationSourceInfo {
            path: "Source",
            entity_tag: EntityTag::new(9),
        };
        let source_primary_key = PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(44));

        for (ordinal, key_kind, target_component) in [
            (
                3,
                PersistedFieldKind::Int128,
                PrimaryKeyComponent::Int128(i128::MIN + 91),
            ),
            (
                4,
                PersistedFieldKind::Nat128,
                PrimaryKeyComponent::Nat128(u128::MAX - 91),
            ),
        ] {
            let relation = relation(ordinal, key_kind);
            let target_key = PrimaryKeyValue::Scalar(target_component);
            let raw = reverse_index_key_for_target_and_source_primary_key_value(
                source,
                &relation,
                &target_key,
                &source_primary_key,
            )
            .expect("reverse key should build")
            .expect("128-bit target component should be index encodable");
            let decoded = raw.decode().expect("reverse key should decode");
            let expected_component = EncodedIndexComponent::from_canonical_bytes(
                EncodedPrimaryKey::encode(target_key)
                    .expect("target primary key should encode")
                    .as_bytes()
                    .to_vec(),
            );

            assert_eq!(
                decoded.key_kind(),
                IndexStoreKeyKind::System,
                "reverse indexes use system key kind",
            );
            assert_eq!(
                decoded.index_id(),
                IndexId::new(
                    EntityTag::new(9),
                    u16::try_from(ordinal).expect("test ordinal fits u16"),
                )
            );
            assert_eq!(decoded.components(), &[expected_component]);
            assert_eq!(
                decoded.primary_key().decode().expect("source key decodes"),
                source_primary_key,
            );

            let bounds = reverse_index_key_bounds_for_target_primary_key_value(
                source,
                &relation,
                &target_key,
            )
            .expect("reverse bounds should build");
            assert!(
                bounds.is_some(),
                "128-bit target component should produce reverse index bounds",
            );
        }
    }

    #[test]
    fn reverse_relation_keys_encode_full_composite_target_primary_key_identity() {
        let source = ReverseRelationSourceInfo {
            path: "Source",
            entity_tag: EntityTag::new(9),
        };
        let relation = relation(5, PersistedFieldKind::Nat64);
        let source_primary_key = PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(44));
        let target_key = PrimaryKeyValue::Composite(
            CompositePrimaryKeyValue::try_from_components(&[
                PrimaryKeyComponent::Nat64(7),
                PrimaryKeyComponent::Ulid(crate::types::Ulid::from_bytes([9; 16])),
            ])
            .expect("composite target key should build"),
        );

        let raw = reverse_index_key_for_target_and_source_primary_key_value(
            source,
            &relation,
            &target_key,
            &source_primary_key,
        )
        .expect("reverse key should build")
        .expect("composite target identity should be index encodable");
        let decoded = raw.decode().expect("reverse key should decode");
        let expected_component = EncodedIndexComponent::from_canonical_bytes(
            EncodedPrimaryKey::encode(target_key)
                .expect("target primary key should encode")
                .as_bytes()
                .to_vec(),
        );

        assert_eq!(decoded.components(), &[expected_component]);
        assert_eq!(
            decoded.primary_key().decode().expect("source key decodes"),
            source_primary_key,
        );

        let bounds =
            reverse_index_key_bounds_for_target_primary_key_value(source, &relation, &target_key)
                .expect("reverse bounds should build")
                .expect("composite target identity should produce reverse index bounds");

        assert!(
            raw.as_bytes() >= bounds.0.as_bytes() && raw.as_bytes() < bounds.1.as_bytes(),
            "reverse bounds should cover the full composite target identity"
        );
    }
}
