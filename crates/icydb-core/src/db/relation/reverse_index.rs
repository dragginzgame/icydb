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
            ScalarValueRef, StorageKey, StructuralRowContract, StructuralSlotReader,
            decode_accepted_relation_target_storage_keys_bytes,
        },
        identity::EntityName,
        index::{
            IndexEntryValue, IndexId, IndexKey, IndexKeyKind, IndexRowIdentity, IndexStore,
            RawIndexStoreKey, encode_canonical_index_component_from_primary_key_value,
            raw_keys_for_component_prefix_with_kind,
        },
        key_taxonomy::PrimaryKeyValue,
        relation::{RelationTargetDecodeContext, RelationTargetMismatchPolicy},
        schema::{PersistedFieldKind, PersistedRelationStrength},
    },
    error::InternalError,
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

#[derive(Clone, Debug)]
pub(in crate::db::relation) struct AcceptedStrongRelationInfo {
    field_index: usize,
    field_name: String,
    field_kind: PersistedFieldKind,
    target: AcceptedStrongRelationTargetIdentity,
}

impl AcceptedStrongRelationInfo {
    #[must_use]
    pub(in crate::db::relation) const fn field_name(&self) -> &str {
        self.field_name.as_str()
    }

    #[must_use]
    pub(in crate::db::relation) const fn field_index(&self) -> usize {
        self.field_index
    }

    #[must_use]
    const fn field_kind(&self) -> &PersistedFieldKind {
        &self.field_kind
    }

    #[must_use]
    pub(in crate::db::relation) const fn target(&self) -> &AcceptedStrongRelationTargetIdentity {
        &self.target
    }
}

#[derive(Clone, Debug)]
pub(in crate::db::relation) struct AcceptedStrongRelationTargetIdentity {
    path: String,
    entity_name: EntityName,
    entity_tag: EntityTag,
    store_path: String,
    key_kind: PersistedFieldKind,
}

impl AcceptedStrongRelationTargetIdentity {
    fn try_new(
        source_path: &str,
        field_name: &str,
        target_path: &str,
        target_entity_name: &str,
        target_entity_tag: EntityTag,
        target_store_path: &str,
        key_kind: &PersistedFieldKind,
    ) -> Result<Self, InternalError> {
        let entity_name = EntityName::try_from_str(target_entity_name).map_err(|err| {
            InternalError::strong_relation_target_name_invalid(
                source_path,
                field_name,
                target_path,
                target_entity_name,
                err,
            )
        })?;

        Ok(Self {
            path: target_path.to_string(),
            entity_name,
            entity_tag: target_entity_tag,
            store_path: target_store_path.to_string(),
            key_kind: key_kind.clone(),
        })
    }

    #[must_use]
    pub(in crate::db::relation) const fn path(&self) -> &str {
        self.path.as_str()
    }

    #[must_use]
    pub(in crate::db::relation) const fn entity_name(&self) -> EntityName {
        self.entity_name
    }

    #[must_use]
    pub(in crate::db::relation) const fn entity_tag(&self) -> EntityTag {
        self.entity_tag
    }

    #[must_use]
    const fn store_path(&self) -> &str {
        self.store_path.as_str()
    }

    #[must_use]
    const fn key_kind(&self) -> &PersistedFieldKind {
        &self.key_kind
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
        if !db.has_runtime_hooks() {
            return Ok(());
        }

        let hook = db
            .runtime_hook_for_entity_tag(self.entity_tag)
            .map_err(|err| {
                InternalError::strong_relation_target_identity_mismatch(
                    source_path,
                    field_name,
                    self.path.as_str(),
                    format!(
                        "target_entity_tag={} is not registered: {err}",
                        self.entity_tag.value()
                    ),
                )
            })?;

        if hook.entity_path != self.path {
            return Err(InternalError::strong_relation_target_identity_mismatch(
                source_path,
                field_name,
                self.path.as_str(),
                format!(
                    "target_entity_tag={} resolves to entity_path={} but relation declares {}",
                    self.entity_tag.value(),
                    hook.entity_path,
                    self.path
                ),
            ));
        }

        if hook.model.name() != self.entity_name.as_str() {
            return Err(InternalError::strong_relation_target_identity_mismatch(
                source_path,
                field_name,
                self.path.as_str(),
                format!(
                    "target_entity_tag={} resolves to entity_name={} but relation declares {}",
                    self.entity_tag.value(),
                    hook.model.name(),
                    self.entity_name.as_str(),
                ),
            ));
        }

        if hook.store_path != self.store_path {
            return Err(InternalError::strong_relation_target_identity_mismatch(
                source_path,
                field_name,
                self.path.as_str(),
                format!(
                    "target_store_path={} does not match runtime store {} for target_entity_tag={}",
                    self.store_path,
                    hook.store_path,
                    self.entity_tag.value(),
                ),
            ));
        }

        Ok(())
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
    let Some((
        target_path,
        target_entity_name,
        target_entity_tag,
        target_store_path,
        key_kind,
        strength,
    )) = accepted_relation_target_from_kind(kind)
    else {
        return Ok(None);
    };
    if strength != PersistedRelationStrength::Strong {
        return Ok(None);
    }
    if target_path_filter.is_some_and(|filter| filter != target_path) {
        return Ok(None);
    }

    Ok(Some(AcceptedStrongRelationInfo {
        field_index,
        field_name: field_name.to_string(),
        field_kind: kind.clone(),
        target: AcceptedStrongRelationTargetIdentity::try_new(
            source_path,
            field_name,
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
            key_kind,
        )?,
    }))
}

fn accepted_relation_target_from_kind(
    kind: &PersistedFieldKind,
) -> Option<(
    &str,
    &str,
    EntityTag,
    &str,
    &PersistedFieldKind,
    PersistedRelationStrength,
)> {
    fn relation_target(
        kind: &PersistedFieldKind,
    ) -> Option<(
        &str,
        &str,
        EntityTag,
        &str,
        &PersistedFieldKind,
        PersistedRelationStrength,
    )> {
        let PersistedFieldKind::Relation {
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
            key_kind,
            strength,
        } = kind
        else {
            return None;
        };

        Some((
            target_path.as_str(),
            target_entity_name.as_str(),
            *target_entity_tag,
            target_store_path.as_str(),
            key_kind.as_ref(),
            *strength,
        ))
    }

    match kind {
        PersistedFieldKind::Relation { .. } => relation_target(kind),
        PersistedFieldKind::List(inner) | PersistedFieldKind::Set(inner) => {
            relation_target(inner.as_ref())
        }
        _ => None,
    }
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

/// Build reverse-index prefix bounds for one target storage key.
pub(super) fn reverse_index_key_bounds_for_target_storage_key(
    source: ReverseRelationSourceInfo,
    relation: &AcceptedStrongRelationInfo,
    target_key_value: StorageKey,
) -> Result<Option<(RawIndexStoreKey, RawIndexStoreKey)>, InternalError> {
    let Ok(encoded_value) =
        encode_canonical_index_component_from_primary_key_value(target_key_value)
    else {
        return Ok(None);
    };

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
fn reverse_index_key_for_target_and_source_storage_key(
    source: ReverseRelationSourceInfo,
    relation: &AcceptedStrongRelationInfo,
    target_key_value: StorageKey,
    source_key_value: StorageKey,
) -> Result<Option<RawIndexStoreKey>, InternalError> {
    let Ok(encoded_value) =
        encode_canonical_index_component_from_primary_key_value(target_key_value)
    else {
        return Ok(None);
    };

    let index_id = reverse_index_id_for_relation(source, relation)?;
    let key = IndexKey::new_from_components_with_kind(
        &index_id,
        IndexKeyKind::System,
        std::slice::from_ref(&encoded_value),
        source_key_value,
    );

    Ok(Some(key.to_raw()))
}

// Read relation-target raw keys directly from one already-decoded structural
// source row so commit preflight can reuse slot readers it has already
// validated for forward-index planning.
fn relation_target_raw_keys_for_source_slots(
    row_fields: &StructuralSlotReader<'_>,
    source_info: ReverseRelationSourceInfo,
    relation: &AcceptedStrongRelationInfo,
) -> Result<Vec<RawDataStoreKey>, InternalError> {
    let keys = relation_target_storage_keys_for_source_slots(row_fields, source_info, relation)?;

    relation_target_raw_keys_from_primary_key_values(source_info, relation, keys)
}

/// Check whether one persisted source row still references one specific target
/// key for the declared strong relation.
///
/// Delete validation uses this narrower helper because the blocked-delete proof
/// loop only needs membership for one candidate target key, not the full
/// canonicalized target-key set.
pub(in crate::db::relation) fn source_row_references_relation_target(
    raw_row: &RawRow,
    source_row_contract: StructuralRowContract,
    source_info: ReverseRelationSourceInfo,
    relation: &AcceptedStrongRelationInfo,
    target_key: StorageKey,
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
    target_key: StorageKey,
) -> Result<bool, InternalError> {
    let keys = relation_target_storage_keys_for_source_slots(row_fields, source_info, relation)?;

    Ok(keys.into_iter().any(|candidate| candidate == target_key))
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

// Convert decoded relation target primary-key values into canonical sorted raw
// keys.
fn relation_target_raw_keys_from_primary_key_values(
    source: ReverseRelationSourceInfo,
    relation: &AcceptedStrongRelationInfo,
    keys: Vec<StorageKey>,
) -> Result<Vec<RawDataStoreKey>, InternalError> {
    let mut keys = keys
        .into_iter()
        .map(|value| raw_relation_target_key_from_primary_key_value(source, relation, value))
        .collect::<Result<Vec<_>, _>>()?;
    canonicalize_relation_target_keys(&mut keys);

    Ok(keys)
}

// Decode one relation field into structural target primary-key values through the
// shared scalar-fast-path or field-bytes path used by delete validation and
// reverse-index mutation preparation.
fn relation_target_storage_keys_for_source_slots(
    row_fields: &StructuralSlotReader<'_>,
    source: ReverseRelationSourceInfo,
    relation: &AcceptedStrongRelationInfo,
) -> Result<Vec<StorageKey>, InternalError> {
    // Phase 1: keep single relation slots on the scalar fast path when the
    // persisted field already uses a primary-key-compatible leaf codec.
    if let Some(keys) = relation_target_storage_keys_from_scalar_slot(row_fields, source, relation)?
    {
        return Ok(keys);
    }

    // Phase 2: decode the declared relation field payload directly into target
    // primary-key values without rebuilding a runtime `Value` container.
    relation_target_storage_keys_from_field_bytes(row_fields, source, relation)
}

// Decode the one strong-relation field payload needed by structural delete
// validation directly into relation target primary-key values from the
// encoded field bytes.
fn relation_target_storage_keys_from_field_bytes(
    row_fields: &StructuralSlotReader<'_>,
    source: ReverseRelationSourceInfo,
    relation: &AcceptedStrongRelationInfo,
) -> Result<Vec<StorageKey>, InternalError> {
    validate_relation_field_kind(relation)?;

    let bytes = row_fields.required_field_bytes(relation.field_index(), relation.field_name())?;
    let keys = decode_accepted_relation_target_storage_keys_bytes(bytes, relation.field_kind())
        .map_err(|err| {
            InternalError::relation_source_row_decode_failed(
                source.path,
                relation.field_name(),
                relation.target().path(),
                err,
            )
        })?;

    Ok(keys)
}

// Decode one singular strong relation directly from the scalar slot codec when
// the relation key kind is already primary-key-compatible on the persisted row.
fn relation_target_storage_keys_from_scalar_slot(
    row_fields: &StructuralSlotReader<'_>,
    source: ReverseRelationSourceInfo,
    relation: &AcceptedStrongRelationInfo,
) -> Result<Option<Vec<StorageKey>>, InternalError> {
    let PersistedFieldKind::Relation { .. } = relation.field_kind() else {
        return Ok(None);
    };

    match row_fields.required_scalar(relation.field_index())? {
        ScalarSlotValueRef::Null => Ok(Some(Vec::new())),
        ScalarSlotValueRef::Value(value) => {
            let primary_key_value =
                primary_key_value_from_relation_scalar(value).ok_or_else(|| {
                    InternalError::relation_source_row_unsupported_scalar_relation_key(
                        source.path,
                        relation.field_name(),
                        relation.target().path(),
                    )
                })?;

            Ok(Some(vec![primary_key_value]))
        }
    }
}

// Convert one scalar relation payload into the decoded primary-key
// representation used by reverse-index and target-row identities.
const fn primary_key_value_from_relation_scalar(value: ScalarValueRef<'_>) -> Option<StorageKey> {
    match value {
        ScalarValueRef::Int(value) => Some(StorageKey::Int(value)),
        ScalarValueRef::Principal(value) => Some(StorageKey::Principal(value)),
        ScalarValueRef::Subaccount(value) => Some(StorageKey::Subaccount(value)),
        ScalarValueRef::Timestamp(value) => Some(StorageKey::Timestamp(value)),
        ScalarValueRef::Nat(value) => Some(StorageKey::Nat(value)),
        ScalarValueRef::Ulid(value) => Some(StorageKey::Ulid(value)),
        ScalarValueRef::Unit => Some(StorageKey::Unit),
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
    value: StorageKey,
) -> Result<RawDataStoreKey, InternalError> {
    DecodedDataStoreKey::raw_from_parts(relation.target().entity_tag(), value).map_err(|err| {
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
    match relation.field_kind() {
        PersistedFieldKind::Relation { .. }
        | PersistedFieldKind::List(_)
        | PersistedFieldKind::Set(_) => validate_relation_key_kind(relation.target().key_kind()),
        other => Err(InternalError::relation_source_row_invalid_field_kind(other)),
    }
}

// Enforce the accepted primary-key-compatible relation key kinds supported by
// the raw relation target-key builder.
fn validate_relation_key_kind(key_kind: &PersistedFieldKind) -> Result<(), InternalError> {
    match key_kind {
        PersistedFieldKind::Account
        | PersistedFieldKind::Int
        | PersistedFieldKind::Principal
        | PersistedFieldKind::Subaccount
        | PersistedFieldKind::Timestamp
        | PersistedFieldKind::Nat
        | PersistedFieldKind::Ulid
        | PersistedFieldKind::Unit => Ok(()),
        PersistedFieldKind::Relation { key_kind, .. } => validate_relation_key_kind(key_kind),
        other => Err(InternalError::relation_source_row_unsupported_key_kind(
            other,
        )),
    }
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
    // Phase 1: derive the single source storage key once from the already-validated
    // commit marker key instead of recomputing it through typed entity ids.
    let mut ops = Vec::new();

    let relations = accepted_strong_relations_for_row_contract(
        source.path,
        &source_rows.source_row_contract,
        None,
    )?;
    if relations.is_empty() {
        return Ok(ops);
    }

    let Some(source_component) = source_primary_key.scalar_component() else {
        return Err(InternalError::serialize_unsupported(format!(
            "reverse relation index maintenance does not support composite source primary keys yet: source={}",
            source.path,
        )));
    };
    let source_storage_key = StorageKey::from(source_component);

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

            let Some(reverse_key) = reverse_index_key_for_target_and_source_storage_key(
                source,
                &relation,
                target_data_key.try_storage_key()?,
                source_storage_key,
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
