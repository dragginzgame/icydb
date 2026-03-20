//! Module: relation::reverse_index
//! Responsibility: maintain reverse-index relation targets for strong relation consistency.
//! Does not own: planner query semantics or execution routing policies.
//! Boundary: applies relation reverse-index mutations during commit pathways.

use crate::{
    db::{
        Db,
        commit::{PreparedIndexDeltaKind, PreparedIndexMutation},
        data::{DataKey, RawDataKey, StorageKey},
        index::{
            EncodedValue, IndexEntry, IndexEntryReader, IndexId, IndexKeyKind, IndexStore,
            RawIndexEntry, RawIndexKey, raw_keys_for_encoded_prefix_with_kind,
        },
        relation::{
            RelationTargetDecodeContext, RelationTargetMismatchPolicy,
            for_each_relation_target_value,
            metadata::{StrongRelationInfo, strong_relations_for_source},
            raw_relation_target_key,
        },
    },
    error::InternalError,
    traits::{CanisterKind, EntityKind, EntityValue, FieldValue},
    types::EntityTag,
    value::Value,
};
use std::{cell::RefCell, collections::BTreeSet, thread::LocalKey};

///
/// ReverseRelationSourceInfo
///
/// Structural authority used while preparing reverse-index mutations.
/// Carries only the source entity path and tag required for diagnostics and
/// reverse-index identity, so the heavy mutation loop does not need `S`.
///

#[derive(Clone, Copy)]
pub(super) struct ReverseRelationSourceInfo {
    path: &'static str,
    entity_tag: EntityTag,
}

impl ReverseRelationSourceInfo {
    /// Lower one typed source entity into the structural authority used by reverse-index prep.
    pub(super) const fn for_type<S>() -> Self
    where
        S: EntityKind,
    {
        Self {
            path: S::PATH,
            entity_tag: S::ENTITY_TAG,
        }
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
    reverse_key: RawIndexKey,
    old_contains: bool,
    new_contains: bool,
}

/// Build the canonical reverse-index id for a `(source entity, relation field)` pair.
fn reverse_index_id_for_relation(
    source: ReverseRelationSourceInfo,
    relation: StrongRelationInfo,
) -> Result<IndexId, InternalError> {
    let ordinal = u16::try_from(relation.field_index).map_err(|err| {
        InternalError::index_internal(format!(
            "reverse index ordinal overflow: source={} field={} target={} ({err})",
            source.path, relation.field_name, relation.target_path,
        ))
    })?;

    Ok(IndexId::new(source.entity_tag, ordinal))
}

/// Build a reverse-index key for one target-key value.
pub(super) fn reverse_index_key_for_target_value(
    source: ReverseRelationSourceInfo,
    relation: StrongRelationInfo,
    target_key_value: &Value,
) -> Result<Option<RawIndexKey>, InternalError> {
    let Ok(encoded_value) = EncodedValue::try_from_ref(target_key_value) else {
        return Ok(None);
    };

    let index_id = reverse_index_id_for_relation(source, relation)?;
    let (key, _) = raw_keys_for_encoded_prefix_with_kind(
        &index_id,
        IndexKeyKind::System,
        1,
        std::slice::from_ref(&encoded_value),
    );

    Ok(Some(key))
}

/// Extract relation-target raw keys from a field value.
fn relation_target_keys_from_value(
    source: ReverseRelationSourceInfo,
    field_name: &str,
    relation: StrongRelationInfo,
    value: &Value,
) -> Result<BTreeSet<RawDataKey>, InternalError> {
    let mut keys = BTreeSet::new();

    for_each_relation_target_value(value, |item| {
        keys.insert(raw_relation_target_key(
            source.path,
            field_name,
            relation,
            item,
        )?);
        Ok(())
    })?;

    Ok(keys)
}

/// Read relation-target key set from one source entity and relation descriptor.
pub(super) fn relation_target_keys_for_source<S>(
    source: &S,
    source_info: ReverseRelationSourceInfo,
    relation: StrongRelationInfo,
) -> Result<BTreeSet<RawDataKey>, InternalError>
where
    S: EntityKind + EntityValue,
{
    let value = source
        .get_value_by_index(relation.field_index)
        .ok_or_else(|| {
            crate::db::error::executor_internal(format!(
                "entity field missing during strong relation processing: source={} field={}",
                source_info.path, relation.field_name,
            ))
        })?;

    relation_target_keys_from_value(source_info, relation.field_name, relation, &value)
}

/// Decode a reverse-index entry into source-key membership.
pub(super) fn decode_reverse_entry(
    source: ReverseRelationSourceInfo,
    relation: StrongRelationInfo,
    index_key: &RawIndexKey,
    raw_entry: &RawIndexEntry,
) -> Result<IndexEntry, InternalError> {
    raw_entry.try_decode().map_err(|err| {
        InternalError::index_corruption(format!(
            "reverse index entry corrupted: source={} field={} target={} key={:?} ({err})",
            source.path, relation.field_name, relation.target_path, index_key,
        ))
    })
}

/// Encode a reverse-index entry with bounded-size error mapping.
fn encode_reverse_entry(
    source: ReverseRelationSourceInfo,
    relation: StrongRelationInfo,
    entry: &IndexEntry,
) -> Result<RawIndexEntry, InternalError> {
    RawIndexEntry::try_from_entry(entry).map_err(|err| {
        InternalError::index_unsupported(format!(
            "reverse index entry encoding failed: source={} field={} target={} ({err})",
            source.path, relation.field_name, relation.target_path,
        ))
    })
}

/// Resolve target store handle for one relation descriptor.
pub(super) fn relation_target_store<C>(
    db: &Db<C>,
    source: ReverseRelationSourceInfo,
    relation: StrongRelationInfo,
) -> Result<&'static LocalKey<RefCell<IndexStore>>, InternalError>
where
    C: CanisterKind,
{
    db.with_store_registry(|reg| reg.try_get_store(relation.target_store_path))
        .map(|store| store.index_store())
        .map_err(|err| {
            crate::db::error::executor_internal(format!(
                "relation target store missing: source={} field={} target={} store={} ({err})",
                source.path, relation.field_name, relation.target_path, relation.target_store_path,
            ))
        })
}

/// Decode one raw relation target key and enforce reverse-index target invariants.
fn decode_relation_target_data_key_for_relation(
    source: ReverseRelationSourceInfo,
    relation: StrongRelationInfo,
    target_raw_key: &RawDataKey,
    context: RelationTargetDecodeContext,
    mismatch_policy: RelationTargetMismatchPolicy,
) -> Result<Option<DataKey>, InternalError> {
    let target_data_key = DataKey::try_from_raw(target_raw_key).map_err(|err| {
        InternalError::identity_corruption(format!(
            "{}: source={} field={} target={} ({err})",
            match context {
                RelationTargetDecodeContext::DeleteValidation => {
                    "delete relation target key decode failed"
                }
                RelationTargetDecodeContext::ReverseIndexPrepare => {
                    "relation target key decode failed while preparing reverse index"
                }
            },
            source.path,
            relation.field_name,
            relation.target_path,
        ))
    })?;

    if target_data_key.entity_tag() != relation.target_entity_tag {
        if matches!(mismatch_policy, RelationTargetMismatchPolicy::Skip) {
            return Ok(None);
        }

        return Err(InternalError::store_corruption(format!(
            "{}: source={} field={} target={} expected={} (tag={}) actual_tag={}",
            match context {
                RelationTargetDecodeContext::DeleteValidation => {
                    "relation target entity mismatch during delete validation"
                }
                RelationTargetDecodeContext::ReverseIndexPrepare => {
                    "relation target entity mismatch while preparing reverse index"
                }
            },
            source.path,
            relation.field_name,
            relation.target_path,
            relation.target_entity_name,
            relation.target_entity_tag.value(),
            target_data_key.entity_tag().value(),
        )));
    }

    Ok(Some(target_data_key))
}

/// Build one reverse-index mutation for one touched target key.
fn prepare_reverse_relation_index_mutation_for_target(
    source: ReverseRelationSourceInfo,
    relation: StrongRelationInfo,
    target: ReverseRelationMutationTarget,
    existing: Option<&RawIndexEntry>,
    old_source_storage_key: Option<&StorageKey>,
    new_source_storage_key: Option<&StorageKey>,
) -> Result<Option<PreparedIndexMutation>, InternalError> {
    if target.old_contains == target.new_contains {
        return Ok(None);
    }

    let mut entry = existing
        .map(|raw| decode_reverse_entry(source, relation, &target.reverse_key, raw))
        .transpose()?;

    let delta_kind = if target.old_contains {
        if let Some(source_key) = old_source_storage_key
            && let Some(current) = entry.as_mut()
        {
            current.remove(*source_key);
        }
        PreparedIndexDeltaKind::ReverseIndexRemove
    } else if target.new_contains {
        if let Some(source_key) = new_source_storage_key {
            if let Some(current) = entry.as_mut() {
                current.insert(*source_key);
            } else {
                entry = Some(IndexEntry::new(*source_key));
            }
        }
        PreparedIndexDeltaKind::ReverseIndexInsert
    } else {
        PreparedIndexDeltaKind::None
    };

    let next_value = if let Some(next_entry) = entry {
        if next_entry.is_empty() {
            None
        } else {
            Some(encode_reverse_entry(source, relation, &next_entry)?)
        }
    } else {
        None
    };

    Ok(Some(PreparedIndexMutation {
        store: target.target_store,
        key: target.reverse_key,
        value: next_value,
        delta_kind,
    }))
}

/// Prepare reverse-index mutations for one source entity transition.
///
/// This derives mechanical index writes/deletes that keep delete-time strong
/// relation validation O(referrers) instead of O(source rows).
pub(crate) fn prepare_reverse_relation_index_mutations_for_source<S>(
    db: &Db<S::Canister>,
    index_reader: &(impl IndexEntryReader<S> + ?Sized),
    old: Option<&S>,
    new: Option<&S>,
) -> Result<Vec<PreparedIndexMutation>, InternalError>
where
    S: EntityKind + EntityValue,
{
    let source = ReverseRelationSourceInfo::for_type::<S>();

    // Phase 1: short-circuit when the source entity has no strong relations.
    let relations = strong_relations_for_source::<S>(None);
    if relations.is_empty() {
        return Ok(Vec::new());
    }

    // Phase 2: capture the old/new source ids used to remove/insert reverse members.
    let old_source_key = old
        .map(|entity| StorageKey::try_from_value(&entity.id().key().to_value()))
        .transpose()?;
    let new_source_key = new
        .map(|entity| StorageKey::try_from_value(&entity.id().key().to_value()))
        .transpose()?;

    let mut ops = Vec::new();

    // Phase 3: evaluate each strong relation independently and derive index deltas.
    for relation in relations {
        // Build target-key sets before/after the mutation to compute membership deltas.
        let old_targets = match old {
            Some(entity) => relation_target_keys_for_source(entity, source, relation)?,
            None => BTreeSet::new(),
        };
        let new_targets = match new {
            Some(entity) => relation_target_keys_for_source(entity, source, relation)?,
            None => BTreeSet::new(),
        };

        // Resolve the reverse-index store for this relation once per relation.
        let target_store = relation_target_store(db, source, relation)?;

        // Only keys touched by either side can produce a reverse-index mutation.
        let touched_target_keys = old_targets
            .union(&new_targets)
            .copied()
            .collect::<BTreeSet<_>>();

        for target_raw_key in touched_target_keys {
            // Determine whether membership actually changed for this target key.
            let old_contains = old_targets.contains(&target_raw_key);
            let new_contains = new_targets.contains(&target_raw_key);

            let Some(target_data_key) = decode_relation_target_data_key_for_relation(
                source,
                relation,
                &target_raw_key,
                RelationTargetDecodeContext::ReverseIndexPrepare,
                RelationTargetMismatchPolicy::Reject,
            )?
            else {
                return Err(crate::db::error::executor_internal(format!(
                    "relation target decode invariant violated while preparing reverse index: source={} field={} target={}",
                    source.path, relation.field_name, relation.target_path,
                )));
            };

            let target_value = target_data_key.storage_key().as_value();
            let Some(reverse_key) =
                reverse_index_key_for_target_value(source, relation, &target_value)?
            else {
                continue;
            };

            let existing = index_reader.read_index_entry(target_store, &reverse_key)?;
            let target = ReverseRelationMutationTarget {
                target_store,
                reverse_key,
                old_contains,
                new_contains,
            };
            let Some(op) = prepare_reverse_relation_index_mutation_for_target(
                source,
                relation,
                target,
                existing.as_ref(),
                old_source_key.as_ref(),
                new_source_key.as_ref(),
            )?
            else {
                continue;
            };

            ops.push(op);
        }
    }

    Ok(ops)
}
