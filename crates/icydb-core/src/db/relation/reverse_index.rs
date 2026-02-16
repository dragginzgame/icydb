use super::{
    RelationTargetDecodeContext, RelationTargetMismatchPolicy,
    decode_relation_target_data_key_for_relation, for_each_relation_target_value,
    metadata::{StrongRelationInfo, strong_relations_for_source},
    raw_relation_target_key,
};
use crate::{
    db::{
        Db,
        commit::PreparedIndexMutation,
        identity::{EntityName, IndexName},
        index::{
            IndexEntry, IndexId, IndexKey, IndexKeyKind, IndexStore, RawIndexEntry, RawIndexKey,
            key::encode_canonical_index_component,
        },
        store::RawDataKey,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::{EntityKind, EntityValue},
    value::Value,
};
use canic_utils::hash::Xxh3;
use std::{cell::RefCell, collections::BTreeSet, thread::LocalKey};

const REVERSE_INDEX_PREFIX: &str = "~ri";

// Build the canonical reverse-index id for a `(source entity, relation field)` pair.
fn reverse_index_id_for_relation<S>(relation: StrongRelationInfo) -> Result<IndexId, InternalError>
where
    S: EntityKind,
{
    let source_entity_name = EntityName::try_from_str(S::ENTITY_NAME).map_err(|err| {
        InternalError::new(
            ErrorClass::Internal,
            ErrorOrigin::Index,
            format!(
                "invalid source entity name while building reverse index id: source={} field={} ({err})",
                S::PATH,
                relation.field_name,
            ),
        )
    })?;

    // Hash relation identity into a bounded ASCII token so index-id fields
    // stay within identity limits even when paths are long.
    let mut hasher = Xxh3::with_seed(0);
    hasher.update(S::PATH.as_bytes());
    hasher.update(b"|");
    hasher.update(relation.field_name.as_bytes());
    hasher.update(b"|");
    hasher.update(relation.target_path.as_bytes());
    let relation_token = format!("h{:032x}", hasher.digest128());

    let fields = [
        REVERSE_INDEX_PREFIX,
        relation.target_entity_name,
        relation_token.as_str(),
    ];
    let name = IndexName::try_from_parts(&source_entity_name, &fields).map_err(|err| {
        InternalError::new(
            ErrorClass::Internal,
            ErrorOrigin::Index,
            format!(
                "reverse index id construction failed: source={} field={} target={} ({err})",
                S::PATH,
                relation.field_name,
                relation.target_path,
            ),
        )
    })?;

    Ok(IndexId(name))
}

// Build a reverse-index key for one target-key value.
pub(super) fn reverse_index_key_for_target_value<S>(
    relation: StrongRelationInfo,
    target_key_value: &Value,
) -> Result<Option<RawIndexKey>, InternalError>
where
    S: EntityKind,
{
    let Ok(component) = encode_canonical_index_component(target_key_value) else {
        return Ok(None);
    };

    let index_id = reverse_index_id_for_relation::<S>(relation)?;
    let prefix = vec![component];
    let (key, _) =
        IndexKey::bounds_for_prefix_with_kind(index_id, IndexKeyKind::System, 1, &prefix);

    Ok(Some(key.to_raw()))
}

// Extract relation-target raw keys from a field value.
fn relation_target_keys_from_value<S>(
    field_name: &str,
    relation: StrongRelationInfo,
    value: &Value,
) -> Result<BTreeSet<RawDataKey>, InternalError>
where
    S: EntityKind + EntityValue,
{
    let mut keys = BTreeSet::new();

    for_each_relation_target_value(value, |item| {
        keys.insert(raw_relation_target_key::<S>(field_name, relation, item)?);
        Ok(())
    })?;

    Ok(keys)
}

// Read relation-target key set from one source entity and relation descriptor.
pub(super) fn relation_target_keys_for_source<S>(
    source: &S,
    relation: StrongRelationInfo,
) -> Result<BTreeSet<RawDataKey>, InternalError>
where
    S: EntityKind + EntityValue,
{
    let value = source.get_value(relation.field_name).ok_or_else(|| {
        InternalError::new(
            ErrorClass::Internal,
            ErrorOrigin::Executor,
            format!(
                "entity field missing during strong relation processing: source={} field={}",
                S::PATH,
                relation.field_name,
            ),
        )
    })?;

    relation_target_keys_from_value::<S>(relation.field_name, relation, &value)
}

// Decode a reverse-index entry into source-key membership.
pub(super) fn decode_reverse_entry<S>(
    relation: StrongRelationInfo,
    index_key: &RawIndexKey,
    raw_entry: &RawIndexEntry,
) -> Result<IndexEntry<S>, InternalError>
where
    S: EntityKind + EntityValue,
{
    raw_entry.try_decode::<S>().map_err(|err| {
        InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Index,
            format!(
                "reverse index entry corrupted: source={} field={} target={} key={:?} ({err})",
                S::PATH,
                relation.field_name,
                relation.target_path,
                index_key,
            ),
        )
    })
}

// Encode a reverse-index entry with bounded-size error mapping.
fn encode_reverse_entry<S>(
    relation: StrongRelationInfo,
    entry: &IndexEntry<S>,
) -> Result<RawIndexEntry, InternalError>
where
    S: EntityKind + EntityValue,
{
    RawIndexEntry::try_from_entry(entry).map_err(|err| {
        InternalError::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Index,
            format!(
                "reverse index entry encoding failed: source={} field={} target={} ({err})",
                S::PATH,
                relation.field_name,
                relation.target_path,
            ),
        )
    })
}

// Resolve target store handle for one relation descriptor.
pub(super) fn relation_target_store<S>(
    db: &Db<S::Canister>,
    relation: StrongRelationInfo,
) -> Result<&'static LocalKey<RefCell<IndexStore>>, InternalError>
where
    S: EntityKind + EntityValue,
{
    db.with_store_registry(|reg| reg.try_get_store(relation.target_store_path))
        .map(|store| store.index_store())
        .map_err(|err| {
            InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Executor,
                format!(
                    "relation target store missing: source={} field={} target={} store={} ({err})",
                    S::PATH,
                    relation.field_name,
                    relation.target_path,
                    relation.target_store_path,
                ),
            )
        })
}

/// Prepare reverse-index mutations for one source entity transition.
///
/// This derives mechanical index writes/deletes that keep delete-time strong
/// relation validation O(referrers) instead of O(source rows).
pub fn prepare_reverse_relation_index_mutations_for_source<S>(
    db: &Db<S::Canister>,
    old: Option<&S>,
    new: Option<&S>,
) -> Result<(Vec<PreparedIndexMutation>, usize, usize), InternalError>
where
    S: EntityKind + EntityValue,
{
    let relations = strong_relations_for_source::<S>(None);
    if relations.is_empty() {
        return Ok((Vec::new(), 0, 0));
    }

    let old_source_key = old.map(|entity| entity.id().key());
    let new_source_key = new.map(|entity| entity.id().key());

    let mut ops = Vec::new();
    let mut remove_count = 0usize;
    let mut insert_count = 0usize;

    for relation in relations {
        let old_targets = match old {
            Some(entity) => relation_target_keys_for_source(entity, relation)?,
            None => BTreeSet::new(),
        };
        let new_targets = match new {
            Some(entity) => relation_target_keys_for_source(entity, relation)?,
            None => BTreeSet::new(),
        };

        let target_store = relation_target_store::<S>(db, relation)?;

        let touched_target_keys = old_targets
            .union(&new_targets)
            .copied()
            .collect::<BTreeSet<_>>();

        for target_raw_key in touched_target_keys {
            let old_contains = old_targets.contains(&target_raw_key);
            let new_contains = new_targets.contains(&target_raw_key);

            if old_contains == new_contains {
                continue;
            }

            let Some(target_data_key) = decode_relation_target_data_key_for_relation::<S>(
                relation,
                &target_raw_key,
                RelationTargetDecodeContext::ReverseIndexPrepare,
                RelationTargetMismatchPolicy::Reject,
            )?
            else {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Executor,
                    format!(
                        "relation target decode invariant violated while preparing reverse index: source={} field={} target={}",
                        S::PATH,
                        relation.field_name,
                        relation.target_path,
                    ),
                ));
            };

            let target_value = target_data_key.storage_key().as_value();
            let Some(reverse_key) =
                reverse_index_key_for_target_value::<S>(relation, &target_value)?
            else {
                continue;
            };

            let existing = target_store.with_borrow(|store| store.get(&reverse_key));
            let mut entry = existing
                .as_ref()
                .map(|raw| decode_reverse_entry::<S>(relation, &reverse_key, raw))
                .transpose()?;

            if old_contains && let Some(source_key) = old_source_key {
                if let Some(current) = entry.as_mut() {
                    current.remove(source_key);
                }
                remove_count = remove_count.saturating_add(1);
            }

            if new_contains && let Some(source_key) = new_source_key {
                if let Some(current) = entry.as_mut() {
                    current.insert(source_key);
                } else {
                    entry = Some(IndexEntry::new(source_key));
                }
                insert_count = insert_count.saturating_add(1);
            }

            let next_value = if let Some(next_entry) = entry {
                if next_entry.is_empty() {
                    None
                } else {
                    Some(encode_reverse_entry::<S>(relation, &next_entry)?)
                }
            } else {
                None
            };

            ops.push(PreparedIndexMutation {
                store: target_store,
                key: reverse_key,
                value: next_value,
            });
        }
    }

    Ok((ops, remove_count, insert_count))
}
