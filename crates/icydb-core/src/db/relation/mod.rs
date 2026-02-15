use crate::{
    db::{
        Db,
        commit::PreparedIndexMutation,
        identity::{EntityName, EntityNameError, IndexName},
        index::{IndexEntry, IndexId, IndexKey, RawIndexEntry, RawIndexKey, fingerprint},
        store::{DataKey, RawDataKey, StorageKey, StorageKeyEncodeError},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::field::{EntityFieldKind, RelationStrength},
    obs::sink::{self, MetricsEvent},
    traits::{EntityKind, EntityValue, Path, Storable},
    value::Value,
};
use canic_utils::hash::Xxh3;
use std::{cell::RefCell, collections::BTreeSet, thread::LocalKey};

///
/// StrongRelationDeleteValidateFn
///
/// Function-pointer contract for delete-side strong relation validators.
///

pub type StrongRelationDeleteValidateFn<C> =
    fn(&Db<C>, &str, &BTreeSet<RawDataKey>) -> Result<(), InternalError>;

///
/// StrongRelationInfo
///
/// Lightweight relation descriptor extracted from runtime field metadata.
///

#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy)]
struct StrongRelationInfo {
    field_name: &'static str,
    target_path: &'static str,
    target_entity_name: &'static str,
    target_store_path: &'static str,
}

///
/// StrongRelationTargetInfo
///
/// Shared target descriptor for strong relation fields.
///

#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy)]
pub struct StrongRelationTargetInfo {
    pub target_path: &'static str,
    pub target_entity_name: &'static str,
    pub target_store_path: &'static str,
}

///
/// RelationTargetRawKeyError
/// Error variants for building a relation target `RawDataKey` from user value.
///

#[derive(Debug)]
pub enum RelationTargetRawKeyError {
    StorageKeyEncode(StorageKeyEncodeError),
    TargetEntityName(EntityNameError),
}

const REVERSE_INDEX_PREFIX: &str = "~ri";

// Resolve a model field-kind into strong relation target metadata (if applicable).
pub const fn strong_relation_target_from_kind(
    kind: &EntityFieldKind,
) -> Option<StrongRelationTargetInfo> {
    match kind {
        EntityFieldKind::Relation {
            target_path,
            target_entity_name,
            target_store_path,
            strength: RelationStrength::Strong,
            ..
        }
        | EntityFieldKind::List(EntityFieldKind::Relation {
            target_path,
            target_entity_name,
            target_store_path,
            strength: RelationStrength::Strong,
            ..
        })
        | EntityFieldKind::Set(EntityFieldKind::Relation {
            target_path,
            target_entity_name,
            target_store_path,
            strength: RelationStrength::Strong,
            ..
        }) => Some(StrongRelationTargetInfo {
            target_path,
            target_entity_name,
            target_store_path,
        }),
        _ => None,
    }
}

// Resolve a model field into strong relation metadata (if applicable).
const fn strong_relation_from_field(
    field_name: &'static str,
    kind: &EntityFieldKind,
) -> Option<StrongRelationInfo> {
    let Some(target) = strong_relation_target_from_kind(kind) else {
        return None;
    };

    Some(StrongRelationInfo {
        field_name,
        target_path: target.target_path,
        target_entity_name: target.target_entity_name,
        target_store_path: target.target_store_path,
    })
}

// Build one relation target raw key from validated entity+storage key components.
fn raw_relation_target_key_from_parts(
    entity_name: EntityName,
    storage_key: StorageKey,
) -> Result<RawDataKey, StorageKeyEncodeError> {
    let entity_bytes = entity_name.to_bytes();
    let key_bytes = storage_key.to_bytes()?;
    let mut raw_bytes = [0u8; DataKey::STORED_SIZE_USIZE];
    raw_bytes[..EntityName::STORED_SIZE_USIZE].copy_from_slice(&entity_bytes);
    raw_bytes[EntityName::STORED_SIZE_USIZE..].copy_from_slice(&key_bytes);

    Ok(<RawDataKey as Storable>::from_bytes(
        std::borrow::Cow::Borrowed(raw_bytes.as_slice()),
    ))
}

/// Convert a relation target `Value` into its canonical `RawDataKey` representation.
pub fn build_relation_target_raw_key(
    target_entity_name: &str,
    value: &Value,
) -> Result<RawDataKey, RelationTargetRawKeyError> {
    let storage_key =
        StorageKey::try_from_value(value).map_err(RelationTargetRawKeyError::StorageKeyEncode)?;
    let entity_name = EntityName::try_from_str(target_entity_name)
        .map_err(RelationTargetRawKeyError::TargetEntityName)?;

    raw_relation_target_key_from_parts(entity_name, storage_key)
        .map_err(RelationTargetRawKeyError::StorageKeyEncode)
}

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
fn reverse_index_key_for_target_value<S>(
    relation: StrongRelationInfo,
    target_key_value: &Value,
) -> Result<Option<RawIndexKey>, InternalError>
where
    S: EntityKind,
{
    let Some(fingerprint) = fingerprint::to_index_fingerprint(target_key_value)? else {
        return Ok(None);
    };

    let index_id = reverse_index_id_for_relation::<S>(relation)?;
    let prefix = [fingerprint];
    let (key, _) = IndexKey::bounds_for_prefix(index_id, 1, &prefix);

    Ok(Some(key.to_raw()))
}

// Resolve strong relation descriptors for a source entity, optionally filtered by target path.
fn strong_relations_for_source<S>(target_path_filter: Option<&str>) -> Vec<StrongRelationInfo>
where
    S: EntityKind,
{
    S::MODEL
        .fields
        .iter()
        .filter_map(|field| strong_relation_from_field(field.name, &field.kind))
        .filter(|relation| {
            target_path_filter.is_none_or(|target_path| relation.target_path == target_path)
        })
        .collect()
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

    match value {
        Value::List(items) => {
            for item in items {
                // Optional relation list entries may be explicit null values.
                if matches!(item, Value::Null) {
                    continue;
                }
                keys.insert(raw_relation_target_key::<S>(field_name, relation, item)?);
            }
        }
        Value::Null => {}
        _ => {
            keys.insert(raw_relation_target_key::<S>(field_name, relation, value)?);
        }
    }

    Ok(keys)
}

// Read relation-target key set from one source entity and relation descriptor.
fn relation_target_keys_for_source<S>(
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
fn decode_reverse_entry<S>(
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
fn relation_target_store<S>(
    db: &Db<S::Canister>,
    relation: StrongRelationInfo,
) -> Result<&'static LocalKey<RefCell<crate::db::index::IndexStore>>, InternalError>
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
#[allow(clippy::too_many_lines)]
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

            let target_data_key = DataKey::try_from_raw(&target_raw_key).map_err(|err| {
                InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Store,
                    format!(
                        "relation target key decode failed while preparing reverse index: source={} field={} target={} ({err})",
                        S::PATH,
                        relation.field_name,
                        relation.target_path,
                    ),
                )
            })?;

            let target_entity = EntityName::try_from_str(relation.target_entity_name).map_err(|err| {
                InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Executor,
                    format!(
                        "relation target entity invalid while preparing reverse index: source={} field={} target={} name={} ({err})",
                        S::PATH,
                        relation.field_name,
                        relation.target_path,
                        relation.target_entity_name,
                    ),
                )
            })?;

            if target_data_key.entity_name() != &target_entity {
                return Err(InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Store,
                    format!(
                        "relation target entity mismatch while preparing reverse index: source={} field={} target={} expected={} actual={}",
                        S::PATH,
                        relation.field_name,
                        relation.target_path,
                        relation.target_entity_name,
                        target_data_key.entity_name(),
                    ),
                ));
            }

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

/// Validate that source rows do not strongly reference target keys selected for delete.
#[allow(clippy::too_many_lines)]
pub fn validate_delete_strong_relations_for_source<S>(
    db: &Db<S::Canister>,
    target_path: &str,
    deleted_target_keys: &BTreeSet<RawDataKey>,
) -> Result<(), InternalError>
where
    S: EntityKind + EntityValue,
{
    if deleted_target_keys.is_empty() {
        return Ok(());
    }

    let relations = strong_relations_for_source::<S>(Some(target_path));
    if relations.is_empty() {
        return Ok(());
    }
    let source_store = db.with_store_registry(|reg| reg.try_get_store(S::Store::PATH))?;

    // Phase 1: resolve reverse-index candidates for each relevant relation field.
    for relation in relations {
        let target_index_store = relation_target_store::<S>(db, relation)?;

        for target_raw_key in deleted_target_keys {
            let target_data_key = DataKey::try_from_raw(target_raw_key).map_err(|err| {
                InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Store,
                    format!(
                        "delete relation target key decode failed: source={} field={} target={} ({err})",
                        S::PATH,
                        relation.field_name,
                        relation.target_path,
                    ),
                )
            })?;

            let target_entity = EntityName::try_from_str(relation.target_entity_name).map_err(|err| {
                InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Executor,
                    format!(
                        "strong relation target entity invalid during delete validation: source={} field={} target={} name={} ({err})",
                        S::PATH,
                        relation.field_name,
                        relation.target_path,
                        relation.target_entity_name,
                    ),
                )
            })?;

            if target_data_key.entity_name() != &target_entity {
                continue;
            }

            let target_value = target_data_key.storage_key().as_value();
            let Some(reverse_key) =
                reverse_index_key_for_target_value::<S>(relation, &target_value)?
            else {
                continue;
            };

            // Relation metrics are emitted as operation deltas so sink aggregation
            // always reflects the exact lookup/block operations performed.
            sink::record(MetricsEvent::RelationValidation {
                entity_path: S::PATH,
                reverse_lookups: 1,
                blocked_deletes: 0,
            });

            let Some(raw_entry) = target_index_store.with_borrow(|store| store.get(&reverse_key))
            else {
                continue;
            };

            let entry = decode_reverse_entry::<S>(relation, &reverse_key, &raw_entry)?;

            // Phase 2: verify each candidate source row before rejecting delete.
            for source_key in entry.iter_ids() {
                let source_data_key = DataKey::try_new::<S>(source_key)?;
                let source_raw_key = source_data_key.to_raw()?;
                let source_raw_row = source_store.with_data(|store| store.get(&source_raw_key));

                let Some(source_raw_row) = source_raw_row else {
                    return Err(InternalError::new(
                        ErrorClass::Corruption,
                        ErrorOrigin::Store,
                        format!(
                            "reverse index points at missing source row: source={} field={} source_id={source_key:?} target={} key={target_value:?}",
                            S::PATH,
                            relation.field_name,
                            relation.target_path,
                        ),
                    ));
                };

                let source = source_raw_row.try_decode::<S>().map_err(|err| {
                    InternalError::new(
                        ErrorClass::Corruption,
                        ErrorOrigin::Serialize,
                        format!(
                            "source row decode failed during delete relation validation: source={} ({err})",
                            S::PATH
                        ),
                    )
                })?;

                let source_targets = relation_target_keys_for_source(&source, relation)?;
                if source_targets.contains(target_raw_key) {
                    sink::record(MetricsEvent::RelationValidation {
                        entity_path: S::PATH,
                        reverse_lookups: 0,
                        blocked_deletes: 1,
                    });
                    return Err(InternalError::new(
                        ErrorClass::Unsupported,
                        ErrorOrigin::Executor,
                        blocked_delete_diagnostic::<S>(relation, source_key, &target_value),
                    ));
                }
            }
        }
    }

    Ok(())
}

// Format operator-facing blocked-delete diagnostics with actionable context.
fn blocked_delete_diagnostic<S>(
    relation: StrongRelationInfo,
    source_key: S::Key,
    target_value: &Value,
) -> String
where
    S: EntityKind + EntityValue,
{
    format!(
        "delete blocked by strong relation: source_entity={} source_field={} source_id={source_key:?} target_entity={} target_key={target_value:?}; action=delete source rows or retarget relation before deleting target",
        S::PATH,
        relation.field_name,
        relation.target_path,
    )
}

// Convert a relation value to its target raw data key representation.
fn raw_relation_target_key<S>(
    field_name: &str,
    relation: StrongRelationInfo,
    value: &Value,
) -> Result<RawDataKey, InternalError>
where
    S: EntityKind + EntityValue,
{
    build_relation_target_raw_key(relation.target_entity_name, value).map_err(|err| match err {
        RelationTargetRawKeyError::StorageKeyEncode(err) => InternalError::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Executor,
            format!(
                "strong relation key not storage-compatible during relation processing: source={} field={} target={} value={value:?} ({err})",
                S::PATH,
                field_name,
                relation.target_path,
            ),
        ),
        RelationTargetRawKeyError::TargetEntityName(err) => InternalError::new(
            ErrorClass::Internal,
            ErrorOrigin::Executor,
            format!(
                "strong relation target entity invalid during relation processing: source={} field={} target={} name={} ({err})",
                S::PATH,
                field_name,
                relation.target_path,
                relation.target_entity_name,
            ),
        ),
    })
}
