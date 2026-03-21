//! Module: relation::reverse_index
//! Responsibility: maintain reverse-index relation targets for strong relation consistency.
//! Does not own: planner query semantics or execution routing policies.
//! Boundary: applies relation reverse-index mutations during commit pathways.

use crate::{
    db::{
        Db,
        codec::deserialize_row,
        commit::{PreparedIndexDeltaKind, PreparedIndexMutation},
        data::{DataKey, RawDataKey, RawRow, StorageKey},
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
    model::field::FieldKind,
    traits::{CanisterKind, EntityKind, EntityValue},
    types::EntityTag,
    value::Value,
};
use serde_cbor::{Value as CborValue, value::from_value as cbor_from_value};
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

/// Read relation-target raw keys directly from one persisted source row.
///
/// This structural path exists for delete validation, where the runtime only
/// needs the relation field payload and should not decode the full typed
/// entity inside the hot blocked-delete proof loop.
pub(super) fn relation_target_keys_for_source_row(
    raw_row: &RawRow,
    source_info: ReverseRelationSourceInfo,
    relation: StrongRelationInfo,
) -> Result<BTreeSet<RawDataKey>, InternalError> {
    let row_map = decode_relation_row_map(raw_row, source_info, relation)?;
    let relation_value = decode_relation_field_from_row_map(&row_map, source_info, relation)?;

    relation_target_keys_from_value(source_info, relation.field_name, relation, &relation_value)
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

// Decode one persisted row into the top-level CBOR object map used by
// structural relation-field extraction.
fn decode_relation_row_map(
    raw_row: &RawRow,
    source: ReverseRelationSourceInfo,
    relation: StrongRelationInfo,
) -> Result<std::collections::BTreeMap<CborValue, CborValue>, InternalError> {
    let decoded = deserialize_row::<CborValue>(raw_row.as_bytes())?;
    let CborValue::Map(map) = unwrap_cbor_tags(decoded) else {
        return Err(InternalError::serialize_corruption(format!(
            "relation source row decode failed: expected CBOR map: source={} field={} target={}",
            source.path, relation.field_name, relation.target_path,
        )));
    };

    Ok(map)
}

// Decode the one strong-relation field payload needed by structural delete
// validation directly from the persisted row object.
fn decode_relation_field_from_row_map(
    row_map: &std::collections::BTreeMap<CborValue, CborValue>,
    source: ReverseRelationSourceInfo,
    relation: StrongRelationInfo,
) -> Result<Value, InternalError> {
    let lookup_key = CborValue::Text(relation.field_name.to_string());
    let Some(raw_value) = row_map.get(&lookup_key) else {
        return Err(InternalError::serialize_corruption(format!(
            "relation source row decode failed: missing field: source={} field={} target={}",
            source.path, relation.field_name, relation.target_path,
        )));
    };

    decode_relation_field_value(raw_value, relation.field_kind).map_err(|message| {
        InternalError::serialize_corruption(format!(
            "relation source row decode failed: source={} field={} target={} ({message})",
            source.path, relation.field_name, relation.target_path,
        ))
    })
}

// Decode one strong-relation field payload using only the narrow storage-key
// field kinds allowed for relation targets.
fn decode_relation_field_value(raw_value: &CborValue, kind: FieldKind) -> Result<Value, String> {
    let raw_value = unwrap_cbor_tags(raw_value.clone());
    if matches!(raw_value, CborValue::Null) {
        return Ok(Value::Null);
    }

    match kind {
        FieldKind::Relation { key_kind, .. } => decode_relation_key_value(raw_value, *key_kind),
        FieldKind::List(FieldKind::Relation { key_kind, .. })
        | FieldKind::Set(FieldKind::Relation { key_kind, .. }) => {
            decode_relation_key_list_value(raw_value, **key_kind)
        }
        other => Err(format!(
            "invalid strong relation field kind during structural decode: {other:?}"
        )),
    }
}

// Decode one storage-key-compatible relation id into the runtime `Value`
// representation used by the existing raw-key builder.
fn decode_relation_key_value(raw_value: CborValue, key_kind: FieldKind) -> Result<Value, String> {
    match key_kind {
        FieldKind::Account => decode_typed_relation_key(raw_value, Value::Account),
        FieldKind::Int => decode_typed_relation_key(raw_value, Value::Int),
        FieldKind::Principal => decode_typed_relation_key(raw_value, Value::Principal),
        FieldKind::Subaccount => decode_typed_relation_key(raw_value, Value::Subaccount),
        FieldKind::Timestamp => decode_typed_relation_key(raw_value, Value::Timestamp),
        FieldKind::Uint => decode_typed_relation_key(raw_value, Value::Uint),
        FieldKind::Ulid => decode_typed_relation_key(raw_value, Value::Ulid),
        FieldKind::Unit => decode_typed_relation_key(raw_value, |()| Value::Unit),
        other => Err(format!(
            "unsupported strong relation key kind during structural decode: {other:?}"
        )),
    }
}

// Decode one list/set relation payload by decoding each storage-key-compatible item.
fn decode_relation_key_list_value(
    raw_value: CborValue,
    key_kind: FieldKind,
) -> Result<Value, String> {
    let CborValue::Array(items) = raw_value else {
        return Err("expected CBOR array for relation collection".to_string());
    };
    let items = items
        .into_iter()
        .map(|item| decode_relation_key_value(item, key_kind))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Value::List(items))
}

// Decode one typed storage-key payload through serde CBOR and map it into one `Value`.
fn decode_typed_relation_key<T>(
    raw_value: CborValue,
    into_value: impl FnOnce(T) -> Value,
) -> Result<Value, String>
where
    T: serde::de::DeserializeOwned,
{
    cbor_from_value::<T>(raw_value)
        .map(into_value)
        .map_err(|err| format!("typed CBOR decode failed: {err}"))
}

// Strip transparent CBOR tags before decoding relation field payloads.
fn unwrap_cbor_tags(mut value: CborValue) -> CborValue {
    while let CborValue::Tag(_, inner) = value {
        value = *inner;
    }

    value
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
pub(crate) fn prepare_reverse_relation_index_mutations_for_source_rows<S>(
    db: &Db<S::Canister>,
    index_reader: &(impl IndexEntryReader<S> + ?Sized),
    source_storage_key: StorageKey,
    old_row: Option<&RawRow>,
    new_row: Option<&RawRow>,
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

    // Phase 2: derive the single source storage key once from the already-validated
    // commit marker key instead of recomputing it through typed entity ids.
    let old_source_key = old_row.map(|_| source_storage_key);
    let new_source_key = new_row.map(|_| source_storage_key);

    let mut ops = Vec::new();

    // Phase 3: evaluate each strong relation independently and derive index deltas
    // directly from persisted row payloads.
    for relation in relations {
        let old_targets = match old_row {
            Some(row) => relation_target_keys_for_source_row(row, source, relation)?,
            None => BTreeSet::new(),
        };
        let new_targets = match new_row {
            Some(row) => relation_target_keys_for_source_row(row, source, relation)?,
            None => BTreeSet::new(),
        };

        let target_store = relation_target_store(db, source, relation)?;

        let touched_target_keys = old_targets
            .union(&new_targets)
            .copied()
            .collect::<BTreeSet<_>>();

        for target_raw_key in touched_target_keys {
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
