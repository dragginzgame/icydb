//! Module: commit::marker
//! Responsibility: define persisted commit-marker payloads and marker-shape validation.
//! Does not own: marker storage backend, commit-window lifecycle, or recovery orchestration.
//! Boundary: commit::{prepare,recovery,store} -> commit::marker (one-way).

use crate::{
    db::{
        codec::MAX_ROW_BYTES,
        commit::{commit_component_corruption, commit_corruption_message},
        data::{DataKey, RawDataKey},
        index::{IndexKey, MAX_INDEX_ENTRY_BYTES, RawIndexEntry, RawIndexKey},
    },
    error::InternalError,
    model::EntityModel,
    traits::EntityKind,
    types::Ulid,
};
use canic_cdk::structures::Storable;
use canic_utils::hash::Xxh3;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

// Commit-marker durability invariant:
// - Persist one marker before any stable mutation.
// - After marker persistence, apply/recovery consume only marker payloads.
// - Recovery replays marker row ops deterministically.
// This makes partial mutations deterministic without a WAL.

pub(crate) const COMMIT_LABEL: &str = "CommitMarker";
const COMMIT_ID_BYTES: usize = 16;
const COMMIT_SCHEMA_FINGERPRINT_BYTES: usize = 16;
const COMMIT_SCHEMA_FINGERPRINT_VERSION: u8 = 1;

pub(in crate::db) type CommitSchemaFingerprint = [u8; COMMIT_SCHEMA_FINGERPRINT_BYTES];

// Conservative upper bound to avoid rejecting valid commits when index entries
// are large; still small enough to fit typical canister constraints.
pub(crate) const MAX_COMMIT_BYTES: u32 = 16 * 1024 * 1024;

///
/// CommitRowOp
///
/// Row-level mutation recorded in a commit marker.
/// Store identity is derived from `entity_path` at apply/recovery time.
///

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::db) struct CommitRowOp {
    pub(crate) entity_path: String,
    pub(crate) key: Vec<u8>,
    pub(crate) before: Option<Vec<u8>>,
    pub(crate) after: Option<Vec<u8>>,
    pub(crate) schema_fingerprint: CommitSchemaFingerprint,
}

impl CommitRowOp {
    /// Construct a row-level commit operation.
    #[must_use]
    pub(crate) fn new(
        entity_path: impl Into<String>,
        key: Vec<u8>,
        before: Option<Vec<u8>>,
        after: Option<Vec<u8>>,
        schema_fingerprint: CommitSchemaFingerprint,
    ) -> Self {
        Self {
            entity_path: entity_path.into(),
            key,
            before,
            after,
            schema_fingerprint,
        }
    }
}

///
/// CommitIndexOp
///
/// Internal index mutation used during row-op preparation/apply.
/// Not persisted in commit markers.
///

#[derive(Clone, Debug)]
pub(crate) struct CommitIndexOp {
    pub(crate) store: String,
    pub(crate) key: Vec<u8>,
    pub(crate) value: Option<Vec<u8>>,
}

///
/// CommitMarker
///
/// Persisted mutation plan covering row-level operations.
/// Recovery replays the marker exactly as stored.
/// Unknown fields are rejected as corruption; commit markers are not forward-compatible.
/// This is internal commit-protocol metadata, not a user-schema type.
///

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommitMarker {
    pub(crate) id: [u8; COMMIT_ID_BYTES],
    pub(crate) row_ops: Vec<CommitRowOp>,
}

impl CommitMarker {
    /// Construct a new commit marker with a fresh commit id.
    pub(crate) fn new(row_ops: Vec<CommitRowOp>) -> Result<Self, InternalError> {
        let id = Ulid::try_generate()
            .map_err(|err| {
                InternalError::store_internal(format!("commit id generation failed: {err}"))
            })?
            .to_bytes();

        Ok(Self { id, row_ops })
    }
}

/// Decode a raw index key and validate its structural invariants.
pub(in crate::db) fn decode_index_key(bytes: &[u8]) -> Result<RawIndexKey, InternalError> {
    // Phase 1: enforce raw-size contract for encoded index keys.
    let len = bytes.len();
    let min = IndexKey::MIN_STORED_SIZE_USIZE;
    let max = IndexKey::STORED_SIZE_USIZE;
    if len < min || len > max {
        return Err(commit_component_corruption(
            "index key",
            format!("invalid length {len}, expected {min}..={max}"),
        ));
    }

    // Phase 2: decode and enforce index-key semantic shape.
    let raw = <RawIndexKey as Storable>::from_bytes(Cow::Borrowed(bytes));
    IndexKey::try_from_raw(&raw).map_err(|err| commit_component_corruption("index key", err))?;

    Ok(raw)
}

/// Decode a raw index entry and validate its structural invariants.
pub(in crate::db) fn decode_index_entry(bytes: &[u8]) -> Result<RawIndexEntry, InternalError> {
    // Phase 1: enforce entry-size upper bound.
    let len = bytes.len();
    let max = MAX_INDEX_ENTRY_BYTES as usize;
    if len > max {
        return Err(commit_component_corruption(
            "index entry",
            format!("invalid length {len}, expected <= {max}"),
        ));
    }

    // Phase 2: decode and validate entry envelope.
    let raw = <RawIndexEntry as Storable>::from_bytes(Cow::Borrowed(bytes));
    raw.validate()
        .map_err(|err| commit_component_corruption("index entry", err))?;

    Ok(raw)
}

/// Decode a raw data key and validate its structural invariants.
pub(in crate::db) fn decode_data_key(bytes: &[u8]) -> Result<(RawDataKey, DataKey), InternalError> {
    // Phase 1: enforce fixed-size contract for data keys.
    let len = bytes.len();
    let expected = DataKey::STORED_SIZE_USIZE;
    if len != expected {
        return Err(commit_component_corruption(
            "data key",
            format!("invalid length {len}, expected {expected}"),
        ));
    }

    // Phase 2: decode and validate key shape.
    let raw = <RawDataKey as Storable>::from_bytes(Cow::Borrowed(bytes));
    let data_key =
        DataKey::try_from_raw(&raw).map_err(|err| commit_component_corruption("data key", err))?;

    Ok((raw, data_key))
}

/// Compute one deterministic schema/index fingerprint for an entity commit planner.
#[must_use]
pub(in crate::db) fn commit_schema_fingerprint_for_entity<E: EntityKind>() -> CommitSchemaFingerprint
{
    // Phase 1: version the fingerprint contract and hash top-level identity.
    let mut hasher = Xxh3::with_seed(0);
    hasher.update(&[COMMIT_SCHEMA_FINGERPRINT_VERSION]);
    hash_labeled_str(&mut hasher, "entity_path", E::PATH);

    // Phase 2: hash the macro-generated entity schema contract consumed by
    // prepare/replay planning (field slot order + index definitions).
    hash_entity_model_for_commit(&mut hasher, E::MODEL);

    hasher.digest128().to_be_bytes()
}

fn hash_entity_model_for_commit(hasher: &mut Xxh3, model: &EntityModel) {
    // Phase 1: hash core entity identity and field-shape contract.
    hash_labeled_str(hasher, "model_path", model.path);
    hash_labeled_str(hasher, "entity_name", model.entity_name);
    hash_labeled_str(hasher, "primary_key", model.primary_key.name);
    hash_labeled_len(hasher, "field_count", model.fields.len());

    for field in model.fields {
        hash_labeled_str(hasher, "field_name", field.name);
    }

    // Phase 2: hash index contract details (names, stores, uniqueness, fields).
    hash_labeled_len(hasher, "index_count", model.indexes.len());
    for index in model.indexes {
        hash_labeled_str(hasher, "index_name", index.name);
        hash_labeled_str(hasher, "index_store", index.store);
        hasher.update(&[u8::from(index.unique)]);
        hash_labeled_len(hasher, "index_field_count", index.fields.len());
        for field in index.fields {
            hash_labeled_str(hasher, "index_field_name", field);
        }
    }
}

fn hash_labeled_str(hasher: &mut Xxh3, label: &str, value: &str) {
    hash_labeled_len(hasher, label, value.len());
    hasher.update(value.as_bytes());
}

fn hash_labeled_len(hasher: &mut Xxh3, label: &str, len: usize) {
    hasher.update(label.as_bytes());
    hasher.update(&u64::try_from(len).unwrap_or(u64::MAX).to_be_bytes());
}

/// Validate commit-marker row-op shape invariants.
///
/// Every row op must represent a concrete mutation:
/// - insert (`before=None`, `after=Some`)
/// - update (`before=Some`, `after=Some`)
/// - delete (`before=Some`, `after=None`)
///
/// The empty shape (`before=None`, `after=None`) is corruption.
pub(crate) fn validate_commit_marker_shape(marker: &CommitMarker) -> Result<(), InternalError> {
    // Phase 1: reject row ops that cannot encode any mutation semantics.
    for row_op in &marker.row_ops {
        if row_op.entity_path.is_empty() {
            return Err(InternalError::store_corruption(commit_corruption_message(
                "row op has empty entity_path",
            )));
        }
        if row_op.before.is_none() && row_op.after.is_none() {
            return Err(InternalError::store_corruption(commit_corruption_message(
                "row op has neither before nor after payload",
            )));
        }

        // Phase 2: guard row payload size at marker-decode boundary so recovery
        // does not classify oversized persisted bytes during apply preparation.
        for (label, payload) in [
            ("before", row_op.before.as_ref()),
            ("after", row_op.after.as_ref()),
        ] {
            if let Some(bytes) = payload
                && bytes.len() > MAX_ROW_BYTES as usize
            {
                return Err(InternalError::store_corruption(commit_corruption_message(
                    format!(
                        "row op {label} payload exceeds max size: {} bytes (limit {MAX_ROW_BYTES})",
                        bytes.len()
                    ),
                )));
            }
        }

        // Phase 3: enforce data-key byte shape and semantic decode.
        if row_op.key.len() != DataKey::STORED_SIZE_USIZE {
            return Err(InternalError::store_corruption(commit_corruption_message(
                format!(
                    "row op key has invalid length: {} bytes (expected {})",
                    row_op.key.len(),
                    DataKey::STORED_SIZE_USIZE
                ),
            )));
        }
        let raw_key = <RawDataKey as Storable>::from_bytes(Cow::Borrowed(row_op.key.as_slice()));
        DataKey::try_from_raw(&raw_key).map_err(|err| {
            InternalError::store_corruption(commit_corruption_message(format!(
                "row op key decode failed: {err}"
            )))
        })?;
    }

    Ok(())
}
