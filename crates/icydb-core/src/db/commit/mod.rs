//! IcyDB commit protocol and atomicity guardrails.
//!
//! Contract:
//! - `begin_commit` persists a marker that fully describes durable mutations.
//! - Durable correctness is owned by marker replay in recovery (row ops).
//! - In-process apply guards are best-effort cleanup only and are not authoritative.
//!
//! ## Commit Boundary and Authority of CommitMarker
//!
//! The `CommitMarker` fully specifies every row mutation. After
//! the marker is persisted, executors must not re-derive semantics or branch
//! on entity/index contents; apply logic deterministically replays row ops.
//! Recovery replays row ops as recorded, not planner logic.

mod decode;
mod guard;
mod memory;
mod recovery;
mod store;
#[cfg(test)]
mod tests;

use crate::{
    db::{
        Db,
        commit::{
            decode::{decode_data_key, decode_index_entry, decode_index_key},
            store::{CommitStore, with_commit_store, with_commit_store_infallible},
        },
        index::{IndexKey, RawIndexEntry, RawIndexKey, plan::plan_index_mutation_for_entity},
        store::{DataKey, DataStore, RawDataKey, RawRow},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::{EntityKind, EntityValue, Path},
    types::Ulid,
};
#[cfg(test)]
use canic_memory::{
    registry::{MemoryRegistry, MemoryRegistryError},
    runtime::registry::MemoryRegistryRuntime,
};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::collections::BTreeSet;
use std::{cell::RefCell, collections::BTreeMap, thread::LocalKey};

pub use guard::CommitApplyGuard;
pub use recovery::{ensure_recovered, ensure_recovered_for_write};

#[cfg(test)]
/// Return true if a commit marker is currently persisted.
pub fn commit_marker_present() -> Result<bool, InternalError> {
    store::commit_marker_present()
}

#[cfg(test)]
/// Initialize commit marker storage for tests.
///
/// This registers a placeholder data-store entry if none exists so the commit
/// memory allocator can select the correct reserved range.
pub fn init_commit_store_for_tests() -> Result<(), InternalError> {
    // Phase 1: ensure the memory registry has at least one reserved range.
    let init_result = MemoryRegistryRuntime::init(Some(("icydb_test", 1, 200)));
    match init_result {
        Ok(_) => {}
        Err(MemoryRegistryError::Overlap { .. }) => {
            MemoryRegistryRuntime::init(None).map_err(|err| {
                InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!("memory registry init failed: {err}"),
                )
            })?;
        }
        Err(err) => {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Store,
                format!("memory registry init failed: {err}"),
            ));
        }
    }

    // Phase 2: ensure a DB-store entry exists so commit memory can be allocated.
    let snapshots = MemoryRegistryRuntime::snapshot_ids_by_range();
    if snapshots.is_empty() {
        return Err(InternalError::new(
            ErrorClass::Internal,
            ErrorOrigin::Store,
            "no memory ranges available for commit marker tests",
        ));
    }
    let has_store_entry = snapshots.iter().any(|snapshot| {
        snapshot.entries.iter().any(|(_, entry)| {
            entry.label.ends_with("DataStore") || entry.label.ends_with("IndexStore")
        })
    });

    if !has_store_entry {
        let snapshot = snapshots.first().ok_or_else(|| {
            InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Store,
                "no memory ranges available for commit marker tests",
            )
        })?;
        let used_ids = snapshot
            .entries
            .iter()
            .map(|(id, _)| *id)
            .collect::<BTreeSet<_>>();
        let dummy_id = (snapshot.range.start..=snapshot.range.end)
            .find(|id| !used_ids.contains(id))
            .ok_or_else(|| {
                InternalError::new(
                    ErrorClass::Unsupported,
                    ErrorOrigin::Store,
                    format!(
                        "no free memory ids available for commit marker tests in range {}-{}",
                        snapshot.range.start, snapshot.range.end
                    ),
                )
            })?;

        MemoryRegistry::register(dummy_id, &snapshot.owner, "commit_test::DataStore").map_err(
            |err| {
                InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!("commit test memory registration failed: {err}"),
                )
            },
        )?;
    }

    // Phase 3: initialize the commit store in the production slot.

    with_commit_store(|_| Ok(()))
}

// Stage-2 invariant:
// - We persist a commit marker before any stable mutation.
// - After marker creation, executor apply phases are infallible or trap.
// - Recovery replays the stored row mutation plan.
// This makes partial mutations deterministic without a WAL.

const COMMIT_LABEL: &str = "CommitMarker";
const COMMIT_ID_BYTES: usize = 16;

// Conservative upper bound to avoid rejecting valid commits when index entries
// are large; still small enough to fit typical canister constraints.
pub const MAX_COMMIT_BYTES: u32 = 16 * 1024 * 1024;

///
/// CommitRowOp
///
/// Row-level mutation recorded in a commit marker.
/// Store identity is derived from `entity_path` at apply/recovery time.
///

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommitRowOp {
    pub entity_path: String,
    pub key: Vec<u8>,
    pub before: Option<Vec<u8>>,
    pub after: Option<Vec<u8>>,
}

impl CommitRowOp {
    /// Construct a row-level commit operation.
    #[must_use]
    pub fn new(
        entity_path: impl Into<String>,
        key: Vec<u8>,
        before: Option<Vec<u8>>,
        after: Option<Vec<u8>>,
    ) -> Self {
        Self {
            entity_path: entity_path.into(),
            key,
            before,
            after,
        }
    }
}

///
/// CommitIndexOp
///
/// Internal index mutation used during row-op preparation/apply.
/// Not persisted in commit markers.

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommitIndexOp {
    pub store: String,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

///
/// CommitDataOp
///
/// Internal data-store mutation used during row-op preparation/apply.
/// Not persisted in commit markers.

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommitDataOp {
    pub store: String,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

///
/// CommitMarker
///
/// Persisted mutation plan covering row-level operations.
/// Recovery replays the marker exactly as stored.
/// Unknown fields are rejected as corruption; commit markers are not forward-compatible.
/// This is internal commit-protocol metadata, not a user-schema type.

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommitMarker {
    pub id: [u8; COMMIT_ID_BYTES],
    pub row_ops: Vec<CommitRowOp>,
}

impl CommitMarker {
    /// Construct a new commit marker with a fresh commit id.
    pub fn new(row_ops: Vec<CommitRowOp>) -> Result<Self, InternalError> {
        let id = Ulid::try_generate()
            .map_err(|err| {
                InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!("commit id generation failed: {err}"),
                )
            })?
            .to_bytes();

        Ok(Self { id, row_ops })
    }
}

///
/// PreparedIndexMutation
///
/// Mechanical index mutation derived from a row op.
///

#[derive(Clone)]
pub struct PreparedIndexMutation {
    pub store: &'static LocalKey<RefCell<crate::db::index::IndexStore>>,
    pub key: RawIndexKey,
    pub value: Option<RawIndexEntry>,
}

///
/// PreparedRowCommitOp
///
/// Mechanical store mutation derived from one row op.
///

#[derive(Clone)]
pub struct PreparedRowCommitOp {
    pub index_ops: Vec<PreparedIndexMutation>,
    pub data_store: &'static LocalKey<RefCell<DataStore>>,
    pub data_key: RawDataKey,
    pub data_value: Option<RawRow>,
    pub index_remove_count: usize,
    pub index_insert_count: usize,
}

impl PreparedRowCommitOp {
    /// Apply the prepared row operation infallibly.
    pub fn apply(self) {
        for index_op in self.index_ops {
            index_op.store.with_borrow_mut(|store| {
                if let Some(value) = index_op.value {
                    store.insert(index_op.key, value);
                } else {
                    store.remove(&index_op.key);
                }
            });
        }

        self.data_store.with_borrow_mut(|store| {
            if let Some(value) = self.data_value {
                store.insert(self.data_key, value);
            } else {
                store.remove(&self.data_key);
            }
        });
    }
}

/// Capture the current store state needed to roll back one prepared row op.
///
/// The returned op writes the prior index/data values back when applied.
#[must_use]
pub fn snapshot_row_rollback(op: &PreparedRowCommitOp) -> PreparedRowCommitOp {
    let mut index_ops = Vec::with_capacity(op.index_ops.len());
    for index_op in &op.index_ops {
        let existing = index_op.store.with_borrow(|store| store.get(&index_op.key));
        index_ops.push(PreparedIndexMutation {
            store: index_op.store,
            key: index_op.key,
            value: existing,
        });
    }

    let data_value = op.data_store.with_borrow(|store| store.get(&op.data_key));

    PreparedRowCommitOp {
        index_ops,
        data_store: op.data_store,
        data_key: op.data_key,
        data_value,
        index_remove_count: 0,
        index_insert_count: 0,
    }
}

/// Prepare a typed row-level commit op for one entity type.
///
/// This resolves store handles and index/data mutations so commit/recovery
/// apply can remain mechanical.
#[expect(clippy::too_many_lines)]
pub fn prepare_row_commit_for_entity<E: EntityKind + EntityValue>(
    db: &Db<E::Canister>,
    op: &CommitRowOp,
) -> Result<PreparedRowCommitOp, InternalError> {
    if op.entity_path != E::PATH {
        return Err(InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Store,
            format!(
                "commit marker entity path mismatch: expected '{}', found '{}'",
                E::PATH,
                op.entity_path
            ),
        ));
    }

    let raw_key = decode_data_key(&op.key)?;
    let data_key = DataKey::try_from_raw(&raw_key).map_err(|err| {
        InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Store,
            format!("commit marker data key corrupted: {err}"),
        )
    })?;
    data_key.try_key::<E>()?;

    let decode_entity = |bytes: &[u8], label: &str| -> Result<(RawRow, E), InternalError> {
        let row = RawRow::try_new(bytes.to_vec())?;
        let entity = row.try_decode::<E>().map_err(|err| {
            InternalError::new(
                ErrorClass::Corruption,
                ErrorOrigin::Serialize,
                format!("commit marker {label} row decode failed: {err}"),
            )
        })?;
        let expected = data_key.try_key::<E>()?;
        let actual = entity.id().key();
        if expected != actual {
            return Err(InternalError::new(
                ErrorClass::Corruption,
                ErrorOrigin::Store,
                format!("commit marker row key mismatch: expected {expected:?}, found {actual:?}"),
            ));
        }

        Ok((row, entity))
    };

    let old_pair = op
        .before
        .as_ref()
        .map(|bytes| decode_entity(bytes, "before"))
        .transpose()?;
    let new_pair = op
        .after
        .as_ref()
        .map(|bytes| decode_entity(bytes, "after"))
        .transpose()?;

    if old_pair.is_none() && new_pair.is_none() {
        return Err(InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Store,
            "commit marker row op is a no-op (before/after both missing)",
        ));
    }

    let index_plan = plan_index_mutation_for_entity::<E>(
        db,
        old_pair.as_ref().map(|(_, entity)| entity),
        new_pair.as_ref().map(|(_, entity)| entity),
    )?;
    let mut index_remove_count = 0usize;
    let mut index_insert_count = 0usize;
    for index in E::INDEXES {
        let old_key = old_pair
            .as_ref()
            .map(|(_, old_entity)| IndexKey::new(old_entity, index))
            .transpose()?
            .flatten()
            .map(|key| key.to_raw());
        let new_key = new_pair
            .as_ref()
            .map(|(_, new_entity)| IndexKey::new(new_entity, index))
            .transpose()?
            .flatten()
            .map(|key| key.to_raw());

        if old_key != new_key {
            if old_key.is_some() {
                index_remove_count = index_remove_count.saturating_add(1);
            }
            if new_key.is_some() {
                index_insert_count = index_insert_count.saturating_add(1);
            }
        }
    }
    let mut index_stores = BTreeMap::new();
    for apply in &index_plan.apply {
        index_stores.insert(apply.index.store, apply.store);
    }

    let mut index_ops = Vec::with_capacity(index_plan.commit_ops.len());
    for index_op in index_plan.commit_ops {
        let store = index_stores
            .get(index_op.store.as_str())
            .copied()
            .ok_or_else(|| {
                InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Index,
                    format!(
                        "missing index store '{}' for entity '{}'",
                        index_op.store,
                        E::PATH
                    ),
                )
            })?;
        let key = decode_index_key(&index_op.key)?;
        let value = index_op
            .value
            .as_ref()
            .map(|bytes| decode_index_entry(bytes))
            .transpose()?;
        index_ops.push(PreparedIndexMutation { store, key, value });
    }

    let data_store = db.with_store_registry(|reg| reg.try_get_store(E::Store::PATH))?;
    let data_value = new_pair.map(|(row, _)| row);

    Ok(PreparedRowCommitOp {
        index_ops,
        data_store: data_store.data_store(),
        data_key: raw_key,
        data_value,
        index_remove_count,
        index_insert_count,
    })
}

///
/// CommitGuard
///
/// In-flight commit handle that clears the marker on completion.
/// Must not be leaked across mutation boundaries.
///

#[derive(Clone, Debug)]
pub struct CommitGuard {
    pub marker: CommitMarker,
}

impl CommitGuard {
    // Clear the commit marker without surfacing errors.
    fn clear(self) {
        let _ = self;
        with_commit_store_infallible(CommitStore::clear_infallible);
    }
}

/// Persist a commit marker and open the commit window.
pub fn begin_commit(marker: CommitMarker) -> Result<CommitGuard, InternalError> {
    with_commit_store(|store| {
        if store.load()?.is_some() {
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Store,
                "commit marker already present before begin",
            ));
        }
        store.set(&marker)?;

        Ok(CommitGuard { marker })
    })
}

/// Apply commit ops and clear the marker regardless of outcome.
///
/// The apply closure performs mechanical marker application only.
/// Any in-process rollback guard used by the closure is non-authoritative
/// transitional cleanup; durable authority remains the commit marker protocol.
pub fn finish_commit(
    mut guard: CommitGuard,
    apply: impl FnOnce(&mut CommitGuard) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    // COMMIT WINDOW:
    // Apply mutates stores from a prevalidated marker payload.
    // Marker durability + recovery replay remain the atomicity authority.
    // We clear the marker on any outcome so recovery does not reapply an
    // already-attempted marker in this process.
    let result = apply(&mut guard);
    let commit_id = guard.marker.id;
    guard.clear();
    // Internal invariant: commit markers must not persist after a finished mutation.
    assert!(
        with_commit_store_infallible(|store| store.is_empty()),
        "commit marker must be cleared after finish_commit (commit_id={commit_id:?})"
    );
    result
}
