//! System-level commit recovery.
//!
//! This module implements a **system recovery step** that restores global
//! database invariants by completing or rolling back a previously started
//! commit before any new operation proceeds.
//!
//! Important semantic notes:
//! - Recovery runs once at startup for read paths.
//! - Write paths perform a cheap marker check and replay if needed.
//! - Reads may observe partial commit state if a trap occurs after startup.
//!
//! Invocation from read or mutation entrypoints is permitted only as an
//! unconditional invariant-restoration step. Recovery must not be
//! interleaved with read logic or mutation planning/apply phases.

use crate::{
    db::{
        Db,
        commit::{
            CommitKind, CommitMarker,
            decode::{decode_data_key, decode_index_entry, decode_index_key},
            store::{commit_marker_present_fast, with_commit_store},
        },
        index::RawIndexEntry,
        store::{DataStore, RawDataKey, RawRow},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
};
use std::{cell::RefCell, sync::OnceLock, thread::LocalKey};

static RECOVERED: OnceLock<()> = OnceLock::new();

#[cfg(test)]
thread_local! {
    static FORCE_RECOVERY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
#[expect(dead_code)]
/// Force the system recovery step to run once on the next call to
/// `ensure_recovered`.
pub fn force_recovery_for_tests() {
    FORCE_RECOVERY.with(|flag| flag.set(true));
}

#[allow(clippy::missing_const_for_fn)]
// Test hook to force a one-shot system recovery run.
fn should_force_recovery() -> bool {
    #[cfg(test)]
    {
        FORCE_RECOVERY.with(|flag| {
            let force = flag.get();
            if force {
                flag.set(false);
            }
            force
        })
    }

    #[cfg(not(test))]
    {
        false
    }
}

/// Ensure global database invariants are restored before proceeding.
///
/// This function performs a **system recovery step**:
/// - It completes or rolls back any previously started commit.
/// - It leaves the database in a fully consistent state on return.
///
/// This function is:
/// - **Not part of mutation atomicity**
/// - **Mandatory before read execution at startup**
/// - **Not conditional on read semantics**
///
/// It may be invoked at operation boundaries (including read or mutation
/// entrypoints), but must always complete **before** any operation-specific
/// planning, validation, or apply phase begins.
pub fn ensure_recovered(db: &Db<impl crate::traits::CanisterKind>) -> Result<(), InternalError> {
    let force = should_force_recovery();
    if !force && RECOVERED.get().is_some() {
        return Ok(());
    }

    perform_recovery(db)
}

/// Ensure recovery has been performed before any write operation proceeds.
///
/// Hybrid model:
/// - Startup recovery runs once.
/// - Writes perform a fast marker check and replay if a marker is present.
///
/// Recovery must be idempotent and safe to run multiple times.
/// All mutation entrypoints must call this before any commit boundary work.
pub fn ensure_recovered_for_write(
    db: &Db<impl crate::traits::CanisterKind>,
) -> Result<(), InternalError> {
    let force = should_force_recovery();
    if force {
        return perform_recovery(db);
    }

    if RECOVERED.get().is_none() {
        return perform_recovery(db);
    }

    if commit_marker_present_fast()? {
        return perform_recovery(db);
    }

    Ok(())
}

fn perform_recovery(db: &Db<impl crate::traits::CanisterKind>) -> Result<(), InternalError> {
    let marker = with_commit_store(|store| store.load())?;
    if let Some(marker) = marker {
        let (index_ops, data_ops) = prevalidate_recovery(db, &marker)?;
        apply_recovery_ops(index_ops, data_ops);
        with_commit_store(|store| {
            store.clear_infallible();
            Ok(())
        })?;
    }

    let _ = RECOVERED.set(());

    Ok(())
}

///
/// DecodedIndexOp
///

struct DecodedIndexOp {
    store: &'static LocalKey<RefCell<crate::db::index::IndexStore>>,
    key: crate::db::index::RawIndexKey,
    value: Option<RawIndexEntry>,
}

///
/// DecodedDataOp
///

struct DecodedDataOp {
    store: &'static LocalKey<RefCell<DataStore>>,
    key: RawDataKey,
    value: Option<RawRow>,
}

/// Validate commit marker payloads and decode recovery ops.
///
/// All validation and decoding is performed **before** any recovery mutation
/// is applied, ensuring the recovery apply phase is mechanical and infallible.
fn prevalidate_recovery(
    db: &Db<impl crate::traits::CanisterKind>,
    marker: &CommitMarker,
) -> Result<(Vec<DecodedIndexOp>, Vec<DecodedDataOp>), InternalError> {
    match marker.kind {
        CommitKind::Save => {
            if marker.data_ops.iter().any(|op| op.value.is_none()) {
                return Err(InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Store,
                    "commit marker corrupted: save op missing data payload",
                ));
            }
        }
        CommitKind::Delete => {
            if marker.data_ops.iter().any(|op| op.value.is_some()) {
                return Err(InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Store,
                    "commit marker corrupted: delete op includes data payload",
                ));
            }
        }
    }

    let mut decoded_index = Vec::with_capacity(marker.index_ops.len());
    let mut decoded_data = Vec::with_capacity(marker.data_ops.len());

    for op in &marker.index_ops {
        let store = db
            .with_index(|reg| reg.try_get_store(&op.store))
            .map_err(|err| {
                InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Index,
                    format!("missing index store '{}': {err}", op.store),
                )
            })?;
        let key = decode_index_key(&op.key)?;
        let value = match &op.value {
            Some(bytes) => Some(decode_index_entry(bytes)?),
            None => None,
        };
        decoded_index.push(DecodedIndexOp { store, key, value });
    }

    for op in &marker.data_ops {
        let store = db
            .with_data(|reg| reg.try_get_store(&op.store))
            .map_err(|err| {
                InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Store,
                    format!("missing data store '{}': {err}", op.store),
                )
            })?;
        let key = decode_data_key(&op.key)?;
        let value = match &op.value {
            Some(bytes) => Some(RawRow::try_new(bytes.clone())?),
            None => None,
        };
        decoded_data.push(DecodedDataOp { store, key, value });
    }

    Ok((decoded_index, decoded_data))
}

/// Apply decoded recovery ops.
///
/// Index operations are applied first, followed by data operations,
/// mirroring executor commit ordering. This function performs only
/// mechanical store mutations and must not fail.
fn apply_recovery_ops(index_ops: Vec<DecodedIndexOp>, data_ops: Vec<DecodedDataOp>) {
    for op in index_ops {
        op.store.with_borrow_mut(|store| {
            if let Some(value) = op.value {
                store.insert(op.key, value);
            } else {
                store.remove(&op.key);
            }
        });
    }

    for op in data_ops {
        op.store.with_borrow_mut(|store| {
            if let Some(value) = op.value {
                store.insert(op.key, value);
            } else {
                store.remove(&op.key);
            }
        });
    }
}
