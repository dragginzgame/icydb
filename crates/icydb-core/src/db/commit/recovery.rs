//! Commit marker recovery.

use crate::{
    db::{
        Db,
        commit::{
            CommitKind, CommitMarker,
            decode::{decode_data_key, decode_index_entry, decode_index_key},
            store::with_commit_store,
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
/// Force recovery to run once on the next call to `ensure_recovered`.
pub fn force_recovery_for_tests() {
    FORCE_RECOVERY.with(|flag| flag.set(true));
}

#[allow(clippy::missing_const_for_fn)]
// Test hook to force a one-shot recovery run.
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

/// Ensure recovery has been applied before exposing any reads or mutations.
///
/// Recovery is invoked from read and mutation entrypoints to prevent
/// observing partial commit state.
pub fn ensure_recovered(db: &Db<impl crate::traits::CanisterKind>) -> Result<(), InternalError> {
    let force = should_force_recovery();
    if !force && RECOVERED.get().is_some() {
        return Ok(());
    }

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

// Validate commit marker payloads and decode ops before applying recovery.
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

// Apply decoded index ops, then data ops, mirroring executor ordering.
fn apply_recovery_ops(index_ops: Vec<DecodedIndexOp>, data_ops: Vec<DecodedDataOp>) {
    // Apply indexes first, then data, mirroring executor ordering.
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
