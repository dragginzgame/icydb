//! Raw decoding helpers for commit marker payloads.

use crate::{
    db::{
        index::{IndexKey, MAX_INDEX_ENTRY_BYTES, RawIndexEntry, RawIndexKey},
        store::{DataKey, RawDataKey},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
};
use canic_cdk::structures::Storable;
use std::borrow::Cow;

/// Decode a raw index key and validate its structural invariants.
pub(super) fn decode_index_key(bytes: &[u8]) -> Result<RawIndexKey, InternalError> {
    if bytes.len() < IndexKey::MIN_STORED_SIZE_USIZE || bytes.len() > IndexKey::STORED_SIZE_USIZE {
        return Err(InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Index,
            "commit marker index key has invalid length",
        ));
    }

    let raw = <RawIndexKey as Storable>::from_bytes(Cow::Borrowed(bytes));
    IndexKey::try_from_raw(&raw).map_err(|err| {
        InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Index,
            format!("commit marker index key corrupted: {err}"),
        )
    })?;

    Ok(raw)
}

/// Decode a raw index entry and validate its structural invariants.
pub(super) fn decode_index_entry(bytes: &[u8]) -> Result<RawIndexEntry, InternalError> {
    if bytes.len() > MAX_INDEX_ENTRY_BYTES as usize {
        return Err(InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Index,
            "commit marker index entry exceeds max size",
        ));
    }

    let raw = <RawIndexEntry as Storable>::from_bytes(Cow::Borrowed(bytes));
    raw.validate().map_err(|err| {
        InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Index,
            format!("commit marker index entry corrupted: {err}"),
        )
    })?;

    Ok(raw)
}

/// Decode a raw data key and validate its structural invariants.
pub(super) fn decode_data_key(bytes: &[u8]) -> Result<RawDataKey, InternalError> {
    if bytes.len() != DataKey::STORED_SIZE_USIZE {
        return Err(InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Store,
            "commit marker data key has invalid length",
        ));
    }

    let raw = <RawDataKey as Storable>::from_bytes(Cow::Borrowed(bytes));
    DataKey::try_from_raw(&raw).map_err(|err| {
        InternalError::new(
            ErrorClass::Corruption,
            ErrorOrigin::Store,
            format!("commit marker data key corrupted: {err}"),
        )
    })?;

    Ok(raw)
}
