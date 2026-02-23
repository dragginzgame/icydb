//! Raw decoding helpers for commit marker payloads.

use crate::{
    db::{
        commit::commit_component_corruption_message,
        data::{DataKey, RawDataKey},
        index::{IndexKey, MAX_INDEX_ENTRY_BYTES, RawIndexEntry, RawIndexKey},
    },
    error::InternalError,
};
use canic_cdk::structures::Storable;
use std::borrow::Cow;

/// Decode a raw index key and validate its structural invariants.
pub(super) fn decode_index_key(bytes: &[u8]) -> Result<RawIndexKey, InternalError> {
    let len = bytes.len();
    let min = IndexKey::MIN_STORED_SIZE_USIZE;
    let max = IndexKey::STORED_SIZE_USIZE;
    if len < min || len > max {
        return Err(InternalError::index_corruption(
            commit_component_corruption_message(
                "index key",
                format!("invalid length {len}, expected {min}..={max}"),
            ),
        ));
    }

    let raw = <RawIndexKey as Storable>::from_bytes(Cow::Borrowed(bytes));
    IndexKey::try_from_raw(&raw).map_err(|err| {
        InternalError::index_corruption(commit_component_corruption_message("index key", err))
    })?;

    Ok(raw)
}

/// Decode a raw index entry and validate its structural invariants.
pub(super) fn decode_index_entry(bytes: &[u8]) -> Result<RawIndexEntry, InternalError> {
    let len = bytes.len();
    let max = MAX_INDEX_ENTRY_BYTES as usize;
    if len > max {
        return Err(InternalError::index_corruption(
            commit_component_corruption_message(
                "index entry",
                format!("invalid length {len}, expected <= {max}"),
            ),
        ));
    }

    let raw = <RawIndexEntry as Storable>::from_bytes(Cow::Borrowed(bytes));
    raw.validate().map_err(|err| {
        InternalError::index_corruption(commit_component_corruption_message("index entry", err))
    })?;

    Ok(raw)
}

/// Decode a raw data key and validate its structural invariants.
pub(super) fn decode_data_key(bytes: &[u8]) -> Result<(RawDataKey, DataKey), InternalError> {
    let len = bytes.len();
    let expected = DataKey::STORED_SIZE_USIZE;
    if len != expected {
        return Err(InternalError::store_corruption(
            commit_component_corruption_message(
                "data key",
                format!("invalid length {len}, expected {expected}"),
            ),
        ));
    }

    let raw = <RawDataKey as Storable>::from_bytes(Cow::Borrowed(bytes));
    let data_key = DataKey::try_from_raw(&raw).map_err(|err| {
        InternalError::store_corruption(commit_component_corruption_message("data key", err))
    })?;

    Ok((raw, data_key))
}
