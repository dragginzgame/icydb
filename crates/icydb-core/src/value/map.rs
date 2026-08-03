//! Module: value::map
//!
//! Responsibility: canonical map normalization and validation for `Value::Map`.
//! Does not own: the `Value` enum shape or storage-level map encoding.
//! Boundary: deterministic map construction helpers shared by runtime surfaces.

use crate::value::Value;
use std::cmp::Ordering;
use std::fmt;

///
/// MapValueError
///
/// Reports invariant violations found while constructing or normalizing
/// `Value::Map` entries. The error carries normalized entry positions where
/// possible so callers can diagnose duplicate-key collisions deterministically.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MapValueError {
    EmptyKey {
        index: usize,
    },
    NonScalarKey {
        index: usize,
        key: Value,
    },
    NonScalarValue {
        index: usize,
        value: Value,
    },
    DuplicateKey {
        left_index: usize,
        right_index: usize,
    },
}

impl fmt::Display for MapValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("map value error")
    }
}

impl std::error::Error for MapValueError {}

///
/// SchemaInvariantError
///
/// Wraps schema/runtime materialization invariant failures that surface through
/// generic conversion traits. This keeps map-specific validation errors intact
/// while preserving the existing `TryFrom` error boundary for `Value`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchemaInvariantError {
    InvalidMapValue(MapValueError),
}

impl fmt::Display for SchemaInvariantError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("schema invariant error")
    }
}

impl std::error::Error for SchemaInvariantError {}

impl From<MapValueError> for SchemaInvariantError {
    fn from(value: MapValueError) -> Self {
        Self::InvalidMapValue(value)
    }
}

/// Validate map entry invariants without changing order.
fn validate_map_entries(entries: &[(Value, Value)]) -> Result<(), MapValueError> {
    for (index, (key, _value)) in entries.iter().enumerate() {
        if matches!(key, Value::Null) {
            return Err(MapValueError::EmptyKey { index });
        }
        if !key.is_scalar() {
            return Err(MapValueError::NonScalarKey {
                index,
                key: key.clone(),
            });
        }
    }

    Ok(())
}

// Compare two map entries by canonical key order.
fn compare_map_entry_keys(left: &(Value, Value), right: &(Value, Value)) -> Ordering {
    Value::canonical_cmp_key(&left.0, &right.0)
}

// Sort map entries in canonical key order without changing ownership.
fn sort_map_entries_in_place(entries: &mut [(Value, Value)]) {
    entries.sort_unstable_by(compare_map_entry_keys);
}

/// Normalize map entries into canonical deterministic order.
pub(super) fn normalize_map_entries(
    mut entries: Vec<(Value, Value)>,
) -> Result<Vec<(Value, Value)>, MapValueError> {
    validate_map_entries(&entries)?;
    sort_map_entries_in_place(entries.as_mut_slice());

    for i in 1..entries.len() {
        let (left_key, _) = &entries[i - 1];
        let (right_key, _) = &entries[i];
        if Value::canonical_cmp_key(left_key, right_key) == Ordering::Equal {
            return Err(MapValueError::DuplicateKey {
                left_index: i - 1,
                right_index: i,
            });
        }
    }

    Ok(entries)
}

impl Value {
    /// Validate map entry invariants without changing order.
    pub fn validate_map_entries(entries: &[(Self, Self)]) -> Result<(), MapValueError> {
        validate_map_entries(entries)
    }

    /// Normalize map entries into canonical deterministic order.
    pub fn normalize_map_entries(
        entries: Vec<(Self, Self)>,
    ) -> Result<Vec<(Self, Self)>, MapValueError> {
        normalize_map_entries(entries)
    }
}
