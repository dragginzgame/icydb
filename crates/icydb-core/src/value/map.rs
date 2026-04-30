//! Module: value::map
//!
//! Responsibility: canonical map normalization and validation for `Value::Map`.
//! Does not own: the `Value` enum shape or storage-level map encoding.
//! Boundary: deterministic map construction helpers shared by runtime surfaces.

use crate::value::Value;
use std::cmp::Ordering;

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

impl std::fmt::Display for MapValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyKey { index } => write!(f, "map key at index {index} must be non-null"),
            Self::NonScalarKey { index, key } => {
                write!(f, "map key at index {index} is not scalar: {key:?}")
            }
            Self::NonScalarValue { index, value } => {
                write!(
                    f,
                    "map value at index {index} is not scalar/ref-like: {value:?}"
                )
            }
            Self::DuplicateKey {
                left_index,
                right_index,
            } => write!(
                f,
                "map contains duplicate keys at normalized positions {left_index} and {right_index}"
            ),
        }
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

impl std::fmt::Display for SchemaInvariantError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidMapValue(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for SchemaInvariantError {}

impl From<MapValueError> for SchemaInvariantError {
    fn from(value: MapValueError) -> Self {
        Self::InvalidMapValue(value)
    }
}

/// Validate map entry invariants without changing order.
pub fn validate_map_entries(entries: &[(Value, Value)]) -> Result<(), MapValueError> {
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
pub(crate) fn compare_map_entry_keys(left: &(Value, Value), right: &(Value, Value)) -> Ordering {
    Value::canonical_cmp_key(&left.0, &right.0)
}

// Sort map entries in canonical key order without changing ownership.
pub(crate) fn sort_map_entries_in_place(entries: &mut [(Value, Value)]) {
    entries.sort_by(compare_map_entry_keys);
}

// Return `true` when map entries are already in strict canonical order and
// therefore contain no duplicate canonical keys.
pub(crate) fn map_entries_are_strictly_canonical(entries: &[(Value, Value)]) -> bool {
    entries.windows(2).all(|pair| {
        let [left, right] = pair else {
            return true;
        };

        compare_map_entry_keys(left, right) == Ordering::Less
    })
}

/// Normalize map entries into canonical deterministic order.
pub fn normalize_map_entries(
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

    // Sort map entries in canonical key order without changing ownership.
    pub(crate) fn sort_map_entries_in_place(entries: &mut [(Self, Self)]) {
        sort_map_entries_in_place(entries);
    }

    // Return `true` when map entries are already in strict canonical order and
    // therefore contain no duplicate canonical keys.
    pub(crate) fn map_entries_are_strictly_canonical(entries: &[(Self, Self)]) -> bool {
        map_entries_are_strictly_canonical(entries)
    }

    /// Normalize map entries into canonical deterministic order.
    pub fn normalize_map_entries(
        entries: Vec<(Self, Self)>,
    ) -> Result<Vec<(Self, Self)>, MapValueError> {
        normalize_map_entries(entries)
    }
}
