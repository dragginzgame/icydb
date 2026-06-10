//! Module: executor::group::key
//! Responsibility: canonical grouped/distinct key materialization and set semantics.
//! Does not own: aggregation fold logic or planner-level grouped query validation.
//! Boundary: canonical equality/hash substrate for grouped execution.

use crate::{
    db::executor::group::{StableHash, stable_hash_value},
    error::InternalError,
    value::{MapValueError, Value},
};
use std::{
    collections::HashSet,
    fmt,
    hash::{Hash, Hasher},
};

///
/// KeyCanonicalError
///
/// KeyCanonicalError reports canonicalization failures while materializing one
/// grouping/distinct key from a runtime value.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum KeyCanonicalError {
    InvalidMapValue(MapValueError),
    HashingFailed { reason: String },
}

impl KeyCanonicalError {
    // Build the canonical grouped-key invariant for invalid map payloads.
    fn invalid_map_value(_err: &MapValueError) -> InternalError {
        InternalError::executor_invariant()
    }

    /// Convert one key-canonicalization failure into the executor error surface.
    pub(in crate::db::executor) fn into_internal_error(self) -> InternalError {
        match self {
            Self::InvalidMapValue(err) => Self::invalid_map_value(&err),
            Self::HashingFailed { reason: _ } => InternalError::executor_internal(),
        }
    }
}

impl fmt::Display for KeyCanonicalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMapValue(err) => write!(f, "{err}"),
            Self::HashingFailed { reason } => write!(f, "{reason}"),
        }
    }
}

impl std::error::Error for KeyCanonicalError {}

///
/// CanonicalValue
///
/// CanonicalValue wraps one recursively normalized value used by grouping and
/// distinct semantics.
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct CanonicalValue(Value);

///
/// GroupKey
///
/// GroupKey is the canonical equality/hash substrate for grouping and distinct
/// execution paths.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct GroupKey {
    raw: CanonicalValue,
    hash: StableHash,
}

impl Hash for GroupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Group keys cache one stable canonical hash so hash-table probes do
        // not need to rehash the full canonical `Value` tree in hot paths.
        state.write_u64(self.hash);
    }
}

/// Compare two grouped keys with canonical grouped-equality semantics.
#[cfg(test)]
#[must_use]
fn canonical_group_key_equals(left: &GroupKey, right: &GroupKey) -> bool {
    left == right
}

impl GroupKey {
    fn from_raw(raw: Value) -> Result<Self, KeyCanonicalError> {
        let hash = stable_hash_value(&raw).map_err(|err| KeyCanonicalError::HashingFailed {
            reason: err.display_with_class(),
        })?;

        Ok(Self::from_raw_with_hash(raw, hash))
    }

    // Materialize one grouped key from an already-canonical value plus one
    // caller-proven stable hash so borrowed grouped fold paths do not rehash
    // the same canonical list during owned key admission.
    const fn from_raw_with_hash(raw: Value, hash: StableHash) -> Self {
        Self {
            raw: CanonicalValue(raw),
            hash,
        }
    }

    #[must_use]
    pub(in crate::db::executor) const fn hash(&self) -> StableHash {
        self.hash
    }

    #[must_use]
    pub(in crate::db::executor) const fn canonical_value(&self) -> &Value {
        &self.raw.0
    }

    // Consume one grouped key and return the owned canonical grouped value
    // so grouped fast paths can keep moving owned key payloads without clones.
    pub(in crate::db::executor) fn into_canonical_value(self) -> Value {
        self.raw.0
    }

    // Materialize one grouped key from owned grouped slot values without
    // cloning them back through the borrowed canonicalization path.
    pub(in crate::db::executor) fn from_group_values(
        group_values: Vec<Value>,
    ) -> Result<Self, KeyCanonicalError> {
        let canonical = canonicalize_owned_value(Value::List(group_values))?;

        Self::from_raw(canonical)
    }

    // Materialize one grouped key from owned grouped slot values while
    // reusing one caller-proven canonical stable hash from the borrowed fold
    // path instead of hashing the same canonical list twice.
    pub(in crate::db::executor) fn from_group_values_with_hash(
        group_values: Vec<Value>,
        hash: StableHash,
    ) -> Result<Self, KeyCanonicalError> {
        let canonical = canonicalize_owned_value(Value::List(group_values))?;

        Ok(Self::from_raw_with_hash(canonical, hash))
    }

    // Materialize one single-field grouped key without first building an
    // intermediate one-element `Vec<Value>` only to wrap it back into a list.
    pub(in crate::db::executor) fn from_single_group_value(
        group_value: Value,
    ) -> Result<Self, KeyCanonicalError> {
        let canonical_group_value = canonicalize_owned_value(group_value)?;

        Self::from_raw(Value::List(vec![canonical_group_value]))
    }

    // Materialize one single-field grouped key while reusing one caller-proven
    // canonical stable hash from the borrowed grouped fold path.
    pub(in crate::db::executor) fn from_single_group_value_with_hash(
        group_value: Value,
        hash: StableHash,
    ) -> Result<Self, KeyCanonicalError> {
        let canonical_group_value = canonicalize_owned_value(group_value)?;

        Ok(Self::from_raw_with_hash(
            Value::List(vec![canonical_group_value]),
            hash,
        ))
    }

    // Materialize one single-field grouped key when the caller already proved
    // the grouped value is in canonical grouped-equality form.
    pub(in crate::db::executor) fn from_single_canonical_group_value(
        group_value: Value,
    ) -> Result<Self, KeyCanonicalError> {
        Self::from_raw(Value::List(vec![group_value]))
    }

    // Materialize one single-field grouped key when the caller already proved
    // the grouped value is canonical and already carries the matching stable hash.
    pub(in crate::db::executor) fn from_single_canonical_group_value_with_hash(
        group_value: Value,
        hash: StableHash,
    ) -> Self {
        Self::from_raw_with_hash(Value::List(vec![group_value]), hash)
    }

    #[cfg(test)]
    #[must_use]
    const fn raw(&self) -> &Value {
        &self.raw.0
    }
}

///
/// CanonicalKey
///
/// CanonicalKey materializes one opaque canonical grouping key from a value.
///

pub(in crate::db::executor) trait CanonicalKey {
    /// Materialize one canonical grouped key from this value.
    fn canonical_key(&self) -> Result<GroupKey, KeyCanonicalError>;
}

impl CanonicalKey for Value {
    fn canonical_key(&self) -> Result<GroupKey, KeyCanonicalError> {
        let canonical = canonicalize_value(self)?;
        GroupKey::from_raw(canonical)
    }
}

impl CanonicalKey for &Value {
    fn canonical_key(&self) -> Result<GroupKey, KeyCanonicalError> {
        (*self).canonical_key()
    }
}

///
/// GroupKeySet
///
/// GroupKeySet tracks canonical distinct keys directly by canonical grouped
/// key identity.
///

#[derive(Debug)]
pub(in crate::db::executor) struct GroupKeySet {
    keys: HashSet<GroupKey>,
}

impl GroupKeySet {
    /// Construct one empty canonical grouped-key set.
    #[must_use]
    pub(in crate::db::executor) fn new() -> Self {
        Self {
            keys: HashSet::new(),
        }
    }

    /// Return true when this canonical key is already present.
    #[must_use]
    pub(in crate::db::executor) fn contains_key(&self, key: &GroupKey) -> bool {
        self.keys.contains(key)
    }

    /// Return the total number of canonical keys tracked by this set.
    #[must_use]
    pub(in crate::db::executor) fn len(&self) -> usize {
        self.keys.len()
    }

    /// Insert one canonical key and return true if it was newly observed.
    pub(in crate::db::executor) fn insert_key(&mut self, key: GroupKey) -> bool {
        self.keys.insert(key)
    }

    /// Canonicalize+insert one raw value and return true when it is new.
    pub(in crate::db::executor) fn insert_value(
        &mut self,
        value: &Value,
    ) -> Result<bool, KeyCanonicalError> {
        let key = value.canonical_key()?;
        Ok(self.insert_key(key))
    }
}

impl Default for GroupKeySet {
    fn default() -> Self {
        Self::new()
    }
}

// Canonicalize one runtime value into grouped-key equality form.
fn canonicalize_value(value: &Value) -> Result<Value, KeyCanonicalError> {
    match value {
        Value::Decimal(decimal) => Ok(Value::Decimal(decimal.normalize())),
        Value::List(items) => items
            .iter()
            .map(canonicalize_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::List),
        Value::Map(entries) => canonicalize_map_entries(entries),
        _ => Ok(value.clone()),
    }
}

// Canonicalize map entries recursively and normalize key ordering.
fn canonicalize_map_entries(entries: &[(Value, Value)]) -> Result<Value, KeyCanonicalError> {
    normalize_canonical_map_entries(
        entries
            .iter()
            .map(|(key, value)| Ok((canonicalize_value(key)?, canonicalize_value(value)?))),
    )
}

// Canonicalize one owned runtime value into grouped-key equality form while
// preserving ownership of already-materialized grouped slot payloads.
fn canonicalize_owned_value(value: Value) -> Result<Value, KeyCanonicalError> {
    match value {
        Value::Decimal(decimal) => Ok(Value::Decimal(decimal.normalize())),
        Value::List(items) => items
            .into_iter()
            .map(canonicalize_owned_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::List),
        Value::Map(entries) => canonicalize_owned_map_entries(entries),
        value => Ok(value),
    }
}

// Canonicalize one owned map payload recursively while preserving stable
// grouped-key map normalization.
fn canonicalize_owned_map_entries(
    entries: Vec<(Value, Value)>,
) -> Result<Value, KeyCanonicalError> {
    normalize_canonical_map_entries(entries.into_iter().map(|(key, value)| {
        Ok((
            canonicalize_owned_value(key)?,
            canonicalize_owned_value(value)?,
        ))
    }))
}

// Normalize already-canonicalized map entries behind the single map ordering
// boundary shared by borrowed and owned grouped-key canonicalization paths.
fn normalize_canonical_map_entries(
    entries: impl IntoIterator<Item = Result<(Value, Value), KeyCanonicalError>>,
) -> Result<Value, KeyCanonicalError> {
    let canonical_entries = entries.into_iter().collect::<Result<Vec<_>, _>>()?;
    let normalized = Value::normalize_map_entries(canonical_entries)
        .map_err(KeyCanonicalError::InvalidMapValue)?;

    Ok(Value::Map(normalized))
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
