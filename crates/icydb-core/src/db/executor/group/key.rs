//! Module: executor::group::key
//! Responsibility: canonical grouped/distinct key materialization and set semantics.
//! Does not own: aggregation fold logic or planner-level grouped query validation.
//! Boundary: canonical equality/hash substrate for grouped execution.

use crate::{
    db::executor::group::{StableHash, stable_hash_value, try_reserve_hash_set_entry},
    error::InternalError,
    value::Value,
};
use std::{
    borrow::Borrow,
    cmp::Ordering,
    collections::HashSet,
    hash::{Hash, Hasher},
};

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

impl GroupKey {
    /// Retain a borrowed value after its canonical hash has already been proved.
    pub(in crate::db::executor) fn from_value_with_hash(
        value: &Value,
        hash: StableHash,
    ) -> Result<Self, InternalError> {
        Ok(Self::from_raw_with_hash(canonicalize_value(value)?, hash))
    }

    /// Retain an already-canonical grouped value with its required stable hash.
    pub(in crate::db::executor) fn from_canonical_value(raw: Value) -> Result<Self, InternalError> {
        let hash = stable_hash_value(&raw)?;

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
    ) -> Result<Self, InternalError> {
        let canonical = canonicalize_owned_value(Value::List(group_values))?;

        Self::from_canonical_value(canonical)
    }

    // Materialize one grouped key from owned grouped slot values while
    // reusing one caller-proven canonical stable hash from the borrowed fold
    // path instead of hashing the same canonical list twice.
    pub(in crate::db::executor) fn from_group_values_with_hash(
        group_values: Vec<Value>,
        hash: StableHash,
    ) -> Result<Self, InternalError> {
        let canonical = canonicalize_owned_value(Value::List(group_values))?;

        Ok(Self::from_raw_with_hash(canonical, hash))
    }

    // Materialize one single-field grouped key without first building an
    // intermediate one-element `Vec<Value>` only to wrap it back into a list.
    pub(in crate::db::executor) fn from_single_group_value(
        group_value: Value,
    ) -> Result<Self, InternalError> {
        let canonical_group_value = canonicalize_owned_value(group_value)?;

        Self::from_canonical_value(Value::List(vec![canonical_group_value]))
    }

    // Materialize one single-field grouped key while reusing one caller-proven
    // canonical stable hash from the borrowed grouped fold path.
    pub(in crate::db::executor) fn from_single_group_value_with_hash(
        group_value: Value,
        hash: StableHash,
    ) -> Result<Self, InternalError> {
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
    ) -> Result<Self, InternalError> {
        Self::from_canonical_value(Value::List(vec![group_value]))
    }

    // Materialize one single-field grouped key when the caller already proved
    // the grouped value is canonical and already carries the matching stable hash.
    pub(in crate::db::executor) fn from_single_canonical_group_value_with_hash(
        group_value: Value,
        hash: StableHash,
    ) -> Self {
        Self::from_raw_with_hash(Value::List(vec![group_value]), hash)
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

// HashSet's borrowed lookup must share the retained key's cached hash and
// canonical equality. This private view permits a stack-only value probe while
// preserving the existing set backing and its resource accounting.
trait GroupKeyIdentity {
    fn identity(&self) -> (StableHash, &Value);
}

impl GroupKeyIdentity for GroupKey {
    fn identity(&self) -> (StableHash, &Value) {
        (self.hash, self.canonical_value())
    }
}

impl GroupKeyIdentity for (StableHash, &Value) {
    fn identity(&self) -> (StableHash, &Value) {
        *self
    }
}

impl Eq for dyn GroupKeyIdentity + '_ {}

impl Hash for dyn GroupKeyIdentity + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.identity().0);
    }
}

impl PartialEq for dyn GroupKeyIdentity + '_ {
    fn eq(&self, other: &Self) -> bool {
        let (left_hash, left) = self.identity();
        let (right_hash, right) = other.identity();
        left_hash == right_hash && compare_borrowed_group_values(left, right).is_eq()
    }
}

impl<'a> Borrow<dyn GroupKeyIdentity + 'a> for GroupKey {
    fn borrow(&self) -> &(dyn GroupKeyIdentity + 'a) {
        self
    }
}

impl GroupKeySet {
    /// Construct one empty canonical grouped-key set.
    #[must_use]
    pub(in crate::db::executor) fn new() -> Self {
        Self {
            keys: HashSet::new(),
        }
    }

    /// Probe a validated value without allocating its retained canonical tree.
    pub(in crate::db::executor) fn contains_value(&self, value: &Value, hash: StableHash) -> bool {
        self.keys.contains(&(hash, value) as &dyn GroupKeyIdentity)
    }

    /// Return the total number of canonical keys tracked by this set.
    #[must_use]
    pub(in crate::db::executor) fn len(&self) -> usize {
        self.keys.len()
    }

    /// Insert one canonical key and return true if it was newly observed.
    pub(in crate::db::executor) fn insert_key(
        &mut self,
        key: GroupKey,
    ) -> Result<bool, InternalError> {
        if self.keys.contains(&key) {
            return Ok(false);
        }
        try_reserve_hash_set_entry(&mut self.keys)?;
        Ok(self.keys.insert(key))
    }
}

impl Default for GroupKeySet {
    fn default() -> Self {
        Self::new()
    }
}

// Canonicalize one runtime value into grouped-key equality form.
fn canonicalize_value(value: &Value) -> Result<Value, InternalError> {
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
fn canonicalize_map_entries(entries: &[(Value, Value)]) -> Result<Value, InternalError> {
    normalize_canonical_map_entries(
        entries
            .iter()
            .map(|(key, value)| Ok((canonicalize_value(key)?, canonicalize_value(value)?))),
    )
}

/// Compare borrowed values as grouped canonical keys without copying payloads.
/// Lists retain variant-ranked tuple ordering; maps use the Value owner's entry
/// order recursively. Callers must validate fallible canonicalization first.
pub(in crate::db::executor) fn compare_borrowed_group_values(
    left: &Value,
    right: &Value,
) -> Ordering {
    match (left, right) {
        (Value::List(left), Value::List(right)) => {
            compare_borrowed_group_sequences(left.iter(), right.iter())
        }
        (Value::Map(left), Value::Map(right)) => compare_borrowed_group_sequences(
            Value::ordered_map_entries(left).flat_map(|(key, value)| [key, value]),
            Value::ordered_map_entries(right).flat_map(|(key, value)| [key, value]),
        ),
        _ => Value::canonical_cmp(left, right),
    }
}

// Lexicographic comparison of borrowed list items or interleaved map keys and
// values shares the retained canonical tree's ordering without a temporary tree.
fn compare_borrowed_group_sequences<'left, 'right>(
    mut left: impl Iterator<Item = &'left Value>,
    mut right: impl Iterator<Item = &'right Value>,
) -> Ordering {
    loop {
        match (left.next(), right.next()) {
            (Some(left), Some(right)) => {
                let ordering = compare_borrowed_group_values(left, right);
                if !ordering.is_eq() {
                    return ordering;
                }
            }
            (Some(_), None) => return Ordering::Greater,
            (None, Some(_)) => return Ordering::Less,
            (None, None) => return Ordering::Equal,
        }
    }
}

/// Validate the fallible part of grouped canonicalization without owning a key.
/// Decimal normalization is infallible; maps retain the canonical Value owner's
/// scalar-key and duplicate-key rules, including inside lists and map values.
pub(in crate::db::executor) fn validate_borrowed_canonical_value(
    value: &Value,
) -> Result<(), InternalError> {
    match value {
        Value::List(items) => {
            for item in items {
                validate_borrowed_canonical_value(item)?;
            }
        }
        Value::Map(entries) => {
            Value::validate_canonical_map_entries(entries)
                .map_err(|_| InternalError::executor_invariant())?;
            for (_, value) in entries {
                validate_borrowed_canonical_value(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Canonicalize one owned value for grouped equality without computing a hash.
pub(in crate::db::executor) fn canonicalize_owned_value(
    value: Value,
) -> Result<Value, InternalError> {
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
fn canonicalize_owned_map_entries(entries: Vec<(Value, Value)>) -> Result<Value, InternalError> {
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
    entries: impl IntoIterator<Item = Result<(Value, Value), InternalError>>,
) -> Result<Value, InternalError> {
    let canonical_entries = entries.into_iter().collect::<Result<Vec<_>, _>>()?;
    let normalized = Value::normalize_map_entries(canonical_entries)
        .map_err(|_| InternalError::executor_invariant())?;

    Ok(Value::Map(normalized))
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
