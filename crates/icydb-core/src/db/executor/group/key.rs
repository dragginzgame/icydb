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
pub(in crate::db) enum KeyCanonicalError {
    InvalidMapValue(MapValueError),
    HashingFailed { reason: String },
}

impl KeyCanonicalError {
    // Build the canonical grouped-key invariant for invalid map payloads.
    fn invalid_map_value(err: &MapValueError) -> InternalError {
        InternalError::executor_invariant(format!(
            "group key canonicalization rejected map value: {err}"
        ))
    }

    /// Convert one key-canonicalization failure into the executor error surface.
    pub(in crate::db) fn into_internal_error(self) -> InternalError {
        match self {
            Self::InvalidMapValue(err) => Self::invalid_map_value(&err),
            Self::HashingFailed { reason } => {
                InternalError::executor_internal(format!("group key hashing failed: {reason}"))
            }
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
pub(in crate::db) struct GroupKey {
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
pub(in crate::db) fn canonical_group_key_equals(left: &GroupKey, right: &GroupKey) -> bool {
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
    pub(in crate::db) const fn hash(&self) -> StableHash {
        self.hash
    }

    #[must_use]
    pub(in crate::db) const fn canonical_value(&self) -> &Value {
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
    pub(in crate::db) const fn raw(&self) -> &Value {
        &self.raw.0
    }
}

///
/// CanonicalKey
///
/// CanonicalKey materializes one opaque canonical grouping key from a value.
///

pub(in crate::db) trait CanonicalKey {
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
pub(in crate::db) struct GroupKeySet {
    keys: HashSet<GroupKey>,
}

impl GroupKeySet {
    /// Construct one empty canonical grouped-key set.
    #[must_use]
    pub(in crate::db) fn new() -> Self {
        Self {
            keys: HashSet::new(),
        }
    }

    /// Return true when this canonical key is already present.
    #[must_use]
    pub(in crate::db) fn contains_key(&self, key: &GroupKey) -> bool {
        self.keys.contains(key)
    }

    /// Return the total number of canonical keys tracked by this set.
    #[must_use]
    pub(in crate::db) fn len(&self) -> usize {
        self.keys.len()
    }

    /// Insert one canonical key and return true if it was newly observed.
    pub(in crate::db) fn insert_key(&mut self, key: GroupKey) -> bool {
        self.keys.insert(key)
    }

    /// Canonicalize+insert one raw value and return true when it is new.
    pub(in crate::db) fn insert_value(&mut self, value: &Value) -> Result<bool, KeyCanonicalError> {
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
    let mut canonical_entries = Vec::with_capacity(entries.len());
    for (key, value) in entries {
        canonical_entries.push((canonicalize_value(key)?, canonicalize_value(value)?));
    }

    let normalized = Value::normalize_map_entries(canonical_entries)
        .map_err(KeyCanonicalError::InvalidMapValue)?;

    Ok(Value::Map(normalized))
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
    let mut canonical_entries = Vec::with_capacity(entries.len());
    for (key, value) in entries {
        canonical_entries.push((
            canonicalize_owned_value(key)?,
            canonicalize_owned_value(value)?,
        ));
    }

    let normalized = Value::normalize_map_entries(canonical_entries)
        .map_err(KeyCanonicalError::InvalidMapValue)?;

    Ok(Value::Map(normalized))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::canonical_group_key_equals;
    use crate::{
        db::executor::group::{CanonicalKey, GroupKey, GroupKeySet, KeyCanonicalError},
        types::Decimal,
        value::{MapValueError, Value, with_test_hash_override},
    };

    fn map_value(entries: Vec<(Value, Value)>) -> Value {
        Value::Map(entries)
    }

    #[test]
    fn canonical_key_normalizes_decimal_scale() {
        let key = Value::Decimal(Decimal::new(100, 2))
            .canonical_key()
            .expect("canonical key");

        let Value::Decimal(normalized) = key.raw() else {
            panic!("canonical decimal value expected");
        };
        assert_eq!(normalized.scale(), 0);
    }

    #[test]
    fn canonical_key_normalizes_map_order() {
        let left = map_value(vec![
            (Value::Text("z".to_string()), Value::Uint(9)),
            (Value::Text("a".to_string()), Value::Uint(1)),
        ]);
        let right = map_value(vec![
            (Value::Text("a".to_string()), Value::Uint(1)),
            (Value::Text("z".to_string()), Value::Uint(9)),
        ]);

        let left_key = left.canonical_key().expect("left canonical key");
        let right_key = right.canonical_key().expect("right canonical key");

        assert_eq!(left_key, right_key);
        assert_eq!(left_key.hash(), right_key.hash());
    }

    #[test]
    fn canonical_key_rejects_duplicate_map_keys_after_normalization() {
        let value = map_value(vec![
            (Value::Text("a".to_string()), Value::Uint(1)),
            (Value::Text("a".to_string()), Value::Uint(2)),
        ]);

        let err = value
            .canonical_key()
            .expect_err("duplicate map keys should fail");
        assert!(matches!(
            err,
            KeyCanonicalError::InvalidMapValue(MapValueError::DuplicateKey { .. })
        ));
    }

    #[test]
    fn group_key_set_deduplicates_canonical_equivalents() {
        let mut set = GroupKeySet::default();
        let first = Value::Decimal(Decimal::new(100, 2));
        let second = Value::Decimal(Decimal::new(1, 0));

        assert!(
            set.insert_value(&first).expect("insert"),
            "first insert should be new"
        );
        assert!(
            !set.insert_value(&second).expect("insert"),
            "second insert should be deduplicated by canonical key equality"
        );
    }

    #[test]
    fn canonical_equal_keys_always_share_stable_hash() {
        let equivalent_pairs = vec![
            (
                Value::Decimal(Decimal::new(1000, 3)),
                Value::Decimal(Decimal::new(1, 0)),
            ),
            (
                Value::Map(vec![
                    (Value::Text("z".to_string()), Value::Uint(9)),
                    (Value::Text("a".to_string()), Value::Uint(1)),
                ]),
                Value::Map(vec![
                    (Value::Text("a".to_string()), Value::Uint(1)),
                    (Value::Text("z".to_string()), Value::Uint(9)),
                ]),
            ),
            (
                Value::List(vec![Value::Decimal(Decimal::new(10, 1)), Value::Uint(4)]),
                Value::List(vec![Value::Decimal(Decimal::new(1, 0)), Value::Uint(4)]),
            ),
            (
                Value::List(vec![
                    Value::Map(vec![
                        (Value::Text("z".to_string()), Value::Uint(9)),
                        (Value::Text("a".to_string()), Value::Uint(1)),
                    ]),
                    Value::Decimal(Decimal::new(2500, 2)),
                ]),
                Value::List(vec![
                    Value::Map(vec![
                        (Value::Text("a".to_string()), Value::Uint(1)),
                        (Value::Text("z".to_string()), Value::Uint(9)),
                    ]),
                    Value::Decimal(Decimal::new(25, 0)),
                ]),
            ),
        ];

        for (left_value, right_value) in equivalent_pairs {
            let left_key = left_value.canonical_key().expect("left canonical key");
            let right_key = right_value.canonical_key().expect("right canonical key");
            assert!(
                canonical_group_key_equals(&left_key, &right_key),
                "pair should be canonical-equal under group key contract",
            );
            assert_eq!(
                left_key.hash(),
                right_key.hash(),
                "canonical-equal keys must hash to the same stable hash",
            );
        }
    }

    #[test]
    fn group_key_set_handles_hash_collisions_with_equality_check() {
        with_test_hash_override([0xAB; 16], || {
            let mut set = GroupKeySet::default();
            let first = Value::Text("alpha".to_string())
                .canonical_key()
                .expect("first canonical key");
            let second = Value::Text("beta".to_string())
                .canonical_key()
                .expect("second canonical key");

            assert_eq!(
                first.hash(),
                second.hash(),
                "test setup requires an artificial hash collision",
            );
            assert!(
                !canonical_group_key_equals(&first, &second),
                "collision pair must remain distinct by canonical equality",
            );
            assert!(
                set.insert_key(first.clone()),
                "first colliding key should insert as new",
            );
            assert!(
                set.insert_key(second.clone()),
                "second colliding key must not be dropped on hash match alone",
            );
            assert!(
                !set.insert_key(first),
                "re-inserting first key should dedupe by canonical equality",
            );
            assert!(
                !set.insert_key(second),
                "re-inserting second key should dedupe by canonical equality",
            );
        });
    }

    #[test]
    fn group_key_from_single_group_value_matches_group_values_path() {
        let single = Value::Decimal(Decimal::new(100, 2));
        let single_owned =
            GroupKey::from_single_group_value(single.clone()).expect("single owned canonical key");
        let list_owned =
            GroupKey::from_group_values(vec![single]).expect("list owned canonical key");

        assert_eq!(single_owned, list_owned);
        assert_eq!(single_owned.hash(), list_owned.hash());
    }

    #[test]
    fn group_key_from_prehashed_paths_match_unhashed_paths() {
        let group_values = vec![
            Value::Decimal(Decimal::new(100, 2)),
            Value::Text("alpha".to_string()),
        ];
        let borrowed_hash = Value::List(group_values.clone())
            .canonical_key()
            .expect("borrowed canonical key")
            .hash();
        let prehashed_multi =
            GroupKey::from_group_values_with_hash(group_values.clone(), borrowed_hash)
                .expect("prehashed multi key");
        let unhashed_multi = GroupKey::from_group_values(group_values).expect("unhashed multi key");

        assert_eq!(prehashed_multi, unhashed_multi);
        assert_eq!(prehashed_multi.hash(), unhashed_multi.hash());

        let single = Value::Decimal(Decimal::new(100, 2));
        let single_hash = Value::List(vec![single.clone()])
            .canonical_key()
            .expect("borrowed single canonical key")
            .hash();
        let prehashed_single =
            GroupKey::from_single_group_value_with_hash(single.clone(), single_hash)
                .expect("prehashed single key");
        let unhashed_single =
            GroupKey::from_single_group_value(single).expect("unhashed single key");

        assert_eq!(prehashed_single, unhashed_single);
        assert_eq!(prehashed_single.hash(), unhashed_single.hash());
    }

    #[test]
    fn group_key_from_single_canonical_group_value_matches_hashed_single_path() {
        let single = Value::Uint(7);
        let single_hash = Value::List(vec![single.clone()])
            .canonical_key()
            .expect("borrowed single canonical key")
            .hash();
        let canonical =
            GroupKey::from_single_canonical_group_value_with_hash(single.clone(), single_hash);
        let hashed = GroupKey::from_single_group_value_with_hash(single, single_hash)
            .expect("hashed single canonical key");

        assert_eq!(canonical, hashed);
        assert_eq!(canonical.hash(), hashed.hash());
    }

    #[test]
    fn group_key_from_group_values_matches_borrowed_canonical_key_path() {
        let group_values = vec![
            Value::Decimal(Decimal::new(100, 2)),
            Value::Text("alpha".to_string()),
            map_value(vec![(Value::Text("z".to_string()), Value::Uint(9))]),
        ];
        let borrowed = Value::List(group_values.clone())
            .canonical_key()
            .expect("borrowed canonical key");
        let owned = GroupKey::from_group_values(group_values).expect("owned canonical key");

        assert_eq!(borrowed, owned);
        assert_eq!(borrowed.hash(), owned.hash());
    }
}
