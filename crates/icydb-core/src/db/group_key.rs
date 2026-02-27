use crate::{
    db::{
        contracts::canonical_group_key_equals,
        hash::{StableHash, stable_hash_value},
    },
    error::InternalError,
    value::{MapValueError, Value},
};
use std::{collections::BTreeMap, fmt};

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
    /// Convert one key-canonicalization failure into the executor error surface.
    pub(in crate::db) fn into_internal_error(self) -> InternalError {
        match self {
            Self::InvalidMapValue(err) => InternalError::executor_invariant(format!(
                "group key canonicalization rejected map value: {err}"
            )),
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

impl GroupKey {
    fn from_raw(raw: Value) -> Result<Self, KeyCanonicalError> {
        let hash = stable_hash_value(&raw).map_err(|err| KeyCanonicalError::HashingFailed {
            reason: err.display_with_class(),
        })?;

        Ok(Self {
            raw: CanonicalValue(raw),
            hash,
        })
    }

    #[must_use]
    pub(in crate::db) const fn hash(&self) -> StableHash {
        self.hash
    }

    #[must_use]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::db) const fn canonical_value(&self) -> &Value {
        &self.raw.0
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
/// GroupKeySet tracks canonical distinct keys by stable-hash bucket while
/// preserving canonical-value equality checks inside each bucket.
///

#[derive(Debug, Default)]
pub(in crate::db) struct GroupKeySet {
    buckets: BTreeMap<StableHash, Vec<GroupKey>>,
}

impl GroupKeySet {
    /// Insert one canonical key and return true if it was newly observed.
    pub(in crate::db) fn insert_key(&mut self, key: GroupKey) -> bool {
        let bucket = self.buckets.entry(key.hash()).or_default();
        if bucket
            .iter()
            .any(|existing| canonical_group_key_equals(existing, &key))
        {
            return false;
        }

        bucket.push(key);
        true
    }

    /// Canonicalize+insert one raw value and return true when it is new.
    pub(in crate::db) fn insert_value(&mut self, value: &Value) -> Result<bool, KeyCanonicalError> {
        let key = value.canonical_key()?;
        Ok(self.insert_key(key))
    }
}

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

fn canonicalize_map_entries(entries: &[(Value, Value)]) -> Result<Value, KeyCanonicalError> {
    let mut canonical_entries = Vec::with_capacity(entries.len());
    for (key, value) in entries {
        canonical_entries.push((canonicalize_value(key)?, canonicalize_value(value)?));
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
    use crate::{
        db::{
            contracts::canonical_group_key_equals,
            group_key::{CanonicalKey, GroupKeySet, KeyCanonicalError},
            value_hash::with_test_hash_override,
        },
        types::Decimal,
        value::{MapValueError, Value},
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
}
