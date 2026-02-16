use crate::{
    db::{
        index::{
            key::{IndexId, IndexKey, encode_canonical_index_component},
            store::IndexStore,
        },
        store::DataKey,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::index::IndexModel,
    traits::EntityKind,
    value::Value,
};
use std::ops::Bound;

impl IndexStore {
    pub(crate) fn resolve_data_values<E: EntityKind>(
        &self,
        index: &IndexModel,
        prefix: &[Value],
    ) -> Result<Vec<DataKey>, InternalError> {
        if prefix.len() > index.fields.len() {
            return Err(InternalError::new(
                ErrorClass::Unsupported,
                ErrorOrigin::Index,
                format!(
                    "index prefix length {} exceeds field count {}",
                    prefix.len(),
                    index.fields.len()
                ),
            ));
        }

        let index_id = IndexId::new::<E>(index);

        let mut components = Vec::with_capacity(prefix.len());
        for value in prefix {
            let component = encode_canonical_index_component(value).map_err(|_| {
                InternalError::new(
                    ErrorClass::Unsupported,
                    ErrorOrigin::Index,
                    "index prefix value is not indexable",
                )
            })?;
            components.push(component);
        }

        let (start, end) = IndexKey::bounds_for_prefix(index_id, index.fields.len(), &components);
        let (start_raw, end_raw) = (start.to_raw(), end.to_raw());

        let mut out = Vec::new();

        for entry in self.entry_map().range(start_raw..=end_raw) {
            let raw_key = entry.key();
            let value = entry.value();

            #[cfg(debug_assertions)]
            if let Err(err) = Self::verify_entry_fingerprint(Some(index), raw_key, &value) {
                panic!(
                    "invariant violation (debug-only): index fingerprint verification failed: {err:?}"
                );
            }

            let raw_entry = value.entry;

            // Validate index key structure.
            IndexKey::try_from_raw(raw_key).map_err(|err| {
                InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Index,
                    format!("index key corrupted during resolve: {err}"),
                )
            })?;

            // Decode storage keys.
            let storage_keys = raw_entry.decode_keys().map_err(|err| {
                InternalError::new(ErrorClass::Corruption, ErrorOrigin::Index, err.to_string())
            })?;

            if index.unique && storage_keys.len() != 1 {
                return Err(InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Index,
                    "unique index entry contains an unexpected number of keys",
                ));
            }

            // Convert to DataKeys (storage boundary — no typed IDs).
            out.extend(
                storage_keys
                    .into_iter()
                    .map(|storage_key| DataKey::from_key::<E>(storage_key)),
            );
        }

        Ok(out)
    }

    pub(crate) fn resolve_data_values_in_range<E: EntityKind>(
        &self,
        index: &IndexModel,
        prefix: &[Value],
        lower: &Bound<Value>,
        upper: &Bound<Value>,
    ) -> Result<Vec<DataKey>, InternalError> {
        if prefix.len() >= index.fields.len() {
            return Err(InternalError::new(
                ErrorClass::Unsupported,
                ErrorOrigin::Index,
                format!(
                    "index range prefix length {} must be less than field count {}",
                    prefix.len(),
                    index.fields.len()
                ),
            ));
        }

        let index_id = IndexId::new::<E>(index);

        let mut prefix_components = Vec::with_capacity(prefix.len());
        for value in prefix {
            let component = encode_canonical_index_component(value).map_err(|_| {
                InternalError::new(
                    ErrorClass::Unsupported,
                    ErrorOrigin::Index,
                    "index range prefix value is not indexable",
                )
            })?;
            prefix_components.push(component);
        }

        let lower_component = encode_index_component_bound(lower, "lower")?;
        let upper_component = encode_index_component_bound(upper, "upper")?;
        let (start, end) = IndexKey::bounds_for_prefix_component_range(
            index_id,
            index.fields.len(),
            &prefix_components,
            lower_component,
            upper_component,
        );

        let start_raw = raw_index_key_bound(start);
        let end_raw = raw_index_key_bound(end);
        if range_is_empty(&start_raw, &end_raw) {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for entry in self.entry_map().range((start_raw, end_raw)) {
            let raw_key = entry.key();
            let value = entry.value();

            #[cfg(debug_assertions)]
            if let Err(err) = Self::verify_entry_fingerprint(Some(index), raw_key, &value) {
                panic!(
                    "invariant violation (debug-only): index fingerprint verification failed: {err:?}"
                );
            }

            let raw_entry = value.entry;

            IndexKey::try_from_raw(raw_key).map_err(|err| {
                InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Index,
                    format!("index key corrupted during range resolve: {err}"),
                )
            })?;

            let storage_keys = raw_entry.decode_keys().map_err(|err| {
                InternalError::new(ErrorClass::Corruption, ErrorOrigin::Index, err.to_string())
            })?;

            if index.unique && storage_keys.len() != 1 {
                return Err(InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Index,
                    "unique index entry contains an unexpected number of keys",
                ));
            }

            out.extend(
                storage_keys
                    .into_iter()
                    .map(|storage_key| DataKey::from_key::<E>(storage_key)),
            );
        }

        Ok(out)
    }
}

fn encode_index_component_bound(
    bound: &Bound<Value>,
    label: &str,
) -> Result<Bound<Vec<u8>>, InternalError> {
    match bound {
        Bound::Unbounded => Ok(Bound::Unbounded),
        Bound::Included(value) => encode_canonical_index_component(value)
            .map(Bound::Included)
            .map_err(|_| {
                InternalError::new(
                    ErrorClass::Unsupported,
                    ErrorOrigin::Index,
                    format!("index range {label} bound value is not indexable"),
                )
            }),
        Bound::Excluded(value) => encode_canonical_index_component(value)
            .map(Bound::Excluded)
            .map_err(|_| {
                InternalError::new(
                    ErrorClass::Unsupported,
                    ErrorOrigin::Index,
                    format!("index range {label} bound value is not indexable"),
                )
            }),
    }
}

fn raw_index_key_bound(bound: Bound<IndexKey>) -> Bound<crate::db::index::RawIndexKey> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(key) => Bound::Included(key.to_raw()),
        Bound::Excluded(key) => Bound::Excluded(key.to_raw()),
    }
}

fn range_is_empty(
    lower: &Bound<crate::db::index::RawIndexKey>,
    upper: &Bound<crate::db::index::RawIndexKey>,
) -> bool {
    let (Some(lower_key), Some(upper_key)) = (bound_key(lower), bound_key(upper)) else {
        return false;
    };

    if lower_key < upper_key {
        return false;
    }
    if lower_key > upper_key {
        return true;
    }

    !matches!(lower, Bound::Included(_)) || !matches!(upper, Bound::Included(_))
}

const fn bound_key(
    bound: &Bound<crate::db::index::RawIndexKey>,
) -> Option<&crate::db::index::RawIndexKey> {
    match bound {
        Bound::Included(value) | Bound::Excluded(value) => Some(value),
        Bound::Unbounded => None,
    }
}
