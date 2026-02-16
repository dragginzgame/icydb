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
}
