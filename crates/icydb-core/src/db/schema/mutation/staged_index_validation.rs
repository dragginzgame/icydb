use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaStagedIndexValidationError {
    IndexKeyDecode,
}

pub(in crate::db::schema) fn staged_index_keys_have_duplicate_unique_components<'a>(
    keys: impl IntoIterator<Item = &'a RawIndexStoreKey>,
) -> Result<bool, SchemaStagedIndexValidationError> {
    let mut previous = None;
    for raw_key in keys {
        let current = IndexKey::try_from_raw(raw_key)
            .map_err(|_| SchemaStagedIndexValidationError::IndexKeyDecode)?;
        if let Some(previous) = previous.as_ref()
            && same_unique_index_components(previous, &current)
        {
            return Ok(true);
        }
        previous = Some(current);
    }

    Ok(false)
}

fn same_unique_index_components(left: &IndexKey, right: &IndexKey) -> bool {
    left.index_id() == right.index_id() && left.has_same_components(right)
}
