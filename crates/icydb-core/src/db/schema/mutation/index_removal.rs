//! Module: db::schema::mutation::index_removal
//! Responsibility: derive one dense accepted-after secondary-index removal.
//! Does not own: source identity, empty-domain proof, publication, or physical writes.
//! Boundary: accepted snapshot plus stable index identity -> accepted-after snapshot.

use crate::{
    db::schema::{PersistedSchemaSnapshot, SchemaIndexId},
    error::InternalError,
};

/// Derive one dense accepted secondary-index removal without physical work.
///
/// Stable logical index identities survive unchanged; only dense physical
/// ordinals are reassigned. The caller must prove the removed physical domain
/// is empty or publish an exact domain replacement with this snapshot.
pub(in crate::db::schema) fn derive_dense_index_removal_candidate(
    before: &PersistedSchemaSnapshot,
    removed_index_id: SchemaIndexId,
) -> Result<PersistedSchemaSnapshot, InternalError> {
    if !before.constraint_activations().is_empty()
        || !before.candidate_indexes().is_empty()
        || !before.candidate_relations().is_empty()
    {
        return Err(InternalError::store_unsupported());
    }
    let removed = before
        .indexes()
        .iter()
        .find(|index| index.schema_id() == removed_index_id)
        .ok_or_else(InternalError::store_unsupported)?;
    let constraint_catalog = if removed.unique() {
        before
            .constraint_catalog()
            .clone()
            .with_removed_unique(removed_index_id)
            .map_err(|_| InternalError::store_unsupported())?
    } else {
        before.constraint_catalog().clone()
    };
    let indexes = before
        .indexes()
        .iter()
        .filter(|index| index.schema_id() != removed_index_id)
        .enumerate()
        .map(|(offset, index)| {
            let ordinal = u16::try_from(offset)
                .ok()
                .and_then(|offset| offset.checked_add(1))
                .ok_or_else(InternalError::store_unsupported)?;
            index
                .clone_with_dense_identities(ordinal, |field_id, slot| Some((field_id, slot)))
                .ok_or_else(InternalError::store_unsupported)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if indexes.len().saturating_add(1) != before.indexes().len() {
        return Err(InternalError::store_unsupported());
    }

    Ok(
        PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
            before.version(),
            before.entity_path().to_string(),
            before.entity_name().to_string(),
            before.primary_key_field_ids().to_vec(),
            before.row_layout().clone(),
            before.fields().to_vec(),
            indexes,
        )
        .with_constraint_catalog(constraint_catalog)
        .with_relation_id_allocator(before.relation_id_allocator())
        .with_relations(before.relations().to_vec()),
    )
}
