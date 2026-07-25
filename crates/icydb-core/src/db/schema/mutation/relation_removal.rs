//! Module: db::schema::mutation::relation_removal
//! Responsibility: derive one exact accepted-after generated relation removal.
//! Does not own: source identity, empty-domain proof, publication, or physical writes.
//! Boundary: accepted snapshot plus stable relation identity -> accepted-after snapshot.

use crate::{
    db::schema::{PersistedSchemaSnapshot, RelationId},
    error::InternalError,
};

/// Derive one exact accepted relation removal without physical work.
///
/// The caller must separately prove that the source entity and the removed
/// reverse physical generation are empty before publishing this candidate.
pub(in crate::db::schema) fn derive_relation_removal_candidate(
    before: &PersistedSchemaSnapshot,
    removed_relation_id: RelationId,
) -> Result<PersistedSchemaSnapshot, InternalError> {
    if !before.constraint_activations().is_empty()
        || !before.candidate_indexes().is_empty()
        || !before.candidate_relations().is_empty()
    {
        return Err(InternalError::store_unsupported());
    }
    let constraint_catalog = before
        .constraint_catalog()
        .clone()
        .with_removed_relation(removed_relation_id)
        .map_err(|_| InternalError::store_unsupported())?;
    let relations = before
        .relations()
        .iter()
        .filter(|relation| relation.id() != removed_relation_id)
        .cloned()
        .collect::<Vec<_>>();
    if relations.len().saturating_add(1) != before.relations().len() {
        return Err(InternalError::store_unsupported());
    }
    let candidate = before
        .clone()
        .with_constraint_catalog(constraint_catalog)
        .with_relations(relations);
    if !candidate.has_valid_integrity() {
        return Err(InternalError::store_invariant());
    }
    Ok(candidate)
}
