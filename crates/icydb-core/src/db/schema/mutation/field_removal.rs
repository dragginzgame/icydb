//! Module: db::schema::mutation::field_removal
//! Responsibility: derive one dense accepted-after field-removal candidate.
//! Does not own: source identity, empty-domain proof, publication, or physical rewrites.
//! Boundary: accepted snapshot plus field identity -> snapshot and exact retained-ID lineage.

use std::collections::BTreeMap;

use crate::db::schema::{
    AcceptedConstraintCatalog, FieldId, PersistedFieldSnapshot, PersistedSchemaSnapshot,
    SchemaFieldSlot, SchemaRowLayout,
};

///
/// DenseFieldRemovalCandidate
///
/// One accepted-after snapshot paired with the exact old-to-new field identity
/// lineage created by dense field and row-slot reassignment.
///

pub(in crate::db::schema) struct DenseFieldRemovalCandidate {
    snapshot: PersistedSchemaSnapshot,
    field_lineage: BTreeMap<FieldId, FieldId>,
}

impl DenseFieldRemovalCandidate {
    /// Consume the derivation and return its accepted-after snapshot.
    #[must_use]
    pub(in crate::db::schema) fn into_snapshot(self) -> PersistedSchemaSnapshot {
        self.snapshot
    }

    /// Resolve one retained pre-removal field identity to its dense successor.
    #[must_use]
    pub(in crate::db::schema) fn retained_field_id(&self, before: FieldId) -> Option<FieldId> {
        self.field_lineage.get(&before).copied()
    }
}

///
/// DenseFieldRemovalError
///
/// Fail-closed reason for a catalog-native dense field-removal derivation.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum DenseFieldRemovalError {
    /// The current physical row-layout identity cannot advance.
    RowLayoutVersionExhausted,
    /// The field or one of its structural dependencies cannot be removed.
    Unsupported,
}

/// Derive one dense accepted field removal without performing physical work.
///
/// The caller must prove that no persisted row requires rewriting before it
/// publishes the returned snapshot. Live activation and candidate generations
/// reject because their identities cannot be remapped independently of their
/// owning transition.
pub(in crate::db::schema) fn derive_dense_field_removal_candidate(
    before: &PersistedSchemaSnapshot,
    removed_field_id: FieldId,
) -> Result<DenseFieldRemovalCandidate, DenseFieldRemovalError> {
    if !before.constraint_activations().is_empty()
        || !before.candidate_indexes().is_empty()
        || !before.candidate_relations().is_empty()
        || before.primary_key_field_ids().contains(&removed_field_id)
    {
        return Err(DenseFieldRemovalError::Unsupported);
    }
    let removed_field = before
        .fields()
        .iter()
        .find(|field| field.id() == removed_field_id)
        .ok_or(DenseFieldRemovalError::Unsupported)?;
    let retained_fields = before
        .fields()
        .iter()
        .filter(|field| field.id() != removed_field.id())
        .collect::<Vec<_>>();
    let dense_identities = retained_fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let ordinal = u32::try_from(index)
                .ok()
                .and_then(|index| index.checked_add(1))
                .ok_or(DenseFieldRemovalError::Unsupported)?;
            Ok((
                field.id(),
                FieldId::new(ordinal),
                SchemaFieldSlot::from_generated_index(index)
                    .ok_or(DenseFieldRemovalError::Unsupported)?,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut field_lineage = BTreeMap::new();
    for (before_id, after_id, _) in &dense_identities {
        field_lineage.insert(*before_id, *after_id);
    }
    let map_field = |field_id: FieldId| field_lineage.get(&field_id).copied();
    let fields = retained_fields
        .iter()
        .zip(&dense_identities)
        .map(|(field, (_, id, slot))| (*field).clone_for_full_layout_rewrite(*id, *slot))
        .collect::<Vec<_>>();
    let rewritten_layout_version = before
        .row_layout()
        .current_version()
        .checked_next()
        .ok_or(DenseFieldRemovalError::RowLayoutVersionExhausted)?;
    let row_layout = SchemaRowLayout::single_version(
        rewritten_layout_version,
        dense_identities
            .iter()
            .map(|(_, id, slot)| (*id, *slot))
            .collect(),
    );
    let primary_key_field_ids = before
        .primary_key_field_ids()
        .iter()
        .map(|field_id| map_field(*field_id))
        .collect::<Option<Vec<_>>>()
        .ok_or(DenseFieldRemovalError::Unsupported)?;
    let indexes = before
        .indexes()
        .iter()
        .map(|index| {
            index.clone_with_dense_identities(index.ordinal(), |field_id, _| {
                map_field(field_id).and_then(|field_id| {
                    row_layout
                        .slot_for_field(field_id)
                        .map(|slot| (field_id, slot))
                })
            })
        })
        .collect::<Option<Vec<_>>>()
        .ok_or(DenseFieldRemovalError::Unsupported)?;
    let relations = before
        .relations()
        .iter()
        .map(|relation| relation.clone_with_mapped_field_ids(map_field))
        .collect::<Option<Vec<_>>>()
        .ok_or(DenseFieldRemovalError::Unsupported)?;
    let constraint_catalog = remap_constraint_catalog(before, removed_field, map_field)?;
    let snapshot = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        before.version(),
        before.entity_path().to_string(),
        before.entity_name().to_string(),
        primary_key_field_ids,
        row_layout,
        fields,
        indexes,
    )
    .with_constraint_catalog(constraint_catalog)
    .with_relation_id_allocator(before.relation_id_allocator())
    .with_relations(relations);

    Ok(DenseFieldRemovalCandidate {
        snapshot,
        field_lineage,
    })
}

fn remap_constraint_catalog(
    before: &PersistedSchemaSnapshot,
    removed_field: &PersistedFieldSnapshot,
    map_field: impl Copy + Fn(FieldId) -> Option<FieldId>,
) -> Result<AcceptedConstraintCatalog, DenseFieldRemovalError> {
    let mut catalog = before.constraint_catalog().clone();
    if !removed_field.nullable() {
        catalog = catalog
            .with_removed_not_null(removed_field.id())
            .map_err(|_| DenseFieldRemovalError::Unsupported)?;
    }
    catalog
        .with_mapped_field_ids(map_field)
        .map_err(|_| DenseFieldRemovalError::Unsupported)
}
