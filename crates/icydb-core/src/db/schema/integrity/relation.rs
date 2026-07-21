//! Persisted schema relation integrity checks.

use crate::db::schema::{
    PersistedFieldSnapshot, PersistedRelationEdgeSnapshot, SchemaRowLayout,
    classify_accepted_field_kind,
};

// Build the first deterministic accepted-relation integrity diagnostic.
// Relation edges are owned by the source entity snapshot; target compatibility
// is checked during schema reconciliation where both entity snapshots are
// available.
pub(in crate::db::schema) fn schema_snapshot_relation_integrity_detail(
    _subject: &str,
    row_layout: &SchemaRowLayout,
    fields: &[PersistedFieldSnapshot],
    relations: &[PersistedRelationEdgeSnapshot],
) -> Option<()> {
    for (relation_offset, relation) in relations.iter().enumerate() {
        if relation.name().is_empty() {
            return Some(());
        }

        if relation.target_path().is_empty() {
            return Some(());
        }

        for other in &relations[relation_offset + 1..] {
            if relation.name() == other.name() {
                return Some(());
            }
        }

        if relation.local_field_ids().is_empty() {
            return Some(());
        }

        for (field_offset, field_id) in relation.local_field_ids().iter().enumerate() {
            if relation.local_field_ids()[..field_offset].contains(field_id) {
                return Some(());
            }

            let Some(row_layout_slot) = row_layout.slot_for_field(*field_id) else {
                return Some(());
            };

            let Some(field) = fields.iter().find(|field| field.id() == *field_id) else {
                return Some(());
            };

            if field.slot() != row_layout_slot {
                return Some(());
            }

            if classify_accepted_field_kind(field.kind()).is_composite() {
                return Some(());
            }
        }
    }

    None
}
