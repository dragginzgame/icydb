//! Persisted schema relation integrity checks.

use crate::db::schema::{PersistedFieldSnapshot, PersistedRelationEdgeSnapshot, SchemaRowLayout};

// Build the first deterministic accepted-relation integrity diagnostic.
// Relation edges are owned by the source entity snapshot; target compatibility
// is checked during schema reconciliation where both entity snapshots are
// available.
pub(in crate::db::schema) fn schema_snapshot_relation_integrity_detail(
    subject: &str,
    row_layout: &SchemaRowLayout,
    fields: &[PersistedFieldSnapshot],
    relations: &[PersistedRelationEdgeSnapshot],
) -> Option<String> {
    for (relation_offset, relation) in relations.iter().enumerate() {
        if relation.name().is_empty() {
            return Some(format!(
                "{subject} empty relation name: relation_offset={relation_offset}",
            ));
        }

        if relation.target_path().is_empty() {
            return Some(format!(
                "{subject} empty relation target path: relation='{}'",
                relation.name(),
            ));
        }

        for other in &relations[relation_offset + 1..] {
            if relation.name() == other.name() {
                return Some(format!(
                    "{subject} duplicate relation name: name='{}'",
                    relation.name(),
                ));
            }
        }

        if relation.local_field_ids().is_empty() {
            return Some(format!(
                "{subject} empty relation local field list: relation='{}'",
                relation.name(),
            ));
        }

        for (field_offset, field_id) in relation.local_field_ids().iter().enumerate() {
            if relation.local_field_ids()[..field_offset].contains(field_id) {
                return Some(format!(
                    "{subject} duplicate relation local field: relation='{}' field_id={}",
                    relation.name(),
                    field_id.get(),
                ));
            }

            let Some(row_layout_slot) = row_layout.slot_for_field(*field_id) else {
                return Some(format!(
                    "{subject} relation local field missing from row layout: relation='{}' field_id={}",
                    relation.name(),
                    field_id.get(),
                ));
            };

            let Some(field) = fields.iter().find(|field| field.id() == *field_id) else {
                return Some(format!(
                    "{subject} relation local field missing from fields: relation='{}' field_id={}",
                    relation.name(),
                    field_id.get(),
                ));
            };

            if field.slot() != row_layout_slot {
                return Some(format!(
                    "{subject} relation local field slot mismatch: relation='{}' field_id={} field_slot={} row_layout_slot={}",
                    relation.name(),
                    field_id.get(),
                    field.slot().get(),
                    row_layout_slot.get(),
                ));
            }
        }
    }

    None
}
