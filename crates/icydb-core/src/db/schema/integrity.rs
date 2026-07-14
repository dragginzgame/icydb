//! Module: db::schema::integrity
//! Responsibility: persisted schema metadata integrity checks.
//! Does not own: reconciliation policy, schema transition decisions, or raw codec parsing.
//! Boundary: reports local metadata inconsistencies before snapshots become accepted authority.

mod index;
mod relation;

use crate::db::schema::{FieldId, PersistedFieldSnapshot, SchemaRowLayout, SchemaVersion};

pub(in crate::db::schema) use index::schema_snapshot_index_integrity_detail;
pub(in crate::db::schema) use relation::schema_snapshot_relation_integrity_detail;

// Build the first deterministic persisted-schema integrity diagnostic. Callers
// decide whether the detail represents a typed caller invariant or raw payload
// corruption, but the schema module owns the actual metadata consistency rules.
pub(in crate::db::schema) fn schema_snapshot_integrity_detail(
    subject: &str,
    version: SchemaVersion,
    primary_key_field_ids: &[FieldId],
    row_layout: &SchemaRowLayout,
    fields: &[PersistedFieldSnapshot],
) -> Option<()> {
    if version.get() == 0 {
        return Some(());
    }

    if row_layout.version() != version {
        return Some(());
    }

    if let Some(detail) = duplicate_row_layout_detail(subject, row_layout) {
        return Some(detail);
    }

    if let Some(detail) = duplicate_field_detail(subject, fields) {
        return Some(detail);
    }

    if primary_key_field_ids.is_empty() {
        return Some(());
    }

    for (index, primary_key_field_id) in primary_key_field_ids.iter().enumerate() {
        if primary_key_field_ids[..index].contains(primary_key_field_id) {
            return Some(());
        }

        if row_layout.slot_for_field(*primary_key_field_id).is_none() {
            return Some(());
        }
    }

    if row_layout.field_to_slot().len() != fields.len() {
        return Some(());
    }

    for (index, ((field_id, slot), field)) in
        row_layout.field_to_slot().iter().zip(fields).enumerate()
    {
        let expected_id = u32::try_from(index).ok()?.checked_add(1)?;
        let expected_slot = u16::try_from(index).ok()?;
        if field_id.get() != expected_id
            || slot.get() != expected_slot
            || field.id() != *field_id
            || field.slot() != *slot
        {
            return Some(());
        }
    }

    let mut matched_primary_key_fields = 0usize;
    for field in fields {
        if primary_key_field_ids.contains(&field.id()) {
            matched_primary_key_fields += 1;
        }

        let Some(row_layout_slot) = row_layout.slot_for_field(field.id()) else {
            return Some(());
        };

        if row_layout_slot != field.slot() {
            return Some(());
        }
    }

    if matched_primary_key_fields != primary_key_field_ids.len() {
        return Some(());
    }

    None
}

// Find duplicate row-layout entries before slot lookup can hide the ambiguity
// by returning only the first matching field ID.
fn duplicate_row_layout_detail(_subject: &str, row_layout: &SchemaRowLayout) -> Option<()> {
    let entries = row_layout.field_to_slot();
    for (index, (field_id, slot)) in entries.iter().enumerate() {
        for (other_field_id, other_slot) in &entries[index + 1..] {
            if field_id == other_field_id {
                return Some(());
            }

            if slot == other_slot {
                return Some(());
            }
        }
    }

    None
}

// Find duplicate persisted field entries before name or field-ID lookup can
// become order-dependent. Accepted schema metadata must be unambiguous.
fn duplicate_field_detail(subject: &str, fields: &[PersistedFieldSnapshot]) -> Option<()> {
    for (index, field) in fields.iter().enumerate() {
        for other in &fields[index + 1..] {
            if field.id() == other.id() {
                return Some(());
            }

            if field.name() == other.name() {
                return Some(());
            }
        }

        if let Some(detail) = nested_leaf_detail(subject, field) {
            return Some(detail);
        }
    }

    None
}

// Find ambiguous nested leaf descriptors before accepted field-path inference
// can become first-match dependent. Nested paths are local to their owning
// top-level field, so uniqueness is enforced per field.
fn nested_leaf_detail(_subject: &str, field: &PersistedFieldSnapshot) -> Option<()> {
    for (index, leaf) in field.nested_leaves().iter().enumerate() {
        if leaf.path().is_empty() {
            return Some(());
        }

        for other in &field.nested_leaves()[index + 1..] {
            if leaf.path() == other.path() {
                return Some(());
            }
        }
    }

    None
}
