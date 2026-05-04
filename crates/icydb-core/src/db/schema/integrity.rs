//! Module: db::schema::integrity
//! Responsibility: persisted schema metadata integrity checks.
//! Does not own: reconciliation policy, schema transition decisions, or raw codec parsing.
//! Boundary: reports local metadata inconsistencies before snapshots become accepted authority.

use crate::db::schema::{FieldId, PersistedFieldSnapshot, SchemaRowLayout, SchemaVersion};

// Build the first deterministic persisted-schema integrity diagnostic. Callers
// decide whether the detail represents a typed caller invariant or raw payload
// corruption, but the schema module owns the actual metadata consistency rules.
pub(in crate::db::schema) fn schema_snapshot_integrity_detail(
    subject: &str,
    version: SchemaVersion,
    primary_key_field_id: FieldId,
    row_layout: &SchemaRowLayout,
    fields: &[PersistedFieldSnapshot],
) -> Option<String> {
    if row_layout.version() != version {
        return Some(format!(
            "{subject} row-layout version mismatch: snapshot={} row_layout={}",
            version.get(),
            row_layout.version().get(),
        ));
    }

    if let Some(detail) = duplicate_row_layout_detail(subject, row_layout) {
        return Some(detail);
    }

    if let Some(detail) = duplicate_field_detail(subject, fields) {
        return Some(detail);
    }

    if row_layout.slot_for_field(primary_key_field_id).is_none() {
        return Some(format!(
            "{subject} primary key field missing from row layout: field_id={}",
            primary_key_field_id.get(),
        ));
    }

    if row_layout.field_to_slot().len() != fields.len() {
        return Some(format!(
            "{subject} row-layout field count mismatch: row_layout={} fields={}",
            row_layout.field_to_slot().len(),
            fields.len(),
        ));
    }

    let mut has_primary_key_field = false;
    for field in fields {
        has_primary_key_field |= field.id() == primary_key_field_id;

        let Some(row_layout_slot) = row_layout.slot_for_field(field.id()) else {
            return Some(format!(
                "{subject} missing row-layout slot for field_id={}",
                field.id().get(),
            ));
        };

        if row_layout_slot != field.slot() {
            return Some(format!(
                "{subject} field slot mismatch: field_id={} field_slot={} row_layout_slot={}",
                field.id().get(),
                field.slot().get(),
                row_layout_slot.get(),
            ));
        }
    }

    if !has_primary_key_field {
        return Some(format!(
            "{subject} primary key field missing from fields: field_id={}",
            primary_key_field_id.get(),
        ));
    }

    None
}

// Find duplicate row-layout entries before slot lookup can hide the ambiguity
// by returning only the first matching field ID.
fn duplicate_row_layout_detail(subject: &str, row_layout: &SchemaRowLayout) -> Option<String> {
    let entries = row_layout.field_to_slot();
    for (index, (field_id, slot)) in entries.iter().enumerate() {
        for (other_field_id, other_slot) in &entries[index + 1..] {
            if field_id == other_field_id {
                return Some(format!(
                    "{subject} duplicate row-layout field id: field_id={}",
                    field_id.get(),
                ));
            }

            if slot == other_slot {
                return Some(format!(
                    "{subject} duplicate row-layout slot: slot={}",
                    slot.get(),
                ));
            }
        }
    }

    None
}

// Find duplicate persisted field entries before name or field-ID lookup can
// become order-dependent. Accepted schema metadata must be unambiguous.
fn duplicate_field_detail(subject: &str, fields: &[PersistedFieldSnapshot]) -> Option<String> {
    for (index, field) in fields.iter().enumerate() {
        for other in &fields[index + 1..] {
            if field.id() == other.id() {
                return Some(format!(
                    "{subject} duplicate field id: field_id={}",
                    field.id().get(),
                ));
            }

            if field.name() == other.name() {
                return Some(format!(
                    "{subject} duplicate field name: name='{}'",
                    field.name(),
                ));
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
fn nested_leaf_detail(subject: &str, field: &PersistedFieldSnapshot) -> Option<String> {
    for (index, leaf) in field.nested_leaves().iter().enumerate() {
        if leaf.path().is_empty() {
            return Some(format!(
                "{subject} empty nested leaf path: field_id={}",
                field.id().get(),
            ));
        }

        for other in &field.nested_leaves()[index + 1..] {
            if leaf.path() == other.path() {
                return Some(format!(
                    "{subject} duplicate nested leaf path: field_id={} path={:?}",
                    field.id().get(),
                    leaf.path(),
                ));
            }
        }
    }

    None
}
