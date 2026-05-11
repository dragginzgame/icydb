//! Module: db::schema::integrity
//! Responsibility: persisted schema metadata integrity checks.
//! Does not own: reconciliation policy, schema transition decisions, or raw codec parsing.
//! Boundary: reports local metadata inconsistencies before snapshots become accepted authority.

use crate::db::schema::{
    FieldId, PersistedFieldKind, PersistedFieldSnapshot, PersistedIndexFieldPathSnapshot,
    PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
    SchemaRowLayout, SchemaVersion,
};

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

// Build the first deterministic accepted-index integrity diagnostic. Index
// contracts are validated separately from row-layout integrity so existing
// field-only callers can keep their narrow checks.
pub(in crate::db::schema) fn schema_snapshot_index_integrity_detail(
    subject: &str,
    row_layout: &SchemaRowLayout,
    fields: &[PersistedFieldSnapshot],
    indexes: &[PersistedIndexSnapshot],
) -> Option<String> {
    for (index_offset, index) in indexes.iter().enumerate() {
        if index.name().is_empty() {
            return Some(format!(
                "{subject} empty index name: index_offset={index_offset}",
            ));
        }

        if index.store().is_empty() {
            return Some(format!(
                "{subject} empty index store: index='{}'",
                index.name(),
            ));
        }

        for other in &indexes[index_offset + 1..] {
            if index.ordinal() == other.ordinal() {
                return Some(format!(
                    "{subject} duplicate index ordinal: ordinal={}",
                    index.ordinal(),
                ));
            }

            if index.name() == other.name() {
                return Some(format!(
                    "{subject} duplicate index name: name='{}'",
                    index.name(),
                ));
            }
        }

        if index_key_len(index.key()) == 0 {
            return Some(format!(
                "{subject} empty index key: index='{}'",
                index.name(),
            ));
        }

        if let Some(detail) = index_key_detail(subject, row_layout, fields, index) {
            return Some(detail);
        }
    }

    None
}

const fn index_key_len(key: &PersistedIndexKeySnapshot) -> usize {
    match key {
        PersistedIndexKeySnapshot::FieldPath(paths) => paths.len(),
        PersistedIndexKeySnapshot::Items(items) => items.len(),
    }
}

fn index_key_detail(
    subject: &str,
    row_layout: &SchemaRowLayout,
    fields: &[PersistedFieldSnapshot],
    index: &PersistedIndexSnapshot,
) -> Option<String> {
    match index.key() {
        PersistedIndexKeySnapshot::FieldPath(paths) => paths
            .iter()
            .find_map(|path| index_field_path_detail(subject, row_layout, fields, index, path)),
        PersistedIndexKeySnapshot::Items(items) => items.iter().find_map(|item| match item {
            PersistedIndexKeyItemSnapshot::FieldPath(path) => {
                index_field_path_detail(subject, row_layout, fields, index, path)
            }
            PersistedIndexKeyItemSnapshot::Expression(expression) => {
                index_expression_detail(subject, row_layout, fields, index, expression)
            }
        }),
    }
}

fn index_expression_detail(
    subject: &str,
    row_layout: &SchemaRowLayout,
    fields: &[PersistedFieldSnapshot],
    index: &PersistedIndexSnapshot,
    expression: &crate::db::schema::PersistedIndexExpressionSnapshot,
) -> Option<String> {
    if expression.canonical_text().is_empty() {
        return Some(format!(
            "{subject} empty index expression canonical text: index='{}'",
            index.name(),
        ));
    }

    if expression.input_kind() != expression.source().kind() {
        return Some(format!(
            "{subject} index expression input kind mismatch: index='{}' expression='{}'",
            index.name(),
            expression.canonical_text(),
        ));
    }

    if !expression_output_kind_matches_op(expression.op(), expression.output_kind()) {
        return Some(format!(
            "{subject} index expression output kind mismatch: index='{}' expression='{}'",
            index.name(),
            expression.canonical_text(),
        ));
    }

    index_field_path_detail(subject, row_layout, fields, index, expression.source())
}

const fn expression_output_kind_matches_op(
    op: crate::db::schema::PersistedIndexExpressionOp,
    output_kind: &PersistedFieldKind,
) -> bool {
    match op {
        crate::db::schema::PersistedIndexExpressionOp::Lower
        | crate::db::schema::PersistedIndexExpressionOp::Upper
        | crate::db::schema::PersistedIndexExpressionOp::Trim
        | crate::db::schema::PersistedIndexExpressionOp::LowerTrim => {
            matches!(output_kind, PersistedFieldKind::Text { .. })
        }
        crate::db::schema::PersistedIndexExpressionOp::Date => {
            matches!(output_kind, PersistedFieldKind::Date)
        }
        crate::db::schema::PersistedIndexExpressionOp::Year
        | crate::db::schema::PersistedIndexExpressionOp::Month
        | crate::db::schema::PersistedIndexExpressionOp::Day => {
            matches!(output_kind, PersistedFieldKind::Int)
        }
    }
}

fn index_field_path_detail(
    subject: &str,
    row_layout: &SchemaRowLayout,
    fields: &[PersistedFieldSnapshot],
    index: &PersistedIndexSnapshot,
    path: &PersistedIndexFieldPathSnapshot,
) -> Option<String> {
    let Some(row_layout_slot) = row_layout.slot_for_field(path.field_id()) else {
        return Some(format!(
            "{subject} index field missing from row layout: index='{}' field_id={}",
            index.name(),
            path.field_id().get(),
        ));
    };

    if row_layout_slot != path.slot() {
        return Some(format!(
            "{subject} index field slot mismatch: index='{}' field_id={} index_slot={} row_layout_slot={}",
            index.name(),
            path.field_id().get(),
            path.slot().get(),
            row_layout_slot.get(),
        ));
    }

    if path.path().is_empty() {
        return Some(format!(
            "{subject} empty index field path: index='{}' field_id={}",
            index.name(),
            path.field_id().get(),
        ));
    }

    if !fields.iter().any(|field| field.id() == path.field_id()) {
        return Some(format!(
            "{subject} index field missing from fields: index='{}' field_id={}",
            index.name(),
            path.field_id().get(),
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
