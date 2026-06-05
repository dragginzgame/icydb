//! Persisted schema index integrity checks.

use crate::db::schema::{
    PersistedFieldKind, PersistedFieldSnapshot, PersistedIndexExpressionOp,
    PersistedIndexExpressionSnapshot, PersistedIndexFieldPathSnapshot,
    PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
    SchemaRowLayout,
};

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
    expression: &PersistedIndexExpressionSnapshot,
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
    op: PersistedIndexExpressionOp,
    output_kind: &PersistedFieldKind,
) -> bool {
    match op {
        PersistedIndexExpressionOp::Lower
        | PersistedIndexExpressionOp::Upper
        | PersistedIndexExpressionOp::Trim
        | PersistedIndexExpressionOp::LowerTrim => {
            matches!(output_kind, PersistedFieldKind::Text { .. })
        }
        PersistedIndexExpressionOp::Date => {
            matches!(output_kind, PersistedFieldKind::Date)
        }
        PersistedIndexExpressionOp::Year
        | PersistedIndexExpressionOp::Month
        | PersistedIndexExpressionOp::Day => {
            matches!(output_kind, PersistedFieldKind::Int64)
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
