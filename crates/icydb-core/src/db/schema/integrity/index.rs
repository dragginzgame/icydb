//! Persisted schema index integrity checks.

use crate::db::schema::{
    AcceptedFieldKind, PersistedFieldSnapshot, PersistedIndexExpressionOp,
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
) -> Option<()> {
    for (index_offset, index) in indexes.iter().enumerate() {
        if index.name().is_empty() {
            return Some(());
        }

        if index.store().is_empty() {
            return Some(());
        }

        for other in &indexes[index_offset + 1..] {
            if index.ordinal() == other.ordinal() {
                return Some(());
            }

            if index.name() == other.name() {
                return Some(());
            }
        }

        if index_key_len(index.key()) == 0 {
            return Some(());
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
) -> Option<()> {
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
) -> Option<()> {
    if expression.canonical_text().is_empty() {
        return Some(());
    }

    if expression.input_kind() != expression.source().kind() {
        return Some(());
    }

    if !expression_output_kind_matches_op(expression.op(), expression.output_kind()) {
        return Some(());
    }

    index_field_path_detail(subject, row_layout, fields, index, expression.source())
}

const fn expression_output_kind_matches_op(
    op: PersistedIndexExpressionOp,
    output_kind: &AcceptedFieldKind,
) -> bool {
    match op {
        PersistedIndexExpressionOp::Lower
        | PersistedIndexExpressionOp::Upper
        | PersistedIndexExpressionOp::Trim
        | PersistedIndexExpressionOp::LowerTrim => {
            matches!(output_kind, AcceptedFieldKind::Text { .. })
        }
        PersistedIndexExpressionOp::Date => {
            matches!(output_kind, AcceptedFieldKind::Date)
        }
        PersistedIndexExpressionOp::Year
        | PersistedIndexExpressionOp::Month
        | PersistedIndexExpressionOp::Day => {
            matches!(output_kind, AcceptedFieldKind::Int64)
        }
    }
}

fn index_field_path_detail(
    _subject: &str,
    row_layout: &SchemaRowLayout,
    fields: &[PersistedFieldSnapshot],
    _index: &PersistedIndexSnapshot,
    path: &PersistedIndexFieldPathSnapshot,
) -> Option<()> {
    let Some(row_layout_slot) = row_layout.slot_for_field(path.field_id()) else {
        return Some(());
    };

    if row_layout_slot != path.slot() {
        return Some(());
    }

    if path.path().is_empty() {
        return Some(());
    }

    if !fields.iter().any(|field| field.id() == path.field_id()) {
        return Some(());
    }

    None
}
