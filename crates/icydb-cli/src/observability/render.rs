//! Module: observability rendering helpers.
//! Responsibility: share small value-formatting helpers across CLI reports.
//! Does not own: command execution, report layout, or table rendering.
//! Boundary: exposes owner-scoped text formatting to CLI renderers.

pub(super) fn render_field_list(fields: &[String]) -> String {
    if fields.is_empty() {
        "-".to_string()
    } else {
        fields.join(", ")
    }
}

pub(super) const fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

// Both schema reports and SQL DESCRIBE project the same accepted field facts.
pub(crate) fn schema_field_cells(field: &icydb::db::EntityFieldDescription) -> [String; 15] {
    [
        field.name().to_string(),
        field
            .slot()
            .map_or_else(|| "-".to_string(), |slot| slot.to_string()),
        field.kind().to_string(),
        yes_no(field.nullable()).to_string(),
        yes_no(field.primary_key()).to_string(),
        yes_no(field.queryable()).to_string(),
        field.origin().to_string(),
        field.insert_omission().unwrap_or("-").to_string(),
        field.insert_default().unwrap_or("-").to_string(),
        field
            .insert_default_bytes()
            .map_or_else(|| "-".to_string(), |bytes| bytes.to_string()),
        field.insert_default_hash().unwrap_or("-").to_string(),
        field
            .introduced_in_layout()
            .map_or_else(|| "-".to_string(), |layout| layout.to_string()),
        field.historical_fill().unwrap_or("-").to_string(),
        field
            .historical_fill_bytes()
            .map_or_else(|| "-".to_string(), |bytes| bytes.to_string()),
        field.historical_fill_hash().unwrap_or("-").to_string(),
    ]
}
