//! Module: observability rendering helpers.
//! Responsibility: share small formatting helpers across observability reports.
//! Does not own: command execution, report layout, or table rendering.
//! Boundary: exposes owner-scoped value formatting helpers to observability modules.

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
