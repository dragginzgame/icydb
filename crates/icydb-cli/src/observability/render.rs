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
