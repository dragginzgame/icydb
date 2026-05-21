pub(super) fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "none".to_string(), |value| value.to_string())
}

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

pub(super) fn table_width<'a>(heading: &str, values: impl Iterator<Item = &'a str>) -> usize {
    values.map(str::len).max().unwrap_or(0).max(heading.len())
}
