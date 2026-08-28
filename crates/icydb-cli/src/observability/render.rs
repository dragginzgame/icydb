//! Module: observability rendering helpers.
//! Responsibility: share small value-formatting helpers across CLI reports.
//! Does not own: command execution, report layout, or table rendering.
//! Boundary: exposes owner-scoped text formatting to CLI renderers.

pub(crate) fn render_hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut rendered = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        rendered.push(char::from(HEX[usize::from(byte >> 4)]));
        rendered.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    rendered
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

#[cfg(test)]
mod tests {
    use super::render_hex_lower;

    #[test]
    fn lowercase_hex_rendering_is_exact() {
        assert_eq!(render_hex_lower(&[]), "");
        assert_eq!(
            render_hex_lower(&[0x00, 0x0f, 0x10, 0xab, 0xff]),
            "000f10abff"
        );
    }
}
