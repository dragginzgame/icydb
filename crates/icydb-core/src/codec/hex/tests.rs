use super::encode_hex_lower;
use std::fmt::Write as _;

#[test]
fn lowercase_hex_preserves_every_byte_without_a_prefix() {
    let bytes: Vec<_> = (0..=u8::MAX).collect();
    let mut expected = String::new();
    for byte in &bytes {
        write!(expected, "{byte:02x}").unwrap();
    }
    assert_eq!(encode_hex_lower(&bytes), expected);
    assert_eq!(encode_hex_lower(&[]), "");
}
