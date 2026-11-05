use super::*;

#[test]
fn phone_normalizer_preserves_canonical_e164() {
    let normalizer = E164PhoneNumber;
    let mut value = "  +15551234567  ".to_string();

    normalizer.normalize(&mut value).unwrap();

    assert_eq!(value, "+15551234567");
}

#[test]
fn phone_normalizer_adds_plus_to_plain_digits() {
    let normalizer = E164PhoneNumber;
    let mut value = "15551234567".to_string();

    normalizer.normalize(&mut value).unwrap();

    assert_eq!(value, "+15551234567");
}

#[test]
fn phone_normalizer_preserves_non_digit_input_for_validator_rejection() {
    let normalizer = E164PhoneNumber;
    let mut value = "+1 (555) 123-4567".to_string();

    normalizer.normalize(&mut value).unwrap();

    assert_eq!(value, "+1 (555) 123-4567");
}
