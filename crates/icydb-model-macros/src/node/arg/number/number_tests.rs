use super::*;

#[test]
fn test_integer_literals() {
    assert_eq!(
        ArgNumber::parse_numeric_string("42i8").unwrap(),
        ArgNumber::Int8(42)
    );
    assert_eq!(
        ArgNumber::parse_numeric_string("-100").unwrap(),
        ArgNumber::Int32(-100)
    );
    assert_eq!(
        ArgNumber::parse_numeric_string("1000i16").unwrap(),
        ArgNumber::Int16(1000)
    );
    assert_eq!(
        ArgNumber::parse_numeric_string("-30000i32").unwrap(),
        ArgNumber::Int32(-30000)
    );
    assert_eq!(
        ArgNumber::parse_numeric_string("500000i64").unwrap(),
        ArgNumber::Int64(500_000)
    );
    assert_eq!(
        ArgNumber::parse_numeric_string("42u8").unwrap(),
        ArgNumber::Nat8(42)
    );
    assert_eq!(
        ArgNumber::parse_numeric_string("65535u16").unwrap(),
        ArgNumber::Nat16(65535)
    );
    assert_eq!(
        ArgNumber::parse_numeric_string("4000000000u32").unwrap(),
        ArgNumber::Nat32(4_000_000_000)
    );
    assert_eq!(
        ArgNumber::parse_numeric_string("-10_i8").unwrap(),
        ArgNumber::Int8(-10)
    );
}

#[test]
fn test_integer_uscores() {
    assert_eq!(
        ArgNumber::parse_numeric_string("10_000").unwrap(),
        ArgNumber::Int32(10_000)
    );
    assert_eq!(
        ArgNumber::parse_numeric_string("10_000_u64").unwrap(),
        ArgNumber::Nat64(10_000)
    );
    assert_eq!(
        ArgNumber::parse_numeric_string("10_000_i64").unwrap(),
        ArgNumber::Int64(10_000)
    );
}

#[test]
fn test_float_literals() {
    assert_eq!(
        ArgNumber::parse_numeric_string("3.12_f32").unwrap(),
        ArgNumber::Float32(3.12)
    );
    assert_eq!(
        ArgNumber::parse_numeric_string("3.13_f64").unwrap(),
        ArgNumber::Float64(3.13)
    );
    assert_eq!(
        ArgNumber::parse_numeric_string("3.15").unwrap(),
        ArgNumber::Float64(3.15)
    );
}

#[test]
fn test_large_integers() {
    // Should overflow i32, fall back to i64
    assert_eq!(
        ArgNumber::parse_numeric_string("5000000000").unwrap(),
        ArgNumber::Int64(5_000_000_000)
    );

    // Should fit in u64
    assert_eq!(
        ArgNumber::parse_numeric_string("18446744073709551615u64").unwrap(),
        ArgNumber::Nat64(u64::MAX)
    );

    // Unsuffixed literals keep widening to 128-bit integers when needed.
    assert_eq!(
        ArgNumber::parse_numeric_string("9223372036854775808").unwrap(),
        ArgNumber::Int128(9_223_372_036_854_775_808)
    );
    assert_eq!(
        ArgNumber::parse_numeric_string("340282366920938463463374607431768211455").unwrap(),
        ArgNumber::Nat128(u128::MAX)
    );
}

#[test]
fn test_negative_with_suffix() {
    assert_eq!(
        ArgNumber::parse_numeric_string("-128i8").unwrap(),
        ArgNumber::Int8(-128)
    );
}

#[test]
fn test_invalid_literals() {
    assert!(ArgNumber::parse_numeric_string("hello").is_err());
    assert!(ArgNumber::parse_numeric_string("42x").is_err());
    assert!(ArgNumber::parse_numeric_string("4.2.5_f32").is_err());
}

#[test]
fn test_to_tokens_integer() {
    let num = ArgNumber::parse_numeric_string("10").unwrap();
    let tokens = quote!(#num);
    assert_eq!(tokens.to_string(), "10i32");
}

#[test]
fn test_to_tokens_float() {
    let num = ArgNumber::parse_numeric_string("3.14").unwrap();
    let tokens = quote!(#num);
    assert_eq!(tokens.to_string(), "3.14f64");

    let num = ArgNumber::parse_numeric_string("3.14_f64").unwrap();
    let tokens = quote!(#num);
    assert_eq!(tokens.to_string(), "3.14f64");
}
