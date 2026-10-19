use crate::db::codec::writer::{
    BoundedByteWriter, checked_len_u32, write_i64_be, write_i128_be, write_len_prefixed_bytes_be,
    write_len_prefixed_bytes_le, write_len_u32_le, write_u128_be,
};

#[test]
fn primitive_writes_keep_exact_endian_and_signed_bytes() {
    let mut bytes = Vec::new();
    write_len_prefixed_bytes_be(&mut bytes, b"xy").unwrap();
    write_len_prefixed_bytes_le(&mut bytes, b"z").unwrap();
    assert_eq!(bytes, b"\0\0\0\x02xy\x01\0\0\0z");
    bytes.clear();
    write_i64_be(&mut bytes, -2);
    write_i128_be(&mut bytes, -1);
    write_u128_be(&mut bytes, 1);
    assert_eq!(&bytes[..8], &[255, 255, 255, 255, 255, 255, 255, 254]);
    assert_eq!(&bytes[8..24], &[255; 16]);
    assert_eq!(&bytes[24..39], &[0; 15]);
    assert_eq!(bytes[39], 1);
}

#[test]
fn bounded_writes_accept_exact_ceiling_and_keep_overflow_sticky() {
    let mut exact = BoundedByteWriter::<7>::new();
    exact.push_u16(0x1234);
    exact.push_string("x").unwrap();
    assert_eq!(exact.finish().unwrap(), b"\x12\x34\0\0\0\x01x");
    let mut overflow = BoundedByteWriter::<5>::new();
    overflow.push_u32(1);
    overflow.push_u16(2);
    overflow.push_u8(3); // This fits, but cannot turn prior overflow into success.
    assert!(overflow.finish().is_err());
    let mut empty = BoundedByteWriter::<0>::new();
    empty.push_bytes(&[]);
    assert_eq!(empty.finish().unwrap(), b"");
}

#[test]
fn unrepresentable_lengths_leave_the_destination_untouched() {
    let mut bytes = vec![7];
    assert_eq!(checked_len_u32(u32::MAX as usize).unwrap(), u32::MAX);
    if let Ok(oversized) = usize::try_from(u64::from(u32::MAX) + 1) {
        assert!(write_len_u32_le(&mut bytes, oversized).is_err());
        assert_eq!(bytes, [7]);
    }
}
