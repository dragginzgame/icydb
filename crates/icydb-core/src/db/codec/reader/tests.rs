use super::{ByteDecodeError, ByteReader};

#[test]
fn little_endian_reads_preserve_known_wire_bytes() {
    assert_eq!(ByteReader::new(&[0x34, 0x12]).read_u16_le(), Ok(0x1234));
    assert_eq!(
        ByteReader::new(&[0x78, 0x56, 0x34, 0x12]).read_u32_le(),
        Ok(0x1234_5678)
    );
    let bytes = [0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01];
    assert_eq!(
        ByteReader::new(&bytes).read_u64_le(),
        Ok(0x0123_4567_89ab_cdef)
    );
    let bytes = [
        0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01, 0, 0, 0, 0, 0, 0, 0, 0x80,
    ];
    assert_eq!(
        ByteReader::new(&bytes).read_u128_le(),
        Ok(0x8000_0000_0000_0000_0123_4567_89ab_cdef)
    );
    let framed = [2, 0, 0, 0, b'a', b'b'];
    let mut reader = ByteReader::new(&framed);
    assert_eq!(reader.read_len_prefixed_bytes_le(), Ok(b"ab".as_slice()));
    assert_eq!(reader.finish(), Ok(()));
    for end in 0..framed.len() {
        assert_eq!(
            ByteReader::new(&framed[..end]).read_len_prefixed_bytes_le(),
            Err(ByteDecodeError)
        );
    }
}

#[test]
fn failed_byte_reads_preserve_the_unread_payload() {
    let mut reader = ByteReader::new(&[1, 2, 3]);
    assert_eq!(reader.read_u8(), Ok(1));
    assert_eq!(reader.read_exact(usize::MAX), Err(ByteDecodeError));
    assert_eq!(reader.read_array::<3>(), Err(ByteDecodeError));
    assert_eq!(reader.read_exact(2), Ok([2, 3].as_slice()));
    assert_eq!(reader.finish(), Ok(()));
}

#[test]
fn length_and_text_boundaries_reject_malformed_payloads() {
    let bytes = [0, 0, 0, 2, b'a', b'b'];
    assert_eq!(
        ByteReader::new(&bytes).read_bounded_string(2),
        Ok("ab".into())
    );
    assert_eq!(
        ByteReader::new(&bytes).read_bounded_string(1),
        Err(ByteDecodeError)
    );
    for end in 0..bytes.len() {
        assert_eq!(
            ByteReader::new(&bytes[..end]).read_string(),
            Err(ByteDecodeError)
        );
    }
    assert_eq!(
        ByteReader::new(&[0, 0, 0, 1, 0xff]).read_string(),
        Err(ByteDecodeError)
    );
    assert_eq!(ByteReader::new(&bytes).finish(), Err(ByteDecodeError));
}
