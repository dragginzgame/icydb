use super::*;

#[test]
fn cursor_text_round_trips_canonical_vectors_and_whitespace() {
    for (raw, encoded) in [
        (b"f".as_slice(), "Zg"),
        (b"fo".as_slice(), "Zm8"),
        (b"foo".as_slice(), "Zm9v"),
        ([0xfb, 0xff].as_slice(), "-_8"),
        ([0x00, 0x01, 0x0a, 0xff].as_slice(), "AAEK_w"),
    ] {
        assert_eq!(encode_cursor(raw), encoded);
        assert_eq!(decode_cursor(&format!("  {encoded} \n")).unwrap(), raw);
    }
}

#[test]
fn cursor_text_rejects_empty_invalid_length_alphabet_and_trailing_bits() {
    for text in ["", " \n\t"] {
        assert_eq!(decode_cursor(text), Err(CursorDecodeError::Empty));
    }
    assert_eq!(decode_cursor("A"), Err(CursorDecodeError::InvalidLength));
    for text in ["A!", "A/", "A+", "AB", "Aé"] {
        assert_eq!(
            decode_cursor(text),
            Err(CursorDecodeError::InvalidBase64 { position: 1 })
        );
    }
    for text in ["AA=", "AA==", "AA A"] {
        assert!(decode_cursor(text).is_err());
    }
}

#[test]
fn cursor_text_enforces_binary_budget_for_all_final_symbol_lengths() {
    for len in [
        0,
        1,
        2,
        3,
        MAX_CURSOR_TOKEN_BYTES - 2,
        MAX_CURSOR_TOKEN_BYTES - 1,
        MAX_CURSOR_TOKEN_BYTES,
    ] {
        let raw = vec![0xff; len];
        let text = encode_cursor(&raw);
        assert_eq!(text.len(), (len * 4).div_ceil(3));
        assert_eq!(encoded_cursor_len(len), text.len());
        if len == 0 {
            continue;
        }
        assert_eq!(decode_cursor(&text).unwrap(), raw);
    }
    let rejected = encode_cursor(&vec![0; MAX_CURSOR_TOKEN_BYTES + 1]);
    assert_eq!(encoded_cursor_len(usize::MAX), usize::MAX);
    assert_eq!(
        decode_cursor(&rejected),
        Err(CursorDecodeError::TooLong {
            len: rejected.len(),
            max: MAX_CURSOR_TOKEN_TEXT_LEN,
        })
    );
}
