use super::{
    casefold_text, lower_text, lower_text_construction_allowance, upper_text,
    upper_text_construction_allowance,
};
use sha2::{Digest, Sha256};

#[test]
fn unicode_scalar_mappings_and_properties_match_the_persisted_contract() {
    let mut hash = Sha256::new();
    hash.update(b"icydb.unicode-text-contract.v1");
    // Pin all scalar mappings and text properties, independently of the
    // compiler's version label. Contextual str casing has separate vectors.
    for scalar in (0..=u32::from(char::MAX)).filter_map(char::from_u32) {
        hash.update(u32::from(scalar).to_le_bytes());
        let lower = scalar.to_lowercase();
        hash.update([u8::try_from(lower.clone().count()).unwrap()]);
        for mapped in lower {
            hash.update(u32::from(mapped).to_le_bytes());
        }
        let upper = scalar.to_uppercase();
        hash.update([u8::try_from(upper.clone().count()).unwrap()]);
        for mapped in upper {
            hash.update(u32::from(mapped).to_le_bytes());
        }
        hash.update([
            u8::from(scalar.is_whitespace()),
            u8::from(scalar.is_lowercase()),
            u8::from(scalar.is_uppercase()),
            u8::from(scalar.is_alphabetic()),
        ]);
    }
    assert_eq!(
        <[u8; 32]>::from(hash.finalize()),
        [
            0xe0, 0xa7, 0x19, 0x73, 0xd5, 0x33, 0xbb, 0x6f, 0x10, 0x04, 0xc0, 0xe3, 0xa0, 0xa5,
            0xe8, 0x40, 0x28, 0x79, 0xe7, 0xdb, 0xf0, 0x49, 0x1f, 0xd3, 0x18, 0x2b, 0xe9, 0x85,
            0x6e, 0x90, 0x82, 0xf8
        ],
    );
}

#[test]
fn unicode_text_transform_goldens_cover_new_pairs_and_contextual_sigma() {
    for (input, lower, upper) in [
        ("ɤƛ", "ɤƛ", "\u{a7cb}\u{a7dc}"),
        ("\u{a7cb}\u{a7dc}", "ɤƛ", "\u{a7cb}\u{a7dc}"),
        ("\u{1c89}", "\u{1c8a}", "\u{1c89}"),
        ("İßﬃ", "i\u{307}ßﬃ", "İSSFFI"),
        ("ΟΣ", "ος", "ΟΣ"),
        ("ΟΣ\u{301}", "ος\u{301}", "ΟΣ\u{301}"),
        ("ΟΣ\u{301}Α", "οσ\u{301}α", "ΟΣ\u{301}Α"),
        ("ΣΑ", "σα", "ΣΑ"),
        ("Σ", "σ", "Σ"),
    ] {
        assert_eq!(lower_text(input), lower);
        assert_eq!(casefold_text(input), lower);
        assert_eq!(upper_text(input), upper);
    }
}

#[test]
fn uppercase_allowance_covers_unicode_expansion_and_cumulative_growth() {
    for scalar in (0..=u32::from(char::MAX)).filter_map(char::from_u32) {
        let output_bytes: usize = scalar.to_uppercase().map(char::len_utf8).sum();
        assert!(output_bytes <= 3 * scalar.len_utf8(), "{scalar:?}");
    }
    for text in ["", "a", "ß", "ΐ", "aΐ", "ﬃ", "é🍋", "ASCII"] {
        for repeats in [1, 2, 16, 1024] {
            let input = text.repeat(repeats);
            let output = upper_text(&input);
            let initial = input.len();
            let first_growth = (2 * initial).max(8);
            let requested = if output.capacity() > first_growth {
                initial + first_growth + output.capacity()
            } else if output.capacity() > initial {
                initial + output.capacity()
            } else {
                initial
            };
            let (backing, work) = upper_text_construction_allowance(input.len());
            assert!(requested as u64 <= backing, "{input:?}");
            assert!(output.len() as u64 <= work);
            assert_eq!(output, input.to_uppercase());
        }
    }
    assert_eq!(upper_text_construction_allowance(0), (0, 0));
    assert!(upper_text_construction_allowance(usize::MAX).0 >= usize::MAX as u64);
}

#[test]
fn lowercase_allowance_covers_unicode_expansion_and_output_growth() {
    // Allocation-free qualification of every scalar mapping in the pinned
    // toolchain. Contextual sigma changes spelling, not UTF-8 width.
    for scalar in (0..=u32::from(char::MAX)).filter_map(char::from_u32) {
        let output_bytes: usize = scalar.to_lowercase().map(char::len_utf8).sum();
        assert!(output_bytes <= 2 * scalar.len_utf8(), "{scalar:?}");
    }
    for text in ["", "A", "İ", "Aİ", "İΣ", "ΟΣ\u{301}", "ΣΑ", "ASCII"] {
        for repeats in [1, 2, 16, 1024] {
            let input = text.repeat(repeats);
            let output = lower_text(&input);
            let (backing, _) = lower_text_construction_allowance(input.len());
            let requested = if output.capacity() > input.len() {
                input.len() + output.capacity()
            } else {
                input.len()
            };
            assert!(requested as u64 <= backing);
            assert_eq!(output, input.to_lowercase());
        }
    }
}

#[test]
fn canonical_text_transforms_preserve_current_ascii_and_unicode_semantics() {
    assert_eq!(casefold_text("IcYDB"), "icydb");
    assert_eq!(lower_text("IcYDB"), "icydb");
    assert_eq!(upper_text("IcYDB"), "ICYDB");

    assert_eq!(casefold_text("Straße"), "straße");
    assert_eq!(lower_text("Straße"), "straße");
    assert_eq!(upper_text("Straße"), "STRASSE");
}
