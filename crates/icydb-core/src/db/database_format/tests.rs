use super::*;
use crate::db::{commit::select_commit_memory_allocation, registry::StoreRegistry};

#[test]
fn observation_and_admission_agree_on_control_memory_virginity() {
    thread_local! {
        static STORES: StoreRegistry = StoreRegistry::new();
    }
    select_commit_memory_allocation(247, "icydb.test.format-observation.commit.v1");
    let memory = commit_memory_handle(current_commit_memory_allocation().unwrap()).unwrap();
    let page_bytes = usize::try_from(WASM_PAGE_BYTES).unwrap();
    assert_eq!(
        observe_database_format(&STORES).unwrap(),
        DatabaseFormatObservation::Uninitialized
    );
    assert_eq!(admit_or_initialize_database_format(&memory, &[]), Ok(true));
    assert_eq!(memory.size(), 0);
    assert_eq!(memory.as_ref().grow(1), Ok(0));
    assert_eq!(
        observe_database_format(&STORES).unwrap(),
        DatabaseFormatObservation::Uninitialized
    );
    assert_eq!(admit_or_initialize_database_format(&memory, &[]), Ok(true));

    for (offset, bytes) in [
        (0, &b"SCL"[..]),
        (DATABASE_BOOT_RECORD_BYTES as u64, &[1][..]),
        (VIRGIN_SCAN_CHUNK_BYTES as u64 - 1, &[1][..]),
        (VIRGIN_SCAN_CHUNK_BYTES as u64, &[1][..]),
        (WASM_PAGE_BYTES - 1, &[1][..]),
    ] {
        memory.write(offset, bytes);
        let mut before = vec![0; page_bytes];
        memory.read(0, &mut before);
        let observed = observe_database_format(&STORES)
            .expect_err("non-virgin control memory must not appear fresh");
        let admitted = admit_or_initialize_database_format(&memory, &[])
            .map_err(map_admission_error)
            .unwrap_err();
        assert_eq!(observed.diagnostic(), admitted.diagnostic());
        assert_eq!(observed.diagnostic_facts(), admitted.diagnostic_facts());
        let mut after = vec![0; page_bytes];
        memory.read(0, &mut after);
        assert_eq!(before, after);
        memory.write(offset, &[0; 3][..bytes.len()]);
    }

    assert_eq!(memory.as_ref().grow(1), Ok(1));
    let error = observe_database_format(&STORES).unwrap_err();
    let admitted = admit_or_initialize_database_format(&memory, &[])
        .map_err(map_admission_error)
        .unwrap_err();
    assert_eq!(error.diagnostic(), admitted.diagnostic());
    memory.write(0, &DatabaseBootRecord::current().encode());
    assert_eq!(
        observe_database_format(&STORES).unwrap(),
        DatabaseFormatObservation::Current
    );
    assert_eq!(admit_or_initialize_database_format(&memory, &[]), Ok(false));
    assert_eq!(memory.size(), 2);
}

#[test]
fn retired_boot_identity_refuses_without_rewriting_any_retained_bytes() {
    thread_local! {
        static STORES: StoreRegistry = StoreRegistry::new();
    }
    select_commit_memory_allocation(246, "icydb.test.retired-format.commit.v1");
    let memory = commit_memory_handle(current_commit_memory_allocation().unwrap()).unwrap();
    memory.grow(1);
    for retired_magic in [b"ICYDBCTL", b"ICYDBRT\0", b"ICYDBST\0", b"ICYDBNX\0"] {
        let mut retired = DatabaseBootRecord::current().encode();
        retired[..8].copy_from_slice(retired_magic);
        let checksum = crc32c(&retired[..DATABASE_BOOT_CHECKSUM_OFFSET]);
        retired[DATABASE_BOOT_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_be_bytes());
        memory.write(0, &retired);
        // Retained bytes beyond the gate must never be decoded or reinitialized.
        memory.write(128, &[0xa5; 32]);
        let mut before = vec![0; usize::try_from(WASM_PAGE_BYTES).unwrap()];
        memory.read(0, &mut before);
        let observed = observe_database_format(&STORES).unwrap_err();
        let admitted = admit_or_initialize_database_format(&memory, &[])
            .map_err(map_admission_error)
            .unwrap_err();
        let validated = validate_current_boot_record(&memory).unwrap_err();
        assert_eq!(observed.diagnostic(), admitted.diagnostic());
        assert_eq!(observed.diagnostic(), validated.diagnostic());
        let mut after = vec![0; before.len()];
        memory.read(0, &mut after);
        assert_eq!(after, before);
        assert_eq!(memory.size(), 1);
    }
}

#[test]
fn crc32c_matches_known_vectors_and_bit_recurrence() {
    assert_eq!(crc32c(b""), 0);
    assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    assert_eq!(crc32c(&[0; 32]), 0x8a91_36aa);
    assert_eq!(crc32c(&[255; 32]), 0x62a8_ab43);

    // Independently exercise every byte transition and long/tail lengths.
    let bytes: Vec<u8> = (0_u8..=255)
        .map(|byte| byte.wrapping_mul(73))
        .cycle()
        .take(4097)
        .collect();
    for len in (0..=256).chain([511, 512, 513, 4096, 4097]) {
        let mut remainder = u32::MAX;
        for byte in &bytes[..len] {
            remainder ^= u32::from(*byte);
            for _ in 0..8 {
                remainder = if remainder & 1 == 0 {
                    remainder >> 1
                } else {
                    (remainder >> 1) ^ 0x82f6_3b78
                };
            }
        }
        assert_eq!(crc32c(&bytes[..len]), !remainder, "length {len}");
    }
}

#[test]
fn crc32c_trailer_preserves_borrowing_and_rejects_corruption() {
    // Independent standard vector; do not construct it with the encoder.
    let encoded = b"123456789\xe3\x06\x92\x83";
    let body = verify_crc32c_trailer(encoded).expect("known checksum should verify");
    assert_eq!(body, b"123456789");
    assert!(std::ptr::eq(body.as_ptr(), encoded.as_ptr()));
    for end in 0..encoded.len() {
        assert_eq!(verify_crc32c_trailer(&encoded[..end]), None);
    }
    for offset in 0..encoded.len() {
        for bit in 0..8 {
            let mut damaged = *encoded;
            damaged[offset] ^= 1 << bit;
            assert_eq!(verify_crc32c_trailer(&damaged), None);
        }
    }
    // An empty checked body is valid for the primitive. Record-specific
    // minimum lengths still reject it before calling this verifier.
    assert_eq!(verify_crc32c_trailer(&[0; 4]), Some([].as_slice()));
}
