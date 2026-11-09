use super::*;

const SEED: [u8; 32] = [17; 32];

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn next_u128_seeds_from_system_when_unseeded() {
    clear_for_tests();

    assert!(next_u128().is_ok());
}

#[cfg(target_arch = "wasm32")]
#[test]
fn next_u128_fails_when_rng_is_unseeded_without_native_entropy() {
    clear_for_tests();

    std::assert_matches!(next_u128(), Err(RandomError::NotInitialized));
}

#[test]
fn seed_from_makes_next_u128_deterministic() {
    seed_from(SEED);
    let first = next_u128().expect("seeded rng should produce a value");
    let second = next_u128().expect("seeded rng should produce a second value");

    seed_from(SEED);
    assert_eq!(
        next_u128().expect("seeded rng should replay first value"),
        first
    );
    assert_eq!(
        next_u128().expect("seeded rng should replay second value"),
        second
    );
}

#[test]
fn seeded_stream_preserves_previous_high_word_low_word_order() {
    // Captured from the pre-extraction ChaCha20Rng 0.10 high/low construction.
    seed_from(SEED);
    for expected in [
        0xe991_98aa_592d_220c_890f_4ece_9296_664b,
        0x7a8f_4a8c_5d9b_c0da_e462_b8c1_60b3_ab52,
        0xf06e_4003_7155_55db_5c44_06f6_fe11_705e,
        0x2d21_cbf4_e9d7_0d3f_e3c8_8e39_6bb9_2f66,
    ] {
        assert_eq!(next_u128().unwrap(), expected);
    }
}

#[test]
fn exhausted_stream_fails_without_automatic_reseeding() {
    RNG.with_borrow_mut(|rng| *rng = Some(Generator::from_seed(SEED, 16)));
    assert_eq!(
        next_u128().unwrap(),
        0xe991_98aa_592d_220c_890f_4ece_9296_664b
    );
    for _ in 0..2 {
        assert!(matches!(next_u128(), Err(RandomError::NotInitialized)));
        RNG.with_borrow(|rng| assert_eq!(rng.as_ref().unwrap().remaining_bytes(), 0));
    }
    clear_for_tests();
}
