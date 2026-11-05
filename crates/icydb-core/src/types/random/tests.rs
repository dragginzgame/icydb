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
