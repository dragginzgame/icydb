use super::*;

#[test]
fn stable_key_segment_policy_requires_a_lowercase_letter_prefix() {
    for segment in ["db", "demo_rpg", "store_1", "v1"] {
        assert!(stable_key_segment_is_canonical(segment));
    }

    for segment in [
        "",
        "1db",
        "_db",
        "Demo",
        "demo-rpg",
        "demo.rpg",
        "canic.owned",
    ] {
        assert!(!stable_key_segment_is_canonical(segment));
    }
}

#[test]
fn full_stable_key_policy_rejects_reserved_and_malformed_keys() {
    assert!(stable_key_is_canonical("icydb.demo_rpg.characters.data.v1"));

    for key in [
        "canic.demo_rpg.characters.data.v1",
        "icydb.demo_rpg.characters.data",
        "icydb.demo-rpg.characters.data.v1",
        "icydb.demo_rpg..data.v1",
        "icydb.Demo.characters.data.v1",
        "icydb.demo_rpg.characters.data.v2",
        "icydb.1demo.characters.data.v1",
        "icydb._demo.characters.data.v1",
    ] {
        assert!(!stable_key_is_canonical(key), "key should fail: {key}");
    }

    let maximum = format!("icydb.{}.data.v1", "a".repeat(114));
    assert_eq!(maximum.len(), 128);
    assert!(stable_key_is_canonical(&maximum));

    let oversized = format!("icydb.{}.data.v1", "a".repeat(115));
    assert_eq!(oversized.len(), 129);
    assert!(!stable_key_is_canonical(&oversized));
}
