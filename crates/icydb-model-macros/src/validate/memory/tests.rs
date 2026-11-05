use super::*;

#[test]
fn stable_key_segment_policy_accepts_only_canonical_segments() {
    for segment in ["db", "demo_rpg", "store_1", "v1"] {
        assert!(
            stable_key_segment_is_canonical(segment),
            "compiler segment policy must accept valid segment {segment}",
        );
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
        assert!(
            !stable_key_segment_is_canonical(segment),
            "compiler segment policy must reject invalid segment {segment:?}",
        );
    }
}
