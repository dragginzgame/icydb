//! Module: validate::memory
//! Responsibility: derive-side validation helpers.
//! Does not own: runtime validation.
//! Boundary: parse-time checks.

/// Return whether a stable memory name segment is canonical.
#[must_use]
pub(crate) fn stable_key_segment_is_canonical(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

#[cfg(test)]
mod tests {
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
}
