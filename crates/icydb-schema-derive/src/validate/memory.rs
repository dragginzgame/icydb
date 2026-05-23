pub(crate) use icydb_schema::node::{APP_MEMORY_ID_MAX, APP_MEMORY_ID_MIN};

/// Return a range-validation error message for a memory id, if invalid.
pub(crate) fn memory_id_out_of_range_error(
    label: &str,
    memory_id: u8,
    min: u8,
    max: u8,
) -> Option<String> {
    (!icydb_schema::node::memory_id_is_in_range(memory_id, min, max))
        .then(|| format!("{label} {memory_id} outside of range {min}-{max}"))
}

/// Return a reserved-id validation message for a memory id, if invalid.
pub(crate) fn memory_id_reserved_error(label: &str, memory_id: u8) -> Option<String> {
    icydb_schema::node::memory_id_is_reserved(memory_id)
        .then(|| format!("{label} {memory_id} is reserved for stable-structures internals"))
}

/// Return an app-owned range validation message for a memory id, if invalid.
pub(crate) fn app_memory_id_error(label: &str, memory_id: u8) -> Option<String> {
    (!icydb_schema::node::app_memory_id_is_valid(memory_id)).then(|| {
        format!(
            "{label} {memory_id} outside of app-owned stable memory range {APP_MEMORY_ID_MIN}-{APP_MEMORY_ID_MAX}"
        )
    })
}

/// Return whether a stable memory name segment is canonical.
#[must_use]
pub(crate) fn stable_key_segment_is_canonical(value: &str) -> bool {
    icydb_schema::node::stable_key_segment_is_canonical(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_memory_id_policy_matches_schema_policy() {
        for memory_id in APP_MEMORY_ID_MIN..=APP_MEMORY_ID_MAX {
            assert!(
                app_memory_id_error("memory_id", memory_id).is_none(),
                "derive should accept app memory id {memory_id}",
            );
            assert!(
                memory_id_reserved_error("memory_id", memory_id).is_none(),
                "derive should not classify app memory id {memory_id} as reserved",
            );
        }

        for memory_id in [0, APP_MEMORY_ID_MIN - 1] {
            assert!(
                app_memory_id_error("memory_id", memory_id).is_some(),
                "derive should reject below-range app memory id {memory_id}",
            );
        }

        assert!(
            app_memory_id_error("memory_id", u8::MAX).is_some(),
            "derive should reject reserved id as outside the app-owned range",
        );
        assert!(
            memory_id_reserved_error("memory_id", u8::MAX).is_some(),
            "derive should reject reserved id explicitly",
        );
    }

    #[test]
    fn stable_key_segment_policy_matches_schema_policy() {
        for segment in ["db", "demo_rpg", "store_1", "v1"] {
            assert_eq!(
                stable_key_segment_is_canonical(segment),
                icydb_schema::node::stable_key_segment_is_canonical(segment),
                "derive/schema segment policy must match for valid segment {segment}",
            );
            assert!(stable_key_segment_is_canonical(segment));
        }

        for segment in ["", "Demo", "demo-rpg", "demo.rpg", "canic.owned"] {
            assert_eq!(
                stable_key_segment_is_canonical(segment),
                icydb_schema::node::stable_key_segment_is_canonical(segment),
                "derive/schema segment policy must match for invalid segment {segment:?}",
            );
            assert!(!stable_key_segment_is_canonical(segment));
        }
    }
}
