const RESERVED_INTERNAL_MEMORY_ID: u8 = u8::MAX;
pub const APP_MEMORY_ID_MIN: u8 = 100;
pub const APP_MEMORY_ID_MAX: u8 = 254;

/// Return a range-validation error message for a memory id, if invalid.
pub fn memory_id_out_of_range_error(
    label: &str,
    memory_id: u8,
    min: u8,
    max: u8,
) -> Option<String> {
    (memory_id < min || memory_id > max)
        .then(|| format!("{label} {memory_id} outside of range {min}-{max}"))
}

/// Return a reserved-id validation message for a memory id, if invalid.
pub fn memory_id_reserved_error(label: &str, memory_id: u8) -> Option<String> {
    (memory_id == RESERVED_INTERNAL_MEMORY_ID)
        .then(|| format!("{label} {memory_id} is reserved for stable-structures internals"))
}

/// Return an app-owned range validation message for a memory id, if invalid.
pub fn app_memory_id_error(label: &str, memory_id: u8) -> Option<String> {
    (!(APP_MEMORY_ID_MIN..=APP_MEMORY_ID_MAX).contains(&memory_id)).then(|| {
        format!(
            "{label} {memory_id} outside of app-owned stable memory range {APP_MEMORY_ID_MIN}-{APP_MEMORY_ID_MAX}"
        )
    })
}

/// Return whether a stable memory name segment is canonical.
#[must_use]
pub fn stable_key_segment_is_canonical(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}
