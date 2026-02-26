const RESERVED_INTERNAL_MEMORY_ID: u8 = u8::MAX;

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
