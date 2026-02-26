use crate::{err, error::ErrorTree};

const RESERVED_INTERNAL_MEMORY_ID: u8 = u8::MAX;

// Validate one memory id against the declared canister range.
pub(crate) fn validate_memory_id_in_range(
    errs: &mut ErrorTree,
    label: &str,
    memory_id: u8,
    min: u8,
    max: u8,
) {
    if memory_id < min || memory_id > max {
        err!(errs, "{label} {memory_id} outside of range {min}-{max}");
    }
}

// Reject memory id values reserved by stable-structures internals.
pub(crate) fn validate_memory_id_not_reserved(errs: &mut ErrorTree, label: &str, memory_id: u8) {
    if memory_id == RESERVED_INTERNAL_MEMORY_ID {
        err!(
            errs,
            "{label} {memory_id} is reserved for stable-structures internals",
        );
    }
}
