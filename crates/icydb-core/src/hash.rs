///
/// FNV-1a 64-bit hash (compile-time safe).
///
/// This variant is used only for **static, non-cryptographic identifiers** such as
/// schema or entity constants (e.g. `ENTITY_ID`).
///
/// - ✅ **Deterministic** across compilers and platforms
/// - ✅ **`const fn`-compatible**, so hashes can be computed at compile time
/// - ⚙️ **Lightweight**: no dependencies, minimal CPU cost
/// - ⚠️ **Not cryptographically secure** — should *never* be used for
///   runtime routing, certified data, or signatures
///
/// For dynamic or security-sensitive hashing, use `xxhash64` (fast, uniform)
/// or `blake2b_256` (cryptographic) from `canic::hash`.
///
/// Reference: Fowler–Noll–Vo hash, FNV-1a variant (64-bit, prime = 0x100000001b3)
///
#[must_use]
#[allow(clippy::unreadable_literal)]
pub const fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    let mut i = 0;

    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(0x100000001b3);
        i += 1;
    }

    hash
}
