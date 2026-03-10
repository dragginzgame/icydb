//! Module: types::identity::generate_key
//! Responsibility: module-local ownership and contracts for types::identity::generate_key.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

///
/// GenerateKey
///
/// Marker trait for primary-key types that can be generated locally.
/// This is intentionally not implemented for externally supplied keys
/// (for example `Principal`, `String`, or small numeric primitives).
///

pub trait GenerateKey: Sized {
    /// Generate a new key value for the implementing primary-key type.
    fn generate() -> Self;
}
