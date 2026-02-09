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
