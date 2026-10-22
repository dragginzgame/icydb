//! Module: types::identity::generate_key
//! Defines the marker trait for primary-key types that can be generated
//! locally by runtime code.

///
/// GenerateKey
///
/// Marker trait for primary-key types that can be generated locally.
/// This is intentionally not implemented for externally supplied keys
/// (for example `Principal`, `String`, or small numeric primitives).
///

pub trait GenerateKey: Sized {
    /// Generate a new key value for the implementing primary-key type.
    ///
    /// Generated keys are public identifiers, not authorization tokens. On IC,
    /// [`crate::types::Ulid`] generation is deterministic: its timestamp and
    /// process-local sequence are predictable, including after startup entropy
    /// has been supplied. Native entropy-backed generation does not establish
    /// an unpredictability guarantee for deployed canisters.
    ///
    /// # Errors
    ///
    /// Returns an internal generation error when the runtime cannot obtain
    /// entropy or exhausts the generator's monotonic sequence.
    fn generate() -> Result<Self, crate::error::InternalError>;
}
