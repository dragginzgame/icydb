///
/// ContinuationSignature
///
/// Stable, deterministic hash of continuation-relevant plan semantics.
/// Excludes windowing state (`limit`, `offset`) and cursor boundaries.
/// Hex/display formatting is codec-owned in `db::codec::cursor`.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ContinuationSignature([u8; 32]);

impl ContinuationSignature {
    pub(crate) const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(crate) const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}
