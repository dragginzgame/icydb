use crate::db::identity::IndexName;
use derive_more::Display;

///
/// IndexId
///
/// Logical identifier for an index.
/// Combines entity identity and indexed field set into a stable, ordered name.
/// Used as the prefix component of all index keys.
///

#[derive(Clone, Copy, Debug, Display, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct IndexId(pub(crate) IndexName);

impl IndexId {
    /// Maximum sentinel value for test-only stable-memory bound checks.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn max_storable() -> Self {
        Self(IndexName::max_storable())
    }
}
