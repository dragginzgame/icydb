//! Module: index::key
//! Responsibility: canonical index-key construction and raw encoding layers.
//! Does not own: index-store scanning or unique-constraint policy.
//! Boundary: used by planner/store/range modules as key authority.

mod build;
mod codec;
mod id;
mod ordered;

#[cfg(test)]
mod tests;

pub(crate) use codec::{IndexKey, IndexKeyKind, RawIndexKey};
pub(crate) use id::IndexId;
pub(crate) use ordered::{EncodedValue, OrderedValueEncodeError};
