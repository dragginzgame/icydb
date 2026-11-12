//! Module: base::validator::len
//!
//! Responsibility: base validator definitions.
//! Does not own: normalization policy, persistence, or schema mutation semantics.
//! Boundary: reports typed visitor issues for facade schema values.

use crate::{prelude::*, visitor::Validator};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    hash::BuildHasher,
};

///
/// HasLen
///

#[expect(clippy::len_without_is_empty)]
pub trait HasLen {
    fn len(&self) -> usize;
}

impl HasLen for Blob {
    fn len(&self) -> usize {
        Self::len(self)
    }
}

impl HasLen for str {
    fn len(&self) -> usize {
        self.chars().count()
    }
}

impl HasLen for String {
    fn len(&self) -> usize {
        self.chars().count()
    }
}

impl<T> HasLen for [T] {
    fn len(&self) -> usize {
        <[T]>::len(self)
    }
}

impl<T> HasLen for Vec<T> {
    fn len(&self) -> usize {
        Self::len(self)
    }
}

impl<T, S: BuildHasher> HasLen for HashSet<T, S> {
    fn len(&self) -> usize {
        Self::len(self)
    }
}

impl<K, V, S: BuildHasher> HasLen for HashMap<K, V, S> {
    fn len(&self) -> usize {
        Self::len(self)
    }
}

impl<T> HasLen for BTreeSet<T> {
    fn len(&self) -> usize {
        Self::len(self)
    }
}

impl<K, V> HasLen for BTreeMap<K, V> {
    fn len(&self) -> usize {
        Self::len(self)
    }
}

//
// ============================================================================
// Equal
// ============================================================================
//

#[validator]
pub struct Equal {
    target: usize,
}

impl Equal {
    pub fn new(target: impl TryInto<usize>) -> Self {
        Self {
            target: target.try_into().unwrap_or_default(),
        }
    }
}

impl<T: HasLen + ?Sized> Validator<T> for Equal {
    fn validate(&self, t: &T, ctx: &mut dyn VisitorContext) {
        let len = t.len();

        if len != self.target {
            ctx.issue(format!("length ({len}) is not equal to {}", self.target));
        }
    }
}

///
/// Min
///

#[validator]
pub struct Min {
    target: usize,
}

impl Min {
    pub fn new(target: impl TryInto<usize>) -> Self {
        Self {
            target: target.try_into().unwrap_or_default(),
        }
    }
}

impl<T: HasLen + ?Sized> Validator<T> for Min {
    fn validate(&self, t: &T, ctx: &mut dyn VisitorContext) {
        let len = t.len();

        if len < self.target {
            ctx.issue(format!(
                "length ({len}) is lower than minimum of {}",
                self.target
            ));
        }
    }
}

///
/// Max
///

#[validator]
pub struct Max {
    target: usize,
}

impl Max {
    pub fn new(target: impl TryInto<usize>) -> Self {
        Self {
            target: target.try_into().unwrap_or_default(),
        }
    }
}

impl<T: HasLen + ?Sized> Validator<T> for Max {
    fn validate(&self, t: &T, ctx: &mut dyn VisitorContext) {
        let len = t.len();

        if len > self.target {
            ctx.issue(format!(
                "length ({len}) is greater than maximum of {}",
                self.target
            ));
        }
    }
}

///
/// Range
///

#[validator]
pub struct Range {
    min: usize,
    max: usize,
}

impl Range {
    pub fn new(min: impl TryInto<usize>, max: impl TryInto<usize>) -> Self {
        Self {
            min: min.try_into().unwrap_or_default(),
            max: max.try_into().unwrap_or_default(),
        }
    }
}

impl<T: HasLen + ?Sized> Validator<T> for Range {
    fn validate(&self, t: &T, ctx: &mut dyn VisitorContext) {
        let len = t.len();

        if len < self.min || len > self.max {
            ctx.issue(format!(
                "length ({len}) must be between {} and {} (inclusive)",
                self.min, self.max
            ));
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
