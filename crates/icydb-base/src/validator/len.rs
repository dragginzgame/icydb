#![allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]

use crate::{core::traits::Validator, prelude::*};
use std::{
    collections::{HashMap, HashSet},
    hash::BuildHasher,
};

///
/// HasLen
///

#[allow(clippy::len_without_is_empty)]
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
        Self::len(self)
    }
}

impl HasLen for String {
    fn len(&self) -> usize {
        Self::len(self)
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

///
/// Equal
///

#[validator]
pub struct Equal {
    target: i32,
}

impl Equal {
    pub fn new(target: impl Into<i32>) -> Self {
        let target = target.into();
        assert!(target >= 0, "equal target must be non-negative");

        Self { target }
    }
}

impl<T: HasLen + ?Sized> Validator<T> for Equal {
    fn validate(&self, t: &T) -> Result<(), String> {
        let len = t.len() as i32;

        if len == self.target {
            Ok(())
        } else {
            Err(format!("length ({}) is not equal to {}", len, self.target))
        }
    }
}

///
/// Min
///

#[validator]
pub struct Min {
    target: i32,
}

impl Min {
    pub fn new(target: impl Into<i32>) -> Self {
        let target = target.into();
        assert!(target >= 0, "min target must be non-negative");

        Self { target }
    }
}

impl<T: HasLen + ?Sized> Validator<T> for Min {
    fn validate(&self, t: &T) -> Result<(), String> {
        let len = t.len() as i32;

        if len < self.target {
            Err(format!(
                "length ({}) is lower than minimum of {}",
                len, self.target
            ))
        } else {
            Ok(())
        }
    }
}

///
/// Max
///

#[validator]
pub struct Max {
    target: i32,
}

impl Max {
    pub fn new(target: impl Into<i32>) -> Self {
        let target = target.into();
        assert!(target >= 0, "max target must be non-negative");

        Self { target }
    }
}

impl<T: HasLen + ?Sized> Validator<T> for Max {
    fn validate(&self, t: &T) -> Result<(), String> {
        let len = t.len() as i32;

        if len > self.target {
            Err(format!(
                "length ({}) is greater than maximum of {}",
                len, self.target
            ))
        } else {
            Ok(())
        }
    }
}

///
/// Range
///

#[validator]
pub struct Range {
    min: i32,
    max: i32,
}

impl Range {
    pub fn new(min: impl Into<i32>, max: impl Into<i32>) -> Self {
        let (min, max) = (min.into(), max.into());
        assert!(min >= 0, "min target must be non-negative");
        assert!(max >= 0, "max target must be non-negative");
        assert!(min <= max, "range requires min <= max");

        Self { min, max }
    }
}

impl<T: HasLen + ?Sized> Validator<T> for Range {
    fn validate(&self, t: &T) -> Result<(), String> {
        let len = t.len() as i32;

        if len < self.min || len > self.max {
            Err(format!(
                "length ({len}) must be between {} and {} (inclusive)",
                self.min, self.max
            ))
        } else {
            Ok(())
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_ok() {
        let r = Range::new(2, 5);
        assert!(r.validate("hey").is_ok()); // len = 3
    }

    #[test]
    fn test_range_err() {
        let r = Range::new(2, 5);
        assert!(r.validate("hello world").is_err()); // len = 11
    }
}
