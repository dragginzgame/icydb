use crate::{
    core::{traits::Validator, visitor::ValidateIssue},
    prelude::*,
};
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
    target: usize,
    #[serde(skip)]
    error: Option<ValidateIssue>,
}

impl Equal {
    pub fn new(target: impl TryInto<usize>) -> Self {
        match target.try_into() {
            Ok(target) => Self {
                target,
                error: None,
            },
            Err(_) => Self {
                target: 0,
                error: Some(ValidateIssue::invalid_config(
                    "Equal target must be non-negative",
                )),
            },
        }
    }
}

impl<T: HasLen + ?Sized> Validator<T> for Equal {
    fn validate(&self, t: &T) -> Result<(), ValidateIssue> {
        if let Some(err) = &self.error {
            return Err(err.clone());
        }

        let len = t.len();
        if len == self.target {
            Ok(())
        } else {
            Err(ValidateIssue::validation(format!(
                "length ({len}) is not equal to {}",
                self.target
            )))
        }
    }
}

///
/// Min
///

#[validator]
pub struct Min {
    target: usize,
    #[serde(skip)]
    error: Option<ValidateIssue>,
}

impl Min {
    pub fn new(target: impl TryInto<usize>) -> Self {
        match target.try_into() {
            Ok(target) => Self {
                target,
                error: None,
            },
            Err(_) => Self {
                target: 0,
                error: Some(ValidateIssue::invalid_config(
                    "Min target must be non-negative",
                )),
            },
        }
    }
}

impl<T: HasLen + ?Sized> Validator<T> for Min {
    fn validate(&self, t: &T) -> Result<(), ValidateIssue> {
        if let Some(err) = &self.error {
            return Err(err.clone());
        }

        let len = t.len();
        if len < self.target {
            Err(ValidateIssue::validation(format!(
                "length ({len}) is lower than minimum of {}",
                self.target
            )))
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
    target: usize,
    #[serde(skip)]
    error: Option<ValidateIssue>,
}

impl Max {
    pub fn new(target: impl TryInto<usize>) -> Self {
        match target.try_into() {
            Ok(target) => Self {
                target,
                error: None,
            },
            Err(_) => Self {
                target: 0,
                error: Some(ValidateIssue::invalid_config(
                    "Max target must be non-negative",
                )),
            },
        }
    }
}

impl<T: HasLen + ?Sized> Validator<T> for Max {
    fn validate(&self, t: &T) -> Result<(), ValidateIssue> {
        if let Some(err) = &self.error {
            return Err(err.clone());
        }

        let len = t.len();
        if len > self.target {
            Err(ValidateIssue::validation(format!(
                "length ({len}) is greater than maximum of {}",
                self.target
            )))
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
    min: usize,
    max: usize,
    #[serde(skip)]
    error: Option<ValidateIssue>,
}

impl Range {
    pub fn new(min: impl TryInto<usize>, max: impl TryInto<usize>) -> Self {
        let min = min.try_into();
        let max = max.try_into();

        match (min, max) {
            (Ok(min), Ok(max)) if min <= max => Self {
                min,
                max,
                error: None,
            },
            (Ok(_), Ok(_)) => Self {
                min: 0,
                max: 0,
                error: Some(ValidateIssue::invalid_config("range requires min <= max")),
            },
            _ => Self {
                min: 0,
                max: 0,
                error: Some(ValidateIssue::invalid_config(
                    "range bounds must be non-negative",
                )),
            },
        }
    }
}

impl<T: HasLen + ?Sized> Validator<T> for Range {
    fn validate(&self, t: &T) -> Result<(), ValidateIssue> {
        if let Some(err) = &self.error {
            return Err(err.clone());
        }

        let len = t.len();
        if len < self.min || len > self.max {
            Err(ValidateIssue::validation(format!(
                "length ({len}) must be between {} and {} (inclusive)",
                self.min, self.max
            )))
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
        assert!(r.validate("hey").is_ok());
    }

    #[test]
    fn test_range_err() {
        let r = Range::new(2, 5);
        assert!(r.validate("hello world").is_err());
    }

    #[test]
    fn test_invalid_range_config() {
        let r = Range::new(5, 2);
        assert!(r.validate("hey").is_err());
    }
}
