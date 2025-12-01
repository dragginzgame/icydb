use crate::{core::traits::Validator, prelude::*};
use canic_utils::case::{Case, Casing};

///
/// Kebab
///

#[validator]
pub struct Kebab {}

impl Validator<str> for Kebab {
    fn validate(&self, s: &str) -> Result<(), String> {
        if s.is_case(Case::Kebab) {
            Ok(())
        } else {
            Err(format!("'{s}' is not kebab-case"))
        }
    }
}

///
/// Lower
///

#[validator]
pub struct Lower {}

impl Validator<str> for Lower {
    fn validate(&self, s: &str) -> Result<(), String> {
        if s.is_case(Case::Lower) {
            Ok(())
        } else {
            Err(format!("'{s}' is not lower case"))
        }
    }
}

///
/// LowerUscore
///

#[validator]
pub struct LowerUscore {}

impl Validator<str> for LowerUscore {
    fn validate(&self, s: &str) -> Result<(), String> {
        if s.chars().all(|c| c.is_lowercase() || c == '_') {
            Ok(())
        } else {
            Err(format!("'{s}' is not lower case with_underscores"))
        }
    }
}

///
/// Snake
///

#[validator]
pub struct Snake {}

impl Validator<str> for Snake {
    fn validate(&self, s: &str) -> Result<(), String> {
        if s.is_case(Case::Snake) {
            Ok(())
        } else {
            Err(format!("'{s}' is not snake_case"))
        }
    }
}

///
/// Title
///

#[validator]
pub struct Title {}

impl Validator<str> for Title {
    fn validate(&self, s: &str) -> Result<(), String> {
        if s.is_case(Case::Title) {
            Ok(())
        } else {
            Err(format!("'{s}' Is Not Title Case"))
        }
    }
}

///
/// Upper
///

#[validator]
pub struct Upper {}

impl Validator<str> for Upper {
    fn validate(&self, s: &str) -> Result<(), String> {
        if s.is_case(Case::Upper) {
            Ok(())
        } else {
            Err(format!("'{s}' is not UPPER CASE"))
        }
    }
}

///
/// UpperCamel
///

#[validator]
pub struct UpperCamel {}

impl Validator<str> for UpperCamel {
    fn validate(&self, s: &str) -> Result<(), String> {
        if s.is_case(Case::UpperCamel) {
            Ok(())
        } else {
            Err(format!("'{s}' is not UpperCamelCase"))
        }
    }
}

///
/// UpperSnake
///

#[validator]
pub struct UpperSnake {}

impl Validator<str> for UpperSnake {
    fn validate(&self, s: &str) -> Result<(), String> {
        if s.is_case(Case::UpperSnake) {
            Ok(())
        } else {
            Err(format!("'{s}' is not UPPER_SNAKE_CASE"))
        }
    }
}
