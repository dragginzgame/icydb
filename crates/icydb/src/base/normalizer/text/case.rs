//! Module: base::normalizer::text::case
//!
//! Responsibility: base normalizer definitions.
//! Does not own: validation policy, persistence, or schema mutation semantics.
//! Boundary: mutates schema field values through facade normalizer traits.

use crate::{design::prelude::*, visitor::Normalizer};

use icydb_utils::{Case, Casing};

///
/// Kebab
///

#[normalizer]
pub struct Kebab;

impl Normalizer<String> for Kebab {
    fn normalize(&self, value: &mut String) -> Result<(), String> {
        *value = value.to_case(Case::Kebab);

        Ok(())
    }
}

///
/// Lower
///

#[normalizer]
pub struct Lower;

impl Normalizer<String> for Lower {
    fn normalize(&self, value: &mut String) -> Result<(), String> {
        // Unicode-aware lowercase; allocates
        *value = value.to_lowercase();

        Ok(())
    }
}

///
/// Snake
///

#[normalizer]
pub struct Snake;

impl Normalizer<String> for Snake {
    fn normalize(&self, value: &mut String) -> Result<(), String> {
        *value = value.to_case(Case::Snake);

        Ok(())
    }
}

///
/// Title
///

#[normalizer]
pub struct Title;

impl Normalizer<String> for Title {
    fn normalize(&self, value: &mut String) -> Result<(), String> {
        *value = value.to_case(Case::Title);

        Ok(())
    }
}

///
/// Upper
///

#[normalizer]
pub struct Upper;

impl Normalizer<String> for Upper {
    fn normalize(&self, value: &mut String) -> Result<(), String> {
        // Unicode-aware uppercase; allocates
        *value = value.to_uppercase();

        Ok(())
    }
}

///
/// UpperCamel
///

#[normalizer]
pub struct UpperCamel;

impl Normalizer<String> for UpperCamel {
    fn normalize(&self, value: &mut String) -> Result<(), String> {
        *value = value.to_case(Case::UpperCamel);

        Ok(())
    }
}

///
/// UpperSnake
///

#[normalizer]
pub struct UpperSnake;

impl Normalizer<String> for UpperSnake {
    fn normalize(&self, value: &mut String) -> Result<(), String> {
        *value = value.to_case(Case::UpperSnake);

        Ok(())
    }
}
