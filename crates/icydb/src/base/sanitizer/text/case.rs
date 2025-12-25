use crate::{core::traits::Sanitizer, design::prelude::*};
use canic_utils::case::{Case, Casing};

///
/// Kebab
///

#[sanitizer]
pub struct Kebab;

impl Sanitizer<String> for Kebab {
    fn sanitize(&self, value: &mut String) -> Result<(), String> {
        *value = value.to_case(Case::Kebab);

        Ok(())
    }
}

///
/// Lower
///

#[sanitizer]
pub struct Lower;

impl Sanitizer<String> for Lower {
    fn sanitize(&self, value: &mut String) -> Result<(), String> {
        // Unicode-aware lowercase; allocates
        *value = value.to_lowercase();

        Ok(())
    }
}

///
/// Snake
///

#[sanitizer]
pub struct Snake;

impl Sanitizer<String> for Snake {
    fn sanitize(&self, value: &mut String) -> Result<(), String> {
        *value = value.to_case(Case::Snake);

        Ok(())
    }
}

///
/// Title
///

#[sanitizer]
pub struct Title;

impl Sanitizer<String> for Title {
    fn sanitize(&self, value: &mut String) -> Result<(), String> {
        *value = value.to_case(Case::Title);

        Ok(())
    }
}

///
/// Upper
///

#[sanitizer]
pub struct Upper;

impl Sanitizer<String> for Upper {
    fn sanitize(&self, value: &mut String) -> Result<(), String> {
        // Unicode-aware uppercase; allocates
        *value = value.to_uppercase();

        Ok(())
    }
}

///
/// UpperCamel
///

#[sanitizer]
pub struct UpperCamel;

impl Sanitizer<String> for UpperCamel {
    fn sanitize(&self, value: &mut String) -> Result<(), String> {
        *value = value.to_case(Case::UpperCamel);

        Ok(())
    }
}

///
/// UpperSnake
///

#[sanitizer]
pub struct UpperSnake;

impl Sanitizer<String> for UpperSnake {
    fn sanitize(&self, value: &mut String) -> Result<(), String> {
        *value = value.to_case(Case::UpperSnake);

        Ok(())
    }
}
