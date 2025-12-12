use crate::{core::traits::Sanitizer, prelude::*};
use canic_utils::case::{Case, Casing};

///
/// Kebab
///

#[sanitizer]
pub struct Kebab;

impl Sanitizer<String> for Kebab {
    fn sanitize(&self, value: String) -> String {
        value.to_case(Case::Kebab)
    }
}

///
/// Lower
///

#[sanitizer]
pub struct Lower;

impl Sanitizer<String> for Lower {
    fn sanitize(&self, value: String) -> String {
        value.to_lowercase()
    }
}

///
/// Snake
///

#[sanitizer]
pub struct Snake;

impl Sanitizer<String> for Snake {
    fn sanitize(&self, value: String) -> String {
        value.to_case(Case::Snake)
    }
}

///
/// Title
///

#[sanitizer]
pub struct Title;

impl Sanitizer<String> for Title {
    fn sanitize(&self, value: String) -> String {
        value.to_case(Case::Title)
    }
}

///
/// Upper
///

#[sanitizer]
pub struct Upper;

impl Sanitizer<String> for Upper {
    fn sanitize(&self, value: String) -> String {
        value.to_uppercase()
    }
}

///
/// UpperCamel
///

#[sanitizer]
pub struct UpperCamel;

impl Sanitizer<String> for UpperCamel {
    fn sanitize(&self, value: String) -> String {
        value.to_case(Case::UpperCamel)
    }
}

///
/// UpperSnake
///

#[sanitizer]
pub struct UpperSnake;

impl Sanitizer<String> for UpperSnake {
    fn sanitize(&self, value: String) -> String {
        value.to_case(Case::UpperSnake)
    }
}
